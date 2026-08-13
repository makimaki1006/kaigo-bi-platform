"""決算PDF 金額抽出バッチ → financials テーブル

前提（実測 2026-08-12、層化サンプル518ファイル / rule_v2）:
  - 決算PDFは自由書式のアップロード。約半数(47.5%)しかテキスト層がない
  - 取れるのは PL収益 28.4% / BS総資産+純資産 27.2%
  - 検算（資産=負債+純資産）の一致率は 90.4%。取れたときの値はおおむね正しい
  - よって「取れた分だけを、根拠付きで」入れる。取れないものは無理に埋めない

rule_v2 での改善（v1比）:
  - 2段組の貸借対照表をページ中央で分割してから行を組み直す
    （「資産の部合計｜負債純資産の部合計」が1行に結合されて取れなかった）
  - 表ごとに列見出しを判定する（左右まとめると右表の値が左表の列に割り当たる）

このスクリプトは取れたものだけを financials に入れ、
    extraction_method / text_layer / identity_ok / unit_source
などの来歴を必ず一緒に記録する。UI側はこれを見て出し分ける。

実行:
  python scripts/fin_extract_batch.py --from-dir data/financial_survey/pdf   # DL済みを解析
  python scripts/fin_extract_batch.py --limit 500 --parallel 8               # 未処理をDLして解析
  python scripts/fin_extract_batch.py --limit 500 --dry-run                  # 書き込まない
"""
import argparse
import csv
import hashlib
import json
import logging
import re
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import fitz
import requests

from fin_analyze_pdfs import (ACCT_SIGNATURES, ALL_CONCEPTS, FORM_PATTERNS,
                              PERIOD_RE, SCOPE_PATTERNS, UNIT_PATTERNS,
                              YEAR_ONLY_RE, detect_columns, page_rows,
                              page_rows_split, pick_value)
from fin_common import ROOT, query, to_abs_url, turso

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

EXTRA_KEYS = ["depreciation", "interest_expense", "jigyohi", "jimuhi", "sga", "subsidy",
              "kaigo_revenue", "expense_total", "expense_service", "current_assets",
              "fixed_assets", "current_liabilities", "fixed_liabilities", "cash",
              "short_debt", "long_debt", "cf_op_in", "cf_op_out", "cf_op_net",
              "cf_inv_net", "cf_fin_net", "cf_net", "cf_begin", "cf_end"]

MIGRATIONS = [
    "ALTER TABLE financials ADD COLUMN fiscal_year INTEGER",
    "ALTER TABLE financials ADD COLUMN unit TEXT",
    "ALTER TABLE financials ADD COLUMN unit_source TEXT",
    "ALTER TABLE financials ADD COLUMN scope TEXT",
    "ALTER TABLE financials ADD COLUMN form TEXT",
    "ALTER TABLE financials ADD COLUMN acct_standard TEXT",
    "ALTER TABLE financials ADD COLUMN extraction_method TEXT",
    "ALTER TABLE financials ADD COLUMN text_layer INTEGER",
    "ALTER TABLE financials ADD COLUMN identity_ok INTEGER",
    "ALTER TABLE financials ADD COLUMN uploaded_at TEXT",
    "ALTER TABLE financials ADD COLUMN content_hash TEXT",
    "ALTER TABLE financials ADD COLUMN extra_json TEXT",
]

_tl = threading.local()


def keep_awake(on: bool):
    """実行中はWindowsのアイドルスリープを抑止する。

    このバッチは10万ファイル規模で3時間ほどかかる。放置中にスリープすると
    プロセスが落ちて途中で止まる（2026-08-12 に実際に発生）。
    ただし**ノートを閉じた場合のスリープは抑止できない**（電源設定側の動作）。
    閉じて落ちたときは同じコマンドで再実行すれば、済んだ分はスキップして再開する。
    """
    if not sys.platform.startswith("win"):
        return
    try:
        import ctypes
        ES_CONTINUOUS = 0x80000000
        ES_SYSTEM_REQUIRED = 0x00000001
        flags = (ES_CONTINUOUS | ES_SYSTEM_REQUIRED) if on else ES_CONTINUOUS
        ctypes.windll.kernel32.SetThreadExecutionState(flags)
    except Exception as e:  # noqa: BLE001
        logger.warning("スリープ抑止を設定できませんでした: %s", e)


def session():
    if not hasattr(_tl, "s"):
        s = requests.Session()
        s.headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) kaigo-bi extract"
        _tl.s = s
    return _tl.s


def migrate():
    from turso_helpers import execute_single
    url, headers = turso()
    for sql in MIGRATIONS:
        try:
            execute_single(url, headers, sql)
        except Exception as e:  # 既に存在する場合は duplicate column name
            if "duplicate column" not in str(e).lower():
                logger.warning("migration: %s", str(e)[:120])
    logger.info("financials スキーマ確認完了")


# ---------------------------------------------------------------
# 抽出本体
# ---------------------------------------------------------------

def extract_from_bytes(content: bytes, doc_type: str) -> dict:
    """PDFバイト列から1レコード分を抽出する"""
    out = {"doc_type": doc_type, "text_layer": 0, "extraction_method": EXTRACTOR_VERSION}
    out["content_hash"] = hashlib.md5(content).hexdigest()

    try:
        doc = fitz.open(stream=content, filetype="pdf")
    except Exception as e:  # noqa: BLE001
        out["notes"] = f"PDFを開けない: {type(e).__name__}"
        out["confidence"] = "low"
        return out

    texts, rows, split_groups = [], [], []
    for page in doc:
        texts.append(page.get_text())
        rows.extend(page_rows(page))
        split_groups.extend(page_rows_split(page))   # 2段組の貸借対照表対策
    doc.close()

    text = "\n".join(texts)
    import unicodedata
    ntext = unicodedata.normalize("NFKC", text).replace(" ", "")
    out["text_layer"] = 1 if len(text.strip()) >= 200 else 0
    if not out["text_layer"]:
        out["notes"] = "スキャン画像のみ（テキスト層なし）。金額は未抽出。原本PDFを参照。"
        out["confidence"] = "low"
        return out

    # テキスト由来のハッシュ（同一法人が事業所ごとに再アップロードしても一致する）
    out["content_hash"] = hashlib.md5(re.sub(r"\s+", "", ntext).encode()).hexdigest()

    forms = [n for n, p in FORM_PATTERNS if re.search(p, ntext)]
    out["form"] = "/".join(forms[:3]) or None
    accts = [n for n, p in ACCT_SIGNATURES if re.search(p, ntext)]
    out["acct_standard"] = accts[0] if accts else None
    scopes = [n for n, p in SCOPE_PATTERNS if re.search(p, ntext)]
    out["scope"] = scopes[0] if scopes else None

    m = PERIOD_RE.search(ntext)
    if m:
        era, y = m.group(1), m.group(2)
        yy = 1 if y == "元" else int(y)
        out["fiscal_period"] = m.group(0)[:60]
        out["fiscal_year"] = (2018 + yy) if era == "令和" else (1988 + yy)
    else:
        m2 = YEAR_ONLY_RE.search(ntext)
        if m2:
            yy = 1 if m2.group(2) == "元" else int(m2.group(2))
            out["fiscal_period"] = m2.group(0)
            out["fiscal_year"] = (2018 + yy) if m2.group(1) == "令和" else (1988 + yy)

    unit = next((n for n, p in UNIT_PATTERNS if re.search(p, ntext)), None)
    cols = detect_columns(rows)

    # 表ごとに列見出しを判定する（2段組を左右まとめて扱うと列がずれる）
    groups = [(rows, cols)] + [(g, detect_columns(g)) for g in split_groups]
    hits = {}
    for rows_g, cols_g in groups:
        for r in rows_g:
            if not r["nums"] or len(r["label"]) < 2:
                continue
            for concepts in ALL_CONCEPTS.values():
                for key, pat in concepts.items():
                    if re.fullmatch(pat, r["label"]):
                        v, _, src = pick_value(r["nums"], cols_g)
                        if v is None:
                            continue
                        prev = hits.get(key)
                        if prev is None or abs(v) > abs(prev[0]):
                            hits[key] = (v, r["label"], src)

    # 資金収支計算書の「当期収支差額」は資金の増減であって損益ではない。
    # これを当期純利益として入れると、資金繰りの数字が利益として表示されてしまう
    # （実測例: 3571501950 の CF に net_income=6,000 が入っていた）。
    # 収支計算書をPL代わりに使う小規模法人もあるので、CF書類のときだけ落とす。
    if doc_type == "CF" and "net_income" in hits and "収支差額" in hits["net_income"][1]:
        hits.pop("net_income")

    vals = {k: v[0] for k, v in hits.items()}

    # 単位。PDFに明記がなければ金額の桁から推定する（1000倍の取り違えを避ける）
    revenue = vals.get("revenue_service") or vals.get("revenue_total")
    if unit:
        unit_source = "pdf"
    elif revenue is not None and abs(revenue) < 1_000_000:
        unit, unit_source = "千円", "inferred"
    else:
        unit, unit_source = "円", "assumed"
    factor = {"円": 1, "千円": 1000, "百万円": 1_000_000}.get(unit, 1)
    out["unit"], out["unit_source"] = unit, unit_source

    def scaled(key):
        v = vals.get(key)
        return None if v is None else v * factor

    out["revenue"] = scaled("revenue_service") if "revenue_service" in vals else scaled("revenue_total")
    out["personnel_cost"] = scaled("personnel_cost")
    out["operating_income"] = scaled("operating_income")
    out["ordinary_income"] = scaled("ordinary_income")
    out["net_income"] = scaled("net_income")
    out["total_assets"] = scaled("total_assets")
    out["net_assets"] = scaled("net_assets")
    out["total_liabilities"] = scaled("total_liabilities")

    extra = {k: scaled(k) for k in EXTRA_KEYS if k in vals}
    extra["_labels"] = {k: hits[k][1] for k in hits}
    extra["_pick"] = {k: hits[k][2] for k in hits}
    out["extra_json"] = json.dumps(extra, ensure_ascii=False)

    # 恒等式チェック: 資産 = 負債 + 純資産
    a, l, ne = out["total_assets"], out["total_liabilities"], out["net_assets"]
    if a and l is not None and ne is not None:
        out["identity_ok"] = 1 if abs((l + ne) - a) / max(abs(a), 1) < 0.01 else 0
    else:
        out["identity_ok"] = None

    n_found = sum(1 for k in ("revenue", "personnel_cost", "operating_income", "net_income",
                              "total_assets", "net_assets") if out.get(k) is not None)
    if out["identity_ok"] == 1 or n_found >= 4:
        out["confidence"] = "high"
    elif n_found >= 2:
        out["confidence"] = "medium"
    else:
        out["confidence"] = "low"

    notes = []
    if out["identity_ok"] == 0:
        notes.append("資産≠負債+純資産（抽出値のいずれかが誤り）")
    if unit_source != "pdf":
        notes.append(f"単位はPDFに明記なし（{unit}と{'推定' if unit_source=='inferred' else '仮定'}）")
    if not out.get("fiscal_year"):
        notes.append("会計期間の記載を特定できず")
    if not out["scope"]:
        notes.append("法人全体か拠点単位かの記載なし")
    if n_found == 0:
        notes.append("標準的な勘定科目名に一致せず（自由書式）")
    out["notes"] = " / ".join(notes) or None
    return out


# ---------------------------------------------------------------

EXTRACTOR_VERSION = "rule_v3"


def load_done():
    """現行バージョンで処理済みのものだけをスキップ対象にする。

    旧バージョン(rule_v1)のレコードは作り直す。
    抽出器を上げたら自動で再処理されるので、長時間バッチが途中で落ちても
    そのまま同じコマンドで再開できる。
    """
    done, old = set(), 0
    for r in query("SELECT jigyosho_number, doc_type, extraction_method FROM financials",
                   timeout=300):
        if r.get("extraction_method") == EXTRACTOR_VERSION:
            done.add((r["jigyosho_number"], r["doc_type"]))
        else:
            old += 1
    logger.info("%s で処理済み %s 件をスキップ（旧バージョン %s 件は作り直す）",
                EXTRACTOR_VERSION, len(done), old)
    return done


def load_urls_from_csv(ids):
    """URLはローカルの unei_results.csv から引く。
    Tursoから14万件分の長大URLをHTTPで引くと数十MBになるため。"""
    src = ROOT / "data" / "output" / "unei_financial" / "unei_results.csv"
    want, out = set(ids), {}
    with open(src, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            j = row["jigyosho_number"]
            if j in want and j not in out:
                out[j] = {"PL": row["財務DL_事業活動計算書"],
                          "BS": row["財務DL_貸借対照表"],
                          "CF": row["財務DL_資金収支計算書"]}
    return out


def targets_from_db(limit, skip_done):
    """法人単位で1施設に絞った対象リストを作る。

    同一法人は同じ決算書を事業所ごとに再アップロードしている（テキストが完全一致する）
    ので、法人番号で重複排除する。法人番号が空なら事業所番号を代用。
    """
    done = load_done() if skip_done else set()

    # 主キーだけをページングで取る（GROUP BY をサーバ側でやるとURL列が乗って重い）
    rows, offset, page = [], 0, 20000
    while True:
        chunk = query(f"""
            SELECT "事業所番号" j, "法人番号" cn FROM facilities
            WHERE COALESCE("財務DL_事業活動計算書",'') != ''
            ORDER BY rowid LIMIT {page} OFFSET {offset}
        """, timeout=300)
        rows.extend(chunk)
        logger.info("  対象取得 %s件", len(rows))
        if len(chunk) < page:
            break
        offset += page

    seen, picked = set(), []
    for r in rows:
        key = r["cn"] or r["j"]
        if key in seen:
            continue
        seen.add(key)
        picked.append(r)
        if limit and len(picked) >= limit:
            break
    logger.info("法人単位に重複排除: %s → %s", len(rows), len(picked))

    urls = load_urls_from_csv([p["j"] for p in picked])
    out = []
    for p in picked:
        u = urls.get(p["j"])
        if not u:
            continue
        for dt in ("PL", "BS", "CF"):
            if u[dt] and (p["j"], dt) not in done:
                out.append({"jigyosho": p["j"], "corp_number": p["cn"],
                            "doc_type": dt, "url": to_abs_url(u[dt])})
    return out


def targets_from_dir(d):
    manifest = {}
    mpath = ROOT / "data" / "financial_survey" / "manifest.csv"
    if mpath.exists():
        with open(mpath, encoding="utf-8-sig") as f:
            for r in csv.DictReader(f):
                manifest[r["file"]] = r
    out = []
    for p in sorted(Path(ROOT / d).glob("*.pdf")):
        jig, dt = p.stem.rsplit("_", 1)
        m = manifest.get(p.name, {})
        out.append({"jigyosho": jig, "doc_type": dt, "path": p,
                    "corp_number": m.get("corp_number"), "url": m.get("url")})
    return out


def process(t):
    try:
        if t.get("path"):
            content = Path(t["path"]).read_bytes()
        else:
            r = session().get(t["url"], timeout=90)
            if r.status_code != 200 or r.content[:4] != b"%PDF":
                return {"jigyosho_number": t["jigyosho"], "doc_type": t["doc_type"],
                        "skip": f"http={r.status_code}/not-pdf"}
            content = r.content
            time.sleep(0.4)
    except Exception as e:  # noqa: BLE001
        return {"jigyosho_number": t["jigyosho"], "doc_type": t["doc_type"],
                "skip": f"{type(e).__name__}"}

    rec = extract_from_bytes(content, t["doc_type"])
    rec["jigyosho_number"] = t["jigyosho"]
    rec["corp_number"] = t.get("corp_number")
    rec["source_url"] = t.get("url")
    if t.get("url"):
        m = re.search(r"\?(\d{10})$", t["url"])
        if m:
            rec["uploaded_at"] = time.strftime("%Y-%m-%d", time.localtime(int(m.group(1))))
    return rec


COLS = ["id", "jigyosho_number", "corp_number", "doc_type", "fiscal_period", "revenue",
        "personnel_cost", "operating_income", "ordinary_income", "net_income",
        "total_assets", "net_assets", "total_liabilities", "confidence", "source_url",
        "notes", "fiscal_year", "unit", "unit_source", "scope", "form", "acct_standard",
        "extraction_method", "text_layer", "identity_ok", "uploaded_at", "content_hash",
        "extra_json"]


def write_rows(records):
    from turso_helpers import execute_sql, make_arg
    url, headers = turso()
    updates = ", ".join(f"{c} = excluded.{c}" for c in COLS if c not in ("id", "jigyosho_number", "doc_type"))
    sql = (f"INSERT INTO financials ({', '.join(COLS)}) VALUES ({', '.join('?' * len(COLS))}) "
           f"ON CONFLICT (jigyosho_number, doc_type) DO UPDATE SET {updates}, "
           f"extracted_at = datetime('now')")
    def stmt_for(rec):
        args = [make_arg(str(uuid.uuid4()))]
        for c in COLS[1:]:
            args.append(make_arg(rec.get(c)))
        return {"type": "execute", "stmt": {"sql": sql, "args": args}}

    written = errors = 0
    for i in range(0, len(records), 40):
        chunk = records[i:i + 40]
        try:
            res = execute_sql(url, headers, [stmt_for(r) for r in chunk], timeout=180)
        except Exception as e:  # noqa: BLE001
            # 1件でも不正値があるとチャンク全体が400で落ちる。
            # ここで例外を通すと数時間分の処理が巻き添えになるので、
            # 1件ずつ入れ直して壊れたレコードだけを捨てる
            logger.warning("チャンク書き込み失敗（1件ずつ再試行）: %s", str(e)[:160])
            for rec in chunk:
                try:
                    execute_sql(url, headers, [stmt_for(rec)], timeout=120)
                    written += 1
                except Exception as e2:  # noqa: BLE001
                    errors += 1
                    logger.error("  スキップ %s/%s: %s",
                                 rec.get("jigyosho_number"), rec.get("doc_type"),
                                 str(e2)[:120])
            continue
        for r in res.get("results", []):
            if "error" in r:
                errors += 1
                if errors <= 3:
                    logger.error("  %s", r["error"]["message"][:200])
            else:
                written += 1
    return written, errors


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-dir")
    ap.add_argument("--limit", type=int, default=300)
    ap.add_argument("--parallel", type=int, default=8)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-skip-done", action="store_true")
    args = ap.parse_args()

    keep_awake(True)
    if not args.dry_run:
        migrate()

    targets = targets_from_dir(args.from_dir) if args.from_dir else \
        targets_from_db(args.limit, not args.no_skip_done)
    logger.info("対象 %s ファイル", len(targets))

    # 全件をメモリに溜めてから書くと、途中で落ちたとき数時間分が消える。
    # 一定件数ごとに流し込む（ON CONFLICT で冪等なので再実行しても安全）
    FLUSH = 2000
    buf, stats = [], {"n": 0, "skip": 0, "tl": 0, "rev": 0, "idok": 0,
                      "high": 0, "medium": 0, "low": 0, "written": 0, "err": 0}
    start = time.time()
    samples = []

    def flush():
        if not buf or args.dry_run:
            return
        w, e = write_rows(buf)
        stats["written"] += w
        stats["err"] += e
        buf.clear()

    with ThreadPoolExecutor(max_workers=args.parallel) as pool:
        futs = [pool.submit(process, t) for t in targets]
        for k, f in enumerate(as_completed(futs), 1):
            r = f.result()
            if r.get("skip"):
                stats["skip"] += 1
            else:
                buf.append(r)
                stats["n"] += 1
                if r.get("text_layer"):
                    stats["tl"] += 1
                if r.get("revenue") is not None:
                    stats["rev"] += 1
                    if len(samples) < 3:
                        samples.append(r)
                if r.get("identity_ok") == 1:
                    stats["idok"] += 1
                stats[r.get("confidence", "low")] = stats.get(r.get("confidence", "low"), 0) + 1
                if len(buf) >= FLUSH:
                    flush()
            if k % 500 == 0:
                el = time.time() - start
                eta = (len(targets) - k) / (k / el) / 3600 if el else 0
                logger.info("  %s/%s (%.1f件/s, 残り%.1fh) テキスト層%s revenue%s 書込%s",
                            k, len(targets), k / el if el else 0, eta,
                            stats["tl"], stats["rev"], stats["written"])
    flush()

    n = max(stats["n"], 1)
    logger.info("=== 完了 ===")
    logger.info("解析 %s件 / DL失敗 %s件", stats["n"], stats["skip"])
    logger.info("  テキスト層あり %s (%.1f%%)", stats["tl"], stats["tl"] / n * 100)
    logger.info("  revenue抽出   %s (%.1f%%)", stats["rev"], stats["rev"] / n * 100)
    logger.info("  恒等式一致    %s", stats["idok"])
    for c in ("high", "medium", "low"):
        logger.info("  confidence=%-6s %s", c, stats.get(c, 0))

    if args.dry_run:
        logger.info("dry-run のため書き込みなし")
        print(json.dumps(samples, ensure_ascii=False, indent=1)[:2000])
        return

    logger.info("financials 書き込み: %s件成功 / %s件エラー", stats["written"], stats["err"])
    keep_awake(False)


if __name__ == "__main__":
    main()
