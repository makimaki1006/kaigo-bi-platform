"""決算PDF 層化サンプリング＋ダウンロード

目的: 「どの項目なら網羅的に構造化できるか」を判断するための実測サンプルを作る。
      法人種別 × 会計種類 で層化し、PL/BS/CF を取得する。

実行:
  python scripts/fin_sample_download.py [--per-stratum 12] [--parallel 8]

出力:
  data/financial_survey/pdf/{事業所番号}_{PL|BS|CF}.pdf
  data/financial_survey/manifest.csv
"""
import argparse
import csv
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests

from fin_common import ROOT, query, to_abs_url

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

OUT = ROOT / "data" / "financial_survey"
PDF_DIR = OUT / "pdf"
MANIFEST = OUT / "manifest.csv"

DOC_COLS = {
    "PL": "財務DL_事業活動計算書",
    "CF": "財務DL_資金収支計算書",
    "BS": "財務DL_貸借対照表",
}

CORP_TYPES = ["株式会社・有限会社等", "社会福祉法人", "医療法人",
              "その他法人", "NPO法人", "社会医療法人", "社団法人", "財団法人"]

ACCT_TYPES = ["社会福祉法人会計基準", "病院会計準則及び医療法人会計基準",
              "その他（企業会計原則、公益法人会計基準 等）", "ＮＰＯ法人会計基準",
              "介護老人保健施設会計・経理準則及び介護医療院会計・経理準則", "企業会計"]

# 抽選は主キー1列だけで行う。URL列まで含めて ORDER BY RANDOM() すると
# 223,103行のソートに長大なTEXTが乗って数分返らない（実測でタイムアウト）。
PICK = """
SELECT "事業所番号" AS jigyosho FROM facilities
WHERE COALESCE("財務DL_事業活動計算書",'') != ''
"""

META = """
SELECT "事業所番号" AS jigyosho, "事業所名" AS fac_name, "法人名" AS corp_name,
       "法人番号" AS corp_number, corp_type, prefecture, "サービス名" AS service,
       "サービスコード" AS service_code, "会計種類" AS acct, "定員" AS capacity,
       "従業者_合計" AS staff
FROM facilities WHERE "事業所番号" IN ({})
"""

_tl = threading.local()


def load_urls(ids):
    """URLはローカルの unei_results.csv から引く（Tursoから長大TEXTを引かない）"""
    src = ROOT / "data" / "output" / "unei_financial" / "unei_results.csv"
    want, out = set(ids), {}
    with open(src, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            j = row["jigyosho_number"]
            if j in want and j not in out:
                out[j] = {"pl": row["財務DL_事業活動計算書"],
                          "cf": row["財務DL_資金収支計算書"],
                          "bs": row["財務DL_貸借対照表"]}
    return out


def session():
    if not hasattr(_tl, "s"):
        s = requests.Session()
        s.headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) kaigo-bi survey"
        _tl.s = s
    return _tl.s


def collect_targets(per_stratum: int):
    stratum_of, order = {}, []

    def pick(where, args, name):
        rows = query(PICK + where + " ORDER BY RANDOM() LIMIT ?",
                     args=args + [{"type": "integer", "value": str(per_stratum)}], timeout=300)
        new = 0
        for r in rows:
            j = r["jigyosho"]
            if j not in stratum_of:
                stratum_of[j] = name
                order.append(j)
                new += 1
        logger.info("層 %-34s -> %s件（新規 %s）", name[:34], len(rows), new)

    for ct in CORP_TYPES:
        pick(" AND corp_type = ?", [{"type": "text", "value": ct}], f"corp:{ct}")
    for at in ACCT_TYPES:
        pick(' AND "会計種類" = ?', [{"type": "text", "value": at}], f"acct:{at}")
    pick(' AND "会計種類" IS NULL', [], "acct:未記載")

    logger.info("抽選 %s 件。メタデータ取得中...", len(order))
    meta = {}
    for i in range(0, len(order), 200):
        chunk = order[i:i + 200]
        rows = query(META.format(",".join("?" * len(chunk))),
                     args=[{"type": "text", "value": j} for j in chunk], timeout=300)
        for r in rows:
            meta[r["jigyosho"]] = r

    logger.info("URL突合中（unei_results.csv）...")
    urls = load_urls(order)

    targets = []
    for j in order:
        rec = meta.get(j)
        if not rec or j not in urls:
            continue
        rec.update(urls[j])
        rec["stratum"] = stratum_of[j]
        targets.append(rec)
    return targets


def fetch(rec, doc_type, delay):
    rel = rec.get(doc_type.lower())
    if not rel:
        return None
    time.sleep(delay)
    url = to_abs_url(rel)
    dest = PDF_DIR / f"{rec['jigyosho']}_{doc_type}.pdf"
    status, size = "skip(exists)", (dest.stat().st_size if dest.exists() else 0)
    if not dest.exists():
        try:
            r = session().get(url, timeout=90)
            if r.status_code == 200 and r.content[:4] == b"%PDF":
                dest.write_bytes(r.content)
                status, size = "ok", len(r.content)
            else:
                status = f"NG(http={r.status_code})"
        except Exception as e:  # noqa: BLE001
            status = f"ERROR({type(e).__name__})"
    row = dict(rec)
    row.pop("pl", None); row.pop("bs", None); row.pop("cf", None)
    row.update(doc_type=doc_type, url=url, status=status, bytes=size,
               file=dest.name if dest.exists() else "")
    return row


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--per-stratum", type=int, default=12)
    ap.add_argument("--parallel", type=int, default=8)
    ap.add_argument("--delay", type=float, default=0.8)
    args = ap.parse_args()

    PDF_DIR.mkdir(parents=True, exist_ok=True)
    targets = collect_targets(args.per_stratum)
    logger.info("対象施設 %s 件", len(targets))

    jobs = [(t, d) for t in targets for d in ("PL", "BS", "CF") if t.get(d.lower())]
    logger.info("ダウンロード対象 %s ファイル", len(jobs))

    rows, done = [], 0
    with ThreadPoolExecutor(max_workers=args.parallel) as pool:
        futs = [pool.submit(fetch, t, d, args.delay) for t, d in jobs]
        for f in as_completed(futs):
            r = f.result()
            done += 1
            if r:
                rows.append(r)
            if done % 50 == 0:
                logger.info("進捗 %s/%s", done, len(jobs))

    fields = ["jigyosho", "fac_name", "corp_name", "corp_number", "corp_type", "prefecture",
              "service", "service_code", "acct", "capacity", "staff", "stratum",
              "doc_type", "url", "status", "bytes", "file"]
    with open(MANIFEST, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)

    ok = sum(1 for r in rows if r["status"].startswith(("ok", "skip")))
    logger.info("完了 %s/%s 成功。manifest=%s", ok, len(rows), MANIFEST)


if __name__ == "__main__":
    main()
