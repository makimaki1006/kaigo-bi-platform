"""決算PDF 実測解析

やること:
  1. テキスト層の有無を測る（スキャン画像なら機械抽出は不可能）
  2. 様式・会計基準・集計単位（法人全体/拠点区分/サービス区分）を判定
  3. 単語座標から行を再構成し、勘定科目ラベル → 金額を抽出
  4. 「どの項目が何%で取れるか」を出す（= 網羅的に構造化できる項目の判定材料）

実行:
  python scripts/fin_analyze_pdfs.py [--dir data/financial_survey/pdf] [--out data/financial_survey]

出力:
  {out}/analysis.json   全件の詳細
  {out}/coverage.csv    項目別の取得率
"""
import argparse
import csv
import json
import logging
import re
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path

import fitz

from fin_common import ROOT, parse_amount

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------
# 判定用の辞書
# ---------------------------------------------------------------

# 様式（PDFの見出しから）
FORM_PATTERNS = [
    ("事業活動計算書", r"事業活動計算書"),
    ("資金収支計算書", r"資金収支計算書"),
    ("貸借対照表", r"貸借対照表"),
    ("損益計算書", r"損益計算書"),
    ("活動計算書", r"活動計算書"),
    ("正味財産増減計算書", r"正味財産増減計算書"),
    ("収支計算書", r"収支計算書"),
    ("財産目録", r"財産目録"),
    ("キャッシュフロー計算書", r"キャッシュ・?フロー計算書"),
    ("注記", r"財務諸表に対する注記|重要な会計方針"),
]

# 会計基準のシグネチャ（本文中の特徴語）
ACCT_SIGNATURES = [
    ("社福会計基準", r"サービス活動増減の部|サービス活動収益計|サービス活動費用計|当期活動増減差額"),
    ("医療法人会計", r"医業収益|医業費用|医業利益|本来業務事業収益"),
    ("企業会計", r"売上高|販売費及び一般管理費|営業利益"),
    ("NPO会計基準", r"経常収益|受取会費|特定非営利活動に係る事業"),
    ("公益法人会計", r"正味財産増減の部|一般正味財産|指定正味財産"),
    ("老健準則", r"介護老人保健施設|施設運営収益"),
]

# 集計単位
SCOPE_PATTERNS = [
    ("サービス区分", r"サービス区分"),
    ("拠点区分", r"拠点区分"),
    ("事業区分", r"事業区分"),
    ("法人単位", r"法人単位|法人全体"),
]

UNIT_PATTERNS = [
    ("千円", r"単位\s*[:：]?\s*千円"),
    ("百万円", r"単位\s*[:：]?\s*百万円"),
    ("円", r"単位\s*[:：]?\s*円"),
]

PERIOD_RE = re.compile(
    r"(令和|平成)\s*(\d{1,2}|元)\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日"
    r"\s*(?:から|〜|～|~|-|－)\s*"
    r"(?:(令和|平成)\s*)?(\d{1,2}|元)\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日"
)
YEAR_ONLY_RE = re.compile(r"(令和|平成)\s*(\d{1,2}|元)\s*年度")

# 抽出したい勘定科目。key = 概念、value = ラベル正規表現（正規化後の文字列に対して）
CONCEPTS_PL = {
    "revenue_service": r"(サービス活動収益計|サービス活動収益合計|サービス活動収益)",
    "revenue_total": r"(経常収益計|経常収益合計|収益計|収益合計|事業収益計|事業収益合計|事業収益|当期収入合計|収入合計|収入の部合計|事業活動収入計|営業収益合計|営業収益計|売上高|純売上高|売上高計|医業収益|医業収益計|医業介護収益|本来業務事業収益|施設運営収益|サービス活動収益)",
    "kaigo_revenue": r"(介護保険事業収益|介護保険事業収入|介護報酬収益|介護事業収益|介護収益)",
    "personnel_cost": r"(人件費計|人件費合計|人件費|給与費計|給与費|人件費支出計|人件費支出)",
    "jigyohi": r"(事業費計|事業費合計|事業費)",
    "jimuhi": r"(事務費計|事務費合計|事務費)",
    "sga": r"(販売費一般管理費|販売費一般管理費計|販売費一般管理費合計)",
    "depreciation": r"(減価償却費|減価償却費計|減価償却費合計)",
    "expense_service": r"(サービス活動費用計|サービス活動費用合計)",
    "expense_total": r"(経常費用計|経常費用合計|費用計|費用合計|当期支出合計|支出合計|支出の部合計|医業費用|医業費用計|売上原価|医業原価)",
    "operating_income": r"(サービス活動増減差額|営業利益|営業損失|営業損益|医業利益|医業損失|医業損益|売上総利益|本来業務事業利益)",
    "ordinary_income": r"(経常増減差額|経常利益|経常損失|経常損益|経常収支差額)",
    "net_income": r"(当期活動増減差額|当期純利益|当期純損失|当期純損益|当期正味財産増減額|当期収支差額|当期一般正味財産増減額|税引後当期純利益|当期経常増減差額)",
    "pretax_income": r"(税引前当期純利益|税引前当期純損失|税引前当期活動増減差額|税引前当期純損益)",
    "interest_expense": r"(支払利息|借入金利息|支払利息計)",
    "subsidy": r"(補助金収益|補助金等収益|受取補助金等|経常経費寄附金収益|運営費補助金収益)",
}

CONCEPTS_BS = {
    "current_assets": r"(流動資産合計|流動資産計|流動資産の部合計)",
    "fixed_assets": r"(固定資産合計|固定資産計|固定資産の部合計)",
    "total_assets": r"(資産の部合計|資産合計|資産計|総資産|資産の部計)",
    "cash": r"(現金預金|現金預金計|現金預金合計)",
    "current_liabilities": r"(流動負債合計|流動負債計|流動負債の部合計)",
    "fixed_liabilities": r"(固定負債合計|固定負債計|固定負債の部合計)",
    "total_liabilities": r"(負債の部合計|負債合計|負債計)",
    "net_assets": r"(純資産の部合計|純資産合計|正味財産合計|純資産計|正味財産の部合計|資本合計|純資産の部計)",
    "short_debt": r"(短期運営資金借入金|短期借入金|1年以内返済予定長期借入金|1年以内返済予定設備資金借入金|1年以内返済予定長期運営資金借入金)",
    "long_debt": r"(長期運営資金借入金|長期借入金|設備資金借入金)",
}

CONCEPTS_CF = {
    "cf_op_in": r"(事業活動収入計|事業活動による収入計|事業活動収入合計)",
    "cf_op_out": r"(事業活動支出計|事業活動による支出計|事業活動支出合計)",
    "cf_op_net": r"(事業活動資金収支差額)",
    "cf_inv_net": r"(施設整備等資金収支差額)",
    "cf_fin_net": r"(その他の活動資金収支差額)",
    "cf_net": r"(当期資金収支差額合計)",
    "cf_begin": r"(前期末支払資金残高)",
    "cf_end": r"(当期末支払資金残高)",
}

ALL_CONCEPTS = {"PL": CONCEPTS_PL, "BS": CONCEPTS_BS, "CF": CONCEPTS_CF}

# 見出し行から列を特定する
COL_CURRENT = re.compile(r"当年度|当期|本年度|当年|今年度|当月")
COL_PRIOR = re.compile(r"前年度|前期|前年|前月")
COL_DIFF = re.compile(r"増減|差額|対比")


def normalize(s: str) -> str:
    """ラベル正規化: 全角→半角、空白・括弧内注記・勘定科目コードを除去

    「1年以内返済予定長期借入金」の先頭数字を消さないよう、
    行番号は「1.」「(1)」のように区切り記号を伴う形だけを落とす。
    """
    s = unicodedata.normalize("NFKC", s or "")
    s = s.replace("　", "").replace(" ", "")
    s = re.sub(r"[（(][^)）]{0,12}[)）]", "", s)   # (注1) (介護報酬収益) など
    s = s.replace("・", "").replace("･", "").replace("及び", "")
    s = re.sub(r"^\(?\d{1,2}\)?[\.．、]", "", s)   # 行番号
    s = re.sub(r"\d{3,}$", "", s)                  # 末尾の勘定科目コード(4112 等)
    s = re.sub(r"^[ⅠⅡⅢⅣⅤⅥⅦⅧⅨⅩ]+", "", s)         # 全角ローマ数字の節番号
    s = re.sub(r"^(?:VII|VIII|IX|IV|VI|V|III|II|I|X)(?=[^\x00-\x7F])", "", s)  # 半角ローマ数字
    s = re.sub(r"[〔〕【】「」\[\]]", "", s)          # 〔医業収益〕
    return s.strip("　 :：*|｜")


# 列見出しの語彙。前年系を先に判定しないと「前残高」が「残高」に食われる
HEADER_TOKENS = [
    ("prior", r"前年度|前期|前年|前月|前残高|前期末|昨年"),
    ("budget", r"予算"),
    ("diff", r"増減|差異|差額|対比|構成比|比率|伸び"),
    ("debit", r"借方"),
    ("credit", r"貸方"),
    ("current", r"当年度|当期|本年度|当年|今年度|当月|決算|残高|金額|本年"),
]


NUM_TOKEN = re.compile(r"^[△▲\(\-]?[0-9０-９][0-9０-９,，\.]*\)?$")


def page_rows(page):
    """単語座標から行を再構成する。
    PyMuPDF の get_text() は段組を無視して読み順が壊れるため、
    y座標でクラスタリングして自前で行を作る。
    """
    return _rows_from_words(page.get_text("words"))


def page_rows_split(page):
    """ページ中央で左右に分けた行も作る。

    貸借対照表は「資産の部｜負債・純資産の部」の2段組が多く、
    そのままy座標でまとめると
      資産の部合計 100 負債純資産の部合計 100
    のように1行に結合されてラベルが一致しなくなる（実測で頻出）。
    左右に割ってから行を組み直すと拾える。
    """
    words = page.get_text("words")
    if not words:
        return []
    mid = page.rect.width / 2
    left = [w for w in words if w[2] <= mid]
    right = [w for w in words if w[0] >= mid]
    if not left or not right:
        return []
    # 左右は別々の表なので、列見出しの判定も別々にやる必要がある。
    # まとめて返すと右表の数値が左表の「当年度」列に割り当てられて値がずれる。
    return [_rows_from_words(left), _rows_from_words(right)]


def _rows_from_words(words):
    if not words:
        return []
    items = sorted(((round((w[1] + w[3]) / 2, 1), w[0], w[4]) for w in words))
    rows, cur, cur_y = [], [], None
    for y, x, txt in items:
        if cur_y is None or abs(y - cur_y) <= 3.0:
            cur.append((x, txt))
            cur_y = y if cur_y is None else (cur_y + y) / 2
        else:
            rows.append(sorted(cur))
            cur, cur_y = [(x, txt)], y
    if cur:
        rows.append(sorted(cur))

    out = []
    for r in rows:
        label_parts, nums, words = [], [], []
        for x, t in r:
            words.append((x, t))
            tn = unicodedata.normalize("NFKC", t)
            if NUM_TOKEN.match(tn):
                v = parse_amount(tn)
                if v is not None:
                    nums.append((x, v))
                    continue
            label_parts.append(t)
        out.append({"label": normalize("".join(label_parts)),
                    "raw": "".join(label_parts), "nums": nums, "words": words})
    return out


def detect_columns(rows):
    """列見出し行を探し、{列種別: x} を返す。

    様式が統一されていないため（正規の財務諸表 / 会計ソフトの試算表 /
    予算対比表が混在する）、「何列目が当年度か」は固定できない。
    見出し語のx座標を取って数値をそこへ割り当てる。
    """
    best, best_score = None, 0
    for r in rows[:60]:
        if len(r["nums"]) > 2:      # 数値だらけの行は見出しではない
            continue
        cols = {}
        for x, t in r["words"]:
            tn = unicodedata.normalize("NFKC", t).replace(" ", "")
            for kind, pat in HEADER_TOKENS:
                if re.search(pat, tn):
                    cols.setdefault(kind, []).append(x)
                    break
        score = len(cols)
        if score >= 2 and score > best_score:
            best, best_score = {k: sum(v) / len(v) for k, v in cols.items()}, score
    return best


def pick_value(nums, cols):
    """行内の数値から当年度の値を選ぶ。

    戻り値: (値, 数値の個数, 選択根拠)
    """
    if not nums:
        return None, 0, "none"
    if len(nums) == 1:
        return nums[0][1], 1, "single"
    if cols:
        # 各数値を最も近い見出しx へ割り当て、current 列のものを採る
        cands = []
        for x, v in nums:
            kind = min(cols.items(), key=lambda kv: abs(kv[1] - x))[0]
            if kind == "current":
                cands.append(v)
        if cands:
            return cands[-1], len(nums), "header:current"
    return nums[0][1], len(nums), "fallback:first"


def analyze_pdf(path: Path):
    rec = {"file": path.name, "path": str(path)}
    try:
        doc = fitz.open(path)
    except Exception as e:  # noqa: BLE001
        rec["error"] = f"{type(e).__name__}: {e}"
        return rec

    rec["pages"] = doc.page_count
    full_text, all_rows, split_groups = [], [], []
    n_images = 0
    for page in doc:
        full_text.append(page.get_text())
        n_images += len(page.get_images())
        all_rows.extend(page_rows(page))
        split_groups.extend(page_rows_split(page))   # 2段組BS対策
    text = "\n".join(full_text)
    rec["chars"] = len(text.strip())
    rec["images"] = n_images
    rec["has_text_layer"] = rec["chars"] >= 200
    ntext = unicodedata.normalize("NFKC", text).replace(" ", "")

    rec["forms"] = [n for n, p in FORM_PATTERNS if re.search(p, ntext)]
    rec["acct_signatures"] = [n for n, p in ACCT_SIGNATURES if re.search(p, ntext)]
    rec["scopes"] = [n for n, p in SCOPE_PATTERNS if re.search(p, ntext)]
    rec["unit"] = next((n for n, p in UNIT_PATTERNS if re.search(p, ntext)), None)

    m = PERIOD_RE.search(ntext)
    if m:
        era, y, mo, d, era2, y2, mo2, d2 = m.groups()
        yy = 1 if y == "元" else int(y)
        rec["period"] = f"{era}{yy}年{int(mo)}月{int(d)}日-{era2 or era}{y2}年{int(mo2)}月{int(d2)}日"
        rec["fiscal_year"] = (2018 + yy) if era == "令和" else (1988 + yy)
    else:
        m2 = YEAR_ONLY_RE.search(ntext)
        if m2:
            yy = 1 if m2.group(2) == "元" else int(m2.group(2))
            rec["period"] = f"{m2.group(1)}{yy}年度"
            rec["fiscal_year"] = (2018 + yy) if m2.group(1) == "令和" else (1988 + yy)

    rec["n_rows"] = len(all_rows)
    rec["n_rows_with_num"] = sum(1 for r in all_rows if r["nums"])
    cols = detect_columns(all_rows)
    rec["col_header"] = sorted(cols) if cols else None

    # 概念抽出（PL/BS/CF すべての辞書を当てる。1つのPDFに複数表が入ることがある）
    # 表ごとに列見出しを判定する。2段組を左右まとめて扱うと列がずれる
    groups = [(all_rows, cols)] + [(g, detect_columns(g)) for g in split_groups]
    found = {}
    hits = defaultdict(list)
    for rows_g, cols_g in groups:
        for r in rows_g:
            # 縦書き見出しが1文字ずつ行になるのでラベル1文字は捨てる
            if not r["nums"] or len(r["label"]) < 2:
                continue
            for concepts in ALL_CONCEPTS.values():
                for key, pat in concepts.items():
                    if re.fullmatch(pat, r["label"]):
                        v, ncol, src = pick_value(r["nums"], cols_g)
                        hits[key].append({"label": r["label"], "value": v,
                                          "ncols": ncol, "src": src})
    for k, v in hits.items():
        # 同じ概念が複数行に出たら、最も絶対値が大きいものを採る（小計の重複対策）
        best = max(v, key=lambda x: abs(x["value"] or 0))
        found[k] = best["value"]
        found[k + "__meta"] = f"{best['label']}|{best['ncols']}col|{best['src']}"
    rec["extracted"] = found
    rec["n_concepts"] = len([k for k in found if not k.endswith("__meta")])
    doc.close()
    return rec


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default="data/financial_survey/pdf")
    ap.add_argument("--extra-dir", default="data/financial_pilot")
    ap.add_argument("--out", default="data/financial_survey")
    args = ap.parse_args()

    paths = sorted(Path(ROOT / args.dir).glob("*.pdf"))
    if args.extra_dir:
        paths += sorted(Path(ROOT / args.extra_dir).glob("*/*.pdf"))
    logger.info("解析対象 %s ファイル", len(paths))

    results = []
    for i, p in enumerate(paths, 1):
        results.append(analyze_pdf(p))
        if i % 50 == 0:
            logger.info("  %s/%s", i, len(paths))

    outdir = ROOT / args.out
    outdir.mkdir(parents=True, exist_ok=True)
    (outdir / "analysis.json").write_text(
        json.dumps(results, ensure_ascii=False, indent=1), encoding="utf-8")

    # サマリー
    n = len(results)
    text_ok = [r for r in results if r.get("has_text_layer")]
    logger.info("=== テキスト層 %s/%s (%.1f%%) ===", len(text_ok), n, len(text_ok) / n * 100 if n else 0)

    cnt = Counter()
    for r in text_ok:
        for k in r.get("extracted", {}):
            if not k.endswith("__meta"):
                cnt[k] += 1
    with open(outdir / "coverage.csv", "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["concept", "count", "rate_of_textlayer", "rate_of_all"])
        for k, c in cnt.most_common():
            w.writerow([k, c, round(c / len(text_ok) * 100, 1) if text_ok else 0,
                        round(c / n * 100, 1) if n else 0])
    logger.info("出力: %s", outdir / "coverage.csv")


if __name__ == "__main__":
    main()
