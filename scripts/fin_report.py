"""決算PDF 実測レポート

analysis.json（PDF解析結果）と manifest.csv（法人種別・会計種類）を突合し、
「どの項目が、どの母集団で、何%取れるか」を出す。
恒等式チェック（資産 = 負債 + 純資産）で抽出の正しさも測る。

実行: python scripts/fin_report.py
出力: claudedocs 用の数値をstdoutへ + data/financial_survey/report.json
"""
import csv
import json
from collections import Counter, defaultdict

from fin_common import ROOT

SURVEY = ROOT / "data" / "financial_survey"


def load():
    analysis = json.loads((SURVEY / "analysis.json").read_text(encoding="utf-8"))
    meta = {}
    with open(SURVEY / "manifest.csv", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            meta[r["file"]] = r
    for a in analysis:
        m = meta.get(a["file"], {})
        a["corp_type"] = m.get("corp_type") or "(パイロット)"
        a["acct"] = m.get("acct") or "(未記載)"
        a["service"] = m.get("service") or ""
        a["stratum"] = m.get("stratum") or "pilot"
        a["doc_type"] = a["file"].rsplit("_", 1)[-1].replace(".pdf", "")
    return analysis


def pct(a, b):
    return f"{a/b*100:5.1f}%" if b else "    -"


def section(title):
    print(f"\n{'='*72}\n{title}\n{'='*72}")


def val(rec, key):
    return rec.get("extracted", {}).get(key)


def main():
    data = load()
    n = len(data)
    survey = [d for d in data if d["stratum"] != "pilot"]

    section("1. 母集団")
    print(f"解析ファイル総数: {n}（層化サンプル {len(survey)} + 既存パイロット {n-len(survey)}）")
    print(f"施設数: {len({d['file'].split('_')[0] for d in data})}")
    for dt, c in Counter(d["doc_type"] for d in data).most_common():
        print(f"  {dt}: {c}")

    section("2. テキスト層（機械抽出の可否そのもの）")
    tl = [d for d in data if d.get("has_text_layer")]
    print(f"全体: {len(tl)}/{n} = {pct(len(tl), n)}")
    print(f"\n{'法人種別':26s}{'件数':>6s}{'テキスト層':>10s}")
    by_corp = defaultdict(list)
    for d in data:
        by_corp[d["corp_type"]].append(d)
    for k, v in sorted(by_corp.items(), key=lambda kv: -len(kv[1])):
        t = sum(1 for x in v if x.get("has_text_layer"))
        print(f"{k[:24]:26s}{len(v):>6d}{pct(t, len(v)):>10s}")
    print(f"\n{'会計種類':40s}{'件数':>6s}{'テキスト層':>10s}")
    by_acct = defaultdict(list)
    for d in data:
        by_acct[d["acct"]].append(d)
    for k, v in sorted(by_acct.items(), key=lambda kv: -len(kv[1]))[:10]:
        t = sum(1 for x in v if x.get("has_text_layer"))
        print(f"{k[:38]:40s}{len(v):>6d}{pct(t, len(v)):>10s}")

    section("3. 中身の様式（1ファイルに複数表が入ることがある）")
    fc = Counter()
    for d in tl:
        for f in d.get("forms", []):
            fc[f] += 1
    for k, c in fc.most_common():
        print(f"  {k:22s}{c:5d}  {pct(c, len(tl))}（テキスト層ありの中で）")
    print("\n会計基準シグネチャ:")
    sc = Counter()
    for d in tl:
        for s in d.get("acct_signatures", []):
            sc[s] += 1
    for k, c in sc.most_common():
        print(f"  {k:22s}{c:5d}  {pct(c, len(tl))}")
    print("\n集計単位の記載:")
    pc = Counter()
    for d in tl:
        for s in d.get("scopes", []) or ["(記載なし)"]:
            pc[s] += 1
    for k, c in pc.most_common():
        print(f"  {k:22s}{c:5d}  {pct(c, len(tl))}")

    section("4. 会計期間・単位")
    withp = [d for d in tl if d.get("period")]
    print(f"会計期間を特定できた: {len(withp)}/{len(tl)} = {pct(len(withp), len(tl))}")
    for k, c in Counter(d.get("fiscal_year") for d in tl).most_common(8):
        print(f"  年度 {k}: {c}")
    print("\n単位表記:")
    for k, c in Counter(d.get("unit") for d in tl).most_common():
        print(f"  {str(k):8s}: {c}  {pct(c, len(tl))}")

    section("5. 項目別の抽出率（doc_type別・分母はそのdoc_typeの全ファイル）")
    groups = {
        "PL": ["revenue_total", "revenue_service", "kaigo_revenue", "personnel_cost",
               "expense_total", "operating_income", "ordinary_income", "net_income",
               "depreciation", "interest_expense", "jigyohi", "jimuhi", "sga", "subsidy"],
        "BS": ["total_assets", "total_liabilities", "net_assets", "current_assets",
               "fixed_assets", "current_liabilities", "fixed_liabilities", "cash",
               "short_debt", "long_debt"],
        "CF": ["cf_op_in", "cf_op_out", "cf_op_net", "cf_inv_net", "cf_fin_net",
               "cf_net", "cf_begin", "cf_end"],
    }
    coverage = {}
    for dt, keys in groups.items():
        files = [d for d in data if d["doc_type"] == dt]
        tfiles = [d for d in files if d.get("has_text_layer")]
        print(f"\n[{dt}] 全{len(files)}件 / テキスト層あり{len(tfiles)}件")
        print(f"  {'項目':20s}{'件数':>6s}{'全体比':>9s}{'テキスト層比':>13s}")
        for k in keys:
            c = sum(1 for d in files if val(d, k) is not None)
            coverage[f"{dt}.{k}"] = {"n": c, "of_all": len(files), "of_text": len(tfiles)}
            print(f"  {k:20s}{c:>6d}{pct(c, len(files)):>9s}{pct(c, len(tfiles)):>13s}")

    section("6. 抽出の正しさ（恒等式チェック）")
    bs = [d for d in data if d["doc_type"] == "BS"]
    ok = ng = na = 0
    for d in bs:
        a, l, ne = val(d, "total_assets"), val(d, "total_liabilities"), val(d, "net_assets")
        if a is None or l is None or ne is None:
            na += 1
            continue
        if a and abs((l + ne) - a) / max(abs(a), 1) < 0.01:
            ok += 1
        else:
            ng += 1
    print(f"BS 資産 = 負債 + 純資産")
    print(f"  3項目そろった: {ok+ng}/{len(bs)} = {pct(ok+ng, len(bs))}")
    print(f"  うち一致    : {ok}/{ok+ng} = {pct(ok, ok+ng)}  ← 抽出値の信頼性の実測")
    print(f"  不一致      : {ng}")

    pl = [d for d in data if d["doc_type"] == "PL"]
    pok = png = 0
    for d in pl:
        r, e, p = val(d, "revenue_total"), val(d, "expense_total"), val(d, "operating_income")
        if r is None or e is None or p is None:
            continue
        if abs((r - e) - p) / max(abs(r), 1) < 0.02:
            pok += 1
        else:
            png += 1
    print(f"\nPL 収益 - 費用 = 利益")
    print(f"  3項目そろった: {pok+png}/{len(pl)} = {pct(pok+png, len(pl))}")
    print(f"  うち一致    : {pok}/{pok+png} = {pct(pok, pok+png)}")

    section("7. 『使えるレコード』の歩留まり")
    pl_usable = [d for d in pl
                 if (val(d, "revenue_total") or val(d, "revenue_service")) is not None]
    pl_full = [d for d in pl
               if (val(d, "revenue_total") or val(d, "revenue_service")) is not None
               and val(d, "personnel_cost") is not None]
    bs_usable = [d for d in bs if val(d, "total_assets") is not None
                 and val(d, "net_assets") is not None]
    print(f"PL 収益が取れた            : {len(pl_usable)}/{len(pl)} = {pct(len(pl_usable), len(pl))}")
    print(f"PL 収益 + 人件費が取れた    : {len(pl_full)}/{len(pl)} = {pct(len(pl_full), len(pl))}")
    print(f"BS 総資産 + 純資産が取れた  : {len(bs_usable)}/{len(bs)} = {pct(len(bs_usable), len(bs))}")

    print("\n法人種別ごとの PL 収益 取得率:")
    for k, v in sorted(by_corp.items(), key=lambda kv: -len(kv[1])):
        vp = [d for d in v if d["doc_type"] == "PL"]
        c = sum(1 for d in vp if (val(d, "revenue_total") or val(d, "revenue_service")) is not None)
        if vp:
            print(f"  {k[:24]:26s}{c:>4d}/{len(vp):<4d}{pct(c, len(vp)):>9s}")

    (SURVEY / "report.json").write_text(
        json.dumps({"n": n, "text_layer": len(tl), "coverage": coverage},
                   ensure_ascii=False, indent=1), encoding="utf-8")


if __name__ == "__main__":
    main()
