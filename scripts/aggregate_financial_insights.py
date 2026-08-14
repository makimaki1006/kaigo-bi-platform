"""決算データ × 公表データ のクロス集計 → kpi_cache

scripts/fin_explore.py で「信号があるか」を確かめてから残した4つだけを集計する。
落とした切り口: 人件費率 × 離職率（相関係数 r = 0.007 で無相関だった）

集計単位の制約:
  決算書は法人単位か事業所単位かの記載が8割で無い。
  そのため「収益 ÷ 職員数」は
      法人の事業所が1つ かつ その事業所のサービスが1つ
  に限定する。ここなら法人決算＝その事業所の決算とみなせる。
  （従業者_合計 はサービス単位に入っているため、複数サービスだと
    分母だけが1サービス分になり、指標が過大になる。実測で
    福祉用具貸与が 907.8万円/人 → 429.7万円/人 に是正された）

実行: python scripts/aggregate_financial_insights.py
書き込むキー:
  financial_revenue_per_staff / financial_equity_by_corp_size
  financial_disclosure_vs_quality / financial_personnel_ratio_by_prefecture
"""
import json
import logging
import statistics
from collections import defaultdict

from fin_common import query, turso

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

MIN_N = 30


def f(v):
    try:
        return float(v) if v not in (None, "") else None
    except (TypeError, ValueError):
        return None


def med(xs):
    return round(statistics.median(xs), 1) if xs else None


def quartiles(xs):
    if len(xs) < 4:
        return None
    q = statistics.quantiles(sorted(xs), n=4)
    return {"p25": round(q[0], 1), "p50": round(q[1], 1), "p75": round(q[2], 1)}


def main():
    entries = []

    # ------------------------------------------------------------------
    logger.info("[1/4] 職員1人あたり収益（単一事業所・単一サービスの法人のみ）")
    rows = query("""
        SELECT fi.revenue, fa."従業者_合計" staff, fa."サービス名" svc, fa.corp_type
        FROM financials fi
        JOIN facilities fa ON fa."事業所番号" = fi.jigyosho_number
        JOIN (SELECT "法人番号" cn, COUNT(DISTINCT "事業所番号") fc FROM facilities
              WHERE COALESCE("法人番号",'') != '' GROUP BY 1) c
          ON c.cn = fa."法人番号" AND c.fc = 1
        JOIN (SELECT "事業所番号" j, COUNT(*) sr FROM facilities GROUP BY 1) s
          ON s.j = fa."事業所番号" AND s.sr = 1
        WHERE fi.doc_type = 'PL' AND fi.revenue IS NOT NULL
    """, timeout=500)
    allv, by_svc, by_corp = [], defaultdict(list), defaultdict(list)
    for x in rows:
        rev, st = f(x["revenue"]), f(x["staff"])
        if not rev or not st or st < 3 or rev <= 1_000_000:
            continue
        v = rev / st / 10_000                 # 万円
        if not (50 <= v <= 3000):             # 桁違いの誤読を落とす
            continue
        allv.append(v)
        if x["svc"]:
            by_svc[x["svc"]].append(v)
        if x["corp_type"]:
            by_corp[x["corp_type"]].append(v)

    rps = {
        "unit": "万円/人・年",
        "n": len(allv),
        "median": med(allv),
        "quartiles": quartiles(allv),
        "scope_note": "法人の事業所が1つ、かつその事業所のサービスが1つのものに限定。"
                      "決算書に集計単位の記載が無いため、法人決算＝事業所決算とみなせる範囲だけを使う",
        "by_service": [
            {"service": k, "n": len(v), "median": med(v)}
            for k, v in sorted(by_svc.items(), key=lambda kv: -statistics.median(kv[1]))
            if len(v) >= MIN_N
        ],
        "by_corp_type": [
            {"corp_type": k, "n": len(v), "median": med(v)}
            for k, v in sorted(by_corp.items(), key=lambda kv: -len(kv[1]))
            if len(v) >= MIN_N
        ],
        "min_n": MIN_N,
    }
    entries.append(("financial_revenue_per_staff", rps, len(allv)))
    logger.info("  n=%s 中央値 %s万円/人 / サービス%s種", len(allv), rps["median"],
                len(rps["by_service"]))

    # ------------------------------------------------------------------
    logger.info("[2/4] 自己資本比率 × 法人の事業所数")
    rows = query("""
        SELECT fi.total_assets ta, fi.net_assets na, c.fc
        FROM financials fi
        JOIN facilities fa ON fa."事業所番号" = fi.jigyosho_number
        JOIN (SELECT "法人番号" cn, COUNT(DISTINCT "事業所番号") fc FROM facilities
              WHERE COALESCE("法人番号",'') != '' GROUP BY 1) c ON c.cn = fa."法人番号"
        WHERE fi.doc_type = 'BS' AND fi.total_assets IS NOT NULL
          AND fi.net_assets IS NOT NULL
    """, timeout=500)
    band = defaultdict(list)
    for x in rows:
        ta, na, fc = f(x["ta"]), f(x["na"]), f(x["fc"])
        if not ta or ta <= 0 or na is None or fc is None:
            continue
        eq = na / ta * 100
        if not (-200 <= eq <= 100):
            continue
        b = "1事業所" if fc == 1 else "2〜3事業所" if fc <= 3 else \
            "4〜9事業所" if fc <= 9 else "10事業所以上"
        band[b].append(eq)
    size = [{"band": b, "n": len(band[b]), "median": med(band[b]),
             "quartiles": quartiles(band[b])}
            for b in ["1事業所", "2〜3事業所", "4〜9事業所", "10事業所以上"]
            if len(band.get(b, [])) >= MIN_N]
    entries.append(("financial_equity_by_corp_size", size, len(size)))
    for s in size:
        logger.info("  %-12s n=%5d 中央値 %.1f%%", s["band"], s["n"], s["median"])

    # ------------------------------------------------------------------
    logger.info("[3/4] 決算書の開示有無 × 品質スコア・離職率")
    rows = query("""
        SELECT CASE WHEN COALESCE("財務DL_事業活動計算書",'') != '' THEN 1 ELSE 0 END has_pl,
               COUNT(*) n,
               AVG(quality_score) q, COUNT(quality_score) qn,
               AVG(turnover_rate) t, COUNT(turnover_rate) tn,
               AVG("従業者_合計") staff
        FROM facilities GROUP BY has_pl
    """, timeout=500)
    cmp_rows = []
    for x in rows:
        cmp_rows.append({
            "disclosed": x["has_pl"] in (1, "1"),
            "facilities": int(x["n"]),
            "quality_score_avg": round(f(x["q"]), 2) if f(x["q"]) is not None else None,
            "quality_n": int(x["qn"]),
            "turnover_avg": round(f(x["t"]) * 100, 1) if f(x["t"]) is not None else None,
            "turnover_n": int(x["tn"]),
            "staff_avg": round(f(x["staff"]), 1) if f(x["staff"]) is not None else None,
        })
    cmp_rows.sort(key=lambda r: not r["disclosed"])
    payload = {
        "rows": cmp_rows,
        "note": "決算書を出している事業者ほど品質スコアが高く離職率が低い、という関連が出た。"
                "ただし因果ではない（規模の大きい事業者ほど開示も体制整備も進むため）。",
    }
    entries.append(("financial_disclosure_vs_quality", payload, len(cmp_rows)))
    for r in cmp_rows:
        logger.info("  開示%s n=%7d 品質 %.2f 離職 %.1f%%",
                    "あり" if r["disclosed"] else "なし", r["facilities"],
                    r["quality_score_avg"] or 0, r["turnover_avg"] or 0)

    # ------------------------------------------------------------------
    logger.info("[4/4] 都道府県別の人件費率")
    rows = query("""
        SELECT fa.prefecture p, fi.revenue r, fi.personnel_cost pc
        FROM financials fi
        JOIN facilities fa ON fa."事業所番号" = fi.jigyosho_number
        WHERE fi.doc_type = 'PL' AND fi.revenue IS NOT NULL
          AND fi.personnel_cost IS NOT NULL
    """, timeout=500)
    pref = defaultdict(list)
    for x in rows:
        rev, pc = f(x["r"]), f(x["pc"])
        if not rev or rev <= 0 or pc is None or not x["p"]:
            continue
        ratio = pc / rev * 100
        if 5 <= ratio <= 100:
            pref[x["p"]].append(ratio)
    pr = [{"prefecture": k, "n": len(v), "median": med(v)}
          for k, v in pref.items() if len(v) >= MIN_N]
    pr.sort(key=lambda d: -d["median"])
    entries.append(("financial_personnel_ratio_by_prefecture", pr, len(pr)))
    logger.info("  %s都道府県（n>=%s）: 最高 %s / 最低 %s", len(pr), MIN_N,
                pr[0]["prefecture"] if pr else "-", pr[-1]["prefecture"] if pr else "-")

    # ------------------------------------------------------------------
    url, headers = turso()
    from turso_helpers import execute_sql, make_arg
    stmts = [{"type": "execute", "stmt": {
        "sql": "INSERT OR REPLACE INTO kpi_cache (key, filter_key, value, updated_at, row_count) "
               "VALUES (?, '', ?, datetime('now'), ?)",
        "args": [make_arg(k), make_arg(json.dumps(v, ensure_ascii=False)), make_arg(rc)],
    }} for k, v, rc in entries]
    res = execute_sql(url, headers, stmts, timeout=180)
    errs = [r for r in res.get("results", []) if "error" in r]
    logger.info("kpi_cache 書き込み: %s件成功 / %s件エラー", len(stmts) - len(errs), len(errs))
    for e in errs:
        logger.error("  %s", e["error"]["message"][:200])


if __name__ == "__main__":
    main()
