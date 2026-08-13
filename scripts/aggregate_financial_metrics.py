"""抽出できた金額の集計（Tier-2） → kpi_cache

前提: financials に入っているのは「機械抽出できた分だけ」で、母集団を代表しない。
      実測でPL収益が取れたのは24.9%、しかも取れやすいのは
      テキスト層のある＝会計ソフト出力の事業者に偏る。

したがってこの集計は
  - 必ず n（件数）を一緒に返す
  - n が閾値未満のセルは出さない
  - 「全国平均」ではなく「抽出できた範囲の中央値」と明示する
という約束で作る。平均値を独り歩きさせないための制約。

実行: python scripts/aggregate_financial_metrics.py
書き込むキー: financial_metrics_summary / financial_metrics_by_corp_type
"""
import json
import logging
import statistics

from fin_common import query, turso

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

MIN_N = 30  # これ未満のセルは公開しない


def med(vals):
    return round(statistics.median(vals), 1) if vals else None


def quartiles(vals):
    if len(vals) < 4:
        return None
    s = sorted(vals)
    q = statistics.quantiles(s, n=4)
    return {"p25": round(q[0], 1), "p50": round(q[1], 1), "p75": round(q[2], 1)}


def main():
    # 単位が推定/仮定のものは 1000倍ずれのリスクがあるので比率計算からも外す
    rows = query("""
        SELECT f.jigyosho_number, f.doc_type, f.revenue, f.personnel_cost,
               f.total_assets, f.net_assets, f.confidence, f.unit_source,
               f.identity_ok, f.fiscal_year, c.corp_type, c.prefecture
        FROM financials f
        LEFT JOIN facilities c ON c."事業所番号" = f.jigyosho_number
        WHERE f.revenue IS NOT NULL OR f.total_assets IS NOT NULL
    """, timeout=300)
    logger.info("financials 有効行 %s", len(rows))

    def f(v):
        try:
            return float(v) if v not in (None, "") else None
        except (TypeError, ValueError):
            return None

    # 施設単位に PL/BS をまとめる
    by_fac = {}
    for r in rows:
        d = by_fac.setdefault(r["jigyosho_number"], {"corp_type": r["corp_type"],
                                                     "prefecture": r["prefecture"]})
        for k in ("revenue", "personnel_cost", "total_assets", "net_assets"):
            v = f(r[k])
            if v is not None:
                d[k] = v
        d["unit_source"] = r["unit_source"] or d.get("unit_source")
        if r["identity_ok"] is not None:
            d["identity_ok"] = str(r["identity_ok"])

    personnel_ratios, equity_ratios = [], []
    per_corp = {}
    for jig, d in by_fac.items():
        # 比率なので分子と分母で単位（円/千円）は打ち消し合う。
        # 単位の取り違えは倍率1000のズレになるが、それは下のレンジ判定で落ちる。
        rev, per = d.get("revenue"), d.get("personnel_cost")
        ta, na = d.get("total_assets"), d.get("net_assets")
        ct = d.get("corp_type") or "不明"
        slot = per_corp.setdefault(ct, {"personnel": [], "equity": []})

        if rev and per and rev > 0:
            ratio = per / rev * 100
            if 5 <= ratio <= 100:      # 明らかな取り違えを弾く
                personnel_ratios.append(ratio)
                slot["personnel"].append(ratio)
        if ta and na is not None and ta > 0:
            ratio = na / ta * 100
            if -200 <= ratio <= 100:
                equity_ratios.append(ratio)
                slot["equity"].append(ratio)

    summary = {
        "source": "決算PDFから機械抽出できた施設のみ。全国の代表値ではない",
        "facilities_with_any_amount": len(by_fac),
        "personnel_ratio": {
            "n": len(personnel_ratios),
            "median": med(personnel_ratios),
            "quartiles": quartiles(personnel_ratios),
            "published": len(personnel_ratios) >= MIN_N,
        },
        "equity_ratio": {
            "n": len(equity_ratios),
            "median": med(equity_ratios),
            "quartiles": quartiles(equity_ratios),
            "published": len(equity_ratios) >= MIN_N,
        },
        "min_n": MIN_N,
    }

    by_corp = []
    for ct, v in sorted(per_corp.items(), key=lambda kv: -len(kv[1]["personnel"])):
        row = {"corp_type": ct,
               "personnel_n": len(v["personnel"]), "equity_n": len(v["equity"])}
        if len(v["personnel"]) >= MIN_N:
            row["personnel_median"] = med(v["personnel"])
        if len(v["equity"]) >= MIN_N:
            row["equity_median"] = med(v["equity"])
        by_corp.append(row)

    logger.info("人件費率 n=%s median=%s / 自己資本比率 n=%s median=%s",
                len(personnel_ratios), summary["personnel_ratio"]["median"],
                len(equity_ratios), summary["equity_ratio"]["median"])

    url, headers = turso()
    from turso_helpers import execute_sql, make_arg
    stmts = []
    for key, value, rc in [("financial_metrics_summary", summary, len(by_fac)),
                           ("financial_metrics_by_corp_type", by_corp, len(by_corp))]:
        stmts.append({"type": "execute", "stmt": {
            "sql": "INSERT OR REPLACE INTO kpi_cache (key, filter_key, value, updated_at, row_count) "
                   "VALUES (?, '', ?, datetime('now'), ?)",
            "args": [make_arg(key), make_arg(json.dumps(value, ensure_ascii=False)), make_arg(rc)],
        }})
    res = execute_sql(url, headers, stmts, timeout=120)
    errs = [r for r in res.get("results", []) if "error" in r]
    logger.info("kpi_cache 書き込み: %s件成功 / %s件エラー", len(stmts) - len(errs), len(errs))
    print(json.dumps(summary, ensure_ascii=False, indent=1))
    print(json.dumps(by_corp, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
