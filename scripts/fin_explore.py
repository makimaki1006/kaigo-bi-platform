"""決算データの使い道を検証する探索スクリプト

「この切り口は指標になるか」を実データで確かめてから画面に出す。
出せない切り口を先に落とすのが目的。

実行: python scripts/fin_explore.py
"""
import statistics
from collections import defaultdict

from fin_common import query


def f(v):
    try:
        return float(v) if v not in (None, "") else None
    except (TypeError, ValueError):
        return None


def med(xs):
    return round(statistics.median(xs), 1) if xs else None


def q1q3(xs):
    if len(xs) < 4:
        return (None, None)
    q = statistics.quantiles(sorted(xs), n=4)
    return (round(q[0], 1), round(q[2], 1))


def head(t):
    print(f"\n{'='*74}\n{t}\n{'='*74}")


def main():
    # ------------------------------------------------------------------
    head("0. 集計単位の問題: 法人が1事業所だけなら法人決算＝事業所決算")
    r = query("""
        SELECT
          COUNT(*) n,
          SUM(CASE WHEN c.fac_count = 1 THEN 1 ELSE 0 END) single,
          SUM(CASE WHEN c.fac_count = 1 AND fi.revenue IS NOT NULL THEN 1 ELSE 0 END) single_rev
        FROM financials fi
        JOIN facilities fa ON fa."事業所番号" = fi.jigyosho_number
        JOIN (SELECT "法人番号" cn, COUNT(DISTINCT "事業所番号") fac_count
              FROM facilities WHERE COALESCE("法人番号",'') != '' GROUP BY 1) c
          ON c.cn = fa."法人番号"
        WHERE fi.doc_type = 'PL'
    """, timeout=400)[0]
    print(f"PLレコード {r['n']} / うち単一事業所法人 {r['single']} / さらに収益あり {r['single_rev']}")

    # ------------------------------------------------------------------
    head("1. 単一事業所法人での「職員1人あたり収益」")
    rows = query("""
        SELECT fi.revenue, fa."従業者_合計" staff, fa."定員" cap, fa.corp_type,
               fa."サービス名" svc, fa.prefecture, fa.turnover_rate, fa.quality_score
        FROM financials fi
        JOIN facilities fa ON fa."事業所番号" = fi.jigyosho_number
        JOIN (SELECT "法人番号" cn, COUNT(DISTINCT "事業所番号") fc
              FROM facilities WHERE COALESCE("法人番号",'') != '' GROUP BY 1) c
          ON c.cn = fa."法人番号" AND c.fc = 1
        WHERE fi.doc_type = 'PL' AND fi.revenue IS NOT NULL
    """, timeout=400)
    per_staff, by_svc, by_corp = [], defaultdict(list), defaultdict(list)
    for x in rows:
        rev, st = f(x["revenue"]), f(x["staff"])
        if rev and st and st >= 3 and rev > 1_000_000:
            v = rev / st / 10_000          # 万円
            if 50 <= v <= 3000:            # 明らかな外れ値を落とす
                per_staff.append(v)
                by_svc[x["svc"]].append(v)
                by_corp[x["corp_type"]].append(v)
    lo, hi = q1q3(per_staff)
    print(f"n={len(per_staff)}  中央値 {med(per_staff)}万円/人  四分位 {lo}〜{hi}万円")
    print("\nサービス種別（n>=30）:")
    for k, v in sorted(by_svc.items(), key=lambda kv: -len(kv[1]))[:12]:
        if len(v) >= 30:
            print(f"  {str(k)[:26]:28s} n={len(v):5d}  中央値 {med(v):7.1f}万円")
    print("\n法人種別（n>=30）:")
    for k, v in sorted(by_corp.items(), key=lambda kv: -len(kv[1])):
        if len(v) >= 30:
            print(f"  {str(k)[:26]:28s} n={len(v):5d}  中央値 {med(v):7.1f}万円")

    # ------------------------------------------------------------------
    head("2. 人件費率 × 離職率（関係があるか）")
    rows = query("""
        SELECT fi.revenue, fi.personnel_cost, fa.turnover_rate, fa.quality_score,
               fa.corp_type, fa.prefecture
        FROM financials fi
        JOIN facilities fa ON fa."事業所番号" = fi.jigyosho_number
        WHERE fi.doc_type = 'PL' AND fi.revenue IS NOT NULL
          AND fi.personnel_cost IS NOT NULL
    """, timeout=400)
    buckets = defaultdict(list)
    pairs = []
    for x in rows:
        rev, per, tr = f(x["revenue"]), f(x["personnel_cost"]), f(x["turnover_rate"])
        if not rev or not per or rev <= 0:
            continue
        ratio = per / rev * 100
        if not (5 <= ratio <= 100):
            continue
        if tr is not None and 0 <= tr <= 1:
            pairs.append((ratio, tr * 100))
            b = "〜55%" if ratio < 55 else "55-65%" if ratio < 65 else \
                "65-75%" if ratio < 75 else "75%〜"
            buckets[b].append(tr * 100)
    print(f"人件費率と離職率が両方ある: n={len(pairs)}")
    for b in ["〜55%", "55-65%", "65-75%", "75%〜"]:
        v = buckets.get(b, [])
        if len(v) >= 30:
            print(f"  人件費率 {b:8s} n={len(v):5d}  離職率 中央値 {med(v):5.1f}%")
    if len(pairs) >= 30:
        xs = [p[0] for p in pairs]; ys = [p[1] for p in pairs]
        mx, my = sum(xs)/len(xs), sum(ys)/len(ys)
        cov = sum((a-mx)*(b-my) for a, b in pairs)
        sx = (sum((a-mx)**2 for a in xs))**0.5
        sy = (sum((b-my)**2 for b in ys))**0.5
        print(f"  相関係数 r = {cov/(sx*sy):.3f}" if sx and sy else "")

    # ------------------------------------------------------------------
    head("3. 自己資本比率 × 法人の施設数（規模）")
    rows = query("""
        SELECT fi.total_assets, fi.net_assets, c.fc
        FROM financials fi
        JOIN facilities fa ON fa."事業所番号" = fi.jigyosho_number
        JOIN (SELECT "法人番号" cn, COUNT(DISTINCT "事業所番号") fc
              FROM facilities WHERE COALESCE("法人番号",'') != '' GROUP BY 1) c
          ON c.cn = fa."法人番号"
        WHERE fi.doc_type = 'BS' AND fi.total_assets IS NOT NULL
          AND fi.net_assets IS NOT NULL
    """, timeout=400)
    size_b = defaultdict(list)
    for x in rows:
        ta, na, fc = f(x["total_assets"]), f(x["net_assets"]), f(x["fc"])
        if not ta or ta <= 0 or na is None or fc is None:
            continue
        eq = na / ta * 100
        if not (-200 <= eq <= 100):
            continue
        b = "1事業所" if fc == 1 else "2-3" if fc <= 3 else "4-9" if fc <= 9 else "10以上"
        size_b[b].append(eq)
    for b in ["1事業所", "2-3", "4-9", "10以上"]:
        v = size_b.get(b, [])
        if len(v) >= 30:
            lo, hi = q1q3(v)
            print(f"  {b:8s} n={len(v):5d}  自己資本比率 中央値 {med(v):6.1f}%  四分位 {lo}〜{hi}")

    # ------------------------------------------------------------------
    head("4. 決算書を出している事業者は品質スコアが高いか")
    rows = query("""
        SELECT CASE WHEN COALESCE("財務DL_事業活動計算書",'') != '' THEN 1 ELSE 0 END has_pl,
               AVG(quality_score) avg_q, COUNT(quality_score) n,
               AVG(turnover_rate) avg_t
        FROM facilities GROUP BY has_pl
    """, timeout=400)
    for x in rows:
        lbl = "開示あり" if x["has_pl"] == "1" else "開示なし"
        aq, at = f(x["avg_q"]), f(x["avg_t"])
        print(f"  {lbl}  n={x['n']:>8}  品質スコア平均 {aq:.1f}" % () if False else
              f"  {lbl}  n={x['n']:>8}  品質スコア平均 {aq:.2f}  離職率平均 {at*100:.1f}%"
              if aq is not None and at is not None else f"  {lbl} n={x['n']}")

    # ------------------------------------------------------------------
    head("5. 都道府県別の人件費率（n>=30）")
    rows = query("""
        SELECT fa.prefecture p, fi.revenue, fi.personnel_cost
        FROM financials fi
        JOIN facilities fa ON fa."事業所番号" = fi.jigyosho_number
        WHERE fi.doc_type='PL' AND fi.revenue IS NOT NULL AND fi.personnel_cost IS NOT NULL
    """, timeout=400)
    pref = defaultdict(list)
    for x in rows:
        rev, per = f(x["revenue"]), f(x["personnel_cost"])
        if rev and per and rev > 0:
            ratio = per / rev * 100
            if 5 <= ratio <= 100 and x["p"]:
                pref[x["p"]].append(ratio)
    ok = [(k, med(v), len(v)) for k, v in pref.items() if len(v) >= 30]
    ok.sort(key=lambda t: -t[1])
    print(f"  対象 {len(ok)} 都道府県")
    for k, m, n in ok[:5]:
        print(f"   高い  {k:6s} {m:5.1f}%  (n={n})")
    for k, m, n in ok[-5:]:
        print(f"   低い  {k:6s} {m:5.1f}%  (n={n})")


if __name__ == "__main__":
    main()
