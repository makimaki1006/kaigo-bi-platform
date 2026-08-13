"""決算書「開示状況」の事前集計 → kpi_cache

金額（PLの収益など）は網羅的に取れない（実測: PL収益 24.9%）。
一方、**決算書が出ているか / いつ出したか / どの形式か** は
facilities のURL列から100%機械的に決まる。ここはBI指標として使える。

URLの ?1738223780 はアップロード時刻のUnix秒。実測で146,278件すべてに入っている。

実行:
  $env:TURSO_DATABASE_URL / TURSO_AUTH_TOKEN を設定して
  python scripts/aggregate_financial_disclosure.py

書き込むキー:
  financial_disclosure_kpi / _by_prefecture / _by_corp_type / _by_service
  financial_disclosure_freshness / _by_acct_type / financial_extraction_status
"""
import json
import logging
import time

from fin_common import query, turso

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# 3つのURL列を1度だけ展開する共通CTE。
# instr/substr でクエリ文字列（=アップロード時刻）を取り出す。
BASE_CTE = """
WITH d AS (
  SELECT
    "事業所番号"      AS jigyosho,
    prefecture,
    corp_type,
    "サービス名"      AS service,
    "法人番号"        AS corp_number,
    COALESCE("会計種類", '(未記載)') AS acct,
    CASE WHEN COALESCE("財務DL_事業活動計算書",'') != '' THEN 1 ELSE 0 END AS has_pl,
    CASE WHEN COALESCE("財務DL_貸借対照表",'')     != '' THEN 1 ELSE 0 END AS has_bs,
    CASE WHEN COALESCE("財務DL_資金収支計算書",'') != '' THEN 1 ELSE 0 END AS has_cf,
    CASE WHEN instr(COALESCE("財務DL_事業活動計算書",''), '.csv') > 0 THEN 1 ELSE 0 END AS is_csv,
    CAST(NULLIF(substr("財務DL_事業活動計算書",
         instr("財務DL_事業活動計算書",'?') + 1), '') AS INTEGER) AS ts
  FROM facilities
)
"""

# 妥当なUnix秒だけを採る（2020-09-13以降）。桁欠けや空文字を弾く足切り。
TS_OK = "ts > 1600000000"


def q(sql, timeout=300):
    t = time.time()
    rows = query(BASE_CTE + sql, timeout=timeout)
    logger.info("  クエリ %.1fs → %s行", time.time() - t, len(rows))
    return rows


def i(v):
    return int(v) if v not in (None, "") else 0


def rate(a, b):
    return round(a / b * 100, 1) if b else 0.0


def main():
    entries = []

    # ---------------------------------------------------------------
    logger.info("[1/7] 全国KPI")
    r = q(f"""
      SELECT COUNT(*) n, COUNT(DISTINCT jigyosho) n_jigyosho,
             COUNT(DISTINCT NULLIF(corp_number,'')) n_corp,
             SUM(has_pl) pl, SUM(has_bs) bs, SUM(has_cf) cf,
             SUM(CASE WHEN has_pl+has_bs+has_cf > 0 THEN 1 ELSE 0 END) any_doc,
             SUM(has_pl*has_bs*has_cf) full3,
             SUM(is_csv) csv_cnt,
             SUM(CASE WHEN {TS_OK} THEN 1 ELSE 0 END) ts_ok,
             MIN(CASE WHEN {TS_OK} THEN date(ts,'unixepoch') END) oldest,
             MAX(CASE WHEN {TS_OK} THEN date(ts,'unixepoch') END) latest,
             SUM(CASE WHEN {TS_OK} AND ts > strftime('%s','now') - 365*86400
                      THEN 1 ELSE 0 END) fresh_1y
      FROM d""")[0]
    n = i(r["n"])
    kpi = {
        "facilities": n,
        "jigyosho": i(r["n_jigyosho"]),
        "corporations": i(r["n_corp"]),
        "with_any": i(r["any_doc"]), "with_any_rate": rate(i(r["any_doc"]), n),
        "with_pl": i(r["pl"]), "with_pl_rate": rate(i(r["pl"]), n),
        "with_bs": i(r["bs"]), "with_bs_rate": rate(i(r["bs"]), n),
        "with_cf": i(r["cf"]), "with_cf_rate": rate(i(r["cf"]), n),
        "full_set": i(r["full3"]), "full_set_rate": rate(i(r["full3"]), n),
        "csv_count": i(r["csv_cnt"]),
        "timestamp_available": i(r["ts_ok"]),
        "oldest_upload": r["oldest"], "latest_upload": r["latest"],
        "fresh_within_1y": i(r["fresh_1y"]),
        "fresh_within_1y_rate": rate(i(r["fresh_1y"]), i(r["pl"])),
    }
    entries.append(("financial_disclosure_kpi", kpi, n))

    # ---------------------------------------------------------------
    logger.info("[2/7] 都道府県別")
    rows = q(f"""
      SELECT prefecture, COUNT(*) n, SUM(has_pl) pl, SUM(has_pl*has_bs*has_cf) full3,
             SUM(CASE WHEN {TS_OK} AND ts > strftime('%s','now') - 365*86400
                      THEN 1 ELSE 0 END) fresh
      FROM d WHERE prefecture IS NOT NULL AND prefecture != ''
      GROUP BY prefecture ORDER BY n DESC""")
    pref = [{"prefecture": x["prefecture"], "facilities": i(x["n"]),
             "with_pl": i(x["pl"]), "disclosure_rate": rate(i(x["pl"]), i(x["n"])),
             "full_set": i(x["full3"]), "full_set_rate": rate(i(x["full3"]), i(x["n"])),
             "fresh_1y": i(x["fresh"]), "fresh_rate": rate(i(x["fresh"]), i(x["n"]))}
            for x in rows]
    entries.append(("financial_disclosure_by_prefecture", pref, len(pref)))

    # ---------------------------------------------------------------
    logger.info("[3/7] 法人種別別")
    rows = q("""
      SELECT corp_type, COUNT(*) n, SUM(has_pl) pl, SUM(has_bs) bs, SUM(has_cf) cf,
             SUM(has_pl*has_bs*has_cf) full3, SUM(is_csv) csv_cnt
      FROM d GROUP BY corp_type ORDER BY n DESC""")
    corp = [{"corp_type": x["corp_type"], "facilities": i(x["n"]),
             "with_pl": i(x["pl"]), "with_bs": i(x["bs"]), "with_cf": i(x["cf"]),
             "disclosure_rate": rate(i(x["pl"]), i(x["n"])),
             "full_set": i(x["full3"]), "full_set_rate": rate(i(x["full3"]), i(x["n"])),
             "csv_count": i(x["csv_cnt"])} for x in rows]
    entries.append(("financial_disclosure_by_corp_type", corp, len(corp)))

    # ---------------------------------------------------------------
    logger.info("[4/7] サービス種別別")
    rows = q("""
      SELECT service, COUNT(*) n, SUM(has_pl) pl, SUM(has_pl*has_bs*has_cf) full3
      FROM d WHERE service IS NOT NULL AND service != ''
      GROUP BY service HAVING n >= 100 ORDER BY n DESC""")
    svc = [{"service": x["service"], "facilities": i(x["n"]), "with_pl": i(x["pl"]),
            "disclosure_rate": rate(i(x["pl"]), i(x["n"])),
            "full_set_rate": rate(i(x["full3"]), i(x["n"]))} for x in rows]
    entries.append(("financial_disclosure_by_service", svc, len(svc)))

    # ---------------------------------------------------------------
    logger.info("[5/7] 開示鮮度（アップロード年月）")
    rows = q(f"""
      SELECT strftime('%Y-%m', ts, 'unixepoch') ym, COUNT(*) n
      FROM d WHERE {TS_OK} GROUP BY ym ORDER BY ym""")
    fresh = [{"month": x["ym"], "count": i(x["n"])} for x in rows]
    entries.append(("financial_disclosure_freshness", fresh, len(fresh)))

    # ---------------------------------------------------------------
    logger.info("[6/7] 会計種類別")
    rows = q("""
      SELECT acct, COUNT(*) n, SUM(has_pl) pl
      FROM d GROUP BY acct HAVING n >= 50 ORDER BY n DESC""")
    accts = [{"acct_type": x["acct"], "facilities": i(x["n"]), "with_pl": i(x["pl"]),
              "disclosure_rate": rate(i(x["pl"]), i(x["n"]))} for x in rows]
    entries.append(("financial_disclosure_by_acct_type", accts, len(accts)))

    # ---------------------------------------------------------------
    logger.info("[7/8] 開示ギャップ（未開示・更新停滞）")
    # 営業・DDで直接使うセグメント。決算書を出していない/長く更新していない事業者。
    g = q(f"""
      SELECT
        SUM(CASE WHEN has_pl + has_bs + has_cf = 0 THEN 1 ELSE 0 END) no_doc,
        COUNT(DISTINCT CASE WHEN has_pl + has_bs + has_cf = 0
                            THEN NULLIF(corp_number,'') END) no_doc_corp,
        SUM(CASE WHEN has_pl = 1 AND {TS_OK}
                  AND ts < strftime('%s','now') - 730*86400 THEN 1 ELSE 0 END) stale2y,
        SUM(CASE WHEN has_pl = 1 AND {TS_OK}
                  AND ts < strftime('%s','now') - 365*86400 THEN 1 ELSE 0 END) stale1y
      FROM d""")[0]
    gap_pref = q(f"""
      SELECT prefecture,
             SUM(CASE WHEN has_pl + has_bs + has_cf = 0 THEN 1 ELSE 0 END) no_doc,
             SUM(CASE WHEN has_pl = 1 AND {TS_OK}
                       AND ts < strftime('%s','now') - 730*86400 THEN 1 ELSE 0 END) stale2y,
             COUNT(*) n
      FROM d WHERE prefecture IS NOT NULL AND prefecture != ''
      GROUP BY prefecture ORDER BY no_doc DESC""")
    gap = {
        "no_disclosure": i(g["no_doc"]),
        "no_disclosure_corporations": i(g["no_doc_corp"]),
        "no_disclosure_rate": rate(i(g["no_doc"]), n),
        "stale_over_2y": i(g["stale2y"]),
        "stale_over_1y": i(g["stale1y"]),
        "note": "施設マスタで financial_status=none / stale を指定すると同じ条件で一覧・CSV出力できる",
        "by_prefecture": [
            {"prefecture": x["prefecture"], "facilities": i(x["n"]),
             "no_disclosure": i(x["no_doc"]),
             "no_disclosure_rate": rate(i(x["no_doc"]), i(x["n"])),
             "stale_over_2y": i(x["stale2y"])}
            for x in gap_pref
        ],
    }
    entries.append(("financial_disclosure_gap", gap, i(g["no_doc"])))
    logger.info("  未開示 %s施設 / 2年以上更新なし %s施設", gap["no_disclosure"], gap["stale_over_2y"])

    # ---------------------------------------------------------------
    logger.info("[8/8] 金額抽出の実測値（financials の実データから算出）")
    # 固定値を書くと抽出器を変えたときに嘘になるので、都度DBから測る
    st = query("""
      SELECT COUNT(*) n,
             SUM(CASE WHEN text_layer = 1 THEN 1 ELSE 0 END) tl,
             SUM(CASE WHEN doc_type = 'PL' THEN 1 ELSE 0 END) pl_n,
             SUM(CASE WHEN doc_type = 'PL' AND revenue IS NOT NULL THEN 1 ELSE 0 END) pl_rev,
             SUM(CASE WHEN doc_type = 'PL' AND revenue IS NOT NULL
                       AND personnel_cost IS NOT NULL THEN 1 ELSE 0 END) pl_rev_per,
             SUM(CASE WHEN doc_type = 'BS' THEN 1 ELSE 0 END) bs_n,
             SUM(CASE WHEN doc_type = 'BS' AND total_assets IS NOT NULL
                       AND net_assets IS NOT NULL THEN 1 ELSE 0 END) bs_two,
             SUM(CASE WHEN identity_ok = 1 THEN 1 ELSE 0 END) id_ok,
             SUM(CASE WHEN identity_ok IS NOT NULL THEN 1 ELSE 0 END) id_all,
             SUM(CASE WHEN fiscal_year IS NOT NULL THEN 1 ELSE 0 END) fy,
             SUM(CASE WHEN scope IS NOT NULL THEN 1 ELSE 0 END) sc,
             SUM(CASE WHEN unit_source = 'pdf' THEN 1 ELSE 0 END) unit_pdf,
             COUNT(DISTINCT jigyosho_number) fac
      FROM financials""", timeout=300)[0]
    tn, tl = i(st["n"]), i(st["tl"])
    status = {
        "analyzed_files": tn,
        "analyzed_facilities": i(st["fac"]),
        "text_layer_rate": rate(tl, tn),
        "pl_files": i(st["pl_n"]),
        "pl_revenue_rate": rate(i(st["pl_rev"]), i(st["pl_n"])),
        "pl_revenue_and_personnel_rate": rate(i(st["pl_rev_per"]), i(st["pl_n"])),
        "bs_files": i(st["bs_n"]),
        "bs_assets_and_equity_rate": rate(i(st["bs_two"]), i(st["bs_n"])),
        "bs_identity_match_rate": rate(i(st["id_ok"]), i(st["id_all"])),
        "period_detect_rate": rate(i(st["fy"]), tl),
        "scope_stated_rate": rate(i(st["sc"]), tl),
        "unit_stated_rate": rate(i(st["unit_pdf"]), tl),
        "note": "決算PDFは自由書式のアップロードで、半分以上がスキャン画像。"
                "金額の機械抽出は網羅できないため、取れた分だけを件数付きで扱う。"
                "会計期間・集計単位・単位の各率はテキスト層があるファイルを分母とする。",
        "surveyed_at": None,  # 下で更新日を入れる
    }
    upd = query("SELECT MAX(extracted_at) m FROM financials", timeout=120)[0]["m"]
    status["surveyed_at"] = (upd or "")[:10] or None
    entries.append(("financial_extraction_status", status, tn))
    logger.info("  抽出実測: %s", json.dumps(status, ensure_ascii=False)[:300])

    # ---------------------------------------------------------------
    logger.info("kpi_cache へ書き込み（%s件）", len(entries))
    url, headers = turso()
    from turso_helpers import execute_sql, make_arg

    stmts = []
    for key, value, rc in entries:
        stmts.append({"type": "execute", "stmt": {
            "sql": "INSERT OR REPLACE INTO kpi_cache (key, filter_key, value, updated_at, row_count) "
                   "VALUES (?, '', ?, datetime('now'), ?)",
            "args": [make_arg(key), make_arg(json.dumps(value, ensure_ascii=False)), make_arg(rc)],
        }})
    res = execute_sql(url, headers, stmts, timeout=180)
    errs = [r for r in res.get("results", []) if "error" in r]
    for e in errs:
        logger.error("  %s", e["error"]["message"][:200])
    logger.info("完了: %s件成功 / %s件エラー", len(entries) - len(errs), len(errs))

    print("\n--- 主要な実測値 ---")
    print(json.dumps(kpi, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
