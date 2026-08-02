"""法人単位の事前集計テーブル corp_summary を作る。

背景:
    M&Aスクリーニングは facilities を "法人番号" で GROUP BY し、
    施設数・従業者数・離職率・展開地域・サービス種別・財務フラグを都度集計していた。
    223,103 行 / 190,003 法人に対する集約で、実測 115 秒（ORDER BY facility_count DESC
    のため LIMIT が効かず全グループを作る必要がある）。API 全体では数分かかっていた。

    法人単位の集計は施設データを更新したときしか変わらないので、
    事前に 1 テーブルへ落としておく。

使い方:
    python scripts/build_corp_summary.py --build     # 作成
    python scripts/build_corp_summary.py --refresh   # 作り直し（データ更新後）
    python scripts/build_corp_summary.py --check     # 件数と速度を確認

環境変数: TURSO_DATABASE_URL / TURSO_AUTH_TOKEN
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from geocode_facilities import turso  # noqa: E402

TABLE = "corp_summary"

# 行政処分・指導の「実質的な記載あり」判定。空・記号・「なし」を除く
VIOLATION = (
    "MAX(CASE WHEN COALESCE(\"行政処分内容\",'') NOT IN ('','-','ー','なし','無し') "
    "        OR COALESCE(\"行政指導内容\",'') NOT IN ('','-','ー','なし','無し') "
    "     THEN 1 ELSE 0 END)"
)

BUILD_SQL = f"""
CREATE TABLE {TABLE} AS
SELECT
    "法人番号"                                         AS corp_number,
    MAX(COALESCE("法人名", ''))                        AS corp_name,
    MAX(corp_type)                                     AS corp_type,
    COUNT(*)                                           AS facility_count,
    SUM(CAST(COALESCE(NULLIF("従業者_合計", ''), '0') AS REAL))  AS total_staff,
    AVG(CASE WHEN turnover_rate BETWEEN 0.0 AND 1.0 THEN turnover_rate END) AS avg_turnover,
    AVG(CAST(COALESCE(NULLIF("定員", ''), '0') AS REAL))         AS avg_capacity,
    GROUP_CONCAT(DISTINCT prefecture)                  AS prefectures,
    GROUP_CONCAT(DISTINCT "サービス名")                AS service_names,
    {VIOLATION}                                        AS has_violation,
    MAX(latitude)                                      AS latitude,
    MAX(longitude)                                     AS longitude
FROM facilities
WHERE "法人番号" IS NOT NULL AND "法人番号" != ''
GROUP BY "法人番号"
"""

# 財務フラグは financials 側から一括で更新する（52行しかないので個別UPDATEで足りる）
FIN_SQL = [
    f"ALTER TABLE {TABLE} ADD COLUMN has_financials INTEGER DEFAULT 0",
    f"ALTER TABLE {TABLE} ADD COLUMN is_insolvent INTEGER DEFAULT 0",
    f"ALTER TABLE {TABLE} ADD COLUMN has_operating_loss INTEGER DEFAULT 0",
    f"ALTER TABLE {TABLE} ADD COLUMN prefecture_count INTEGER DEFAULT 0",
]

FIN_UPDATE = [
    f"""UPDATE {TABLE} SET has_financials = 1
        WHERE corp_number IN (SELECT corp_number FROM financials WHERE corp_number IS NOT NULL)""",
    f"""UPDATE {TABLE} SET is_insolvent = 1
        WHERE corp_number IN (SELECT corp_number FROM financials
                              WHERE doc_type = 'BS' AND net_assets < 0)""",
    f"""UPDATE {TABLE} SET has_operating_loss = 1
        WHERE corp_number IN (SELECT corp_number FROM financials
                              WHERE doc_type = 'PL' AND operating_income < 0)""",
    # 展開都道府県数（GROUP_CONCAT の要素数）
    f"""UPDATE {TABLE} SET prefecture_count =
        CASE WHEN COALESCE(prefectures,'') = '' THEN 0
             ELSE LENGTH(prefectures) - LENGTH(REPLACE(prefectures, ',', '')) + 1 END""",
]

INDEXES = [
    f"CREATE INDEX IF NOT EXISTS idx_corp_summary_fac ON {TABLE}(facility_count DESC)",
    f"CREATE INDEX IF NOT EXISTS idx_corp_summary_staff ON {TABLE}(total_staff DESC)",
    f"CREATE INDEX IF NOT EXISTS idx_corp_summary_type ON {TABLE}(corp_type)",
    f"CREATE UNIQUE INDEX IF NOT EXISTS idx_corp_summary_num ON {TABLE}(corp_number)",
]


def build(run, drop_first: bool) -> None:
    if drop_first:
        run([f"DROP TABLE IF EXISTS {TABLE}"])
        print(f"既存の {TABLE} を削除")

    exists = run([
        f"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='{TABLE}'"
    ])[0][0][0]
    if int(exists):
        n = int(run([f"SELECT COUNT(*) FROM {TABLE}"])[0][0][0])
        print(f"{TABLE} は既に存在します（{n:,} 法人）。作り直すには --refresh")
        return

    t = time.time()
    run([BUILD_SQL])
    print(f"集計テーブル作成: {time.time() - t:.1f}秒")

    for sql in FIN_SQL:
        try:
            run([sql])
        except Exception as e:                       # noqa: BLE001
            print(f"  カラム追加スキップ: {str(e)[:60]}")

    t = time.time()
    for sql in FIN_UPDATE:
        run([sql])
    print(f"財務フラグ・都道府県数の更新: {time.time() - t:.1f}秒")

    t = time.time()
    run(INDEXES)
    print(f"インデックス作成: {time.time() - t:.1f}秒")

    n = int(run([f"SELECT COUNT(*) FROM {TABLE}"])[0][0][0])
    print(f"完了: {n:,} 法人")


def check(run) -> None:
    n = int(run([f"SELECT COUNT(*) FROM {TABLE}"])[0][0][0])
    print(f"{TABLE}: {n:,} 法人")

    t = time.time()
    rows = run([
        f"SELECT corp_name, facility_count, total_staff, prefecture_count, "
        f"       has_financials, has_violation "
        f"FROM {TABLE} ORDER BY facility_count DESC LIMIT 5"
    ])[0]
    print(f"上位法人の取得: {time.time() - t:.2f}秒")
    for r in rows:
        print(f"   {str(r[0])[:26]:26} 施設{r[1]:>4}  従業者{float(r[2]):>8.0f}  "
              f"{r[3]}県  財務{r[4]} 処分{r[5]}")

    t = time.time()
    agg = run([
        f"SELECT COUNT(*), SUM(has_financials), SUM(has_violation), "
        f"       SUM(CASE WHEN facility_count >= 10 THEN 1 ELSE 0 END) FROM {TABLE}"
    ])[0][0]
    print(f"全体集計: {time.time() - t:.2f}秒")
    print(f"   法人 {int(agg[0]):,} / 財務あり {int(agg[1]):,} / 処分指導あり {int(agg[2]):,} "
          f"/ 10施設以上 {int(agg[3]):,}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--build", action="store_true")
    ap.add_argument("--refresh", action="store_true")
    ap.add_argument("--check", action="store_true")
    a = ap.parse_args()
    if not (a.build or a.refresh or a.check):
        ap.print_help()
        return
    run = turso()
    if a.build or a.refresh:
        build(run, drop_first=a.refresh)
    if a.check:
        check(run)


if __name__ == "__main__":
    main()
