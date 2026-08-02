"""ベンチマーク用の数値化済み指標テーブル facility_metrics を作る。

背景:
    ベンチマークのパーセンタイルは 8 指標 × 3 スコープ（全国/県内/同サービス）で
    「この施設より下位の割合」を求める。
    facilities の 従業者_合計・定員・quality_score・kasan_count は TEXT 型で、
    集計のたびに CAST が要る。CAST を挟むと式インデックスがないため範囲スキャンが
    効かず、実測でリクエスト全体 73 秒、指標ごとにクエリを分けても 47 秒だった。

    あらかじめ REAL に変換した列を持つテーブルへ写し、各列に索引を張る。

使い方:
    python scripts/build_facility_metrics.py --build
    python scripts/build_facility_metrics.py --refresh   # 施設データ更新後
    python scripts/build_facility_metrics.py --check

環境変数: TURSO_DATABASE_URL / TURSO_AUTH_TOKEN
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from geocode_facilities import turso  # noqa: E402

TABLE = "facility_metrics"

BUILD_SQL = f"""
CREATE TABLE {TABLE} AS
SELECT
    "事業所番号"                                        AS jigyosho_number,
    prefecture,
    "サービス名"                                        AS service_name,
    -- 従業者数・定員には誤入力が混じる（定員欄に 20251215 のような日付が 97 件）。
    -- 上限は Rust 側の MAX_CAPACITY_FILTER と揃える。
    CASE WHEN CAST(NULLIF("従業者_合計", '') AS REAL) BETWEEN 0 AND 10000
         THEN CAST(NULLIF("従業者_合計", '') AS REAL) END   AS staff,
    CASE WHEN CAST(NULLIF("定員", '') AS REAL) BETWEEN 1 AND 500
         THEN CAST(NULLIF("定員", '') AS REAL) END          AS capacity,
    CASE WHEN turnover_rate BETWEEN 0.0 AND 1.0 THEN turnover_rate END      AS turnover,
    CASE WHEN fulltime_ratio BETWEEN 0.0 AND 1.0 THEN fulltime_ratio END    AS fulltime,
    CASE WHEN years_in_business > 0 AND years_in_business <= 100
         THEN years_in_business END                                          AS years,
    CASE WHEN occupancy_rate BETWEEN 0.0 AND 3.0 THEN occupancy_rate END    AS occupancy,
    CAST(NULLIF(quality_score, '') AS REAL)             AS quality,
    CAST(NULLIF(kasan_count, '') AS REAL)               AS kasan
FROM facilities
"""

METRICS = ["staff", "capacity", "turnover", "fulltime", "years", "occupancy", "quality", "kasan"]

INDEXES = (
    [f"CREATE INDEX IF NOT EXISTS idx_fm_{m} ON {TABLE}({m})" for m in METRICS]
    + [f"CREATE INDEX IF NOT EXISTS idx_fm_pref_{m} ON {TABLE}(prefecture, {m})" for m in METRICS]
    + [f"CREATE INDEX IF NOT EXISTS idx_fm_svc_{m} ON {TABLE}(service_name, {m})" for m in METRICS]
    + [f"CREATE INDEX IF NOT EXISTS idx_fm_jno ON {TABLE}(jigyosho_number)"]
)


def build(run, drop_first: bool) -> None:
    if drop_first:
        run([f"DROP TABLE IF EXISTS {TABLE}"])
        print(f"既存の {TABLE} を削除")

    exists = int(run([
        f"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='{TABLE}'"
    ])[0][0][0])
    if exists:
        n = int(run([f"SELECT COUNT(*) FROM {TABLE}"])[0][0][0])
        print(f"{TABLE} は既に存在します（{n:,} 行）。作り直すには --refresh")
        return

    t = time.time()
    run([BUILD_SQL])
    print(f"テーブル作成: {time.time() - t:.1f}秒")

    for sql in INDEXES:
        t = time.time()
        try:
            run([sql])
            name = sql.split("EXISTS ")[1].split(" ")[0]
            print(f"  {name:24} {time.time() - t:5.1f}秒")
        except Exception as e:                       # noqa: BLE001
            print(f"  索引失敗: {str(e)[:70]}")

    n = int(run([f"SELECT COUNT(*) FROM {TABLE}"])[0][0][0])
    print(f"完了: {n:,} 行")


def check(run) -> None:
    n = int(run([f"SELECT COUNT(*) FROM {TABLE}"])[0][0][0])
    print(f"{TABLE}: {n:,} 行")
    cols = ", ".join(f"COUNT({m})" for m in METRICS)
    r = run([f"SELECT {cols} FROM {TABLE}"])[0][0]
    for m, v in zip(METRICS, r):
        print(f"  {m:10} 非NULL {int(v):>8,}")

    t = time.time()
    run([f"SELECT COUNT(*), SUM(CASE WHEN turnover > 0.2 THEN 1 ELSE 0 END) "
         f"FROM {TABLE} WHERE turnover IS NOT NULL"])
    print(f"\n範囲集計の所要: {time.time() - t:.2f}秒")


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
