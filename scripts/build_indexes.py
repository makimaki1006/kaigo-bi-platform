"""集計クエリ用のインデックスをまとめて作成する。

背景:
    実測で秒〜分単位かかっていたクエリの多くは、223,103 行の全表スキャンが原因だった。
    GROUP BY のキーと集計対象を並べた複合インデックス（カバリングインデックス）を
    張ると、テーブル本体を読まずに済むため劇的に速くなる。

    例: 都道府県別の離職率集計
        索引なし 33.5 秒 → (prefecture, turnover_rate) 索引あり 0.2 秒

使い方:
    python scripts/build_indexes.py --build   # 作成（既存はスキップ）
    python scripts/build_indexes.py --list    # 現在の索引を表示

環境変数: TURSO_DATABASE_URL / TURSO_AUTH_TOKEN
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from geocode_facilities import turso  # noqa: E402

# (索引名, 対象, 目的)
INDEXES = [
    ("idx_facilities_latlon", "facilities(latitude, longitude)",
     "周辺施設マップの矩形検索。40.6秒 → 0.79秒"),
    ("idx_fac_pref_turnover", "facilities(prefecture, turnover_rate)",
     "都道府県別の離職率集計。33.5秒 → 0.2秒"),
    ("idx_fac_pref_staff", 'facilities(prefecture, "従業者_合計")',
     "都道府県別の従業者数集計"),
    ("idx_fac_service_turnover", 'facilities("サービス名", turnover_rate)',
     "サービス種別ごとの離職率集計"),
    ("idx_fac_corp_fac", 'facilities("法人番号", "事業所名")',
     "法人配下の施設一覧（加算ヒートマップ・DDレポート）"),
    ("idx_financials_corp_doc", "financials(corp_number, doc_type)",
     "法人の財務フラグ判定"),
    ("idx_corp_summary_name", "corp_summary(corp_name)",
     "DD支援の法人名検索。138秒 → 1秒未満"),
    # ベンチマークのパーセンタイル算出は指標ごとに範囲スキャンする
    ("idx_fac_turnover", "facilities(turnover_rate)", "ベンチマーク: 定着率"),
    ("idx_fac_fulltime", "facilities(fulltime_ratio)", "ベンチマーク: 常勤比率"),
    ("idx_fac_years", "facilities(years_in_business)", "ベンチマーク: 事業年数"),
    ("idx_fac_occupancy", "facilities(occupancy_rate)", "ベンチマーク: 稼働率"),
    ("idx_fac_quality", "facilities(quality_score)", "ベンチマーク: 品質スコア"),
    ("idx_fac_kasan_count", "facilities(kasan_count)", "ベンチマーク: 加算取得数"),
    ("idx_fac_staff", 'facilities("従業者_合計")', "ベンチマーク: 従業者数"),
    ("idx_fac_capacity", 'facilities("定員")', "ベンチマーク: 定員"),
]


def build(run) -> None:
    for name, target, why in INDEXES:
        t = time.time()
        try:
            run([f"CREATE INDEX IF NOT EXISTS {name} ON {target}"])
            print(f"  {name:28} {time.time() - t:6.1f}秒  — {why}")
        except Exception as e:                       # noqa: BLE001
            print(f"  {name:28} 失敗: {str(e)[:70]}")


def show(run) -> None:
    for table in ("facilities", "financials", "corp_summary"):
        try:
            rows = run([f"PRAGMA index_list({table})"])[0]
        except Exception:                            # noqa: BLE001
            continue
        print(f"{table}:")
        for r in rows:
            print(f"  - {r[1]}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--build", action="store_true")
    ap.add_argument("--list", action="store_true")
    a = ap.parse_args()
    if not (a.build or a.list):
        ap.print_help()
        return
    run = turso()
    if a.build:
        build(run)
    if a.list:
        show(run)


if __name__ == "__main__":
    main()
