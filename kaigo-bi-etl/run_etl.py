"""
介護BIツール ETLパイプライン
============================
スクレイピング済みCSVを正規化・エンリッチし、
RustバックエンドがParquetで読み込める形式に変換する。

使い方:
  python kaigo-bi-etl/run_etl.py
  python kaigo-bi-etl/run_etl.py --input data/output/kaigo_scraping/kaigo_full_20260319.csv
  python kaigo-bi-etl/run_etl.py --input data/output/kaigo_scraping/tokyo_day_care_150_20260319.csv
"""

import argparse
import sys
import time
from pathlib import Path

# Windows環境でのUTF-8出力対応
sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
sys.stderr.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]

# パッケージインポート用にパスを追加
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from importlib import import_module

# パッケージとしてインポート
_normalize = import_module("kaigo-bi-etl.normalize")
_enrich = import_module("kaigo-bi-etl.enrich")
_aggregate = import_module("kaigo-bi-etl.aggregate")

normalize = _normalize.normalize
enrich = _enrich.enrich
save_aggregations = _aggregate.save_aggregations

# デフォルトの入出力パス
DEFAULT_INPUT_DIR = _PROJECT_ROOT / "data" / "output" / "kaigo_scraping"
DEFAULT_OUTPUT_DIR = DEFAULT_INPUT_DIR / "processed"

# CSVファイル検出の優先順位
_FILE_PATTERNS = [
    "kaigo_full_*.csv",
    "kaigo_fast_*.csv",
    "by_service/*.csv",
    "tokyo_day_care_*.csv",
]


def find_input_csv(input_dir: Path) -> Path | None:
    """入力CSVファイルを優先順位に基づいて自動検出する。

    検出優先順位:
    1. kaigo_full_*.csv（フルデータ）
    2. kaigo_fast_*.csv（高速スクレイピングデータ）
    3. by_service/*.csv（サービス別）
    4. tokyo_day_care_*.csv（東京通所介護サンプル）

    Args:
        input_dir: 検索ディレクトリ

    Returns:
        見つかったCSVファイルのパス（なければNone）
    """
    for pattern in _FILE_PATTERNS:
        matches = sorted(input_dir.glob(pattern))
        if matches:
            return matches[-1]
    return None


def run_pipeline(input_path: Path, output_dir: Path) -> None:
    """ETLパイプラインを実行する。

    Args:
        input_path: 入力CSVファイルパス
        output_dir: 出力ディレクトリパス
    """
    start_time = time.time()

    print("=" * 60)
    print("介護BIツール ETLパイプライン")
    print("=" * 60)
    print(f"入力: {input_path}")
    print(f"出力: {output_dir}")
    print()

    # --- Step 1: 正規化 ---
    print("[1/4] CSV読み込み・正規化...")
    df = normalize(str(input_path))
    print()

    # --- Step 2: エンリッチメント ---
    print("[2/4] 派生指標計算...")
    df = enrich(df)
    print()

    # --- Step 3: 施設データ保存 ---
    print("[3/4] 施設データ保存...")
    output_dir.mkdir(parents=True, exist_ok=True)
    facilities_path = output_dir / "facilities.parquet"
    df.to_parquet(facilities_path, index=False, engine="pyarrow")
    file_size_mb = facilities_path.stat().st_size / (1024 * 1024)
    print(f"  facilities.parquet: {len(df)}件, {len(df.columns)}カラム, {file_size_mb:.2f}MB")
    print()

    # --- Step 4: 集計テーブル生成 ---
    print("[4/4] 集計テーブル生成...")
    agg_results = save_aggregations(df, output_dir)
    print()

    # --- サマリー ---
    elapsed = time.time() - start_time
    print("=" * 60)
    print("ETL完了サマリー")
    print("=" * 60)
    print(f"処理時間: {elapsed:.1f}秒")
    print(f"入力レコード数: {len(df)}件")
    print(f"カラム数: {len(df.columns)}")
    print()

    print("出力ファイル:")
    print(f"  {facilities_path}")
    for label, path in agg_results.items():
        print(f"  {path} ({label})")
    print()

    # 主要指標のサンプル表示
    _print_summary_stats(df)


def _print_summary_stats(df) -> None:
    """主要な統計情報を表示する。"""
    print("--- 主要指標（基本）---")

    if "都道府県" in df.columns:
        top_prefs = df["都道府県"].value_counts().head(5)
        print("都道府県別 (上位5):")
        for pref, count in top_prefs.items():
            print(f"  {pref}: {count}件")

    if "法人種別" in df.columns:
        corp_dist = df["法人種別"].value_counts()
        print("法人種別分布:")
        for corp, count in corp_dist.items():
            print(f"  {corp}: {count}件")

    if "従業者_合計" in df.columns:
        mean_emp = df["従業者_合計"].mean()
        median_emp = df["従業者_合計"].median()
        if mean_emp is not None:
            print(f"従業者数: 平均 {mean_emp:.1f}人, 中央値 {median_emp:.0f}人")

    if "離職率" in df.columns:
        mean_turnover = df["離職率"].mean()
        if mean_turnover is not None:
            print(f"平均離職率: {mean_turnover:.1f}%")

    if "従業者規模区分" in df.columns:
        scale_dist = df["従業者規模区分"].value_counts()
        print("従業者規模区分:")
        for scale, count in scale_dist.items():
            print(f"  {scale}: {count}件")

    # --- 新規指標 ---
    new_metrics_exist = False
    for col in ["稼働率", "加算取得数", "品質スコア", "重度率", "要介護度平均",
                 "賃金_代表月額", "推定月間収益", "利用者対平均比"]:
        if col in df.columns and df[col].notna().any():
            new_metrics_exist = True
            break

    if new_metrics_exist:
        print()
        print("--- 主要指標（新規追加）---")

        if "稼働率" in df.columns:
            val = df["稼働率"].mean()
            if val is not None and not pd.isna(val):
                print(f"平均稼働率: {val:.1f}%")

        if "加算取得数" in df.columns:
            val = df["加算取得数"].mean()
            if val is not None and not pd.isna(val):
                print(f"平均加算取得数: {val:.1f}個")

        if "品質スコア" in df.columns:
            val = df["品質スコア"].mean()
            if val is not None and not pd.isna(val):
                print(f"平均品質スコア: {val:.1f}点")

        if "品質ランク" in df.columns:
            rank_dist = df["品質ランク"].value_counts()
            if not rank_dist.empty:
                print("品質ランク分布:")
                for rank in ["S", "A", "B", "C", "D"]:
                    if rank in rank_dist.index:
                        print(f"  {rank}: {rank_dist[rank]}件")

        if "重度率" in df.columns:
            val = df["重度率"].mean()
            if val is not None and not pd.isna(val):
                print(f"平均重度率: {val:.1f}%")

        if "要介護度平均" in df.columns:
            val = df["要介護度平均"].mean()
            if val is not None and not pd.isna(val):
                print(f"平均要介護度: {val:.2f}")

        if "賃金_代表月額" in df.columns:
            val = df["賃金_代表月額"].median()
            if val is not None and not pd.isna(val):
                print(f"賃金中央値: {val:,.0f}円")

        if "推定月間収益" in df.columns:
            val = df["推定月間収益"].median()
            if val is not None and not pd.isna(val):
                print(f"推定月間収益中央値: {val:,.0f}円")

        if "利用者対平均比" in df.columns:
            val = df["利用者対平均比"].mean()
            if val is not None and not pd.isna(val):
                print(f"平均利用者対都道府県平均比: {val:.2f}")

        if "処遇改善加算レベル" in df.columns:
            level_dist = df["処遇改善加算レベル"].value_counts()
            if not level_dist.empty:
                print("処遇改善加算レベル:")
                for level in ["I", "II", "III", "IV"]:
                    if level in level_dist.index:
                        print(f"  {level}: {level_dist[level]}件")

    print()


import pandas as pd  # noqa: E402 - サマリー表示関数内で使用


def main() -> None:
    """メインエントリポイント。"""
    parser = argparse.ArgumentParser(
        description="介護BIツール ETLパイプライン: CSV → Parquet変換",
    )
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help="入力CSVファイルパス（省略時は自動検出）",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help=f"出力ディレクトリ（デフォルト: {DEFAULT_OUTPUT_DIR}）",
    )
    args = parser.parse_args()

    # 入力ファイルの決定
    if args.input:
        input_path = Path(args.input)
        if not input_path.exists():
            print(f"エラー: 入力ファイルが見つかりません: {input_path}", file=sys.stderr)
            sys.exit(1)
    else:
        input_path = find_input_csv(DEFAULT_INPUT_DIR)
        if input_path is None:
            print(
                f"エラー: {DEFAULT_INPUT_DIR} にCSVファイルが見つかりません。\n"
                f"--input オプションでファイルを指定してください。",
                file=sys.stderr,
            )
            sys.exit(1)

    # 出力ディレクトリの決定
    output_dir = Path(args.output) if args.output else DEFAULT_OUTPUT_DIR

    run_pipeline(input_path, output_dir)


if __name__ == "__main__":
    main()
