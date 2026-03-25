"""
aggregate.py - 集計テーブル生成モジュール
==========================================
派生指標付きDataFrameから、各種集計テーブルを生成する。
加算取得率平均、品質スコア平均、稼働率平均、重度率平均を含む。
"""

from pathlib import Path

import pandas as pd


# 集計に含める拡張指標カラム（存在すれば集計に含める）
_EXTENDED_METRICS = [
    ("加算取得数", "平均加算取得数", "mean"),
    ("品質スコア", "平均品質スコア", "mean"),
    ("稼働率", "平均稼働率", "mean"),
    ("重度率", "平均重度率", "mean"),
    ("要介護度平均", "平均要介護度", "mean"),
    ("経験10年以上割合", "平均経験10年以上割合", "mean"),
    ("推定月間収益", "平均推定月間収益", "mean"),
    ("賃金_代表月額", "平均賃金_代表月額", "mean"),
]

# 加算クロス集計用カラム
_KASAN_COLUMNS = [
    "加算_処遇改善I",
    "加算_処遇改善II",
    "加算_処遇改善III",
    "加算_処遇改善IV",
    "加算_特定事業所I",
    "加算_特定事業所II",
    "加算_特定事業所III",
    "加算_特定事業所IV",
    "加算_特定事業所V",
    "加算_認知症ケアI",
    "加算_認知症ケアII",
    "加算_口腔連携",
    "加算_緊急時",
]


def _safe_agg(df: pd.DataFrame, group_col: str) -> pd.DataFrame:
    """指定カラムでグループ化し、共通の集計指標を計算する。

    基本指標（施設数、従業者数、定員、離職率）に加え、
    拡張指標（加算取得数、品質スコア、稼働率、重度率等）も集計する。

    Args:
        df: 入力DataFrame
        group_col: グループ化カラム名

    Returns:
        集計済みDataFrame
    """
    if group_col not in df.columns:
        print(f"  警告: '{group_col}' カラムが存在しません。空のDataFrameを返します。")
        return pd.DataFrame()

    # NaNを除外してグループ化
    valid = df[df[group_col].notna()].copy()

    agg_dict: dict[str, tuple[str, str]] = {}

    # 施設数（常に計算可能）
    agg_dict["施設数"] = (group_col, "count")

    # 総従業者数・平均従業者数
    if "従業者_合計" in valid.columns:
        agg_dict["総従業者数"] = ("従業者_合計", "sum")
        agg_dict["平均従業者数"] = ("従業者_合計", "mean")

    # 平均定員
    if "定員" in valid.columns:
        agg_dict["平均定員"] = ("定員", "mean")

    # 平均離職率
    if "離職率" in valid.columns:
        agg_dict["平均離職率"] = ("離職率", "mean")

    # 拡張指標を動的に追加
    for src_col, agg_name, agg_func in _EXTENDED_METRICS:
        if src_col in valid.columns:
            agg_dict[agg_name] = (src_col, agg_func)

    result = valid.groupby(group_col).agg(**agg_dict).reset_index()

    # 小数点丸め（全てのmean系カラム）
    float_cols = [
        "平均従業者数", "平均定員", "平均離職率",
        "平均加算取得数", "平均品質スコア", "平均稼働率",
        "平均重度率", "平均要介護度", "平均経験10年以上割合",
        "平均推定月間収益", "平均賃金_代表月額",
    ]
    for col in float_cols:
        if col in result.columns:
            result[col] = result[col].round(1)

    # 施設数で降順ソート
    result = result.sort_values("施設数", ascending=False).reset_index(drop=True)

    return result


def aggregate_by_prefecture(df: pd.DataFrame) -> pd.DataFrame:
    """都道府県別の集計テーブルを生成する。"""
    return _safe_agg(df, "都道府県")


def aggregate_by_service(df: pd.DataFrame) -> pd.DataFrame:
    """サービス大分類別の集計テーブルを生成する。"""
    return _safe_agg(df, "サービス大分類")


def aggregate_by_corp_type(df: pd.DataFrame) -> pd.DataFrame:
    """法人種別別の集計テーブルを生成する。"""
    return _safe_agg(df, "法人種別")


def aggregate_kasan_cross(df: pd.DataFrame) -> pd.DataFrame:
    """加算項目別の都道府県×サービス大分類クロス取得率テーブルを生成する。

    各セルの値: 該当グループ内での加算取得率（%）

    Args:
        df: 派生指標付きDataFrame

    Returns:
        クロス集計DataFrame（行: 都道府県×サービス大分類、列: 加算項目取得率）
    """
    # 必要カラムの存在確認
    existing_kasan = [c for c in _KASAN_COLUMNS if c in df.columns]
    if not existing_kasan:
        print("  警告: 加算カラムが存在しません。空のDataFrameを返します。")
        return pd.DataFrame()

    if "都道府県" not in df.columns or "サービス大分類" not in df.columns:
        print("  警告: 都道府県またはサービス大分類カラムが存在しません。")
        return pd.DataFrame()

    valid = df[df["都道府県"].notna() & df["サービス大分類"].notna()].copy()
    if valid.empty:
        return pd.DataFrame()

    # グループごとの取得率を計算
    grouped = valid.groupby(["都道府県", "サービス大分類"])

    # 各加算カラムの取得率（True / 全件 * 100）
    agg_dict = {}
    for col in existing_kasan:
        # boolカラムのmean * 100 = 取得率（%）
        agg_dict[col + "_取得率"] = (col, "mean")

    result = grouped.agg(**agg_dict).reset_index()

    # 取得率を百分率に変換・丸め
    rate_cols = [c + "_取得率" for c in existing_kasan]
    for col in rate_cols:
        if col in result.columns:
            result[col] = (result[col] * 100).round(1)

    # 施設数も追加
    counts = grouped.size().reset_index(name="施設数")
    result = result.merge(counts, on=["都道府県", "サービス大分類"], how="left")

    return result


def save_aggregations(
    df: pd.DataFrame,
    output_dir: str | Path,
) -> dict[str, Path]:
    """全集計テーブルを生成し、Parquetファイルとして保存する。

    Args:
        df: 派生指標付きDataFrame
        output_dir: 出力ディレクトリパス

    Returns:
        出力ファイルパスの辞書
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    results: dict[str, Path] = {}

    # 都道府県別
    agg_pref = aggregate_by_prefecture(df)
    if not agg_pref.empty:
        path = output_dir / "agg_prefecture.parquet"
        agg_pref.to_parquet(path, index=False, engine="pyarrow")
        results["都道府県別"] = path
        print(f"  都道府県別集計: {len(agg_pref)}件, {len(agg_pref.columns)}カラム → {path.name}")

    # サービス大分類別
    agg_svc = aggregate_by_service(df)
    if not agg_svc.empty:
        path = output_dir / "agg_service.parquet"
        agg_svc.to_parquet(path, index=False, engine="pyarrow")
        results["サービス大分類別"] = path
        print(f"  サービス大分類別集計: {len(agg_svc)}件, {len(agg_svc.columns)}カラム → {path.name}")

    # 法人種別別
    agg_corp = aggregate_by_corp_type(df)
    if not agg_corp.empty:
        path = output_dir / "agg_corp_type.parquet"
        agg_corp.to_parquet(path, index=False, engine="pyarrow")
        results["法人種別別"] = path
        print(f"  法人種別別集計: {len(agg_corp)}件, {len(agg_corp.columns)}カラム → {path.name}")

    # 加算クロス集計（新規）
    agg_kasan = aggregate_kasan_cross(df)
    if not agg_kasan.empty:
        path = output_dir / "agg_kasan.parquet"
        agg_kasan.to_parquet(path, index=False, engine="pyarrow")
        results["加算クロス集計"] = path
        print(f"  加算クロス集計: {len(agg_kasan)}件, {len(agg_kasan.columns)}カラム → {path.name}")

    return results
