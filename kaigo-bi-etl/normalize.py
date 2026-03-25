"""
normalize.py - データの正規化・型変換モジュール
==============================================
入力CSVを読み込み、型変換・正規化を行う。
19カラム（簡易版）でも76カラム（フル版）でも動作する。
"""

import re
from pathlib import Path

import pandas as pd


# 数値変換対象カラム（基本19カラム）
NUMERIC_COLUMNS = [
    "従業者_常勤",
    "従業者_非常勤",
    "従業者_合計",
    "定員",
    "前年度採用数",
    "前年度退職数",
]

# 日付変換対象カラム
DATE_COLUMNS = [
    "事業開始日",
]

# 76カラム用: 加算フィールド（'○' → True）
# 新データの実際のカラム名に対応
ADDITION_COLUMNS = [
    # 旧19カラム互換
    "サービス提供体制強化加算",
    "介護職員処遇改善加算",
    "介護職員等特定処遇改善加算",
    "介護職員等ベースアップ等支援加算",
    "中山間地域等における小規模事業所加算",
    "中重度者ケア体制加算",
    "認知症加算",
    "若年性認知症利用者受入加算",
    "栄養アセスメント加算",
    "口腔・栄養スクリーニング加算",
    "科学的介護推進体制加算",
    "入浴介助加算",
    "ADL維持等加算",
    # 新76カラム（処遇改善加算I〜IV）
    "加算_処遇改善I",
    "加算_処遇改善II",
    "加算_処遇改善III",
    "加算_処遇改善IV",
    # 特定事業所加算I〜V
    "加算_特定事業所I",
    "加算_特定事業所II",
    "加算_特定事業所III",
    "加算_特定事業所IV",
    "加算_特定事業所V",
    # 認知症ケア加算
    "加算_認知症ケアI",
    "加算_認知症ケアII",
    # その他加算
    "加算_口腔連携",
    "加算_緊急時",
]

# 76カラム用: 品質フィールド（'○' → True）
QUALITY_COLUMNS = [
    "品質_BCP策定",
    "品質_ICT活用",
    "品質_第三者評価",
    "品質_損害賠償保険",
]

# 76カラム用: 賃金フィールド（'200,000円' → 200000.0）
WAGE_COLUMNS = [
    # 旧19カラム互換
    "賃金_基本給_常勤",
    "賃金_基本給_非常勤",
    "賃金_手当_常勤",
    "賃金_手当_非常勤",
    "賃金_合計_常勤",
    "賃金_合計_非常勤",
    # 新76カラム: 賃金_月額1〜5
    "賃金_月額1",
    "賃金_月額2",
    "賃金_月額3",
    "賃金_月額4",
    "賃金_月額5",
]

# 76カラム用: 追加の数値カラム
EXTRA_NUMERIC_COLUMNS = [
    "利用者総数",
    "要介護1",
    "要介護2",
    "要介護3",
    "要介護4",
    "要介護5",
    "要支援1",
    "要支援2",
    "平均要介護度",
]

# 76カラム用: Float変換カラム（都道府県平均、年齢、勤続年数等）
FLOAT_COLUMNS = [
    "利用者_都道府県平均",
    "賃金_平均年齢1",
    "賃金_平均勤続1",
    "賃金_平均年齢2",
    "賃金_平均勤続2",
    "賃金_平均年齢3",
    "賃金_平均勤続3",
    "賃金_平均年齢4",
    "賃金_平均勤続4",
    "賃金_平均年齢5",
    "賃金_平均勤続5",
]

# 行政処分・指導の日付カラム
EXTRA_DATE_COLUMNS = [
    "行政処分日",
    "行政指導日",
]


def read_csv(filepath: str | Path) -> pd.DataFrame:
    """CSVファイルを文字列型で読み込む（BOM対応）。

    Args:
        filepath: 入力CSVファイルパス

    Returns:
        全カラムが文字列型のDataFrame
    """
    return pd.read_csv(
        filepath,
        dtype=str,
        encoding="utf-8-sig",
        na_values=["", "NA", "N/A"],
        keep_default_na=True,
    )


def _convert_numeric(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """指定カラムをInt64型（Nullable Integer）に変換する。

    Args:
        df: 対象DataFrame
        columns: 変換対象カラム名リスト

    Returns:
        変換後のDataFrame
    """
    for col in columns:
        if col not in df.columns:
            continue
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df


def _convert_float(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """指定カラムをFloat64型に変換する。

    Args:
        df: 対象DataFrame
        columns: 変換対象カラム名リスト

    Returns:
        変換後のDataFrame
    """
    for col in columns:
        if col not in df.columns:
            continue
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Float64")
    return df


def _convert_dates(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """指定カラムをdatetime型に変換する。

    Args:
        df: 対象DataFrame
        columns: 変換対象カラム名リスト

    Returns:
        変換後のDataFrame
    """
    for col in columns:
        if col not in df.columns:
            continue
        df[col] = pd.to_datetime(df[col], format="mixed", errors="coerce")
    return df


def _convert_boolean(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """加算・品質フィールドをブール型に変換する（'○' → True）。

    Args:
        df: 対象DataFrame
        columns: 変換対象カラム名リスト

    Returns:
        変換後のDataFrame
    """
    for col in columns:
        if col not in df.columns:
            continue
        df[col] = df[col].apply(lambda x: x == "○" if pd.notna(x) else False)
        df[col] = df[col].astype(bool)
    return df


def _convert_wages(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """賃金フィールドを数値型に変換する（'200,000円' → 200000.0）。

    コンマ、円、全角数字、全角コンマ等を除去してFloat変換する。
    範囲表記（'月給261000～300000'）の場合は中央値を採用する。
    変換不能な値はNoneにする。

    Args:
        df: 対象DataFrame
        columns: 変換対象カラム名リスト

    Returns:
        変換後のDataFrame
    """
    clean_pattern = re.compile(r"[,，円\s月給時給日給年収]")
    range_pattern = re.compile(r"(\d+(?:\.\d+)?)\s*[～〜~\-ー－]\s*(\d+(?:\.\d+)?)")

    def _parse_wage(x):
        if pd.isna(x) or not x:
            return None
        cleaned = clean_pattern.sub("", str(x))
        # 範囲表記の検出（例: '261000～300000'）
        range_match = range_pattern.search(cleaned)
        if range_match:
            low = float(range_match.group(1))
            high = float(range_match.group(2))
            return (low + high) / 2
        # 単純な数値
        try:
            if cleaned:
                return float(cleaned)
        except ValueError:
            pass
        return None

    for col in columns:
        if col not in df.columns:
            continue
        df[col] = df[col].apply(_parse_wage)
    return df


def _convert_percentage(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """パーセント表記を数値に変換する（'72.7％' → 72.7）。

    全角・半角の％/% を除去してFloat変換する。

    Args:
        df: 対象DataFrame
        columns: 変換対象カラム名リスト

    Returns:
        変換後のDataFrame
    """
    pattern = re.compile(r"[％%]")
    for col in columns:
        if col not in df.columns:
            continue
        df[col] = df[col].apply(
            lambda x: float(pattern.sub("", x.strip()))
            if pd.notna(x) and pattern.sub("", x.strip())
            else None
        )
        df[col] = df[col].astype("Float64")
    return df


def _normalize_phone(phone: str | None) -> str | None:
    """全角数字・ハイフンを半角に変換する。

    既存データに全角電話番号が含まれている場合の正規化用。

    Args:
        phone: 電話番号文字列

    Returns:
        半角変換後の電話番号文字列（NoneはNoneのまま）
    """
    if not phone or not isinstance(phone, str) or pd.isna(phone):
        return phone
    table = str.maketrans(
        '０１２３４５６７８９－（）　',
        '0123456789-() '
    )
    return phone.translate(table).strip()


def _normalize_accounting_type(val: str | None) -> str | None:
    """会計種類の文字列を正規化する。

    先頭・末尾の空白除去、全角空白→半角変換を行う。

    Args:
        val: 会計種類文字列

    Returns:
        正規化後の文字列
    """
    if not val or not isinstance(val, str) or pd.isna(val):
        return None
    normalized = val.strip().replace("　", " ")
    return normalized if normalized else None


# 電話番号カラム
PHONE_COLUMNS = [
    "電話番号",
    "FAX番号",
]

# パーセント表記カラム
PERCENTAGE_COLUMNS = [
    "経験10年以上割合",
]


def normalize(filepath: str | Path) -> pd.DataFrame:
    """CSVファイルを読み込み、正規化済みDataFrameを返す。

    19カラム（簡易版）でも76カラム（フル版）でも動作する。
    存在しないカラムは自動的にスキップされる。

    Args:
        filepath: 入力CSVファイルパス

    Returns:
        正規化済みDataFrame
    """
    df = read_csv(filepath)
    print(f"  読み込み完了: {len(df)}件, {len(df.columns)}カラム")

    # 数値カラムの変換（基本 + 76カラム用追加）
    all_numeric = NUMERIC_COLUMNS + EXTRA_NUMERIC_COLUMNS
    df = _convert_numeric(df, all_numeric)

    # Float型カラムの変換（都道府県平均、年齢、勤続年数等）
    df = _convert_float(df, FLOAT_COLUMNS)

    # 日付カラムの変換
    all_dates = DATE_COLUMNS + EXTRA_DATE_COLUMNS
    df = _convert_dates(df, all_dates)

    # 加算フィールドのブール変換
    df = _convert_boolean(df, ADDITION_COLUMNS)

    # 品質フィールドのブール変換
    df = _convert_boolean(df, QUALITY_COLUMNS)

    # 賃金フィールドの数値変換
    df = _convert_wages(df, WAGE_COLUMNS)

    # パーセント表記の数値変換（'72.7％' → 72.7）
    df = _convert_percentage(df, PERCENTAGE_COLUMNS)

    # 会計種類の文字列正規化
    if "会計種類" in df.columns:
        df["会計種類"] = df["会計種類"].apply(_normalize_accounting_type)

    # 電話番号の全角→半角変換
    for col in PHONE_COLUMNS:
        if col in df.columns:
            df[col] = df[col].apply(_normalize_phone)
    phone_cols = [c for c in PHONE_COLUMNS if c in df.columns]
    if phone_cols:
        print(f"  電話番号正規化: {len(phone_cols)}カラム（全角→半角変換）")

    # 正規化完了カラム数をレポート
    converted_cols = [c for c in all_numeric if c in df.columns]
    print(f"  数値変換: {len(converted_cols)}カラム")

    float_cols = [c for c in FLOAT_COLUMNS if c in df.columns]
    if float_cols:
        print(f"  Float変換: {len(float_cols)}カラム")

    bool_cols = [c for c in ADDITION_COLUMNS + QUALITY_COLUMNS if c in df.columns]
    if bool_cols:
        print(f"  ブール変換: {len(bool_cols)}カラム（加算+品質フィールド）")

    wage_cols = [c for c in WAGE_COLUMNS if c in df.columns]
    if wage_cols:
        print(f"  賃金変換: {len(wage_cols)}カラム")

    pct_cols = [c for c in PERCENTAGE_COLUMNS if c in df.columns]
    if pct_cols:
        print(f"  パーセント変換: {len(pct_cols)}カラム")

    date_extra_cols = [c for c in EXTRA_DATE_COLUMNS if c in df.columns]
    if date_extra_cols:
        print(f"  行政処分日付変換: {len(date_extra_cols)}カラム")

    return df
