"""
enrich.py - 派生指標の計算モジュール
=====================================
正規化済みDataFrameに対して、分析用の派生指標を追加する。
22の派生指標（既存9 + 新規13）を計算する。
"""

import re

import numpy as np
import pandas as pd

from .normalize import ADDITION_COLUMNS, QUALITY_COLUMNS

# 都道府県抽出用の正規表現
_PREFECTURE_PATTERN = re.compile(r"(北海道|東京都|(?:京都|大阪)府|.+?県)")

# 市区町村抽出用の正規表現（都道府県の後に続く市区町村名）
_CITY_PATTERN = re.compile(
    r"(?:北海道|東京都|(?:京都|大阪)府|.+?県)"
    r"(.+?[市区町村])"
)

# 法人種別判定パターン（順序重要: 先にマッチしたものを採用）
_CORP_TYPE_PATTERNS: list[tuple[list[str], str]] = [
    (["株式会社", "有限会社", "合同会社", "合資会社"], "営利法人"),
    (["社会福祉法人"], "社会福祉法人"),
    (["社会医療法人"], "社会医療法人"),
    (["医療法人"], "医療法人"),
    (["特定非営利活動法人", "NPO", "ＮＰＯ"], "NPO法人"),
    (["社団法人", "一般社団"], "社団法人"),
    (["財団法人", "一般財団"], "財団法人"),
    (["事業団", "広域連合", "地方公共団体"], "地方公共団体"),
]

# 法人種別詳細: 社団・財団を分離するための判定パターン
_CORP_TYPE_DETAIL_PATTERNS: list[tuple[list[str], str]] = [
    (["株式会社"], "株式会社"),
    (["有限会社"], "有限会社"),
    (["合同会社"], "合同会社"),
    (["合資会社"], "合資会社"),
    (["社会福祉法人"], "社会福祉法人"),
    (["社会医療法人"], "社会医療法人"),
    (["医療法人"], "医療法人"),
    (["特定非営利活動法人", "NPO", "ＮＰＯ"], "NPO法人"),
    (["一般社団法人"], "一般社団法人"),
    (["公益社団法人"], "公益社団法人"),
    (["一般財団法人"], "一般財団法人"),
    (["公益財団法人"], "公益財団法人"),
    (["社団法人"], "社団法人"),
    (["財団法人"], "財団法人"),
    (["事業団", "広域連合", "地方公共団体"], "地方公共団体"),
]

# 行政区画名の末尾判定（ends_with）用
_LOCAL_GOV_SUFFIXES: list[str] = ["市", "区", "町", "村", "県", "都", "府", "道"]

# サービスコード → 大分類マッピング
_SERVICE_CATEGORY_MAP: dict[str, str] = {
    "11": "訪問系",
    "12": "訪問系",
    "13": "訪問系",
    "14": "訪問系",
    "15": "通所系",
    "16": "通所系",
    "17": "居宅支援",
    "21": "入所系",
    "22": "入所系",
    "31": "居宅支援",
    "32": "入所系",
    "33": "入所系",
    "34": "入所系",
    "35": "入所系",
    "46": "居宅支援",
    "51": "入所系",
    "61": "地域密着型",
    "62": "地域密着型",
    "63": "地域密着型",
    "64": "地域密着型",
    "65": "地域密着型",
    "66": "地域密着型",
    "67": "地域密着型",
    "68": "地域密着型",
    "71": "地域密着型",
    "72": "地域密着型",
}

# 事業所番号先頭2桁→都道府県名マッピング（フォールバック用）
_PREF_CODE_MAP: dict[int, str] = {
    1: '北海道', 2: '青森県', 3: '岩手県', 4: '宮城県', 5: '秋田県',
    6: '山形県', 7: '福島県', 8: '茨城県', 9: '栃木県', 10: '群馬県',
    11: '埼玉県', 12: '千葉県', 13: '東京都', 14: '神奈川県', 15: '新潟県',
    16: '富山県', 17: '石川県', 18: '福井県', 19: '山梨県', 20: '長野県',
    21: '岐阜県', 22: '静岡県', 23: '愛知県', 24: '三重県', 25: '滋賀県',
    26: '京都府', 27: '大阪府', 28: '兵庫県', 29: '奈良県', 30: '和歌山県',
    31: '鳥取県', 32: '島根県', 33: '岡山県', 34: '広島県', 35: '山口県',
    36: '徳島県', 37: '香川県', 38: '愛媛県', 39: '高知県', 40: '福岡県',
    41: '佐賀県', 42: '長崎県', 43: '熊本県', 44: '大分県', 45: '宮崎県',
    46: '鹿児島県', 47: '沖縄県',
}

# サービスコード別基本単価テーブル（1人1日あたり、要介護3想定、単位: 円）
# 仮の値: 後で調整可能
SERVICE_UNIT_PRICES: dict[str, int] = {
    "11": 3960,   # 訪問介護（身体30分〜1h）
    "12": 12500,  # 訪問入浴
    "13": 8190,   # 訪問看護
    "14": 3020,   # 訪問リハ
    "15": 7500,   # 通所介護（7h以上8h未満、要介護3）
    "16": 7260,   # 通所リハ
    "17": 1000,   # 福祉用具（月額概算）
    "21": 8290,   # 短期入所生活
    "22": 8270,   # 短期入所療養
    "31": 1000,   # 居宅介護支援（月額概算）
    "32": 8200,   # 特養（要介護3）
    "33": 8100,   # 老健（要介護3）
    "34": 7500,   # 介護療養型
    "35": 7800,   # 介護医療院
    "46": 500,    # 居宅療養管理
    "51": 7600,   # 特定施設入居者
    "61": 5700,   # 定期巡回
    "62": 3800,   # 夜間対応型訪問
    "63": 7500,   # 地域密着型通所
    "64": 9800,   # 認知症対応型通所
    "65": 8000,   # 小規模多機能
    "66": 8000,   # グループホーム（要介護3）
    "67": 7600,   # 地域密着型特定施設
    "68": 8200,   # 地域密着型老人福祉
    "71": 12400,  # 看護小規模多機能
    "72": 10000,  # 複合型サービス
}

# サービスコード別の取得可能加算数の上限（サービス種別で異なる）
# 13加算のうち、サービスの性質上取得不可なものを除いた数
_MAX_ADDITIONS_BY_SERVICE: dict[str, int] = {
    "15": 13,  # 通所介護: 全13加算が取得可能
    "16": 11,  # 通所リハ
    "32": 10,  # 特養
    "33": 10,  # 老健
    "63": 13,  # 地域密着型通所
    "66": 8,   # グループホーム
}
_DEFAULT_MAX_ADDITIONS = 10  # デフォルト取得可能数


# 新76カラムの加算カラム名（実際のCSVのカラム名）
NEW_ADDITION_COLUMNS = [
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


def _extract_prefecture(address: str | None) -> str | None:
    """住所から都道府県を抽出する。"""
    if not address or not isinstance(address, str):
        return None
    match = _PREFECTURE_PATTERN.search(address)
    return match.group(1) if match else None


def _extract_prefecture_with_fallback(address: str | None, jigyosho_no: str | None) -> str | None:
    """住所から都道府県を抽出し、失敗した場合は事業所番号先頭2桁から推定する。"""
    pref = _extract_prefecture(address)
    if pref:
        return pref
    if jigyosho_no and isinstance(jigyosho_no, str) and len(jigyosho_no) >= 2:
        code_str = jigyosho_no[:2]
        if code_str.isdigit():
            return _PREF_CODE_MAP.get(int(code_str))
    return None


def _extract_city(address: str | None) -> str | None:
    """住所から市区町村を抽出する。"""
    if not address or not isinstance(address, str):
        return None
    match = _CITY_PATTERN.search(address)
    return match.group(1) if match else None


def _classify_corp_type(corp_name: str | None) -> str:
    """法人名から法人種別を推定する。"""
    if not corp_name or not isinstance(corp_name, str):
        return "その他"
    name = corp_name.strip()
    for keywords, category in _CORP_TYPE_PATTERNS:
        if any(kw in name for kw in keywords):
            return category
    if any(name.endswith(suffix) for suffix in _LOCAL_GOV_SUFFIXES):
        return "地方公共団体"
    return "その他"


def _classify_corp_type_detail(corp_name: str | None) -> str:
    """法人名から法人種別詳細を推定する（社団法人・財団法人を分離）。"""
    if not corp_name or not isinstance(corp_name, str):
        return "その他"
    name = corp_name.strip()
    for keywords, category in _CORP_TYPE_DETAIL_PATTERNS:
        if any(kw in name for kw in keywords):
            return category
    if any(name.endswith(suffix) for suffix in _LOCAL_GOV_SUFFIXES):
        return "地方公共団体"
    return "その他"


def _classify_employee_scale(total: int | None) -> str | None:
    """従業者合計数から規模区分を返す。"""
    if total is None or pd.isna(total):
        return None
    if total <= 10:
        return "小規模"
    if total <= 30:
        return "中規模"
    if total <= 50:
        return "中大規模"
    return "大規模"


def _classify_service_category(jigyosho_no: str | None) -> str:
    """事業所番号の3-4桁目からサービス大分類を判定する。"""
    if not jigyosho_no or not isinstance(jigyosho_no, str) or len(jigyosho_no) < 4:
        return "その他"
    code = jigyosho_no[2:4]
    return _SERVICE_CATEGORY_MAP.get(code, "その他")


def _get_service_code(jigyosho_no: str | None) -> str | None:
    """事業所番号の3-4桁目からサービスコードを抽出する。"""
    if not jigyosho_no or not isinstance(jigyosho_no, str) or len(jigyosho_no) < 4:
        return None
    return jigyosho_no[2:4]


def _get_treatment_level(row: pd.Series) -> str | None:
    """処遇改善加算の最高ランクを判定する（I〜IVのうち最高）。

    Args:
        row: DataFrame行

    Returns:
        "I", "II", "III", "IV", または None（未取得）
    """
    levels = [
        ("加算_処遇改善I", "I"),
        ("加算_処遇改善II", "II"),
        ("加算_処遇改善III", "III"),
        ("加算_処遇改善IV", "IV"),
    ]
    for col, level in levels:
        if col in row.index and row.get(col) is True:
            return level
    return None


def _compute_quality_score(row: pd.Series, pref_avg_turnover: dict[str, float]) -> float:
    """品質スコア（100点満点）を計算する。

    スコアリングモデル:
      安全・リスク管理（30点）:
        BCP策定(10) + 損害賠償保険(10) + 行政処分なし(10)
      品質管理（25点）:
        第三者評価(15) + ICT活用(10)
      人材安定性（25点）:
        低離職率(10) + 高常勤比率(8) + 経験10年以上(7)
      収益安定性（20点）:
        高稼働率(10) + 多加算取得(10)

    Args:
        row: DataFrame行
        pref_avg_turnover: 都道府県別平均離職率

    Returns:
        品質スコア（0〜100）
    """
    score = 0.0

    # --- 安全・リスク管理（30点）---
    if row.get("品質_BCP策定") is True:
        score += 10.0
    if row.get("品質_損害賠償保険") is True:
        score += 10.0
    # 行政処分なし = 行政処分日がNaT（データなし）
    if pd.isna(row.get("行政処分日")):
        score += 10.0

    # --- 品質管理（25点）---
    if row.get("品質_第三者評価") is True:
        score += 15.0
    if row.get("品質_ICT活用") is True:
        score += 10.0

    # --- 人材安定性（25点）---
    turnover = row.get("離職率")
    pref = row.get("都道府県")
    if pd.notna(turnover) and pref and pref in pref_avg_turnover:
        if turnover < pref_avg_turnover[pref]:
            score += 10.0
    elif pd.notna(turnover) and turnover < 15.0:
        # 都道府県平均が不明な場合、全国平均15%を仮基準とする
        score += 10.0

    fulltime_ratio = row.get("常勤比率")
    if pd.notna(fulltime_ratio) and fulltime_ratio > 50.0:
        score += 8.0

    exp_ratio = row.get("経験10年以上割合")
    if pd.notna(exp_ratio) and exp_ratio > 30.0:
        score += 7.0

    # --- 収益安定性（20点）---
    occupancy = row.get("稼働率")
    if pd.notna(occupancy) and occupancy > 80.0:
        score += 10.0

    addition_count = row.get("加算取得数")
    if pd.notna(addition_count) and addition_count >= 5:
        score += 10.0

    return round(score, 1)


def _quality_rank(score: float | None) -> str | None:
    """品質スコアからランクを判定する。"""
    if score is None or pd.isna(score):
        return None
    if score >= 80:
        return "S"
    if score >= 65:
        return "A"
    if score >= 50:
        return "B"
    if score >= 35:
        return "C"
    return "D"


def enrich(df: pd.DataFrame) -> pd.DataFrame:
    """DataFrameに派生指標を追加する。

    19カラム（簡易版）でも76カラム（フル版）でも動作する。
    存在しないカラムを参照する計算は自動的にスキップされる。

    追加される派生指標（最大22項目）:
    【既存9】都道府県、市区町村、法人種別、離職率、採用率、常勤比率、
            事業年数、従業者規模区分、サービス大分類
    【新規13】稼働率、加算取得数、処遇改善加算レベル、品質スコア、品質ランク、
             要介護度平均、重度率、利用者対平均比、1人当たり利用者、
             加算改善余地、推定月間収益、賃金_代表月額、法人種別詳細

    Args:
        df: 正規化済みDataFrame

    Returns:
        派生指標が追加されたDataFrame
    """
    enriched_count = 0

    # --- 住所系指標 ---
    if "住所" in df.columns:
        if "事業所番号" in df.columns:
            df["都道府県"] = df.apply(
                lambda row: _extract_prefecture_with_fallback(
                    row.get("住所"), row.get("事業所番号")
                ),
                axis=1,
            )
        else:
            df["都道府県"] = df["住所"].apply(_extract_prefecture)
        df["市区町村"] = df["住所"].apply(_extract_city)
        enriched_count += 2
        pref_count = df["都道府県"].notna().sum()
        print(f"  都道府県抽出: {pref_count}/{len(df)}件")
    # フォールバック: 都道府県名カラムが存在する場合（76カラムCSV）
    elif "都道府県名" in df.columns and "都道府県" not in df.columns:
        df["都道府県"] = df["都道府県名"]
        enriched_count += 1

    # --- 法人種別 ---
    if "法人名" in df.columns:
        df["法人種別"] = df["法人名"].apply(_classify_corp_type)
        enriched_count += 1

    # --- 法人種別詳細（新規: 社団法人・財団法人を分離）---
    if "法人名" in df.columns:
        df["法人種別詳細"] = df["法人名"].apply(_classify_corp_type_detail)
        enriched_count += 1

    # --- 離職率 ---
    if "前年度退職数" in df.columns and "従業者_合計" in df.columns:
        total_f = df["従業者_合計"].astype("Float64")
        retire_f = df["前年度退職数"].astype("Float64")
        denominator = total_f + retire_f
        mask = denominator.fillna(0) > 0
        df["離職率"] = pd.Series(np.nan, index=df.index, dtype="Float64")
        df.loc[mask, "離職率"] = (retire_f[mask] / denominator[mask] * 100).round(1)
        enriched_count += 1

    # --- 採用率 ---
    if "前年度採用数" in df.columns and "従業者_合計" in df.columns:
        total_f = df["従業者_合計"].astype("Float64")
        hire_f = df["前年度採用数"].astype("Float64")
        mask = total_f.fillna(0) > 0
        df["採用率"] = pd.Series(np.nan, index=df.index, dtype="Float64")
        df.loc[mask, "採用率"] = (hire_f[mask] / total_f[mask] * 100).round(1)
        enriched_count += 1

    # --- 常勤比率 ---
    if "従業者_常勤" in df.columns and "従業者_合計" in df.columns:
        total_f = df["従業者_合計"].astype("Float64")
        fulltime_f = df["従業者_常勤"].astype("Float64")
        mask = total_f.fillna(0) > 0
        df["常勤比率"] = pd.Series(np.nan, index=df.index, dtype="Float64")
        df.loc[mask, "常勤比率"] = (fulltime_f[mask] / total_f[mask] * 100).round(1)
        enriched_count += 1

    # --- 事業年数 ---
    if "事業開始日" in df.columns:
        current_year = 2026
        df["事業年数"] = df["事業開始日"].apply(
            lambda x: current_year - x.year if pd.notna(x) else None
        )
        df["事業年数"] = df["事業年数"].astype("Int64")
        enriched_count += 1

    # --- 従業者規模区分 ---
    if "従業者_合計" in df.columns:
        df["従業者規模区分"] = df["従業者_合計"].apply(_classify_employee_scale)
        enriched_count += 1

    # --- サービス大分類 ---
    if "事業所番号" in df.columns:
        df["サービス大分類"] = df["事業所番号"].apply(_classify_service_category)
        enriched_count += 1

    # =========================================================
    # 以下: 新規追加の派生指標（76カラム用、存在しないカラムはスキップ）
    # =========================================================

    # --- 1. 稼働率 = 利用者総数 / 定員 * 100 ---
    if "利用者総数" in df.columns and "定員" in df.columns:
        cap_f = df["定員"].astype("Float64")
        users_f = df["利用者総数"].astype("Float64")
        mask = cap_f.fillna(0) > 0
        df["稼働率"] = pd.Series(np.nan, index=df.index, dtype="Float64")
        df.loc[mask, "稼働率"] = (users_f[mask] / cap_f[mask] * 100).round(1)
        enriched_count += 1
        valid = df["稼働率"].notna().sum()
        print(f"  稼働率計算: {valid}件")

    # --- 2. 加算取得数 = 13加算項目のTrueカウント ---
    # 新76カラム名と旧カラム名の両方に対応
    existing_new_addition_cols = [c for c in NEW_ADDITION_COLUMNS if c in df.columns]
    existing_old_addition_cols = [c for c in ADDITION_COLUMNS if c in df.columns and c not in NEW_ADDITION_COLUMNS]
    # 新カラムを優先、なければ旧カラムを使う
    addition_cols_for_count = existing_new_addition_cols if existing_new_addition_cols else existing_old_addition_cols
    if addition_cols_for_count:
        df["加算取得数"] = df[addition_cols_for_count].sum(axis=1).astype("Int64")
        enriched_count += 1
        print(f"  加算取得数計算: {len(addition_cols_for_count)}項目から集計")

    # --- 3. 処遇改善加算レベル = I〜IVのうち最高ランク ---
    treatment_cols = [c for c in ["加算_処遇改善I", "加算_処遇改善II", "加算_処遇改善III", "加算_処遇改善IV"] if c in df.columns]
    if treatment_cols:
        df["処遇改善加算レベル"] = df.apply(_get_treatment_level, axis=1)
        enriched_count += 1

    # --- 6. 要介護度平均 = (1*要介護1 + 2*要介護2 + ... + 5*要介護5) / 利用者総数 ---
    care_level_cols = ["要介護1", "要介護2", "要介護3", "要介護4", "要介護5"]
    existing_cl = [c for c in care_level_cols if c in df.columns]
    if existing_cl and "利用者総数" in df.columns:
        weights = {"要介護1": 1, "要介護2": 2, "要介護3": 3, "要介護4": 4, "要介護5": 5}
        weighted_sum = pd.Series(0.0, index=df.index)
        for col in existing_cl:
            weighted_sum += df[col].astype("Float64").fillna(0) * weights[col]
        users_f = df["利用者総数"].astype("Float64")
        mask = users_f.fillna(0) > 0
        df["要介護度平均"] = pd.Series(np.nan, index=df.index, dtype="Float64")
        df.loc[mask, "要介護度平均"] = (weighted_sum[mask] / users_f[mask]).round(2)
        enriched_count += 1

    # --- 7. 重度率 = (要介護4 + 要介護5) / 利用者総数 * 100 ---
    if "要介護4" in df.columns and "要介護5" in df.columns and "利用者総数" in df.columns:
        cl4 = df["要介護4"].astype("Float64").fillna(0)
        cl5 = df["要介護5"].astype("Float64").fillna(0)
        users_f = df["利用者総数"].astype("Float64")
        mask = users_f.fillna(0) > 0
        df["重度率"] = pd.Series(np.nan, index=df.index, dtype="Float64")
        df.loc[mask, "重度率"] = ((cl4[mask] + cl5[mask]) / users_f[mask] * 100).round(1)
        enriched_count += 1

    # --- 8. 利用者対平均比 = 利用者総数 / 利用者_都道府県平均 ---
    if "利用者総数" in df.columns and "利用者_都道府県平均" in df.columns:
        users_f = df["利用者総数"].astype("Float64")
        avg_f = df["利用者_都道府県平均"].astype("Float64")
        mask = avg_f.fillna(0) > 0
        df["利用者対平均比"] = pd.Series(np.nan, index=df.index, dtype="Float64")
        df.loc[mask, "利用者対平均比"] = (users_f[mask] / avg_f[mask]).round(2)
        enriched_count += 1

    # --- 9. 1人当たり利用者 = 利用者総数 / 従業者_合計 ---
    if "利用者総数" in df.columns and "従業者_合計" in df.columns:
        users_f = df["利用者総数"].astype("Float64")
        staff_f = df["従業者_合計"].astype("Float64")
        mask = staff_f.fillna(0) > 0
        df["1人当たり利用者"] = pd.Series(np.nan, index=df.index, dtype="Float64")
        df.loc[mask, "1人当たり利用者"] = (users_f[mask] / staff_f[mask]).round(2)
        enriched_count += 1

    # --- 10. 加算改善余地 = 取得可能加算数 - 現取得数 ---
    if "加算取得数" in df.columns and "事業所番号" in df.columns:
        def _calc_improvement_room(row):
            code = _get_service_code(row.get("事業所番号"))
            max_additions = _MAX_ADDITIONS_BY_SERVICE.get(code, _DEFAULT_MAX_ADDITIONS) if code else _DEFAULT_MAX_ADDITIONS
            current = row.get("加算取得数")
            if pd.isna(current):
                return None
            return max(0, max_additions - int(current))
        df["加算改善余地"] = df.apply(_calc_improvement_room, axis=1).astype("Int64")
        enriched_count += 1

    # --- 11. 推定月間収益 = 利用者総数 * サービス基本単価 * (1 + 加算率) * 30日 ---
    if "利用者総数" in df.columns and "事業所番号" in df.columns:
        def _calc_revenue_estimate(row):
            users = row.get("利用者総数")
            if pd.isna(users) or users == 0:
                return None
            code = _get_service_code(row.get("事業所番号"))
            if not code or code not in SERVICE_UNIT_PRICES:
                return None
            unit_price = SERVICE_UNIT_PRICES[code]
            # 加算率: 加算取得数に応じて概算（1加算あたり約3%）
            addition_count = row.get("加算取得数")
            addition_rate = 0.0
            if pd.notna(addition_count):
                addition_rate = float(addition_count) * 0.03
            # 月間 = 1日単価 * 利用者数 * 30日 * (1 + 加算率)
            return round(float(users) * unit_price * 30 * (1 + addition_rate))
        df["推定月間収益"] = df.apply(_calc_revenue_estimate, axis=1).astype("Float64")
        enriched_count += 1

    # --- 12. 賃金_代表月額 = 賃金_月額1〜5の中央値 ---
    wage_monthly_cols = [f"賃金_月額{i}" for i in range(1, 6)]
    existing_wage_cols = [c for c in wage_monthly_cols if c in df.columns]
    if existing_wage_cols:
        # 各行で非NaN値の中央値を計算
        df["賃金_代表月額"] = df[existing_wage_cols].median(axis=1).astype("Float64")
        enriched_count += 1
        valid_wage = df["賃金_代表月額"].notna().sum()
        print(f"  賃金_代表月額計算: {valid_wage}件（{len(existing_wage_cols)}カラムの中央値）")

    # --- 4/5. 品質スコア + 品質ランク（新スコアリングモデル）---
    # 品質スコアの計算には離職率・常勤比率・稼働率・加算取得数が必要
    # 存在する品質カラムがあれば計算を試みる
    quality_cols_present = [c for c in QUALITY_COLUMNS if c in df.columns]
    if quality_cols_present:
        # 都道府県別平均離職率を計算（品質スコア計算用）
        pref_avg_turnover: dict[str, float] = {}
        if "都道府県" in df.columns and "離職率" in df.columns:
            pref_turnover = df.groupby("都道府県")["離職率"].mean()
            pref_avg_turnover = pref_turnover.dropna().to_dict()

        df["品質スコア"] = df.apply(
            lambda row: _compute_quality_score(row, pref_avg_turnover), axis=1
        ).astype("Float64")
        df["品質ランク"] = df["品質スコア"].apply(_quality_rank)
        enriched_count += 2
        print(f"  品質スコア計算: {df['品質スコア'].notna().sum()}件")
        if "品質ランク" in df.columns:
            rank_dist = df["品質ランク"].value_counts()
            print(f"  品質ランク分布: {dict(rank_dist)}")
    elif [c for c in ADDITION_COLUMNS if c in df.columns]:
        # 品質カラムがない場合は旧ロジック（加算ベースの簡易スコア）
        _QUALITY_WEIGHTS_OLD: dict[str, float] = {
            "サービス提供体制強化加算": 10.0,
            "介護職員処遇改善加算": 10.0,
            "介護職員等特定処遇改善加算": 8.0,
            "介護職員等ベースアップ等支援加算": 8.0,
            "中重度者ケア体制加算": 7.0,
            "認知症加算": 7.0,
            "栄養アセスメント加算": 6.0,
            "口腔・栄養スクリーニング加算": 6.0,
            "科学的介護推進体制加算": 8.0,
            "入浴介助加算": 5.0,
            "ADL維持等加算": 8.0,
            "若年性認知症利用者受入加算": 5.0,
            "中山間地域等における小規模事業所加算": 2.0,
        }
        existing_old = [c for c in ADDITION_COLUMNS if c in df.columns and c not in NEW_ADDITION_COLUMNS]
        if existing_old:
            score = pd.Series(0.0, index=df.index)
            for col in existing_old:
                weight = _QUALITY_WEIGHTS_OLD.get(col, 5.0)
                score += df[col].astype(float) * weight
            max_possible = sum(_QUALITY_WEIGHTS_OLD.get(c, 5.0) for c in existing_old)
            if max_possible > 0:
                df["品質スコア"] = (score / max_possible * 100).round(1).astype("Float64")
            else:
                df["品質スコア"] = pd.Series(dtype="Float64")
            enriched_count += 1

    print(f"  派生指標追加: {enriched_count}カラム")
    return df
