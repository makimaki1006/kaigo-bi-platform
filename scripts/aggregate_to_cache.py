"""
Turso facilities テーブルから全データを取得し、全APIエンドポイント用の
集計結果を kpi_cache テーブルに書き込む事前集計スクリプト。

Rustアグリゲータの完全な代替として機能する。
冪等性あり（何度実行しても同じ結果）。
"""

import json
import math
import os
import sys
import time
from datetime import datetime

import pandas as pd
import requests

from turso_helpers import (
    get_turso_config,
    get_headers,
    execute_sql as _execute_sql_raw,
    execute_single as _execute_single_raw,
    make_arg,
    safe_float as _safe_float_base,
)

# Windows環境でのUTF-8出力対応
sys.stdout.reconfigure(encoding="utf-8")

# ============================================================
# Turso接続設定（環境変数必須、フォールバックなし）
# ============================================================
TURSO_URL, TURSO_TOKEN = get_turso_config()
HEADERS = get_headers(TURSO_TOKEN)
PAGE_SIZE = 5000

# ============================================================
# 加算カラム定義
# ============================================================
KASAN_COLUMNS = [
    "加算_処遇改善I", "加算_処遇改善II", "加算_処遇改善III", "加算_処遇改善IV",
    "加算_特定事業所I", "加算_特定事業所II", "加算_特定事業所III", "加算_特定事業所IV",
    "加算_特定事業所V", "加算_認知症ケアI", "加算_認知症ケアII",
    "加算_口腔連携", "加算_緊急時",
]

# 品質カラム定義
QUALITY_COLUMNS = ["品質_BCP策定", "品質_ICT活用", "品質_第三者評価", "品質_損害賠償保険"]

# 賃金カラム定義
SALARY_JOB_COLS = [f"賃金_職種{i}" for i in range(1, 6)]
SALARY_AMOUNT_COLS = [f"賃金_月額{i}" for i in range(1, 6)]
SALARY_AGE_COLS = [f"賃金_平均年齢{i}" for i in range(1, 6)]
SALARY_TENURE_COLS = [f"賃金_平均勤続{i}" for i in range(1, 6)]


# ============================================================
# Turso API ヘルパー（turso_helpers のラッパー）
# ============================================================
def execute_sql(statements):
    """Turso HTTP API v2 pipeline でSQLを実行"""
    return _execute_sql_raw(TURSO_URL, HEADERS, statements)


def execute_single(sql, args=None):
    """単一SQLを実行"""
    return _execute_single_raw(TURSO_URL, HEADERS, sql, args)


# ============================================================
# データ取得
# ============================================================
def fetch_all_rows():
    """facilities テーブルから全行をページネーションで取得"""
    print("[1/4] facilities テーブルからデータ取得中...")

    # 総件数取得
    result = execute_single("SELECT COUNT(*) FROM facilities")
    total = int(result["results"][0]["response"]["result"]["rows"][0][0]["value"])
    print(f"  総件数: {total:,}")

    if total == 0:
        return pd.DataFrame()

    # カラム名取得（最初の1行で確認）
    result = execute_single("SELECT * FROM facilities LIMIT 1")
    cols_info = result["results"][0]["response"]["result"]["cols"]
    col_names = [c["name"] for c in cols_info]
    print(f"  カラム数: {len(col_names)}")

    # ページネーションで全行取得
    all_rows = []
    offset = 0
    start_time = time.time()

    while offset < total:
        sql = f"SELECT * FROM facilities LIMIT {PAGE_SIZE} OFFSET {offset}"
        result = execute_single(sql)
        # レスポンスのエラーチェック
        res = result.get("results", [{}])[0]
        if "error" in res:
            print(f"\n  SQLエラー (offset={offset}): {res['error']}")
            break
        try:
            rows_data = res["response"]["result"]["rows"]
        except (KeyError, TypeError) as e:
            print(f"\n  レスポンス解析エラー (offset={offset}): {e}")
            print(f"  レスポンス: {str(res)[:200]}")
            break

        for row in rows_data:
            parsed = {}
            for i, cell in enumerate(row):
                if cell["type"] == "null":
                    parsed[col_names[i]] = None
                else:
                    parsed[col_names[i]] = cell.get("value")
            all_rows.append(parsed)

        offset += PAGE_SIZE
        elapsed = time.time() - start_time
        pct = min(offset, total) / total * 100
        rate = len(all_rows) / elapsed if elapsed > 0 else 0
        sys.stdout.write(
            f"\r  取得中: {min(offset, total):,}/{total:,} ({pct:.1f}%) "
            f"| {rate:.0f}件/秒"
        )
        sys.stdout.flush()

    print(f"\n  取得完了: {len(all_rows):,}件 ({time.time() - start_time:.1f}秒)")
    return pd.DataFrame(all_rows)


# ============================================================
# パース・変換ユーティリティ
# ============================================================
def safe_float(val):
    """文字列を安全にfloatに変換"""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip()
    if not s:
        return None
    # ％記号除去
    s = s.replace("％", "").replace("%", "")
    # カンマ除去
    s = s.replace(",", "")
    # 全角数字を半角に変換
    s = s.translate(str.maketrans("０１２３４５６７８９．", "0123456789."))
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def safe_int(val):
    """文字列を安全にintに変換"""
    f = safe_float(val)
    if f is None:
        return None
    return int(f)


def is_kasan_true(val):
    """加算カラムの値が有効（取得済み）かを判定"""
    if val is None:
        return False
    s = str(val).strip()
    if not s:
        return False
    # 「×」「なし」は false
    if s in ("×", "なし", "―", "-", "0"):
        return False
    # 「○」「あり」またはその他の非空値は true
    return True


def parse_experience_ratio(val):
    """経験10年以上割合をパース（パーセント値 → 0-1の比率）"""
    f = safe_float(val)
    if f is None:
        return None
    # 100以上の場合はパーセント値として扱う
    if f > 1:
        return f / 100.0
    return f


def round_safe(val, digits=4):
    """NaN/None安全なround"""
    if val is None or (isinstance(val, float) and (math.isnan(val) or math.isinf(val))):
        return None
    return round(val, digits)


# ============================================================
# DataFrame前処理
# ============================================================
def preprocess(df):
    """数値カラムの型変換・派生値計算"""
    print("[2/4] データ前処理中...")

    # 数値カラム変換
    numeric_cols = [
        "従業者_常勤", "従業者_非常勤", "従業者_合計",
        "前年度採用数", "前年度退職数",
        "利用者総数", "定員",
        "要介護1", "要介護2", "要介護3", "要介護4", "要介護5",
        "turnover_rate", "fulltime_ratio", "years_in_business",
        # deltaスクレイピング: 職種別従業者（6職種×常勤/非常勤/合計）
        "介護職員_常勤", "介護職員_非常勤", "介護職員_合計",
        "看護職員_常勤", "看護職員_非常勤", "看護職員_合計",
        "生活相談員_常勤", "生活相談員_非常勤", "生活相談員_合計",
        "機能訓練指導員_常勤", "機能訓練指導員_非常勤", "機能訓練指導員_合計",
        "管理栄養士_常勤", "管理栄養士_非常勤", "管理栄養士_合計",
        "事務員_常勤", "事務員_非常勤", "事務員_合計",
        # deltaスクレイピング: 資格保有者数
        "介護福祉士数", "実務者研修数", "初任者研修数", "介護支援専門員数",
        # deltaスクレイピング: 夜勤・宿直
        "夜勤人数", "宿直人数",
        # deltaスクレイピング: 認知症研修
        "認知症指導者研修数", "認知症リーダー研修数", "認知症実践者研修数",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].apply(safe_float)

    # 加算カラム → bool変換
    for col in KASAN_COLUMNS:
        if col in df.columns:
            df[f"{col}_bool"] = df[col].apply(is_kasan_true)

    # 加算取得数
    kasan_bool_cols = [f"{c}_bool" for c in KASAN_COLUMNS if f"{c}_bool" in df.columns]
    if kasan_bool_cols:
        df["kasan_count"] = df[kasan_bool_cols].sum(axis=1)

    # 品質カラム → bool変換
    for col in QUALITY_COLUMNS:
        if col in df.columns:
            df[f"{col}_bool"] = df[col].apply(is_kasan_true)

    # 経験10年以上割合
    if "経験10年以上割合" in df.columns:
        df["exp_10yr_ratio"] = df["経験10年以上割合"].apply(parse_experience_ratio)

    # 稼働率計算（定員があるサービスのみ）
    if "利用者総数" in df.columns and "定員" in df.columns:
        users = df["利用者総数"]
        cap = df["定員"]
        # 定員が1-500の有効範囲のみ
        valid_mask = cap.notna() & (cap >= 1) & (cap <= 500) & users.notna()
        df["occupancy"] = None
        df.loc[valid_mask, "occupancy"] = users[valid_mask] / cap[valid_mask]

    print(f"  前処理完了: {len(df):,}件")
    return df


# ============================================================
# 集計関数群
# ============================================================

def agg_dashboard_kpi(df):
    """dashboard_kpi: メインKPI"""
    total = len(df)
    corps = df["法人番号"].dropna().nunique()
    avg_staff = round_safe(df["従業者_合計"].mean())

    # turnover_rate: 0-1範囲のみ
    tr = df["turnover_rate"].dropna()
    tr = tr[(tr >= 0) & (tr <= 1)]
    avg_tr = round_safe(tr.mean())

    # fulltime_ratio: 0-1範囲のみ
    fr = df["fulltime_ratio"].dropna()
    fr = fr[(fr >= 0) & (fr <= 1)]
    avg_fr = round_safe(fr.mean())

    avg_years = round_safe(df["years_in_business"].dropna().mean())

    avg_kasan = round_safe(df["kasan_count"].mean()) if "kasan_count" in df.columns else None

    return {
        "total_facilities": total,
        "total_corps": corps,
        "avg_staff": avg_staff,
        "avg_turnover_rate": avg_tr,
        "avg_fulltime_ratio": avg_fr,
        "avg_years": avg_years,
        "avg_kasan_count": avg_kasan,
    }


def agg_dashboard_by_prefecture(df):
    """dashboard_by_prefecture: 都道府県別KPI"""
    valid = df[df["prefecture"].notna()].copy()
    tr = valid["turnover_rate"].copy()
    tr[(tr < 0) | (tr > 1)] = None
    valid["tr_clean"] = tr

    grouped = valid.groupby("prefecture").agg(
        facility_count=("prefecture", "size"),
        avg_staff=("従業者_合計", "mean"),
        avg_turnover_rate=("tr_clean", "mean"),
    ).reset_index()

    result = []
    for _, row in grouped.iterrows():
        result.append({
            "prefecture": row["prefecture"],
            "facility_count": int(row["facility_count"]),
            "avg_staff": round_safe(row["avg_staff"]),
            "avg_turnover_rate": round_safe(row["avg_turnover_rate"]),
        })
    return result


def agg_dashboard_by_service(df):
    """dashboard_by_service: サービス種別件数"""
    if "サービスコード" not in df.columns:
        return []
    grouped = df.groupby(["サービスコード", "サービス名"]).size().reset_index(name="facility_count")
    result = []
    for _, row in grouped.iterrows():
        result.append({
            "service_code": row["サービスコード"] or "",
            "service_name": row["サービス名"] or "",
            "facility_count": int(row["facility_count"]),
        })
    return sorted(result, key=lambda x: x["facility_count"], reverse=True)


def agg_market_choropleth(df):
    """market_choropleth: 都道府県別施設数"""
    valid = df[df["prefecture"].notna()]
    grouped = valid.groupby("prefecture").size().reset_index(name="facility_count")
    return [
        {"prefecture": row["prefecture"], "facility_count": int(row["facility_count"])}
        for _, row in grouped.iterrows()
    ]


def agg_market_by_service(df):
    """market_by_service: サービス種別件数（marketページ用）"""
    return agg_dashboard_by_service(df)


def agg_market_corp_donut(df):
    """market_corp_donut: 法人種別ドーナツチャート"""
    if "corp_type" not in df.columns:
        return []
    grouped = df.groupby("corp_type").size().reset_index(name="count")
    return [
        {"corp_type": row["corp_type"] or "不明", "count": int(row["count"])}
        for _, row in grouped.iterrows()
    ]


def agg_workforce_kpi(df):
    """workforce_kpi: 労働力KPI"""
    tr = df["turnover_rate"].dropna()
    tr = tr[(tr >= 0) & (tr <= 1)]

    # 採用率: 前年度採用数 / 従業者_合計
    # 採用率: ベクトル演算で高速化
    hired = df["前年度採用数"].dropna()
    total_staff = df["従業者_合計"].dropna()
    # 両方に値がある行のみ
    valid_mask = hired.index.intersection(total_staff.index)
    hired_v = hired.loc[valid_mask]
    total_v = total_staff.loc[valid_mask]
    valid = (total_v > 0) & (hired_v >= 0)
    hire_rates = hired_v[valid] / total_v[valid]
    hire_rates = hire_rates[(hire_rates >= 0) & (hire_rates <= 1)]
    avg_hire = round_safe(hire_rates.mean()) if len(hire_rates) > 0 else None

    fr = df["fulltime_ratio"].dropna()
    fr = fr[(fr >= 0) & (fr <= 1)]

    exp = df["exp_10yr_ratio"].dropna() if "exp_10yr_ratio" in df.columns else pd.Series(dtype=float)

    return {
        "avg_turnover_rate": round_safe(tr.mean()),
        "avg_hire_rate": avg_hire,
        "avg_fulltime_ratio": round_safe(fr.mean()),
        "avg_experience_10yr_ratio": round_safe(exp.mean()),
    }


def agg_workforce_turnover_dist(df):
    """workforce_turnover_dist: 離職率分布"""
    tr = df["turnover_rate"].dropna()
    tr = tr[(tr >= 0) & (tr <= 1)]
    # パーセント換算
    tr_pct = tr * 100

    bins = [
        ("0-5%", 0, 5),
        ("5-10%", 5, 10),
        ("10-15%", 10, 15),
        ("15-20%", 15, 20),
        ("20-25%", 20, 25),
        ("25-30%", 25, 30),
        ("30%+", 30, 101),
    ]
    result = []
    for label, lo, hi in bins:
        count = int(((tr_pct >= lo) & (tr_pct < hi)).sum())
        result.append({"range": label, "count": count})
    return result


def agg_workforce_by_prefecture(df):
    """workforce_by_prefecture: 都道府県別労働力指標"""
    valid = df[df["prefecture"].notna()].copy()
    tr = valid["turnover_rate"].copy()
    tr[(tr < 0) | (tr > 1)] = None
    valid["tr_clean"] = tr

    fr = valid["fulltime_ratio"].copy()
    fr[(fr < 0) | (fr > 1)] = None
    valid["fr_clean"] = fr

    grouped = valid.groupby("prefecture").agg(
        avg_turnover_rate=("tr_clean", "mean"),
        avg_fulltime_ratio=("fr_clean", "mean"),
        facility_count=("prefecture", "size"),
    ).reset_index()

    return [
        {
            "prefecture": row["prefecture"],
            "avg_turnover_rate": round_safe(row["avg_turnover_rate"]),
            "avg_fulltime_ratio": round_safe(row["avg_fulltime_ratio"]),
            "facility_count": int(row["facility_count"]),
        }
        for _, row in grouped.iterrows()
    ]


def agg_workforce_by_size(df):
    """workforce_by_size: 従業者規模別労働力指標"""
    staff = df["従業者_合計"].copy()
    tr = df["turnover_rate"].copy()
    tr[(tr < 0) | (tr > 1)] = None
    fr = df["fulltime_ratio"].copy()
    fr[(fr < 0) | (fr > 1)] = None

    bins = [
        ("1-10人", 1, 10),
        ("11-30人", 11, 30),
        ("31-50人", 31, 50),
        ("51-100人", 51, 100),
        ("101人+", 101, 99999),
    ]
    result = []
    for label, lo, hi in bins:
        mask = staff.notna() & (staff >= lo) & (staff <= hi)
        count = int(mask.sum())
        avg_tr = round_safe(tr[mask].mean()) if count > 0 else None
        avg_fr = round_safe(fr[mask].mean()) if count > 0 else None
        result.append({
            "size_label": label,
            "avg_turnover_rate": avg_tr,
            "avg_fulltime_ratio": avg_fr,
            "count": count,
        })
    return result


def agg_workforce_exp_dist(df):
    """workforce_exp_dist: 経験10年以上割合の分布"""
    if "exp_10yr_ratio" not in df.columns:
        return []
    exp = df["exp_10yr_ratio"].dropna()
    exp_pct = exp * 100

    bins = [
        ("0-10%", 0, 10),
        ("10-20%", 10, 20),
        ("20-30%", 20, 30),
        ("30-40%", 30, 40),
        ("40-50%", 40, 50),
        ("50%+", 50, 101),
    ]
    result = []
    for label, lo, hi in bins:
        count = int(((exp_pct >= lo) & (exp_pct < hi)).sum())
        result.append({"range": label, "count": count})
    return result


def agg_workforce_exp_turnover(df):
    """workforce_exp_turnover: 経験率×離職率（散布図データ）"""
    if "exp_10yr_ratio" not in df.columns:
        return []

    valid = df[df["prefecture"].notna()].copy()
    tr = valid["turnover_rate"].copy()
    tr[(tr < 0) | (tr > 1)] = None
    valid["tr_clean"] = tr

    exp = valid["exp_10yr_ratio"].copy()
    valid["exp_clean"] = exp

    # 都道府県ごとに平均値を算出
    grouped = valid.groupby("prefecture").agg(
        avg_experience_ratio=("exp_clean", "mean"),
        avg_turnover_rate=("tr_clean", "mean"),
    ).reset_index()

    return [
        {
            "avg_experience_ratio": round_safe(row["avg_experience_ratio"]),
            "avg_turnover_rate": round_safe(row["avg_turnover_rate"]),
            "prefecture": row["prefecture"],
        }
        for _, row in grouped.iterrows()
        if row["avg_experience_ratio"] is not None
        and not (isinstance(row["avg_experience_ratio"], float) and math.isnan(row["avg_experience_ratio"]))
    ]


def agg_revenue_kpi(df):
    """revenue_kpi: 収益系KPI"""
    total = len(df)

    # 平均加算取得数
    avg_kasan = round_safe(df["kasan_count"].mean()) if "kasan_count" in df.columns else None

    # 処遇改善取得率（いずれかの処遇改善が true）
    shogu_cols = [f"加算_処遇改善{r}_bool" for r in ["I", "II", "III", "IV"]
                  if f"加算_処遇改善{r}_bool" in df.columns]
    if shogu_cols:
        any_shogu = df[shogu_cols].any(axis=1)
        shogu_rate = round_safe(any_shogu.sum() / total)
    else:
        shogu_rate = None

    # 平均稼働率（定員のあるサービスのみ）
    occ = df["occupancy"].dropna() if "occupancy" in df.columns else pd.Series(dtype=float)
    avg_occ = round_safe(occ.mean()) if len(occ) > 0 else None

    # 平均定員（1-500の有効範囲）
    cap = df["定員"].dropna()
    cap = cap[(cap >= 1) & (cap <= 500)]
    avg_cap = round_safe(cap.mean()) if len(cap) > 0 else None

    # 品質スコア（後で計算するのと同じロジック）
    scores = compute_quality_scores(df)
    avg_qs = round_safe(scores.mean()) if len(scores) > 0 else None

    # 平均利用者数
    users = df["利用者総数"].dropna()
    avg_users = round_safe(users.mean()) if len(users) > 0 else None

    return {
        "avg_kasan_count": avg_kasan,
        "syogu_kaizen_rate": shogu_rate,
        "avg_occupancy_rate": avg_occ,
        "avg_capacity": avg_cap,
        "avg_quality_score": avg_qs,
        "avg_users": avg_users,
    }


def agg_revenue_kasan_rates(df):
    """revenue_kasan_rates: 各加算の取得率"""
    total = len(df)
    result = []
    for col in KASAN_COLUMNS:
        bool_col = f"{col}_bool"
        if bool_col in df.columns:
            count_true = int(df[bool_col].sum())
            rate = round_safe(count_true / total) if total > 0 else 0
            result.append({
                "name": col,
                "rate": rate,
                "count_true": count_true,
                "total": total,
            })
    return result


def agg_kasan_detail_rates(df):
    """kasan_detail_rates: 加算_全項目JSONから全加算のサービス種別別取得率を集計"""
    if "加算_全項目" not in df.columns:
        return []

    # サービスコード別に集計
    from collections import defaultdict
    import json as _json

    # 全体集計用
    kasan_stats = defaultdict(lambda: {"count": 0, "total": 0, "service_codes": set()})

    # サービスコードカラムを事前に特定
    svc_col = "service_code" if "service_code" in df.columns else "サービスコード"
    kasan_col_idx = df.columns.get_loc("加算_全項目")
    svc_col_idx = df.columns.get_loc(svc_col) if svc_col in df.columns else None
    positive_values = {"○", "あり", "✓", "✔", "●"}

    for tup in df.itertuples(index=False):
        raw = tup[kasan_col_idx]
        if not raw or not isinstance(raw, str) or raw.strip() == "":
            continue
        try:
            kasan_dict = _json.loads(raw)
        except (ValueError, TypeError):
            continue

        svc = str(tup[svc_col_idx]) if svc_col_idx is not None else ""

        for kasan_name, value in kasan_dict.items():
            key = kasan_name.strip()
            if not key:
                continue
            kasan_stats[key]["total"] += 1
            kasan_stats[key]["service_codes"].add(svc)
            if value in positive_values:
                kasan_stats[key]["count"] += 1

    # 結果をリスト化（取得率でソート）
    result = []
    for name, stats in sorted(kasan_stats.items(), key=lambda x: -x[1]["count"]):
        rate = round_safe(stats["count"] / stats["total"]) if stats["total"] > 0 else 0
        result.append({
            "kasan_name": name,
            "rate": rate,
            "count": stats["count"],
            "total": stats["total"],
            "service_codes": sorted(stats["service_codes"]),
        })

    return result


def agg_workforce_staff_breakdown(df):
    """workforce_staff_breakdown: 職種別従業者の内訳（常勤/非常勤/合計、常勤比率）"""
    job_types = ["介護職員", "看護職員", "生活相談員", "機能訓練指導員", "管理栄養士", "事務員"]
    result = []
    for jt in job_types:
        ft_col = f"{jt}_常勤"
        pt_col = f"{jt}_非常勤"
        total_col = f"{jt}_合計"
        if total_col not in df.columns:
            continue
        vals = df[total_col].dropna()
        vals = vals[vals > 0]
        if len(vals) == 0:
            continue

        # 常勤比率: 常勤合計 / 合計合計
        fulltime_ratio = None
        if ft_col in df.columns:
            ft_sum = df[ft_col].dropna().sum()
            total_sum = df[total_col].dropna().sum()
            if total_sum > 0:
                fulltime_ratio = round_safe(ft_sum / total_sum)

        result.append({
            "job_type": jt,
            "avg_count": round_safe(vals.mean(), 2),
            "total_count": int(vals.sum()),
            "facility_count": len(vals),
            "fulltime_ratio": fulltime_ratio,
        })
    return result


def agg_workforce_qualifications(df):
    """workforce_qualifications: 資格保有者数の集計"""
    quals = ["介護福祉士数", "実務者研修数", "初任者研修数", "介護支援専門員数"]
    result = []
    for q in quals:
        if q not in df.columns:
            continue
        vals = df[q].dropna()
        vals = vals[vals >= 0]
        if len(vals) == 0:
            continue
        result.append({
            "qualification": q.replace("数", ""),
            "total": int(vals.sum()),
            "avg_per_facility": round_safe(vals.mean(), 2),
            "facility_count": len(vals),
        })
    return result


def agg_kasan_all_items(df):
    """kasan_all_items: 全加算項目の取得率（加算_全項目JSONから集計）"""
    if "加算_全項目" not in df.columns:
        return []

    from collections import defaultdict
    import json as _json

    kasan_counts = defaultdict(lambda: {"true": 0, "total": 0, "services": set()})

    svc_col = "service_code" if "service_code" in df.columns else "サービスコード"
    positive_values = {"○", "あり", "対応可能", "✓", "✔", "●"}

    kasan_col_idx = df.columns.get_loc("加算_全項目")
    svc_col_idx = df.columns.get_loc(svc_col) if svc_col in df.columns else None

    for tup in df.itertuples(index=False):
        raw = tup[kasan_col_idx]
        if not raw or not isinstance(raw, str) or raw.strip() == "":
            continue
        try:
            kasan_dict = _json.loads(raw)
        except (ValueError, TypeError):
            continue

        svc = str(tup[svc_col_idx]) if svc_col_idx is not None else ""

        for name, val in kasan_dict.items():
            key = name.strip()
            if not key:
                continue
            kasan_counts[key]["total"] += 1
            kasan_counts[key]["services"].add(svc)
            if val in positive_values:
                kasan_counts[key]["true"] += 1

    result = []
    for name, data in sorted(kasan_counts.items(), key=lambda x: -x[1]["true"]):
        result.append({
            "name": name,
            "rate": round_safe(data["true"] / max(data["total"], 1)),
            "count": data["true"],
            "total": data["total"],
            "service_codes": sorted(data["services"]),
        })
    return result


def agg_workforce_night_shift(df):
    """workforce_night_shift: 夜勤・宿直体制の集計"""
    result = {}
    for col in ["夜勤人数", "宿直人数"]:
        if col not in df.columns:
            continue
        vals = df[col].dropna()
        vals = vals[vals > 0]
        if len(vals) == 0:
            continue
        result[col] = {
            "avg": round_safe(vals.mean(), 2),
            "facility_count": len(vals),
            "total": int(vals.sum()),
        }
    return result


def agg_workforce_dementia_training(df):
    """workforce_dementia_training: 認知症研修修了者数の集計"""
    cols = ["認知症指導者研修数", "認知症リーダー研修数", "認知症実践者研修数"]
    result = []
    for col in cols:
        if col not in df.columns:
            continue
        vals = df[col].dropna()
        vals = vals[vals >= 0]
        if len(vals) == 0:
            continue
        result.append({
            "training": col.replace("数", ""),
            "total": int(vals.sum()),
            "avg_per_facility": round_safe(vals.mean(), 2),
            "facility_count": len(vals),
        })
    return result


def agg_revenue_occupancy_dist(df):
    """revenue_occupancy_dist: 稼働率分布"""
    if "occupancy" not in df.columns:
        return []
    occ = df["occupancy"].dropna()
    occ_pct = occ * 100

    bins = [
        ("0-50%", 0, 50),
        ("50-70%", 50, 70),
        ("70-90%", 70, 90),
        ("90-100%", 90, 100),
        ("100%+", 100, 99999),
    ]
    result = []
    for label, lo, hi in bins:
        count = int(((occ_pct >= lo) & (occ_pct < hi)).sum())
        result.append({"range": label, "count": count})
    return result


def agg_salary_kpi(df):
    """salary_kpi: 賃金KPI（月額1-5をunpivot）"""
    all_salaries = []
    for col in SALARY_AMOUNT_COLS:
        if col in df.columns:
            vals = df[col].apply(safe_float).dropna()
            # 合理的な範囲（月額1万〜200万）
            vals = vals[(vals >= 10000) & (vals <= 2000000)]
            all_salaries.extend(vals.tolist())

    if not all_salaries:
        return {"avg_salary": None, "median_salary": None, "max_salary": None,
                "min_salary": None, "data_count": 0}

    s = pd.Series(all_salaries)
    return {
        "avg_salary": round_safe(s.mean(), 0),
        "median_salary": round_safe(s.median(), 0),
        "max_salary": round_safe(s.max(), 0),
        "min_salary": round_safe(s.min(), 0),
        "data_count": len(s),
    }


def agg_salary_by_job_type(df):
    """salary_by_job_type: 職種別賃金（unpivot、ベクトル化版）"""
    frames = []
    for j in range(1, 6):
        job_col = f"賃金_職種{j}"
        amt_col = f"賃金_月額{j}"
        age_col = f"賃金_平均年齢{j}"
        ten_col = f"賃金_平均勤続{j}"
        if job_col not in df.columns or amt_col not in df.columns:
            continue
        sub = df[[job_col, amt_col, age_col, ten_col]].copy()
        sub.columns = ["job_type", "salary", "age", "tenure"]
        sub["salary"] = sub["salary"].apply(safe_float)
        sub["age"] = sub["age"].apply(safe_float)
        sub["tenure"] = sub["tenure"].apply(safe_float)
        # 有効レコードのみ
        mask = (
            sub["job_type"].notna()
            & (sub["job_type"].astype(str).str.strip() != "")
            & sub["salary"].notna()
            & (sub["salary"] >= 10000)
            & (sub["salary"] <= 2000000)
        )
        sub = sub[mask].copy()
        sub["job_type"] = sub["job_type"].astype(str).str.strip()
        frames.append(sub)

    if not frames:
        return []

    rec_df = pd.concat(frames, ignore_index=True)
    if rec_df.empty:
        return []

    grouped = rec_df.groupby("job_type").agg(
        avg_salary=("salary", "mean"),
        avg_age=("age", "mean"),
        avg_tenure=("tenure", "mean"),
        count=("salary", "size"),
    ).reset_index()

    return grouped.apply(
        lambda row: {
            "job_type": row["job_type"],
            "avg_salary": round_safe(row["avg_salary"], 0),
            "avg_age": round_safe(row["avg_age"], 1),
            "avg_tenure": round_safe(row["avg_tenure"], 1),
            "count": int(row["count"]),
        },
        axis=1,
    ).tolist()


def agg_salary_by_prefecture(df):
    """salary_by_prefecture: 都道府県別賃金（ベクトル化版）"""
    frames = []
    valid_pref = df["prefecture"].notna()
    for j in range(1, 6):
        amt_col = f"賃金_月額{j}"
        if amt_col not in df.columns:
            continue
        sub = df.loc[valid_pref, ["prefecture", amt_col]].copy()
        sub.columns = ["prefecture", "salary"]
        sub["salary"] = sub["salary"].apply(safe_float)
        mask = sub["salary"].notna() & (sub["salary"] >= 10000) & (sub["salary"] <= 2000000)
        frames.append(sub[mask])

    if not frames:
        return []

    rec_df = pd.concat(frames, ignore_index=True)
    if rec_df.empty:
        return []

    grouped = rec_df.groupby("prefecture").agg(
        avg_salary=("salary", "mean"),
        count=("salary", "size"),
    ).reset_index()

    return grouped.apply(
        lambda row: {
            "prefecture": row["prefecture"],
            "avg_salary": round_safe(row["avg_salary"], 0),
            "count": int(row["count"]),
        },
        axis=1,
    ).tolist()


def compute_quality_scores(df):
    """品質スコアを計算（4軸モデル、各25点満点、合計100点）"""
    scores = pd.Series(0.0, index=df.index)

    # Safety (25pts): BCP(12.5) + Insurance(12.5)
    if "品質_BCP策定_bool" in df.columns:
        scores += df["品質_BCP策定_bool"].astype(float) * 12.5
    if "品質_損害賠償保険_bool" in df.columns:
        scores += df["品質_損害賠償保険_bool"].astype(float) * 12.5

    # Quality (25pts): ThirdParty(12.5) + ICT(12.5)
    if "品質_第三者評価_bool" in df.columns:
        scores += df["品質_第三者評価_bool"].astype(float) * 12.5
    if "品質_ICT活用_bool" in df.columns:
        scores += df["品質_ICT活用_bool"].astype(float) * 12.5

    # HR (25pts): fulltime_ratio*12.5 + (1-turnover_rate)*12.5
    fr = df["fulltime_ratio"].copy()
    fr[(fr < 0) | (fr > 1)] = None
    scores += fr.fillna(0) * 12.5

    tr = df["turnover_rate"].copy()
    tr[(tr < 0) | (tr > 1)] = None
    scores += (1 - tr.fillna(1)) * 12.5

    # Operations (25pts): kasan_count/13*12.5 + min(years_in_business/20,1)*12.5
    if "kasan_count" in df.columns:
        scores += (df["kasan_count"].fillna(0) / 13.0) * 12.5

    yib = df["years_in_business"].copy()
    yib_norm = yib.fillna(0).clip(upper=20) / 20.0
    scores += yib_norm * 12.5

    return scores


def agg_quality_kpi(df):
    """quality_kpi: 品質KPI"""
    total = len(df)
    scores = compute_quality_scores(df)

    bcp_rate = round_safe(df["品質_BCP策定_bool"].sum() / total) if "品質_BCP策定_bool" in df.columns and total > 0 else None
    ict_rate = round_safe(df["品質_ICT活用_bool"].sum() / total) if "品質_ICT活用_bool" in df.columns and total > 0 else None
    tp_rate = round_safe(df["品質_第三者評価_bool"].sum() / total) if "品質_第三者評価_bool" in df.columns and total > 0 else None
    ins_rate = round_safe(df["品質_損害賠償保険_bool"].sum() / total) if "品質_損害賠償保険_bool" in df.columns and total > 0 else None

    exp = df["exp_10yr_ratio"].dropna() if "exp_10yr_ratio" in df.columns else pd.Series(dtype=float)

    return {
        "avg_quality_score": round_safe(scores.mean()),
        "bcp_rate": bcp_rate,
        "ict_rate": ict_rate,
        "third_party_rate": tp_rate,
        "insurance_rate": ins_rate,
        "avg_experience_10yr_ratio": round_safe(exp.mean()),
    }


def agg_quality_score_dist(df):
    """quality_score_dist: 品質スコア分布"""
    scores = compute_quality_scores(df)
    bins = [
        ("0-20", 0, 20),
        ("20-40", 20, 40),
        ("40-60", 40, 60),
        ("60-80", 60, 80),
        ("80-100", 80, 101),
    ]
    result = []
    for label, lo, hi in bins:
        count = int(((scores >= lo) & (scores < hi)).sum())
        result.append({"range": label, "count": count})
    return result


def agg_quality_rank_dist(df):
    """quality_rank_dist: 品質ランク分布"""
    scores = compute_quality_scores(df)
    ranks = [
        ("S", 80, 101),
        ("A", 60, 80),
        ("B", 40, 60),
        ("C", 20, 40),
        ("D", 0, 20),
    ]
    result = []
    for rank, lo, hi in ranks:
        count = int(((scores >= lo) & (scores < hi)).sum())
        result.append({"rank": rank, "count": count})
    return result


def agg_quality_radar(df):
    """quality_radar: 品質レーダー（4軸平均）"""
    total = len(df)

    # Safety
    safety = 0.0
    if "品質_BCP策定_bool" in df.columns:
        safety += df["品質_BCP策定_bool"].astype(float).mean() * 12.5
    if "品質_損害賠償保険_bool" in df.columns:
        safety += df["品質_損害賠償保険_bool"].astype(float).mean() * 12.5

    # Quality
    quality = 0.0
    if "品質_第三者評価_bool" in df.columns:
        quality += df["品質_第三者評価_bool"].astype(float).mean() * 12.5
    if "品質_ICT活用_bool" in df.columns:
        quality += df["品質_ICT活用_bool"].astype(float).mean() * 12.5

    # HR
    fr = df["fulltime_ratio"].copy()
    fr[(fr < 0) | (fr > 1)] = None
    tr = df["turnover_rate"].copy()
    tr[(tr < 0) | (tr > 1)] = None
    hr = fr.fillna(0).mean() * 12.5 + (1 - tr.fillna(1)).mean() * 12.5

    # Operations
    ops = 0.0
    if "kasan_count" in df.columns:
        ops += (df["kasan_count"].fillna(0) / 13.0).mean() * 12.5
    yib = df["years_in_business"].fillna(0).clip(upper=20) / 20.0
    ops += yib.mean() * 12.5

    return {
        "safety": round_safe(safety),
        "quality": round_safe(quality),
        "hr": round_safe(hr),
        "operations": round_safe(ops),
    }


def agg_quality_by_prefecture(df):
    """quality_by_prefecture: 都道府県別品質スコア"""
    scores = compute_quality_scores(df)
    df_temp = df[["prefecture"]].copy()
    df_temp["score"] = scores

    valid = df_temp[df_temp["prefecture"].notna()]
    grouped = valid.groupby("prefecture").agg(
        avg_score=("score", "mean"),
        facility_count=("score", "size"),
    ).reset_index()

    return [
        {
            "prefecture": row["prefecture"],
            "avg_score": round_safe(row["avg_score"]),
            "facility_count": int(row["facility_count"]),
        }
        for _, row in grouped.iterrows()
    ]


def agg_growth_kpi(df):
    """growth_kpi: 成長KPI"""
    yib = df["years_in_business"].dropna()
    return {
        "total_facilities": len(df),
        "new_facilities_5yr": int((yib <= 5).sum()) if len(yib) > 0 else 0,
        "avg_years": round_safe(yib.mean()),
        "oldest_years": round_safe(yib.max()),
    }


def agg_growth_trend(df):
    """growth_trend: 設立年別件数"""
    yib = df["years_in_business"].dropna()
    est_year = (datetime.now().year - yib).astype(int)
    grouped = est_year.value_counts().sort_index()

    return [
        {"year": int(year), "count": int(count)}
        for year, count in grouped.items()
    ]


def agg_growth_years_dist(df):
    """growth_years_dist: 事業年数分布"""
    yib = df["years_in_business"].dropna()
    bins = [
        ("0-5年", 0, 5),
        ("5-10年", 5, 10),
        ("10-15年", 10, 15),
        ("15-20年", 15, 20),
        ("20-30年", 20, 30),
        ("30年+", 30, 9999),
    ]
    result = []
    for label, lo, hi in bins:
        count = int(((yib >= lo) & (yib < hi)).sum())
        result.append({"range": label, "count": count})
    return result


def agg_corp_group_kpi(df):
    """corp_group_kpi: 法人グループKPI"""
    corps = df[df["法人番号"].notna()].groupby("法人番号").size()
    total_corps = len(corps)
    multi = int((corps >= 2).sum())
    avg_fac = round_safe(corps.mean())
    max_fac = int(corps.max()) if len(corps) > 0 else 0

    return {
        "total_corps": total_corps,
        "multi_facility_corps": multi,
        "avg_facilities_per_corp": avg_fac,
        "max_facilities": max_fac,
    }


def agg_corp_group_size_dist(df):
    """corp_group_size_dist: 法人施設数分布"""
    corps = df[df["法人番号"].notna()].groupby("法人番号").size()
    bins = [
        ("1施設", 1, 1),
        ("2-5施設", 2, 5),
        ("6-10施設", 6, 10),
        ("11-20施設", 11, 20),
        ("21-50施設", 21, 50),
        ("51施設+", 51, 99999),
    ]
    result = []
    for label, lo, hi in bins:
        count = int(((corps >= lo) & (corps <= hi)).sum())
        result.append({"size": label, "count": count})
    return result


def agg_corp_group_top_corps(df):
    """corp_group_top_corps: 施設数上位20法人"""
    valid = df[df["法人番号"].notna()].copy()
    tr = valid["turnover_rate"].copy()
    tr[(tr < 0) | (tr > 1)] = None
    valid["tr_clean"] = tr

    # 法人ごとに集計
    grouped = valid.groupby("法人番号").agg(
        corp_name=("法人名", "first"),
        facility_count=("法人番号", "size"),
        avg_turnover_rate=("tr_clean", "mean"),
        avg_staff=("従業者_合計", "mean"),
    ).reset_index()

    # サービス名リストを取得
    svc_grouped = valid.groupby("法人番号")["サービス名"].apply(
        lambda x: list(x.dropna().unique())
    ).reset_index(name="services")

    merged = grouped.merge(svc_grouped, on="法人番号", how="left")
    top20 = merged.nlargest(20, "facility_count")

    return [
        {
            "corp_number": row["法人番号"],
            "corp_name": row["corp_name"] or "",
            "facility_count": int(row["facility_count"]),
            "avg_turnover_rate": round_safe(row["avg_turnover_rate"]),
            "avg_staff": round_safe(row["avg_staff"]),
            "services": row["services"] if isinstance(row["services"], list) else [],
        }
        for _, row in top20.iterrows()
    ]


def agg_corp_group_kasan_heatmap(df):
    """corp_group_kasan_heatmap: 上位20法人の加算取得率ヒートマップ"""
    valid = df[df["法人番号"].notna()].copy()

    # 上位20法人を特定
    corp_counts = valid.groupby("法人番号").size()
    top20_corps = corp_counts.nlargest(20).index.tolist()
    top20_df = valid[valid["法人番号"].isin(top20_corps)]

    result = []
    for corp_num in top20_corps:
        corp_df = top20_df[top20_df["法人番号"] == corp_num]
        corp_name = corp_df["法人名"].iloc[0] if len(corp_df) > 0 else ""
        total = len(corp_df)

        entry = {
            "corp_name": corp_name or "",
            "corp_number": corp_num,
        }
        for i, col in enumerate(KASAN_COLUMNS):
            bool_col = f"{col}_bool"
            if bool_col in corp_df.columns:
                rate = round_safe(corp_df[bool_col].sum() / total) if total > 0 else 0
                entry[f"kasan_{i+1}_rate"] = rate
        result.append(entry)

    return result


def agg_meta(df):
    """meta: メタ情報"""
    prefectures = sorted(df["prefecture"].dropna().unique().tolist())

    # サービス種別
    svc = df.groupby(["サービスコード", "サービス名"]).size().reset_index(name="count")
    service_types = [
        {"code": row["サービスコード"] or "", "name": row["サービス名"] or "", "count": int(row["count"])}
        for _, row in svc.iterrows()
    ]

    # 法人種別
    ct = df.groupby("corp_type").size().reset_index(name="count") if "corp_type" in df.columns else pd.DataFrame()
    corp_types = [
        {"name": row["corp_type"] or "不明", "count": int(row["count"])}
        for _, row in ct.iterrows()
    ] if len(ct) > 0 else []

    return {
        "total_count": len(df),
        "prefectures": prefectures,
        "service_types": service_types,
        "corp_types": corp_types,
    }


# ============================================================
# キャッシュ書き込み
# ============================================================
def write_cache(cache_entries):
    """集計結果をkpi_cacheテーブルに書き込み"""
    print(f"\n[4/4] kpi_cache テーブルに書き込み中... ({len(cache_entries)}件)")

    # バッチで書き込み（50件ずつ）
    batch_size = 50
    written = 0
    errors = 0

    for i in range(0, len(cache_entries), batch_size):
        batch = cache_entries[i:i + batch_size]
        statements = []
        for key, value, row_count in batch:
            sql = (
                "INSERT OR REPLACE INTO kpi_cache (key, filter_key, value, updated_at, row_count) "
                "VALUES (?, '', ?, datetime('now'), ?)"
            )
            json_val = json.dumps(value, ensure_ascii=False, default=str)
            statements.append({
                "type": "execute",
                "stmt": {
                    "sql": sql,
                    "args": [
                        make_arg(key),
                        make_arg(json_val),
                        make_arg(row_count),
                    ],
                },
            })

        try:
            result = execute_sql(statements)
            for r in result.get("results", []):
                if "error" in r:
                    print(f"  エラー: {r['error']['message'][:200]}")
                    errors += 1
                else:
                    written += 1
        except Exception as e:
            print(f"  バッチ書き込みエラー: {e}")
            errors += len(batch)

    print(f"  書き込み完了: {written}件成功, {errors}件エラー")
    return written, errors


# ============================================================
# メイン処理
# ============================================================
def main():
    print("=" * 70)
    print("Turso DB: facilities → kpi_cache 事前集計スクリプト")
    print(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    start_time = time.time()

    # データ取得
    df = fetch_all_rows()
    if df.empty:
        print("データが0件のため終了します")
        return

    # 前処理
    df = preprocess(df)

    # 集計実行
    print("\n[3/4] 集計処理中...")
    cache_entries = []

    aggregations = {
        # Dashboard
        "dashboard_kpi": (agg_dashboard_kpi, False),
        "dashboard_by_prefecture": (agg_dashboard_by_prefecture, True),
        "dashboard_by_service": (agg_dashboard_by_service, True),

        # Market
        "market_choropleth": (agg_market_choropleth, True),
        "market_by_service": (agg_market_by_service, True),
        "market_corp_donut": (agg_market_corp_donut, True),

        # Workforce
        "workforce_kpi": (agg_workforce_kpi, False),
        "workforce_turnover_dist": (agg_workforce_turnover_dist, True),
        "workforce_by_prefecture": (agg_workforce_by_prefecture, True),
        "workforce_by_size": (agg_workforce_by_size, True),
        "workforce_exp_dist": (agg_workforce_exp_dist, True),
        "workforce_exp_turnover": (agg_workforce_exp_turnover, True),
        "workforce_staff_breakdown": (agg_workforce_staff_breakdown, True),
        "workforce_qualifications": (agg_workforce_qualifications, True),
        "workforce_night_shift": (agg_workforce_night_shift, False),
        "workforce_dementia_training": (agg_workforce_dementia_training, True),

        # Revenue
        "revenue_kpi": (agg_revenue_kpi, False),
        "revenue_kasan_rates": (agg_revenue_kasan_rates, True),
        "kasan_detail_rates": (agg_kasan_detail_rates, True),
        "kasan_all_items": (agg_kasan_all_items, True),
        "revenue_occupancy_dist": (agg_revenue_occupancy_dist, True),

        # Salary
        "salary_kpi": (agg_salary_kpi, False),
        "salary_by_job_type": (agg_salary_by_job_type, True),
        "salary_by_prefecture": (agg_salary_by_prefecture, True),

        # Quality
        "quality_kpi": (agg_quality_kpi, False),
        "quality_score_dist": (agg_quality_score_dist, True),
        "quality_rank_dist": (agg_quality_rank_dist, True),
        "quality_radar": (agg_quality_radar, False),
        "quality_by_prefecture": (agg_quality_by_prefecture, True),

        # Growth
        "growth_kpi": (agg_growth_kpi, False),
        "growth_trend": (agg_growth_trend, True),
        "growth_years_dist": (agg_growth_years_dist, True),

        # Corp Group
        "corp_group_kpi": (agg_corp_group_kpi, False),
        "corp_group_size_dist": (agg_corp_group_size_dist, True),
        "corp_group_top_corps": (agg_corp_group_top_corps, True),
        "corp_group_kasan_heatmap": (agg_corp_group_kasan_heatmap, True),

        # Meta
        "meta": (agg_meta, False),
    }

    for key, (func, is_list) in aggregations.items():
        try:
            result = func(df)
            row_count = len(result) if is_list else 1
            cache_entries.append((key, result, row_count))
            if is_list:
                print(f"  {key}: {row_count}件")
            else:
                # dict型の場合は主要値を表示
                if isinstance(result, dict):
                    preview = {k: v for k, v in list(result.items())[:3]}
                    print(f"  {key}: {preview}")
                else:
                    print(f"  {key}: OK")
        except Exception as e:
            print(f"  {key}: エラー - {e}")
            import traceback
            traceback.print_exc()

    # キャッシュ書き込み
    written, errors = write_cache(cache_entries)

    # サマリー
    elapsed = time.time() - start_time
    print(f"\n{'=' * 70}")
    print(f"集計完了サマリー")
    print(f"{'=' * 70}")
    print(f"  データ件数: {len(df):,}")
    print(f"  集計キー数: {len(cache_entries)}")
    print(f"  書き込み成功: {written}")
    print(f"  書き込みエラー: {errors}")
    print(f"  所要時間: {elapsed:.1f}秒")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
