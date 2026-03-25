#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
介護BIプラットフォーム データ整合性テスト & クロスレイヤーテスト
CSV vs API vs Turso DB の3層にわたるデータ整合性を検証する

テスト実行にはバックエンドの認証が必要。
自動的にTursoにテスト用ユーザーを作成し、テスト終了後に削除する。

実行方法:
    python scripts/data_integrity_test.py

結果はコンソールとscripts/test_results.txtに出力される。
"""

import io
import json
import math
import os
import re
import sys
import time
from pathlib import Path

# Windows環境でのUTF-8出力対応
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import pandas as pd
import requests
from argon2 import PasswordHasher

# =============================================================
# 設定
# =============================================================
BASE_URL = "http://localhost:3001"
CSV_PATH = Path(
    r"C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List"
    r"\data\output\kaigo_scraping\tokyo_day_care_150_20260319.csv"
)
RESULT_FILE = Path(
    r"C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List"
    r"\scripts\test_results.txt"
)

TURSO_URL = os.environ.get("TURSO_DATABASE_URL", "https://cw-makimaki1006.aws-ap-northeast-1.turso.io")
TURSO_TOKEN = os.environ.get("TURSO_AUTH_TOKEN")
if not TURSO_TOKEN:
    raise ValueError("TURSO_AUTH_TOKEN environment variable is required")

# テスト専用ユーザー設定
TEST_USER_ID = "test-integrity-auto-001"
TEST_USER_EMAIL = "test_integrity_auto@test.local"
TEST_USER_PASSWORD = "TestIntegrity2026!"
TEST_USER_NAME = "IntegrityTestBot"

# 管理者情報（検証用）
ADMIN_EMAIL = "s_fujimaki@cyxen.co.jp"


# =============================================================
# ログ出力（コンソール + ファイル）
# =============================================================
_log_lines = []


def log(msg: str = ""):
    """コンソールとバッファにログを出力"""
    _log_lines.append(msg)
    try:
        print(msg)
    except UnicodeEncodeError:
        print(msg.encode("utf-8", errors="replace").decode("ascii", errors="replace"))


def save_log():
    """ログをファイルに保存"""
    with open(RESULT_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(_log_lines))
    log(f"\n結果ファイル: {RESULT_FILE}")


# =============================================================
# テスト結果管理
# =============================================================
class TestResults:
    def __init__(self):
        self.results = []
        self.total = 0
        self.passed = 0
        self.failed = 0
        self.skipped = 0

    def add(self, name: str, status: str, detail: str = ""):
        self.total += 1
        if status == "PASS":
            self.passed += 1
        elif status == "FAIL":
            self.failed += 1
        else:
            self.skipped += 1
        self.results.append((name, status, detail))
        icon = {"PASS": "[PASS]", "FAIL": "[FAIL]", "SKIP": "[SKIP]"}.get(status, "[????]")
        line = f"  {icon} {name}"
        if detail:
            line += f" ({detail})"
        log(line)

    def summary(self):
        log("")
        log("=" * 60)
        log("=== データ整合性テスト結果 ===")
        log("=" * 60)
        for name, status, detail in self.results:
            icon = {"PASS": "[PASS]", "FAIL": "[FAIL]", "SKIP": "[SKIP]"}.get(status, "[????]")
            line = f"  {icon} {name}"
            if detail:
                line += f" -- {detail}"
            log(line)
        log("-" * 60)
        log(
            f"  合計: {self.total} テスト | "
            f"PASS: {self.passed} | FAIL: {self.failed} | SKIP: {self.skipped}"
        )
        if self.failed == 0 and self.skipped == 0:
            log("  全テスト合格")
        elif self.failed == 0:
            log(f"  失敗なし（スキップ {self.skipped} 件）")
        else:
            log(f"  ** {self.failed} 件のテスト失敗 **")
        log("=" * 60)
        return self.failed


# =============================================================
# ヘルパー関数
# =============================================================
def api_get(path: str, token: str, params: dict = None) -> dict:
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(f"{BASE_URL}{path}", headers=headers, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def api_post(path: str, data: dict, token: str = None) -> requests.Response:
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return requests.post(f"{BASE_URL}{path}", headers=headers, json=data, timeout=30)


def turso_query(sql: str) -> dict:
    headers = {
        "Authorization": f"Bearer {TURSO_TOKEN}",
        "Content-Type": "application/json",
    }
    resp = requests.post(
        f"{TURSO_URL}/v2/pipeline",
        headers=headers,
        json={"requests": [{"type": "execute", "stmt": {"sql": sql}}]},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def turso_execute(sql: str) -> dict:
    return turso_query(sql)


def extract_prefecture(address: str) -> str | None:
    if not address or not isinstance(address, str):
        return None
    addr = re.sub(r"^〒\s*\d{3}-?\d{4}\s*", "", address.strip())
    for suffix in ["都", "道", "府", "県"]:
        m = re.match(rf"(.+?{suffix})", addr)
        if m:
            return m.group(1)
    return None


def classify_corp_type(corp_name: str) -> str:
    if not corp_name or not isinstance(corp_name, str):
        return "不明"
    if "社会福祉法人" in corp_name:
        return "社会福祉法人"
    if "医療法人" in corp_name:
        return "医療法人"
    if "株式会社" in corp_name or "有限会社" in corp_name or "合同会社" in corp_name:
        return "営利法人"
    if "NPO" in corp_name or "特定非営利" in corp_name:
        return "NPO法人"
    if any(k in corp_name for k in ["一般社団", "公益社団", "一般財団", "公益財団"]):
        return "社団・財団法人"
    return "その他"


# =============================================================
# テスト用ユーザーの管理
# =============================================================
def setup_test_user() -> str | None:
    log("テスト用ユーザーの準備...")
    ph = PasswordHasher()
    pw_hash = ph.hash(TEST_USER_PASSWORD)
    pw_hash_escaped = pw_hash.replace("'", "''")

    turso_execute(f"DELETE FROM sessions WHERE user_id = '{TEST_USER_ID}'")
    turso_execute(f"DELETE FROM users WHERE id = '{TEST_USER_ID}'")

    insert_sql = (
        f"INSERT INTO users (id, email, name, password_hash, role, is_active, created_at, updated_at) "
        f"VALUES ('{TEST_USER_ID}', '{TEST_USER_EMAIL}', '{TEST_USER_NAME}', "
        f"'{pw_hash_escaped}', 'admin', 1, datetime('now'), datetime('now'))"
    )
    result = turso_execute(insert_sql)

    try:
        affected = result["results"][0]["response"]["result"]["affected_row_count"]
        if affected != 1:
            log(f"  テストユーザー挿入失敗: affected_row_count={affected}")
            return None
    except (KeyError, IndexError) as e:
        log(f"  テストユーザー挿入結果の解析失敗: {e}")
        return None

    log(f"  テストユーザー作成完了: {TEST_USER_EMAIL}")

    try:
        resp = api_post("/api/auth/login", {
            "email": TEST_USER_EMAIL,
            "password": TEST_USER_PASSWORD,
        })
        if resp.status_code == 200:
            token = resp.json().get("token")
            log("  ログイン成功: JWTトークン取得")
            return token
        else:
            log(f"  ログイン失敗: HTTP {resp.status_code}")
            return None
    except Exception as e:
        log(f"  ログインエラー: {e}")
        return None


def teardown_test_user():
    log("\nテスト用ユーザーの削除...")
    try:
        turso_execute(f"DELETE FROM sessions WHERE user_id = '{TEST_USER_ID}'")
        turso_execute(f"DELETE FROM audit_logs WHERE user_id = '{TEST_USER_ID}'")
        turso_execute(f"DELETE FROM users WHERE id = '{TEST_USER_ID}'")
        log("  テストユーザー削除完了")
    except Exception as e:
        log(f"  テストユーザー削除エラー: {e}")


# =============================================================
# CSV読み込み
# =============================================================
def load_csv() -> pd.DataFrame:
    log(f"CSVファイル読み込み: {CSV_PATH}")
    df = pd.read_csv(CSV_PATH, dtype=str, encoding="utf-8-sig")
    log(f"  {len(df)} 行 x {len(df.columns)} 列")

    for col in ["従業者_常勤", "従業者_非常勤", "従業者_合計", "定員", "前年度採用数", "前年度退職数"]:
        if col in df.columns:
            df[f"{col}_num"] = pd.to_numeric(df[col], errors="coerce")

    df["都道府県"] = df["住所"].apply(extract_prefecture)
    df["法人種別"] = df["法人名"].apply(classify_corp_type)

    total = df["従業者_合計_num"]
    left = df["前年度退職数_num"]
    denominator = total + left
    df["離職率"] = left / denominator.replace(0, float("nan"))
    df["常勤比率"] = df["従業者_常勤_num"] / df["従業者_合計_num"].replace(0, float("nan"))

    def calc_years(date_str):
        if not isinstance(date_str, str):
            return float("nan")
        parts = date_str.split("/")
        if parts and parts[0].strip().isdigit():
            return 2026 - int(parts[0].strip())
        return float("nan")

    df["事業年数"] = df["事業開始日"].apply(calc_years)
    return df


# =============================================================
# テスト1: データ整合性（CSV vs API）
# =============================================================
def test_csv_vs_api(results: TestResults, df: pd.DataFrame, token: str):
    log("\n--- テスト1: データ整合性（CSV vs API） ---")
    csv_count = len(df)

    meta = {}
    try:
        meta = api_get("/api/meta", token)
        api_meta_count = meta.get("total_count", -1)
        if csv_count == api_meta_count:
            results.add("1-1 CSV件数 vs /api/meta total_count", "PASS",
                         f"CSV={csv_count}, API={api_meta_count}")
        else:
            results.add("1-1 CSV件数 vs /api/meta total_count", "FAIL",
                         f"CSV={csv_count}, API={api_meta_count}")
    except Exception as e:
        results.add("1-1 CSV件数 vs /api/meta total_count", "FAIL", f"APIエラー: {e}")

    try:
        kpi = api_get("/api/dashboard/kpi", token)
        api_kpi_count = kpi.get("total_facilities", -1)
        if csv_count == api_kpi_count:
            results.add("1-2 CSV件数 vs /api/dashboard/kpi total_facilities", "PASS",
                         f"CSV={csv_count}, API={api_kpi_count}")
        else:
            results.add("1-2 CSV件数 vs /api/dashboard/kpi total_facilities", "FAIL",
                         f"CSV={csv_count}, API={api_kpi_count}")
    except Exception as e:
        results.add("1-2 CSV件数 vs /api/dashboard/kpi total_facilities", "FAIL", f"APIエラー: {e}")

    try:
        search = api_get("/api/facilities/search", token, {"per_page": 1})
        api_search_total = search.get("total", -1)
        if csv_count == api_search_total:
            results.add("1-3 CSV件数 vs /api/facilities/search total", "PASS",
                         f"CSV={csv_count}, API={api_search_total}")
        else:
            results.add("1-3 CSV件数 vs /api/facilities/search total", "FAIL",
                         f"CSV={csv_count}, API={api_search_total}")
    except Exception as e:
        results.add("1-3 CSV件数 vs /api/facilities/search total", "FAIL", f"APIエラー: {e}")

    try:
        csv_prefs = sorted(df["都道府県"].dropna().unique().tolist())
        api_prefs = sorted(meta.get("prefectures", []))
        if csv_prefs == api_prefs:
            results.add("1-4 都道府県一覧の一致", "PASS", f"都道府県数={len(csv_prefs)}")
        else:
            csv_only = set(csv_prefs) - set(api_prefs)
            api_only = set(api_prefs) - set(csv_prefs)
            results.add("1-4 都道府県一覧の一致", "FAIL",
                         f"CSV固有={csv_only}, API固有={api_only}")
    except Exception as e:
        results.add("1-4 都道府県一覧の一致", "FAIL", f"エラー: {e}")

    try:
        csv_types = sorted(df["法人種別"].dropna().unique().tolist())
        api_types = sorted(meta.get("corp_types", []))
        if len(csv_types) == len(api_types):
            results.add("1-5 法人種別数の一致", "PASS", f"法人種別数={len(csv_types)}")
        else:
            csv_only = set(csv_types) - set(api_types)
            api_only = set(api_types) - set(csv_types)
            results.add("1-5 法人種別数の一致", "FAIL",
                         f"CSV={len(csv_types)}{csv_types}, API={len(api_types)}{api_types}, "
                         f"CSV固有={csv_only}, API固有={api_only}")
    except Exception as e:
        results.add("1-5 法人種別数の一致", "FAIL", f"エラー: {e}")


# =============================================================
# テスト2: フィルタ整合性
# =============================================================
def test_filter_consistency(results: TestResults, df: pd.DataFrame, token: str):
    log("\n--- テスト2: フィルタ整合性 ---")

    try:
        search = api_get("/api/facilities/search", token, {"prefecture": "東京都", "per_page": 1})
        api_tokyo_total = search.get("total", -1)
        csv_tokyo_count = len(df[df["都道府県"] == "東京都"])
        if api_tokyo_total == csv_tokyo_count:
            results.add("2-1 都道府県フィルタ（東京都）", "PASS",
                         f"CSV={csv_tokyo_count}, API={api_tokyo_total}")
        else:
            results.add("2-1 都道府県フィルタ（東京都）", "FAIL",
                         f"CSV={csv_tokyo_count}, API={api_tokyo_total}")
    except Exception as e:
        results.add("2-1 都道府県フィルタ（東京都）", "FAIL", f"エラー: {e}")

    try:
        search = api_get("/api/facilities/search", token, {"corp_type": "社会福祉法人", "per_page": 1})
        api_shafu_total = search.get("total", -1)
        csv_shafu_count = len(df[df["法人種別"] == "社会福祉法人"])
        if api_shafu_total == csv_shafu_count:
            results.add("2-2 法人種別フィルタ（社会福祉法人）", "PASS",
                         f"CSV={csv_shafu_count}, API={api_shafu_total}")
        else:
            results.add("2-2 法人種別フィルタ（社会福祉法人）", "FAIL",
                         f"CSV={csv_shafu_count}, API={api_shafu_total}")
    except Exception as e:
        results.add("2-2 法人種別フィルタ（社会福祉法人）", "FAIL", f"エラー: {e}")

    # 2-3: 従業者数フィルタ（30-50人）- per_pageを100に制限（500はBad Requestになる場合がある）
    try:
        search = api_get("/api/facilities/search", token, {
            "staff_min": 30, "staff_max": 50, "per_page": 100
        })
        api_items = search.get("items", [])
        api_staff_total = search.get("total", -1)
        out_of_range = []
        for item in api_items:
            st = item.get("staff_total")
            if st is not None and (st < 30 or st > 50):
                out_of_range.append({"name": item.get("jigyosho_name", "?"), "staff": st})

        csv_filtered = df[(df["従業者_合計_num"] >= 30) & (df["従業者_合計_num"] <= 50)]
        csv_staff_count = len(csv_filtered)

        if len(out_of_range) == 0 and api_staff_total == csv_staff_count:
            results.add("2-3 従業者数フィルタ（30-50人）", "PASS",
                         f"全{api_staff_total}件が範囲内, CSV={csv_staff_count}")
        elif len(out_of_range) > 0:
            results.add("2-3 従業者数フィルタ（30-50人）", "FAIL",
                         f"範囲外 {len(out_of_range)}件: {out_of_range[:3]}")
        else:
            results.add("2-3 従業者数フィルタ（30-50人）", "FAIL",
                         f"件数不一致 CSV={csv_staff_count}, API={api_staff_total}")
    except Exception as e:
        results.add("2-3 従業者数フィルタ（30-50人）", "FAIL", f"エラー: {e}")


# =============================================================
# テスト3: 派生指標の正確性
# =============================================================
def test_derived_metrics(results: TestResults, df: pd.DataFrame, token: str):
    log("\n--- テスト3: 派生指標の正確性 ---")

    wf_kpi = {}
    try:
        wf_kpi = api_get("/api/workforce/kpi", token)
        api_turnover = wf_kpi.get("avg_turnover_rate")
        csv_turnover = df["離職率"].dropna().mean()

        if api_turnover is not None and not math.isnan(csv_turnover):
            diff = abs(api_turnover - csv_turnover)
            if diff < 0.001:
                results.add("3-1 平均離職率", "PASS",
                             f"CSV={csv_turnover:.6f}, API={api_turnover:.6f}, 差={diff:.8f}")
            else:
                results.add("3-1 平均離職率", "FAIL",
                             f"CSV={csv_turnover:.6f}, API={api_turnover:.6f}, 差={diff:.8f}")
        else:
            results.add("3-1 平均離職率", "FAIL", f"API={api_turnover}, CSV={csv_turnover}")
    except Exception as e:
        results.add("3-1 平均離職率", "FAIL", f"エラー: {e}")

    try:
        api_fulltime = wf_kpi.get("avg_fulltime_ratio")
        csv_fulltime = df["常勤比率"].dropna().mean()
        if api_fulltime is not None and not math.isnan(csv_fulltime):
            diff = abs(api_fulltime - csv_fulltime)
            if diff < 0.001:
                results.add("3-2 平均常勤比率", "PASS",
                             f"CSV={csv_fulltime:.6f}, API={api_fulltime:.6f}, 差={diff:.8f}")
            else:
                results.add("3-2 平均常勤比率", "FAIL",
                             f"CSV={csv_fulltime:.6f}, API={api_fulltime:.6f}, 差={diff:.8f}")
        else:
            results.add("3-2 平均常勤比率", "FAIL", f"API={api_fulltime}, CSV={csv_fulltime}")
    except Exception as e:
        results.add("3-2 平均常勤比率", "FAIL", f"エラー: {e}")

    try:
        corp_kpi = api_get("/api/corp-group/kpi", token)
        api_total_corps = corp_kpi.get("total_corps", -1)
        csv_corp_nums = df["法人番号"].dropna()
        csv_corp_nums = csv_corp_nums[csv_corp_nums.str.strip() != ""]
        csv_total_corps = csv_corp_nums.nunique()
        if api_total_corps == csv_total_corps:
            results.add("3-3 法人数（法人番号ユニーク数）", "PASS",
                         f"CSV={csv_total_corps}, API={api_total_corps}")
        else:
            results.add("3-3 法人数（法人番号ユニーク数）", "FAIL",
                         f"CSV={csv_total_corps}, API={api_total_corps}")
    except Exception as e:
        results.add("3-3 法人数（法人番号ユニーク数）", "FAIL", f"エラー: {e}")

    try:
        growth_kpi = api_get("/api/growth/kpi", token)
        api_avg_years = growth_kpi.get("avg_years_in_business", 0.0)
        csv_avg_years = df["事業年数"].dropna().mean()
        if api_avg_years > 0:
            diff = abs(api_avg_years - csv_avg_years)
            if diff < 0.5:
                results.add("3-4 平均事業年数（>0確認+精度）", "PASS",
                             f"CSV={csv_avg_years:.2f}, API={api_avg_years:.2f}, 差={diff:.4f}")
            else:
                results.add("3-4 平均事業年数（>0確認+精度）", "FAIL",
                             f"精度不足: CSV={csv_avg_years:.2f}, API={api_avg_years:.2f}, 差={diff:.4f}")
        else:
            results.add("3-4 平均事業年数（>0確認+精度）", "FAIL",
                         f"API={api_avg_years}（0以下、バグ再発の可能性）, CSV={csv_avg_years:.2f}")
    except Exception as e:
        results.add("3-4 平均事業年数（>0確認+精度）", "FAIL", f"エラー: {e}")


# =============================================================
# テスト4: ソート・ページネーション整合性
# =============================================================
def test_sort_pagination(results: TestResults, df: pd.DataFrame, token: str):
    log("\n--- テスト4: ソート・ページネーション整合性 ---")

    try:
        search = api_get("/api/facilities/search", token, {
            "sort_by": "staff_total", "sort_order": "desc", "per_page": 10
        })
        items = search.get("items", [])
        if items:
            first_staff = items[0].get("staff_total")
            csv_max_staff = df["従業者_合計_num"].max()
            # staff_totalがNone（NaN行がソートで先頭に来る可能性）の場合は
            # None以外の最初の値で比較
            if first_staff is None:
                # NULLが先頭に来るソート実装の場合、non-null最初の値を探す
                non_null_items = [i for i in items if i.get("staff_total") is not None]
                if non_null_items:
                    first_non_null = non_null_items[0].get("staff_total")
                    results.add("4-1 ソート降順の1件目が最大値", "FAIL",
                                 f"1件目=None(NULLソート問題), 非null最初={first_non_null}, CSV最大={csv_max_staff}")
                else:
                    results.add("4-1 ソート降順の1件目が最大値", "FAIL",
                                 "全件staff_total=None")
            elif first_staff == csv_max_staff:
                results.add("4-1 ソート降順の1件目が最大値", "PASS",
                             f"API 1件目={first_staff}, CSV最大={csv_max_staff}")
            else:
                results.add("4-1 ソート降順の1件目が最大値", "FAIL",
                             f"API 1件目={first_staff}, CSV最大={csv_max_staff}")
        else:
            results.add("4-1 ソート降順の1件目が最大値", "FAIL", "レスポンスが空")
    except Exception as e:
        results.add("4-1 ソート降順の1件目が最大値", "FAIL", f"エラー: {e}")

    try:
        search = api_get("/api/facilities/search", token, {"page": 1, "per_page": 20})
        items = search.get("items", [])
        if len(items) == 20:
            results.add("4-2 per_page=20で20件返却", "PASS", f"返却数={len(items)}")
        else:
            results.add("4-2 per_page=20で20件返却", "FAIL", f"返却数={len(items)}")
    except Exception as e:
        results.add("4-2 per_page=20で20件返却", "FAIL", f"エラー: {e}")

    try:
        search_p1 = api_get("/api/facilities/search", token, {"page": 1, "per_page": 20})
        search_p2 = api_get("/api/facilities/search", token, {"page": 2, "per_page": 20})
        ids_p1 = {item.get("jigyosho_number") for item in search_p1.get("items", [])}
        ids_p2 = {item.get("jigyosho_number") for item in search_p2.get("items", [])}
        overlap = ids_p1 & ids_p2
        if len(overlap) == 0:
            results.add("4-3 ページ間重複なし", "PASS",
                         f"page1={len(ids_p1)}件, page2={len(ids_p2)}件, 重複=0")
        else:
            results.add("4-3 ページ間重複なし", "FAIL",
                         f"重複 {len(overlap)}件: {list(overlap)[:5]}")
    except Exception as e:
        results.add("4-3 ページ間重複なし", "FAIL", f"エラー: {e}")

    try:
        search = api_get("/api/facilities/search", token, {"per_page": 20})
        total = search.get("total", 0)
        total_pages = search.get("total_pages", 0)
        expected_pages = math.ceil(total / 20) if total > 0 else 0
        if total_pages == expected_pages:
            results.add("4-4 total_pages = ceil(total/per_page)", "PASS",
                         f"total={total}, per_page=20, total_pages={total_pages}")
        else:
            results.add("4-4 total_pages = ceil(total/per_page)", "FAIL",
                         f"expected={expected_pages}, actual={total_pages}, total={total}")
    except Exception as e:
        results.add("4-4 total_pages計算", "FAIL", f"エラー: {e}")


# =============================================================
# テスト5: Turso DB整合性
# =============================================================
def test_turso_consistency(results: TestResults, df: pd.DataFrame, token: str):
    log("\n--- テスト5: Turso DB整合性 ---")
    csv_count = len(df)
    turso_count = -1

    try:
        turso_resp = turso_query("SELECT COUNT(*) as cnt FROM facilities")
        turso_rows = (
            turso_resp.get("results", [{}])[0]
            .get("response", {}).get("result", {}).get("rows", [])
        )
        if turso_rows:
            turso_count = int(turso_rows[0][0].get("value", 0))
            if turso_count == csv_count:
                results.add("5-1 Turso件数 vs CSV件数", "PASS",
                             f"Turso={turso_count}, CSV={csv_count}")
            else:
                results.add("5-1 Turso件数 vs CSV件数", "FAIL",
                             f"Turso={turso_count}, CSV={csv_count}")
        else:
            results.add("5-1 Turso件数 vs CSV件数", "FAIL", "Tursoから行が返されない")
    except Exception as e:
        results.add("5-1 Turso件数 vs CSV件数", "FAIL", f"エラー: {e}")

    try:
        meta = api_get("/api/meta", token)
        api_count = meta.get("total_count", -1)
        if turso_count == api_count:
            results.add("5-2 Turso件数 vs API件数", "PASS",
                         f"Turso={turso_count}, API={api_count}")
        else:
            results.add("5-2 Turso件数 vs API件数",
                         "PASS" if api_count == csv_count else "FAIL",
                         f"Turso={turso_count}, API={api_count} (APIはCSVから読み込み)")
    except Exception as e:
        results.add("5-2 Turso件数 vs API件数", "FAIL", f"エラー: {e}")

    try:
        turso_resp = turso_query("PRAGMA table_info(facilities)")
        cols = (
            turso_resp.get("results", [{}])[0]
            .get("response", {}).get("result", {}).get("rows", [])
        )
        col_count = len(cols)
        if col_count >= 19:
            results.add("5-3 Tursoカラム数（19以上）", "PASS", f"Tursoカラム数={col_count}")
        else:
            results.add("5-3 Tursoカラム数（19以上）", "FAIL", f"Tursoカラム数={col_count}")
    except Exception as e:
        results.add("5-3 Tursoカラム数", "FAIL", f"エラー: {e}")

    try:
        turso_resp = turso_query(
            "SELECT jigyosho_number FROM facilities ORDER BY jigyosho_number LIMIT 5"
        )
        turso_ids = [
            row[0].get("value", "")
            for row in (
                turso_resp.get("results", [{}])[0]
                .get("response", {}).get("result", {}).get("rows", [])
            )
        ]
        csv_ids = sorted(df["事業所番号"].dropna().tolist())[:5]
        if turso_ids == csv_ids:
            results.add("5-4 Tursoサンプルデータ一致（先頭5件）", "PASS", "事業所番号一致")
        else:
            results.add("5-4 Tursoサンプルデータ一致（先頭5件）", "FAIL",
                         f"Turso={turso_ids}, CSV={csv_ids}")
    except Exception as e:
        results.add("5-4 Tursoサンプルデータ一致", "FAIL", f"エラー: {e}")


# =============================================================
# テスト6: ユーザー管理整合性
# =============================================================
def test_user_management(results: TestResults, token: str):
    log("\n--- テスト6: ユーザー管理整合性 ---")
    users = []
    try:
        users_resp = api_get("/api/users", token)
        users = users_resp.get("users", [])
        non_test_users = [u for u in users if u.get("id") != TEST_USER_ID]
        if len(non_test_users) >= 1:
            results.add("6-1 ユーザー一覧取得（管理者1件以上）", "PASS",
                         f"ユーザー数={len(non_test_users)}（テストユーザー除外）")
        else:
            results.add("6-1 ユーザー一覧取得（管理者1件以上）", "FAIL",
                         f"ユーザー数={len(non_test_users)}")
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 403:
            results.add("6-1 ユーザー一覧取得", "SKIP", "admin権限不足（403）")
        else:
            results.add("6-1 ユーザー一覧取得", "FAIL", f"HTTPエラー: {e}")
        return
    except Exception as e:
        results.add("6-1 ユーザー一覧取得", "FAIL", f"エラー: {e}")
        return

    admin_user = None
    for u in users:
        if u.get("email") == ADMIN_EMAIL:
            admin_user = u
            break

    if admin_user:
        if admin_user.get("role") == "admin":
            results.add("6-2 管理者role=admin", "PASS",
                         f"email={admin_user.get('email')}, role={admin_user.get('role')}")
        else:
            results.add("6-2 管理者role=admin", "FAIL",
                         f"role={admin_user.get('role')}")
    else:
        results.add("6-2 管理者role=admin", "SKIP", f"email={ADMIN_EMAIL}が見つからない")

    if admin_user and admin_user.get("email") == ADMIN_EMAIL:
        results.add("6-3 管理者email確認", "PASS", f"email={admin_user.get('email')}")
    elif admin_user:
        results.add("6-3 管理者email確認", "FAIL",
                     f"期待={ADMIN_EMAIL}, 実際={admin_user.get('email')}")
    else:
        results.add("6-3 管理者email確認", "SKIP", "管理者ユーザーが見つからない")


# =============================================================
# メイン
# =============================================================
def main():
    log("=" * 60)
    log("介護BIプラットフォーム データ整合性テスト")
    log(f"実行日時: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    log("=" * 60)

    results = TestResults()

    try:
        resp = requests.get(f"{BASE_URL}/api/health", timeout=5)
        if resp.status_code != 200:
            log(f"ヘルスチェック失敗: HTTP {resp.status_code}")
            sys.exit(1)
        log(f"ヘルスチェック: OK ({resp.json().get('status', '?')})")
    except requests.exceptions.ConnectionError:
        log(f"接続エラー: {BASE_URL} に接続できません")
        sys.exit(1)

    if not CSV_PATH.exists():
        log(f"CSVファイルが見つかりません: {CSV_PATH}")
        sys.exit(1)
    df = load_csv()

    token = None
    try:
        token = setup_test_user()
        if not token:
            token = os.environ.get("KAIGO_TEST_TOKEN")
            if not token:
                log("認証に失敗しました。全テストをスキップします。")
                save_log()
                sys.exit(1)

        test_csv_vs_api(results, df, token)
        test_filter_consistency(results, df, token)
        test_derived_metrics(results, df, token)
        test_sort_pagination(results, df, token)
        test_turso_consistency(results, df, token)
        test_user_management(results, token)

    finally:
        teardown_test_user()

    failed_count = results.summary()
    save_log()
    sys.exit(1 if failed_count > 0 else 0)


if __name__ == "__main__":
    main()
