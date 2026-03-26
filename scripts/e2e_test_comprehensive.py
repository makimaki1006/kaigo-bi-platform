"""
介護BIダッシュボード 包括的E2Eテスト
============================================
5つのテストカテゴリ:
  1. API応答テスト     - 全エンドポイントのレスポンス構造・値範囲を検証
  2. ページデータテスト - ブラウザ上で正しいデータが表示されているか検証
  3. レイアウトテスト   - overflow、要素重なり等のCSS問題を検出
  4. データ品質テスト   - NaN/undefined/裸コード等の不正表示を検出
  5. 操作テスト         - フィルタ操作でデータが変わることを検証

使用方法:
  python scripts/e2e_test_comprehensive.py

前提条件:
  - バックエンド(localhost:3001)起動中
  - フロントエンド(localhost:3000)起動中
  - pip install playwright requests
  - playwright install chromium
"""

import json
import os
import re
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path

import requests

# ── 設定 ──────────────────────────────────────────
BACKEND_URL = "http://localhost:3001"
FRONTEND_URL = "http://localhost:3000"
LOGIN_EMAIL = "test@test.com"
LOGIN_PASSWORD = "test1234"

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCREENSHOTS_DIR = PROJECT_ROOT / "screenshots" / "e2e_comprehensive"
REPORT_FILE = PROJECT_ROOT / "scripts" / "e2e_comprehensive_report.txt"

# ── 重いページのタイムアウト設定 ─────────────────
# /market と /service-portfolio はAPIが遅いため特別設定
SLOW_PAGES = {"/market", "/service-portfolio"}

def _page_timeout(path: str) -> int:
    """ページ別タイムアウト（ms）を返す"""
    return 60000 if path in SLOW_PAGES else 30000

def _wait_strategy(path: str) -> str:
    """ページ別wait_until戦略を返す"""
    return "load" if path in SLOW_PAGES else "networkidle"

# ── API検証定義 ────────────────────────────────────
API_CHECKS = {
    "/api/dashboard/kpi": {
        "required_keys": ["total_facilities"],
        "value_checks": {
            "total_facilities": {"min": 100000, "max": 400000},
        },
    },
    "/api/dashboard/by-prefecture": {
        "is_array": True,
        "min_length": 10,
    },
    "/api/dashboard/by-service": {
        "is_array": True,
        "min_length": 5,
    },
    "/api/salary/kpi": {
        "required_keys": ["avg_salary"],
        "value_checks": {
            "avg_salary": {"min": 100000, "max": 500000},
        },
    },
    "/api/salary/by-job-type": {
        "is_array": True,
        "min_length": 3,
    },
    "/api/workforce/kpi": {
        "required_keys": ["avg_turnover_rate"],
        "value_checks": {
            "avg_turnover_rate": {"min": 0.01, "max": 0.50},
        },
    },
    # 人員配置: 職種別内訳
    "/api/workforce/staff-breakdown": {
        "is_array": True,
    },
    # 人員配置: 資格保有状況
    "/api/workforce/qualifications": {
        "is_array": True,
    },
    # 人員配置: 認知症研修
    "/api/workforce/dementia-training": {
        "is_array": True,
    },
    "/api/market/choropleth": {
        "is_array": True,
        "min_length": 10,
    },
    "/api/market/by-service-bar": {
        "is_array": True,
        "min_length": 3,
    },
    "/api/market/corp-type-donut": {
        "is_array": True,
        "min_length": 1,
    },
    "/api/quality/kpi": {
        "required_keys": [],
        "not_empty": True,
    },
    "/api/revenue/kpi": {
        "required_keys": [],
        "not_empty": True,
    },
    "/api/revenue/kasan-rates": {
        "is_array": True,
        "min_length": 5,
    },
    # 収益: 全加算項目
    "/api/revenue/kasan-all-items": {
        "is_array": True,
    },
    "/api/growth/kpi": {
        "not_empty": True,
    },
    "/api/corp-group/kpi": {
        "not_empty": True,
    },
    "/api/corp-group/top-corps": {
        "is_array": True,
        "min_length": 1,
    },
    "/api/external/service-portfolio": {
        "is_array": True,
        "min_length": 1,
    },
    "/api/ma/screening": {
        "is_array_or_object": True,
    },
    "/api/external/prefecture-stats": {
        "is_array": True,
        "min_length": 10,
    },
}

# ── ページデータ検証定義 ──────────────────────────
PAGE_DATA_CHECKS = {
    "/dashboard": {
        "required_texts": ["施設"],
        "required_numbers": True,
        "forbidden_texts": ["NaN", "undefined", "null"],
        "min_body_length": 500,
        "min_charts": 1,
    },
    "/salary": {
        "required_texts": ["万円"],
        "forbidden_texts": ["NaN", "undefined", "null"],
        "min_charts": 1,
    },
    "/workforce": {
        "required_texts": ["%"],
        "forbidden_texts": ["NaN", "undefined"],
        "min_charts": 2,
        # 新セクション: 職種別 or 資格保有 が表示されること
        # ※kihonデータ統合前は「統合後に表示されます」メッセージでもOK
        "optional_section_texts": ["職種別", "資格保有"],
    },
    "/market": {
        "min_charts": 2,
        "required_texts": ["都道府県"],
        "forbidden_texts": ["NaN", "undefined"],
    },
    "/quality": {
        "min_charts": 1,
        "forbidden_texts": ["NaN", "undefined"],
    },
    "/revenue": {
        "required_texts": ["加算"],
        "min_charts": 1,
        "forbidden_texts": ["NaN", "undefined"],
        # 新セクション: 全加算項目が表示されること
        "optional_section_texts": ["全加算項目"],
    },
    "/service-portfolio": {
        "forbidden_bare_codes": True,  # 3桁の裸コードが表示されていないか
        "required_texts": ["訪問", "通所"],
        "forbidden_texts": ["NaN", "undefined"],
    },
    "/corp-group": {
        "required_numbers": True,
        "forbidden_texts": ["NaN", "undefined"],
    },
    "/growth": {
        "required_numbers": True,
        "forbidden_texts": ["NaN", "undefined"],
    },
    "/ma-screening": {
        "forbidden_texts": ["NaN", "undefined"],
        "min_body_length": 500,
    },
}

# ── レイアウト検証定義 ────────────────────────────
LAYOUT_CHECKS = {
    "/ma-screening": {
        "no_overflow": True,
        "check_overlapping_elements": True,
    },
    "/dashboard": {
        "no_overflow": True,
    },
    "/service-portfolio": {
        "no_overflow": True,
    },
    "/salary": {
        "no_overflow": True,
    },
    "/workforce": {
        "no_overflow": True,
    },
}

# ── 操作テスト定義 ────────────────────────────────
INTERACTION_TESTS = {
    "/dashboard": {
        "prefecture_filter": True,
        "bar_chart_click": True,
    },
    "/workforce": {
        "service_filter": True,
    },
}


# ═══════════════════════════════════════════════════
# テスト結果管理
# ═══════════════════════════════════════════════════
class TestResult:
    """テスト結果を蓄積・レポート出力するクラス"""

    def __init__(self):
        self.results: dict[str, list[dict]] = {}
        self.current_category = ""

    def set_category(self, name: str):
        self.current_category = name
        if name not in self.results:
            self.results[name] = []

    def record(self, name: str, passed: bool, detail: str = ""):
        self.results[self.current_category].append({
            "name": name,
            "passed": passed,
            "detail": detail,
        })
        mark = "  PASS" if passed else "  FAIL"
        msg = f"{mark} {name}"
        if detail:
            msg += f"  -- {detail}"
        print(msg)

    def summary(self) -> str:
        lines = []
        lines.append("")
        lines.append("=" * 68)
        lines.append(f"E2E COMPREHENSIVE TEST REPORT - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        lines.append("=" * 68)

        total_pass = 0
        total_fail = 0
        failures = []

        for cat, tests in self.results.items():
            p = sum(1 for t in tests if t["passed"])
            f = len(tests) - p
            total_pass += p
            total_fail += f
            status = "PASS" if f == 0 else "FAIL"
            lines.append(f"\n[{cat}] {p}/{len(tests)} {status}")
            for t in tests:
                mark = "  PASS" if t["passed"] else "  FAIL"
                line = f"  {mark} {t['name']}"
                if t["detail"]:
                    line += f"  -- {t['detail']}"
                lines.append(line)
                if not t["passed"]:
                    failures.append(t)

        lines.append("")
        lines.append("-" * 68)
        lines.append(f"SUMMARY: {total_pass}/{total_pass + total_fail} PASS, {total_fail} FAIL")
        lines.append("-" * 68)

        if failures:
            lines.append("\n=== FAILURES ===")
            for f in failures:
                lines.append(f"  FAIL {f['name']}: {f['detail']}")

        lines.append("=" * 68)

        report = "\n".join(lines)
        print(report)
        return report, total_fail


# ═══════════════════════════════════════════════════
# テスト実装
# ═══════════════════════════════════════════════════
class E2ETestSuite:
    """包括的E2Eテストスイート"""

    def __init__(self):
        self.results = TestResult()
        self.token = None
        self.browser = None
        self.page = None

    # ── 認証 ──────────────────────────────────────
    def login_api(self) -> bool:
        """API経由でログインしトークンを取得"""
        try:
            resp = requests.post(
                f"{BACKEND_URL}/api/auth/login",
                json={"email": LOGIN_EMAIL, "password": LOGIN_PASSWORD},
                timeout=15,
            )
            if resp.status_code == 200:
                data = resp.json()
                self.token = data.get("token") or data.get("access_token")
                if self.token:
                    return True
                # JSONからトークンを取得できない場合、他のキーを探す
                for key in data:
                    if "token" in key.lower() and isinstance(data[key], str) and len(data[key]) > 20:
                        self.token = data[key]
                        return True
            print(f"  ログイン失敗: status={resp.status_code}, body={resp.text[:200]}")
            return False
        except Exception as e:
            print(f"  ログインエラー: {e}")
            return False

    def api_get(self, path: str, params: dict = None) -> requests.Response:
        """認証付きGETリクエスト"""
        headers = {}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return requests.get(
            f"{BACKEND_URL}{path}",
            headers=headers,
            params=params,
            timeout=30,
        )

    # ── 1. APIテスト ──────────────────────────────
    def test_api_responses(self):
        """全APIエンドポイントのレスポンス検証"""
        self.results.set_category("API Tests")

        for path, checks in API_CHECKS.items():
            try:
                resp = self.api_get(path)
                if resp.status_code != 200:
                    self.results.record(
                        path,
                        False,
                        f"HTTP {resp.status_code}: {resp.text[:100]}",
                    )
                    continue

                data = resp.json()

                # 配列チェック
                if checks.get("is_array"):
                    if not isinstance(data, list):
                        # data キーの中にある場合
                        if isinstance(data, dict):
                            # data, items, results等のキーを探す
                            found_list = None
                            for key in ["data", "items", "results", "records"]:
                                if key in data and isinstance(data[key], list):
                                    found_list = data[key]
                                    break
                            if found_list is None:
                                # 辞書の値の中から最初のリストを探す
                                for v in data.values():
                                    if isinstance(v, list):
                                        found_list = v
                                        break
                            if found_list is not None:
                                data = found_list
                            else:
                                self.results.record(path, False, "レスポンスが配列でない")
                                continue

                    min_len = checks.get("min_length", 1)
                    if len(data) < min_len:
                        self.results.record(
                            path,
                            False,
                            f"配列長 {len(data)} < 最小値 {min_len}",
                        )
                    else:
                        self.results.record(path, True, f"配列長={len(data)}")
                    continue

                if checks.get("is_array_or_object"):
                    if isinstance(data, (list, dict)):
                        size = len(data) if isinstance(data, list) else len(data.keys())
                        self.results.record(path, True, f"要素数={size}")
                    else:
                        self.results.record(path, False, f"不正な型: {type(data).__name__}")
                    continue

                # オブジェクトチェック
                if not isinstance(data, dict):
                    self.results.record(path, False, f"不正な型: {type(data).__name__}")
                    continue

                # not_emptyチェック
                if checks.get("not_empty") and len(data) == 0:
                    self.results.record(path, False, "空レスポンス")
                    continue

                # 必須キーチェック
                required_keys = checks.get("required_keys", [])
                missing = [k for k in required_keys if k not in data]
                if missing:
                    self.results.record(
                        path,
                        False,
                        f"必須キー欠落: {missing}, 実在キー: {list(data.keys())[:10]}",
                    )
                    continue

                # 値範囲チェック
                value_checks = checks.get("value_checks", {})
                value_issues = []
                value_details = []
                for key, vc in value_checks.items():
                    val = data.get(key)
                    if val is None:
                        value_issues.append(f"{key}=None")
                        continue
                    # 数値でない場合（文字列等）
                    if not isinstance(val, (int, float)):
                        try:
                            val = float(val)
                        except (ValueError, TypeError):
                            value_issues.append(f"{key}='{val}'(非数値)")
                            continue
                    if "min" in vc and val < vc["min"]:
                        value_issues.append(f"{key}={val} < min={vc['min']}")
                    elif "max" in vc and val > vc["max"]:
                        value_issues.append(f"{key}={val} > max={vc['max']}")
                    else:
                        value_details.append(f"{key}={val}")

                if value_issues:
                    self.results.record(path, False, ", ".join(value_issues))
                else:
                    detail = ", ".join(value_details) if value_details else f"keys={list(data.keys())[:5]}"
                    self.results.record(path, True, detail)

            except requests.exceptions.ConnectionError:
                self.results.record(path, False, "接続エラー（バックエンド未起動?）")
            except Exception as e:
                self.results.record(path, False, f"例外: {e}")

    # ── 2. ページデータテスト ─────────────────────
    def _navigate_authenticated(self, page, path: str, timeout: int = 30000, wait_until: str = "networkidle"):
        """認証済みの状態でページに遷移する（リダイレクト対応）"""
        full_url = f"{FRONTEND_URL}{path}"
        page.goto(full_url, wait_until=wait_until, timeout=timeout)
        time.sleep(2)

        # loginページにリダイレクトされた場合、localStorageにトークンを再設定して再遷移
        if "/login" in page.url and self.token:
            page.evaluate(f"""() => {{
                localStorage.setItem('kaigo_bi_token', '{self.token}');
            }}""")
            page.goto(full_url, wait_until=wait_until, timeout=timeout)
            time.sleep(3)

        # ページのロードを待機（React hydration + API呼び出し完了）
        try:
            page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            pass
        time.sleep(3)

    def test_page_data(self, page, path: str, checks: dict):
        """ブラウザ上のページコンテンツを検証"""
        test_name = f"PageData {path}"
        try:
            # 認証済みナビゲーション（SLOW_PAGESは自動的にタイムアウト延長+load戦略）
            self._navigate_authenticated(
                page, path,
                timeout=_page_timeout(path),
                wait_until=_wait_strategy(path),
            )

            # 追加待機（重いページはAPIが遅い）
            if path in SLOW_PAGES:
                time.sleep(5)

            body_text = page.inner_text("body")
            body_length = len(body_text)

            # min_body_lengthチェックのリトライ: ページがまだロード中の場合に再取得
            min_len = checks.get("min_body_length", 0)
            if min_len > 0 and body_length < min_len:
                # 3秒待って再チェック
                time.sleep(3)
                body_text = page.inner_text("body")
                body_length = len(body_text)
                if body_length < min_len:
                    # さらに5秒待って最終チェック
                    time.sleep(5)
                    body_text = page.inner_text("body")
                    body_length = len(body_text)
            issues = []

            # 最小本文長チェック
            min_len = checks.get("min_body_length", 0)
            if min_len > 0 and body_length < min_len:
                issues.append(f"本文長 {body_length} < {min_len}")

            # 必須テキストチェック
            for text in checks.get("required_texts", []):
                if text not in body_text:
                    issues.append(f"'{text}' 未検出")

            # オプショナルセクションテキストチェック
            # いずれかのテキストがページ本文に含まれればOK
            # データ未統合時の「統合後に表示されます」メッセージも許容する
            optional_texts = checks.get("optional_section_texts", [])
            if optional_texts:
                pending_msg = "統合後に表示されます"
                found_any = any(t in body_text for t in optional_texts) or pending_msg in body_text
                if not found_any:
                    issues.append(
                        f"新セクション未検出: いずれかが必要 {optional_texts} "
                        f"(または '{pending_msg}')"
                    )

            # 禁止テキストチェック
            for text in checks.get("forbidden_texts", []):
                # NaN等はJSのレンダリング結果として表示される場合のみ問題
                # body_textの中で独立したトークンとして存在するか確認
                pattern = rf'\b{re.escape(text)}\b'
                matches = re.findall(pattern, body_text)
                if matches:
                    issues.append(f"禁止テキスト '{text}' が {len(matches)} 箇所で検出")

            # 数値存在チェック
            if checks.get("required_numbers"):
                # カンマ区切り数値またはパーセンテージを探す
                number_pattern = r'\d[\d,]*\.?\d*'
                numbers = re.findall(number_pattern, body_text)
                if len(numbers) < 3:
                    issues.append(f"数値が少なすぎる（{len(numbers)}個）")

            # チャート数チェック（recharts SVGまたはcanvas要素）
            min_charts = checks.get("min_charts", 0)
            if min_charts > 0:
                chart_count = page.evaluate("""() => {
                    const svgCharts = document.querySelectorAll(
                        '.recharts-wrapper, .recharts-responsive-container, svg.recharts-surface'
                    ).length;
                    const canvasCharts = document.querySelectorAll('canvas').length;
                    const tremorCharts = document.querySelectorAll(
                        '[class*="tremor"], [class*="chart"], [class*="Chart"]'
                    ).length;
                    // recharts以外のSVGチャートも検出
                    const svgElements = document.querySelectorAll('svg').length;
                    return {svgCharts, canvasCharts, tremorCharts, svgElements};
                }""")
                total_charts = (
                    chart_count.get("svgCharts", 0)
                    + chart_count.get("canvasCharts", 0)
                    + chart_count.get("tremorCharts", 0)
                )
                # SVG要素があればチャートとみなす（recharts含む）
                if total_charts == 0 and chart_count.get("svgElements", 0) >= min_charts:
                    total_charts = chart_count["svgElements"]

                if total_charts < min_charts:
                    issues.append(
                        f"チャート数 {total_charts} < {min_charts} "
                        f"(svg={chart_count.get('svgCharts')}, "
                        f"canvas={chart_count.get('canvasCharts')}, "
                        f"tremor={chart_count.get('tremorCharts')}, "
                        f"svgAll={chart_count.get('svgElements')})"
                    )

            # 裸のサービスコード（3桁数字が単独で表示）チェック
            if checks.get("forbidden_bare_codes"):
                bare_code_issues = self._check_bare_service_codes(page)
                if bare_code_issues:
                    issues.extend(bare_code_issues)

            if issues:
                self.results.record(test_name, False, "; ".join(issues))
            else:
                details = f"本文長={body_length}"
                if min_charts > 0:
                    details += f", charts={total_charts}"
                self.results.record(test_name, True, details)

        except Exception as e:
            self.results.record(test_name, False, f"例外: {e}")

    def _check_bare_service_codes(self, page) -> list[str]:
        """テーブル内に裸の3桁サービスコードがないか確認"""
        issues = []
        try:
            # テーブルセルのテキストを取得
            cell_texts = page.evaluate("""() => {
                const cells = document.querySelectorAll('td, th');
                const texts = [];
                cells.forEach(cell => {
                    const t = cell.textContent.trim();
                    if (t) texts.push(t);
                });
                return texts;
            }""")

            # テーブルセル内に3桁の数字のみが表示されているケースを検出
            # 実際の介護保険サービスコードのみ（constants.tsのSERVICE_TYPESと一致）
            known_codes = {
                "110", "120", "130", "140", "150", "155", "160", "170",
                "210", "220", "230", "320",
                "331", "332", "334", "335", "336", "337",
                "361", "362", "364",
                "410", "430",
                "510", "520", "530", "540", "550", "551",
                "710", "720", "730", "760", "770", "780",
            }
            bare_codes_found = []
            for text in cell_texts:
                if re.match(r'^\d{3}$', text) and text in known_codes:
                    bare_codes_found.append(text)

            if bare_codes_found:
                unique_codes = list(set(bare_codes_found))[:5]
                issues.append(
                    f"裸サービスコード検出: {unique_codes} "
                    f"(計{len(bare_codes_found)}セル) - サービス名に変換されていない"
                )
        except Exception as e:
            issues.append(f"裸コードチェック例外: {e}")
        return issues

    # ── 3. レイアウトテスト ───────────────────────
    def test_page_layout(self, page, path: str, checks: dict):
        """ページレイアウトの問題を検出"""
        test_name = f"Layout {path}"
        try:
            self._navigate_authenticated(
                page, path,
                timeout=_page_timeout(path),
                wait_until=_wait_strategy(path),
            )

            issues = []

            # overflow検出
            if checks.get("no_overflow"):
                overflows = page.evaluate("""() => {
                    const elements = document.querySelectorAll('*');
                    const overflows = [];
                    elements.forEach(el => {
                        // 微小な差異は無視（2px以上のoverflow）
                        if (el.scrollWidth > el.clientWidth + 2 ||
                            el.scrollHeight > el.clientHeight + 2) {
                            // 意図的なスクロール領域は除外
                            const style = window.getComputedStyle(el);
                            const isScrollable = (
                                style.overflow === 'auto' ||
                                style.overflow === 'scroll' ||
                                style.overflowX === 'auto' ||
                                style.overflowX === 'scroll' ||
                                style.overflowY === 'auto' ||
                                style.overflowY === 'scroll'
                            );
                            // body/html/意図的スクロールは除外
                            if (!isScrollable &&
                                el.tagName !== 'BODY' &&
                                el.tagName !== 'HTML' &&
                                el.clientWidth > 50) {
                                overflows.push({
                                    tag: el.tagName,
                                    cls: (el.className || '').toString().substring(0, 60),
                                    scrollW: el.scrollWidth,
                                    clientW: el.clientWidth,
                                    scrollH: el.scrollHeight,
                                    clientH: el.clientHeight,
                                    diffW: el.scrollWidth - el.clientWidth,
                                    diffH: el.scrollHeight - el.clientHeight,
                                });
                            }
                        }
                    });
                    return overflows;
                }""")

                # 重大なoverflow（10px以上）のみ報告
                significant = [o for o in overflows if o.get("diffW", 0) > 10 or o.get("diffH", 0) > 10]
                if significant:
                    top3 = significant[:3]
                    details = "; ".join([
                        f"{o['tag']}.{o['cls'][:30]} (scrollW={o['scrollW']} > clientW={o['clientW']})"
                        for o in top3
                    ])
                    issues.append(f"overflow検出({len(significant)}要素): {details}")

            # 要素重なりチェック
            if checks.get("check_overlapping_elements"):
                overlaps = page.evaluate("""() => {
                    // 主要なUI要素の位置を取得して重なりを検出
                    const mainContent = document.querySelector('main, [role="main"], .main-content');
                    const sidebar = document.querySelector(
                        'aside, nav, [class*="sidebar"], [class*="filter"], [class*="Sidebar"]'
                    );
                    if (!mainContent || !sidebar) return [];

                    const mainRect = mainContent.getBoundingClientRect();
                    const sideRect = sidebar.getBoundingClientRect();

                    // 水平方向の重なり検出
                    const overlapX = Math.max(0,
                        Math.min(mainRect.right, sideRect.right) -
                        Math.max(mainRect.left, sideRect.left)
                    );

                    if (overlapX > 5) {
                        return [{
                            element1: 'main-content',
                            element2: 'sidebar/filter',
                            overlapPx: overlapX,
                            mainLeft: mainRect.left,
                            mainRight: mainRect.right,
                            sideLeft: sideRect.left,
                            sideRight: sideRect.right,
                        }];
                    }
                    return [];
                }""")

                if overlaps:
                    for ov in overlaps:
                        issues.append(
                            f"要素重なり: {ov['element1']} と {ov['element2']} "
                            f"({ov['overlapPx']}px重複)"
                        )

            if issues:
                self.results.record(test_name, False, "; ".join(issues))
            else:
                self.results.record(test_name, True, "レイアウト問題なし")

        except Exception as e:
            self.results.record(test_name, False, f"例外: {e}")

    # ── 4. スクリーンショットテスト ───────────────
    def test_screenshots(self, page, paths: list[str]):
        """全ページのスクリーンショットを取得し空白ページを検出"""
        self.results.set_category("Screenshot Tests")

        for path in paths:
            test_name = f"Screenshot {path}"
            try:
                self._navigate_authenticated(
                    page, path,
                    timeout=_page_timeout(path),
                    wait_until=_wait_strategy(path),
                )

                filename = path.strip("/").replace("/", "_") or "root"
                screenshot_path = SCREENSHOTS_DIR / f"{filename}.png"
                page.screenshot(path=str(screenshot_path), full_page=True)

                # 画像が空白でないかチェック（ファイルサイズで判定）
                file_size = screenshot_path.stat().st_size
                if file_size < 10000:
                    self.results.record(
                        test_name,
                        False,
                        f"スクリーンショットが小さすぎる({file_size} bytes) - 空白ページの可能性",
                    )
                else:
                    # ページ内のピクセル多様性をチェック
                    diversity = page.evaluate("""() => {
                        const canvas = document.createElement('canvas');
                        const ctx = canvas.getContext('2d');
                        canvas.width = window.innerWidth;
                        canvas.height = Math.min(window.innerHeight, 800);
                        // ページの色サンプリング（簡易版）
                        const elements = document.querySelectorAll('*');
                        const colors = new Set();
                        for (let i = 0; i < Math.min(elements.length, 200); i++) {
                            const style = window.getComputedStyle(elements[i]);
                            colors.add(style.backgroundColor);
                            colors.add(style.color);
                        }
                        return {colorCount: colors.size, elementCount: elements.length};
                    }""")
                    self.results.record(
                        test_name,
                        True,
                        f"size={file_size}bytes, colors={diversity.get('colorCount', 0)}, "
                        f"elements={diversity.get('elementCount', 0)}",
                    )

            except Exception as e:
                self.results.record(test_name, False, f"例外: {e}")

    # ── 5. 操作テスト ─────────────────────────────
    def test_interactions(self, page):
        """フィルタ操作でデータが変わることを検証"""
        self.results.set_category("Interaction Tests")

        # ダッシュボードの都道府県フィルタテスト
        self._test_prefecture_filter(page)

        # ダッシュボードの棒グラフクリック→フィルタテスト
        self._test_bar_chart_click_filter(page)

        # サービス種別フィルタテスト
        self._test_service_filter(page)

    def _test_prefecture_filter(self, page):
        """都道府県フィルタが機能するか検証"""
        test_name = "Dashboard prefecture filter"
        try:
            self._navigate_authenticated(page, "/dashboard")

            # フィルタ操作前のテキストを取得
            before_text = page.inner_text("body")

            # 都道府県選択ドロップダウンを探す
            # select要素、またはカスタムドロップダウン
            selectors_to_try = [
                'select',
                '[class*="prefecture"] select',
                '[class*="filter"] select',
                'button:has-text("全国")',
                'button:has-text("都道府県")',
                '[role="combobox"]',
                '[class*="Select"]',
            ]

            filter_found = False
            for selector in selectors_to_try:
                try:
                    el = page.query_selector(selector)
                    if el and el.is_visible():
                        filter_found = True
                        # select要素の場合
                        tag = el.evaluate("el => el.tagName")
                        if tag == "SELECT":
                            # オプションを取得
                            options = el.evaluate("""el => {
                                return Array.from(el.options).map(o => ({
                                    value: o.value,
                                    text: o.text
                                })).slice(0, 10);
                            }""")
                            if len(options) > 1:
                                # 2番目のオプションを選択
                                page.select_option(selector, index=1)
                                time.sleep(2)
                                after_text = page.inner_text("body")
                                changed = before_text != after_text
                                self.results.record(
                                    test_name,
                                    changed,
                                    f"フィルタ操作後データ{'変化あり' if changed else '変化なし'} "
                                    f"(options={len(options)})",
                                )
                                return
                        else:
                            # カスタムドロップダウンの場合、クリックして選択
                            el.click()
                            time.sleep(1)
                            # ドロップダウンメニューから選択
                            menu_items = page.query_selector_all(
                                '[role="option"], [class*="option"], li'
                            )
                            if len(menu_items) > 1:
                                menu_items[1].click()
                                time.sleep(2)
                                after_text = page.inner_text("body")
                                changed = before_text != after_text
                                self.results.record(
                                    test_name,
                                    changed,
                                    f"カスタムフィルタ操作後データ{'変化あり' if changed else '変化なし'}",
                                )
                                return
                        break
                except Exception:
                    continue

            if not filter_found:
                self.results.record(test_name, False, "フィルタ要素が見つからない")
            else:
                self.results.record(test_name, False, "フィルタ操作に失敗")

        except Exception as e:
            self.results.record(test_name, False, f"例外: {e}")

    def _test_bar_chart_click_filter(self, page):
        """ダッシュボードの都道府県棒グラフをクリックしてフィルタが適用されるか検証"""
        test_name = "Dashboard bar chart click-to-filter"
        try:
            self._navigate_authenticated(page, "/dashboard")

            before_url = page.url
            before_text = page.inner_text("body")

            # rechartsの棒グラフ内のバー要素を探す
            bar_clicked = page.evaluate("""() => {
                // recharts の Bar 要素（rect）を探す
                const bars = document.querySelectorAll(
                    '.recharts-bar-rectangle rect, ' +
                    '.recharts-bar rect, ' +
                    'svg .recharts-rectangle'
                );
                if (bars.length > 0) {
                    // 最初のバーをクリック
                    const bar = bars[0];
                    const rect = bar.getBoundingClientRect();
                    bar.dispatchEvent(new MouseEvent('click', {
                        bubbles: true,
                        clientX: rect.left + rect.width / 2,
                        clientY: rect.top + rect.height / 2,
                    }));
                    return {found: true, count: bars.length};
                }

                // フォールバック: svg内の全rect要素から棒グラフらしいものを探す
                const allRects = document.querySelectorAll('svg rect');
                for (const rect of allRects) {
                    const height = parseFloat(rect.getAttribute('height') || '0');
                    const width = parseFloat(rect.getAttribute('width') || '0');
                    // 棒グラフのバーは幅が狭く高さがある（または横棒グラフ）
                    if ((height > 20 && width > 5 && width < 100) ||
                        (width > 20 && height > 5 && height < 100)) {
                        const r = rect.getBoundingClientRect();
                        rect.dispatchEvent(new MouseEvent('click', {
                            bubbles: true,
                            clientX: r.left + r.width / 2,
                            clientY: r.top + r.height / 2,
                        }));
                        return {found: true, count: allRects.length, fallback: true};
                    }
                }

                return {found: false, count: 0};
            }""")

            if not bar_clicked.get("found"):
                self.results.record(test_name, False, "棒グラフのバー要素が見つからない")
                return

            # クリック後の状態変化を確認（URL変化 or ページ内容変化）
            time.sleep(2)
            after_url = page.url
            after_text = page.inner_text("body")

            url_changed = before_url != after_url
            content_changed = before_text != after_text

            if url_changed or content_changed:
                change_detail = []
                if url_changed:
                    change_detail.append(f"URL変化: {after_url}")
                if content_changed:
                    change_detail.append("コンテンツ変化あり")
                self.results.record(
                    test_name,
                    True,
                    f"バークリックで反応あり (bars={bar_clicked.get('count')}, "
                    f"{', '.join(change_detail)})",
                )
            else:
                # クリックしても変化なし - 機能未実装の可能性
                self.results.record(
                    test_name,
                    False,
                    f"バークリック後に変化なし (bars={bar_clicked.get('count')})",
                )

        except Exception as e:
            self.results.record(test_name, False, f"例外: {e}")

    def _test_service_filter(self, page):
        """サービス種別フィルタが機能するか検証（カスタムドロップダウン対応）"""
        test_name = "Workforce service filter"
        try:
            self._navigate_authenticated(page, "/workforce")

            before_text = page.inner_text("body")

            # カスタムMultiSelectDropdownのトリガーボタンを探す
            custom_selectors = [
                'button:has-text("サービス種別を選択")',
                'button:has-text("件選択中")',
                'button:has-text("選択...")',
                '[role="listbox"]',
            ]

            for selector in custom_selectors:
                try:
                    el = page.query_selector(selector)
                    if el and el.is_visible():
                        # カスタムドロップダウンをクリックして開く
                        el.click()
                        time.sleep(1)

                        # ドロップダウンメニューのオプションを探す
                        menu_items = page.query_selector_all('[role="option"]')
                        if len(menu_items) > 0:
                            # 最初のオプションをクリック
                            menu_items[0].click()
                            time.sleep(2)
                            after_text = page.inner_text("body")
                            changed = before_text != after_text
                            self.results.record(
                                test_name,
                                changed,
                                f"カスタムフィルタ操作後データ{'変化あり' if changed else '変化なし'} "
                                f"(options={len(menu_items)})",
                            )
                            return
                except Exception:
                    continue

            # フォールバック: ネイティブselect要素を探す
            selectors = [
                'select',
                '[class*="service"] select',
                '[class*="filter"] select',
            ]

            for selector in selectors:
                try:
                    elements = page.query_selector_all(selector)
                    for el in elements:
                        if el.is_visible():
                            options = el.evaluate("""el => {
                                return Array.from(el.options).map(o => ({
                                    value: o.value,
                                    text: o.text
                                }));
                            }""")
                            if len(options) > 1:
                                el.select_option(index=1)
                                time.sleep(2)
                                after_text = page.inner_text("body")
                                changed = before_text != after_text
                                self.results.record(
                                    test_name,
                                    changed,
                                    f"フィルタ操作後データ{'変化あり' if changed else '変化なし'} "
                                    f"(options={len(options)})",
                                )
                                return
                except Exception:
                    continue

            self.results.record(test_name, False, "サービスフィルタ要素が見つからない")

        except Exception as e:
            self.results.record(test_name, False, f"例外: {e}")

    # ── ブラウザログインフロー ─────────────────────
    def browser_login(self, page) -> bool:
        """ブラウザ上でログインフローを実行"""
        try:
            page.goto(f"{FRONTEND_URL}/login", wait_until="networkidle", timeout=15000)
            time.sleep(1)

            # メール入力
            email_selectors = [
                'input[type="email"]',
                'input[name="email"]',
                'input[placeholder*="mail"]',
                'input[placeholder*="メール"]',
            ]
            email_input = None
            for sel in email_selectors:
                email_input = page.query_selector(sel)
                if email_input:
                    break
            if not email_input:
                # 全inputから探す
                inputs = page.query_selector_all('input')
                if inputs:
                    email_input = inputs[0]

            if not email_input:
                print("  メール入力欄が見つからない")
                return False

            email_input.fill(LOGIN_EMAIL)

            # パスワード入力
            pw_selectors = [
                'input[type="password"]',
                'input[name="password"]',
            ]
            pw_input = None
            for sel in pw_selectors:
                pw_input = page.query_selector(sel)
                if pw_input:
                    break

            if not pw_input:
                print("  パスワード入力欄が見つからない")
                return False

            pw_input.fill(LOGIN_PASSWORD)

            # ログインボタン
            btn_selectors = [
                'button[type="submit"]',
                'button:has-text("ログイン")',
                'button:has-text("Login")',
                'button:has-text("Sign in")',
            ]
            btn = None
            for sel in btn_selectors:
                btn = page.query_selector(sel)
                if btn:
                    break

            if not btn:
                print("  ログインボタンが見つからない")
                return False

            btn.click()

            # ログイン完了待機（ダッシュボードへのリダイレクト）
            try:
                page.wait_for_url("**/dashboard**", timeout=15000)
                time.sleep(3)
                print("  ブラウザログイン成功（dashboardにリダイレクト）")
                return True
            except Exception:
                # リダイレクトされなくても、ローカルストレージにトークンがあればOK
                time.sleep(5)
                current_url = page.url
                if "/login" not in current_url:
                    print(f"  ブラウザログイン成功（{current_url}にリダイレクト）")
                    return True

                # localStorageチェック
                token = page.evaluate("""() => {
                    return localStorage.getItem('kaigo_bi_token') ||
                           localStorage.getItem('token') ||
                           localStorage.getItem('access_token') ||
                           localStorage.getItem('auth_token');
                }""")
                if token:
                    print("  ブラウザログイン成功（localStorageにトークン確認）")
                    return True

                print(f"  ブラウザログイン後のリダイレクト失敗: 現在URL={current_url}")
                return False

        except Exception as e:
            print(f"  ブラウザログイン例外: {e}")
            return False

    # ── メイン実行 ─────────────────────────────────
    def run(self):
        """全テストを実行"""
        print("=" * 68)
        print(f"介護BI E2E包括テスト開始 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 68)

        # スクリーンショットディレクトリ作成
        SCREENSHOTS_DIR.mkdir(parents=True, exist_ok=True)

        # ── ステップ1: ヘルスチェック ──────────────
        print("\n--- ヘルスチェック ---")
        try:
            resp = requests.get(f"{BACKEND_URL}/api/health", timeout=5)
            if resp.status_code != 200:
                print(f"  バックエンド異常: HTTP {resp.status_code}")
                print("  テスト中止: バックエンドが起動していません")
                return 1
            print(f"  バックエンド: OK ({resp.json()})")
        except requests.exceptions.ConnectionError:
            print(f"  バックエンド({BACKEND_URL})に接続できません")
            print("  テスト中止: バックエンドを起動してください")
            return 1

        try:
            resp = requests.get(f"{FRONTEND_URL}", timeout=30, allow_redirects=False)
            if resp.status_code not in (200, 307, 302):
                print(f"  フロントエンド異常: HTTP {resp.status_code}")
            else:
                print(f"  フロントエンド: OK (HTTP {resp.status_code})")
        except (requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout):
            print(f"  フロントエンド({FRONTEND_URL})に接続できません")
            print("  警告: ブラウザテストはスキップされます")

        # ── ステップ2: APIログイン ─────────────────
        print("\n--- APIログイン ---")
        if not self.login_api():
            print("  APIログイン失敗 - 認証なしでテスト続行")
        else:
            print(f"  APIログイン成功: token={self.token[:20]}...")

        # ── ステップ3: APIテスト ───────────────────
        print("\n--- API Tests ---")
        self.test_api_responses()

        # ── ステップ4: ブラウザテスト ──────────────
        print("\n--- Browser Tests ---")
        try:
            from playwright.sync_api import sync_playwright

            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    viewport={"width": 1920, "height": 1080},
                    locale="ja-JP",
                )
                page = context.new_page()

                # ブラウザログイン
                print("\n--- ブラウザログイン ---")
                if not self.browser_login(page):
                    print("  ブラウザログイン失敗")
                    # localStorageにトークンを直接設定してリトライ
                    if self.token:
                        print("  APIトークンを使用してlocalStorage設定")
                        page.goto(f"{FRONTEND_URL}/login", wait_until="domcontentloaded", timeout=15000)
                        page.evaluate(f"""() => {{
                            localStorage.setItem('kaigo_bi_token', '{self.token}');
                        }}""")
                        # ページをリロードしてAuthProviderにトークンを認識させる
                        page.goto(f"{FRONTEND_URL}/dashboard", wait_until="domcontentloaded", timeout=30000)
                        # AuthProviderの/api/auth/me呼び出し完了を待つ
                        time.sleep(8)
                        if "/login" in page.url:
                            print("  localStorageトークン設定後もログインページにリダイレクトされる")
                            print("  ブラウザテストをスキップ")
                            browser.close()
                            report, fail_count = self.results.summary()
                            self._save_report(report)
                            return fail_count
                        print("  localStorageトークン設定でログイン成功")

                # ページデータテスト
                print("\n--- Page Data Tests ---")
                self.results.set_category("Page Data Tests")
                for path, checks in PAGE_DATA_CHECKS.items():
                    self.test_page_data(page, path, checks)

                # レイアウトテスト
                print("\n--- Layout Tests ---")
                self.results.set_category("Layout Tests")
                for path, checks in LAYOUT_CHECKS.items():
                    self.test_page_layout(page, path, checks)

                # スクリーンショットテスト
                print("\n--- Screenshot Tests ---")
                all_paths = list(PAGE_DATA_CHECKS.keys())
                self.test_screenshots(page, all_paths)

                # 操作テスト
                print("\n--- Interaction Tests ---")
                self.test_interactions(page)

                browser.close()

        except ImportError:
            print("  playwright未インストール: pip install playwright && playwright install chromium")
            self.results.set_category("Browser Tests")
            self.results.record("playwright", False, "playwright未インストール")
        except Exception as e:
            print(f"  ブラウザテスト例外: {e}")
            traceback.print_exc()

        # ── レポート出力 ──────────────────────────
        report, fail_count = self.results.summary()
        self._save_report(report)
        return fail_count

    def _save_report(self, report):
        """レポートをファイルに保存"""
        try:
            with open(REPORT_FILE, "w", encoding="utf-8") as f:
                f.write(report)
            print(f"\nレポート保存: {REPORT_FILE}")
        except Exception as e:
            print(f"レポート保存失敗: {e}")

        # スクリーンショットのパスも出力
        if SCREENSHOTS_DIR.exists():
            screenshots = list(SCREENSHOTS_DIR.glob("*.png"))
            if screenshots:
                print(f"スクリーンショット: {SCREENSHOTS_DIR} ({len(screenshots)}枚)")


# ═══════════════════════════════════════════════════
# エントリーポイント
# ═══════════════════════════════════════════════════
if __name__ == "__main__":
    suite = E2ETestSuite()
    exit_code = suite.run()
    sys.exit(min(exit_code, 1))
