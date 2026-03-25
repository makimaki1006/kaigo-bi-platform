import json, os, time, sys, requests
sys.stdout.reconfigure(encoding='utf-8')
from playwright.sync_api import sync_playwright

SCREENSHOTS_DIR = r"C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List\screenshots\e2e_test_20260325"
os.makedirs(SCREENSHOTS_DIR, exist_ok=True)

PAGES = [
    ("/dashboard", "01_dashboard", "critical"),
    ("/market", "02_market", "critical"),
    ("/salary", "03_salary", "critical"),
    ("/growth", "04_growth", "critical"),
    ("/workforce", "05_workforce", "critical"),
    ("/quality", "06_quality", "critical"),
    ("/revenue", "07_revenue", "critical"),
    ("/corp-group", "08_corp_group", "secondary"),
    ("/facilities", "09_facilities", "secondary"),
    ("/hiring-weather", "10_hiring_weather", "secondary"),
    ("/financial-health", "11_financial_health", "secondary"),
    ("/cost-estimation", "12_cost_estimation", "secondary"),
    ("/ma-screening", "13_ma_screening", "secondary"),
    ("/due-diligence", "14_due_diligence", "secondary"),
    ("/list-export", "15_list_export", "secondary"),
    ("/corp-compare", "16_corp_compare", "secondary"),
    ("/pmi-synergy", "17_pmi_synergy", "secondary"),
    ("/service-portfolio", "18_service_portfolio", "secondary"),
    ("/benchmark", "19_benchmark", "secondary"),
    ("/trends", "20_trends", "secondary"),
    ("/insights", "21_insights", "secondary"),
    ("/health-check", "22_health_check", "secondary"),
    ("/data-quality", "23_data_quality", "secondary"),
]

results = []

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    context = browser.new_context(viewport={"width": 1920, "height": 1080})
    page = context.new_page()

    # ログイン: フォームベースで直接ログイン
    print("=== Logging in via form ===")
    page.goto("http://localhost:3000/login")
    page.wait_for_load_state("networkidle")
    time.sleep(2)

    email_input = page.query_selector('input[type="email"], input[name="email"], input[placeholder*="メール"], input[placeholder*="email"], input[placeholder*="Email"]')
    password_input = page.query_selector('input[type="password"], input[name="password"]')
    if email_input and password_input:
        email_input.fill("test@test.com")
        password_input.fill("test1234")
        time.sleep(0.5)
        submit = page.query_selector('button[type="submit"], button:has-text("ログイン"), button:has-text("Login")')
        if submit:
            submit.click()
            time.sleep(5)
            page.wait_for_load_state("networkidle")
            print(f"  After login URL: {page.url}")
        else:
            print("  Submit button not found, trying API token approach")
    else:
        print("  Form not found, trying API token approach")

    # フォームログインが失敗した場合のフォールバック
    if "/login" in page.url:
        print("  Form login didn't redirect. Trying API token + localStorage...")
        resp = requests.post("http://localhost:3001/api/auth/login",
                             json={"email": "test@test.com", "password": "test1234"})
        if resp.status_code == 200:
            token = resp.json().get("token", "")
            user = resp.json().get("user", {})
            user_json = json.dumps(user, ensure_ascii=False)
            page.evaluate(f"""() => {{
                localStorage.setItem('token', '{token}');
                localStorage.setItem('user', JSON.stringify({user_json}));
            }}""")
            page.goto("http://localhost:3000/dashboard")
            page.wait_for_load_state("networkidle")
            time.sleep(3)
            print(f"  After token set URL: {page.url}")

    page.screenshot(path=os.path.join(SCREENSHOTS_DIR, "00_after_login.png"), full_page=True)

    # 各ページをテスト
    for path, name, priority in PAGES:
        print(f"\n--- Testing {path} ---")
        try:
            page.goto(f"http://localhost:3000{path}", timeout=30000)
            page.wait_for_load_state("networkidle", timeout=15000)
            time.sleep(4)  # チャートレンダリング待ち

            # スクリーンショット取得
            ss_path = os.path.join(SCREENSHOTS_DIR, f"{name}.png")
            page.screenshot(path=ss_path, full_page=True)

            # ページ内容を分析
            issues = []

            # body テキスト長チェック
            body_text = page.text_content("body") or ""
            if len(body_text) < 50:
                issues.append("Page appears empty (body < 50 chars)")

            # ローディングスピナーチェック
            spinners = page.query_selector_all('.animate-spin, .animate-pulse')
            if len(spinners) > 2:
                issues.append(f"Loading spinners still visible: {len(spinners)}")

            # 「データ準備中」プレースホルダチェック
            placeholders = page.query_selector_all('text="データ準備中"')
            if placeholders:
                issues.append(f"DataPendingPlaceholder: {len(placeholders)} instances")

            # Rechartsチャートチェック
            charts = page.query_selector_all('.recharts-wrapper, svg.recharts-surface')

            # KPI数値チェック
            has_numbers = any(c.isdigit() for c in body_text[:2000])

            # ログインリダイレクトチェック
            current_url = page.url
            if "/login" in current_url and path != "/login":
                issues.append("Redirected to login (auth failure)")

            # エラーメッセージチェック
            error_elements = page.query_selector_all('[class*="error"], [class*="Error"]')
            if error_elements:
                issues.append(f"Error elements found: {len(error_elements)}")

            status = "FAIL" if issues else "PASS"
            result = {
                "page": path,
                "name": name,
                "priority": priority,
                "status": status,
                "charts_count": len(charts),
                "body_length": len(body_text),
                "has_numbers": has_numbers,
                "issues": issues,
                "screenshot": f"{name}.png",
            }
            results.append(result)

            status_icon = "✅" if status == "PASS" else "❌"
            print(f"  {status_icon} {path}: charts={len(charts)}, body={len(body_text)}, issues={issues or 'none'}")

        except Exception as e:
            results.append({
                "page": path, "name": name, "priority": priority,
                "status": "ERROR", "issues": [str(e)], "screenshot": f"{name}.png"
            })
            print(f"  ❌ ERROR: {e}")

    browser.close()

# サマリー出力
print("\n" + "="*70)
print("E2E TEST RESULTS SUMMARY")
print("="*70)
passed = sum(1 for r in results if r["status"] == "PASS")
failed = sum(1 for r in results if r["status"] == "FAIL")
errors = sum(1 for r in results if r["status"] == "ERROR")
print(f"PASS: {passed} | FAIL: {failed} | ERROR: {errors} | Total: {len(results)}")
print()
for r in results:
    icon = "✅" if r["status"] == "PASS" else "❌" if r["status"] == "FAIL" else "💥"
    charts = r.get("charts_count", "?")
    issues_str = str(r['issues']) if r['issues'] else ""
    print(f"  {icon} {r['page']:<30s} charts={str(charts):>3s}  {issues_str}")

# 結果をJSONに保存
with open(os.path.join(SCREENSHOTS_DIR, "results.json"), "w", encoding="utf-8") as f:
    json.dump(results, f, ensure_ascii=False, indent=2)
print(f"\nResults saved to {SCREENSHOTS_DIR}/results.json")
