"""
Turso DBにfacilitiesテーブルを作成し、東京都通所介護データをアップロードするスクリプト

データソース: data/output/kaigo_scraping/tokyo_day_care_150_20260319.csv (1,546件)
Turso HTTP API (v2 pipeline) を使用
"""

import csv
import os
import re
import sys
import time
import requests
from datetime import datetime

# Turso接続情報
TURSO_URL = os.environ.get("TURSO_DATABASE_URL", "https://cw-makimaki1006.aws-ap-northeast-1.turso.io")
TURSO_TOKEN = os.environ.get("TURSO_AUTH_TOKEN")
if not TURSO_TOKEN:
    raise ValueError("TURSO_AUTH_TOKEN environment variable is required")

# CSVファイルパス
CSV_PATH = r"C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List\data\output\kaigo_scraping\tokyo_day_care_150_20260319.csv"

# バッチサイズ（Turso pipelineの制限を考慮）
BATCH_SIZE = 50

HEADERS = {
    "Authorization": f"Bearer {TURSO_TOKEN}",
    "Content-Type": "application/json",
}


def execute_sql(statements: list[dict]) -> dict:
    """Turso HTTP API v2 pipeline でSQLを実行"""
    payload = {"requests": statements}
    resp = requests.post(f"{TURSO_URL}/v2/pipeline", headers=HEADERS, json=payload)
    if resp.status_code != 200:
        print(f"HTTP {resp.status_code}: {resp.text[:500]}")
        raise Exception(f"Turso APIエラー: HTTP {resp.status_code}")
    return resp.json()


def execute_single(sql: str, args: list = None) -> dict:
    """単一SQLを実行"""
    stmt = {"type": "execute", "stmt": {"sql": sql}}
    if args:
        stmt["stmt"]["args"] = args
    return execute_sql([stmt])


def extract_prefecture(address: str) -> str | None:
    """住所から都道府県を抽出"""
    if not address:
        return None
    # 〒コードとスペースをスキップ
    cleaned = re.sub(r"^〒?\d{3}-?\d{4}\s*", "", address)
    # 都道府県パターン
    match = re.match(r"(東京都|北海道|(?:京都|大阪)府|.{2,3}県)", cleaned)
    return match.group(1) if match else None


def classify_corp_type(corp_name: str) -> str:
    """法人名から法人種別を推定"""
    if not corp_name:
        return "不明"
    patterns = {
        "社会福祉法人": "社会福祉法人",
        "医療法人": "医療法人",
        "株式会社": "株式会社",
        "有限会社": "有限会社",
        "合同会社": "合同会社",
        "NPO法人": "NPO法人",
        "特定非営利活動法人": "NPO法人",
        "一般社団法人": "一般社団法人",
        "一般財団法人": "一般財団法人",
        "公益社団法人": "公益社団法人",
        "公益財団法人": "公益財団法人",
        "合資会社": "合資会社",
        "地方公共団体": "地方公共団体",
    }
    for keyword, corp_type in patterns.items():
        if keyword in corp_name:
            return corp_type
    return "その他"


def parse_int(value: str) -> int | None:
    """文字列を整数に変換（空やエラーはNone）"""
    if not value or not value.strip():
        return None
    try:
        return int(float(value.strip()))
    except (ValueError, TypeError):
        return None


def parse_float(value: str) -> float | None:
    """文字列を浮動小数点に変換"""
    if not value or not value.strip():
        return None
    try:
        return float(value.strip())
    except (ValueError, TypeError):
        return None


def compute_turnover_rate(staff_total: int | None, left_last_year: int | None) -> float | None:
    """離職率: 退職数 / (合計 + 退職数)"""
    if staff_total is None or left_last_year is None:
        return None
    denominator = staff_total + left_last_year
    if denominator <= 0:
        return None
    return round(left_last_year / denominator, 4)


def compute_fulltime_ratio(staff_fulltime: int | None, staff_total: int | None) -> float | None:
    """常勤比率: 常勤 / 合計"""
    if staff_fulltime is None or staff_total is None or staff_total <= 0:
        return None
    return round(staff_fulltime / staff_total, 4)


def compute_years_in_business(start_date: str) -> int | None:
    """事業年数: 2026 - 事業開始年"""
    if not start_date or not start_date.strip():
        return None
    try:
        year = int(start_date.strip().split("/")[0])
        return 2026 - year
    except (ValueError, IndexError):
        return None


def create_table():
    """facilitiesテーブルを作成"""
    print("=== テーブル作成 ===")

    # 既存テーブルの確認
    result = execute_single("SELECT name FROM sqlite_master WHERE type='table' AND name='facilities'")
    rows = result.get("results", [{}])[0].get("response", {}).get("result", {}).get("rows", [])
    if rows:
        print("facilitiesテーブルが既に存在します。DROP & RECREATEします。")
        execute_single("DROP TABLE IF EXISTS facilities")

    create_sql = """
    CREATE TABLE IF NOT EXISTS facilities (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        jigyosho_number TEXT UNIQUE NOT NULL,
        service_code TEXT,
        service_name TEXT,
        prefecture_code TEXT,
        prefecture_name TEXT,
        facility_name TEXT,
        manager_name TEXT,
        manager_title TEXT,
        representative_name TEXT,
        representative_title TEXT,
        corp_name TEXT,
        corp_number TEXT,
        phone TEXT,
        fax TEXT,
        address TEXT,
        homepage TEXT,
        staff_fulltime INTEGER,
        staff_parttime INTEGER,
        staff_total INTEGER,
        capacity INTEGER,
        start_date TEXT,
        hired_last_year INTEGER,
        left_last_year INTEGER,
        scraped_at TEXT,
        prefecture TEXT,
        corp_type TEXT,
        turnover_rate REAL,
        fulltime_ratio REAL,
        years_in_business INTEGER
    )
    """
    execute_single(create_sql)
    print("テーブル作成完了")

    # インデックス作成
    indices = [
        "CREATE INDEX IF NOT EXISTS idx_facilities_phone ON facilities(phone)",
        "CREATE INDEX IF NOT EXISTS idx_facilities_corp_number ON facilities(corp_number)",
        "CREATE INDEX IF NOT EXISTS idx_facilities_prefecture ON facilities(prefecture)",
        "CREATE INDEX IF NOT EXISTS idx_facilities_corp_type ON facilities(corp_type)",
    ]
    for idx_sql in indices:
        execute_single(idx_sql)
    print("インデックス作成完了")


def make_arg(value):
    """Turso API用の引数を作成"""
    if value is None:
        return {"type": "null"}
    elif isinstance(value, int):
        return {"type": "integer", "value": str(value)}
    elif isinstance(value, float):
        return {"type": "float", "value": value}
    else:
        return {"type": "text", "value": str(value)}


def upload_data():
    """CSVデータを読み込み、Tursoにアップロード"""
    print(f"\n=== データアップロード ===")
    print(f"CSVファイル: {CSV_PATH}")

    # CSV読み込み（BOM付きUTF-8対応）
    with open(CSV_PATH, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    total = len(rows)
    print(f"読み込み件数: {total}")

    insert_sql = """
    INSERT INTO facilities (
        jigyosho_number, facility_name,
        manager_name, manager_title,
        representative_name, representative_title,
        corp_name, corp_number,
        phone, fax, address, homepage,
        staff_fulltime, staff_parttime, staff_total,
        capacity, start_date,
        hired_last_year, left_last_year,
        scraped_at,
        prefecture, corp_type,
        turnover_rate, fulltime_ratio, years_in_business
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    uploaded = 0
    errors = 0
    start_time = time.time()

    for batch_start in range(0, total, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, total)
        batch = rows[batch_start:batch_end]

        statements = []
        for row in batch:
            # 元データ
            jigyosho_number = row.get("事業所番号", "").strip()
            facility_name = row.get("事業所名", "").strip()
            manager_name = row.get("管理者名", "").strip() or None
            manager_title = row.get("管理者職名", "").strip() or None
            representative_name = row.get("代表者名", "").strip() or None
            representative_title = row.get("代表者職名", "").strip() or None
            corp_name = row.get("法人名", "").strip() or None
            corp_number = row.get("法人番号", "").strip() or None
            phone = row.get("電話番号", "").strip() or None
            fax = row.get("FAX番号", "").strip() or None
            address = row.get("住所", "").strip() or None
            homepage = row.get("HP", "").strip() or None
            start_date = row.get("事業開始日", "").strip() or None

            # 数値変換
            staff_fulltime = parse_int(row.get("従業者_常勤", ""))
            staff_parttime = parse_int(row.get("従業者_非常勤", ""))
            staff_total = parse_int(row.get("従業者_合計", ""))
            capacity = parse_int(row.get("定員", ""))
            hired_last_year = parse_int(row.get("前年度採用数", ""))
            left_last_year = parse_int(row.get("前年度退職数", ""))

            # 派生カラム計算
            prefecture = extract_prefecture(address)
            corp_type = classify_corp_type(corp_name) if corp_name else None
            turnover_rate = compute_turnover_rate(staff_total, left_last_year)
            fulltime_ratio = compute_fulltime_ratio(staff_fulltime, staff_total)
            years_in_business = compute_years_in_business(start_date)

            scraped_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            args = [
                make_arg(jigyosho_number),
                make_arg(facility_name),
                make_arg(manager_name),
                make_arg(manager_title),
                make_arg(representative_name),
                make_arg(representative_title),
                make_arg(corp_name),
                make_arg(corp_number),
                make_arg(phone),
                make_arg(fax),
                make_arg(address),
                make_arg(homepage),
                make_arg(staff_fulltime),
                make_arg(staff_parttime),
                make_arg(staff_total),
                make_arg(capacity),
                make_arg(start_date),
                make_arg(hired_last_year),
                make_arg(left_last_year),
                make_arg(scraped_at),
                make_arg(prefecture),
                make_arg(corp_type),
                make_arg(turnover_rate),
                make_arg(fulltime_ratio),
                make_arg(years_in_business),
            ]

            statements.append({
                "type": "execute",
                "stmt": {"sql": insert_sql, "args": args},
            })

        # バッチ実行
        try:
            result = execute_sql(statements)
            # エラーチェック
            batch_errors = 0
            for i, res in enumerate(result.get("results", [])):
                if "error" in res:
                    batch_errors += 1
                    if batch_errors <= 3:
                        print(f"  エラー (行 {batch_start + i + 1}): {res['error']}")
            uploaded += len(batch) - batch_errors
            errors += batch_errors
        except Exception as e:
            print(f"  バッチエラー ({batch_start+1}-{batch_end}): {e}")
            errors += len(batch)

        # 進捗表示
        progress = batch_end / total * 100
        elapsed = time.time() - start_time
        print(f"  [{progress:5.1f}%] {batch_end}/{total} 件完了 ({elapsed:.1f}秒)", end="\r")

    elapsed = time.time() - start_time
    print(f"\n\n=== アップロード完了 ===")
    print(f"成功: {uploaded} 件")
    print(f"エラー: {errors} 件")
    print(f"処理時間: {elapsed:.1f} 秒")


def verify_upload():
    """アップロード結果を検証"""
    print(f"\n=== 検証 ===")

    # 件数確認
    result = execute_single("SELECT COUNT(*) as cnt FROM facilities")
    count = result["results"][0]["response"]["result"]["rows"][0][0]["value"]
    print(f"facilities テーブルの件数: {count}")

    # サンプルデータ表示
    result = execute_single("SELECT jigyosho_number, facility_name, prefecture, corp_type, phone FROM facilities LIMIT 5")
    rows = result["results"][0]["response"]["result"]["rows"]
    print("\nサンプルデータ (5件):")
    for row in rows:
        values = [col.get("value", "NULL") for col in row]
        print(f"  {values[0]} | {values[1]} | {values[2]} | {values[3]} | {values[4]}")

    # 都道府県別集計
    result = execute_single("SELECT prefecture, COUNT(*) as cnt FROM facilities GROUP BY prefecture ORDER BY cnt DESC LIMIT 5")
    rows = result["results"][0]["response"]["result"]["rows"]
    print("\n都道府県別集計 (上位5):")
    for row in rows:
        pref = row[0].get("value", "NULL")
        cnt = row[1].get("value", "0")
        print(f"  {pref}: {cnt} 件")

    # 法人種別集計
    result = execute_single("SELECT corp_type, COUNT(*) as cnt FROM facilities GROUP BY corp_type ORDER BY cnt DESC")
    rows = result["results"][0]["response"]["result"]["rows"]
    print("\n法人種別集計:")
    for row in rows:
        ct = row[0].get("value", "NULL")
        cnt = row[1].get("value", "0")
        print(f"  {ct}: {cnt} 件")


def main():
    print("=" * 60)
    print("Turso DB: facilities テーブル作成 & データアップロード")
    print("=" * 60)

    # Step 1: テーブル作成
    create_table()

    # Step 2: データアップロード
    upload_data()

    # Step 3: 検証
    verify_upload()

    print("\n完了しました。")


if __name__ == "__main__":
    main()
