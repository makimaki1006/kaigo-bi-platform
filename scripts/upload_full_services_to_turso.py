"""
完了済みサービスCSVをTurso DBにアップロードするスクリプト
対象: data/output/kaigo_scraping/by_service/ 配下の完了済みCSV

既存データはDELETE済み前提（テーブル構造はそのまま）
"""

import csv
import os
import re
import sys
import time
import glob
import requests

TURSO_URL = os.environ.get("TURSO_DATABASE_URL", "https://cw-makimaki1006.aws-ap-northeast-1.turso.io")
TURSO_TOKEN = os.environ.get("TURSO_AUTH_TOKEN")
if not TURSO_TOKEN:
    raise ValueError("TURSO_AUTH_TOKEN environment variable is required")
HEADERS = {"Authorization": f"Bearer {TURSO_TOKEN}", "Content-Type": "application/json"}
BATCH_SIZE = 50
CSV_DIR = r"C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List\data\output\kaigo_scraping\by_service"


def execute_sql(statements):
    resp = requests.post(f"{TURSO_URL}/v2/pipeline", headers=HEADERS, json={"requests": statements})
    if resp.status_code != 200:
        raise Exception(f"Turso APIエラー: HTTP {resp.status_code}: {resp.text[:300]}")
    return resp.json()


def execute_single(sql, args=None):
    stmt = {"type": "execute", "stmt": {"sql": sql}}
    if args:
        stmt["stmt"]["args"] = args
    return execute_sql([stmt])


def make_arg(value):
    if value is None:
        return {"type": "null"}
    elif isinstance(value, int):
        return {"type": "integer", "value": str(value)}
    elif isinstance(value, float):
        return {"type": "float", "value": value}
    else:
        return {"type": "text", "value": str(value)}


VALID_PREFECTURES = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県",
    "岐阜県", "静岡県", "愛知県", "三重県",
    "滋賀県", "京都府", "大阪府", "兵庫県", "奈良県", "和歌山県",
    "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県",
    "福岡県", "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
]


def extract_prefecture(address):
    """住所文字列から都道府県名を抽出する（47都道府県ホワイトリスト方式）"""
    if not address:
        return None
    # 郵便番号・先頭の空白・制御文字を除去
    cleaned = re.sub(r"^〒?\d{3}-?\d{4}\s*", "", address.strip())
    # 先頭一致を優先
    for pref in VALID_PREFECTURES:
        if cleaned.startswith(pref):
            return pref
    # フォールバック: 文字列内に都道府県名が含まれる場合
    for pref in VALID_PREFECTURES:
        if pref in cleaned:
            return pref
    return None


def classify_corp_type(corp_name):
    if not corp_name:
        return "不明"
    for kw, ct in {"社会福祉法人": "社会福祉法人", "医療法人": "医療法人",
                    "株式会社": "株式会社・有限会社等", "有限会社": "株式会社・有限会社等",
                    "合同会社": "株式会社・有限会社等", "合資会社": "株式会社・有限会社等",
                    "NPO法人": "NPO法人", "特定非営利活動法人": "NPO法人",
                    "一般社団法人": "社団法人", "公益社団法人": "社団法人",
                    "一般財団法人": "財団法人", "公益財団法人": "財団法人",
                    "社会医療法人": "社会医療法人",
                    "地方公共団体": "地方公共団体"}.items():
        if kw in corp_name:
            return ct
    return "その他法人"


def parse_int(value):
    if not value or not value.strip():
        return None
    try:
        return int(float(value.strip()))
    except (ValueError, TypeError):
        return None


def compute_derived(row):
    """派生カラムを計算"""
    address = row.get("住所", "")
    corp_name = row.get("法人名", "")
    staff_fulltime = parse_int(row.get("従業者_常勤", ""))
    staff_total = parse_int(row.get("従業者_合計", ""))
    left_last_year = parse_int(row.get("前年度退職数", ""))
    start_date = row.get("事業開始日", "")

    prefecture = extract_prefecture(address)
    corp_type = classify_corp_type(corp_name)

    turnover_rate = None
    if staff_total is not None and left_last_year is not None:
        denom = staff_total + left_last_year
        if denom > 0:
            turnover_rate = round(left_last_year / denom, 4)

    fulltime_ratio = None
    if staff_fulltime is not None and staff_total is not None and staff_total > 0:
        fulltime_ratio = round(staff_fulltime / staff_total, 4)

    years_in_business = None
    if start_date and start_date.strip():
        try:
            year = int(start_date.strip().split("/")[0])
            years_in_business = 2026 - year
        except (ValueError, IndexError):
            pass

    return prefecture, corp_type, turnover_rate, fulltime_ratio, years_in_business


def upload_csv(csv_path, csv_columns):
    """1つのCSVファイルをTursoにアップロード"""
    filename = csv_path.split("\\")[-1].split("/")[-1]
    print(f"\n--- {filename} ---")

    with open(csv_path, "r", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))

    total = len(rows)
    print(f"  件数: {total}")

    all_cols = csv_columns + ["prefecture", "corp_type", "turnover_rate", "fulltime_ratio", "years_in_business"]
    placeholders = ", ".join(["?"] * len(all_cols))
    col_names = ", ".join([f'"{c}"' for c in all_cols])
    insert_sql = f'INSERT INTO facilities ({col_names}) VALUES ({placeholders})'

    uploaded = 0
    errors = 0
    start_time = time.time()

    for batch_start in range(0, total, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, total)
        batch = rows[batch_start:batch_end]

        statements = []
        for row in batch:
            args = [make_arg(row.get(col, "") or None) for col in csv_columns]
            pref, ct, tr, fr, yib = compute_derived(row)
            args.extend([make_arg(pref), make_arg(ct), make_arg(tr), make_arg(fr), make_arg(yib)])
            statements.append({"type": "execute", "stmt": {"sql": insert_sql, "args": args}})

        try:
            result = execute_sql(statements)
            batch_errors = sum(1 for r in result.get("results", []) if "error" in r)
            uploaded += len(batch) - batch_errors
            errors += batch_errors
            if batch_errors > 0:
                for i, r in enumerate(result.get("results", [])):
                    if "error" in r:
                        print(f"\n  エラー (行{batch_start+i+1}): {r['error']['message'][:100]}")
        except Exception as e:
            print(f"\n  バッチエラー ({batch_start+1}-{batch_end}): {e}")
            errors += len(batch)

        elapsed = time.time() - start_time
        rate = batch_end / elapsed if elapsed > 0 else 0
        remaining = (total - batch_end) / rate / 60 if rate > 0 else 0
        sys.stdout.write(f"\r  [{batch_end}/{total}] {batch_end/total*100:.1f}% | {rate:.1f}件/秒 | 残り{remaining:.1f}分    ")
        sys.stdout.flush()

    elapsed = time.time() - start_time
    print(f"\n  完了: {uploaded}件成功, {errors}件エラー ({elapsed:.1f}秒)")
    return uploaded, errors


def main():
    print("=" * 60)
    print("Turso DB: 完了済みサービスCSVアップロード")
    print("=" * 60)

    csv_files = sorted(glob.glob(f"{CSV_DIR}/*.csv"))
    if not csv_files:
        print("CSVがありません")
        return

    print(f"\n対象ファイル: {len(csv_files)}件")
    for f in csv_files:
        name = f.replace("\\", "/").split("/")[-1]
        print(f"  {name}")

    # CSVカラム名取得
    with open(csv_files[0], "r", encoding="utf-8-sig") as f:
        csv_columns = csv.DictReader(f).fieldnames
    print(f"CSVカラム数: {len(csv_columns)}")

    # 現在の件数
    resp = execute_single("SELECT COUNT(*) FROM facilities")
    current = resp["results"][0]["response"]["result"]["rows"][0][0]["value"]
    print(f"Turso現在の件数: {current}")

    # アップロード
    total_uploaded = 0
    total_errors = 0
    overall_start = time.time()

    for csv_path in csv_files:
        u, e = upload_csv(csv_path, csv_columns)
        total_uploaded += u
        total_errors += e

    overall_elapsed = time.time() - overall_start

    # 検証
    print(f"\n{'=' * 60}")
    print(f"全体: {total_uploaded}件成功, {total_errors}件エラー ({overall_elapsed:.1f}秒)")

    resp = execute_single("SELECT COUNT(*) FROM facilities")
    print(f"Turso最終件数: {resp['results'][0]['response']['result']['rows'][0][0]['value']}")

    resp = execute_single('SELECT "サービスコード", COUNT(*) FROM facilities GROUP BY "サービスコード" ORDER BY COUNT(*) DESC')
    print("\nサービスコード別:")
    for row in resp["results"][0]["response"]["result"]["rows"]:
        print(f"  {row[0]['value']}: {row[1]['value']}件")

    resp = execute_single("SELECT prefecture, COUNT(*) FROM facilities GROUP BY prefecture ORDER BY COUNT(*) DESC LIMIT 10")
    print("\n都道府県別 (Top 10):")
    for row in resp["results"][0]["response"]["result"]["rows"]:
        pref = row[0].get("value", "NULL") if row[0]["type"] != "null" else "不明"
        print(f"  {pref}: {row[1]['value']}件")

    print("\n完了しました。")


if __name__ == "__main__":
    main()
