"""
Turso DBに未アップロードのサービスCSVを追加アップロードするスクリプト

処理フロー:
1. Tursoの既存サービスコード別件数を取得
2. CSVファイルと比較して不足分を特定
3. 不足分のみアップロード（INSERT OR IGNORE で重複防止）
"""

import csv
import re
import sys
import time
import glob
import os
import requests

TURSO_URL = "https://cw-makimaki1006.aws-ap-northeast-1.turso.io"
TURSO_TOKEN = "eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9.eyJhIjoicnciLCJpYXQiOjE3NzM5NDgwNTgsImlkIjoiMDE5ZDA3OGItMjEwMS03OGU1LWE5ZjctZjMyOTUwODEyZjE1IiwicmlkIjoiMDMwMDA1YzctOGI2YS00NWUwLWExMWMtZGNhN2FiMTc3MDFjIn0.k47AO8qry9b9J9bcR7cAQAKYCJhtFCmvaLS1K2UZ5HdeVlKcjO6iiQZT600AzJFcML12JzQd6-viSqnS3UOwCw"
HEADERS = {"Authorization": f"Bearer {TURSO_TOKEN}", "Content-Type": "application/json"}
BATCH_SIZE = 50
CSV_DIR = r"C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List\data\output\kaigo_scraping\by_service"


def execute_sql(statements):
    """Turso Pipeline APIでSQL文を実行"""
    resp = requests.post(f"{TURSO_URL}/v2/pipeline", headers=HEADERS, json={"requests": statements})
    if resp.status_code != 200:
        raise Exception(f"Turso APIエラー: HTTP {resp.status_code}: {resp.text[:300]}")
    return resp.json()


def execute_single(sql, args=None):
    """単一SQL文を実行"""
    stmt = {"type": "execute", "stmt": {"sql": sql}}
    if args:
        stmt["stmt"]["args"] = args
    return execute_sql([stmt])


def make_arg(value):
    """Turso API用の引数フォーマット"""
    if value is None:
        return {"type": "null"}
    elif isinstance(value, int):
        return {"type": "integer", "value": str(value)}
    elif isinstance(value, float):
        return {"type": "float", "value": value}
    else:
        return {"type": "text", "value": str(value)}


def extract_prefecture(address):
    """住所から都道府県を抽出"""
    if not address:
        return None
    cleaned = re.sub(r"^〒?\d{3}-?\d{4}\s*", "", address)
    match = re.match(r"(東京都|北海道|(?:京都|大阪)府|.{2,3}県)", cleaned)
    return match.group(1) if match else None


def classify_corp_type(corp_name):
    """法人名から法人種別を分類"""
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
    """文字列を整数に変換（空文字・不正値はNone）"""
    if not value or not value.strip():
        return None
    try:
        return int(float(value.strip()))
    except (ValueError, TypeError):
        return None


def compute_derived(row):
    """派生カラムを計算（元スクリプトと同一ロジック）"""
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


def get_turso_service_counts():
    """Tursoの既存サービスコード別件数を取得"""
    resp = execute_single(
        'SELECT "サービスコード", COUNT(*) as cnt FROM facilities GROUP BY "サービスコード" ORDER BY "サービスコード"'
    )
    counts = {}
    for row in resp["results"][0]["response"]["result"]["rows"]:
        code = row[0].get("value", "") if row[0]["type"] != "null" else ""
        cnt = int(row[1]["value"])
        counts[code] = cnt
    return counts


def get_csv_info():
    """CSVファイルのサービスコード・件数マッピングを取得"""
    csv_files = sorted(glob.glob(f"{CSV_DIR}/*.csv"))
    info = {}
    for f in csv_files:
        name = os.path.basename(f)
        code = name.split("_")[0]
        with open(f, "r", encoding="utf-8-sig") as fp:
            count = sum(1 for _ in csv.reader(fp)) - 1
        info[code] = {"path": f, "name": name, "count": count}
    return info


def upload_csv(csv_path, csv_columns):
    """1つのCSVファイルをTursoにアップロード（INSERT OR IGNORE）"""
    filename = os.path.basename(csv_path)
    print(f"\n--- {filename} ---")

    with open(csv_path, "r", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))

    total = len(rows)
    print(f"  件数: {total}")

    all_cols = csv_columns + ["prefecture", "corp_type", "turnover_rate", "fulltime_ratio", "years_in_business"]
    placeholders = ", ".join(["?"] * len(all_cols))
    col_names = ", ".join([f'"{c}"' for c in all_cols])
    insert_sql = f'INSERT OR IGNORE INTO facilities ({col_names}) VALUES ({placeholders})'

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
    print("Turso DB: 不足サービスCSV追加アップロード")
    print("=" * 60)

    # 1. Turso既存件数を取得
    print("\n[STEP 1] Turso既存データ確認...")
    turso_counts = get_turso_service_counts()
    turso_total = sum(turso_counts.values())
    print(f"  Turso合計: {turso_total}件 ({len(turso_counts)}サービスコード)")
    for code, cnt in sorted(turso_counts.items()):
        print(f"    {code}: {cnt}件")

    # 2. CSVファイル情報を取得
    print("\n[STEP 2] CSVファイル確認...")
    csv_info = get_csv_info()
    csv_total = sum(v["count"] for v in csv_info.values())
    print(f"  CSV合計: {csv_total}件 ({len(csv_info)}サービスコード)")

    # 3. 不足分を特定
    print("\n[STEP 3] 不足サービスコード特定...")
    missing = {}
    partial = {}
    for code, info in sorted(csv_info.items()):
        turso_cnt = turso_counts.get(code, 0)
        csv_cnt = info["count"]
        if turso_cnt == 0:
            missing[code] = info
            print(f"  [未登録] {code}: CSV {csv_cnt}件 → アップロード対象")
        elif turso_cnt < csv_cnt:
            diff = csv_cnt - turso_cnt
            partial[code] = {**info, "turso_count": turso_cnt, "diff": diff}
            print(f"  [不足] {code}: Turso {turso_cnt}件 / CSV {csv_cnt}件 (差分 {diff}件) → アップロード対象")
        else:
            print(f"  [完了] {code}: Turso {turso_cnt}件 / CSV {csv_cnt}件")

    # アップロード対象の合算
    missing_total = sum(v["count"] for v in missing.values())
    partial_total = sum(v["diff"] for v in partial.values())
    upload_target = missing_total + partial_total
    print(f"\n  アップロード対象: {len(missing)}件（未登録） + {len(partial)}件（不足） = 約{upload_target}件")

    if not missing and not partial:
        print("\n全サービスコードが既にアップロード済みです。")
        return

    # 4. CSVカラム名を取得（最初のCSVから）
    first_csv = list(csv_info.values())[0]["path"]
    with open(first_csv, "r", encoding="utf-8-sig") as f:
        csv_columns = csv.DictReader(f).fieldnames
    print(f"\nCSVカラム数: {len(csv_columns)}")

    # 5. アップロード実行
    print(f"\n[STEP 4] アップロード開始...")
    total_uploaded = 0
    total_errors = 0
    overall_start = time.time()

    # 未登録サービスコード（全件アップロード）
    for code, info in sorted(missing.items()):
        u, e = upload_csv(info["path"], csv_columns)
        total_uploaded += u
        total_errors += e

    # 不足サービスコード（INSERT OR IGNOREで重複防止、全件投入）
    for code, info in sorted(partial.items()):
        print(f"\n  注: {code} は既存{info['turso_count']}件あり、INSERT OR IGNOREで重複スキップ")
        u, e = upload_csv(info["path"], csv_columns)
        total_uploaded += u
        total_errors += e

    overall_elapsed = time.time() - overall_start

    # 6. 検証
    print(f"\n{'=' * 60}")
    print(f"アップロード結果")
    print(f"{'=' * 60}")
    print(f"  成功: {total_uploaded}件")
    print(f"  エラー: {total_errors}件")
    print(f"  所要時間: {overall_elapsed:.1f}秒")

    # 最終件数確認
    resp = execute_single("SELECT COUNT(*) FROM facilities")
    final_total = resp["results"][0]["response"]["result"]["rows"][0][0]["value"]
    print(f"\n  Before: {turso_total}件")
    print(f"  After:  {final_total}件")
    print(f"  増加:   {int(final_total) - turso_total}件")

    # サービスコード別最終件数
    print("\nサービスコード別（最終）:")
    resp = execute_single('SELECT "サービスコード", COUNT(*) FROM facilities GROUP BY "サービスコード" ORDER BY "サービスコード"')
    for row in resp["results"][0]["response"]["result"]["rows"]:
        code = row[0].get("value", "NULL") if row[0]["type"] != "null" else "NULL"
        cnt = row[1]["value"]
        print(f"  {code}: {cnt}件")

    print("\n完了しました。")


if __name__ == "__main__":
    main()
