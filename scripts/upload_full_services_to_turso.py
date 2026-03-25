"""
完了済みサービスCSVをTurso DBにアップロードするスクリプト
対象: data/output/kaigo_scraping/by_service/ 配下の完了済みCSV

既存データはDELETE済み前提（テーブル構造はそのまま）
"""

import csv
import os
import sys
import time
import glob
import requests
from pathlib import Path

from turso_helpers import (
    get_turso_config,
    get_headers,
    execute_sql as _execute_sql_raw,
    execute_single as _execute_single_raw,
    make_arg,
    extract_prefecture,
    classify_corp_type,
    parse_int,
    compute_derived,
)

TURSO_URL, TURSO_TOKEN = get_turso_config()
HEADERS = get_headers(TURSO_TOKEN)
BATCH_SIZE = 50
CSV_DIR = str(Path(__file__).resolve().parent.parent / "data" / "output" / "kaigo_scraping" / "by_service")


def execute_sql(statements):
    return _execute_sql_raw(TURSO_URL, HEADERS, statements)


def execute_single(sql, args=None):
    return _execute_single_raw(TURSO_URL, HEADERS, sql, args)


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
