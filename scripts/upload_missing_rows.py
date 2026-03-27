"""
不足レコードだけをTursoにINSERTするスクリプト
DROPせず、既存テーブルに追加のみ行う
再実行しても安全（INSERT OR IGNORE）
"""
import csv
import json
import os
import sys
import time
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

sys.path.insert(0, str(Path(__file__).resolve().parent))
from turso_helpers import (
    get_turso_config, get_headers, execute_sql as _execute_sql_raw,
    make_arg, compute_derived,
)

TURSO_URL, TURSO_TOKEN = get_turso_config()
HEADERS = get_headers(TURSO_TOKEN)
BATCH_SIZE = 50
MERGED_CSV = Path(__file__).resolve().parent.parent / "data" / "output" / "kaigo_scraping" / "kaigo_merged_20260326.csv"
DERIVED_COLUMNS = ["prefecture", "corp_type", "turnover_rate", "fulltime_ratio", "years_in_business"]


def execute_sql(stmts):
    import requests
    resp = requests.post(f"{TURSO_URL}/v2/pipeline", headers=HEADERS, json={"requests": stmts}, timeout=300)
    if resp.status_code != 200:
        raise Exception(f"HTTP {resp.status_code}: {resp.text[:200]}")
    return resp.json()


def execute_single(sql, args=None):
    stmt = {"type": "execute", "stmt": {"sql": sql}}
    if args:
        stmt["stmt"]["args"] = args
    return execute_sql([stmt])


def main():
    print("=" * 60)
    print("不足レコード補完アップロード")
    print("=" * 60)

    if not MERGED_CSV.exists():
        print(f"エラー: {MERGED_CSV} が見つかりません")
        sys.exit(1)

    # 1. Turso現在の件数と事業所番号リスト取得
    print("\n[1/3] Turso既存データ確認中...")
    result = execute_single("SELECT COUNT(*) FROM facilities")
    current_count = int(result["results"][0]["response"]["result"]["rows"][0][0]["value"])
    print(f"  現在の件数: {current_count:,}")

    # 既存事業所番号をページネーションで全取得
    existing = set()
    offset = 0
    while True:
        r = execute_single(f'SELECT "事業所番号" FROM facilities LIMIT 50000 OFFSET {offset}')
        rows = r["results"][0]["response"]["result"]["rows"]
        if not rows:
            break
        for row in rows:
            existing.add(row[0]["value"])
        offset += len(rows)
        sys.stdout.write(f"\r  既存事業所番号取得中: {len(existing):,}件")
        sys.stdout.flush()
    print(f"\n  既存ユニーク事業所番号: {len(existing):,}")

    # 2. CSVから不足分を特定してINSERT
    print(f"\n[2/3] 不足分をアップロード中...")
    with open(MERGED_CSV, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        all_cols = list(reader.fieldnames)
        insert_cols = all_cols + DERIVED_COLUMNS
        col_refs = ", ".join(f'"{c}"' for c in insert_cols)
        placeholders = ", ".join("?" * len(insert_cols))
        insert_sql = f'INSERT OR IGNORE INTO facilities ({col_refs}) VALUES ({placeholders})'

        batch = []
        total = 0
        uploaded = 0
        skipped = 0
        errors = 0
        start = time.time()

        for row in reader:
            total += 1
            key = row.get("事業所番号", "").strip()

            if key in existing:
                skipped += 1
            else:
                pref, ct, tr, fr, yib = compute_derived(row)
                args = [make_arg(row.get(c, "") or None) for c in all_cols]
                args.extend([make_arg(pref), make_arg(ct), make_arg(tr), make_arg(fr), make_arg(yib)])
                batch.append({"type": "execute", "stmt": {"sql": insert_sql, "args": args}})

            if len(batch) >= BATCH_SIZE:
                try:
                    result = execute_sql(batch)
                    batch_errors = sum(1 for r in result.get("results", []) if "error" in r)
                    uploaded += len(batch) - batch_errors
                    errors += batch_errors
                except Exception as e:
                    print(f"\n  バッチエラー: {e}")
                    errors += len(batch)
                batch = []

            if total % 10000 == 0:
                elapsed = time.time() - start
                rate = total / elapsed if elapsed > 0 else 0
                sys.stdout.write(
                    f"\r  {total:,}行 | 追加:{uploaded:,} スキップ:{skipped:,} エラー:{errors} | {rate:.0f}行/秒"
                )
                sys.stdout.flush()

        # 残りバッチ
        if batch:
            try:
                result = execute_sql(batch)
                batch_errors = sum(1 for r in result.get("results", []) if "error" in r)
                uploaded += len(batch) - batch_errors
                errors += batch_errors
            except Exception as e:
                errors += len(batch)

    elapsed = time.time() - start
    print(f"\n\n  完了:")
    print(f"    CSV総行数: {total:,}")
    print(f"    新規追加: {uploaded:,}")
    print(f"    スキップ(既存): {skipped:,}")
    print(f"    エラー: {errors}")
    print(f"    所要時間: {elapsed:.0f}秒")

    # 3. 最終件数確認
    print(f"\n[3/3] 最終件数確認...")
    r = execute_single("SELECT COUNT(*) FROM facilities")
    final = int(r["results"][0]["response"]["result"]["rows"][0][0]["value"])
    print(f"  Turso最終件数: {final:,}")
    print(f"  不足: {223107 - final:,}件")

    if final < 223107 and errors > 0:
        print(f"\n  ※まだ{223107 - final:,}件不足しています。もう一度実行してください。")


if __name__ == "__main__":
    main()
