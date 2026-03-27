"""
facilities_newの不足分を補完し、テーブルを切り替える
"""
import csv, os, sys, time, requests
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

sys.path.insert(0, str(Path(__file__).resolve().parent))
from turso_helpers import get_turso_config, get_headers, make_arg, compute_derived

TURSO_URL, TURSO_TOKEN = get_turso_config()
HEADERS = get_headers(TURSO_TOKEN)
MERGED_CSV = Path(__file__).resolve().parent.parent / "data" / "output" / "kaigo_scraping" / "kaigo_merged_20260326.csv"
DERIVED_COLUMNS = ["prefecture", "corp_type", "turnover_rate", "fulltime_ratio", "years_in_business"]
TABLE = "facilities_new"

def execute_sql(stmts, timeout=300):
    resp = requests.post(f"{TURSO_URL}/v2/pipeline", headers=HEADERS,
                         json={"requests": stmts}, timeout=timeout)
    if resp.status_code != 200:
        raise Exception(f"HTTP {resp.status_code}")
    return resp.json()

def execute_single(sql, timeout=300):
    return execute_sql([{"type": "execute", "stmt": {"sql": sql}}], timeout=timeout)

def main():
    print("=" * 60)
    print("facilities_new 不足分補完 + テーブル切替")
    print("=" * 60)

    # 1. 現在の件数
    r = execute_single(f"SELECT COUNT(*) FROM {TABLE}")
    current = int(r["results"][0]["response"]["result"]["rows"][0][0]["value"])
    print(f"\n  {TABLE} 現在: {current:,}件")
    print(f"  目標: 223,103件")
    print(f"  不足: {223103 - current:,}件")

    if current >= 223100:
        print("  既に十分な件数です。テーブル切替に進みます。")
    else:
        # 2. 既存キーを取得
        print("\n  既存キー取得中...")
        existing = set()
        offset = 0
        while True:
            r = execute_single(
                f'SELECT "事業所番号" || \'-\' || "サービスコード" FROM {TABLE} LIMIT 50000 OFFSET {offset}')
            rows = r["results"][0]["response"]["result"]["rows"]
            if not rows:
                break
            for row in rows:
                existing.add(row[0]["value"])
            offset += len(rows)
            sys.stdout.write(f"\r  {len(existing):,}件取得済み")
            sys.stdout.flush()
        print()

        # 3. 不足分をINSERT
        print("  不足分アップロード中...")
        with open(MERGED_CSV, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            all_cols = list(reader.fieldnames)
            insert_cols = all_cols + DERIVED_COLUMNS
            col_refs = ", ".join(f'"{c}"' for c in insert_cols)
            placeholders = ", ".join("?" * len(insert_cols))
            insert_sql = f'INSERT OR IGNORE INTO {TABLE} ({col_refs}) VALUES ({placeholders})'

            batch = []
            total = 0
            uploaded = 0
            skipped = 0
            errors = 0
            start = time.time()

            for row in reader:
                total += 1
                key = f"{row.get('事業所番号','').strip()}-{row.get('サービスコード','').strip()}"
                if key in existing:
                    skipped += 1
                else:
                    pref, ct, tr, fr, yib = compute_derived(row)
                    args = [make_arg(row.get(c, "") or None) for c in all_cols]
                    args.extend([make_arg(pref), make_arg(ct), make_arg(tr), make_arg(fr), make_arg(yib)])
                    batch.append({"type": "execute", "stmt": {"sql": insert_sql, "args": args}})

                if len(batch) >= 50:
                    try:
                        result = execute_sql(batch)
                        be = sum(1 for r in result.get("results", []) if "error" in r)
                        uploaded += len(batch) - be
                        errors += be
                    except Exception as e:
                        print(f"\n  バッチエラー: {e}")
                        errors += len(batch)
                    batch = []

                if total % 10000 == 0:
                    elapsed = time.time() - start
                    rate = total / elapsed if elapsed > 0 else 0
                    sys.stdout.write(
                        f"\r  {total:,}行 | 追加:{uploaded:,} スキップ:{skipped:,} エラー:{errors} | {rate:.0f}行/秒")
                    sys.stdout.flush()

            if batch:
                try:
                    result = execute_sql(batch)
                    be = sum(1 for r in result.get("results", []) if "error" in r)
                    uploaded += len(batch) - be
                    errors += be
                except:
                    errors += len(batch)

        print(f"\n  追加: {uploaded:,} / エラー: {errors}")

        # 再確認
        r = execute_single(f"SELECT COUNT(*) FROM {TABLE}")
        current = int(r["results"][0]["response"]["result"]["rows"][0][0]["value"])
        print(f"  {TABLE}: {current:,}件")

        if errors > 0 and current < 220000:
            print("  まだ不足があります。回線確認後にもう一度実行してください。")
            return

    # 4. テーブル切替
    print("\n  テーブル切替中...")
    try:
        execute_sql([
            {"type": "execute", "stmt": {"sql": "DROP TABLE IF EXISTS facilities"}},
            {"type": "execute", "stmt": {"sql": f"ALTER TABLE {TABLE} RENAME TO facilities"}},
        ], timeout=600)
        print("  完了: facilities_new → facilities")
    except Exception as e:
        print(f"  切替エラー: {e}")
        return

    # 5. インデックス
    print("  インデックス作成中...")
    for idx in [
        'CREATE INDEX IF NOT EXISTS idx_fac_jigyosho ON facilities("事業所番号")',
        'CREATE INDEX IF NOT EXISTS idx_fac_service ON facilities("サービスコード")',
        'CREATE INDEX IF NOT EXISTS idx_fac_pref ON facilities("prefecture")',
        'CREATE INDEX IF NOT EXISTS idx_fac_corp_type ON facilities("corp_type")',
        'CREATE INDEX IF NOT EXISTS idx_fac_corp_num ON facilities("法人番号")',
    ]:
        try:
            execute_single(idx, timeout=300)
        except Exception as e:
            print(f"  IDXエラー: {e}")
    print("  完了")

    r = execute_single("SELECT COUNT(*) FROM facilities")
    final = int(r["results"][0]["response"]["result"]["rows"][0][0]["value"])
    print(f"\n  最終件数: {final:,}")
    print("\n  次のステップ: python scripts/aggregate_to_cache.py")

if __name__ == "__main__":
    main()
