"""
facilitiesテーブルを複合キー（事業所番号+サービスコード）で再構築
DROPタイムアウト対策: 新テーブルを別名で作成→旧テーブルDROP→リネーム
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
    get_turso_config, get_headers,
    make_arg, compute_derived,
)

TURSO_URL, TURSO_TOKEN = get_turso_config()
HEADERS = get_headers(TURSO_TOKEN)
BATCH_SIZE = 50
MERGED_CSV = Path(__file__).resolve().parent.parent / "data" / "output" / "kaigo_scraping" / "kaigo_merged_20260326.csv"
DERIVED_COLUMNS = ["prefecture", "corp_type", "turnover_rate", "fulltime_ratio", "years_in_business"]

import requests

def execute_sql(stmts, timeout=300):
    resp = requests.post(f"{TURSO_URL}/v2/pipeline", headers=HEADERS,
                         json={"requests": stmts}, timeout=timeout)
    if resp.status_code != 200:
        raise Exception(f"HTTP {resp.status_code}: {resp.text[:200]}")
    return resp.json()


def execute_single(sql, args=None, timeout=300):
    stmt = {"type": "execute", "stmt": {"sql": sql}}
    if args:
        stmt["stmt"]["args"] = args
    return execute_sql([stmt], timeout=timeout)


def build_create_table_sql(table_name, all_csv_cols):
    integer_cols = {
        "従業者_常勤", "従業者_非常勤", "従業者_合計", "定員",
        "前年度採用数", "前年度退職数", "利用者総数",
        "要介護1", "要介護2", "要介護3", "要介護4", "要介護5",
        "介護職員_常勤", "介護職員_非常勤", "介護職員_合計",
        "看護職員_常勤", "看護職員_非常勤", "看護職員_合計",
        "生活相談員_常勤", "生活相談員_非常勤", "生活相談員_合計",
        "機能訓練指導員_常勤", "機能訓練指導員_非常勤", "機能訓練指導員_合計",
        "管理栄養士_常勤", "管理栄養士_非常勤", "管理栄養士_合計",
        "事務員_常勤", "事務員_非常勤", "事務員_合計",
        "介護福祉士数", "実務者研修数", "初任者研修数", "介護支援専門員数",
        "夜勤人数", "宿直人数",
        "認知症指導者研修数", "認知症リーダー研修数", "認知症実践者研修数",
        "years_in_business",
    }
    real_cols = {"turnover_rate", "fulltime_ratio"}

    lines = [
        "  id INTEGER PRIMARY KEY AUTOINCREMENT",
        '  "事業所番号" TEXT NOT NULL',
        '  "サービスコード" TEXT NOT NULL',
    ]

    for col in all_csv_cols:
        if col in ("事業所番号", "サービスコード"):
            continue
        if col in integer_cols:
            col_type = "INTEGER"
        elif col in real_cols:
            col_type = "REAL"
        else:
            col_type = "TEXT"
        lines.append(f'  "{col}" {col_type}')

    # 派生カラム
    lines.append('  "prefecture" TEXT')
    lines.append('  "corp_type" TEXT')
    lines.append('  "turnover_rate" REAL')
    lines.append('  "fulltime_ratio" REAL')
    lines.append('  "years_in_business" INTEGER')

    # 複合UNIQUE制約
    lines.append('  UNIQUE("事業所番号", "サービスコード")')

    return f"CREATE TABLE IF NOT EXISTS {table_name} (\n" + ",\n".join(lines) + "\n)"


def main():
    print("=" * 60)
    print("facilitiesテーブル再構築（複合キー対応）")
    print("=" * 60)

    if not MERGED_CSV.exists():
        print(f"エラー: {MERGED_CSV} が見つかりません")
        sys.exit(1)

    # CSVヘッダー取得
    with open(MERGED_CSV, "r", encoding="utf-8-sig") as f:
        all_csv_cols = list(csv.DictReader(f).fieldnames)
    print(f"  CSVカラム数: {len(all_csv_cols)}")

    # Step 1: 新テーブルを別名で作成
    new_table = "facilities_new"
    print(f"\n[1/4] 新テーブル '{new_table}' を作成中...")
    execute_single(f"DROP TABLE IF EXISTS {new_table}", timeout=600)
    create_sql = build_create_table_sql(new_table, all_csv_cols)
    execute_single(create_sql, timeout=600)
    print("  作成完了")

    # Step 2: データをアップロード
    print(f"\n[2/4] データアップロード中...")
    insert_cols = all_csv_cols + DERIVED_COLUMNS
    col_refs = ", ".join(f'"{c}"' for c in insert_cols)
    placeholders = ", ".join("?" * len(insert_cols))
    insert_sql = f'INSERT OR IGNORE INTO {new_table} ({col_refs}) VALUES ({placeholders})'

    total = 0
    uploaded = 0
    errors = 0
    start = time.time()

    with open(MERGED_CSV, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        batch = []

        for row in reader:
            total += 1
            pref, ct, tr, fr, yib = compute_derived(row)
            args = [make_arg(row.get(c, "") or None) for c in all_csv_cols]
            args.extend([make_arg(pref), make_arg(ct), make_arg(tr), make_arg(fr), make_arg(yib)])
            batch.append({"type": "execute", "stmt": {"sql": insert_sql, "args": args}})

            if len(batch) >= BATCH_SIZE:
                try:
                    result = execute_sql(batch)
                    batch_errors = sum(1 for r in result.get("results", []) if "error" in r)
                    uploaded += len(batch) - batch_errors
                    errors += batch_errors
                except Exception as e:
                    print(f"\n  バッチエラー ({total}行目付近): {e}")
                    errors += len(batch)
                batch = []

            if total % 5000 == 0:
                elapsed = time.time() - start
                rate = total / elapsed if elapsed > 0 else 0
                remaining = (223107 - total) / rate / 60 if rate > 0 else 0
                sys.stdout.write(
                    f"\r  {total:,}/{223107:,} ({total/223107*100:.1f}%) | "
                    f"成功:{uploaded:,} エラー:{errors} | "
                    f"{rate:.0f}行/秒 | 残り{remaining:.1f}分"
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
    print(f"\n\n  アップロード完了:")
    print(f"    総行数: {total:,}")
    print(f"    成功: {uploaded:,}")
    print(f"    エラー: {errors}")
    print(f"    所要時間: {elapsed:.0f}秒 ({elapsed/60:.1f}分)")

    # エラーが多い場合は中断
    if errors > total * 0.1:
        print(f"\n  エラー率が10%超のため、テーブル切替を中止します。")
        print(f"  回線を確認して再実行してください。")
        return

    # Step 3: テーブル切替（旧DROP→新リネーム）
    print(f"\n[3/4] テーブル切替中...")
    try:
        execute_sql([
            {"type": "execute", "stmt": {"sql": "DROP TABLE IF EXISTS facilities"}},
            {"type": "execute", "stmt": {"sql": f"ALTER TABLE {new_table} RENAME TO facilities"}},
        ], timeout=600)
        print("  切替完了: facilities_new → facilities")
    except Exception as e:
        print(f"  切替エラー: {e}")
        print(f"  手動で実行: DROP TABLE facilities; ALTER TABLE {new_table} RENAME TO facilities;")
        return

    # Step 4: インデックス作成
    print(f"\n[4/4] インデックス作成中...")
    indices = [
        'CREATE INDEX IF NOT EXISTS idx_fac_jigyosho ON facilities("事業所番号")',
        'CREATE INDEX IF NOT EXISTS idx_fac_service ON facilities("サービスコード")',
        'CREATE INDEX IF NOT EXISTS idx_fac_pref ON facilities("prefecture")',
        'CREATE INDEX IF NOT EXISTS idx_fac_corp_type ON facilities("corp_type")',
        'CREATE INDEX IF NOT EXISTS idx_fac_corp_num ON facilities("法人番号")',
    ]
    for idx in indices:
        try:
            execute_single(idx, timeout=300)
        except Exception as e:
            print(f"  インデックスエラー: {e}")
    print("  インデックス作成完了")

    # 最終確認
    r = execute_single("SELECT COUNT(*) FROM facilities")
    final = int(r["results"][0]["response"]["result"]["rows"][0][0]["value"])
    print(f"\n  最終件数: {final:,}")
    print(f"  期待値: 223,103 (4件の真重複を除く)")

    if final >= 220000:
        print("\n  完了後、集計を実行してください:")
        print("  python scripts/aggregate_to_cache.py")


if __name__ == "__main__":
    main()
