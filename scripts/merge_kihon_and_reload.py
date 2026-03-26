"""
マージパイプライン: 既存CSVとdelta CSVを統合し、Tursoにアップロード後、集計を再実行する。

3ステップを1コマンドで実行:
  Step 1: 既存CSV + delta CSV をマージ（事業所番号でleft join）
  Step 2: マージ済みデータをTursoにアップロード（facilitiesテーブル再作成）
  Step 3: aggregate_to_cache.py の集計ロジックを再実行

使い方:
  $env:TURSO_DATABASE_URL = "https://cw-makimaki1006.aws-ap-northeast-1.turso.io"
  $env:TURSO_AUTH_TOKEN = "your-token"
  python scripts/merge_kihon_and_reload.py
"""

import csv
import glob
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import requests

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

# Windows環境でのUTF-8出力対応
sys.stdout.reconfigure(encoding="utf-8")

# ============================================================
# パス定義
# ============================================================
BASE_DIR = Path(__file__).resolve().parent.parent
SCRAPING_DIR = BASE_DIR / "data" / "output" / "kaigo_scraping"
EXISTING_CSV = SCRAPING_DIR / "kaigo_fast_20260324.csv"
DELTA_PATTERN = str(SCRAPING_DIR / "kihon_delta_*.csv")

# バッチサイズ（Turso pipeline制限を考慮）
BATCH_SIZE = 50

# ============================================================
# Turso接続設定（環境変数必須、フォールバックなし）
# ============================================================
TURSO_URL = None
TURSO_TOKEN = None
HEADERS = {}


def check_env():
    """環境変数チェック・Turso接続情報を初期化"""
    global TURSO_URL, TURSO_TOKEN, HEADERS
    try:
        TURSO_URL, TURSO_TOKEN = get_turso_config()
        HEADERS = get_headers(TURSO_TOKEN)
    except ValueError as e:
        print(f"エラー: {e}")
        print("  PowerShell: $env:TURSO_DATABASE_URL = 'https://...'")
        print("  PowerShell: $env:TURSO_AUTH_TOKEN = 'your-token'")
        sys.exit(1)


def init_headers():
    """後方互換性のためのスタブ（check_envで初期化済み）"""
    pass


# ============================================================
# Turso APIヘルパー（turso_helpers のラッパー）
# ============================================================
def execute_sql(statements: list[dict]) -> dict:
    """Turso HTTP API v2 pipeline でSQLを実行"""
    return _execute_sql_raw(TURSO_URL, HEADERS, statements)


def execute_single(sql: str, args: list = None) -> dict:
    """単一SQLを実行"""
    return _execute_single_raw(TURSO_URL, HEADERS, sql, args)


# ============================================================
# ユーティリティ関数（turso_helpersから再エクスポート済み）
# compute_turnover_rate, compute_fulltime_ratio, compute_years_in_business は
# compute_derived() 経由で使用
# ============================================================


def find_latest_delta() -> Path | None:
    """最新のdeltaファイルを検索"""
    delta_files = sorted(glob.glob(DELTA_PATTERN))
    if not delta_files:
        return None
    return Path(delta_files[-1])


# ============================================================
# Step 1: CSVマージ
# ============================================================
def step1_merge_csv() -> Path:
    """既存CSVとdelta CSVを事業所番号でleft joinしてマージ"""
    print("=" * 70)
    print("Step 1: CSV マージ（既存 + delta）")
    print("=" * 70)

    # 既存CSVの確認
    if not EXISTING_CSV.exists():
        print(f"エラー: 既存CSV が見つかりません: {EXISTING_CSV}")
        sys.exit(1)

    # 最新deltaファイルの検索
    delta_path = find_latest_delta()
    if delta_path is None:
        print(f"エラー: delta CSV が見つかりません: {DELTA_PATTERN}")
        sys.exit(1)

    print(f"  既存CSV: {EXISTING_CSV.name}")
    print(f"  delta CSV: {delta_path.name}")

    # delta CSV読み込み
    print("  delta CSV 読み込み中...")
    delta_map = {}
    delta_cols = []
    with open(delta_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        delta_cols = [c for c in reader.fieldnames if c not in ("事業所番号", "サービスコード", "都道府県コード")]
        for row in reader:
            key = row.get("事業所番号", "").strip()
            if key:
                delta_map[key] = {c: row.get(c, "") for c in delta_cols}
    print(f"  delta レコード数: {len(delta_map)}")
    print(f"  delta 新規カラム数: {len(delta_cols)}")
    print(f"  delta カラム: {', '.join(delta_cols[:10])}{'...' if len(delta_cols) > 10 else ''}")

    # 既存CSV読み込み + マージ + 出力
    today = datetime.now().strftime("%Y%m%d")
    output_path = SCRAPING_DIR / f"kaigo_merged_{today}.csv"

    print(f"  既存CSV 読み込み・マージ中...")
    matched = 0
    total = 0
    start_time = time.time()

    with open(EXISTING_CSV, "r", encoding="utf-8-sig") as fin:
        reader = csv.DictReader(fin)
        existing_cols = list(reader.fieldnames)

        # 出力カラム = 既存カラム + deltaの新規カラム
        output_cols = existing_cols + delta_cols

        with open(output_path, "w", encoding="utf-8-sig", newline="") as fout:
            writer = csv.DictWriter(fout, fieldnames=output_cols, extrasaction="ignore")
            writer.writeheader()

            for row in reader:
                total += 1
                key = row.get("事業所番号", "").strip()

                # delta側のデータがあればマージ
                if key in delta_map:
                    row.update(delta_map[key])
                    matched += 1
                else:
                    # deltaカラムは空で埋める
                    for c in delta_cols:
                        row[c] = ""

                writer.writerow(row)

                if total % 50000 == 0:
                    elapsed = time.time() - start_time
                    print(f"    {total:,} 行処理済み ({elapsed:.1f}秒)")

    elapsed = time.time() - start_time
    print(f"\n  マージ完了:")
    print(f"    総行数: {total:,}")
    print(f"    deltaマッチ数: {matched:,}")
    print(f"    カラム数: {len(output_cols)} (既存{len(existing_cols)} + delta{len(delta_cols)})")
    print(f"    出力ファイル: {output_path.name}")
    print(f"    処理時間: {elapsed:.1f}秒")

    return output_path


# ============================================================
# Step 2: Tursoアップロード
# ============================================================

# 既存CSVの78カラム定義
EXISTING_COLUMNS = [
    "事業所番号", "サービスコード", "サービス名", "都道府県コード", "都道府県名",
    "事業所名", "管理者名", "管理者職名", "代表者名", "代表者職名",
    "法人名", "法人番号", "電話番号", "FAX番号", "住所", "HP",
    "従業者_常勤", "従業者_非常勤", "従業者_合計", "定員", "事業開始日",
    "前年度採用数", "前年度退職数", "利用者総数", "利用者_都道府県平均",
    "経験10年以上割合", "サービス提供地域",
    "要介護1", "要介護2", "要介護3", "要介護4", "要介護5",
    "加算_処遇改善I", "加算_処遇改善II", "加算_処遇改善III", "加算_処遇改善IV",
    "加算_特定事業所I", "加算_特定事業所II", "加算_特定事業所III",
    "加算_特定事業所IV", "加算_特定事業所V",
    "加算_認知症ケアI", "加算_認知症ケアII", "加算_口腔連携", "加算_緊急時",
    "品質_BCP策定", "品質_ICT活用", "品質_第三者評価", "品質_損害賠償保険",
    "会計種類", "財務DL_事業活動計算書", "財務DL_資金収支計算書", "財務DL_貸借対照表",
    "賃金_職種1", "賃金_月額1", "賃金_平均年齢1", "賃金_平均勤続1",
    "賃金_職種2", "賃金_月額2", "賃金_平均年齢2", "賃金_平均勤続2",
    "賃金_職種3", "賃金_月額3", "賃金_平均年齢3", "賃金_平均勤続3",
    "賃金_職種4", "賃金_月額4", "賃金_平均年齢4", "賃金_平均勤続4",
    "賃金_職種5", "賃金_月額5", "賃金_平均年齢5", "賃金_平均勤続5",
    "行政処分日", "行政処分内容", "行政指導日", "行政指導内容",
    "スクレイピング日",
]

# deltaの新規カラム定義
DELTA_COLUMNS = [
    "介護職員_常勤", "介護職員_非常勤", "介護職員_合計",
    "看護職員_常勤", "看護職員_非常勤", "看護職員_合計",
    "生活相談員_常勤", "生活相談員_非常勤", "生活相談員_合計",
    "機能訓練指導員_常勤", "機能訓練指導員_非常勤", "機能訓練指導員_合計",
    "管理栄養士_常勤", "管理栄養士_非常勤", "管理栄養士_合計",
    "事務員_常勤", "事務員_非常勤", "事務員_合計",
    "介護福祉士数", "実務者研修数", "初任者研修数", "介護支援専門員数",
    "夜勤人数", "宿直人数",
    "認知症指導者研修数", "認知症リーダー研修数", "認知症実践者研修数",
    "加算_全項目",
]

# 派生カラム
DERIVED_COLUMNS = [
    "prefecture", "corp_type", "turnover_rate", "fulltime_ratio", "years_in_business",
]


def sanitize_col_name(col: str) -> str:
    """日本語カラム名をSQLカラム名に変換"""
    # 日本語カラムはそのまま使用（ダブルクォートでエスケープ）
    return f'"{col}"'


def build_create_table_sql(all_csv_cols: list[str]) -> str:
    """全カラムを含むCREATE TABLE文を構築"""
    # 整数型にするカラム
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
        '  "事業所番号" TEXT UNIQUE NOT NULL',
    ]

    # CSVカラム（事業所番号以外）
    for col in all_csv_cols:
        if col == "事業所番号":
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

    return "CREATE TABLE IF NOT EXISTS facilities (\n" + ",\n".join(lines) + "\n)"


def step2_upload_to_turso(merged_csv_path: Path):
    """マージ済みCSVをTursoにアップロード"""
    print("\n" + "=" * 70)
    print("Step 2: Turso アップロード")
    print("=" * 70)

    # CSVヘッダー読み取り
    with open(merged_csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        all_csv_cols = list(reader.fieldnames)

    print(f"  CSVカラム数: {len(all_csv_cols)}")

    # テーブル再作成（DROP + CREATE を同一バッチで実行）
    create_sql = build_create_table_sql(all_csv_cols)
    print("  既存テーブルをDROP + 新規テーブル作成中...")
    stmts = [
        {"type": "execute", "stmt": {"sql": "DROP TABLE IF EXISTS facilities"}},
        {"type": "execute", "stmt": {"sql": create_sql}},
    ]
    execute_sql(stmts)
    print("  テーブル作成完了")

    # インデックス作成
    indices = [
        'CREATE INDEX IF NOT EXISTS idx_facilities_phone ON facilities("電話番号")',
        'CREATE INDEX IF NOT EXISTS idx_facilities_corp_number ON facilities("法人番号")',
        'CREATE INDEX IF NOT EXISTS idx_facilities_prefecture ON facilities("prefecture")',
        'CREATE INDEX IF NOT EXISTS idx_facilities_corp_type ON facilities("corp_type")',
        'CREATE INDEX IF NOT EXISTS idx_facilities_service ON facilities("サービスコード")',
        'CREATE INDEX IF NOT EXISTS idx_facilities_pref_code ON facilities("都道府県コード")',
    ]
    for idx_sql in indices:
        execute_single(idx_sql)
    print("  インデックス作成完了")

    # INSERT文構築
    # 全カラム + 派生カラム
    insert_cols = all_csv_cols + DERIVED_COLUMNS
    col_refs = ", ".join(f'"{c}"' for c in insert_cols)
    placeholders = ", ".join("?" * len(insert_cols))
    insert_sql = f"INSERT OR REPLACE INTO facilities ({col_refs}) VALUES ({placeholders})"

    # データアップロード
    print(f"\n  データアップロード中...")
    total = 0
    uploaded = 0
    errors = 0
    start_time = time.time()

    with open(merged_csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        batch_rows = []

        for row in reader:
            total += 1
            batch_rows.append(row)

            if len(batch_rows) >= BATCH_SIZE:
                batch_ok, batch_err = _upload_batch(batch_rows, all_csv_cols, insert_sql)
                uploaded += batch_ok
                errors += batch_err
                batch_rows = []

                # 進捗表示（1000行ごと）
                if total % 1000 == 0:
                    elapsed = time.time() - start_time
                    rate = total / elapsed if elapsed > 0 else 0
                    sys.stdout.write(
                        f"\r    {total:>8,} 行処理済み "
                        f"| 成功: {uploaded:,} | エラー: {errors:,} "
                        f"| {rate:.0f} 行/秒"
                    )
                    sys.stdout.flush()

        # 残りのバッチ
        if batch_rows:
            batch_ok, batch_err = _upload_batch(batch_rows, all_csv_cols, insert_sql)
            uploaded += batch_ok
            errors += batch_err

    elapsed = time.time() - start_time
    print(f"\n\n  アップロード完了:")
    print(f"    総行数: {total:,}")
    print(f"    成功: {uploaded:,}")
    print(f"    エラー: {errors:,}")
    print(f"    処理時間: {elapsed:.1f}秒 ({total / elapsed:.0f} 行/秒)")

    # 検証
    _verify_upload()


def _upload_batch(rows: list[dict], all_csv_cols: list[str], insert_sql: str) -> tuple[int, int]:
    """バッチ単位でTursoにINSERT"""
    statements = []

    for row in rows:
        args = []

        # CSVカラムの値を順番に追加
        for col in all_csv_cols:
            val = row.get(col, "").strip() if row.get(col) else ""
            if not val:
                args.append(make_arg(None))
            else:
                # 整数カラムの変換
                int_val = parse_int(val) if _is_integer_col(col) else None
                if int_val is not None:
                    args.append(make_arg(int_val))
                else:
                    args.append(make_arg(val))

        # 派生カラム計算（turso_helpers.compute_derived を使用）
        prefecture, corp_type, turnover_rate, fulltime_ratio, years_in_business = compute_derived(row)

        args.append(make_arg(prefecture))
        args.append(make_arg(corp_type))
        args.append(make_arg(turnover_rate))
        args.append(make_arg(fulltime_ratio))
        args.append(make_arg(years_in_business))

        statements.append({
            "type": "execute",
            "stmt": {"sql": insert_sql, "args": args},
        })

    ok = 0
    err = 0
    try:
        result = execute_sql(statements)
        for i, res in enumerate(result.get("results", [])):
            if "error" in res:
                err += 1
                if err <= 5:
                    msg = res.get("error", {})
                    if isinstance(msg, dict):
                        msg = msg.get("message", str(msg))
                    print(f"\n    INSERT エラー: {str(msg)[:200]}")
            else:
                ok += 1
    except Exception as e:
        print(f"\n    バッチエラー: {e}")
        err += len(rows)

    return ok, err


# 整数型カラムのセット（高速判定用）
_INTEGER_COLS = {
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
}


def _is_integer_col(col: str) -> bool:
    """整数型カラムか判定"""
    return col in _INTEGER_COLS


def _verify_upload():
    """アップロード結果を検証"""
    print("\n  検証中...")

    # 件数確認
    result = execute_single("SELECT COUNT(*) as cnt FROM facilities")
    count = result["results"][0]["response"]["result"]["rows"][0][0]["value"]
    print(f"    facilities テーブル件数: {count}")

    # カラム数確認
    result = execute_single("SELECT * FROM facilities LIMIT 1")
    cols = result["results"][0]["response"]["result"]["cols"]
    print(f"    カラム数: {len(cols)}")

    # deltaカラムに値があるレコード数を確認
    result = execute_single('SELECT COUNT(*) FROM facilities WHERE "介護職員_合計" IS NOT NULL')
    delta_count = result["results"][0]["response"]["result"]["rows"][0][0]["value"]
    print(f"    deltaデータ有り件数 (介護職員_合計): {delta_count}")

    # 都道府県別上位5
    result = execute_single(
        'SELECT "prefecture", COUNT(*) as cnt FROM facilities '
        'WHERE "prefecture" IS NOT NULL '
        'GROUP BY "prefecture" ORDER BY cnt DESC LIMIT 5'
    )
    rows = result["results"][0]["response"]["result"]["rows"]
    print("    都道府県別 (上位5):")
    for row in rows:
        pref = row[0].get("value", "NULL")
        cnt = row[1].get("value", "0")
        print(f"      {pref}: {cnt} 件")


# ============================================================
# Step 3: 集計再実行
# ============================================================
def step3_reaggregate():
    """aggregate_to_cache.py の main() を呼び出して集計を再実行"""
    print("\n" + "=" * 70)
    print("Step 3: 集計再実行（aggregate_to_cache.py）")
    print("=" * 70)

    # aggregate_to_cache.py をインポートして実行
    scripts_dir = str(BASE_DIR / "scripts")
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)

    try:
        import aggregate_to_cache
        # モジュールを再読込（キャッシュ対策）
        import importlib
        importlib.reload(aggregate_to_cache)

        aggregate_to_cache.main()
    except ImportError as e:
        print(f"  aggregate_to_cache.py のインポートに失敗: {e}")
        print("  フォールバック: サブプロセスで実行します...")
        import subprocess
        result = subprocess.run(
            [sys.executable, str(BASE_DIR / "scripts" / "aggregate_to_cache.py")],
            capture_output=False,
            text=True,
            cwd=str(BASE_DIR),
            env={**os.environ, "TURSO_DATABASE_URL": TURSO_URL, "TURSO_AUTH_TOKEN": TURSO_TOKEN},
        )
        if result.returncode != 0:
            print(f"  集計スクリプトが異常終了しました (exit code: {result.returncode})")
            sys.exit(1)
    except Exception as e:
        print(f"  集計実行エラー: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


# ============================================================
# メイン処理
# ============================================================
def main():
    print("=" * 70)
    print("マージパイプライン: CSV統合 -> Tursoアップロード -> 集計再実行")
    print(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    pipeline_start = time.time()

    # 環境チェック
    check_env()
    init_headers()

    # Step 1: CSVマージ
    merged_csv = step1_merge_csv()

    # Step 2: Tursoアップロード
    step2_upload_to_turso(merged_csv)

    # Step 3: 集計再実行
    step3_reaggregate()

    # サマリー
    total_elapsed = time.time() - pipeline_start
    print("\n" + "=" * 70)
    print("パイプライン完了サマリー")
    print("=" * 70)
    print(f"  マージ済みCSV: {merged_csv.name}")
    print(f"  総処理時間: {total_elapsed:.1f}秒 ({total_elapsed / 60:.1f}分)")
    print("=" * 70)


if __name__ == "__main__":
    main()
