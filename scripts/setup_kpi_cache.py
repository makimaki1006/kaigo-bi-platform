"""
Turso DBにkpi_cacheテーブルとfacilitiesテーブル用インデックスを作成するスクリプト

作成対象:
  - kpi_cache テーブル（KPIキャッシュ用）
  - facilities テーブルへのインデックス4件（prefecture, サービスコード, corp_type, 法人番号）
"""

import sys

from turso_helpers import (
    get_turso_config,
    get_headers,
    execute_sql as _execute_sql_raw,
    execute_single as _execute_single_raw,
)

# stdout をUTF-8に設定（Windows環境対応）
sys.stdout.reconfigure(encoding="utf-8")

# Turso接続設定（環境変数必須、フォールバックなし）
TURSO_URL, TURSO_TOKEN = get_turso_config()
HEADERS = get_headers(TURSO_TOKEN)


def execute_sql(statements):
    """Turso HTTP APIでSQLステートメントを実行する"""
    return _execute_sql_raw(TURSO_URL, HEADERS, statements)


def execute_single(sql):
    """単一SQLを実行して結果を返す"""
    return _execute_single_raw(TURSO_URL, HEADERS, sql)


def main():
    print("=" * 60)
    print("Turso DB: kpi_cache テーブル & インデックス作成")
    print("=" * 60)

    # ステップ1: kpi_cache テーブル作成
    print("\n[1/3] kpi_cache テーブルを作成中...")
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS kpi_cache (
            key TEXT NOT NULL,
            filter_key TEXT NOT NULL DEFAULT '',
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            row_count INTEGER DEFAULT 0,
            PRIMARY KEY (key, filter_key)
        )
    """
    try:
        result = execute_single(create_table_sql)
        # エラーチェック
        if "results" in result:
            for r in result["results"]:
                if r.get("type") == "error":
                    raise Exception(f"SQLエラー: {r['error']['message']}")
        print("  -> kpi_cache テーブル作成完了")
    except Exception as e:
        print(f"  -> エラー: {e}")
        return

    # ステップ2: facilities テーブル用インデックス作成
    print("\n[2/3] facilities テーブル用インデックスを作成中...")
    indexes = [
        ("idx_facilities_prefecture", 'CREATE INDEX IF NOT EXISTS idx_facilities_prefecture ON facilities(prefecture)'),
        ("idx_facilities_service_code", 'CREATE INDEX IF NOT EXISTS idx_facilities_service_code ON facilities("サービスコード")'),
        ("idx_facilities_corp_type", 'CREATE INDEX IF NOT EXISTS idx_facilities_corp_type ON facilities(corp_type)'),
        ("idx_facilities_corp_number", 'CREATE INDEX IF NOT EXISTS idx_facilities_corp_number ON facilities("法人番号")'),
    ]

    for idx_name, idx_sql in indexes:
        try:
            result = execute_single(idx_sql)
            if "results" in result:
                for r in result["results"]:
                    if r.get("type") == "error":
                        raise Exception(f"SQLエラー: {r['error']['message']}")
            print(f"  -> {idx_name} 作成完了")
        except Exception as e:
            print(f"  -> {idx_name} エラー: {e}")

    # ステップ3: 検証 - テーブル情報を取得
    print("\n[3/3] テーブル作成を検証中...")
    try:
        result = execute_single("PRAGMA table_info(kpi_cache)")
        if "results" in result:
            for r in result["results"]:
                if r.get("type") == "error":
                    raise Exception(f"SQLエラー: {r['error']['message']}")

            # カラム情報を表示
            response = r.get("response", {}).get("result", {})
            cols = response.get("cols", [])
            rows = response.get("rows", [])

            if rows:
                print(f"\n  kpi_cache テーブル構造 ({len(rows)} カラム):")
                print(f"  {'No':<4} {'カラム名':<16} {'型':<10} {'NOT NULL':<10} {'デフォルト':<16} {'PK'}")
                print(f"  {'-'*4} {'-'*16} {'-'*10} {'-'*10} {'-'*16} {'-'*4}")
                for row in rows:
                    values = [cell.get("value", "NULL") for cell in row]
                    cid = values[0]
                    name = values[1]
                    col_type = values[2]
                    notnull = "YES" if values[3] == "1" else "NO"
                    default_val = values[4] if values[4] != "NULL" else "-"
                    pk = "YES" if values[5] != "0" else "-"
                    print(f"  {cid:<4} {name:<16} {col_type:<10} {notnull:<10} {default_val:<16} {pk}")
            else:
                print("  -> 警告: テーブル情報が取得できませんでした")
    except Exception as e:
        print(f"  -> 検証エラー: {e}")

    # インデックス一覧も確認
    try:
        result = execute_single("SELECT name, tbl_name FROM sqlite_master WHERE type='index' AND (tbl_name='facilities' OR tbl_name='kpi_cache') ORDER BY tbl_name, name")
        if "results" in result:
            for r in result["results"]:
                if r.get("type") == "error":
                    raise Exception(f"SQLエラー: {r['error']['message']}")

            response = r.get("response", {}).get("result", {})
            rows = response.get("rows", [])

            if rows:
                print(f"\n  関連インデックス一覧 ({len(rows)} 件):")
                for row in rows:
                    values = [cell.get("value", "NULL") for cell in row]
                    print(f"    - {values[0]} (テーブル: {values[1]})")
    except Exception as e:
        print(f"  -> インデックス確認エラー: {e}")

    print("\n" + "=" * 60)
    print("セットアップ完了")
    print("=" * 60)


if __name__ == "__main__":
    main()
