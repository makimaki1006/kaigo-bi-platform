"""
Turso DB の facilities テーブルに含まれる不正な prefecture 値を修正するスクリプト。

原因: extract_prefecture() の正規表現 `.{2,3}県` がゴミ文字を含むアドレスで
      `-岡山県`, `i石川県`, `い岩手県`, `岡山県県`, `ｼ静岡県` 等を誤抽出していた。

修正方針:
  1. 不正な prefecture を持つレコードを検出
  2. 住所カラムから正しい都道府県を再抽出
  3. UPDATE で修正
"""

import sys

from turso_helpers import (
    get_turso_config,
    get_headers,
    execute_sql as _execute_sql_raw,
    execute_single as _execute_single_raw,
    make_arg,
    extract_prefecture,
    VALID_PREFECTURES_SET,
)

# Windows環境でのUTF-8出力対応
sys.stdout.reconfigure(encoding="utf-8")

# Turso接続設定（環境変数必須、フォールバックなし）
TURSO_URL, TURSO_TOKEN = get_turso_config()
HEADERS = get_headers(TURSO_TOKEN)


def execute_sql(statements):
    """Turso HTTP API v2 pipeline でSQLを実行"""
    return _execute_sql_raw(TURSO_URL, HEADERS, statements)


def execute_single(sql, args=None):
    """単一SQLを実行"""
    return _execute_single_raw(TURSO_URL, HEADERS, sql, args)


def make_text_arg(value):
    """Turso API用のテキスト引数を作成"""
    if value is None:
        return {"type": "null"}
    return {"type": "text", "value": str(value)}


def main():
    print("=" * 60)
    print("都道府県データ修正スクリプト")
    print("=" * 60)

    # ステップ1: 現在の prefecture 一覧を取得して不正値を検出
    print("\n[1/4] 現在の prefecture 一覧を取得...")
    result = execute_single(
        "SELECT DISTINCT prefecture FROM facilities WHERE prefecture IS NOT NULL ORDER BY prefecture"
    )
    rows = result["results"][0]["response"]["result"]["rows"]
    all_prefs = [row[0]["value"] for row in rows]

    invalid_prefs = [p for p in all_prefs if p not in VALID_PREFECTURES_SET]
    valid_count = len(all_prefs) - len(invalid_prefs)

    print(f"  有効な都道府県: {valid_count}件")
    print(f"  不正な都道府県: {len(invalid_prefs)}件")

    if not invalid_prefs:
        print("\n不正な都道府県は見つかりませんでした。修正不要です。")
        return

    print("\n  不正値一覧:")
    for p in invalid_prefs:
        print(f"    - '{p}'")

    # ステップ2: 不正 prefecture のレコードを取得して正しい値を特定
    print(f"\n[2/4] 不正レコードの住所から正しい都道府県を再抽出...")
    fix_map = {}  # {不正prefecture: 正しいprefecture}
    unfixable = {}  # {不正prefecture: 件数}

    for inv_pref in invalid_prefs:
        result = execute_single(
            "SELECT prefecture, 住所, COUNT(*) as cnt FROM facilities WHERE prefecture = ? GROUP BY prefecture, 住所",
            [make_text_arg(inv_pref)],
        )
        rows = result["results"][0]["response"]["result"]["rows"]
        total = 0
        correct_pref = None

        for row in rows:
            address = row[1]["value"] if row[1]["type"] != "null" else None
            cnt = int(row[2]["value"])
            total += cnt

            if address:
                extracted = extract_prefecture(address)
                if extracted and extracted != correct_pref:
                    if correct_pref is None:
                        correct_pref = extracted
                    # 複数の正しい都道府県が見つかった場合は住所ベースで個別対応

        if correct_pref:
            fix_map[inv_pref] = correct_pref
            print(f"    '{inv_pref}' -> '{correct_pref}' ({total}件)")
        else:
            unfixable[inv_pref] = total
            print(f"    '{inv_pref}' -> 住所から特定不可 ({total}件) -> NULLに設定")

    # ステップ3: 修正実行
    print(f"\n[3/4] 修正を実行...")
    total_fixed = 0

    statements = []
    for inv_pref, correct_pref in fix_map.items():
        statements.append({
            "type": "execute",
            "stmt": {
                "sql": "UPDATE facilities SET prefecture = ? WHERE prefecture = ?",
                "args": [make_text_arg(correct_pref), make_text_arg(inv_pref)],
            },
        })

    for inv_pref in unfixable:
        statements.append({
            "type": "execute",
            "stmt": {
                "sql": "UPDATE facilities SET prefecture = NULL WHERE prefecture = ?",
                "args": [make_text_arg(inv_pref)],
            },
        })

    if statements:
        # バッチで実行（Turso pipeline は複数ステートメント対応）
        result = execute_sql(statements)
        for i, res in enumerate(result["results"]):
            if "response" in res and "result" in res["response"]:
                affected = res["response"]["result"].get("affected_row_count", 0)
                total_fixed += affected

    print(f"  修正完了: {total_fixed}件のレコードを更新")

    # ステップ4: 修正後の検証
    print(f"\n[4/4] 修正後の検証...")
    result = execute_single(
        "SELECT DISTINCT prefecture FROM facilities WHERE prefecture IS NOT NULL ORDER BY prefecture"
    )
    rows = result["results"][0]["response"]["result"]["rows"]
    post_prefs = [row[0]["value"] for row in rows]
    post_invalid = [p for p in post_prefs if p not in VALID_PREFECTURES_SET]

    if post_invalid:
        print(f"  警告: まだ不正な都道府県が残っています: {post_invalid}")
    else:
        print(f"  検証OK: 全 {len(post_prefs)} 都道府県が有効です")

    # 件数サマリー
    result = execute_single("SELECT COUNT(*) FROM facilities")
    total_rows = result["results"][0]["response"]["result"]["rows"][0][0]["value"]
    result = execute_single("SELECT COUNT(*) FROM facilities WHERE prefecture IS NOT NULL")
    pref_rows = result["results"][0]["response"]["result"]["rows"][0][0]["value"]

    print(f"\n  全レコード数: {total_rows}")
    print(f"  prefecture有効: {pref_rows}")
    print(f"  prefecture NULL: {int(total_rows) - int(pref_rows)}")

    print("\n" + "=" * 60)
    print("修正完了")
    print("=" * 60)
    print("\n次のステップ: aggregate_to_cache.py を実行して kpi_cache を更新してください")
    print("  python scripts/aggregate_to_cache.py")


if __name__ == "__main__":
    main()
