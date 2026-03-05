"""
CSV列AE以降から日本の電話番号を抽出するスクリプト
"""
import pandas as pd
import re
import sys
from pathlib import Path

# Windows環境でのUTF-8出力設定
sys.stdout.reconfigure(encoding='utf-8')

def extract_japanese_phone_numbers(text):
    """日本の電話番号パターンを抽出"""
    if pd.isna(text) or not isinstance(text, str):
        return []

    # 日本の電話番号パターン（ハイフンあり・なし両対応）
    patterns = [
        r'0\d{1,4}-\d{1,4}-\d{3,4}',  # 0XX-XXXX-XXXX形式
        r'0\d{9,10}',  # 連続10-11桁
    ]

    found_numbers = []
    for pattern in patterns:
        matches = re.findall(pattern, text)
        found_numbers.extend(matches)

    return found_numbers

def main():
    csv_path = Path(r"C:\Users\fuji1\Downloads\doda_google_search_urls.csv")

    print("=" * 80)
    print("CSV読み込み中...")
    print("=" * 80)

    # dtype=str で全列を文字列として読み込み
    df = pd.read_csv(csv_path, encoding='utf-8-sig', dtype=str)

    total_columns = len(df.columns)
    print(f"\n総列数: {total_columns}")

    # 列AE（インデックス30）以降の列を特定
    start_index = 30  # 列AE（0始まりなのでA=0, B=1, ... AE=30）

    if start_index >= total_columns:
        print(f"\n警告: 列AE（インデックス{start_index}）は存在しません")
        return

    print(f"\n列AE（インデックス{start_index}）以降の列:")
    print("=" * 80)
    target_columns = df.columns[start_index:]
    for idx, col_name in enumerate(target_columns, start=start_index):
        print(f"  インデックス {idx}: {col_name}")

    # 電話番号抽出
    print("\n" + "=" * 80)
    print("電話番号抽出中...")
    print("=" * 80)

    phone_results = []

    for row_idx, row in df.iterrows():
        # 列AE以降のセルをスキャン
        for col_idx in range(start_index, total_columns):
            col_name = df.columns[col_idx]
            cell_value = row[col_name]

            phone_numbers = extract_japanese_phone_numbers(cell_value)

            if phone_numbers:
                for phone in phone_numbers:
                    phone_results.append({
                        'row': row_idx + 2,  # Excelの行番号（ヘッダー=1行目なので+2）
                        'column_index': col_idx,
                        'column_name': col_name,
                        'phone': phone
                    })

    # 結果サマリー
    print(f"\n検出された電話番号: {len(phone_results)} 件")
    print("=" * 80)

    if phone_results:
        # 行番号でグループ化して表示（最初の10行のみ）
        from collections import defaultdict
        by_row = defaultdict(list)

        for result in phone_results:
            by_row[result['row']].append(result)

        print("\n【最初の10行のサンプル】")
        for row_num in sorted(by_row.keys())[:10]:
            print(f"\n【行 {row_num}】")
            for result in by_row[row_num]:
                print(f"  列: {result['column_name']} (インデックス {result['column_index']})")
                print(f"  電話番号: {result['phone']}")

        if len(by_row) > 10:
            print(f"\n... （残り {len(by_row) - 10} 行省略）")

        # ユニークな電話番号の統計
        unique_phones = set(r['phone'] for r in phone_results)
        print("\n" + "=" * 80)
        print(f"ユニークな電話番号: {len(unique_phones)} 件")
        print("=" * 80)

        # 出現頻度の高い電話番号TOP20を表示
        from collections import Counter
        phone_counter = Counter(r['phone'] for r in phone_results)
        print("\n【出現頻度TOP20】")
        for phone, count in phone_counter.most_common(20):
            print(f"  {phone}: {count} 回出現")
    else:
        print("\n電話番号は見つかりませんでした")

if __name__ == '__main__':
    main()
