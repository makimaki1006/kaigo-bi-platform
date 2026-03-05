"""
doda Google検索結果から電話番号を抽出

dodaのGoogle検索結果CSVから企業情報と電話番号を抽出し、正規化・重複排除を行う。
"""

import sys
import re
import pandas as pd
from pathlib import Path

# Windows環境での文字化け防止
sys.stdout.reconfigure(encoding='utf-8')

def normalize_phone(phone_str):
    """
    電話番号を正規化（ハイフン除去、0始まり10-11桁のみ保持）

    Args:
        phone_str: 電話番号文字列

    Returns:
        str: 正規化された電話番号（10-11桁）、無効な場合は空文字
    """
    if not phone_str:
        return ''

    # ハイフン除去
    normalized = phone_str.replace('-', '').replace('(', '').replace(')', '').replace(' ', '')

    # 0始まり10-11桁のみ有効
    if re.match(r'^0\d{9,10}$', normalized):
        return normalized
    return ''

def extract_phones_from_text(text):
    """
    テキストから電話番号パターンを抽出

    Args:
        text: 検索対象テキスト

    Returns:
        list: 抽出された電話番号のリスト
    """
    if pd.isna(text) or not text:
        return []

    # 電話番号パターン: ハイフン区切り、または連続数字
    patterns = [
        r'0\d{1,4}-\d{1,4}-\d{3,4}',  # ハイフン区切り
        r'0\d{9,10}'                   # 連続10-11桁
    ]

    phones = []
    for pattern in patterns:
        matches = re.findall(pattern, str(text))
        phones.extend(matches)

    return phones

def main():
    """メイン処理"""

    # ファイルパス
    input_csv = Path(r'C:\Users\fuji1\Downloads\doda_google_search_urls.csv')

    print("=" * 80)
    print("doda Google検索結果 - 電話番号抽出スクリプト")
    print("=" * 80)
    print()

    # CSVファイル存在確認
    if not input_csv.exists():
        print(f"エラー: CSVファイルが見つかりません: {input_csv}")
        return

    # CSV読み込み（全列を文字列として読み込み）
    print(f"CSV読み込み中: {input_csv}")
    df = pd.read_csv(input_csv, encoding='utf-8-sig', dtype=str)
    print(f"読み込み完了: {len(df)} 行 × {len(df.columns)} 列")
    print()

    # 全カラム名を表示
    print("=" * 80)
    print("全カラム名（インデックス番号付き）")
    print("=" * 80)
    for idx, col in enumerate(df.columns):
        sample_value = df[col].dropna().iloc[0] if not df[col].dropna().empty else ''
        sample_preview = str(sample_value)[:50] + '...' if len(str(sample_value)) > 50 else str(sample_value)
        print(f"{idx:3d}: {col}")
        print(f"      サンプル: {sample_preview}")
    print()

    # データ抽出
    print("=" * 80)
    print("電話番号抽出処理開始")
    print("=" * 80)
    print()

    results = []

    for idx, row in df.iterrows():
        # 企業名（インデックス7: PageTitle-module_title__2RYke）
        company_name = row.iloc[7] if len(row) > 7 else ''

        # doda URL（インデックス0）
        doda_url = row.iloc[0] if len(row) > 0 else ''

        # インデックス30以降の全列から電話番号を抽出
        phones_found = []
        additional_info = []

        for col_idx in range(30, len(row)):
            cell_value = row.iloc[col_idx]

            if pd.notna(cell_value) and cell_value:
                # 電話番号抽出
                extracted_phones = extract_phones_from_text(cell_value)
                phones_found.extend(extracted_phones)

                # その他の有用情報（住所パターン検出）
                if '県' in str(cell_value) or '都' in str(cell_value) or '市' in str(cell_value):
                    additional_info.append(str(cell_value)[:100])

        # 電話番号が見つかった場合のみ記録
        if phones_found:
            # 正規化
            normalized_phones = [normalize_phone(p) for p in phones_found]
            normalized_phones = [p for p in normalized_phones if p]  # 空文字除去

            if normalized_phones:
                # 重複除去（行内）
                unique_phones = list(dict.fromkeys(normalized_phones))

                results.append({
                    'company_name': company_name,
                    'doda_url': doda_url,
                    'phones_raw': ', '.join(phones_found),
                    'phones_normalized': ', '.join(unique_phones),
                    'phone_count': len(unique_phones),
                    'additional_info': ' | '.join(additional_info[:3]) if additional_info else ''
                })

    # 結果をDataFrameに変換
    result_df = pd.DataFrame(results)

    print(f"電話番号抽出完了: {len(result_df)} 件")
    print()

    # 正規化電話番号で重複排除（最初の出現を保持）
    print("=" * 80)
    print("重複排除処理（正規化電話番号ベース）")
    print("=" * 80)
    print()

    # 各行の最初の正規化電話番号をキーとして重複排除
    result_df['first_phone'] = result_df['phones_normalized'].str.split(', ').str[0]
    dedup_df = result_df.drop_duplicates(subset=['first_phone'], keep='first')

    print(f"重複排除前: {len(result_df)} 件")
    print(f"重複排除後: {len(dedup_df)} 件")
    print(f"削除件数: {len(result_df) - len(dedup_df)} 件")
    print()

    # サマリー統計
    print("=" * 80)
    print("サマリー統計")
    print("=" * 80)
    print()

    total_phones = dedup_df['phone_count'].sum()
    avg_phones = dedup_df['phone_count'].mean()

    print(f"ユニーク企業数: {len(dedup_df)}")
    print(f"総電話番号数: {total_phones}")
    print(f"平均電話番号数/企業: {avg_phones:.2f}")
    print()

    # 電話番号数の分布
    phone_count_dist = dedup_df['phone_count'].value_counts().sort_index()
    print("電話番号数の分布:")
    for count, freq in phone_count_dist.items():
        print(f"  {count}件: {freq} 社")
    print()

    # サンプル表示（最初の10件）
    print("=" * 80)
    print("抽出結果サンプル（最初の10件）")
    print("=" * 80)
    print()

    for idx, row in dedup_df.head(10).iterrows():
        print(f"--- [{idx + 1}] ---")
        print(f"企業名: {row['company_name']}")
        print(f"doda URL: {row['doda_url']}")
        print(f"電話番号（元）: {row['phones_raw']}")
        print(f"電話番号（正規化）: {row['phones_normalized']}")
        print(f"電話番号件数: {row['phone_count']}")
        if row['additional_info']:
            print(f"追加情報: {row['additional_info']}")
        print()

    # 全件表示（コンソール確認用）
    print("=" * 80)
    print("全抽出結果")
    print("=" * 80)
    print()

    for idx, row in dedup_df.iterrows():
        print(f"{row['company_name']}\t{row['phones_normalized']}\t{row['doda_url']}")

    print()
    print("=" * 80)
    print("処理完了")
    print("=" * 80)

if __name__ == '__main__':
    main()
