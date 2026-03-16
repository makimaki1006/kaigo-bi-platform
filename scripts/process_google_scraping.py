"""
Google検索スクレイピングデータから電話番号を抽出し、Salesforce突合用データを生成する。
"""
import pandas as pd
import re
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')

# === 設定 ===
INPUT_FILE = Path(r'C:\Users\fuji1\Downloads\google.com-から詳細をスクレイピングします--12--2026-03-09.csv')
OUTPUT_DIR = Path(r'C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List\data\output\google_scraping')
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def normalize_phone(phone_str):
    """電話番号を正規化（ハイフン除去、10-11桁チェック）"""
    if not phone_str or pd.isna(phone_str):
        return None
    phone_str = str(phone_str)
    # 数字とハイフンのみ抽出
    digits = re.sub(r'[^\d]', '', phone_str)
    if len(digits) >= 10 and len(digits) <= 11:
        return digits
    return None


def extract_phone_from_text(text):
    """テキストから電話番号パターンを抽出"""
    if not text or pd.isna(text):
        return None
    text = str(text)
    # 電話番号パターン: 0X-XXXX-XXXX, 0XX-XXX-XXXX, 0XXX-XX-XXXX 等
    patterns = [
        r'(?:Tel[：:]|電話番号[：:]|電話[：:]|TEL[：:])\s*([\d\-]+)',
        r'(\d{2,4}[-\-]\d{2,4}[-\-]\d{3,4})',
        r'(?:^|\s)(0\d{9,10})(?:\s|$|／)',
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            phone = normalize_phone(match.group(1))
            if phone:
                return phone
    return None


def extract_phone_from_zh0Yff(val):
    """zh0Yff カラムから電話番号抽出（「電話番号： XXX」形式）"""
    if not val or pd.isna(val):
        return None
    text = str(val)
    match = re.search(r'電話番号[：:]\s*([\d\-]+)', text)
    if match:
        return normalize_phone(match.group(1))
    return extract_phone_from_text(text)


def extract_phone_from_pwimAb(val):
    """pwimAb カラムから電話番号抽出（先頭に電話番号がある）"""
    if not val or pd.isna(val):
        return None
    text = str(val)
    match = re.match(r'([\d\-]+)', text)
    if match:
        return normalize_phone(match.group(1))
    return None


def extract_facility_name(row):
    """施設名を抽出（PZPZlf優先）"""
    if pd.notna(row.get('PZPZlf')):
        return str(row['PZPZlf']).strip()
    # pwimAbから施設名抽出（電話番号の後の部分）
    if pd.notna(row.get('pwimAb')):
        text = str(row['pwimAb'])
        parts = text.split('\n')
        if len(parts) > 1:
            name = parts[1].replace('、メインの電話番号', '').strip()
            if name:
                return name
    # Google検索URLからデコード
    if pd.notna(row.get('Google検索URL')):
        url = str(row['Google検索URL'])
        match = re.search(r'q=([^&]+)', url)
        if match:
            from urllib.parse import unquote
            query = unquote(match.group(1)).replace('+', ' ')
            # 「電話番号」を除去
            query = re.sub(r'\s*電話番号\s*', '', query)
            return query.strip()
    return None


def extract_address(row):
    """住所を抽出（zloOqf優先）"""
    if pd.notna(row.get('zloOqf')):
        text = str(row['zloOqf'])
        # 「所在地：」プレフィックスを除去
        text = re.sub(r'^所在地[：:]\s*', '', text)
        # 〒XXX-XXXX 以降を取得
        text = text.strip()
        return text
    return None


def extract_search_query_info(url):
    """Google検索URLからクエリ情報を抽出"""
    if not url or pd.isna(url):
        return None, None
    from urllib.parse import unquote
    match = re.search(r'q=([^&]+)', str(url))
    if match:
        query = unquote(match.group(1)).replace('+', ' ')
        # 「電話番号」を除去して施設名+地域を取得
        query = re.sub(r'\s*電話番号\s*', '', query).strip()
        return query, None
    return None, None


def main():
    print("=" * 60)
    print("Google検索スクレイピングデータ 電話番号抽出")
    print("=" * 60)

    df = pd.read_csv(INPUT_FILE, encoding='utf-8-sig')
    print(f"\n入力データ: {len(df)}件")

    # === Step 1: 主要カラム（zh0Yff）から電話番号抽出 ===
    print("\n--- Step 1: zh0Yff（電話番号フィールド）から抽出 ---")
    df['phone_extracted'] = df['zh0Yff'].apply(extract_phone_from_zh0Yff)
    step1_count = df['phone_extracted'].notna().sum()
    print(f"  抽出成功: {step1_count}件")

    # pwimAbからも補完
    mask_no_phone = df['phone_extracted'].isna()
    print(f"\n--- Step 1b: pwimAb（電話+施設名）から補完 ---")
    df.loc[mask_no_phone, 'phone_extracted'] = df.loc[mask_no_phone, 'pwimAb'].apply(extract_phone_from_pwimAb)
    step1b_count = df['phone_extracted'].notna().sum()
    print(f"  累計抽出: {step1b_count}件（+{step1b_count - step1_count}件）")

    # === Step 2: フォールバックカラムから電話番号抽出 ===
    fallback_cols = ['YNk70c', 'zloOqf (2)', 'VwiC3b', 'VwiC3b (2)', 'VwiC3b (3)',
                     'VwiC3b (4)', 'VwiC3b (5)', 'VwiC3b (6)', 'VwiC3b (7)']

    for col in fallback_cols:
        mask_no_phone = df['phone_extracted'].isna()
        remaining = mask_no_phone.sum()
        if remaining == 0:
            break
        extracted = df.loc[mask_no_phone, col].apply(extract_phone_from_text)
        new_count = extracted.notna().sum()
        if new_count > 0:
            df.loc[mask_no_phone, 'phone_extracted'] = extracted
            print(f"  {col}: +{new_count}件抽出")

    total_extracted = df['phone_extracted'].notna().sum()
    no_phone = df['phone_extracted'].isna().sum()
    print(f"\n=== 電話番号抽出結果 ===")
    print(f"  抽出成功: {total_extracted}件 / {len(df)}件 ({total_extracted/len(df)*100:.1f}%)")
    print(f"  抽出不可: {no_phone}件")

    # === Step 3: 施設名・住所の抽出 ===
    print("\n--- Step 3: 施設名・住所抽出 ---")
    df['facility_name'] = df.apply(extract_facility_name, axis=1)
    df['address'] = df.apply(extract_address, axis=1)

    name_count = df['facility_name'].notna().sum()
    addr_count = df['address'].notna().sum()
    print(f"  施設名あり: {name_count}件")
    print(f"  住所あり: {addr_count}件")

    # === Step 4: 電話番号ありレコードのみ抽出 ===
    df_with_phone = df[df['phone_extracted'].notna()].copy()
    print(f"\n=== 電話番号あり: {len(df_with_phone)}件 ===")

    # 電話番号の重複チェック
    dup_phones = df_with_phone['phone_extracted'].duplicated(keep=False)
    dup_count = dup_phones.sum()
    unique_phones = df_with_phone['phone_extracted'].nunique()
    print(f"  ユニーク電話番号: {unique_phones}件")
    print(f"  重複電話番号レコード: {dup_count}件")

    # 重複除去（最初のレコードを保持）
    df_unique = df_with_phone.drop_duplicates(subset='phone_extracted', keep='first').copy()
    print(f"  重複除去後: {len(df_unique)}件")

    # === Step 5: 出力用データ整形 ===
    output_df = pd.DataFrame({
        'facility_name': df_unique['facility_name'],
        'phone_normalized': df_unique['phone_extracted'],
        'address': df_unique['address'],
        'google_url': df_unique['Google検索URL'],
        'category': df_unique['rjxHPb'].apply(lambda x: str(x).split('\n')[-1].strip() if pd.notna(x) else ''),
    })

    # 電話番号をハイフン付きに戻す（表示用）
    def format_phone(digits):
        if not digits:
            return ''
        if len(digits) == 11:
            if digits.startswith('0120'):
                return f'{digits[:4]}-{digits[4:6]}-{digits[6:]}'
            elif digits[:2] in ('09', '08', '07'):
                return f'{digits[:3]}-{digits[3:7]}-{digits[7:]}'
            else:
                return f'{digits[:2]}-{digits[2:6]}-{digits[6:]}'
        elif len(digits) == 10:
            if digits[:2] == '03' or digits[:2] == '06':
                return f'{digits[:2]}-{digits[2:6]}-{digits[6:]}'
            else:
                return f'{digits[:4]}-{digits[4:6]}-{digits[6:]}'
        return digits

    output_df['phone_display'] = output_df['phone_normalized'].apply(format_phone)

    # 出力
    output_file = OUTPUT_DIR / 'google_scraping_cleaned_20260309.csv'
    output_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"\n出力ファイル: {output_file}")
    print(f"出力件数: {len(output_df)}件")

    # サンプル表示
    print("\n=== サンプル（先頭10件） ===")
    sample = output_df[['facility_name', 'phone_display', 'address', 'category']].head(10)
    for i, row in sample.iterrows():
        print(f"  {row['facility_name']} | {row['phone_display']} | {str(row['address'])[:40]} | {row['category']}")

    # 電話番号なしレコードも出力（確認用）
    df_no_phone = df[df['phone_extracted'].isna()].copy()
    if len(df_no_phone) > 0:
        no_phone_file = OUTPUT_DIR / 'google_scraping_no_phone_20260309.csv'
        df_no_phone[['Google検索URL', 'PZPZlf', 'zloOqf']].to_csv(
            no_phone_file, index=False, encoding='utf-8-sig')
        print(f"\n電話番号なしレコード: {no_phone_file} ({len(df_no_phone)}件)")

    return output_df


if __name__ == '__main__':
    main()
