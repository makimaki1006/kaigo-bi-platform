"""
介護求人データ（kaigojob）とGoogle検索スクレイピングデータをURL突合し、
電話番号等を補完した統合データを生成する。
"""
import pandas as pd
import re
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')

# === 設定 ===
KAIGOJOB_FILE = Path(r'C:\Users\fuji1\Downloads\kaigojob_with_search_url.csv')
GOOGLE_FILE = Path(r'C:\Users\fuji1\Downloads\google.com-から詳細をスクレイピングします--12--2026-03-09.csv')
OUTPUT_DIR = Path(r'C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List\data\output\google_scraping')
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def normalize_phone(phone_str):
    """電話番号を正規化（ハイフン除去、10-11桁チェック）"""
    if not phone_str or pd.isna(phone_str):
        return None
    digits = re.sub(r'[^\d]', '', str(phone_str))
    if 10 <= len(digits) <= 11:
        return digits
    return None


def extract_phone_from_text(text):
    """テキストから電話番号パターンを抽出"""
    if not text or pd.isna(text):
        return None
    text = str(text)
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
    """zh0Yff カラムから電話番号抽出"""
    if not val or pd.isna(val):
        return None
    match = re.search(r'電話番号[：:]\s*([\d\-]+)', str(val))
    if match:
        return normalize_phone(match.group(1))
    return extract_phone_from_text(val)


def extract_phone_from_pwimAb(val):
    """pwimAb カラムから電話番号抽出"""
    if not val or pd.isna(val):
        return None
    match = re.match(r'([\d\-]+)', str(val))
    if match:
        return normalize_phone(match.group(1))
    return None


def extract_address(val):
    """住所を抽出（zloOqf）"""
    if not val or pd.isna(val):
        return None
    text = re.sub(r'^所在地[：:]\s*', '', str(val)).strip()
    return text if text else None


def extract_category(val):
    """カテゴリ抽出（rjxHPb）"""
    if not val or pd.isna(val):
        return ''
    parts = str(val).split('\n')
    return parts[-1].strip()


def main():
    print("=" * 70)
    print("介護求人 × Google検索スクレイピング URL突合")
    print("=" * 70)

    # === データ読み込み ===
    df_kaigo = pd.read_csv(KAIGOJOB_FILE, encoding='utf-8-sig')
    df_google = pd.read_csv(GOOGLE_FILE, encoding='utf-8-sig')
    print(f"\n介護求人データ: {len(df_kaigo)}件")
    print(f"Google検索データ: {len(df_google)}件")

    # === Google検索URLの正規化 ===
    # URLの末尾スペースやクエリパラメータの差異を吸収
    df_kaigo['search_url_norm'] = df_kaigo['Google検索URL'].str.strip()
    df_google['search_url_norm'] = df_google['Google検索URL'].str.strip()

    # === Step 1: Google検索データから電話番号・住所・カテゴリ抽出 ===
    print("\n--- Step 1: Google検索データから情報抽出 ---")

    # 電話番号抽出（優先順位: zh0Yff → pwimAb → フォールバック）
    df_google['phone'] = df_google['zh0Yff'].apply(extract_phone_from_zh0Yff)

    mask = df_google['phone'].isna()
    df_google.loc[mask, 'phone'] = df_google.loc[mask, 'pwimAb'].apply(extract_phone_from_pwimAb)

    fallback_cols = ['YNk70c', 'zloOqf (2)', 'VwiC3b', 'VwiC3b (2)', 'VwiC3b (3)',
                     'VwiC3b (4)', 'VwiC3b (5)', 'VwiC3b (6)', 'VwiC3b (7)']
    for col in fallback_cols:
        mask = df_google['phone'].isna()
        if mask.sum() == 0:
            break
        extracted = df_google.loc[mask, col].apply(extract_phone_from_text)
        new_count = extracted.notna().sum()
        if new_count > 0:
            df_google.loc[mask, 'phone'] = extracted

    phone_count = df_google['phone'].notna().sum()
    print(f"  電話番号抽出: {phone_count}件 / {len(df_google)}件")

    # 施設名（PZPZlf）
    df_google['google_facility_name'] = df_google['PZPZlf']
    # 住所
    df_google['google_address'] = df_google['zloOqf'].apply(extract_address)
    # カテゴリ
    df_google['google_category'] = df_google['rjxHPb'].apply(extract_category)

    # 突合用カラムだけ残す
    google_merge = df_google[['search_url_norm', 'phone', 'google_facility_name',
                               'google_address', 'google_category']].copy()

    # Google側のURL重複を除去（最初の有効レコード優先）
    # phone付きを優先
    google_merge['has_phone'] = google_merge['phone'].notna().astype(int)
    google_merge = google_merge.sort_values('has_phone', ascending=False)
    google_merge = google_merge.drop_duplicates(subset='search_url_norm', keep='first')
    google_merge = google_merge.drop(columns=['has_phone'])
    print(f"  Google側ユニークURL: {len(google_merge)}件")

    # === Step 2: URL突合 ===
    print("\n--- Step 2: URL突合 ---")
    df_merged = df_kaigo.merge(google_merge, on='search_url_norm', how='left')

    matched = df_merged['phone'].notna().sum()
    google_matched = df_merged['google_facility_name'].notna().sum()
    print(f"  URL突合成功（Google情報あり）: {google_matched}件 / {len(df_kaigo)}件")
    print(f"  電話番号取得: {matched}件 / {len(df_kaigo)}件")
    print(f"  電話番号なし: {len(df_kaigo) - matched}件")

    # === Step 3: 統合データ整形 ===
    print("\n--- Step 3: 統合データ整形 ---")

    # 勤務地から都道府県抽出（タイトルから）
    def extract_prefecture(title):
        if not title or pd.isna(title):
            return ''
        match = re.search(r'/(東京都|北海道|(?:京都|大阪)府|.{2,3}県)/', str(title))
        if match:
            return match.group(1)
        return ''

    df_merged['prefecture'] = df_merged['タイトル'].apply(extract_prefecture)

    # 施設名抽出（タイトルから施設名部分を取得）
    def extract_facility_from_title(title):
        if not title or pd.isna(title):
            return ''
        parts = str(title).split('/')
        if len(parts) >= 3:
            return parts[2].strip()
        return ''

    df_merged['facility_name_from_title'] = df_merged['タイトル'].apply(extract_facility_from_title)

    # 職種抽出
    def extract_job_type(title):
        if not title or pd.isna(title):
            return ''
        parts = str(title).split('/')
        if len(parts) >= 1:
            return parts[0].strip()
        return ''

    df_merged['job_type'] = df_merged['タイトル'].apply(extract_job_type)

    # 出力用DataFrame
    output_df = pd.DataFrame({
        'company_name': df_merged['企業・施設名'],
        'facility_name': df_merged['facility_name_from_title'],
        'google_facility_name': df_merged['google_facility_name'],
        'job_type': df_merged['job_type'],
        'salary': df_merged['給与'],
        'employment_type': df_merged['雇用形態'],
        'location': df_merged['勤務地'],
        'prefecture': df_merged['prefecture'],
        'phone_normalized': df_merged['phone'],
        'address': df_merged['google_address'],
        'category': df_merged['google_category'],
        'kaigojob_url': df_merged['URL'],
        'google_search_url': df_merged['Google検索URL'],
    })

    # === Step 4: 電話番号ありデータの重複整理 ===
    print("\n--- Step 4: 重複整理 ---")
    df_with_phone = output_df[output_df['phone_normalized'].notna()].copy()
    df_no_phone = output_df[output_df['phone_normalized'].isna()].copy()

    # 同一電話番号の重複（同じ施設の複数求人）
    dup_phones = df_with_phone['phone_normalized'].duplicated(keep=False)
    print(f"  電話番号あり: {len(df_with_phone)}件")
    print(f"  うち電話番号重複: {dup_phones.sum()}件")
    print(f"  ユニーク電話番号: {df_with_phone['phone_normalized'].nunique()}件")

    # 同一電話番号のレコードを統合（求人情報はまとめる）
    # 一旦、電話番号でグループ化して追加求人情報を集約
    def aggregate_jobs(group):
        first = group.iloc[0].copy()
        if len(group) > 1:
            job_types = group['job_type'].unique()
            first['job_type'] = ' / '.join([j for j in job_types if j])
            salaries = group['salary'].dropna()
            if len(salaries) > 0:
                first['salary'] = f"{int(salaries.min())}～{int(salaries.max())}" if salaries.min() != salaries.max() else str(int(salaries.iloc[0]))
            first['job_count'] = len(group)
        else:
            first['job_count'] = 1
        return first

    df_aggregated = df_with_phone.groupby('phone_normalized', group_keys=False).apply(aggregate_jobs).reset_index(drop=True)
    print(f"  電話番号ベース統合後: {len(df_aggregated)}件")

    # === Step 5: 出力 ===
    # 全データ出力（電話あり、統合済み）
    output_file = OUTPUT_DIR / 'kaigojob_merged_20260309.csv'
    df_aggregated.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"\n出力ファイル（電話番号あり・統合済み）: {output_file}")
    print(f"出力件数: {len(df_aggregated)}件")

    # 電話番号なしも出力
    no_phone_file = OUTPUT_DIR / 'kaigojob_no_phone_20260309.csv'
    df_no_phone.to_csv(no_phone_file, index=False, encoding='utf-8-sig')
    print(f"\n電話番号なしレコード: {no_phone_file} ({len(df_no_phone)}件)")

    # Salesforce突合用（電話番号のみ抽出）
    sf_match_file = OUTPUT_DIR / 'kaigojob_phones_for_sf_match_20260309.csv'
    df_aggregated[['company_name', 'facility_name', 'phone_normalized', 'address', 'location',
                    'prefecture', 'job_type', 'salary', 'employment_type', 'category',
                    'kaigojob_url', 'job_count']].to_csv(
        sf_match_file, index=False, encoding='utf-8-sig')
    print(f"\nSalesforce突合用: {sf_match_file}")

    # === サマリー ===
    print("\n" + "=" * 70)
    print("サマリー")
    print("=" * 70)
    print(f"  介護求人データ入力: {len(df_kaigo)}件")
    print(f"  Google検索突合成功: {google_matched}件 ({google_matched/len(df_kaigo)*100:.1f}%)")
    print(f"  電話番号取得成功: {matched}件 ({matched/len(df_kaigo)*100:.1f}%)")
    print(f"  電話番号ユニーク（統合後）: {len(df_aggregated)}件")
    print(f"  電話番号なし: {len(df_no_phone)}件")

    # 職種分布
    print("\n--- 職種分布（上位10） ---")
    job_dist = df_aggregated['job_type'].value_counts().head(10)
    for job, cnt in job_dist.items():
        print(f"  {job}: {cnt}件")

    # カテゴリ分布
    print("\n--- カテゴリ分布（上位10） ---")
    cat_dist = df_aggregated['category'].value_counts().head(10)
    for cat, cnt in cat_dist.items():
        if cat:
            print(f"  {cat}: {cnt}件")

    # 都道府県分布
    print("\n--- 都道府県分布（上位10） ---")
    pref_dist = df_aggregated['prefecture'].value_counts().head(10)
    for pref, cnt in pref_dist.items():
        if pref:
            print(f"  {pref}: {cnt}件")

    return df_aggregated


if __name__ == '__main__':
    main()
