"""
介護求人データ統合v2:
1. kaigojob_data.csv（法人名・事業所名・サービス種別あり）
2. Google検索スクレイピング（旧+新）→ 電話番号・住所
3. URL突合で統合
4. Salesforce突合
5. ハローワーク従業員数補完
"""
import pandas as pd
import re
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')

BASE_DIR = Path(r'C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List')
OUTPUT_DIR = BASE_DIR / 'data/output/google_scraping'

# 入力ファイル
KAIGOJOB_DETAIL = Path(r'C:\Users\fuji1\OneDrive\デスクトップ\kaigojob_data.csv')
KAIGOJOB_URL = Path(r'C:\Users\fuji1\Downloads\kaigojob_with_search_url.csv')
GOOGLE_OLD = Path(r'C:\Users\fuji1\Downloads\google.com-から詳細をスクレイピングします--12--2026-03-09.csv')
GOOGLE_NEW = Path(r'C:\Users\fuji1\Downloads\google.com-から詳細をスクレイピングします--12--2026-03-09 (1).csv')

# SF
LEAD_FILE = BASE_DIR / 'data/output/Lead_20260305_115825.csv'
ACCOUNT_FILE = BASE_DIR / 'data/output/Account_20260305_115035.csv'
CONTRACT_FILE = BASE_DIR / 'data/output/contract_accounts_20260305_114315.csv'

# HW
HW_FILES = [
    Path(r'C:\Users\fuji1\OneDrive\デスクトップ\RCMEB002002_M100.csv'),
    Path(r'C:\Users\fuji1\OneDrive\デスクトップ\RCMEB002002_M100 (2).csv'),
    Path(r'C:\Users\fuji1\OneDrive\デスクトップ\RCMEB002002_M100 (3).csv'),
    Path(r'C:\Users\fuji1\OneDrive\デスクトップ\ハローワーク2025.02.25\RCMEB002002_M100.csv'),
    Path(r'C:\Users\fuji1\OneDrive\デスクトップ\ハローワーク2025.02.25\RCMEB002002_M100 (2).csv'),
    Path(r'C:\Users\fuji1\OneDrive\デスクトップ\ハローワーク2025.02.25\RCMEB002002_M100 (3).csv'),
    Path(r'C:\Users\fuji1\OneDrive\デスクトップ\ハローワーク2025.02.25\RCMEB002002_M100 (4).csv'),
    Path(r'C:\Users\fuji1\OneDrive\デスクトップ\ハローワーク2025.02.25\RCMEB002002_M100①.csv'),
]


def normalize_phone(phone_str):
    if not phone_str or pd.isna(phone_str):
        return None
    digits = re.sub(r'[^\d]', '', str(phone_str))
    if 10 <= len(digits) <= 11:
        return digits
    return None


def extract_phone_from_text(text):
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


def extract_address(val):
    if not val or pd.isna(val):
        return None
    text = re.sub(r'^所在地[：:]\s*', '', str(val)).strip()
    return text if text else None


def extract_city(addr):
    if not addr or pd.isna(addr):
        return ''
    addr = str(addr)
    addr = re.sub(r'〒?\d{3}[-\-]?\d{4}\s*', '', addr)
    addr = re.sub(r'^(東京都|北海道|(?:京都|大阪)府|.{2,3}県)', '', addr)
    match = re.match(r'(.+?[市区町村郡])', addr)
    return match.group(1).strip() if match else addr[:6].strip()


def normalize_name(name):
    if not name or pd.isna(name):
        return ''
    name = str(name).strip()
    name = name.translate(str.maketrans(
        'ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ０１２３４５６７８９',
        'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'
    ))
    name = re.sub(r'[\s　・\-\.\(\)（）【】「」\u3000]+', '', name)
    return name.lower()


def process_google_data(df_google):
    """Googleスクレイピングデータから電話番号・住所を抽出"""
    # 電話番号抽出（優先順位付き）
    df_google['phone'] = df_google['zh0Yff'].apply(
        lambda x: normalize_phone(re.search(r'電話番号[：:]\s*([\d\-]+)', str(x)).group(1))
        if pd.notna(x) and re.search(r'電話番号[：:]\s*([\d\-]+)', str(x)) else None
    )
    # pwimAb補完
    mask = df_google['phone'].isna()
    df_google.loc[mask, 'phone'] = df_google.loc[mask, 'pwimAb'].apply(
        lambda x: normalize_phone(re.match(r'([\d\-]+)', str(x)).group(1))
        if pd.notna(x) and re.match(r'([\d\-]+)', str(x)) else None
    )
    # フォールバック
    for col in ['YNk70c', 'zloOqf (2)', 'VwiC3b', 'VwiC3b (2)', 'VwiC3b (3)',
                'VwiC3b (4)', 'VwiC3b (5)', 'VwiC3b (6)', 'VwiC3b (7)']:
        mask = df_google['phone'].isna()
        if mask.sum() == 0:
            break
        extracted = df_google.loc[mask, col].apply(extract_phone_from_text)
        df_google.loc[mask, 'phone'] = extracted

    df_google['address'] = df_google['zloOqf'].apply(extract_address)
    df_google['facility_name_google'] = df_google['PZPZlf']

    return df_google[['Google検索URL', 'phone', 'address', 'facility_name_google']].copy()


def main():
    print("=" * 70)
    print("介護求人データ統合 v2（法人名補完+電話番号補完+SF突合）")
    print("=" * 70)

    # === Step 1: データ読み込み ===
    print("\n--- Step 1: データ読み込み ---")

    # 介護求人詳細データ（法人名あり）
    df_detail = pd.read_csv(KAIGOJOB_DETAIL, encoding='utf-8-sig')
    print(f"  kaigojob_data（法人名あり）: {len(df_detail)}件")

    # URL付きデータ
    df_url = pd.read_csv(KAIGOJOB_URL, encoding='utf-8-sig')
    print(f"  kaigojob_with_search_url: {len(df_url)}件")

    # Googleスクレイピング（旧+新統合）
    df_google_old = pd.read_csv(GOOGLE_OLD, encoding='utf-8-sig')
    df_google_new = pd.read_csv(GOOGLE_NEW, encoding='utf-8-sig')
    print(f"  Googleスクレイピング旧: {len(df_google_old)}件")
    print(f"  Googleスクレイピング新: {len(df_google_new)}件")

    # === Step 2: kaigojob_data と kaigojob_with_search_url をURL突合 ===
    print("\n--- Step 2: kaigojob詳細 × URL付きデータ URL突合 ---")
    df_merged = df_detail.merge(df_url[['URL', 'Google検索URL']], on='URL', how='left')
    url_matched = df_merged['Google検索URL'].notna().sum()
    print(f"  URL突合成功: {url_matched}件 / {len(df_detail)}件")

    # === Step 3: Google検索データ統合・電話番号抽出 ===
    print("\n--- Step 3: Googleデータから電話番号抽出 ---")
    # 新旧統合（新を優先、URLで重複除去）
    df_google_all = pd.concat([df_google_new, df_google_old], ignore_index=True)
    df_google_all = df_google_all.drop_duplicates(subset='Google検索URL', keep='first')
    print(f"  Google統合（重複除去後）: {len(df_google_all)}件")

    google_processed = process_google_data(df_google_all)
    phone_count = google_processed['phone'].notna().sum()
    print(f"  電話番号抽出成功: {phone_count}件")

    # === Step 4: メインデータにGoogle情報をURL突合 ===
    print("\n--- Step 4: メインデータ × Google情報 URL突合 ---")
    # Google検索URLで重複除去（phone付きを優先）
    google_processed['has_phone'] = google_processed['phone'].notna().astype(int)
    google_processed = google_processed.sort_values('has_phone', ascending=False)
    google_processed = google_processed.drop_duplicates(subset='Google検索URL', keep='first')
    google_processed = google_processed.drop(columns=['has_phone'])

    df_merged = df_merged.merge(
        google_processed, on='Google検索URL', how='left'
    )
    phone_matched = df_merged['phone'].notna().sum()
    print(f"  電話番号取得: {phone_matched}件 / {len(df_merged)}件 ({phone_matched/len(df_merged)*100:.1f}%)")

    # === Step 5: 電話番号で重複統合 ===
    print("\n--- Step 5: 電話番号ベース統合 ---")
    df_with_phone = df_merged[df_merged['phone'].notna()].copy()
    df_no_phone = df_merged[df_merged['phone'].isna()].copy()

    def aggregate_jobs(group):
        first = group.iloc[0].copy()
        if len(group) > 1:
            jobs = group['募集職種'].unique()
            first['募集職種'] = ' / '.join([j for j in jobs if pd.notna(j)])
            services = group['サービス種別'].unique()
            first['サービス種別'] = ' / '.join([s for s in services if pd.notna(s)])
            first['job_count'] = len(group)
        else:
            first['job_count'] = 1
        return first

    df_unique = df_with_phone.groupby('phone', group_keys=False).apply(
        aggregate_jobs, include_groups=False
    ).reset_index()
    # phoneカラムがindexに入るのでリネーム
    df_unique = df_unique.rename(columns={'index': 'phone'}) if 'index' in df_unique.columns else df_unique
    print(f"  電話番号あり: {len(df_with_phone)}件 → 統合後: {len(df_unique)}件")
    print(f"  電話番号なし: {len(df_no_phone)}件")

    # === Step 6: Salesforce突合 ===
    print("\n--- Step 6: Salesforce突合 ---")

    # SF電話番号インデックス
    df_lead = pd.read_csv(LEAD_FILE, encoding='utf-8-sig',
                          usecols=['Id', 'Phone', 'MobilePhone', 'Phone2__c', 'MobilePhone2__c'], dtype=str)
    df_account = pd.read_csv(ACCOUNT_FILE, encoding='utf-8-sig',
                             usecols=['Id', 'Phone', 'PersonMobilePhone', 'Phone2__c'], dtype=str)
    df_contract = pd.read_csv(CONTRACT_FILE, encoding='utf-8-sig', dtype=str)

    sf_phones = set()
    for col in ['Phone', 'MobilePhone', 'Phone2__c', 'MobilePhone2__c']:
        for p in df_lead[col].dropna():
            n = normalize_phone(p)
            if n: sf_phones.add(n)
    for col in ['Phone', 'PersonMobilePhone', 'Phone2__c']:
        for p in df_account[col].dropna():
            n = normalize_phone(p)
            if n: sf_phones.add(n)

    contract_phones = set()
    for p in df_contract['Phone'].dropna():
        n = normalize_phone(p)
        if n: contract_phones.add(n)

    contract_corp = set()
    for col in ['CorporateNumber__c', 'CorporateIdentificationNumber__c']:
        if col in df_contract.columns:
            for cn in df_contract[col].dropna():
                cn = str(cn).replace('.0', '').strip()
                if cn and len(cn) >= 10:
                    contract_corp.add(cn)

    print(f"  SF電話番号: {len(sf_phones)}件")
    print(f"  成約先電話番号: {len(contract_phones)}件")
    print(f"  成約先法人番号: {len(contract_corp)}件")

    # 突合
    results = []
    stats = {'contract_phone': 0, 'sf_existing': 0, 'new': 0}

    for _, row in df_unique.iterrows():
        phone = row.get('phone')
        match_type = 'NEW'

        if phone in contract_phones:
            match_type = 'CONTRACT_EXCLUDED'
            stats['contract_phone'] += 1
        elif phone in sf_phones:
            match_type = 'SF_EXISTING'
            stats['sf_existing'] += 1
        else:
            stats['new'] += 1

        result = row.to_dict()
        result['sf_match_type'] = match_type
        results.append(result)

    df_results = pd.DataFrame(results)

    # 法人番号で成約先追加除外
    # (kaigojob_dataには法人番号がないが、後でHW補完後に実施)

    print(f"\n  成約先除外（電話番号）: {stats['contract_phone']}件")
    print(f"  SF既存: {stats['sf_existing']}件")
    print(f"  新規リード候補: {stats['new']}件")

    # === Step 7: 出力 ===
    print("\n--- Step 7: 出力 ---")

    # 新規リード候補
    df_new = df_results[df_results['sf_match_type'] == 'NEW'].copy()
    new_file = OUTPUT_DIR / 'kaigojob_new_leads_v2_20260309.csv'
    df_new.to_csv(new_file, index=False, encoding='utf-8-sig')
    print(f"  新規リード候補: {new_file} ({len(df_new)}件)")

    # SF既存
    df_existing = df_results[df_results['sf_match_type'] == 'SF_EXISTING'].copy()
    existing_file = OUTPUT_DIR / 'kaigojob_sf_existing_v2_20260309.csv'
    df_existing.to_csv(existing_file, index=False, encoding='utf-8-sig')
    print(f"  SF既存: {existing_file} ({len(df_existing)}件)")

    # 成約先除外
    df_contract_ex = df_results[df_results['sf_match_type'] == 'CONTRACT_EXCLUDED'].copy()
    contract_file = OUTPUT_DIR / 'kaigojob_contract_excluded_v2_20260309.csv'
    df_contract_ex.to_csv(contract_file, index=False, encoding='utf-8-sig')
    print(f"  成約先除外: {contract_file} ({len(df_contract_ex)}件)")

    # 電話番号なし
    no_phone_file = OUTPUT_DIR / 'kaigojob_no_phone_v2_20260309.csv'
    df_no_phone.to_csv(no_phone_file, index=False, encoding='utf-8-sig')
    print(f"  電話番号なし: {no_phone_file} ({len(df_no_phone)}件)")

    # === サマリー ===
    print("\n" + "=" * 70)
    print("最終サマリー")
    print("=" * 70)
    print(f"  介護求人データ入力: {len(df_detail)}件")
    print(f"  電話番号取得成功: {phone_matched}件 ({phone_matched/len(df_detail)*100:.1f}%)")
    print(f"  電話番号ユニーク（統合後）: {len(df_unique)}件")
    print(f"  成約先除外: {stats['contract_phone']}件")
    print(f"  SF既存: {stats['sf_existing']}件")
    print(f"  ★ 新規リード候補: {stats['new']}件")
    print(f"  電話番号なし: {len(df_no_phone)}件")

    # 新規リードの法人名分布
    if len(df_new) > 0:
        print(f"\n--- 新規リード サービス種別分布（上位10） ---")
        svc = df_new['サービス種別'].value_counts().head(10)
        for s, c in svc.items():
            print(f"  {s}: {c}件")

        print(f"\n--- 新規リード サービス区分 ---")
        cat = df_new['サービス区分'].value_counts()
        for s, c in cat.items():
            print(f"  {s}: {c}件")

    return df_results


if __name__ == '__main__':
    main()
