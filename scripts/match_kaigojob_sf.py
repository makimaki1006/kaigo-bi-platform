"""
介護求人データをSalesforce Lead/Account/Contactと電話番号で突合し、
新規・既存を分類する。
"""
import pandas as pd
import re
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')

# === 設定 ===
BASE_DIR = Path(r'C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List')
KAIGOJOB_FILE = BASE_DIR / 'data/output/google_scraping/kaigojob_merged_20260309.csv'
LEAD_FILE = BASE_DIR / 'data/output/Lead_20260305_115825.csv'
ACCOUNT_FILE = BASE_DIR / 'data/output/Account_20260305_115035.csv'
CONTRACT_FILE = BASE_DIR / 'data/output/contract_accounts_20260305_114315.csv'
OUTPUT_DIR = BASE_DIR / 'data/output/google_scraping'


def normalize_phone(phone_str):
    """電話番号正規化"""
    if not phone_str or pd.isna(phone_str):
        return None
    digits = re.sub(r'[^\d]', '', str(phone_str))
    if 10 <= len(digits) <= 11:
        return digits
    return None


def build_phone_index(df, phone_columns, id_col='Id'):
    """電話番号→IDのインデックスを構築"""
    phone_to_ids = {}
    for col in phone_columns:
        if col not in df.columns:
            continue
        for idx, row in df[[id_col, col]].dropna(subset=[col]).iterrows():
            phone = normalize_phone(row[col])
            if phone:
                if phone not in phone_to_ids:
                    phone_to_ids[phone] = set()
                phone_to_ids[phone].add(row[id_col])
    return phone_to_ids


def main():
    print("=" * 70)
    print("介護求人データ × Salesforce 電話番号突合")
    print("=" * 70)

    # === 介護求人データ読み込み ===
    df_kaigo = pd.read_csv(KAIGOJOB_FILE, encoding='utf-8-sig', dtype={'phone_normalized': str})
    # 先頭0が欠落している場合は復元
    def fix_phone_leading_zero(p):
        if pd.isna(p) or not p:
            return None
        p = str(p).replace('.0', '')
        if p and p[0] != '0':
            p = '0' + p
        digits = re.sub(r'[^\d]', '', p)
        if 10 <= len(digits) <= 11:
            return digits
        return None
    df_kaigo['phone_normalized'] = df_kaigo['phone_normalized'].apply(fix_phone_leading_zero)
    print(f"\n介護求人データ: {len(df_kaigo)}件（電話番号あり）")
    print(f"  電話番号有効: {df_kaigo['phone_normalized'].notna().sum()}件")

    # === 成約先電話番号読み込み（除外用） ===
    print("\n--- 成約先データ読み込み ---")
    df_contract = pd.read_csv(CONTRACT_FILE, encoding='utf-8-sig')
    contract_phones = set()
    for col in ['Phone']:
        for phone in df_contract[col].dropna():
            p = normalize_phone(phone)
            if p:
                contract_phones.add(p)
    print(f"  成約先電話番号: {len(contract_phones)}件")

    # === Lead電話番号インデックス構築 ===
    print("\n--- Leadデータ読み込み ---")
    lead_phone_cols = ['Phone', 'MobilePhone', 'Phone2__c', 'MobilePhone2__c']
    df_lead = pd.read_csv(LEAD_FILE, encoding='utf-8-sig',
                          usecols=['Id', 'Phone', 'MobilePhone', 'Phone2__c', 'MobilePhone2__c',
                                   'Company', 'Status', 'IsConverted'],
                          dtype=str)
    print(f"  Lead件数: {len(df_lead)}")
    lead_phone_index = build_phone_index(df_lead, lead_phone_cols)
    print(f"  Lead電話番号インデックス: {len(lead_phone_index)}件")

    # === Account電話番号インデックス構築 ===
    print("\n--- Accountデータ読み込み ---")
    account_phone_cols = ['Phone', 'PersonMobilePhone', 'Phone2__c']
    df_account = pd.read_csv(ACCOUNT_FILE, encoding='utf-8-sig',
                             usecols=['Id', 'Phone', 'PersonMobilePhone', 'Phone2__c', 'Name'],
                             dtype=str)
    print(f"  Account件数: {len(df_account)}")
    account_phone_index = build_phone_index(df_account, account_phone_cols)
    print(f"  Account電話番号インデックス: {len(account_phone_index)}件")

    # === 突合処理 ===
    print("\n--- 突合処理 ---")
    results = []
    stats = {
        'contract_excluded': 0,
        'lead_match': 0,
        'account_match': 0,
        'both_match': 0,
        'new_lead': 0,
        'no_phone': 0,
    }

    for idx, row in df_kaigo.iterrows():
        phone = normalize_phone(row.get('phone_normalized'))
        if not phone:
            stats['no_phone'] += 1
            continue

        # 成約先チェック
        if phone in contract_phones:
            stats['contract_excluded'] += 1
            results.append({
                **row.to_dict(),
                'match_type': 'CONTRACT_EXCLUDED',
                'lead_ids': '',
                'account_ids': '',
            })
            continue

        lead_ids = lead_phone_index.get(phone, set())
        account_ids = account_phone_index.get(phone, set())

        if lead_ids and account_ids:
            match_type = 'BOTH'
            stats['both_match'] += 1
        elif lead_ids:
            match_type = 'LEAD'
            stats['lead_match'] += 1
        elif account_ids:
            match_type = 'ACCOUNT'
            stats['account_match'] += 1
        else:
            match_type = 'NEW'
            stats['new_lead'] += 1

        results.append({
            **row.to_dict(),
            'match_type': match_type,
            'lead_ids': ';'.join(lead_ids) if lead_ids else '',
            'account_ids': ';'.join(account_ids) if account_ids else '',
        })

    df_results = pd.DataFrame(results)

    # === 結果サマリー ===
    print("\n" + "=" * 70)
    print("突合結果サマリー")
    print("=" * 70)
    print(f"  入力件数: {len(df_kaigo)}件")
    print(f"  成約先除外: {stats['contract_excluded']}件 ⚠️")
    print(f"  Lead一致: {stats['lead_match']}件")
    print(f"  Account一致: {stats['account_match']}件")
    print(f"  Lead+Account両方一致: {stats['both_match']}件")
    total_existing = stats['lead_match'] + stats['account_match'] + stats['both_match']
    print(f"  既存一致合計: {total_existing}件")
    print(f"  ★ 新規リード候補: {stats['new_lead']}件")

    # === ファイル出力 ===
    # 新規リード候補
    df_new = df_results[df_results['match_type'] == 'NEW'].copy()
    new_file = OUTPUT_DIR / 'kaigojob_new_leads_20260309.csv'
    df_new.to_csv(new_file, index=False, encoding='utf-8-sig')
    print(f"\n新規リード候補: {new_file} ({len(df_new)}件)")

    # 既存Lead一致
    df_lead_match = df_results[df_results['match_type'].isin(['LEAD', 'BOTH'])].copy()
    lead_match_file = OUTPUT_DIR / 'kaigojob_lead_match_20260309.csv'
    df_lead_match.to_csv(lead_match_file, index=False, encoding='utf-8-sig')
    print(f"既存Lead一致: {lead_match_file} ({len(df_lead_match)}件)")

    # 既存Account一致
    df_account_match = df_results[df_results['match_type'] == 'ACCOUNT'].copy()
    account_match_file = OUTPUT_DIR / 'kaigojob_account_match_20260309.csv'
    df_account_match.to_csv(account_match_file, index=False, encoding='utf-8-sig')
    print(f"既存Account一致: {account_match_file} ({len(df_account_match)}件)")

    # 成約先除外
    df_contract_excluded = df_results[df_results['match_type'] == 'CONTRACT_EXCLUDED'].copy()
    contract_file = OUTPUT_DIR / 'kaigojob_contract_excluded_20260309.csv'
    df_contract_excluded.to_csv(contract_file, index=False, encoding='utf-8-sig')
    print(f"成約先除外: {contract_file} ({len(df_contract_excluded)}件)")

    # 全結果
    all_file = OUTPUT_DIR / 'kaigojob_sf_match_results_20260309.csv'
    df_results.to_csv(all_file, index=False, encoding='utf-8-sig')
    print(f"全結果: {all_file} ({len(df_results)}件)")

    # === 新規リードの詳細 ===
    if len(df_new) > 0:
        print(f"\n--- 新規リード候補の詳細 ---")
        print(f"  都道府県分布:")
        pref_dist = df_new['prefecture'].value_counts().head(10)
        for pref, cnt in pref_dist.items():
            if pref:
                print(f"    {pref}: {cnt}件")

        print(f"\n  職種分布:")
        job_dist = df_new['job_type'].value_counts().head(10)
        for job, cnt in job_dist.items():
            if job:
                print(f"    {job}: {cnt}件")

        print(f"\n  カテゴリ分布:")
        cat_dist = df_new['category'].value_counts().head(10)
        for cat, cnt in cat_dist.items():
            if cat:
                print(f"    {cat}: {cnt}件")

        print(f"\n  サンプル（先頭5件）:")
        for i, row in df_new.head(5).iterrows():
            print(f"    {row.get('company_name','')} | {row.get('facility_name','')} | {row.get('phone_normalized','')} | {row.get('location','')}")

    return df_results


if __name__ == '__main__':
    main()
