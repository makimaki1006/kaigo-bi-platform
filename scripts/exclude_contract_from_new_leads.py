# -*- coding: utf-8 -*-
"""
新規リードから成約先を除外
"""
import pandas as pd
import re
import sys
import io
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

BASE_DIR = Path(__file__).parent.parent
CONTRACT_FILE = BASE_DIR / 'data' / 'output' / 'hellowork' / 'contract_accounts_with_corp.csv'
NEW_LEADS_FILE = BASE_DIR / 'data' / 'output' / 'media_matching' / 'kiracare_kango_new_leads_20260116.csv'

def normalize_phone(phone):
    """電話番号を正規化"""
    if pd.isna(phone) or phone == '' or str(phone) == 'nan':
        return None
    phone_str = str(phone).strip()
    if phone_str.endswith('.0'):
        phone_str = phone_str[:-2]
    digits = re.sub(r'\D', '', phone_str)
    if len(digits) == 10 and not digits.startswith('0'):
        digits = '0' + digits
    if 10 <= len(digits) <= 11:
        return digits
    return None

def main():
    print('=== 成約先除外処理 ===')
    print()

    # 成約先データ読み込み
    print('1. 成約先データ読み込み...')
    df_contract = pd.read_csv(CONTRACT_FILE, dtype=str)
    print(f'   成約先: {len(df_contract):,}件')

    # 成約先電話番号セット作成
    contract_phones = set()
    for col in ['Phone', 'Phone2__c']:
        if col in df_contract.columns:
            phones = df_contract[col].apply(normalize_phone)
            valid_phones = phones[phones.notna()].tolist()
            contract_phones.update(valid_phones)
    print(f'   成約先電話番号セット: {len(contract_phones):,}件')
    print()

    # 新規リード読み込み
    print('2. 新規リード読み込み...')
    df_new = pd.read_csv(NEW_LEADS_FILE, dtype=str)
    print(f'   新規リード: {len(df_new):,}件')
    print()

    # 成約先除外
    print('3. 成約先除外...')
    df_new['phone_normalized'] = df_new['Phone'].apply(normalize_phone)
    is_contract = df_new['phone_normalized'].isin(contract_phones)

    excluded = df_new[is_contract]
    filtered = df_new[~is_contract]

    print(f'   除外件数: {is_contract.sum():,}件')
    print(f'   残り: {len(filtered):,}件')
    print()

    # 保存
    print('4. 保存...')
    output_file = BASE_DIR / 'data' / 'output' / 'media_matching' / 'kiracare_kango_new_leads_20260116_filtered.csv'
    filtered.drop(columns=['phone_normalized'], errors='ignore').to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f'   保存: {output_file}')

    if len(excluded) > 0:
        excluded_file = BASE_DIR / 'data' / 'output' / 'media_matching' / 'kiracare_kango_excluded_contract.csv'
        excluded.to_csv(excluded_file, index=False, encoding='utf-8-sig')
        print(f'   除外データ保存: {excluded_file}')

    print()
    print('=== 完了 ===')
    print(f'最終新規リード: {len(filtered):,}件')

if __name__ == '__main__':
    main()
