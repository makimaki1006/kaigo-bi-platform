# -*- coding: utf-8 -*-
"""
募集確認日をメモ欄に追加してCSV再生成
"""
import pandas as pd
import re
import sys
import io
from pathlib import Path
from datetime import datetime

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.path.insert(0, str(Path(__file__).parent.parent))

BASE_DIR = Path(__file__).parent.parent
OUTPUT_DIR = BASE_DIR / 'data' / 'output' / 'media_matching'
TODAY = datetime.now().strftime('%Y%m%d')
TODAY_DATE = datetime.now().strftime('%Y-%m-%d')
BATCH_ID = f'BATCH_{TODAY}_KIRACARE_KANGOOSHIGOTO'

def normalize_phone(phone):
    if pd.isna(phone) or phone == '' or str(phone) == 'nan':
        return None
    phone_str = str(phone).strip()
    phone_str = re.sub(r'^電話番号[：:]\s*', '', phone_str)
    if phone_str.startswith('所在地'):
        return None
    digits = re.sub(r'\D', '', phone_str)
    if len(digits) == 10 and not digits.startswith('0'):
        digits = '0' + digits
    if 10 <= len(digits) <= 11:
        return digits
    return None

def extract_prefecture(address):
    if pd.isna(address) or not address:
        return None
    prefectures = [
        '北海道', '青森県', '岩手県', '宮城県', '秋田県', '山形県', '福島県',
        '茨城県', '栃木県', '群馬県', '埼玉県', '千葉県', '東京都', '神奈川県',
        '新潟県', '富山県', '石川県', '福井県', '山梨県', '長野県', '岐阜県',
        '静岡県', '愛知県', '三重県', '滋賀県', '京都府', '大阪府', '兵庫県',
        '奈良県', '和歌山県', '鳥取県', '島根県', '岡山県', '広島県', '山口県',
        '徳島県', '香川県', '愛媛県', '高知県', '福岡県', '佐賀県', '長崎県',
        '熊本県', '大分県', '宮崎県', '鹿児島県', '沖縄県'
    ]
    for pref in prefectures:
        if address.startswith(pref):
            return pref
    return None

def extract_city(address, prefecture):
    if not address or not prefecture:
        return None
    remaining = address[len(prefecture):]
    match = re.match(r'^(.+?[市区町村])', remaining)
    if match:
        return match.group(1)
    return None

def load_population_data():
    pop_file = BASE_DIR / 'data' / 'output' / 'population' / 'municipality_population_density.csv'
    df_pop = pd.read_csv(pop_file, dtype=str, encoding='utf-8-sig')
    df_pop['population'] = pd.to_numeric(df_pop['population'], errors='coerce')
    df_pop['population_density_m2'] = pd.to_numeric(df_pop['population_density_m2'], errors='coerce')
    return df_pop

def get_population_for_address(address, df_pop):
    prefecture = extract_prefecture(address)
    if not prefecture:
        return None, None
    city = extract_city(address, prefecture)
    if city:
        key = prefecture + city
        match = df_pop[df_pop['key'] == key]
        if not match.empty:
            return match.iloc[0]['population'], match.iloc[0]['population_density_m2']
    key_pref = prefecture
    match = df_pop[df_pop['key'] == key_pref]
    if not match.empty:
        return match.iloc[0]['population'], match.iloc[0]['population_density_m2']
    return None, None

def extract_confirm_date_kira(text):
    """きらケア: 募集状況確認日：2026/01/13"""
    if pd.isna(text):
        return None
    match = re.search(r'募集状況確認日[：:](\d{4}/\d{2}/\d{2})', str(text))
    if match:
        return match.group(1)
    return None

def extract_confirm_date_kango(text):
    """看護のお仕事: 2026/01/08に募集状況を確認しました"""
    if pd.isna(text):
        return None
    match = re.search(r'(\d{4}/\d{2}/\d{2})に募集状況を確認', str(text))
    if match:
        return match.group(1)
    return None

def create_memo(row, is_new=True):
    """メモ欄作成（募集確認日追加、レポート抽出用キーワード付き）

    キーワード4種類:
    - [KIRACARE_NEW_20260116] きらケア × 新規
    - [KIRACARE_UPDATE_20260116] きらケア × 更新
    - [KANGO_NEW_20260116] 看護のお仕事 × 新規
    - [KANGO_UPDATE_20260116] 看護のお仕事 × 更新
    """
    source = row.get('Paid_Media__c', '')
    address = row.get('Street', '')
    job_type = row.get('Paid_RecruitmentType__c', '')
    emp_type = row.get('Paid_EmploymentType__c', '')
    confirm_date = row.get('confirm_date', '')

    # レポート抽出用キーワード（媒体×操作で4種類）
    media_key = 'KIRACARE' if source == 'きらケア' else 'KANGO'
    action_key = 'NEW' if is_new else 'UPDATE'
    report_key = f'[{media_key}_{action_key}_{TODAY}]'

    prefix = '【新規作成】' if is_new else '【既存更新】'

    memo = f"""{report_key}
{prefix}
【{BATCH_ID}】
【{source}】
住所: {address}
職種: {job_type}
雇用形態: {emp_type}
募集確認日: {confirm_date if confirm_date else '不明'}
取得日: {TODAY_DATE}"""
    return memo

def main():
    print('=== メモ欄に募集確認日を追加して再生成 ===')
    print()

    # 人口データ
    df_pop = load_population_data()

    # 元データ読み込み
    df_kira = pd.read_csv(r'C:\Users\fuji1\Downloads\kiracare-2026-01-14-with-phone.csv', dtype=str)
    df_kango = pd.read_csv(r'C:\Users\fuji1\Downloads\kango-oshigoto-2026-01-14-with-phone.csv', dtype=str)
    print(f'きらケア: {len(df_kira)}件')
    print(f'看護のお仕事: {len(df_kango)}件')

    # きらケア処理
    kira_records = []
    for _, row in df_kira.iterrows():
        phone = normalize_phone(row.get('Google電話番号', ''))
        if not phone:
            continue
        address = row.get('p-jobCard__address', '')
        prefecture = extract_prefecture(address)
        population, pop_density = get_population_for_address(address, df_pop)
        confirm_date = extract_confirm_date_kira(row.get('p-jobCard__updateDate', ''))

        record = {
            'phone_normalized': phone,
            'Company': row.get('p-jobCard__officeNameHead', ''),
            'CompanyName__c': row.get('p-jobCard__ownerNameHead', ''),
            'Street': address,
            'Prefecture__c': prefecture,
            'Paid_URL__c': row.get('p-jobCard__link href', ''),
            'Paid_RecruitmentType__c': row.get('p-jobCard__recruitTtl', ''),
            'Paid_EmploymentType__c': row.get('c-tag__status', ''),
            'LeadSource': 'きらケア',
            'Paid_Media__c': 'きらケア',
            'Paid_DataSource__c': 'きらケア',
            'Paid_DataExportDate__c': TODAY_DATE,
            'Population__c': population,
            'PopulationDensity__c': pop_density,
            'confirm_date': confirm_date,
            'source': 'kiracare'
        }
        kira_records.append(record)

    # 看護のお仕事処理
    kango_records = []
    for _, row in df_kango.iterrows():
        phone = normalize_phone(row.get('Google電話番号', ''))
        if not phone:
            continue
        address_short = row.get('p-jobCard__infoAddress', '')
        combined = row.get('合わせ', '')
        parts = str(combined).split('　') if combined else []
        full_address = parts[-1] if len(parts) >= 3 else address_short
        prefecture = extract_prefecture(full_address) or extract_prefecture(address_short)
        population, pop_density = get_population_for_address(full_address, df_pop)
        confirm_date = extract_confirm_date_kango(row.get('p-jobCard__updateText', ''))

        record = {
            'phone_normalized': phone,
            'Company': row.get('p-jobCard__headerOfficeName', ''),
            'CompanyName__c': row.get('p-jobCard__headerOfficeInfoItem', ''),
            'Street': full_address,
            'Prefecture__c': prefecture,
            'Paid_URL__c': row.get('p-jobCard__linkWrap href', ''),
            'Paid_RecruitmentType__c': row.get('p-jobCard__headerTag (2)', ''),
            'Paid_EmploymentType__c': row.get('p-jobCard__headerTag (3)', ''),
            'Paid_Industry__c': row.get('p-jobCard__headerOfficeInfoItem (2)', ''),
            'LeadSource': '看護のお仕事',
            'Paid_Media__c': '看護のお仕事',
            'Paid_DataSource__c': '看護のお仕事',
            'Paid_DataExportDate__c': TODAY_DATE,
            'Population__c': population,
            'PopulationDensity__c': pop_density,
            'confirm_date': confirm_date,
            'source': 'kango'
        }
        kango_records.append(record)

    df_kira_processed = pd.DataFrame(kira_records)
    df_kango_processed = pd.DataFrame(kango_records)

    # 統合・重複除去
    df_combined = pd.concat([df_kira_processed, df_kango_processed], ignore_index=True)
    df_combined = df_combined.drop_duplicates(subset=['phone_normalized'], keep='first')
    print(f'統合後: {len(df_combined)}件')

    # Salesforceデータ読み込み
    df_acc = pd.read_csv(BASE_DIR / 'data' / 'output' / 'Account_phones_20260116_full.csv', dtype=str)
    df_lead_all = pd.read_csv(BASE_DIR / 'data' / 'output' / 'Lead_phones_20260116.csv', dtype=str)

    # 今日作成・削除したリードを除外
    df_created = pd.read_csv(OUTPUT_DIR / 'created_lead_ids_20260116_kiracare_kango.csv', dtype=str)
    df_deleted = pd.read_csv(OUTPUT_DIR / 'duplicate_leads_to_delete_20260116.csv', dtype=str)
    exclude_ids = set(df_created['sf__Id'].tolist()) | set(df_deleted['Id'].tolist())
    df_lead = df_lead_all[~df_lead_all['Id'].isin(exclude_ids)]

    # 電話番号セット
    acc_phones = {}
    for _, row in df_acc.iterrows():
        for col in ['Phone', 'Phone2__c']:
            p = normalize_phone(row.get(col))
            if p:
                acc_phones[p] = row['Id']

    lead_phones = {}
    for _, row in df_lead.iterrows():
        for col in ['Phone', 'MobilePhone']:
            p = normalize_phone(row.get(col))
            if p:
                lead_phones[p] = row['Id']

    # 成約先除外
    contract_file = BASE_DIR / 'data' / 'output' / 'hellowork' / 'contract_accounts_with_corp.csv'
    df_contract = pd.read_csv(contract_file, dtype=str)
    contract_phones = set()
    for col in ['Phone', 'Phone2__c']:
        if col in df_contract.columns:
            phones = df_contract[col].apply(normalize_phone)
            contract_phones.update(phones[phones.notna()].tolist())

    df_combined = df_combined[~df_combined['phone_normalized'].isin(contract_phones)]
    print(f'成約先除外後: {len(df_combined)}件')

    # 突合
    account_matches = []
    lead_matches = []
    new_leads = []

    for _, row in df_combined.iterrows():
        phone = row['phone_normalized']
        if phone in acc_phones:
            row_dict = row.to_dict()
            row_dict['Id'] = acc_phones[phone]
            account_matches.append(row_dict)
        elif phone in lead_phones:
            row_dict = row.to_dict()
            row_dict['Id'] = lead_phones[phone]
            lead_matches.append(row_dict)
        else:
            new_leads.append(row.to_dict())

    print(f'Account一致: {len(account_matches)}件')
    print(f'Lead一致: {len(lead_matches)}件')
    print(f'新規リード: {len(new_leads)}件')
    print()

    # CSV生成
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Account更新
    df_acc_update = pd.DataFrame(account_matches)
    df_acc_update['Paid_Memo__c'] = df_acc_update.apply(lambda r: create_memo(r, is_new=False), axis=1)
    acc_columns = ['Id', 'Paid_Media__c', 'Paid_Memo__c', 'Paid_DataExportDate__c',
                   'Paid_DataSource__c', 'Paid_URL__c', 'Paid_RecruitmentType__c',
                   'Paid_EmploymentType__c', 'Population__c', 'PopulationDensity__c']
    df_acc_out = df_acc_update[[c for c in acc_columns if c in df_acc_update.columns]]
    df_acc_out.to_csv(OUTPUT_DIR / f'kiracare_kango_account_updates_{TODAY}_v3.csv', index=False, encoding='utf-8-sig')
    print(f'Account更新CSV: {len(df_acc_out)}件')

    # Lead更新（既存）
    df_lead_update = pd.DataFrame(lead_matches)
    df_lead_update['Paid_Memo__c'] = df_lead_update.apply(lambda r: create_memo(r, is_new=False), axis=1)
    lead_columns = ['Id', 'Paid_Media__c', 'Paid_Memo__c', 'Paid_DataExportDate__c',
                   'Paid_DataSource__c', 'Paid_URL__c', 'Paid_RecruitmentType__c',
                   'Paid_EmploymentType__c', 'Population__c', 'PopulationDensity__c']
    df_lead_out = df_lead_update[[c for c in lead_columns if c in df_lead_update.columns]]
    df_lead_out.to_csv(OUTPUT_DIR / f'kiracare_kango_lead_updates_{TODAY}_v3.csv', index=False, encoding='utf-8-sig')
    print(f'Lead更新CSV: {len(df_lead_out)}件')

    # 新規リード（今日作成分とマッチング）
    df_new = pd.DataFrame(new_leads)
    df_new['Paid_Memo__c'] = df_new.apply(lambda r: create_memo(r, is_new=True), axis=1)

    # 作成済みリードとマッチ
    df_created_valid = df_created[~df_created['sf__Id'].isin(df_deleted['Id'].tolist())]
    df_created_leads = df_lead_all[df_lead_all['Id'].isin(df_created_valid['sf__Id'].tolist())][['Id', 'Phone']]
    df_created_leads['phone_normalized'] = df_created_leads['Phone'].apply(normalize_phone)

    df_new_merged = df_new.merge(df_created_leads[['Id', 'phone_normalized']], on='phone_normalized', how='left')
    df_new_update = df_new_merged[df_new_merged['Id'].notna()].copy()

    new_columns = ['Id', 'Paid_Media__c', 'Paid_Memo__c', 'Paid_DataExportDate__c',
                   'Paid_DataSource__c', 'Paid_URL__c', 'Paid_RecruitmentType__c',
                   'Paid_EmploymentType__c', 'Paid_Industry__c', 'Population__c',
                   'PopulationDensity__c', 'Street', 'Prefecture__c', 'CompanyName__c']
    df_new_out = df_new_update[[c for c in new_columns if c in df_new_update.columns]]
    df_new_out.to_csv(OUTPUT_DIR / f'kiracare_kango_created_lead_updates_{TODAY}_v3.csv', index=False, encoding='utf-8-sig')
    print(f'作成済みLead更新CSV: {len(df_new_out)}件')

    print()
    print('=' * 60)
    print('サンプルメモ欄（きらケア）:')
    print('=' * 60)
    print(df_acc_out.iloc[0]['Paid_Memo__c'])
    print()
    print('=' * 60)
    print('サンプルメモ欄（看護のお仕事）:')
    print('=' * 60)
    kango_sample = df_acc_out[df_acc_out['Paid_Media__c'] == '看護のお仕事']
    if len(kango_sample) > 0:
        print(kango_sample.iloc[0]['Paid_Memo__c'])
    else:
        kango_sample = df_new_out[df_new_out['Paid_Media__c'] == '看護のお仕事']
        if len(kango_sample) > 0:
            print(kango_sample.iloc[0]['Paid_Memo__c'])

if __name__ == '__main__':
    main()
