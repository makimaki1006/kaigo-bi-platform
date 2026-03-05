# -*- coding: utf-8 -*-
"""
きらケア・看護のお仕事 リスト処理
- Salesforce突合
- 成約先除外
- 新規リード/更新CSV作成
"""
import pandas as pd
import re
import sys
import io
from pathlib import Path
from datetime import datetime

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.api.salesforce_client import SalesforceClient
import requests

# ========================================
# 設定
# ========================================
BASE_DIR = Path(__file__).parent.parent
OUTPUT_DIR = BASE_DIR / 'data' / 'output' / 'media_matching'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

KIRACARE_FILE = Path(r'C:\Users\fuji1\Downloads\kiracare-2026-01-14-with-phone.csv')
KANGO_OSHIGOTO_FILE = Path(r'C:\Users\fuji1\Downloads\kango-oshigoto-2026-01-14-with-phone.csv')

TODAY = datetime.now().strftime('%Y%m%d')
BATCH_ID = f'BATCH_{TODAY}_KIRACARE_KANGOOSHIGOTO'

# 都道府県リスト
PREFECTURES = [
    '北海道', '青森県', '岩手県', '宮城県', '秋田県', '山形県', '福島県',
    '茨城県', '栃木県', '群馬県', '埼玉県', '千葉県', '東京都', '神奈川県',
    '新潟県', '富山県', '石川県', '福井県', '山梨県', '長野県',
    '岐阜県', '静岡県', '愛知県', '三重県',
    '滋賀県', '京都府', '大阪府', '兵庫県', '奈良県', '和歌山県',
    '鳥取県', '島根県', '岡山県', '広島県', '山口県',
    '徳島県', '香川県', '愛媛県', '高知県',
    '福岡県', '佐賀県', '長崎県', '熊本県', '大分県', '宮崎県', '鹿児島県', '沖縄県'
]

LEGAL_PREFIXES = [
    '医療法人社団', '医療法人財団', '医療法人', '社会福祉法人', '社会医療法人',
    '株式会社', '有限会社', '合同会社', '一般社団法人', '公益社団法人',
    '特定医療法人', '独立行政法人', '国立研究開発法人'
]

# ========================================
# ユーティリティ関数
# ========================================
def normalize_phone(phone):
    """電話番号を正規化"""
    if pd.isna(phone) or phone == '' or str(phone) == 'nan':
        return None
    phone_str = str(phone).strip()
    # 「電話番号：」プレフィックスを除去
    phone_str = re.sub(r'^電話番号[：:]\s*', '', phone_str)
    # 「所在地：」の場合はスキップ
    if phone_str.startswith('所在地'):
        return None
    # 数字のみ抽出
    digits = re.sub(r'\D', '', phone_str)
    # 10桁で0始まりでない場合は先頭に0を補完
    if len(digits) == 10 and not digits.startswith('0'):
        digits = '0' + digits
    if 10 <= len(digits) <= 11:
        return digits
    return None

def is_mobile_phone(phone):
    """携帯電話判定"""
    if not phone:
        return False
    return phone.startswith(('090', '080', '070'))

def extract_prefecture(address):
    """住所から都道府県を抽出"""
    if not address:
        return '', ''
    address = str(address).strip()
    for pref in PREFECTURES:
        if address.startswith(pref):
            return pref, address[len(pref):]
    return '', address

def normalize_company_name(name):
    """会社名を正規化（法人格除去）"""
    if not name:
        return ''
    name = str(name).strip()
    for prefix in LEGAL_PREFIXES:
        name = name.replace(prefix, '')
    # 空白・記号を除去
    name = re.sub(r'[\s　・\-－]', '', name)
    return name

def is_similar_name(name1, name2, threshold=0.7):
    """会社名の類似度判定"""
    n1 = normalize_company_name(name1)
    n2 = normalize_company_name(name2)
    if not n1 or not n2:
        return False
    # 完全一致
    if n1 == n2:
        return True
    # 部分文字列
    shorter = n1 if len(n1) <= len(n2) else n2
    longer = n2 if len(n1) <= len(n2) else n1
    if len(shorter) >= 4 and shorter in longer:
        return True
    # Jaccard類似度
    if len(n1) <= 4 or len(n2) <= 4:
        return n1 == n2
    set1, set2 = set(n1), set(n2)
    intersection = len(set1 & set2)
    union = len(set1 | set2)
    similarity = intersection / union if union > 0 else 0
    return similarity >= threshold

def extract_city(address):
    """住所から都道府県+市区町村を抽出"""
    if not address:
        return None, None
    address = str(address).strip()
    pref = None
    for p in PREFECTURES:
        if address.startswith(p):
            pref = p
            address = address[len(p):]
            break
    if not pref:
        return None, None
    # 市区町村抽出
    match = re.match(r'^(.+?[市区])|(.*?郡.+?[町村])', address)
    if match:
        return pref, match.group(0)
    return pref, None

# ========================================
# メイン処理
# ========================================
def main():
    print('=' * 60)
    print('きらケア・看護のお仕事 リスト処理')
    print('=' * 60)
    print()

    # 1. データ読み込み
    print('1. データ読み込み...')

    df_kiracare = pd.read_csv(KIRACARE_FILE, dtype=str)
    print(f'   きらケア: {len(df_kiracare):,}件')

    df_kango_oshigoto = pd.read_csv(KANGO_OSHIGOTO_FILE, dtype=str)
    print(f'   看護のお仕事: {len(df_kango_oshigoto):,}件')
    print()

    # 2. 電話番号正規化・抽出
    print('2. 電話番号正規化...')

    # きらケア
    df_kiracare['phone_normalized'] = df_kiracare['Google電話番号'].apply(normalize_phone)
    df_kiracare['company_name'] = df_kiracare['p-jobCard__ownerNameHead'].fillna('') + '　' + df_kiracare['p-jobCard__officeNameHead'].fillna('')
    df_kiracare['company_name'] = df_kiracare['company_name'].str.strip()
    df_kiracare['address'] = df_kiracare['p-jobCard__address']
    df_kiracare['job_type'] = df_kiracare['p-jobCard__recruitTtl']
    df_kiracare['employment_type'] = df_kiracare['c-tag__status']
    df_kiracare['source'] = 'きらケア'

    kiracare_valid = df_kiracare['phone_normalized'].notna()
    print(f'   きらケア 有効電話: {kiracare_valid.sum():,}件 / {len(df_kiracare):,}件')

    # 看護のお仕事
    df_kango_oshigoto['phone_normalized'] = df_kango_oshigoto['Google電話番号'].apply(normalize_phone)
    df_kango_oshigoto['company_name'] = df_kango_oshigoto['p-jobCard__headerOfficeInfoItem'].fillna('') + '　' + df_kango_oshigoto['p-jobCard__headerOfficeName'].fillna('')
    df_kango_oshigoto['company_name'] = df_kango_oshigoto['company_name'].str.strip()
    df_kango_oshigoto['address'] = df_kango_oshigoto['p-jobCard__infoAddress']
    df_kango_oshigoto['facility_type'] = df_kango_oshigoto['p-jobCard__headerOfficeInfoItem (2)']
    df_kango_oshigoto['license_type'] = df_kango_oshigoto['p-jobCard__headerTag (2)']
    df_kango_oshigoto['employment_type'] = df_kango_oshigoto['p-jobCard__headerTag (3)']
    df_kango_oshigoto['source'] = '看護のお仕事'

    kango_valid = df_kango_oshigoto['phone_normalized'].notna()
    print(f'   看護のお仕事 有効電話: {kango_valid.sum():,}件 / {len(df_kango_oshigoto):,}件')
    print()

    # 3. 統合・重複除去
    print('3. データ統合・重複除去...')

    # 共通列を統一
    common_cols = ['phone_normalized', 'company_name', 'address', 'source']

    df_kiracare_slim = df_kiracare[kiracare_valid][common_cols + ['job_type', 'employment_type']].copy()
    df_kiracare_slim['facility_type'] = ''
    df_kiracare_slim['license_type'] = ''

    df_kango_slim = df_kango_oshigoto[kango_valid][common_cols + ['facility_type', 'license_type', 'employment_type']].copy()
    df_kango_slim['job_type'] = ''

    df_combined = pd.concat([df_kiracare_slim, df_kango_slim], ignore_index=True)
    print(f'   統合後: {len(df_combined):,}件')

    # 電話番号で重複除去
    df_combined = df_combined.drop_duplicates(subset=['phone_normalized'], keep='first')
    print(f'   電話番号重複除去後: {len(df_combined):,}件')
    print()

    # 4. Salesforce認証・データ取得
    print('4. Salesforce接続...')
    client = SalesforceClient()
    client.authenticate()

    # Account取得
    print('   Account取得中...')
    acc_soql = """
        SELECT Id, Name, Phone, PersonMobilePhone, Phone2__c, Address__c,
               BillingState, BillingCity, BillingStreet, RecordType.Name
        FROM Account
        WHERE Phone != null OR PersonMobilePhone != null OR Phone2__c != null
    """
    url = f"{client.instance_url}/services/data/{client.api_version}/query"
    all_accounts = []
    params = {'q': acc_soql}
    while True:
        response = requests.get(url, headers=client._get_headers(), params=params)
        response.raise_for_status()
        data = response.json()
        all_accounts.extend(data['records'])
        if data.get('nextRecordsUrl'):
            url = client.instance_url + data['nextRecordsUrl']
            params = None
        else:
            break
    print(f'   Account: {len(all_accounts):,}件')

    # Lead取得
    print('   Lead取得中...')
    lead_soql = """
        SELECT Id, Company, Phone, MobilePhone, Phone2__c, Street, Prefecture__c, State
        FROM Lead
        WHERE IsConverted = false AND (Phone != null OR MobilePhone != null OR Phone2__c != null)
    """
    url = f"{client.instance_url}/services/data/{client.api_version}/query"
    all_leads = []
    params = {'q': lead_soql}
    while True:
        response = requests.get(url, headers=client._get_headers(), params=params)
        response.raise_for_status()
        data = response.json()
        all_leads.extend(data['records'])
        if data.get('nextRecordsUrl'):
            url = client.instance_url + data['nextRecordsUrl']
            params = None
        else:
            break
    print(f'   Lead: {len(all_leads):,}件')
    print()

    # 5. 電話番号インデックス作成
    print('5. 突合インデックス作成...')

    # Account電話インデックス
    acc_phone_index = {}
    contract_phones = set()
    contract_location_index = {}

    for rec in all_accounts:
        is_contract = rec.get('RecordType', {}).get('Name') == '成約先（様付け）'

        for col in ['Phone', 'PersonMobilePhone', 'Phone2__c']:
            phone = normalize_phone(rec.get(col))
            if phone:
                if phone not in acc_phone_index:
                    acc_phone_index[phone] = rec
                if is_contract:
                    contract_phones.add(phone)
                break

        # 成約先の住所インデックス
        if is_contract:
            addr = rec.get('Address__c') or (rec.get('BillingState', '') + rec.get('BillingCity', ''))
            if addr:
                pref, city = extract_city(addr)
                if pref and city:
                    key = (pref, city)
                    name = normalize_company_name(rec.get('Name', ''))
                    if key not in contract_location_index:
                        contract_location_index[key] = []
                    contract_location_index[key].append(name)

    print(f'   Account電話インデックス: {len(acc_phone_index):,}件')
    print(f'   成約先電話: {len(contract_phones):,}件')
    print(f'   成約先住所インデックス: {len(contract_location_index):,}件')

    # Lead電話インデックス
    lead_phone_index = {}
    for rec in all_leads:
        for col in ['Phone', 'MobilePhone', 'Phone2__c']:
            phone = normalize_phone(rec.get(col))
            if phone:
                if phone not in lead_phone_index:
                    lead_phone_index[phone] = rec
                break

    print(f'   Lead電話インデックス: {len(lead_phone_index):,}件')
    print()

    # 6. 突合処理
    print('6. 突合処理...')

    acc_matches = []
    lead_matches = []
    new_leads = []
    excluded_contract = []

    for _, row in df_combined.iterrows():
        phone = row['phone_normalized']
        company = row['company_name']
        address = row['address']
        source = row['source']

        # Account電話マッチ
        if phone in acc_phone_index:
            acc = acc_phone_index[phone]
            acc_matches.append({
                'Id': acc['Id'],
                'Name': acc.get('Name', ''),
                'media_company': company,
                'media_phone': phone,
                'media_address': address,
                'media_source': source
            })
            continue

        # Lead電話マッチ
        if phone in lead_phone_index:
            lead = lead_phone_index[phone]
            lead_matches.append({
                'Id': lead['Id'],
                'Company': lead.get('Company', ''),
                'media_company': company,
                'media_phone': phone,
                'media_address': address,
                'media_source': source
            })
            continue

        # 成約先除外（電話番号）
        if phone in contract_phones:
            excluded_contract.append({
                'company': company,
                'phone': phone,
                'reason': '成約先電話マッチ'
            })
            continue

        # 成約先除外（住所+名前）
        pref, city = extract_city(address)
        is_contract_location = False
        if pref and city:
            key = (pref, city)
            if key in contract_location_index:
                for contract_name in contract_location_index[key]:
                    if is_similar_name(company, contract_name):
                        excluded_contract.append({
                            'company': company,
                            'phone': phone,
                            'reason': f'成約先住所+名前マッチ ({pref}{city})'
                        })
                        is_contract_location = True
                        break

        if is_contract_location:
            continue

        # 新規リード候補
        new_leads.append(row.to_dict())

    print(f'   Account更新候補: {len(acc_matches):,}件')
    print(f'   Lead更新候補: {len(lead_matches):,}件')
    print(f'   成約先除外: {len(excluded_contract):,}件')
    print(f'   新規リード候補: {len(new_leads):,}件')
    print()

    # 7. CSV出力
    print('7. CSV出力...')

    # Account更新CSV
    if acc_matches:
        df_acc_update = pd.DataFrame(acc_matches)
        df_acc_update['Paid_Memo__c'] = df_acc_update.apply(
            lambda r: f"★\n【{BATCH_ID}】[{r['media_source']}]\n取得日: {datetime.now().strftime('%Y-%m-%d')}",
            axis=1
        )
        acc_update_file = OUTPUT_DIR / f'kiracare_kango_account_updates_{TODAY}.csv'
        df_acc_update[['Id', 'Paid_Memo__c']].to_csv(acc_update_file, index=False, encoding='utf-8-sig')
        print(f'   Account更新: {acc_update_file}')

    # Lead更新CSV
    if lead_matches:
        df_lead_update = pd.DataFrame(lead_matches)
        df_lead_update['Paid_Memo__c'] = df_lead_update.apply(
            lambda r: f"★\n【{BATCH_ID}】[{r['media_source']}]\n取得日: {datetime.now().strftime('%Y-%m-%d')}",
            axis=1
        )
        lead_update_file = OUTPUT_DIR / f'kiracare_kango_lead_updates_{TODAY}.csv'
        df_lead_update[['Id', 'Paid_Memo__c']].to_csv(lead_update_file, index=False, encoding='utf-8-sig')
        print(f'   Lead更新: {lead_update_file}')

    # 新規リードCSV
    if new_leads:
        df_new = pd.DataFrame(new_leads)

        # バリデーション
        valid_company = df_new['company_name'].notna() & (df_new['company_name'] != '')
        valid_phone = df_new['phone_normalized'].notna() & (df_new['phone_normalized'] != '')
        valid_mask = valid_company & valid_phone

        skipped_company = (~valid_company).sum()
        skipped_phone = (~valid_phone).sum()

        if skipped_company > 0:
            print(f'   ※Company空でスキップ: {skipped_company}件')
        if skipped_phone > 0:
            print(f'   ※Phone空でスキップ: {skipped_phone}件')

        df_valid = df_new[valid_mask].copy()

        # Salesforceフィールドマッピング
        df_valid['Company'] = df_valid['company_name']
        df_valid['LastName'] = '担当者'
        df_valid['Phone'] = df_valid['phone_normalized']
        df_valid['MobilePhone'] = df_valid['phone_normalized'].apply(lambda x: x if is_mobile_phone(x) else '')

        # 住所分解
        df_valid[['Prefecture__c', 'Street']] = df_valid['address'].apply(
            lambda x: pd.Series(extract_prefecture(x))
        )

        # メモ
        df_valid['Paid_Memo__c'] = df_valid.apply(
            lambda r: f"【{BATCH_ID}】\n【{r['source']}】\n住所: {r['address']}\n取得日: {datetime.now().strftime('%Y-%m-%d')}",
            axis=1
        )
        df_valid['LeadSource'] = df_valid['source']
        df_valid['Paid_Media__c'] = df_valid['source']
        df_valid['Paid_DataExportDate__c'] = datetime.now().strftime('%Y-%m-%d')
        df_valid['Paid_DataSource__c'] = df_valid['source']

        export_cols = [
            'Company', 'LastName', 'Phone', 'MobilePhone',
            'Prefecture__c', 'Street', 'Paid_Memo__c',
            'LeadSource', 'Paid_Media__c', 'Paid_DataExportDate__c', 'Paid_DataSource__c'
        ]

        new_leads_file = OUTPUT_DIR / f'kiracare_kango_new_leads_{TODAY}.csv'
        df_valid[export_cols].to_csv(new_leads_file, index=False, encoding='utf-8-sig')
        print(f'   新規リード: {new_leads_file} ({len(df_valid):,}件)')

    # 成約先除外CSV
    if excluded_contract:
        df_excluded = pd.DataFrame(excluded_contract)
        excluded_file = OUTPUT_DIR / f'kiracare_kango_excluded_{TODAY}.csv'
        df_excluded.to_csv(excluded_file, index=False, encoding='utf-8-sig')
        print(f'   成約先除外: {excluded_file}')

    print()
    print('=' * 60)
    print('サマリー')
    print('=' * 60)
    print(f'入力データ:')
    print(f'  きらケア: {len(df_kiracare):,}件')
    print(f'  看護のお仕事: {len(df_kango_oshigoto):,}件')
    print(f'  統合・重複除去後: {len(df_combined):,}件')
    print()
    print(f'突合結果:')
    print(f'  Account更新候補: {len(acc_matches):,}件')
    print(f'  Lead更新候補: {len(lead_matches):,}件')
    print(f'  成約先除外: {len(excluded_contract):,}件')
    print(f'  新規リード候補: {len(new_leads):,}件')
    if new_leads:
        print(f'  新規リード有効: {len(df_valid):,}件')
    print('=' * 60)

if __name__ == '__main__':
    main()
