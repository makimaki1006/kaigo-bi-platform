# -*- coding: utf-8 -*-
"""住所+名前マッチのサンプル抽出"""

import pandas as pd
import re
from pathlib import Path

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

def normalize_phone(phone):
    if pd.isna(phone) or phone == '' or str(phone) == 'nan':
        return None
    phone_str = str(phone).strip()
    if phone_str.endswith('.0'):
        phone_str = phone_str[:-2]
    digits = re.sub(r'\D', '', phone_str)
    if len(digits) == 10 and not digits.startswith('0'):
        digits = '0' + digits
    if len(digits) >= 10 and len(digits) <= 11:
        return digits
    return None

def extract_city(address):
    if pd.isna(address) or not address:
        return None, None
    address = str(address).strip().replace('　', ' ').replace(' ', '')
    pref = None
    for p in PREFECTURES:
        if address.startswith(p):
            pref = p
            address = address[len(p):]
            break
    if not pref:
        return None, None
    city_pattern = re.compile(r'^(.+?[市区])|(.*?郡.+?[町村])')
    match = city_pattern.match(address)
    if match:
        city = match.group(0)
        return pref, city
    return pref, None

def normalize_company_name(name):
    if pd.isna(name) or not name:
        return ''
    name = str(name).strip()
    name = name.replace('　', '').replace(' ', '')
    for prefix in ['医療法人社団', '医療法人財団', '医療法人', '社会福祉法人',
                   '株式会社', '有限会社', '合同会社', '一般社団法人', '公益社団法人']:
        name = name.replace(prefix, '')
    return name

def is_similar_name(name1, name2, threshold=0.85):
    if not name1 or not name2:
        return False
    n1 = normalize_company_name(name1)
    n2 = normalize_company_name(name2)
    if not n1 or not n2:
        return False
    if n1 == n2:
        return True
    if len(n1) >= 5 and len(n2) >= 5:
        shorter = n1 if len(n1) <= len(n2) else n2
        longer = n2 if len(n1) <= len(n2) else n1
        if shorter in longer and len(shorter) >= 5:
            return True
    if len(n1) <= 5 or len(n2) <= 5:
        return n1 == n2
    set1 = set(n1)
    set2 = set(n2)
    intersection = len(set1 & set2)
    union = len(set1 | set2)
    similarity = intersection / union if union > 0 else 0
    return similarity >= threshold

def main():
    # 媒体データを読み込み
    KANGO_FILE = r'C:\Users\fuji1\OneDrive\デスクトップ\pythonスクリプト置き場\final_kango_with_google_v2.xlsx'
    df_kango = pd.read_excel(KANGO_FILE, dtype=str)

    NURSE_FILE = r'C:\Users\fuji1\OneDrive\デスクトップ\pythonスクリプト置き場\final_fallback_nursejinzaibank_final_structured_v3.xlsx'
    df_nurse = pd.read_excel(NURSE_FILE, dtype=str)

    # Salesforce Account読み込み
    acc_files = sorted(Path('data/output').glob('Account_*.csv'), reverse=True)
    acc_cols = ['Id', 'Name', 'Phone', 'PersonMobilePhone', 'Phone2__c', 'Address__c']
    df_acc = pd.read_csv(acc_files[0], usecols=lambda c: c in acc_cols, dtype=str, encoding='utf-8')

    # 電話番号インデックス
    phone_to_acc = {}
    for _, row in df_acc.iterrows():
        for col in ['Phone', 'PersonMobilePhone', 'Phone2__c']:
            if col in row:
                normalized = normalize_phone(row[col])
                if normalized:
                    phone_to_acc[normalized] = row
                    break

    # 住所インデックス
    acc_location_index = {}
    for _, row in df_acc.iterrows():
        name = row.get('Name', '')
        address = row.get('Address__c', '')
        if not pd.notna(name) or not name:
            continue
        pref, city = extract_city(address)
        if pref and city:
            key = (pref, city)
            if key not in acc_location_index:
                acc_location_index[key] = []
            acc_location_index[key].append((normalize_company_name(name), str(name).strip(), row['Id'], row))

    print(f"Account電話番号インデックス: {len(phone_to_acc)}件")
    print(f"Account住所インデックス: {len(acc_location_index)}地域")

    # 住所+名前マッチを検出
    location_matches = []

    # 看護roo
    for _, row in df_kango.iterrows():
        phone = normalize_phone(row.get('phone_cleaned', ''))
        if not phone:
            phone = normalize_phone(row.get('google_phone', ''))
        if not phone:
            continue

        if phone in phone_to_acc:
            continue

        company = row.get('shisetsu_name', '')
        address = row.get('font-xs', '')
        if not company or address in ['NEW', '']:
            continue

        pref, city = extract_city(address)
        if not pref or not city:
            continue

        location_key = (pref, city)
        if location_key in acc_location_index:
            company_normalized = normalize_company_name(company)
            for acc_normalized, acc_original, acc_id, acc_row in acc_location_index[location_key]:
                if is_similar_name(company_normalized, acc_normalized):
                    location_matches.append({
                        'source': '看護roo',
                        'media_company': company,
                        'media_address': address,
                        'media_phone': phone,
                        'sf_account_id': acc_id,
                        'sf_account_name': acc_original,
                        'sf_account_address': acc_row.get('Address__c', ''),
                        'sf_account_phone': acc_row.get('Phone', ''),
                        'match_location': f'{pref}{city}'
                    })
                    break

    # ナース専科
    for _, row in df_nurse.iterrows():
        phone = normalize_phone(row.get('phone_cleaned', ''))
        if not phone:
            continue

        if phone in phone_to_acc:
            continue

        company = row.get('名称', '')
        address = row.get('所在地', '')
        if not company:
            continue

        pref, city = extract_city(address)
        if not pref or not city:
            continue

        location_key = (pref, city)
        if location_key in acc_location_index:
            company_normalized = normalize_company_name(company)
            for acc_normalized, acc_original, acc_id, acc_row in acc_location_index[location_key]:
                if is_similar_name(company_normalized, acc_normalized):
                    location_matches.append({
                        'source': 'ナース専科',
                        'media_company': company,
                        'media_address': address,
                        'media_phone': phone,
                        'sf_account_id': acc_id,
                        'sf_account_name': acc_original,
                        'sf_account_address': acc_row.get('Address__c', ''),
                        'sf_account_phone': acc_row.get('Phone', ''),
                        'match_location': f'{pref}{city}'
                    })
                    break

    print(f"\n住所+名前マッチ総数: {len(location_matches)}件")

    # CSV出力
    df_matches = pd.DataFrame(location_matches)
    df_matches.to_csv('data/output/media_matching/location_match_sample.csv', index=False, encoding='utf-8-sig')

    # サンプル20件を出力
    print("\n" + "=" * 60)
    print("サンプル20件")
    print("=" * 60)

    for i, m in enumerate(location_matches[:20]):
        print(f"""
{i+1}. [{m['source']}]
   媒体会社名: {m['media_company']}
   媒体住所: {m['media_address']}
   媒体電話: {m['media_phone']}
   ---
   SF Account名: {m['sf_account_name']}
   SF Account ID: {m['sf_account_id']}
   SF住所: {m['sf_account_address']}
   SF電話: {m['sf_account_phone']}
   マッチ地域: {m['match_location']}
""")

if __name__ == '__main__':
    main()
