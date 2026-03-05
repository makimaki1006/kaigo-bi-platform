# -*- coding: utf-8 -*-
"""新規リード作成用CSV生成スクリプト"""
import pandas as pd
import json
import re
import sys
from datetime import date
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')

# 設定
PREFECTURES = [
    '北海道','青森県','岩手県','宮城県','秋田県','山形県','福島県',
    '茨城県','栃木県','群馬県','埼玉県','千葉県','東京都','神奈川県',
    '新潟県','富山県','石川県','福井県','山梨県','長野県',
    '岐阜県','静岡県','愛知県','三重県',
    '滋賀県','京都府','大阪府','兵庫県','奈良県','和歌山県',
    '鳥取県','島根県','岡山県','広島県','山口県',
    '徳島県','香川県','愛媛県','高知県',
    '福岡県','佐賀県','長崎県','熊本県','大分県','宮崎県','鹿児島県','沖縄県'
]

OWNER_IDS = {
    '篠木': '005dc00000HgmfxAAB',
    '市来': '005dc00000FwuKXAAZ',
    '服部': '005J3000000EYYjIAO',
    '深堀': '0055i00000CwKEhAAN',
}


def extract_prefecture(address):
    if pd.isna(address) or not address:
        return ''
    for pref in PREFECTURES:
        if str(address).startswith(pref):
            return pref
    return ''


def extract_street(address):
    if pd.isna(address) or not address:
        return ''
    addr = str(address)
    for pref in PREFECTURES:
        if addr.startswith(pref):
            return addr[len(pref):]
    return addr


def extract_municipality(address):
    if pd.isna(address) or not address:
        return None
    addr = str(address)
    for pref in PREFECTURES:
        if addr.startswith(pref):
            addr = addr[len(pref):]
            break
    m = re.match(r'^(.+?市)(.+?区)?', addr)
    if m:
        return m.group(1)
    m = re.match(r'^(.+?区)', addr)
    if m:
        return m.group(1)
    m = re.match(r'^(.+?郡)(.+?[町村])', addr)
    if m:
        return m.group(2)
    return None


def get_population(address, pop_map):
    muni = extract_municipality(address)
    if not muni:
        return ''
    for key, pop in pop_map.items():
        if muni in key or key in muni:
            return int(pop)
    return ''


def clean_int(val):
    if pd.isna(val) or val == '' or val == 'nan':
        return ''
    try:
        return str(int(float(val)))
    except:
        return ''


def clean_date(val):
    if pd.isna(val) or val == '' or val == 'nan':
        return ''
    return str(val)[:10]


def clean_str(val):
    if pd.isna(val) or val == 'nan':
        return ''
    return str(val)


def assign_owners(df_subset, owners):
    n = len(df_subset)
    per_owner = n // len(owners)
    remainder = n % len(owners)

    owner_ids = []
    for i, owner in enumerate(owners):
        count = per_owner + (1 if i < remainder else 0)
        owner_ids.extend([OWNER_IDS[owner]] * count)

    df_subset = df_subset.reset_index(drop=True)
    df_subset['OwnerId'] = owner_ids[:len(df_subset)]
    return df_subset


def main():
    base_dir = Path(__file__).parent.parent

    # 新規リード候補を読み込み
    df = pd.read_csv(base_dir / 'data/output/hellowork/true_new_leads.csv', dtype=str, encoding='utf-8-sig')
    print(f'新規リード候補: {len(df)}件')

    # 人口マッピング
    with open(base_dir / 'data/population/population_mapping.json', 'r', encoding='utf-8') as f:
        pop_map = json.load(f)

    # クリニック系（一般診療所）とその他に分ける
    clinic_mask = df['産業分類（名称）'] == '一般診療所'
    clinic_df = df[clinic_mask].copy()
    other_df = df[~clinic_mask].copy()

    print(f'クリニック系: {len(clinic_df)}件')
    print(f'その他: {len(other_df)}件')

    # 所有者割り当て
    clinic_df = assign_owners(clinic_df, ['篠木', '市来'])
    other_df = assign_owners(other_df, ['服部', '深堀'])

    df_all = pd.concat([clinic_df, other_df], ignore_index=True)

    today = date.today().strftime('%Y-%m-%d')
    today_short = date.today().strftime('%Y%m%d')

    records = []
    for _, row in df_all.iterrows():
        job_type = clean_str(row.get('職種', ''))
        emp_type = clean_str(row.get('雇用形態', ''))

        # LeadSourceMemo__c
        lead_source_memo = f'{today_short}_ハロワ_{job_type}【{emp_type}'

        # Publish_ImportText__c
        details = []
        if clean_str(row.get('産業分類（名称）')):
            details.append(f"産業分類: {clean_str(row['産業分類（名称）'])}")
        if job_type:
            details.append(f"職種: {job_type}")
        if emp_type:
            details.append(f"雇用形態: {emp_type}")
        if clean_int(row.get('採用人数')):
            details.append(f"採用人数: {clean_int(row['採用人数'])}")
        if clean_int(row.get('従業員数企業全体')):
            details.append(f"従業員数: {clean_int(row['従業員数企業全体'])}")

        publish_import_text = f"[{today} ハロワ新規]\n" + '\n'.join(details)

        # LastName
        last_name = clean_str(row.get('選考担当者', ''))
        if not last_name:
            last_name = '担当者'

        # 事業所名漢字
        company = clean_str(row.get('事業所名漢字', ''))

        record = {
            'Company': company,
            'LastName': last_name,
            'Phone': clean_str(row.get('選考担当者ＴＥＬ', '')),
            'MobilePhone': clean_str(row.get('選考担当者ＴＥＬ（携帯）', '')),
            'PostalCode': clean_str(row.get('事業所郵便番号', '')),
            'Prefecture__c': extract_prefecture(row.get('事業所所在地', '')),
            'Street': extract_street(row.get('事業所所在地', '')),
            'NumberOfEmployees': clean_int(row.get('従業員数企業全体', '')),
            'Website': clean_str(row.get('事業所ホームページ', '')),
            'PresidentName__c': clean_str(row.get('代表者名', '')),
            'PresidentTitle__c': clean_str(row.get('代表者役職', '')),
            'Hellowork_Industry__c': clean_str(row.get('産業分類（名称）', '')),
            'Hellowork_RecuritmentType__c': job_type,
            'Hellowork_EmploymentType__c': emp_type,
            'Hellowork_RecruitmentReasonCategory__c': clean_str(row.get('募集理由区分', '')),
            'Hellowork_NumberOfRecruitment__c': clean_int(row.get('採用人数', '')),
            'Hellowork_NumberOfEmployee_Office__c': clean_int(row.get('従業員数就業場所', '')),
            'Hellowork_JobPublicationDate__c': clean_date(row.get('受付年月日（西暦）', '')),
            'Hellowork_JobClosedDate__c': clean_date(row.get('求人有効年月日（西暦）', '')),
            'Hellowork_DataImportDate__c': today,
            'Population__c': get_population(row.get('事業所所在地', ''), pop_map),
            'LeadSource': 'Other',
            'LeadSourceMemo__c': lead_source_memo,
            'Publish_ImportText__c': publish_import_text,
            'OwnerId': row.get('OwnerId', ''),
        }
        records.append(record)

    df_out = pd.DataFrame(records)

    # バリデーション
    before = len(df_out)
    df_out = df_out[df_out['Phone'].notna() & (df_out['Phone'] != '')]
    print(f'Phone必須チェック後: {len(df_out)}件（除外: {before - len(df_out)}件）')

    before = len(df_out)
    df_out = df_out[df_out['Company'].notna() & (df_out['Company'] != '')]
    print(f'Company必須チェック後: {len(df_out)}件（除外: {before - len(df_out)}件）')

    # 保存
    output_path = base_dir / f'data/output/hellowork/new_leads_{today_short}.csv'
    df_out.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f'')
    print(f'保存: {output_path}')
    print(f'最終件数: {len(df_out)}件')

    # 所有者別集計
    print()
    print('=== 所有者別集計 ===')
    owner_names = {v: k for k, v in OWNER_IDS.items()}
    for owner_id, count in df_out['OwnerId'].value_counts().items():
        print(f'  {owner_names.get(owner_id, owner_id)}: {count}件')

    # サンプル
    print()
    print('=== サンプル（1件目） ===')
    if len(df_out) > 0:
        for col, val in df_out.iloc[0].items():
            if val:
                print(f'  {col}: {str(val)[:80]}')


if __name__ == '__main__':
    main()
