# -*- coding: utf-8 -*-
"""
有料媒体スクレイピングデータ突合パイプライン v2
電話済みリストの除外を追加
"""

import pandas as pd
import numpy as np
import re
import os
from datetime import datetime
from pathlib import Path

OUTPUT_DIR = Path("data/output/media_matching")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def normalize_phone(phone):
    """電話番号を正規化（数字のみ10-11桁）"""
    if pd.isna(phone) or phone == '' or phone == 'nan':
        return None
    phone_str = str(phone).strip()
    digits = re.sub(r'\D', '', phone_str)
    if len(digits) >= 10 and len(digits) <= 11:
        return digits
    return None

def load_already_called_phones():
    """電話済みリストから電話番号を抽出（全シート対応）"""
    print("=== 電話済みリスト読み込み ===")

    xlsx = pd.ExcelFile('C:/Users/fuji1/Downloads/媒体掲載中のリスト.xlsx')
    print(f"シート数: {len(xlsx.sheet_names)} ({', '.join(xlsx.sheet_names)})")

    phone_pattern = re.compile(r'0\d{1,4}[-\s]?\d{1,4}[-\s]?\d{3,4}|0\d{9,10}')

    called_phones = set()
    for sheet in xlsx.sheet_names:
        df = pd.read_excel(xlsx, sheet_name=sheet, header=None)
        sheet_count = 0
        for col in df.columns:
            for val in df[col].astype(str):
                matches = phone_pattern.findall(val)
                for m in matches:
                    digits = re.sub(r'\D', '', m)
                    if 10 <= len(digits) <= 11:
                        called_phones.add(digits)
                        sheet_count += 1
        print(f"  {sheet}: {sheet_count}件")

    print(f"電話済み電話番号（合計ユニーク）: {len(called_phones)}件")
    return called_phones

def load_scraping_data():
    """スクレイピングデータを読み込み"""
    print("\n=== スクレイピングデータ読み込み ===")

    # PT・OT・STネット
    df_pt = pd.read_excel('PT・OT・STネット_スクレイピングデータ.xlsx')
    print(f"PT・OT・STネット: {len(df_pt)}件")

    phone_cols = [c for c in df_pt.columns if '電話番号' in c]
    pt_records = []
    for idx, row in df_pt.iterrows():
        phones = set()
        for col in phone_cols:
            normalized = normalize_phone(row[col])
            if normalized:
                phones.add(normalized)

        for phone in phones:
            pt_records.append({
                'source': 'PT・OT・STネット',
                'company_name': row.get('事業所名', ''),
                'job_type': row.get('募集職種', ''),
                'address': row.get('所在地・勤務地', ''),
                'contact_name': row.get('担当者', ''),
                'phone_normalized': phone,
                'url': row.get('URL', ''),
                'employment_type': row.get('雇用形態', ''),
                'contact_info': row.get('連絡先', ''),
            })

    # ジョブポスター
    df_jp = pd.read_excel('ジョブポスター.xlsx')
    print(f"ジョブポスター: {len(df_jp)}件")

    jp_records = []
    for idx, row in df_jp.iterrows():
        normalized = normalize_phone(row.get('電話番号', ''))
        if normalized:
            jp_records.append({
                'source': 'ジョブポスター',
                'company_name': str(row.get('会社情報', '')).split('\n')[0].strip(),
                'job_type': row.get('職 種', ''),
                'address': row.get('勤務地', ''),
                'contact_name': row.get('応募担当者名', ''),
                'phone_normalized': normalized,
                'url': row.get('URL', ''),
                'website': row.get('ホームページ', ''),
                'contact_info': '',
            })

    all_records = pt_records + jp_records
    df_all = pd.DataFrame(all_records)
    df_all = df_all.drop_duplicates(subset=['phone_normalized'], keep='first')

    print(f"統合後（電話番号ユニーク）: {len(df_all)}件")

    return df_all

def load_salesforce_data():
    """Salesforceデータを読み込み"""
    print("\n=== Salesforceデータ読み込み ===")

    lead_cols = ['Id', 'Company', 'LastName', 'Phone', 'MobilePhone', 'Phone2__c', 'MobilePhone2__c',
                 'Street', 'City', 'State', 'PostalCode', 'Status', 'LeadSource', 'Description',
                 'ConvertedAccountId', 'IsConverted',
                 'Paid_Media__c', 'Paid_JobTitle__c', 'Paid_URL__c', 'Paid_DataExportDate__c', 'Paid_DataSource__c',
                 'Paid_EmploymentType__c', 'Paid_Memo__c']
    df_lead = pd.read_csv('data/output/Lead_20260107_003026.csv',
                          usecols=lambda c: c in lead_cols,
                          dtype=str, encoding='utf-8')
    print(f"  Lead: {len(df_lead)}件")

    acc_cols = ['Id', 'Name', 'Phone', 'PersonMobilePhone', 'Phone2__c',
                'BillingStreet', 'BillingCity', 'BillingState', 'BillingPostalCode', 'Description',
                'Paid_Media__c', 'Paid_JobTitle__c', 'Paid_URL__c', 'Paid_DataExportDate__c', 'Paid_DataSource__c',
                'Paid_EmploymentType__c', 'Paid_Memo__c']
    df_acc = pd.read_csv('data/output/Account_20260107_003958.csv',
                         usecols=lambda c: c in acc_cols,
                         dtype=str, encoding='utf-8')
    print(f"  Account: {len(df_acc)}件")

    con_cols = ['Id', 'AccountId', 'LastName', 'Phone', 'MobilePhone', 'Phone2__c', 'MobilePhone2__c']
    df_con = pd.read_csv('data/output/Contact_20260107_004329.csv',
                         usecols=lambda c: c in con_cols,
                         dtype=str, encoding='utf-8')
    print(f"  Contact: {len(df_con)}件")

    df_contract = pd.read_csv('data/output/contract_accounts_20260107_125913.csv',
                              dtype=str, encoding='utf-8')
    print(f"  成約先: {len(df_contract)}件")

    return df_lead, df_acc, df_con, df_contract

def build_phone_index(df_lead, df_acc, df_con, df_contract):
    """電話番号インデックスを構築"""
    print("\n=== 電話番号インデックス構築 ===")

    phone_to_records = {}

    # 成約先電話番号セット
    contract_phones = set()
    for _, row in df_contract.iterrows():
        normalized = normalize_phone(row.get('Phone', ''))
        if normalized:
            contract_phones.add(normalized)
    print(f"成約先電話番号: {len(contract_phones)}件")

    # Lead
    lead_phone_cols = ['Phone', 'MobilePhone', 'Phone2__c', 'MobilePhone2__c']
    lead_count = 0
    for _, row in df_lead.iterrows():
        for col in lead_phone_cols:
            if col in row:
                normalized = normalize_phone(row[col])
                if normalized:
                    if normalized not in phone_to_records:
                        phone_to_records[normalized] = []
                    phone_to_records[normalized].append(('Lead', row['Id'], row))
                    lead_count += 1
                    break
    print(f"Lead電話番号登録: {lead_count}件")

    # Account
    acc_phone_cols = ['Phone', 'PersonMobilePhone', 'Phone2__c']
    acc_count = 0
    for _, row in df_acc.iterrows():
        for col in acc_phone_cols:
            if col in row:
                normalized = normalize_phone(row[col])
                if normalized:
                    if normalized not in phone_to_records:
                        phone_to_records[normalized] = []
                    phone_to_records[normalized].append(('Account', row['Id'], row))
                    acc_count += 1
                    break
    print(f"Account電話番号登録: {acc_count}件")

    # Contact
    con_phone_cols = ['Phone', 'MobilePhone', 'Phone2__c', 'MobilePhone2__c']
    con_count = 0
    for _, row in df_con.iterrows():
        for col in con_phone_cols:
            if col in row:
                normalized = normalize_phone(row[col])
                if normalized:
                    if normalized not in phone_to_records:
                        phone_to_records[normalized] = []
                    phone_to_records[normalized].append(('Contact', row['Id'], row))
                    con_count += 1
                    break
    print(f"Contact電話番号登録: {con_count}件")

    return phone_to_records, contract_phones

def match_records(df_scraping, phone_to_records, contract_phones, called_phones):
    """突合処理（電話済みリスト除外追加）"""
    print("\n=== 突合処理 ===")

    matched = []
    new_leads = []
    excluded_contract = []
    excluded_called = []

    for idx, row in df_scraping.iterrows():
        phone = row['phone_normalized']

        # 成約先チェック
        if phone in contract_phones:
            excluded_contract.append({
                **row.to_dict(),
                'exclude_reason': '成約先電話番号'
            })
            continue

        # 電話済みチェック
        if phone in called_phones:
            excluded_called.append({
                **row.to_dict(),
                'exclude_reason': '電話済み'
            })
            continue

        # 既存レコードとの突合
        if phone in phone_to_records:
            records = phone_to_records[phone]
            best_match = None
            for obj_type, obj_id, record in records:
                if obj_type == 'Lead' and (best_match is None or best_match[0] != 'Lead'):
                    best_match = (obj_type, obj_id, record)
                elif obj_type == 'Account' and best_match is None:
                    best_match = (obj_type, obj_id, record)
                elif obj_type == 'Contact' and best_match is None:
                    best_match = (obj_type, obj_id, record)

            if best_match:
                matched.append({
                    **row.to_dict(),
                    'match_object': best_match[0],
                    'match_id': best_match[1],
                })
        else:
            new_leads.append(row.to_dict())

    print(f"突合結果:")
    print(f"  - 既存マッチ: {len(matched)}件")
    print(f"  - 新規リード候補: {len(new_leads)}件")
    print(f"  - 成約先除外: {len(excluded_contract)}件")
    print(f"  - 電話済み除外: {len(excluded_called)}件")

    return matched, new_leads, excluded_contract, excluded_called

def generate_update_csvs(matched, df_lead, df_acc, df_con):
    """更新用CSV生成"""
    print("\n=== 更新用CSV生成 ===")

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    today = datetime.now().strftime('%Y-%m-%d')

    # Leadマッチ分
    df_matched = pd.DataFrame(matched)
    lead_matched = df_matched[df_matched['match_object'] == 'Lead']

    lead_updates = []
    for _, row in lead_matched.iterrows():
        lead_id = row['match_id']
        lead_row = df_lead[df_lead['Id'] == lead_id]
        if len(lead_row) == 0:
            continue
        lead_row = lead_row.iloc[0]

        update_row = {'Id': lead_id}
        updated = False

        # Paid_Media__c（空欄の場合のみ）
        if pd.isna(lead_row.get('Paid_Media__c')) or lead_row.get('Paid_Media__c') == '':
            update_row['Paid_Media__c'] = row['source']
            updated = True

        # Paid_JobTitle__c
        if pd.isna(lead_row.get('Paid_JobTitle__c')) or lead_row.get('Paid_JobTitle__c') == '':
            if pd.notna(row.get('job_type')) and row.get('job_type') != '':
                update_row['Paid_JobTitle__c'] = row['job_type']
                updated = True

        # Paid_EmploymentType__c
        if pd.isna(lead_row.get('Paid_EmploymentType__c')) or lead_row.get('Paid_EmploymentType__c') == '':
            if pd.notna(row.get('employment_type')) and row.get('employment_type') != '':
                update_row['Paid_EmploymentType__c'] = row['employment_type']
                updated = True

        # Paid_URL__c
        if pd.isna(lead_row.get('Paid_URL__c')) or lead_row.get('Paid_URL__c') == '':
            if pd.notna(row.get('url')) and row.get('url') != '':
                update_row['Paid_URL__c'] = row['url']
                updated = True

        # Paid_DataExportDate__c（常に更新）
        update_row['Paid_DataExportDate__c'] = today

        # Paid_DataSource__c
        if pd.isna(lead_row.get('Paid_DataSource__c')) or lead_row.get('Paid_DataSource__c') == '':
            update_row['Paid_DataSource__c'] = row['source']
            updated = True

        # Description追記
        new_desc = f"【{row['source']}】{today}取得\n職種: {row.get('job_type', 'N/A')}\nURL: {row.get('url', 'N/A')}"
        existing_desc = lead_row.get('Description', '')
        if pd.isna(existing_desc):
            existing_desc = ''
        if row['source'] not in str(existing_desc):
            update_row['Description'] = f"{existing_desc}\n\n{new_desc}".strip()
            updated = True

        if updated:
            lead_updates.append(update_row)

    df_lead_updates = pd.DataFrame(lead_updates)
    lead_path = OUTPUT_DIR / f"lead_updates_v2_{timestamp}.csv"
    df_lead_updates.to_csv(lead_path, index=False, encoding='utf-8-sig')
    print(f"Lead更新: {lead_path} ({len(df_lead_updates)}件)")

    # Account/Contactマッチ分
    acc_con_matched = df_matched[df_matched['match_object'].isin(['Account', 'Contact'])]

    # Contact→AccountIDマッピング
    acc_to_source = {}
    for _, row in acc_con_matched.iterrows():
        if row['match_object'] == 'Account':
            acc_to_source[row['match_id']] = row
        elif row['match_object'] == 'Contact':
            con_row = df_con[df_con['Id'] == row['match_id']]
            if len(con_row) > 0 and pd.notna(con_row.iloc[0].get('AccountId')):
                acc_id = con_row.iloc[0]['AccountId']
                if acc_id not in acc_to_source:
                    acc_to_source[acc_id] = row

    acc_updates = []
    for acc_id, source_row in acc_to_source.items():
        acc_row = df_acc[df_acc['Id'] == acc_id]
        if len(acc_row) == 0:
            continue
        acc_row = acc_row.iloc[0]

        update_row = {'Id': acc_id}
        updated = False

        # Paid_Media__c
        if pd.isna(acc_row.get('Paid_Media__c')) or acc_row.get('Paid_Media__c') == '':
            update_row['Paid_Media__c'] = source_row['source']
            updated = True

        # Paid_JobTitle__c
        if pd.isna(acc_row.get('Paid_JobTitle__c')) or acc_row.get('Paid_JobTitle__c') == '':
            if pd.notna(source_row.get('job_type')) and source_row.get('job_type') != '':
                update_row['Paid_JobTitle__c'] = source_row['job_type']
                updated = True

        # Paid_URL__c
        if pd.isna(acc_row.get('Paid_URL__c')) or acc_row.get('Paid_URL__c') == '':
            if pd.notna(source_row.get('url')) and source_row.get('url') != '':
                update_row['Paid_URL__c'] = source_row['url']
                updated = True

        # Paid_DataExportDate__c
        update_row['Paid_DataExportDate__c'] = today

        # Paid_DataSource__c
        if pd.isna(acc_row.get('Paid_DataSource__c')) or acc_row.get('Paid_DataSource__c') == '':
            update_row['Paid_DataSource__c'] = source_row['source']
            updated = True

        # Description追記
        new_desc = f"【{source_row['source']}】{today}取得\n職種: {source_row.get('job_type', 'N/A')}\nURL: {source_row.get('url', 'N/A')}"
        existing_desc = acc_row.get('Description', '')
        if pd.isna(existing_desc):
            existing_desc = ''
        if source_row['source'] not in str(existing_desc):
            update_row['Description'] = f"{existing_desc}\n\n{new_desc}".strip()
            updated = True

        if updated:
            acc_updates.append(update_row)

    df_acc_updates = pd.DataFrame(acc_updates)
    acc_path = OUTPUT_DIR / f"account_updates_v2_{timestamp}.csv"
    df_acc_updates.to_csv(acc_path, index=False, encoding='utf-8-sig')
    print(f"Account更新: {acc_path} ({len(df_acc_updates)}件)")

    return lead_path, acc_path

def generate_output(matched, new_leads, excluded_contract, excluded_called):
    """出力ファイル生成"""
    print("\n=== 出力ファイル生成 ===")

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    if matched:
        df_matched = pd.DataFrame(matched)
        path = OUTPUT_DIR / f"matched_records_v2_{timestamp}.csv"
        df_matched.to_csv(path, index=False, encoding='utf-8-sig')
        print(f"既存マッチ: {path}")

    if new_leads:
        df_new = pd.DataFrame(new_leads)
        path = OUTPUT_DIR / f"new_leads_v2_{timestamp}.csv"
        df_new.to_csv(path, index=False, encoding='utf-8-sig')
        print(f"新規リード候補: {path}")

    if excluded_contract:
        df = pd.DataFrame(excluded_contract)
        path = OUTPUT_DIR / f"excluded_contract_v2_{timestamp}.csv"
        df.to_csv(path, index=False, encoding='utf-8-sig')
        print(f"成約先除外: {path}")

    if excluded_called:
        df = pd.DataFrame(excluded_called)
        path = OUTPUT_DIR / f"excluded_called_v2_{timestamp}.csv"
        df.to_csv(path, index=False, encoding='utf-8-sig')
        print(f"電話済み除外: {path}")

    return timestamp

def main():
    print("=" * 60)
    print("有料媒体スクレイピングデータ突合パイプライン v2")
    print("（電話済みリスト除外対応）")
    print("=" * 60)

    # 電話済みリスト読み込み
    called_phones = load_already_called_phones()

    # スクレイピングデータ読み込み
    df_scraping = load_scraping_data()

    # Salesforceデータ読み込み
    df_lead, df_acc, df_con, df_contract = load_salesforce_data()

    # 電話番号インデックス構築
    phone_to_records, contract_phones = build_phone_index(df_lead, df_acc, df_con, df_contract)

    # 突合処理
    matched, new_leads, excluded_contract, excluded_called = match_records(
        df_scraping, phone_to_records, contract_phones, called_phones
    )

    # 出力
    timestamp = generate_output(matched, new_leads, excluded_contract, excluded_called)

    # 更新CSV生成
    generate_update_csvs(matched, df_lead, df_acc, df_con)

    print("\n" + "=" * 60)
    print("処理完了")
    print("=" * 60)

    return matched, new_leads, excluded_contract, excluded_called

if __name__ == "__main__":
    main()
