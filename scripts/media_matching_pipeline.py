# -*- coding: utf-8 -*-
"""
有料媒体スクレイピングデータ突合パイプライン
PT・OT・STネット、ジョブポスターのデータをSalesforceと突合
"""

import pandas as pd
import numpy as np
import re
import os
from datetime import datetime
from pathlib import Path

# 出力ディレクトリ
OUTPUT_DIR = Path("data/output/media_matching")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def normalize_phone(phone):
    """電話番号を正規化（数字のみ10-11桁）"""
    if pd.isna(phone) or phone == '' or phone == 'nan':
        return None
    phone_str = str(phone).strip()
    # 数字のみ抽出
    digits = re.sub(r'\D', '', phone_str)
    # 10-11桁の場合のみ有効
    if len(digits) >= 10 and len(digits) <= 11:
        return digits
    return None

def load_scraping_data():
    """スクレイピングデータを読み込み"""
    print("=== スクレイピングデータ読み込み ===")

    # PT・OT・STネット
    df_pt = pd.read_excel('PT・OT・STネット_スクレイピングデータ.xlsx')
    print(f"PT・OT・STネット: {len(df_pt)}件")

    # 電話番号列を統合（複数列から取得）
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
                'company_name': str(row.get('会社情報', '')).split('\n')[0].strip(),  # 改行前の会社名
                'job_type': row.get('職 種', ''),
                'address': row.get('勤務地', ''),
                'contact_name': row.get('応募担当者名', ''),
                'phone_normalized': normalized,
                'url': row.get('URL', ''),
                'website': row.get('ホームページ', ''),
                'contact_info': '',
            })

    # 統合
    all_records = pt_records + jp_records
    df_all = pd.DataFrame(all_records)

    # 電話番号で重複除去（同じ電話番号は最初の1件を採用）
    df_all = df_all.drop_duplicates(subset=['phone_normalized'], keep='first')

    print(f"統合後（電話番号ユニーク）: {len(df_all)}件")
    print(f"  - PT・OT・STネット: {len(df_all[df_all['source'] == 'PT・OT・STネット'])}件")
    print(f"  - ジョブポスター: {len(df_all[df_all['source'] == 'ジョブポスター'])}件")

    return df_all

def load_salesforce_data():
    """Salesforceデータを読み込み、電話番号正規化"""
    print("\n=== Salesforceデータ読み込み ===")

    # Lead
    print("Lead読み込み中...")
    lead_cols = ['Id', 'Company', 'LastName', 'Phone', 'MobilePhone', 'Phone2__c', 'MobilePhone2__c',
                 'Street', 'City', 'State', 'PostalCode', 'Status', 'LeadSource', 'Description',
                 'ConvertedAccountId', 'IsConverted']
    df_lead = pd.read_csv('data/output/Lead_20260107_003026.csv',
                          usecols=lambda c: c in lead_cols,
                          dtype=str, encoding='utf-8')
    print(f"  Lead: {len(df_lead)}件")

    # Account
    print("Account読み込み中...")
    acc_cols = ['Id', 'Name', 'Phone', 'PersonMobilePhone', 'Phone2__c',
                'BillingStreet', 'BillingCity', 'BillingState', 'BillingPostalCode']
    df_acc = pd.read_csv('data/output/Account_20260107_003958.csv',
                         usecols=lambda c: c in acc_cols,
                         dtype=str, encoding='utf-8')
    print(f"  Account: {len(df_acc)}件")

    # Contact
    print("Contact読み込み中...")
    con_cols = ['Id', 'AccountId', 'LastName', 'Phone', 'MobilePhone', 'Phone2__c', 'MobilePhone2__c',
                'MailingStreet', 'MailingCity', 'MailingState', 'MailingPostalCode']
    df_con = pd.read_csv('data/output/Contact_20260107_004329.csv',
                         usecols=lambda c: c in con_cols,
                         dtype=str, encoding='utf-8')
    print(f"  Contact: {len(df_con)}件")

    # 成約先
    print("成約先読み込み中...")
    df_contract = pd.read_csv('data/output/contract_accounts_20260107_125913.csv',
                              dtype=str, encoding='utf-8')
    print(f"  成約先: {len(df_contract)}件")

    return df_lead, df_acc, df_con, df_contract

def build_phone_index(df_lead, df_acc, df_con, df_contract):
    """電話番号インデックスを構築"""
    print("\n=== 電話番号インデックス構築 ===")

    phone_to_records = {}  # phone -> list of (object_type, id, record)

    # 成約先電話番号セット（除外用）
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
                    break  # 1レコードにつき1回のみ登録
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

    print(f"ユニーク電話番号総数: {len(phone_to_records)}件")

    return phone_to_records, contract_phones

def match_records(df_scraping, phone_to_records, contract_phones):
    """突合処理"""
    print("\n=== 突合処理 ===")

    matched = []      # 既存レコードにマッチ
    new_leads = []    # 新規リード候補
    excluded = []     # 成約先除外

    for idx, row in df_scraping.iterrows():
        phone = row['phone_normalized']

        # 成約先チェック
        if phone in contract_phones:
            excluded.append({
                **row.to_dict(),
                'exclude_reason': '成約先電話番号'
            })
            continue

        # 既存レコードとの突合
        if phone in phone_to_records:
            records = phone_to_records[phone]
            # 優先順位: Lead > Account > Contact
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
            # 新規リード候補
            new_leads.append(row.to_dict())

    print(f"突合結果:")
    print(f"  - 既存マッチ: {len(matched)}件")
    print(f"  - 新規リード候補: {len(new_leads)}件")
    print(f"  - 成約先除外: {len(excluded)}件")

    return matched, new_leads, excluded

def generate_output(matched, new_leads, excluded):
    """出力ファイル生成"""
    print("\n=== 出力ファイル生成 ===")

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    # マッチ結果
    if matched:
        df_matched = pd.DataFrame(matched)
        matched_path = OUTPUT_DIR / f"matched_records_{timestamp}.csv"
        df_matched.to_csv(matched_path, index=False, encoding='utf-8-sig')
        print(f"既存マッチ: {matched_path}")

    # 新規リード
    if new_leads:
        df_new = pd.DataFrame(new_leads)
        new_path = OUTPUT_DIR / f"new_leads_{timestamp}.csv"
        df_new.to_csv(new_path, index=False, encoding='utf-8-sig')
        print(f"新規リード候補: {new_path}")

    # 除外リスト
    if excluded:
        df_excluded = pd.DataFrame(excluded)
        excluded_path = OUTPUT_DIR / f"excluded_records_{timestamp}.csv"
        df_excluded.to_csv(excluded_path, index=False, encoding='utf-8-sig')
        print(f"成約先除外: {excluded_path}")

    return timestamp

def main():
    print("=" * 60)
    print("有料媒体スクレイピングデータ突合パイプライン")
    print("=" * 60)

    # 1. スクレイピングデータ読み込み
    df_scraping = load_scraping_data()

    # 2. Salesforceデータ読み込み
    df_lead, df_acc, df_con, df_contract = load_salesforce_data()

    # 3. 電話番号インデックス構築
    phone_to_records, contract_phones = build_phone_index(df_lead, df_acc, df_con, df_contract)

    # 4. 突合処理
    matched, new_leads, excluded = match_records(df_scraping, phone_to_records, contract_phones)

    # 5. 出力
    timestamp = generate_output(matched, new_leads, excluded)

    print("\n" + "=" * 60)
    print("処理完了")
    print("=" * 60)

    return matched, new_leads, excluded

if __name__ == "__main__":
    main()
