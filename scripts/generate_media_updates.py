# -*- coding: utf-8 -*-
"""
有料媒体データ更新CSV生成スクリプト
Lead/Account の Paid_* カラムを更新
"""

import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path

OUTPUT_DIR = Path("data/output/media_matching")
TODAY = datetime.now().strftime('%Y-%m-%d')
TIMESTAMP = datetime.now().strftime('%Y%m%d_%H%M%S')

def load_matched_data():
    """マッチ結果を読み込み"""
    df = pd.read_csv('data/output/media_matching/matched_records_20260107_200818.csv',
                     encoding='utf-8-sig', dtype=str)
    return df

def generate_lead_updates(df_matched):
    """Lead更新用CSV生成"""
    print("=== Lead更新データ生成 ===")

    # Leadマッチ分のみ
    df_lead_match = df_matched[df_matched['match_object'] == 'Lead'].copy()
    print(f"Leadマッチ件数: {len(df_lead_match)}")

    # 既存Leadデータ読み込み
    lead_cols = ['Id', 'Company', 'Phone', 'Description',
                 'Paid_Media__c', 'Paid_JobTitle__c', 'Paid_RecruitmentType__c',
                 'Paid_EmploymentType__c', 'Paid_URL__c', 'Paid_DataExportDate__c',
                 'Paid_DataSource__c', 'Paid_Memo__c']
    df_lead = pd.read_csv('data/output/Lead_20260107_003026.csv',
                          usecols=lambda c: c in lead_cols,
                          dtype=str, encoding='utf-8')

    lead_ids = df_lead_match['match_id'].tolist()
    df_lead_target = df_lead[df_lead['Id'].isin(lead_ids)].copy()

    # マッチデータとマージ
    df_merged = df_lead_target.merge(
        df_lead_match[['match_id', 'source', 'job_type', 'employment_type', 'url', 'company_name', 'address']],
        left_on='Id', right_on='match_id', how='left'
    )

    # 更新用データフレーム作成
    updates = []
    for _, row in df_merged.iterrows():
        update_row = {'Id': row['Id']}
        updated = False

        # Paid_Media__c（空欄の場合のみ）
        if pd.isna(row.get('Paid_Media__c')) or row.get('Paid_Media__c') == '':
            update_row['Paid_Media__c'] = row['source']
            updated = True

        # Paid_JobTitle__c（空欄の場合のみ）
        if pd.isna(row.get('Paid_JobTitle__c')) or row.get('Paid_JobTitle__c') == '':
            if pd.notna(row.get('job_type')) and row.get('job_type') != '':
                update_row['Paid_JobTitle__c'] = row['job_type']
                updated = True

        # Paid_EmploymentType__c（空欄の場合のみ）
        if pd.isna(row.get('Paid_EmploymentType__c')) or row.get('Paid_EmploymentType__c') == '':
            if pd.notna(row.get('employment_type')) and row.get('employment_type') != '':
                update_row['Paid_EmploymentType__c'] = row['employment_type']
                updated = True

        # Paid_URL__c（空欄の場合のみ）
        if pd.isna(row.get('Paid_URL__c')) or row.get('Paid_URL__c') == '':
            if pd.notna(row.get('url')) and row.get('url') != '':
                update_row['Paid_URL__c'] = row['url']
                updated = True

        # Paid_DataExportDate__c（常に更新）
        update_row['Paid_DataExportDate__c'] = TODAY
        updated = True

        # Paid_DataSource__c（空欄の場合のみ）
        if pd.isna(row.get('Paid_DataSource__c')) or row.get('Paid_DataSource__c') == '':
            update_row['Paid_DataSource__c'] = row['source']
            updated = True

        # Description追記（常に）
        new_desc = f"【{row['source']}】{TODAY}取得\n職種: {row.get('job_type', 'N/A')}\nURL: {row.get('url', 'N/A')}"
        existing_desc = row.get('Description', '')
        if pd.isna(existing_desc):
            existing_desc = ''
        # 既に同じ媒体の情報がない場合のみ追記
        if row['source'] not in str(existing_desc):
            update_row['Description'] = f"{existing_desc}\n\n{new_desc}".strip()
            updated = True

        if updated:
            updates.append(update_row)

    df_updates = pd.DataFrame(updates)

    # CSV出力
    output_path = OUTPUT_DIR / f"lead_updates_{TIMESTAMP}.csv"
    df_updates.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"Lead更新CSV出力: {output_path}")
    print(f"更新対象: {len(df_updates)}件")

    # 更新内容サマリー
    print("\n更新フィールド内訳:")
    for col in df_updates.columns:
        if col != 'Id':
            non_null = df_updates[col].notna().sum()
            print(f"  {col}: {non_null}件")

    return df_updates, output_path

def generate_account_updates(df_matched):
    """Account更新用CSV生成"""
    print("\n=== Account更新データ生成 ===")

    # Account/Contactマッチ分
    df_acc_match = df_matched[df_matched['match_object'].isin(['Account', 'Contact'])].copy()
    print(f"Account/Contactマッチ件数: {len(df_acc_match)}")

    # Accountマッチ分のID取得
    acc_direct = df_acc_match[df_acc_match['match_object'] == 'Account']['match_id'].tolist()

    # Contactマッチ分のAccountId取得
    con_ids = df_acc_match[df_acc_match['match_object'] == 'Contact']['match_id'].tolist()
    if con_ids:
        df_contact = pd.read_csv('data/output/Contact_20260107_004329.csv',
                                  usecols=['Id', 'AccountId'], dtype=str, encoding='utf-8')
        acc_from_contact = df_contact[df_contact['Id'].isin(con_ids)]['AccountId'].dropna().tolist()
    else:
        acc_from_contact = []

    all_acc_ids = list(set(acc_direct + acc_from_contact))
    print(f"Account ID総数: {len(all_acc_ids)}")

    # 既存Accountデータ読み込み
    acc_cols = ['Id', 'Name', 'Phone', 'Description',
                'Paid_Media__c', 'Paid_JobTitle__c', 'Paid_RecruitmentType__c',
                'Paid_EmploymentType__c', 'Paid_URL__c', 'Paid_DataExportDate__c',
                'Paid_DataSource__c', 'Paid_Memo__c']
    df_acc = pd.read_csv('data/output/Account_20260107_003958.csv',
                         usecols=lambda c: c in acc_cols,
                         dtype=str, encoding='utf-8')
    df_acc_target = df_acc[df_acc['Id'].isin(all_acc_ids)].copy()
    print(f"Account取得: {len(df_acc_target)}件")

    # マッチデータを電話番号で逆引き（Account直接マッチの場合）
    # 簡易的にAccountIDでマッチング
    acc_to_source = {}
    for _, row in df_acc_match.iterrows():
        if row['match_object'] == 'Account':
            acc_to_source[row['match_id']] = row

    # Contact経由の場合
    for _, row in df_acc_match.iterrows():
        if row['match_object'] == 'Contact':
            con_id = row['match_id']
            if con_ids:
                acc_id_row = df_contact[df_contact['Id'] == con_id]
                if len(acc_id_row) > 0 and pd.notna(acc_id_row.iloc[0]['AccountId']):
                    acc_id = acc_id_row.iloc[0]['AccountId']
                    if acc_id not in acc_to_source:
                        acc_to_source[acc_id] = row

    # 更新データ生成
    updates = []
    for _, acc_row in df_acc_target.iterrows():
        acc_id = acc_row['Id']
        if acc_id not in acc_to_source:
            continue

        source_row = acc_to_source[acc_id]
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
        update_row['Paid_DataExportDate__c'] = TODAY
        updated = True

        # Paid_DataSource__c
        if pd.isna(acc_row.get('Paid_DataSource__c')) or acc_row.get('Paid_DataSource__c') == '':
            update_row['Paid_DataSource__c'] = source_row['source']
            updated = True

        # Description追記
        new_desc = f"【{source_row['source']}】{TODAY}取得\n職種: {source_row.get('job_type', 'N/A')}\nURL: {source_row.get('url', 'N/A')}"
        existing_desc = acc_row.get('Description', '')
        if pd.isna(existing_desc):
            existing_desc = ''
        if source_row['source'] not in str(existing_desc):
            update_row['Description'] = f"{existing_desc}\n\n{new_desc}".strip()
            updated = True

        if updated:
            updates.append(update_row)

    df_updates = pd.DataFrame(updates)

    # CSV出力
    output_path = OUTPUT_DIR / f"account_updates_{TIMESTAMP}.csv"
    df_updates.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"Account更新CSV出力: {output_path}")
    print(f"更新対象: {len(df_updates)}件")

    # 更新内容サマリー
    print("\n更新フィールド内訳:")
    for col in df_updates.columns:
        if col != 'Id':
            non_null = df_updates[col].notna().sum()
            print(f"  {col}: {non_null}件")

    return df_updates, output_path

def main():
    print("=" * 60)
    print("有料媒体データ更新CSV生成")
    print("=" * 60)

    # マッチ結果読み込み
    df_matched = load_matched_data()

    # Lead更新CSV生成
    df_lead_updates, lead_path = generate_lead_updates(df_matched)

    # Account更新CSV生成
    df_acc_updates, acc_path = generate_account_updates(df_matched)

    print("\n" + "=" * 60)
    print("生成完了")
    print("=" * 60)
    print(f"Lead更新: {lead_path}")
    print(f"Account更新: {acc_path}")

    return df_lead_updates, df_acc_updates

if __name__ == "__main__":
    main()
