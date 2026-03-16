"""
有料媒体インポート リトライ処理 2026-03-10
- 新規リード残り6件: LeadSourceMemo__c 切り詰め
- Lead更新残り: LeadSourceMemo__c切り詰め + LastName除外 + コンバート済み除外 + Paid_EmploymentType__c切り詰め
- Accountレポート作成
"""
import pandas as pd
import sys
import os
import io
import re
import time
import requests
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))
sys.stdout.reconfigure(encoding='utf-8')

from api.salesforce_client import SalesforceClient

BASE_DIR = Path(__file__).parent.parent
OUTPUT_DIR = BASE_DIR / 'data/output/media_matching'
PROCESS_DATE = '2026-03-10'

FAILED_NEW_FILE = OUTPUT_DIR / 'failed_new_leads_20260310.csv'
FAILED_LU_FILE = OUTPUT_DIR / 'failed_lead_updates_20260310.csv'
CREATED_IDS_FILE = OUTPUT_DIR / 'created_lead_ids_20260310.csv'

# 255文字制限フィールド
STRING_FIELDS_255 = [
    'LeadSourceMemo__c', 'Paid_EmploymentType__c', 'Paid_Industry__c',
    'Paid_RecruitmentType__c', 'Paid_JobTitle__c', 'Website'
]

def zen_to_han(text):
    """全角数字を半角に変換"""
    if not isinstance(text, str):
        return text
    return re.sub(r'[０-９]', lambda m: chr(ord(m.group()) - 0xFEE0), text)


def truncate_fields(df):
    """255文字制限フィールドを切り詰め"""
    for col in STRING_FIELDS_255:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda x: (x[:252] + '...') if isinstance(x, str) and len(x) > 255 else x
            )
    return df


def bulk_operation(client, object_name, operation, df, label=''):
    """Bulk API 2.0で操作を実行"""
    headers = client._get_headers()
    api_version = 'v59.0'
    job_url = f'{client.instance_url}/services/data/{api_version}/jobs/ingest'

    job_payload = {
        'object': object_name,
        'operation': operation,
        'contentType': 'CSV',
        'lineEnding': 'CRLF',
    }
    resp = requests.post(job_url, headers=headers, json=job_payload)
    resp.raise_for_status()
    job_id = resp.json()['id']
    print(f'  ジョブ作成: {job_id}')

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, lineterminator='\r\n')
    csv_data = csv_buffer.getvalue()

    upload_url = f'{job_url}/{job_id}/batches'
    upload_headers = {**headers, 'Content-Type': 'text/csv'}
    resp = requests.put(upload_url, headers=upload_headers, data=csv_data.encode('utf-8'))
    resp.raise_for_status()
    print(f'  アップロード: {len(df)}件')

    close_url = f'{job_url}/{job_id}'
    resp = requests.patch(close_url, headers=headers, json={'state': 'UploadComplete'})
    resp.raise_for_status()

    for i in range(60):
        time.sleep(5)
        resp = requests.get(close_url, headers=headers)
        resp.raise_for_status()
        state = resp.json()['state']
        processed = resp.json().get('numberRecordsProcessed', 0)
        failed = resp.json().get('numberRecordsFailed', 0)
        print(f'  状態: {state} (処理済: {processed}, 失敗: {failed})')
        if state in ('JobComplete', 'Failed', 'Aborted'):
            break

    result = resp.json()

    success_ids = []
    success_url = f'{job_url}/{job_id}/successfulResults'
    resp = requests.get(success_url, headers=headers)
    if resp.text.strip():
        lines = resp.text.strip().split('\n')
        if len(lines) > 1:
            reader = pd.read_csv(io.StringIO(resp.text), dtype=str)
            if 'sf__Id' in reader.columns:
                success_ids = reader['sf__Id'].tolist()

    failed_records = []
    fail_url = f'{job_url}/{job_id}/failedResults'
    resp = requests.get(fail_url, headers=headers)
    if resp.text.strip():
        lines = resp.text.strip().split('\n')
        if len(lines) > 1:
            fail_reader = pd.read_csv(io.StringIO(resp.text), dtype=str)
            failed_records = fail_reader.to_dict('records')

    return {
        'job_id': job_id,
        'state': result['state'],
        'total': result.get('numberRecordsProcessed', 0),
        'failed': result.get('numberRecordsFailed', 0),
        'success_ids': success_ids,
        'failed_records': failed_records,
    }


def create_report(client, name, report_type, columns, filters):
    """Salesforceレポート作成"""
    headers = client._get_headers()
    api_version = 'v59.0'
    report_metadata = {
        'reportMetadata': {
            'name': name,
            'reportFormat': 'TABULAR',
            'reportType': {'type': report_type},
            'detailColumns': columns,
            'reportFilters': filters,
        }
    }
    url = f'{client.instance_url}/services/data/{api_version}/analytics/reports'
    resp = requests.post(url, headers=headers, json=report_metadata)
    resp.raise_for_status()
    report_id = resp.json()['reportMetadata']['id']
    return report_id


def main():
    print('=' * 70)
    print(f'有料媒体 リトライ処理 {PROCESS_DATE}')
    print('=' * 70)

    client = SalesforceClient()
    client.authenticate()
    print('SF認証成功')

    # === Step 1: 新規リード残り ===
    print('\n--- Step 1: 新規リード リトライ ---')
    df_new_fail = pd.read_csv(FAILED_NEW_FILE, encoding='utf-8-sig', dtype=str)
    # sf__Id, sf__Error 列を除外
    drop_cols = [c for c in ['sf__Id', 'sf__Error'] if c in df_new_fail.columns]
    df_new = df_new_fail.drop(columns=drop_cols)

    # フィールド切り詰め
    df_new = truncate_fields(df_new)

    # 全角数字変換
    if 'Paid_NumberOfRecruitment__c' in df_new.columns:
        df_new['Paid_NumberOfRecruitment__c'] = df_new['Paid_NumberOfRecruitment__c'].apply(zen_to_han)

    print(f'  対象: {len(df_new)}件')
    result_new = bulk_operation(client, 'Lead', 'insert', df_new, '新規リード リトライ')
    print(f'  成功: {len(result_new["success_ids"])}件, 失敗: {result_new["failed"]}件')

    if result_new['success_ids']:
        # 既存IDファイルに追記
        existing_ids = pd.read_csv(CREATED_IDS_FILE, dtype=str)
        new_ids = pd.DataFrame({'Id': result_new['success_ids']})
        combined = pd.concat([existing_ids, new_ids], ignore_index=True)
        combined.to_csv(CREATED_IDS_FILE, index=False, encoding='utf-8-sig')
        print(f'  IDファイル更新: {len(combined)}件（+{len(result_new["success_ids"])}件）')

    if result_new['failed_records']:
        print(f'  まだ失敗:')
        for rec in result_new['failed_records'][:5]:
            print(f'    {rec.get("sf__Error", "unknown")[:100]}')

    # === Step 2: Lead更新 リトライ ===
    print('\n--- Step 2: Lead更新 リトライ ---')
    df_lu_fail = pd.read_csv(FAILED_LU_FILE, encoding='utf-8-sig', dtype=str)

    # エラー種別で分類
    converted = df_lu_fail[df_lu_fail['sf__Error'].str.contains('CANNOT_UPDATE_CONVERTED', na=False)]
    lastname_err = df_lu_fail[df_lu_fail['sf__Error'].str.contains('FIELD_CUSTOM_VALIDATION_EXCEPTION', na=False)]
    other_fail = df_lu_fail[
        ~df_lu_fail['sf__Error'].str.contains('CANNOT_UPDATE_CONVERTED', na=False) &
        ~df_lu_fail['sf__Error'].str.contains('FIELD_CUSTOM_VALIDATION_EXCEPTION', na=False)
    ]

    print(f'  コンバート済み（スキップ）: {len(converted)}件')
    print(f'  LastNameバリデーション: {len(lastname_err)}件')
    print(f'  その他エラー: {len(other_fail)}件')

    # LastNameエラー分: LastName列除外して再投入
    if len(lastname_err) > 0:
        print('\n  --- LastNameエラー分 リトライ（LastName除外）---')
        df_ln = lastname_err.drop(columns=[c for c in ['sf__Id', 'sf__Error'] if c in lastname_err.columns])
        if 'LastName' in df_ln.columns:
            df_ln = df_ln.drop(columns=['LastName'])
        df_ln = truncate_fields(df_ln)
        if 'Paid_NumberOfRecruitment__c' in df_ln.columns:
            df_ln['Paid_NumberOfRecruitment__c'] = df_ln['Paid_NumberOfRecruitment__c'].apply(zen_to_han)

        print(f'  対象: {len(df_ln)}件')
        result_ln = bulk_operation(client, 'Lead', 'update', df_ln, 'LastName除外リトライ')
        print(f'  成功: {len(result_ln["success_ids"])}件, 失敗: {result_ln["failed"]}件')
        if result_ln['failed_records']:
            for rec in result_ln['failed_records'][:3]:
                print(f'    {rec.get("sf__Error", "unknown")[:100]}')

    # その他エラー分: フィールド切り詰めして再投入
    if len(other_fail) > 0:
        print('\n  --- その他エラー分 リトライ ---')
        df_ot = other_fail.drop(columns=[c for c in ['sf__Id', 'sf__Error'] if c in other_fail.columns])
        df_ot = truncate_fields(df_ot)
        if 'Paid_NumberOfRecruitment__c' in df_ot.columns:
            df_ot['Paid_NumberOfRecruitment__c'] = df_ot['Paid_NumberOfRecruitment__c'].apply(zen_to_han)

        # LastName列にスペース含むか確認して除外
        if 'LastName' in df_ot.columns:
            has_space = df_ot['LastName'].str.contains(r'[\s　]', na=False, regex=True)
            if has_space.any():
                print(f'  LastNameにスペース含む: {has_space.sum()}件 → LastName除外')
                df_ot.loc[has_space, 'LastName'] = pd.NA
            # NaN/空のLastNameは列ごと除外できないので、空の行だけ別処理
            df_ot_with_ln = df_ot[df_ot['LastName'].notna() & (df_ot['LastName'] != '')]
            df_ot_no_ln = df_ot[df_ot['LastName'].isna() | (df_ot['LastName'] == '')]

            if len(df_ot_with_ln) > 0:
                print(f'  LastNameあり: {len(df_ot_with_ln)}件')
                result_ot1 = bulk_operation(client, 'Lead', 'update', df_ot_with_ln, 'その他(LastNameあり)')
                print(f'  成功: {len(result_ot1["success_ids"])}件, 失敗: {result_ot1["failed"]}件')

            if len(df_ot_no_ln) > 0:
                df_ot_no_ln = df_ot_no_ln.drop(columns=['LastName'])
                print(f'  LastNameなし: {len(df_ot_no_ln)}件')
                result_ot2 = bulk_operation(client, 'Lead', 'update', df_ot_no_ln, 'その他(LastNameなし)')
                print(f'  成功: {len(result_ot2["success_ids"])}件, 失敗: {result_ot2["failed"]}件')
                if result_ot2['failed_records']:
                    for rec in result_ot2['failed_records'][:3]:
                        print(f'    {rec.get("sf__Error", "unknown")[:100]}')
        else:
            print(f'  対象: {len(df_ot)}件')
            result_ot = bulk_operation(client, 'Lead', 'update', df_ot, 'その他リトライ')
            print(f'  成功: {len(result_ot["success_ids"])}件, 失敗: {result_ot["failed"]}件')

    # === Step 3: Accountレポート作成 ===
    print('\n--- Step 3: Accountレポート作成 ---')
    try:
        # AccountListレポートタイプのカラム名を使用
        r3_id = create_report(
            client,
            name=f'有料媒体_既存更新Account_{PROCESS_DATE}',
            report_type='AccountList',
            columns=[
                'ACCOUNT_NAME',
                'PHONE1',
                'Account.Paid_DataExportDate__c',
                'USERS.NAME',
            ],
            filters=[
                {'column': 'Account.Paid_DataExportDate__c', 'operator': 'equals', 'value': PROCESS_DATE},
            ]
        )
        r3_url = f'{client.instance_url}/lightning/r/Report/{r3_id}/view'
        print(f'  Account更新レポート: {r3_url}')
    except Exception as e:
        print(f'  レポートエラー: {e}')
        # Description列を除いてリトライ
        try:
            r3_id = create_report(
                client,
                name=f'有料媒体_既存更新Account_{PROCESS_DATE}',
                report_type='AccountList',
                columns=[
                    'ACCOUNT_NAME',
                    'PHONE1',
                    'USERS.NAME',
                ],
                filters=[
                    {'column': 'Account.Paid_DataExportDate__c', 'operator': 'equals', 'value': PROCESS_DATE},
                ]
            )
            r3_url = f'{client.instance_url}/lightning/r/Report/{r3_id}/view'
            print(f'  Account更新レポート（簡易版）: {r3_url}')
        except Exception as e2:
            print(f'  レポートエラー（リトライ）: {e2}')

    print('\n' + '=' * 70)
    print('リトライ処理完了')
    print('=' * 70)


if __name__ == '__main__':
    main()
