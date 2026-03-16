"""
有料媒体インポート実行（PT・OT・STネット + ジョブポスター）2026-03-10
- 新規リード作成 (Bulk API 2.0)
- 既存Lead更新 (Bulk API 2.0)
- 既存Account更新 (Bulk API 2.0)
- レポート作成 (Analytics API)
"""
import pandas as pd
import sys
import os
import io
import time
import requests
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))
sys.stdout.reconfigure(encoding='utf-8')

from api.salesforce_client import SalesforceClient

BASE_DIR = Path(__file__).parent.parent
OUTPUT_DIR = BASE_DIR / 'data/output/media_matching'
PROCESS_DATE = '2026-03-10'

NEW_LEADS_FILE = OUTPUT_DIR / 'new_leads_20260310_180059.csv'
LEAD_UPDATES_FILE = OUTPUT_DIR / 'lead_updates_20260310_180059.csv'
ACCOUNT_UPDATES_FILE = OUTPUT_DIR / 'account_updates_20260310_180059.csv'


def bulk_operation(client, object_name, operation, df, label=''):
    """Bulk API 2.0で操作を実行"""
    headers = client._get_headers()
    api_version = 'v59.0'
    job_url = f'{client.instance_url}/services/data/{api_version}/jobs/ingest'

    # ジョブ作成
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

    # CSVアップロード
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, lineterminator='\r\n')
    csv_data = csv_buffer.getvalue()

    upload_url = f'{job_url}/{job_id}/batches'
    upload_headers = {**headers, 'Content-Type': 'text/csv'}
    resp = requests.put(upload_url, headers=upload_headers, data=csv_data.encode('utf-8'))
    resp.raise_for_status()
    print(f'  アップロード: {len(df)}件')

    # ジョブクローズ
    close_url = f'{job_url}/{job_id}'
    resp = requests.patch(close_url, headers=headers, json={'state': 'UploadComplete'})
    resp.raise_for_status()

    # 完了待ち
    for i in range(120):
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

    # 成功レコード取得
    success_ids = []
    success_url = f'{job_url}/{job_id}/successfulResults'
    resp = requests.get(success_url, headers=headers)
    if resp.text.strip():
        lines = resp.text.strip().split('\n')
        if len(lines) > 1:
            reader = pd.read_csv(io.StringIO(resp.text), dtype=str)
            if 'sf__Id' in reader.columns:
                success_ids = reader['sf__Id'].tolist()

    # 失敗レコード取得
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
    print(f'有料媒体 Salesforceインポート実行 {PROCESS_DATE}')
    print('=' * 70)

    # SF認証
    client = SalesforceClient()
    client.authenticate()
    print('SF認証成功')

    results = {}

    # === Step 1: 新規リード作成 ===
    print('\n--- Step 1: 新規リード作成 ---')
    df_new = pd.read_csv(NEW_LEADS_FILE, encoding='utf-8-sig', dtype=str)
    print(f'  対象: {len(df_new)}件')

    result_new = bulk_operation(client, 'Lead', 'insert', df_new, '新規リード')
    results['new'] = result_new
    print(f'  成功: {len(result_new["success_ids"])}件, 失敗: {result_new["failed"]}件')

    if result_new['success_ids']:
        ids_file = OUTPUT_DIR / 'created_lead_ids_20260310.csv'
        pd.DataFrame({'Id': result_new['success_ids']}).to_csv(ids_file, index=False, encoding='utf-8-sig')
        print(f'  作成済みID: {ids_file}')

    if result_new['failed_records']:
        fail_file = OUTPUT_DIR / 'failed_new_leads_20260310.csv'
        pd.DataFrame(result_new['failed_records']).to_csv(fail_file, index=False, encoding='utf-8-sig')
        print(f'  失敗レコード: {fail_file}')
        for rec in result_new['failed_records'][:5]:
            print(f'    {rec.get("sf__Error", "unknown")}')

    # === Step 2: 既存Lead更新 ===
    print('\n--- Step 2: 既存Lead更新 ---')
    df_lu = pd.read_csv(LEAD_UPDATES_FILE, encoding='utf-8-sig', dtype=str)
    print(f'  対象: {len(df_lu)}件')

    result_lu = bulk_operation(client, 'Lead', 'update', df_lu, 'Lead更新')
    results['lead_update'] = result_lu
    print(f'  成功: {len(result_lu["success_ids"])}件, 失敗: {result_lu["failed"]}件')

    if result_lu['failed_records']:
        fail_file = OUTPUT_DIR / 'failed_lead_updates_20260310.csv'
        pd.DataFrame(result_lu['failed_records']).to_csv(fail_file, index=False, encoding='utf-8-sig')
        print(f'  失敗レコード:')
        for rec in result_lu['failed_records'][:5]:
            print(f'    {rec.get("sf__Error", "unknown")}')

    # === Step 3: 既存Account更新 ===
    print('\n--- Step 3: 既存Account更新 ---')
    df_au = pd.read_csv(ACCOUNT_UPDATES_FILE, encoding='utf-8-sig', dtype=str)
    print(f'  対象: {len(df_au)}件')

    result_au = bulk_operation(client, 'Account', 'update', df_au, 'Account更新')
    results['account_update'] = result_au
    print(f'  成功: {len(result_au["success_ids"])}件, 失敗: {result_au["failed"]}件')

    if result_au['failed_records']:
        fail_file = OUTPUT_DIR / 'failed_account_updates_20260310.csv'
        pd.DataFrame(result_au['failed_records']).to_csv(fail_file, index=False, encoding='utf-8-sig')
        print(f'  失敗レコード:')
        for rec in result_au['failed_records'][:5]:
            print(f'    {rec.get("sf__Error", "unknown")}')

    # === Step 4: レポート作成 ===
    print('\n--- Step 4: レポート作成 ---')

    # レポート1: 新規作成リード
    try:
        r1_id = create_report(
            client,
            name=f'有料媒体_新規作成リード_{PROCESS_DATE}',
            report_type='LeadList',
            columns=[
                'LAST_NAME', 'COMPANY', 'PHONE', 'MOBILE_PHONE',
                'Lead.Prefecture__c', 'STREET',
                'Lead.Paid_Media__c', 'Lead.Paid_JobTitle__c',
                'Lead.Paid_Memo__c',
                'CREATED_DATE', 'OWNER',
            ],
            filters=[
                {'column': 'Lead.LeadSourceMemo__c', 'operator': 'contains', 'value': f'【新規作成】有料媒体突合 {PROCESS_DATE}'},
            ]
        )
        r1_url = f'{client.instance_url}/lightning/r/Report/{r1_id}/view'
        print(f'  新規作成リード: {r1_url}')
    except Exception as e:
        print(f'  新規作成リードレポートエラー: {e}')
        r1_url = None

    # レポート2: 既存更新リード
    try:
        r2_id = create_report(
            client,
            name=f'有料媒体_既存更新リード_{PROCESS_DATE}',
            report_type='LeadList',
            columns=[
                'LAST_NAME', 'COMPANY', 'PHONE',
                'Lead.Paid_Media__c', 'Lead.Paid_DataExportDate__c',
                'Lead.LeadSourceMemo__c',
                'OWNER',
            ],
            filters=[
                {'column': 'Lead.LeadSourceMemo__c', 'operator': 'contains', 'value': f'【既存更新】有料媒体突合 {PROCESS_DATE}'},
            ]
        )
        r2_url = f'{client.instance_url}/lightning/r/Report/{r2_id}/view'
        print(f'  既存更新リード: {r2_url}')
    except Exception as e:
        print(f'  既存更新リードレポートエラー: {e}')
        r2_url = None

    # レポート3: Account更新
    try:
        r3_id = create_report(
            client,
            name=f'有料媒体_既存更新Account_{PROCESS_DATE}',
            report_type='AccountList',
            columns=[
                'ACCOUNT_NAME', 'PHONE1',
                'Account.Paid_DataExportDate__c',
                'Account.Description',
                'USERS.NAME',
            ],
            filters=[
                {'column': 'Account.Paid_DataExportDate__c', 'operator': 'equals', 'value': PROCESS_DATE},
            ]
        )
        r3_url = f'{client.instance_url}/lightning/r/Report/{r3_id}/view'
        print(f'  既存更新Account: {r3_url}')
    except Exception as e:
        print(f'  Account更新レポートエラー: {e}')
        r3_url = None

    # === サマリー ===
    print('\n' + '=' * 70)
    print('完了サマリー')
    print('=' * 70)
    print(f'  新規リード: {len(result_new["success_ids"])}件成功 / {result_new["failed"]}件失敗')
    print(f'  Lead更新: {len(result_lu["success_ids"])}件成功 / {result_lu["failed"]}件失敗')
    print(f'  Account更新: {len(result_au["success_ids"])}件成功 / {result_au["failed"]}件失敗')
    if r1_url:
        print(f'  レポート（新規）: {r1_url}')
    if r2_url:
        print(f'  レポート（Lead更新）: {r2_url}')
    if r3_url:
        print(f'  レポート（Account更新）: {r3_url}')


if __name__ == '__main__':
    main()
