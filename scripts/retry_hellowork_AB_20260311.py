"""
ハローワーク A/B 失敗レコードリトライ + レポート作成
================================================
- CorporateNumber__c .0除去修正
- レポートカラム名修正
"""

import sys
sys.stdout.reconfigure(encoding='utf-8')

import pandas as pd
import re
import time
import requests
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'src'))
from src.api.salesforce_client import SalesforceClient

BASE_DIR = Path(r'C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List')
DATA_DIR = BASE_DIR / 'data' / 'output' / 'hellowork_segments'
FAILED_FILE = DATA_DIR / 'import_ready' / 'failed_AB_20260311.csv'
CREATED_IDS_FILE = DATA_DIR / 'import_ready' / 'created_lead_ids_20260311.csv'
TODAY = datetime.now().strftime('%Y%m%d')
TODAY_ISO = datetime.now().strftime('%Y-%m-%d')

OWNERS = [
    {'name': '藤巻 真弥', 'id': '0055i00000BeOKbAAN'},
    {'name': '服部 翔太郎', 'id': '005J3000000EYYjIAO'},
    {'name': '深堀 勇侍', 'id': '0055i00000CwKEhAAN'},
]


def clean_corp_num(val):
    """法人番号から.0を除去"""
    s = str(val).strip()
    if s in ('', 'nan', 'None'):
        return ''
    s = re.sub(r'\.0$', '', s)
    if len(s) > 13:
        s = s[:13]
    return s


def main():
    print('=' * 60)
    print('ハローワーク A/B リトライ + レポート作成')
    print('=' * 60)

    # 失敗データ読み込み
    df = pd.read_csv(FAILED_FILE, encoding='utf-8-sig', dtype=str)
    print(f'\n失敗レコード: {len(df)}件')

    # sf__Id, sf__Error列を除去
    sf_cols = [c for c in df.columns if not c.startswith('sf__')]
    df = df[sf_cols]

    # CorporateNumber__c修正
    df['CorporateNumber__c'] = df['CorporateNumber__c'].apply(clean_corp_num)
    print(f'CorporateNumber__c修正完了')

    # nan → 空文字
    df = df.fillna('')
    for col in df.columns:
        df[col] = df[col].astype(str).replace('nan', '')

    # Salesforce認証
    print('\n--- Salesforce認証 ---')
    client = SalesforceClient()
    client.authenticate()
    print('  認証成功')

    headers = client._get_headers()
    api_ver = client.api_version if hasattr(client, 'api_version') else 'v59.0'
    base_url = f'{client.instance_url}/services/data/{api_ver}'

    # Bulk API 2.0
    print('\n--- Bulk API 2.0 リトライ ---')
    job_body = {
        'object': 'Lead',
        'operation': 'insert',
        'contentType': 'CSV',
        'lineEnding': 'CRLF',
    }
    resp = requests.post(f'{base_url}/jobs/ingest', headers=headers, json=job_body)
    resp.raise_for_status()
    job = resp.json()
    job_id = job['id']
    print(f'  ジョブID: {job_id}')

    csv_data = df.to_csv(index=False, encoding='utf-8')
    upload_headers = {**headers, 'Content-Type': 'text/csv'}
    resp = requests.put(f'{base_url}/jobs/ingest/{job_id}/batches',
                       headers=upload_headers, data=csv_data.encode('utf-8'))
    resp.raise_for_status()

    resp = requests.patch(f'{base_url}/jobs/ingest/{job_id}',
                         headers=headers, json={'state': 'UploadComplete'})
    resp.raise_for_status()

    print('  処理中', end='', flush=True)
    for _ in range(60):
        time.sleep(5)
        resp = requests.get(f'{base_url}/jobs/ingest/{job_id}', headers=headers)
        status = resp.json()
        state = status.get('state', '')
        if state in ('JobComplete', 'Failed', 'Aborted'):
            break
        print('.', end='', flush=True)

    print()
    processed = int(status.get('numberRecordsProcessed', 0))
    failed = int(status.get('numberRecordsFailed', 0))
    print(f'  状態: {state}')
    print(f'  成功: {processed}件')
    print(f'  失敗: {failed}件')

    # 成功ID取得
    new_ids = []
    if processed > 0:
        resp = requests.get(f'{base_url}/jobs/ingest/{job_id}/successfulResults',
                           headers=headers)
        if resp.status_code == 200:
            lines = resp.text.strip().split('\n')
            for line in lines[1:]:
                parts = line.split(',')
                if parts and parts[0]:
                    new_ids.append(parts[0].strip('"'))

    # 既存IDに追加
    existing_ids = pd.read_csv(CREATED_IDS_FILE, encoding='utf-8-sig')['Id'].tolist()
    all_ids = existing_ids + new_ids
    id_df = pd.DataFrame({'Id': all_ids})
    id_df.to_csv(CREATED_IDS_FILE, index=False, encoding='utf-8-sig')
    print(f'  作成済みID合計: {len(all_ids)}件（既存{len(existing_ids)} + 新規{len(new_ids)}）')

    # 失敗があれば保存
    if failed > 0:
        resp = requests.get(f'{base_url}/jobs/ingest/{job_id}/failedResults',
                           headers=headers)
        if resp.status_code == 200:
            failed_path = DATA_DIR / 'import_ready' / f'failed_AB_retry_{TODAY}.csv'
            with open(failed_path, 'w', encoding='utf-8-sig') as f:
                f.write(resp.text)
            print(f'  残失敗: {failed_path}')

    # --- レポート作成 ---
    print('\n--- レポート作成 ---')
    batch_date = TODAY_ISO

    detail_columns = [
        'Lead.Name', 'Lead.Company', 'Lead.Phone',
        'Lead.NumberOfEmployees', 'Lead.Status__c',
        'Lead.Publish_ImportText__c',
        'Lead.Hellowork_RecruitmentReasonCategory__c',
        'Lead.Hellowork_Industry__c',
        'Lead.Hellowork_RecuritmentType__c',
        'Lead.LeadSource__c',
        'OWNER.ALIAS',
    ]

    base_filters = [
        {
            'column': 'Lead.Hellowork_DataImportDate__c',
            'operator': 'equals',
            'value': batch_date,
        },
    ]

    def create_report(name, filters):
        report_metadata = {
            'reportMetadata': {
                'name': name,
                'reportFormat': 'TABULAR',
                'reportType': {'type': 'LeadList'},
                'detailColumns': detail_columns,
                'reportFilters': filters,
            }
        }
        resp = requests.post(f'{base_url}/analytics/reports',
                            headers=headers, json=report_metadata)
        if resp.status_code in (200, 201):
            report_id = resp.json().get('reportMetadata', {}).get('id', '')
            url = f'{client.instance_url}/lightning/r/Report/{report_id}/view'
            print(f'  OK: {name}')
            print(f'      {url}')
            return url
        else:
            error_text = resp.text[:300]
            print(f'  FAIL: {name} ({resp.status_code})')
            print(f'  {error_text}')
            return None

    # 全体レポート
    report_all = create_report(f'ハロワA/B新規_{batch_date}_全体', base_filters)

    # 所有者別レポート
    report_urls = {}
    for owner in OWNERS:
        owner_filters = base_filters + [
            {
                'column': 'Lead.OwnerId',
                'operator': 'equals',
                'value': owner['id'],
            },
        ]
        url = create_report(f'ハロワA/B新規_{batch_date}_{owner["name"]}', owner_filters)
        report_urls[owner['name']] = url

    # サマリー
    print('\n' + '=' * 60)
    print('完了サマリー')
    print('=' * 60)
    print(f'  リトライ成功: {len(new_ids)}件')
    print(f'  リトライ失敗: {failed}件')
    print(f'  インポート総計: {len(all_ids)}件')
    print(f'\n  レポート:')
    if report_all:
        print(f'    全体: {report_all}')
    for name, url in report_urls.items():
        if url:
            print(f'    {name}: {url}')


if __name__ == '__main__':
    main()
