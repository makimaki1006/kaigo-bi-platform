"""
ハローワーク A/B リトライv2 + レポート作成
- CorporateNumber__c .0除去（dtype=str読み込み）
- Prefecture__c 無効値クリア（Describe APIで有効値取得）
- レポートカラム名修正済み
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
TODAY_ISO = datetime.now().strftime('%Y-%m-%d')

OWNERS = [
    {'name': '藤巻真弥', 'id': '0055i00000BeOKbAAN'},
    {'name': '服部翔太郎', 'id': '005J3000000EYYjIAO'},
    {'name': '深堀勇侍', 'id': '0055i00000CwKEhAAN'},
]


def main():
    print('=' * 60)
    print('ハローワーク A/B リトライv2 + レポート作成')
    print('=' * 60)

    # 失敗データ読み込み（全カラムstr型）
    df = pd.read_csv(FAILED_FILE, encoding='utf-8-sig', dtype=str)
    print(f'\n失敗レコード: {len(df)}件')

    # sf__列除去
    sf_cols = [c for c in df.columns if not c.startswith('sf__')]
    df = df[sf_cols].copy()

    # CorporateNumber__c .0除去
    df['CorporateNumber__c'] = df['CorporateNumber__c'].apply(
        lambda x: re.sub(r'\.0$', '', str(x).strip()) if pd.notna(x) and str(x).strip() not in ('', 'nan') else ''
    )
    # 13文字超えチェック
    over13 = df['CorporateNumber__c'].str.len() > 13
    if over13.any():
        print(f'  13文字超え: {over13.sum()}件 → 切り詰め')
        df.loc[over13, 'CorporateNumber__c'] = df.loc[over13, 'CorporateNumber__c'].str[:13]

    # Salesforce認証
    print('\n--- Salesforce認証 ---')
    client = SalesforceClient()
    client.authenticate()
    print('  認証成功')

    headers = client._get_headers()
    api_ver = client.api_version if hasattr(client, 'api_version') else 'v59.0'
    base_url = f'{client.instance_url}/services/data/{api_ver}'

    # Prefecture__c 有効値取得
    print('\n--- Prefecture__c 有効値取得 ---')
    desc_resp = requests.get(f'{base_url}/sobjects/Lead/describe', headers=headers)
    valid_prefs = set()
    if desc_resp.status_code == 200:
        for field in desc_resp.json().get('fields', []):
            if field['name'] == 'Prefecture__c':
                for pv in field.get('picklistValues', []):
                    if pv.get('active'):
                        valid_prefs.add(pv['value'])
                break
    print(f'  有効Prefecture: {len(valid_prefs)}件')

    # 無効Prefecture__cをクリア
    invalid_pref_mask = ~df['Prefecture__c'].isin(valid_prefs) & (df['Prefecture__c'] != '')
    invalid_count = invalid_pref_mask.sum()
    if invalid_count > 0:
        print(f'  無効Prefecture修正: {invalid_count}件')
        invalid_vals = df.loc[invalid_pref_mask, 'Prefecture__c'].value_counts()
        for v, c in invalid_vals.head(5).items():
            print(f'    "{v}": {c}件')
        df.loc[invalid_pref_mask, 'Prefecture__c'] = ''

    # nan → 空文字
    df = df.fillna('')
    for col in df.columns:
        df[col] = df[col].astype(str)
        df[col] = df[col].replace('nan', '')

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
    job_id = resp.json()['id']
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
                    clean_id = parts[0].strip('"')
                    if clean_id and clean_id != 'sf__Id':
                        new_ids.append(clean_id)

    # 既存IDと合わせる
    existing_ids = pd.read_csv(CREATED_IDS_FILE, encoding='utf-8-sig')['Id'].tolist()
    all_ids = existing_ids + new_ids
    # 重複除去
    all_ids = list(dict.fromkeys(all_ids))
    id_df = pd.DataFrame({'Id': all_ids})
    id_df.to_csv(CREATED_IDS_FILE, index=False, encoding='utf-8-sig')
    print(f'  作成済みID合計: {len(all_ids)}件（既存{len(existing_ids)} + 新規{len(new_ids)}）')

    if failed > 0:
        resp = requests.get(f'{base_url}/jobs/ingest/{job_id}/failedResults',
                           headers=headers)
        if resp.status_code == 200:
            failed_path = DATA_DIR / 'import_ready' / f'failed_AB_v2_{datetime.now().strftime("%Y%m%d")}.csv'
            with open(failed_path, 'w', encoding='utf-8-sig') as f:
                f.write(resp.text)
            print(f'  残失敗: {failed_path}')

    # --- レポート作成 ---
    print('\n--- レポート作成 ---')

    detail_columns = [
        'LEAD.OWNER_FULL_NAME',
        'COMPANY',
        'Lead.LastName',
        'PHONE1',
        'Lead.MobilePhone',
        'Lead.Street',
        'Lead.Prefecture__c',
        'Lead.NumberOfEmployees',
        'Lead.Website',
        'Lead.Hellowork_Industry__c',
        'Lead.Hellowork_RecuritmentType__c',
        'Lead.Hellowork_EmploymentType__c',
        'Lead.Hellowork_RecruitmentReasonCategory__c',
        'Lead.Hellowork_NumberOfRecruitment__c',
        'Lead.Hellowork_DataImportDate__c',
        'Lead.Status',
        'Lead.Publish_ImportText__c',
    ]

    base_filters = [
        {
            'column': 'Lead.Hellowork_DataImportDate__c',
            'operator': 'equals',
            'value': TODAY_ISO,
        },
        {
            'column': 'Lead.LeadSource',
            'operator': 'equals',
            'value': 'ハローワーク',
        },
        {
            'column': 'Lead.Status',
            'operator': 'equals',
            'value': '未架電',
        },
    ]

    def create_report(name, filters):
        url = f'{base_url}/analytics/reports'
        report_metadata = {
            'reportMetadata': {
                'name': name,
                'reportFormat': 'TABULAR',
                'reportType': {'type': 'LeadList'},
                'detailColumns': detail_columns,
                'reportFilters': filters,
            }
        }
        resp = requests.post(url, headers=headers, json=report_metadata)
        if resp.status_code in (200, 201):
            report_id = resp.json()['reportMetadata']['id']
            report_url = f'{client.instance_url}/lightning/r/Report/{report_id}/view'
            print(f'  OK: {name}')
            print(f'      {report_url}')
            return report_url
        else:
            print(f'  NG: {name} ({resp.status_code})')
            print(f'      {resp.text[:300]}')
            return None

    # 全体レポート
    report_all = create_report(f'ハロワA/B新規_{TODAY_ISO}_全体', base_filters)

    # 所有者別レポート
    report_urls = {}
    for owner in OWNERS:
        owner_filters = base_filters + [
            {
                'column': 'LEAD.OWNER_FULL_NAME',
                'operator': 'equals',
                'value': owner['name'],
            },
        ]
        url = create_report(f'ハロワA/B新規_{TODAY_ISO}_{owner["name"]}', owner_filters)
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
