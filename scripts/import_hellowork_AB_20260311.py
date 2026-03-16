"""
ハローワーク A/B セグメント インポート + レポート作成
=================================================
- 130件を藤巻・服部・深堀に均等配分
- Bulk API 2.0でSalesforceに新規Lead作成
- 所有者別レポート作成
"""

import sys
sys.stdout.reconfigure(encoding='utf-8')

import pandas as pd
import time
import json
import requests
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'src'))
from src.api.salesforce_client import SalesforceClient

BASE_DIR = Path(r'C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List')
DATA_DIR = BASE_DIR / 'data' / 'output' / 'hellowork_segments'
IMPORT_FILE = DATA_DIR / 'import_ready' / 'import_AB_20260311.csv'
TODAY = datetime.now().strftime('%Y%m%d')
TODAY_ISO = datetime.now().strftime('%Y-%m-%d')

# 所有者定義
OWNERS = [
    {'name': '藤巻 真弥', 'id': '0055i00000BeOKbAAN'},
    {'name': '服部 翔太郎', 'id': '005J3000000EYYjIAO'},
    {'name': '深堀 勇侍', 'id': '0055i00000CwKEhAAN'},
]


def assign_owners(df):
    """3名に均等配分"""
    n = len(df)
    base = n // 3
    remainder = n % 3

    owner_ids = []
    owner_names = []
    idx = 0
    for i, owner in enumerate(OWNERS):
        count = base + (1 if i < remainder else 0)
        owner_ids.extend([owner['id']] * count)
        owner_names.extend([owner['name']] * count)
        print(f"  {owner['name']}: {count}件")
        idx += count

    df = df.copy()
    df['OwnerId'] = owner_ids[:n]
    df['_owner_name'] = owner_names[:n]
    return df


def bulk_insert_leads(client, df):
    """Bulk API 2.0で新規Lead作成"""
    # インポート用カラムのみ（_始まりの内部カラムを除外）
    sf_cols = [c for c in df.columns if not c.startswith('_')]
    import_df = df[sf_cols].copy()

    # nan → 空文字
    import_df = import_df.fillna('')
    for col in import_df.columns:
        import_df[col] = import_df[col].astype(str).replace('nan', '')

    print(f'\n  インポート件数: {len(import_df)}件')
    print(f'  カラム数: {len(sf_cols)}')

    # CSV生成
    csv_path = DATA_DIR / 'import_ready' / f'sf_import_AB_{TODAY}.csv'
    import_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    print(f'  CSV: {csv_path}')

    # Bulk API 2.0 ジョブ作成
    headers = client._get_headers()
    api_ver = client.api_version if hasattr(client, 'api_version') else 'v59.0'
    base_url = f'{client.instance_url}/services/data/{api_ver}'

    # ジョブ作成
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

    # CSVアップロード
    csv_data = import_df.to_csv(index=False, encoding='utf-8')
    upload_headers = {**headers, 'Content-Type': 'text/csv'}
    resp = requests.put(f'{base_url}/jobs/ingest/{job_id}/batches',
                       headers=upload_headers, data=csv_data.encode('utf-8'))
    resp.raise_for_status()

    # ジョブ開始
    resp = requests.patch(f'{base_url}/jobs/ingest/{job_id}',
                         headers=headers, json={'state': 'UploadComplete'})
    resp.raise_for_status()

    # 完了待機
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
    print(f'  状態: {state}')
    print(f'  成功: {status.get("numberRecordsProcessed", 0)}件')
    print(f'  失敗: {status.get("numberRecordsFailed", 0)}件')

    # 成功レコードのID取得
    created_ids = []
    if int(status.get('numberRecordsProcessed', 0)) > 0:
        resp = requests.get(f'{base_url}/jobs/ingest/{job_id}/successfulResults',
                           headers=headers)
        if resp.status_code == 200:
            lines = resp.text.strip().split('\n')
            for line in lines[1:]:  # ヘッダスキップ
                parts = line.split(',')
                if parts and parts[0].startswith('"'):
                    created_ids.append(parts[0].strip('"'))
                elif parts:
                    created_ids.append(parts[0])

    # 失敗レコード確認
    if int(status.get('numberRecordsFailed', 0)) > 0:
        resp = requests.get(f'{base_url}/jobs/ingest/{job_id}/failedResults',
                           headers=headers)
        if resp.status_code == 200:
            failed_path = DATA_DIR / 'import_ready' / f'failed_AB_{TODAY}.csv'
            with open(failed_path, 'w', encoding='utf-8-sig') as f:
                f.write(resp.text)
            print(f'  失敗レコード: {failed_path}')

    # 作成済みID保存
    if created_ids:
        id_df = pd.DataFrame({'Id': created_ids})
        id_path = DATA_DIR / 'import_ready' / f'created_lead_ids_{TODAY}.csv'
        id_df.to_csv(id_path, index=False, encoding='utf-8-sig')
        print(f'  作成済みID: {id_path} ({len(created_ids)}件)')

    return created_ids, status


def create_report(client, name, filters, detail_columns=None):
    """Salesforceレポート作成"""
    headers = client._get_headers()
    api_ver = client.api_version if hasattr(client, 'api_version') else 'v59.0'
    base_url = f'{client.instance_url}/services/data/{api_ver}'

    if detail_columns is None:
        detail_columns = [
            'FULL_NAME', 'LEAD.COMPANY', 'PHONE1',
            'Lead.NumberOfEmployees', 'Lead.Status',
            'Lead.Publish_ImportText__c',
            'Lead.Hellowork_RecruitmentReasonCategory__c',
            'Lead.Hellowork_NumberOfRecruitment__c',
            'Lead.Hellowork_Industry__c',
            'Lead.Hellowork_RecuritmentType__c',
            'Lead.LeadSource',
            'Lead.OwnerId',
        ]

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
        report_url = f'{client.instance_url}/lightning/r/Report/{report_id}/view'
        print(f'  レポート作成成功: {name}')
        print(f'  URL: {report_url}')
        return report_id, report_url
    else:
        print(f'  レポート作成失敗: {resp.status_code}')
        print(f'  {resp.text[:500]}')
        return None, None


def main():
    print('=' * 60)
    print('ハローワーク A/B セグメント インポート')
    print(f'実行日: {TODAY_ISO}')
    print('=' * 60)

    # データ読み込み
    df = pd.read_csv(IMPORT_FILE, encoding='utf-8-sig')
    print(f'\n入力: {len(df)}件')

    # 所有者割り当て
    print('\n--- 所有者割り当て ---')
    df = assign_owners(df)

    # Salesforce認証
    print('\n--- Salesforce認証 ---')
    client = SalesforceClient()
    client.authenticate()
    print('  認証成功')

    # インポート
    print('\n--- Bulk API 2.0 インポート ---')
    created_ids, status = bulk_insert_leads(client, df)

    if not created_ids:
        print('\n❌ インポート失敗 - レポート作成をスキップ')
        return

    # レポート作成
    print('\n--- レポート作成 ---')
    batch_date = TODAY_ISO

    # 全体レポート
    all_filters = [
        {
            'column': 'Lead.Hellowork_DataImportDate__c',
            'operator': 'equals',
            'value': batch_date,
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

    report_all_id, report_all_url = create_report(
        client,
        f'ハロワA/B新規_{batch_date}_全体',
        all_filters,
    )

    # 所有者別レポート
    report_urls = {}
    for owner in OWNERS:
        owner_filters = all_filters + [
            {
                'column': 'Lead.OwnerId',
                'operator': 'equals',
                'value': owner['id'],
            },
        ]
        report_id, report_url = create_report(
            client,
            f'ハロワA/B新規_{batch_date}_{owner["name"]}',
            owner_filters,
        )
        report_urls[owner['name']] = report_url

    # サマリー
    print('\n' + '=' * 60)
    print('完了サマリー')
    print('=' * 60)
    print(f'  インポート成功: {len(created_ids)}件')
    print(f'  インポート失敗: {status.get("numberRecordsFailed", 0)}件')
    print(f'\n  レポート:')
    if report_all_url:
        print(f'    全体: {report_all_url}')
    for name, url in report_urls.items():
        if url:
            print(f'    {name}: {url}')


if __name__ == '__main__':
    main()
