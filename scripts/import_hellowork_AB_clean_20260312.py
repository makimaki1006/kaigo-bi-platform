"""
ハローワーク A/B セグメント クリーンインポート
=============================================
- dtype=strで全フィールド読み込み（.0問題回避）
- Prefecture__c Describe APIで検証
- UTF-8エンコーディング厳格管理
- 130件を藤巻・服部・深堀に均等配分
"""

import sys
sys.stdout.reconfigure(encoding='utf-8')

import pandas as pd
import re
import time
import io
import csv
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

OWNERS = [
    {'name': '藤巻真弥', 'id': '0055i00000BeOKbAAN'},
    {'name': '服部翔太郎', 'id': '005J3000000EYYjIAO'},
    {'name': '深堀勇侍', 'id': '0055i00000CwKEhAAN'},
]


def main():
    print('=' * 60)
    print('ハローワーク A/B クリーンインポート')
    print(f'実行日: {TODAY_ISO}')
    print('=' * 60)

    # === データ読み込み（dtype=str） ===
    df = pd.read_csv(IMPORT_FILE, encoding='utf-8-sig', dtype=str)
    df = df.fillna('')
    for col in df.columns:
        df[col] = df[col].astype(str).replace('nan', '')
    print(f'\n入力: {len(df)}件')

    # === Salesforce認証 ===
    client = SalesforceClient()
    client.authenticate()
    headers = client._get_headers()
    api_ver = client.api_version if hasattr(client, 'api_version') else 'v59.0'
    base_url = f'{client.instance_url}/services/data/{api_ver}'
    print('認証成功')

    # === Prefecture__c 有効値取得 ===
    desc_resp = requests.get(f'{base_url}/sobjects/Lead/describe', headers=headers)
    valid_prefs = set()
    if desc_resp.status_code == 200:
        for field in desc_resp.json().get('fields', []):
            if field['name'] == 'Prefecture__c':
                for pv in field.get('picklistValues', []):
                    if pv.get('active'):
                        valid_prefs.add(pv['value'])
                break
    print(f'有効Prefecture: {len(valid_prefs)}件')

    # === データクレンジング ===
    print('\n--- データクレンジング ---')

    # CorporateNumber__c .0除去 + 13桁チェック
    def clean_corp(val):
        s = str(val).strip()
        if s in ('', 'nan', 'None'):
            return ''
        s = re.sub(r'\.0$', '', s)
        s = re.sub(r'[^\d]', '', s)
        return s if len(s) == 13 else ''

    df['CorporateNumber__c'] = df['CorporateNumber__c'].apply(clean_corp)
    corp_ok = (df['CorporateNumber__c'] != '').sum()
    print(f'  CorporateNumber__c有効: {corp_ok}件')

    # Prefecture__c 検証
    invalid_pref = ~df['Prefecture__c'].isin(valid_prefs) & (df['Prefecture__c'] != '')
    if invalid_pref.sum() > 0:
        print(f'  Prefecture__c無効値クリア: {invalid_pref.sum()}件')
        df.loc[invalid_pref, 'Prefecture__c'] = ''

    # Phone先頭ゼロ確認
    def fix_phone(p):
        s = str(p).strip()
        if s in ('', 'nan'):
            return ''
        digits = re.sub(r'[^0-9]', '', s)
        if len(digits) == 9:
            digits = '0' + digits
        return digits

    df['Phone'] = df['Phone'].apply(fix_phone)
    df['MobilePhone'] = df['MobilePhone'].apply(lambda x: fix_phone(x) if str(x).strip() not in ('', 'nan') else '')

    # LastNameにスペースや記号があるものは「担当者」に置換
    def clean_lastname(name):
        s = str(name).strip()
        if s in ('', 'nan'):
            return '担当者'
        if re.search(r'[\s　]', s):
            return '担当者'
        return s

    df['LastName'] = df['LastName'].apply(clean_lastname)

    # === 所有者割り当て ===
    print('\n--- 所有者割り当て ---')
    n = len(df)
    base = n // 3
    remainder = n % 3
    owner_ids = []
    for i, owner in enumerate(OWNERS):
        count = base + (1 if i < remainder else 0)
        owner_ids.extend([owner['id']] * count)
        print(f'  {owner["name"]}: {count}件')
    df['OwnerId'] = owner_ids[:n]

    # === インポート用カラムのみ抽出 ===
    sf_cols = [c for c in df.columns if not c.startswith('_')]
    import_df = df[sf_cols].copy()

    # === Bulk API 2.0 インポート ===
    print('\n--- Bulk API 2.0 インポート ---')

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

    # CSV生成（StringIOでUTF-8保証）
    output = io.StringIO()
    import_df.to_csv(output, index=False)
    csv_str = output.getvalue()

    # UTF-8バイト列でアップロード
    upload_headers = {**headers, 'Content-Type': 'text/csv; charset=UTF-8'}
    resp = requests.put(
        f'{base_url}/jobs/ingest/{job_id}/batches',
        headers=upload_headers,
        data=csv_str.encode('utf-8'),
    )
    resp.raise_for_status()

    resp = requests.patch(
        f'{base_url}/jobs/ingest/{job_id}',
        headers=headers,
        json={'state': 'UploadComplete'},
    )
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

    # 失敗レコード
    if failed > 0:
        resp = requests.get(f'{base_url}/jobs/ingest/{job_id}/failedResults', headers=headers)
        if resp.status_code == 200:
            failed_path = DATA_DIR / 'import_ready' / f'failed_AB_clean_{TODAY}.csv'
            with open(failed_path, 'w', encoding='utf-8-sig') as f:
                f.write(resp.text)
            print(f'  失敗詳細: {failed_path}')
            # エラー内容表示
            lines = resp.text.strip().split('\n')
            errors = {}
            for line in lines[1:]:
                if 'sf__Error' not in line and '"' in line:
                    parts = line.split('","')
                    if len(parts) >= 2:
                        err = parts[1].split(':')[0] if ':' in parts[1] else parts[1][:50]
                        errors[err] = errors.get(err, 0) + 1
            for err, cnt in sorted(errors.items(), key=lambda x: -x[1])[:5]:
                print(f'    [{cnt}件] {err}')

    # 成功ID保存
    created_ids = []
    if processed > 0:
        resp = requests.get(f'{base_url}/jobs/ingest/{job_id}/successfulResults', headers=headers)
        if resp.status_code == 200:
            lines = resp.text.strip().split('\n')
            for line in lines[1:]:
                parts = line.split(',')
                if parts and parts[0]:
                    clean_id = parts[0].strip('"')
                    if clean_id and len(clean_id) >= 15:
                        created_ids.append(clean_id)

        id_path = DATA_DIR / 'import_ready' / f'created_lead_ids_{TODAY}.csv'
        pd.DataFrame({'Id': created_ids}).to_csv(id_path, index=False, encoding='utf-8-sig')
        print(f'  作成済みID: {id_path} ({len(created_ids)}件)')

    # === レポート作成 ===
    print('\n--- レポート作成 ---')
    detail_columns = [
        'OWNER', 'COMPANY', 'LAST_NAME', 'PHONE', 'MOBILE_PHONE',
        'STREET', 'Lead.Prefecture__c', 'EMPLOYEES',
        'Lead.Hellowork_Industry__c', 'Lead.Hellowork_RecuritmentType__c',
        'Lead.Hellowork_EmploymentType__c',
        'Lead.Hellowork_RecruitmentReasonCategory__c',
        'Lead.Hellowork_NumberOfRecruitment__c',
        'Lead.Hellowork_DataImportDate__c',
        'STATUS', 'Lead.Publish_ImportText__c',
    ]

    base_filters = [
        {'column': 'Lead.Hellowork_DataImportDate__c', 'operator': 'equals', 'value': '2026-03-11'},
        {'column': 'STATUS', 'operator': 'equals', 'value': '未架電'},
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
        resp = requests.post(f'{base_url}/analytics/reports', headers=headers, json=report_metadata)
        if resp.status_code in (200, 201):
            report_id = resp.json()['reportMetadata']['id']
            url = f'{client.instance_url}/lightning/r/Report/{report_id}/view'
            print(f'  OK: {name} -> {url}')
            return url
        else:
            print(f'  NG: {name} ({resp.status_code}) {resp.text[:200]}')
            return None

    report_all = create_report(f'ハロワA/B新規_2026-03-11_全体', base_filters)

    report_urls = {}
    for owner in OWNERS:
        owner_filters = base_filters + [
            {'column': 'OWNER', 'operator': 'equals', 'value': owner['name']},
        ]
        url = create_report(f'ハロワA/B新規_2026-03-11_{owner["name"]}', owner_filters)
        report_urls[owner['name']] = url

    # === サマリー ===
    print('\n' + '=' * 60)
    print('完了サマリー')
    print('=' * 60)
    print(f'  インポート成功: {len(created_ids)}件')
    print(f'  インポート失敗: {failed}件')
    print(f'\n  レポート:')
    if report_all:
        print(f'    全体: {report_all}')
    for name, url in report_urls.items():
        if url:
            print(f'    {name}: {url}')


if __name__ == '__main__':
    main()
