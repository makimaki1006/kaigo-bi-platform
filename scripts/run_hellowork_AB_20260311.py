"""
ハローワーク セグメントA/B 2026-03-11
STEP 0: SFデータリフレッシュ → パイプライン実行
"""
import pandas as pd
import sys
import io
import time
import requests
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'src'))
sys.stdout.reconfigure(encoding='utf-8')

from api.salesforce_client import SalesforceClient

BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE_DIR / 'data' / 'output' / 'hellowork_segments'
TODAY = datetime.now().strftime('%Y%m%d')
TODAY_ISO = datetime.now().strftime('%Y-%m-%d')


def export_sf_data(client):
    """Salesforceから最新データをエクスポート"""
    print('=' * 60)
    print('STEP 0: Salesforceデータ リフレッシュ')
    print('=' * 60)

    headers = {**client._get_headers(), 'Content-Type': 'application/json'}
    api_version = client.api_version

    objects = {
        'Lead': "SELECT Id,Phone,MobilePhone,Phone2__c,MobilePhone2__c,Company,Status,CorporateNumber__c FROM Lead",
        'Account': "SELECT Id,Phone,Phone2__c,Name,CorporateNumber__c FROM Account",
        'Contact': "SELECT Id,Phone,Phone2__c,MobilePhone,MobilePhone2__c,AccountId FROM Contact",
    }

    for obj_name, soql in objects.items():
        print(f'\n  {obj_name} エクスポート中...')
        job_url = f'{client.instance_url}/services/data/{api_version}/jobs/query'
        resp = requests.post(job_url, headers=headers, json={
            'operation': 'query', 'query': soql, 'contentType': 'CSV'
        })
        resp.raise_for_status()
        job_id = resp.json()['id']

        for _ in range(120):
            time.sleep(5)
            sr = requests.get(f'{job_url}/{job_id}', headers=client._get_headers())
            state = sr.json()['state']
            if state == 'JobComplete':
                break
            if state in ('Failed', 'Aborted'):
                raise Exception(f'{obj_name} export failed: {sr.json()}')
            print(f'    {state}...')

        result = requests.get(f'{job_url}/{job_id}/results', headers=client._get_headers())
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        out_path = OUTPUT_DIR / f'{obj_name}_{ts}.csv'
        with open(out_path, 'wb') as f:
            f.write(result.content)
        lines = result.text.count('\n')
        print(f'  {obj_name}: {lines:,}件 → {out_path.name}')

    # 成約先エクスポート
    print(f'\n  成約先エクスポート中...')
    contract_soql = (
        "SELECT Id,Name,Phone,Phone2__c,CorporateIdentificationNumber__c,CorporateNumber__c,Status__c,RelatedAccountFlg__c "
        "FROM Account WHERE Status__c LIKE '%商談中%' "
        "OR Status__c LIKE '%プロジェクト進行中%' "
        "OR Status__c LIKE '%深耕対象%' "
        "OR Status__c LIKE '%過去客%' "
        "OR RelatedAccountFlg__c = 'グループ案件進行中' "
        "OR RelatedAccountFlg__c = 'グループ過去案件実績あり'"
    )
    resp = requests.post(job_url, headers=headers, json={
        'operation': 'query', 'query': contract_soql, 'contentType': 'CSV'
    })
    resp.raise_for_status()
    job_id = resp.json()['id']

    for _ in range(120):
        time.sleep(5)
        sr = requests.get(f'{job_url}/{job_id}', headers=client._get_headers())
        state = sr.json()['state']
        if state == 'JobComplete':
            break
        if state in ('Failed', 'Aborted'):
            raise Exception(f'Contract export failed: {sr.json()}')
        print(f'    {state}...')

    result = requests.get(f'{job_url}/{job_id}/results', headers=client._get_headers())
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    out_path = OUTPUT_DIR / f'contract_accounts_{ts}.csv'
    with open(out_path, 'wb') as f:
        f.write(result.content)
    lines = result.text.count('\n')
    print(f'  成約先: {lines:,}件 → {out_path.name}')

    print('\n  SFデータ リフレッシュ完了')


def main():
    print('━' * 60)
    print(f'ハローワーク セグメントA/B 実行 {TODAY_ISO}')
    print('━' * 60)

    client = SalesforceClient()
    client.authenticate()

    # STEP 0: SFデータリフレッシュ
    export_sf_data(client)

    # パイプライン実行
    print('\n\n')
    from pipeline_hellowork_AB import main as run_pipeline
    import_df = run_pipeline()

    print(f'\n最終結果: {len(import_df):,}件のインポート候補')
    return import_df


if __name__ == '__main__':
    main()
