# -*- coding: utf-8 -*-
"""
Population__cを更新し、人口1万人以下のリードを削除
"""
import pandas as pd
import sys
import io
import time
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.api.salesforce_client import SalesforceClient
import requests

BASE_DIR = Path(__file__).parent.parent
UPDATE_FILE = BASE_DIR / 'data' / 'output' / 'hellowork' / 'hw_202601_population_update.csv'
DELETE_FILE = BASE_DIR / 'data' / 'output' / 'hellowork' / 'hw_202601_leads_to_delete.csv'

def bulk_update(client, df, object_name='Lead'):
    """Bulk API 2.0 でupsert"""
    print(f'Bulk API 2.0 で {object_name} を更新中...')

    # CSVをBulk形式に変換（Id列は必須）
    csv_data = df.to_csv(index=False, encoding='utf-8')

    # ジョブ作成
    job_url = f"{client.instance_url}/services/data/{client.api_version}/jobs/ingest"
    job_payload = {
        "object": object_name,
        "contentType": "CSV",
        "operation": "update",
        "lineEnding": "CRLF"
    }

    response = requests.post(job_url, headers=client._get_headers(), json=job_payload)
    response.raise_for_status()
    job_id = response.json()['id']
    print(f'  ジョブ作成: {job_id}')

    # データアップロード
    upload_url = f"{job_url}/{job_id}/batches"
    headers = client._get_headers()
    headers['Content-Type'] = 'text/csv'
    response = requests.put(upload_url, headers=headers, data=csv_data.encode('utf-8'))
    response.raise_for_status()
    print(f'  データアップロード完了')

    # ジョブクローズ
    close_url = f"{job_url}/{job_id}"
    response = requests.patch(close_url, headers=client._get_headers(), json={"state": "UploadComplete"})
    response.raise_for_status()
    print(f'  ジョブクローズ')

    # 完了待ち
    while True:
        time.sleep(3)
        response = requests.get(close_url, headers=client._get_headers())
        response.raise_for_status()
        state = response.json()['state']
        print(f'  状態: {state}')
        if state in ['JobComplete', 'Failed', 'Aborted']:
            break

    # 結果取得
    result = response.json()
    print(f'  処理完了: {result.get("numberRecordsProcessed", 0)} 件')
    print(f'  成功: {result.get("numberRecordsProcessed", 0) - result.get("numberRecordsFailed", 0)} 件')
    print(f'  失敗: {result.get("numberRecordsFailed", 0)} 件')

    return result

def bulk_delete(client, ids, object_name='Lead'):
    """Bulk API 2.0 で削除"""
    print(f'\nBulk API 2.0 で {len(ids)} 件の {object_name} を削除中...')

    # CSVを作成（Id列のみ）
    csv_data = 'Id\n' + '\n'.join(ids)

    # ジョブ作成
    job_url = f"{client.instance_url}/services/data/{client.api_version}/jobs/ingest"
    job_payload = {
        "object": object_name,
        "contentType": "CSV",
        "operation": "delete",
        "lineEnding": "CRLF"
    }

    response = requests.post(job_url, headers=client._get_headers(), json=job_payload)
    response.raise_for_status()
    job_id = response.json()['id']
    print(f'  ジョブ作成: {job_id}')

    # データアップロード
    upload_url = f"{job_url}/{job_id}/batches"
    headers = client._get_headers()
    headers['Content-Type'] = 'text/csv'
    response = requests.put(upload_url, headers=headers, data=csv_data.encode('utf-8'))
    response.raise_for_status()
    print(f'  データアップロード完了')

    # ジョブクローズ
    close_url = f"{job_url}/{job_id}"
    response = requests.patch(close_url, headers=client._get_headers(), json={"state": "UploadComplete"})
    response.raise_for_status()
    print(f'  ジョブクローズ')

    # 完了待ち
    while True:
        time.sleep(3)
        response = requests.get(close_url, headers=client._get_headers())
        response.raise_for_status()
        state = response.json()['state']
        print(f'  状態: {state}')
        if state in ['JobComplete', 'Failed', 'Aborted']:
            break

    # 結果取得
    result = response.json()
    print(f'  処理完了: {result.get("numberRecordsProcessed", 0)} 件')
    print(f'  成功: {result.get("numberRecordsProcessed", 0) - result.get("numberRecordsFailed", 0)} 件')
    print(f'  失敗: {result.get("numberRecordsFailed", 0)} 件')

    return result

def main():
    print('=' * 60)
    print('Population__c 更新 & 人口1万人以下リード削除')
    print('=' * 60)
    print()

    # ファイル読み込み
    df_update = pd.read_csv(UPDATE_FILE, dtype={'Id': str, 'Population__c': int})
    df_delete = pd.read_csv(DELETE_FILE, dtype=str)

    print(f'更新対象: {len(df_update):,} 件')
    print(f'削除対象: {len(df_delete):,} 件')
    print()

    # Salesforce認証
    client = SalesforceClient()
    client.authenticate()

    # Step 1: Population__cを更新
    print('=' * 40)
    print('Step 1: Population__c を更新')
    print('=' * 40)
    update_result = bulk_update(client, df_update[['Id', 'Population__c']], 'Lead')

    # Step 2: 人口1万人以下を削除
    print()
    print('=' * 40)
    print('Step 2: 人口1万人以下のリードを削除')
    print('=' * 40)
    delete_ids = df_delete['Id'].tolist()
    delete_result = bulk_delete(client, delete_ids, 'Lead')

    # サマリー
    print()
    print('=' * 60)
    print('完了サマリー')
    print('=' * 60)
    print(f'Population__c 更新: {update_result.get("numberRecordsProcessed", 0) - update_result.get("numberRecordsFailed", 0)} 件成功')
    print(f'リード削除: {delete_result.get("numberRecordsProcessed", 0) - delete_result.get("numberRecordsFailed", 0)} 件成功')
    final_count = len(df_update) - len(df_delete)
    print(f'最終リード数: {final_count:,} 件')
    print('=' * 60)

if __name__ == '__main__':
    main()
