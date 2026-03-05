# -*- coding: utf-8 -*-
"""人口1万人以下のリードを削除（改行コード修正版）"""
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
DELETE_FILE = BASE_DIR / 'data' / 'output' / 'hellowork' / 'hw_202601_leads_to_delete.csv'

def bulk_delete(client, ids, object_name='Lead'):
    """Bulk API 2.0 で削除（CRLF改行）"""
    print(f'Bulk API 2.0 で {len(ids)} 件の {object_name} を削除中...')

    # CSVを作成（Id列のみ、CRLF改行）
    csv_data = 'Id\r\n' + '\r\n'.join(ids) + '\r\n'

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
    processed = result.get("numberRecordsProcessed", 0)
    failed = result.get("numberRecordsFailed", 0)

    print(f'  処理完了: {processed} 件')
    print(f'  成功: {processed - failed} 件')
    print(f'  失敗: {failed} 件')

    if result.get('state') == 'Failed':
        print(f'  エラー: {result.get("errorMessage", "不明")}')

    return result

def main():
    print('=' * 60)
    print('人口1万人以下リード削除')
    print('=' * 60)
    print()

    # 削除対象読み込み
    df_delete = pd.read_csv(DELETE_FILE, dtype=str)
    print(f'削除対象: {len(df_delete):,} 件')

    # サンプル表示
    print()
    print('削除対象サンプル（先頭5件）:')
    print(df_delete[['Id', 'Company', 'Municipality', 'Population__c']].head().to_string(index=False))
    print()

    # Salesforce認証
    client = SalesforceClient()
    client.authenticate()

    # 削除実行
    delete_ids = df_delete['Id'].tolist()
    result = bulk_delete(client, delete_ids, 'Lead')

    print()
    print('=' * 60)
    if result.get('state') == 'JobComplete':
        success = result.get("numberRecordsProcessed", 0) - result.get("numberRecordsFailed", 0)
        print(f'削除成功: {success} 件')
    else:
        print(f'削除失敗: {result.get("errorMessage", "不明")}')
    print('=' * 60)

if __name__ == '__main__':
    main()
