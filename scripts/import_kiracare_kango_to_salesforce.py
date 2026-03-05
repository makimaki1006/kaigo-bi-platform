# -*- coding: utf-8 -*-
"""
きらケア・看護のお仕事 Salesforceインポート
"""
import pandas as pd
import sys
import io
import time
from pathlib import Path
from datetime import datetime

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.api.salesforce_client import SalesforceClient
import requests

BASE_DIR = Path(__file__).parent.parent
OUTPUT_DIR = BASE_DIR / 'data' / 'output' / 'media_matching'
TODAY = datetime.now().strftime('%Y%m%d')

def bulk_operation(client, df, object_name, operation):
    """Bulk API 2.0で操作実行"""
    print(f'  Bulk API 2.0: {object_name} {operation} ({len(df)}件)...')

    # CSV生成（CRLF改行）
    csv_data = df.to_csv(index=False, lineterminator='\r\n')

    # ジョブ作成
    job_url = f"{client.instance_url}/services/data/{client.api_version}/jobs/ingest"
    job_payload = {
        "object": object_name,
        "contentType": "CSV",
        "operation": operation,
        "lineEnding": "CRLF"
    }

    response = requests.post(job_url, headers=client._get_headers(), json=job_payload)
    response.raise_for_status()
    job_id = response.json()['id']
    print(f'    ジョブ作成: {job_id}')

    # データアップロード
    upload_url = f"{job_url}/{job_id}/batches"
    headers = client._get_headers()
    headers['Content-Type'] = 'text/csv'
    response = requests.put(upload_url, headers=headers, data=csv_data.encode('utf-8'))
    response.raise_for_status()

    # ジョブクローズ
    close_url = f"{job_url}/{job_id}"
    response = requests.patch(close_url, headers=client._get_headers(), json={"state": "UploadComplete"})
    response.raise_for_status()

    # 完了待ち
    while True:
        time.sleep(3)
        response = requests.get(close_url, headers=client._get_headers())
        response.raise_for_status()
        result = response.json()
        state = result['state']
        print(f'    状態: {state}')
        if state in ['JobComplete', 'Failed', 'Aborted']:
            break

    processed = result.get('numberRecordsProcessed', 0)
    failed = result.get('numberRecordsFailed', 0)
    print(f'    処理: {processed}件, 成功: {processed - failed}件, 失敗: {failed}件')

    # 成功結果を取得（insert時はIDを保存）
    if operation == 'insert' and state == 'JobComplete':
        success_url = f"{close_url}/successfulResults"
        response = requests.get(success_url, headers=client._get_headers())
        if response.status_code == 200:
            return response.text, result
    return None, result

def main():
    print('=' * 60)
    print('きらケア・看護のお仕事 Salesforceインポート')
    print('=' * 60)
    print()

    # Salesforce認証
    client = SalesforceClient()
    client.authenticate()
    print()

    results = {}

    # 1. Account更新
    print('1. Account更新')
    acc_file = OUTPUT_DIR / f'kiracare_kango_account_updates_{TODAY}.csv'
    if acc_file.exists():
        df_acc = pd.read_csv(acc_file, dtype=str)
        _, results['account'] = bulk_operation(client, df_acc, 'Account', 'update')
    else:
        print('  ファイルなし')
    print()

    # 2. Lead更新
    print('2. Lead更新')
    lead_file = OUTPUT_DIR / f'kiracare_kango_lead_updates_{TODAY}.csv'
    if lead_file.exists():
        df_lead = pd.read_csv(lead_file, dtype=str)
        _, results['lead_update'] = bulk_operation(client, df_lead, 'Lead', 'update')
    else:
        print('  ファイルなし')
    print()

    # 3. 新規リード作成
    print('3. 新規リード作成')
    new_file = OUTPUT_DIR / f'kiracare_kango_new_leads_{TODAY}_filtered.csv'
    if new_file.exists():
        df_new = pd.read_csv(new_file, dtype=str)
        # phone_normalizedがあれば削除
        if 'phone_normalized' in df_new.columns:
            df_new = df_new.drop(columns=['phone_normalized'])
        success_csv, results['new_lead'] = bulk_operation(client, df_new, 'Lead', 'insert')

        # 作成IDを保存
        if success_csv:
            created_ids_file = OUTPUT_DIR / f'created_lead_ids_{TODAY}_kiracare_kango.csv'
            with open(created_ids_file, 'w', encoding='utf-8-sig') as f:
                f.write(success_csv)
            print(f'    作成ID保存: {created_ids_file}')
    else:
        print('  ファイルなし')
    print()

    # サマリー
    print('=' * 60)
    print('サマリー')
    print('=' * 60)
    if 'account' in results:
        r = results['account']
        print(f"Account更新: {r.get('numberRecordsProcessed', 0) - r.get('numberRecordsFailed', 0)}件成功")
    if 'lead_update' in results:
        r = results['lead_update']
        print(f"Lead更新: {r.get('numberRecordsProcessed', 0) - r.get('numberRecordsFailed', 0)}件成功")
    if 'new_lead' in results:
        r = results['new_lead']
        print(f"新規リード作成: {r.get('numberRecordsProcessed', 0) - r.get('numberRecordsFailed', 0)}件成功")
    print('=' * 60)

if __name__ == '__main__':
    main()
