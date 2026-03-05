# -*- coding: utf-8 -*-
"""
きらケア・看護のお仕事 Salesforceインポート（v3版）
レポート抽出用キーワード付き
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

    # 失敗結果を取得
    if failed > 0:
        fail_url = f"{close_url}/failedResults"
        response = requests.get(fail_url, headers=client._get_headers())
        if response.status_code == 200:
            print(f'    失敗詳細（先頭5件）:')
            lines = response.text.strip().split('\n')
            for line in lines[1:6]:  # ヘッダー除く先頭5件
                print(f'      {line[:200]}')

    return result

def main():
    print('=' * 60)
    print('きらケア・看護のお仕事 Salesforceインポート（v3版）')
    print('=' * 60)
    print()

    # Salesforce認証
    client = SalesforceClient()
    client.authenticate()
    print()

    results = {}

    # 1. Account更新
    print('1. Account更新')
    acc_file = OUTPUT_DIR / f'kiracare_kango_account_updates_{TODAY}_v3.csv'
    if acc_file.exists():
        df_acc = pd.read_csv(acc_file, dtype=str)
        print(f'  ファイル: {acc_file.name} ({len(df_acc)}件)')
        results['account'] = bulk_operation(client, df_acc, 'Account', 'update')
    else:
        print(f'  ファイルなし: {acc_file}')
    print()

    # 2. Lead更新（既存）
    print('2. Lead更新（既存）')
    lead_file = OUTPUT_DIR / f'kiracare_kango_lead_updates_{TODAY}_v3.csv'
    if lead_file.exists():
        df_lead = pd.read_csv(lead_file, dtype=str)
        print(f'  ファイル: {lead_file.name} ({len(df_lead)}件)')
        results['lead_update'] = bulk_operation(client, df_lead, 'Lead', 'update')
    else:
        print(f'  ファイルなし: {lead_file}')
    print()

    # 3. Lead更新（今日作成したリード）
    print('3. Lead更新（今日作成したリード）')
    created_file = OUTPUT_DIR / f'kiracare_kango_created_lead_updates_{TODAY}_v3.csv'
    if created_file.exists():
        df_created = pd.read_csv(created_file, dtype=str)
        print(f'  ファイル: {created_file.name} ({len(df_created)}件)')
        results['created_lead'] = bulk_operation(client, df_created, 'Lead', 'update')
    else:
        print(f'  ファイルなし: {created_file}')
    print()

    # サマリー
    print('=' * 60)
    print('インポートサマリー')
    print('=' * 60)
    if 'account' in results:
        r = results['account']
        success = r.get('numberRecordsProcessed', 0) - r.get('numberRecordsFailed', 0)
        print(f"Account更新: {success}件成功 / {r.get('numberRecordsFailed', 0)}件失敗")
    if 'lead_update' in results:
        r = results['lead_update']
        success = r.get('numberRecordsProcessed', 0) - r.get('numberRecordsFailed', 0)
        print(f"Lead更新（既存）: {success}件成功 / {r.get('numberRecordsFailed', 0)}件失敗")
    if 'created_lead' in results:
        r = results['created_lead']
        success = r.get('numberRecordsProcessed', 0) - r.get('numberRecordsFailed', 0)
        print(f"Lead更新（今日作成）: {success}件成功 / {r.get('numberRecordsFailed', 0)}件失敗")
    print('=' * 60)
    print()

    # レコード数確認（SOQL）
    print('=' * 60)
    print('レポート用キーワード別レコード数')
    print('=' * 60)

    url = f"{client.instance_url}/services/data/{client.api_version}/query"

    keywords = [
        ('KIRACARE_NEW_20260116', 'Lead', 'きらケア新規'),
        ('KIRACARE_UPDATE_20260116', 'Lead', 'きらケア更新（Lead）'),
        ('KIRACARE_UPDATE_20260116', 'Account', 'きらケア更新（Account）'),
        ('KANGO_NEW_20260116', 'Lead', '看護のお仕事新規'),
        ('KANGO_UPDATE_20260116', 'Lead', '看護のお仕事更新（Lead）'),
        ('KANGO_UPDATE_20260116', 'Account', '看護のお仕事更新（Account）'),
    ]

    for keyword, obj, label in keywords:
        soql = f"SELECT COUNT() FROM {obj} WHERE Paid_Memo__c LIKE '%{keyword}%'"
        response = requests.get(url, headers=client._get_headers(), params={'q': soql})
        if response.status_code == 200:
            count = response.json().get('totalSize', 0)
            print(f'{label}: {count}件')
        else:
            print(f'{label}: エラー ({response.status_code})')

    print('=' * 60)
    print()
    print('レポート作成用フィルタ条件:')
    print('  Lead新規（きらケア）: Paid_Memo__c に "KIRACARE_NEW_20260116" を含む')
    print('  Lead新規（看護）: Paid_Memo__c に "KANGO_NEW_20260116" を含む')
    print('  Lead更新（きらケア）: Paid_Memo__c に "KIRACARE_UPDATE_20260116" を含む')
    print('  Lead更新（看護）: Paid_Memo__c に "KANGO_UPDATE_20260116" を含む')
    print('  Account更新（きらケア）: Paid_Memo__c に "KIRACARE_UPDATE_20260116" を含む')
    print('  Account更新（看護）: Paid_Memo__c に "KANGO_UPDATE_20260116" を含む')
    print('=' * 60)

if __name__ == '__main__':
    main()
