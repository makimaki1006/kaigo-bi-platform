"""
ウェルミージョブ 101件 Salesforceインポート + レポート作成
"""
import pandas as pd
import sys
import os
import time
import json
import requests
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))
sys.stdout.reconfigure(encoding='utf-8')

from api.salesforce_client import SalesforceClient

BASE_DIR = Path(__file__).parent.parent
INPUT_FILE = BASE_DIR / 'data/output/google_scraping/kaigojob_sf_import_20260309.csv'
OUTPUT_DIR = BASE_DIR / 'data/output/google_scraping'

BATCH_LABEL = '【BATCH_20260309_KAIGOJOB】'


def create_lead_bulk(client, df):
    """Bulk API 2.0でリード作成"""
    import io
    import csv

    headers = client._get_headers()
    api_version = 'v59.0'

    # Bulk API 2.0 ジョブ作成
    job_url = f'{client.instance_url}/services/data/{api_version}/jobs/ingest'
    job_payload = {
        'object': 'Lead',
        'operation': 'insert',
        'contentType': 'CSV',
        'lineEnding': 'CRLF',
    }
    resp = requests.post(job_url, headers=headers, json=job_payload)
    resp.raise_for_status()
    job_id = resp.json()['id']
    print(f"  ジョブ作成: {job_id}")

    # CSVデータアップロード
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, lineterminator='\r\n')
    csv_data = csv_buffer.getvalue()

    upload_url = f'{job_url}/{job_id}/batches'
    upload_headers = {**headers, 'Content-Type': 'text/csv'}
    resp = requests.put(upload_url, headers=upload_headers, data=csv_data.encode('utf-8'))
    resp.raise_for_status()
    print(f"  データアップロード完了: {len(df)}件")

    # ジョブクローズ
    close_url = f'{job_url}/{job_id}'
    resp = requests.patch(close_url, headers=headers, json={'state': 'UploadComplete'})
    resp.raise_for_status()
    print(f"  ジョブクローズ")

    # 完了待ち
    for i in range(60):
        time.sleep(5)
        resp = requests.get(close_url, headers=headers)
        resp.raise_for_status()
        state = resp.json()['state']
        processed = resp.json().get('numberRecordsProcessed', 0)
        failed = resp.json().get('numberRecordsFailed', 0)
        print(f"  状態: {state} (処理済: {processed}, 失敗: {failed})")
        if state in ('JobComplete', 'Failed', 'Aborted'):
            break

    result = resp.json()

    # 成功レコード取得
    success_url = f'{job_url}/{job_id}/successfulResults'
    resp = requests.get(success_url, headers=headers)
    resp.raise_for_status()

    success_ids = []
    if resp.text.strip():
        lines = resp.text.strip().split('\n')
        if len(lines) > 1:
            reader = pd.read_csv(pd.io.common.StringIO(resp.text))
            success_ids = reader['sf__Id'].tolist()

    # 失敗レコード取得
    fail_url = f'{job_url}/{job_id}/failedResults'
    resp = requests.get(fail_url, headers=headers)
    failed_records = []
    if resp.text.strip():
        lines = resp.text.strip().split('\n')
        if len(lines) > 1:
            fail_reader = pd.read_csv(pd.io.common.StringIO(resp.text))
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
    """Salesforceレポートを作成"""
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
    print("=" * 70)
    print("ウェルミージョブ Salesforceインポート実行")
    print("=" * 70)

    # データ読み込み
    df = pd.read_csv(INPUT_FILE, encoding='utf-8-sig', dtype=str)
    print(f"\nインポート対象: {len(df)}件")
    print(f"所有者: 藤巻 真弥（全件）")

    # SF認証
    client = SalesforceClient()
    client.authenticate()
    print("SF認証成功")

    # === Step 1: リード作成 ===
    print("\n--- Step 1: リード作成（Bulk API 2.0）---")
    result = create_lead_bulk(client, df)

    print(f"\n  結果: {result['state']}")
    print(f"  処理: {result['total']}件")
    print(f"  成功: {len(result['success_ids'])}件")
    print(f"  失敗: {result['failed']}件")

    # 成功IDを保存
    if result['success_ids']:
        ids_file = OUTPUT_DIR / 'created_lead_ids_20260309.csv'
        pd.DataFrame({'Id': result['success_ids']}).to_csv(ids_file, index=False, encoding='utf-8-sig')
        print(f"  作成済みID: {ids_file}")

    # 失敗レコードを保存
    if result['failed_records']:
        fail_file = OUTPUT_DIR / 'failed_leads_20260309.csv'
        pd.DataFrame(result['failed_records']).to_csv(fail_file, index=False, encoding='utf-8-sig')
        print(f"  失敗レコード: {fail_file}")
        print(f"  失敗詳細:")
        for rec in result['failed_records'][:5]:
            print(f"    {rec.get('sf__Error', 'unknown')}")

    # === Step 2: レポート作成 ===
    if len(result['success_ids']) > 0:
        print("\n--- Step 2: レポート作成 ---")

        # 作成日でフィルタ（今日）
        today = datetime.now().strftime('%Y-%m-%d')

        try:
            report_id = create_report(
                client,
                name=f'ウェルミージョブ新規リード_{today}',
                report_type='LeadList',
                columns=[
                    'LEAD.NAME',
                    'COMPANY',
                    'LEAD.PHONE',
                    'Lead.NumberOfEmployees',
                    'Lead.CorporateNumber__c',
                    'Lead.Prefecture__c',
                    'LEAD.STREET',
                    'Lead.PresidentName__c',
                    'LEAD.TITLE',
                    'Lead.Establish__c',
                    'Lead.Name_Kana__c',
                    'Lead.Paid_Memo__c',
                    'LEAD.CREATED_DATE',
                    'OWNER',
                ],
                filters=[
                    {
                        'column': 'LEAD.CREATED_DATE',
                        'operator': 'equals',
                        'value': 'TODAY',
                    },
                    {
                        'column': 'OWNER',
                        'operator': 'equals',
                        'value': '藤巻 真弥',
                    },
                    {
                        'column': 'Lead.Paid_Memo__c',
                        'operator': 'contains',
                        'value': 'ウェルミージョブ',
                    },
                ]
            )
            report_url = f'{client.instance_url}/lightning/r/Report/{report_id}/view'
            print(f"  レポート作成成功: {report_url}")
        except Exception as e:
            print(f"  レポート作成エラー: {e}")
            # フォールバック: よりシンプルなフィルタで再試行
            try:
                report_id = create_report(
                    client,
                    name=f'ウェルミージョブ新規リード_{today}',
                    report_type='LeadList',
                    columns=[
                        'LEAD.NAME',
                        'COMPANY',
                        'LEAD.PHONE',
                        'Lead.NumberOfEmployees',
                        'Lead.Prefecture__c',
                        'Lead.Paid_Memo__c',
                        'LEAD.CREATED_DATE',
                        'OWNER',
                    ],
                    filters=[
                        {
                            'column': 'LEAD.CREATED_DATE',
                            'operator': 'equals',
                            'value': 'TODAY',
                        },
                        {
                            'column': 'Lead.Paid_Memo__c',
                            'operator': 'contains',
                            'value': 'ウェルミージョブ',
                        },
                    ]
                )
                report_url = f'{client.instance_url}/lightning/r/Report/{report_id}/view'
                print(f"  レポート作成成功（簡易版）: {report_url}")
            except Exception as e2:
                print(f"  レポート作成エラー（リトライ）: {e2}")

    # === サマリー ===
    print("\n" + "=" * 70)
    print("完了サマリー")
    print("=" * 70)
    print(f"  インポート対象: {len(df)}件")
    print(f"  成功: {len(result['success_ids'])}件")
    print(f"  失敗: {result['failed']}件")
    print(f"  所有者: 藤巻 真弥")


if __name__ == '__main__':
    main()
