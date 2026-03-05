# -*- coding: utf-8 -*-
"""
きらケア・看護のお仕事 レポート作成
"""
import sys
import io
from pathlib import Path
from datetime import datetime
import json

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.api.salesforce_client import SalesforceClient
import requests

TODAY = datetime.now().strftime('%Y%m%d')
BATCH_ID = f'BATCH_{TODAY}_KIRACARE_KANGOOSHIGOTO'

def create_report(client, name, object_name, filter_column, filter_value, columns):
    """レポートを作成"""
    print(f'レポート作成: {name}')

    # レポートメタデータ
    report_metadata = {
        "reportMetadata": {
            "name": name,
            "reportFormat": "TABULAR",
            "reportType": {"type": object_name + "s" if object_name != "Lead" else "LeadList"},
            "detailColumns": columns,
            "reportFilters": [
                {
                    "column": filter_column,
                    "operator": "contains",
                    "value": filter_value
                }
            ]
        }
    }

    # Analytics APIでレポート作成
    url = f"{client.instance_url}/services/data/{client.api_version}/analytics/reports"
    response = requests.post(url, headers=client._get_headers(), json=report_metadata)

    if response.status_code == 200:
        result = response.json()
        report_id = result.get('attributes', {}).get('reportId')
        print(f'  作成成功: {report_id}')
        return report_id
    else:
        print(f'  作成失敗: {response.status_code}')
        print(f'  {response.text[:500]}')
        return None

def create_simple_report_via_metadata(client, name, description):
    """Metadata APIでシンプルなレポート作成"""
    # Tooling APIを使用
    url = f"{client.instance_url}/services/data/{client.api_version}/tooling/sobjects/Report"

    payload = {
        "Name": name,
        "DeveloperName": name.replace(' ', '_').replace('・', '_').replace('（', '_').replace('）', '_'),
        "Description": description,
        "FolderName": "unfiled$public"
    }

    response = requests.post(url, headers=client._get_headers(), json=payload)
    print(f'Response: {response.status_code}')
    if response.status_code in [200, 201]:
        return response.json().get('id')
    else:
        print(response.text[:500])
        return None

def main():
    print('=' * 60)
    print('きらケア・看護のお仕事 レポート作成')
    print('=' * 60)
    print()

    client = SalesforceClient()
    client.authenticate()
    print()

    print(f'バッチID: {BATCH_ID}')
    print()

    # 作成済みリードのIDを使ってレポートURL生成
    base_dir = Path(__file__).parent.parent
    ids_file = base_dir / 'data' / 'output' / 'media_matching' / f'created_lead_ids_{TODAY}_kiracare_kango.csv'

    if ids_file.exists():
        import pandas as pd
        df_ids = pd.read_csv(ids_file, dtype=str)
        # sf__Idまたはidカラムを確認
        if 'sf__Id' in df_ids.columns:
            created_ids = df_ids['sf__Id'].dropna().tolist()
        elif 'id' in df_ids.columns:
            created_ids = df_ids['id'].dropna().tolist()
        else:
            # 最初の列を使用
            created_ids = df_ids.iloc[:, 0].dropna().tolist()

        print(f'作成済みリードID: {len(created_ids)}件')

        # 最初のいくつかのIDを確認
        print(f'  サンプルID: {created_ids[:3]}')
    else:
        print('作成済みリードIDファイルが見つかりません')
        created_ids = []

    print()
    print('=' * 60)
    print('レポートURLガイド')
    print('=' * 60)
    print()
    print('以下の条件でSalesforceでレポートを手動作成してください:')
    print()
    print('1. 新規リード作成レポート (621件)')
    print(f'   オブジェクト: Lead')
    print(f'   フィルタ: Paid_Memo__c に "{BATCH_ID}" を含む')
    print(f'   または: 作成日 = 今日 AND LeadSource = "きらケア" OR "看護のお仕事"')
    print()
    print('2. Lead更新レポート (288件)')
    print(f'   オブジェクト: Lead')
    print(f'   フィルタ: Paid_Memo__c に "{BATCH_ID}" を含む AND 作成日 != 今日')
    print()
    print('3. Account更新レポート (1,643件)')
    print(f'   オブジェクト: Account')
    print(f'   フィルタ: Paid_Memo__c に "{BATCH_ID}" を含む')
    print()

    # SOQL でレコード数を確認
    print('=== レコード数確認 ===')

    # 新規リード
    soql = f"SELECT COUNT() FROM Lead WHERE Paid_Memo__c LIKE '%{BATCH_ID}%' AND CreatedDate = TODAY"
    url = f"{client.instance_url}/services/data/{client.api_version}/query"
    response = requests.get(url, headers=client._get_headers(), params={'q': soql})
    if response.status_code == 200:
        count = response.json().get('totalSize', 0)
        print(f'新規リード（今日作成）: {count}件')

    # Lead更新
    soql2 = f"SELECT COUNT() FROM Lead WHERE Paid_Memo__c LIKE '%{BATCH_ID}%' AND CreatedDate != TODAY"
    response2 = requests.get(url, headers=client._get_headers(), params={'q': soql2})
    if response2.status_code == 200:
        count2 = response2.json().get('totalSize', 0)
        print(f'Lead更新（既存）: {count2}件')

    # Account更新
    soql3 = f"SELECT COUNT() FROM Account WHERE Paid_Memo__c LIKE '%{BATCH_ID}%'"
    response3 = requests.get(url, headers=client._get_headers(), params={'q': soql3})
    if response3.status_code == 200:
        count3 = response3.json().get('totalSize', 0)
        print(f'Account更新: {count3}件')

    print()
    print('=' * 60)

if __name__ == '__main__':
    main()
