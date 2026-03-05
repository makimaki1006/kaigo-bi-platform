# -*- coding: utf-8 -*-
"""
アタックリスト用レポート作成
優先度S/A/B/Cのセグメント別（Accountベース）
"""

import sys
import io
from pathlib import Path
import json
import requests

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.services.opportunity_service import OpportunityService


def main():
    service = OpportunityService()
    service.authenticate()

    print('='*80)
    print('アタックリスト用レポート作成')
    print('='*80)

    headers = {
        'Authorization': f'Bearer {service.access_token}',
        'Content-Type': 'application/json'
    }

    # レポートタイプを調べる
    print('\n■ 利用可能なレポートタイプを確認')
    all_report_types = []
    account_report_type = None
    try:
        url = f"{service.instance_url}/services/data/{service.api_version}/analytics/reportTypes"
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            all_report_types = response.json()
            print(f'  全レポートタイプ数: {len(all_report_types)}')
            for rt in all_report_types:
                # 全フィールドを表示
                print(f'    {rt}')
                label = rt.get("label", "")
                if "取引先" in label and "取引先責任者" in label:
                    account_report_type = rt
        else:
            print(f'  取得失敗: {response.status_code}')
    except Exception as e:
        print(f'  エラー: {e}')

    # AccountListレポートタイプの詳細を取得
    print('\n■ AccountListレポートタイプの詳細（カラム情報）')
    account_columns = {}
    try:
        # describeURLから詳細を取得（report-types形式）
        url = f"{service.instance_url}/services/data/{service.api_version}/analytics/report-types/AccountList"
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            rt_detail = response.json()
            columns = rt_detail.get('columns', {})
            account_columns = columns
            print(f'  使用可能なカラム数: {len(columns)}')
            # 主要なカラムを表示
            for col_name, col_info in list(columns.items())[:20]:
                label = col_info.get('label', '')
                print(f'    {col_name}: {label}')
        else:
            print(f'  取得失敗（report-types形式）: {response.status_code}')
            # 代替形式を試す
            url2 = f"{service.instance_url}/services/data/{service.api_version}/analytics/reportTypes/AccountList"
            response2 = requests.get(url2, headers=headers)
            if response2.status_code == 200:
                rt_detail = response2.json()
                print(f'  代替形式で成功: {list(rt_detail.keys())}')
            else:
                print(f'  代替形式も失敗: {response2.status_code}')
    except Exception as e:
        print(f'  エラー: {e}')

    # 標準フィールドのみでシンプルなレポートを作成してみる
    print('\n■ シンプルなレポート作成テスト')
    test_report_metadata = {
        "reportMetadata": {
            "name": "テスト_Account一覧",
            "reportFormat": "TABULAR",
            "reportType": {"type": "AccountList"},
            "detailColumns": [
                "ACCOUNT_NAME",
                "PHONE1"
            ]
        }
    }

    try:
        url = f"{service.instance_url}/services/data/{service.api_version}/analytics/reports"
        response = requests.post(url, headers=headers, json=test_report_metadata)
        if response.status_code in [200, 201]:
            result = response.json()
            report_id = result.get('reportMetadata', {}).get('id')
            print(f'  テストレポート作成成功: {report_id}')
        else:
            print(f'  テストレポート作成失敗: {response.status_code}')
            print(f'  {response.text[:500]}')
    except Exception as e:
        print(f'  エラー: {e}')

    # REST APIでクエリ実行
    def run_query(soql):
        url = f"{service.instance_url}/services/data/{service.api_version}/query"
        params = {'q': soql}
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Query failed: {response.text}")

    # Accountベースで優先度別の件数を確認
    queries = {
        'S': """
            SELECT COUNT(Id) cnt
            FROM Account
            WHERE LegalPersonality__c = '株式会社'
              AND ServiceType__c LIKE '%訪問%'
        """,
        'A': """
            SELECT COUNT(Id) cnt
            FROM Account
            WHERE LegalPersonality__c = '株式会社'
              AND (NOT ServiceType__c LIKE '%訪問%')
        """,
        'B': """
            SELECT COUNT(Id) cnt
            FROM Account
            WHERE LegalPersonality__c != '株式会社'
              AND LegalPersonality__c != null
              AND (ServiceType__c LIKE '%訪問%' OR ServiceType__c LIKE '%通所%')
        """,
        'C': """
            SELECT COUNT(Id) cnt
            FROM Account
            WHERE LegalPersonality__c != '株式会社'
              AND LegalPersonality__c != null
              AND (NOT ServiceType__c LIKE '%訪問%')
              AND (NOT ServiceType__c LIKE '%通所%')
        """
    }

    print('\n■ Account件数確認')
    counts = {}
    for name, soql in queries.items():
        try:
            result = run_query(soql)
            cnt = result['records'][0]['cnt'] if result['records'] else 0
            counts[name] = cnt
            print(f'  優先度{name}: {cnt:,}件')
        except Exception as e:
            print(f'  優先度{name}: エラー - {e}')
            counts[name] = 0

    total = sum(counts.values())
    print(f'\n  合計: {total:,}件')

    # セグメント定義サマリー
    print('\n' + '='*80)
    print('【セグメント定義サマリー】')
    print('='*80)

    print(f'''
┌──────┬───────────────────────┬─────────────┐
│優先度│ 条件                  │ 件数        │
├──────┼───────────────────────┼─────────────┤
│  S   │ 株式会社 × 訪問系     │ {counts.get('S', 0):>9,}件 │
│  A   │ 株式会社 × その他     │ {counts.get('A', 0):>9,}件 │
│  B   │ その他法人 × 訪問/通所│ {counts.get('B', 0):>9,}件 │
│  C   │ その他法人 × 入所/他  │ {counts.get('C', 0):>9,}件 │
├──────┼───────────────────────┼─────────────┤
│ 合計 │                       │ {total:>9,}件 │
└──────┴───────────────────────┴─────────────┘
''')

    # Salesforce上で手動作成するガイド
    print('='*80)
    print('【レポート作成ガイド】')
    print('='*80)

    instance_url = service.instance_url

    print(f'''
Salesforce Lightning Experienceでレポートを作成してください。

■ レポートビルダーへのアクセス
  {instance_url}/lightning/o/Report/new

■ レポートタイプ選択
  「取引先」または「取引先と取引先責任者」を選択

■ 作成するレポート（4つ）

┌─────────────────────────────────────────────────────────────────────┐
│ 1. アタックリスト_優先度S_株式会社×訪問系                          │
├─────────────────────────────────────────────────────────────────────┤
│ フィルタ:                                                           │
│   - 法人格 (LegalPersonality__c) = 株式会社                        │
│   - サービス種別 (ServiceType__c) に「訪問」を含む                 │
│ 予想件数: {counts.get('S', 0):,}件                                                │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ 2. アタックリスト_優先度A_株式会社×その他                          │
├─────────────────────────────────────────────────────────────────────┤
│ フィルタ:                                                           │
│   - 法人格 (LegalPersonality__c) = 株式会社                        │
│   - サービス種別 (ServiceType__c) に「訪問」を含まない             │
│ 予想件数: {counts.get('A', 0):,}件                                                │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ 3. アタックリスト_優先度B_その他法人×訪問通所                      │
├─────────────────────────────────────────────────────────────────────┤
│ フィルタ:                                                           │
│   - 法人格 (LegalPersonality__c) ≠ 株式会社                        │
│   - サービス種別 (ServiceType__c) に「訪問」または「通所」を含む   │
│ ※フィルターロジック: 1 AND (2 OR 3) で設定                        │
│ 予想件数: {counts.get('B', 0):,}件                                                │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ 4. アタックリスト_優先度C_その他法人×入所等                        │
├─────────────────────────────────────────────────────────────────────┤
│ フィルタ:                                                           │
│   - 法人格 (LegalPersonality__c) ≠ 株式会社                        │
│   - サービス種別 (ServiceType__c) に「訪問」を含まない             │
│   - サービス種別 (ServiceType__c) に「通所」を含まない             │
│ 予想件数: {counts.get('C', 0):,}件                                               │
└─────────────────────────────────────────────────────────────────────┘

■ 推奨表示列（アウトライン）
  - 取引先名 (Name)
  - 法人格 (LegalPersonality__c)
  - サービス種別 (ServiceType__c)
  - 都道府県 (Prefectures__c)
  - 電話番号 (Phone)
  - 所有者 (Owner)

■ 保存先フォルダ
  「公開レポート」または任意のフォルダ

■ 注意事項
  - 各レポートはフォーマット「表形式」で作成
  - フィルターの「含む/含まない」は部分一致で動作
  - 優先度Bはフィルターロジックの設定が必要
''')

    # SOQLでの抽出用クエリも提供
    print('='*80)
    print('【SOQL抽出クエリ（データローダー用）】')
    print('='*80)

    print('''
■ 優先度S（株式会社×訪問系）
SELECT Id, Name, LegalPersonality__c, ServiceType__c, Prefectures__c, Phone
FROM Account
WHERE LegalPersonality__c = '株式会社'
  AND ServiceType__c LIKE '%訪問%'

■ 優先度A（株式会社×その他）
SELECT Id, Name, LegalPersonality__c, ServiceType__c, Prefectures__c, Phone
FROM Account
WHERE LegalPersonality__c = '株式会社'
  AND (NOT ServiceType__c LIKE '%訪問%')

■ 優先度B（その他法人×訪問/通所）
SELECT Id, Name, LegalPersonality__c, ServiceType__c, Prefectures__c, Phone
FROM Account
WHERE LegalPersonality__c != '株式会社'
  AND LegalPersonality__c != null
  AND (ServiceType__c LIKE '%訪問%' OR ServiceType__c LIKE '%通所%')

■ 優先度C（その他法人×入所/他）
SELECT Id, Name, LegalPersonality__c, ServiceType__c, Prefectures__c, Phone
FROM Account
WHERE LegalPersonality__c != '株式会社'
  AND LegalPersonality__c != null
  AND (NOT ServiceType__c LIKE '%訪問%')
  AND (NOT ServiceType__c LIKE '%通所%')
''')

    return counts


if __name__ == "__main__":
    counts = main()
