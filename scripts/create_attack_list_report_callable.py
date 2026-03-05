# -*- coding: utf-8 -*-
"""
アタックリスト用レポート作成（架電可能のみ）
優先度S/A/B/Cのセグメント別（Accountベース）
成約先・商談中・過去客を除外
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
    print('アタックリスト用レポート作成（架電可能のみ）')
    print('='*80)

    headers = {
        'Authorization': f'Bearer {service.access_token}',
        'Content-Type': 'application/json'
    }

    # REST APIでクエリ実行
    def run_query(soql):
        url = f"{service.instance_url}/services/data/{service.api_version}/query"
        params = {'q': soql}
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Query failed: {response.text}")

    # 架電可能条件（完全版）
    # 1. Status__c に「商談中」「プロジェクト進行中」「深耕対象」「過去客」を含まない
    # 2. RelatedAccountFlg__c が「グループ案件進行中」「グループ過去案件実績あり」でない
    # 3. ApproachNG__c = false（アプローチ禁止でない）
    # 4. CallNotApplicable__c = false（架電対象外でない）
    # 5. Phone != null（電話番号あり）
    callable_condition = """
        AND (Status__c = null OR (
            (NOT Status__c LIKE '%商談中%')
            AND (NOT Status__c LIKE '%プロジェクト進行中%')
            AND (NOT Status__c LIKE '%深耕対象%')
            AND (NOT Status__c LIKE '%過去客%')
        ))
        AND (RelatedAccountFlg__c = null OR (
            RelatedAccountFlg__c != 'グループ案件進行中'
            AND RelatedAccountFlg__c != 'グループ過去案件実績あり'
        ))
        AND (ApproachNG__c = false OR ApproachNG__c = null)
        AND (CallNotApplicable__c = false OR CallNotApplicable__c = null)
        AND Phone != null
    """

    # Accountベースで優先度別の件数を確認（架電可能のみ）
    queries = {
        'S': f"""
            SELECT COUNT(Id) cnt
            FROM Account
            WHERE LegalPersonality__c = '株式会社'
              AND ServiceType__c LIKE '%訪問%'
              {callable_condition}
        """,
        'A': f"""
            SELECT COUNT(Id) cnt
            FROM Account
            WHERE LegalPersonality__c = '株式会社'
              AND (NOT ServiceType__c LIKE '%訪問%')
              {callable_condition}
        """,
        'B': f"""
            SELECT COUNT(Id) cnt
            FROM Account
            WHERE LegalPersonality__c != '株式会社'
              AND LegalPersonality__c != null
              AND (ServiceType__c LIKE '%訪問%' OR ServiceType__c LIKE '%通所%')
              {callable_condition}
        """,
        'C': f"""
            SELECT COUNT(Id) cnt
            FROM Account
            WHERE LegalPersonality__c != '株式会社'
              AND LegalPersonality__c != null
              AND (NOT ServiceType__c LIKE '%訪問%')
              AND (NOT ServiceType__c LIKE '%通所%')
              {callable_condition}
        """
    }

    print('\n■ 架電可能Account件数確認')
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
    print('【セグメント定義サマリー（架電可能のみ）】')
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

※ 除外条件:
  - Status__c に「商談中」「プロジェクト進行中」「深耕対象」「過去客」を含む
  - RelatedAccountFlg__c が「グループ案件進行中」「グループ過去案件実績あり」
  - ApproachNG__c = true（アプローチ禁止）
  - CallNotApplicable__c = true（架電対象外）
  - Phone = null（電話番号なし）
''')

    # レポート作成
    print('\n' + '='*80)
    print('【レポート作成】')
    print('='*80)

    # 共通の除外フィルタ（完全版）
    # Analytics APIでは複数フィルタを使う
    exclude_filters = [
        # ステータス除外
        {
            "column": "Account.Status__c",
            "operator": "notContain",
            "value": "商談中"
        },
        {
            "column": "Account.Status__c",
            "operator": "notContain",
            "value": "プロジェクト進行中"
        },
        {
            "column": "Account.Status__c",
            "operator": "notContain",
            "value": "深耕対象"
        },
        {
            "column": "Account.Status__c",
            "operator": "notContain",
            "value": "過去客"
        },
        # 関連アカウントフラグ除外
        {
            "column": "Account.RelatedAccountFlg__c",
            "operator": "notEqual",
            "value": "グループ案件進行中"
        },
        {
            "column": "Account.RelatedAccountFlg__c",
            "operator": "notEqual",
            "value": "グループ過去案件実績あり"
        },
        # アプローチ禁止除外
        {
            "column": "Account.ApproachNG__c",
            "operator": "equals",
            "value": "0"
        },
        # 架電対象外除外
        {
            "column": "Account.CallNotApplicable__c",
            "operator": "equals",
            "value": "0"
        },
        # 電話番号なし除外
        {
            "column": "PHONE1",
            "operator": "notEqual",
            "value": ""
        }
    ]

    # セグメント別フィルタ
    segment_filters = {
        'S_株式会社_訪問系': [
            {"column": "Account.LegalPersonality__c", "operator": "equals", "value": "株式会社"},
            {"column": "Account.ServiceType__c", "operator": "contains", "value": "訪問"}
        ],
        'A_株式会社_その他': [
            {"column": "Account.LegalPersonality__c", "operator": "equals", "value": "株式会社"},
            {"column": "Account.ServiceType__c", "operator": "notContain", "value": "訪問"}
        ],
        'B_その他法人_訪問通所': [
            {"column": "Account.LegalPersonality__c", "operator": "notEqual", "value": "株式会社"},
            # 訪問または通所を含むはフィルターロジックで対応が必要
            # ここではシンプルにサービス種別でフィルタしない（後でUIで調整）
        ],
        'C_その他法人_入所等': [
            {"column": "Account.LegalPersonality__c", "operator": "notEqual", "value": "株式会社"},
            {"column": "Account.ServiceType__c", "operator": "notContain", "value": "訪問"},
            {"column": "Account.ServiceType__c", "operator": "notContain", "value": "通所"}
        ]
    }

    # 表示列
    detail_columns = [
        "ACCOUNT.NAME",
        "Account.LegalPersonality__c",
        "Account.ServiceType__c",
        "Account.Prefectures__c",
        "PHONE1",
        "USERS.NAME",
        "Account.Status__c"
    ]

    created_reports = {}

    for segment_name, seg_filters in segment_filters.items():
        print(f'\n■ {segment_name} レポート作成中...')

        # フィルタを結合
        all_filters = seg_filters + exclude_filters

        report_metadata = {
            "reportMetadata": {
                "name": f"アタックリスト_架電可能_{segment_name}",
                "reportFormat": "TABULAR",
                "reportType": {"type": "AccountList"},
                "detailColumns": detail_columns,
                "reportFilters": all_filters
            }
        }

        try:
            url = f"{service.instance_url}/services/data/{service.api_version}/analytics/reports"
            response = requests.post(url, headers=headers, json=report_metadata)

            if response.status_code in [200, 201]:
                result = response.json()
                report_id = result.get('reportMetadata', {}).get('id')
                print(f'  作成成功: {report_id}')
                created_reports[segment_name] = report_id
            else:
                print(f'  作成失敗: {response.status_code}')
                print(f'  {response.text[:500]}')
        except Exception as e:
            print(f'  エラー: {e}')

    # 結果サマリー
    print('\n' + '='*80)
    print('【作成結果サマリー】')
    print('='*80)

    instance_url = service.instance_url
    for segment_name, report_id in created_reports.items():
        print(f'\n{segment_name}:')
        print(f'  ID: {report_id}')
        print(f'  URL: {instance_url}/lightning/r/Report/{report_id}/view')

    print('\n' + '='*80)
    print('【注意】')
    print('='*80)
    print('''
優先度B（その他法人×訪問/通所）は「訪問 OR 通所」の条件が
Analytics APIでは直接表現できないため、レポートビルダーで
フィルターロジックを手動で調整してください:

  1 AND 2 AND 3 AND 4 AND 5 AND 6 AND 7 AND (8 OR 9)

  ※ 8: ServiceType__c contains '訪問'
  ※ 9: ServiceType__c contains '通所'
''')

    return created_reports


if __name__ == "__main__":
    created_reports = main()
