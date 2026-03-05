# -*- coding: utf-8 -*-
"""
アタックリスト用レポート作成（従業員規模セグメント含む）
優先度S/A/B/C × 従業員規模（小/中/大/不明）
"""

import sys
import io
from pathlib import Path
import requests

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.services.opportunity_service import OpportunityService


def main():
    service = OpportunityService()
    service.authenticate()

    print('='*80)
    print('アタックリスト用レポート作成（従業員規模セグメント含む）')
    print('='*80)

    headers = {
        'Authorization': f'Bearer {service.access_token}',
        'Content-Type': 'application/json'
    }

    def run_query(soql):
        url = f"{service.instance_url}/services/data/{service.api_version}/query"
        params = {'q': soql}
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Query failed: {response.text}")

    # 架電可能条件（完全版）
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

    # 優先度条件
    priority_conditions = {
        'S': "LegalPersonality__c = '株式会社' AND ServiceType__c LIKE '%訪問%'",
        'A': "LegalPersonality__c = '株式会社' AND (NOT ServiceType__c LIKE '%訪問%')",
        'B': "LegalPersonality__c != '株式会社' AND LegalPersonality__c != null AND (ServiceType__c LIKE '%訪問%' OR ServiceType__c LIKE '%通所%')",
        'C': "LegalPersonality__c != '株式会社' AND LegalPersonality__c != null AND (NOT ServiceType__c LIKE '%訪問%') AND (NOT ServiceType__c LIKE '%通所%')"
    }

    # 従業員規模条件
    employee_conditions = {
        '小規模_50人以下': 'NumberOfEmployees <= 50',
        '中規模_51_200人': 'NumberOfEmployees > 50 AND NumberOfEmployees <= 200',
        '大規模_200人超': 'NumberOfEmployees > 200',
        '規模不明': 'NumberOfEmployees = null'
    }

    # 件数集計
    print('\n■ セグメント別件数')
    print('='*80)

    counts = {}
    for priority, p_cond in priority_conditions.items():
        for emp_name, emp_cond in employee_conditions.items():
            segment_name = f'{priority}_{emp_name}'
            soql = f"""
                SELECT COUNT(Id) cnt FROM Account
                WHERE {p_cond}
                {callable_condition}
                AND {emp_cond}
            """
            try:
                result = run_query(soql)
                cnt = result['records'][0]['cnt']
                counts[segment_name] = cnt
            except Exception as e:
                print(f'  エラー({segment_name}): {e}')
                counts[segment_name] = 0

    # サマリー表示
    print(f'''
┌──────┬────────────┬────────────┬────────────┬────────────┬────────────┐
│優先度│ 小規模     │ 中規模     │ 大規模     │ 規模不明   │ 合計       │
│      │ (~50人)    │ (51-200人) │ (200人+)   │            │            │
├──────┼────────────┼────────────┼────────────┼────────────┼────────────┤''')

    for priority in ['S', 'A', 'B', 'C']:
        small = counts.get(f'{priority}_小規模_50人以下', 0)
        medium = counts.get(f'{priority}_中規模_51_200人', 0)
        large = counts.get(f'{priority}_大規模_200人超', 0)
        unknown = counts.get(f'{priority}_規模不明', 0)
        total = small + medium + large + unknown
        print(f'│  {priority}   │ {small:>9,}件│ {medium:>9,}件│ {large:>9,}件│ {unknown:>9,}件│ {total:>9,}件│')

    # 合計行
    total_small = sum(counts.get(f'{p}_小規模_50人以下', 0) for p in ['S','A','B','C'])
    total_medium = sum(counts.get(f'{p}_中規模_51_200人', 0) for p in ['S','A','B','C'])
    total_large = sum(counts.get(f'{p}_大規模_200人超', 0) for p in ['S','A','B','C'])
    total_unknown = sum(counts.get(f'{p}_規模不明', 0) for p in ['S','A','B','C'])
    grand_total = total_small + total_medium + total_large + total_unknown

    print(f'''├──────┼────────────┼────────────┼────────────┼────────────┼────────────┤
│ 合計 │ {total_small:>9,}件│ {total_medium:>9,}件│ {total_large:>9,}件│ {total_unknown:>9,}件│ {grand_total:>9,}件│
└──────┴────────────┴────────────┴────────────┴────────────┴────────────┘
''')

    # レポート作成
    print('='*80)
    print('【レポート作成】')
    print('='*80)

    # 共通の除外フィルタ
    exclude_filters = [
        {"column": "Account.Status__c", "operator": "notContain", "value": "商談中"},
        {"column": "Account.Status__c", "operator": "notContain", "value": "プロジェクト進行中"},
        {"column": "Account.Status__c", "operator": "notContain", "value": "深耕対象"},
        {"column": "Account.Status__c", "operator": "notContain", "value": "過去客"},
        {"column": "Account.RelatedAccountFlg__c", "operator": "notEqual", "value": "グループ案件進行中"},
        {"column": "Account.RelatedAccountFlg__c", "operator": "notEqual", "value": "グループ過去案件実績あり"},
        {"column": "Account.ApproachNG__c", "operator": "equals", "value": "0"},
        {"column": "Account.CallNotApplicable__c", "operator": "equals", "value": "0"},
        {"column": "PHONE1", "operator": "notEqual", "value": ""}
    ]

    # 優先度別フィルタ
    priority_filters = {
        'S': [
            {"column": "Account.LegalPersonality__c", "operator": "equals", "value": "株式会社"},
            {"column": "Account.ServiceType__c", "operator": "contains", "value": "訪問"}
        ],
        'A': [
            {"column": "Account.LegalPersonality__c", "operator": "equals", "value": "株式会社"},
            {"column": "Account.ServiceType__c", "operator": "notContain", "value": "訪問"}
        ],
        'B': [
            {"column": "Account.LegalPersonality__c", "operator": "notEqual", "value": "株式会社"}
            # 訪問OR通所はフィルターロジックで対応必要
        ],
        'C': [
            {"column": "Account.LegalPersonality__c", "operator": "notEqual", "value": "株式会社"},
            {"column": "Account.ServiceType__c", "operator": "notContain", "value": "訪問"},
            {"column": "Account.ServiceType__c", "operator": "notContain", "value": "通所"}
        ]
    }

    # 従業員規模別フィルタ（EMPLOYEES が正しいカラム名）
    employee_filters = {
        '小規模_50人以下': [
            {"column": "EMPLOYEES", "operator": "lessOrEqual", "value": "50"}
        ],
        '中規模_51_200人': [
            {"column": "EMPLOYEES", "operator": "greaterThan", "value": "50"},
            {"column": "EMPLOYEES", "operator": "lessOrEqual", "value": "200"}
        ],
        '大規模_200人超': [
            {"column": "EMPLOYEES", "operator": "greaterThan", "value": "200"}
        ]
        # 規模不明はフィルタなし（EMPLOYEES = null はフィルタで表現困難）
    }

    # 表示列
    detail_columns = [
        "ACCOUNT.NAME",
        "Account.LegalPersonality__c",
        "Account.ServiceType__c",
        "EMPLOYEES",
        "Account.Prefectures__c",
        "PHONE1",
        "USERS.NAME",
        "Account.Status__c"
    ]

    created_reports = {}

    # 主要セグメントのレポートを作成（中規模を優先）
    # 作成順: S中規模 → A中規模 → S小規模 → A小規模 → S大規模 → A大規模 → B/C系

    segments_to_create = [
        # 株式会社 × 中規模（最優先）
        ('S', '中規模_51_200人', '株式会社×訪問系×中規模'),
        ('A', '中規模_51_200人', '株式会社×その他×中規模'),
        # 株式会社 × 小規模
        ('S', '小規模_50人以下', '株式会社×訪問系×小規模'),
        ('A', '小規模_50人以下', '株式会社×その他×小規模'),
        # 株式会社 × 大規模
        ('S', '大規模_200人超', '株式会社×訪問系×大規模'),
        ('A', '大規模_200人超', '株式会社×その他×大規模'),
        # その他法人 × 中規模
        ('B', '中規模_51_200人', 'その他法人×訪問通所×中規模'),
        ('C', '中規模_51_200人', 'その他法人×入所等×中規模'),
        # その他法人 × 小規模
        ('B', '小規模_50人以下', 'その他法人×訪問通所×小規模'),
        ('C', '小規模_50人以下', 'その他法人×入所等×小規模'),
        # その他法人 × 大規模
        ('B', '大規模_200人超', 'その他法人×訪問通所×大規模'),
        ('C', '大規模_200人超', 'その他法人×入所等×大規模'),
    ]

    for priority, emp_size, label in segments_to_create:
        segment_key = f'{priority}_{emp_size}'
        cnt = counts.get(segment_key, 0)

        print(f'\n■ {priority}_{emp_size} ({cnt:,}件) レポート作成中...')

        # フィルタを結合
        all_filters = priority_filters[priority] + employee_filters.get(emp_size, []) + exclude_filters

        report_metadata = {
            "reportMetadata": {
                "name": f"アタックリスト_{priority}_{label}",
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
                created_reports[segment_key] = {
                    'id': report_id,
                    'name': f'{priority}_{label}',
                    'count': cnt
                }
            else:
                print(f'  作成失敗: {response.status_code}')
                print(f'  {response.text[:300]}')
        except Exception as e:
            print(f'  エラー: {e}')

    # 結果サマリー
    print('\n' + '='*80)
    print('【作成結果サマリー】')
    print('='*80)

    instance_url = service.instance_url

    print('\n■ 株式会社系（優先度S/A）')
    for key in ['S_中規模_51_200人', 'A_中規模_51_200人', 'S_小規模_50人以下', 'A_小規模_50人以下', 'S_大規模_200人超', 'A_大規模_200人超']:
        if key in created_reports:
            r = created_reports[key]
            print(f"  {r['name']}: {r['count']:,}件")
            print(f"    {instance_url}/lightning/r/Report/{r['id']}/view")

    print('\n■ その他法人系（優先度B/C）')
    for key in ['B_中規模_51_200人', 'C_中規模_51_200人', 'B_小規模_50人以下', 'C_小規模_50人以下', 'B_大規模_200人超', 'C_大規模_200人超']:
        if key in created_reports:
            r = created_reports[key]
            print(f"  {r['name']}: {r['count']:,}件")
            print(f"    {instance_url}/lightning/r/Report/{r['id']}/view")

    print('\n' + '='*80)
    print('【注意】')
    print('='*80)
    print('''
・「規模不明」セグメント（157,098件）はレポート未作成
  → 必要であれば従業員数フィルタなしの優先度別レポートを使用

・優先度Bは「訪問 OR 通所」の条件をレポートビルダーで手動調整が必要
''')

    return created_reports


if __name__ == "__main__":
    created_reports = main()
