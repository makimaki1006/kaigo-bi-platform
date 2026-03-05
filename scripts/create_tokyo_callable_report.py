# -*- coding: utf-8 -*-
"""
東京住所 × 架電可能リスト レポート作成
Account と Lead 両方を対象

条件:
- 東京に住所がある（Prefectures__c/Prefecture__c = '東京都'）
- 未成約・未商談（Status__c に「商談中」「プロジェクト進行中」「深耕対象」「過去客」を含まない）
- 架電可能（ApproachNG__c = false, CallNotApplicable__c = false, Phone != null）
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
    print('東京住所 × 架電可能リスト レポート作成')
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

    # =====================================================
    # 架電可能条件（共通）
    # =====================================================
    # 1. Status__c に「商談中」「プロジェクト進行中」「深耕対象」「過去客」を含まない
    # 2. RelatedAccountFlg__c が「グループ案件進行中」「グループ過去案件実績あり」でない
    # 3. ApproachNG__c = false（アプローチ禁止でない）
    # 4. CallNotApplicable__c = false（架電対象外でない）
    # 5. Phone != null（電話番号あり）

    # =====================================================
    # Account: 東京 × 架電可能 件数確認
    # =====================================================
    account_query = """
        SELECT COUNT(Id) cnt
        FROM Account
        WHERE Prefectures__c = '東京都'
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

    print('\n■ Account: 東京 × 架電可能 件数確認')
    try:
        result = run_query(account_query)
        account_count = result['records'][0]['cnt'] if result['records'] else 0
        print(f'  東京Account（架電可能）: {account_count:,}件')
    except Exception as e:
        print(f'  エラー: {e}')
        account_count = 0

    # =====================================================
    # Lead: 東京 × 架電可能 件数確認
    # =====================================================
    lead_query = """
        SELECT COUNT(Id) cnt
        FROM Lead
        WHERE IsConverted = false
          AND Prefecture__c = '東京都'
          AND Phone != null
    """

    print('\n■ Lead: 東京 × 架電可能 件数確認')
    try:
        result = run_query(lead_query)
        lead_count = result['records'][0]['cnt'] if result['records'] else 0
        print(f'  東京Lead（架電可能）: {lead_count:,}件')
    except Exception as e:
        print(f'  エラー: {e}')
        lead_count = 0

    print(f'\n  合計: {account_count + lead_count:,}件')

    # =====================================================
    # レポート作成: Account
    # =====================================================
    print('\n' + '='*80)
    print('【レポート作成】')
    print('='*80)

    created_reports = {}

    # Account レポート
    print('\n■ Account レポート作成中...')

    account_filters = [
        # 東京都
        {"column": "Account.Prefectures__c", "operator": "equals", "value": "東京都"},
        # ステータス除外
        {"column": "Account.Status__c", "operator": "notContain", "value": "商談中"},
        {"column": "Account.Status__c", "operator": "notContain", "value": "プロジェクト進行中"},
        {"column": "Account.Status__c", "operator": "notContain", "value": "深耕対象"},
        {"column": "Account.Status__c", "operator": "notContain", "value": "過去客"},
        # 関連アカウントフラグ除外
        {"column": "Account.RelatedAccountFlg__c", "operator": "notEqual", "value": "グループ案件進行中"},
        {"column": "Account.RelatedAccountFlg__c", "operator": "notEqual", "value": "グループ過去案件実績あり"},
        # アプローチ禁止除外
        {"column": "Account.ApproachNG__c", "operator": "equals", "value": "0"},
        # 架電対象外除外
        {"column": "Account.CallNotApplicable__c", "operator": "equals", "value": "0"},
        # 電話番号あり
        {"column": "PHONE1", "operator": "notEqual", "value": ""}
    ]

    account_columns = [
        "ACCOUNT.NAME",
        "Account.LegalPersonality__c",
        "Account.ServiceType__c",
        "Account.Prefectures__c",
        "PHONE1",
        "USERS.NAME",
        "Account.Status__c"
    ]

    account_report_metadata = {
        "reportMetadata": {
            "name": "東京_架電可能_Account_未商談",
            "reportFormat": "TABULAR",
            "reportType": {"type": "AccountList"},
            "detailColumns": account_columns,
            "reportFilters": account_filters
        }
    }

    try:
        url = f"{service.instance_url}/services/data/{service.api_version}/analytics/reports"
        response = requests.post(url, headers=headers, json=account_report_metadata)

        if response.status_code in [200, 201]:
            result = response.json()
            report_id = result.get('reportMetadata', {}).get('id')
            print(f'  作成成功: {report_id}')
            created_reports['Account'] = report_id
        else:
            print(f'  作成失敗: {response.status_code}')
            print(f'  {response.text[:500]}')
    except Exception as e:
        print(f'  エラー: {e}')

    # =====================================================
    # レポート作成: Lead
    # =====================================================
    print('\n■ Lead レポート作成中...')

    lead_filters = [
        # 東京都
        {"column": "Lead.Prefecture__c", "operator": "equals", "value": "東京都"},
        # 未コンバート
        {"column": "CONVERTED", "operator": "equals", "value": "0"},
        # 電話番号あり
        {"column": "PHONE", "operator": "notEqual", "value": ""}
    ]

    lead_columns = [
        "COMPANY",
        "Lead.Prefecture__c",
        "PHONE",
        "STATUS",
        "OWNER"
    ]

    lead_report_metadata = {
        "reportMetadata": {
            "name": "東京_架電可能_Lead_未商談",
            "reportFormat": "TABULAR",
            "reportType": {"type": "LeadList"},
            "detailColumns": lead_columns,
            "reportFilters": lead_filters
        }
    }

    try:
        url = f"{service.instance_url}/services/data/{service.api_version}/analytics/reports"
        response = requests.post(url, headers=headers, json=lead_report_metadata)

        if response.status_code in [200, 201]:
            result = response.json()
            report_id = result.get('reportMetadata', {}).get('id')
            print(f'  作成成功: {report_id}')
            created_reports['Lead'] = report_id
        else:
            print(f'  作成失敗: {response.status_code}')
            print(f'  {response.text[:500]}')
    except Exception as e:
        print(f'  エラー: {e}')

    # =====================================================
    # 結果サマリー
    # =====================================================
    print('\n' + '='*80)
    print('【作成結果サマリー】')
    print('='*80)

    instance_url = service.instance_url.replace('https://', '').replace('.my.salesforce.com', '')
    base_url = f"https://{instance_url}.my.salesforce.com"

    print(f'\n対象条件:')
    print(f'  - 住所: 東京都')
    print(f'  - ステータス: 商談中/プロジェクト進行中/深耕対象/過去客 を除外')
    print(f'  - 架電可能: アプローチ禁止=false, 架電対象外=false, 電話番号あり')

    print(f'\n件数:')
    print(f'  Account: {account_count:,}件')
    print(f'  Lead: {lead_count:,}件')
    print(f'  合計: {account_count + lead_count:,}件')

    print(f'\nレポートURL:')
    for obj_type, report_id in created_reports.items():
        print(f'\n{obj_type}:')
        print(f'  ID: {report_id}')
        print(f'  URL: {base_url}/lightning/r/Report/{report_id}/view')

    return created_reports


if __name__ == "__main__":
    created_reports = main()
