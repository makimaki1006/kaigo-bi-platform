# -*- coding: utf-8 -*-
"""
アタックリスト: 株式会社統合レポート（S+A統合）改善版
人口フィルタ（5-100万人）適用 → 株式会社の成約率19.1-25.7%ゾーンに絞込
従業員数フィルタは不要（株式会社は14.9-20.3%でフラット）
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
    print('アタックリスト: 株式会社統合（S+A）レポート作成')
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

    # 架電可能条件
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

    # 件数確認（全体）
    soql = f"""
        SELECT COUNT(Id) cnt FROM Account
        WHERE LegalPersonality__c = '株式会社'
        {callable_condition}
    """
    result = run_query(soql)
    total = result['records'][0]['cnt']
    print(f'\n■ 株式会社（S+A統合）架電可能件数（全体）: {total:,}件')

    # 件数確認（人口フィルタ適用: 5-100万人）
    soql_pop = f"""
        SELECT COUNT(Id) cnt FROM Account
        WHERE LegalPersonality__c = '株式会社'
        AND Population__c >= 50000
        AND Population__c <= 1000000
        {callable_condition}
    """
    result_pop = run_query(soql_pop)
    total_pop = result_pop['records'][0]['cnt']
    print(f'■ 人口フィルタ適用（5-100万人）: {total_pop:,}件')
    print(f'  → 全体の{total_pop/total*100:.1f}%に絞込（成約率19.1-25.7%ゾーン）')

    # 表示列（従業員数・人口・人口密度・都道府県を追加）
    detail_columns = [
        "ACCOUNT.NAME",
        "Account.LegalPersonality__c",
        "Account.ServiceType__c",
        "EMPLOYEES",
        "Account.Prefectures__c",
        "Account.Population__c",
        "Account.PopulationDensity__c",
        "PHONE1",
        "USERS.NAME",
        "Account.Status__c"
    ]

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

    # 株式会社フィルタ（S+A統合: サービス形態の制限なし）
    segment_filters = [
        {"column": "Account.LegalPersonality__c", "operator": "equals", "value": "株式会社"}
    ]

    # 人口フィルタ（5-100万人 = 株式会社の成約率19.1-25.7%ゾーン）
    population_filters = [
        {"column": "Account.Population__c", "operator": "greaterOrEqual", "value": "50000"},
        {"column": "Account.Population__c", "operator": "lessOrEqual", "value": "1000000"}
    ]

    all_filters = segment_filters + population_filters + exclude_filters

    report_metadata = {
        "reportMetadata": {
            "name": "アタックリスト_株式会社_統合_人口5-100万",
            "reportFormat": "TABULAR",
            "reportType": {"type": "AccountList"},
            "detailColumns": detail_columns,
            "reportFilters": all_filters
        }
    }

    print('\n■ レポート作成中...')
    print(f'  表示列: 会社名, 法人格, サービス形態, 従業員数, 都道府県, 人口, 人口密度, 電話, 担当者, ステータス')

    try:
        url = f"{service.instance_url}/services/data/{service.api_version}/analytics/reports"
        response = requests.post(url, headers=headers, json=report_metadata)

        if response.status_code in [200, 201]:
            result = response.json()
            report_id = result.get('reportMetadata', {}).get('id')
            print(f'  作成成功: {report_id}')
            print(f'  URL: {service.instance_url}/lightning/r/Report/{report_id}/view')
            return report_id
        else:
            print(f'  作成失敗: {response.status_code}')
            error_text = response.text[:500]
            print(f'  {error_text}')

            # 列名エラーの場合、人口・人口密度の列名を調整して再試行
            if '列' in error_text and '無効' in error_text:
                print('\n■ 列名調整して再試行...')
                # Account.Population__c → カスタムフィールド名で再試行
                detail_columns_retry = [
                    "ACCOUNT.NAME",
                    "Account.LegalPersonality__c",
                    "Account.ServiceType__c",
                    "EMPLOYEES",
                    "Account.Prefectures__c",
                    "Account.Population__c",
                    "Account.PopulationDensity__c",
                    "PHONE1",
                    "USERS.NAME",
                    "Account.Status__c"
                ]
                report_metadata["reportMetadata"]["detailColumns"] = detail_columns_retry
                response2 = requests.post(url, headers=headers, json=report_metadata)
                if response2.status_code in [200, 201]:
                    result2 = response2.json()
                    report_id2 = result2.get('reportMetadata', {}).get('id')
                    print(f'  再試行成功: {report_id2}')
                    print(f'  URL: {service.instance_url}/lightning/r/Report/{report_id2}/view')
                    return report_id2
                else:
                    print(f'  再試行も失敗: {response2.status_code}')
                    print(f'  {response2.text[:500]}')
    except Exception as e:
        print(f'  エラー: {e}')

    return None


if __name__ == "__main__":
    report_id = main()
