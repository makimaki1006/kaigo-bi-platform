# -*- coding: utf-8 -*-
"""
アタックリスト: 株式会社統合レポート（リードベース）改善版
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
    print('アタックリスト: 株式会社統合（S+A）リードレポート作成')
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

    # Lead件数確認（全体）
    soql = """
        SELECT COUNT(Id) cnt FROM Lead
        WHERE LegalPersonality__c = '株式会社'
        AND IsConverted = false
        AND Phone != null
    """
    result = run_query(soql)
    total = result['records'][0]['cnt']
    print(f'\n■ 株式会社リード（架電可能）件数（全体）: {total:,}件')

    # 件数確認（人口フィルタ適用: 5-100万人）
    soql_pop = """
        SELECT COUNT(Id) cnt FROM Lead
        WHERE LegalPersonality__c = '株式会社'
        AND Population__c >= 50000
        AND Population__c <= 1000000
        AND IsConverted = false
        AND Phone != null
    """
    result_pop = run_query(soql_pop)
    total_pop = result_pop['records'][0]['cnt']
    print(f'■ 人口フィルタ適用（5-100万人）: {total_pop:,}件')
    print(f'  → 全体の{total_pop/total*100:.1f}%に絞込（成約率19.1-25.7%ゾーン）')

    # Lead用レポート表示列（正しい列名）
    detail_columns = [
        "COMPANY",
        "Lead.LegalPersonality__c",
        "Lead.ServiceType1__c",
        "EMPLOYEES",
        "Lead.Prefecture__c",
        "Lead.Population__c",
        "Lead.PopulationDensity__c",
        "PHONE",
        "OWNER",
        "STATUS"
    ]

    # フィルタ
    report_filters = [
        {"column": "Lead.LegalPersonality__c", "operator": "equals", "value": "株式会社"},
        {"column": "Lead.Population__c", "operator": "greaterOrEqual", "value": "50000"},
        {"column": "Lead.Population__c", "operator": "lessOrEqual", "value": "1000000"},
        {"column": "PHONE", "operator": "notEqual", "value": ""},
        {"column": "CONVERTED", "operator": "equals", "value": "0"}
    ]

    report_metadata = {
        "reportMetadata": {
            "name": "アタックリスト_株式会社_リード_人口5-100万",
            "reportFormat": "TABULAR",
            "reportType": {"type": "LeadList"},
            "detailColumns": detail_columns,
            "reportFilters": report_filters
        }
    }

    print('\n■ リードレポート作成中...')
    print(f'  表示列: 会社名, 法人格, サービス形態, 従業員数, 都道府県, 人口, 人口密度, 電話, 担当者, ステータス')

    url = f"{service.instance_url}/services/data/{service.api_version}/analytics/reports"

    try:
        response = requests.post(url, headers=headers, json=report_metadata)

        if response.status_code in [200, 201]:
            result = response.json()
            report_id = result.get('reportMetadata', {}).get('id')
            print(f'  作成成功: {report_id}')
            print(f'  URL: {service.instance_url}/lightning/r/Report/{report_id}/view')
            return report_id
        else:
            print(f'  作成失敗: {response.status_code}')
            print(f'  {response.text[:500]}')

    except Exception as e:
        print(f'  エラー: {e}')

    return None


if __name__ == "__main__":
    report_id = main()
