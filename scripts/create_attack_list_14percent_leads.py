# -*- coding: utf-8 -*-
"""
アタックリスト: 14%戦略リードレポート作成（株式・社福中心）
- A-Tier: 株式会社 × 代表者名あり × 通所除外
- A-Tier: 社福 × 代表者名あり × 51-100人 × 通所除外
- B-Tier: 有限会社 × 代表者名あり × 通所除外（補完用）
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
    print('14%戦略アタックリスト: リードレポート作成（株式・社福中心）')
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

    # 九州エリアの条件
    kyushu_prefectures = ['福岡県', '佐賀県', '長崎県', '熊本県', '大分県', '宮崎県', '鹿児島県']
    kyushu_condition = "Prefecture__c IN ('" + "','".join(kyushu_prefectures) + "')"

    # 通所系除外条件（無効化 - 全サービス含める）
    exclude_service = ""

    # 架電可能条件
    callable_condition = """
        AND IsConverted = false
        AND Phone != null
        AND (Status != '不通' AND Status != 'アプローチ不可' AND Status != '不在')
    """

    # ========================================
    # 1. A-Tier: 株式会社 × 代表者名あり
    # ========================================
    print('\n' + '='*60)
    print('【A-Tier】株式会社 × 代表者名あり（全サービス）')
    print('='*60)

    soql_kabu = f"""
        SELECT COUNT(Id) cnt FROM Lead
        WHERE LegalPersonality__c = '株式会社'
        AND PresidentName__c != null
        AND {kyushu_condition}
        {exclude_service}
        {callable_condition}
    """
    result_kabu = run_query(soql_kabu)
    cnt_kabu = result_kabu['records'][0]['cnt']
    print(f'  件数: {cnt_kabu:,}件')
    print(f'  期待成約率: 17.6%')

    # ========================================
    # 2. A-Tier: 社福 × 代表者名あり（従業員数制限なし）
    # ========================================
    print('\n' + '='*60)
    print('【A-Tier】社福 × 代表者名あり（全サービス・従業員数制限なし）')
    print('='*60)

    soql_shafu = f"""
        SELECT COUNT(Id) cnt FROM Lead
        WHERE LegalPersonality__c = '社会福祉法人'
        AND PresidentName__c != null
        AND {kyushu_condition}
        {exclude_service}
        {callable_condition}
    """
    result_shafu = run_query(soql_shafu)
    cnt_shafu = result_shafu['records'][0]['cnt']
    print(f'  件数: {cnt_shafu:,}件')
    print(f'  期待成約率: 7.6-15.1%（従業員数による）')

    # ========================================
    # 3. B-Tier: 有限会社 × 代表者名あり（補完用）
    # ========================================
    print('\n' + '='*60)
    print('【B-Tier】有限会社 × 代表者名あり（全サービス・補完用）')
    print('='*60)

    soql_yugen = f"""
        SELECT COUNT(Id) cnt FROM Lead
        WHERE LegalPersonality__c = '有限会社'
        AND PresidentName__c != null
        AND {kyushu_condition}
        {exclude_service}
        {callable_condition}
    """
    result_yugen = run_query(soql_yugen)
    cnt_yugen = result_yugen['records'][0]['cnt']
    print(f'  件数: {cnt_yugen:,}件')
    print(f'  期待成約率: 14.9%')

    # ========================================
    # 合計
    # ========================================
    total = cnt_kabu + cnt_shafu + cnt_yugen
    print('\n' + '='*60)
    print(f'【合計】{total:,}件')
    print('='*60)

    # ========================================
    # レポート作成
    # ========================================
    print('\n■ Salesforceレポート作成中...')

    # Lead用レポート表示列（標準フィールドは大文字、カスタムはLead.Field__c形式）
    detail_columns = [
        "COMPANY",
        "Lead.LegalPersonality__c",
        "Lead.PresidentName__c",
        "Lead.ServiceType1__c",
        "EMPLOYEES",
        "Lead.Prefecture__c",
        "PHONE",
        "MOBILE_PHONE",
        "OWNER",
        "STATUS"
    ]

    reports_to_create = [
        {
            "name": "14%戦略_A-Tier_株式会社×代表者名あり_全サービス",
            "filters": [
                {"column": "Lead.LegalPersonality__c", "operator": "equals", "value": "株式会社"},
                {"column": "Lead.PresidentName__c", "operator": "notEqual", "value": ""},
                {"column": "Lead.Prefecture__c", "operator": "equals", "value": ",".join(kyushu_prefectures)},
                {"column": "PHONE", "operator": "notEqual", "value": ""},
                {"column": "CONVERTED", "operator": "equals", "value": "0"}
            ],
            "expected_rate": "17.6%"
        },
        {
            "name": "14%戦略_A-Tier_社福×代表者名あり_全サービス",
            "filters": [
                {"column": "Lead.LegalPersonality__c", "operator": "equals", "value": "社会福祉法人"},
                {"column": "Lead.PresidentName__c", "operator": "notEqual", "value": ""},
                {"column": "Lead.Prefecture__c", "operator": "equals", "value": ",".join(kyushu_prefectures)},
                {"column": "PHONE", "operator": "notEqual", "value": ""},
                {"column": "CONVERTED", "operator": "equals", "value": "0"}
            ],
            "expected_rate": "7.6-15.1%"
        },
        {
            "name": "14%戦略_B-Tier_有限会社×代表者名あり_全サービス",
            "filters": [
                {"column": "Lead.LegalPersonality__c", "operator": "equals", "value": "有限会社"},
                {"column": "Lead.PresidentName__c", "operator": "notEqual", "value": ""},
                {"column": "Lead.Prefecture__c", "operator": "equals", "value": ",".join(kyushu_prefectures)},
                {"column": "PHONE", "operator": "notEqual", "value": ""},
                {"column": "CONVERTED", "operator": "equals", "value": "0"}
            ],
            "expected_rate": "14.9%"
        }
    ]

    created_reports = []

    for report_config in reports_to_create:
        report_metadata = {
            "reportMetadata": {
                "name": report_config["name"],
                "reportFormat": "TABULAR",
                "reportType": {"type": "LeadList"},
                "detailColumns": detail_columns,
                "reportFilters": report_config["filters"]
            }
        }

        url = f"{service.instance_url}/services/data/{service.api_version}/analytics/reports"

        try:
            response = requests.post(url, headers=headers, json=report_metadata)

            if response.status_code in [200, 201]:
                result = response.json()
                report_id = result.get('reportMetadata', {}).get('id')
                report_url = f"{service.instance_url}/lightning/r/Report/{report_id}/view"
                print(f'\n  ✅ {report_config["name"]}')
                print(f'     期待成約率: {report_config["expected_rate"]}')
                print(f'     URL: {report_url}')
                created_reports.append({
                    "name": report_config["name"],
                    "id": report_id,
                    "url": report_url,
                    "expected_rate": report_config["expected_rate"]
                })
            else:
                print(f'\n  ❌ {report_config["name"]} - 作成失敗')
                print(f'     Status: {response.status_code}')
                print(f'     Error: {response.text[:300]}')

        except Exception as e:
            print(f'\n  ❌ {report_config["name"]} - エラー: {e}')

    # ========================================
    # サマリー
    # ========================================
    print('\n' + '='*80)
    print('■ 作成完了サマリー')
    print('='*80)
    for r in created_reports:
        print(f'  - {r["name"]} ({r["expected_rate"]})')
        print(f'    {r["url"]}')

    return created_reports


if __name__ == "__main__":
    reports = main()
