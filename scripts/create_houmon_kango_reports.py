# -*- coding: utf-8 -*-
"""
訪問看護セグメントの商談レポート作成
介護法人の訪問看護（新規） vs 医療法人の訪問看護（新規）
"""
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / 'src'))

import os
os.environ['PYTHONIOENCODING'] = 'utf-8'

import pandas as pd
import numpy as np
import requests
import json
import warnings
warnings.filterwarnings('ignore')
from src.services.opportunity_service import OpportunityService
from src.api.salesforce_client import SalesforceClient

INDUSTRY_MAP = {'介護': '介護（高齢者）', '医療': '医療', '障害福祉': '障がい福祉', '保育': '保育', 'その他': 'その他'}
SERVICE_TYPE_MAP = {
    '訪問介護': '介護（高齢者）', '通所介護': '介護（高齢者）', '短期入所生活介護': '介護（高齢者）',
    '認知症対応型共同生活介護': '介護（高齢者）', '居宅介護支援': '介護（高齢者）',
    '地域密着型通所介護': '介護（高齢者）', '介護老人福祉施設': '介護（高齢者）',
    '介護老人保健施設': '介護（高齢者）', '訪問入浴介護': '介護（高齢者）',
    '有料老人ホーム': '介護（高齢者）', '訪問看護': '医療', '訪問リハビリテーション': '医療',
    '通所リハビリテーション': '医療', '介護医療院': '医療', 'クリニック': '医療',
    '放課後等デイサービス': '障がい福祉', '就労定着支援': '障がい福祉', '生活介護': '障がい福祉',
    '障がい者施設': '障がい福祉', '障害者施設': '障がい福祉', '保育園': '保育',
}


def complement_facility(row):
    if pd.notna(row.get('FacilityType_Large__c')):
        return row['FacilityType_Large__c']
    ic = row.get('Account.IndustryCategory__c')
    if pd.notna(ic):
        fc = str(ic).split(';')[0].strip()
        if fc in INDUSTRY_MAP:
            return INDUSTRY_MAP[fc]
    st = row.get('Account.ServiceType__c')
    if pd.notna(st) and st in SERVICE_TYPE_MAP:
        return SERVICE_TYPE_MAP[st]
    return None


def main():
    print('=' * 80)
    print('訪問看護セグメント レポート作成')
    print('=' * 80)

    service = OpportunityService()
    service.authenticate()

    # 全商談履歴（新規判定用）
    print('\n全商談履歴を取得...')
    soql_all = """SELECT Id, AccountId, CloseDate, IsWon, IsClosed
        FROM Opportunity WHERE IsClosed = true"""
    df_all = service.bulk_query(soql_all, '全商談')
    df_all['is_won'] = df_all['IsWon'].map({'true': 1, 'false': 0, True: 1, False: 0}).astype(int)
    df_all['CloseDate'] = pd.to_datetime(df_all['CloseDate'], errors='coerce')

    # FY2025以前の履歴
    pre_fy = df_all[df_all['CloseDate'] < '2025-04-01']
    pre_accounts = set(pre_fy['AccountId'].unique())
    print(f'  FY2025以前に商談歴あるAccount: {len(pre_accounts):,}')

    # FY2025詳細データ
    print('\nFY2025詳細データを取得...')
    soql_fy = """SELECT Id, AccountId, Name, CloseDate, IsWon, IsClosed, OpportunityCategory__c,
        FacilityType_Large__c, StageName, Amount,
        Account.Name, Account.WonOpportunityies__c, Account.LegalPersonality__c,
        Account.IndustryCategory__c, Account.ServiceType__c,
        Account.Prefectures__c, Account.NumberOfEmployees
        FROM Opportunity WHERE IsClosed = true
        AND CloseDate >= 2025-04-01 AND CloseDate < 2026-02-01"""
    df_fy = service.bulk_query(soql_fy, 'FY2025詳細')
    df_fy['is_won'] = df_fy['IsWon'].map({'true': 1, 'false': 0, True: 1, False: 0}).astype(int)
    df_fy['CloseDate'] = pd.to_datetime(df_fy['CloseDate'], errors='coerce')

    # 新規判定
    df_fy['is_new'] = ~df_fy['AccountId'].isin(pre_accounts)

    # 施設形態補完
    df_fy['facility'] = df_fy.apply(complement_facility, axis=1).fillna('不明')

    # 初回商談のみ
    first = df_fy[df_fy['OpportunityCategory__c'] == '初回商談'].copy()

    # 訪問看護を抽出
    houmon = first[first['Account.ServiceType__c'] == '訪問看護']

    # 2セグメントに分割
    kaigo_houmon = houmon[(houmon['facility'] == '介護（高齢者）') & (houmon['is_new'] == True)]
    iryo_houmon = houmon[(houmon['facility'] == '医療') & (houmon['is_new'] == True)]

    print(f'\n介護法人の訪問看護（新規）: {len(kaigo_houmon)}件 受注{int(kaigo_houmon["is_won"].sum())} 率{kaigo_houmon["is_won"].mean()*100:.1f}%')
    print(f'医療法人の訪問看護（新規）: {len(iryo_houmon)}件 受注{int(iryo_houmon["is_won"].sum())} 率{iryo_houmon["is_won"].mean()*100:.1f}%')

    # CSV出力
    output_dir = project_root / 'data' / 'output' / 'analysis'
    output_dir.mkdir(parents=True, exist_ok=True)

    for label, data, fname in [
        ('介護法人の訪問看護（新規）', kaigo_houmon, 'houmon_kango_kaigo_new.csv'),
        ('医療法人の訪問看護（新規）', iryo_houmon, 'houmon_kango_iryo_new.csv')
    ]:
        cols = ['Id', 'Account.Name', 'Name', 'CloseDate', 'IsWon', 'StageName',
                'FacilityType_Large__c', 'Account.ServiceType__c',
                'Account.IndustryCategory__c', 'Account.LegalPersonality__c',
                'Account.Prefectures__c', 'Account.NumberOfEmployees']
        out = data[cols].copy()
        out.to_csv(output_dir / fname, index=False, encoding='utf-8-sig')
        print(f'\n{label} → {fname}')
        print(f'  ID一覧: {", ".join(data["Id"].tolist()[:5])}...')

    # ============================================================
    # Salesforceレポート作成
    # ============================================================
    print(f'\n{"=" * 80}')
    print('Salesforceレポート作成')
    print(f'{"=" * 80}')

    client = SalesforceClient()
    client.authenticate()
    headers = client._get_headers()
    instance_url = client.instance_url
    api_version = 'v59.0'

    kaigo_ids = kaigo_houmon['Id'].tolist()
    iryo_ids = iryo_houmon['Id'].tolist()

    reports_created = []

    # 条件ベースフィルタ（介護/医療をIndustryCategory__cで区別）
    base_filters = [
        {'column': 'CLOSE_DATE', 'operator': 'greaterOrEqual', 'value': '2025-04-01'},
        {'column': 'CLOSE_DATE', 'operator': 'lessThan', 'value': '2026-02-01'},
        {'column': 'CLOSED', 'operator': 'equals', 'value': 'true'},
        {'column': 'Account.ServiceType__c', 'operator': 'equals', 'value': '訪問看護'},
        {'column': 'Opportunity.OpportunityCategory__c', 'operator': 'equals', 'value': '初回商談'},
    ]

    for label, opp_ids, report_name, industry_filter in [
        ('介護法人×訪問看護（新規）', kaigo_ids,
         '【分析】介護法人×訪問看護 新規商談 FY2025',
         {'column': 'Account.IndustryCategory__c', 'operator': 'includes', 'value': '介護'}),
        ('医療法人×訪問看護（新規）', iryo_ids,
         '【分析】医療法人×訪問看護 新規商談 FY2025',
         {'column': 'Account.IndustryCategory__c', 'operator': 'includes', 'value': '医療'}),
    ]:
        print(f'\n  {label} ({len(opp_ids)}件)...')

        report_metadata = {
            'reportMetadata': {
                'name': report_name,
                'reportFormat': 'TABULAR',
                'reportType': {'type': 'Opportunity'},
                'detailColumns': [
                    'OPPORTUNITY_NAME',
                    'ACCOUNT_NAME',
                    'CLOSE_DATE',
                    'STAGE_NAME',
                    'WON',
                    'Opportunity.FacilityType_Large__c',
                    'Account.ServiceType__c',
                    'Account.IndustryCategory__c',
                    'Account.LegalPersonality__c',
                    'Account.Prefectures__c',
                    'EMPLOYEES',
                    'Opportunity.OpportunityCategory__c'
                ],
                'reportFilters': base_filters + [industry_filter]
            }
        }

        url = f'{instance_url}/services/data/{api_version}/analytics/reports'
        try:
            response = requests.post(url, headers=headers, json=report_metadata)
            if response.status_code in (200, 201):
                result = response.json()
                report_id = result.get('reportMetadata', {}).get('id', 'unknown')
                report_url = f'{instance_url}/lightning/r/Report/{report_id}/view'
                print(f'  レポート作成成功: {report_url}')
                print(f'  ※条件ベース: 訪問看護×初回商談×{industry_filter["value"]}（新規/再商談の区別なし）')
                reports_created.append((label, report_url))
            else:
                print(f'  レポート作成失敗: {response.status_code}')
                print(f'  {response.text[:500]}')
        except Exception as e:
            print(f'  エラー: {e}')

    # サマリ
    print(f'\n{"=" * 80}')
    print('作成結果サマリ')
    print(f'{"=" * 80}')
    for label, url in reports_created:
        print(f'  {label}: {url}')

    print(f'\nCSV出力先: {output_dir}')
    print('  - houmon_kango_kaigo_new.csv (介護法人×訪問看護 新規)')
    print('  - houmon_kango_iryo_new.csv (医療法人×訪問看護 新規)')

    print()
    print('=' * 80)
    print('完了')
    print('=' * 80)


if __name__ == '__main__':
    main()
