# -*- coding: utf-8 -*-
"""
サービス種別（事業形態）別 詳細分析
"""

import sys
import io
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

from src.services.opportunity_service import OpportunityService


def main():
    opp_service = OpportunityService()
    opp_service.authenticate()

    soql = """
        SELECT Id, IsWon, CreatedDate,
               Account.NumberOfEmployees,
               Account.Population__c,
               Account.PopulationDensity__c,
               Account.LegalPersonality__c,
               Account.ServiceType__c,
               Account.WonOpportunityies__c,
               OpportunityType__c,
               Hearing_Authority__c
        FROM Opportunity
        WHERE IsClosed = true AND CreatedDate >= 2025-04-01T00:00:00Z
    """
    df = opp_service.bulk_query(soql, 'サービス種別詳細分析')

    df['is_won'] = df['IsWon'].map({'true': 1, 'false': 0, True: 1, False: 0})
    df['won_count'] = pd.to_numeric(df.get('Account.WonOpportunityies__c', 0), errors='coerce').fillna(0)
    df['past_won_count'] = df.apply(lambda row: row['won_count'] - 1 if row['is_won'] == 1 else row['won_count'], axis=1)
    df = df[df['past_won_count'] == 0].copy().reset_index(drop=True)

    df['employees'] = pd.to_numeric(df['Account.NumberOfEmployees'], errors='coerce')
    df['is_decision_maker'] = df['OpportunityType__c'].apply(lambda x: 1 if '決裁者' in str(x) else 0)
    df['has_authority'] = df['Hearing_Authority__c'].apply(lambda x: 1 if str(x) == 'あり' else 0)
    df['can_reach_decision_maker'] = ((df['is_decision_maker'] == 1) | (df['has_authority'] == 1)).astype(int)

    # 従業員規模カテゴリ
    df['emp_size'] = pd.cut(df['employees'], bins=[0, 30, 100, 500, float('inf')],
                            labels=['小(~30)', '中(31-100)', '大(101-500)', '超大(500+)'])

    print('='*80)
    print('サービス種別（事業形態）別 詳細分析')
    print('='*80)

    # 主要サービス種別（30件以上）を特定
    service_counts = df.groupby('Account.ServiceType__c').size().reset_index(name='count')
    major_services = service_counts[service_counts['count'] >= 50]['Account.ServiceType__c'].tolist()

    print(f'\n分析対象サービス種別（50件以上）: {len(major_services)} 種類')

    # 1. 全サービス種別のサマリー
    print('\n' + '='*80)
    print('1. サービス種別サマリー（成約率順）')
    print('='*80)

    service_summary = df.groupby('Account.ServiceType__c').agg(
        件数=('is_won', 'count'),
        成約数=('is_won', 'sum'),
        成約率=('is_won', 'mean'),
        決裁者到達率=('can_reach_decision_maker', 'mean'),
        平均従業員数=('employees', 'mean')
    ).query('件数 >= 50').sort_values('成約率', ascending=False).round(4)

    print(service_summary.to_string())

    # 2. 各サービス種別ごとの従業員規模別分析
    print('\n' + '='*80)
    print('2. サービス種別 × 従業員規模 詳細分析')
    print('='*80)

    for service in major_services[:15]:  # 上位15種別
        df_service = df[df['Account.ServiceType__c'] == service]
        total = len(df_service)
        won = df_service['is_won'].sum()
        win_rate = df_service['is_won'].mean()

        print(f'\n【{service}】 (n={total}, 成約{won}件, 成約率{win_rate:.1%})')
        print('-'*60)

        # 従業員規模別
        emp_stats = df_service.groupby('emp_size', observed=True).agg(
            件数=('is_won', 'count'),
            成約数=('is_won', 'sum'),
            成約率=('is_won', 'mean'),
            決裁者到達率=('can_reach_decision_maker', 'mean')
        ).round(4)

        if len(emp_stats) > 0:
            print('\n  従業員規模別:')
            for idx, row in emp_stats.iterrows():
                win_rate_str = f'{row["成約率"]*100:.1f}%'
                reach_str = f'{row["決裁者到達率"]*100:.1f}%'
                print(f'    {idx}: {int(row["件数"])}件, 成約{int(row["成約数"])}件, '
                      f'成約率{win_rate_str}, 決裁者到達率{reach_str}')

        # 法人格別（上位5）
        legal_stats = df_service.groupby('Account.LegalPersonality__c').agg(
            件数=('is_won', 'count'),
            成約数=('is_won', 'sum'),
            成約率=('is_won', 'mean'),
            決裁者到達率=('can_reach_decision_maker', 'mean')
        ).query('件数 >= 10').sort_values('成約率', ascending=False).head(5).round(4)

        if len(legal_stats) > 0:
            print('\n  法人格別（成約率TOP5）:')
            for legal, row in legal_stats.iterrows():
                win_rate_str = f'{row["成約率"]*100:.1f}%'
                reach_str = f'{row["決裁者到達率"]*100:.1f}%'
                print(f'    {legal}: {int(row["件数"])}件, 成約{int(row["成約数"])}件, '
                      f'成約率{win_rate_str}, 決裁者到達率{reach_str}')

    # 3. 最強セグメント発掘（サービス種別×従業員規模×法人格）
    print('\n' + '='*80)
    print('3. 最強セグメント発掘（成約率8%以上、20件以上）')
    print('='*80)

    # 3次元クロス集計
    df_filtered = df[df['Account.ServiceType__c'].isin(major_services)].copy()
    major_legal = ['株式会社', '医療法人', '社会福祉法人', '有限会社', '合同会社']
    df_filtered = df_filtered[df_filtered['Account.LegalPersonality__c'].isin(major_legal)]

    cross_stats = df_filtered.groupby(
        ['Account.ServiceType__c', 'emp_size', 'Account.LegalPersonality__c'], observed=True
    ).agg(
        件数=('is_won', 'count'),
        成約数=('is_won', 'sum'),
        成約率=('is_won', 'mean'),
        決裁者到達率=('can_reach_decision_maker', 'mean')
    ).query('件数 >= 20 and 成約率 >= 0.08').sort_values('成約率', ascending=False).round(4)

    print('\n【サービス種別×従業員規模×法人格 ベストセグメント】')
    if len(cross_stats) > 0:
        for (service, emp, legal), row in cross_stats.head(20).iterrows():
            print(f'  {service} × {emp} × {legal}:')
            print(f'    件数{int(row["件数"])}, 成約{int(row["成約数"])}件, '
                  f'成約率{row["成約率"]*100:.1f}%, 決裁者到達率{row["決裁者到達率"]*100:.1f}%')
    else:
        print('  該当セグメントなし')

    # 4. 決裁者到達率が高いセグメント
    print('\n' + '='*80)
    print('4. 決裁者到達率が高いセグメント（20%以上）')
    print('='*80)

    high_reach = df_filtered.groupby(
        ['Account.ServiceType__c', 'emp_size', 'Account.LegalPersonality__c'], observed=True
    ).agg(
        件数=('is_won', 'count'),
        成約数=('is_won', 'sum'),
        成約率=('is_won', 'mean'),
        決裁者到達率=('can_reach_decision_maker', 'mean')
    ).query('件数 >= 20 and 決裁者到達率 >= 0.20').sort_values('決裁者到達率', ascending=False).round(4)

    print('\n【決裁者到達率20%以上のセグメント】')
    if len(high_reach) > 0:
        for (service, emp, legal), row in high_reach.head(15).iterrows():
            print(f'  {service} × {emp} × {legal}:')
            print(f'    件数{int(row["件数"])}, 決裁者到達率{row["決裁者到達率"]*100:.1f}%, '
                  f'成約率{row["成約率"]*100:.1f}%')
    else:
        print('  該当セグメントなし')

    # 5. サービス種別ごとの最適従業員規模
    print('\n' + '='*80)
    print('5. サービス種別ごとの最適従業員規模')
    print('='*80)

    for service in major_services[:10]:
        df_service = df[df['Account.ServiceType__c'] == service]

        best_emp = df_service.groupby('emp_size', observed=True).agg(
            件数=('is_won', 'count'),
            成約率=('is_won', 'mean')
        ).query('件数 >= 10')

        if len(best_emp) > 0:
            best = best_emp['成約率'].idxmax()
            best_rate = best_emp.loc[best, '成約率']
            print(f'  {service}: 最適 → {best} (成約率{best_rate*100:.1f}%)')

    # 6. 意外な発見（成約率が高いニッチセグメント）
    print('\n' + '='*80)
    print('6. 意外な発見（件数30-100件で成約率10%以上）')
    print('='*80)

    niche_stats = df_filtered.groupby(
        ['Account.ServiceType__c', 'emp_size', 'Account.LegalPersonality__c'], observed=True
    ).agg(
        件数=('is_won', 'count'),
        成約数=('is_won', 'sum'),
        成約率=('is_won', 'mean'),
        決裁者到達率=('can_reach_decision_maker', 'mean')
    ).query('件数 >= 30 and 件数 <= 100 and 成約率 >= 0.10').sort_values('成約率', ascending=False).round(4)

    if len(niche_stats) > 0:
        print('\n【ニッチ高成約セグメント】')
        for (service, emp, legal), row in niche_stats.iterrows():
            print(f'  {service} × {emp} × {legal}:')
            print(f'    件数{int(row["件数"])}, 成約{int(row["成約数"])}件, '
                  f'成約率{row["成約率"]*100:.1f}%')
    else:
        print('  該当セグメントなし')

    return df, service_summary, cross_stats


if __name__ == "__main__":
    df, service_summary, cross_stats = main()
