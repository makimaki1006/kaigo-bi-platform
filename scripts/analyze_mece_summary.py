# -*- coding: utf-8 -*-
"""
MECE網羅的分析サマリー
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
               Account.LegalPersonality__c,
               Account.ServiceType__c,
               Account.WonOpportunityies__c,
               OpportunityType__c,
               Hearing_Authority__c
        FROM Opportunity
        WHERE IsClosed = true AND CreatedDate >= 2025-04-01T00:00:00Z
    """
    df = opp_service.bulk_query(soql, 'MECE網羅分析')

    df['is_won'] = df['IsWon'].map({'true': 1, 'false': 0, True: 1, False: 0})
    df['won_count'] = pd.to_numeric(df.get('Account.WonOpportunityies__c', 0), errors='coerce').fillna(0)
    df['past_won_count'] = df.apply(lambda row: row['won_count'] - 1 if row['is_won'] == 1 else row['won_count'], axis=1)
    df = df[df['past_won_count'] == 0].copy().reset_index(drop=True)

    df['employees'] = pd.to_numeric(df['Account.NumberOfEmployees'], errors='coerce')

    # 商談タイプの分類
    df['is_decision_maker_opp'] = df['OpportunityType__c'].apply(lambda x: 1 if '決裁者' in str(x) else 0)
    df['is_representative'] = df['OpportunityType__c'].apply(lambda x: 1 if '代表者' in str(x) else 0)
    df['has_authority'] = df['Hearing_Authority__c'].apply(lambda x: 1 if str(x) == 'あり' else 0)

    # MECE分類
    # A: 決裁者商談（OpportunityType__cに「決裁者」を含む）
    # B: 非決裁者商談だが決裁権あり（Hearing_Authority__c = あり）
    # C: 非決裁者商談で決裁権なし

    def classify_mece(row):
        if row['is_decision_maker_opp'] == 1:
            return 'A: 決裁者商談'
        elif row['has_authority'] == 1:
            return 'B: 担当者商談（決裁権あり）'
        else:
            return 'C: 担当者商談（決裁権なし）'

    df['mece_category'] = df.apply(classify_mece, axis=1)

    print('='*80)
    print('MECE網羅的分析サマリー')
    print('='*80)

    # 1. 商談タイプ別MECE分類
    print('\n' + '='*80)
    print('1. 商談タイプ MECE分類')
    print('='*80)

    mece_stats = df.groupby('mece_category').agg(
        件数=('is_won', 'count'),
        成約数=('is_won', 'sum'),
        成約率=('is_won', 'mean')
    ).round(4)

    total = len(df)
    mece_stats['構成比'] = (mece_stats['件数'] / total * 100).round(1)
    print(mece_stats.to_string())

    # 2. さらに詳細：代表者 vs 担当者 × 決裁者 vs 非決裁者
    print('\n' + '='*80)
    print('2. OpportunityType__c 詳細分類')
    print('='*80)

    opp_type_stats = df.groupby('OpportunityType__c').agg(
        件数=('is_won', 'count'),
        成約数=('is_won', 'sum'),
        成約率=('is_won', 'mean')
    ).sort_values('件数', ascending=False).round(4)
    opp_type_stats['構成比'] = (opp_type_stats['件数'] / total * 100).round(1)
    print(opp_type_stats.to_string())

    # 3. 決裁権の有無（Hearing_Authority__c）
    print('\n' + '='*80)
    print('3. Hearing_Authority__c（決裁権）分布')
    print('='*80)

    auth_stats = df.groupby('Hearing_Authority__c').agg(
        件数=('is_won', 'count'),
        成約数=('is_won', 'sum'),
        成約率=('is_won', 'mean')
    ).round(4)
    auth_stats['構成比'] = (auth_stats['件数'] / total * 100).round(1)
    print(auth_stats.to_string())

    # 4. MECE分類 × 従業員規模
    print('\n' + '='*80)
    print('4. MECE分類 × 従業員規模')
    print('='*80)

    df['emp_size'] = pd.cut(df['employees'], bins=[0, 30, 100, 500, float('inf')],
                            labels=['小(~30)', '中(31-100)', '大(101-500)', '超大(500+)'])

    for cat in ['A: 決裁者商談', 'B: 担当者商談（決裁権あり）', 'C: 担当者商談（決裁権なし）']:
        df_cat = df[df['mece_category'] == cat]
        print(f'\n【{cat}】 (n={len(df_cat)}, 成約率{df_cat["is_won"].mean():.1%})')

        emp_stats = df_cat.groupby('emp_size', observed=True).agg(
            件数=('is_won', 'count'),
            成約数=('is_won', 'sum'),
            成約率=('is_won', 'mean')
        ).round(4)
        print(emp_stats.to_string())

    # 5. MECE分類 × 法人格
    print('\n' + '='*80)
    print('5. MECE分類 × 法人格（主要5種）')
    print('='*80)

    major_legal = ['株式会社', '医療法人', '社会福祉法人', '有限会社', '合同会社']
    df_legal = df[df['Account.LegalPersonality__c'].isin(major_legal)]

    for cat in ['A: 決裁者商談', 'B: 担当者商談（決裁権あり）', 'C: 担当者商談（決裁権なし）']:
        df_cat = df_legal[df_legal['mece_category'] == cat]
        print(f'\n【{cat}】')

        legal_stats = df_cat.groupby('Account.LegalPersonality__c').agg(
            件数=('is_won', 'count'),
            成約数=('is_won', 'sum'),
            成約率=('is_won', 'mean')
        ).sort_values('成約率', ascending=False).round(4)
        print(legal_stats.to_string())

    # 6. MECE分類 × サービス種別
    print('\n' + '='*80)
    print('6. MECE分類 × サービス種別（主要種別）')
    print('='*80)

    service_counts = df.groupby('Account.ServiceType__c').size()
    major_services = service_counts[service_counts >= 50].index.tolist()
    df_service = df[df['Account.ServiceType__c'].isin(major_services)]

    for cat in ['A: 決裁者商談', 'B: 担当者商談（決裁権あり）', 'C: 担当者商談（決裁権なし）']:
        df_cat = df_service[df_service['mece_category'] == cat]
        print(f'\n【{cat}】')

        service_stats = df_cat.groupby('Account.ServiceType__c').agg(
            件数=('is_won', 'count'),
            成約数=('is_won', 'sum'),
            成約率=('is_won', 'mean')
        ).query('件数 >= 10').sort_values('成約率', ascending=False).round(4)
        print(service_stats.to_string())

    # 7. 決裁者到達予測セグメント（従業員×法人格×サービス）
    print('\n' + '='*80)
    print('7. 決裁者到達しやすいセグメント（決裁者到達率20%以上）')
    print('='*80)

    df['can_reach_decision_maker'] = ((df['is_decision_maker_opp'] == 1) | (df['has_authority'] == 1)).astype(int)

    df_combo = df[df['Account.LegalPersonality__c'].isin(major_legal) &
                  df['Account.ServiceType__c'].isin(major_services)].copy()

    reach_stats = df_combo.groupby(
        ['Account.ServiceType__c', 'emp_size', 'Account.LegalPersonality__c'], observed=True
    ).agg(
        件数=('is_won', 'count'),
        決裁者到達率=('can_reach_decision_maker', 'mean'),
        成約率=('is_won', 'mean')
    ).query('件数 >= 15 and 決裁者到達率 >= 0.20').sort_values('決裁者到達率', ascending=False).round(4)

    print(reach_stats.to_string())

    # 8. 成約率が高いセグメント（MECE分類別）
    print('\n' + '='*80)
    print('8. 成約率TOP セグメント（MECE分類別）')
    print('='*80)

    for cat in ['A: 決裁者商談', 'B: 担当者商談（決裁権あり）', 'C: 担当者商談（決裁権なし）']:
        df_cat = df_combo[df_combo['mece_category'] == cat]
        print(f'\n【{cat}】')

        combo_stats = df_cat.groupby(
            ['Account.ServiceType__c', 'emp_size', 'Account.LegalPersonality__c'], observed=True
        ).agg(
            件数=('is_won', 'count'),
            成約数=('is_won', 'sum'),
            成約率=('is_won', 'mean')
        ).query('件数 >= 5 and 成約率 > 0').sort_values('成約率', ascending=False).head(10).round(4)

        if len(combo_stats) > 0:
            print(combo_stats.to_string())
        else:
            print('  該当なし')

    # 9. 総合優先度マトリクス
    print('\n' + '='*80)
    print('9. 総合営業優先度マトリクス')
    print('='*80)

    # 全セグメントの成約率計算
    all_combo = df_combo.groupby(
        ['Account.ServiceType__c', 'emp_size', 'Account.LegalPersonality__c'], observed=True
    ).agg(
        件数=('is_won', 'count'),
        成約数=('is_won', 'sum'),
        成約率=('is_won', 'mean'),
        決裁者到達率=('can_reach_decision_maker', 'mean')
    ).query('件数 >= 20').round(4)

    # スコア計算（成約率×2 + 決裁者到達率）
    all_combo['スコア'] = (all_combo['成約率'] * 2 + all_combo['決裁者到達率']).round(4)
    all_combo = all_combo.sort_values('スコア', ascending=False)

    print('\n【総合スコア TOP15（成約率×2 + 決裁者到達率）】')
    print(all_combo.head(15).to_string())

    return df, mece_stats


if __name__ == "__main__":
    df, mece_stats = main()
