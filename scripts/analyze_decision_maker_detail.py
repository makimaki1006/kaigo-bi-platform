# -*- coding: utf-8 -*-
"""
決裁者到達の詳細分析
- 代表者商談（決裁者）
- 担当者商談（決裁者）
- 担当者商談（決裁権あり）
"""

import sys
import io
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import cross_val_score
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
               Account.Prefectures__c,
               Account.IndustryCategory__c,
               Account.WonOpportunityies__c,
               OpportunityType__c,
               Hearing_Authority__c
        FROM Opportunity
        WHERE IsClosed = true AND CreatedDate >= 2025-04-01T00:00:00Z
    """
    df = opp_service.bulk_query(soql, '決裁者到達詳細分析')

    df['is_won'] = df['IsWon'].map({'true': 1, 'false': 0, True: 1, False: 0})
    df['won_count'] = pd.to_numeric(df.get('Account.WonOpportunityies__c', 0), errors='coerce').fillna(0)
    df['past_won_count'] = df.apply(lambda row: row['won_count'] - 1 if row['is_won'] == 1 else row['won_count'], axis=1)
    df = df[df['past_won_count'] == 0].copy().reset_index(drop=True)

    # 基本特徴量
    df['employees'] = pd.to_numeric(df['Account.NumberOfEmployees'], errors='coerce')
    df['population'] = pd.to_numeric(df['Account.Population__c'], errors='coerce')
    df['pop_density'] = pd.to_numeric(df['Account.PopulationDensity__c'], errors='coerce')

    # 決裁者関連の詳細分類
    def classify_decision_type(row):
        opp_type = str(row['OpportunityType__c'])
        authority = str(row['Hearing_Authority__c'])

        if '決裁者' in opp_type and '代表者' in opp_type:
            return 'A: 代表者商談（決裁者）'
        elif '決裁者' in opp_type and '担当者' in opp_type:
            return 'B: 担当者商談（決裁者）'
        elif authority == 'あり':
            return 'C: 担当者商談（決裁権あり）'
        else:
            return 'D: 決裁権なし'

    df['decision_type'] = df.apply(classify_decision_type, axis=1)

    # バイナリ変数
    df['is_rep_decision'] = (df['decision_type'] == 'A: 代表者商談（決裁者）').astype(int)
    df['is_staff_decision'] = (df['decision_type'] == 'B: 担当者商談（決裁者）').astype(int)
    df['is_staff_authority'] = (df['decision_type'] == 'C: 担当者商談（決裁権あり）').astype(int)
    df['can_reach_any'] = (df['decision_type'] != 'D: 決裁権なし').astype(int)

    print('='*80)
    print('決裁者到達の詳細分析')
    print('='*80)
    print(f'\n総件数: {len(df):,}件')

    # 1. 決裁者タイプ別の分布
    print('\n' + '='*80)
    print('1. 決裁者タイプ別 分布と成約率')
    print('='*80)

    type_stats = df.groupby('decision_type').agg(
        件数=('is_won', 'count'),
        成約数=('is_won', 'sum'),
        成約率=('is_won', 'mean')
    ).round(4)
    type_stats['構成比'] = (type_stats['件数'] / len(df) * 100).round(1)
    print(type_stats.to_string())

    # カテゴリ変数の準備
    df['emp_band'] = pd.cut(df['employees'],
                            bins=[0, 10, 30, 50, 100, 200, 500, 1000, float('inf')],
                            labels=['1-10', '11-30', '31-50', '51-100', '101-200', '201-500', '501-1000', '1001+'])
    df['pop_band'] = pd.cut(df['population']/10000,
                            bins=[0, 5, 10, 20, 50, 100, float('inf')],
                            labels=['~5万', '5-10万', '10-20万', '20-50万', '50-100万', '100万+'])
    df['density_band'] = pd.cut(df['pop_density'],
                                bins=[0, 500, 1000, 2000, 5000, float('inf')],
                                labels=['~500', '500-1k', '1k-2k', '2k-5k', '5k+'])

    # 2. 従業員数別の詳細
    print('\n' + '='*80)
    print('2. 従業員数別 × 決裁者タイプ別')
    print('='*80)

    for emp in ['1-10', '11-30', '31-50', '51-100', '101-200', '201-500', '501-1000', '1001+']:
        df_emp = df[df['emp_band'] == emp]
        if len(df_emp) < 30:
            continue

        print(f'\n【{emp}人】 (n={len(df_emp)})')
        emp_type = df_emp.groupby('decision_type').agg(
            件数=('is_won', 'count'),
            成約率=('is_won', 'mean')
        ).round(4)
        emp_type['割合'] = (emp_type['件数'] / len(df_emp) * 100).round(1)

        for idx, row in emp_type.iterrows():
            print(f'  {idx}: {int(row["件数"])}件 ({row["割合"]:.1f}%), 成約率{row["成約率"]*100:.1f}%')

    # 3. 人口帯別の詳細
    print('\n' + '='*80)
    print('3. 人口帯別 × 決裁者タイプ別')
    print('='*80)

    for pop in ['~5万', '5-10万', '10-20万', '20-50万', '50-100万', '100万+']:
        df_pop = df[df['pop_band'] == pop]
        if len(df_pop) < 30:
            continue

        print(f'\n【{pop}】 (n={len(df_pop)})')
        pop_type = df_pop.groupby('decision_type').agg(
            件数=('is_won', 'count'),
            成約率=('is_won', 'mean')
        ).round(4)
        pop_type['割合'] = (pop_type['件数'] / len(df_pop) * 100).round(1)

        for idx, row in pop_type.iterrows():
            print(f'  {idx}: {int(row["件数"])}件 ({row["割合"]:.1f}%), 成約率{row["成約率"]*100:.1f}%')

    # 4. 人口密度帯別の詳細
    print('\n' + '='*80)
    print('4. 人口密度帯別 × 決裁者タイプ別')
    print('='*80)

    for density in ['~500', '500-1k', '1k-2k', '2k-5k', '5k+']:
        df_density = df[df['density_band'] == density]
        if len(df_density) < 30:
            continue

        print(f'\n【{density}人/km²】 (n={len(df_density)})')
        density_type = df_density.groupby('decision_type').agg(
            件数=('is_won', 'count'),
            成約率=('is_won', 'mean')
        ).round(4)
        density_type['割合'] = (density_type['件数'] / len(df_density) * 100).round(1)

        for idx, row in density_type.iterrows():
            print(f'  {idx}: {int(row["件数"])}件 ({row["割合"]:.1f}%), 成約率{row["成約率"]*100:.1f}%')

    # 5. 都道府県別（TOP10）
    print('\n' + '='*80)
    print('5. 都道府県別 決裁者到達率 TOP10')
    print('='*80)

    pref_stats = df.groupby('Account.Prefectures__c').agg(
        件数=('is_won', 'count'),
        代表者決裁者率=('is_rep_decision', 'mean'),
        担当者決裁者率=('is_staff_decision', 'mean'),
        担当者決裁権率=('is_staff_authority', 'mean'),
        決裁者到達率=('can_reach_any', 'mean'),
        成約率=('is_won', 'mean')
    ).query('件数 >= 50').sort_values('決裁者到達率', ascending=False).head(15).round(4)
    print(pref_stats.to_string())

    # 6. サービス種別別
    print('\n' + '='*80)
    print('6. サービス種別別 決裁者タイプ分布')
    print('='*80)

    service_stats = df.groupby('Account.ServiceType__c').agg(
        件数=('is_won', 'count'),
        代表者決裁者率=('is_rep_decision', 'mean'),
        担当者決裁者率=('is_staff_decision', 'mean'),
        担当者決裁権率=('is_staff_authority', 'mean'),
        決裁者到達率=('can_reach_any', 'mean'),
        成約率=('is_won', 'mean')
    ).query('件数 >= 50').sort_values('決裁者到達率', ascending=False).round(4)
    print(service_stats.to_string())

    # 7. 法人格別
    print('\n' + '='*80)
    print('7. 法人格別 決裁者タイプ分布')
    print('='*80)

    legal_stats = df.groupby('Account.LegalPersonality__c').agg(
        件数=('is_won', 'count'),
        代表者決裁者率=('is_rep_decision', 'mean'),
        担当者決裁者率=('is_staff_decision', 'mean'),
        担当者決裁権率=('is_staff_authority', 'mean'),
        決裁者到達率=('can_reach_any', 'mean'),
        成約率=('is_won', 'mean')
    ).query('件数 >= 30').sort_values('決裁者到達率', ascending=False).round(4)
    print(legal_stats.to_string())

    # 8. 予測モデル（各タイプ別）
    print('\n' + '='*80)
    print('8. 決裁者タイプ別 予測モデル')
    print('='*80)

    # 特徴量準備
    df_model = df.dropna(subset=['employees', 'population', 'pop_density']).copy()

    for col in ['Account.LegalPersonality__c', 'Account.ServiceType__c', 'Account.Prefectures__c']:
        if col in df_model.columns:
            le = LabelEncoder()
            df_model[col + '_enc'] = le.fit_transform(df_model[col].fillna('Unknown').astype(str))

    X = df_model[['employees', 'population', 'pop_density',
                  'Account.LegalPersonality__c_enc', 'Account.ServiceType__c_enc',
                  'Account.Prefectures__c_enc']].fillna(0)

    targets = [
        ('A: 代表者商談（決裁者）', 'is_rep_decision'),
        ('B: 担当者商談（決裁者）', 'is_staff_decision'),
        ('C: 担当者商談（決裁権あり）', 'is_staff_authority'),
        ('全体: 決裁者到達', 'can_reach_any')
    ]

    feature_names = ['従業員数', '人口', '人口密度', '法人格', 'サービス種別', '都道府県']

    for name, col in targets:
        y = df_model[col].values
        if y.sum() < 20:
            print(f'\n【{name}】サンプル数不足')
            continue

        rf = RandomForestClassifier(n_estimators=100, max_depth=8, random_state=42)
        cv_score = cross_val_score(rf, X, y, cv=5, scoring='roc_auc').mean()
        rf.fit(X, y)

        print(f'\n【{name}】')
        print(f'  CV AUC: {cv_score:.4f}')
        print('  特徴量重要度:')

        importance = pd.DataFrame({
            'feature': feature_names,
            'importance': rf.feature_importances_
        }).sort_values('importance', ascending=False)

        for _, row in importance.iterrows():
            print(f'    {row["feature"]}: {row["importance"]*100:.1f}%')

    # 9. 最強セグメント（各タイプ別）
    print('\n' + '='*80)
    print('9. 決裁者タイプ別 最強セグメント')
    print('='*80)

    major_legal = ['株式会社', '医療法人', '社会福祉法人', '有限会社', '合同会社']
    df_combo = df[df['Account.LegalPersonality__c'].isin(major_legal)].copy()
    df_combo['emp_cat'] = pd.cut(df_combo['employees'], bins=[0, 30, 100, 500, float('inf')],
                                  labels=['小(~30)', '中(31-100)', '大(101-500)', '超大(500+)'])

    for name, col in targets[:3]:  # A, B, C のみ
        print(f'\n【{name}】')

        combo = df_combo.groupby(['Account.ServiceType__c', 'emp_cat', 'Account.LegalPersonality__c'], observed=True).agg(
            件数=('is_won', 'count'),
            該当率=(col, 'mean'),
            成約率=('is_won', 'mean')
        ).query('件数 >= 20 and 該当率 > 0').sort_values('該当率', ascending=False).head(10).round(4)

        if len(combo) > 0:
            print(combo.to_string())
        else:
            combo2 = df_combo.groupby(['Account.ServiceType__c', 'emp_cat', 'Account.LegalPersonality__c'], observed=True).agg(
                件数=('is_won', 'count'),
                該当率=(col, 'mean'),
                成約率=('is_won', 'mean')
            ).query('件数 >= 10 and 該当率 > 0').sort_values('該当率', ascending=False).head(10).round(4)
            if len(combo2) > 0:
                print(combo2.to_string())
            else:
                print('  該当セグメントなし')

    # 10. 「担当者商談（決裁権あり）」の詳細分析
    print('\n' + '='*80)
    print('10.「担当者商談（決裁権あり）」の詳細分析')
    print('='*80)

    df_auth = df[df['decision_type'] == 'C: 担当者商談（決裁権あり）'].copy()
    print(f'\n総件数: {len(df_auth)}件, 成約率: {df_auth["is_won"].mean():.1%}')

    print('\n■ 従業員数別')
    auth_emp = df_auth.groupby('emp_band', observed=True).agg(
        件数=('is_won', 'count'),
        成約数=('is_won', 'sum'),
        成約率=('is_won', 'mean')
    ).round(4)
    print(auth_emp.to_string())

    print('\n■ 人口帯別')
    auth_pop = df_auth.groupby('pop_band', observed=True).agg(
        件数=('is_won', 'count'),
        成約数=('is_won', 'sum'),
        成約率=('is_won', 'mean')
    ).round(4)
    print(auth_pop.to_string())

    print('\n■ サービス種別別')
    auth_service = df_auth.groupby('Account.ServiceType__c').agg(
        件数=('is_won', 'count'),
        成約数=('is_won', 'sum'),
        成約率=('is_won', 'mean')
    ).query('件数 >= 5').sort_values('成約率', ascending=False).round(4)
    print(auth_service.to_string())

    print('\n■ 法人格別')
    auth_legal = df_auth.groupby('Account.LegalPersonality__c').agg(
        件数=('is_won', 'count'),
        成約数=('is_won', 'sum'),
        成約率=('is_won', 'mean')
    ).query('件数 >= 5').sort_values('成約率', ascending=False).round(4)
    print(auth_legal.to_string())

    return df, type_stats


if __name__ == "__main__":
    df, type_stats = main()
