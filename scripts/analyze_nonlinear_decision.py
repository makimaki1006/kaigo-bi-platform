# -*- coding: utf-8 -*-
"""
非線形分析 + 決裁者商談予測分析
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
from sklearn.metrics import roc_auc_score, classification_report
from sklearn.model_selection import cross_val_score, StratifiedKFold
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
    df = opp_service.bulk_query(soql, '非線形+決裁者予測分析')

    df['is_won'] = df['IsWon'].map({'true': 1, 'false': 0, True: 1, False: 0})
    df['won_count'] = pd.to_numeric(df.get('Account.WonOpportunityies__c', 0), errors='coerce').fillna(0)
    df['past_won_count'] = df.apply(lambda row: row['won_count'] - 1 if row['is_won'] == 1 else row['won_count'], axis=1)
    df = df[df['past_won_count'] == 0].copy().reset_index(drop=True)

    df['employees'] = pd.to_numeric(df['Account.NumberOfEmployees'], errors='coerce')
    df['population'] = pd.to_numeric(df['Account.Population__c'], errors='coerce')
    df['pop_density'] = pd.to_numeric(df['Account.PopulationDensity__c'], errors='coerce')

    # 決裁者関連
    df['is_decision_maker'] = df['OpportunityType__c'].apply(lambda x: 1 if '決裁者' in str(x) else 0)
    df['is_representative'] = df['OpportunityType__c'].apply(lambda x: 1 if '代表者' in str(x) else 0)
    df['has_authority'] = df['Hearing_Authority__c'].apply(lambda x: 1 if str(x) == 'あり' else 0)

    # 決裁者に会える = 決裁者商談 OR 決裁権あり
    df['can_reach_decision_maker'] = ((df['is_decision_maker'] == 1) | (df['has_authority'] == 1)).astype(int)

    print('='*70)
    print('1. 非線形分析：数値特徴量と成約率の関係')
    print('='*70)

    # 従業員数の区間別成約率
    print('\n【従業員数】')
    emp_bins = [0, 10, 30, 50, 100, 200, 500, 1000, float('inf')]
    emp_labels = ['1-10', '11-30', '31-50', '51-100', '101-200', '201-500', '501-1000', '1001+']
    df['emp_bin'] = pd.cut(df['employees'], bins=emp_bins, labels=emp_labels)

    emp_stats = df.groupby('emp_bin', observed=True).agg(
        件数=('is_won', 'count'),
        成約数=('is_won', 'sum'),
        成約率=('is_won', 'mean'),
        決裁者到達率=('can_reach_decision_maker', 'mean')
    ).round(4)
    print(emp_stats.to_string())

    # 人口の区間別成約率
    print('\n【人口（万人）】')
    df['pop_bin'] = pd.cut(df['population']/10000, bins=[0, 5, 10, 20, 50, 100, float('inf')],
                          labels=['~5万', '5-10万', '10-20万', '20-50万', '50-100万', '100万+'])

    pop_stats = df.groupby('pop_bin', observed=True).agg(
        件数=('is_won', 'count'),
        成約数=('is_won', 'sum'),
        成約率=('is_won', 'mean'),
        決裁者到達率=('can_reach_decision_maker', 'mean')
    ).round(4)
    print(pop_stats.to_string())

    # 人口密度の区間別成約率
    print('\n【人口密度（人/km2）】')
    df['density_bin'] = pd.cut(df['pop_density'], bins=[0, 500, 1000, 2000, 5000, 10000, float('inf')],
                               labels=['~500', '500-1k', '1k-2k', '2k-5k', '5k-10k', '10k+'])

    density_stats = df.groupby('density_bin', observed=True).agg(
        件数=('is_won', 'count'),
        成約数=('is_won', 'sum'),
        成約率=('is_won', 'mean'),
        決裁者到達率=('can_reach_decision_maker', 'mean')
    ).round(4)
    print(density_stats.to_string())

    print('\n' + '='*70)
    print('2. 決裁者商談の予測（事前情報から）')
    print('='*70)

    # 商談タイプの内訳
    print('\n【OpportunityType__c の分布】')
    opp_type_stats = df.groupby('OpportunityType__c').agg(
        件数=('is_won', 'count'),
        成約率=('is_won', 'mean')
    ).sort_values('件数', ascending=False)
    print(opp_type_stats.to_string())

    # 法人格 × 決裁者到達率
    print('\n【法人格別 決裁者到達率】')
    legal_stats = df.groupby('Account.LegalPersonality__c').agg(
        件数=('is_won', 'count'),
        決裁者到達率=('can_reach_decision_maker', 'mean'),
        成約率=('is_won', 'mean')
    ).query('件数 >= 30').sort_values('決裁者到達率', ascending=False).round(4)
    print(legal_stats.to_string())

    # ServiceType × 決裁者到達率
    print('\n【サービス種別 × 決裁者到達率】')
    service_stats = df.groupby('Account.ServiceType__c').agg(
        件数=('is_won', 'count'),
        決裁者到達率=('can_reach_decision_maker', 'mean'),
        成約率=('is_won', 'mean')
    ).query('件数 >= 30').sort_values('決裁者到達率', ascending=False).round(4)
    print(service_stats.to_string())

    # 従業員数 × 決裁者到達率（詳細）
    print('\n【従業員規模 × 決裁者到達率】')
    emp_decision = df.groupby('emp_bin', observed=True).agg(
        件数=('is_won', 'count'),
        決裁者商談率=('is_decision_maker', 'mean'),
        決裁権あり率=('has_authority', 'mean'),
        決裁者到達率=('can_reach_decision_maker', 'mean'),
        成約率=('is_won', 'mean')
    ).round(4)
    print(emp_decision.to_string())

    print('\n' + '='*70)
    print('3. 事前情報から決裁者到達を予測するモデル')
    print('='*70)

    # 特徴量準備
    df_model = df.dropna(subset=['employees', 'Account.LegalPersonality__c', 'Account.ServiceType__c'])

    le_legal = LabelEncoder()
    le_service = LabelEncoder()

    df_model['legal_enc'] = le_legal.fit_transform(df_model['Account.LegalPersonality__c'].fillna('Unknown'))
    df_model['service_enc'] = le_service.fit_transform(df_model['Account.ServiceType__c'].fillna('Unknown'))

    # 従業員数カテゴリ
    df_model['emp_cat'] = pd.cut(df_model['employees'], bins=[0, 30, 100, 500, float('inf')],
                                  labels=[0, 1, 2, 3])
    df_model['emp_cat'] = df_model['emp_cat'].cat.codes

    X = df_model[['employees', 'emp_cat', 'legal_enc', 'service_enc']].fillna(0)
    y = df_model['can_reach_decision_maker'].values

    # モデル学習
    rf = RandomForestClassifier(n_estimators=100, max_depth=5, random_state=42)
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    cv_scores = cross_val_score(rf, X, y, cv=cv, scoring='roc_auc')

    print(f'\n決裁者到達予測モデル（従業員数、法人格、サービス種別）')
    print(f'CV AUC: {cv_scores.mean():.4f} (+/- {cv_scores.std():.4f})')

    # 特徴量重要度
    rf.fit(X, y)
    importance = pd.DataFrame({
        'feature': ['employees', 'emp_cat', 'legal_enc', 'service_enc'],
        'importance': rf.feature_importances_
    }).sort_values('importance', ascending=False)
    print('\n特徴量重要度:')
    for _, row in importance.iterrows():
        print(f'  {row["feature"]}: {row["importance"]:.4f}')

    # 予測確率でグループ分け
    df_model['pred_prob'] = rf.predict_proba(X)[:, 1]
    df_model['pred_group'] = pd.qcut(df_model['pred_prob'], q=4, labels=['低', '中低', '中高', '高'])

    print('\n【予測確率グループ別の実績】')
    pred_stats = df_model.groupby('pred_group', observed=True).agg(
        件数=('is_won', 'count'),
        実際の決裁者到達率=('can_reach_decision_maker', 'mean'),
        成約率=('is_won', 'mean')
    ).round(4)
    print(pred_stats.to_string())

    print('\n' + '='*70)
    print('4. 組み合わせ分析（法人格 × 従業員規模）')
    print('='*70)

    # 法人格を主要なものに絞る
    major_legal = ['株式会社', '医療法人', '社会福祉法人', '有限会社', '合同会社']
    df_combo = df[df['Account.LegalPersonality__c'].isin(major_legal)].copy()
    df_combo['emp_size'] = pd.cut(df_combo['employees'], bins=[0, 30, 100, 500, float('inf')],
                                   labels=['小(~30)', '中(31-100)', '大(101-500)', '超大(500+)'])

    combo_stats = df_combo.groupby(['Account.LegalPersonality__c', 'emp_size'], observed=True).agg(
        件数=('is_won', 'count'),
        決裁者到達率=('can_reach_decision_maker', 'mean'),
        成約率=('is_won', 'mean')
    ).query('件数 >= 20').round(4)

    print('\n【法人格 × 従業員規模 → 決裁者到達率・成約率】')
    print(combo_stats.to_string())

    print('\n' + '='*70)
    print('5. 結論：決裁者に会いやすいセグメント')
    print('='*70)

    # 決裁者到達率が高いセグメントを特定
    high_reach = df_combo.groupby(['Account.LegalPersonality__c', 'emp_size'], observed=True).agg(
        件数=('is_won', 'count'),
        決裁者到達率=('can_reach_decision_maker', 'mean'),
        成約率=('is_won', 'mean')
    ).query('件数 >= 20 and 決裁者到達率 >= 0.15').sort_values('成約率', ascending=False).round(4)

    print('\n【決裁者到達率15%以上のセグメント（成約率順）】')
    print(high_reach.to_string())


if __name__ == "__main__":
    main()
