# -*- coding: utf-8 -*-
"""
3軸MECE分析：
1. お金を出せる顧客（予算・支払能力）
2. 決裁者到達予測（事前情報から）
3. 決裁者商談の場合の成約要因
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
from sklearn.metrics import roc_auc_score
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
    df = opp_service.bulk_query(soql, '3軸MECE分析')

    df['is_won'] = df['IsWon'].map({'true': 1, 'false': 0, True: 1, False: 0})
    df['won_count'] = pd.to_numeric(df.get('Account.WonOpportunityies__c', 0), errors='coerce').fillna(0)
    df['past_won_count'] = df.apply(lambda row: row['won_count'] - 1 if row['is_won'] == 1 else row['won_count'], axis=1)
    df = df[df['past_won_count'] == 0].copy().reset_index(drop=True)

    # 基本特徴量
    df['employees'] = pd.to_numeric(df['Account.NumberOfEmployees'], errors='coerce')
    df['population'] = pd.to_numeric(df['Account.Population__c'], errors='coerce')
    df['pop_density'] = pd.to_numeric(df['Account.PopulationDensity__c'], errors='coerce')

    # 決裁者関連
    df['is_decision_maker_opp'] = df['OpportunityType__c'].apply(lambda x: 1 if '決裁者' in str(x) else 0)
    df['has_authority'] = df['Hearing_Authority__c'].apply(lambda x: 1 if str(x) == 'あり' else 0)
    df['can_reach_decision_maker'] = ((df['is_decision_maker_opp'] == 1) | (df['has_authority'] == 1)).astype(int)

    print('='*80)
    print('3軸MECE分析')
    print('='*80)
    print(f'\n総件数: {len(df):,}件, 成約率: {df["is_won"].mean():.1%}')

    # ========================================
    # 軸1: お金を出せる顧客（予算・支払能力）
    # ========================================
    print('\n' + '='*80)
    print('【軸1】お金を出せる顧客の特徴量分析')
    print('='*80)

    # 人口帯別
    print('\n■ 人口帯別 成約率')
    df['pop_band'] = pd.cut(df['population']/10000,
                            bins=[0, 5, 10, 20, 50, 100, float('inf')],
                            labels=['~5万', '5-10万', '10-20万', '20-50万', '50-100万', '100万+'])
    pop_stats = df.groupby('pop_band', observed=True).agg(
        件数=('is_won', 'count'),
        成約数=('is_won', 'sum'),
        成約率=('is_won', 'mean')
    ).round(4)
    print(pop_stats.to_string())

    # 人口密度帯別
    print('\n■ 人口密度帯別 成約率')
    df['density_band'] = pd.cut(df['pop_density'],
                                bins=[0, 500, 1000, 2000, 5000, 10000, float('inf')],
                                labels=['~500', '500-1k', '1k-2k', '2k-5k', '5k-10k', '10k+'])
    density_stats = df.groupby('density_band', observed=True).agg(
        件数=('is_won', 'count'),
        成約数=('is_won', 'sum'),
        成約率=('is_won', 'mean')
    ).round(4)
    print(density_stats.to_string())

    # 従業員数帯別
    print('\n■ 従業員数帯別 成約率')
    df['emp_band'] = pd.cut(df['employees'],
                            bins=[0, 10, 30, 50, 100, 200, 500, 1000, float('inf')],
                            labels=['1-10', '11-30', '31-50', '51-100', '101-200', '201-500', '501-1000', '1001+'])
    emp_stats = df.groupby('emp_band', observed=True).agg(
        件数=('is_won', 'count'),
        成約数=('is_won', 'sum'),
        成約率=('is_won', 'mean')
    ).round(4)
    print(emp_stats.to_string())

    # お金を出せる顧客の予測モデル
    print('\n■ 「お金を出せる顧客」予測モデル（事前情報のみ）')
    df_model1 = df.dropna(subset=['employees', 'population', 'pop_density']).copy()

    # カテゴリエンコード
    for col in ['Account.LegalPersonality__c', 'Account.ServiceType__c', 'Account.Prefectures__c']:
        if col in df_model1.columns:
            le = LabelEncoder()
            df_model1[col + '_enc'] = le.fit_transform(df_model1[col].fillna('Unknown').astype(str))

    X1 = df_model1[['employees', 'population', 'pop_density',
                    'Account.LegalPersonality__c_enc', 'Account.ServiceType__c_enc',
                    'Account.Prefectures__c_enc']].fillna(0)
    y1 = df_model1['is_won'].values

    rf1 = RandomForestClassifier(n_estimators=100, max_depth=8, random_state=42)
    cv_score1 = cross_val_score(rf1, X1, y1, cv=5, scoring='roc_auc').mean()
    rf1.fit(X1, y1)

    print(f'  CV AUC: {cv_score1:.4f}')
    print('\n  特徴量重要度:')
    importance1 = pd.DataFrame({
        'feature': ['従業員数', '人口', '人口密度', '法人格', 'サービス種別', '都道府県'],
        'importance': rf1.feature_importances_
    }).sort_values('importance', ascending=False)
    for _, row in importance1.iterrows():
        print(f'    {row["feature"]}: {row["importance"]*100:.1f}%')

    # ========================================
    # 軸2: 決裁者到達予測（事前情報から）
    # ========================================
    print('\n' + '='*80)
    print('【軸2】決裁者到達予測（事前情報から）')
    print('='*80)

    print('\n■ 決裁者到達の定義')
    print('  決裁者商談 OR 決裁権あり = 決裁者に到達')

    reach_rate = df['can_reach_decision_maker'].mean()
    print(f'\n■ 全体の決裁者到達率: {reach_rate:.1%}')

    # 決裁者到達時の成約率
    reach_win = df[df['can_reach_decision_maker'] == 1]['is_won'].mean()
    no_reach_win = df[df['can_reach_decision_maker'] == 0]['is_won'].mean()
    print(f'  決裁者到達時の成約率: {reach_win:.1%}')
    print(f'  決裁者未到達時の成約率: {no_reach_win:.1%}')
    print(f'  差: {reach_win/no_reach_win:.1f}倍')

    # 従業員数別
    print('\n■ 従業員数別 決裁者到達率')
    emp_reach = df.groupby('emp_band', observed=True).agg(
        件数=('is_won', 'count'),
        決裁者到達率=('can_reach_decision_maker', 'mean'),
        成約率=('is_won', 'mean')
    ).round(4)
    print(emp_reach.to_string())

    # 決裁者到達予測モデル
    print('\n■ 決裁者到達予測モデル（事前情報のみ）')
    X2 = df_model1[['employees', 'population', 'pop_density',
                    'Account.LegalPersonality__c_enc', 'Account.ServiceType__c_enc']].fillna(0)
    y2 = df_model1['can_reach_decision_maker'].values

    rf2 = RandomForestClassifier(n_estimators=100, max_depth=8, random_state=42)
    cv_score2 = cross_val_score(rf2, X2, y2, cv=5, scoring='roc_auc').mean()
    rf2.fit(X2, y2)

    print(f'  CV AUC: {cv_score2:.4f}')
    print('\n  特徴量重要度:')
    importance2 = pd.DataFrame({
        'feature': ['従業員数', '人口', '人口密度', '法人格', 'サービス種別'],
        'importance': rf2.feature_importances_
    }).sort_values('importance', ascending=False)
    for _, row in importance2.iterrows():
        print(f'    {row["feature"]}: {row["importance"]*100:.1f}%')

    # ========================================
    # 軸3: 決裁者商談の場合の成約要因
    # ========================================
    print('\n' + '='*80)
    print('【軸3】決裁者商談の場合の成約要因')
    print('='*80)

    # 決裁者商談のみ抽出
    df_decision = df[df['can_reach_decision_maker'] == 1].copy()
    print(f'\n■ 決裁者到達商談: {len(df_decision):,}件, 成約率: {df_decision["is_won"].mean():.1%}')

    # 人口帯別（決裁者商談のみ）
    print('\n■ 人口帯別 成約率（決裁者商談のみ）')
    pop_decision = df_decision.groupby('pop_band', observed=True).agg(
        件数=('is_won', 'count'),
        成約数=('is_won', 'sum'),
        成約率=('is_won', 'mean')
    ).query('件数 >= 10').round(4)
    print(pop_decision.to_string())

    # 人口密度別（決裁者商談のみ）
    print('\n■ 人口密度帯別 成約率（決裁者商談のみ）')
    density_decision = df_decision.groupby('density_band', observed=True).agg(
        件数=('is_won', 'count'),
        成約数=('is_won', 'sum'),
        成約率=('is_won', 'mean')
    ).query('件数 >= 10').round(4)
    print(density_decision.to_string())

    # 従業員数別（決裁者商談のみ）
    print('\n■ 従業員数帯別 成約率（決裁者商談のみ）')
    emp_decision = df_decision.groupby('emp_band', observed=True).agg(
        件数=('is_won', 'count'),
        成約数=('is_won', 'sum'),
        成約率=('is_won', 'mean')
    ).query('件数 >= 10').round(4)
    print(emp_decision.to_string())

    # 決裁者商談の場合の成約予測モデル
    print('\n■ 決裁者商談の成約予測モデル')
    df_decision_model = df_decision.dropna(subset=['employees', 'population', 'pop_density']).copy()

    for col in ['Account.LegalPersonality__c', 'Account.ServiceType__c', 'Account.Prefectures__c']:
        if col in df_decision_model.columns:
            le = LabelEncoder()
            df_decision_model[col + '_enc'] = le.fit_transform(df_decision_model[col].fillna('Unknown').astype(str))

    if len(df_decision_model) >= 100:
        X3 = df_decision_model[['employees', 'population', 'pop_density',
                                'Account.LegalPersonality__c_enc', 'Account.ServiceType__c_enc',
                                'Account.Prefectures__c_enc']].fillna(0)
        y3 = df_decision_model['is_won'].values

        rf3 = RandomForestClassifier(n_estimators=100, max_depth=6, random_state=42)
        cv_score3 = cross_val_score(rf3, X3, y3, cv=5, scoring='roc_auc').mean()
        rf3.fit(X3, y3)

        print(f'  CV AUC: {cv_score3:.4f}')
        print('\n  特徴量重要度（決裁者商談の場合）:')
        importance3 = pd.DataFrame({
            'feature': ['従業員数', '人口', '人口密度', '法人格', 'サービス種別', '都道府県'],
            'importance': rf3.feature_importances_
        }).sort_values('importance', ascending=False)
        for _, row in importance3.iterrows():
            print(f'    {row["feature"]}: {row["importance"]*100:.1f}%')
    else:
        print('  サンプル数不足')

    # ========================================
    # 3軸の統合サマリー
    # ========================================
    print('\n' + '='*80)
    print('【3軸統合】最強セグメント')
    print('='*80)

    # 人口×人口密度×従業員数の組み合わせ
    df['pop_cat'] = pd.cut(df['population']/10000, bins=[0, 20, 100, float('inf')],
                           labels=['小都市(~20万)', '中都市(20-100万)', '大都市(100万+)'])
    df['density_cat'] = pd.cut(df['pop_density'], bins=[0, 1000, 5000, float('inf')],
                               labels=['低密度(~1k)', '中密度(1k-5k)', '高密度(5k+)'])
    df['emp_cat'] = pd.cut(df['employees'], bins=[0, 30, 100, float('inf')],
                           labels=['小規模(~30)', '中規模(31-100)', '大規模(100+)'])

    print('\n■ 人口×人口密度×従業員規模 の成約率')
    combo = df.groupby(['pop_cat', 'density_cat', 'emp_cat'], observed=True).agg(
        件数=('is_won', 'count'),
        成約数=('is_won', 'sum'),
        成約率=('is_won', 'mean'),
        決裁者到達率=('can_reach_decision_maker', 'mean')
    ).query('件数 >= 30').sort_values('成約率', ascending=False).round(4)
    print(combo.head(15).to_string())

    print('\n■ 3軸すべてを満たす最強セグメント')
    best = combo[combo['成約率'] >= 0.06].sort_values('成約率', ascending=False)
    if len(best) > 0:
        print(best.to_string())
    else:
        print('  該当なし（閾値を下げます）')
        best = combo.head(10)
        print(best.to_string())

    return df, importance1, importance2


if __name__ == "__main__":
    df, imp1, imp2 = main()
