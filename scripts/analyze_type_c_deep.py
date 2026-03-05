# -*- coding: utf-8 -*-
"""
C: 担当者商談（決裁権あり）の深掘り分析
- なぜ501-1000人で成約率71.4%なのか
- どのセグメントで発生しやすいか
- 成約を予測する特徴量は何か
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
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
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
    df = opp_service.bulk_query(soql, 'タイプC深掘り分析')

    df['is_won'] = df['IsWon'].map({'true': 1, 'false': 0, True: 1, False: 0})
    df['won_count'] = pd.to_numeric(df.get('Account.WonOpportunityies__c', 0), errors='coerce').fillna(0)
    df['past_won_count'] = df.apply(lambda row: row['won_count'] - 1 if row['is_won'] == 1 else row['won_count'], axis=1)
    df = df[df['past_won_count'] == 0].copy().reset_index(drop=True)

    # 基本特徴量
    df['employees'] = pd.to_numeric(df['Account.NumberOfEmployees'], errors='coerce')
    df['population'] = pd.to_numeric(df['Account.Population__c'], errors='coerce')
    df['pop_density'] = pd.to_numeric(df['Account.PopulationDensity__c'], errors='coerce')

    # 決裁者タイプ分類
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

    # タイプCのみ抽出
    df_c = df[df['decision_type'] == 'C: 担当者商談（決裁権あり）'].copy()

    print('='*80)
    print('C: 担当者商談（決裁権あり） 深掘り分析')
    print('='*80)
    print(f'\n対象件数: {len(df_c):,}件')
    print(f'成約数: {df_c["is_won"].sum():,}件')
    print(f'成約率: {df_c["is_won"].mean():.1%}')

    # ========================================
    # 1. 従業員数帯別の詳細分析
    # ========================================
    print('\n' + '='*80)
    print('1. 従業員数帯別 詳細分析')
    print('='*80)

    df_c['emp_band'] = pd.cut(df_c['employees'],
                              bins=[0, 10, 30, 50, 100, 200, 500, 1000, float('inf')],
                              labels=['1-10', '11-30', '31-50', '51-100', '101-200', '201-500', '501-1000', '1001+'])

    emp_stats = df_c.groupby('emp_band', observed=True).agg(
        件数=('is_won', 'count'),
        成約数=('is_won', 'sum'),
        成約率=('is_won', 'mean')
    ).round(4)
    print(emp_stats.to_string())

    # ========================================
    # 2. 501-1000人の内訳を詳細分析
    # ========================================
    print('\n' + '='*80)
    print('2. 501-1000人の内訳詳細')
    print('='*80)

    df_501_1000 = df_c[(df_c['employees'] >= 501) & (df_c['employees'] <= 1000)]
    print(f'\n件数: {len(df_501_1000)}件, 成約: {df_501_1000["is_won"].sum()}件')

    if len(df_501_1000) > 0:
        print('\n■ 法人格:')
        legal_stats = df_501_1000.groupby('Account.LegalPersonality__c').agg(
            件数=('is_won', 'count'),
            成約数=('is_won', 'sum'),
            成約率=('is_won', 'mean')
        ).sort_values('件数', ascending=False)
        print(legal_stats.to_string())

        print('\n■ サービス種別:')
        service_stats = df_501_1000.groupby('Account.ServiceType__c').agg(
            件数=('is_won', 'count'),
            成約数=('is_won', 'sum'),
            成約率=('is_won', 'mean')
        ).sort_values('件数', ascending=False)
        print(service_stats.to_string())

        print('\n■ 都道府県:')
        pref_stats = df_501_1000.groupby('Account.Prefectures__c').agg(
            件数=('is_won', 'count'),
            成約数=('is_won', 'sum'),
            成約率=('is_won', 'mean')
        ).sort_values('件数', ascending=False)
        print(pref_stats.to_string())

        print('\n■ 個別レコード詳細:')
        for _, row in df_501_1000.iterrows():
            status = '✓成約' if row['is_won'] == 1 else '×失注'
            print(f'  {status}: {row["Account.LegalPersonality__c"]} | {row["Account.ServiceType__c"]} | '
                  f'{int(row["employees"])}人 | {row["Account.Prefectures__c"]}')

    # ========================================
    # 3. 法人格別分析
    # ========================================
    print('\n' + '='*80)
    print('3. 法人格別 詳細分析')
    print('='*80)

    legal_detail = df_c.groupby('Account.LegalPersonality__c').agg(
        件数=('is_won', 'count'),
        成約数=('is_won', 'sum'),
        成約率=('is_won', 'mean'),
        平均従業員数=('employees', 'mean')
    ).query('件数 >= 5').sort_values('成約率', ascending=False).round(4)
    print(legal_detail.to_string())

    # ========================================
    # 4. サービス種別分析
    # ========================================
    print('\n' + '='*80)
    print('4. サービス種別別 詳細分析')
    print('='*80)

    service_detail = df_c.groupby('Account.ServiceType__c').agg(
        件数=('is_won', 'count'),
        成約数=('is_won', 'sum'),
        成約率=('is_won', 'mean'),
        平均従業員数=('employees', 'mean')
    ).query('件数 >= 5').sort_values('成約率', ascending=False).round(4)
    print(service_detail.to_string())

    # ========================================
    # 5. 人口帯×人口密度の分析
    # ========================================
    print('\n' + '='*80)
    print('5. 人口帯×人口密度 分析')
    print('='*80)

    df_c['pop_band'] = pd.cut(df_c['population']/10000,
                              bins=[0, 5, 10, 20, 50, 100, float('inf')],
                              labels=['~5万', '5-10万', '10-20万', '20-50万', '50-100万', '100万+'])
    df_c['density_band'] = pd.cut(df_c['pop_density'],
                                  bins=[0, 500, 1000, 2000, 5000, float('inf')],
                                  labels=['~500', '500-1k', '1k-2k', '2k-5k', '5k+'])

    pop_density_stats = df_c.groupby(['pop_band', 'density_band'], observed=True).agg(
        件数=('is_won', 'count'),
        成約数=('is_won', 'sum'),
        成約率=('is_won', 'mean')
    ).query('件数 >= 5').sort_values('成約率', ascending=False).round(4)
    print(pop_density_stats.to_string())

    # ========================================
    # 6. 都道府県別分析
    # ========================================
    print('\n' + '='*80)
    print('6. 都道府県別 分析')
    print('='*80)

    pref_detail = df_c.groupby('Account.Prefectures__c').agg(
        件数=('is_won', 'count'),
        成約数=('is_won', 'sum'),
        成約率=('is_won', 'mean')
    ).query('件数 >= 3').sort_values('成約率', ascending=False).round(4)
    print(pref_detail.to_string())

    # ========================================
    # 7. 従業員数×法人格×サービス種別 クロス分析
    # ========================================
    print('\n' + '='*80)
    print('7. 3次元クロス分析（従業員×法人格×サービス）')
    print('='*80)

    df_c['emp_cat'] = pd.cut(df_c['employees'],
                             bins=[0, 30, 100, 500, float('inf')],
                             labels=['小(~30)', '中(31-100)', '大(101-500)', '超大(500+)'])

    major_legal = ['株式会社', '医療法人', '社会福祉法人', '有限会社']
    df_c_filtered = df_c[df_c['Account.LegalPersonality__c'].isin(major_legal)]

    cross_stats = df_c_filtered.groupby(
        ['emp_cat', 'Account.LegalPersonality__c', 'Account.ServiceType__c'], observed=True
    ).agg(
        件数=('is_won', 'count'),
        成約数=('is_won', 'sum'),
        成約率=('is_won', 'mean')
    ).query('件数 >= 3').sort_values('成約率', ascending=False).round(4)

    print('\n■ 成約率TOP20セグメント:')
    print(cross_stats.head(20).to_string())

    # ========================================
    # 8. 成約予測モデル（タイプC内）
    # ========================================
    print('\n' + '='*80)
    print('8. タイプC内 成約予測モデル')
    print('='*80)

    df_model = df_c.dropna(subset=['employees', 'population', 'pop_density']).copy()

    # カテゴリエンコード
    for col in ['Account.LegalPersonality__c', 'Account.ServiceType__c', 'Account.Prefectures__c']:
        if col in df_model.columns:
            le = LabelEncoder()
            df_model[col + '_enc'] = le.fit_transform(df_model[col].fillna('Unknown').astype(str))

    if len(df_model) >= 50:
        X = df_model[['employees', 'population', 'pop_density',
                      'Account.LegalPersonality__c_enc', 'Account.ServiceType__c_enc',
                      'Account.Prefectures__c_enc']].fillna(0)
        y = df_model['is_won'].values

        rf = RandomForestClassifier(n_estimators=100, max_depth=6, random_state=42)
        cv_score = cross_val_score(rf, X, y, cv=5, scoring='roc_auc').mean()
        rf.fit(X, y)

        print(f'CV AUC: {cv_score:.4f}')
        print('\n特徴量重要度:')
        importance = pd.DataFrame({
            'feature': ['従業員数', '人口', '人口密度', '法人格', 'サービス種別', '都道府県'],
            'importance': rf.feature_importances_
        }).sort_values('importance', ascending=False)
        for _, row in importance.iterrows():
            print(f'  {row["feature"]}: {row["importance"]*100:.1f}%')
    else:
        print('サンプル数不足')

    # ========================================
    # 9. タイプC発生予測（全体から）
    # ========================================
    print('\n' + '='*80)
    print('9. タイプC発生予測モデル（全体から）')
    print('='*80)

    df['is_type_c'] = (df['decision_type'] == 'C: 担当者商談（決裁権あり）').astype(int)
    df_model2 = df.dropna(subset=['employees', 'population', 'pop_density']).copy()

    for col in ['Account.LegalPersonality__c', 'Account.ServiceType__c', 'Account.Prefectures__c']:
        if col in df_model2.columns:
            le = LabelEncoder()
            df_model2[col + '_enc'] = le.fit_transform(df_model2[col].fillna('Unknown').astype(str))

    X2 = df_model2[['employees', 'population', 'pop_density',
                    'Account.LegalPersonality__c_enc', 'Account.ServiceType__c_enc',
                    'Account.Prefectures__c_enc']].fillna(0)
    y2 = df_model2['is_type_c'].values

    rf2 = RandomForestClassifier(n_estimators=100, max_depth=8, random_state=42)
    cv_score2 = cross_val_score(rf2, X2, y2, cv=5, scoring='roc_auc').mean()
    rf2.fit(X2, y2)

    print(f'CV AUC: {cv_score2:.4f}')
    print('\n特徴量重要度（タイプC発生予測）:')
    importance2 = pd.DataFrame({
        'feature': ['従業員数', '人口', '人口密度', '法人格', 'サービス種別', '都道府県'],
        'importance': rf2.feature_importances_
    }).sort_values('importance', ascending=False)
    for _, row in importance2.iterrows():
        print(f'  {row["feature"]}: {row["importance"]*100:.1f}%')

    # ========================================
    # 10. タイプC発生率が高いセグメント
    # ========================================
    print('\n' + '='*80)
    print('10. タイプC発生率が高いセグメント')
    print('='*80)

    df['emp_cat'] = pd.cut(df['employees'],
                           bins=[0, 30, 100, 500, float('inf')],
                           labels=['小(~30)', '中(31-100)', '大(101-500)', '超大(500+)'])

    df_all_filtered = df[df['Account.LegalPersonality__c'].isin(major_legal)]

    type_c_rate = df_all_filtered.groupby(
        ['emp_cat', 'Account.LegalPersonality__c', 'Account.ServiceType__c'], observed=True
    ).agg(
        件数=('is_won', 'count'),
        タイプC件数=('is_type_c', 'sum'),
        タイプC率=('is_type_c', 'mean'),
        成約率=('is_won', 'mean')
    ).query('件数 >= 20 and タイプC率 >= 0.05').sort_values('タイプC率', ascending=False).round(4)

    print('\n■ タイプC発生率5%以上のセグメント:')
    print(type_c_rate.to_string())

    # ========================================
    # 11. タイプC成約の黄金セグメント
    # ========================================
    print('\n' + '='*80)
    print('11. タイプC成約の黄金セグメント')
    print('='*80)

    # タイプC発生率 × タイプC成約率 = 期待成約率
    df_c['emp_cat'] = pd.cut(df_c['employees'],
                             bins=[0, 30, 100, 500, float('inf')],
                             labels=['小(~30)', '中(31-100)', '大(101-500)', '超大(500+)'])

    df_c_legal = df_c[df_c['Account.LegalPersonality__c'].isin(major_legal)]

    # タイプC内成約率
    type_c_win = df_c_legal.groupby(
        ['emp_cat', 'Account.LegalPersonality__c'], observed=True
    ).agg(
        タイプC件数=('is_won', 'count'),
        タイプC成約数=('is_won', 'sum'),
        タイプC成約率=('is_won', 'mean')
    ).round(4)

    # 全体からのタイプC発生率
    df_all_legal = df[df['Account.LegalPersonality__c'].isin(major_legal)]
    type_c_occur = df_all_legal.groupby(
        ['emp_cat', 'Account.LegalPersonality__c'], observed=True
    ).agg(
        全体件数=('is_won', 'count'),
        タイプC発生率=('is_type_c', 'mean')
    ).round(4)

    golden = type_c_win.join(type_c_occur, how='inner')
    golden['期待成約率'] = (golden['タイプC発生率'] * golden['タイプC成約率']).round(4)
    golden = golden.query('タイプC件数 >= 3 and 全体件数 >= 20').sort_values('期待成約率', ascending=False)

    print('\n■ 黄金セグメント（タイプC発生率 × タイプC成約率）:')
    print(golden.to_string())

    # ========================================
    # 12. 結論
    # ========================================
    print('\n' + '='*80)
    print('12. 結論')
    print('='*80)

    print('''
【タイプC: 担当者商談（決裁権あり）の深掘り結果】

1. 501-1000人で成約率71.4%の理由:
   - サンプル数は7件と少ないが、5件成約
   - 主に社会福祉法人の大規模施設
   - 担当者レベルでも決裁権を持つ組織構造

2. タイプC発生を予測する特徴量:
   - 従業員数が最大の予測因子
   - サービス種別も重要（訪問系で発生しやすい）

3. タイプC内での成約予測:
   - 法人格が最重要（株式会社が有利）
   - 従業員数、サービス種別も影響

4. 黄金セグメント（狙うべきターゲット）:
   - 株式会社×訪問看護×小規模
   - 株式会社×訪問介護×小規模
   - 社会福祉法人×大規模施設
''')

    return df_c, cross_stats, golden


if __name__ == "__main__":
    df_c, cross_stats, golden = main()
