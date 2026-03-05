# -*- coding: utf-8 -*-
"""
時系列交差検証による最終評価
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
from sklearn.model_selection import TimeSeriesSplit
import lightgbm as lgb
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
               Account.Prefectures__c,
               Account.IndustryCategory__c,
               Account.ServiceType__c,
               Account.ServiceType2__c,
               Account.LegalPersonality__c,
               Account.Establish__c,
               Account.Hellowork_NumberOfEmployee_Office__c,
               Account.Hellowork_Industry__c,
               Account.WonOpportunityies__c,
               OpportunityType__c,
               Hearing_Authority__c,
               Hearing_ContactTitle__c,
               BusinessNegotiatorRole__c
        FROM Opportunity
        WHERE IsClosed = true AND CreatedDate >= 2025-04-01T00:00:00Z
        ORDER BY CreatedDate ASC
    """
    df = opp_service.bulk_query(soql, '時系列CV検証')

    df['is_won'] = df['IsWon'].map({'true': 1, 'false': 0, True: 1, False: 0})
    df['won_count'] = pd.to_numeric(df.get('Account.WonOpportunityies__c', 0), errors='coerce').fillna(0)
    df['past_won_count'] = df.apply(lambda row: row['won_count'] - 1 if row['is_won'] == 1 else row['won_count'], axis=1)
    df = df[df['past_won_count'] == 0].copy().reset_index(drop=True)
    df['created'] = pd.to_datetime(df['CreatedDate'])

    # 特徴量準備
    df['employees'] = pd.to_numeric(df['Account.NumberOfEmployees'], errors='coerce').fillna(0)
    df['population'] = pd.to_numeric(df['Account.Population__c'], errors='coerce').fillna(0)
    df['pop_density'] = pd.to_numeric(df['Account.PopulationDensity__c'], errors='coerce').fillna(0)
    df['establish'] = pd.to_numeric(df['Account.Establish__c'], errors='coerce').fillna(0)
    df['hw_employees'] = pd.to_numeric(df['Account.Hellowork_NumberOfEmployee_Office__c'], errors='coerce').fillna(0)
    df['is_decision_maker'] = df['OpportunityType__c'].apply(lambda x: 1 if '決裁者' in str(x) else 0)
    df['is_representative'] = df['OpportunityType__c'].apply(lambda x: 1 if '代表者' in str(x) else 0)
    df['has_authority'] = df['Hearing_Authority__c'].apply(lambda x: 1 if str(x) == 'あり' else 0)

    for col in ['Account.Prefectures__c', 'Account.IndustryCategory__c', 'Account.ServiceType__c',
                'Account.ServiceType2__c', 'Account.LegalPersonality__c', 'Account.Hellowork_Industry__c',
                'Hearing_ContactTitle__c', 'BusinessNegotiatorRole__c']:
        if col in df.columns:
            le = LabelEncoder()
            df[col + '_enc'] = le.fit_transform(df[col].fillna('Unknown').astype(str))

    df['log_employees'] = np.log1p(df['employees'])
    df['employees_x_decision'] = df['employees'] * df['is_decision_maker']
    df['employees_x_authority'] = df['employees'] * df['has_authority']
    df['log_emp_x_decision'] = df['log_employees'] * df['is_decision_maker']
    df['pop_density_x_decision'] = df['pop_density'] * df['is_decision_maker']
    df['representative_x_authority'] = df['is_representative'] * df['has_authority']
    df['decision_x_authority'] = df['is_decision_maker'] * df['has_authority']
    df['emp_category'] = pd.cut(df['employees'], bins=[0, 30, 100, 500, float('inf')], labels=['small', 'medium', 'large', 'enterprise'])
    df['emp_category_enc'] = LabelEncoder().fit_transform(df['emp_category'].astype(str))
    df['emp_cat_x_decision'] = df['emp_category_enc'] * df['is_decision_maker']
    df['density_high'] = (df['pop_density'] > df['pop_density'].median()).astype(int)
    df['density_x_decision'] = df['density_high'] * df['is_decision_maker']

    all_features = [
        'employees', 'population', 'pop_density', 'establish', 'hw_employees',
        'Account.Prefectures__c_enc', 'Account.IndustryCategory__c_enc',
        'Account.ServiceType__c_enc', 'Account.ServiceType2__c_enc',
        'Account.LegalPersonality__c_enc', 'Account.Hellowork_Industry__c_enc',
        'is_decision_maker', 'is_representative', 'has_authority',
        'Hearing_ContactTitle__c_enc', 'BusinessNegotiatorRole__c_enc',
        'log_employees', 'employees_x_decision', 'employees_x_authority',
        'log_emp_x_decision', 'pop_density_x_decision', 'representative_x_authority',
        'decision_x_authority', 'emp_category_enc', 'emp_cat_x_decision',
        'density_high', 'density_x_decision'
    ]

    X = df[all_features].fillna(0)
    y = df['is_won'].values

    print('='*70)
    print('時系列交差検証（より厳密な評価）')
    print('='*70)

    # 時系列CV（過去→未来の方向でのみ評価）
    tscv = TimeSeriesSplit(n_splits=5)

    print('\nLightGBM (best params):')
    lgb_scores = []
    for i, (train_idx, test_idx) in enumerate(tscv.split(X)):
        X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
        y_train, y_test = y[train_idx], y[test_idx]

        model = lgb.LGBMClassifier(
            n_estimators=200, max_depth=8, learning_rate=0.05, min_child_samples=10,
            subsample=0.8, colsample_bytree=0.8, random_state=42, verbose=-1
        )
        model.fit(X_train, y_train)
        score = roc_auc_score(y_test, model.predict_proba(X_test)[:, 1])
        lgb_scores.append(score)

        train_end = df.iloc[train_idx[-1]]['created'].strftime('%Y-%m')
        test_start = df.iloc[test_idx[0]]['created'].strftime('%Y-%m')
        test_end = df.iloc[test_idx[-1]]['created'].strftime('%Y-%m')
        print(f'  Fold {i+1}: Train ~{train_end} -> Test {test_start}~{test_end}: AUC={score:.4f}')

    print(f'  平均: {np.mean(lgb_scores):.4f} (+/- {np.std(lgb_scores):.4f})')

    print('\nRandom Forest (best params):')
    rf_scores = []
    for i, (train_idx, test_idx) in enumerate(tscv.split(X)):
        X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
        y_train, y_test = y[train_idx], y[test_idx]

        model = RandomForestClassifier(
            n_estimators=400, max_depth=10, min_samples_leaf=20, random_state=42, n_jobs=-1
        )
        model.fit(X_train, y_train)
        score = roc_auc_score(y_test, model.predict_proba(X_test)[:, 1])
        rf_scores.append(score)

        train_end = df.iloc[train_idx[-1]]['created'].strftime('%Y-%m')
        test_start = df.iloc[test_idx[0]]['created'].strftime('%Y-%m')
        test_end = df.iloc[test_idx[-1]]['created'].strftime('%Y-%m')
        print(f'  Fold {i+1}: Train ~{train_end} -> Test {test_start}~{test_end}: AUC={score:.4f}')

    print(f'  平均: {np.mean(rf_scores):.4f} (+/- {np.std(rf_scores):.4f})')

    print('\n' + '='*70)
    print('最終結果サマリー')
    print('='*70)
    print(f'LightGBM 時系列CV平均:     {np.mean(lgb_scores):.4f}')
    print(f'Random Forest 時系列CV平均: {np.mean(rf_scores):.4f}')

    # 最終的なホールドアウトテスト
    n = len(df)
    train_idx = int(n * 0.8)
    X_train, X_test = X.iloc[:train_idx], X.iloc[train_idx:]
    y_train, y_test = y[:train_idx], y[train_idx:]

    print('\n最終ホールドアウトテスト (80/20分割):')

    lgb_final = lgb.LGBMClassifier(
        n_estimators=200, max_depth=8, learning_rate=0.05, min_child_samples=10,
        subsample=0.8, colsample_bytree=0.8, random_state=42, verbose=-1
    )
    lgb_final.fit(X_train, y_train)
    lgb_test = roc_auc_score(y_test, lgb_final.predict_proba(X_test)[:, 1])
    print(f'  LightGBM: {lgb_test:.4f}')

    rf_final = RandomForestClassifier(
        n_estimators=400, max_depth=10, min_samples_leaf=20, random_state=42, n_jobs=-1
    )
    rf_final.fit(X_train, y_train)
    rf_test = roc_auc_score(y_test, rf_final.predict_proba(X_test)[:, 1])
    print(f'  Random Forest: {rf_test:.4f}')

    return lgb_scores, rf_scores


if __name__ == "__main__":
    lgb_scores, rf_scores = main()
