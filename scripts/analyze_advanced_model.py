# -*- coding: utf-8 -*-
"""
成約予測モデル（交互作用 + XGBoost/LightGBM版）
"""

import sys
import io
from pathlib import Path
from datetime import datetime

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import cross_val_score, StratifiedKFold
import xgboost as xgb
import lightgbm as lgb
import warnings
warnings.filterwarnings('ignore')

from src.services.opportunity_service import OpportunityService


def main():
    print('='*70)
    print('成約予測モデル（交互作用 + XGBoost/LightGBM版）')
    print('='*70)
    print(f'実行日時: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')

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
    df = opp_service.bulk_query(soql, '交互作用+高度モデル分析')

    # 新規営業フィルタ
    df['is_won'] = df['IsWon'].map({'true': 1, 'false': 0, True: 1, False: 0})
    df['won_count'] = pd.to_numeric(df.get('Account.WonOpportunityies__c', 0), errors='coerce').fillna(0)
    df['past_won_count'] = df.apply(
        lambda row: row['won_count'] - 1 if row['is_won'] == 1 else row['won_count'], axis=1
    )
    df = df[df['past_won_count'] == 0].copy().reset_index(drop=True)

    print(f'\nデータ件数: {len(df):,} 件')
    print(f'成約: {df["is_won"].sum():,} 件 ({df["is_won"].mean():.1%})')

    # 基本特徴量の準備
    df['employees'] = pd.to_numeric(df['Account.NumberOfEmployees'], errors='coerce').fillna(0)
    df['population'] = pd.to_numeric(df['Account.Population__c'], errors='coerce').fillna(0)
    df['pop_density'] = pd.to_numeric(df['Account.PopulationDensity__c'], errors='coerce').fillna(0)
    df['establish'] = pd.to_numeric(df['Account.Establish__c'], errors='coerce').fillna(0)
    df['hw_employees'] = pd.to_numeric(df['Account.Hellowork_NumberOfEmployee_Office__c'], errors='coerce').fillna(0)

    # 決裁者関連の派生特徴量
    df['is_decision_maker'] = df['OpportunityType__c'].apply(lambda x: 1 if '決裁者' in str(x) else 0)
    df['is_representative'] = df['OpportunityType__c'].apply(lambda x: 1 if '代表者' in str(x) else 0)
    df['has_authority'] = df['Hearing_Authority__c'].apply(lambda x: 1 if str(x) == 'あり' else 0)

    # カテゴリ変数のエンコーディング
    cat_cols = ['Account.Prefectures__c', 'Account.IndustryCategory__c', 'Account.ServiceType__c',
                'Account.ServiceType2__c', 'Account.LegalPersonality__c', 'Account.Hellowork_Industry__c',
                'Hearing_ContactTitle__c', 'BusinessNegotiatorRole__c']

    for col in cat_cols:
        if col in df.columns:
            le = LabelEncoder()
            df[col + '_enc'] = le.fit_transform(df[col].fillna('Unknown').astype(str))

    # ========================================
    # 交互作用特徴量の作成
    # ========================================
    print('\n交互作用特徴量を作成中...')

    # 従業員数の対数変換
    df['log_employees'] = np.log1p(df['employees'])

    # 交互作用特徴量
    df['employees_x_decision'] = df['employees'] * df['is_decision_maker']
    df['employees_x_authority'] = df['employees'] * df['has_authority']
    df['log_emp_x_decision'] = df['log_employees'] * df['is_decision_maker']
    df['pop_density_x_decision'] = df['pop_density'] * df['is_decision_maker']
    df['representative_x_authority'] = df['is_representative'] * df['has_authority']
    df['decision_x_authority'] = df['is_decision_maker'] * df['has_authority']

    # 従業員数カテゴリ
    df['emp_category'] = pd.cut(df['employees'], bins=[0, 30, 100, 500, float('inf')],
                                labels=['small', 'medium', 'large', 'enterprise'])
    df['emp_category_enc'] = LabelEncoder().fit_transform(df['emp_category'].astype(str))

    # 従業員数カテゴリ×決裁者
    df['emp_cat_x_decision'] = df['emp_category_enc'] * df['is_decision_maker']

    # 人口密度カテゴリ
    df['density_high'] = (df['pop_density'] > df['pop_density'].median()).astype(int)
    df['density_x_decision'] = df['density_high'] * df['is_decision_maker']

    # 特徴量リスト
    base_features = [
        'employees', 'population', 'pop_density', 'establish', 'hw_employees',
        'Account.Prefectures__c_enc', 'Account.IndustryCategory__c_enc',
        'Account.ServiceType__c_enc', 'Account.ServiceType2__c_enc',
        'Account.LegalPersonality__c_enc', 'Account.Hellowork_Industry__c_enc',
        'is_decision_maker', 'is_representative', 'has_authority',
        'Hearing_ContactTitle__c_enc', 'BusinessNegotiatorRole__c_enc'
    ]

    interaction_features = [
        'log_employees', 'employees_x_decision', 'employees_x_authority',
        'log_emp_x_decision', 'pop_density_x_decision', 'representative_x_authority',
        'decision_x_authority', 'emp_category_enc', 'emp_cat_x_decision',
        'density_high', 'density_x_decision'
    ]

    all_features = base_features + interaction_features

    # 特徴量行列作成
    X = df[all_features].fillna(0)
    y = df['is_won'].values

    print(f'特徴量数: {len(all_features)} (基本{len(base_features)} + 交互作用{len(interaction_features)})')

    # 時系列分割
    n = len(df)
    train_idx = int(n * 0.8)
    X_train, X_test = X.iloc[:train_idx], X.iloc[train_idx:]
    y_train, y_test = y[:train_idx], y[train_idx:]

    # CV設定
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)

    print('\n' + '='*70)
    print('モデル比較')
    print('='*70)

    results = []

    # 1. Random Forest（ベースライン）
    print('\n1. Random Forest...')
    rf = RandomForestClassifier(n_estimators=200, max_depth=10, min_samples_leaf=20, random_state=42, n_jobs=-1)
    rf.fit(X_train, y_train)
    cv_rf = cross_val_score(rf, X, y, cv=cv, scoring='roc_auc').mean()
    auc_rf = roc_auc_score(y_test, rf.predict_proba(X_test)[:, 1])
    results.append(('Random Forest', cv_rf, auc_rf))
    print(f'   CV={cv_rf:.4f}, Test={auc_rf:.4f}')

    # 2. Gradient Boosting
    print('2. Gradient Boosting...')
    gb = GradientBoostingClassifier(n_estimators=200, max_depth=5, min_samples_leaf=20,
                                     learning_rate=0.05, random_state=42)
    gb.fit(X_train, y_train)
    cv_gb = cross_val_score(gb, X, y, cv=cv, scoring='roc_auc').mean()
    auc_gb = roc_auc_score(y_test, gb.predict_proba(X_test)[:, 1])
    results.append(('Gradient Boosting', cv_gb, auc_gb))
    print(f'   CV={cv_gb:.4f}, Test={auc_gb:.4f}')

    # 3. XGBoost
    print('3. XGBoost...')
    xgb_model = xgb.XGBClassifier(
        n_estimators=200, max_depth=5, learning_rate=0.05,
        min_child_weight=20, subsample=0.8, colsample_bytree=0.8,
        random_state=42, use_label_encoder=False, eval_metric='auc'
    )
    xgb_model.fit(X_train, y_train)
    cv_xgb = cross_val_score(xgb_model, X, y, cv=cv, scoring='roc_auc').mean()
    auc_xgb = roc_auc_score(y_test, xgb_model.predict_proba(X_test)[:, 1])
    results.append(('XGBoost', cv_xgb, auc_xgb))
    print(f'   CV={cv_xgb:.4f}, Test={auc_xgb:.4f}')

    # 4. LightGBM
    print('4. LightGBM...')
    lgb_model = lgb.LGBMClassifier(
        n_estimators=200, max_depth=5, learning_rate=0.05,
        min_child_samples=20, subsample=0.8, colsample_bytree=0.8,
        random_state=42, verbose=-1
    )
    lgb_model.fit(X_train, y_train)
    cv_lgb = cross_val_score(lgb_model, X, y, cv=cv, scoring='roc_auc').mean()
    auc_lgb = roc_auc_score(y_test, lgb_model.predict_proba(X_test)[:, 1])
    results.append(('LightGBM', cv_lgb, auc_lgb))
    print(f'   CV={cv_lgb:.4f}, Test={auc_lgb:.4f}')

    # 5. XGBoost（チューニング版）
    print('5. XGBoost (tuned)...')
    xgb_tuned = xgb.XGBClassifier(
        n_estimators=300, max_depth=4, learning_rate=0.03,
        min_child_weight=30, subsample=0.7, colsample_bytree=0.7,
        reg_alpha=0.1, reg_lambda=1.0,
        random_state=42, use_label_encoder=False, eval_metric='auc'
    )
    xgb_tuned.fit(X_train, y_train)
    cv_xgb2 = cross_val_score(xgb_tuned, X, y, cv=cv, scoring='roc_auc').mean()
    auc_xgb2 = roc_auc_score(y_test, xgb_tuned.predict_proba(X_test)[:, 1])
    results.append(('XGBoost (tuned)', cv_xgb2, auc_xgb2))
    print(f'   CV={cv_xgb2:.4f}, Test={auc_xgb2:.4f}')

    # 6. LightGBM（チューニング版）
    print('6. LightGBM (tuned)...')
    lgb_tuned = lgb.LGBMClassifier(
        n_estimators=300, max_depth=4, learning_rate=0.03,
        min_child_samples=30, subsample=0.7, colsample_bytree=0.7,
        reg_alpha=0.1, reg_lambda=1.0,
        random_state=42, verbose=-1
    )
    lgb_tuned.fit(X_train, y_train)
    cv_lgb2 = cross_val_score(lgb_tuned, X, y, cv=cv, scoring='roc_auc').mean()
    auc_lgb2 = roc_auc_score(y_test, lgb_tuned.predict_proba(X_test)[:, 1])
    results.append(('LightGBM (tuned)', cv_lgb2, auc_lgb2))
    print(f'   CV={cv_lgb2:.4f}, Test={auc_lgb2:.4f}')

    # 7. XGBoost（さらにチューニング）
    print('7. XGBoost (aggressive)...')
    xgb_agg = xgb.XGBClassifier(
        n_estimators=500, max_depth=3, learning_rate=0.02,
        min_child_weight=50, subsample=0.6, colsample_bytree=0.6,
        reg_alpha=0.5, reg_lambda=2.0, gamma=0.1,
        random_state=42, use_label_encoder=False, eval_metric='auc'
    )
    xgb_agg.fit(X_train, y_train)
    cv_xgb3 = cross_val_score(xgb_agg, X, y, cv=cv, scoring='roc_auc').mean()
    auc_xgb3 = roc_auc_score(y_test, xgb_agg.predict_proba(X_test)[:, 1])
    results.append(('XGBoost (aggressive)', cv_xgb3, auc_xgb3))
    print(f'   CV={cv_xgb3:.4f}, Test={auc_xgb3:.4f}')

    # 結果サマリー
    print('\n' + '='*70)
    print('結果サマリー')
    print('='*70)
    results_df = pd.DataFrame(results, columns=['Model', 'CV_AUC', 'Test_AUC'])
    results_df = results_df.sort_values('CV_AUC', ascending=False)
    print(results_df.to_string(index=False))

    best = results_df.iloc[0]
    print(f'\nベストモデル: {best["Model"]}')
    print(f'  CV AUC: {best["CV_AUC"]:.4f}')
    print(f'  Test AUC: {best["Test_AUC"]:.4f}')

    # 特徴量重要度（ベストモデル）
    print('\n' + '='*70)
    print('特徴量重要度 TOP15')
    print('='*70)

    # LightGBM tunedの重要度を使用
    importance = pd.DataFrame({
        'feature': all_features,
        'importance': lgb_tuned.feature_importances_
    }).sort_values('importance', ascending=False)

    for i, (_, row) in enumerate(importance.head(15).iterrows()):
        marker = '★' if row['feature'] in interaction_features else ' '
        print(f'  {i+1:>2}. {marker} {row["feature"]:<35} {row["importance"]:>6.0f}')

    print('\n★ = 交互作用特徴量')

    # 保存
    output_dir = project_root / 'data' / 'output' / 'analysis'
    output_dir.mkdir(parents=True, exist_ok=True)
    results_df.to_csv(output_dir / 'model_comparison_advanced.csv', index=False, encoding='utf-8-sig')
    importance.to_csv(output_dir / 'feature_importance_advanced.csv', index=False, encoding='utf-8-sig')

    print(f'\n保存完了:')
    print(f'  - model_comparison_advanced.csv')
    print(f'  - feature_importance_advanced.csv')

    return results_df, importance


if __name__ == "__main__":
    results_df, importance = main()
