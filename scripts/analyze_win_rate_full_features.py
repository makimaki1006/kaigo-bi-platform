# -*- coding: utf-8 -*-
"""
成約率分析スクリプト（全特徴量版）

2025年4月以降のデータで、逆因果ではない全ての特徴量を使用。
様々なモデル・特徴量の組み合わせを試す。
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
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import cross_val_score
import warnings
warnings.filterwarnings('ignore')

from src.services.opportunity_service import OpportunityService


def main():
    print('='*70)
    print('成約予測モデル（2025年4月以降・全特徴量版）')
    print('='*70)
    print(f'実行日時: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')

    opp_service = OpportunityService()
    opp_service.authenticate()

    # 使える特徴量を全て取得
    soql = """
        SELECT Id, IsWon, CreatedDate,
               Account.NumberOfEmployees,
               Account.Population__c,
               Account.PopulationDensity__c,
               Account.Prefectures__c,
               Account.IndustryCategory__c,
               Account.ServiceType__c,
               Account.ServiceType2__c,
               Account.ServiceType3__c,
               Account.LegalPersonality__c,
               Account.Establish__c,
               Account.NumberOfFacilities__c,
               Account.CorporateNumber__c,
               Account.Industry,
               Account.Website,
               Account.AllHeadOffice__c,
               Account.IntroductionFlg_Company__c,
               Account.IntroductionFlg_Facility__c,
               Account.PresidentTitle__c,
               Account.PresidentName__c,
               Account.Hellowork_NumberOfEmployee_Office__c,
               Account.Hellowork_Industry__c,
               Account.Hellowork_RecuritmentType__c,
               Account.Hellowork_EmploymentType__c,
               Account.Hellowork_RecruitmentReasonCategory__c,
               Account.Hellowork_NumberOfRecruitment__c,
               Account.Paid_Media__c,
               Account.Paid_RecruitmentType__c,
               Account.Paid_EmploymentType__c,
               Account.Paid_Industry__c,
               Account.WonOpportunityies__c
        FROM Opportunity
        WHERE IsClosed = true AND CreatedDate >= 2025-04-01T00:00:00Z
        ORDER BY CreatedDate ASC
    """
    df = opp_service.bulk_query(soql, '2025年4月以降（全特徴量）')

    # 新規営業フィルタ
    df['is_won'] = df['IsWon'].map({'true': 1, 'false': 0, True: 1, False: 0})
    df['won_count'] = pd.to_numeric(df.get('Account.WonOpportunityies__c', 0), errors='coerce').fillna(0)
    df['past_won_count'] = df.apply(
        lambda row: row['won_count'] - 1 if row['is_won'] == 1 else row['won_count'], axis=1
    )
    df = df[df['past_won_count'] == 0].copy().reset_index(drop=True)
    df['created'] = pd.to_datetime(df['CreatedDate'])

    print(f'\nデータ件数: {len(df):,} 件')
    print(f'成約: {df["is_won"].sum():,} 件 ({df["is_won"].mean():.1%})')

    # 派生特徴量
    df['has_website'] = (df['Account.Website'].notna() & (df['Account.Website'] != '')).astype(int)
    df['has_corporate_number'] = (df['Account.CorporateNumber__c'].notna() & (df['Account.CorporateNumber__c'] != '')).astype(int)
    df['has_president'] = (df['Account.PresidentName__c'].notna() & (df['Account.PresidentName__c'] != '')).astype(int)

    # 特徴量設定
    feature_configs = {
        # 数値変数
        'Account.NumberOfEmployees': 'numeric',
        'Account.Population__c': 'numeric',
        'Account.PopulationDensity__c': 'numeric',
        'Account.Establish__c': 'numeric',
        'Account.NumberOfFacilities__c': 'numeric',
        'Account.Hellowork_NumberOfEmployee_Office__c': 'numeric',
        'Account.Hellowork_NumberOfRecruitment__c': 'numeric',

        # カテゴリ変数
        'Account.Prefectures__c': 'category',
        'Account.IndustryCategory__c': 'category',
        'Account.ServiceType__c': 'category',
        'Account.ServiceType2__c': 'category',
        'Account.ServiceType3__c': 'category',
        'Account.LegalPersonality__c': 'category',
        'Account.Industry': 'category',
        # PresidentTitle__c は空=成約率0%のデータリークのため除外
        'Account.Hellowork_Industry__c': 'category',
        'Account.Hellowork_RecuritmentType__c': 'category',
        'Account.Hellowork_EmploymentType__c': 'category',
        'Account.Hellowork_RecruitmentReasonCategory__c': 'category',
        'Account.Paid_Media__c': 'category',
        'Account.Paid_RecruitmentType__c': 'category',
        'Account.Paid_EmploymentType__c': 'category',
        'Account.Paid_Industry__c': 'category',

        # バイナリ変数
        'Account.AllHeadOffice__c': 'binary',
        # IntroductionFlg_Company__c, IntroductionFlg_Facility__c は逆因果（データリーク）のため除外
        # has_website, has_president は空=成約率0%のデータリークのため除外
        'has_corporate_number': 'binary',  # 法人番号は正常な差（5.2% vs 5.7%）
    }

    # 特徴量準備
    X_frames = []
    feature_names = []

    print('\n使用特徴量:')
    for col, dtype in feature_configs.items():
        if col in df.columns:
            non_null = df[col].notna().sum()
            coverage = non_null / len(df) * 100
            if coverage >= 3:
                if dtype == 'category':
                    le = LabelEncoder()
                    values = df[col].fillna('Unknown').astype(str)
                    X_frames.append(pd.Series(le.fit_transform(values), name=col))
                elif dtype == 'binary':
                    values = df[col].fillna(False)
                    if values.dtype == 'object':
                        values = values.map({'true': 1, 'false': 0, True: 1, False: 0}).fillna(0)
                    X_frames.append(pd.Series(values.astype(int).values, name=col))
                else:
                    values = pd.to_numeric(df[col], errors='coerce').fillna(0)
                    X_frames.append(pd.Series(values.values, name=col))
                feature_names.append(col)
                print(f'  ✅ {col}: {coverage:.1f}%')

    X = pd.concat(X_frames, axis=1)
    y = df['is_won'].values

    print(f'\n特徴量数: {len(feature_names)}')

    # 時系列分割
    n = len(df)
    train_idx = int(n * 0.8)
    X_train, X_test = X.iloc[:train_idx], X.iloc[train_idx:]
    y_train, y_test = y[:train_idx], y[train_idx:]

    train_end = df.iloc[train_idx-1]['created'].strftime('%Y-%m-%d')
    test_start = df.iloc[train_idx]['created'].strftime('%Y-%m-%d')

    print(f'\n時系列分割:')
    print(f'  訓練: ~{train_end} ({len(X_train):,}件, 成約率{y_train.mean():.1%})')
    print(f'  テスト: {test_start}~ ({len(X_test):,}件, 成約率{y_test.mean():.1%})')

    # 様々なモデルを試す
    print('\n' + '='*70)
    print('モデル比較（バイアスなし・様々な組み合わせ）')
    print('='*70)

    results = []

    # 1. Random Forest（デフォルト）
    rf1 = RandomForestClassifier(n_estimators=100, random_state=42, n_jobs=-1)
    rf1.fit(X_train, y_train)
    cv_rf1 = cross_val_score(rf1, X, y, cv=5, scoring='roc_auc').mean()
    auc_rf1 = roc_auc_score(y_test, rf1.predict_proba(X_test)[:, 1])
    results.append(('RF (default)', cv_rf1, auc_rf1, rf1))
    print(f'\n1. Random Forest (default):  CV={cv_rf1:.4f}, Test={auc_rf1:.4f}')

    # 2. Random Forest（チューニング）
    rf2 = RandomForestClassifier(n_estimators=200, max_depth=8, min_samples_leaf=30, random_state=42, n_jobs=-1)
    rf2.fit(X_train, y_train)
    cv_rf2 = cross_val_score(rf2, X, y, cv=5, scoring='roc_auc').mean()
    auc_rf2 = roc_auc_score(y_test, rf2.predict_proba(X_test)[:, 1])
    results.append(('RF (tuned)', cv_rf2, auc_rf2, rf2))
    print(f'2. Random Forest (tuned):    CV={cv_rf2:.4f}, Test={auc_rf2:.4f}')

    # 3. Gradient Boosting
    gb = GradientBoostingClassifier(n_estimators=100, max_depth=4, min_samples_leaf=30, random_state=42)
    gb.fit(X_train, y_train)
    cv_gb = cross_val_score(gb, X, y, cv=5, scoring='roc_auc').mean()
    auc_gb = roc_auc_score(y_test, gb.predict_proba(X_test)[:, 1])
    results.append(('Gradient Boosting', cv_gb, auc_gb, gb))
    print(f'3. Gradient Boosting:        CV={cv_gb:.4f}, Test={auc_gb:.4f}')

    # 4. Logistic Regression
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    X_scaled = scaler.fit_transform(X)

    lr = LogisticRegression(max_iter=1000, random_state=42)
    lr.fit(X_train_scaled, y_train)
    cv_lr = cross_val_score(lr, X_scaled, y, cv=5, scoring='roc_auc').mean()
    auc_lr = roc_auc_score(y_test, lr.predict_proba(X_test_scaled)[:, 1])
    results.append(('Logistic Regression', cv_lr, auc_lr, None))
    print(f'4. Logistic Regression:      CV={cv_lr:.4f}, Test={auc_lr:.4f}')

    # 特徴量サブセットの実験
    print('\n' + '='*70)
    print('特徴量サブセット実験')
    print('='*70)

    # 基本特徴量のみ（以前の11個）
    basic_features = [
        'Account.NumberOfEmployees', 'Account.Population__c', 'Account.PopulationDensity__c',
        'Account.Prefectures__c', 'Account.IndustryCategory__c', 'Account.ServiceType__c',
        'Account.ServiceType2__c', 'Account.LegalPersonality__c', 'Account.Establish__c',
        'Account.Hellowork_NumberOfEmployee_Office__c', 'Account.Hellowork_Industry__c'
    ]
    basic_idx = [i for i, f in enumerate(feature_names) if f in basic_features]
    X_basic = X.iloc[:, basic_idx]
    X_train_basic = X_basic.iloc[:train_idx]
    X_test_basic = X_basic.iloc[train_idx:]

    rf_basic = RandomForestClassifier(n_estimators=100, random_state=42, n_jobs=-1)
    rf_basic.fit(X_train_basic, y_train)
    cv_basic = cross_val_score(rf_basic, X_basic, y, cv=5, scoring='roc_auc').mean()
    auc_basic = roc_auc_score(y_test, rf_basic.predict_proba(X_test_basic)[:, 1])
    print(f'\n基本11特徴量:     CV={cv_basic:.4f}, Test={auc_basic:.4f}')

    # 数値特徴量のみ
    numeric_idx = [i for i, f in enumerate(feature_names) if feature_configs.get(f) == 'numeric']
    X_numeric = X.iloc[:, numeric_idx]
    X_train_numeric = X_numeric.iloc[:train_idx]
    X_test_numeric = X_numeric.iloc[train_idx:]

    rf_numeric = RandomForestClassifier(n_estimators=100, random_state=42, n_jobs=-1)
    rf_numeric.fit(X_train_numeric, y_train)
    cv_numeric = cross_val_score(rf_numeric, X_numeric, y, cv=5, scoring='roc_auc').mean()
    auc_numeric = roc_auc_score(y_test, rf_numeric.predict_proba(X_test_numeric)[:, 1])
    print(f'数値特徴量のみ:   CV={cv_numeric:.4f}, Test={auc_numeric:.4f}')

    # 重要特徴量TOP10のみ
    rf_full = RandomForestClassifier(n_estimators=100, random_state=42, n_jobs=-1)
    rf_full.fit(X, y)
    importance = pd.DataFrame({
        'feature': feature_names,
        'importance': rf_full.feature_importances_
    }).sort_values('importance', ascending=False)

    top10_features = importance.head(10)['feature'].tolist()
    top10_idx = [i for i, f in enumerate(feature_names) if f in top10_features]
    X_top10 = X.iloc[:, top10_idx]
    X_train_top10 = X_top10.iloc[:train_idx]
    X_test_top10 = X_top10.iloc[train_idx:]

    rf_top10 = RandomForestClassifier(n_estimators=100, random_state=42, n_jobs=-1)
    rf_top10.fit(X_train_top10, y_train)
    cv_top10 = cross_val_score(rf_top10, X_top10, y, cv=5, scoring='roc_auc').mean()
    auc_top10 = roc_auc_score(y_test, rf_top10.predict_proba(X_test_top10)[:, 1])
    print(f'重要TOP10のみ:    CV={cv_top10:.4f}, Test={auc_top10:.4f}')

    # 特徴量重要度
    print('\n' + '='*70)
    print('特徴量重要度 TOP20')
    print('='*70)

    for i, (_, row) in enumerate(importance.head(20).iterrows()):
        print(f'  {i+1:>2}. {row["feature"]:<45} {row["importance"]:.4f} ({row["importance"]*100:.1f}%)')

    # ベストモデルでの詳細分析
    best_model = max(results, key=lambda x: x[2])
    print('\n' + '='*70)
    print(f'ベストモデル: {best_model[0]}')
    print('='*70)
    print(f'  CV AUC: {best_model[1]:.4f}')
    print(f'  テストAUC: {best_model[2]:.4f}')

    # 保存
    output_dir = project_root / 'data' / 'output' / 'analysis'
    output_dir.mkdir(parents=True, exist_ok=True)
    importance.to_csv(output_dir / 'feature_importance_2025apr_full.csv', index=False, encoding='utf-8-sig')

    # 結果サマリー
    summary = pd.DataFrame(results, columns=['Model', 'CV_AUC', 'Test_AUC', 'model_obj'])
    summary[['Model', 'CV_AUC', 'Test_AUC']].to_csv(
        output_dir / 'model_comparison_2025apr.csv', index=False, encoding='utf-8-sig'
    )

    print(f'\n保存完了:')
    print(f'  - feature_importance_2025apr_full.csv')
    print(f'  - model_comparison_2025apr.csv')

    print('\n' + '='*70)
    print('分析完了')
    print('='*70)

    return df, importance, best_model


if __name__ == "__main__":
    df, importance, best_model = main()
