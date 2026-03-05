# -*- coding: utf-8 -*-
"""
成約率分析スクリプト（クリーン版）

商談前にわかる情報のみを使用して成約を予測する。
逆因果・結果指標を除外した純粋な予測モデル。

除外する特徴量:
- Amount: 営業の提案金額（営業判断が入っている）
- Call_Sum, Connect_Sum, Appoint_Sum: 結果指標（難攻不落度）
- CustomerSegment: 成約後に更新される
- Hearing_Authority, Hearing_ContactTitle: 商談時に判明
- Owner.Name: 担当割当前に予測したい
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
from scipy import stats
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import LabelEncoder
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score
import warnings
warnings.filterwarnings('ignore')

from src.services.opportunity_service import OpportunityService


class CleanWinRateAnalyzer:
    """クリーンな成約率分析クラス"""

    def __init__(self):
        self.opp_service = OpportunityService()
        self.output_dir = project_root / 'data' / 'output' / 'analysis'
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def authenticate(self):
        self.opp_service.authenticate()

    def get_available_fields(self, object_name: str) -> list:
        url = f"{self.opp_service.instance_url}/services/data/{self.opp_service.api_version}/sobjects/{object_name}/describe"
        response = self.opp_service.session.get(url, headers=self.opp_service._headers())
        response.raise_for_status()
        return [f['name'] for f in response.json()['fields']]

    def export_data(self) -> pd.DataFrame:
        """データを抽出"""
        print("\n[1/2] データを抽出中...")

        opp_fields = self.get_available_fields('Opportunity')
        account_fields = self.get_available_fields('Account')

        fields = [
            'Id', 'Name', 'AccountId',
            'StageName', 'IsClosed', 'IsWon',
            'CreatedDate'
        ]

        # Accountの事前情報（商談前にわかる情報のみ）
        account_pre_sales = [
            'NumberOfEmployees',
            'Population__c',
            'PopulationDensity__c',
            'Prefectures__c',
            'IndustryCategory__c',
            'ServiceType__c',
            'ServiceType2__c',
            'Hellowork_Industry__c',
            'Hellowork_NumberOfEmployee_Office__c',
            'Establish__c',
            'LegalPersonality__c',  # 法人格（株式会社、医療法人等）
            'WonOpportunityies__c',  # 新規営業フィルタ用
        ]

        for cf in account_pre_sales:
            if cf in account_fields:
                fields.append(f'Account.{cf}')
                print(f"    ✅ Account.{cf}")

        # Opportunityの事前情報
        opp_pre_sales = [
            'FacilityType_Large__c',
            'FacilityType_Middle__c',
            'FacilityType_Small__c',
        ]

        for cf in opp_pre_sales:
            if cf in opp_fields:
                fields.append(cf)
                print(f"    ✅ {cf}")

        fields = list(dict.fromkeys(fields))

        soql = f"""
            SELECT {', '.join(fields)}
            FROM Opportunity
            WHERE IsClosed = true
        """

        df = self.opp_service.bulk_query(soql, "商談データ抽出（クリーン版）")
        print(f"  取得件数: {len(df):,} 件")

        return df

    def prepare_features(self, df: pd.DataFrame) -> tuple:
        """特徴量を準備（事前情報のみ）"""
        print("\n[2/2] 特徴量エンジニアリング中...")

        df = df.reset_index(drop=True)

        # 目的変数
        df['is_won'] = df['IsWon'].map({'true': 1, 'false': 0, True: 1, False: 0})
        y = df['is_won'].values

        # 事前にわかる特徴量のみ
        feature_configs = {
            # 数値変数（事前情報）
            'Account.NumberOfEmployees': 'numeric',
            'Account.Population__c': 'numeric',
            'Account.PopulationDensity__c': 'numeric',
            'Account.Hellowork_NumberOfEmployee_Office__c': 'numeric',
            'Account.Establish__c': 'numeric',

            # カテゴリ変数（事前情報）
            'Account.Prefectures__c': 'category',
            'Account.IndustryCategory__c': 'category',
            'Account.ServiceType__c': 'category',
            'Account.ServiceType2__c': 'category',
            'Account.Hellowork_Industry__c': 'category',
            'Account.LegalPersonality__c': 'category',  # 法人格
            'FacilityType_Large__c': 'category',
            'FacilityType_Middle__c': 'category',
            'FacilityType_Small__c': 'category',
        }

        available_features = {}
        for col, dtype in feature_configs.items():
            if col in df.columns:
                non_null = df[col].notna().sum()
                coverage = non_null / len(df) * 100
                if coverage >= 5:
                    available_features[col] = dtype
                    print(f"  ✅ {col}: {coverage:.1f}% ({dtype})")
                else:
                    print(f"  ⚠️ {col}: カバレッジ不足 ({coverage:.1f}%)")

        X_frames = []
        feature_names = []

        for col, dtype in available_features.items():
            if dtype == 'category':
                le = LabelEncoder()
                values = df[col].fillna('Unknown').astype(str)
                encoded = le.fit_transform(values)
                X_frames.append(pd.Series(encoded, name=col))
                feature_names.append(col)
            else:
                values = pd.to_numeric(df[col], errors='coerce').fillna(0)
                X_frames.append(pd.Series(values.values, name=col))
                feature_names.append(col)

        X = pd.concat(X_frames, axis=1)

        print(f"\n  特徴量数: {len(feature_names)}")
        print(f"  サンプル数: {len(X)}")

        return X, y, feature_names, df

    def train_model(self, X: pd.DataFrame, y: np.ndarray, feature_names: list):
        """モデルを訓練"""
        print("\n" + "="*60)
        print("成約予測モデル（商談前情報のみ）")
        print("="*60)

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )

        print(f"\n訓練データ: {len(X_train):,} 件")
        print(f"テストデータ: {len(X_test):,} 件")
        print(f"成約率（全体）: {y.mean():.1%}")

        rf = RandomForestClassifier(n_estimators=100, random_state=42, n_jobs=-1)
        rf.fit(X_train, y_train)

        cv_scores = cross_val_score(rf, X, y, cv=5, scoring='roc_auc')
        print(f"\n【Random Forest】")
        print(f"  交差検証 AUC: {cv_scores.mean():.4f} ± {cv_scores.std():.4f}")

        y_pred = rf.predict(X_test)
        y_proba = rf.predict_proba(X_test)[:, 1]

        print(f"  テストAUC: {roc_auc_score(y_test, y_proba):.4f}")

        cm = confusion_matrix(y_test, y_pred)
        print(f"\n  混同行列:")
        print(f"              予測:失注  予測:成約")
        print(f"    実際:失注    {cm[0,0]:>5}    {cm[0,1]:>5}")
        print(f"    実際:成約    {cm[1,0]:>5}    {cm[1,1]:>5}")

        # 特徴量重要度
        print("\n【特徴量重要度】")
        importance = pd.DataFrame({
            'feature': feature_names,
            'importance': rf.feature_importances_
        }).sort_values('importance', ascending=False)

        for i, row in importance.iterrows():
            print(f"  {row['feature']:<45} {row['importance']:.4f} ({row['importance']*100:.1f}%)")

        return rf, importance

    def analyze_segments(self, df: pd.DataFrame, model, X: pd.DataFrame, feature_names: list):
        """セグメント別の予測成約率を分析"""
        print("\n" + "="*60)
        print("セグメント別 予測成約率")
        print("="*60)

        # 予測確率を追加
        df = df.copy()
        df['predicted_prob'] = model.predict_proba(X)[:, 1]
        df['actual_won'] = df['is_won']

        # 都道府県別
        if 'Account.Prefectures__c' in df.columns:
            print("\n【都道府県別】")
            grouped = df.groupby('Account.Prefectures__c').agg({
                'predicted_prob': 'mean',
                'actual_won': ['mean', 'count']
            }).reset_index()
            grouped.columns = ['都道府県', '予測成約率', '実績成約率', '件数']
            grouped = grouped[grouped['件数'] >= 50].sort_values('予測成約率', ascending=False)

            print(f"  {'都道府県':<10} {'予測':>8} {'実績':>8} {'件数':>8}")
            print("  " + "-"*40)
            for _, row in grouped.head(10).iterrows():
                print(f"  {str(row['都道府県']):<10} {row['予測成約率']:>7.1%} {row['実績成約率']:>7.1%} {int(row['件数']):>8}")

        # サービス形態別
        if 'Account.ServiceType__c' in df.columns:
            print("\n【サービス形態別】")
            grouped = df.groupby('Account.ServiceType__c').agg({
                'predicted_prob': 'mean',
                'actual_won': ['mean', 'count']
            }).reset_index()
            grouped.columns = ['サービス形態', '予測成約率', '実績成約率', '件数']
            grouped = grouped[grouped['件数'] >= 20].sort_values('予測成約率', ascending=False)

            print(f"  {'サービス形態':<20} {'予測':>8} {'実績':>8} {'件数':>8}")
            print("  " + "-"*50)
            for _, row in grouped.head(10).iterrows():
                print(f"  {str(row['サービス形態'])[:20]:<20} {row['予測成約率']:>7.1%} {row['実績成約率']:>7.1%} {int(row['件数']):>8}")

        # 法人格別
        if 'Account.LegalPersonality__c' in df.columns:
            print("\n【法人格別】")
            grouped = df.groupby('Account.LegalPersonality__c').agg({
                'predicted_prob': 'mean',
                'actual_won': ['mean', 'count']
            }).reset_index()
            grouped.columns = ['法人格', '予測成約率', '実績成約率', '件数']
            grouped = grouped[grouped['件数'] >= 50].sort_values('予測成約率', ascending=False)

            print(f"  {'法人格':<15} {'予測':>8} {'実績':>8} {'件数':>8}")
            print("  " + "-"*45)
            for _, row in grouped.head(10).iterrows():
                print(f"  {str(row['法人格'])[:15]:<15} {row['予測成約率']:>7.1%} {row['実績成約率']:>7.1%} {int(row['件数']):>8}")

        # 従業員規模別
        if 'Account.NumberOfEmployees' in df.columns:
            df['emp_segment'] = pd.cut(
                pd.to_numeric(df['Account.NumberOfEmployees'], errors='coerce'),
                bins=[0, 10, 50, 100, 500, float('inf')],
                labels=['1-10人', '11-50人', '51-100人', '101-500人', '500人以上']
            )
            print("\n【従業員規模別】")
            grouped = df.groupby('emp_segment').agg({
                'predicted_prob': 'mean',
                'actual_won': ['mean', 'count']
            }).reset_index()
            grouped.columns = ['従業員規模', '予測成約率', '実績成約率', '件数']
            grouped = grouped.dropna()

            print(f"  {'従業員規模':<12} {'予測':>8} {'実績':>8} {'件数':>8}")
            print("  " + "-"*40)
            for _, row in grouped.iterrows():
                print(f"  {str(row['従業員規模']):<12} {row['予測成約率']:>7.1%} {row['実績成約率']:>7.1%} {int(row['件数']):>8}")

        # 人口密度別
        if 'Account.PopulationDensity__c' in df.columns:
            pop_density = pd.to_numeric(df['Account.PopulationDensity__c'], errors='coerce')
            df['density_segment'] = pd.cut(
                pop_density,
                bins=[0, 500, 1000, 3000, 10000, float('inf')],
                labels=['~500', '500-1000', '1000-3000', '3000-10000', '10000~']
            )
            print("\n【人口密度別（人/km²）】")
            grouped = df.groupby('density_segment').agg({
                'predicted_prob': 'mean',
                'actual_won': ['mean', 'count']
            }).reset_index()
            grouped.columns = ['人口密度', '予測成約率', '実績成約率', '件数']
            grouped = grouped.dropna()

            print(f"  {'人口密度':<12} {'予測':>8} {'実績':>8} {'件数':>8}")
            print("  " + "-"*40)
            for _, row in grouped.iterrows():
                print(f"  {str(row['人口密度']):<12} {row['予測成約率']:>7.1%} {row['実績成約率']:>7.1%} {int(row['件数']):>8}")

        return df

    def run_analysis(self):
        """分析を実行"""
        print("="*60)
        print("成約率分析（商談前情報のみ・クリーン版）")
        print("="*60)
        print(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        self.authenticate()

        df = self.export_data()

        # 新規営業のみにフィルタ
        print("\n" + "="*60)
        print("新規営業フィルタリング")
        print("="*60)

        original_count = len(df)
        df['is_won'] = df['IsWon'].map({'true': 1, 'false': 0, True: 1, False: 0})
        df['won_count'] = pd.to_numeric(df.get('Account.WonOpportunityies__c', 0), errors='coerce').fillna(0)
        df['past_won_count'] = df.apply(
            lambda row: row['won_count'] - 1 if row['is_won'] == 1 else row['won_count'],
            axis=1
        )
        df = df[df['past_won_count'] == 0].copy()

        print(f"  フィルタ前: {original_count:,} 件")
        print(f"  フィルタ後: {len(df):,} 件（新規営業のみ）")

        # データ概要
        print("\n" + "="*60)
        print("データ概要")
        print("="*60)
        won_count = (df['is_won'] == 1).sum()
        lost_count = (df['is_won'] == 0).sum()
        print(f"総商談数: {len(df):,}")
        print(f"成約: {won_count:,} ({won_count/len(df)*100:.1f}%)")
        print(f"失注: {lost_count:,} ({lost_count/len(df)*100:.1f}%)")

        # 特徴量準備
        X, y, feature_names, df = self.prepare_features(df)

        # モデル訓練
        model, importance = self.train_model(X, y, feature_names)

        # セグメント分析
        df = self.analyze_segments(df, model, X, feature_names)

        # 結果保存
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        importance_file = self.output_dir / f'clean_feature_importance_{timestamp}.csv'
        importance.to_csv(importance_file, index=False, encoding='utf-8-sig')
        print(f"\n特徴量重要度保存: {importance_file}")

        print("\n" + "="*60)
        print("分析完了")
        print("="*60)

        return df, importance, model


if __name__ == "__main__":
    analyzer = CleanWinRateAnalyzer()
    df, importance, model = analyzer.run_analysis()
