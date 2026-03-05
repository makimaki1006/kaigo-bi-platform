# -*- coding: utf-8 -*-
"""
商談金額（Amount）予測分析スクリプト

商談金額を予測するための特徴量を特定する。
「高額商談になりやすい会社の特徴」を明らかにし、
商談前のターゲティングに活用する。
"""

import sys
import io
from pathlib import Path
from datetime import datetime

# Windows環境でのUTF-8出力対応
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
from scipy import stats
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import LabelEncoder
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import warnings
warnings.filterwarnings('ignore')

from src.services.opportunity_service import OpportunityService


class AmountPredictor:
    """商談金額予測分析クラス"""

    def __init__(self):
        self.opp_service = OpportunityService()
        self.output_dir = project_root / 'data' / 'output' / 'analysis'
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def authenticate(self):
        """Salesforce認証"""
        self.opp_service.authenticate()

    def get_available_fields(self, object_name: str) -> list:
        """利用可能なフィールド一覧を取得"""
        url = f"{self.opp_service.instance_url}/services/data/{self.opp_service.api_version}/sobjects/{object_name}/describe"
        response = self.opp_service.session.get(url, headers=self.opp_service._headers())
        response.raise_for_status()
        return [f['name'] for f in response.json()['fields']]

    def export_data(self) -> pd.DataFrame:
        """商談・取引先データを抽出"""
        print("\n[1/2] データを抽出中...")

        # Opportunityのフィールドを確認
        opp_fields = self.get_available_fields('Opportunity')
        account_fields = self.get_available_fields('Account')

        # 基本フィールド
        fields = [
            'Id', 'Name', 'AccountId',
            'StageName', 'Amount',
            'IsClosed', 'IsWon',
            'CreatedDate'
        ]

        # Account関連の全カスタムフィールドを網羅的に取得
        account_custom = [
            # 基本情報
            'NumberOfEmployees',
            'BillingState', 'BillingCity',
            # カスタムフィールド（存在するもの全て）
            'Population__c',
            'PopulationDensity__c',
            'Prefectures__c',
            'Industry2__c',
            'IndustryCategory__c',
            'NumberOfFacilities__c',
            'ServiceType__c',
            'ServiceType2__c',
            'ServiceType3__c',
            'CustomerSegment_Small__c',
            'CustomerSegment_Large__c',
            'Call_Sum__c',
            'Connect_Sum__c',
            'Appoint_Sum__c',
            'WonOpportunityies__c',
            'LostOpportunityies__c',
            'RoyalCustomer__c',
            'Hellowork_Industry__c',
            'Hellowork_NumberOfEmployee_Office__c',
            'Hellowork_Capital__c',
            'Establish__c',
            'AnnualRevenue__c',
            'EmployeeGrowthRate__c',
            'FacilityCapacity__c',
            'BedCount__c',
            'PatientCount__c',
            'CorporateNumber__c',
        ]

        for cf in account_custom:
            if cf in account_fields:
                fields.append(f'Account.{cf}')
                print(f"    ✅ Account.{cf}")

        # 重複を除去
        fields = list(dict.fromkeys(fields))

        # SOQL実行（Amountがあるレコードのみ）
        field_list = ', '.join(fields)
        soql = f"""
            SELECT {field_list}
            FROM Opportunity
            WHERE IsClosed = true
            AND Amount != null
            AND Amount > 0
        """

        df = self.opp_service.bulk_query(soql, "商談データ抽出（Amount予測用）")
        print(f"  取得件数: {len(df):,} 件")

        return df

    def prepare_features(self, df: pd.DataFrame) -> tuple:
        """特徴量を準備（全特徴量を網羅的に）"""
        print("\n[2/2] 特徴量エンジニアリング中...")

        # インデックスをリセット
        df = df.reset_index(drop=True)

        # 目的変数（Amount）
        y = pd.to_numeric(df['Amount'], errors='coerce').fillna(0).values

        # 全ての利用可能な特徴量を自動検出
        feature_configs = {}

        # Account系のカラムを自動検出
        for col in df.columns:
            if col.startswith('Account.') and col != 'Account.Id':
                # データ型を推定
                sample = df[col].dropna()
                if len(sample) == 0:
                    continue

                # 数値かカテゴリかを判定
                try:
                    numeric_vals = pd.to_numeric(sample, errors='coerce')
                    non_null_ratio = numeric_vals.notna().sum() / len(sample)
                    if non_null_ratio > 0.5:
                        feature_configs[col] = 'numeric'
                    else:
                        feature_configs[col] = 'category'
                except:
                    feature_configs[col] = 'category'

        # 利用可能な特徴量のみ使用（カバレッジ5%以上）
        available_features = {}
        for col, dtype in feature_configs.items():
            if col in df.columns:
                non_null = df[col].notna().sum()
                coverage = non_null / len(df) * 100
                if coverage >= 5:  # 5%以上のカバレッジ
                    available_features[col] = dtype
                    print(f"  ✅ {col}: {coverage:.1f}% ({dtype})")
                else:
                    print(f"  ⚠️ {col}: カバレッジ不足 ({coverage:.1f}%)")

        # 特徴量行列を作成
        X_frames = []
        feature_names = []
        label_encoders = {}

        for col, dtype in available_features.items():
            if dtype == 'category':
                le = LabelEncoder()
                values = df[col].fillna('Unknown').astype(str)
                encoded = le.fit_transform(values)
                X_frames.append(pd.Series(encoded, name=col))
                feature_names.append(col)
                label_encoders[col] = le
            else:
                values = pd.to_numeric(df[col], errors='coerce').fillna(0)
                X_frames.append(pd.Series(values.values, name=col))
                feature_names.append(col)

        X = pd.concat(X_frames, axis=1)

        print(f"\n  特徴量数: {len(feature_names)}")
        print(f"  サンプル数: {len(X)}")

        return X, y, feature_names, label_encoders

    def analyze_correlation(self, df: pd.DataFrame):
        """Amountとの相関分析"""
        print("\n" + "="*60)
        print("相関分析（Amount vs 各変数）")
        print("="*60)

        amount = pd.to_numeric(df['Amount'], errors='coerce')

        # 数値変数との相関
        numeric_cols = [col for col in df.columns if col.startswith('Account.')]

        print("\n【Amountとの相関係数（Spearman）】")

        correlations = []
        for col in numeric_cols:
            df_pair = pd.DataFrame({
                'amount': amount,
                'feature': pd.to_numeric(df[col], errors='coerce')
            }).dropna()

            if len(df_pair) < 100:
                continue

            corr, pval = stats.spearmanr(df_pair['amount'], df_pair['feature'])
            sig = "***" if pval < 0.001 else "**" if pval < 0.01 else "*" if pval < 0.05 else ""

            correlations.append({
                'feature': col,
                'correlation': corr,
                'pval': pval,
                'n': len(df_pair),
                'sig': sig
            })

        # 相関係数の絶対値でソート
        correlations = sorted(correlations, key=lambda x: abs(x['correlation']), reverse=True)

        for c in correlations[:20]:
            print(f"  {c['feature']:<45} r={c['correlation']:+.4f} (p={c['pval']:.4f}, n={c['n']:,}) {c['sig']}")

        return correlations

    def analyze_by_category(self, df: pd.DataFrame, category_col: str, label: str):
        """カテゴリ別の平均Amount分析"""
        print(f"\n【{label}別 平均Amount】")

        if category_col not in df.columns:
            print(f"  列 {category_col} が見つかりません")
            return

        df_analysis = df[[category_col, 'Amount']].copy()
        df_analysis['Amount'] = pd.to_numeric(df_analysis['Amount'], errors='coerce')
        df_analysis = df_analysis.dropna()

        # グループ別集計
        grouped = df_analysis.groupby(category_col).agg({
            'Amount': ['mean', 'median', 'count', 'std']
        }).reset_index()
        grouped.columns = [category_col, '平均Amount', '中央値', '件数', '標準偏差']
        grouped = grouped[grouped['件数'] >= 10]  # 10件以上
        grouped = grouped.sort_values('平均Amount', ascending=False)

        print(f"  {'カテゴリ':<25} {'平均Amount':>12} {'中央値':>12} {'件数':>8}")
        print("  " + "-"*60)

        for _, row in grouped.head(15).iterrows():
            cat = str(row[category_col])[:25]
            print(f"  {cat:<25} {row['平均Amount']:>12,.0f} {row['中央値']:>12,.0f} {int(row['件数']):>8}")

        return grouped

    def train_model(self, X: pd.DataFrame, y: np.ndarray, feature_names: list):
        """回帰モデルを訓練"""
        print("\n" + "="*60)
        print("Amount予測モデル構築（Random Forest Regressor）")
        print("="*60)

        # 訓練/テスト分割
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )

        print(f"\n訓練データ: {len(X_train):,} 件")
        print(f"テストデータ: {len(X_test):,} 件")
        print(f"Amount平均: {y.mean():,.0f} 円")
        print(f"Amount中央値: {np.median(y):,.0f} 円")

        # ランダムフォレスト回帰
        print("\n【Random Forest Regressor】")
        rf = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
        rf.fit(X_train, y_train)

        # 交差検証
        cv_scores = cross_val_score(rf, X, y, cv=5, scoring='r2')
        print(f"  交差検証 R²: {cv_scores.mean():.4f} ± {cv_scores.std():.4f}")

        # テストデータで評価
        y_pred = rf.predict(X_test)

        r2 = r2_score(y_test, y_pred)
        mae = mean_absolute_error(y_test, y_pred)
        rmse = np.sqrt(mean_squared_error(y_test, y_pred))

        print(f"  テストR²: {r2:.4f}")
        print(f"  MAE（平均絶対誤差）: {mae:,.0f} 円")
        print(f"  RMSE（二乗平均平方根誤差）: {rmse:,.0f} 円")

        # 特徴量重要度
        print("\n【特徴量重要度（上位20）】")
        importance = pd.DataFrame({
            'feature': feature_names,
            'importance': rf.feature_importances_
        }).sort_values('importance', ascending=False)

        for i, row in importance.head(20).iterrows():
            print(f"  {row['feature']:<45} {row['importance']:.4f} ({row['importance']*100:.1f}%)")

        return rf, importance

    def run_analysis(self):
        """分析を実行"""
        print("="*60)
        print("商談金額（Amount）予測分析")
        print("="*60)
        print(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("目的: 高額商談になりやすい会社の特徴を特定")

        # 認証
        self.authenticate()

        # データ抽出
        df = self.export_data()

        # Amount概要
        print("\n" + "="*60)
        print("Amount概要")
        print("="*60)
        amount = pd.to_numeric(df['Amount'], errors='coerce')
        print(f"  件数: {len(amount):,}")
        print(f"  平均: {amount.mean():,.0f} 円")
        print(f"  中央値: {amount.median():,.0f} 円")
        print(f"  最小: {amount.min():,.0f} 円")
        print(f"  最大: {amount.max():,.0f} 円")
        print(f"  標準偏差: {amount.std():,.0f} 円")

        # 相関分析
        correlations = self.analyze_correlation(df)

        # カテゴリ別分析
        if 'Account.ServiceType__c' in df.columns:
            self.analyze_by_category(df, 'Account.ServiceType__c', 'サービス形態')
        if 'Account.Prefectures__c' in df.columns:
            self.analyze_by_category(df, 'Account.Prefectures__c', '都道府県')
        if 'Account.IndustryCategory__c' in df.columns:
            self.analyze_by_category(df, 'Account.IndustryCategory__c', '業界カテゴリ')
        if 'Account.CustomerSegment_Large__c' in df.columns:
            self.analyze_by_category(df, 'Account.CustomerSegment_Large__c', '顧客セグメント（大）')

        # 特徴量準備
        X, y, feature_names, label_encoders = self.prepare_features(df)

        # モデル訓練
        if len(X) > 50:
            model, importance = self.train_model(X, y, feature_names)

            # 結果を保存
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            importance_file = self.output_dir / f'amount_feature_importance_{timestamp}.csv'
            importance.to_csv(importance_file, index=False, encoding='utf-8-sig')
            print(f"\n特徴量重要度保存: {importance_file}")

            # 相関分析結果も保存
            corr_df = pd.DataFrame(correlations)
            corr_file = self.output_dir / f'amount_correlation_{timestamp}.csv'
            corr_df.to_csv(corr_file, index=False, encoding='utf-8-sig')
            print(f"相関分析保存: {corr_file}")

        print("\n" + "="*60)
        print("分析完了")
        print("="*60)

        return df, importance


if __name__ == "__main__":
    predictor = AmountPredictor()
    df, importance = predictor.run_analysis()
