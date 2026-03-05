# -*- coding: utf-8 -*-
"""
成約率分析スクリプト（教師あり学習）

商談（Opportunity）と取引先（Account）のデータを使用して、
成約/失注を予測するモデルを構築し、重要な特徴量を特定する。

分析軸:
- 業種・業態
- 都道府県・市町村
- 従業員数（企業規模）
- リードソース
- 営業担当者
- 商談種別（担当者商談/代表者商談/掲載商談）
- 人口・人口密度
- 有効求人倍率
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
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score
import warnings
warnings.filterwarnings('ignore')

from src.services.opportunity_service import OpportunityService


class WinRateAnalyzer:
    """成約率分析クラス"""

    def __init__(self):
        self.opp_service = OpportunityService()
        self.output_dir = project_root / 'data' / 'output' / 'analysis'
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # 外部データパス
        self.population_file = project_root / 'data' / 'output' / 'population' / 'municipality_population_density.csv'
        self.job_ratio_file = project_root / 'data' / 'job_openings_ratio' / 'job_ratio_all_20260120_165953.csv'

    def authenticate(self):
        """Salesforce認証"""
        self.opp_service.authenticate()

    def describe_object(self, object_name: str) -> dict:
        """オブジェクトのフィールド情報を取得"""
        url = f"{self.opp_service.instance_url}/services/data/{self.opp_service.api_version}/sobjects/{object_name}/describe"
        response = self.opp_service.session.get(url, headers=self.opp_service._headers())
        response.raise_for_status()
        return response.json()

    def get_available_fields(self, object_name: str) -> list:
        """利用可能なフィールド一覧を取得"""
        describe = self.describe_object(object_name)
        return [f['name'] for f in describe['fields']]

    def export_opportunities(self) -> pd.DataFrame:
        """商談データを抽出"""
        print("\n[1/4] 商談(Opportunity)データを抽出中...")

        # Opportunityのフィールドを確認
        opp_fields = self.get_available_fields('Opportunity')
        print(f"  利用可能フィールド数: {len(opp_fields)}")

        # 基本フィールド
        fields = [
            'Id', 'Name', 'AccountId',
            'StageName', 'CloseDate', 'Amount',
            'OwnerId', 'Owner.Name',
            'LeadSource',
            'CreatedDate', 'LastModifiedDate',
            'IsClosed', 'IsWon'
        ]

        # カスタムフィールドを追加（存在するもののみ）
        custom_fields = [
            'OpportunityType__c',  # 商談型
            'OpportunityCategory__c',  # 商談区分
            'FacilityType_Large__c',  # サービス形態（大分類）
            'FacilityType_Middle__c',  # サービス形態（中分類）
            'FacilityType_Small__c',  # サービス形態（小分類）
            'Hearing_ContactTitle__c',  # 担当者の役職
            'Hearing_Authority__c',  # 決裁権の有無
            'AppointRank__c',  # アポランク
            'ReferralCase__c',  # 紹介案件
            'DeepCultivationCase__c',  # 深掘案件
            'Appointer__c',  # アポ取得者
            'BussinessNegotiator__c',  # 商談実施者
            'LostReason_Large__c',  # 失注理由（大分類）
            'LostScene__c',  # 失注シーン
            'WonReason__c',  # 受注理由
            'WonScene__c',  # 受注シーン
        ]

        for cf in custom_fields:
            if cf in opp_fields:
                fields.append(cf)
                print(f"    ✅ カスタムフィールド追加: {cf}")
            else:
                print(f"    ❌ フィールド未検出: {cf}")

        # Account関連フィールド
        fields.extend([
            'Account.Name',
            'Account.Industry',
            'Account.NumberOfEmployees',
            'Account.BillingState',
            'Account.BillingCity',
        ])

        # Account側のカスタムフィールドを確認
        account_fields = self.get_available_fields('Account')
        account_custom = [
            'Population__c',  # 人口
            'PopulationDensity__c',  # 人口密度
            'Prefectures__c',  # 都道府県
            'Industry2__c',  # 業種(2)
            'IndustryCategory__c',  # 業界カテゴリー
            'NumberOfFacilities__c',  # 保有施設数
            'ServiceType__c',  # サービス形態
            'ServiceType2__c',  # サービス形態（障害福祉）
            'ServiceType3__c',  # サービス形態（医療）
            'CustomerSegment_Small__c',  # 顧客セグメント（小分類）
            'CustomerSegment_Large__c',  # 顧客セグメント（大分類）
            'Call_Sum__c',  # 架電数
            'Connect_Sum__c',  # 担当者接触数
            'Appoint_Sum__c',  # アポ獲得数
            'WonOpportunityies__c',  # 受注件数
            'LostOpportunityies__c',  # 失注件数
            'RoyalCustomer__c',  # ロイヤル顧客
            'Hellowork_Industry__c',  # ハローワーク産業分類
            'Hellowork_NumberOfEmployee_Office__c',  # 従業員数（事業所）
        ]

        for cf in account_custom:
            if cf in account_fields:
                fields.append(f'Account.{cf}')
                print(f"    ✅ Accountカスタムフィールド追加: Account.{cf}")

        # 重複を除去
        fields = list(dict.fromkeys(fields))

        # SOQL実行
        field_list = ', '.join(fields)
        soql = f"""
            SELECT {field_list}
            FROM Opportunity
            WHERE IsClosed = true
        """

        df = self.opp_service.bulk_query(soql, "商談データ抽出（成約・失注）")
        print(f"  取得件数: {len(df):,} 件")

        return df

    def export_accounts(self) -> pd.DataFrame:
        """取引先データを抽出"""
        print("\n[2/4] 取引先(Account)データを抽出中...")

        fields = [
            'Id', 'Name',
            'Industry', 'NumberOfEmployees',
            'BillingState', 'BillingCity', 'BillingPostalCode',
        ]

        # カスタムフィールドを確認して追加
        account_fields = self.get_available_fields('Account')
        custom_fields = [
            'Population__c',
            'PopulationDensity__c',
            'JobRatio__c',
            'BusinessType__c',
            'Prefecture__c',
            'City__c',
        ]

        for cf in custom_fields:
            if cf in account_fields:
                fields.append(cf)

        field_list = ', '.join(fields)
        soql = f"SELECT {field_list} FROM Account"

        df = self.opp_service.bulk_query(soql, "取引先データ抽出")
        print(f"  取得件数: {len(df):,} 件")

        return df

    def load_population_data(self) -> pd.DataFrame:
        """人口・人口密度データを読み込む"""
        print("\n[3/4] 人口・人口密度データを読み込み中...")

        if not self.population_file.exists():
            print(f"  ⚠️ 人口データファイルが見つかりません: {self.population_file}")
            return pd.DataFrame()

        df = pd.read_csv(self.population_file, encoding='utf-8-sig')
        print(f"  読み込み件数: {len(df):,} 件")

        # key列を作成（都道府県+市区町村）
        if 'key' not in df.columns:
            df['key'] = df['prefecture'] + df['city']

        return df

    def load_job_ratio_data(self) -> pd.DataFrame:
        """有効求人倍率データを読み込む"""
        print("\n[3/4] 有効求人倍率データを読み込み中...")

        if not self.job_ratio_file.exists():
            print(f"  ⚠️ 有効求人倍率ファイルが見つかりません: {self.job_ratio_file}")
            return pd.DataFrame()

        df = pd.read_csv(self.job_ratio_file, encoding='utf-8-sig')
        print(f"  読み込み件数: {len(df):,} 件")

        return df

    def merge_data(self, df_opp: pd.DataFrame, df_acc: pd.DataFrame,
                   df_pop: pd.DataFrame, df_ratio: pd.DataFrame) -> pd.DataFrame:
        """データを結合"""
        print("\n[4/4] データを結合中...")

        # 商談データを基準にする
        df = df_opp.copy()

        # IsWonを数値化（目的変数）
        df['is_won'] = df['IsWon'].map({'true': 1, 'false': 0, True: 1, False: 0})

        # 都道府県・市区町村を正規化
        state_col = 'Account.BillingState' if 'Account.BillingState' in df.columns else 'BillingState'
        city_col = 'Account.BillingCity' if 'Account.BillingCity' in df.columns else 'BillingCity'

        df['prefecture'] = df.get(state_col, '').fillna('')
        df['city'] = df.get(city_col, '').fillna('')

        # 人口データをマージ
        if not df_pop.empty:
            df['pop_key'] = df['prefecture'] + df['city']
            df_pop_slim = df_pop[['key', 'population', 'population_density_km2']].copy()
            df_pop_slim = df_pop_slim.rename(columns={'key': 'pop_key'})

            df = df.merge(df_pop_slim, on='pop_key', how='left')
            match_count = df['population'].notna().sum()
            print(f"  人口データマッチ: {match_count:,} / {len(df):,} 件 ({match_count/len(df)*100:.1f}%)")

        # 有効求人倍率をマージ（都道府県単位）
        if not df_ratio.empty:
            # 都道府県別の平均求人倍率を計算
            df_ratio_pref = df_ratio.groupby('prefecture')['ratio'].mean().reset_index()
            df_ratio_pref.columns = ['prefecture', 'job_ratio']

            df = df.merge(df_ratio_pref, on='prefecture', how='left')
            match_count = df['job_ratio'].notna().sum()
            print(f"  有効求人倍率マッチ: {match_count:,} / {len(df):,} 件 ({match_count/len(df)*100:.1f}%)")

        print(f"  結合後データ件数: {len(df):,} 件")

        return df

    def prepare_features(self, df: pd.DataFrame, new_business_only: bool = False) -> tuple:
        """特徴量を準備"""
        print("\n特徴量エンジニアリング中...")

        # インデックスをリセット（フィルタリング後のギャップを解消）
        df = df.reset_index(drop=True)

        # 目的変数
        y = df['is_won'].values

        # 特徴量候補
        feature_configs = {
            # カテゴリカル変数（Opportunity）
            'Account.Industry': 'category',
            'LeadSource': 'category',
            'Owner.Name': 'category',
            'prefecture': 'category',
            'OpportunityType__c': 'category',  # 商談型
            'OpportunityCategory__c': 'category',  # 商談区分
            'FacilityType_Large__c': 'category',  # サービス形態（大分類）
            'FacilityType_Middle__c': 'category',  # サービス形態（中分類）
            'FacilityType_Small__c': 'category',  # サービス形態（小分類）
            'Hearing_ContactTitle__c': 'category',  # 担当者の役職
            'Hearing_Authority__c': 'category',  # 決裁権の有無
            'AppointRank__c': 'category',  # アポランク
            'ReferralCase__c': 'category',  # 紹介案件
            'DeepCultivationCase__c': 'category',  # 深掘案件

            # カテゴリカル変数（Account）
            'Account.Prefectures__c': 'category',  # 都道府県
            'Account.Industry2__c': 'category',  # 業種(2)
            'Account.IndustryCategory__c': 'category',  # 業界カテゴリー
            'Account.ServiceType__c': 'category',  # サービス形態
            'Account.ServiceType2__c': 'category',  # サービス形態（障害福祉）
            'Account.CustomerSegment_Small__c': 'category',  # 顧客セグメント（小分類）
            'Account.CustomerSegment_Large__c': 'category',  # 顧客セグメント（大分類）
            'Account.RoyalCustomer__c': 'category',  # ロイヤル顧客
            'Account.Hellowork_Industry__c': 'category',  # ハローワーク産業分類

            # 数値変数
            'Account.NumberOfEmployees': 'numeric',
            # 'Amount': 'numeric',  # 除外：提案金額は営業判断が入っており逆因果
            'Account.Population__c': 'numeric',  # 人口（Account直接）
            'Account.PopulationDensity__c': 'numeric',  # 人口密度（Account直接）
            'Account.NumberOfFacilities__c': 'numeric',  # 保有施設数
            'Account.Call_Sum__c': 'numeric',  # 架電数
            'Account.Connect_Sum__c': 'numeric',  # 担当者接触数
            'Account.Appoint_Sum__c': 'numeric',  # アポ獲得数
            'Account.Hellowork_NumberOfEmployee_Office__c': 'numeric',  # 従業員数（事業所）
            'population': 'numeric',  # 外部マージ人口
            'population_density_km2': 'numeric',  # 外部マージ人口密度
            'job_ratio': 'numeric',  # 有効求人倍率
        }

        # 新規営業のみの場合、過去受注・失注件数と顧客セグメントは除外
        # （顧客セグメントは成約後に更新されるフィールドなのでデータリーケージになる）
        if new_business_only:
            # 顧客セグメント関連を除外
            feature_configs.pop('Account.CustomerSegment_Small__c', None)
            feature_configs.pop('Account.CustomerSegment_Large__c', None)
            feature_configs.pop('Account.RoyalCustomer__c', None)
        else:
            feature_configs['Account.WonOpportunityies__c'] = 'numeric'  # 受注件数（過去）
            feature_configs['Account.LostOpportunityies__c'] = 'numeric'  # 失注件数（過去）

        # 利用可能な特徴量のみ使用
        available_features = {}
        for col, dtype in feature_configs.items():
            if col in df.columns:
                non_null = df[col].notna().sum()
                coverage = non_null / len(df) * 100
                if coverage > 10:  # 10%以上のデータがある場合のみ使用
                    available_features[col] = dtype
                    print(f"  ✅ {col}: {coverage:.1f}% ({dtype})")
                else:
                    print(f"  ⚠️ {col}: カバレッジ不足 ({coverage:.1f}%)")
            else:
                print(f"  ❌ {col}: 列なし")

        # 特徴量行列を作成
        X_frames = []
        feature_names = []

        for col, dtype in available_features.items():
            if dtype == 'category':
                # ラベルエンコーディング
                le = LabelEncoder()
                values = df[col].fillna('Unknown').astype(str)
                encoded = le.fit_transform(values)
                X_frames.append(pd.Series(encoded, name=col))
                feature_names.append(col)
            else:
                # 数値変数
                values = pd.to_numeric(df[col], errors='coerce').fillna(0)
                X_frames.append(pd.Series(values, name=col))
                feature_names.append(col)

        X = pd.concat(X_frames, axis=1)

        print(f"\n  特徴量数: {len(feature_names)}")
        print(f"  サンプル数: {len(X)}")

        return X, y, feature_names

    def analyze_correlation(self, df: pd.DataFrame):
        """相関分析"""
        print("\n" + "="*60)
        print("相関分析")
        print("="*60)

        # 成約フラグと数値変数の相関
        numeric_cols = [
            'Amount', 'Account.NumberOfEmployees',
            'Account.Population__c', 'Account.PopulationDensity__c',
            'Account.NumberOfFacilities__c',
            'Account.Call_Sum__c', 'Account.Connect_Sum__c', 'Account.Appoint_Sum__c',
            'Account.WonOpportunityies__c', 'Account.LostOpportunityies__c',
            'Account.Hellowork_NumberOfEmployee_Office__c',
            'population', 'population_density_km2', 'job_ratio'
        ]
        available_cols = [c for c in numeric_cols if c in df.columns]

        if not available_cols:
            print("  数値変数が見つかりません")
            return

        print("\n【成約との相関係数（Spearman）】")

        for col in available_cols:
            # 個別に相関を計算（pairwise deletion）
            df_pair = df[['is_won', col]].copy()
            df_pair[col] = pd.to_numeric(df_pair[col], errors='coerce')
            df_pair = df_pair.dropna()

            if len(df_pair) < 10:
                print(f"  {col}: データ不足（{len(df_pair)}件）")
                continue

            corr, pval = stats.spearmanr(df_pair['is_won'], df_pair[col])
            sig = "***" if pval < 0.001 else "**" if pval < 0.01 else "*" if pval < 0.05 else ""
            print(f"  {col}: r={corr:.4f} (p={pval:.4f}, n={len(df_pair):,}) {sig}")

    def analyze_by_category(self, df: pd.DataFrame, category_col: str, label: str):
        """カテゴリ別成約率分析"""
        print(f"\n【{label}別 成約率】")

        if category_col not in df.columns:
            print(f"  列 {category_col} が見つかりません")
            return

        # グループ別集計
        grouped = df.groupby(category_col).agg({
            'is_won': ['sum', 'count', 'mean']
        }).reset_index()
        grouped.columns = [category_col, '成約数', '総数', '成約率']
        grouped = grouped.sort_values('成約率', ascending=False)

        # サンプル数が5件以上のもののみ表示
        grouped_filtered = grouped[grouped['総数'] >= 5]

        print(f"  {'カテゴリ':<20} {'成約数':>8} {'総数':>8} {'成約率':>10}")
        print("  " + "-"*50)

        for _, row in grouped_filtered.head(15).iterrows():
            cat = str(row[category_col])[:20]
            print(f"  {cat:<20} {int(row['成約数']):>8} {int(row['総数']):>8} {row['成約率']:>9.1%}")

        # カイ二乗検定
        if len(grouped_filtered) >= 2:
            contingency = pd.crosstab(df[category_col], df['is_won'])
            if contingency.shape[0] >= 2 and contingency.shape[1] >= 2:
                chi2, pval, dof, expected = stats.chi2_contingency(contingency)
                sig = "***" if pval < 0.001 else "**" if pval < 0.01 else "*" if pval < 0.05 else ""
                print(f"\n  カイ二乗検定: χ²={chi2:.2f}, p={pval:.4f} {sig}")

    def train_model(self, X: pd.DataFrame, y: np.ndarray, feature_names: list):
        """機械学習モデルを訓練"""
        print("\n" + "="*60)
        print("教師あり学習モデル構築")
        print("="*60)

        # 訓練/テスト分割
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )

        print(f"\n訓練データ: {len(X_train):,} 件")
        print(f"テストデータ: {len(X_test):,} 件")
        print(f"成約率（全体）: {y.mean():.1%}")

        # ランダムフォレスト
        print("\n【Random Forest】")
        rf = RandomForestClassifier(n_estimators=100, random_state=42, n_jobs=-1)
        rf.fit(X_train, y_train)

        # 交差検証
        cv_scores = cross_val_score(rf, X, y, cv=5, scoring='roc_auc')
        print(f"  交差検証 AUC: {cv_scores.mean():.4f} ± {cv_scores.std():.4f}")

        # テストデータで評価
        y_pred = rf.predict(X_test)
        y_proba = rf.predict_proba(X_test)[:, 1]

        print(f"  テストAUC: {roc_auc_score(y_test, y_proba):.4f}")

        # 混同行列
        cm = confusion_matrix(y_test, y_pred)
        print(f"\n  混同行列:")
        print(f"              予測:失注  予測:成約")
        print(f"    実際:失注    {cm[0,0]:>5}    {cm[0,1]:>5}")
        print(f"    実際:成約    {cm[1,0]:>5}    {cm[1,1]:>5}")

        # 特徴量重要度
        print("\n【特徴量重要度（上位10）】")
        importance = pd.DataFrame({
            'feature': feature_names,
            'importance': rf.feature_importances_
        }).sort_values('importance', ascending=False)

        for i, row in importance.head(10).iterrows():
            print(f"  {row['feature']:<30} {row['importance']:.4f}")

        return rf, importance

    def run_analysis(self, new_business_only: bool = False):
        """分析を実行

        Args:
            new_business_only: True の場合、新規営業のみ（過去受注なしの取引先）を対象
        """
        print("="*60)
        if new_business_only:
            print("成約率分析（教師あり学習）- 新規営業のみ")
        else:
            print("成約率分析（教師あり学習）")
        print("="*60)
        print(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # 認証
        self.authenticate()

        # データ抽出
        df_opp = self.export_opportunities()
        df_acc = self.export_accounts()
        df_pop = self.load_population_data()
        df_ratio = self.load_job_ratio_data()

        # データ結合
        df = self.merge_data(df_opp, df_acc, df_pop, df_ratio)

        # 新規営業のみにフィルタリング
        if new_business_only:
            print("\n" + "="*60)
            print("新規営業フィルタリング")
            print("="*60)

            original_count = len(df)

            # 過去受注がない取引先のみ（WonOpportunityies__c == 0 or null）
            # ただし、今回の商談自体が成約の場合は1件目としてカウントされるので、
            # 成約の場合は WonOpportunityies__c <= 1、失注の場合は WonOpportunityies__c == 0
            df['won_count'] = pd.to_numeric(df.get('Account.WonOpportunityies__c', 0), errors='coerce').fillna(0)

            # 今回の商談を除いた過去の受注件数を計算
            # IsWon=true の場合、WonOpportunityies__c には今回の商談が含まれているので -1
            df['past_won_count'] = df.apply(
                lambda row: row['won_count'] - 1 if row['is_won'] == 1 else row['won_count'],
                axis=1
            )

            # 過去受注0件のみ（新規営業）
            df = df[df['past_won_count'] == 0].copy()

            filtered_count = len(df)
            print(f"  フィルタ前: {original_count:,} 件")
            print(f"  フィルタ後: {filtered_count:,} 件（新規営業のみ）")
            print(f"  除外: {original_count - filtered_count:,} 件（既存顧客への商談）")

        # 成約/失注の内訳
        print("\n" + "="*60)
        print("データ概要")
        print("="*60)
        print(f"総商談数: {len(df):,}")
        won_count = (df['is_won'] == 1).sum()
        lost_count = (df['is_won'] == 0).sum()
        print(f"成約: {won_count:,} ({won_count/len(df)*100:.1f}%)")
        print(f"失注: {lost_count:,} ({lost_count/len(df)*100:.1f}%)")

        # データ保存
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = self.output_dir / f'win_rate_analysis_data_{timestamp}.csv'
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"\nデータ保存: {output_file}")

        # 相関分析
        self.analyze_correlation(df)

        # カテゴリ別分析
        if 'Account.Industry' in df.columns:
            self.analyze_by_category(df, 'Account.Industry', '業種')
        if 'LeadSource' in df.columns:
            self.analyze_by_category(df, 'LeadSource', 'リードソース')
        if 'Owner.Name' in df.columns:
            self.analyze_by_category(df, 'Owner.Name', '営業担当者')
        if 'prefecture' in df.columns:
            self.analyze_by_category(df, 'prefecture', '都道府県')
        if 'OpportunityType__c' in df.columns:
            self.analyze_by_category(df, 'OpportunityType__c', '商談型')
        if 'FacilityType_Large__c' in df.columns:
            self.analyze_by_category(df, 'FacilityType_Large__c', 'サービス形態（大分類）')
        if 'FacilityType_Middle__c' in df.columns:
            self.analyze_by_category(df, 'FacilityType_Middle__c', 'サービス形態（中分類）')
        if 'Hearing_ContactTitle__c' in df.columns:
            self.analyze_by_category(df, 'Hearing_ContactTitle__c', '担当者の役職')
        if 'Hearing_Authority__c' in df.columns:
            self.analyze_by_category(df, 'Hearing_Authority__c', '決裁権の有無')
        if 'AppointRank__c' in df.columns:
            self.analyze_by_category(df, 'AppointRank__c', 'アポランク')
        if 'ReferralCase__c' in df.columns:
            self.analyze_by_category(df, 'ReferralCase__c', '紹介案件')
        if 'DeepCultivationCase__c' in df.columns:
            self.analyze_by_category(df, 'DeepCultivationCase__c', '深掘案件')

        # Account系カテゴリ分析
        if 'Account.Prefectures__c' in df.columns:
            self.analyze_by_category(df, 'Account.Prefectures__c', '都道府県（Account）')
        if 'Account.Industry2__c' in df.columns:
            self.analyze_by_category(df, 'Account.Industry2__c', '業種（Account）')
        if 'Account.ServiceType__c' in df.columns:
            self.analyze_by_category(df, 'Account.ServiceType__c', 'サービス形態（Account）')
        if 'Account.CustomerSegment_Large__c' in df.columns:
            self.analyze_by_category(df, 'Account.CustomerSegment_Large__c', '顧客セグメント（大分類）')
        if 'Account.CustomerSegment_Small__c' in df.columns:
            self.analyze_by_category(df, 'Account.CustomerSegment_Small__c', '顧客セグメント（小分類）')
        if 'Account.RoyalCustomer__c' in df.columns:
            self.analyze_by_category(df, 'Account.RoyalCustomer__c', 'ロイヤル顧客')

        # 特徴量準備
        X, y, feature_names = self.prepare_features(df, new_business_only=new_business_only)

        # モデル訓練
        if len(X) > 50:  # 十分なサンプル数がある場合のみ
            model, importance = self.train_model(X, y, feature_names)

            # 重要度を保存
            importance_file = self.output_dir / f'feature_importance_{timestamp}.csv'
            importance.to_csv(importance_file, index=False, encoding='utf-8-sig')
            print(f"\n特徴量重要度保存: {importance_file}")
        else:
            print(f"\n⚠️ サンプル数不足のためモデル訓練をスキップ ({len(X)} 件)")

        print("\n" + "="*60)
        print("分析完了")
        print("="*60)

        return df


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="成約率分析（教師あり学習）")
    parser.add_argument("--new-business", action="store_true",
                       help="新規営業のみを対象（過去受注なしの取引先）")
    args = parser.parse_args()

    analyzer = WinRateAnalyzer()
    df = analyzer.run_analysis(new_business_only=args.new_business)
