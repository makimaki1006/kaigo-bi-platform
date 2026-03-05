# -*- coding: utf-8 -*-
"""
月別 × 施設形態 × 法人格 MECEクロス集計

全セグメントの組み合わせを網羅的に集計し、
月ごとの成約率パターンを明確化する。
"""

import sys
import io
from pathlib import Path
from datetime import datetime

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['PYTHONIOENCODING'] = 'utf-8'

import pandas as pd
import numpy as np
from scipy import stats
import warnings
warnings.filterwarnings('ignore')

from src.services.opportunity_service import OpportunityService


class MECECrossAnalyzer:
    """月別×施設形態×法人格 MECEクロス集計"""

    def __init__(self):
        self.service = OpportunityService()
        self.output_dir = project_root / 'data' / 'output' / 'analysis'
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.report_dir = project_root / 'claudedocs'
        self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    def extract(self) -> pd.DataFrame:
        """必要フィールドを抽出"""
        self.service.authenticate()

        # フィールド存在確認
        url = f"{self.service.instance_url}/services/data/{self.service.api_version}/sobjects/Opportunity/describe"
        resp = self.service.session.get(url, headers=self.service._headers())
        resp.raise_for_status()
        opp_fields = {f['name'] for f in resp.json()['fields']}

        url_acc = f"{self.service.instance_url}/services/data/{self.service.api_version}/sobjects/Account/describe"
        resp_acc = self.service.session.get(url_acc, headers=self.service._headers())
        resp_acc.raise_for_status()
        acc_fields = {f['name'] for f in resp_acc.json()['fields']}

        # 基本フィールド
        fields = [
            'Id', 'CloseDate', 'IsWon', 'IsClosed', 'Amount',
            'Account.Name',
            'Account.WonOpportunityies__c',
        ]

        # 施設形態
        for f in ['FacilityType_Large__c', 'FacilityType_Middle__c', 'FacilityType_Small__c']:
            if f in opp_fields:
                fields.append(f)
                print(f"  ✅ {f}")

        # 法人格（Account側）
        if 'LegalPersonality__c' in acc_fields:
            fields.append('Account.LegalPersonality__c')
            print("  ✅ Account.LegalPersonality__c")
        else:
            print("  ❌ Account.LegalPersonality__c が見つかりません")

        # 事業形態
        if 'ServiceType__c' in acc_fields:
            fields.append('Account.ServiceType__c')
            print("  ✅ Account.ServiceType__c")

        field_list = ', '.join(fields)
        soql = f"SELECT {field_list} FROM Opportunity WHERE IsClosed = true"

        df = self.service.bulk_query(soql, 'MECEクロス集計用データ抽出')
        print(f"  取得件数: {len(df):,}")
        return df

    def prepare(self, df: pd.DataFrame) -> pd.DataFrame:
        """前処理"""
        df['is_won'] = df['IsWon'].map({'true': 1, 'false': 0, True: 1, False: 0})
        df['CloseDate'] = pd.to_datetime(df['CloseDate'], errors='coerce')
        df['month'] = df['CloseDate'].dt.month
        df['amount'] = pd.to_numeric(df.get('Amount', 0), errors='coerce').fillna(0)

        # 新規営業フィルタ
        df['won_count'] = pd.to_numeric(
            df.get('Account.WonOpportunityies__c', 0), errors='coerce'
        ).fillna(0)
        df['past_won_count'] = df.apply(
            lambda r: r['won_count'] - 1 if r['is_won'] == 1 else r['won_count'], axis=1
        )

        total = len(df)
        df = df[df['past_won_count'] == 0].copy().reset_index(drop=True)
        print(f"  新規営業フィルタ: {total:,} → {len(df):,}")

        # 法人格の正規化（少数カテゴリを「その他法人格」にまとめる）
        if 'Account.LegalPersonality__c' in df.columns:
            legal_counts = df['Account.LegalPersonality__c'].value_counts()
            major_legal = legal_counts[legal_counts >= 100].index.tolist()
            df['legal_group'] = df['Account.LegalPersonality__c'].apply(
                lambda x: x if x in major_legal else 'その他法人格' if pd.notna(x) and x != '' else '不明'
            )
            print(f"  主要法人格（100件以上）: {major_legal}")

        # 施設形態の正規化
        if 'FacilityType_Large__c' in df.columns:
            df['facility'] = df['FacilityType_Large__c'].fillna('不明')

        return df

    def cross_tabulate(self, df: pd.DataFrame):
        """MECEクロス集計"""

        facility_col = 'facility'
        legal_col = 'legal_group'
        results = []

        print("\n" + "=" * 100)
        print("MECE クロス集計: 月 × 施設形態（大分類） × 法人格")
        print("=" * 100)

        # 全組み合わせの集計
        facilities = sorted(df[facility_col].unique())
        legals = sorted(df[legal_col].unique())
        months = list(range(1, 13))

        # ========================================
        # 1. 施設形態×法人格 → 月別成約率ピボット
        # ========================================
        print("\n" + "=" * 100)
        print("1. 施設形態 × 法人格 別 月別成約率（全組み合わせ）")
        print("=" * 100)

        for fac in facilities:
            for leg in legals:
                df_seg = df[(df[facility_col] == fac) & (df[legal_col] == leg)]
                total = len(df_seg)
                if total < 10:
                    continue

                won = df_seg['is_won'].sum()
                annual_rate = df_seg['is_won'].mean()

                monthly_rates = {}
                monthly_counts = {}
                for m in months:
                    df_m = df_seg[df_seg['month'] == m]
                    monthly_counts[m] = len(df_m)
                    monthly_rates[m] = df_m['is_won'].mean() if len(df_m) >= 3 else None

                results.append({
                    'facility': fac,
                    'legal': leg,
                    'total': total,
                    'won': int(won),
                    'annual_rate': annual_rate,
                    **{f'm{m}_rate': monthly_rates[m] for m in months},
                    **{f'm{m}_count': monthly_counts[m] for m in months},
                })

        results_df = pd.DataFrame(results)

        if results_df.empty:
            print("  十分なデータがありません")
            return pd.DataFrame(), pd.DataFrame()

        # 年間成約率でソート
        results_df = results_df.sort_values('annual_rate', ascending=False)

        # 表示
        header = f"  {'施設形態':<18} {'法人格':<14} {'件数':>6} {'成約':>5} {'年間率':>6}"
        for m in months:
            header += f" {m:>4}月"
        print(header)
        print("  " + "-" * (60 + 6 * 12))

        for _, row in results_df.iterrows():
            line = f"  {str(row['facility'])[:17]:<18} {str(row['legal'])[:13]:<14} "
            line += f"{int(row['total']):>6} {int(row['won']):>5} {row['annual_rate']*100:>5.1f}%"

            for m in months:
                rate = row[f'm{m}_rate']
                count = row[f'm{m}_count']
                if rate is not None and count >= 3:
                    diff = rate - row['annual_rate']
                    if diff < -0.05:
                        line += f" {rate*100:>4.1f}▼"
                    elif diff > 0.05:
                        line += f" {rate*100:>4.1f}▲"
                    else:
                        line += f" {rate*100:>5.1f}"
                else:
                    line += f"   {'--':>3}"
            print(line)

        # ========================================
        # 2. 4月フォーカス: 施設形態×法人格の成約率ランキング
        # ========================================
        print("\n" + "=" * 100)
        print("2. 4月フォーカス: 施設形態 × 法人格 成約率ランキング")
        print("=" * 100)

        april_data = []
        for _, row in results_df.iterrows():
            if row['m4_rate'] is not None and row['m4_count'] >= 5:
                april_data.append({
                    'facility': row['facility'],
                    'legal': row['legal'],
                    'april_rate': row['m4_rate'],
                    'annual_rate': row['annual_rate'],
                    'diff': row['m4_rate'] - row['annual_rate'],
                    'april_count': int(row['m4_count']),
                    'total': int(row['total']),
                })

        april_df = pd.DataFrame(april_data)

        if not april_df.empty:
            # 攻めるべき
            attack = april_df[april_df['diff'] > 0].sort_values('diff', ascending=False)
            print("\n【4月に攻めるべき組み合わせ】")
            print(f"  {'施設形態':<18} {'法人格':<14} {'4月率':>6} {'年間率':>6} {'差分':>8} {'4月件数':>7} {'年間件数':>7}")
            print("  " + "-" * 80)
            for _, r in attack.iterrows():
                print(f"  {str(r['facility'])[:17]:<18} {str(r['legal'])[:13]:<14} "
                      f"{r['april_rate']*100:>5.1f}% {r['annual_rate']*100:>5.1f}% "
                      f"{r['diff']*100:>+7.1f}pt {int(r['april_count']):>7} {int(r['total']):>7}")

            # 避けるべき
            avoid = april_df[april_df['diff'] < 0].sort_values('diff', ascending=True)
            print("\n【4月に避けるべき組み合わせ】")
            print(f"  {'施設形態':<18} {'法人格':<14} {'4月率':>6} {'年間率':>6} {'差分':>8} {'4月件数':>7} {'年間件数':>7}")
            print("  " + "-" * 80)
            for _, r in avoid.iterrows():
                print(f"  {str(r['facility'])[:17]:<18} {str(r['legal'])[:13]:<14} "
                      f"{r['april_rate']*100:>5.1f}% {r['annual_rate']*100:>5.1f}% "
                      f"{r['diff']*100:>+7.1f}pt {int(r['april_count']):>7} {int(r['total']):>7}")

        # ========================================
        # 3. 月別ベスト/ワースト（施設形態×法人格）
        # ========================================
        print("\n" + "=" * 100)
        print("3. 各組み合わせの ベスト月 / ワースト月")
        print("=" * 100)

        print(f"\n  {'施設形態':<18} {'法人格':<14} {'年間率':>6} {'件数':>6} "
              f"{'ベスト月':>8} {'成約率':>6} {'ワースト月':>9} {'成約率':>6}")
        print("  " + "-" * 95)

        for _, row in results_df.iterrows():
            monthly = {m: row[f'm{m}_rate'] for m in months if row[f'm{m}_rate'] is not None and row[f'm{m}_count'] >= 3}
            if len(monthly) < 4:
                continue

            best_m = max(monthly, key=monthly.get)
            worst_m = min(monthly, key=monthly.get)

            print(f"  {str(row['facility'])[:17]:<18} {str(row['legal'])[:13]:<14} "
                  f"{row['annual_rate']*100:>5.1f}% {int(row['total']):>6} "
                  f"  {best_m:>2}月   {monthly[best_m]*100:>5.1f}% "
                  f"    {worst_m:>2}月   {monthly[worst_m]*100:>5.1f}%")

        # ========================================
        # 4. 全月×施設形態×法人格のヒートマップデータ
        # ========================================
        heatmap_rows = []
        for _, row in results_df.iterrows():
            for m in months:
                heatmap_rows.append({
                    'facility': row['facility'],
                    'legal': row['legal'],
                    'month': m,
                    'win_rate': row[f'm{m}_rate'],
                    'count': int(row[f'm{m}_count']),
                    'annual_rate': row['annual_rate'],
                    'annual_total': int(row['total']),
                })

        heatmap_df = pd.DataFrame(heatmap_rows)

        return results_df, april_df, heatmap_df

    def generate_report(self, df: pd.DataFrame, results_df: pd.DataFrame,
                        april_df: pd.DataFrame) -> str:
        """Markdownレポート生成"""
        report = []
        report.append("# MECEクロス集計: 月 × 施設形態 × 法人格 成約率分析")
        report.append(f"\n**分析日**: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        report.append(f"**対象**: 新規営業（past_won_count=0）の確定商談 {len(df):,} 件")
        report.append(f"**年間平均成約率**: {df['is_won'].mean()*100:.1f}%")

        # 全体月別
        report.append("\n## 全体月別成約率")
        report.append("\n| 月 | 商談数 | 成約数 | 成約率 |")
        report.append("|---|---|---|---|")
        annual_avg = df['is_won'].mean()
        for m in range(1, 13):
            dm = df[df['month'] == m]
            rate = dm['is_won'].mean() if len(dm) > 0 else 0
            diff = rate - annual_avg
            marker = " :red_circle:" if diff < -0.02 else " :green_circle:" if diff > 0.02 else ""
            report.append(f"| {m}月 | {len(dm):,} | {int(dm['is_won'].sum())} | {rate*100:.1f}%{marker} |")

        # クロス集計テーブル
        report.append("\n## 施設形態 × 法人格 × 月別成約率")

        if not results_df.empty:
            # 施設形態ごとにグループ化
            for fac in results_df['facility'].unique():
                fac_data = results_df[results_df['facility'] == fac]
                if len(fac_data) == 0:
                    continue

                report.append(f"\n### {fac}")
                header = "| 法人格 | 件数 | 年間 |"
                separator = "|---|---|---|"
                for m in range(1, 13):
                    header += f" {m}月 |"
                    separator += "---|"
                report.append(header)
                report.append(separator)

                for _, row in fac_data.iterrows():
                    line = f"| {row['legal']} | {int(row['total'])} | {row['annual_rate']*100:.1f}% |"
                    for m in range(1, 13):
                        rate = row[f'm{m}_rate']
                        if rate is not None:
                            line += f" {rate*100:.1f}% |"
                        else:
                            line += " -- |"
                    report.append(line)

        # 4月推奨
        if not april_df.empty:
            report.append("\n## 4月の攻め先推奨（施設形態×法人格）")

            attack = april_df[april_df['diff'] > 0].sort_values('diff', ascending=False)
            if not attack.empty:
                report.append("\n### 攻めるべき組み合わせ")
                report.append("\n| 施設形態 | 法人格 | 4月成約率 | 年間平均 | 差分 | 4月件数 |")
                report.append("|---|---|---|---|---|---|")
                for _, r in attack.iterrows():
                    report.append(f"| {r['facility']} | {r['legal']} | {r['april_rate']*100:.1f}% | "
                                 f"{r['annual_rate']*100:.1f}% | {r['diff']*100:+.1f}pt | {int(r['april_count'])} |")

            avoid = april_df[april_df['diff'] < 0].sort_values('diff', ascending=True)
            if not avoid.empty:
                report.append("\n### 避けるべき組み合わせ")
                report.append("\n| 施設形態 | 法人格 | 4月成約率 | 年間平均 | 差分 | 4月件数 |")
                report.append("|---|---|---|---|---|---|")
                for _, r in avoid.iterrows():
                    report.append(f"| {r['facility']} | {r['legal']} | {r['april_rate']*100:.1f}% | "
                                 f"{r['annual_rate']*100:.1f}% | {r['diff']*100:+.1f}pt | {int(r['april_count'])} |")

        report_text = "\n".join(report)
        report_path = self.report_dir / 'mece_cross_analysis_report.md'
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(report_text)
        print(f"\n  レポート保存: {report_path}")

        return report_text

    def run(self):
        """実行"""
        print("=" * 100)
        print("MECE クロス集計: 月 × 施設形態 × 法人格")
        print(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 100)

        # データ抽出
        df_raw = self.extract()

        # 前処理
        df = self.prepare(df_raw)

        # クロス集計
        results_df, april_df, heatmap_df = self.cross_tabulate(df)

        # CSV保存
        if not results_df.empty:
            path = self.output_dir / f'mece_cross_results_{self.timestamp}.csv'
            results_df.to_csv(path, index=False, encoding='utf-8-sig')
            print(f"\n  クロス集計結果保存: {path}")

        if not heatmap_df.empty:
            path = self.output_dir / f'mece_heatmap_{self.timestamp}.csv'
            heatmap_df.to_csv(path, index=False, encoding='utf-8-sig')
            print(f"  ヒートマップデータ保存: {path}")

        # レポート生成
        self.generate_report(df, results_df, april_df)

        print("\n" + "=" * 100)
        print("分析完了")
        print("=" * 100)

        return df, results_df, april_df


if __name__ == "__main__":
    analyzer = MECECrossAnalyzer()
    df, results_df, april_df = analyzer.run()
