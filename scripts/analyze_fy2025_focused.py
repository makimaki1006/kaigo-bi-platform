# -*- coding: utf-8 -*-
"""
FY2025期間限定分析: 2025年4月～2026年1月

全時系列データとの対比として、直近10ヶ月（2025年4月～2026年1月）のみに
絞ったMECEクロス集計＋受注率フォーカス分析を実施する。
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
import warnings
warnings.filterwarnings('ignore')

from src.services.opportunity_service import OpportunityService


# 分析対象期間
DATE_FROM = '2025-04-01'
DATE_TO = '2026-01-31'
PERIOD_LABEL = '2025年4月～2026年1月'


class FY2025Analyzer:
    """FY2025期間限定 MECEクロス集計＋受注率フォーカス"""

    def __init__(self):
        self.service = OpportunityService()
        self.output_dir = project_root / 'data' / 'output' / 'analysis'
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.report_dir = project_root / 'claudedocs'
        self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    def extract(self) -> pd.DataFrame:
        """期間限定でデータ抽出"""
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

        fields = [
            'Id', 'CloseDate', 'IsWon', 'IsClosed', 'Amount',
            'OpportunityCategory__c',
            'Account.Name',
            'Account.WonOpportunityies__c',
        ]

        for f in ['FacilityType_Large__c', 'FacilityType_Middle__c', 'FacilityType_Small__c']:
            if f in opp_fields:
                fields.append(f)
                print(f"  ✅ {f}")

        if 'LegalPersonality__c' in acc_fields:
            fields.append('Account.LegalPersonality__c')
            print("  ✅ Account.LegalPersonality__c")

        if 'ServiceType__c' in acc_fields:
            fields.append('Account.ServiceType__c')
            print("  ✅ Account.ServiceType__c")

        field_list = ', '.join(fields)
        soql = (f"SELECT {field_list} FROM Opportunity "
                f"WHERE IsClosed = true "
                f"AND CloseDate >= {DATE_FROM} "
                f"AND CloseDate <= {DATE_TO}")

        print(f"\n  対象期間: {PERIOD_LABEL}")
        df = self.service.bulk_query(soql, f'FY2025期間限定データ抽出（{PERIOD_LABEL}）')
        print(f"  取得件数: {len(df):,}")
        return df

    def prepare(self, df: pd.DataFrame) -> pd.DataFrame:
        """前処理"""
        df['is_won'] = df['IsWon'].map({'true': 1, 'false': 0, True: 1, False: 0})
        df['CloseDate'] = pd.to_datetime(df['CloseDate'], errors='coerce')
        df['month'] = df['CloseDate'].dt.month
        df['amount'] = pd.to_numeric(df.get('Amount', 0), errors='coerce').fillna(0)

        # 新規営業フィルタ（past_won_count == 0）
        df['won_count'] = pd.to_numeric(
            df.get('Account.WonOpportunityies__c', 0), errors='coerce'
        ).fillna(0)
        df['past_won_count'] = df.apply(
            lambda r: r['won_count'] - 1 if r['is_won'] == 1 else r['won_count'], axis=1
        )

        total = len(df)
        df = df[df['past_won_count'] == 0].copy().reset_index(drop=True)
        print(f"  新規営業フィルタ（past_won_count=0）: {total:,} → {len(df):,}")

        # 初回商談フィルタ（再商談を除外）
        before_cat = len(df)
        df = df[df['OpportunityCategory__c'] == '初回商談'].copy().reset_index(drop=True)
        print(f"  初回商談フィルタ（OpportunityCategory__c='初回商談'）: {before_cat:,} → {len(df):,}")

        # 法人格の正規化
        if 'Account.LegalPersonality__c' in df.columns:
            legal_counts = df['Account.LegalPersonality__c'].value_counts()
            major_legal = legal_counts[legal_counts >= 30].index.tolist()
            df['legal_group'] = df['Account.LegalPersonality__c'].apply(
                lambda x: x if x in major_legal else 'その他法人格' if pd.notna(x) and x != '' else '不明'
            )
            print(f"  主要法人格（30件以上）: {major_legal}")

        # 施設形態の正規化
        if 'FacilityType_Large__c' in df.columns:
            df['facility'] = df['FacilityType_Large__c'].fillna('不明')

        return df

    def cross_tabulate(self, df: pd.DataFrame):
        """MECEクロス集計"""
        facility_col = 'facility'
        legal_col = 'legal_group'
        results = []

        facilities = sorted(df[facility_col].unique())
        legals = sorted(df[legal_col].unique())
        # 対象期間の月（4月～1月 = 4,5,6,7,8,9,10,11,12,1）
        months = [4, 5, 6, 7, 8, 9, 10, 11, 12, 1]
        month_names = {1:'1月',2:'2月',3:'3月',4:'4月',5:'5月',6:'6月',
                       7:'7月',8:'8月',9:'9月',10:'10月',11:'11月',12:'12月'}

        print("\n" + "=" * 120)
        print(f"MECE クロス集計（{PERIOD_LABEL}限定）: 月 × 施設形態 × 法人格")
        print("=" * 120)

        for fac in facilities:
            for leg in legals:
                df_seg = df[(df[facility_col] == fac) & (df[legal_col] == leg)]
                total = len(df_seg)
                if total < 20:
                    continue

                won = df_seg['is_won'].sum()
                annual_rate = df_seg['is_won'].mean()

                monthly_rates = {}
                monthly_counts = {}
                for m in months:
                    df_m = df_seg[df_seg['month'] == m]
                    monthly_counts[m] = len(df_m)
                    # 月別10件以上でのみ受注率を算出（小サンプルによる100%等を排除）
                    monthly_rates[m] = df_m['is_won'].mean() if len(df_m) >= 10 else None

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

        results_df = results_df.sort_values('annual_rate', ascending=False)

        # 表示: 全組み合わせ
        print(f"\n  {'施設形態':<18} {'法人格':<14} {'件数':>6} {'成約':>5} {'期間率':>6}", end='')
        for m in months:
            print(f" {month_names[m]:>4}", end='')
        print()
        print("  " + "-" * (50 + 5 * len(months)))

        for _, row in results_df.iterrows():
            line = f"  {str(row['facility'])[:17]:<18} {str(row['legal'])[:13]:<14} "
            line += f"{int(row['total']):>6} {int(row['won']):>5} {row['annual_rate']*100:>5.1f}%"

            for m in months:
                rate = row[f'm{m}_rate']
                count = row[f'm{m}_count']
                if rate is not None and count >= 10:
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

        # ヒートマップデータ
        heatmap_rows = []
        for _, row in results_df.iterrows():
            for m in months:
                heatmap_rows.append({
                    'facility': row['facility'],
                    'legal': row['legal'],
                    'month': m,
                    'win_rate': row[f'm{m}_rate'],
                    'count': int(row[f'm{m}_count']),
                    'period_rate': row['annual_rate'],
                    'period_total': int(row['total']),
                })

        heatmap_df = pd.DataFrame(heatmap_rows)

        return results_df, heatmap_df

    def winrate_focus(self, results_df: pd.DataFrame):
        """受注率フォーカス分析"""
        months = [4, 5, 6, 7, 8, 9, 10, 11, 12, 1]
        month_names = {1:'1月',2:'2月',3:'3月',4:'4月',5:'5月',6:'6月',
                       7:'7月',8:'8月',9:'9月',10:'10月',11:'11月',12:'12月'}

        report = []
        report.append(f"# 受注率フォーカス分析（{PERIOD_LABEL}限定）")
        report.append(f"\n**分析日**: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        report.append(f"**対象期間**: {PERIOD_LABEL}（直近10ヶ月）")
        report.append("**フィルタ**: 初回商談のみ（再商談除外）、新規営業（past_won_count=0）")
        report.append("**最低件数**: セグメント期間20件以上、月別受注率は10件以上でのみ算出")
        report.append("**方針**: 受注率の絶対値を軸に、各月で狙うべき組み合わせを明確化")
        report.append("\n---")

        # ========================================
        # 1. 各月の受注率ランキング TOP10
        # ========================================
        print("\n" + "=" * 120)
        print(f"各月の受注率ランキング（{PERIOD_LABEL}限定、月別10件以上）")
        print("=" * 120)

        report.append("\n## 1. 各月の受注率ランキング TOP10")
        report.append(f"\n月ごとに「受注率が高い施設形態×法人格」のTOP10を表示。最低月別10件以上。")

        for m in months:
            rate_col = f'm{m}_rate'
            count_col = f'm{m}_count'

            # 月別10件以上（小サンプルによる100%等を排除）
            df_m = results_df[(results_df[count_col] >= 10) & (results_df[rate_col].notna())].copy()
            df_m = df_m.sort_values(rate_col, ascending=False)

            print(f"\n{'─' * 120}")
            print(f"  {month_names[m]} 受注率ランキング TOP10")
            print(f"{'─' * 120}")
            print(f"  {'#':>2} {'施設形態':<18} {'法人格':<16} {'受注率':>8} "
                  f"{'月件数':>6} {'期間率':>7} {'期間件数':>7}")

            report.append(f"\n### {month_names[m]}")
            report.append(f"\n| # | 施設形態 | 法人格 | **受注率** | 月件数 | 期間率 | 期間件数 |")
            report.append(f"|---|---|---|---|---|---|---|")

            for i, (_, row) in enumerate(df_m.head(10).iterrows()):
                rate = row[rate_col] * 100
                period = row['annual_rate'] * 100
                print(f"  {i+1:>2} {str(row['facility'])[:17]:<18} {str(row['legal'])[:15]:<16} "
                      f"{rate:>7.1f}% {int(row[count_col]):>6} {period:>6.1f}% {int(row['total']):>7}")
                report.append(f"| {i+1} | {row['facility']} | {row['legal']} | "
                             f"**{rate:.1f}%** | {int(row[count_col])} | {period:.1f}% | {int(row['total'])} |")

        # ========================================
        # 2. 受注率が安定して高いセグメント
        # ========================================
        print("\n\n" + "=" * 120)
        print(f"受注率が安定して高いセグメント（{PERIOD_LABEL}）")
        print("=" * 120)

        report.append("\n---")
        report.append("\n## 2. 受注率が安定して高いセグメント")
        report.append(f"\n期間件数50件以上、かつ月別10件以上のデータがある月が3ヶ月以上のセグメントで、平均受注率順。")

        rate_cols_valid = []
        df_stable = results_df[results_df['total'] >= 50].copy()
        for m in months:
            col_name = f'valid_m{m}'
            df_stable[col_name] = df_stable.apply(
                lambda r: r[f'm{m}_rate'] if r[f'm{m}_count'] >= 10 and pd.notna(r[f'm{m}_rate']) else np.nan, axis=1
            )
            rate_cols_valid.append(col_name)

        df_stable['avg_rate'] = df_stable[rate_cols_valid].mean(axis=1)
        df_stable['std_rate'] = df_stable[rate_cols_valid].std(axis=1)
        df_stable['valid_months'] = df_stable[rate_cols_valid].notna().sum(axis=1)

        df_stable = df_stable[df_stable['valid_months'] >= 3]
        df_stable = df_stable.sort_values('avg_rate', ascending=False)

        print(f"\n  {'#':>2} {'施設形態':<18} {'法人格':<16} {'平均受注率':>9} {'ブレ幅(σ)':>9} "
              f"{'期間率':>7} {'期間件数':>7} {'有効月数':>7}")
        print(f"  " + "-" * 100)

        report.append(f"\n| # | 施設形態 | 法人格 | **平均受注率** | ブレ幅(σ) | 期間率 | 期間件数 | 有効月数 |")
        report.append(f"|---|---|---|---|---|---|---|---|")

        for i, (_, row) in enumerate(df_stable.head(15).iterrows()):
            avg = row['avg_rate'] * 100
            std = row['std_rate'] * 100 if pd.notna(row['std_rate']) else 0
            period = row['annual_rate'] * 100
            print(f"  {i+1:>2} {str(row['facility'])[:17]:<18} {str(row['legal'])[:15]:<16} "
                  f"{avg:>8.1f}% {std:>8.1f}% {period:>6.1f}% {int(row['total']):>7} {int(row['valid_months']):>7}")
            report.append(f"| {i+1} | {row['facility']} | {row['legal']} | "
                         f"**{avg:.1f}%** | {std:.1f}% | {period:.1f}% | {int(row['total'])} | {int(row['valid_months'])} |")

        # ========================================
        # 3. 受注率×件数マトリクス
        # ========================================
        print("\n\n" + "=" * 120)
        print(f"月別 受注率マトリクス（{PERIOD_LABEL}、期間50件以上）")
        print("=" * 120)

        report.append("\n---")
        report.append(f"\n## 3. 月別受注率マトリクス（{PERIOD_LABEL}）")
        report.append("\n期間50件以上のセグメントを受注率順に並べ、全月の受注率を一覧化。")
        report.append("\n**凡例**: 数値は受注率(%)。月別10件未満は `--`。受注率15%以上は**太字**。")

        df_matrix = results_df[results_df['total'] >= 50].copy()
        df_matrix = df_matrix.sort_values('annual_rate', ascending=False)

        header = f"  {'施設形態':<16} {'法人格':<14} {'期間':>5} {'件数':>5}"
        for m in months:
            header += f" {month_names[m]:>4}"
        print(header)
        print(f"  " + "-" * (42 + 5 * len(months)))

        report_header = "| 施設形態 | 法人格 | 期間 | 件数 |"
        report_sep = "|---|---|---|---|"
        for m in months:
            report_header += f" {month_names[m]} |"
            report_sep += "---|"
        report.append(f"\n{report_header}")
        report.append(report_sep)

        for _, row in df_matrix.iterrows():
            line = f"  {str(row['facility'])[:15]:<16} {str(row['legal'])[:13]:<14} "
            line += f"{row['annual_rate']*100:>4.1f}% {int(row['total']):>5}"

            rpt_line = f"| {row['facility']} | {row['legal']} | {row['annual_rate']*100:.1f}% | {int(row['total'])} |"

            for m in months:
                rate = row[f'm{m}_rate']
                count = row[f'm{m}_count']
                if pd.notna(rate) and count >= 10:
                    val = rate * 100
                    if val >= 15:
                        line += f" {val:>4.0f}*"
                        rpt_line += f" **{val:.0f}%** |"
                    elif val == 0:
                        line += f"    0 "
                        rpt_line += f" 0% |"
                    else:
                        line += f" {val:>5.1f}"
                        rpt_line += f" {val:.1f}% |"
                else:
                    line += f"   -- "
                    rpt_line += " -- |"
            print(line)
            report.append(rpt_line)

        # ========================================
        # 4. 4月の受注率視点での推奨
        # ========================================
        print("\n\n" + "=" * 120)
        print(f"4月: 受注率で見た推奨（{PERIOD_LABEL}、期間50件以上・4月10件以上）")
        print("=" * 120)

        report.append("\n---")
        report.append(f"\n## 4. 4月の受注率ベース推奨（{PERIOD_LABEL}）")

        df_apr = results_df[
            (results_df['total'] >= 50) &
            (results_df['m4_count'] >= 10) &
            (results_df['m4_rate'].notna())
        ].copy()
        df_apr = df_apr.sort_values('m4_rate', ascending=False)

        print(f"\n  {'#':>2} {'施設形態':<18} {'法人格':<16} {'4月受注率':>9} "
              f"{'4月件数':>7} {'期間率':>7} {'期間件数':>7} {'判定':<6}")
        print(f"  " + "-" * 95)

        report.append(f"\n| # | 施設形態 | 法人格 | **4月受注率** | 4月件数 | 期間率 | 期間件数 | 判定 |")
        report.append(f"|---|---|---|---|---|---|---|---|")

        for i, (_, row) in enumerate(df_apr.iterrows()):
            apr = row['m4_rate'] * 100
            period = row['annual_rate'] * 100

            if apr >= 15:
                verdict = "◎攻め"
            elif apr >= 8:
                verdict = "○可"
            elif apr >= 3:
                verdict = "△注意"
            else:
                verdict = "✕避け"

            print(f"  {i+1:>2} {str(row['facility'])[:17]:<18} {str(row['legal'])[:15]:<16} "
                  f"{apr:>8.1f}% {int(row['m4_count']):>7} {period:>6.1f}% {int(row['total']):>7} {verdict}")
            report.append(f"| {i+1} | {row['facility']} | {row['legal']} | "
                         f"**{apr:.1f}%** | {int(row['m4_count'])} | {period:.1f}% | {int(row['total'])} | {verdict} |")

        # ========================================
        # 5. 各月のベストセグメント一覧（受注率TOP3）
        # ========================================
        print("\n\n" + "=" * 120)
        print(f"月別ベストセグメント（{PERIOD_LABEL}、受注率TOP3、期間50件以上・月10件以上）")
        print("=" * 120)

        report.append("\n---")
        report.append(f"\n## 5. 月別ベストセグメント（受注率TOP3）")
        report.append(f"\n期間50件以上かつ月別10件以上のセグメントから受注率TOP3を選出。")

        df_50 = results_df[results_df['total'] >= 50].copy()

        for m in months:
            rate_col = f'm{m}_rate'
            count_col = f'm{m}_count'
            df_m = df_50[(df_50[count_col] >= 10) & (df_50[rate_col].notna())].copy()
            df_m = df_m.sort_values(rate_col, ascending=False)

            top3 = df_m.head(3)
            if len(top3) == 0:
                continue

            print(f"\n  {month_names[m]}:")
            report.append(f"\n**{month_names[m]}:**")

            for _, row in top3.iterrows():
                rate = row[rate_col] * 100
                cnt = int(row[count_col])
                print(f"    → {row['facility']} × {row['legal']}: "
                      f"受注率 {rate:.1f}%（{cnt}件）")
                report.append(f"- {row['facility']} × {row['legal']}: **受注率 {rate:.1f}%**（{cnt}件）")

        # ========================================
        # 6. 全期間データとの比較サマリー
        # ========================================
        report.append("\n---")
        report.append(f"\n## 6. 全期間データとの比較ポイント")
        report.append(f"\nこのレポートは **{PERIOD_LABEL}のみ** の結果です。")
        report.append("全期間（全時系列）の結果と併せて、以下の観点で比較してください：")
        report.append("\n- 全期間と直近期間で受注率ランキングに変動がないか")
        report.append("- 4月の傾向は直近でも同様か、改善/悪化しているか")
        report.append("- 安定的に高い受注率のセグメントは全期間と一致するか")

        return report

    def generate_report(self, df: pd.DataFrame, results_df: pd.DataFrame, report_lines: list):
        """統合レポート生成"""
        full_report = []

        # 全体サマリーを先頭に追加
        full_report.append(f"# FY2025期間限定分析（{PERIOD_LABEL}）")
        full_report.append(f"\n**分析日**: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        full_report.append(f"**対象期間**: {PERIOD_LABEL}")
        full_report.append(f"**対象**: 新規営業の確定商談 {len(df):,} 件")
        full_report.append(f"**期間平均受注率**: {df['is_won'].mean()*100:.1f}%")

        # 月別全体受注率
        months_ordered = [4, 5, 6, 7, 8, 9, 10, 11, 12, 1]
        month_names = {1:'1月',2:'2月',3:'3月',4:'4月',5:'5月',6:'6月',
                       7:'7月',8:'8月',9:'9月',10:'10月',11:'11月',12:'12月'}
        avg = df['is_won'].mean()

        full_report.append("\n## 全体月別受注率")
        full_report.append("\n| 月 | 商談数 | 成約数 | 成約率 | vs期間平均 |")
        full_report.append("|---|---|---|---|---|")

        print("\n" + "=" * 120)
        print(f"全体月別受注率（{PERIOD_LABEL}）")
        print("=" * 120)
        print(f"  期間平均: {avg*100:.1f}%")
        print(f"\n  {'月':>4} {'商談数':>7} {'成約数':>7} {'成約率':>7} {'vs平均':>8}")
        print(f"  " + "-" * 40)

        for m in months_ordered:
            dm = df[df['month'] == m]
            if len(dm) == 0:
                continue
            rate = dm['is_won'].mean()
            diff = rate - avg
            marker = "▼" if diff < -0.02 else "▲" if diff > 0.02 else ""
            print(f"  {month_names[m]:>4} {len(dm):>7} {int(dm['is_won'].sum()):>7} "
                  f"{rate*100:>6.1f}% {diff*100:>+7.1f}pt {marker}")
            full_report.append(f"| {month_names[m]} | {len(dm):,} | {int(dm['is_won'].sum())} | "
                              f"{rate*100:.1f}% | {diff*100:+.1f}pt |")

        # 受注率フォーカスレポートを結合
        full_report.append("\n---\n")
        full_report.extend(report_lines)

        # 保存
        report_text = "\n".join(full_report)
        report_path = self.report_dir / 'fy2025_focused_analysis.md'
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(report_text)
        print(f"\n\nレポート保存: {report_path}")
        return report_path

    def run(self):
        """実行"""
        print("=" * 120)
        print(f"FY2025期間限定分析（{PERIOD_LABEL}）")
        print(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 120)

        # データ抽出
        df_raw = self.extract()

        # 前処理
        df = self.prepare(df_raw)

        # クロス集計
        results_df, heatmap_df = self.cross_tabulate(df)

        if results_df.empty:
            print("十分なデータがありません。終了します。")
            return

        # CSV保存
        path_results = self.output_dir / f'fy2025_cross_results_{self.timestamp}.csv'
        results_df.to_csv(path_results, index=False, encoding='utf-8-sig')
        print(f"\n  クロス集計結果保存: {path_results}")

        if not heatmap_df.empty:
            path_heat = self.output_dir / f'fy2025_heatmap_{self.timestamp}.csv'
            heatmap_df.to_csv(path_heat, index=False, encoding='utf-8-sig')
            print(f"  ヒートマップデータ保存: {path_heat}")

        # 受注率フォーカス分析
        report_lines = self.winrate_focus(results_df)

        # 統合レポート生成
        self.generate_report(df, results_df, report_lines)

        print("\n" + "=" * 120)
        print("分析完了")
        print("=" * 120)


if __name__ == "__main__":
    analyzer = FY2025Analyzer()
    analyzer.run()
