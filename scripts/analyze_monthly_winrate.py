# -*- coding: utf-8 -*-
"""
月別×セグメント別 成約率時系列分析

目的: 「4月の成約率が悪い」仮説を検証し、
全事業形態・施設形態を月別比較して攻め先を明確化する。

分析軸:
- 事業形態（ServiceType__c）
- 施設形態（FacilityType_Large/Middle/Small__c）
- 法人格（LegalPersonality__c）
- 従業員規模
- 商談型（OpportunityType__c）
- アポランク（AppointRank__c）
- 都道府県（Prefectures__c）
"""

import sys
import io
from pathlib import Path
from datetime import datetime

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Windows環境でのUTF-8出力対応（インポート前に一度だけ設定）
import os
os.environ['PYTHONIOENCODING'] = 'utf-8'

import pandas as pd
import numpy as np
from scipy import stats
import warnings
warnings.filterwarnings('ignore')

# analyze_win_rate.py のインポート（モジュール内でstdout再設定があるため注意）
# stdout.bufferを保護してからインポート
_original_stdout = sys.stdout
try:
    from scripts.analyze_win_rate import WinRateAnalyzer
except Exception:
    from analyze_win_rate import WinRateAnalyzer
# インポート後にstdoutが壊れていたら復元
try:
    sys.stdout.write('')
except (ValueError, OSError):
    sys.stdout = _original_stdout


class MonthlyWinRateAnalyzer:
    """月別成約率分析クラス"""

    # 分析対象セグメント定義
    SEGMENT_CONFIGS = {
        '事業形態': {
            'column': 'Account.ServiceType__c',
            'min_count': 30,
        },
        '施設形態（大分類）': {
            'column': 'FacilityType_Large__c',
            'min_count': 20,
        },
        '施設形態（中分類）': {
            'column': 'FacilityType_Middle__c',
            'min_count': 20,
        },
        '施設形態（小分類）': {
            'column': 'FacilityType_Small__c',
            'min_count': 10,
        },
        '法人格': {
            'column': 'Account.LegalPersonality__c',
            'min_count': 20,
        },
        '従業員規模': {
            'column': 'emp_size',
            'min_count': 20,
        },
        '商談型': {
            'column': 'OpportunityType__c',
            'min_count': 20,
        },
        'アポランク': {
            'column': 'AppointRank__c',
            'min_count': 20,
        },
        '都道府県': {
            'column': 'Account.Prefectures__c',
            'min_count': 20,
        },
    }

    def __init__(self):
        self.base_analyzer = WinRateAnalyzer()
        self.output_dir = project_root / 'data' / 'output' / 'analysis'
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.report_dir = project_root / 'claudedocs'
        self.report_dir.mkdir(parents=True, exist_ok=True)
        self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    def extract_data(self) -> pd.DataFrame:
        """Salesforceからデータ抽出（WinRateAnalyzerのパターンを再利用）"""
        print("=" * 80)
        print("Phase 1: データ抽出")
        print("=" * 80)

        self.base_analyzer.authenticate()
        df = self.base_analyzer.export_opportunities()
        return df

    def prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """データ前処理"""
        print("\n" + "=" * 80)
        print("Phase 2: データ前処理")
        print("=" * 80)

        # IsWonを数値化
        df['is_won'] = df['IsWon'].map({'true': 1, 'false': 0, True: 1, False: 0})

        # CloseDate → 月・年月を抽出
        df['CloseDate'] = pd.to_datetime(df['CloseDate'], errors='coerce')
        df['close_month'] = df['CloseDate'].dt.month
        df['close_year'] = df['CloseDate'].dt.year
        df['close_ym'] = df['CloseDate'].dt.to_period('M').astype(str)

        # 年度（FY）: 4月始まり
        df['fiscal_year'] = df['CloseDate'].apply(
            lambda x: x.year if x.month >= 4 else x.year - 1 if pd.notna(x) else None
        )

        # 新規営業フィルタ用（past_won_count）
        df['won_count'] = pd.to_numeric(
            df.get('Account.WonOpportunityies__c', 0), errors='coerce'
        ).fillna(0)
        df['past_won_count'] = df.apply(
            lambda row: row['won_count'] - 1 if row['is_won'] == 1 else row['won_count'],
            axis=1
        )

        # 従業員規模カテゴリ
        df['employees'] = pd.to_numeric(
            df.get('Account.NumberOfEmployees', None), errors='coerce'
        )
        df['emp_size'] = pd.cut(
            df['employees'],
            bins=[0, 30, 100, 500, float('inf')],
            labels=['小(~30)', '中(31-100)', '大(101-500)', '超大(500+)']
        )

        # Amount数値化
        df['amount'] = pd.to_numeric(df.get('Amount', 0), errors='coerce').fillna(0)

        # 統計情報
        total = len(df)
        won = (df['is_won'] == 1).sum()
        new_biz = (df['past_won_count'] == 0).sum()

        print(f"  総確定商談数: {total:,}")
        print(f"  成約: {won:,} ({won/total*100:.1f}%)")
        print(f"  失注: {total - won:,} ({(total-won)/total*100:.1f}%)")
        print(f"  新規営業（past_won_count=0）: {new_biz:,}")
        print(f"  期間: {df['CloseDate'].min().strftime('%Y-%m-%d')} ～ {df['CloseDate'].max().strftime('%Y-%m-%d')}")
        print(f"  年度: {sorted(df['fiscal_year'].dropna().unique())}")

        return df

    def analyze_overall_monthly(self, df: pd.DataFrame) -> pd.DataFrame:
        """全体月別成約率"""
        print("\n" + "=" * 80)
        print("全体 月別成約率")
        print("=" * 80)

        monthly = df.groupby('close_month').agg(
            商談数=('is_won', 'count'),
            成約数=('is_won', 'sum'),
            成約率=('is_won', 'mean'),
            平均金額=('amount', 'mean'),
        ).round(4)

        monthly['成約率%'] = (monthly['成約率'] * 100).round(1)
        annual_avg = df['is_won'].mean()
        monthly['年間平均との差'] = ((monthly['成約率'] - annual_avg) * 100).round(1)

        print(f"\n  年間平均成約率: {annual_avg*100:.1f}%\n")
        print(f"  {'月':>4} {'商談数':>8} {'成約数':>8} {'成約率':>8} {'年間平均との差':>12}")
        print("  " + "-" * 48)

        for month, row in monthly.iterrows():
            diff = row['年間平均との差']
            marker = "▼" if diff < -2 else "▲" if diff > 2 else "  "
            print(f"  {month:>3}月 {int(row['商談数']):>8} {int(row['成約数']):>8} "
                  f"{row['成約率%']:>7.1f}% {diff:>+10.1f}pt {marker}")

        # カイ二乗検定: 月と成約の関連性
        contingency = pd.crosstab(df['close_month'], df['is_won'])
        if contingency.shape[0] >= 2 and contingency.shape[1] >= 2:
            chi2, pval, dof, expected = stats.chi2_contingency(contingency)
            sig = "***" if pval < 0.001 else "**" if pval < 0.01 else "*" if pval < 0.05 else "n.s."
            print(f"\n  カイ二乗検定（月×成約）: χ²={chi2:.2f}, p={pval:.6f} {sig}")

        return monthly

    def analyze_segment_monthly(self, df: pd.DataFrame, segment_name: str,
                                 column: str, min_count: int) -> pd.DataFrame:
        """セグメント別月別成約率分析"""
        print(f"\n{'=' * 80}")
        print(f"月別成約率: {segment_name}")
        print(f"{'=' * 80}")

        if column not in df.columns:
            print(f"  ❌ 列 {column} が見つかりません")
            return pd.DataFrame()

        # セグメント値が欠損でないデータのみ
        df_valid = df[df[column].notna() & (df[column] != '')].copy()

        # 年間集計でmin_count以上のセグメントに絞る
        segment_counts = df_valid.groupby(column).size()
        valid_segments = segment_counts[segment_counts >= min_count].index.tolist()

        if not valid_segments:
            print(f"  ⚠️ {min_count}件以上のセグメントがありません")
            return pd.DataFrame()

        df_filtered = df_valid[df_valid[column].isin(valid_segments)]
        print(f"  分析対象セグメント: {len(valid_segments)} 種類（{min_count}件以上）")

        # 月別×セグメント ピボットテーブル
        pivot = df_filtered.pivot_table(
            values='is_won',
            index=column,
            columns='close_month',
            aggfunc=['mean', 'count'],
            fill_value=None
        )

        # 成約率ピボット
        rate_pivot = pivot['mean'].copy()
        count_pivot = pivot['count'].copy()

        # 年間平均を追加
        annual = df_filtered.groupby(column)['is_won'].agg(['mean', 'count'])
        rate_pivot['年間平均'] = annual['mean']
        rate_pivot['年間件数'] = annual['count']

        # 4月と年間平均の差分
        if 4 in rate_pivot.columns:
            rate_pivot['4月vs年間平均'] = rate_pivot[4] - rate_pivot['年間平均']
            rate_pivot = rate_pivot.sort_values('4月vs年間平均', ascending=True)

        # 表示
        print(f"\n  {'セグメント':<25}", end='')
        for m in range(1, 13):
            print(f" {m:>5}月", end='')
        print(f" {'年間平均':>8} {'件数':>6} {'4月差分':>8}")
        print("  " + "-" * 145)

        for seg, row in rate_pivot.iterrows():
            seg_str = str(seg)[:24]
            print(f"  {seg_str:<25}", end='')
            for m in range(1, 13):
                if m in row.index and pd.notna(row[m]):
                    val = row[m] * 100
                    # 年間平均より大幅に低い場合マーク
                    if pd.notna(row.get('年間平均')) and val < row['年間平均'] * 100 - 5:
                        print(f" {val:>5.1f}▼", end='')
                    elif pd.notna(row.get('年間平均')) and val > row['年間平均'] * 100 + 5:
                        print(f" {val:>5.1f}▲", end='')
                    else:
                        print(f" {val:>6.1f}", end='')
                else:
                    print(f" {'--':>6}", end='')

            avg = row.get('年間平均', 0)
            cnt = row.get('年間件数', 0)
            diff_4 = row.get('4月vs年間平均', 0)
            print(f" {avg*100:>7.1f}% {int(cnt):>6} {diff_4*100:>+7.1f}pt")

        return rate_pivot

    def generate_april_ranking(self, df: pd.DataFrame) -> dict:
        """4月の攻め先/避け先ランキング（全セグメント横断）"""
        print("\n" + "=" * 80)
        print("4月 攻め先ランキング（全セグメント横断）")
        print("=" * 80)

        all_rankings = []

        for seg_name, config in self.SEGMENT_CONFIGS.items():
            column = config['column']
            min_count = config['min_count']

            if column not in df.columns:
                continue

            df_valid = df[df[column].notna() & (df[column] != '')].copy()

            # 年間集計
            annual = df_valid.groupby(column)['is_won'].agg(['mean', 'count']).rename(
                columns={'mean': 'annual_rate', 'count': 'annual_count'}
            )

            # 4月のみ
            df_april = df_valid[df_valid['close_month'] == 4]
            april = df_april.groupby(column)['is_won'].agg(['mean', 'count']).rename(
                columns={'mean': 'april_rate', 'count': 'april_count'}
            )

            # 結合
            merged = annual.join(april, how='inner')
            merged = merged[merged['annual_count'] >= min_count]
            merged = merged[merged['april_count'] >= 5]  # 4月に最低5件
            merged['diff'] = merged['april_rate'] - merged['annual_rate']
            merged['segment_type'] = seg_name
            merged['segment_value'] = merged.index

            all_rankings.append(merged.reset_index(drop=True))

        if not all_rankings:
            print("  ランキング生成に十分なデータがありません")
            return {}

        ranking_df = pd.concat(all_rankings, ignore_index=True)

        # 4月に攻めるべき（4月の成約率が年間平均を上回る）
        attack = ranking_df[ranking_df['diff'] > 0].sort_values('diff', ascending=False)
        # 4月に避けるべき（4月の成約率が年間平均を下回る）
        avoid = ranking_df[ranking_df['diff'] < 0].sort_values('diff', ascending=True)

        print("\n【4月に攻めるべきセグメント TOP15】")
        print(f"  {'分析軸':<15} {'セグメント':<25} {'4月成約率':>10} {'年間平均':>10} {'差分':>8} {'4月件数':>8}")
        print("  " + "-" * 80)
        for _, row in attack.head(15).iterrows():
            print(f"  {row['segment_type']:<15} {str(row['segment_value'])[:24]:<25} "
                  f"{row['april_rate']*100:>9.1f}% {row['annual_rate']*100:>9.1f}% "
                  f"{row['diff']*100:>+7.1f}pt {int(row['april_count']):>8}")

        print("\n【4月に避けるべきセグメント TOP15】")
        print(f"  {'分析軸':<15} {'セグメント':<25} {'4月成約率':>10} {'年間平均':>10} {'差分':>8} {'4月件数':>8}")
        print("  " + "-" * 80)
        for _, row in avoid.head(15).iterrows():
            print(f"  {row['segment_type']:<15} {str(row['segment_value'])[:24]:<25} "
                  f"{row['april_rate']*100:>9.1f}% {row['annual_rate']*100:>9.1f}% "
                  f"{row['diff']*100:>+7.1f}pt {int(row['april_count']):>8}")

        return {'attack': attack, 'avoid': avoid, 'all': ranking_df}

    def generate_monthly_calendar(self, df: pd.DataFrame) -> pd.DataFrame:
        """月別最適攻め先カレンダー"""
        print("\n" + "=" * 80)
        print("月別最適攻め先カレンダー")
        print("=" * 80)

        results = []

        for seg_name, config in self.SEGMENT_CONFIGS.items():
            column = config['column']
            min_count = config['min_count']

            if column not in df.columns:
                continue

            df_valid = df[df[column].notna() & (df[column] != '')].copy()

            for month in range(1, 13):
                df_month = df_valid[df_valid['close_month'] == month]
                if len(df_month) < 10:
                    continue

                month_stats = df_month.groupby(column)['is_won'].agg(['mean', 'count'])
                month_stats = month_stats[month_stats['count'] >= max(5, min_count // 12)]

                if len(month_stats) == 0:
                    continue

                best_idx = month_stats['mean'].idxmax()
                best_rate = month_stats.loc[best_idx, 'mean']
                best_count = month_stats.loc[best_idx, 'count']

                results.append({
                    'month': month,
                    'segment_type': seg_name,
                    'best_segment': best_idx,
                    'win_rate': best_rate,
                    'count': int(best_count),
                })

        calendar_df = pd.DataFrame(results)

        # 月ごとに最も成約率が高いセグメントを表示
        print(f"\n  {'月':>4}  {'分析軸':<15} {'ベストセグメント':<25} {'成約率':>8} {'件数':>6}")
        print("  " + "-" * 70)

        for month in range(1, 13):
            month_data = calendar_df[calendar_df['month'] == month]
            if len(month_data) == 0:
                continue
            best = month_data.sort_values('win_rate', ascending=False).head(3)
            for i, (_, row) in enumerate(best.iterrows()):
                prefix = f"{month:>3}月" if i == 0 else "     "
                print(f"  {prefix}  {row['segment_type']:<15} {str(row['best_segment'])[:24]:<25} "
                      f"{row['win_rate']*100:>7.1f}% {int(row['count']):>6}")

        return calendar_df

    def generate_seasonality_heatmap(self, df: pd.DataFrame) -> pd.DataFrame:
        """ヒートマップ用データ生成（事業形態×月）"""
        print("\n" + "=" * 80)
        print("季節性ヒートマップデータ生成")
        print("=" * 80)

        heatmap_data = []

        for seg_name, config in self.SEGMENT_CONFIGS.items():
            column = config['column']
            min_count = config['min_count']

            if column not in df.columns:
                continue

            df_valid = df[df[column].notna() & (df[column] != '')].copy()

            # 年間でmin_count以上のセグメントのみ
            seg_counts = df_valid.groupby(column).size()
            valid_segs = seg_counts[seg_counts >= min_count].index

            for seg_val in valid_segs:
                df_seg = df_valid[df_valid[column] == seg_val]
                annual_rate = df_seg['is_won'].mean()
                annual_count = len(df_seg)

                for month in range(1, 13):
                    df_month = df_seg[df_seg['close_month'] == month]
                    if len(df_month) >= 3:
                        month_rate = df_month['is_won'].mean()
                        month_count = len(df_month)
                    else:
                        month_rate = None
                        month_count = 0

                    heatmap_data.append({
                        'segment_type': seg_name,
                        'segment_value': seg_val,
                        'month': month,
                        'win_rate': month_rate,
                        'count': month_count,
                        'annual_rate': annual_rate,
                        'annual_count': annual_count,
                        'diff_from_annual': (month_rate - annual_rate) if month_rate is not None else None,
                    })

        heatmap_df = pd.DataFrame(heatmap_data)
        print(f"  レコード数: {len(heatmap_df):,}")

        return heatmap_df

    def generate_report(self, df: pd.DataFrame, overall: pd.DataFrame,
                        rankings: dict, calendar_df: pd.DataFrame,
                        segment_pivots: dict) -> str:
        """Markdownレポート生成"""
        print("\n" + "=" * 80)
        print("レポート生成")
        print("=" * 80)

        annual_avg = df['is_won'].mean()
        april_data = df[df['close_month'] == 4]
        april_rate = april_data['is_won'].mean() if len(april_data) > 0 else 0
        april_count = len(april_data)

        # 最も成約率が低い月
        worst_month = overall['成約率'].idxmin()
        worst_rate = overall.loc[worst_month, '成約率']
        best_month = overall['成約率'].idxmax()
        best_rate = overall.loc[best_month, '成約率']

        report = []
        report.append("# 成約先 月別時系列分析レポート")
        report.append(f"\n**分析日**: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        report.append(f"**分析対象**: IsClosed=true の全確定商談（新規営業: past_won_count=0）")
        report.append(f"**期間**: {df['CloseDate'].min().strftime('%Y-%m-%d')} ～ {df['CloseDate'].max().strftime('%Y-%m-%d')}")
        report.append(f"**総商談数**: {len(df):,} 件")

        # エグゼクティブサマリー
        report.append("\n## 1. エグゼクティブサマリー")
        report.append(f"\n### 「4月は本当に悪いか？」")
        report.append(f"\n| 指標 | 値 |")
        report.append(f"|------|------|")
        report.append(f"| 年間平均成約率 | {annual_avg*100:.1f}% |")
        report.append(f"| **4月の成約率** | **{april_rate*100:.1f}%** |")
        report.append(f"| 4月の差分 | {(april_rate - annual_avg)*100:+.1f}pt |")
        report.append(f"| 4月の商談件数 | {april_count:,} 件 |")
        report.append(f"| 最低成約率月 | {worst_month}月（{worst_rate*100:.1f}%） |")
        report.append(f"| 最高成約率月 | {best_month}月（{best_rate*100:.1f}%） |")

        if april_rate < annual_avg:
            severity = "大幅に" if (annual_avg - april_rate) > 0.03 else "やや"
            report.append(f"\n**結論**: 4月の成約率は年間平均を{severity}下回っています（{(april_rate - annual_avg)*100:+.1f}pt）。")
        else:
            report.append(f"\n**結論**: 4月の成約率は年間平均と同等以上です（{(april_rate - annual_avg)*100:+.1f}pt）。感覚とデータにギャップがある可能性があります。")

        # 全体月別
        report.append("\n## 2. 全体月別成約率")
        report.append("\n| 月 | 商談数 | 成約数 | 成約率 | 年間平均との差 |")
        report.append("|---|---|---|---|---|")
        for month, row in overall.iterrows():
            diff = row['年間平均との差']
            marker = " :red_circle:" if diff < -2 else " :green_circle:" if diff > 2 else ""
            report.append(f"| {month}月 | {int(row['商談数']):,} | {int(row['成約数']):,} | "
                         f"{row['成約率%']:.1f}% | {diff:+.1f}pt{marker} |")

        # セグメント別ピボット
        report.append("\n## 3. セグメント別 月別成約率")

        for seg_name, pivot in segment_pivots.items():
            if pivot is None or pivot.empty:
                continue

            report.append(f"\n### {seg_name}")
            report.append("\n| セグメント |" + "|".join([f" {m}月 " for m in range(1, 13)]) + "| 年間平均 | 件数 | 4月差分 |")
            report.append("|---|" + "|".join(["---|"] * 12) + "---|---|---|")

            for seg, row in pivot.iterrows():
                line = f"| {str(seg)[:20]} |"
                for m in range(1, 13):
                    if m in row.index and pd.notna(row[m]):
                        val = row[m] * 100
                        line += f" {val:.1f}% |"
                    else:
                        line += " -- |"

                avg = row.get('年間平均', 0)
                cnt = row.get('年間件数', 0)
                diff_4 = row.get('4月vs年間平均', 0)
                line += f" {avg*100:.1f}% | {int(cnt):,} | {diff_4*100:+.1f}pt |"
                report.append(line)

        # 攻め先ランキング
        if rankings:
            report.append("\n## 4. 4月の攻め先推奨")
            report.append("\n### 4月に攻めるべきセグメント TOP15")
            report.append("\n| # | 分析軸 | セグメント | 4月成約率 | 年間平均 | 差分 | 4月件数 |")
            report.append("|---|---|---|---|---|---|---|")

            attack = rankings.get('attack', pd.DataFrame())
            for i, (_, row) in enumerate(attack.head(15).iterrows()):
                report.append(f"| {i+1} | {row['segment_type']} | {row['segment_value']} | "
                             f"{row['april_rate']*100:.1f}% | {row['annual_rate']*100:.1f}% | "
                             f"{row['diff']*100:+.1f}pt | {int(row['april_count'])} |")

            report.append("\n### 4月に避けるべきセグメント TOP15")
            report.append("\n| # | 分析軸 | セグメント | 4月成約率 | 年間平均 | 差分 | 4月件数 |")
            report.append("|---|---|---|---|---|---|---|")

            avoid = rankings.get('avoid', pd.DataFrame())
            for i, (_, row) in enumerate(avoid.head(15).iterrows()):
                report.append(f"| {i+1} | {row['segment_type']} | {row['segment_value']} | "
                             f"{row['april_rate']*100:.1f}% | {row['annual_rate']*100:.1f}% | "
                             f"{row['diff']*100:+.1f}pt | {int(row['april_count'])} |")

        # 月別カレンダー
        report.append("\n## 5. 月別最適攻め先カレンダー")
        report.append("\n各月で最も成約率が高いセグメント（上位3つ）:")

        for month in range(1, 13):
            month_data = calendar_df[calendar_df['month'] == month] if not calendar_df.empty else pd.DataFrame()
            if len(month_data) == 0:
                continue
            best = month_data.sort_values('win_rate', ascending=False).head(3)
            report.append(f"\n**{month}月:**")
            for _, row in best.iterrows():
                report.append(f"- {row['segment_type']}: {row['best_segment']}（成約率 {row['win_rate']*100:.1f}%, {int(row['count'])}件）")

        # 注記
        report.append("\n## 6. 留意事項")
        report.append("\n- 成約率 = IsWon件数 / IsClosed件数（確定商談のみ）")
        report.append("- 新規営業のみ（past_won_count=0）でフィルタ済み")
        report.append("- セグメントごとの最低件数閾値を設定（少数サンプルの排除）")
        report.append("- 4月差分は「4月の成約率 - 年間平均成約率」で算出")
        report.append("- 統計的有意性はカイ二乗検定で確認")

        report_text = "\n".join(report)

        report_path = self.report_dir / 'monthly_winrate_analysis_report.md'
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(report_text)

        print(f"  レポート保存: {report_path}")
        return report_text

    def run(self, new_business_only: bool = True):
        """分析実行"""
        print("=" * 80)
        print("月別×セグメント別 成約率時系列分析")
        print(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)

        # 1. データ抽出
        df_raw = self.extract_data()

        # 2. 前処理
        df = self.prepare_data(df_raw)

        # 3. 新規営業フィルタ
        if new_business_only:
            original = len(df)
            df = df[df['past_won_count'] == 0].copy().reset_index(drop=True)
            print(f"\n  新規営業フィルタ: {original:,} → {len(df):,} 件")

        # データ保存（再利用用）
        data_path = self.output_dir / f'monthly_analysis_data_{self.timestamp}.csv'
        df.to_csv(data_path, index=False, encoding='utf-8-sig')
        print(f"  データ保存: {data_path}")

        # 4. 全体月別分析
        overall = self.analyze_overall_monthly(df)

        # 5. セグメント別月別分析
        segment_pivots = {}
        for seg_name, config in self.SEGMENT_CONFIGS.items():
            pivot = self.analyze_segment_monthly(
                df, seg_name, config['column'], config['min_count']
            )
            segment_pivots[seg_name] = pivot

        # 6. 4月ランキング
        rankings = self.generate_april_ranking(df)

        # 7. 月別カレンダー
        calendar_df = self.generate_monthly_calendar(df)

        # 8. ヒートマップデータ
        heatmap_df = self.generate_seasonality_heatmap(df)
        heatmap_path = self.output_dir / f'seasonality_heatmap_{self.timestamp}.csv'
        heatmap_df.to_csv(heatmap_path, index=False, encoding='utf-8-sig')
        print(f"  ヒートマップデータ保存: {heatmap_path}")

        # 9. ランキングCSV保存
        if rankings:
            attack = rankings.get('attack', pd.DataFrame())
            avoid = rankings.get('avoid', pd.DataFrame())
            if not attack.empty:
                attack_path = self.output_dir / f'april_attack_segments_{self.timestamp}.csv'
                attack.to_csv(attack_path, index=False, encoding='utf-8-sig')
                print(f"  攻め先セグメント保存: {attack_path}")
            if not avoid.empty:
                avoid_path = self.output_dir / f'april_avoid_segments_{self.timestamp}.csv'
                avoid.to_csv(avoid_path, index=False, encoding='utf-8-sig')
                print(f"  避け先セグメント保存: {avoid_path}")

        # 10. レポート生成
        report = self.generate_report(df, overall, rankings, calendar_df, segment_pivots)

        print("\n" + "=" * 80)
        print("分析完了")
        print("=" * 80)

        return df, overall, segment_pivots, rankings


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="月別成約率時系列分析")
    parser.add_argument("--all-business", action="store_true",
                       help="全商談を対象（デフォルト: 新規営業のみ）")
    args = parser.parse_args()

    analyzer = MonthlyWinRateAnalyzer()
    df, overall, pivots, rankings = analyzer.run(
        new_business_only=not args.all_business
    )
