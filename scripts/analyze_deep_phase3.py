"""
深掘り分析 Phase 3: 成約との比較
3-1. 成約商談の特徴
3-2. 到達フェーズ別
"""

import pandas as pd
import numpy as np
from pathlib import Path

# 出力設定
output_dir = Path(r"C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List\data\output\analysis")
report_file = output_dir / 'deep_phase3_report.txt'

# データ読み込み
lost_path = output_dir / 'lost_opportunity_full_20260126.csv'
won_path = output_dir / 'won_opportunity_full_20260126.csv'

df_lost = pd.read_csv(lost_path, encoding='utf-8-sig', low_memory=False)
df_won = pd.read_csv(won_path, encoding='utf-8-sig', low_memory=False)

# 日付変換
df_lost['CloseDate'] = pd.to_datetime(df_lost['CloseDate'])
df_won['CloseDate'] = pd.to_datetime(df_won['CloseDate'])

# 2025年度のみ
df_lost_25 = df_lost[(df_lost['CloseDate'] >= '2025-04-01') & (df_lost['CloseDate'] <= '2026-01-27')].copy()
df_won_25 = df_won[(df_won['CloseDate'] >= '2025-04-01') & (df_won['CloseDate'] <= '2026-01-27')].copy()

# 2024年度
df_lost_24 = df_lost[(df_lost['CloseDate'] >= '2024-04-01') & (df_lost['CloseDate'] < '2025-04-01')].copy()
df_won_24 = df_won[(df_won['CloseDate'] >= '2024-04-01') & (df_won['CloseDate'] < '2025-04-01')].copy()

# 懸念系カテゴリ
concern_category = "サービスの価値は感じているが、懸念点を払拭できなかった"

with open(report_file, 'w', encoding='utf-8') as f:
    f.write("=" * 80 + "\n")
    f.write("深掘り分析 Phase 3: 成約との比較\n")
    f.write("=" * 80 + "\n\n")

    # ========================================
    # 3-0. 基本統計
    # ========================================
    f.write("=" * 80 + "\n")
    f.write("3-0. 基本統計\n")
    f.write("=" * 80 + "\n\n")

    f.write("【年度別 成約/失注件数】\n")
    f.write("-" * 60 + "\n")
    f.write(f"{'年度':<15} {'成約':>10} {'失注':>10} {'成約率':>10}\n")
    f.write("-" * 60 + "\n")

    total_24 = len(df_won_24) + len(df_lost_24)
    total_25 = len(df_won_25) + len(df_lost_25)
    win_rate_24 = len(df_won_24) / total_24 * 100 if total_24 > 0 else 0
    win_rate_25 = len(df_won_25) / total_25 * 100 if total_25 > 0 else 0

    f.write(f"  2024年度      {len(df_won_24):>10}件 {len(df_lost_24):>10}件 {win_rate_24:>8.1f}%\n")
    f.write(f"  2025年度      {len(df_won_25):>10}件 {len(df_lost_25):>10}件 {win_rate_25:>8.1f}%\n")
    f.write(f"  差分          {'':<10} {'':<10} {win_rate_25 - win_rate_24:>+8.1f}pt\n")

    # ========================================
    # 3-1. 成約商談の特徴（施設タイプ別）
    # ========================================
    f.write("\n" + "=" * 80 + "\n")
    f.write("3-1. 成約 vs 失注 施設タイプ別構成比（2025年度）\n")
    f.write("=" * 80 + "\n\n")

    facility_col = 'FacilityType_Large__c'

    won_facility = df_won_25[facility_col].value_counts(normalize=True) * 100
    lost_facility = df_lost_25[facility_col].value_counts(normalize=True) * 100

    f.write("【施設タイプ別 構成比】\n")
    f.write("-" * 80 + "\n")
    f.write(f"{'施設タイプ':<25} {'成約':>10} {'失注':>10} {'差分':>10}\n")
    f.write("-" * 80 + "\n")

    all_facilities = set(won_facility.index) | set(lost_facility.index)
    facility_comp = []
    for fac in all_facilities:
        if pd.isna(fac):
            continue
        won_pct = won_facility.get(fac, 0)
        lost_pct = lost_facility.get(fac, 0)
        diff = won_pct - lost_pct
        facility_comp.append((fac, won_pct, lost_pct, diff))

    facility_comp.sort(key=lambda x: x[3], reverse=True)

    for fac, won_pct, lost_pct, diff in facility_comp:
        sign = "+" if diff > 0 else ""
        fac_name = str(fac)[:23] if len(str(fac)) > 23 else fac
        f.write(f"  {fac_name:<23} {won_pct:>8.1f}% {lost_pct:>8.1f}% {sign}{diff:>8.1f}%\n")

    # ========================================
    # 3-1b. 成約商談の特徴（金額別）
    # ========================================
    f.write("\n" + "=" * 80 + "\n")
    f.write("3-1b. 成約 vs 失注 金額別構成比（2025年度）\n")
    f.write("=" * 80 + "\n\n")

    def amount_band(amount):
        if pd.isna(amount) or amount == 0:
            return '0_未設定'
        elif amount < 50000:
            return '1_5万円未満'
        elif amount < 100000:
            return '2_5-10万円'
        elif amount < 200000:
            return '3_10-20万円'
        elif amount < 500000:
            return '4_20-50万円'
        else:
            return '5_50万円以上'

    df_won_25['AmountBand'] = df_won_25['Amount'].apply(amount_band)
    df_lost_25['AmountBand'] = df_lost_25['Amount'].apply(amount_band)

    won_amount = df_won_25['AmountBand'].value_counts(normalize=True) * 100
    lost_amount = df_lost_25['AmountBand'].value_counts(normalize=True) * 100

    f.write("【金額帯別 構成比】\n")
    f.write("-" * 80 + "\n")
    f.write(f"{'金額帯':<20} {'成約':>10} {'失注':>10} {'差分':>10}\n")
    f.write("-" * 80 + "\n")

    for band in ['0_未設定', '1_5万円未満', '2_5-10万円', '3_10-20万円', '4_20-50万円', '5_50万円以上']:
        won_pct = won_amount.get(band, 0)
        lost_pct = lost_amount.get(band, 0)
        diff = won_pct - lost_pct
        sign = "+" if diff > 0 else ""
        band_name = band.split('_')[1] if '_' in band else band
        f.write(f"  {band_name:<18} {won_pct:>8.1f}% {lost_pct:>8.1f}% {sign}{diff:>8.1f}%\n")

    # ========================================
    # 3-1c. 成約商談の特徴（リードタイム別）
    # ========================================
    f.write("\n" + "=" * 80 + "\n")
    f.write("3-1c. 成約 vs 失注 リードタイム別構成比（2025年度）\n")
    f.write("=" * 80 + "\n\n")

    def leadtime_band(days):
        if pd.isna(days):
            return '0_未設定'
        elif days <= 7:
            return '1_1週間以内'
        elif days <= 14:
            return '2_2週間以内'
        elif days <= 30:
            return '3_1ヶ月以内'
        elif days <= 60:
            return '4_2ヶ月以内'
        elif days <= 90:
            return '5_3ヶ月以内'
        else:
            return '6_3ヶ月超'

    df_won_25['LeadTimeBand'] = df_won_25['LeadTime__c'].apply(leadtime_band)
    df_lost_25['LeadTimeBand'] = df_lost_25['LeadTime__c'].apply(leadtime_band)

    won_lt = df_won_25['LeadTimeBand'].value_counts(normalize=True) * 100
    lost_lt = df_lost_25['LeadTimeBand'].value_counts(normalize=True) * 100

    f.write("【リードタイム別 構成比】\n")
    f.write("-" * 80 + "\n")
    f.write(f"{'リードタイム':<20} {'成約':>10} {'失注':>10} {'差分':>10}\n")
    f.write("-" * 80 + "\n")

    for band in ['0_未設定', '1_1週間以内', '2_2週間以内', '3_1ヶ月以内', '4_2ヶ月以内', '5_3ヶ月以内', '6_3ヶ月超']:
        won_pct = won_lt.get(band, 0)
        lost_pct = lost_lt.get(band, 0)
        diff = won_pct - lost_pct
        sign = "+" if diff > 0 else ""
        band_name = band.split('_')[1] if '_' in band else band
        f.write(f"  {band_name:<18} {won_pct:>8.1f}% {lost_pct:>8.1f}% {sign}{diff:>8.1f}%\n")

    # ========================================
    # 3-2. 到達フェーズ別 失注理由
    # ========================================
    f.write("\n" + "=" * 80 + "\n")
    f.write("3-2. 到達フェーズ別 失注理由（2025年度）\n")
    f.write("=" * 80 + "\n\n")

    # LastReachedStage__c を使用
    stage_col = 'LastReachedStage__c'

    f.write("【到達フェーズ別 失注件数・懸念系率】\n")
    f.write("-" * 80 + "\n")
    f.write(f"{'到達フェーズ':<30} {'件数':>10} {'懸念系':>10} {'率':>10}\n")
    f.write("-" * 80 + "\n")

    stage_stats = df_lost_25.groupby(stage_col).agg({
        'Id': 'count',
        'LostReason_Large__c': lambda x: (x == concern_category).sum()
    }).reset_index()
    stage_stats.columns = ['Stage', 'Total', 'ConcernCount']
    stage_stats['ConcernRate'] = stage_stats['ConcernCount'] / stage_stats['Total'] * 100
    stage_stats = stage_stats.sort_values('Total', ascending=False)

    for _, row in stage_stats.iterrows():
        stage_name = str(row['Stage'])[:28] if len(str(row['Stage'])) > 28 else row['Stage']
        f.write(f"  {stage_name:<28} {int(row['Total']):>10}件 {int(row['ConcernCount']):>10}件 {row['ConcernRate']:>8.1f}%\n")

    # ========================================
    # 3-3. 商談タイプ別 成約率
    # ========================================
    f.write("\n" + "=" * 80 + "\n")
    f.write("3-3. 商談タイプ別 成約率（年度比較）\n")
    f.write("=" * 80 + "\n\n")

    opp_type_col = 'OpportunityType__c'

    f.write("【商談タイプ別 成約率】\n")
    f.write("-" * 90 + "\n")
    f.write(f"{'商談タイプ':<25} {'2024成約率':>12} {'2025成約率':>12} {'差分':>10}\n")
    f.write("-" * 90 + "\n")

    for opp_type in ['担当者商談', '代表者商談', '担当者商談（決裁者）', '代表者商談（決裁者）']:
        won_24 = len(df_won_24[df_won_24[opp_type_col] == opp_type])
        lost_24 = len(df_lost_24[df_lost_24[opp_type_col] == opp_type])
        total_24 = won_24 + lost_24
        rate_24 = won_24 / total_24 * 100 if total_24 > 0 else 0

        won_25 = len(df_won_25[df_won_25[opp_type_col] == opp_type])
        lost_25 = len(df_lost_25[df_lost_25[opp_type_col] == opp_type])
        total_25 = won_25 + lost_25
        rate_25 = won_25 / total_25 * 100 if total_25 > 0 else 0

        diff = rate_25 - rate_24
        sign = "+" if diff > 0 else ""

        if total_24 > 0 or total_25 > 0:
            f.write(f"  {opp_type:<23} {rate_24:>10.1f}% {rate_25:>10.1f}% {sign}{diff:>8.1f}pt\n")

    # ========================================
    # 3-4. 営業人員別 成約率（年度比較）
    # ========================================
    f.write("\n" + "=" * 80 + "\n")
    f.write("3-4. 営業人員別 成約率（2025年度、30件以上）\n")
    f.write("=" * 80 + "\n\n")

    # 2025年度の全商談を結合
    df_all_25 = pd.concat([
        df_won_25.assign(IsWon=True),
        df_lost_25.assign(IsWon=False)
    ])

    owner_stats = df_all_25.groupby('Owner.Name').agg({
        'Id': 'count',
        'IsWon': 'sum'
    }).reset_index()
    owner_stats.columns = ['Owner', 'Total', 'WonCount']
    owner_stats['WinRate'] = owner_stats['WonCount'] / owner_stats['Total'] * 100
    owner_stats = owner_stats[owner_stats['Total'] >= 30]
    owner_stats = owner_stats.sort_values('WinRate', ascending=False)

    f.write("【成約率 TOP15】\n")
    f.write("-" * 70 + "\n")
    f.write(f"{'営業人員':<15} {'全商談':>10} {'成約':>10} {'成約率':>10}\n")
    f.write("-" * 70 + "\n")

    for _, row in owner_stats.head(15).iterrows():
        f.write(f"  {row['Owner']:<13} {int(row['Total']):>10}件 {int(row['WonCount']):>10}件 {row['WinRate']:>8.1f}%\n")

    f.write("\n【成約率 WORST15】\n")
    f.write("-" * 70 + "\n")
    f.write(f"{'営業人員':<15} {'全商談':>10} {'成約':>10} {'成約率':>10}\n")
    f.write("-" * 70 + "\n")

    for _, row in owner_stats.tail(15).iterrows():
        f.write(f"  {row['Owner']:<13} {int(row['Total']):>10}件 {int(row['WonCount']):>10}件 {row['WinRate']:>8.1f}%\n")

    # ========================================
    # Phase 3 サマリー
    # ========================================
    f.write("\n" + "=" * 80 + "\n")
    f.write("Phase 3 サマリー\n")
    f.write("=" * 80 + "\n\n")

    f.write("【成約率の変化】\n")
    f.write(f"  ・全体: {win_rate_24:.1f}% → {win_rate_25:.1f}% ({win_rate_25 - win_rate_24:+.1f}pt)\n\n")

    f.write("【成約しやすい案件特性（成約に多く、失注に少ない）】\n")
    # 成約に多い施設タイプ
    best_facility = max(facility_comp, key=lambda x: x[3])
    f.write(f"  ・施設タイプ: {best_facility[0]}（成約{best_facility[1]:.1f}% vs 失注{best_facility[2]:.1f}%）\n")

    f.write("\n【成約率TOP/WORST】\n")
    top_owner = owner_stats.head(1).iloc[0]
    worst_owner = owner_stats.tail(1).iloc[0]
    f.write(f"  ・TOP: {top_owner['Owner']} ({top_owner['WinRate']:.1f}%)\n")
    f.write(f"  ・WORST: {worst_owner['Owner']} ({worst_owner['WinRate']:.1f}%)\n")

    f.write("\n" + "=" * 80 + "\n")
    f.write("Phase 3 完了\n")
    f.write("=" * 80 + "\n")

print(f"Phase 3 レポート出力完了: {report_file}")
