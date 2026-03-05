"""
深掘り分析 Phase 2: 案件特性の検証
2-1. 施設タイプ別
2-2. 金額別
2-3. リードタイム別
"""

import pandas as pd
import numpy as np
from pathlib import Path

# 出力設定
output_dir = Path(r"C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List\data\output\analysis")
report_file = output_dir / 'deep_phase2_report.txt'

# データ読み込み
data_path = output_dir / 'lost_opportunity_full_20260126.csv'
df = pd.read_csv(data_path, encoding='utf-8-sig', low_memory=False)

# 日付変換
df['CloseDate'] = pd.to_datetime(df['CloseDate'])

# 期間分割
df_fy2024 = df[(df['CloseDate'] >= '2024-04-01') & (df['CloseDate'] < '2025-04-01')].copy()
df_fy2025 = df[(df['CloseDate'] >= '2025-04-01') & (df['CloseDate'] <= '2026-01-27')].copy()

# 懸念系カテゴリ
concern_category = "サービスの価値は感じているが、懸念点を払拭できなかった"

with open(report_file, 'w', encoding='utf-8') as f:
    f.write("=" * 80 + "\n")
    f.write("深掘り分析 Phase 2: 案件特性の検証\n")
    f.write("=" * 80 + "\n\n")

    # ========================================
    # 2-1. 施設タイプ別
    # ========================================
    f.write("=" * 80 + "\n")
    f.write("2-1. 施設タイプ別（大分類）懸念系失注率\n")
    f.write("=" * 80 + "\n\n")

    # 施設タイプ大分類
    facility_col = 'FacilityType_Large__c'

    for year_label, year_df in [('2024年度', df_fy2024), ('2025年度', df_fy2025)]:
        f.write(f"■ {year_label}\n")
        f.write("-" * 80 + "\n")
        f.write(f"{'施設タイプ':<25} {'件数':>10} {'懸念系':>10} {'率':>10}\n")
        f.write("-" * 80 + "\n")

        facility_stats = year_df.groupby(facility_col).agg({
            'Id': 'count',
            'LostReason_Large__c': lambda x: (x == concern_category).sum()
        }).reset_index()
        facility_stats.columns = ['Facility', 'Total', 'ConcernCount']
        facility_stats['ConcernRate'] = facility_stats['ConcernCount'] / facility_stats['Total'] * 100
        facility_stats = facility_stats[facility_stats['Total'] >= 30]  # 30件以上
        facility_stats = facility_stats.sort_values('ConcernRate', ascending=False)

        for _, row in facility_stats.iterrows():
            facility_name = str(row['Facility'])[:23] if len(str(row['Facility'])) > 23 else row['Facility']
            f.write(f"  {facility_name:<23} {int(row['Total']):>10}件 {int(row['ConcernCount']):>10}件 {row['ConcernRate']:>8.1f}%\n")
        f.write("\n")

    # 年度間比較
    f.write("【施設タイプ別 年度間比較】\n")
    f.write("-" * 90 + "\n")
    f.write(f"{'施設タイプ':<25} {'2024率':>10} {'2025率':>10} {'差分':>10}\n")
    f.write("-" * 90 + "\n")

    # 2024年度
    facility_24 = df_fy2024.groupby(facility_col).agg({
        'Id': 'count',
        'LostReason_Large__c': lambda x: (x == concern_category).sum()
    }).reset_index()
    facility_24.columns = ['Facility', 'Total_24', 'Concern_24']
    facility_24['Rate_24'] = facility_24['Concern_24'] / facility_24['Total_24'] * 100

    # 2025年度
    facility_25 = df_fy2025.groupby(facility_col).agg({
        'Id': 'count',
        'LostReason_Large__c': lambda x: (x == concern_category).sum()
    }).reset_index()
    facility_25.columns = ['Facility', 'Total_25', 'Concern_25']
    facility_25['Rate_25'] = facility_25['Concern_25'] / facility_25['Total_25'] * 100

    facility_comp = facility_24.merge(facility_25, on='Facility', how='outer')
    facility_comp['Diff'] = facility_comp['Rate_25'] - facility_comp['Rate_24']
    facility_comp = facility_comp[(facility_comp['Total_24'] >= 30) | (facility_comp['Total_25'] >= 30)]
    facility_comp = facility_comp.sort_values('Diff', ascending=False)

    for _, row in facility_comp.iterrows():
        facility_name = str(row['Facility'])[:23] if len(str(row['Facility'])) > 23 else row['Facility']
        rate_24 = f"{row['Rate_24']:.1f}%" if pd.notna(row['Rate_24']) else "N/A"
        rate_25 = f"{row['Rate_25']:.1f}%" if pd.notna(row['Rate_25']) else "N/A"
        diff = f"{row['Diff']:+.1f}pt" if pd.notna(row['Diff']) else "N/A"
        f.write(f"  {facility_name:<23} {rate_24:>10} {rate_25:>10} {diff:>10}\n")

    # ========================================
    # 2-2. 金額別
    # ========================================
    f.write("\n" + "=" * 80 + "\n")
    f.write("2-2. 金額別 懸念系失注率\n")
    f.write("=" * 80 + "\n\n")

    # 金額帯を作成
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

    df_fy2024['AmountBand'] = df_fy2024['Amount'].apply(amount_band)
    df_fy2025['AmountBand'] = df_fy2025['Amount'].apply(amount_band)

    for year_label, year_df in [('2024年度', df_fy2024), ('2025年度', df_fy2025)]:
        f.write(f"■ {year_label}\n")
        f.write("-" * 70 + "\n")
        f.write(f"{'金額帯':<20} {'件数':>10} {'懸念系':>10} {'率':>10}\n")
        f.write("-" * 70 + "\n")

        amount_stats = year_df.groupby('AmountBand').agg({
            'Id': 'count',
            'LostReason_Large__c': lambda x: (x == concern_category).sum()
        }).reset_index()
        amount_stats.columns = ['AmountBand', 'Total', 'ConcernCount']
        amount_stats['ConcernRate'] = amount_stats['ConcernCount'] / amount_stats['Total'] * 100
        amount_stats = amount_stats.sort_values('AmountBand')

        for _, row in amount_stats.iterrows():
            band_name = row['AmountBand'].split('_')[1] if '_' in str(row['AmountBand']) else row['AmountBand']
            f.write(f"  {band_name:<18} {int(row['Total']):>10}件 {int(row['ConcernCount']):>10}件 {row['ConcernRate']:>8.1f}%\n")
        f.write("\n")

    # 年度間比較
    f.write("【金額帯別 年度間比較】\n")
    f.write("-" * 80 + "\n")
    f.write(f"{'金額帯':<20} {'2024率':>10} {'2025率':>10} {'差分':>10}\n")
    f.write("-" * 80 + "\n")

    amount_24 = df_fy2024.groupby('AmountBand').agg({
        'Id': 'count',
        'LostReason_Large__c': lambda x: (x == concern_category).sum()
    }).reset_index()
    amount_24.columns = ['AmountBand', 'Total_24', 'Concern_24']
    amount_24['Rate_24'] = amount_24['Concern_24'] / amount_24['Total_24'] * 100

    amount_25 = df_fy2025.groupby('AmountBand').agg({
        'Id': 'count',
        'LostReason_Large__c': lambda x: (x == concern_category).sum()
    }).reset_index()
    amount_25.columns = ['AmountBand', 'Total_25', 'Concern_25']
    amount_25['Rate_25'] = amount_25['Concern_25'] / amount_25['Total_25'] * 100

    amount_comp = amount_24.merge(amount_25, on='AmountBand', how='outer')
    amount_comp['Diff'] = amount_comp['Rate_25'] - amount_comp['Rate_24']
    amount_comp = amount_comp.sort_values('AmountBand')

    for _, row in amount_comp.iterrows():
        band_name = row['AmountBand'].split('_')[1] if '_' in str(row['AmountBand']) else row['AmountBand']
        rate_24 = f"{row['Rate_24']:.1f}%" if pd.notna(row['Rate_24']) else "N/A"
        rate_25 = f"{row['Rate_25']:.1f}%" if pd.notna(row['Rate_25']) else "N/A"
        diff = f"{row['Diff']:+.1f}pt" if pd.notna(row['Diff']) else "N/A"
        f.write(f"  {band_name:<18} {rate_24:>10} {rate_25:>10} {diff:>10}\n")

    # ========================================
    # 2-3. リードタイム別
    # ========================================
    f.write("\n" + "=" * 80 + "\n")
    f.write("2-3. リードタイム別 懸念系失注率\n")
    f.write("=" * 80 + "\n\n")

    # LeadTime__c を使用
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

    df_fy2024['LeadTimeBand'] = df_fy2024['LeadTime__c'].apply(leadtime_band)
    df_fy2025['LeadTimeBand'] = df_fy2025['LeadTime__c'].apply(leadtime_band)

    for year_label, year_df in [('2024年度', df_fy2024), ('2025年度', df_fy2025)]:
        f.write(f"■ {year_label}\n")
        f.write("-" * 70 + "\n")
        f.write(f"{'リードタイム':<20} {'件数':>10} {'懸念系':>10} {'率':>10}\n")
        f.write("-" * 70 + "\n")

        lt_stats = year_df.groupby('LeadTimeBand').agg({
            'Id': 'count',
            'LostReason_Large__c': lambda x: (x == concern_category).sum()
        }).reset_index()
        lt_stats.columns = ['LeadTimeBand', 'Total', 'ConcernCount']
        lt_stats['ConcernRate'] = lt_stats['ConcernCount'] / lt_stats['Total'] * 100
        lt_stats = lt_stats.sort_values('LeadTimeBand')

        for _, row in lt_stats.iterrows():
            band_name = row['LeadTimeBand'].split('_')[1] if '_' in str(row['LeadTimeBand']) else row['LeadTimeBand']
            f.write(f"  {band_name:<18} {int(row['Total']):>10}件 {int(row['ConcernCount']):>10}件 {row['ConcernRate']:>8.1f}%\n")
        f.write("\n")

    # 年度間比較
    f.write("【リードタイム別 年度間比較】\n")
    f.write("-" * 80 + "\n")
    f.write(f"{'リードタイム':<20} {'2024率':>10} {'2025率':>10} {'差分':>10}\n")
    f.write("-" * 80 + "\n")

    lt_24 = df_fy2024.groupby('LeadTimeBand').agg({
        'Id': 'count',
        'LostReason_Large__c': lambda x: (x == concern_category).sum()
    }).reset_index()
    lt_24.columns = ['LeadTimeBand', 'Total_24', 'Concern_24']
    lt_24['Rate_24'] = lt_24['Concern_24'] / lt_24['Total_24'] * 100

    lt_25 = df_fy2025.groupby('LeadTimeBand').agg({
        'Id': 'count',
        'LostReason_Large__c': lambda x: (x == concern_category).sum()
    }).reset_index()
    lt_25.columns = ['LeadTimeBand', 'Total_25', 'Concern_25']
    lt_25['Rate_25'] = lt_25['Concern_25'] / lt_25['Total_25'] * 100

    lt_comp = lt_24.merge(lt_25, on='LeadTimeBand', how='outer')
    lt_comp['Diff'] = lt_comp['Rate_25'] - lt_comp['Rate_24']
    lt_comp = lt_comp.sort_values('LeadTimeBand')

    for _, row in lt_comp.iterrows():
        band_name = row['LeadTimeBand'].split('_')[1] if '_' in str(row['LeadTimeBand']) else row['LeadTimeBand']
        rate_24 = f"{row['Rate_24']:.1f}%" if pd.notna(row['Rate_24']) else "N/A"
        rate_25 = f"{row['Rate_25']:.1f}%" if pd.notna(row['Rate_25']) else "N/A"
        diff = f"{row['Diff']:+.1f}pt" if pd.notna(row['Diff']) else "N/A"
        f.write(f"  {band_name:<18} {rate_24:>10} {rate_25:>10} {diff:>10}\n")

    # ========================================
    # Phase 2 サマリー
    # ========================================
    f.write("\n" + "=" * 80 + "\n")
    f.write("Phase 2 サマリー\n")
    f.write("=" * 80 + "\n\n")

    # 施設タイプで最も悪化したもの
    worst_facility = facility_comp[facility_comp['Diff'].notna()].sort_values('Diff', ascending=False).head(3)
    f.write("【施設タイプ - 懸念系失注率の悪化TOP3】\n")
    for _, row in worst_facility.iterrows():
        f.write(f"  ・{row['Facility']}: {row['Diff']:+.1f}pt\n")

    f.write("\n【金額帯 - 懸念系失注率の傾向】\n")
    # 50万以上の変化
    high_amount = amount_comp[amount_comp['AmountBand'] == '5_50万円以上']
    if len(high_amount) > 0:
        row = high_amount.iloc[0]
        f.write(f"  ・50万円以上: {row['Rate_24']:.1f}% → {row['Rate_25']:.1f}% ({row['Diff']:+.1f}pt)\n")

    f.write("\n【リードタイム - 懸念系失注率の傾向】\n")
    # 長期商談の変化
    long_lt = lt_comp[lt_comp['LeadTimeBand'] == '6_3ヶ月超']
    if len(long_lt) > 0:
        row = long_lt.iloc[0]
        f.write(f"  ・3ヶ月超: {row['Rate_24']:.1f}% → {row['Rate_25']:.1f}% ({row['Diff']:+.1f}pt)\n")

    f.write("\n" + "=" * 80 + "\n")
    f.write("Phase 2 完了\n")
    f.write("=" * 80 + "\n")

print(f"Phase 2 レポート出力完了: {report_file}")
