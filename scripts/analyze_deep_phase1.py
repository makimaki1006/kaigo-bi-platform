"""
深掘り分析 Phase 1: 営業スキル問題の検証
1-1. 営業人員別 懸念系失注率
1-2. 商談フェーズ別（担当者商談 vs 代表者商談）
1-3. 懸念系 小分類詳細
"""

import pandas as pd
from pathlib import Path

# 出力設定
output_dir = Path(r"C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List\data\output\analysis")
report_file = output_dir / 'deep_phase1_report.txt'

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
    f.write("深掘り分析 Phase 1: 営業スキル問題の検証\n")
    f.write("=" * 80 + "\n\n")

    # ========================================
    # 1-1. 営業人員別 懸念系失注率
    # ========================================
    f.write("=" * 80 + "\n")
    f.write("1-1. 営業人員別 懸念系失注率\n")
    f.write("=" * 80 + "\n\n")

    # 2025年度の営業人員別分析（件数が多い人のみ）
    owner_stats_25 = df_fy2025.groupby('Owner.Name').agg({
        'Id': 'count',
        'LostReason_Large__c': lambda x: (x == concern_category).sum()
    }).reset_index()
    owner_stats_25.columns = ['Owner', 'Total', 'ConcernCount']
    owner_stats_25['ConcernRate'] = owner_stats_25['ConcernCount'] / owner_stats_25['Total'] * 100
    owner_stats_25 = owner_stats_25[owner_stats_25['Total'] >= 30]  # 30件以上
    owner_stats_25 = owner_stats_25.sort_values('ConcernRate', ascending=False)

    # 2024年度も同様に計算
    owner_stats_24 = df_fy2024.groupby('Owner.Name').agg({
        'Id': 'count',
        'LostReason_Large__c': lambda x: (x == concern_category).sum()
    }).reset_index()
    owner_stats_24.columns = ['Owner', 'Total_24', 'ConcernCount_24']
    owner_stats_24['ConcernRate_24'] = owner_stats_24['ConcernCount_24'] / owner_stats_24['Total_24'] * 100

    # マージして年度間比較
    owner_comparison = owner_stats_25.merge(owner_stats_24[['Owner', 'ConcernRate_24', 'Total_24']],
                                             on='Owner', how='left')
    owner_comparison['Diff'] = owner_comparison['ConcernRate'] - owner_comparison['ConcernRate_24']
    owner_comparison = owner_comparison.sort_values('ConcernRate', ascending=False)

    f.write("【2025年度 懸念系失注率 TOP15（30件以上）】\n")
    f.write("-" * 90 + "\n")
    f.write(f"{'営業人員':<15} {'2025件数':>10} {'懸念系':>8} {'率':>8} {'2024率':>8} {'差分':>8}\n")
    f.write("-" * 90 + "\n")

    for _, row in owner_comparison.head(15).iterrows():
        rate_24 = f"{row['ConcernRate_24']:.1f}%" if pd.notna(row['ConcernRate_24']) else "N/A"
        diff = f"{row['Diff']:+.1f}pt" if pd.notna(row['Diff']) else "N/A"
        f.write(f"  {row['Owner']:<13} {int(row['Total']):>10}件 {int(row['ConcernCount']):>8}件 {row['ConcernRate']:>6.1f}% {rate_24:>8} {diff:>8}\n")

    # 全体平均
    avg_concern_rate_25 = df_fy2025['LostReason_Large__c'].apply(lambda x: x == concern_category).mean() * 100
    avg_concern_rate_24 = df_fy2024['LostReason_Large__c'].apply(lambda x: x == concern_category).mean() * 100

    f.write("-" * 90 + "\n")
    f.write(f"  {'【全体平均】':<13} {'':<10} {'':<8} {avg_concern_rate_25:>6.1f}% {avg_concern_rate_24:>6.1f}% {avg_concern_rate_25 - avg_concern_rate_24:>+6.1f}pt\n")

    # 悪化が大きい人
    f.write("\n【年度間で懸念系失注率が大きく悪化した人（+10pt以上）】\n")
    f.write("-" * 90 + "\n")

    worsened = owner_comparison[owner_comparison['Diff'] >= 10].sort_values('Diff', ascending=False)
    if len(worsened) > 0:
        for _, row in worsened.iterrows():
            f.write(f"  {row['Owner']:<13} 2024: {row['ConcernRate_24']:.1f}% → 2025: {row['ConcernRate']:.1f}% ({row['Diff']:+.1f}pt)\n")
    else:
        f.write("  該当者なし\n")

    # 改善した人
    f.write("\n【年度間で懸念系失注率が改善した人（-5pt以上）】\n")
    f.write("-" * 90 + "\n")

    improved = owner_comparison[owner_comparison['Diff'] <= -5].sort_values('Diff')
    if len(improved) > 0:
        for _, row in improved.iterrows():
            f.write(f"  {row['Owner']:<13} 2024: {row['ConcernRate_24']:.1f}% → 2025: {row['ConcernRate']:.1f}% ({row['Diff']:+.1f}pt)\n")
    else:
        f.write("  該当者なし\n")

    # ========================================
    # 1-2. 商談フェーズ別（担当者商談 vs 代表者商談）
    # ========================================
    f.write("\n" + "=" * 80 + "\n")
    f.write("1-2. 商談フェーズ別（担当者商談 vs 代表者商談）\n")
    f.write("=" * 80 + "\n\n")

    f.write("【OpportunityType__c 別 懸念系失注率】\n\n")

    for year_label, year_df in [('2024年度', df_fy2024), ('2025年度', df_fy2025)]:
        f.write(f"■ {year_label}\n")
        f.write("-" * 70 + "\n")
        f.write(f"{'商談タイプ':<25} {'件数':>10} {'懸念系':>10} {'率':>10}\n")
        f.write("-" * 70 + "\n")

        for opp_type in ['担当者商談', '代表者商談', '担当者商談（決裁者）', '代表者商談（決裁者）']:
            type_df = year_df[year_df['OpportunityType__c'] == opp_type]
            if len(type_df) > 0:
                concern_count = (type_df['LostReason_Large__c'] == concern_category).sum()
                concern_rate = concern_count / len(type_df) * 100
                f.write(f"  {opp_type:<23} {len(type_df):>10}件 {concern_count:>10}件 {concern_rate:>8.1f}%\n")
        f.write("\n")

    # 年度間比較（担当者商談）
    f.write("【担当者商談 年度間比較】\n")
    tantou_24 = df_fy2024[df_fy2024['OpportunityType__c'] == '担当者商談']
    tantou_25 = df_fy2025[df_fy2025['OpportunityType__c'] == '担当者商談']
    tantou_concern_24 = (tantou_24['LostReason_Large__c'] == concern_category).mean() * 100
    tantou_concern_25 = (tantou_25['LostReason_Large__c'] == concern_category).mean() * 100
    f.write(f"  2024年度: {tantou_concern_24:.1f}% → 2025年度: {tantou_concern_25:.1f}% ({tantou_concern_25 - tantou_concern_24:+.1f}pt)\n\n")

    # 年度間比較（代表者商談）
    f.write("【代表者商談 年度間比較】\n")
    daihyo_24 = df_fy2024[df_fy2024['OpportunityType__c'] == '代表者商談']
    daihyo_25 = df_fy2025[df_fy2025['OpportunityType__c'] == '代表者商談']
    daihyo_concern_24 = (daihyo_24['LostReason_Large__c'] == concern_category).mean() * 100 if len(daihyo_24) > 0 else 0
    daihyo_concern_25 = (daihyo_25['LostReason_Large__c'] == concern_category).mean() * 100 if len(daihyo_25) > 0 else 0
    f.write(f"  2024年度: {daihyo_concern_24:.1f}% → 2025年度: {daihyo_concern_25:.1f}% ({daihyo_concern_25 - daihyo_concern_24:+.1f}pt)\n")

    # ========================================
    # 1-3. 懸念系 小分類詳細
    # ========================================
    f.write("\n" + "=" * 80 + "\n")
    f.write("1-3. 懸念系 小分類詳細（年度間比較）\n")
    f.write("=" * 80 + "\n\n")

    # 懸念系のみ抽出
    concern_24 = df_fy2024[df_fy2024['LostReason_Large__c'] == concern_category]
    concern_25 = df_fy2025[df_fy2025['LostReason_Large__c'] == concern_category]

    f.write(f"懸念系失注件数: 2024年度 {len(concern_24)}件 → 2025年度 {len(concern_25)}件\n\n")

    # 小分類の構成比
    small_24 = concern_24['LostReason_Small__c'].value_counts(normalize=True) * 100
    small_25 = concern_25['LostReason_Small__c'].value_counts(normalize=True) * 100

    # 件数も取得
    small_24_count = concern_24['LostReason_Small__c'].value_counts()
    small_25_count = concern_25['LostReason_Small__c'].value_counts()

    all_small = set(small_24.index) | set(small_25.index)
    comparison = []
    for cat in all_small:
        pct_24 = small_24.get(cat, 0)
        pct_25 = small_25.get(cat, 0)
        cnt_24 = small_24_count.get(cat, 0)
        cnt_25 = small_25_count.get(cat, 0)
        diff = pct_25 - pct_24
        comparison.append((cat, cnt_24, pct_24, cnt_25, pct_25, diff))

    comparison.sort(key=lambda x: x[4], reverse=True)  # 2025年度の率でソート

    f.write("【小分類 構成比の年度間比較】\n")
    f.write("-" * 100 + "\n")
    f.write(f"{'小分類':<40} {'2024件数':>8} {'2024率':>8} {'2025件数':>8} {'2025率':>8} {'差分':>8}\n")
    f.write("-" * 100 + "\n")

    for cat, cnt_24, pct_24, cnt_25, pct_25, diff in comparison:
        sign = "+" if diff > 0 else ""
        cat_display = cat[:38] if len(str(cat)) > 38 else cat
        f.write(f"  {cat_display:<38} {cnt_24:>8}件 {pct_24:>6.1f}% {cnt_25:>8}件 {pct_25:>6.1f}% {sign}{diff:>6.1f}%\n")

    # ========================================
    # 1-4. サマリー
    # ========================================
    f.write("\n" + "=" * 80 + "\n")
    f.write("Phase 1 サマリー\n")
    f.write("=" * 80 + "\n\n")

    f.write("【営業人員別】\n")
    high_concern = owner_comparison[owner_comparison['ConcernRate'] >= 25]
    f.write(f"  ・懸念系失注率25%以上の人: {len(high_concern)}名\n")
    f.write(f"  ・年度間で+10pt以上悪化した人: {len(worsened)}名\n")
    f.write(f"  ・年度間で-5pt以上改善した人: {len(improved)}名\n\n")

    f.write("【商談フェーズ別】\n")
    f.write(f"  ・担当者商談: {tantou_concern_24:.1f}% → {tantou_concern_25:.1f}% ({tantou_concern_25 - tantou_concern_24:+.1f}pt)\n")
    f.write(f"  ・代表者商談: {daihyo_concern_24:.1f}% → {daihyo_concern_25:.1f}% ({daihyo_concern_25 - daihyo_concern_24:+.1f}pt)\n\n")

    f.write("【懸念系 小分類】\n")
    # 最も増えた小分類
    top_increase = max(comparison, key=lambda x: x[5])
    top_decrease = min(comparison, key=lambda x: x[5])
    f.write(f"  ・最も増加: {top_increase[0]} ({top_increase[5]:+.1f}pt)\n")
    f.write(f"  ・最も減少: {top_decrease[0]} ({top_decrease[5]:+.1f}pt)\n")

    f.write("\n" + "=" * 80 + "\n")
    f.write("Phase 1 完了\n")
    f.write("=" * 80 + "\n")

print(f"Phase 1 レポート出力完了: {report_file}")

# CSVも出力
owner_comparison.to_csv(output_dir / 'phase1_owner_concern_comparison.csv', index=False, encoding='utf-8-sig')
print(f"CSV出力完了: phase1_owner_concern_comparison.csv")
