"""
失注理由 深掘り分析スクリプト v2
- OpportunityCategory__c（初回商談/再商談）を使用
- 年度別の再商談率変化
- 初回商談 vs 再商談の失注理由比較
"""

import pandas as pd
from pathlib import Path

# 出力設定
output_dir = Path(r"C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List\data\output\analysis")
report_file = output_dir / 'deep_analysis_v2_report.txt'

# データ読み込み
data_path = output_dir / 'lost_opportunity_full_20260126.csv'
df = pd.read_csv(data_path, encoding='utf-8-sig', low_memory=False)

# 日付変換
df['CloseDate'] = pd.to_datetime(df['CloseDate'])
df['CreatedDate'] = pd.to_datetime(df['CreatedDate'])

# 期間分割
df_fy2024 = df[(df['CloseDate'] >= '2024-04-01') & (df['CloseDate'] < '2025-04-01')].copy()
df_fy2025 = df[(df['CloseDate'] >= '2025-04-01') & (df['CloseDate'] <= '2026-01-27')].copy()

with open(report_file, 'w', encoding='utf-8') as f:
    f.write("=" * 80 + "\n")
    f.write("失注理由 深掘り分析レポート v2\n")
    f.write("OpportunityCategory__c（初回商談/再商談）による分析\n")
    f.write("=" * 80 + "\n\n")

    # ========================================
    # 1. 基本統計
    # ========================================
    f.write("=" * 80 + "\n")
    f.write("1. 基本統計\n")
    f.write("=" * 80 + "\n\n")

    f.write("【OpportunityCategory__c 全体分布】\n")
    cat_dist = df['OpportunityCategory__c'].value_counts(dropna=False)
    for val, cnt in cat_dist.items():
        pct = cnt / len(df) * 100
        f.write(f"  {val}: {cnt:,}件 ({pct:.1f}%)\n")

    # ========================================
    # 2. 年度別 再商談率の比較
    # ========================================
    f.write("\n" + "=" * 80 + "\n")
    f.write("2. 年度別 再商談率の比較\n")
    f.write("=" * 80 + "\n\n")

    # 2024年度
    fy2024_first = len(df_fy2024[df_fy2024['OpportunityCategory__c'] == '初回商談'])
    fy2024_re = len(df_fy2024[df_fy2024['OpportunityCategory__c'] == '再商談'])
    fy2024_total = len(df_fy2024)
    fy2024_re_rate = fy2024_re / fy2024_total * 100 if fy2024_total > 0 else 0

    # 2025年度
    fy2025_first = len(df_fy2025[df_fy2025['OpportunityCategory__c'] == '初回商談'])
    fy2025_re = len(df_fy2025[df_fy2025['OpportunityCategory__c'] == '再商談'])
    fy2025_total = len(df_fy2025)
    fy2025_re_rate = fy2025_re / fy2025_total * 100 if fy2025_total > 0 else 0

    f.write("【年度別 商談カテゴリ内訳】\n")
    f.write("-" * 60 + "\n")
    f.write(f"{'年度':<15} {'初回商談':>12} {'再商談':>12} {'再商談率':>12}\n")
    f.write("-" * 60 + "\n")
    f.write(f"  2024年度      {fy2024_first:>10,}件 {fy2024_re:>10,}件 {fy2024_re_rate:>10.1f}%\n")
    f.write(f"  2025年度      {fy2025_first:>10,}件 {fy2025_re:>10,}件 {fy2025_re_rate:>10.1f}%\n")
    f.write("-" * 60 + "\n")
    f.write(f"  差分                                        {fy2025_re_rate - fy2024_re_rate:>+10.1f}pt\n")

    # ========================================
    # 3. 月別 再商談率推移
    # ========================================
    f.write("\n" + "=" * 80 + "\n")
    f.write("3. 月別 再商談率推移\n")
    f.write("=" * 80 + "\n\n")

    df['YearMonth'] = df['CloseDate'].dt.to_period('M')
    df_monthly = df[df['CloseDate'] >= '2024-04-01'].copy()

    monthly_stats = df_monthly.groupby('YearMonth').apply(
        lambda x: pd.Series({
            'Total': len(x),
            'FirstOpp': len(x[x['OpportunityCategory__c'] == '初回商談']),
            'ReOpp': len(x[x['OpportunityCategory__c'] == '再商談']),
        })
    ).reset_index()
    monthly_stats['ReOppRate'] = monthly_stats['ReOpp'] / monthly_stats['Total'] * 100

    f.write("【月別 再商談率推移】\n")
    f.write("-" * 70 + "\n")
    f.write(f"{'年月':<12} {'商談数':>10} {'初回商談':>10} {'再商談':>10} {'再商談率':>12}\n")
    f.write("-" * 70 + "\n")

    for _, row in monthly_stats.iterrows():
        f.write(f"  {str(row['YearMonth']):<10} {int(row['Total']):>10}件 {int(row['FirstOpp']):>10}件 {int(row['ReOpp']):>10}件 {row['ReOppRate']:>10.1f}%\n")

    # ========================================
    # 4. 初回商談 vs 再商談 失注理由比較
    # ========================================
    f.write("\n" + "=" * 80 + "\n")
    f.write("4. 初回商談 vs 再商談 失注理由比較（2025年度）\n")
    f.write("=" * 80 + "\n\n")

    first_opps = df_fy2025[df_fy2025['OpportunityCategory__c'] == '初回商談']
    re_opps = df_fy2025[df_fy2025['OpportunityCategory__c'] == '再商談']

    f.write(f"初回商談: {len(first_opps):,}件 / 再商談: {len(re_opps):,}件\n\n")

    f.write("【大分類構成比の比較】\n")
    f.write("-" * 85 + "\n")
    f.write(f"{'大分類':<45} {'初回':>10} {'再商談':>10} {'差分':>10}\n")
    f.write("-" * 85 + "\n")

    first_large = first_opps['LostReason_Large__c'].value_counts(normalize=True) * 100
    re_large = re_opps['LostReason_Large__c'].value_counts(normalize=True) * 100

    all_large_cats = set(first_large.index) | set(re_large.index)
    comparison = []
    for cat in all_large_cats:
        first_pct = first_large.get(cat, 0)
        re_pct = re_large.get(cat, 0)
        diff = re_pct - first_pct
        comparison.append((cat, first_pct, re_pct, diff))

    comparison.sort(key=lambda x: abs(x[3]), reverse=True)
    for cat, first_pct, re_pct, diff in comparison:
        sign = "+" if diff > 0 else ""
        cat_display = cat[:43] if len(str(cat)) > 43 else cat
        f.write(f"  {cat_display:<43} {first_pct:>8.1f}% {re_pct:>8.1f}% {sign}{diff:>8.1f}%\n")

    # ========================================
    # 5. 初回商談 vs 再商談 失注理由比較（2024年度）
    # ========================================
    f.write("\n" + "=" * 80 + "\n")
    f.write("5. 初回商談 vs 再商談 失注理由比較（2024年度）\n")
    f.write("=" * 80 + "\n\n")

    first_opps_24 = df_fy2024[df_fy2024['OpportunityCategory__c'] == '初回商談']
    re_opps_24 = df_fy2024[df_fy2024['OpportunityCategory__c'] == '再商談']

    f.write(f"初回商談: {len(first_opps_24):,}件 / 再商談: {len(re_opps_24):,}件\n\n")

    f.write("【大分類構成比の比較】\n")
    f.write("-" * 85 + "\n")
    f.write(f"{'大分類':<45} {'初回':>10} {'再商談':>10} {'差分':>10}\n")
    f.write("-" * 85 + "\n")

    first_large_24 = first_opps_24['LostReason_Large__c'].value_counts(normalize=True) * 100
    re_large_24 = re_opps_24['LostReason_Large__c'].value_counts(normalize=True) * 100

    all_large_cats_24 = set(first_large_24.index) | set(re_large_24.index)
    comparison_24 = []
    for cat in all_large_cats_24:
        first_pct = first_large_24.get(cat, 0)
        re_pct = re_large_24.get(cat, 0)
        diff = re_pct - first_pct
        comparison_24.append((cat, first_pct, re_pct, diff))

    comparison_24.sort(key=lambda x: abs(x[3]), reverse=True)
    for cat, first_pct, re_pct, diff in comparison_24:
        sign = "+" if diff > 0 else ""
        cat_display = cat[:43] if len(str(cat)) > 43 else cat
        f.write(f"  {cat_display:<43} {first_pct:>8.1f}% {re_pct:>8.1f}% {sign}{diff:>8.1f}%\n")

    # ========================================
    # 6. 懸念系失注の詳細（初回 vs 再商談）
    # ========================================
    f.write("\n" + "=" * 80 + "\n")
    f.write("6. 「懸念点払拭できず」の詳細分析（2025年度）\n")
    f.write("=" * 80 + "\n\n")

    concern_category = "サービスの価値は感じているが、懸念点を払拭できなかった"

    first_concern = first_opps[first_opps['LostReason_Large__c'] == concern_category]
    re_concern = re_opps[re_opps['LostReason_Large__c'] == concern_category]

    first_concern_rate = len(first_concern) / len(first_opps) * 100 if len(first_opps) > 0 else 0
    re_concern_rate = len(re_concern) / len(re_opps) * 100 if len(re_opps) > 0 else 0

    f.write(f"【懸念系失注率】\n")
    f.write(f"  初回商談: {len(first_concern)}件 ({first_concern_rate:.1f}%)\n")
    f.write(f"  再商談:   {len(re_concern)}件 ({re_concern_rate:.1f}%)\n")
    f.write(f"  差分:     {re_concern_rate - first_concern_rate:+.1f}pt\n\n")

    if len(first_concern) > 0 and len(re_concern) > 0:
        f.write("【小分類比較（懸念系のみ）】\n")
        f.write("-" * 85 + "\n")
        f.write(f"{'小分類':<45} {'初回':>10} {'再商談':>10} {'差分':>10}\n")
        f.write("-" * 85 + "\n")

        first_small = first_concern['LostReason_Small__c'].value_counts(normalize=True) * 100
        re_small = re_concern['LostReason_Small__c'].value_counts(normalize=True) * 100

        all_small = set(first_small.index) | set(re_small.index)
        small_comparison = []
        for cat in all_small:
            first_pct = first_small.get(cat, 0)
            re_pct = re_small.get(cat, 0)
            diff = re_pct - first_pct
            small_comparison.append((cat, first_pct, re_pct, diff))

        small_comparison.sort(key=lambda x: abs(x[3]), reverse=True)
        for cat, first_pct, re_pct, diff in small_comparison:
            sign = "+" if diff > 0 else ""
            cat_display = cat[:43] if len(str(cat)) > 43 else cat
            f.write(f"  {cat_display:<43} {first_pct:>8.1f}% {re_pct:>8.1f}% {sign}{diff:>8.1f}%\n")

    # ========================================
    # 7. 年度間比較（初回商談のみ）
    # ========================================
    f.write("\n" + "=" * 80 + "\n")
    f.write("7. 年度間比較（初回商談のみ）\n")
    f.write("=" * 80 + "\n\n")

    f.write("※再商談の影響を除外して、純粋な初回商談の変化を確認\n\n")

    f.write("【大分類構成比の比較（初回商談のみ）】\n")
    f.write("-" * 85 + "\n")
    f.write(f"{'大分類':<45} {'2024年度':>10} {'2025年度':>10} {'差分':>10}\n")
    f.write("-" * 85 + "\n")

    first_24_large = first_opps_24['LostReason_Large__c'].value_counts(normalize=True) * 100
    first_25_large = first_opps['LostReason_Large__c'].value_counts(normalize=True) * 100

    all_cats = set(first_24_large.index) | set(first_25_large.index)
    yoy_comparison = []
    for cat in all_cats:
        fy24_pct = first_24_large.get(cat, 0)
        fy25_pct = first_25_large.get(cat, 0)
        diff = fy25_pct - fy24_pct
        yoy_comparison.append((cat, fy24_pct, fy25_pct, diff))

    yoy_comparison.sort(key=lambda x: abs(x[3]), reverse=True)
    for cat, fy24_pct, fy25_pct, diff in yoy_comparison:
        sign = "+" if diff > 0 else ""
        cat_display = cat[:43] if len(str(cat)) > 43 else cat
        f.write(f"  {cat_display:<43} {fy24_pct:>8.1f}% {fy25_pct:>8.1f}% {sign}{diff:>8.1f}%\n")

    # ========================================
    # 8. 年度間比較（再商談のみ）
    # ========================================
    f.write("\n" + "=" * 80 + "\n")
    f.write("8. 年度間比較（再商談のみ）\n")
    f.write("=" * 80 + "\n\n")

    f.write("【大分類構成比の比較（再商談のみ）】\n")
    f.write("-" * 85 + "\n")
    f.write(f"{'大分類':<45} {'2024年度':>10} {'2025年度':>10} {'差分':>10}\n")
    f.write("-" * 85 + "\n")

    re_24_large = re_opps_24['LostReason_Large__c'].value_counts(normalize=True) * 100
    re_25_large = re_opps['LostReason_Large__c'].value_counts(normalize=True) * 100

    all_cats_re = set(re_24_large.index) | set(re_25_large.index)
    yoy_re_comparison = []
    for cat in all_cats_re:
        fy24_pct = re_24_large.get(cat, 0)
        fy25_pct = re_25_large.get(cat, 0)
        diff = fy25_pct - fy24_pct
        yoy_re_comparison.append((cat, fy24_pct, fy25_pct, diff))

    yoy_re_comparison.sort(key=lambda x: abs(x[3]), reverse=True)
    for cat, fy24_pct, fy25_pct, diff in yoy_re_comparison:
        sign = "+" if diff > 0 else ""
        cat_display = cat[:43] if len(str(cat)) > 43 else cat
        f.write(f"  {cat_display:<43} {fy24_pct:>8.1f}% {fy25_pct:>8.1f}% {sign}{diff:>8.1f}%\n")

    # ========================================
    # 9. 仮説検証サマリー
    # ========================================
    f.write("\n" + "=" * 80 + "\n")
    f.write("9. 仮説検証サマリー\n")
    f.write("=" * 80 + "\n\n")

    f.write("【仮説1】再商談（過去商談先）の割合が増えている\n")
    f.write(f"  → 再商談率: 2024年度 {fy2024_re_rate:.1f}% → 2025年度 {fy2025_re_rate:.1f}%\n")
    if fy2025_re_rate > fy2024_re_rate:
        f.write(f"  → 検証結果: ✅ 支持される（+{fy2025_re_rate - fy2024_re_rate:.1f}pt増加）\n")
    else:
        f.write(f"  → 検証結果: ❌ 支持されない（{fy2025_re_rate - fy2024_re_rate:+.1f}pt変化）\n")

    f.write("\n【仮説2】再商談は懸念系失注が多い（2025年度）\n")
    f.write(f"  → 懸念系失注率: 初回 {first_concern_rate:.1f}% vs 再商談 {re_concern_rate:.1f}%\n")
    if re_concern_rate > first_concern_rate + 3:  # 3pt以上の差があれば「支持」
        f.write(f"  → 検証結果: ✅ 支持される（再商談が+{re_concern_rate - first_concern_rate:.1f}pt高い）\n")
    elif re_concern_rate > first_concern_rate:
        f.write(f"  → 検証結果: △ 弱く支持（再商談が+{re_concern_rate - first_concern_rate:.1f}pt高い）\n")
    else:
        f.write(f"  → 検証結果: ❌ 支持されない\n")

    f.write("\n【仮説3】初回商談でも懸念系失注が増えている（構造問題とは別）\n")
    concern_24 = first_opps_24[first_opps_24['LostReason_Large__c'] == concern_category]
    concern_24_rate = len(concern_24) / len(first_opps_24) * 100 if len(first_opps_24) > 0 else 0
    f.write(f"  → 初回商談の懸念系失注率: 2024年度 {concern_24_rate:.1f}% → 2025年度 {first_concern_rate:.1f}%\n")
    if first_concern_rate > concern_24_rate:
        f.write(f"  → 検証結果: ✅ 初回商談でも懸念系が+{first_concern_rate - concern_24_rate:.1f}pt増加\n")
        f.write(f"     → これは「営業スキル」の問題を示唆\n")
    else:
        f.write(f"  → 検証結果: ❌ 初回商談では懸念系は増えていない\n")

    f.write("\n" + "=" * 80 + "\n")
    f.write("分析完了\n")
    f.write("=" * 80 + "\n")

print(f"レポート出力完了: {report_file}")
