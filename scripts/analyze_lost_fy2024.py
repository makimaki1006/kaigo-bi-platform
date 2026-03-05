"""
2024年度（2024年4月～2025年3月）の失注理由分析スクリプト
営業人員別の傾向、大分類・小分類の内訳を分析
"""

import pandas as pd
from pathlib import Path

# 出力ファイル設定
output_dir = Path(r"C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List\data\output\analysis")
report_file = output_dir / 'owner_analysis_fy2024_report.txt'

# データ読み込み
data_path = Path(r"C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List\data\output\analysis\lost_opportunity_full_20260126.csv")
df = pd.read_csv(data_path, encoding='utf-8-sig', low_memory=False)

# 日付変換
df['CloseDate'] = pd.to_datetime(df['CloseDate'])
df['LostDate__c'] = pd.to_datetime(df['LostDate__c'], errors='coerce')

# 2024年度（2024年4月～2025年3月）のデータに絞る
df_fy2024 = df[(df['CloseDate'] >= '2024-04-01') & (df['CloseDate'] < '2025-04-01')].copy()

with open(report_file, 'w', encoding='utf-8') as f:
    f.write("=" * 80 + "\n")
    f.write("2024年度 失注理由分析\n")
    f.write("対象期間: 2024年4月1日 〜 2025年3月31日\n")
    f.write("=" * 80 + "\n")

    total_records = len(df_fy2024)
    f.write(f"\n対象件数: {total_records:,}件\n")

    # 営業人員別の件数を確認
    owner_counts = df_fy2024['Owner.Name'].value_counts()
    f.write(f"営業人員数: {len(owner_counts)}名\n")

    # ========================================
    # 1. 営業人員別 失注件数サマリー
    # ========================================
    f.write("\n" + "=" * 80 + "\n")
    f.write("1. 営業人員別 失注件数サマリー（上位20名）\n")
    f.write("=" * 80 + "\n\n")

    for i, (owner, count) in enumerate(owner_counts.head(20).items()):
        f.write(f"  {i+1:2}. {owner}: {count}件\n")

    # ========================================
    # 2. 大分類カテゴリ別 全体件数
    # ========================================
    f.write("\n" + "=" * 80 + "\n")
    f.write("2. 大分類カテゴリ別 全体件数\n")
    f.write("=" * 80 + "\n\n")

    large_cats = df_fy2024['LostReason_Large__c'].value_counts()
    for cat, count in large_cats.items():
        pct = count / total_records * 100
        f.write(f"  {cat}: {count}件 ({pct:.1f}%)\n")

    # ========================================
    # 3. 小分類 全体ランキング（TOP15）
    # ========================================
    f.write("\n" + "=" * 80 + "\n")
    f.write("3. 小分類 全体ランキング（TOP15）\n")
    f.write("=" * 80 + "\n\n")

    small_cats = df_fy2024['LostReason_Small__c'].value_counts()
    for i, (cat, count) in enumerate(small_cats.head(15).items()):
        pct = count / total_records * 100
        f.write(f"  {i+1:2}. {cat}: {count}件 ({pct:.1f}%)\n")

    # ========================================
    # 4. クロス集計
    # ========================================
    cross_large = pd.crosstab(
        df_fy2024['Owner.Name'],
        df_fy2024['LostReason_Large__c'],
        margins=True
    )

    cross_large_pct = pd.crosstab(
        df_fy2024['Owner.Name'],
        df_fy2024['LostReason_Large__c'],
        normalize='index'
    ) * 100

    # 上位10名の営業人員を抽出
    top_owners = owner_counts.head(10).index.tolist()

    # 主要大分類（上位6カテゴリ）
    main_categories = large_cats.head(6).index.tolist()

    f.write("\n" + "=" * 80 + "\n")
    f.write("4. 上位10名の営業人員 × 大分類構成比\n")
    f.write("=" * 80 + "\n")

    for owner in top_owners:
        if owner in cross_large_pct.index:
            total = cross_large.loc[owner, 'All'] if owner in cross_large.index else 0
            f.write(f"\n{'='*70}\n")
            f.write(f"【{owner}】 失注件数: {total}件\n")
            f.write(f"{'='*70}\n")

            # 大分類
            f.write("\n■ 大分類内訳:\n")
            df_owner = df_fy2024[df_fy2024['Owner.Name'] == owner]
            large_breakdown = df_owner['LostReason_Large__c'].value_counts()
            large_pct = df_owner['LostReason_Large__c'].value_counts(normalize=True) * 100
            for cat, count in large_breakdown.items():
                pct = large_pct[cat]
                f.write(f"  {cat}: {count}件 ({pct:.1f}%)\n")

            # 小分類（上位5つ）
            f.write("\n■ 小分類TOP5:\n")
            small_breakdown = df_owner['LostReason_Small__c'].value_counts().head(5)
            small_pct = df_owner['LostReason_Small__c'].value_counts(normalize=True) * 100
            for cat, count in small_breakdown.items():
                pct = small_pct[cat]
                f.write(f"  {cat}: {count}件 ({pct:.1f}%)\n")

    # ========================================
    # 5. 特徴的な営業人員の抽出
    # ========================================
    f.write("\n" + "=" * 80 + "\n")
    f.write("5. 特徴的な傾向を持つ営業人員（全体平均との比較）\n")
    f.write("=" * 80 + "\n")

    # 全体平均を計算
    overall_large_pct = df_fy2024['LostReason_Large__c'].value_counts(normalize=True) * 100

    for cat in main_categories[:4]:  # 上位4カテゴリについて
        avg_pct = overall_large_pct.get(cat, 0)
        f.write(f"\n【{cat}】全体平均: {avg_pct:.1f}%\n")
        f.write("  平均より高い営業人員:\n")

        deviations = []
        for owner in top_owners:
            if owner in cross_large_pct.index and cat in cross_large_pct.columns:
                pct = cross_large_pct.loc[owner, cat]
                total = cross_large.loc[owner, 'All'] if owner in cross_large.index else 0
                diff = pct - avg_pct
                if diff > 5:  # 5pt以上高い場合
                    deviations.append((owner, pct, diff, total))

        deviations.sort(key=lambda x: x[2], reverse=True)
        if deviations:
            for owner, pct, diff, total in deviations:
                f.write(f"    {owner}: {pct:.1f}% (+{diff:.1f}pt) / {total}件\n")
        else:
            f.write("    該当者なし\n")

    # ========================================
    # 6. 2025年4月以降との比較
    # ========================================
    f.write("\n" + "=" * 80 + "\n")
    f.write("6. 2024年度 vs 2025年度（4月以降） 比較\n")
    f.write("=" * 80 + "\n")

    # 2025年度のデータ（4月以降、1月15日まで）
    df_fy2025 = df[(df['CloseDate'] >= '2025-04-01') & (df['CloseDate'] <= '2026-01-15')].copy()
    fy2025_total = len(df_fy2025)

    f.write("\n【基本統計】\n")
    f.write(f"  2024年度: {total_records:,}件\n")
    f.write(f"  2025年度（途中）: {fy2025_total:,}件\n")

    f.write("\n【大分類構成比の比較】\n")
    f.write("-" * 80 + "\n")
    f.write(f"{'大分類':<40} {'2024年度':>12} {'2025年度':>12} {'差分':>10}\n")
    f.write("-" * 80 + "\n")

    fy2024_large_pct = df_fy2024['LostReason_Large__c'].value_counts(normalize=True) * 100
    fy2025_large_pct = df_fy2025['LostReason_Large__c'].value_counts(normalize=True) * 100

    # 全カテゴリを結合
    all_cats = set(fy2024_large_pct.index) | set(fy2025_large_pct.index)
    comparison_data = []
    for cat in all_cats:
        fy24 = fy2024_large_pct.get(cat, 0)
        fy25 = fy2025_large_pct.get(cat, 0)
        diff = fy25 - fy24
        comparison_data.append((cat, fy24, fy25, diff))

    # 2025年度の割合でソート
    comparison_data.sort(key=lambda x: x[2], reverse=True)

    for cat, fy24, fy25, diff in comparison_data:
        sign = "+" if diff > 0 else ""
        f.write(f"  {cat:<38} {fy24:>10.1f}% {fy25:>10.1f}% {sign}{diff:>8.1f}%\n")

    f.write("\n【小分類構成比の比較（TOP10）】\n")
    f.write("-" * 90 + "\n")
    f.write(f"{'小分類':<50} {'2024年度':>12} {'2025年度':>12} {'差分':>10}\n")
    f.write("-" * 90 + "\n")

    fy2024_small_pct = df_fy2024['LostReason_Small__c'].value_counts(normalize=True) * 100
    fy2025_small_pct = df_fy2025['LostReason_Small__c'].value_counts(normalize=True) * 100

    # 2025年度のTOP10について比較
    for cat in fy2025_small_pct.head(10).index:
        fy24 = fy2024_small_pct.get(cat, 0)
        fy25 = fy2025_small_pct.get(cat, 0)
        diff = fy25 - fy24
        sign = "+" if diff > 0 else ""
        cat_display = cat[:48] if len(cat) > 48 else cat
        f.write(f"  {cat_display:<48} {fy24:>10.1f}% {fy25:>10.1f}% {sign}{diff:>8.1f}%\n")

    # ========================================
    # 7. CSV出力
    # ========================================
    # 営業人員×大分類クロス集計
    cross_large_output = cross_large.reset_index()
    cross_large_output.to_csv(output_dir / 'owner_large_category_crosstab_fy2024.csv', index=False, encoding='utf-8-sig')

    # 営業人員×小分類クロス集計
    cross_small = pd.crosstab(
        df_fy2024['Owner.Name'],
        df_fy2024['LostReason_Small__c'],
        margins=True
    )
    cross_small_output = cross_small.reset_index()
    cross_small_output.to_csv(output_dir / 'owner_small_category_crosstab_fy2024.csv', index=False, encoding='utf-8-sig')

    f.write("\n" + "=" * 80 + "\n")
    f.write("CSV出力完了\n")
    f.write(f"  - owner_large_category_crosstab_fy2024.csv\n")
    f.write(f"  - owner_small_category_crosstab_fy2024.csv\n")
    f.write("=" * 80 + "\n")

print(f"レポート出力完了: {report_file}")
