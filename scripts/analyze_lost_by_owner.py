"""
営業人員ごとの失注理由分析スクリプト
2025年4月以降のデータを対象に、各営業担当者の失注理由（大分類・小分類）を分析
"""

import pandas as pd
from pathlib import Path

# 出力ファイル設定
output_dir = Path(r"C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List\data\output\analysis")
report_file = output_dir / 'owner_analysis_report.txt'

# データ読み込み
data_path = Path(r"C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List\data\output\analysis\lost_opportunity_full_20260126.csv")
df = pd.read_csv(data_path, encoding='utf-8-sig', low_memory=False)

# 日付変換
df['CloseDate'] = pd.to_datetime(df['CloseDate'])
df['LostDate__c'] = pd.to_datetime(df['LostDate__c'], errors='coerce')

# 2025年4月以降、かつ2026-01-15以前のデータに絞る（実績データのみ）
df_post_april = df[(df['CloseDate'] >= '2025-04-01') & (df['CloseDate'] <= '2026-01-15')].copy()

with open(report_file, 'w', encoding='utf-8') as f:
    f.write("=" * 80 + "\n")
    f.write("営業人員別 失注理由分析\n")
    f.write("対象期間: 2025年4月1日 〜 2026年1月15日（実績データ）\n")
    f.write("=" * 80 + "\n")

    total_records = len(df_post_april)
    f.write(f"\n対象件数: {total_records:,}件\n")

    # 営業人員別の件数を確認
    owner_counts = df_post_april['Owner.Name'].value_counts()
    f.write(f"営業人員数: {len(owner_counts)}名\n")

    # ========================================
    # 1. 営業人員別 大分類内訳（全体サマリー）
    # ========================================
    f.write("\n" + "=" * 80 + "\n")
    f.write("1. 営業人員別 失注件数サマリー\n")
    f.write("=" * 80 + "\n")

    f.write("\n【営業人員別 失注件数（上位20名）】\n")
    for i, (owner, count) in enumerate(owner_counts.head(20).items()):
        f.write(f"  {i+1:2}. {owner}: {count}件\n")

    # ========================================
    # 2. 大分類の構成比を営業人員別に算出
    # ========================================
    f.write("\n" + "=" * 80 + "\n")
    f.write("2. 営業人員別 大分類構成比\n")
    f.write("=" * 80 + "\n")

    # クロス集計
    cross_large = pd.crosstab(
        df_post_april['Owner.Name'],
        df_post_april['LostReason_Large__c'],
        margins=True
    )

    # 構成比計算
    cross_large_pct = pd.crosstab(
        df_post_april['Owner.Name'],
        df_post_april['LostReason_Large__c'],
        normalize='index'
    ) * 100

    # 主要な大分類カテゴリ（実際のデータから確認）
    large_cats = df_post_april['LostReason_Large__c'].value_counts()
    f.write("\n【大分類カテゴリ別 全体件数】\n")
    for cat, count in large_cats.items():
        pct = count / total_records * 100
        f.write(f"  {cat}: {count}件 ({pct:.1f}%)\n")

    # 上位10名の営業人員を抽出
    top_owners = owner_counts.head(10).index.tolist()

    f.write("\n【上位10名の営業人員 × 大分類（構成比%）】\n")
    f.write("-" * 120 + "\n")

    # 主要大分類（上位6カテゴリ）
    main_categories = large_cats.head(6).index.tolist()

    for owner in top_owners:
        if owner in cross_large_pct.index:
            total = cross_large.loc[owner, 'All'] if owner in cross_large.index else 0
            f.write(f"\n{owner} （{total}件）:\n")
            for cat in main_categories:
                if cat in cross_large_pct.columns:
                    pct = cross_large_pct.loc[owner, cat]
                    cnt = cross_large.loc[owner, cat] if cat in cross_large.columns else 0
                    f.write(f"  {cat}: {pct:.1f}% ({cnt}件)\n")

    # ========================================
    # 3. 主要小分類の詳細分析
    # ========================================
    f.write("\n" + "=" * 80 + "\n")
    f.write("3. 小分類 全体ランキング（TOP15）\n")
    f.write("=" * 80 + "\n")

    small_cats = df_post_april['LostReason_Small__c'].value_counts()
    f.write("\n")
    for i, (cat, count) in enumerate(small_cats.head(15).items()):
        pct = count / total_records * 100
        f.write(f"  {i+1:2}. {cat}: {count}件 ({pct:.1f}%)\n")

    # ========================================
    # 4. 各営業人員の詳細分析（上位10名）
    # ========================================
    f.write("\n" + "=" * 80 + "\n")
    f.write("4. 営業人員別 詳細分析（上位10名）\n")
    f.write("=" * 80 + "\n")

    for owner in top_owners:
        df_owner = df_post_april[df_post_april['Owner.Name'] == owner]
        f.write(f"\n{'='*70}\n")
        f.write(f"【{owner}】 失注件数: {len(df_owner)}件\n")
        f.write(f"{'='*70}\n")

        # 大分類
        f.write("\n■ 大分類内訳:\n")
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
    overall_large_pct = df_post_april['LostReason_Large__c'].value_counts(normalize=True) * 100

    for cat in main_categories[:3]:  # 上位3カテゴリについて
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
        for owner, pct, diff, total in deviations:
            f.write(f"    {owner}: {pct:.1f}% (+{diff:.1f}pt) / {total}件\n")

    # ========================================
    # 6. CSV出力（詳細データ）
    # ========================================
    # 営業人員×大分類クロス集計
    cross_large_output = cross_large.reset_index()
    cross_large_output.to_csv(output_dir / 'owner_large_category_crosstab.csv', index=False, encoding='utf-8-sig')

    # 営業人員×小分類クロス集計
    cross_small = pd.crosstab(
        df_post_april['Owner.Name'],
        df_post_april['LostReason_Small__c'],
        margins=True
    )
    cross_small_output = cross_small.reset_index()
    cross_small_output.to_csv(output_dir / 'owner_small_category_crosstab.csv', index=False, encoding='utf-8-sig')

    f.write("\n" + "=" * 80 + "\n")
    f.write("CSV出力完了\n")
    f.write(f"  - owner_large_category_crosstab.csv\n")
    f.write(f"  - owner_small_category_crosstab.csv\n")
    f.write("=" * 80 + "\n")

print(f"レポート出力完了: {report_file}")
