"""
2025年4月以前の失注理由分析スクリプト
営業人員別の傾向、大分類・小分類の内訳を分析
"""

import pandas as pd
from pathlib import Path

# 出力ファイル設定
output_dir = Path(r"C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List\data\output\analysis")
report_file = output_dir / 'owner_analysis_before_april_report.txt'

# データ読み込み
data_path = Path(r"C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List\data\output\analysis\lost_opportunity_full_20260126.csv")
df = pd.read_csv(data_path, encoding='utf-8-sig', low_memory=False)

# 日付変換
df['CloseDate'] = pd.to_datetime(df['CloseDate'])
df['LostDate__c'] = pd.to_datetime(df['LostDate__c'], errors='coerce')

# 2025年4月以前のデータに絞る
df_before_april = df[df['CloseDate'] < '2025-04-01'].copy()

with open(report_file, 'w', encoding='utf-8') as f:
    f.write("=" * 80 + "\n")
    f.write("2025年4月以前 失注理由分析\n")
    f.write("対象期間: 〜 2025年3月31日\n")
    f.write("=" * 80 + "\n")

    total_records = len(df_before_april)
    f.write(f"\n対象件数: {total_records:,}件\n")

    # 営業人員別の件数を確認
    owner_counts = df_before_april['Owner.Name'].value_counts()
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

    large_cats = df_before_april['LostReason_Large__c'].value_counts()
    for cat, count in large_cats.items():
        pct = count / total_records * 100
        f.write(f"  {cat}: {count}件 ({pct:.1f}%)\n")

    # ========================================
    # 3. 小分類 全体ランキング（TOP15）
    # ========================================
    f.write("\n" + "=" * 80 + "\n")
    f.write("3. 小分類 全体ランキング（TOP15）\n")
    f.write("=" * 80 + "\n\n")

    small_cats = df_before_april['LostReason_Small__c'].value_counts()
    for i, (cat, count) in enumerate(small_cats.head(15).items()):
        pct = count / total_records * 100
        f.write(f"  {i+1:2}. {cat}: {count}件 ({pct:.1f}%)\n")

    # ========================================
    # 4. クロス集計
    # ========================================
    cross_large = pd.crosstab(
        df_before_april['Owner.Name'],
        df_before_april['LostReason_Large__c'],
        margins=True
    )

    cross_large_pct = pd.crosstab(
        df_before_april['Owner.Name'],
        df_before_april['LostReason_Large__c'],
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
            df_owner = df_before_april[df_before_april['Owner.Name'] == owner]
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
    overall_large_pct = df_before_april['LostReason_Large__c'].value_counts(normalize=True) * 100

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
        if deviations:
            for owner, pct, diff, total in deviations:
                f.write(f"    {owner}: {pct:.1f}% (+{diff:.1f}pt) / {total}件\n")
        else:
            f.write("    該当者なし\n")

    # ========================================
    # 6. 4月以降との比較
    # ========================================
    f.write("\n" + "=" * 80 + "\n")
    f.write("6. 2025年4月以前 vs 4月以降 比較\n")
    f.write("=" * 80 + "\n")

    # 4月以降のデータ
    df_after_april = df[(df['CloseDate'] >= '2025-04-01') & (df['CloseDate'] <= '2026-01-15')].copy()
    after_total = len(df_after_april)

    f.write("\n【大分類構成比の比較】\n")
    f.write("-" * 80 + "\n")
    f.write(f"{'大分類':<40} {'4月以前':>12} {'4月以降':>12} {'差分':>10}\n")
    f.write("-" * 80 + "\n")

    before_large_pct = df_before_april['LostReason_Large__c'].value_counts(normalize=True) * 100
    after_large_pct = df_after_april['LostReason_Large__c'].value_counts(normalize=True) * 100

    # 全カテゴリを結合
    all_cats = set(before_large_pct.index) | set(after_large_pct.index)
    comparison_data = []
    for cat in all_cats:
        before = before_large_pct.get(cat, 0)
        after = after_large_pct.get(cat, 0)
        diff = after - before
        comparison_data.append((cat, before, after, diff))

    # 4月以降の割合でソート
    comparison_data.sort(key=lambda x: x[2], reverse=True)

    for cat, before, after, diff in comparison_data:
        sign = "+" if diff > 0 else ""
        f.write(f"  {cat:<38} {before:>10.1f}% {after:>10.1f}% {sign}{diff:>8.1f}%\n")

    f.write("\n【小分類構成比の比較（TOP10）】\n")
    f.write("-" * 90 + "\n")
    f.write(f"{'小分類':<50} {'4月以前':>12} {'4月以降':>12} {'差分':>10}\n")
    f.write("-" * 90 + "\n")

    before_small_pct = df_before_april['LostReason_Small__c'].value_counts(normalize=True) * 100
    after_small_pct = df_after_april['LostReason_Small__c'].value_counts(normalize=True) * 100

    # 4月以降のTOP10について比較
    for cat in after_small_pct.head(10).index:
        before = before_small_pct.get(cat, 0)
        after = after_small_pct.get(cat, 0)
        diff = after - before
        sign = "+" if diff > 0 else ""
        cat_display = cat[:48] if len(cat) > 48 else cat
        f.write(f"  {cat_display:<48} {before:>10.1f}% {after:>10.1f}% {sign}{diff:>8.1f}%\n")

    # ========================================
    # 7. CSV出力
    # ========================================
    # 営業人員×大分類クロス集計
    cross_large_output = cross_large.reset_index()
    cross_large_output.to_csv(output_dir / 'owner_large_category_crosstab_before_april.csv', index=False, encoding='utf-8-sig')

    # 営業人員×小分類クロス集計
    cross_small = pd.crosstab(
        df_before_april['Owner.Name'],
        df_before_april['LostReason_Small__c'],
        margins=True
    )
    cross_small_output = cross_small.reset_index()
    cross_small_output.to_csv(output_dir / 'owner_small_category_crosstab_before_april.csv', index=False, encoding='utf-8-sig')

    f.write("\n" + "=" * 80 + "\n")
    f.write("CSV出力完了\n")
    f.write(f"  - owner_large_category_crosstab_before_april.csv\n")
    f.write(f"  - owner_small_category_crosstab_before_april.csv\n")
    f.write("=" * 80 + "\n")

print(f"レポート出力完了: {report_file}")
