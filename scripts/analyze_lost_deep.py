"""
失注理由 深掘り分析スクリプト
- 新規 vs 過去商談の判別
- 同一Accountの複数商談分析
- 月別推移
- 仮説検証
"""

import pandas as pd
from pathlib import Path

# 出力設定
output_dir = Path(r"C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List\data\output\analysis")
report_file = output_dir / 'deep_analysis_report.txt'

# データ読み込み
data_path = output_dir / 'lost_opportunity_full_20260126.csv'
df = pd.read_csv(data_path, encoding='utf-8-sig', low_memory=False)

# 日付変換
df['CloseDate'] = pd.to_datetime(df['CloseDate'])
df['CreatedDate'] = pd.to_datetime(df['CreatedDate'])
df['GetAppointDate__c'] = pd.to_datetime(df['GetAppointDate__c'], errors='coerce')

# 期間分割
df_fy2024 = df[(df['CloseDate'] >= '2024-04-01') & (df['CloseDate'] < '2025-04-01')].copy()
df_fy2025 = df[(df['CloseDate'] >= '2025-04-01') & (df['CloseDate'] <= '2026-01-27')].copy()

with open(report_file, 'w', encoding='utf-8') as f:
    f.write("=" * 80 + "\n")
    f.write("失注理由 深掘り分析レポート\n")
    f.write("=" * 80 + "\n\n")

    # ========================================
    # 1. OpportunityType / Category の確認
    # ========================================
    f.write("=" * 80 + "\n")
    f.write("1. 商談タイプ・カテゴリの確認\n")
    f.write("=" * 80 + "\n\n")

    f.write("【OpportunityType__c の値】\n")
    opp_type = df['OpportunityType__c'].value_counts(dropna=False)
    for val, cnt in opp_type.items():
        f.write(f"  {val}: {cnt}件\n")

    f.write("\n【OpportunityCategory__c の値】\n")
    opp_cat = df['OpportunityCategory__c'].value_counts(dropna=False)
    for val, cnt in opp_cat.items():
        f.write(f"  {val}: {cnt}件\n")

    # ========================================
    # 2. 同一Accountの複数商談分析
    # ========================================
    f.write("\n" + "=" * 80 + "\n")
    f.write("2. 同一Accountの複数商談分析（リピート商談）\n")
    f.write("=" * 80 + "\n\n")

    # Account別商談数
    account_opp_count = df.groupby('AccountId').size()

    f.write("【Account別 商談回数分布】\n")
    count_dist = account_opp_count.value_counts().sort_index()
    for times, cnt in count_dist.head(10).items():
        f.write(f"  {times}回: {cnt}社\n")

    # 2回以上商談があるAccount
    repeat_accounts = account_opp_count[account_opp_count >= 2].index.tolist()
    repeat_opps = df[df['AccountId'].isin(repeat_accounts)]

    f.write(f"\n【リピート商談の規模】\n")
    f.write(f"  2回以上商談があるAccount数: {len(repeat_accounts):,}社\n")
    f.write(f"  そのAccountの商談数合計: {len(repeat_opps):,}件\n")
    f.write(f"  全商談に占める割合: {len(repeat_opps)/len(df)*100:.1f}%\n")

    # ========================================
    # 3. 2024年度 vs 2025年度 リピート商談率の比較
    # ========================================
    f.write("\n" + "=" * 80 + "\n")
    f.write("3. リピート商談率の年度比較\n")
    f.write("=" * 80 + "\n\n")

    # 各商談が「過去に同一Accountで商談があったか」を判定
    def is_repeat_opportunity(row, all_df):
        account_id = row['AccountId']
        created_date = row['CreatedDate']
        # 同一Accountで、この商談より前に作成された商談があるか
        past_opps = all_df[(all_df['AccountId'] == account_id) &
                           (all_df['CreatedDate'] < created_date)]
        return len(past_opps) > 0

    # 全データで過去商談の有無を計算（時間かかるのでサンプリング）
    f.write("【商談作成時点での過去商談有無】\n")
    f.write("※同一Accountで過去に商談があった = リピート商談\n\n")

    # より効率的な方法：Account別に最初の商談日を取得
    first_opp_date = df.groupby('AccountId')['CreatedDate'].min().reset_index()
    first_opp_date.columns = ['AccountId', 'FirstOppDate']

    df_merged = df.merge(first_opp_date, on='AccountId')
    df_merged['IsRepeat'] = df_merged['CreatedDate'] > df_merged['FirstOppDate']

    # 年度別にリピート率を計算
    df_fy2024_m = df_merged[(df_merged['CloseDate'] >= '2024-04-01') & (df_merged['CloseDate'] < '2025-04-01')]
    df_fy2025_m = df_merged[(df_merged['CloseDate'] >= '2025-04-01') & (df_merged['CloseDate'] <= '2026-01-27')]

    repeat_rate_2024 = df_fy2024_m['IsRepeat'].mean() * 100
    repeat_rate_2025 = df_fy2025_m['IsRepeat'].mean() * 100

    f.write(f"  2024年度 リピート商談率: {repeat_rate_2024:.1f}% ({df_fy2024_m['IsRepeat'].sum():,}/{len(df_fy2024_m):,}件)\n")
    f.write(f"  2025年度 リピート商談率: {repeat_rate_2025:.1f}% ({df_fy2025_m['IsRepeat'].sum():,}/{len(df_fy2025_m):,}件)\n")
    f.write(f"  差分: {repeat_rate_2025 - repeat_rate_2024:+.1f}pt\n")

    # ========================================
    # 4. 新規 vs リピートの失注理由比較
    # ========================================
    f.write("\n" + "=" * 80 + "\n")
    f.write("4. 新規 vs リピート商談の失注理由比較（2025年度）\n")
    f.write("=" * 80 + "\n\n")

    new_opps = df_fy2025_m[df_fy2025_m['IsRepeat'] == False]
    repeat_opps_fy25 = df_fy2025_m[df_fy2025_m['IsRepeat'] == True]

    f.write(f"新規商談: {len(new_opps):,}件 / リピート商談: {len(repeat_opps_fy25):,}件\n\n")

    f.write("【大分類構成比の比較】\n")
    f.write("-" * 80 + "\n")
    f.write(f"{'大分類':<40} {'新規':>10} {'リピート':>10} {'差分':>10}\n")
    f.write("-" * 80 + "\n")

    new_large = new_opps['LostReason_Large__c'].value_counts(normalize=True) * 100
    repeat_large = repeat_opps_fy25['LostReason_Large__c'].value_counts(normalize=True) * 100

    all_large_cats = set(new_large.index) | set(repeat_large.index)
    comparison = []
    for cat in all_large_cats:
        new_pct = new_large.get(cat, 0)
        repeat_pct = repeat_large.get(cat, 0)
        diff = repeat_pct - new_pct
        comparison.append((cat, new_pct, repeat_pct, diff))

    comparison.sort(key=lambda x: abs(x[3]), reverse=True)
    for cat, new_pct, repeat_pct, diff in comparison:
        sign = "+" if diff > 0 else ""
        cat_display = cat[:38] if len(str(cat)) > 38 else cat
        f.write(f"  {cat_display:<38} {new_pct:>8.1f}% {repeat_pct:>8.1f}% {sign}{diff:>8.1f}%\n")

    # ========================================
    # 5. 月別推移分析
    # ========================================
    f.write("\n" + "=" * 80 + "\n")
    f.write("5. 月別推移分析\n")
    f.write("=" * 80 + "\n\n")

    df_merged['YearMonth'] = df_merged['CloseDate'].dt.to_period('M')

    # 2024年4月以降のデータ
    df_monthly = df_merged[df_merged['CloseDate'] >= '2024-04-01'].copy()

    monthly_stats = df_monthly.groupby('YearMonth').agg({
        'Id': 'count',
        'IsRepeat': 'mean'
    }).reset_index()
    monthly_stats.columns = ['YearMonth', 'Count', 'RepeatRate']
    monthly_stats['RepeatRate'] = monthly_stats['RepeatRate'] * 100

    f.write("【月別 商談数・リピート率推移】\n")
    f.write("-" * 60 + "\n")
    f.write(f"{'年月':<12} {'商談数':>10} {'リピート率':>12}\n")
    f.write("-" * 60 + "\n")

    for _, row in monthly_stats.iterrows():
        f.write(f"  {str(row['YearMonth']):<10} {int(row['Count']):>10}件 {row['RepeatRate']:>10.1f}%\n")

    # ========================================
    # 6. 懸念系失注の新規vsリピート比較
    # ========================================
    f.write("\n" + "=" * 80 + "\n")
    f.write("6. 「懸念系」失注の詳細分析\n")
    f.write("=" * 80 + "\n\n")

    # 懸念系の大分類
    concern_category = "サービスの価値は感じているが、懸念点を払拭できなかった"

    new_concern = new_opps[new_opps['LostReason_Large__c'] == concern_category]
    repeat_concern = repeat_opps_fy25[repeat_opps_fy25['LostReason_Large__c'] == concern_category]

    f.write(f"【「懸念点払拭できず」の内訳】\n\n")
    f.write(f"新規商談での発生: {len(new_concern)}件 ({len(new_concern)/len(new_opps)*100:.1f}%)\n")
    f.write(f"リピート商談での発生: {len(repeat_concern)}件 ({len(repeat_concern)/len(repeat_opps_fy25)*100:.1f}%)\n\n")

    f.write("【小分類比較（懸念系のみ）】\n")
    f.write("-" * 80 + "\n")

    if len(new_concern) > 0 and len(repeat_concern) > 0:
        new_small = new_concern['LostReason_Small__c'].value_counts(normalize=True) * 100
        repeat_small = repeat_concern['LostReason_Small__c'].value_counts(normalize=True) * 100

        all_small = set(new_small.index) | set(repeat_small.index)
        small_comparison = []
        for cat in all_small:
            new_pct = new_small.get(cat, 0)
            repeat_pct = repeat_small.get(cat, 0)
            diff = repeat_pct - new_pct
            small_comparison.append((cat, new_pct, repeat_pct, diff))

        small_comparison.sort(key=lambda x: x[2], reverse=True)  # リピートの割合でソート

        f.write(f"{'小分類':<45} {'新規':>10} {'リピート':>10} {'差分':>10}\n")
        f.write("-" * 80 + "\n")
        for cat, new_pct, repeat_pct, diff in small_comparison[:10]:
            sign = "+" if diff > 0 else ""
            cat_display = cat[:43] if len(str(cat)) > 43 else cat
            f.write(f"  {cat_display:<43} {new_pct:>8.1f}% {repeat_pct:>8.1f}% {sign}{diff:>8.1f}%\n")

    # ========================================
    # 7. 仮説検証サマリー
    # ========================================
    f.write("\n" + "=" * 80 + "\n")
    f.write("7. 仮説検証サマリー\n")
    f.write("=" * 80 + "\n\n")

    f.write("【仮説】過去商談先の再利用が増えている\n")
    f.write(f"  → リピート商談率: 2024年度 {repeat_rate_2024:.1f}% → 2025年度 {repeat_rate_2025:.1f}%\n")
    if repeat_rate_2025 > repeat_rate_2024:
        f.write(f"  → 検証結果: ✅ 支持される（+{repeat_rate_2025 - repeat_rate_2024:.1f}pt増加）\n")
    else:
        f.write(f"  → 検証結果: ❌ 支持されない（{repeat_rate_2025 - repeat_rate_2024:.1f}pt変化）\n")

    f.write("\n【仮説】リピート商談は懸念系失注が多い\n")
    new_concern_rate = len(new_concern) / len(new_opps) * 100 if len(new_opps) > 0 else 0
    repeat_concern_rate = len(repeat_concern) / len(repeat_opps_fy25) * 100 if len(repeat_opps_fy25) > 0 else 0
    f.write(f"  → 懸念系失注率: 新規 {new_concern_rate:.1f}% vs リピート {repeat_concern_rate:.1f}%\n")
    if repeat_concern_rate > new_concern_rate:
        f.write(f"  → 検証結果: ✅ 支持される（リピートが+{repeat_concern_rate - new_concern_rate:.1f}pt高い）\n")
    else:
        f.write(f"  → 検証結果: ❌ 支持されない\n")

    f.write("\n" + "=" * 80 + "\n")
    f.write("分析完了\n")
    f.write("=" * 80 + "\n")

print(f"レポート出力完了: {report_file}")
