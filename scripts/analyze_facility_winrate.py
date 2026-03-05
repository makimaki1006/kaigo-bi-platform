"""
施設タイプ別 成約率の年度比較
"""

import pandas as pd
from pathlib import Path

output_dir = Path(r"C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List\data\output\analysis")
report_file = output_dir / 'facility_winrate_report.txt'

# データ読み込み
lost_path = output_dir / 'lost_opportunity_full_20260126.csv'
won_path = output_dir / 'won_opportunity_full_20260126.csv'

df_lost = pd.read_csv(lost_path, encoding='utf-8-sig', low_memory=False)
df_won = pd.read_csv(won_path, encoding='utf-8-sig', low_memory=False)

# 日付変換
df_lost['CloseDate'] = pd.to_datetime(df_lost['CloseDate'])
df_won['CloseDate'] = pd.to_datetime(df_won['CloseDate'])

# 年度分割
df_lost_24 = df_lost[(df_lost['CloseDate'] >= '2024-04-01') & (df_lost['CloseDate'] < '2025-04-01')]
df_won_24 = df_won[(df_won['CloseDate'] >= '2024-04-01') & (df_won['CloseDate'] < '2025-04-01')]
df_lost_25 = df_lost[(df_lost['CloseDate'] >= '2025-04-01') & (df_lost['CloseDate'] <= '2026-01-27')]
df_won_25 = df_won[(df_won['CloseDate'] >= '2025-04-01') & (df_won['CloseDate'] <= '2026-01-27')]

facility_col = 'FacilityType_Large__c'

with open(report_file, 'w', encoding='utf-8') as f:
    f.write("=" * 80 + "\n")
    f.write("施設タイプ別 成約率の年度比較\n")
    f.write("=" * 80 + "\n\n")

    # 2024年度
    won_24_fac = df_won_24.groupby(facility_col).size()
    lost_24_fac = df_lost_24.groupby(facility_col).size()

    # 2025年度
    won_25_fac = df_won_25.groupby(facility_col).size()
    lost_25_fac = df_lost_25.groupby(facility_col).size()

    all_facilities = set(won_24_fac.index) | set(lost_24_fac.index) | set(won_25_fac.index) | set(lost_25_fac.index)

    f.write("【施設タイプ別 成約率】\n")
    f.write("-" * 100 + "\n")
    f.write(f"{'施設タイプ':<20} {'2024成約':>8} {'2024失注':>8} {'2024率':>8} {'2025成約':>8} {'2025失注':>8} {'2025率':>8} {'差分':>8}\n")
    f.write("-" * 100 + "\n")

    results = []
    for fac in all_facilities:
        if pd.isna(fac):
            continue

        w24 = won_24_fac.get(fac, 0)
        l24 = lost_24_fac.get(fac, 0)
        t24 = w24 + l24
        r24 = w24 / t24 * 100 if t24 > 0 else 0

        w25 = won_25_fac.get(fac, 0)
        l25 = lost_25_fac.get(fac, 0)
        t25 = w25 + l25
        r25 = w25 / t25 * 100 if t25 > 0 else 0

        diff = r25 - r24
        results.append((fac, w24, l24, r24, w25, l25, r25, diff))

    # 差分でソート
    results.sort(key=lambda x: x[7])

    for fac, w24, l24, r24, w25, l25, r25, diff in results:
        fac_name = str(fac)[:18] if len(str(fac)) > 18 else fac
        sign = "+" if diff > 0 else ""
        f.write(f"  {fac_name:<18} {w24:>8} {l24:>8} {r24:>6.1f}% {w25:>8} {l25:>8} {r25:>6.1f}% {sign}{diff:>6.1f}pt\n")

    # 全体
    total_w24 = len(df_won_24)
    total_l24 = len(df_lost_24)
    total_r24 = total_w24 / (total_w24 + total_l24) * 100

    total_w25 = len(df_won_25)
    total_l25 = len(df_lost_25)
    total_r25 = total_w25 / (total_w25 + total_l25) * 100

    f.write("-" * 100 + "\n")
    f.write(f"  {'【全体】':<18} {total_w24:>8} {total_l24:>8} {total_r24:>6.1f}% {total_w25:>8} {total_l25:>8} {total_r25:>6.1f}% {total_r25 - total_r24:>+6.1f}pt\n")

    # 提案金額の分析（失注データにAmountがあるか確認）
    f.write("\n" + "=" * 80 + "\n")
    f.write("提案金額の分析\n")
    f.write("=" * 80 + "\n\n")

    f.write("【失注データのAmount分布】\n")
    f.write(f"  2024年度: 非ゼロ件数 {(df_lost_24['Amount'] > 0).sum()}/{len(df_lost_24)}件\n")
    f.write(f"  2025年度: 非ゼロ件数 {(df_lost_25['Amount'] > 0).sum()}/{len(df_lost_25)}件\n\n")

    # 失注データのAmount平均
    lost_24_amt = df_lost_24[df_lost_24['Amount'] > 0]['Amount']
    lost_25_amt = df_lost_25[df_lost_25['Amount'] > 0]['Amount']
    won_24_amt = df_won_24[df_won_24['Amount'] > 0]['Amount']
    won_25_amt = df_won_25[df_won_25['Amount'] > 0]['Amount']

    f.write("【金額の年度比較（Amountが入っているもののみ）】\n")
    f.write("-" * 80 + "\n")
    f.write(f"{'区分':<15} {'2024平均':>15} {'2025平均':>15} {'差分':>15}\n")
    f.write("-" * 80 + "\n")

    if len(lost_24_amt) > 0 and len(lost_25_amt) > 0:
        f.write(f"  失注金額      {lost_24_amt.mean():>13,.0f}円 {lost_25_amt.mean():>13,.0f}円 {lost_25_amt.mean() - lost_24_amt.mean():>+13,.0f}円\n")
    if len(won_24_amt) > 0 and len(won_25_amt) > 0:
        f.write(f"  成約金額      {won_24_amt.mean():>13,.0f}円 {won_25_amt.mean():>13,.0f}円 {won_25_amt.mean() - won_24_amt.mean():>+13,.0f}円\n")

    # 施設タイプ別の金額変化
    f.write("\n【施設タイプ別 失注案件の平均金額】\n")
    f.write("-" * 80 + "\n")
    f.write(f"{'施設タイプ':<20} {'2024平均':>15} {'2025平均':>15} {'差分':>15}\n")
    f.write("-" * 80 + "\n")

    for fac in ['介護（高齢者）', '医療', '障がい福祉', '保育', 'その他']:
        fac_24 = df_lost_24[(df_lost_24[facility_col] == fac) & (df_lost_24['Amount'] > 0)]['Amount']
        fac_25 = df_lost_25[(df_lost_25[facility_col] == fac) & (df_lost_25['Amount'] > 0)]['Amount']

        if len(fac_24) > 0 and len(fac_25) > 0:
            avg_24 = fac_24.mean()
            avg_25 = fac_25.mean()
            diff = avg_25 - avg_24
            sign = "+" if diff > 0 else ""
            f.write(f"  {fac:<18} {avg_24:>13,.0f}円 {avg_25:>13,.0f}円 {sign}{diff:>13,.0f}円\n")

    f.write("\n" + "=" * 80 + "\n")
    f.write("分析完了\n")
    f.write("=" * 80 + "\n")

print(f"レポート出力: {report_file}")
