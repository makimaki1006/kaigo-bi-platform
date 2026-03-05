# -*- coding: utf-8 -*-
"""
成約セグメント分析（2025年4月〜2026年1月）

次元:
- 業界(IndustryCategory)
- サービス種別(ServiceType)
- 法人格(LegalPersonality)
- 人口帯(Population)
- 従業員数(NumberOfEmployees)
- 受注金額(Amount)
- 商談形式(OpportunityType / OpportunityCategory)
- 決裁権(Hearing_Authority)
- 担当者役職(Hearing_ContactTitle)
- 求人数(Hearing_NuberOfRecruitment)
- 施設形態(FacilityType)
"""

import sys
import io
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np

output_dir = project_root / 'data' / 'output' / 'analysis'
output_dir.mkdir(parents=True, exist_ok=True)


def load_data():
    """成約+失注データを結合し、期間フィルタ"""
    won = pd.read_csv(output_dir / 'won_opportunity_full_20260126.csv', encoding='utf-8-sig', low_memory=False)
    lost = pd.read_csv(output_dir / 'lost_opportunity_full_20260126.csv', encoding='utf-8-sig', low_memory=False)
    df = pd.concat([won, lost], ignore_index=True)
    df['CreatedDate'] = pd.to_datetime(df['CreatedDate'])
    mask = (df['CreatedDate'] >= '2025-04-01') & (df['CreatedDate'] < '2026-02-01')
    df = df[mask].copy()
    return df


def segment_analysis(df, col, label, bins=None, labels_list=None):
    """セグメント別成約率を計算"""
    if col not in df.columns:
        return None

    work = df.copy()
    if bins is not None:
        work['_seg'] = pd.cut(work[col], bins=bins, labels=labels_list, right=False)
    else:
        work['_seg'] = work[col]

    result = work.groupby('_seg', observed=True).agg(
        商談数=('IsWon', 'count'),
        成約数=('IsWon', 'sum'),
    )
    result['成約率'] = (result['成約数'] / result['商談数'] * 100).round(1)
    result = result[result['商談数'] >= 5].sort_values('成約率', ascending=False)
    result.index.name = label
    return result


def cross_analysis(df, col1, col2, label1, label2):
    """2次元クロス集計"""
    work = df[[col1, col2, 'IsWon']].dropna()
    if len(work) == 0:
        return None
    ct = work.groupby([col1, col2], observed=True).agg(
        商談数=('IsWon', 'count'),
        成約数=('IsWon', 'sum'),
    )
    ct['成約率'] = (ct['成約数'] / ct['商談数'] * 100).round(1)
    ct = ct[ct['商談数'] >= 5].sort_values('成約率', ascending=False)
    return ct


def amount_analysis(df):
    """受注金額（成約のみ）の分析"""
    won = df[df['IsWon'] == True].copy()
    if 'Amount' not in won.columns:
        return None
    won['Amount'] = pd.to_numeric(won['Amount'], errors='coerce')
    won = won[won['Amount'].notna() & (won['Amount'] > 0)]

    # セグメント別の平均受注金額
    results = {}
    for col, label in [
        ('Account.IndustryCategory__c', '業界'),
        ('Account.ServiceType__c', 'サービス種別'),
        ('Account.LegalPersonality__c', '法人格'),
        ('OpportunityType__c', '商談形式'),
    ]:
        if col in won.columns:
            grp = won.groupby(col, observed=True).agg(
                成約数=('Amount', 'count'),
                平均受注額=('Amount', 'mean'),
                中央値=('Amount', 'median'),
                合計=('Amount', 'sum'),
            )
            grp['平均受注額'] = grp['平均受注額'].round(0).astype(int)
            grp['中央値'] = grp['中央値'].round(0).astype(int)
            grp['合計'] = grp['合計'].round(0).astype(int)
            grp = grp[grp['成約数'] >= 3].sort_values('平均受注額', ascending=False)
            grp.index.name = label
            results[label] = grp
    return results


def main():
    print("=" * 70)
    print("成約セグメント分析（2025年4月〜2026年1月）")
    print("=" * 70)

    df = load_data()
    total = len(df)
    won_count = df['IsWon'].sum()
    print(f"\n対象期間: 2025/04 - 2026/01")
    print(f"総商談数: {total:,}")
    print(f"成約数: {won_count:,}")
    print(f"全体成約率: {won_count/total*100:.1f}%")

    # --- 1次元セグメント分析 ---
    segments = [
        ('Account.IndustryCategory__c', '業界カテゴリ', None, None),
        ('Account.ServiceType__c', 'サービス種別', None, None),
        ('Account.LegalPersonality__c', '法人格', None, None),
        ('OpportunityType__c', '商談形式', None, None),
        ('OpportunityCategory__c', '商談カテゴリ', None, None),
        ('Hearing_Authority__c', '決裁権', None, None),
        ('Hearing_ContactTitle__c', '担当者役職', None, None),
        ('FacilityType_Large__c', '施設形態(大)', None, None),
        ('FacilityType_Middle__c', '施設形態(中)', None, None),
        ('Account.Prefectures__c', '都道府県', None, None),
    ]

    results_text = []
    results_text.append(f"# 成約セグメント分析（2025年4月〜2026年1月）\n")
    results_text.append(f"- 総商談数: {total:,}")
    results_text.append(f"- 成約数: {won_count:,}")
    results_text.append(f"- 全体成約率: {won_count/total*100:.1f}%\n")

    for col, label, bins, lbls in segments:
        result = segment_analysis(df, col, label, bins, lbls)
        if result is not None and len(result) > 0:
            print(f"\n{'='*50}")
            print(f"【{label}別】")
            print(f"{'='*50}")
            print(result.to_string())
            results_text.append(f"\n## {label}別\n")
            results_text.append(f"| {label} | 商談数 | 成約数 | 成約率 |")
            results_text.append(f"|---|---|---|---|")
            for idx, row in result.iterrows():
                results_text.append(f"| {idx} | {int(row['商談数'])} | {int(row['成約数'])} | {row['成約率']}% |")

    # 人口帯
    pop_bins = [0, 10000, 50000, 100000, 200000, 500000, float('inf')]
    pop_labels = ['~1万', '1-5万', '5-10万', '10-20万', '20-50万', '50万~']
    df['Account.Population__c'] = pd.to_numeric(df['Account.Population__c'], errors='coerce')
    result = segment_analysis(df, 'Account.Population__c', '人口帯', pop_bins, pop_labels)
    if result is not None and len(result) > 0:
        print(f"\n{'='*50}")
        print(f"【人口帯別】")
        print(f"{'='*50}")
        print(result.to_string())
        results_text.append(f"\n## 人口帯別\n")
        results_text.append(f"| 人口帯 | 商談数 | 成約数 | 成約率 |")
        results_text.append(f"|---|---|---|---|")
        for idx, row in result.iterrows():
            results_text.append(f"| {idx} | {int(row['商談数'])} | {int(row['成約数'])} | {row['成約率']}% |")

    # 従業員数帯
    emp_bins = [0, 10, 30, 50, 100, 300, 500, 1000, float('inf')]
    emp_labels = ['~10', '10-30', '30-50', '50-100', '100-300', '300-500', '500-1000', '1000~']
    df['Account.NumberOfEmployees'] = pd.to_numeric(df['Account.NumberOfEmployees'], errors='coerce')
    result = segment_analysis(df, 'Account.NumberOfEmployees', '従業員数', emp_bins, emp_labels)
    if result is not None and len(result) > 0:
        print(f"\n{'='*50}")
        print(f"【従業員数別】")
        print(f"{'='*50}")
        print(result.to_string())
        results_text.append(f"\n## 従業員数別\n")
        results_text.append(f"| 従業員数 | 商談数 | 成約数 | 成約率 |")
        results_text.append(f"|---|---|---|---|")
        for idx, row in result.iterrows():
            results_text.append(f"| {idx} | {int(row['商談数'])} | {int(row['成約数'])} | {row['成約率']}% |")

    # 求人数帯
    df['Hearing_NuberOfRecruitment__c'] = pd.to_numeric(df['Hearing_NuberOfRecruitment__c'], errors='coerce')
    rec_bins = [0, 1, 2, 3, 5, 10, float('inf')]
    rec_labels = ['0', '1', '2', '3-4', '5-9', '10~']
    result = segment_analysis(df, 'Hearing_NuberOfRecruitment__c', '求人数', rec_bins, rec_labels)
    if result is not None and len(result) > 0:
        print(f"\n{'='*50}")
        print(f"【求人数別】")
        print(f"{'='*50}")
        print(result.to_string())
        results_text.append(f"\n## 求人数別\n")
        results_text.append(f"| 求人数 | 商談数 | 成約数 | 成約率 |")
        results_text.append(f"|---|---|---|---|")
        for idx, row in result.iterrows():
            results_text.append(f"| {idx} | {int(row['商談数'])} | {int(row['成約数'])} | {row['成約率']}% |")

    # 受注金額帯（成約のみ）
    amt_bins = [0, 200000, 400000, 600000, 800000, 1000000, float('inf')]
    amt_labels = ['~20万', '20-40万', '40-60万', '60-80万', '80-100万', '100万~']
    df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce')
    won_df = df[df['IsWon'] == True].copy()
    won_amt = won_df[won_df['Amount'].notna() & (won_df['Amount'] > 0)]
    won_amt['_seg'] = pd.cut(won_amt['Amount'], bins=amt_bins, labels=amt_labels, right=False)
    amt_dist = won_amt.groupby('_seg', observed=True).agg(
        成約数=('IsWon', 'count'),
        平均金額=('Amount', 'mean'),
    )
    amt_dist['平均金額'] = amt_dist['平均金額'].round(0).astype(int)
    if len(amt_dist) > 0:
        print(f"\n{'='*50}")
        print(f"【受注金額分布（成約のみ）】")
        print(f"{'='*50}")
        print(amt_dist.to_string())
        print(f"\n成約合計金額: {won_amt['Amount'].sum():,.0f}円")
        print(f"成約平均金額: {won_amt['Amount'].mean():,.0f}円")
        print(f"成約中央値: {won_amt['Amount'].median():,.0f}円")
        results_text.append(f"\n## 受注金額分布（成約のみ）\n")
        results_text.append(f"- 成約合計: {won_amt['Amount'].sum():,.0f}円")
        results_text.append(f"- 平均: {won_amt['Amount'].mean():,.0f}円")
        results_text.append(f"- 中央値: {won_amt['Amount'].median():,.0f}円\n")
        results_text.append(f"| 金額帯 | 成約数 | 平均金額 |")
        results_text.append(f"|---|---|---|")
        for idx, row in amt_dist.iterrows():
            results_text.append(f"| {idx} | {int(row['成約数'])} | {int(row['平均金額']):,}円 |")

    # --- セグメント別平均受注金額 ---
    print(f"\n{'='*50}")
    print(f"【セグメント別 平均受注金額（成約のみ）】")
    print(f"{'='*50}")
    amt_results = amount_analysis(df)
    if amt_results:
        for label, grp in amt_results.items():
            print(f"\n--- {label}別 ---")
            print(grp.to_string())
            results_text.append(f"\n## セグメント別 平均受注金額 - {label}\n")
            results_text.append(f"| {label} | 成約数 | 平均受注額 | 中央値 | 合計 |")
            results_text.append(f"|---|---|---|---|---|")
            for idx, row in grp.iterrows():
                results_text.append(f"| {idx} | {int(row['成約数'])} | {int(row['平均受注額']):,}円 | {int(row['中央値']):,}円 | {int(row['合計']):,}円 |")

    # --- クロス分析 ---
    print(f"\n\n{'='*70}")
    print(f"【クロス分析】")
    print(f"{'='*70}")

    cross_pairs = [
        ('Account.IndustryCategory__c', 'OpportunityType__c', '業界', '商談形式'),
        ('Account.LegalPersonality__c', 'Hearing_Authority__c', '法人格', '決裁権'),
        ('Account.IndustryCategory__c', 'Hearing_Authority__c', '業界', '決裁権'),
    ]

    for col1, col2, l1, l2 in cross_pairs:
        ct = cross_analysis(df, col1, col2, l1, l2)
        if ct is not None and len(ct) > 0:
            print(f"\n--- {l1} × {l2} ---")
            print(ct.to_string())
            results_text.append(f"\n## クロス分析: {l1} × {l2}\n")
            results_text.append(f"| {l1} | {l2} | 商談数 | 成約数 | 成約率 |")
            results_text.append(f"|---|---|---|---|---|")
            for (idx1, idx2), row in ct.iterrows():
                results_text.append(f"| {idx1} | {idx2} | {int(row['商談数'])} | {int(row['成約数'])} | {row['成約率']}% |")

    # --- 月別推移 ---
    df['month'] = df['CreatedDate'].dt.to_period('M')
    monthly = df.groupby('month').agg(
        商談数=('IsWon', 'count'),
        成約数=('IsWon', 'sum'),
    )
    monthly['成約率'] = (monthly['成約数'] / monthly['商談数'] * 100).round(1)
    print(f"\n{'='*50}")
    print(f"【月別推移】")
    print(f"{'='*50}")
    print(monthly.to_string())
    results_text.append(f"\n## 月別推移\n")
    results_text.append(f"| 月 | 商談数 | 成約数 | 成約率 |")
    results_text.append(f"|---|---|---|---|")
    for idx, row in monthly.iterrows():
        results_text.append(f"| {idx} | {int(row['商談数'])} | {int(row['成約数'])} | {row['成約率']}% |")

    # レポート保存
    report_path = project_root / 'claudedocs' / 'ANALYSIS_WonSegments_202504_202601.md'
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(results_text))
    print(f"\n\nレポート保存: {report_path}")


if __name__ == '__main__':
    main()
