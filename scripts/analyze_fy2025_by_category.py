# -*- coding: utf-8 -*-
"""
FY2025期間限定分析: 初回商談 vs 再商談 分割版

新規営業の商談を「初回商談」「再商談」に分けて、
月別×施設形態×法人格のMECEクロス集計＋受注率フォーカスを実施する。
"""

import sys
from pathlib import Path
from datetime import datetime

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['PYTHONIOENCODING'] = 'utf-8'

import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

from src.services.opportunity_service import OpportunityService

DATE_FROM = '2025-04-01'
DATE_TO = '2026-01-31'
PERIOD_LABEL = '2025年4月～2026年1月'

# 月別最低件数（受注率算出に必要）
MIN_MONTHLY = 10
# セグメント最低件数
MIN_SEGMENT = 20


def extract_data():
    """Salesforceからデータ抽出"""
    service = OpportunityService()
    service.authenticate()

    url = f"{service.instance_url}/services/data/{service.api_version}/sobjects/Opportunity/describe"
    resp = service.session.get(url, headers=service._headers())
    resp.raise_for_status()
    opp_fields = {f['name'] for f in resp.json()['fields']}

    url_acc = f"{service.instance_url}/services/data/{service.api_version}/sobjects/Account/describe"
    resp_acc = service.session.get(url_acc, headers=service._headers())
    resp_acc.raise_for_status()
    acc_fields = {f['name'] for f in resp_acc.json()['fields']}

    fields = [
        'Id', 'CloseDate', 'IsWon', 'IsClosed', 'Amount',
        'OpportunityCategory__c',
        'Account.Name',
        'Account.WonOpportunityies__c',
    ]

    for f in ['FacilityType_Large__c', 'FacilityType_Middle__c', 'FacilityType_Small__c']:
        if f in opp_fields:
            fields.append(f)

    if 'LegalPersonality__c' in acc_fields:
        fields.append('Account.LegalPersonality__c')

    if 'ServiceType__c' in acc_fields:
        fields.append('Account.ServiceType__c')

    field_list = ', '.join(fields)
    soql = (f"SELECT {field_list} FROM Opportunity "
            f"WHERE IsClosed = true "
            f"AND CloseDate >= {DATE_FROM} "
            f"AND CloseDate <= {DATE_TO}")

    df = service.bulk_query(soql, f'FY2025データ抽出（{PERIOD_LABEL}）')
    print(f"  取得件数: {len(df):,}")
    return df


def prepare_data(df):
    """前処理"""
    df['is_won'] = df['IsWon'].map({'true': 1, 'false': 0, True: 1, False: 0})
    df['CloseDate'] = pd.to_datetime(df['CloseDate'], errors='coerce')
    df['month'] = df['CloseDate'].dt.month

    # 法人格の正規化
    if 'Account.LegalPersonality__c' in df.columns:
        legal_counts = df['Account.LegalPersonality__c'].value_counts()
        major_legal = legal_counts[legal_counts >= 30].index.tolist()
        df['legal_group'] = df['Account.LegalPersonality__c'].apply(
            lambda x: x if x in major_legal else 'その他法人格' if pd.notna(x) and x != '' else '不明'
        )

    # 施設形態の正規化
    if 'FacilityType_Large__c' in df.columns:
        df['facility'] = df['FacilityType_Large__c'].fillna('不明')

    return df


def analyze_segment(df, label, months, report):
    """1つのカテゴリ（初回/再商談）のクロス集計＋受注率フォーカスを実行"""
    month_names = {1:'1月',2:'2月',3:'3月',4:'4月',5:'5月',6:'6月',
                   7:'7月',8:'8月',9:'9月',10:'10月',11:'11月',12:'12月'}

    total_deals = len(df)
    total_won = int(df['is_won'].sum())
    avg_rate = df['is_won'].mean() * 100

    print(f"\n{'=' * 120}")
    print(f"【{label}】 {total_deals:,}件  受注={total_won}件  受注率={avg_rate:.1f}%")
    print(f"{'=' * 120}")

    report.append(f"\n# 【{label}】")
    report.append(f"\n**商談数**: {total_deals:,}件 | **受注**: {total_won}件 | **受注率**: {avg_rate:.1f}%")

    # 月別全体受注率
    period_avg = df['is_won'].mean()
    report.append(f"\n## 月別全体受注率（{label}）")
    report.append("\n| 月 | 商談数 | 受注数 | 受注率 | vs平均 |")
    report.append("|---|---|---|---|---|")

    print(f"\n  {'月':>4} {'商談数':>7} {'受注数':>7} {'受注率':>7} {'vs平均':>8}")
    print(f"  " + "-" * 40)

    for m in months:
        dm = df[df['month'] == m]
        if len(dm) == 0:
            continue
        rate = dm['is_won'].mean()
        diff = rate - period_avg
        marker = "▼" if diff < -0.02 else "▲" if diff > 0.02 else ""
        print(f"  {month_names[m]:>4} {len(dm):>7} {int(dm['is_won'].sum()):>7} "
              f"{rate*100:>6.1f}% {diff*100:>+7.1f}pt {marker}")
        report.append(f"| {month_names[m]} | {len(dm):,} | {int(dm['is_won'].sum())} | "
                      f"{rate*100:.1f}% | {diff*100:+.1f}pt |")

    # クロス集計
    facility_col = 'facility'
    legal_col = 'legal_group'
    results = []

    facilities = sorted(df[facility_col].unique())
    legals = sorted(df[legal_col].unique())

    for fac in facilities:
        for leg in legals:
            df_seg = df[(df[facility_col] == fac) & (df[legal_col] == leg)]
            total = len(df_seg)
            if total < MIN_SEGMENT:
                continue

            won = df_seg['is_won'].sum()
            seg_rate = df_seg['is_won'].mean()

            monthly_rates = {}
            monthly_counts = {}
            for m in months:
                df_m = df_seg[df_seg['month'] == m]
                monthly_counts[m] = len(df_m)
                monthly_rates[m] = df_m['is_won'].mean() if len(df_m) >= MIN_MONTHLY else None

            results.append({
                'facility': fac,
                'legal': leg,
                'total': total,
                'won': int(won),
                'rate': seg_rate,
                **{f'm{m}_rate': monthly_rates[m] for m in months},
                **{f'm{m}_count': monthly_counts[m] for m in months},
            })

    results_df = pd.DataFrame(results)
    if results_df.empty:
        print("  十分なデータがありません")
        report.append("\n*十分なデータがありません。*")
        return results_df

    results_df = results_df.sort_values('rate', ascending=False)

    # 受注率マトリクス
    print(f"\n  月別受注率マトリクス（セグメント{MIN_SEGMENT}件以上、月別{MIN_MONTHLY}件以上）")
    print(f"\n  {'施設形態':<16} {'法人格':<14} {'率':>5} {'件数':>5}", end='')
    for m in months:
        print(f" {month_names[m]:>4}", end='')
    print()
    print("  " + "-" * (42 + 5 * len(months)))

    report.append(f"\n## 月別受注率マトリクス（{label}）")
    report.append(f"\nセグメント{MIN_SEGMENT}件以上、月別{MIN_MONTHLY}件以上で受注率算出。")

    rpt_h = "| 施設形態 | 法人格 | 率 | 件数 |"
    rpt_s = "|---|---|---|---|"
    for m in months:
        rpt_h += f" {month_names[m]} |"
        rpt_s += "---|"
    report.append(f"\n{rpt_h}")
    report.append(rpt_s)

    for _, row in results_df.iterrows():
        line = f"  {str(row['facility'])[:15]:<16} {str(row['legal'])[:13]:<14} "
        line += f"{row['rate']*100:>4.1f}% {int(row['total']):>5}"

        rpt_line = f"| {row['facility']} | {row['legal']} | {row['rate']*100:.1f}% | {int(row['total'])} |"

        for m in months:
            rate = row[f'm{m}_rate']
            count = row[f'm{m}_count']
            if pd.notna(rate) and count >= MIN_MONTHLY:
                val = rate * 100
                if val >= 15:
                    line += f" {val:>4.0f}*"
                    rpt_line += f" **{val:.0f}%** |"
                else:
                    line += f" {val:>5.1f}"
                    rpt_line += f" {val:.1f}% |"
            else:
                line += f"   -- "
                rpt_line += " -- |"
        print(line)
        report.append(rpt_line)

    # 4月推奨
    df_apr = results_df[
        (results_df['m4_count'] >= MIN_MONTHLY) &
        (results_df['m4_rate'].notna())
    ].copy()
    df_apr = df_apr.sort_values('m4_rate', ascending=False)

    if not df_apr.empty:
        print(f"\n  4月 受注率ランキング（{label}、4月{MIN_MONTHLY}件以上）")
        print(f"  {'#':>2} {'施設形態':<18} {'法人格':<16} {'4月受注率':>9} "
              f"{'4月件数':>7} {'期間率':>7} {'期間件数':>7} {'判定':<6}")
        print(f"  " + "-" * 95)

        report.append(f"\n## 4月の推奨（{label}）")
        report.append(f"\n| # | 施設形態 | 法人格 | **4月受注率** | 4月件数 | 期間率 | 期間件数 | 判定 |")
        report.append(f"|---|---|---|---|---|---|---|---|")

        for i, (_, row) in enumerate(df_apr.iterrows()):
            apr = row['m4_rate'] * 100
            period = row['rate'] * 100

            if apr >= 15:
                verdict = "◎攻め"
            elif apr >= 8:
                verdict = "○可"
            elif apr >= 3:
                verdict = "△注意"
            else:
                verdict = "✕避け"

            print(f"  {i+1:>2} {str(row['facility'])[:17]:<18} {str(row['legal'])[:15]:<16} "
                  f"{apr:>8.1f}% {int(row['m4_count']):>7} {period:>6.1f}% {int(row['total']):>7} {verdict}")
            report.append(f"| {i+1} | {row['facility']} | {row['legal']} | "
                         f"**{apr:.1f}%** | {int(row['m4_count'])} | {period:.1f}% | {int(row['total'])} | {verdict} |")

    return results_df


def main():
    output_dir = project_root / 'data' / 'output' / 'analysis'
    output_dir.mkdir(parents=True, exist_ok=True)
    report_dir = project_root / 'claudedocs'
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    months = [4, 5, 6, 7, 8, 9, 10, 11, 12, 1]

    print("=" * 120)
    print(f"FY2025分析: 初回商談 vs 再商談 分割版（{PERIOD_LABEL}）")
    print(f"最低件数: セグメント{MIN_SEGMENT}件以上、月別{MIN_MONTHLY}件以上")
    print("=" * 120)

    # データ抽出
    df = extract_data()
    df = prepare_data(df)

    # 全体サマリー
    print(f"\n{'=' * 120}")
    print("全体データ内訳")
    print(f"{'=' * 120}")

    report = []
    report.append(f"# FY2025分析: 初回商談 vs 再商談（{PERIOD_LABEL}）")
    report.append(f"\n**分析日**: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    report.append(f"**対象期間**: {PERIOD_LABEL}")
    report.append(f"**最低件数**: セグメント{MIN_SEGMENT}件以上、月別受注率は{MIN_MONTHLY}件以上で算出")

    report.append("\n## データ内訳")
    report.append("\n| カテゴリ | 商談数 | 受注数 | 受注率 |")
    report.append("|---|---|---|---|")

    categories = {
        '初回商談': df[df['OpportunityCategory__c'] == '初回商談'],
        '再商談': df[df['OpportunityCategory__c'] == '再商談'],
        '未設定': df[df['OpportunityCategory__c'].isna()],
    }

    for cat_name, cat_df in categories.items():
        cnt = len(cat_df)
        won = int(cat_df['is_won'].sum())
        rate = cat_df['is_won'].mean() * 100 if cnt > 0 else 0
        print(f"  {cat_name:<8}: {cnt:>6,}件  受注={won:>4}件  受注率={rate:.1f}%")
        report.append(f"| {cat_name} | {cnt:,} | {won} | {rate:.1f}% |")

    total = len(df)
    total_won = int(df['is_won'].sum())
    total_rate = df['is_won'].mean() * 100
    print(f"  {'合計':<8}: {total:>6,}件  受注={total_won:>4}件  受注率={total_rate:.1f}%")
    report.append(f"| **合計** | **{total:,}** | **{total_won}** | **{total_rate:.1f}%** |")

    report.append("\n---")

    # 初回商談の分析
    df_first = categories['初回商談']
    results_first = analyze_segment(df_first, '初回商談', months, report)

    report.append("\n---")

    # 再商談の分析
    df_re = categories['再商談']
    results_re = analyze_segment(df_re, '再商談', months, report)

    # 比較サマリー
    report.append("\n---")
    report.append("\n# 初回商談 vs 再商談 比較サマリー")

    month_names = {1:'1月',2:'2月',3:'3月',4:'4月',5:'5月',6:'6月',
                   7:'7月',8:'8月',9:'9月',10:'10月',11:'11月',12:'12月'}

    report.append("\n## 月別受注率比較")
    report.append("\n| 月 | 初回商談 | 再商談 | 差分 |")
    report.append("|---|---|---|---|")

    print(f"\n{'=' * 120}")
    print("初回商談 vs 再商談 月別受注率比較")
    print(f"{'=' * 120}")
    print(f"  {'月':>4} {'初回商談':>10} {'再商談':>10} {'差分':>8}")
    print(f"  " + "-" * 40)

    for m in months:
        dm_f = df_first[df_first['month'] == m]
        dm_r = df_re[df_re['month'] == m]
        rate_f = dm_f['is_won'].mean() * 100 if len(dm_f) > 0 else 0
        rate_r = dm_r['is_won'].mean() * 100 if len(dm_r) > 0 else 0
        diff = rate_f - rate_r
        print(f"  {month_names[m]:>4} {rate_f:>9.1f}% {rate_r:>9.1f}% {diff:>+7.1f}pt")
        report.append(f"| {month_names[m]} | {rate_f:.1f}% | {rate_r:.1f}% | {diff:+.1f}pt |")

    # CSV保存
    if not results_first.empty:
        path = output_dir / f'fy2025_first_meeting_{timestamp}.csv'
        results_first.to_csv(path, index=False, encoding='utf-8-sig')
        print(f"\n  初回商談クロス集計保存: {path}")

    if not results_re.empty:
        path = output_dir / f'fy2025_re_meeting_{timestamp}.csv'
        results_re.to_csv(path, index=False, encoding='utf-8-sig')
        print(f"  再商談クロス集計保存: {path}")

    # レポート保存
    report_text = "\n".join(report)
    report_path = report_dir / 'fy2025_by_category_analysis.md'
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(report_text)
    print(f"\n  レポート保存: {report_path}")

    print(f"\n{'=' * 120}")
    print("分析完了")
    print(f"{'=' * 120}")


if __name__ == "__main__":
    main()
