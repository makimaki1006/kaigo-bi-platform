# -*- coding: utf-8 -*-
"""
FY2025分析: 施設形態を補完した版

FacilityType_Large__c が未入力の商談に対して、
Account.IndustryCategory__c / Account.ServiceType__c から施設形態を補完し、
生存者バイアスを排除した正確な受注率分析を行う。
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
MIN_MONTHLY = 10
MIN_SEGMENT = 20

# IndustryCategory__c → 施設形態マッピング
INDUSTRY_MAP = {
    '介護': '介護（高齢者）',
    '医療': '医療',
    '障害福祉': '障がい福祉',
    '保育': '保育',
    'その他': 'その他',
}

# ServiceType__c → 施設形態マッピング（主要なもの）
SERVICE_TYPE_MAP = {
    # 介護
    '訪問介護': '介護（高齢者）',
    '通所介護': '介護（高齢者）',
    '短期入所生活介護': '介護（高齢者）',
    '認知症対応型共同生活介護': '介護（高齢者）',
    '居宅介護支援': '介護（高齢者）',
    '地域密着型通所介護': '介護（高齢者）',
    '特定施設入居者生活介護（有料老人ホーム）': '介護（高齢者）',
    '小規模多機能型居宅介護': '介護（高齢者）',
    '介護老人福祉施設': '介護（高齢者）',
    '看護小規模多機能型居宅介護（複合型サービス）': '介護（高齢者）',
    '介護老人保健施設': '介護（高齢者）',
    '地域密着型介護老人福祉施設入所者生活介護': '介護（高齢者）',
    '訪問入浴介護': '介護（高齢者）',
    '認知症対応型通所介護': '介護（高齢者）',
    '福祉用具貸与': '介護（高齢者）',
    '有料老人ホーム': '介護（高齢者）',
    '特定施設入居者生活介護（有料老人ホーム（サービス付き高齢者向け住宅））': '介護（高齢者）',
    '特定施設入居者生活介護（軽費老人ホーム）': '介護（高齢者）',
    '地域密着型特定施設入居者生活介護（有料老人ホーム）': '介護（高齢者）',
    '地域密着型特定施設入居者生活介護（有料老人ホーム（サービス付き高齢者向け住宅））': '介護（高齢者）',
    '短期入所療養介護（介護老人保健施設）': '介護（高齢者）',
    # 医療
    '訪問看護': '医療',
    '訪問リハビリテーション': '医療',
    '通所リハビリテーション': '医療',
    '介護医療院': '医療',
    '短期入所療養介護(療養病床を有する病院等）': '医療',
    'クリニック': '医療',
    # 障がい福祉
    '放課後等デイサービス': '障がい福祉',
    '就労定着支援': '障がい福祉',
    '生活介護': '障がい福祉',
    '障がい者施設': '障がい福祉',
    '障害者施設': '障がい福祉',
    'ショートステイ': '障がい福祉',
    '複合施設': '障がい福祉',
    # 保育
    '保育園': '保育',
}


def complement_facility(row):
    """施設形態を補完するロジック"""
    # 元の値があればそのまま
    if pd.notna(row['FacilityType_Large__c']):
        return row['FacilityType_Large__c']

    # IndustryCategory__cから補完
    ic = row.get('Account.IndustryCategory__c')
    if pd.notna(ic):
        # 複合カテゴリ（「介護;障害福祉」等）は最初のものを使用
        first_cat = str(ic).split(';')[0].strip()
        if first_cat in INDUSTRY_MAP:
            return INDUSTRY_MAP[first_cat]

    # ServiceType__cから補完
    st = row.get('Account.ServiceType__c')
    if pd.notna(st):
        if st in SERVICE_TYPE_MAP:
            return SERVICE_TYPE_MAP[st]

    return None  # 補完不可


def extract_data():
    """データ抽出"""
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

    for f in ['FacilityType_Large__c']:
        if f in opp_fields:
            fields.append(f)

    for f in ['LegalPersonality__c', 'ServiceType__c', 'IndustryCategory__c']:
        if f in acc_fields:
            fields.append(f'Account.{f}')

    field_list = ', '.join(fields)
    soql = (f"SELECT {field_list} FROM Opportunity "
            f"WHERE IsClosed = true "
            f"AND CloseDate >= {DATE_FROM} "
            f"AND CloseDate <= {DATE_TO}")

    df = service.bulk_query(soql, f'FY2025補完版データ抽出')
    print(f"  取得件数: {len(df):,}")
    return df


def prepare_data(df):
    """前処理＋施設形態補完"""
    df['is_won'] = df['IsWon'].map({'true': 1, 'false': 0, True: 1, False: 0})
    df['CloseDate'] = pd.to_datetime(df['CloseDate'], errors='coerce')
    df['month'] = df['CloseDate'].dt.month

    # 施設形態補完
    original_filled = df['FacilityType_Large__c'].notna().sum()
    df['facility'] = df.apply(complement_facility, axis=1)
    df['facility'] = df['facility'].fillna('不明')
    after_filled = (df['facility'] != '不明').sum()

    print(f"\n  施設形態補完:")
    print(f"    元の入力あり: {original_filled:,}件")
    print(f"    補完後入力あり: {after_filled:,}件（+{after_filled - original_filled:,}件補完）")
    print(f"    補完不可（不明）: {(df['facility'] == '不明').sum():,}件")

    # 補完元の内訳
    from_ic = 0
    from_st = 0
    for _, row in df[df['FacilityType_Large__c'].isna()].iterrows():
        ic = row.get('Account.IndustryCategory__c')
        st = row.get('Account.ServiceType__c')
        if pd.notna(ic) and str(ic).split(';')[0].strip() in INDUSTRY_MAP:
            from_ic += 1
        elif pd.notna(st) and st in SERVICE_TYPE_MAP:
            from_st += 1
    print(f"    IndustryCategory__cから: {from_ic:,}件")
    print(f"    ServiceType__cから: {from_st:,}件")

    # 法人格の正規化
    if 'Account.LegalPersonality__c' in df.columns:
        legal_counts = df['Account.LegalPersonality__c'].value_counts()
        major_legal = legal_counts[legal_counts >= 30].index.tolist()
        df['legal_group'] = df['Account.LegalPersonality__c'].apply(
            lambda x: x if x in major_legal else 'その他法人格' if pd.notna(x) and x != '' else '不明'
        )

    return df


def analyze_category(df, label, months, report):
    """初回商談 or 再商談の分析"""
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

    # 月別全体
    period_avg = df['is_won'].mean()
    report.append(f"\n## 月別受注率（{label}）")
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
    results = []
    facilities = sorted(df['facility'].unique())
    legals = sorted(df['legal_group'].unique())

    for fac in facilities:
        for leg in legals:
            df_seg = df[(df['facility'] == fac) & (df['legal_group'] == leg)]
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
        return results_df

    results_df = results_df.sort_values('rate', ascending=False)

    # マトリクス表示
    print(f"\n  月別受注率マトリクス（{MIN_SEGMENT}件以上、月別{MIN_MONTHLY}件以上）")
    header = f"  {'施設形態':<16} {'法人格':<14} {'率':>5} {'件数':>5}"
    for m in months:
        header += f" {month_names[m]:>4}"
    print(header)
    print("  " + "-" * (42 + 5 * len(months)))

    report.append(f"\n## 月別受注率マトリクス（{label}）")
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
    month_names = {1:'1月',2:'2月',3:'3月',4:'4月',5:'5月',6:'6月',
                   7:'7月',8:'8月',9:'9月',10:'10月',11:'11月',12:'12月'}

    print("=" * 120)
    print(f"FY2025分析: 施設形態補完版（{PERIOD_LABEL}）")
    print(f"最低件数: セグメント{MIN_SEGMENT}件以上、月別{MIN_MONTHLY}件以上")
    print("=" * 120)

    df = extract_data()
    df = prepare_data(df)

    # 補完後の施設形態分布
    print(f"\n{'=' * 120}")
    print("補完後の施設形態分布")
    print(f"{'=' * 120}")

    report = []
    report.append(f"# FY2025分析: 施設形態補完版（{PERIOD_LABEL}）")
    report.append(f"\n**分析日**: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    report.append(f"**対象期間**: {PERIOD_LABEL}")
    report.append(f"**施設形態補完**: IndustryCategory__c + ServiceType__c から補完")
    report.append(f"**最低件数**: セグメント{MIN_SEGMENT}件以上、月別{MIN_MONTHLY}件以上")

    report.append("\n## 補完後の施設形態分布")
    report.append("\n| 施設形態 | 失注 | 受注 | 合計 | 受注率 |")
    report.append("|---|---|---|---|---|")

    print(f"  {'施設形態':<16} {'失注':>6} {'受注':>6} {'合計':>6} {'受注率':>7}")
    print("  " + "-" * 50)

    for fac in sorted(df['facility'].unique()):
        grp = df[df['facility'] == fac]
        lost = int((grp['is_won'] == 0).sum())
        won = int((grp['is_won'] == 1).sum())
        total = lost + won
        rate = won / total * 100
        print(f"  {fac:<16} {lost:>6} {won:>6} {total:>6} {rate:>6.1f}%")
        report.append(f"| {fac} | {lost} | {won} | {total} | {rate:.1f}% |")

    report.append("\n---")

    # カテゴリ別内訳
    print(f"\n{'=' * 120}")
    print("OpportunityCategory__c 内訳")
    print(f"{'=' * 120}")

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

    report.append("\n---")

    # 初回商談の分析
    results_first = analyze_category(categories['初回商談'], '初回商談', months, report)
    report.append("\n---")

    # 再商談の分析
    results_re = analyze_category(categories['再商談'], '再商談', months, report)

    # 月別比較
    report.append("\n---")
    report.append("\n# 初回商談 vs 再商談 月別比較")
    report.append("\n| 月 | 初回商談 | 再商談 | 差分 |")
    report.append("|---|---|---|---|")

    print(f"\n{'=' * 120}")
    print("初回商談 vs 再商談 月別受注率比較")
    print(f"{'=' * 120}")
    print(f"  {'月':>4} {'初回商談':>10} {'再商談':>10} {'差分':>8}")
    print(f"  " + "-" * 40)

    df_first = categories['初回商談']
    df_re = categories['再商談']
    for m in months:
        dm_f = df_first[df_first['month'] == m]
        dm_r = df_re[df_re['month'] == m]
        rate_f = dm_f['is_won'].mean() * 100 if len(dm_f) > 0 else 0
        rate_r = dm_r['is_won'].mean() * 100 if len(dm_r) > 0 else 0
        diff = rate_f - rate_r
        print(f"  {month_names[m]:>4} {rate_f:>9.1f}% {rate_r:>9.1f}% {diff:>+7.1f}pt")
        report.append(f"| {month_names[m]} | {rate_f:.1f}% | {rate_r:.1f}% | {diff:+.1f}pt |")

    # CSV保存
    if results_first is not None and not results_first.empty:
        path = output_dir / f'fy2025_complemented_first_{timestamp}.csv'
        results_first.to_csv(path, index=False, encoding='utf-8-sig')
        print(f"\n  初回商談結果保存: {path}")

    if results_re is not None and not results_re.empty:
        path = output_dir / f'fy2025_complemented_re_{timestamp}.csv'
        results_re.to_csv(path, index=False, encoding='utf-8-sig')
        print(f"  再商談結果保存: {path}")

    # レポート保存
    report_text = "\n".join(report)
    report_path = report_dir / 'fy2025_complemented_analysis.md'
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(report_text)
    print(f"\n  レポート保存: {report_path}")

    print(f"\n{'=' * 120}")
    print("分析完了")
    print(f"{'=' * 120}")


if __name__ == "__main__":
    main()
