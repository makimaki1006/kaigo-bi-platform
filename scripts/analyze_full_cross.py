# -*- coding: utf-8 -*-
"""
完全クロス分析: 施設形態補完 × 初回/再商談 × 全期間/FY2025

全データを施設形態補完した上で、以下の4軸で徹底分析:
  A) 全期間 × 初回商談
  B) 全期間 × 再商談
  C) FY2025（2025/4-2026/1） × 初回商談
  D) FY2025（2025/4-2026/1） × 再商談
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

MIN_MONTHLY = 10
MIN_SEGMENT = 30

# === 施設形態補完マッピング ===

INDUSTRY_MAP = {
    '介護': '介護（高齢者）', '医療': '医療', '障害福祉': '障がい福祉',
    '保育': '保育', 'その他': 'その他',
}

SERVICE_TYPE_MAP = {
    '訪問介護': '介護（高齢者）', '通所介護': '介護（高齢者）',
    '短期入所生活介護': '介護（高齢者）', '認知症対応型共同生活介護': '介護（高齢者）',
    '居宅介護支援': '介護（高齢者）', '地域密着型通所介護': '介護（高齢者）',
    '特定施設入居者生活介護（有料老人ホーム）': '介護（高齢者）',
    '小規模多機能型居宅介護': '介護（高齢者）', '介護老人福祉施設': '介護（高齢者）',
    '看護小規模多機能型居宅介護（複合型サービス）': '介護（高齢者）',
    '介護老人保健施設': '介護（高齢者）',
    '地域密着型介護老人福祉施設入所者生活介護': '介護（高齢者）',
    '訪問入浴介護': '介護（高齢者）', '認知症対応型通所介護': '介護（高齢者）',
    '福祉用具貸与': '介護（高齢者）', '有料老人ホーム': '介護（高齢者）',
    '特定施設入居者生活介護（有料老人ホーム（サービス付き高齢者向け住宅））': '介護（高齢者）',
    '特定施設入居者生活介護（軽費老人ホーム）': '介護（高齢者）',
    '地域密着型特定施設入居者生活介護（有料老人ホーム）': '介護（高齢者）',
    '地域密着型特定施設入居者生活介護（有料老人ホーム（サービス付き高齢者向け住宅））': '介護（高齢者）',
    '短期入所療養介護（介護老人保健施設）': '介護（高齢者）',
    '訪問看護': '医療', '訪問リハビリテーション': '医療',
    '通所リハビリテーション': '医療', '介護医療院': '医療',
    '短期入所療養介護(療養病床を有する病院等）': '医療', 'クリニック': '医療',
    '放課後等デイサービス': '障がい福祉', '就労定着支援': '障がい福祉',
    '生活介護': '障がい福祉', '障がい者施設': '障がい福祉',
    '障害者施設': '障がい福祉', 'ショートステイ': '障がい福祉',
    '複合施設': '障がい福祉', '保育園': '保育',
}


def complement_facility(row):
    if pd.notna(row['FacilityType_Large__c']):
        return row['FacilityType_Large__c']
    ic = row.get('Account.IndustryCategory__c')
    if pd.notna(ic):
        first_cat = str(ic).split(';')[0].strip()
        if first_cat in INDUSTRY_MAP:
            return INDUSTRY_MAP[first_cat]
    st = row.get('Account.ServiceType__c')
    if pd.notna(st) and st in SERVICE_TYPE_MAP:
        return SERVICE_TYPE_MAP[st]
    return None


def extract_all():
    """全期間データ抽出"""
    service = OpportunityService()
    service.authenticate()

    # フィールド確認
    url = f"{service.instance_url}/services/data/{service.api_version}/sobjects/Opportunity/describe"
    resp = service.session.get(url, headers=service._headers())
    opp_fields = {f['name'] for f in resp.json()['fields']}

    url_acc = f"{service.instance_url}/services/data/{service.api_version}/sobjects/Account/describe"
    resp_acc = service.session.get(url_acc, headers=service._headers())
    acc_fields = {f['name'] for f in resp_acc.json()['fields']}

    fields = ['Id', 'CloseDate', 'IsWon', 'IsClosed', 'Amount',
              'OpportunityCategory__c', 'Account.Name', 'Account.WonOpportunityies__c']

    if 'FacilityType_Large__c' in opp_fields:
        fields.append('FacilityType_Large__c')
    for f in ['LegalPersonality__c', 'ServiceType__c', 'IndustryCategory__c']:
        if f in acc_fields:
            fields.append(f'Account.{f}')

    soql = f"SELECT {', '.join(fields)} FROM Opportunity WHERE IsClosed = true"
    df = service.bulk_query(soql, '全期間データ抽出（施設形態補完用）')
    print(f"  全期間取得件数: {len(df):,}")
    return df


def prepare(df):
    """前処理＋新規フィルタ＋施設形態補完"""
    df['is_won'] = df['IsWon'].map({'true': 1, 'false': 0, True: 1, False: 0}).astype(int)
    df['CloseDate'] = pd.to_datetime(df['CloseDate'], errors='coerce')
    df['month'] = df['CloseDate'].dt.month

    # 新規商談フィルタ（既存顧客のリピート商談を除外）
    won_opp = df['Account.WonOpportunityies__c'].fillna(0).astype(float)
    df['past_won_count'] = won_opp - df['is_won']
    before = len(df)
    excluded = df[df['past_won_count'] > 0]
    print(f"  既存顧客除外: {len(excluded):,}件（受注率{excluded['is_won'].mean()*100:.1f}%）")
    df = df[df['past_won_count'] == 0].copy()
    print(f"  新規フィルタ: {before:,} → {len(df):,}件（-{before-len(df):,}）")

    # 施設形態補完
    orig = df['FacilityType_Large__c'].notna().sum()
    df['facility'] = df.apply(complement_facility, axis=1).fillna('不明')
    comp = (df['facility'] != '不明').sum()
    print(f"  施設形態: 元{orig:,} → 補完後{comp:,}（+{comp-orig:,}）/ 不明{(df['facility']=='不明').sum():,}")

    # 法人格
    if 'Account.LegalPersonality__c' in df.columns:
        lc = df['Account.LegalPersonality__c'].value_counts()
        major = lc[lc >= 100].index.tolist()
        df['legal'] = df['Account.LegalPersonality__c'].apply(
            lambda x: x if x in major else 'その他法人格' if pd.notna(x) and x != '' else '不明'
        )
    return df


def cross_analyze(df, label, months, month_names):
    """クロス集計＋受注率分析を実行し、結果を返す"""
    total = len(df)
    won = int(df['is_won'].sum())
    avg = df['is_won'].mean() * 100
    lines = []  # コンソール出力用
    report = []  # Markdownレポート用

    lines.append(f"\n{'='*120}")
    lines.append(f"【{label}】 {total:,}件  受注={won}件  受注率={avg:.1f}%")
    lines.append(f"{'='*120}")

    report.append(f"\n# {label}")
    report.append(f"\n**商談数**: {total:,} | **受注**: {won} | **受注率**: {avg:.1f}%")

    # 月別全体
    period_avg = df['is_won'].mean()
    lines.append(f"\n  {'月':>4} {'商談':>6} {'受注':>5} {'率':>6} {'vs平均':>8}")
    lines.append(f"  {'-'*38}")
    report.append(f"\n## 月別受注率")
    report.append("\n| 月 | 商談 | 受注 | 受注率 | vs平均 |")
    report.append("|---|---|---|---|---|")

    for m in months:
        dm = df[df['month'] == m]
        if len(dm) == 0:
            continue
        r = dm['is_won'].mean()
        d = r - period_avg
        mk = "▼" if d < -0.02 else "▲" if d > 0.02 else ""
        lines.append(f"  {month_names[m]:>4} {len(dm):>6} {int(dm['is_won'].sum()):>5} {r*100:>5.1f}% {d*100:>+7.1f}pt {mk}")
        report.append(f"| {month_names[m]} | {len(dm):,} | {int(dm['is_won'].sum())} | {r*100:.1f}% | {d*100:+.1f}pt |")

    # 施設形態別サマリー
    lines.append(f"\n  施設形態別サマリー:")
    lines.append(f"  {'施設形態':<16} {'件数':>6} {'受注':>5} {'受注率':>7}")
    lines.append(f"  {'-'*38}")
    report.append(f"\n## 施設形態別サマリー")
    report.append("\n| 施設形態 | 件数 | 受注 | 受注率 |")
    report.append("|---|---|---|---|")

    for fac in sorted(df['facility'].unique()):
        g = df[df['facility'] == fac]
        r = g['is_won'].mean() * 100
        lines.append(f"  {fac:<16} {len(g):>6} {int(g['is_won'].sum()):>5} {r:>6.1f}%")
        report.append(f"| {fac} | {len(g):,} | {int(g['is_won'].sum())} | {r:.1f}% |")

    # クロス集計
    results = []
    for fac in sorted(df['facility'].unique()):
        for leg in sorted(df['legal'].unique()):
            seg = df[(df['facility'] == fac) & (df['legal'] == leg)]
            if len(seg) < MIN_SEGMENT:
                continue
            seg_won = seg['is_won'].sum()
            seg_rate = seg['is_won'].mean()
            mr, mc = {}, {}
            for m in months:
                dm = seg[seg['month'] == m]
                mc[m] = len(dm)
                mr[m] = dm['is_won'].mean() if len(dm) >= MIN_MONTHLY else None
            results.append({
                'facility': fac, 'legal': leg,
                'total': len(seg), 'won': int(seg_won), 'rate': seg_rate,
                **{f'm{m}_rate': mr[m] for m in months},
                **{f'm{m}_count': mc[m] for m in months},
            })

    rdf = pd.DataFrame(results)
    if rdf.empty:
        lines.append("  十分なデータがありません")
        report.append("\n*データ不足*")
        for l in lines:
            print(l)
        return rdf, report

    rdf = rdf.sort_values('rate', ascending=False)

    # マトリクス
    lines.append(f"\n  月別受注率マトリクス（{MIN_SEGMENT}件以上、月{MIN_MONTHLY}件以上）")
    h = f"  {'施設形態':<14} {'法人格':<12} {'率':>5} {'件数':>5}"
    for m in months:
        h += f" {month_names[m]:>4}"
    lines.append(h)
    lines.append("  " + "-" * (38 + 5 * len(months)))

    rpt_h = "| 施設形態 | 法人格 | 率 | 件数 |"
    rpt_s = "|---|---|---|---|"
    for m in months:
        rpt_h += f" {month_names[m]} |"
        rpt_s += "---|"
    report.append(f"\n## 月別受注率マトリクス")
    report.append(f"\n{rpt_h}")
    report.append(rpt_s)

    for _, row in rdf.iterrows():
        l = f"  {str(row['facility'])[:13]:<14} {str(row['legal'])[:11]:<12} "
        l += f"{row['rate']*100:>4.1f}% {int(row['total']):>5}"
        rl = f"| {row['facility']} | {row['legal']} | {row['rate']*100:.1f}% | {int(row['total'])} |"
        for m in months:
            rate = row[f'm{m}_rate']
            cnt = row[f'm{m}_count']
            if pd.notna(rate) and cnt >= MIN_MONTHLY:
                v = rate * 100
                if v >= 15:
                    l += f" {v:>4.0f}*"
                    rl += f" **{v:.0f}%** |"
                else:
                    l += f" {v:>5.1f}"
                    rl += f" {v:.1f}% |"
            else:
                l += "   -- "
                rl += " -- |"
        lines.append(l)
        report.append(rl)

    # 4月推奨
    apr = rdf[(rdf['m4_count'] >= MIN_MONTHLY) & (rdf['m4_rate'].notna())].copy()
    apr = apr.sort_values('m4_rate', ascending=False)

    if not apr.empty:
        lines.append(f"\n  4月受注率ランキング（4月{MIN_MONTHLY}件以上）")
        lines.append(f"  {'#':>2} {'施設形態':<16} {'法人格':<14} {'4月率':>7} {'4月件':>5} {'期間率':>6} {'件数':>5} {'判定'}")
        lines.append(f"  {'-'*80}")

        report.append(f"\n## 4月の推奨")
        report.append("\n| # | 施設形態 | 法人格 | **4月受注率** | 4月件数 | 期間率 | 期間件数 | 判定 |")
        report.append("|---|---|---|---|---|---|---|---|")

        for i, (_, row) in enumerate(apr.iterrows()):
            a = row['m4_rate'] * 100
            p = row['rate'] * 100
            v = "◎攻め" if a >= 15 else "○可" if a >= 8 else "△注意" if a >= 3 else "✕避け"
            lines.append(f"  {i+1:>2} {str(row['facility'])[:15]:<16} {str(row['legal'])[:13]:<14} "
                        f"{a:>6.1f}% {int(row['m4_count']):>5} {p:>5.1f}% {int(row['total']):>5} {v}")
            report.append(f"| {i+1} | {row['facility']} | {row['legal']} | "
                         f"**{a:.1f}%** | {int(row['m4_count'])} | {p:.1f}% | {int(row['total'])} | {v} |")

    # ベスト/ワースト月
    lines.append(f"\n  セグメント別ベスト月/ワースト月")
    lines.append(f"  {'施設形態':<14} {'法人格':<12} {'期間率':>6} {'件数':>5}  {'BEST':>6}       {'WORST':>6}")
    lines.append(f"  {'-'*80}")

    report.append(f"\n## セグメント別ベスト月/ワースト月")
    report.append("\n| 施設形態 | 法人格 | 期間率 | 件数 | ベスト月 | 率 | ワースト月 | 率 |")
    report.append("|---|---|---|---|---|---|---|---|")

    for _, row in rdf.iterrows():
        monthly = {m: row[f'm{m}_rate'] for m in months
                   if pd.notna(row[f'm{m}_rate']) and row[f'm{m}_count'] >= MIN_MONTHLY}
        if len(monthly) < 3:
            continue
        best = max(monthly, key=monthly.get)
        worst = min(monthly, key=monthly.get)
        lines.append(f"  {str(row['facility'])[:13]:<14} {str(row['legal'])[:11]:<12} "
                    f"{row['rate']*100:>5.1f}% {int(row['total']):>5}  "
                    f"{month_names[best]:>3} {monthly[best]*100:>5.1f}%  "
                    f"{month_names[worst]:>3} {monthly[worst]*100:>5.1f}%")
        report.append(f"| {row['facility']} | {row['legal']} | {row['rate']*100:.1f}% | {int(row['total'])} | "
                     f"{month_names[best]} | {monthly[best]*100:.1f}% | {month_names[worst]} | {monthly[worst]*100:.1f}% |")

    for l in lines:
        print(l)
    return rdf, report


def main():
    output_dir = project_root / 'data' / 'output' / 'analysis'
    output_dir.mkdir(parents=True, exist_ok=True)
    report_dir = project_root / 'claudedocs'
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')

    all_months = list(range(1, 13))
    fy_months = [4, 5, 6, 7, 8, 9, 10, 11, 12, 1]
    mn = {1:'1月',2:'2月',3:'3月',4:'4月',5:'5月',6:'6月',
          7:'7月',8:'8月',9:'9月',10:'10月',11:'11月',12:'12月'}

    print("=" * 120)
    print("完全クロス分析: 施設形態補完 × 初回/再商談 × 全期間/FY2025")
    print(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"閾値: セグメント{MIN_SEGMENT}件以上、月別{MIN_MONTHLY}件以上")
    print("=" * 120)

    # データ抽出＋前処理
    df = extract_all()
    df = prepare(df)

    # 全体サマリー
    master_report = []
    master_report.append("# 完全クロス分析: 施設形態補完版")
    master_report.append(f"\n**分析日**: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    master_report.append(f"**施設形態補完**: Account.IndustryCategory__c + ServiceType__c から補完")
    master_report.append(f"**閾値**: セグメント{MIN_SEGMENT}件以上、月別受注率は{MIN_MONTHLY}件以上で算出")
    master_report.append(f"**全商談数**: {len(df):,}件")

    # 施設形態分布
    print(f"\n{'='*120}")
    print("補完後の施設形態分布（全期間）")
    print(f"{'='*120}")
    master_report.append("\n## 施設形態分布（補完後・全期間）")
    master_report.append("\n| 施設形態 | 失注 | 受注 | 合計 | 受注率 |")
    master_report.append("|---|---|---|---|---|")

    for fac in sorted(df['facility'].unique()):
        g = df[df['facility'] == fac]
        lost = int((g['is_won'] == 0).sum())
        won = int((g['is_won'] == 1).sum())
        t = lost + won
        r = won / t * 100
        print(f"  {fac:<16} 失注={lost:>6} 受注={won:>5} 計={t:>6} 率={r:>5.1f}%")
        master_report.append(f"| {fac} | {lost:,} | {won} | {t:,} | {r:.1f}% |")

    # カテゴリ分布
    print(f"\n  カテゴリ別:")
    master_report.append("\n## カテゴリ分布")
    master_report.append("\n| カテゴリ | 件数 | 受注 | 受注率 |")
    master_report.append("|---|---|---|---|")
    for cat_name, cat_df in [
        ('初回商談', df[df['OpportunityCategory__c'] == '初回商談']),
        ('再商談', df[df['OpportunityCategory__c'] == '再商談']),
        ('未設定', df[df['OpportunityCategory__c'].isna()]),
    ]:
        r = cat_df['is_won'].mean() * 100 if len(cat_df) > 0 else 0
        print(f"  {cat_name:<8}: {len(cat_df):>6,}件 受注={int(cat_df['is_won'].sum()):>5} 率={r:.1f}%")
        master_report.append(f"| {cat_name} | {len(cat_df):,} | {int(cat_df['is_won'].sum())} | {r:.1f}% |")

    master_report.append("\n---\n---")

    # FY2025抽出
    df_fy = df[(df['CloseDate'] >= '2025-04-01') & (df['CloseDate'] <= '2026-01-31')]

    # 4パターン分析
    analyses = [
        ('A) 全期間 × 初回商談', df[df['OpportunityCategory__c'] == '初回商談'], all_months),
        ('B) 全期間 × 再商談', df[df['OpportunityCategory__c'] == '再商談'], all_months),
        ('C) FY2025 × 初回商談', df_fy[df_fy['OpportunityCategory__c'] == '初回商談'], fy_months),
        ('D) FY2025 × 再商談', df_fy[df_fy['OpportunityCategory__c'] == '再商談'], fy_months),
    ]

    all_results = {}
    for label, data, months in analyses:
        rdf, rpt = cross_analyze(data, label, months, mn)
        master_report.extend(rpt)
        master_report.append("\n---\n---")
        tag = label.split(')')[0].strip() + ')'
        all_results[tag] = rdf

        if rdf is not None and not rdf.empty:
            safe = label.replace(' ', '_').replace('×', 'x').replace('（', '').replace('）', '')
            path = output_dir / f'full_cross_{safe}_{ts}.csv'
            rdf.to_csv(path, index=False, encoding='utf-8-sig')

    # 初回vs再商談 比較テーブル
    print(f"\n{'='*120}")
    print("初回商談 vs 再商談 月別比較（全期間）")
    print(f"{'='*120}")
    master_report.append("\n# 初回商談 vs 再商談 月別比較")

    # 全期間
    df_f = df[df['OpportunityCategory__c'] == '初回商談']
    df_r = df[df['OpportunityCategory__c'] == '再商談']

    master_report.append("\n## 全期間")
    master_report.append("\n| 月 | 初回商談 | 再商談 | 差分 |")
    master_report.append("|---|---|---|---|")
    print(f"  {'月':>4} {'初回':>8} {'再商談':>8} {'差分':>7}")
    print(f"  {'-'*30}")
    for m in all_months:
        rf = df_f[df_f['month'] == m]['is_won'].mean() * 100 if len(df_f[df_f['month'] == m]) > 0 else 0
        rr = df_r[df_r['month'] == m]['is_won'].mean() * 100 if len(df_r[df_r['month'] == m]) > 0 else 0
        print(f"  {mn[m]:>4} {rf:>7.1f}% {rr:>7.1f}% {rf-rr:>+6.1f}pt")
        master_report.append(f"| {mn[m]} | {rf:.1f}% | {rr:.1f}% | {rf-rr:+.1f}pt |")

    # FY2025
    df_ff = df_fy[df_fy['OpportunityCategory__c'] == '初回商談']
    df_rf = df_fy[df_fy['OpportunityCategory__c'] == '再商談']

    print(f"\n  FY2025（2025/4-2026/1）:")
    master_report.append("\n## FY2025")
    master_report.append("\n| 月 | 初回商談 | 再商談 | 差分 |")
    master_report.append("|---|---|---|---|")
    print(f"  {'月':>4} {'初回':>8} {'再商談':>8} {'差分':>7}")
    print(f"  {'-'*30}")
    for m in fy_months:
        rf = df_ff[df_ff['month'] == m]['is_won'].mean() * 100 if len(df_ff[df_ff['month'] == m]) > 0 else 0
        rr = df_rf[df_rf['month'] == m]['is_won'].mean() * 100 if len(df_rf[df_rf['month'] == m]) > 0 else 0
        print(f"  {mn[m]:>4} {rf:>7.1f}% {rr:>7.1f}% {rf-rr:>+6.1f}pt")
        master_report.append(f"| {mn[m]} | {rf:.1f}% | {rr:.1f}% | {rf-rr:+.1f}pt |")

    # レポート保存
    report_text = "\n".join(master_report)
    report_path = report_dir / 'full_cross_analysis.md'
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(report_text)
    print(f"\n  レポート保存: {report_path}")

    print(f"\n{'='*120}")
    print("完全クロス分析 完了")
    print(f"{'='*120}")


if __name__ == "__main__":
    main()
