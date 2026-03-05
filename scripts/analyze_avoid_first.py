# -*- coding: utf-8 -*-
"""
逆転発想分析: 「避けるべきセグメント」から考える
攻め先がないなら、避けるべき先を明確にして残りに集中する
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
    if pd.notna(row.get('FacilityType_Large__c')):
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


def main():
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    print('=' * 80)
    print('逆転発想分析: 避けるべきセグメントから考える')
    print(f'実行日時: {datetime.now().strftime("%Y-%m-%d %H:%M")}')
    print('=' * 80)

    # === データ取得 ===
    service = OpportunityService()
    service.authenticate()

    url = f'{service.instance_url}/services/data/{service.api_version}/sobjects/Opportunity/describe'
    resp = service.session.get(url, headers=service._headers())
    opp_fields = {f['name'] for f in resp.json()['fields']}

    url_acc = f'{service.instance_url}/services/data/{service.api_version}/sobjects/Account/describe'
    resp_acc = service.session.get(url_acc, headers=service._headers())
    acc_fields = {f['name'] for f in resp_acc.json()['fields']}

    fields = ['Id', 'CloseDate', 'IsWon', 'IsClosed', 'OpportunityCategory__c',
              'Account.Name', 'Account.WonOpportunityies__c']
    if 'FacilityType_Large__c' in opp_fields:
        fields.append('FacilityType_Large__c')
    for f in ['LegalPersonality__c', 'ServiceType__c', 'IndustryCategory__c']:
        if f in acc_fields:
            fields.append(f'Account.{f}')

    soql = f"SELECT {', '.join(fields)} FROM Opportunity WHERE IsClosed = true"
    df = service.bulk_query(soql, '逆転分析用')
    print(f'取得: {len(df):,}件')

    # === 前処理 ===
    df['is_won'] = df['IsWon'].map({'true': 1, 'false': 0, True: 1, False: 0}).astype(int)
    df['CloseDate'] = pd.to_datetime(df['CloseDate'], errors='coerce')
    df['month'] = df['CloseDate'].dt.month

    # 新規フィルタ
    won_opp = df['Account.WonOpportunityies__c'].fillna(0).astype(float)
    df['past_won_count'] = won_opp - df['is_won']
    df = df[df['past_won_count'] == 0].copy()

    # 施設形態補完
    df['facility'] = df.apply(complement_facility, axis=1).fillna('不明')

    # 法人格
    lc = df['Account.LegalPersonality__c'].value_counts()
    major = lc[lc >= 100].index.tolist()
    df['legal'] = df['Account.LegalPersonality__c'].apply(
        lambda x: x if x in major else 'その他法人格' if pd.notna(x) and x != '' else '不明'
    )

    # === 初回商談のみ ===
    first = df[df['OpportunityCategory__c'] == '初回商談'].copy()
    fy25 = first[(first['CloseDate'] >= '2025-04-01') & (first['CloseDate'] < '2026-02-01')]

    print(f'初回商談 全期間: {len(first):,}件  FY2025: {len(fy25):,}件')

    report = []
    report.append('# 逆転発想分析: 避けるべきセグメントから考える')
    report.append(f'\n**分析日**: {datetime.now().strftime("%Y-%m-%d %H:%M")}')
    report.append('**方針**: 攻め先がないなら、避けるべき先を明確にして残りに集中する')
    report.append(f'**データ**: 初回商談・新規のみ')

    # =========================================================
    # 4月の詳細分析（全期間 + FY2025）
    # =========================================================
    for label, data in [('全期間', first), ('FY2025', fy25)]:
        apr = data[data['month'] == 4]
        total_apr = len(apr)
        won_apr = int(apr['is_won'].sum())
        rate_apr = won_apr / total_apr * 100 if total_apr > 0 else 0

        print(f'\n{"=" * 80}')
        print(f'【{label}】4月初回商談: {total_apr}件 受注{won_apr}件 受注率{rate_apr:.1f}%')
        print(f'{"=" * 80}')

        report.append(f'\n---\n\n# 【{label}】4月の逆転分析')
        report.append(f'\n**4月全体**: {total_apr}件 / 受注{won_apr}件 / 受注率{rate_apr:.1f}%')

        # --- 1. 施設形態レベル ---
        print(f'\n  --- 施設形態レベル（避けるべき順） ---')
        report.append('\n## 施設形態レベル（避けるべき順）')
        report.append('\n| # | 施設形態 | 件数 | 受注 | 受注率 | ここまで避けると → | 残り件数 | 残り受注率 |')
        report.append('|---|---|---|---|---|---|---|---|')

        fac_stats = []
        for fac in sorted(apr['facility'].unique()):
            sub = apr[apr['facility'] == fac]
            fac_stats.append({
                'facility': fac, 'count': len(sub),
                'won': int(sub['is_won'].sum()),
                'rate': sub['is_won'].mean() * 100
            })
        fac_stats.sort(key=lambda x: (x['rate'], -x['count']))

        cum_avoid = 0
        cum_avoid_won = 0
        hdr = f'  {"#":>2} {"施設形態":<15} {"件数":>5} {"受注":>4} {"率":>6}  | 避けた累計→ {"残件":>5} {"残率":>6}'
        print(hdr)
        print('  ' + '-' * 75)

        for i, s in enumerate(fac_stats, 1):
            cum_avoid += s['count']
            cum_avoid_won += s['won']
            remain = total_apr - cum_avoid
            remain_won = won_apr - cum_avoid_won
            remain_rate = remain_won / remain * 100 if remain > 0 else 0
            marker = '✕' if s['rate'] <= 3.0 else '△' if s['rate'] <= 5.0 else ''
            print(f'  {i:>2} {s["facility"]:<15} {s["count"]:>5} {s["won"]:>4} {s["rate"]:>5.1f}% {marker} | →{remain:>5} {remain_rate:>5.1f}%')
            report.append(f'| {i} | {s["facility"]} | {s["count"]} | {s["won"]} | {s["rate"]:.1f}% | {"✕避け" if s["rate"]<=3.0 else "△注意" if s["rate"]<=5.0 else ""} | {remain} | {remain_rate:.1f}% |')

        # --- 2. 施設形態×法人格クロス（5件以上） ---
        print(f'\n  --- 施設形態×法人格クロス（避けるべき順・5件以上） ---')
        report.append('\n## 施設形態×法人格クロス（避けるべき順）')
        report.append('\n| # | 施設形態 | 法人格 | 件数 | 受注 | 受注率 | 判定 | 避け累計 | 残り件数 | 残り受注率 |')
        report.append('|---|---|---|---|---|---|---|---|---|---|')

        cross_stats = []
        for (fac, leg), sub in apr.groupby(['facility', 'legal']):
            if len(sub) < 5:
                continue
            cross_stats.append({
                'facility': fac, 'legal': leg, 'count': len(sub),
                'won': int(sub['is_won'].sum()),
                'rate': sub['is_won'].mean() * 100
            })
        cross_stats.sort(key=lambda x: (x['rate'], -x['count']))

        cum_avoid = 0
        cum_avoid_won = 0
        hdr2 = f'  {"#":>2} {"施設形態":<15} {"法人格":<18} {"件数":>5} {"受注":>3} {"率":>6} {"判定":<4} | -{"累計":>4} →{"残件":>4} {"残率":>6}'
        print(hdr2)
        print('  ' + '-' * 90)

        for i, s in enumerate(cross_stats, 1):
            cum_avoid += s['count']
            cum_avoid_won += s['won']
            remain = total_apr - cum_avoid
            remain_won = won_apr - cum_avoid_won
            remain_rate = remain_won / remain * 100 if remain > 0 else 0

            if s['rate'] == 0:
                judge = '✕✕'
            elif s['rate'] <= 3.0:
                judge = '✕'
            elif s['rate'] <= 5.0:
                judge = '△'
            elif s['rate'] <= 8.0:
                judge = '─'
            else:
                judge = '○'

            print(f'  {i:>2} {s["facility"]:<15} {s["legal"]:<18} {s["count"]:>5} {s["won"]:>3} {s["rate"]:>5.1f}% {judge:<4} | -{cum_avoid:>4} →{remain:>4} {remain_rate:>5.1f}%')
            report.append(f'| {i} | {s["facility"]} | {s["legal"]} | {s["count"]} | {s["won"]} | {s["rate"]:.1f}% | {judge} | {cum_avoid} | {remain} | {remain_rate:.1f}% |')

        # --- 3. シミュレーション ---
        print(f'\n  --- 避け方シミュレーション ---')
        report.append('\n## 避け方シミュレーション')
        report.append('\n| 基準 | 避ける件数 | 避ける割合 | 残り件数 | 残り受注率 | 効果 |')
        report.append('|---|---|---|---|---|---|')

        for threshold in [0.0, 2.0, 3.0, 5.0, 8.0]:
            avoid_segs = [s for s in cross_stats if s['rate'] <= threshold]
            avoid_n = sum(s['count'] for s in avoid_segs)
            avoid_won = sum(s['won'] for s in avoid_segs)
            remain = total_apr - avoid_n
            remain_won = won_apr - avoid_won
            remain_rate = remain_won / remain * 100 if remain > 0 else 0
            effect = remain_rate - rate_apr
            label_thr = f'受注率{threshold:.0f}%以下を避ける'
            print(f'  {label_thr:<22} → 避ける{avoid_n:>4}件（{avoid_n/total_apr*100:.0f}%） → 残り{remain:>4}件 受注率{remain_rate:.1f}%（+{effect:.1f}pt）')
            report.append(f'| {label_thr} | {avoid_n}件 | {avoid_n/total_apr*100:.0f}% | {remain}件 | {remain_rate:.1f}% | +{effect:.1f}pt |')

    # =========================================================
    # FY2025 全月の避けるべきパターン
    # =========================================================
    print(f'\n{"=" * 80}')
    print(f'【FY2025 全月】月別 避けるべき施設形態パターン')
    print(f'{"=" * 80}')

    report.append('\n---\n\n# FY2025 月別: 避けるべきパターン一覧')
    report.append('\n| 月 | 全体率 | 件数 | ✕避け（受注率3%以下） | 避けた場合の残り率 | ○残り（受注率10%以上） |')
    report.append('|---|---|---|---|---|---|')

    for m in [4, 5, 6, 7, 8, 9, 10, 11, 12, 1]:
        mdata = fy25[fy25['month'] == m]
        if len(mdata) == 0:
            continue
        total_m = len(mdata)
        won_m = int(mdata['is_won'].sum())
        rate_m = won_m / total_m * 100

        fac_rates = []
        for fac in sorted(mdata['facility'].unique()):
            sub = mdata[mdata['facility'] == fac]
            if len(sub) >= 10:
                fac_rates.append((fac, len(sub), int(sub['is_won'].sum()), sub['is_won'].mean() * 100))
        fac_rates.sort(key=lambda x: x[3])

        avoid_list = [(f[0], f[1], f[3]) for f in fac_rates if f[3] <= 3.0]
        attack_list = [(f[0], f[1], f[3]) for f in fac_rates if f[3] >= 10.0]

        # 避けた場合の残り率計算
        avoid_n = sum(a[1] for a in avoid_list)
        avoid_won = sum(f[2] for f in fac_rates if f[3] <= 3.0)
        remain = total_m - avoid_n
        remain_won = won_m - avoid_won
        remain_rate = remain_won / remain * 100 if remain > 0 else 0

        avoid_str = ' / '.join([f'{a[0]}({a[1]}件/{a[2]:.1f}%)' for a in avoid_list]) if avoid_list else 'なし'
        attack_str = ' / '.join([f'{a[0]}({a[1]}件/{a[2]:.1f}%)' for a in attack_list]) if attack_list else 'なし'

        print(f'\n  {m:>2}月 全体{rate_m:.1f}%（{total_m}件）→ 避けた後{remain_rate:.1f}%（{remain}件）')
        if avoid_list:
            print(f'    ✕避け: {avoid_str}')
        else:
            print(f'    ✕避け: なし')
        if attack_list:
            print(f'    ○残り: {attack_str}')
        else:
            print(f'    ○残り: 10%超セグメントなし')

        report.append(f'| {m}月 | {rate_m:.1f}% | {total_m} | {avoid_str} | {remain_rate:.1f}% | {attack_str} |')

    # =========================================================
    # 「年間を通じて常に避けるべき」セグメント
    # =========================================================
    print(f'\n{"=" * 80}')
    print('【通年】常に避けるべきセグメント（全月で受注率5%以下）')
    print(f'{"=" * 80}')

    report.append('\n---\n\n# 通年で避けるべきセグメント')

    # 施設形態×法人格で、十分なデータがあり常に低調なもの
    always_bad = []
    for (fac, leg), sub in fy25.groupby(['facility', 'legal']):
        if len(sub) < 30:
            continue
        overall_rate = sub['is_won'].mean() * 100
        # 月別で10件以上ある月の受注率
        monthly_rates = []
        for m_check in [4, 5, 6, 7, 8, 9, 10, 11, 12, 1]:
            msub = sub[sub['month'] == m_check]
            if len(msub) >= 10:
                monthly_rates.append((m_check, msub['is_won'].mean() * 100))

        if overall_rate <= 5.0 and len(sub) >= 30:
            always_bad.append({
                'facility': fac, 'legal': leg,
                'count': len(sub), 'won': int(sub['is_won'].sum()),
                'rate': overall_rate, 'monthly': monthly_rates
            })

    always_bad.sort(key=lambda x: (x['rate'], -x['count']))

    report.append('\n| # | 施設形態 | 法人格 | 件数 | 受注 | 受注率 | 月別パターン |')
    report.append('|---|---|---|---|---|---|---|')

    print(f'  {"#":>2} {"施設形態":<15} {"法人格":<18} {"件数":>5} {"受注":>3} {"率":>6}  月別パターン')
    print('  ' + '-' * 90)
    for i, s in enumerate(always_bad, 1):
        monthly_str = ', '.join([f'{m}月{r:.0f}%' for m, r in s['monthly']]) if s['monthly'] else '月別データ不足'
        print(f'  {i:>2} {s["facility"]:<15} {s["legal"]:<18} {s["count"]:>5} {s["won"]:>3} {s["rate"]:>5.1f}%  {monthly_str}')
        report.append(f'| {i} | {s["facility"]} | {s["legal"]} | {s["count"]} | {s["won"]} | {s["rate"]:.1f}% | {monthly_str} |')

    # 通年避けの累積効果
    total_fy25 = len(fy25)
    won_fy25 = int(fy25['is_won'].sum())
    avoid_total = sum(s['count'] for s in always_bad)
    avoid_won_total = sum(s['won'] for s in always_bad)
    remain_total = total_fy25 - avoid_total
    remain_won_total = won_fy25 - avoid_won_total
    remain_rate_total = remain_won_total / remain_total * 100 if remain_total > 0 else 0

    print(f'\n  通年避けの効果:')
    print(f'    避ける: {avoid_total}件（全体の{avoid_total/total_fy25*100:.0f}%）')
    print(f'    残り:   {remain_total}件 受注率{remain_rate_total:.1f}%（元{won_fy25/total_fy25*100:.1f}% → +{remain_rate_total - won_fy25/total_fy25*100:.1f}pt）')

    report.append(f'\n### 通年避けの効果')
    report.append(f'\n- **避ける**: {avoid_total}件（全体の{avoid_total/total_fy25*100:.0f}%）')
    report.append(f'- **残り**: {remain_total}件 / 受注率{remain_rate_total:.1f}%（元{won_fy25/total_fy25*100:.1f}% → **+{remain_rate_total - won_fy25/total_fy25*100:.1f}pt**）')

    # === レポート保存 ===
    out_dir = project_root / 'claudedocs'
    out_dir.mkdir(exist_ok=True)
    report_path = out_dir / 'avoid_first_analysis.md'
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report))
    print(f'\nレポート保存: {report_path}')

    print(f'\n{"=" * 80}')
    print('逆転発想分析 完了')
    print(f'{"=" * 80}')


if __name__ == '__main__':
    main()
