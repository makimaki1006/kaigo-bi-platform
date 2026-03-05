# -*- coding: utf-8 -*-
"""
具体的に避けるべきセグメントの特定
「不明」を具体的な特徴に落とし込む
"""
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['PYTHONIOENCODING'] = 'utf-8'

import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')
from src.services.opportunity_service import OpportunityService

INDUSTRY_MAP = {'介護': '介護（高齢者）', '医療': '医療', '障害福祉': '障がい福祉', '保育': '保育', 'その他': 'その他'}
SERVICE_TYPE_MAP = {
    '訪問介護': '介護（高齢者）', '通所介護': '介護（高齢者）', '短期入所生活介護': '介護（高齢者）',
    '認知症対応型共同生活介護': '介護（高齢者）', '居宅介護支援': '介護（高齢者）',
    '地域密着型通所介護': '介護（高齢者）', '介護老人福祉施設': '介護（高齢者）',
    '介護老人保健施設': '介護（高齢者）', '訪問入浴介護': '介護（高齢者）',
    '有料老人ホーム': '介護（高齢者）', '訪問看護': '医療', '訪問リハビリテーション': '医療',
    '通所リハビリテーション': '医療', '介護医療院': '医療', 'クリニック': '医療',
    '放課後等デイサービス': '障がい福祉', '就労定着支援': '障がい福祉', '生活介護': '障がい福祉',
    '障がい者施設': '障がい福祉', '障害者施設': '障がい福祉', '保育園': '保育',
}


def complement_facility(row):
    if pd.notna(row.get('FacilityType_Large__c')):
        return row['FacilityType_Large__c']
    ic = row.get('Account.IndustryCategory__c')
    if pd.notna(ic):
        fc = str(ic).split(';')[0].strip()
        if fc in INDUSTRY_MAP:
            return INDUSTRY_MAP[fc]
    st = row.get('Account.ServiceType__c')
    if pd.notna(st) and st in SERVICE_TYPE_MAP:
        return SERVICE_TYPE_MAP[st]
    return None


def emp_cat(n):
    try:
        n = float(n)
    except (ValueError, TypeError):
        return '不明/0'
    if pd.isna(n) or n == 0:
        return '不明/0'
    if n <= 10:
        return '1-10人'
    if n <= 30:
        return '11-30人'
    if n <= 50:
        return '31-50人'
    if n <= 100:
        return '51-100人'
    if n <= 300:
        return '101-300人'
    return '301人+'


def print_rate(label, sub, indent=2):
    n = len(sub)
    w = int(sub['is_won'].sum())
    r = sub['is_won'].mean() * 100 if n > 0 else 0
    print(f'{" " * indent}{label:<35} {n:>5}件 受注{w:>3} 率{r:>5.1f}%')


def main():
    print('=' * 80)
    print('具体的に避けるべきセグメントの特定')
    print('=' * 80)

    service = OpportunityService()
    service.authenticate()

    soql = """SELECT Id, CloseDate, IsWon, IsClosed, OpportunityCategory__c,
        FacilityType_Large__c, LeadSource,
        Account.Name, Account.WonOpportunityies__c, Account.LegalPersonality__c,
        Account.IndustryCategory__c, Account.ServiceType__c,
        Account.Prefectures__c, Account.NumberOfEmployees,
        Account.CustomerSegment_Large__c, Account.CustomerSegment_Small__c,
        Account.NumberOfFacilities__c, Account.Industry
        FROM Opportunity WHERE IsClosed = true
        AND CloseDate >= 2025-04-01 AND CloseDate < 2026-02-01"""
    df = service.bulk_query(soql, 'FY2025詳細')
    print(f'取得: {len(df):,}件')

    df['is_won'] = df['IsWon'].map({'true': 1, 'false': 0, True: 1, False: 0}).astype(int)
    df['CloseDate'] = pd.to_datetime(df['CloseDate'], errors='coerce')
    df['month'] = df['CloseDate'].dt.month

    won_opp = df['Account.WonOpportunityies__c'].fillna(0).astype(float)
    df['past_won_count'] = won_opp - df['is_won']
    df = df[df['past_won_count'] == 0].copy()

    df['facility'] = df.apply(complement_facility, axis=1).fillna('不明')
    lc = df['Account.LegalPersonality__c'].value_counts()
    major = lc[lc >= 100].index.tolist()
    df['legal'] = df['Account.LegalPersonality__c'].apply(
        lambda x: x if x in major else 'その他法人格' if pd.notna(x) and x != '' else '不明')

    first = df[df['OpportunityCategory__c'] == '初回商談'].copy()
    first['emp_cat'] = first['Account.NumberOfEmployees'].apply(emp_cat)
    print(f'FY2025 初回商談: {len(first):,}件 受注率{first["is_won"].mean()*100:.1f}%')

    # ============================================================
    # 1. LeadSource別
    # ============================================================
    print(f'\n{"=" * 80}')
    print('1. LeadSource別受注率（20件以上）')
    print(f'{"=" * 80}')

    ls_stats = []
    for src, sub in first.groupby('LeadSource'):
        if len(sub) >= 20:
            ls_stats.append((src, len(sub), int(sub['is_won'].sum()), sub['is_won'].mean() * 100))
    # NaN
    na_ls = first[first['LeadSource'].isna() | (first['LeadSource'] == '')]
    if len(na_ls) >= 20:
        ls_stats.append(('(未設定)', len(na_ls), int(na_ls['is_won'].sum()), na_ls['is_won'].mean() * 100))

    ls_stats.sort(key=lambda x: x[3])
    for src, n, w, r in ls_stats:
        marker = '✕' if r <= 3.0 else '△' if r <= 5.0 else '○' if r >= 10.0 else '─'
        print(f'  {marker} {src:<35} {n:>5}件 受注{w:>3} 率{r:>5.1f}%')

    # ============================================================
    # 2. 従業員規模別
    # ============================================================
    print(f'\n{"=" * 80}')
    print('2. 従業員規模別受注率')
    print(f'{"=" * 80}')

    for cat in ['不明/0', '1-10人', '11-30人', '31-50人', '51-100人', '101-300人', '301人+']:
        sub = first[first['emp_cat'] == cat]
        if len(sub) >= 10:
            marker = '✕' if sub['is_won'].mean() * 100 <= 3.0 else '△' if sub['is_won'].mean() * 100 <= 5.0 else '○' if sub['is_won'].mean() * 100 >= 10.0 else '─'
            print_rate(f'{marker} {cat}', sub)

    # ============================================================
    # 3. 施設形態 × LeadSource（核心分析）
    # ============================================================
    print(f'\n{"=" * 80}')
    print('3. 施設形態 × LeadSource（避けるべき組み合わせ特定）')
    print(f'{"=" * 80}')

    top_sources = first['LeadSource'].value_counts().head(10).index.tolist()
    for fac in ['介護（高齢者）', '医療', '障がい福祉', '保育', '不明']:
        fac_data = first[first['facility'] == fac]
        if len(fac_data) < 30:
            continue
        print(f'\n  ■ {fac} (全体{len(fac_data)}件 率{fac_data["is_won"].mean() * 100:.1f}%)')
        items = []
        for src in top_sources:
            sub = fac_data[fac_data['LeadSource'] == src]
            if len(sub) >= 10:
                items.append((src, len(sub), int(sub['is_won'].sum()), sub['is_won'].mean() * 100))
        na_sub = fac_data[fac_data['LeadSource'].isna() | (fac_data['LeadSource'] == '')]
        if len(na_sub) >= 10:
            items.append(('(未設定)', len(na_sub), int(na_sub['is_won'].sum()), na_sub['is_won'].mean() * 100))
        items.sort(key=lambda x: x[3])
        for src, n, w, r in items:
            marker = '✕' if r <= 3.0 else '△' if r <= 5.0 else '○' if r >= 10.0 else '─'
            print(f'    {marker} {src:<35} {n:>5}件 受注{w:>3} 率{r:>5.1f}%')

    # ============================================================
    # 4. 施設形態 × 従業員規模
    # ============================================================
    print(f'\n{"=" * 80}')
    print('4. 施設形態 × 従業員規模（避けるべき組み合わせ特定）')
    print(f'{"=" * 80}')

    for fac in ['介護（高齢者）', '医療', '障がい福祉', '保育', '不明']:
        fac_data = first[first['facility'] == fac]
        if len(fac_data) < 30:
            continue
        print(f'\n  ■ {fac} (全体{len(fac_data)}件 率{fac_data["is_won"].mean() * 100:.1f}%)')
        for cat in ['不明/0', '1-10人', '11-30人', '31-50人', '51-100人', '101-300人', '301人+']:
            sub = fac_data[fac_data['emp_cat'] == cat]
            if len(sub) >= 10:
                marker = '✕' if sub['is_won'].mean() * 100 <= 3.0 else '△' if sub['is_won'].mean() * 100 <= 5.0 else '○' if sub['is_won'].mean() * 100 >= 10.0 else '─'
                print_rate(f'{marker} {cat}', sub, indent=4)

    # ============================================================
    # 5. 都道府県別
    # ============================================================
    print(f'\n{"=" * 80}')
    print('5. 都道府県別受注率（上位/下位10）')
    print(f'{"=" * 80}')
    pref_rates = []
    for pref, sub in first.groupby('Account.Prefectures__c'):
        if len(sub) >= 20 and pd.notna(pref) and pref != '':
            pref_rates.append((pref, len(sub), int(sub['is_won'].sum()), sub['is_won'].mean() * 100))
    pref_rates.sort(key=lambda x: -x[3])
    print('  ○上位:')
    for p, n, w, r in pref_rates[:10]:
        print(f'    {p:<10} {n:>5}件 受注{w:>3} 率{r:>5.1f}%')
    print('  ✕下位:')
    for p, n, w, r in pref_rates[-10:]:
        print(f'    {p:<10} {n:>5}件 受注{w:>3} 率{r:>5.1f}%')

    # ============================================================
    # 6. 具体的「避けリスト」の作成
    # ============================================================
    print(f'\n{"=" * 80}')
    print('6. 具体的に避けるべき組み合わせ（受注率3%以下 × 30件以上）')
    print(f'{"=" * 80}')

    # 多次元クロス: 施設形態 × LeadSource × 従業員規模
    avoid_combos = []
    for (fac, src), sub in first.groupby(['facility', 'LeadSource']):
        if len(sub) >= 30:
            rate = sub['is_won'].mean() * 100
            if rate <= 3.0:
                avoid_combos.append({'type': 'facility×source',
                    'desc': f'{fac} × {src}',
                    'count': len(sub), 'won': int(sub['is_won'].sum()), 'rate': rate})

    for (fac, emp), sub in first.groupby(['facility', 'emp_cat']):
        if len(sub) >= 30:
            rate = sub['is_won'].mean() * 100
            if rate <= 3.0:
                avoid_combos.append({'type': 'facility×emp',
                    'desc': f'{fac} × 従業員{emp}',
                    'count': len(sub), 'won': int(sub['is_won'].sum()), 'rate': rate})

    for (src, emp), sub in first.groupby(['LeadSource', 'emp_cat']):
        if len(sub) >= 30:
            rate = sub['is_won'].mean() * 100
            if rate <= 3.0:
                avoid_combos.append({'type': 'source×emp',
                    'desc': f'{src} × 従業員{emp}',
                    'count': len(sub), 'won': int(sub['is_won'].sum()), 'rate': rate})

    avoid_combos.sort(key=lambda x: (x['rate'], -x['count']))
    print(f'\n  {"#":>2} {"組み合わせ":<45} {"件数":>5} {"受注":>3} {"率":>6}')
    print('  ' + '-' * 70)
    for i, c in enumerate(avoid_combos, 1):
        print(f'  {i:>2} {c["desc"]:<45} {c["count"]:>5} {c["won"]:>3} {c["rate"]:>5.1f}%')

    # ============================================================
    # 7. 4月限定: 具体的回避リスト
    # ============================================================
    print(f'\n{"=" * 80}')
    print('7. 4月限定: 具体的回避リスト（施設形態×LeadSource×従業員規模、10件以上で0%）')
    print(f'{"=" * 80}')

    apr = first[first['month'] == 4]
    print(f'  4月全体: {len(apr)}件 受注{apr["is_won"].sum()} 率{apr["is_won"].mean()*100:.1f}%')

    apr_avoid = []
    for (fac, src), sub in apr.groupby(['facility', 'LeadSource']):
        if len(sub) >= 10 and sub['is_won'].sum() == 0:
            apr_avoid.append((f'{fac} × {src}', len(sub)))
    for (fac, emp), sub in apr.groupby(['facility', 'emp_cat']):
        if len(sub) >= 10 and sub['is_won'].sum() == 0:
            apr_avoid.append((f'{fac} × 従業員{emp}', len(sub)))

    apr_avoid.sort(key=lambda x: -x[1])
    for desc, n in apr_avoid:
        print(f'  ✕ {desc:<50} {n:>3}件 受注0')

    print()
    print('=' * 80)
    print('分析完了')
    print('=' * 80)


if __name__ == '__main__':
    main()
