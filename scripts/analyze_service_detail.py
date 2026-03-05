# -*- coding: utf-8 -*-
"""
施設形態の中身を掘り下げる: ServiceType__c レベルの受注率分析
障がい福祉/医療/介護 それぞれのサービス種別ごとの成約パターンを特定
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


def print_section(title):
    print(f'\n{"=" * 80}')
    print(title)
    print(f'{"=" * 80}')


def main():
    print('=' * 80)
    print('サービス種別(ServiceType__c)レベルの詳細分析')
    print('障がい福祉/医療/介護 の中身を掘り下げる')
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
    df = service.bulk_query(soql, 'サービス種別詳細')
    print(f'取得: {len(df):,}件')

    # 前処理
    df['is_won'] = df['IsWon'].map({'true': 1, 'false': 0, True: 1, False: 0}).astype(int)
    df['CloseDate'] = pd.to_datetime(df['CloseDate'], errors='coerce')
    df['month'] = df['CloseDate'].dt.month

    # 新規フィルタ
    won_opp = df['Account.WonOpportunityies__c'].fillna(0).astype(float)
    df['past_won_count'] = won_opp - df['is_won']
    df = df[df['past_won_count'] == 0].copy()

    # 施設形態補完
    df['facility'] = df.apply(complement_facility, axis=1).fillna('不明')

    # 初回商談のみ
    first = df[df['OpportunityCategory__c'] == '初回商談'].copy()
    first['emp_cat'] = first['Account.NumberOfEmployees'].apply(emp_cat)

    # ServiceType__c を直接使う（補完済み施設形態との紐付け）
    first['service_type'] = first['Account.ServiceType__c'].fillna('(未設定)')

    print(f'FY2025 初回商談: {len(first):,}件 受注率{first["is_won"].mean()*100:.1f}%')

    # ============================================================
    # 0. ServiceType__c の値分布（全体像把握）
    # ============================================================
    print_section('0. ServiceType__c 値分布（10件以上）')
    st_counts = first['service_type'].value_counts()
    for st, cnt in st_counts.items():
        if cnt >= 10:
            sub = first[first['service_type'] == st]
            w = int(sub['is_won'].sum())
            r = sub['is_won'].mean() * 100
            fac = sub['facility'].mode().iloc[0] if len(sub) > 0 else '?'
            marker = '✕' if r <= 3.0 else '△' if r <= 5.0 else '○' if r >= 10.0 else '─'
            print(f'  {marker} {st:<30} [{fac:<10}] {cnt:>5}件 受注{w:>3} 率{r:>5.1f}%')

    # ============================================================
    # 0.5 CustomerSegment_Large__c / Small__c 値分布
    # ============================================================
    print_section('0.5 CustomerSegment フィールドの値分布')
    for col in ['Account.CustomerSegment_Large__c', 'Account.CustomerSegment_Small__c']:
        print(f'\n  ■ {col}')
        vals = first[col].fillna('(未設定)').value_counts()
        for v, cnt in vals.items():
            if cnt >= 10:
                sub = first[first[col].fillna('(未設定)') == v]
                w = int(sub['is_won'].sum())
                r = sub['is_won'].mean() * 100
                marker = '✕' if r <= 3.0 else '△' if r <= 5.0 else '○' if r >= 10.0 else '─'
                print(f'    {marker} {str(v):<35} {cnt:>5}件 受注{w:>3} 率{r:>5.1f}%')

    # ============================================================
    # 1. 障がい福祉の内訳
    # ============================================================
    print_section('1. 障がい福祉 の ServiceType 別受注率')
    shogai = first[first['facility'] == '障がい福祉']
    print(f'  全体: {len(shogai)}件 受注{int(shogai["is_won"].sum())} 率{shogai["is_won"].mean()*100:.1f}%')

    shogai_st = []
    for st, sub in shogai.groupby('service_type'):
        shogai_st.append((st, len(sub), int(sub['is_won'].sum()), sub['is_won'].mean() * 100))
    shogai_st.sort(key=lambda x: -x[1])
    print(f'\n  {"サービス種別":<30} {"件数":>5} {"受注":>3} {"率":>6}')
    print('  ' + '-' * 55)
    for st, n, w, r in shogai_st:
        if n >= 5:
            marker = '✕' if r <= 3.0 else '△' if r <= 5.0 else '○' if r >= 10.0 else '─'
            print(f'  {marker} {st:<30} {n:>5}件 受注{w:>3} 率{r:>5.1f}%')

    # 障がい福祉 ServiceType × 従業員規模
    print(f'\n  ■ 障がい福祉 ServiceType × 従業員規模（10件以上）')
    for st in [x[0] for x in shogai_st if x[1] >= 30]:
        st_data = shogai[shogai['service_type'] == st]
        print(f'\n    【{st}】 全体{len(st_data)}件 率{st_data["is_won"].mean()*100:.1f}%')
        for cat in ['不明/0', '1-10人', '11-30人', '31-50人', '51-100人', '101-300人', '301人+']:
            sub = st_data[st_data['emp_cat'] == cat]
            if len(sub) >= 10:
                w = int(sub['is_won'].sum())
                r = sub['is_won'].mean() * 100
                marker = '✕' if r <= 3.0 else '△' if r <= 5.0 else '○' if r >= 10.0 else '─'
                print(f'      {marker} {cat:<15} {len(sub):>5}件 受注{w:>3} 率{r:>5.1f}%')

    # ============================================================
    # 2. 医療の内訳
    # ============================================================
    print_section('2. 医療 の ServiceType 別受注率')
    iryo = first[first['facility'] == '医療']
    print(f'  全体: {len(iryo)}件 受注{int(iryo["is_won"].sum())} 率{iryo["is_won"].mean()*100:.1f}%')

    iryo_st = []
    for st, sub in iryo.groupby('service_type'):
        iryo_st.append((st, len(sub), int(sub['is_won'].sum()), sub['is_won'].mean() * 100))
    iryo_st.sort(key=lambda x: -x[1])
    print(f'\n  {"サービス種別":<30} {"件数":>5} {"受注":>3} {"率":>6}')
    print('  ' + '-' * 55)
    for st, n, w, r in iryo_st:
        if n >= 5:
            marker = '✕' if r <= 3.0 else '△' if r <= 5.0 else '○' if r >= 10.0 else '─'
            print(f'  {marker} {st:<30} {n:>5}件 受注{w:>3} 率{r:>5.1f}%')

    # 医療 ServiceType × 従業員規模
    print(f'\n  ■ 医療 ServiceType × 従業員規模（10件以上）')
    for st in [x[0] for x in iryo_st if x[1] >= 30]:
        st_data = iryo[iryo['service_type'] == st]
        print(f'\n    【{st}】 全体{len(st_data)}件 率{st_data["is_won"].mean()*100:.1f}%')
        for cat in ['不明/0', '1-10人', '11-30人', '31-50人', '51-100人', '101-300人', '301人+']:
            sub = st_data[st_data['emp_cat'] == cat]
            if len(sub) >= 10:
                w = int(sub['is_won'].sum())
                r = sub['is_won'].mean() * 100
                marker = '✕' if r <= 3.0 else '△' if r <= 5.0 else '○' if r >= 10.0 else '─'
                print(f'      {marker} {cat:<15} {len(sub):>5}件 受注{w:>3} 率{r:>5.1f}%')

    # ============================================================
    # 3. 介護の内訳
    # ============================================================
    print_section('3. 介護（高齢者）の ServiceType 別受注率')
    kaigo = first[first['facility'] == '介護（高齢者）']
    print(f'  全体: {len(kaigo)}件 受注{int(kaigo["is_won"].sum())} 率{kaigo["is_won"].mean()*100:.1f}%')

    kaigo_st = []
    for st, sub in kaigo.groupby('service_type'):
        kaigo_st.append((st, len(sub), int(sub['is_won'].sum()), sub['is_won'].mean() * 100))
    kaigo_st.sort(key=lambda x: -x[1])
    print(f'\n  {"サービス種別":<30} {"件数":>5} {"受注":>3} {"率":>6}')
    print('  ' + '-' * 55)
    for st, n, w, r in kaigo_st:
        if n >= 5:
            marker = '✕' if r <= 3.0 else '△' if r <= 5.0 else '○' if r >= 10.0 else '─'
            print(f'  {marker} {st:<30} {n:>5}件 受注{w:>3} 率{r:>5.1f}%')

    # 介護 ServiceType × 従業員規模
    print(f'\n  ■ 介護 ServiceType × 従業員規模（10件以上）')
    for st in [x[0] for x in kaigo_st if x[1] >= 30]:
        st_data = kaigo[kaigo['service_type'] == st]
        print(f'\n    【{st}】 全体{len(st_data)}件 率{st_data["is_won"].mean()*100:.1f}%')
        for cat in ['不明/0', '1-10人', '11-30人', '31-50人', '51-100人', '101-300人', '301人+']:
            sub = st_data[st_data['emp_cat'] == cat]
            if len(sub) >= 10:
                w = int(sub['is_won'].sum())
                r = sub['is_won'].mean() * 100
                marker = '✕' if r <= 3.0 else '△' if r <= 5.0 else '○' if r >= 10.0 else '─'
                print(f'      {marker} {cat:<15} {len(sub):>5}件 受注{w:>3} 率{r:>5.1f}%')

    # ============================================================
    # 4. 保育の内訳（放課後等デイが混ざっていないか確認）
    # ============================================================
    print_section('4. 保育 の ServiceType 別受注率')
    hoiku = first[first['facility'] == '保育']
    print(f'  全体: {len(hoiku)}件 受注{int(hoiku["is_won"].sum())} 率{hoiku["is_won"].mean()*100:.1f}%')
    for st, sub in hoiku.groupby('service_type'):
        w = int(sub['is_won'].sum())
        r = sub['is_won'].mean() * 100
        print(f'  {st:<30} {len(sub):>5}件 受注{w:>3} 率{r:>5.1f}%')

    # ============================================================
    # 5. 全体サマリ: 避けるべき/攻めるべき ServiceType
    # ============================================================
    print_section('5. サービス種別 × 従業員規模 回避/推奨リスト（30件以上）')

    avoid_list = []
    good_list = []
    for (st, emp), sub in first.groupby(['service_type', 'emp_cat']):
        n = len(sub)
        if n >= 20:
            r = sub['is_won'].mean() * 100
            w = int(sub['is_won'].sum())
            fac = sub['facility'].mode().iloc[0] if len(sub) > 0 else '?'
            entry = {'service_type': st, 'emp_cat': emp, 'facility': fac,
                     'count': n, 'won': w, 'rate': r}
            if r <= 3.0:
                avoid_list.append(entry)
            elif r >= 12.0:
                good_list.append(entry)

    avoid_list.sort(key=lambda x: (x['rate'], -x['count']))
    good_list.sort(key=lambda x: (-x['rate'], -x['count']))

    print(f'\n  ✕ 回避リスト（受注率3%以下、20件以上）')
    print(f'  {"#":>2} {"施設形態":<10} {"サービス種別":<25} {"従業員規模":<12} {"件数":>5} {"受注":>3} {"率":>6}')
    print('  ' + '-' * 75)
    for i, c in enumerate(avoid_list, 1):
        print(f'  {i:>2} {c["facility"]:<10} {c["service_type"]:<25} {c["emp_cat"]:<12} {c["count"]:>5} {c["won"]:>3} {c["rate"]:>5.1f}%')

    print(f'\n  ○ 推奨リスト（受注率12%以上、20件以上）')
    print(f'  {"#":>2} {"施設形態":<10} {"サービス種別":<25} {"従業員規模":<12} {"件数":>5} {"受注":>3} {"率":>6}')
    print('  ' + '-' * 75)
    for i, c in enumerate(good_list, 1):
        print(f'  {i:>2} {c["facility"]:<10} {c["service_type"]:<25} {c["emp_cat"]:<12} {c["count"]:>5} {c["won"]:>3} {c["rate"]:>5.1f}%')

    # ============================================================
    # 6. IndustryCategory__c の多値分析（兼業パターン）
    # ============================================================
    print_section('6. IndustryCategory__c 兼業パターン（10件以上）')
    first['industry_raw'] = first['Account.IndustryCategory__c'].fillna('(未設定)')
    ic_rates = []
    for ic, sub in first.groupby('industry_raw'):
        if len(sub) >= 10:
            w = int(sub['is_won'].sum())
            r = sub['is_won'].mean() * 100
            ic_rates.append((ic, len(sub), w, r))
    ic_rates.sort(key=lambda x: -x[3])
    for ic, n, w, r in ic_rates:
        marker = '✕' if r <= 3.0 else '△' if r <= 5.0 else '○' if r >= 10.0 else '─'
        print(f'  {marker} {ic:<40} {n:>5}件 受注{w:>3} 率{r:>5.1f}%')

    print()
    print('=' * 80)
    print('分析完了')
    print('=' * 80)


if __name__ == '__main__':
    main()
