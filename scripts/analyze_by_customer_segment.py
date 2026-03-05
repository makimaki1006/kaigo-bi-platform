# -*- coding: utf-8 -*-
"""
CustomerSegment別の詳細分析
過去失注先 vs 真の新規 でセグメント分けし、
それぞれの中で ServiceType × 従業員規模のクロス集計を行う
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


EMP_ORDER = ['不明/0', '1-10人', '11-30人', '31-50人', '51-100人', '101-300人', '301人+']


def marker(r):
    if r <= 3.0:
        return '✕'
    if r <= 5.0:
        return '△'
    if r >= 15.0:
        return '◎'
    if r >= 10.0:
        return '○'
    return '─'


def analyze_group(data, group_name, min_st=10, min_cross=8):
    """グループ内のServiceType × 従業員規模クロス分析"""
    n = len(data)
    w = int(data['is_won'].sum())
    r = data['is_won'].mean() * 100 if n > 0 else 0
    print(f'\n{"=" * 80}')
    print(f'【{group_name}】 {n:,}件 受注{w} 率{r:.1f}%')
    print(f'{"=" * 80}')

    if n < 10:
        print('  件数不足のためスキップ')
        return

    # 施設形態別
    print(f'\n  ■ 施設形態別')
    for fac in ['介護（高齢者）', '医療', '障がい福祉', '保育', '不明']:
        sub = data[data['facility'] == fac]
        if len(sub) >= 5:
            sw = int(sub['is_won'].sum())
            sr = sub['is_won'].mean() * 100
            m = marker(sr)
            print(f'    {m} {fac:<15} {len(sub):>5}件 受注{sw:>3} 率{sr:>5.1f}%')

    # ServiceType別（主要なもの）
    print(f'\n  ■ サービス種別別（{min_st}件以上）')
    st_stats = []
    for st, sub in data.groupby('service_type'):
        if len(sub) >= min_st:
            st_stats.append((st, sub['facility'].mode().iloc[0] if len(sub) > 0 else '?',
                           len(sub), int(sub['is_won'].sum()), sub['is_won'].mean() * 100))
    st_stats.sort(key=lambda x: -x[4])
    for st, fac, sn, sw, sr in st_stats:
        m = marker(sr)
        print(f'    {m} {st:<30} [{fac:<8}] {sn:>5}件 受注{sw:>3} 率{sr:>5.1f}%')

    # 従業員規模別
    print(f'\n  ■ 従業員規模別')
    for cat in EMP_ORDER:
        sub = data[data['emp_cat'] == cat]
        if len(sub) >= min_st:
            sw = int(sub['is_won'].sum())
            sr = sub['is_won'].mean() * 100
            m = marker(sr)
            print(f'    {m} {cat:<15} {len(sub):>5}件 受注{sw:>3} 率{sr:>5.1f}%')

    # 施設形態 × 従業員規模（主要施設のみ）
    print(f'\n  ■ 施設形態 × 従業員規模（{min_cross}件以上）')
    for fac in ['介護（高齢者）', '医療', '障がい福祉']:
        fac_data = data[data['facility'] == fac]
        if len(fac_data) < 20:
            continue
        fr = fac_data['is_won'].mean() * 100
        print(f'\n    【{fac}】全体{len(fac_data)}件 率{fr:.1f}%')
        for cat in EMP_ORDER:
            sub = fac_data[fac_data['emp_cat'] == cat]
            if len(sub) >= min_cross:
                sw = int(sub['is_won'].sum())
                sr = sub['is_won'].mean() * 100
                m = marker(sr)
                print(f'      {m} {cat:<15} {len(sub):>5}件 受注{sw:>3} 率{sr:>5.1f}%')

    # ServiceType × 従業員規模（主要サービスのみ）
    print(f'\n  ■ サービス種別 × 従業員規模（{min_cross}件以上）')
    major_st = [x[0] for x in st_stats if x[2] >= 30]
    for st in major_st:
        st_data = data[data['service_type'] == st]
        sr = st_data['is_won'].mean() * 100
        fac = st_data['facility'].mode().iloc[0] if len(st_data) > 0 else '?'
        print(f'\n    【{st}】({fac}) 全体{len(st_data)}件 率{sr:.1f}%')
        for cat in EMP_ORDER:
            sub = st_data[st_data['emp_cat'] == cat]
            if len(sub) >= min_cross:
                sw = int(sub['is_won'].sum())
                sr2 = sub['is_won'].mean() * 100
                m = marker(sr2)
                print(f'      {m} {cat:<15} {len(sub):>5}件 受注{sw:>3} 率{sr2:>5.1f}%')

    # 回避/推奨サマリ
    print(f'\n  ■ 回避/推奨サマリ')
    avoid = []
    good = []
    for (st, emp), sub in data.groupby(['service_type', 'emp_cat']):
        if len(sub) >= min_cross:
            sr = sub['is_won'].mean() * 100
            sw = int(sub['is_won'].sum())
            fac = sub['facility'].mode().iloc[0] if len(sub) > 0 else '?'
            entry = (fac, st, emp, len(sub), sw, sr)
            if sr <= 3.0 and len(sub) >= 10:
                avoid.append(entry)
            elif sr >= 15.0 and len(sub) >= 10:
                good.append(entry)

    avoid.sort(key=lambda x: (x[5], -x[3]))
    good.sort(key=lambda x: (-x[5], -x[3]))

    if avoid:
        print(f'\n    ✕ 回避（受注率3%以下、10件以上）')
        for fac, st, emp, n2, w2, r2 in avoid:
            print(f'      {fac}／{st}／{emp}  → {n2}件 受注{w2} 率{r2:.1f}%')
    else:
        print(f'\n    ✕ 回避: 該当なし')

    if good:
        print(f'\n    ◎ 推奨（受注率15%以上、10件以上）')
        for fac, st, emp, n2, w2, r2 in good:
            print(f'      {fac}／{st}／{emp}  → {n2}件 受注{w2} 率{r2:.1f}%')
    else:
        print(f'\n    ◎ 推奨: 該当なし')


def main():
    print('=' * 80)
    print('CustomerSegment別 × ServiceType × 従業員規模 詳細分析')
    print('過去失注先 vs 真の新規 でのセグメント比較')
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
    df = service.bulk_query(soql, 'CS別詳細')
    print(f'取得: {len(df):,}件')

    # 前処理
    df['is_won'] = df['IsWon'].map({'true': 1, 'false': 0, True: 1, False: 0}).astype(int)
    df['CloseDate'] = pd.to_datetime(df['CloseDate'], errors='coerce')
    df['month'] = df['CloseDate'].dt.month

    # 新規フィルタ（既存顧客リピート除外）
    won_opp = df['Account.WonOpportunityies__c'].fillna(0).astype(float)
    df['past_won_count'] = won_opp - df['is_won']
    df = df[df['past_won_count'] == 0].copy()

    # 施設形態補完
    df['facility'] = df.apply(complement_facility, axis=1).fillna('不明')

    # 初回商談のみ
    first = df[df['OpportunityCategory__c'] == '初回商談'].copy()
    first['emp_cat'] = first['Account.NumberOfEmployees'].apply(emp_cat)
    first['service_type'] = first['Account.ServiceType__c'].fillna('(未設定)')
    first['cs_small'] = first['Account.CustomerSegment_Small__c'].fillna('(未設定)')

    print(f'\nFY2025 初回商談: {len(first):,}件 受注率{first["is_won"].mean()*100:.1f}%')

    # ============================================================
    # 0. CustomerSegment分布の再確認
    # ============================================================
    print(f'\n{"=" * 80}')
    print('0. CustomerSegment_Small__c 分布')
    print(f'{"=" * 80}')
    for cs, sub in first.groupby('cs_small'):
        w = int(sub['is_won'].sum())
        r = sub['is_won'].mean() * 100
        print(f'  {cs:<35} {len(sub):>5}件 受注{w:>3} 率{r:>5.1f}%')

    # ============================================================
    # グループ分け
    # ============================================================
    # リピーター/既存顧客（分析から除外）
    exclude_cs = ['01-B 一般顧客', '02-C 過去利用客']
    # 過去失注先
    lost_cs = ['02-D 過去失注先']
    # 離脱客
    churn_cs = ['00-C 離脱客（商談あり）']
    # 商談中（進行中で結果未確定に近い）
    active_cs = ['02-B 商談中', '02-A 商談中（価値合意済）']

    # 除外（実質リピーター）
    repeater = first[first['cs_small'].isin(exclude_cs)]
    print(f'\n  除外（リピーター/既存）: {len(repeater)}件 率{repeater["is_won"].mean()*100:.1f}% → 分析対象外')

    # 分析対象 = リピーター除外
    target = first[~first['cs_small'].isin(exclude_cs)].copy()
    print(f'  分析対象: {len(target):,}件 受注{int(target["is_won"].sum())} 率{target["is_won"].mean()*100:.1f}%')

    # グループ分け
    past_lost = target[target['cs_small'].isin(lost_cs)]
    churned = target[target['cs_small'].isin(churn_cs)]
    in_progress = target[target['cs_small'].isin(active_cs)]
    # 真の新規 = 上記いずれにも該当しない
    true_new = target[~target['cs_small'].isin(lost_cs + churn_cs + active_cs + exclude_cs)]

    print(f'\n  グループ分け:')
    print(f'    A. 過去失注先:     {len(past_lost):>5}件 受注{int(past_lost["is_won"].sum()):>3} 率{past_lost["is_won"].mean()*100:.1f}%')
    print(f'    B. 離脱客:        {len(churned):>5}件 受注{int(churned["is_won"].sum()):>3} 率{churned["is_won"].mean()*100:.1f}%')
    print(f'    C. 商談中:        {len(in_progress):>5}件 受注{int(in_progress["is_won"].sum()):>3} 率{in_progress["is_won"].mean()*100:.1f}%')
    print(f'    D. 真の新規:      {len(true_new):>5}件 受注{int(true_new["is_won"].sum()):>3} 率{true_new["is_won"].mean()*100:.1f}%')

    # ============================================================
    # A. 過去失注先の詳細分析
    # ============================================================
    analyze_group(past_lost, 'A. 過去失注先（02-D）', min_st=10, min_cross=8)

    # ============================================================
    # B. 離脱客の詳細分析
    # ============================================================
    analyze_group(churned, 'B. 離脱客（00-C）', min_st=10, min_cross=8)

    # ============================================================
    # D. 真の新規の詳細分析
    # ============================================================
    analyze_group(true_new, 'D. 真の新規（CSなし/その他）', min_st=5, min_cross=5)

    # ============================================================
    # 月別比較: 過去失注先 vs 真の新規
    # ============================================================
    print(f'\n{"=" * 80}')
    print('月別比較: 過去失注先 vs 真の新規')
    print(f'{"=" * 80}')
    print(f'\n  {"月":>3} │ {"過去失注先":^20} │ {"真の新規":^20} │ {"差":>5}')
    print(f'  {"─" * 3}─┼─{"─" * 20}─┼─{"─" * 20}─┼─{"─" * 5}')
    for m in range(4, 14):
        month = m if m <= 12 else m - 12
        pl_m = past_lost[past_lost['month'] == month]
        tn_m = true_new[true_new['month'] == month]
        if len(pl_m) >= 5 and len(tn_m) >= 5:
            pl_r = pl_m['is_won'].mean() * 100
            tn_r = tn_m['is_won'].mean() * 100
            diff = tn_r - pl_r
            print(f'  {month:>2}月 │ {len(pl_m):>5}件 率{pl_r:>5.1f}% │ {len(tn_m):>5}件 率{tn_r:>5.1f}% │ {diff:>+5.1f}pt')

    # ============================================================
    # 4月限定: 過去失注先 vs 真の新規
    # ============================================================
    print(f'\n{"=" * 80}')
    print('4月限定: グループ別の詳細')
    print(f'{"=" * 80}')

    apr_lost = past_lost[past_lost['month'] == 4]
    apr_new = true_new[true_new['month'] == 4]

    print(f'\n  4月 過去失注先: {len(apr_lost)}件 受注{int(apr_lost["is_won"].sum())} 率{apr_lost["is_won"].mean()*100:.1f}%')
    if len(apr_lost) >= 10:
        print(f'    施設形態別:')
        for fac in ['介護（高齢者）', '医療', '障がい福祉', '保育', '不明']:
            sub = apr_lost[apr_lost['facility'] == fac]
            if len(sub) >= 5:
                sw = int(sub['is_won'].sum())
                sr = sub['is_won'].mean() * 100
                m = marker(sr)
                print(f'      {m} {fac:<15} {len(sub):>4}件 受注{sw:>2} 率{sr:>5.1f}%')

    print(f'\n  4月 真の新規: {len(apr_new)}件 受注{int(apr_new["is_won"].sum())} 率{apr_new["is_won"].mean()*100:.1f}%')
    if len(apr_new) >= 10:
        print(f'    施設形態別:')
        for fac in ['介護（高齢者）', '医療', '障がい福祉', '保育', '不明']:
            sub = apr_new[apr_new['facility'] == fac]
            if len(sub) >= 3:
                sw = int(sub['is_won'].sum())
                sr = sub['is_won'].mean() * 100
                m = marker(sr)
                print(f'      {m} {fac:<15} {len(sub):>4}件 受注{sw:>2} 率{sr:>5.1f}%')

    print()
    print('=' * 80)
    print('分析完了')
    print('=' * 80)


if __name__ == '__main__':
    main()
