# -*- coding: utf-8 -*-
"""
訪問看護に特化した深掘り分析
施設形態 × 新規/再商談 × 従業員規模のクロス
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

EMP_ORDER = ['不明/0', '1-10人', '11-30人', '31-50人', '51-100人', '101-300人', '301人+']


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
    if n <= 10: return '1-10人'
    if n <= 30: return '11-30人'
    if n <= 50: return '31-50人'
    if n <= 100: return '51-100人'
    if n <= 300: return '101-300人'
    return '301人+'


def marker(r):
    if r <= 3.0: return '✕'
    if r <= 5.0: return '△'
    if r >= 15.0: return '◎'
    if r >= 10.0: return '○'
    return '─'


def main():
    print('=' * 80)
    print('訪問看護 特化分析')
    print('施設形態(FacilityType) × 新規/再商談 × 従業員規模')
    print('=' * 80)

    service = OpportunityService()
    service.authenticate()

    # 全商談履歴（新規/再商談判定用）
    soql_all = """SELECT Id, AccountId, CloseDate, IsWon, IsClosed
        FROM Opportunity WHERE IsClosed = true"""
    df_all = service.bulk_query(soql_all, '全商談')
    df_all['is_won'] = df_all['IsWon'].map({'true': 1, 'false': 0, True: 1, False: 0}).astype(int)
    df_all['CloseDate'] = pd.to_datetime(df_all['CloseDate'], errors='coerce')

    # FY2025以前の履歴
    pre_fy = df_all[df_all['CloseDate'] < '2025-04-01']
    pre_history = pre_fy.groupby('AccountId').agg(
        pre_opp_count=('Id', 'count'),
        pre_won_count=('is_won', 'sum')
    ).reset_index()

    # FY2025詳細
    soql_fy = """SELECT Id, AccountId, CloseDate, IsWon, IsClosed, OpportunityCategory__c,
        FacilityType_Large__c,
        Account.Name, Account.WonOpportunityies__c, Account.LegalPersonality__c,
        Account.IndustryCategory__c, Account.ServiceType__c,
        Account.Prefectures__c, Account.NumberOfEmployees
        FROM Opportunity WHERE IsClosed = true
        AND CloseDate >= 2025-04-01 AND CloseDate < 2026-02-01"""
    df_fy = service.bulk_query(soql_fy, 'FY2025')
    df_fy['is_won'] = df_fy['IsWon'].map({'true': 1, 'false': 0, True: 1, False: 0}).astype(int)
    df_fy['CloseDate'] = pd.to_datetime(df_fy['CloseDate'], errors='coerce')
    df_fy['month'] = df_fy['CloseDate'].dt.month

    # 履歴結合 & カテゴリ判定
    df_fy = df_fy.merge(pre_history, on='AccountId', how='left')
    df_fy['pre_opp_count'] = df_fy['pre_opp_count'].fillna(0).astype(int)
    df_fy['pre_won_count'] = df_fy['pre_won_count'].fillna(0).astype(int)

    def classify(row):
        if row['pre_opp_count'] == 0: return '新規'
        elif row['pre_won_count'] > 0: return '解約後'
        else: return '失注後'
    df_fy['biz_cat'] = df_fy.apply(classify, axis=1)

    # 施設形態補完
    df_fy['facility'] = df_fy.apply(complement_facility, axis=1).fillna('不明')
    df_fy['service_type'] = df_fy['Account.ServiceType__c'].fillna('(未設定)')
    df_fy['emp_cat'] = df_fy['Account.NumberOfEmployees'].apply(emp_cat)

    # 初回商談 & 解約後除外
    first = df_fy[(df_fy['OpportunityCategory__c'] == '初回商談') & (df_fy['biz_cat'] != '解約後')].copy()

    # ============================================================
    # 訪問看護を抽出
    # ============================================================
    houmon = first[first['service_type'] == '訪問看護']
    other = first[first['service_type'] != '訪問看護']

    print(f'\n全体: {len(first):,}件 受注{int(first["is_won"].sum())} 率{first["is_won"].mean()*100:.1f}%')
    print(f'訪問看護: {len(houmon)}件 受注{int(houmon["is_won"].sum())} 率{houmon["is_won"].mean()*100:.1f}%')
    print(f'訪問看護以外: {len(other):,}件 受注{int(other["is_won"].sum())} 率{other["is_won"].mean()*100:.1f}%')

    # ============================================================
    # 訪問看護 × 施設形態
    # ============================================================
    print(f'\n{"=" * 80}')
    print('1. 訪問看護 × 施設形態（FacilityType_Large__c）')
    print(f'{"=" * 80}')

    for fac in ['介護（高齢者）', '医療', '障がい福祉', '保育', '不明']:
        sub = houmon[houmon['facility'] == fac]
        if len(sub) >= 3:
            r = sub['is_won'].mean() * 100
            print(f'  {marker(r)} {fac:<15} {len(sub):>4}件 受注{int(sub["is_won"].sum()):>3} 率{r:>5.1f}%')

    # ============================================================
    # 訪問看護 × 施設形態 × 新規/再商談
    # ============================================================
    print(f'\n{"=" * 80}')
    print('2. 訪問看護 × 施設形態 × 新規/失注後再商談')
    print(f'{"=" * 80}')

    for fac in ['介護（高齢者）', '医療', '障がい福祉', '不明']:
        fac_data = houmon[houmon['facility'] == fac]
        if len(fac_data) < 5:
            continue
        print(f'\n  ■ 訪問看護 × {fac}（全体{len(fac_data)}件 率{fac_data["is_won"].mean()*100:.1f}%）')
        for cat in ['新規', '失注後']:
            sub = fac_data[fac_data['biz_cat'] == cat]
            if len(sub) >= 3:
                r = sub['is_won'].mean() * 100
                print(f'    {marker(r)} {cat:<10} {len(sub):>4}件 受注{int(sub["is_won"].sum()):>3} 率{r:>5.1f}%')

    # ============================================================
    # 訪問看護 × 施設形態 × 従業員規模
    # ============================================================
    print(f'\n{"=" * 80}')
    print('3. 訪問看護 × 施設形態 × 従業員規模')
    print(f'{"=" * 80}')

    for fac in ['介護（高齢者）', '医療', '障がい福祉', '不明']:
        fac_data = houmon[houmon['facility'] == fac]
        if len(fac_data) < 10:
            continue
        print(f'\n  ■ 訪問看護 × {fac}（全体{len(fac_data)}件 率{fac_data["is_won"].mean()*100:.1f}%）')
        for cat in EMP_ORDER:
            sub = fac_data[fac_data['emp_cat'] == cat]
            if len(sub) >= 3:
                r = sub['is_won'].mean() * 100
                print(f'    {marker(r)} {cat:<15} {len(sub):>4}件 受注{int(sub["is_won"].sum()):>3} 率{r:>5.1f}%')

    # ============================================================
    # 訪問看護（新規のみ）× 施設形態 × 従業員規模
    # ============================================================
    print(f'\n{"=" * 80}')
    print('4. 訪問看護 × 新規のみ × 施設形態 × 従業員規模')
    print(f'{"=" * 80}')

    houmon_new = houmon[houmon['biz_cat'] == '新規']
    print(f'  訪問看護 新規全体: {len(houmon_new)}件 受注{int(houmon_new["is_won"].sum())} 率{houmon_new["is_won"].mean()*100:.1f}%')

    for fac in ['介護（高齢者）', '医療', '障がい福祉', '不明']:
        fac_data = houmon_new[houmon_new['facility'] == fac]
        if len(fac_data) < 3:
            continue
        print(f'\n  ■ 訪問看護 × 新規 × {fac}（{len(fac_data)}件 率{fac_data["is_won"].mean()*100:.1f}%）')
        for cat in EMP_ORDER:
            sub = fac_data[fac_data['emp_cat'] == cat]
            if len(sub) >= 3:
                r = sub['is_won'].mean() * 100
                print(f'    {marker(r)} {cat:<15} {len(sub):>4}件 受注{int(sub["is_won"].sum()):>3} 率{r:>5.1f}%')

    # ============================================================
    # 月別: 訪問看護 × 施設形態
    # ============================================================
    print(f'\n{"=" * 80}')
    print('5. 月別: 訪問看護の施設形態別受注率（新規のみ）')
    print(f'{"=" * 80}')

    print(f'\n  {"月":>3} │ {"介護法人の訪問看護":^22} │ {"医療法人の訪問看護":^22}')
    print(f'  {"─"*3}─┼─{"─"*22}─┼─{"─"*22}')
    for m in range(4, 14):
        month = m if m <= 12 else m - 12
        k_sub = houmon_new[(houmon_new['facility'] == '介護（高齢者）') & (houmon_new['month'] == month)]
        i_sub = houmon_new[(houmon_new['facility'] == '医療') & (houmon_new['month'] == month)]
        k_str = f'{len(k_sub):>3}件 率{k_sub["is_won"].mean()*100:>5.1f}%' if len(k_sub) >= 3 else f'{len(k_sub):>3}件   ---  '
        i_str = f'{len(i_sub):>3}件 率{i_sub["is_won"].mean()*100:>5.1f}%' if len(i_sub) >= 3 else f'{len(i_sub):>3}件   ---  '
        print(f'  {month:>2}月 │ {k_str:^22} │ {i_str:^22}')

    # ============================================================
    # 訪問看護の法人格
    # ============================================================
    print(f'\n{"=" * 80}')
    print('6. 訪問看護（新規）× 法人格')
    print(f'{"=" * 80}')

    houmon_new['legal'] = houmon_new['Account.LegalPersonality__c'].fillna('不明')
    for leg, sub in houmon_new.groupby('legal'):
        if len(sub) >= 3:
            r = sub['is_won'].mean() * 100
            print(f'  {marker(r)} {leg:<20} {len(sub):>4}件 受注{int(sub["is_won"].sum()):>3} 率{r:>5.1f}%')

    # ============================================================
    # 比較: 全サービス種別ランキング（新規のみ、10件以上）
    # ============================================================
    print(f'\n{"=" * 80}')
    print('7. 全サービス種別ランキング（新規のみ、施設形態別、10件以上）')
    print(f'{"=" * 80}')

    new_only = first[first['biz_cat'] == '新規']
    rankings = []
    for (fac, st), sub in new_only.groupby(['facility', 'service_type']):
        if len(sub) >= 10:
            r = sub['is_won'].mean() * 100
            rankings.append((fac, st, len(sub), int(sub['is_won'].sum()), r))
    rankings.sort(key=lambda x: -x[4])

    print(f'\n  {"#":>2} {"施設形態":<12} {"サービス種別":<28} {"件数":>5} {"受注":>3} {"率":>6}')
    print('  ' + '-' * 65)
    for i, (fac, st, n, w, r) in enumerate(rankings, 1):
        m = marker(r)
        print(f'  {i:>2} {m} {fac:<12} {st:<28} {n:>5}件 {w:>3} {r:>5.1f}%')

    print()
    print('=' * 80)
    print('分析完了')
    print('=' * 80)


if __name__ == '__main__':
    main()
