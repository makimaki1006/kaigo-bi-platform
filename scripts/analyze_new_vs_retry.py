# -*- coding: utf-8 -*-
"""
新規商談 vs 失注後再商談 の正確な判定
CustomerSegment（結果指標）ではなく、
Accountの商談履歴から「初めての商談か、過去に失注があるか」を判定する
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


def marker(r):
    if r <= 3.0: return '✕'
    if r <= 5.0: return '△'
    if r >= 15.0: return '◎'
    if r >= 10.0: return '○'
    return '─'


def print_section(title):
    print(f'\n{"=" * 80}')
    print(title)
    print(f'{"=" * 80}')


def analyze_detail(data, label, min_n=10):
    """施設形態別・サービス種別別・従業員規模別・クロスの詳細分析"""
    n = len(data)
    if n < 10:
        print(f'  件数不足({n}件)のためスキップ')
        return

    # 施設形態別
    print(f'\n  ■ 施設形態別')
    for fac in ['介護（高齢者）', '医療', '障がい福祉', '保育', '不明']:
        sub = data[data['facility'] == fac]
        if len(sub) >= 5:
            sr = sub['is_won'].mean() * 100
            print(f'    {marker(sr)} {fac:<15} {len(sub):>5}件 受注{int(sub["is_won"].sum()):>3} 率{sr:>5.1f}%')

    # サービス種別別
    print(f'\n  ■ サービス種別別（{min_n}件以上）')
    st_list = []
    for st, sub in data.groupby('service_type'):
        if len(sub) >= min_n:
            sr = sub['is_won'].mean() * 100
            fac = sub['facility'].mode().iloc[0] if len(sub) > 0 else '?'
            st_list.append((st, fac, len(sub), int(sub['is_won'].sum()), sr))
    st_list.sort(key=lambda x: -x[4])
    for st, fac, sn, sw, sr in st_list:
        print(f'    {marker(sr)} {st:<30} [{fac:<8}] {sn:>5}件 受注{sw:>3} 率{sr:>5.1f}%')

    # 従業員規模別
    print(f'\n  ■ 従業員規模別')
    for cat in EMP_ORDER:
        sub = data[data['emp_cat'] == cat]
        if len(sub) >= min_n:
            sr = sub['is_won'].mean() * 100
            print(f'    {marker(sr)} {cat:<15} {len(sub):>5}件 受注{int(sub["is_won"].sum()):>3} 率{sr:>5.1f}%')

    # 施設形態 × 従業員規模
    print(f'\n  ■ 施設形態 × 従業員規模（{min_n}件以上）')
    for fac in ['介護（高齢者）', '医療', '障がい福祉', '保育']:
        fac_data = data[data['facility'] == fac]
        if len(fac_data) < 20:
            continue
        fr = fac_data['is_won'].mean() * 100
        print(f'\n    【{fac}】全体{len(fac_data)}件 率{fr:.1f}%')
        for cat in EMP_ORDER:
            sub = fac_data[fac_data['emp_cat'] == cat]
            if len(sub) >= min_n:
                sr = sub['is_won'].mean() * 100
                print(f'      {marker(sr)} {cat:<15} {len(sub):>5}件 受注{int(sub["is_won"].sum()):>3} 率{sr:>5.1f}%')

    # サービス種別 × 従業員規模（主要サービスのみ）
    major_st = [x[0] for x in st_list if x[2] >= 30]
    if major_st:
        print(f'\n  ■ サービス種別 × 従業員規模（{min_n}件以上、主要サービス）')
        for st in major_st:
            st_data = data[data['service_type'] == st]
            sr = st_data['is_won'].mean() * 100
            fac = st_data['facility'].mode().iloc[0] if len(st_data) > 0 else '?'
            print(f'\n    【{st}】({fac}) 全体{len(st_data)}件 率{sr:.1f}%')
            for cat in EMP_ORDER:
                sub = st_data[st_data['emp_cat'] == cat]
                if len(sub) >= min_n:
                    sr2 = sub['is_won'].mean() * 100
                    print(f'      {marker(sr2)} {cat:<15} {len(sub):>5}件 受注{int(sub["is_won"].sum()):>3} 率{sr2:>5.1f}%')


def main():
    print('=' * 80)
    print('新規商談 vs 失注後再商談（商談履歴ベースの正確な判定）')
    print('=' * 80)
    print()
    print('方法: Accountの全商談履歴から、各FY2025商談が')
    print('      「そのAccountにとって初めての商談か」を判定する')
    print()

    service = OpportunityService()
    service.authenticate()

    # Step 1: 全期間の全Opportunity（履歴構築用）
    print('Step 1: 全期間の商談履歴を取得...')
    soql_all = """SELECT Id, AccountId, CloseDate, IsWon, IsClosed,
        OpportunityCategory__c
        FROM Opportunity WHERE IsClosed = true"""
    df_all = service.bulk_query(soql_all, '全商談履歴')
    df_all['is_won'] = df_all['IsWon'].map({'true': 1, 'false': 0, True: 1, False: 0}).astype(int)
    df_all['CloseDate'] = pd.to_datetime(df_all['CloseDate'], errors='coerce')
    print(f'  全商談: {len(df_all):,}件')

    # Step 2: FY2025の詳細データ
    print('\nStep 2: FY2025の詳細データを取得...')
    soql_fy = """SELECT Id, AccountId, CloseDate, IsWon, IsClosed, OpportunityCategory__c,
        FacilityType_Large__c,
        Account.Name, Account.WonOpportunityies__c, Account.LegalPersonality__c,
        Account.IndustryCategory__c, Account.ServiceType__c,
        Account.Prefectures__c, Account.NumberOfEmployees,
        Account.CustomerSegment_Large__c, Account.CustomerSegment_Small__c
        FROM Opportunity WHERE IsClosed = true
        AND CloseDate >= 2025-04-01 AND CloseDate < 2026-02-01"""
    df_fy = service.bulk_query(soql_fy, 'FY2025詳細')
    df_fy['is_won'] = df_fy['IsWon'].map({'true': 1, 'false': 0, True: 1, False: 0}).astype(int)
    df_fy['CloseDate'] = pd.to_datetime(df_fy['CloseDate'], errors='coerce')
    print(f'  FY2025商談: {len(df_fy):,}件')

    # Step 3: Accountごとの商談履歴を構築
    print('\nStep 3: Account別の商談履歴を構築...')
    # 各Accountの最初のCloseDate、CloseDate < 2025-04-01の商談数、過去受注数
    history = df_all.groupby('AccountId').agg(
        first_close=('CloseDate', 'min'),
        total_opps=('Id', 'count'),
        total_won=('is_won', 'sum')
    ).reset_index()

    # FY2025以前の商談履歴
    pre_fy = df_all[df_all['CloseDate'] < '2025-04-01']
    pre_history = pre_fy.groupby('AccountId').agg(
        pre_opp_count=('Id', 'count'),
        pre_won_count=('is_won', 'sum'),
        pre_lost_count=('Id', lambda x: len(x) - pre_fy.loc[x.index, 'is_won'].sum())
    ).reset_index()

    print(f'  全Account数: {len(history):,}')
    print(f'  FY2025以前に商談歴あり: {len(pre_history):,}')

    # Step 4: FY2025データに履歴を結合
    df_fy = df_fy.merge(pre_history, on='AccountId', how='left')
    df_fy['pre_opp_count'] = df_fy['pre_opp_count'].fillna(0).astype(int)
    df_fy['pre_won_count'] = df_fy['pre_won_count'].fillna(0).astype(int)
    df_fy['pre_lost_count'] = df_fy['pre_lost_count'].fillna(0).astype(int)

    # カテゴリ判定
    # 新規商談: FY2025以前に商談履歴がない
    # 失注後再商談: FY2025以前に失注履歴がある（受注歴なし）
    # 解約後再商談: FY2025以前に受注履歴がある → 対象外
    def classify(row):
        if row['pre_opp_count'] == 0:
            return '新規商談'
        elif row['pre_won_count'] > 0:
            return '解約後再商談'
        else:
            return '失注後再商談'

    df_fy['biz_cat'] = df_fy.apply(classify, axis=1)

    # 施設形態補完等
    df_fy['facility'] = df_fy.apply(complement_facility, axis=1).fillna('不明')
    df_fy['service_type'] = df_fy['Account.ServiceType__c'].fillna('(未設定)')
    df_fy['emp_cat'] = df_fy['Account.NumberOfEmployees'].apply(emp_cat)
    df_fy['month'] = df_fy['CloseDate'].dt.month

    # 初回商談のみ
    first = df_fy[df_fy['OpportunityCategory__c'] == '初回商談'].copy()

    # ============================================================
    # 全体分布
    # ============================================================
    print_section('1. 3カテゴリ分布（初回商談のみ）')

    print(f'\n  初回商談全体: {len(first):,}件 受注{int(first["is_won"].sum())} 率{first["is_won"].mean()*100:.1f}%')
    print(f'\n  {"カテゴリ":<20} {"件数":>6} {"受注":>4} {"受注率":>7} {"割合":>6}')
    print('  ' + '-' * 55)
    for cat in ['新規商談', '失注後再商談', '解約後再商談']:
        sub = first[first['biz_cat'] == cat]
        if len(sub) > 0:
            r = sub['is_won'].mean() * 100
            pct = len(sub) / len(first) * 100
            print(f'  {cat:<20} {len(sub):>6}件 {int(sub["is_won"].sum()):>4} {r:>6.1f}% {pct:>5.1f}%')

    # 解約後再商談を除外
    target = first[first['biz_cat'] != '解約後再商談'].copy()
    print(f'\n  分析対象（解約後除外）: {len(target):,}件 受注{int(target["is_won"].sum())} 率{target["is_won"].mean()*100:.1f}%')

    # ============================================================
    # 2. 新規商談の詳細
    # ============================================================
    new_data = target[target['biz_cat'] == '新規商談']
    print_section(f'2. 新規商談の詳細分析（{len(new_data):,}件 率{new_data["is_won"].mean()*100:.1f}%）')
    analyze_detail(new_data, '新規商談', min_n=10)

    # ============================================================
    # 3. 失注後再商談の詳細
    # ============================================================
    retry_data = target[target['biz_cat'] == '失注後再商談']
    print_section(f'3. 失注後再商談の詳細分析（{len(retry_data):,}件 率{retry_data["is_won"].mean()*100:.1f}%）')

    # 過去失注回数別
    print(f'\n  ■ 過去失注回数別')
    for n_lost in sorted(retry_data['pre_lost_count'].unique()):
        sub = retry_data[retry_data['pre_lost_count'] == n_lost]
        if len(sub) >= 5:
            sr = sub['is_won'].mean() * 100
            print(f'    {marker(sr)} 過去{int(n_lost)}回失注  {len(sub):>5}件 受注{int(sub["is_won"].sum()):>3} 率{sr:>5.1f}%')

    analyze_detail(retry_data, '失注後再商談', min_n=10)

    # ============================================================
    # 4. 月別比較
    # ============================================================
    print_section('4. 月別: 新規 vs 失注後再商談')
    print(f'\n  {"月":>3} │ {"新規商談":^25} │ {"失注後再商談":^25}')
    print(f'  {"─"*3}─┼─{"─"*25}─┼─{"─"*25}')
    for m in range(4, 14):
        month = m if m <= 12 else m - 12
        new_m = new_data[new_data['month'] == month]
        ret_m = retry_data[retry_data['month'] == month]
        new_str = f'{len(new_m):>5}件 率{new_m["is_won"].mean()*100:>5.1f}%' if len(new_m) >= 3 else f'{len(new_m):>5}件   ---  '
        ret_str = f'{len(ret_m):>5}件 率{ret_m["is_won"].mean()*100:>5.1f}%' if len(ret_m) >= 3 else f'{len(ret_m):>5}件   ---  '
        print(f'  {month:>2}月 │ {new_str:^25} │ {ret_str:^25}')

    # ============================================================
    # 5. 新規 vs 再商談 の主要セグメント比較
    # ============================================================
    print_section('5. 主要セグメント: 新規 vs 失注後再商談 比較')
    # 施設形態別
    print(f'\n  ■ 施設形態別比較')
    print(f'  {"施設形態":<15} │ {"新規":^20} │ {"再商談":^20}')
    print(f'  {"─"*15}─┼─{"─"*20}─┼─{"─"*20}')
    for fac in ['介護（高齢者）', '医療', '障がい福祉', '保育', '不明']:
        new_f = new_data[new_data['facility'] == fac]
        ret_f = retry_data[retry_data['facility'] == fac]
        new_str = f'{len(new_f):>4}件 {new_f["is_won"].mean()*100:>5.1f}%' if len(new_f) >= 5 else f'{len(new_f):>4}件  --- '
        ret_str = f'{len(ret_f):>4}件 {ret_f["is_won"].mean()*100:>5.1f}%' if len(ret_f) >= 5 else f'{len(ret_f):>4}件  --- '
        print(f'  {fac:<15} │ {new_str:^20} │ {ret_str:^20}')

    # サービス種別別
    print(f'\n  ■ 主要サービス種別比較（両方10件以上）')
    all_st = set(new_data['service_type'].unique()) | set(retry_data['service_type'].unique())
    compare_st = []
    for st in all_st:
        new_s = new_data[new_data['service_type'] == st]
        ret_s = retry_data[retry_data['service_type'] == st]
        if len(new_s) >= 10 and len(ret_s) >= 10:
            compare_st.append((st, len(new_s), new_s['is_won'].mean()*100,
                             len(ret_s), ret_s['is_won'].mean()*100))
    compare_st.sort(key=lambda x: -x[2])
    print(f'  {"サービス種別":<28} │ {"新規":^18} │ {"再商談":^18} │ {"差":>5}')
    print(f'  {"─"*28}─┼─{"─"*18}─┼─{"─"*18}─┼─{"─"*5}')
    for st, nn, nr, rn, rr in compare_st:
        diff = nr - rr
        print(f'  {st:<28} │ {nn:>4}件 {nr:>5.1f}% │ {rn:>4}件 {rr:>5.1f}% │ {diff:>+5.1f}')

    # 従業員規模別
    print(f'\n  ■ 従業員規模別比較')
    print(f'  {"規模":<15} │ {"新規":^18} │ {"再商談":^18} │ {"差":>5}')
    print(f'  {"─"*15}─┼─{"─"*18}─┼─{"─"*18}─┼─{"─"*5}')
    for cat in EMP_ORDER:
        new_e = new_data[new_data['emp_cat'] == cat]
        ret_e = retry_data[retry_data['emp_cat'] == cat]
        if len(new_e) >= 10 and len(ret_e) >= 10:
            nr = new_e['is_won'].mean() * 100
            rr = ret_e['is_won'].mean() * 100
            diff = nr - rr
            print(f'  {cat:<15} │ {len(new_e):>4}件 {nr:>5.1f}% │ {len(ret_e):>4}件 {rr:>5.1f}% │ {diff:>+5.1f}')

    print()
    print('=' * 80)
    print('分析完了')
    print('=' * 80)


if __name__ == '__main__':
    main()
