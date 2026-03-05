# -*- coding: utf-8 -*-
"""
3カテゴリ分析: 新規商談 / 失注後再商談 / 成約後解約再商談
CustomerSegment_Small__cを軸に分類し、それぞれの中で詳細クロス分析
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

    w = int(data['is_won'].sum())
    r = data['is_won'].mean() * 100

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
    print('3カテゴリ分析: 新規 / 失注後再商談 / 解約後再商談')
    print('=' * 80)

    service = OpportunityService()
    service.authenticate()

    # 追加フィールド: RecordType, Type 等で新規商談を特定する手がかりを探す
    soql = """SELECT Id, CloseDate, IsWon, IsClosed, OpportunityCategory__c,
        FacilityType_Large__c, LeadSource, Type, RecordTypeId,
        Account.Name, Account.WonOpportunityies__c, Account.LegalPersonality__c,
        Account.IndustryCategory__c, Account.ServiceType__c,
        Account.Prefectures__c, Account.NumberOfEmployees,
        Account.CustomerSegment_Large__c, Account.CustomerSegment_Small__c,
        Account.NumberOfFacilities__c, Account.Industry
        FROM Opportunity WHERE IsClosed = true
        AND CloseDate >= 2025-04-01 AND CloseDate < 2026-02-01"""
    df = service.bulk_query(soql, '3カテゴリ')
    print(f'取得: {len(df):,}件')

    # 前処理
    df['is_won'] = df['IsWon'].map({'true': 1, 'false': 0, True: 1, False: 0}).astype(int)
    df['CloseDate'] = pd.to_datetime(df['CloseDate'], errors='coerce')
    df['month'] = df['CloseDate'].dt.month
    df['facility'] = df.apply(complement_facility, axis=1).fillna('不明')
    df['service_type'] = df['Account.ServiceType__c'].fillna('(未設定)')
    df['cs_small'] = df['Account.CustomerSegment_Small__c'].fillna('(未設定)')
    df['cs_large'] = df['Account.CustomerSegment_Large__c'].fillna('(未設定)')

    # ============================================================
    # フィールド調査: OpportunityCategory, Type, RecordType の分布
    # ============================================================
    print_section('0. フィールド調査: 新規商談を特定するヒント')

    print('\n  ■ OpportunityCategory__c 分布（全6,475件）')
    for v, cnt in df['OpportunityCategory__c'].fillna('(未設定)').value_counts().items():
        sub = df[df['OpportunityCategory__c'].fillna('(未設定)') == v]
        r = sub['is_won'].mean() * 100
        print(f'    {v:<25} {cnt:>5}件 受注率{r:>5.1f}%')

    print('\n  ■ Type 分布')
    for v, cnt in df['Type'].fillna('(未設定)').value_counts().items():
        sub = df[df['Type'].fillna('(未設定)') == v]
        r = sub['is_won'].mean() * 100
        print(f'    {v:<30} {cnt:>5}件 受注率{r:>5.1f}%')

    print('\n  ■ RecordTypeId 分布')
    for v, cnt in df['RecordTypeId'].fillna('(未設定)').value_counts().items():
        sub = df[df['RecordTypeId'].fillna('(未設定)') == v]
        r = sub['is_won'].mean() * 100
        print(f'    {v:<25} {cnt:>5}件 受注率{r:>5.1f}%')

    # ============================================================
    # WonOpportunityies__c の分布確認
    # ============================================================
    print('\n  ■ WonOpportunityies__c 分布（初回商談のみ）')
    first_all = df[df['OpportunityCategory__c'] == '初回商談'].copy()
    won_opp = first_all['Account.WonOpportunityies__c'].fillna(0).astype(float)
    first_all['won_opp_raw'] = won_opp
    first_all['past_won'] = won_opp - first_all['is_won']

    for seg, sub in first_all.groupby('cs_small'):
        avg_won = sub['won_opp_raw'].mean()
        avg_past = sub['past_won'].mean()
        print(f'    {seg:<30} {len(sub):>5}件 WonOpp平均{avg_won:.1f} past_won平均{avg_past:.1f}')

    # 新規フィルタ適用
    first_all['emp_cat'] = first_all['Account.NumberOfEmployees'].apply(emp_cat)
    first = first_all[first_all['past_won'] == 0].copy()
    print(f'\n  past_won=0フィルタ後: {len(first):,}件')

    # ============================================================
    # 3カテゴリ分類
    # ============================================================
    print_section('1. 3カテゴリ分類')

    # 成約後解約の再商談
    churn_seg = ['00-B 離脱客（過去客）', '00-C 離脱客（商談あり）']
    # 失注後の再商談
    lost_seg = ['02-D 過去失注先']
    # 既存顧客（フィルタ漏れ ＝ 分析から除外）
    existing_seg = ['01-A ロイヤル顧客', '01-B 一般顧客', '02-C 過去利用客']
    # 商談中（特殊 ＝ 分析から除外）
    active_seg = ['02-A 商談中（価値合意済）', '02-B 商談中']
    # 新規商談 = 上記いずれにも該当しない
    non_new = churn_seg + lost_seg + existing_seg + active_seg

    cat_churn = first[first['cs_small'].isin(churn_seg)]
    cat_lost = first[first['cs_small'].isin(lost_seg)]
    cat_existing = first[first['cs_small'].isin(existing_seg)]
    cat_active = first[first['cs_small'].isin(active_seg)]
    cat_new = first[~first['cs_small'].isin(non_new)]

    # 分類表示
    print(f'\n  {"カテゴリ":<25} {"件数":>6} {"受注":>4} {"受注率":>7}')
    print('  ' + '-' * 50)
    for label, sub in [('1.新規商談', cat_new), ('2.失注後再商談', cat_lost),
                       ('3.解約後再商談', cat_churn),
                       ('--- 除外:既存顧客', cat_existing), ('--- 除外:商談中', cat_active)]:
        if len(sub) > 0:
            r = sub['is_won'].mean() * 100
            print(f'  {label:<25} {len(sub):>6}件 {int(sub["is_won"].sum()):>4} {r:>6.1f}%')

    # 新規商談の内訳詳細
    print(f'\n  ■ 新規商談 の CustomerSegment 内訳:')
    for seg, sub in cat_new.groupby('cs_small'):
        r = sub['is_won'].mean() * 100
        print(f'    {seg:<35} {len(sub):>5}件 受注{int(sub["is_won"].sum()):>3} 率{r:>5.1f}%')

    # ============================================================
    # 分析対象 = 既存顧客・商談中を除外した3カテゴリ
    # ============================================================
    analysis = first[~first['cs_small'].isin(existing_seg + active_seg)].copy()
    print(f'\n  分析対象（3カテゴリ合計）: {len(analysis):,}件 受注{int(analysis["is_won"].sum())} 率{analysis["is_won"].mean()*100:.1f}%')

    # 3カテゴリごとに商談タイプラベルを付与
    def assign_category(cs):
        if cs in churn_seg:
            return '解約後再商談'
        elif cs in lost_seg:
            return '失注後再商談'
        else:
            return '新規商談'
    analysis['biz_cat'] = analysis['cs_small'].apply(assign_category)

    # ============================================================
    # 2. 失注後再商談の詳細分析（最大ボリューム）
    # ============================================================
    print_section('2. 失注後再商談の詳細分析')
    lost_data = analysis[analysis['biz_cat'] == '失注後再商談']
    n = len(lost_data)
    w = int(lost_data['is_won'].sum())
    r = lost_data['is_won'].mean() * 100
    print(f'  全体: {n:,}件 受注{w} 率{r:.1f}%')

    # 受注した10件の詳細
    won_lost = lost_data[lost_data['is_won'] == 1]
    if len(won_lost) > 0:
        print(f'\n  ■ 受注に至った{len(won_lost)}件の詳細:')
        for _, row in won_lost.iterrows():
            print(f'    {row["facility"]} / {row["service_type"]} / {row["emp_cat"]} / {row.get("Account.Prefectures__c", "?")} / {row["CloseDate"].strftime("%Y-%m") if pd.notna(row["CloseDate"]) else "?"}')

    analyze_detail(lost_data, '失注後再商談', min_n=10)

    # ============================================================
    # 3. 解約後再商談の詳細分析
    # ============================================================
    print_section('3. 解約後再商談の詳細分析')
    churn_data = analysis[analysis['biz_cat'] == '解約後再商談']
    n = len(churn_data)
    w = int(churn_data['is_won'].sum())
    r = churn_data['is_won'].mean() * 100
    print(f'  全体: {n:,}件 受注{w} 率{r:.1f}%')

    # 受注した件の詳細
    won_churn = churn_data[churn_data['is_won'] == 1]
    if len(won_churn) > 0:
        print(f'\n  ■ 受注に至った{len(won_churn)}件の詳細:')
        for _, row in won_churn.iterrows():
            print(f'    {row["facility"]} / {row["service_type"]} / {row["emp_cat"]} / {row.get("Account.Prefectures__c", "?")} / {row["CloseDate"].strftime("%Y-%m") if pd.notna(row["CloseDate"]) else "?"}')

    analyze_detail(churn_data, '解約後再商談', min_n=10)

    # ============================================================
    # 4. 新規商談の詳細分析
    # ============================================================
    print_section('4. 新規商談の詳細分析')
    new_data = analysis[analysis['biz_cat'] == '新規商談']
    n = len(new_data)
    w = int(new_data['is_won'].sum())
    r = new_data['is_won'].mean() * 100
    print(f'  全体: {n:,}件 受注{w} 率{r:.1f}%')

    if n >= 5:
        analyze_detail(new_data, '新規商談', min_n=5)

    # ============================================================
    # 5. 月別: 3カテゴリ比較
    # ============================================================
    print_section('5. 月別: 3カテゴリ比較')
    print(f'\n  {"月":>3} │ {"失注後再商談":^22} │ {"解約後再商談":^22} │ {"新規商談":^22}')
    print(f'  {"─"*3}─┼─{"─"*22}─┼─{"─"*22}─┼─{"─"*22}')
    for m in range(4, 14):
        month = m if m <= 12 else m - 12
        parts = []
        for cat_name, cat_data in [('失注', lost_data), ('解約', churn_data), ('新規', new_data)]:
            m_data = cat_data[cat_data['month'] == month]
            if len(m_data) >= 3:
                mr = m_data['is_won'].mean() * 100
                parts.append(f'{len(m_data):>4}件 率{mr:>5.1f}%')
            else:
                parts.append(f'{len(m_data):>4}件   ---  ')
        print(f'  {month:>2}月 │ {parts[0]:^22} │ {parts[1]:^22} │ {parts[2]:^22}')

    # ============================================================
    # 6. 全体: 受注28件の内訳
    # ============================================================
    print_section('6. 受注28件の完全内訳')
    won_all = analysis[analysis['is_won'] == 1]
    print(f'\n  受注{len(won_all)}件の内訳:')
    for cat_name in ['新規商談', '失注後再商談', '解約後再商談']:
        sub = won_all[won_all['biz_cat'] == cat_name]
        print(f'\n  ■ {cat_name}: {len(sub)}件')
        for _, row in sub.iterrows():
            pref = row.get('Account.Prefectures__c', '?')
            dt = row['CloseDate'].strftime('%Y-%m') if pd.notna(row['CloseDate']) else '?'
            print(f'    {row["facility"]} / {row["service_type"]} / {row["emp_cat"]} / {pref} / {dt}')

    print()
    print('=' * 80)
    print('分析完了')
    print('=' * 80)


if __name__ == '__main__':
    main()
