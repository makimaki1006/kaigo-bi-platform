"""
保育・介護訪問看護の詳細セグメント分析

最高受注率セグメント（保育27.9%、介護法人×訪問看護45.5%）を
多次元で深掘りし、どんな特徴を持つ先が受注しやすいかを明らかにする。
"""
import sys, io
from pathlib import Path

# UTF-8出力強制
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / 'src'))

import pandas as pd
from api.salesforce_client import SalesforceClient
from services.opportunity_service import OpportunityService

# ── 定数 ──
INDUSTRY_MAP = {
    '介護': '介護（高齢者）', '医療': '医療',
    '障害福祉': '障がい福祉', '保育': '保育', 'その他': 'その他'
}
SERVICE_TYPE_MAP = {
    '訪問看護': '医療', '訪問介護': '介護（高齢者）',
    '通所介護（デイサービス）': '介護（高齢者）',
    '居宅介護支援': '介護（高齢者）', '保育園': '保育',
    '特別養護老人ホーム': '介護（高齢者）',
    '有料老人ホーム': '介護（高齢者）',
    'グループホーム': '介護（高齢者）',
    'サービス付き高齢者向け住宅': '介護（高齢者）',
    '病院': '医療', 'クリニック・診療所': '医療',
    '放課後等デイサービス': '障がい福祉',
    '就労継続支援': '障がい福祉',
}

EMP_ORDER = ['不明/0', '1-10人', '11-30人', '31-50人', '51-100人', '101-300人', '301人+']

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

def complement_facility(row):
    ft = row.get('FacilityType_Large__c')
    if pd.notna(ft) and ft:
        return ft
    ic = row.get('Account.IndustryCategory__c')
    if pd.notna(ic) and ic:
        fc = str(ic).split(';')[0].strip()
        if fc in INDUSTRY_MAP:
            return INDUSTRY_MAP[fc]
    st = row.get('Account.ServiceType__c')
    if pd.notna(st) and st in SERVICE_TYPE_MAP:
        return SERVICE_TYPE_MAP[st]
    return '不明'

def marker(rate):
    if rate >= 15: return '**'
    if rate >= 10: return '+ '
    if rate >= 5:  return '  '
    return '--'

def print_group(df, group_col, label, min_n=3):
    """グループ別の受注率を表示"""
    print(f'\n  【{label}】')
    results = []
    for val, sub in df.groupby(group_col):
        if len(sub) >= min_n:
            r = sub['is_won'].mean() * 100
            results.append((val, len(sub), int(sub['is_won'].sum()), r))
    results.sort(key=lambda x: -x[3])
    for val, n, won, r in results:
        print(f'    {marker(r)} {str(val):<30} {n:>4}件 受注{won:>3} 率{r:>5.1f}%')
    if not results:
        print('    （3件以上のグループなし）')

def print_group_yearly(df, group_col, label, min_n=3):
    """グループ別×年度の受注率を表示（複数年一貫性チェック）"""
    print(f'\n  【{label}×年度別】')
    for val, sub in df.groupby(group_col):
        if len(sub) >= min_n:
            r_all = sub['is_won'].mean() * 100
            years = []
            for fy, ysub in sub.groupby('fy'):
                yr = ysub['is_won'].mean() * 100
                years.append(f'{fy}:{yr:.0f}%({len(ysub)})')
            year_str = ' | '.join(years)
            print(f'    {marker(r_all)} {str(val):<25} 全体{r_all:>5.1f}%({len(sub)}件) → {year_str}')

# ── メイン ──
def main():
    # 全期間データ取得（FY2022〜FY2025）
    svc = OpportunityService()
    svc.authenticate()

    query = """
    SELECT Id, AccountId, CloseDate, IsWon, IsClosed, OpportunityCategory__c,
        FacilityType_Large__c, LeadSource, Amount,
        Account.Name, Account.WonOpportunityies__c, Account.LegalPersonality__c,
        Account.IndustryCategory__c, Account.ServiceType__c,
        Account.Prefectures__c, Account.NumberOfEmployees,
        Account.CustomerSegment_Large__c, Account.CustomerSegment_Small__c,
        Account.NumberOfFacilities__c, Account.Industry,
        Account.OwnerName__c
    FROM Opportunity WHERE IsClosed = true
    AND CloseDate >= 2022-04-01 AND CloseDate < 2026-02-01
    """
    print('データ取得中...')
    df = svc.bulk_query(query)
    print(f'取得: {len(df):,}件')

    # カラム名正規化
    col_map = {}
    for c in df.columns:
        if 'Prefectures' in c:
            col_map[c] = 'prefecture'
        elif 'NumberOfEmployees' in c:
            col_map[c] = 'emp'
        elif 'LegalPersonality' in c:
            col_map[c] = 'legal'
        elif 'IndustryCategory' in c:
            col_map[c] = 'ind_cat'
        elif 'ServiceType' in c:
            col_map[c] = 'service_type'
        elif 'NumberOfFacilities' in c:
            col_map[c] = 'num_facilities'
        elif 'CustomerSegment_Large' in c:
            col_map[c] = 'seg_large'
        elif 'CustomerSegment_Small' in c:
            col_map[c] = 'seg_small'
        elif 'Account.Name' in c:
            col_map[c] = 'account_name'
        elif 'Account.Industry' == c:
            col_map[c] = 'industry'

    # 基本変換
    df['is_won'] = df['IsWon'].astype(str).str.lower() == 'true'
    df['close_date'] = pd.to_datetime(df['CloseDate'])
    df['fy'] = df['close_date'].apply(lambda d: f'FY{d.year}' if d.month >= 4 else f'FY{d.year-1}')
    df['month'] = df['close_date'].dt.month
    df['facility'] = df.apply(complement_facility, axis=1)

    # カラム名適用
    for old, new in col_map.items():
        if old in df.columns:
            df[new] = df[old]

    df['emp_cat'] = df['emp'].apply(emp_cat) if 'emp' in df.columns else '不明/0'
    df['legal'] = df['legal'].fillna('不明') if 'legal' in df.columns else '不明'
    df['service_type'] = df['service_type'].fillna('不明') if 'service_type' in df.columns else '不明'
    df['prefecture'] = df['prefecture'].fillna('不明') if 'prefecture' in df.columns else '不明'

    # Account履歴ベースの新規/再商談判定
    print('\n商談履歴を構築中...')
    hist = df.groupby('AccountId').agg(
        first_close=('close_date', 'min'),
        total_opps=('Id', 'count'),
        total_won=('is_won', 'sum')
    ).reset_index()

    # FY2025期間の商談
    fy25_mask = (df['close_date'] >= '2025-04-01') & (df['close_date'] < '2026-02-01')
    df_fy25 = df[fy25_mask].copy()

    # 初回商談フィルタ
    first_mask = df_fy25['OpportunityCategory__c'] == '初回商談'
    first = df_fy25[first_mask].copy()
    print(f'FY2025 初回商談: {len(first):,}件 (全{len(df_fy25):,}件中)')

    # 全期間の初回商談（複数年分析用）
    first_all_mask = df['OpportunityCategory__c'] == '初回商談'
    first_all = df[first_all_mask].copy()
    print(f'全期間 初回商談: {len(first_all):,}件')

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # セグメント1: 保育
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    print('\n' + '='*70)
    print('  セグメント分析①: 保育')
    print('='*70)

    hoiku = first[first['facility'] == '保育'].copy()
    hoiku_all = first_all[first_all['facility'] == '保育'].copy()
    n_h = len(hoiku)
    won_h = int(hoiku['is_won'].sum())
    r_h = hoiku['is_won'].mean() * 100 if n_h > 0 else 0
    print(f'\nFY2025 保育: {n_h}件 受注{won_h} 率{r_h:.1f}%')

    n_hall = len(hoiku_all)
    won_hall = int(hoiku_all['is_won'].sum())
    r_hall = hoiku_all['is_won'].mean() * 100 if n_hall > 0 else 0
    print(f'全期間 保育: {n_hall}件 受注{won_hall} 率{r_hall:.1f}%')

    # 年度別推移
    print('\n  【年度別推移】')
    for fy, sub in hoiku_all.groupby('fy'):
        r = sub['is_won'].mean() * 100
        print(f'    {fy}: {len(sub):>4}件 受注{int(sub["is_won"].sum()):>3} 率{r:>5.1f}%')

    # サービス種別
    print_group(hoiku, 'service_type', 'サービス種別（FY2025）')
    print_group(hoiku_all, 'service_type', 'サービス種別（全期間）')

    # 従業員規模別
    print_group(hoiku, 'emp_cat', '従業員規模（FY2025）')
    print_group(hoiku_all, 'emp_cat', '従業員規模（全期間）')

    # 法人格別
    print_group(hoiku, 'legal', '法人格（FY2025）')
    print_group(hoiku_all, 'legal', '法人格（全期間）')

    # 都道府県別
    print_group(hoiku_all, 'prefecture', '都道府県（全期間、5件以上）', min_n=5)

    # LeadSource別
    print_group(hoiku, 'LeadSource', 'リードソース（FY2025）')
    print_group(hoiku_all, 'LeadSource', 'リードソース（全期間）')

    # 月別
    print('\n  【月別受注率（全期間）】')
    for m in range(1, 13):
        sub = hoiku_all[hoiku_all['month'] == m]
        if len(sub) >= 3:
            r = sub['is_won'].mean() * 100
            print(f'    {m:>2}月: {len(sub):>3}件 率{r:>5.1f}%')

    # 施設数別（もしあれば）
    if 'num_facilities' in hoiku.columns:
        hoiku_all['fac_cat'] = hoiku_all['num_facilities'].apply(
            lambda x: '不明' if pd.isna(x) else ('1施設' if float(x) <= 1 else ('2-5施設' if float(x) <= 5 else '6施設+'))
        )
        print_group(hoiku_all, 'fac_cat', '施設数（全期間）')

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # セグメント2: 介護法人×訪問看護
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    print('\n' + '='*70)
    print('  セグメント分析②: 訪問看護（施設形態別）')
    print('='*70)

    houmon = first[first['service_type'] == '訪問看護'].copy()
    houmon_all = first_all[first_all['service_type'] == '訪問看護'].copy()

    n_hm = len(houmon)
    won_hm = int(houmon['is_won'].sum())
    r_hm = houmon['is_won'].mean() * 100 if n_hm > 0 else 0
    print(f'\nFY2025 訪問看護全体: {n_hm}件 受注{won_hm} 率{r_hm:.1f}%')

    n_hmall = len(houmon_all)
    won_hmall = int(houmon_all['is_won'].sum())
    r_hmall = houmon_all['is_won'].mean() * 100 if n_hmall > 0 else 0
    print(f'全期間 訪問看護全体: {n_hmall}件 受注{won_hmall} 率{r_hmall:.1f}%')

    # 施設形態（大分類）×訪問看護
    print('\n  【施設形態別×訪問看護】')
    for fac in ['介護（高齢者）', '医療', '障がい福祉', '保育', '不明']:
        for period_name, data in [('FY2025', houmon), ('全期間', houmon_all)]:
            sub = data[data['facility'] == fac]
            if len(sub) >= 1:
                r = sub['is_won'].mean() * 100
                print(f'    {marker(r)} {fac:<15} ({period_name}): {len(sub):>4}件 受注{int(sub["is_won"].sum()):>3} 率{r:>5.1f}%')

    # 介護法人×訪問看護の深掘り
    kaigo_houmon = houmon[houmon['facility'] == '介護（高齢者）'].copy()
    kaigo_houmon_all = houmon_all[houmon_all['facility'] == '介護（高齢者）'].copy()

    print(f'\n--- 介護法人×訪問看護の深掘り ---')
    print(f'FY2025: {len(kaigo_houmon)}件 受注{int(kaigo_houmon["is_won"].sum())} 率{kaigo_houmon["is_won"].mean()*100:.1f}%')
    print(f'全期間: {len(kaigo_houmon_all)}件 受注{int(kaigo_houmon_all["is_won"].sum())} 率{kaigo_houmon_all["is_won"].mean()*100:.1f}%')

    # 年度別推移
    print('\n  【年度別推移】')
    for fy, sub in kaigo_houmon_all.groupby('fy'):
        r = sub['is_won'].mean() * 100
        print(f'    {fy}: {len(sub):>4}件 受注{int(sub["is_won"].sum()):>3} 率{r:>5.1f}%')

    # 医療法人×訪問看護の年度別（比較用）
    iryo_houmon_all = houmon_all[houmon_all['facility'] == '医療'].copy()
    print('\n  【比較: 医療法人×訪問看護 年度別】')
    for fy, sub in iryo_houmon_all.groupby('fy'):
        r = sub['is_won'].mean() * 100
        print(f'    {fy}: {len(sub):>4}件 受注{int(sub["is_won"].sum()):>3} 率{r:>5.1f}%')

    # 従業員規模別
    print_group(kaigo_houmon, 'emp_cat', '介護×訪問看護 従業員規模（FY2025）')
    print_group(kaigo_houmon_all, 'emp_cat', '介護×訪問看護 従業員規模（全期間）')

    # 法人格別
    print_group(kaigo_houmon, 'legal', '介護×訪問看護 法人格（FY2025）')
    print_group(kaigo_houmon_all, 'legal', '介護×訪問看護 法人格（全期間）')

    # 都道府県別
    print_group(kaigo_houmon_all, 'prefecture', '介護×訪問看護 都道府県（全期間、3件以上）')

    # LeadSource別
    print_group(kaigo_houmon, 'LeadSource', '介護×訪問看護 リードソース（FY2025）')
    print_group(kaigo_houmon_all, 'LeadSource', '介護×訪問看護 リードソース（全期間）')

    # 月別
    print('\n  【介護×訪問看護 月別受注率（全期間）】')
    for m in range(1, 13):
        sub = kaigo_houmon_all[kaigo_houmon_all['month'] == m]
        if len(sub) >= 2:
            r = sub['is_won'].mean() * 100
            print(f'    {m:>2}月: {len(sub):>3}件 率{r:>5.1f}%')

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # おまけ: 訪問看護以外の介護高受注サービス
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    print('\n' + '='*70)
    print('  参考: 介護（高齢者）のサービス種別別受注率')
    print('='*70)

    kaigo = first[first['facility'] == '介護（高齢者）'].copy()
    kaigo_all = first_all[first_all['facility'] == '介護（高齢者）'].copy()

    print(f'\nFY2025 介護全体: {len(kaigo)}件 受注{int(kaigo["is_won"].sum())} 率{kaigo["is_won"].mean()*100:.1f}%')
    print(f'全期間 介護全体: {len(kaigo_all)}件 受注{int(kaigo_all["is_won"].sum())} 率{kaigo_all["is_won"].mean()*100:.1f}%')

    print_group(kaigo, 'service_type', '介護サービス種別（FY2025）')
    print_group(kaigo_all, 'service_type', '介護サービス種別（全期間）')

    # 年度別一貫性（主要サービス）
    print_group_yearly(kaigo_all, 'service_type', '介護サービス種別')

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # IndustryCategory（業界カテゴリ）での深掘り
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    print('\n' + '='*70)
    print('  参考: IndustryCategory（業種）による分析')
    print('='*70)

    # 訪問看護を持つAccountのIndustryCategoryを確認
    print('\n  【訪問看護のIndustryCategory分布】')
    if 'ind_cat' in houmon_all.columns:
        houmon_all['ind_first'] = houmon_all['ind_cat'].apply(
            lambda x: str(x).split(';')[0].strip() if pd.notna(x) else '不明'
        )
        print_group(houmon_all, 'ind_first', '訪問看護×業種カテゴリ（全期間）')

    # 保育のIndustryCategory
    print('\n  【保育のIndustryCategory分布】')
    if 'ind_cat' in hoiku_all.columns:
        hoiku_all['ind_first'] = hoiku_all['ind_cat'].apply(
            lambda x: str(x).split(';')[0].strip() if pd.notna(x) else '不明'
        )
        print_group(hoiku_all, 'ind_first', '保育×業種カテゴリ（全期間）')

    print('\n分析完了')

if __name__ == '__main__':
    main()
