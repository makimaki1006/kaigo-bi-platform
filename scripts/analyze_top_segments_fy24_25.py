"""
保育・介護訪問看護の詳細セグメント分析（FY2024-FY2025のみ）
"""
import sys, io
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / 'src'))

import pandas as pd
from api.salesforce_client import SalesforceClient
from services.opportunity_service import OpportunityService

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

def mk(rate):
    if rate >= 15: return '**'
    if rate >= 10: return '+ '
    if rate >= 5:  return '  '
    return '--'

def show(df, col, label, min_n=3):
    print(f'\n  【{label}】')
    results = []
    for val, sub in df.groupby(col):
        if len(sub) >= min_n:
            r = sub['is_won'].mean() * 100
            results.append((val, len(sub), int(sub['is_won'].sum()), r))
    results.sort(key=lambda x: -x[3])
    for val, n, won, r in results:
        print(f'    {mk(r)} {str(val):<30} {n:>4}件 受注{won:>3} 率{r:>5.1f}%')
    if not results:
        print('    （該当なし）')

def show_fy(df, col, label, min_n=3):
    print(f'\n  【{label} × 年度】')
    for val, sub in df.groupby(col):
        if len(sub) >= min_n:
            r_all = sub['is_won'].mean() * 100
            parts = []
            for fy, ysub in sub.groupby('fy'):
                yr = ysub['is_won'].mean() * 100
                parts.append(f'{fy}:{yr:.0f}%({len(ysub)})')
            print(f'    {mk(r_all)} {str(val):<25} 合計{r_all:>5.1f}%({len(sub)}件) → {" | ".join(parts)}')

def main():
    svc = OpportunityService()
    svc.authenticate()

    # FY2024-FY2025のみ（2024-04-01 ～ 2026-02-01）
    query = """
    SELECT Id, AccountId, CloseDate, IsWon, IsClosed, OpportunityCategory__c,
        FacilityType_Large__c, LeadSource,
        Account.Name, Account.LegalPersonality__c,
        Account.IndustryCategory__c, Account.ServiceType__c,
        Account.Prefectures__c, Account.NumberOfEmployees
    FROM Opportunity WHERE IsClosed = true
    AND CloseDate >= 2024-04-01 AND CloseDate < 2026-02-01
    """
    print('FY2024-FY2025 データ取得中...')
    df = svc.bulk_query(query)
    print(f'取得: {len(df):,}件')

    df['is_won'] = df['IsWon'].astype(str).str.lower() == 'true'
    df['close_date'] = pd.to_datetime(df['CloseDate'])
    df['fy'] = df['close_date'].apply(lambda d: f'FY{d.year}' if d.month >= 4 else f'FY{d.year-1}')
    df['month'] = df['close_date'].dt.month
    df['facility'] = df.apply(complement_facility, axis=1)

    # カラム正規化
    for c in df.columns:
        if 'NumberOfEmployees' in c:
            df['emp'] = df[c]
        if 'LegalPersonality' in c:
            df['legal'] = df[c]
        if 'ServiceType' in c:
            df['service_type'] = df[c]
        if 'Prefectures' in c:
            df['prefecture'] = df[c]
        if 'IndustryCategory' in c:
            df['ind_cat'] = df[c]

    df['emp_cat'] = df['emp'].apply(emp_cat) if 'emp' in df.columns else '不明/0'
    df['legal'] = df['legal'].fillna('不明') if 'legal' in df.columns else '不明'
    df['service_type'] = df['service_type'].fillna('不明') if 'service_type' in df.columns else '不明'
    df['prefecture'] = df['prefecture'].fillna('不明') if 'prefecture' in df.columns else '不明'

    # 初回商談のみ
    first = df[df['OpportunityCategory__c'] == '初回商談'].copy()
    print(f'初回商談: {len(first):,}件')

    for fy in ['FY2024', 'FY2025']:
        sub = first[first['fy'] == fy]
        r = sub['is_won'].mean() * 100
        print(f'  {fy}: {len(sub):,}件 受注率{r:.1f}%')

    # ============================================================
    print('\n' + '='*70)
    print('  1. 保育 (FY2024+FY2025)')
    print('='*70)

    hoiku = first[first['facility'] == '保育'].copy()
    print(f'\n合計: {len(hoiku)}件 受注{int(hoiku["is_won"].sum())} 率{hoiku["is_won"].mean()*100:.1f}%')
    for fy, sub in hoiku.groupby('fy'):
        print(f'  {fy}: {len(sub)}件 受注{int(sub["is_won"].sum())} 率{sub["is_won"].mean()*100:.1f}%')

    show(hoiku, 'service_type', 'サービス種別')
    show(hoiku, 'emp_cat', '従業員規模')
    show(hoiku, 'legal', '法人格')
    show_fy(hoiku, 'legal', '法人格')
    show(hoiku, 'prefecture', '都道府県（5件以上）', min_n=5)

    # ============================================================
    print('\n' + '='*70)
    print('  2. 訪問看護 全体 (FY2024+FY2025)')
    print('='*70)

    houmon = first[first['service_type'] == '訪問看護'].copy()
    print(f'\n合計: {len(houmon)}件 受注{int(houmon["is_won"].sum())} 率{houmon["is_won"].mean()*100:.1f}%')

    print('\n  【施設形態別】')
    for fac in ['介護（高齢者）', '医療', '障がい福祉', '不明']:
        sub = houmon[houmon['facility'] == fac]
        if len(sub) >= 1:
            r = sub['is_won'].mean() * 100
            # 年度別も
            parts = []
            for fy, ysub in sub.groupby('fy'):
                yr = ysub['is_won'].mean() * 100
                parts.append(f'{fy}:{yr:.0f}%({len(ysub)})')
            print(f'    {mk(r)} {fac:<15} {len(sub):>4}件 受注{int(sub["is_won"].sum()):>3} 率{r:>5.1f}% → {" | ".join(parts)}')

    # ============================================================
    print('\n' + '='*70)
    print('  3. 介護法人×訪問看護 深掘り (FY2024+FY2025)')
    print('='*70)

    kh = houmon[houmon['facility'] == '介護（高齢者）'].copy()
    print(f'\n合計: {len(kh)}件 受注{int(kh["is_won"].sum())} 率{kh["is_won"].mean()*100:.1f}%')
    for fy, sub in kh.groupby('fy'):
        print(f'  {fy}: {len(sub)}件 受注{int(sub["is_won"].sum())} 率{sub["is_won"].mean()*100:.1f}%')

    show(kh, 'legal', '法人格')
    show_fy(kh, 'legal', '法人格')
    show(kh, 'emp_cat', '従業員規模')
    show(kh, 'prefecture', '都道府県（3件以上）')

    # ============================================================
    print('\n' + '='*70)
    print('  4. 介護サービス種別ランキング (FY2024+FY2025)')
    print('='*70)

    kaigo = first[first['facility'] == '介護（高齢者）'].copy()
    print(f'\n介護全体: {len(kaigo)}件 受注{int(kaigo["is_won"].sum())} 率{kaigo["is_won"].mean()*100:.1f}%')

    show(kaigo, 'service_type', 'サービス種別')
    show_fy(kaigo, 'service_type', 'サービス種別（10件以上）', min_n=10)

    # ============================================================
    print('\n' + '='*70)
    print('  5. 介護×法人格 (FY2024+FY2025)')
    print('='*70)
    show(kaigo, 'legal', '法人格')
    show_fy(kaigo, 'legal', '法人格')

    # ============================================================
    print('\n' + '='*70)
    print('  6. 施設形態×法人格クロス (FY2024+FY2025)')
    print('='*70)

    first['fac_legal'] = first['facility'] + ' × ' + first['legal']
    print('\n  【上位セグメント（10件以上、受注率10%超）】')
    results = []
    for val, sub in first.groupby('fac_legal'):
        if len(sub) >= 10:
            r = sub['is_won'].mean() * 100
            if r >= 10:
                parts = []
                for fy, ysub in sub.groupby('fy'):
                    yr = ysub['is_won'].mean() * 100
                    parts.append(f'{fy}:{yr:.0f}%({len(ysub)})')
                results.append((val, len(sub), int(sub['is_won'].sum()), r, ' | '.join(parts)))
    results.sort(key=lambda x: -x[3])
    for val, n, won, r, fy_str in results:
        print(f'    {mk(r)} {val:<40} {n:>4}件 受注{won:>3} 率{r:>5.1f}% → {fy_str}')

    print('\n  【下位セグメント（10件以上、受注率5%未満）】')
    results_low = []
    for val, sub in first.groupby('fac_legal'):
        if len(sub) >= 10:
            r = sub['is_won'].mean() * 100
            if r < 5:
                results_low.append((val, len(sub), int(sub['is_won'].sum()), r))
    results_low.sort(key=lambda x: x[3])
    for val, n, won, r in results_low:
        print(f'    {mk(r)} {val:<40} {n:>4}件 受注{won:>3} 率{r:>5.1f}%')

    print('\n分析完了')

if __name__ == '__main__':
    main()
