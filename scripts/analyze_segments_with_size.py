"""全セグメント再整理: 医療を規模別に切り分け（FY2024+FY2025）"""
import sys, io
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / 'src'))

import pandas as pd
from services.opportunity_service import OpportunityService

INDUSTRY_MAP = {
    '介護': '介護（高齢者）', '医療': '医療',
    '障害福祉': '障がい福祉', '保育': '保育', 'その他': 'その他'
}
SERVICE_TYPE_MAP = {
    '訪問看護': '医療', '訪問介護': '介護（高齢者）', '保育園': '保育',
    '病院': '医療', 'クリニック・診療所': '医療',
    '通所介護（デイサービス）': '介護（高齢者）', '居宅介護支援': '介護（高齢者）',
    '特別養護老人ホーム': '介護（高齢者）', '有料老人ホーム': '介護（高齢者）',
    'グループホーム': '介護（高齢者）', 'サービス付き高齢者向け住宅': '介護（高齢者）',
    '放課後等デイサービス': '障がい福祉', '就労継続支援': '障がい福祉',
}

def complement_facility(row):
    ft = row.get('FacilityType_Large__c')
    if pd.notna(ft) and ft:
        return ft
    for c in row.index:
        if 'IndustryCategory' in str(c):
            ic = row[c]
            if pd.notna(ic):
                fc = str(ic).split(';')[0].strip()
                if fc in INDUSTRY_MAP:
                    return INDUSTRY_MAP[fc]
    for c in row.index:
        if 'ServiceType' in str(c):
            st = row[c]
            if pd.notna(st) and st in SERVICE_TYPE_MAP:
                return SERVICE_TYPE_MAP[st]
    return '不明'

def emp_cat(n):
    if pd.isna(n) or n == 0: return '不明/0'
    if n <= 10: return '1-10人'
    if n <= 30: return '11-30人'
    if n <= 50: return '31-50人'
    if n <= 100: return '51-100人'
    if n <= 300: return '101-300人'
    return '301人+'

def iryo_size(n):
    if pd.isna(n) or n == 0: return '不明'
    if n <= 30: return '小規模(30人以下)'
    if n <= 100: return '中規模(31-100人)'
    return '大規模(101人+)'

def mk(r):
    if r >= 15: return '**'
    if r >= 10: return '+ '
    if r >= 5:  return '  '
    return '--'

def main():
    svc = OpportunityService()
    svc.authenticate()

    query = """
    SELECT Id, IsWon, IsClosed, CloseDate, OpportunityCategory__c,
        FacilityType_Large__c,
        Account.LegalPersonality__c, Account.ServiceType__c,
        Account.NumberOfEmployees, Account.IndustryCategory__c
    FROM Opportunity WHERE IsClosed = true
    AND CloseDate >= 2024-04-01 AND CloseDate < 2026-02-01
    """
    df = svc.bulk_query(query)
    df['is_won'] = df['IsWon'].astype(str).str.lower() == 'true'
    df['close_date'] = pd.to_datetime(df['CloseDate'])
    df['fy'] = df['close_date'].apply(lambda d: f'FY{d.year}' if d.month >= 4 else f'FY{d.year-1}')

    first = df[df['OpportunityCategory__c'] == '初回商談'].copy()
    first['facility'] = first.apply(complement_facility, axis=1)

    for c in first.columns:
        if 'LegalPersonality' in c:
            first['legal'] = first[c].fillna('不明')
        if 'ServiceType' in c:
            first['stype'] = first[c].fillna('不明')
        if 'NumberOfEmployees' in c:
            first['emp'] = pd.to_numeric(first[c], errors='coerce')

    first['emp_cat'] = first['emp'].apply(emp_cat)
    first['iryo_size'] = first['emp'].apply(iryo_size)

    total_r = first['is_won'].mean() * 100
    print(f'全体: {len(first):,}件 受注率{total_r:.1f}%\n')

    # ===================================
    print('=' * 70)
    print('  A. 医療セクター 規模別完全分解')
    print('=' * 70)

    iryo = first[first['facility'] == '医療'].copy()
    print(f'\n医療全体: {len(iryo)}件 受注率{iryo["is_won"].mean()*100:.1f}%')

    # 法人格×規模帯
    print('\n  [法人格 × 規模帯]')
    results = []
    for (lg, sz), sub in iryo.groupby(['legal', 'iryo_size']):
        if len(sub) >= 5:
            r = sub['is_won'].mean() * 100
            parts = []
            for fy, ysub in sub.groupby('fy'):
                yr = ysub['is_won'].mean() * 100
                parts.append(f'{fy}:{yr:.0f}%({len(ysub)})')
            results.append((lg, sz, len(sub), int(sub['is_won'].sum()), r, ' | '.join(parts)))
    results.sort(key=lambda x: -x[4])
    for lg, sz, n, won, r, fy_str in results:
        print(f'    {mk(r)} {lg} x {sz:<18} {n:>4}件 受注{won:>3} 率{r:>5.1f}% | {fy_str}')

    # 医療法人 詳細7段階
    print('\n  [医療法人のみ: 7段階規模]')
    iryo_med = iryo[iryo['legal'] == '医療法人']
    for ec in ['不明/0', '1-10人', '11-30人', '31-50人', '51-100人', '101-300人', '301人+']:
        sub = iryo_med[iryo_med['emp_cat'] == ec]
        if len(sub) >= 3:
            r = sub['is_won'].mean() * 100
            parts = []
            for fy, ysub in sub.groupby('fy'):
                yr = ysub['is_won'].mean() * 100
                parts.append(f'{fy}:{yr:.0f}%({len(ysub)})')
            print(f'    {mk(r)} {ec:<12} {len(sub):>4}件 受注{int(sub["is_won"].sum()):>3} 率{r:>5.1f}% | {" | ".join(parts)}')

    # 医療×不明法人格も
    print('\n  [法人格「不明」の医療: 7段階規模]')
    iryo_unk = iryo[iryo['legal'] == '不明']
    for ec in ['不明/0', '1-10人', '11-30人', '31-50人', '51-100人', '101-300人', '301人+']:
        sub = iryo_unk[iryo_unk['emp_cat'] == ec]
        if len(sub) >= 5:
            r = sub['is_won'].mean() * 100
            print(f'    {mk(r)} {ec:<12} {len(sub):>4}件 受注{int(sub["is_won"].sum()):>3} 率{r:>5.1f}%')

    # ===================================
    print('\n' + '=' * 70)
    print('  B. 全セグメント統合ランキング')
    print('=' * 70)

    # セグメント名: 医療は規模帯つき、他は法人格のみ
    def make_seg(row):
        fac = row['facility']
        lg = row['legal']
        if fac == '医療':
            sz = row['iryo_size']
            return f'{fac} x {lg} x {sz}'
        else:
            return f'{fac} x {lg}'
    first['segment'] = first.apply(make_seg, axis=1)

    print('\n  [Tier S: 受注率15%超, 10件以上]')
    results = []
    for seg, sub in first.groupby('segment'):
        if len(sub) >= 10:
            r = sub['is_won'].mean() * 100
            parts = []
            for fy, ysub in sub.groupby('fy'):
                yr = ysub['is_won'].mean() * 100
                parts.append(f'{fy}:{yr:.0f}%({len(ysub)})')
            results.append((seg, len(sub), int(sub['is_won'].sum()), r, ' | '.join(parts)))
    results.sort(key=lambda x: -x[3])

    for seg, n, won, r, fy_str in results:
        if r >= 15:
            print(f'    {seg:<55} {n:>4}件 受注{won:>3} 率{r:>5.1f}% | {fy_str}')

    print('\n  [Tier A: 受注率10-15%, 10件以上]')
    for seg, n, won, r, fy_str in results:
        if 10 <= r < 15:
            print(f'    {seg:<55} {n:>4}件 受注{won:>3} 率{r:>5.1f}% | {fy_str}')

    print('\n  [Tier B: 受注率5-10%, 10件以上]')
    for seg, n, won, r, fy_str in results:
        if 5 <= r < 10:
            print(f'    {seg:<55} {n:>4}件 受注{won:>3} 率{r:>5.1f}% | {fy_str}')

    print('\n  [Tier C: 受注率5%未満, 10件以上 (回避推奨)]')
    for seg, n, won, r, fy_str in results:
        if r < 5:
            print(f'    {seg:<55} {n:>4}件 受注{won:>3} 率{r:>5.1f}% | {fy_str}')

    # ===================================
    print('\n' + '=' * 70)
    print('  C. 回避シミュレーション')
    print('=' * 70)

    total_n = len(first)
    total_won = int(first['is_won'].sum())
    print(f'\n  ベース: {total_n:,}件 受注{total_won} 受注率{total_r:.1f}%')

    rules = [
        ('不明x不明 (業種不明)', first['facility'] == '不明'),
        ('+医療x大規模(101人+)', (first['facility'] == '医療') & (first['iryo_size'] == '大規模(101人+)')),
        ('+介護x法人格不明', (first['facility'] == '介護（高齢者）') & (first['legal'] == '不明')),
        ('+介護x合同会社', (first['facility'] == '介護（高齢者）') & (first['legal'] == '合同会社')),
        ('+障がい福祉x医療法人', (first['facility'] == '障がい福祉') & (first['legal'] == '医療法人')),
        ('+医療x社会福祉法人', (first['facility'] == '医療') & (first['legal'] == '社会福祉法人')),
        ('+保育x社団法人', (first['facility'] == '保育') & (first['legal'] == '社団法人')),
        ('+保育x学校法人', (first['facility'] == '保育') & (first['legal'] == '学校法人')),
    ]

    cumulative = pd.Series(False, index=first.index)
    for name, mask in rules:
        cumulative = cumulative | mask
        rem = first[~cumulative]
        removed = int(cumulative.sum())
        r = rem['is_won'].mean() * 100
        print(f'  {name:<35} 除外{removed:>5}件 残{len(rem):>5}件 率{r:>5.1f}% (+{r-total_r:.1f}pt)')

    print('\n分析完了')

if __name__ == '__main__':
    main()
