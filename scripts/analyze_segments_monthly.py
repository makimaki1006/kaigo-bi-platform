"""全セグメント × 月別の商談数・成約数・受注率一覧（FY2024+FY2025）"""
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

def iryo_size(n):
    if pd.isna(n) or n == 0: return '不明'
    if n <= 30: return '小規模(30人以下)'
    if n <= 100: return '中規模(31-100人)'
    return '大規模(101人+)'

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
    df['month'] = df['close_date'].dt.month

    first = df[df['OpportunityCategory__c'] == '初回商談'].copy()
    first['facility'] = first.apply(complement_facility, axis=1)

    for c in first.columns:
        if 'LegalPersonality' in c:
            first['legal'] = first[c].fillna('不明')
        if 'NumberOfEmployees' in c:
            first['emp'] = pd.to_numeric(first[c], errors='coerce')

    first['iryo_size'] = first['emp'].apply(iryo_size)

    # セグメント定義
    def make_seg(row):
        fac = row['facility']
        lg = row['legal']
        if fac == '医療':
            sz = row['iryo_size']
            return f'{fac} x {lg} x {sz}'
        else:
            return f'{fac} x {lg}'
    first['segment'] = first.apply(make_seg, axis=1)

    # 全体の月別
    print('=' * 120)
    print('  全体 月別')
    print('=' * 120)
    print(f'{"月":>4} | {"商談数":>6} | {"成約数":>6} | {"受注率":>6}')
    print('-' * 35)
    for m in [4,5,6,7,8,9,10,11,12,1,2,3]:
        sub = first[first['month'] == m]
        if len(sub) > 0:
            n = len(sub)
            w = int(sub['is_won'].sum())
            r = sub['is_won'].mean() * 100
            print(f'{m:>3}月 | {n:>6} | {w:>6} | {r:>5.1f}%')
    total_n = len(first)
    total_w = int(first['is_won'].sum())
    total_r = first['is_won'].mean() * 100
    print('-' * 35)
    print(f'合計 | {total_n:>6} | {total_w:>6} | {total_r:>5.1f}%')

    # 主要セグメントの月別一覧
    # 10件以上のセグメントを受注率順に
    seg_stats = []
    for seg, sub in first.groupby('segment'):
        if len(sub) >= 10:
            r = sub['is_won'].mean() * 100
            seg_stats.append((seg, len(sub), int(sub['is_won'].sum()), r))
    seg_stats.sort(key=lambda x: -x[3])

    # Tier分け
    tiers = {
        'S (15%超)': [(s, n, w, r) for s, n, w, r in seg_stats if r >= 15],
        'A (10-15%)': [(s, n, w, r) for s, n, w, r in seg_stats if 10 <= r < 15],
        'B (5-10%)': [(s, n, w, r) for s, n, w, r in seg_stats if 5 <= r < 10],
        'C (5%未満)': [(s, n, w, r) for s, n, w, r in seg_stats if r < 5],
    }

    for tier_name, tier_segs in tiers.items():
        print(f'\n{"=" * 120}')
        print(f'  Tier {tier_name}')
        print(f'{"=" * 120}')

        for seg, total_n, total_w, total_r in tier_segs:
            print(f'\n  [{seg}] 合計: 商談{total_n} 成約{total_w} 率{total_r:.1f}%')
            print(f'  {"月":>4} | {"商談数":>6} | {"成約数":>6} | {"受注率":>6}')
            print(f'  {"-" * 33}')

            seg_data = first[first['segment'] == seg]
            for m in [4,5,6,7,8,9,10,11,12,1,2,3]:
                sub = seg_data[seg_data['month'] == m]
                if len(sub) > 0:
                    n = len(sub)
                    w = int(sub['is_won'].sum())
                    r = sub['is_won'].mean() * 100
                    # 件数少ない月は注釈
                    note = ' *' if n < 5 else ''
                    print(f'  {m:>3}月 | {n:>6} | {w:>6} | {r:>5.1f}%{note}')
            print(f'  {"-" * 33}')
            print(f'  合計 | {total_n:>6} | {total_w:>6} | {total_r:>5.1f}%')

    # サマリーテーブル（全セグメント横並び月別）
    print(f'\n{"=" * 120}')
    print('  月別サマリー（Tier S + A のみ、横並び）')
    print(f'{"=" * 120}')

    tier_sa = tiers['S (15%超)'] + tiers['A (10-15%)']

    # ヘッダー
    header = f'{"セグメント":<50} | {"合計":>10} |'
    for m in [4,5,6,7,8,9,10,11,12,1,2,3]:
        header += f' {m:>2}月   |'
    print(header)
    print('-' * len(header))

    for seg, total_n, total_w, total_r in tier_sa:
        seg_data = first[first['segment'] == seg]
        line = f'{seg:<50} | {total_n:>4}/{total_w:>3}={total_r:>4.1f}% |'
        for m in [4,5,6,7,8,9,10,11,12,1,2,3]:
            sub = seg_data[seg_data['month'] == m]
            if len(sub) > 0:
                n = len(sub)
                w = int(sub['is_won'].sum())
                r = sub['is_won'].mean() * 100
                line += f' {n:>3}/{w:>2}={r:>4.1f}%|'
            else:
                line += f'     -     |'
        print(line)

    print('\n* = 5件未満（参考値）')
    print('分析完了')

if __name__ == '__main__':
    main()
