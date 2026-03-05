"""
FacilityType_Large__c入力率の月別推移 + 法人格×従業員数ティア（月別）
"""
import sys, io
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / 'src'))

import pandas as pd
import numpy as np
from services.opportunity_service import OpportunityService


def size_bucket_3(emp):
    if pd.isna(emp) or emp == 0:
        return '不明'
    elif emp <= 30:
        return '小規模(30以下)'
    elif emp <= 100:
        return '中規模(31-100)'
    else:
        return '大規模(101+)'


def main():
    output_dir = project_root / 'data' / 'output' / 'legal_inference'

    # 推論結果読み込み
    print('推論結果読み込み...')
    inferred = pd.read_csv(output_dir / 'account_legal_update.csv', encoding='utf-8-sig')
    disagree_path = output_dir / 'update_disagree.csv'
    if disagree_path.exists():
        disagree = pd.read_csv(disagree_path, encoding='utf-8-sig')
        inferred = inferred[~inferred['Id'].isin(set(disagree['Id'].tolist()))]
    inferred_map = dict(zip(inferred['Id'], inferred['LegalPersonality__c']))

    # Opportunity取得
    print('Opportunity取得...')
    svc = OpportunityService()
    svc.authenticate()

    df = svc.bulk_query("""
    SELECT Id, FacilityType_Large__c, CloseDate, IsWon,
        Account.Id, Account.Name, Account.LegalPersonality__c,
        Account.NumberOfEmployees
    FROM Opportunity
    WHERE IsClosed = true AND CloseDate >= 2024-04-01
    """)
    print(f'  {len(df):,}件')

    df['close_dt'] = pd.to_datetime(df['CloseDate'], errors='coerce')
    df['ym'] = df['close_dt'].dt.to_period('M').astype(str)
    df['is_won'] = df['IsWon'].astype(str).str.lower() == 'true'
    df['has_ft'] = df['FacilityType_Large__c'].notna() & (df['FacilityType_Large__c'] != '')

    # ===================================
    # Part 1: FacilityType入力率の月別推移
    # ===================================
    print('\n' + '=' * 75)
    print('FacilityType_Large__c 入力率 月別推移')
    print('=' * 75)
    print(f'{"月":<10} {"全体":>6} {"入力あり":>8} {"入力率":>7} | {"入力あり成約率":>13} {"null成約率":>10}')
    print('-' * 75)

    months = sorted(df['ym'].dropna().unique())
    for ym in months:
        m = df[df['ym'] == ym]
        total = len(m)
        has = int(m['has_ft'].sum())
        rate = has / total * 100 if total > 0 else 0
        has_data = m[m['has_ft']]
        null_data = m[~m['has_ft']]
        has_wr = has_data['is_won'].mean() * 100 if len(has_data) > 0 else 0
        null_wr = null_data['is_won'].mean() * 100 if len(null_data) > 0 else 0
        print(f'{ym:<10} {total:>6} {has:>8} {rate:>6.1f}% | {has_wr:>12.1f}% {null_wr:>9.1f}%')

    # FacilityType_Large__cの値別内訳も月別で
    print('\n' + '=' * 75)
    print('FacilityType_Large__c 値別 月別件数')
    print('=' * 75)

    ft_values = df['FacilityType_Large__c'].fillna('(null)').unique()
    ft_values = sorted(ft_values)

    header = f'{"月":<10}'
    for ft in ft_values:
        header += f' {ft[:8]:>8}'
    print(header)
    print('-' * (10 + 9 * len(ft_values)))

    for ym in months:
        m = df[df['ym'] == ym]
        line = f'{ym:<10}'
        for ft in ft_values:
            if ft == '(null)':
                cnt = len(m[m['FacilityType_Large__c'].isna() | (m['FacilityType_Large__c'] == '')])
            else:
                cnt = len(m[m['FacilityType_Large__c'] == ft])
            line += f' {cnt:>8}'
        print(line)

    # ===================================
    # Part 2: 法人格×従業員数ティア（月別）
    # ===================================
    print('\n' + '=' * 120)
    print('法人格 × 従業員規模 ティア（月別成約率）')
    print('=' * 120)

    # 法人格補完
    def complement_legal(row):
        raw = row.get('Account.LegalPersonality__c', '')
        if pd.notna(raw) and raw and str(raw).strip():
            return raw
        acct_id = row.get('Account.Id', '')
        if acct_id in inferred_map:
            return inferred_map[acct_id]
        return '不明'

    df['legal'] = df.apply(complement_legal, axis=1)
    df['emp'] = pd.to_numeric(df['Account.NumberOfEmployees'], errors='coerce').fillna(0)
    df['size'] = df['emp'].apply(size_bucket_3)

    # 全セグメント集計
    segments = df.groupby(['legal', 'size'])
    rows = []
    for (leg, sz), grp in segments:
        row = {'法人格': leg, '従業員規模': sz}
        total_n = 0
        total_w = 0
        for m in months:
            m_data = grp[grp['ym'] == m]
            n = len(m_data)
            w = int(m_data['is_won'].sum())
            total_n += n
            total_w += w
            row[f'{m}_n'] = n
            row[f'{m}_w'] = w
            row[f'{m}_r'] = round(w / n * 100, 1) if n > 0 else None
        row['合計_n'] = total_n
        row['合計_w'] = total_w
        row['合計_r'] = round(total_w / total_n * 100, 1) if total_n > 0 else 0
        rows.append(row)

    result = pd.DataFrame(rows).sort_values('合計_r', ascending=False)
    result.insert(0, '順位', range(1, len(result) + 1))

    # ティア判定
    def assign_tier(rate, n):
        if n < 10:
            return '-'
        if rate >= 15:
            return 'S'
        elif rate >= 10:
            return 'A'
        elif rate >= 7:
            return 'B'
        elif rate >= 4:
            return 'C'
        else:
            return 'D'

    result['ティア'] = result.apply(lambda r: assign_tier(r['合計_r'], r['合計_n']), axis=1)

    # CSV出力
    csv_rows = []
    for _, row in result.iterrows():
        csv_row = {
            '順位': int(row['順位']),
            'ティア': row['ティア'],
            '法人格': row['法人格'],
            '従業員規模': row['従業員規模'],
        }
        for m in months:
            csv_row[f'{m}_商談'] = int(row.get(f'{m}_n', 0)) if pd.notna(row.get(f'{m}_n')) else 0
            csv_row[f'{m}_成約'] = int(row.get(f'{m}_w', 0)) if pd.notna(row.get(f'{m}_w')) else 0
            csv_row[f'{m}_率'] = row.get(f'{m}_r', '')
        csv_row['合計_商談'] = int(row['合計_n'])
        csv_row['合計_成約'] = int(row['合計_w'])
        csv_row['合計_率'] = row['合計_r']
        csv_rows.append(csv_row)

    csv_df = pd.DataFrame(csv_rows)
    out_path = output_dir / 'tier_legal_size_monthly.csv'
    csv_df.to_csv(out_path, index=False, encoding='utf-8-sig')
    print(f'CSV出力: {out_path}')

    # コンソール表示
    # 月ヘッダー短縮
    m_short = [m[-2:] for m in months]
    header_months = ' '.join([f'{ms:>5}' for ms in m_short])

    print(f'\n{"#":>3} {"T":>1} {"法人格":<20} {"規模":<14} {"合計":>5} {"成約":>4} {"率":>6} | {header_months}')
    print('-' * (60 + 6 * len(months)))

    for _, row in result.iterrows():
        if row['合計_n'] < 5:
            continue
        rank = int(row['順位'])
        tier = row['ティア']
        leg = row['法人格']
        sz = row['従業員規模']
        tn = int(row['合計_n'])
        tw = int(row['合計_w'])
        tr = row['合計_r']

        rates = []
        for m in months:
            r = row.get(f'{m}_r')
            n = row.get(f'{m}_n', 0)
            if pd.isna(r) or n == 0:
                rates.append(f'{"":>5}')
            else:
                rates.append(f'{r:>5.1f}')
        rates_str = ' '.join(rates)

        print(f'{rank:>3} {tier:>1} {leg:<20} {sz:<14} {tn:>5} {tw:>4} {tr:>5.1f}% | {rates_str}')

    print(f'\n月: {"  ".join(m_short)}')
    print(f'\nティア基準: S>=15% A>=10% B>=7% C>=4% D<4% (商談10件未満は「-」)')

    # ===================================
    # Part 3: 法人格のみ（従業員規模統合）
    # ===================================
    print('\n' + '=' * 120)
    print('法人格のみ ティア（月別成約率）')
    print('=' * 120)

    rows2 = []
    for leg in df['legal'].unique():
        grp = df[df['legal'] == leg]
        row = {'法人格': leg}
        total_n = 0
        total_w = 0
        for m in months:
            m_data = grp[grp['ym'] == m]
            n = len(m_data)
            w = int(m_data['is_won'].sum())
            total_n += n
            total_w += w
            row[f'{m}_n'] = n
            row[f'{m}_w'] = w
            row[f'{m}_r'] = round(w / n * 100, 1) if n > 0 else None
        row['合計_n'] = total_n
        row['合計_w'] = total_w
        row['合計_r'] = round(total_w / total_n * 100, 1) if total_n > 0 else 0
        rows2.append(row)

    result2 = pd.DataFrame(rows2).sort_values('合計_r', ascending=False)
    result2['ティア'] = result2.apply(lambda r: assign_tier(r['合計_r'], r['合計_n']), axis=1)

    print(f'\n{"T":>1} {"法人格":<25} {"合計":>5} {"成約":>4} {"率":>6} | {header_months}')
    print('-' * (48 + 6 * len(months)))

    for _, row in result2.iterrows():
        tier = row['ティア']
        leg = row['法人格']
        tn = int(row['合計_n'])
        tw = int(row['合計_w'])
        tr = row['合計_r']
        rates = []
        for m in months:
            r = row.get(f'{m}_r')
            n = row.get(f'{m}_n', 0)
            if pd.isna(r) or n == 0:
                rates.append(f'{"":>5}')
            else:
                rates.append(f'{r:>5.1f}')
        rates_str = ' '.join(rates)
        print(f'{tier:>1} {leg:<25} {tn:>5} {tw:>4} {tr:>5.1f}% | {rates_str}')

    print(f'\n月: {"  ".join(m_short)}')

    print('\n完了')


if __name__ == '__main__':
    main()
