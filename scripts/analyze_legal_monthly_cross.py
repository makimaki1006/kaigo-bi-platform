"""
法人格×施設形態×従業員規模 クロス集計（月単位）
全セグメントを漏れなく一覧化
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
from api.salesforce_client import SalesforceClient
from services.opportunity_service import OpportunityService

INDUSTRY_MAP = {
    '医療，福祉': '介護', '医療、福祉': '介護', '医療,福祉': '介護',
    '医療': '医療', '福祉': '介護',
}
SERVICE_TYPE_MAP = {
    '訪問看護': '介護', '訪問介護': '介護', '通所介護': '介護',
    '居宅介護支援': '介護', '特別養護老人ホーム': '介護',
    '認知症対応型共同生活介護': '介護', '小規模多機能型居宅介護': '介護',
    '短期入所生活介護': '介護', '介護老人保健施設': '介護',
    '地域密着型通所介護': '介護', '通所リハビリテーション': '介護',
    '認知症対応型通所介護': '介護', '訪問リハビリテーション': '介護',
    '訪問入浴介護': '介護', '福祉用具貸与': '介護',
    '定期巡回・随時対応型訪問介護看護': '介護',
    '看護小規模多機能型居宅介護': '介護',
    '夜間対応型訪問介護': '介護', '障害者施設': '介護',
    '病院': '医療', '診療所': '医療', 'クリニック': '医療',
    '歯科': '医療', '薬局': '医療', '調剤薬局': '医療',
    '保育所': '保育', '保育園': '保育', '認定こども園': '保育',
    '幼稚園': '保育', '学童保育': '保育', '児童発達支援': '保育',
    '放課後等デイサービス': '保育',
}

LEGAL_PATTERNS = [
    (r'社会福祉法人', '社会福祉法人'),
    (r'医療法人', '医療法人'),
    (r'株式会社', '株式会社'),
    (r'有限会社', '有限会社'),
    (r'合同会社', '合同会社'),
    (r'特定非営利活動法人|NPO法人', 'NPO法人（特定非営利活動法人）'),
    (r'一般社団法人|公益社団法人|社団法人', '社団法人'),
    (r'一般財団法人|公益財団法人|財団法人', '財団法人'),
    (r'学校法人', '学校法人'),
    (r'宗教法人', '宗教法人'),
    (r'協同組合|協会|組合', '組合'),
    (r'合資会社', '合資会社'),
    (r'合名会社', '合名会社'),
]


def get_facility_type(row):
    ft = row.get('FacilityType_Large__c', '')
    if pd.notna(ft) and ft and ft != '不明':
        return ft
    ind = str(row.get('Account.IndustryCategory__c', ''))
    for key, val in INDUSTRY_MAP.items():
        if key in ind:
            return val
    svc = str(row.get('Account.ServiceType__c', ''))
    for key, val in SERVICE_TYPE_MAP.items():
        if key in svc:
            return val
    return '不明'


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
    output_dir.mkdir(parents=True, exist_ok=True)

    # 推論結果読み込み
    print('Step 1: 推論結果CSV読み込み...')
    inferred = pd.read_csv(output_dir / 'account_legal_update.csv', encoding='utf-8-sig')
    disagree_path = output_dir / 'update_disagree.csv'
    if disagree_path.exists():
        disagree = pd.read_csv(disagree_path, encoding='utf-8-sig')
        inferred = inferred[~inferred['Id'].isin(set(disagree['Id'].tolist()))]
    inferred_map = dict(zip(inferred['Id'], inferred['LegalPersonality__c']))
    print(f'  推論件数: {len(inferred):,}件')

    # Opportunity取得
    print('\nStep 2: Salesforce Opportunity 取得...')
    svc = OpportunityService()
    svc.authenticate()

    query = """
    SELECT Id, Name, StageName, IsWon, CloseDate, Amount,
        OpportunityCategory__c, FacilityType_Large__c,
        Account.Id, Account.Name, Account.LegalPersonality__c,
        Account.NumberOfEmployees,
        Account.IndustryCategory__c, Account.ServiceType__c,
        Account.CorporateNumber__c
    FROM Opportunity
    WHERE IsClosed = true
    AND CloseDate >= 2024-04-01
    """
    opps = svc.bulk_query(query)
    print(f'  商談数: {len(opps):,}件')

    # 法人格補完
    def complement_legal(row):
        raw = row.get('Account.LegalPersonality__c', '')
        if pd.notna(raw) and raw and str(raw).strip():
            return raw
        acct_id = row.get('Account.Id', '')
        if acct_id in inferred_map:
            return inferred_map[acct_id]
        return '不明'

    opps['legal'] = opps.apply(complement_legal, axis=1)
    opps['is_won'] = opps['IsWon'].astype(str).str.lower() == 'true'
    opps['facility'] = opps.apply(get_facility_type, axis=1)
    opps['emp'] = pd.to_numeric(opps['Account.NumberOfEmployees'], errors='coerce').fillna(0)
    opps['size'] = opps['emp'].apply(size_bucket_3)
    opps['close_dt'] = pd.to_datetime(opps['CloseDate'], errors='coerce')
    opps['ym'] = opps['close_dt'].dt.to_period('M').astype(str)

    # 月リスト生成
    months = sorted(opps['ym'].dropna().unique())
    print(f'  月範囲: {months[0]} ~ {months[-1]} ({len(months)}ヶ月)')

    # ===================================
    # クロス集計1: 施設形態×法人格×従業員規模
    # ===================================
    print('\nStep 3: クロス集計生成...')

    # 全組み合わせ
    groups = opps.groupby(['facility', 'legal', 'size'])

    rows = []
    for (fac, leg, sz), grp in groups:
        row = {
            '施設形態': fac,
            '法人格': leg,
            '従業員規模': sz,
        }

        # 月別
        total_n = 0
        total_w = 0
        for m in months:
            m_data = grp[grp['ym'] == m]
            n = len(m_data)
            w = int(m_data['is_won'].sum())
            total_n += n
            total_w += w
            row[f'{m}_商談'] = n
            row[f'{m}_成約'] = w
            row[f'{m}_率'] = round(w / n * 100, 1) if n > 0 else ''

        row['合計_商談'] = total_n
        row['合計_成約'] = total_w
        row['合計_率'] = round(total_w / total_n * 100, 1) if total_n > 0 else 0

        rows.append(row)

    result = pd.DataFrame(rows)
    result = result.sort_values('合計_率', ascending=False)

    # 順位付け
    result.insert(0, '順位', range(1, len(result) + 1))

    # CSV出力
    out_path = output_dir / 'cross_monthly_full.csv'
    result.to_csv(out_path, index=False, encoding='utf-8-sig')
    print(f'  出力: {out_path} ({len(result)}セグメント)')

    # ===================================
    # クロス集計2: 施設形態×法人格（従業員規模なし）
    # ===================================
    groups2 = opps.groupby(['facility', 'legal'])
    rows2 = []
    for (fac, leg), grp in groups2:
        row = {'施設形態': fac, '法人格': leg}
        total_n = 0
        total_w = 0
        for m in months:
            m_data = grp[grp['ym'] == m]
            n = len(m_data)
            w = int(m_data['is_won'].sum())
            total_n += n
            total_w += w
            row[f'{m}_商談'] = n
            row[f'{m}_成約'] = w
            row[f'{m}_率'] = round(w / n * 100, 1) if n > 0 else ''
        row['合計_商談'] = total_n
        row['合計_成約'] = total_w
        row['合計_率'] = round(total_w / total_n * 100, 1) if total_n > 0 else 0
        rows2.append(row)

    result2 = pd.DataFrame(rows2).sort_values('合計_率', ascending=False)
    result2.insert(0, '順位', range(1, len(result2) + 1))

    out_path2 = output_dir / 'cross_monthly_facility_legal.csv'
    result2.to_csv(out_path2, index=False, encoding='utf-8-sig')
    print(f'  出力: {out_path2} ({len(result2)}セグメント)')

    # ===================================
    # クロス集計3: 法人格のみ
    # ===================================
    rows3 = []
    for leg in opps['legal'].unique():
        grp = opps[opps['legal'] == leg]
        row = {'法人格': leg}
        total_n = 0
        total_w = 0
        for m in months:
            m_data = grp[grp['ym'] == m]
            n = len(m_data)
            w = int(m_data['is_won'].sum())
            total_n += n
            total_w += w
            row[f'{m}_商談'] = n
            row[f'{m}_成約'] = w
            row[f'{m}_率'] = round(w / n * 100, 1) if n > 0 else ''
        row['合計_商談'] = total_n
        row['合計_成約'] = total_w
        row['合計_率'] = round(total_w / total_n * 100, 1) if total_n > 0 else 0
        rows3.append(row)

    result3 = pd.DataFrame(rows3).sort_values('合計_率', ascending=False)
    result3.insert(0, '順位', range(1, len(result3) + 1))

    out_path3 = output_dir / 'cross_monthly_legal_only.csv'
    result3.to_csv(out_path3, index=False, encoding='utf-8-sig')
    print(f'  出力: {out_path3} ({len(result3)}セグメント)')

    # ===================================
    # コンソール出力: 合計成約率順位（商談10件以上）
    # ===================================
    print('\n' + '=' * 120)
    print('全セグメント順位（施設形態×法人格×従業員規模、商談10件以上）')
    print('=' * 120)

    sig = result[result['合計_商談'] >= 10].copy()
    print(f'{"#":>3} {"施設形態":<6} {"法人格":<20} {"規模":<14} | {"合計":^15} | 月別成約率推移')
    print(f'{"":>3} {"":>6} {"":>20} {"":>14} | {"商談":>4} {"成約":>4} {"率":>6} |')
    print('-' * 120)

    for _, row in sig.iterrows():
        rank = int(row['順位'])
        fac = row['施設形態']
        leg = row['法人格']
        sz = row['従業員規模']
        tn = int(row['合計_商談'])
        tw = int(row['合計_成約'])
        tr = row['合計_率']

        # 月別率の推移（数字のみ、空は-）
        monthly_rates = []
        for m in months:
            r = row.get(f'{m}_率', '')
            if r == '' or pd.isna(r):
                monthly_rates.append('  -')
            else:
                monthly_rates.append(f'{r:>4.0f}')

        rates_str = ' '.join(monthly_rates)

        print(f'{rank:>3} {fac:<6} {leg:<20} {sz:<14} | {tn:>4} {tw:>4} {tr:>5.1f}% | {rates_str}')

    # 月ヘッダー
    print(f'\n  月: {" ".join([m[-2:] for m in months])}')

    # ===================================
    # コンソール出力: 施設形態×法人格（商談10件以上）
    # ===================================
    print('\n' + '=' * 120)
    print('施設形態×法人格 順位（商談10件以上）')
    print('=' * 120)

    sig2 = result2[result2['合計_商談'] >= 10].copy()
    print(f'{"#":>3} {"施設形態":<6} {"法人格":<20} | {"合計":^15} | 月別成約率推移')
    print(f'{"":>3} {"":>6} {"":>20} | {"商談":>4} {"成約":>4} {"率":>6} |')
    print('-' * 100)

    for _, row in sig2.iterrows():
        rank = int(row['順位'])
        fac = row['施設形態']
        leg = row['法人格']
        tn = int(row['合計_商談'])
        tw = int(row['合計_成約'])
        tr = row['合計_率']

        monthly_rates = []
        for m in months:
            r = row.get(f'{m}_率', '')
            if r == '' or pd.isna(r):
                monthly_rates.append('  -')
            else:
                monthly_rates.append(f'{r:>4.0f}')
        rates_str = ' '.join(monthly_rates)

        print(f'{rank:>3} {fac:<6} {leg:<20} | {tn:>4} {tw:>4} {tr:>5.1f}% | {rates_str}')

    print(f'\n  月: {" ".join([m[-2:] for m in months])}')

    print('\n完了')


if __name__ == '__main__':
    main()
