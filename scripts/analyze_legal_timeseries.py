"""
法人格×時系列の成約率分析

推論した法人格をローカル補完し、以下のテーブルを生成:
1. 法人格(縦) × 時系列(横): 商談数/成約数/成約率
2. 主要法人格×従業員規模: 商談数/成約数/成約率
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

# 施設形態補完ロジック
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


def get_facility_type(row):
    """施設形態を補完"""
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


def get_fy(date_str):
    """日付からFYを取得（4月始まり）"""
    if pd.isna(date_str):
        return None
    try:
        dt = pd.to_datetime(date_str)
        if dt.month >= 4:
            return f'FY{dt.year}'
        else:
            return f'FY{dt.year - 1}'
    except:
        return None


def size_bucket(emp):
    """従業員数バケット"""
    if pd.isna(emp) or emp == 0:
        return '不明/0'
    elif emp <= 10:
        return '1-10'
    elif emp <= 30:
        return '11-30'
    elif emp <= 50:
        return '31-50'
    elif emp <= 100:
        return '51-100'
    elif emp <= 300:
        return '101-300'
    else:
        return '301+'


def size_bucket_3(emp):
    """3段階（医療向け）"""
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

    # ===================================
    # Step 1: 推論結果CSV読み込み
    # ===================================
    print('Step 1: 推論結果CSV読み込み...')
    inferred = pd.read_csv(output_dir / 'account_legal_update.csv', encoding='utf-8-sig')
    # DISAGREE 28件を除外
    disagree_path = output_dir / 'update_disagree.csv'
    if disagree_path.exists():
        disagree = pd.read_csv(disagree_path, encoding='utf-8-sig')
        disagree_ids = set(disagree['Id'].tolist())
        inferred = inferred[~inferred['Id'].isin(disagree_ids)]
    print(f'  推論件数（DISAGREE除外後）: {len(inferred):,}件')

    # Id -> inferred legal のマップ
    inferred_map = dict(zip(inferred['Id'], inferred['LegalPersonality__c']))

    # ===================================
    # Step 2: Salesforce Opportunity データ取得
    # ===================================
    print('\nStep 2: Salesforce Opportunity データ取得...')
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
    """
    opps = svc.bulk_query(query)
    print(f'  商談数: {len(opps):,}件')

    # ===================================
    # Step 3: 法人格補完
    # ===================================
    print('\nStep 3: 法人格補完...')

    # 既存法人格
    opps['legal_raw'] = opps['Account.LegalPersonality__c']

    # 推論で補完
    def complement_legal(row):
        raw = row['legal_raw']
        if pd.notna(raw) and raw and str(raw).strip():
            return raw
        acct_id = row.get('Account.Id', '')
        if acct_id in inferred_map:
            return inferred_map[acct_id]
        return '不明'

    opps['legal'] = opps.apply(complement_legal, axis=1)

    # 補完統計
    had_legal = opps['legal_raw'].notna() & (opps['legal_raw'] != '')
    now_has = opps['legal'] != '不明'
    complemented = (~had_legal) & now_has

    print(f'  補完前: 法人格あり {had_legal.sum():,}件 / {len(opps):,}件 ({had_legal.sum()/len(opps)*100:.1f}%)')
    print(f'  補完後: 法人格あり {now_has.sum():,}件 / {len(opps):,}件 ({now_has.sum()/len(opps)*100:.1f}%)')
    print(f'  今回補完: {complemented.sum():,}件')

    # ===================================
    # Step 4: 基本カラム追加
    # ===================================
    opps['fy'] = opps['CloseDate'].apply(get_fy)
    opps['is_won'] = opps['IsWon'].astype(str).str.lower() == 'true'
    opps['facility'] = opps.apply(get_facility_type, axis=1)
    opps['emp'] = pd.to_numeric(opps['Account.NumberOfEmployees'], errors='coerce').fillna(0)
    opps['size_7'] = opps['emp'].apply(size_bucket)
    opps['size_3'] = opps['emp'].apply(size_bucket_3)
    opps['is_new'] = opps['OpportunityCategory__c'] == '初回商談'

    # FY24+FY25のみ
    opps_fy = opps[opps['fy'].isin(['FY2024', 'FY2025'])].copy()
    print(f'\n  FY2024+FY2025: {len(opps_fy):,}件')

    # ===================================
    # テーブル1: 法人格(縦) × 時系列(横) 成約率
    # ===================================
    print('\n' + '=' * 80)
    print('テーブル1: 法人格 × FY（全体）')
    print('=' * 80)

    def make_pivot(data, row_col, fy_list):
        rows = []
        for val in data[row_col].unique():
            if pd.isna(val):
                continue
            row_data = {'segment': val}
            for fy in fy_list:
                subset = data[(data[row_col] == val) & (data['fy'] == fy)]
                n = len(subset)
                w = subset['is_won'].sum()
                rate = w / n * 100 if n > 0 else 0
                row_data[f'{fy}_n'] = n
                row_data[f'{fy}_w'] = w
                row_data[f'{fy}_r'] = rate
            # 合計
            total = data[data[row_col] == val]
            tn = len(total)
            tw = total['is_won'].sum()
            tr = tw / tn * 100 if tn > 0 else 0
            row_data['total_n'] = tn
            row_data['total_w'] = tw
            row_data['total_r'] = tr
            rows.append(row_data)

        result = pd.DataFrame(rows)
        result = result.sort_values('total_n', ascending=False)
        return result

    fy_list = ['FY2024', 'FY2025']
    pivot1 = make_pivot(opps_fy, 'legal', fy_list)

    # テーブル表示
    print(f'\n{"法人格":<25} | {"FY2024":^25} | {"FY2025":^25} | {"合計":^25}')
    print(f'{"":<25} | {"商談":>5} {"成約":>5} {"率":>7} | {"商談":>5} {"成約":>5} {"率":>7} | {"商談":>5} {"成約":>5} {"率":>7}')
    print('-' * 110)

    for _, row in pivot1.iterrows():
        seg = row['segment']
        line = f'{seg:<25}'
        for prefix in ['FY2024', 'FY2025', 'total']:
            n = int(row.get(f'{prefix}_n', 0))
            w = int(row.get(f'{prefix}_w', 0))
            r = row.get(f'{prefix}_r', 0)
            line += f' | {n:>5} {w:>5} {r:>6.1f}%'
        print(line)

    # ===================================
    # テーブル1b: 施設形態別にも見る
    # ===================================
    print('\n' + '=' * 80)
    print('テーブル1b: 施設形態 × 法人格 × FY')
    print('=' * 80)

    for facility in ['介護', '医療', '保育']:
        fac_data = opps_fy[opps_fy['facility'] == facility]
        if len(fac_data) == 0:
            continue

        print(f'\n--- {facility} ---')
        pivot_fac = make_pivot(fac_data, 'legal', fy_list)

        print(f'{"法人格":<25} | {"FY2024":^25} | {"FY2025":^25} | {"合計":^25}')
        print(f'{"":<25} | {"商談":>5} {"成約":>5} {"率":>7} | {"商談":>5} {"成約":>5} {"率":>7} | {"商談":>5} {"成約":>5} {"率":>7}')
        print('-' * 110)

        for _, row in pivot_fac.iterrows():
            n_total = int(row.get('total_n', 0))
            if n_total < 5:
                continue
            seg = row['segment']
            line = f'{seg:<25}'
            for prefix in ['FY2024', 'FY2025', 'total']:
                n = int(row.get(f'{prefix}_n', 0))
                w = int(row.get(f'{prefix}_w', 0))
                r = row.get(f'{prefix}_r', 0)
                line += f' | {n:>5} {w:>5} {r:>6.1f}%'
            print(line)

    # ===================================
    # テーブル2: 主要法人格 × 従業員規模 × FY
    # ===================================
    print('\n' + '=' * 80)
    print('テーブル2: 主要法人格 × 従業員規模 × FY')
    print('=' * 80)

    # 主要法人格 = 商談数が多い上位
    top_legals = pivot1[pivot1['total_n'] >= 30]['segment'].tolist()

    for legal_name in top_legals:
        legal_data = opps_fy[opps_fy['legal'] == legal_name]
        if len(legal_data) < 10:
            continue

        print(f'\n--- {legal_name}（全体 {len(legal_data)}件, 成約率 {legal_data["is_won"].mean()*100:.1f}%）---')
        pivot_size = make_pivot(legal_data, 'size_7', fy_list)

        # 従業員規模の順序
        size_order = ['不明/0', '1-10', '11-30', '31-50', '51-100', '101-300', '301+']
        pivot_size['sort_key'] = pivot_size['segment'].map(
            {s: i for i, s in enumerate(size_order)}
        )
        pivot_size = pivot_size.sort_values('sort_key')

        print(f'{"従業員規模":<15} | {"FY2024":^25} | {"FY2025":^25} | {"合計":^25}')
        print(f'{"":<15} | {"商談":>5} {"成約":>5} {"率":>7} | {"商談":>5} {"成約":>5} {"率":>7} | {"商談":>5} {"成約":>5} {"率":>7}')
        print('-' * 100)

        for _, row in pivot_size.iterrows():
            seg = row['segment']
            line = f'{seg:<15}'
            for prefix in ['FY2024', 'FY2025', 'total']:
                n = int(row.get(f'{prefix}_n', 0))
                w = int(row.get(f'{prefix}_w', 0))
                r = row.get(f'{prefix}_r', 0)
                line += f' | {n:>5} {w:>5} {r:>6.1f}%'
            print(line)

    # ===================================
    # テーブル3: 施設形態×法人格×従業員規模（有意なセグメント）
    # ===================================
    print('\n' + '=' * 80)
    print('テーブル3: 施設形態 × 法人格 × 従業員規模（商談10件以上）')
    print('=' * 80)

    results = []
    for facility in ['介護', '医療', '保育']:
        for legal in opps_fy['legal'].unique():
            for size in ['小規模(30以下)', '中規模(31-100)', '大規模(101+)', '不明']:
                subset = opps_fy[
                    (opps_fy['facility'] == facility) &
                    (opps_fy['legal'] == legal) &
                    (opps_fy['size_3'] == size)
                ]
                if len(subset) < 10:
                    continue

                for fy in fy_list:
                    fy_sub = subset[subset['fy'] == fy]
                    results.append({
                        'facility': facility,
                        'legal': legal,
                        'size': size,
                        'fy': fy,
                        'n': len(fy_sub),
                        'won': fy_sub['is_won'].sum(),
                        'rate': fy_sub['is_won'].mean() * 100 if len(fy_sub) > 0 else 0
                    })
                # 合計
                results.append({
                    'facility': facility,
                    'legal': legal,
                    'size': size,
                    'fy': '合計',
                    'n': len(subset),
                    'won': subset['is_won'].sum(),
                    'rate': subset['is_won'].mean() * 100
                })

    detail_df = pd.DataFrame(results)

    # ピボット表示
    for facility in ['介護', '医療', '保育']:
        fac_detail = detail_df[detail_df['facility'] == facility]
        if len(fac_detail) == 0:
            continue

        print(f'\n--- {facility} ---')

        # セグメントごとに横展開
        segments = fac_detail[fac_detail['fy'] == '合計'].sort_values('rate', ascending=False)

        print(f'{"法人格":<20} {"規模":<15} | {"FY2024":^25} | {"FY2025":^25} | {"合計":^25}')
        print(f'{"":<20} {"":<15} | {"商談":>5} {"成約":>5} {"率":>7} | {"商談":>5} {"成約":>5} {"率":>7} | {"商談":>5} {"成約":>5} {"率":>7}')
        print('-' * 120)

        for _, seg_row in segments.iterrows():
            legal = seg_row['legal']
            size = seg_row['size']
            line = f'{legal:<20} {size:<15}'

            for fy in ['FY2024', 'FY2025', '合計']:
                match = fac_detail[
                    (fac_detail['legal'] == legal) &
                    (fac_detail['size'] == size) &
                    (fac_detail['fy'] == fy)
                ]
                if len(match) > 0:
                    r = match.iloc[0]
                    line += f' | {int(r["n"]):>5} {int(r["won"]):>5} {r["rate"]:>6.1f}%'
                else:
                    line += f' | {"":>5} {"":>5} {"":>7}'
            print(line)

    # ===================================
    # 補完前後の比較
    # ===================================
    print('\n' + '=' * 80)
    print('補完効果: 法人格不明の減少')
    print('=' * 80)

    before_unknown = opps_fy[
        opps_fy['legal_raw'].isna() | (opps_fy['legal_raw'] == '') | (opps_fy['legal_raw'] == '不明')
    ]
    after_unknown = opps_fy[opps_fy['legal'] == '不明']
    print(f'  補完前「不明」: {len(before_unknown):,}件 ({len(before_unknown)/len(opps_fy)*100:.1f}%)')
    print(f'  補完後「不明」: {len(after_unknown):,}件 ({len(after_unknown)/len(opps_fy)*100:.1f}%)')
    print(f'  解消件数: {len(before_unknown) - len(after_unknown):,}件')

    # 施設形態別
    print(f'\n  施設形態別の不明解消:')
    for fac in ['介護', '医療', '保育', '不明']:
        fac_data = opps_fy[opps_fy['facility'] == fac]
        before = fac_data[fac_data['legal_raw'].isna() | (fac_data['legal_raw'] == '') | (fac_data['legal_raw'] == '不明')]
        after = fac_data[fac_data['legal'] == '不明']
        if len(before) > 0:
            print(f'    {fac}: {len(before):,} → {len(after):,}件 (解消 {len(before)-len(after):,}件)')

    print('\n完了')


if __name__ == '__main__':
    main()
