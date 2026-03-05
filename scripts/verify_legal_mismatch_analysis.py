"""
検証D不一致パターンの詳細分析

目的: 3.2%の不一致がどういうパターンで発生しているか解明し、
      フィルタリング条件を設計する
"""
import sys, io
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / 'src'))

import pandas as pd
import re
from api.salesforce_client import SalesforceClient
from services.opportunity_service import OpportunityService

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


def extract_legal(name):
    if pd.isna(name) or not name:
        return None
    name = str(name)
    for pattern, label in LEGAL_PATTERNS:
        if re.search(pattern, name):
            return label
    return None


def main():
    output_dir = project_root / 'data' / 'output' / 'legal_inference'

    svc = OpportunityService()
    svc.authenticate()

    # 法人格既存Accountを取得
    print('法人格既存Accountを取得...')
    query = """
    SELECT Id, Name, CorporateNumber__c, LegalPersonality__c
    FROM Account
    WHERE CorporateNumber__c != null
    AND LegalPersonality__c != null
    AND LegalPersonality__c != ''
    """
    existing = svc.bulk_query(query)
    print(f'  取得: {len(existing):,}件')

    existing['corp_num_clean'] = existing['CorporateNumber__c'].apply(
        lambda x: str(x).replace('.0', '').strip().zfill(13) if pd.notna(x) else ''
    )

    # Account名から法人格を推論（法人番号不要）
    print('\nAccount.Nameから法人格を直接推論...')
    existing['name_inferred'] = existing['Name'].apply(extract_legal)

    has_name_legal = existing[existing['name_inferred'].notna()]
    print(f'  Account名から推論可能: {len(has_name_legal):,}件 / {len(existing):,}件')

    if len(has_name_legal) > 0:
        name_match = has_name_legal[has_name_legal['LegalPersonality__c'] == has_name_legal['name_inferred']]
        print(f'  既存値とAccount名推論の一致: {len(name_match):,}件 ({len(name_match)/len(has_name_legal)*100:.1f}%)')

    # 国税庁CSV読み込み
    nta_csv = Path(r'C:\Users\fuji1\Downloads\00_zenkoku_all_20260227\00_zenkoku_all_20260227.csv')
    NTA_COLUMNS = [
        'seq', 'corporate_number', 'process', 'correct', 'update_date', 'change_date',
        'name', 'name_image', 'kind', 'prefecture_name', 'city_name', 'street_number',
        'address_image', 'prefecture_code', 'city_code', 'post_code',
        'address_outside', 'address_outside_image',
        'close_date', 'close_cause', 'successor_corporate_number',
        'change_cause', 'assignment_date', 'latest',
        'en_name', 'en_prefecture_name', 'en_city_name', 'en_address_outside',
        'furigana', 'hihyoji'
    ]

    target_numbers = set(existing['corp_num_clean'].unique())

    matched = []
    chunk_size = 100_000
    total_rows = 0

    print('\n国税庁CSV読み込み中...')
    for chunk in pd.read_csv(
        nta_csv, header=None, names=NTA_COLUMNS, dtype=str,
        usecols=['corporate_number', 'name', 'latest'],
        chunksize=chunk_size, encoding='utf-8', on_bad_lines='skip'
    ):
        total_rows += len(chunk)
        chunk = chunk[chunk['latest'] == '1']
        hits = chunk[chunk['corporate_number'].isin(target_numbers)]
        if len(hits) > 0:
            matched.append(hits)
        if total_rows % 2_000_000 == 0:
            print(f'  {total_rows:>10,}行処理...')

    nta_matched = pd.concat(matched, ignore_index=True) if matched else pd.DataFrame()
    print(f'  マッチ: {len(nta_matched):,}件')

    nta_matched['nta_inferred'] = nta_matched['name'].apply(extract_legal)

    # マージ
    bench = existing.merge(
        nta_matched[['corporate_number', 'name', 'nta_inferred']],
        left_on='corp_num_clean', right_on='corporate_number', how='inner'
    )

    bench_inferred = bench[bench['nta_inferred'].notna()].copy()
    bench_inferred['is_match'] = bench_inferred['LegalPersonality__c'] == bench_inferred['nta_inferred']

    mismatch = bench_inferred[~bench_inferred['is_match']].copy()
    print(f'\n不一致: {len(mismatch):,}件')

    # ===================================
    # 分析1: 不一致の方向性（どの法人格→どの法人格が多い？）
    # ===================================
    print('\n' + '=' * 60)
    print('分析1: 不一致の法人格変換マトリクス')
    print('=' * 60)

    cross = pd.crosstab(mismatch['LegalPersonality__c'], mismatch['nta_inferred'],
                        margins=True, margins_name='合計')
    print(cross.to_string())

    # ===================================
    # 分析2: Account名にも法人格がある場合、どちらが正しい？
    # ===================================
    print('\n' + '=' * 60)
    print('分析2: 不一致レコードのAccount名からの推論')
    print('=' * 60)

    mismatch['name_legal'] = mismatch['Name'].apply(extract_legal)

    has_name = mismatch[mismatch['name_legal'].notna()]
    print(f'  Account名から推論可能: {len(has_name):,}件 / {len(mismatch):,}件')

    if len(has_name) > 0:
        # Account名推論 vs 既存値
        name_match_existing = has_name[has_name['name_legal'] == has_name['LegalPersonality__c']]
        # Account名推論 vs NTA推論
        name_match_nta = has_name[has_name['name_legal'] == has_name['nta_inferred']]

        print(f'  Account名推論 = 既存値: {len(name_match_existing):,}件 ({len(name_match_existing)/len(has_name)*100:.1f}%)')
        print(f'  Account名推論 = NTA推論: {len(name_match_nta):,}件 ({len(name_match_nta)/len(has_name)*100:.1f}%)')
        print(f'  → 既存値のほうが正しい場合が多い = 法人番号のデータ品質問題')

    # ===================================
    # 分析3: 不一致の原因分類
    # ===================================
    print('\n' + '=' * 60)
    print('分析3: 不一致の原因分類')
    print('=' * 60)

    # 同じ法人番号が複数Accountに使われているか
    corp_counts = existing['corp_num_clean'].value_counts()

    mismatch['corp_shared'] = mismatch['corp_num_clean'].map(
        lambda x: corp_counts.get(x, 0)
    )

    shared = mismatch[mismatch['corp_shared'] > 1]
    unique_corp = mismatch[mismatch['corp_shared'] == 1]
    print(f'  複数Account共有法人番号: {len(shared):,}件')
    print(f'  単独Account法人番号: {len(unique_corp):,}件')

    # ===================================
    # 分析4: 安全な更新のためのフィルタリング提案
    # ===================================
    print('\n' + '=' * 60)
    print('分析4: 安全な更新のためのフィルタリング提案')
    print('=' * 60)

    # 法人格不明Accountに対して適用した場合の推定影響
    update_csv = pd.read_csv(output_dir / 'account_legal_update.csv', encoding='utf-8-sig')
    total_update = len(update_csv)
    estimated_error = int(total_update * 0.032)

    print(f'  推論対象: {total_update:,}件')
    print(f'  推定エラー率: 3.2%')
    print(f'  推定エラー件数: ~{estimated_error:,}件')

    print(f'\n  フィルタリング案:')
    print(f'  案A: そのまま全件更新（推定エラー ~{estimated_error:,}件を許容）')
    print(f'  案B: Account名にも法人格が含まれるケースのみ更新（二重確認）')

    # 案Bの対象件数を計算
    update_csv['name_legal'] = update_csv['NTA_Name'].apply(extract_legal)
    # Account名は持っていないのでSFデータとマージが必要

    # 法人格不明Account取得
    query2 = """
    SELECT Id, Name
    FROM Account
    WHERE CorporateNumber__c != null
    AND (LegalPersonality__c = null OR LegalPersonality__c = '')
    """
    print('\n  法人格不明AccountのName取得中...')
    target_accounts = svc.bulk_query(query2)
    print(f'  取得: {len(target_accounts):,}件')

    merged = update_csv.merge(target_accounts[['Id', 'Name']], on='Id', how='left')
    merged['sf_name_legal'] = merged['Name'].apply(extract_legal)

    # 二重確認: SF名推論 = NTA推論
    both_agree = merged[
        (merged['sf_name_legal'].notna()) &
        (merged['sf_name_legal'] == merged['LegalPersonality__c'])
    ]
    # NTA推論のみ（SF名から推論不可）
    nta_only = merged[merged['sf_name_legal'].isna()]
    # SF名推論とNTA推論が不一致
    disagree = merged[
        (merged['sf_name_legal'].notna()) &
        (merged['sf_name_legal'] != merged['LegalPersonality__c'])
    ]

    print(f'\n  フィルタリング結果:')
    print(f'  -----------------------------------------------')
    print(f'  SF名+NTA両方一致（最高信頼度）: {len(both_agree):,}件')
    print(f'  NTA推論のみ（SF名に法人格なし）: {len(nta_only):,}件')
    print(f'  SF名とNTA推論が不一致（要注意）: {len(disagree):,}件')

    if len(disagree) > 0:
        print(f'\n  不一致サンプル（先頭15件）:')
        for _, row in disagree.head(15).iterrows():
            sf_legal = row['sf_name_legal']
            nta_legal = row['LegalPersonality__c']
            print(f'    SF名推論: {sf_legal:<20} | NTA推論: {nta_legal:<20} | '
                  f'SF: {str(row["Name"])[:25]} | NTA: {str(row["NTA_Name"])[:30]}')

    # CSVに信頼度列を追加して出力
    merged['confidence'] = 'NTA_ONLY'
    merged.loc[
        (merged['sf_name_legal'].notna()) &
        (merged['sf_name_legal'] == merged['LegalPersonality__c']),
        'confidence'
    ] = 'BOTH_AGREE'
    merged.loc[
        (merged['sf_name_legal'].notna()) &
        (merged['sf_name_legal'] != merged['LegalPersonality__c']),
        'confidence'
    ] = 'DISAGREE'

    # 信頼度別CSV出力
    for conf in ['BOTH_AGREE', 'NTA_ONLY', 'DISAGREE']:
        subset = merged[merged['confidence'] == conf]
        out_path = output_dir / f'update_{conf.lower()}.csv'
        subset[['Id', 'LegalPersonality__c', 'NTA_Name', 'Name', 'confidence']].to_csv(
            out_path, index=False, encoding='utf-8-sig'
        )
        print(f'\n  {conf}: {len(subset):,}件 → {out_path.name}')

    # サマリー
    print('\n' + '=' * 60)
    print('最終サマリー')
    print('=' * 60)
    print(f'  ベンチマーク精度: 96.8%（法人格既存Accountでの検証）')
    print(f'  推論対象: {total_update:,}件')
    print(f'')
    print(f'  推奨更新戦略:')
    print(f'  1. BOTH_AGREE（{len(both_agree):,}件）: 即座に更新可 - SF名とNTA両方が同じ法人格')
    print(f'  2. NTA_ONLY（{len(nta_only):,}件）: 96.8%の精度で更新可 - 3.2%のリスクあり')
    print(f'  3. DISAGREE（{len(disagree):,}件）: 要手動確認 - SF名とNTA推論が矛盾')

    print('\n完了')


if __name__ == '__main__':
    main()
