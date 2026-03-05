"""
法人番号→法人格推論の精度検証スクリプト

検証観点:
1. 推論結果と国税庁法人名の整合性（法人名に法人格が含まれているか）
2. Salesforceの既存フィールドとの矛盾チェック
3. ランダムサンプリングによる詳細確認
4. 法人格別の分布分析
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

# ── 法人格パターン（推論スクリプトと同一） ──
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


def verify_name_consistency(df):
    """検証1: 推論結果が法人名に実際に含まれているか"""
    print('=' * 60)
    print('検証1: 推論結果と法人名の整合性チェック')
    print('=' * 60)

    results = []
    for _, row in df.iterrows():
        name = str(row['NTA_Name']) if pd.notna(row['NTA_Name']) else ''
        inferred = str(row['LegalPersonality__c'])

        # 推論された法人格に対応するパターンで法人名を検索
        found = False
        for pattern, label in LEGAL_PATTERNS:
            if label == inferred:
                if re.search(pattern, name):
                    found = True
                break
        results.append(found)

    df['name_consistent'] = results
    consistent = sum(results)
    total = len(results)
    print(f'  整合: {consistent:,}件 / {total:,}件 ({consistent/total*100:.2f}%)')
    print(f'  不整合: {total - consistent:,}件')

    # 不整合サンプル表示
    inconsistent = df[~df['name_consistent']]
    if len(inconsistent) > 0:
        print(f'\n  不整合サンプル（先頭20件）:')
        for _, row in inconsistent.head(20).iterrows():
            print(f'    推論: {row["LegalPersonality__c"]:<20} | 法人名: {row["NTA_Name"]}')

    return df


def verify_against_salesforce(df):
    """検証2: Salesforceの既存法人格フィールドとのクロスチェック"""
    print('\n' + '=' * 60)
    print('検証2: Salesforce既存フィールドとのクロスチェック')
    print('=' * 60)

    # 対象AccountのID取得
    account_ids = df['Id'].tolist()
    print(f'  検証対象Account: {len(account_ids):,}件')

    # Salesforceから既存の法人格関連フィールドを取得
    svc = OpportunityService()
    svc.authenticate()

    query = """
    SELECT Id, Name, LegalPersonality__c,
        LegalPersonality_AccountName__c, LegalPersonality_CompanyName__c,
        CorporateNumber__c
    FROM Account
    WHERE CorporateNumber__c != null
    AND (LegalPersonality__c = null OR LegalPersonality__c = '')
    """
    print('  Salesforceから既存データ取得中...')
    accounts = svc.bulk_query(query)
    print(f'  取得件数: {len(accounts):,}件')

    # マージ
    merged = df.merge(
        accounts[['Id', 'Name', 'LegalPersonality_AccountName__c', 'LegalPersonality_CompanyName__c']],
        on='Id',
        how='left'
    )

    # 検証2a: LegalPersonality_AccountName__c との比較
    has_acct_legal = merged[merged['LegalPersonality_AccountName__c'].notna() &
                           (merged['LegalPersonality_AccountName__c'] != '')]
    print(f'\n  LegalPersonality_AccountName__c が既に存在: {len(has_acct_legal):,}件')

    if len(has_acct_legal) > 0:
        match = has_acct_legal[has_acct_legal['LegalPersonality__c'] == has_acct_legal['LegalPersonality_AccountName__c']]
        print(f'    推論結果と一致: {len(match):,}件 ({len(match)/len(has_acct_legal)*100:.1f}%)')
        mismatch = has_acct_legal[has_acct_legal['LegalPersonality__c'] != has_acct_legal['LegalPersonality_AccountName__c']]
        print(f'    不一致: {len(mismatch):,}件')
        if len(mismatch) > 0:
            print(f'\n    不一致サンプル（先頭10件）:')
            for _, row in mismatch.head(10).iterrows():
                print(f'      推論: {row["LegalPersonality__c"]:<20} | '
                      f'既存: {row["LegalPersonality_AccountName__c"]:<20} | '
                      f'Account: {row["Name"]}')

    # 検証2b: LegalPersonality_CompanyName__c との比較
    has_comp_legal = merged[merged['LegalPersonality_CompanyName__c'].notna() &
                           (merged['LegalPersonality_CompanyName__c'] != '')]
    print(f'\n  LegalPersonality_CompanyName__c が既に存在: {len(has_comp_legal):,}件')

    if len(has_comp_legal) > 0:
        match = has_comp_legal[has_comp_legal['LegalPersonality__c'] == has_comp_legal['LegalPersonality_CompanyName__c']]
        print(f'    推論結果と一致: {len(match):,}件 ({len(match)/len(has_comp_legal)*100:.1f}%)')
        mismatch = has_comp_legal[has_comp_legal['LegalPersonality__c'] != has_comp_legal['LegalPersonality_CompanyName__c']]
        print(f'    不一致: {len(mismatch):,}件')
        if len(mismatch) > 0:
            print(f'\n    不一致サンプル（先頭10件）:')
            for _, row in mismatch.head(10).iterrows():
                print(f'      推論: {row["LegalPersonality__c"]:<20} | '
                      f'既存: {row["LegalPersonality_CompanyName__c"]:<20} | '
                      f'Account: {row["Name"]}')

    # 検証2c: Account.Nameとの名前一致度チェック
    print(f'\n  Account.Name と NTA法人名の一致度チェック:')
    name_checks = []
    for _, row in merged.iterrows():
        sf_name = str(row['Name']) if pd.notna(row['Name']) else ''
        nta_name = str(row['NTA_Name']) if pd.notna(row['NTA_Name']) else ''
        # 完全一致
        if sf_name == nta_name:
            name_checks.append('exact')
        # NTA名がSF名を含む（法人格付き名前 vs 法人格なし名前）
        elif sf_name in nta_name or nta_name in sf_name:
            name_checks.append('partial')
        # 法人格を除去して比較
        else:
            nta_core = nta_name
            for pattern, _ in LEGAL_PATTERNS:
                nta_core = re.sub(pattern, '', nta_core).strip()
            if sf_name == nta_core or sf_name in nta_core or nta_core in sf_name:
                name_checks.append('core_match')
            else:
                name_checks.append('different')

    merged['name_match'] = name_checks
    for match_type in ['exact', 'partial', 'core_match', 'different']:
        cnt = name_checks.count(match_type)
        label = {
            'exact': '完全一致',
            'partial': '部分一致（包含関係）',
            'core_match': '法人格除去後一致',
            'different': '名前不一致'
        }[match_type]
        print(f'    {label}: {cnt:,}件 ({cnt/len(name_checks)*100:.1f}%)')

    # 名前不一致サンプル
    diff_names = merged[merged['name_match'] == 'different']
    if len(diff_names) > 0:
        print(f'\n    名前不一致サンプル（先頭15件）:')
        for _, row in diff_names.head(15).iterrows():
            print(f'      SF: {row["Name"][:30]:<30} | NTA: {row["NTA_Name"][:40]}')

    return merged


def distribution_analysis(df):
    """検証3: 法人格別分布分析"""
    print('\n' + '=' * 60)
    print('検証3: 法人格別分布分析')
    print('=' * 60)

    dist = df['LegalPersonality__c'].value_counts()
    total = len(df)
    print(f'\n  法人格別件数:')
    for val, cnt in dist.items():
        print(f'    {val:<30} {cnt:>6,}件 ({cnt/total*100:.1f}%)')

    print(f'\n  合計: {total:,}件')


def random_sample_check(df, n=30):
    """検証4: ランダムサンプルの詳細表示"""
    print('\n' + '=' * 60)
    print(f'検証4: ランダムサンプル（{n}件）')
    print('=' * 60)

    sample = df.sample(n=min(n, len(df)), random_state=42)
    for i, (_, row) in enumerate(sample.iterrows(), 1):
        nta_name = row['NTA_Name'] if pd.notna(row['NTA_Name']) else 'N/A'
        consistent = 'OK' if row.get('name_consistent', True) else 'NG'
        print(f'  {i:>2}. [{consistent}] 推論: {row["LegalPersonality__c"]:<20} | NTA: {nta_name}')


def main():
    output_dir = project_root / 'data' / 'output' / 'legal_inference'

    # 推論結果CSV読み込み
    csv_path = output_dir / 'account_legal_update.csv'
    print(f'推論結果CSV読み込み: {csv_path}')
    df = pd.read_csv(csv_path, encoding='utf-8-sig')
    print(f'  レコード数: {len(df):,}件')

    # 検証1: 法人名との整合性
    df = verify_name_consistency(df)

    # 検証2: Salesforce既存フィールドとのクロスチェック
    merged = verify_against_salesforce(df)

    # 検証3: 分布分析
    distribution_analysis(df)

    # 検証4: ランダムサンプル
    random_sample_check(df)

    # ===================================
    # サマリー
    # ===================================
    print('\n' + '=' * 60)
    print('精度検証サマリー')
    print('=' * 60)

    consistent_rate = df['name_consistent'].sum() / len(df) * 100
    print(f'  法人名との整合率: {consistent_rate:.2f}%')
    print(f'  総推論件数: {len(df):,}件')
    print(f'  整合件数: {df["name_consistent"].sum():,}件')
    print(f'  不整合件数: {(~df["name_consistent"]).sum():,}件')

    # 結果をファイルに保存
    report_path = output_dir / 'verification_report.txt'

    # 不整合レコードのCSV出力
    inconsistent = df[~df['name_consistent']]
    if len(inconsistent) > 0:
        inconsistent_path = output_dir / 'inconsistent_records.csv'
        inconsistent.to_csv(inconsistent_path, index=False, encoding='utf-8-sig')
        print(f'\n  不整合レコードCSV: {inconsistent_path}')

    print('\n完了')


if __name__ == '__main__':
    main()
