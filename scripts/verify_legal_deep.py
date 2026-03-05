"""
法人番号マッチング精度の深掘り検証

問題: Account.Name(事業所名) と NTA法人名(法人名) が81%不一致
仮説: 介護事業所は法人が複数事業所を運営するため、法人名≠事業所名は正常
検証: 法人番号が正しいなら、同じ法人番号のAccountが複数存在するはず
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


def main():
    output_dir = project_root / 'data' / 'output' / 'legal_inference'

    # 推論結果CSV読み込み
    csv_path = output_dir / 'account_legal_update.csv'
    df = pd.read_csv(csv_path, encoding='utf-8-sig')
    print(f'推論結果: {len(df):,}件')

    # Salesforceから全Account(法人番号あり)を取得
    print('\nSalesforceから法人番号ありAccountを取得...')
    svc = OpportunityService()
    svc.authenticate()

    query = """
    SELECT Id, Name, CorporateNumber__c, LegalPersonality__c,
        ServiceType__c, ParentId, Parent.Name
    FROM Account
    WHERE CorporateNumber__c != null
    AND (LegalPersonality__c = null OR LegalPersonality__c = '')
    """
    accounts = svc.bulk_query(query)
    print(f'  取得件数: {len(accounts):,}件')

    # 法人番号正規化
    accounts['corp_num_clean'] = accounts['CorporateNumber__c'].apply(
        lambda x: str(x).replace('.0', '').strip().zfill(13) if pd.notna(x) else ''
    )

    # ===================================
    # 検証A: 同一法人番号のAccount数を確認
    # ===================================
    print('\n' + '=' * 60)
    print('検証A: 同一法人番号を持つAccount数の分布')
    print('=' * 60)

    corp_counts = accounts['corp_num_clean'].value_counts()
    dist = corp_counts.value_counts().sort_index()
    for n_accounts, n_corps in dist.head(20).items():
        print(f'  {n_accounts}事業所を持つ法人: {n_corps:,}件')

    multi_corp = corp_counts[corp_counts > 1]
    single_corp = corp_counts[corp_counts == 1]
    print(f'\n  複数事業所法人: {len(multi_corp):,}件（全{multi_corp.sum():,}Account）')
    print(f'  単一事業所法人: {len(single_corp):,}件')

    # ===================================
    # 検証B: 名前不一致の内訳分析
    # ===================================
    print('\n' + '=' * 60)
    print('検証B: 名前不一致パターンの分析')
    print('=' * 60)

    # マージ
    merged = accounts.merge(
        df[['Id', 'LegalPersonality__c', 'NTA_Name']],
        on='Id',
        how='inner'
    )

    # 名前マッチ判定
    def classify_name_match(row):
        sf_name = str(row['Name']) if pd.notna(row['Name']) else ''
        nta_name = str(row['NTA_Name']) if pd.notna(row['NTA_Name']) else ''

        if sf_name == nta_name:
            return 'exact'

        # 法人格除去してコア名で比較
        nta_core = nta_name
        for pattern, _ in LEGAL_PATTERNS:
            nta_core = re.sub(pattern, '', nta_core).strip()

        if sf_name == nta_core:
            return 'legal_prefix_only'

        if nta_core in sf_name or sf_name in nta_core:
            return 'partial_overlap'

        # 法人名の一部がAccount名に含まれるか
        # 例: 「社会福祉法人恵寿会」→「恵寿会」が「グループホーム恵寿会」に含まれるか
        if len(nta_core) >= 2 and nta_core in sf_name:
            return 'core_in_account'

        # Account名に法人名の主要部分が含まれるか
        if len(nta_core) >= 3:
            # 3文字以上の一致を探す
            for i in range(len(nta_core) - 2):
                substr = nta_core[i:i+3]
                if substr in sf_name:
                    return 'substr_overlap'

        return 'completely_different'

    merged['match_type'] = merged.apply(classify_name_match, axis=1)

    for mt in ['exact', 'legal_prefix_only', 'partial_overlap', 'core_in_account',
               'substr_overlap', 'completely_different']:
        cnt = (merged['match_type'] == mt).sum()
        label = {
            'exact': '完全一致',
            'legal_prefix_only': '法人格の差のみ',
            'partial_overlap': '部分重複あり',
            'core_in_account': 'コア名がAccount名に含まれる',
            'substr_overlap': '3文字以上の部分一致',
            'completely_different': '完全に異なる'
        }[mt]
        print(f'  {label:<30} {cnt:>6,}件 ({cnt/len(merged)*100:.1f}%)')

    # ===================================
    # 検証C: 完全に異なるケースの詳細
    # ===================================
    completely_diff = merged[merged['match_type'] == 'completely_different']
    print(f'\n' + '=' * 60)
    print(f'検証C: 完全に異なるケースの詳細分析（{len(completely_diff):,}件）')
    print('=' * 60)

    # 複数事業所法人か単一事業所法人か
    diff_multi = completely_diff[completely_diff['corp_num_clean'].isin(multi_corp.index)]
    diff_single = completely_diff[~completely_diff['corp_num_clean'].isin(multi_corp.index)]
    print(f'  複数事業所を持つ法人の事業所: {len(diff_multi):,}件')
    print(f'  単一事業所法人: {len(diff_single):,}件')

    # 複数事業所の例
    print(f'\n  複数事業所法人の例（先頭10法人）:')
    shown = 0
    for corp_num in diff_multi['corp_num_clean'].unique()[:10]:
        corp_accounts = accounts[accounts['corp_num_clean'] == corp_num]
        nta_name = merged[merged['corp_num_clean'] == corp_num]['NTA_Name'].iloc[0]
        print(f'\n    法人: {nta_name}')
        print(f'    法人番号: {corp_num}')
        print(f'    事業所数: {len(corp_accounts)}')
        for _, acc in corp_accounts.head(5).iterrows():
            print(f'      - {acc["Name"]}')
        if len(corp_accounts) > 5:
            print(f'      ... 他{len(corp_accounts)-5}事業所')
        shown += 1

    # 単一事業所で完全不一致の例（これが最もリスクの高いケース）
    print(f'\n  単一事業所で名前完全不一致のサンプル（先頭15件）:')
    for _, row in diff_single.head(15).iterrows():
        print(f'    SF: {row["Name"][:35]:<35} | NTA: {row["NTA_Name"][:40]}')
        print(f'       法人番号: {row["corp_num_clean"]} | サービス: {row.get("ServiceType__c", "N/A")}')

    # ===================================
    # 検証D: LegalPersonality__c既存値ありAccountとの比較（精度のベンチマーク）
    # ===================================
    print(f'\n' + '=' * 60)
    print(f'検証D: 法人格既存Accountで推論の正確性をベンチマーク')
    print('=' * 60)

    # 法人格が既に入っているAccountから、法人番号で推論して一致するか
    query2 = """
    SELECT Id, Name, CorporateNumber__c, LegalPersonality__c
    FROM Account
    WHERE CorporateNumber__c != null
    AND LegalPersonality__c != null
    AND LegalPersonality__c != ''
    """
    print('  法人格既存Accountを取得中...')
    existing = svc.bulk_query(query2)
    print(f'  取得件数: {len(existing):,}件')

    if len(existing) > 0:
        # 法人番号正規化
        existing['corp_num_clean'] = existing['CorporateNumber__c'].apply(
            lambda x: str(x).replace('.0', '').strip().zfill(13) if pd.notna(x) else ''
        )

        # 国税庁CSVから法人名を取得して推論
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
        print(f'  ベンチマーク対象法人番号: {len(target_numbers):,}件')

        # 国税庁CSVからマッチ
        matched = []
        chunk_size = 100_000
        total_rows = 0

        print('  国税庁CSV読み込み中...')
        for chunk in pd.read_csv(
            nta_csv,
            header=None,
            names=NTA_COLUMNS,
            dtype=str,
            usecols=['corporate_number', 'name', 'latest'],
            chunksize=chunk_size,
            encoding='utf-8',
            on_bad_lines='skip'
        ):
            total_rows += len(chunk)
            chunk = chunk[chunk['latest'] == '1']
            hits = chunk[chunk['corporate_number'].isin(target_numbers)]
            if len(hits) > 0:
                matched.append(hits)
            if total_rows % 2_000_000 == 0:
                found = sum(len(m) for m in matched)
                print(f'    {total_rows:>10,}行処理... マッチ{found}件')

        if matched:
            nta_matched = pd.concat(matched, ignore_index=True)
        else:
            nta_matched = pd.DataFrame(columns=['corporate_number', 'name', 'latest'])

        print(f'  国税庁CSVマッチ: {len(nta_matched):,}件')

        # 法人格推論
        def extract_legal(name):
            if pd.isna(name) or not name:
                return None
            name = str(name)
            for pattern, label in LEGAL_PATTERNS:
                if re.search(pattern, name):
                    return label
            return None

        nta_matched['inferred'] = nta_matched['name'].apply(extract_legal)

        # 既存値とマージして比較
        bench = existing.merge(
            nta_matched[['corporate_number', 'name', 'inferred']],
            left_on='corp_num_clean',
            right_on='corporate_number',
            how='inner'
        )

        # 推論できたもの
        bench_inferred = bench[bench['inferred'].notna()]
        print(f'\n  ベンチマーク（推論成功分）: {len(bench_inferred):,}件')

        if len(bench_inferred) > 0:
            match_exact = bench_inferred[bench_inferred['LegalPersonality__c'] == bench_inferred['inferred']]
            print(f'  既存値と推論一致: {len(match_exact):,}件 ({len(match_exact)/len(bench_inferred)*100:.1f}%)')

            mismatch = bench_inferred[bench_inferred['LegalPersonality__c'] != bench_inferred['inferred']]
            print(f'  不一致: {len(mismatch):,}件 ({len(mismatch)/len(bench_inferred)*100:.1f}%)')

            if len(mismatch) > 0:
                print(f'\n  不一致サンプル（先頭20件）:')
                for _, row in mismatch.head(20).iterrows():
                    print(f'    既存: {row["LegalPersonality__c"]:<20} | '
                          f'推論: {row["inferred"]:<20} | '
                          f'NTA: {row["name"]}')

    print('\n完了')


if __name__ == '__main__':
    main()
