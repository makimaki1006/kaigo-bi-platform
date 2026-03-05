"""
法人番号→国税庁全件CSVから法人格を推論し、Salesforce更新用CSVを生成する。

対象: 介護×法人格不明のAccount（LegalPersonality__cが空）
データソース: 国税庁法人番号公表サイト全件CSV
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

# ── 国税庁CSVのカラム定義（ヘッダーなし） ──
# 参考: https://www.houjin-bangou.nta.go.jp/download/zenken/
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

# ── 法人格抽出パターン ──
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
    """法人名から法人格を抽出"""
    if pd.isna(name) or not name:
        return None
    name = str(name)
    for pattern, label in LEGAL_PATTERNS:
        if re.search(pattern, name):
            return label
    return None


def main():
    nta_csv = Path(r'C:\Users\fuji1\Downloads\00_zenkoku_all_20260227\00_zenkoku_all_20260227.csv')
    output_dir = project_root / 'data' / 'output' / 'legal_inference'
    output_dir.mkdir(parents=True, exist_ok=True)

    # ===================================
    # Step 1: Salesforceから法人格不明Accountを取得
    # ===================================
    print('Step 1: Salesforceから法人格不明のAccountを取得...')
    svc = OpportunityService()
    svc.authenticate()

    query = """
    SELECT Id, Name, CorporateNumber__c, LegalPersonality__c,
        ServiceType__c, NumberOfEmployees
    FROM Account
    WHERE CorporateNumber__c != null
    AND (LegalPersonality__c = null OR LegalPersonality__c = '')
    """
    print('  Account取得中...')
    accounts = svc.bulk_query(query)
    print(f'  法人番号あり×法人格不明のAccount: {len(accounts):,}件')

    if len(accounts) == 0:
        print('対象Accountがありません。終了。')
        return

    # 法人番号を正規化（.0除去、13桁ゼロ埋め）
    accounts['corp_num_clean'] = accounts['CorporateNumber__c'].apply(
        lambda x: str(x).replace('.0', '').strip() if pd.notna(x) else ''
    )
    # 13桁に満たない場合はゼロ埋め
    accounts['corp_num_clean'] = accounts['corp_num_clean'].apply(
        lambda x: x.zfill(13) if len(x) > 0 and len(x) < 13 else x
    )

    target_numbers = set(accounts['corp_num_clean'].unique())
    print(f'  ユニーク法人番号: {len(target_numbers):,}件')

    # ===================================
    # Step 2: 国税庁CSVから対象法人番号を検索
    # ===================================
    print(f'\nStep 2: 国税庁CSV読み込み中... ({nta_csv.stat().st_size / 1e9:.1f}GB)')
    print('  （1.2GBファイルのため数十秒かかります）')

    # 必要カラムのみ読み込み（法人番号=1, 法人名=6, 最新フラグ=23）
    # chunkで読み込んでメモリ節約
    matched = []
    chunk_size = 100_000
    total_rows = 0

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
        # 最新データのみ（latest=1）
        chunk = chunk[chunk['latest'] == '1']
        # 対象法人番号にマッチするものを抽出
        hits = chunk[chunk['corporate_number'].isin(target_numbers)]
        if len(hits) > 0:
            matched.append(hits)

        if total_rows % 1_000_000 == 0:
            found = sum(len(m) for m in matched)
            print(f'  {total_rows:>10,}行処理... マッチ{found}件')

    if matched:
        nta_matched = pd.concat(matched, ignore_index=True)
    else:
        nta_matched = pd.DataFrame(columns=['corporate_number', 'name', 'latest'])

    print(f'  全{total_rows:,}行処理完了')
    print(f'  マッチ: {len(nta_matched):,}件 / {len(target_numbers):,}件')

    # ===================================
    # Step 3: 法人名から法人格を抽出
    # ===================================
    print('\nStep 3: 法人名から法人格を抽出...')
    nta_matched['inferred_legal'] = nta_matched['name'].apply(extract_legal)

    # 結果サマリー
    inferred_counts = nta_matched['inferred_legal'].value_counts(dropna=False)
    print('\n  推論結果:')
    for val, cnt in inferred_counts.items():
        label = val if val else '推論不可'
        print(f'    {label:<30} {cnt:>5}件')

    success = nta_matched['inferred_legal'].notna().sum()
    print(f'\n  推論成功: {success:,}件 / {len(nta_matched):,}件 ({success/len(nta_matched)*100:.1f}%)')

    # ===================================
    # Step 4: Accountとマージして更新用CSV生成
    # ===================================
    print('\nStep 4: 更新用CSV生成...')

    # マージ
    merged = accounts.merge(
        nta_matched[['corporate_number', 'name', 'inferred_legal']],
        left_on='corp_num_clean',
        right_on='corporate_number',
        how='left'
    )

    # 法人格が推論できたもの
    has_legal = merged[merged['inferred_legal'].notna()].copy()
    print(f'  法人格推論成功Account: {len(has_legal):,}件')

    # Salesforce更新用CSV（Id, LegalPersonality__c）
    update_df = has_legal[['Id', 'inferred_legal', 'name']].copy()
    update_df.columns = ['Id', 'LegalPersonality__c', 'NTA_Name']

    # 重複除去（同じAccountが複数回出る場合）
    update_df = update_df.drop_duplicates(subset='Id')
    print(f'  更新対象（重複除去後）: {len(update_df):,}件')

    # 法人格別内訳
    print('\n  法人格別内訳:')
    for val, cnt in update_df['LegalPersonality__c'].value_counts().items():
        print(f'    {val:<30} {cnt:>5}件')

    # CSV出力
    output_path = output_dir / 'account_legal_update.csv'
    update_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f'\n  更新用CSV出力: {output_path}')

    # サンプル表示
    print('\n  サンプル（先頭10件）:')
    for _, row in update_df.head(10).iterrows():
        print(f'    {row["Id"]} | {row["LegalPersonality__c"]} | {row["NTA_Name"]}')

    # 推論不可のサンプル
    no_legal = merged[merged['inferred_legal'].isna() & merged['corporate_number'].notna()]
    if len(no_legal) > 0:
        print(f'\n  法人番号ありだが推論不可: {len(no_legal)}件')
        print('  サンプル:')
        for _, row in no_legal.head(10).iterrows():
            print(f'    {row["corp_num_clean"]} | {row.get("name", "N/A")} | {row["Name"]}')

    # マッチしなかった法人番号
    no_match = merged[merged['corporate_number'].isna()]
    print(f'\n  国税庁CSVにマッチしなかった: {len(no_match)}件')

    print('\n完了')


if __name__ == '__main__':
    main()
