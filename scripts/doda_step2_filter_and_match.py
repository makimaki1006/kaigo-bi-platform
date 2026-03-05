"""
dodaデータ処理パイプライン - フィルタ・重複排除・突合

処理フロー:
1. 施工管理・設備系職種でフィルタ
2. 電話番号抽出・正規化・重複排除
3. 成約先除外
4. 架電済み除外
5. Salesforce突合（Lead/Account/Contact）
6. 結果サマリー出力
7. 中間結果をpickle保存
"""

import sys
import pandas as pd
import re
import pickle
from pathlib import Path
from typing import Dict, List, Set, Any
from collections import defaultdict

# UTF-8出力設定
sys.stdout.reconfigure(encoding='utf-8')

# パス設定
BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_FILE = Path(r"C:\Users\fuji1\Downloads\doda_google_search_urls.csv")
OUTPUT_DIR = BASE_DIR / "data" / "output" / "media_matching"

# 職種フィルタキーワード
JOB_KEYWORDS = [
    '施工管理', '施工', '工事', '設備', 'メンテナンス', '保全',
    '管工事', '電気工事', '建築', '建設', '土木', '現場', '監督',
    '配管', '空調', '給排水', '防水', '塗装', '解体', '左官',
    '鳶', '足場', '内装', '外装', '改修', 'リフォーム',
    '住宅', '不動産', 'ビル管理', 'マンション管理',
    '設計', 'CAD', '測量', '積算',
    '作業', 'スタッフ', '製造', 'ライン', '整備', '点検',
    '清掃', '警備', 'ドライバー', '運転', '配送', '物流', '倉庫'
]


def normalize_phone(phone: str) -> str:
    """電話番号正規化"""
    if not phone:
        return ''
    # 数字のみ抽出
    digits = re.sub(r'[^\d]', '', str(phone))
    # 10-11桁、0始まりのみ有効
    if len(digits) in [10, 11] and digits.startswith('0'):
        return digits
    return ''


def extract_phones_from_row(row: pd.Series) -> List[str]:
    """行から電話番号を抽出（VwiC3b列 = index 30以降）"""
    phones = []
    # index 30以降の全列を対象
    for idx in range(30, len(row)):
        text = str(row.iloc[idx])
        if pd.isna(row.iloc[idx]) or text == 'nan':
            continue

        # 正規表現: 0\d{1,4}-\d{1,4}-\d{3,4} または 0\d{9,10}
        matches = re.findall(r'0\d{1,4}[-\s]?\d{1,4}[-\s]?\d{3,4}|0\d{9,10}', text)
        for match in matches:
            normalized = normalize_phone(match)
            if normalized and normalized not in phones:
                phones.append(normalized)

    return phones


def filter_by_job_keywords(df: pd.DataFrame) -> pd.DataFrame:
    """職種キーワードでフィルタ（col index 8）"""
    print("=" * 60)
    print("STEP 1: 職種フィルタ")
    print("=" * 60)

    # col index 8 = 職種名
    job_col = df.iloc[:, 8]

    # キーワード一致チェック
    mask = job_col.apply(
        lambda x: any(keyword in str(x) for keyword in JOB_KEYWORDS)
        if pd.notna(x) else False
    )

    filtered_df = df[mask].copy()

    print(f"総行数: {len(df):,}")
    print(f"フィルタ後: {len(filtered_df):,}")
    print(f"除外: {len(df) - len(filtered_df):,}")
    print()
    print("サンプル職種名（20件）:")
    print("-" * 60)
    for i, job_title in enumerate(filtered_df.iloc[:, 8].head(20), 1):
        print(f"{i:2}. {job_title}")
    print()

    return filtered_df


def deduplicate_by_phone(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """電話番号で重複排除 + データ抽出"""
    print("=" * 60)
    print("STEP 2: 電話番号抽出・重複排除")
    print("=" * 60)

    phone_to_record = {}  # 最初のレコードを保持
    total_phones_extracted = 0
    rows_with_phones = 0

    for idx, row in df.iterrows():
        phones = extract_phones_from_row(row)

        if not phones:
            continue

        rows_with_phones += 1
        total_phones_extracted += len(phones)

        # 最初の正規化電話番号をキーとして重複排除
        primary_phone = phones[0]

        if primary_phone not in phone_to_record:
            phone_to_record[primary_phone] = {
                'company': str(row.iloc[7]) if pd.notna(row.iloc[7]) else '',
                'job_title': str(row.iloc[8]) if pd.notna(row.iloc[8]) else '',
                'doda_url': str(row.iloc[0]) if pd.notna(row.iloc[0]) else '',
                'phones': phones,
                'snippet': ' | '.join([str(row.iloc[i]) for i in range(30, len(row)) if pd.notna(row.iloc[i])])[:200]
            }

    records = list(phone_to_record.values())

    print(f"電話番号抽出行: {rows_with_phones:,}")
    print(f"抽出電話番号総数: {total_phones_extracted:,}")
    print(f"重複排除後レコード数: {len(records):,}")
    print()
    print("サンプルレコード（5件）:")
    print("-" * 60)
    for i, rec in enumerate(records[:5], 1):
        print(f"{i}. {rec['company']}")
        print(f"   職種: {rec['job_title']}")
        print(f"   電話: {', '.join(rec['phones'])}")
        print(f"   URL: {rec['doda_url'][:80]}...")
        print()

    return records


def exclude_contract_accounts(records: List[Dict[str, Any]]) -> tuple:
    """成約先除外"""
    print("=" * 60)
    print("STEP 3: 成約先除外")
    print("=" * 60)

    contract_phones_path = OUTPUT_DIR / "contract_phones.pkl"

    if not contract_phones_path.exists():
        print(f"⚠️ {contract_phones_path} が見つかりません（スキップ）")
        return records, []

    with open(contract_phones_path, 'rb') as f:
        contract_phones: Set[str] = pickle.load(f)

    print(f"成約先電話番号数: {len(contract_phones):,}")

    excluded = []
    remaining = []

    for rec in records:
        if any(phone in contract_phones for phone in rec['phones']):
            excluded.append(rec)
        else:
            remaining.append(rec)

    print(f"除外: {len(excluded):,}")
    print(f"残存: {len(remaining):,}")
    print()

    return remaining, excluded


def exclude_called_list(records: List[Dict[str, Any]]) -> tuple:
    """架電済み除外"""
    print("=" * 60)
    print("STEP 4: 架電済み除外")
    print("=" * 60)

    called_phones_path = OUTPUT_DIR / "called_phones.pkl"

    if not called_phones_path.exists():
        print(f"⚠️ {called_phones_path} が見つかりません（スキップ）")
        return records, []

    with open(called_phones_path, 'rb') as f:
        called_phones: Set[str] = pickle.load(f)

    print(f"架電済み電話番号数: {len(called_phones):,}")

    excluded = []
    remaining = []

    for rec in records:
        if any(phone in called_phones for phone in rec['phones']):
            excluded.append(rec)
        else:
            remaining.append(rec)

    print(f"除外: {len(excluded):,}")
    print(f"残存: {len(remaining):,}")
    print()

    return remaining, excluded


def match_salesforce(records: List[Dict[str, Any]]) -> Dict[str, List]:
    """Salesforce突合"""
    print("=" * 60)
    print("STEP 5: Salesforce突合")
    print("=" * 60)

    # pickle読み込み
    lead_phones_path = OUTPUT_DIR / "lead_phones.pkl"
    account_phones_path = OUTPUT_DIR / "account_phones.pkl"
    contact_phones_path = OUTPUT_DIR / "contact_phones.pkl"

    lead_phones = {}
    account_phones = {}
    contact_phones = {}

    if lead_phones_path.exists():
        with open(lead_phones_path, 'rb') as f:
            lead_phones = pickle.load(f)
        print(f"✓ Lead電話番号: {len(lead_phones):,}")
    else:
        print(f"⚠️ {lead_phones_path} が見つかりません")

    if account_phones_path.exists():
        with open(account_phones_path, 'rb') as f:
            account_phones = pickle.load(f)
        print(f"✓ Account電話番号: {len(account_phones):,}")
    else:
        print(f"⚠️ {account_phones_path} が見つかりません")

    if contact_phones_path.exists():
        with open(contact_phones_path, 'rb') as f:
            contact_phones = pickle.load(f)
        print(f"✓ Contact電話番号: {len(contact_phones):,}")
    else:
        print(f"⚠️ {contact_phones_path} が見つかりません")

    print()

    # 突合処理
    new_leads = []
    lead_updates = []
    account_updates = []

    for rec in records:
        matched = False

        # Lead突合
        for phone in rec['phones']:
            if phone in lead_phones:
                lead_records = lead_phones[phone]
                rec['matched_leads'] = lead_records
                rec['matched_phone'] = phone
                lead_updates.append(rec)
                matched = True
                break

        if matched:
            continue

        # Account突合
        for phone in rec['phones']:
            if phone in account_phones:
                account_records = account_phones[phone]
                rec['matched_accounts'] = account_records
                rec['matched_phone'] = phone
                account_updates.append(rec)
                matched = True
                break

        if matched:
            continue

        # Contact突合（Account更新と同じ扱い）
        for phone in rec['phones']:
            if phone in contact_phones:
                contact_records = contact_phones[phone]
                rec['matched_contacts'] = contact_records
                rec['matched_phone'] = phone
                account_updates.append(rec)
                matched = True
                break

        if not matched:
            new_leads.append(rec)

    print(f"Lead更新対象: {len(lead_updates):,}")
    print(f"Account更新対象: {len(account_updates):,}")
    print(f"新規Lead候補: {len(new_leads):,}")
    print()

    return {
        'new_leads': new_leads,
        'lead_updates': lead_updates,
        'account_updates': account_updates
    }


def print_summary(results: Dict[str, Any], stats: Dict[str, int]):
    """結果サマリー出力"""
    print("=" * 60)
    print("STEP 6: 結果サマリー")
    print("=" * 60)

    print("\n【処理統計】")
    print("-" * 60)
    print(f"{'カテゴリ':<30} {'件数':>10}")
    print("-" * 60)
    print(f"{'総フィルタ行数':<30} {stats['total_filtered']:>10,}")
    print(f"{'電話番号重複排除後':<30} {stats['after_dedup']:>10,}")
    print(f"{'除外（成約先）':<30} {stats['excluded_contract']:>10,}")
    print(f"{'除外（架電済み）':<30} {stats['excluded_called']:>10,}")
    print(f"{'Lead更新対象':<30} {len(results['lead_updates']):>10,}")
    print(f"{'Account更新対象':<30} {len(results['account_updates']):>10,}")
    print(f"{'新規Lead候補':<30} {len(results['new_leads']):>10,}")
    print("-" * 60)

    # サンプル表示
    print("\n【サンプルレコード（各5件）】")
    print("-" * 60)

    if results['new_leads']:
        print("\n■ 新規Lead候補:")
        for i, rec in enumerate(results['new_leads'][:5], 1):
            print(f"{i}. {rec['company']} | {rec['job_title']}")
            print(f"   電話: {', '.join(rec['phones'][:3])}")

    if results['lead_updates']:
        print("\n■ Lead更新対象:")
        for i, rec in enumerate(results['lead_updates'][:5], 1):
            print(f"{i}. {rec['company']} | {rec['job_title']}")
            print(f"   電話: {rec['matched_phone']}")
            if 'matched_leads' in rec and rec['matched_leads']:
                lead = rec['matched_leads'][0]
                print(f"   マッチ: {lead.get('Company', '')} (ID: {lead.get('Id', '')})")

    if results['account_updates']:
        print("\n■ Account更新対象:")
        for i, rec in enumerate(results['account_updates'][:5], 1):
            print(f"{i}. {rec['company']} | {rec['job_title']}")
            print(f"   電話: {rec['matched_phone']}")
            if 'matched_accounts' in rec and rec['matched_accounts']:
                acc = rec['matched_accounts'][0]
                print(f"   マッチ: {acc.get('Name', '')} (ID: {acc.get('Id', '')})")

    print()


def save_results(results: Dict[str, Any], excluded_contract: List, excluded_called: List):
    """結果をpickle保存"""
    print("=" * 60)
    print("STEP 7: 結果保存")
    print("=" * 60)

    output_file = OUTPUT_DIR / "doda_results.pkl"
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    save_data = {
        'new_leads': results['new_leads'],
        'lead_updates': results['lead_updates'],
        'account_updates': results['account_updates'],
        'excluded_contract': excluded_contract,
        'excluded_called': excluded_called
    }

    with open(output_file, 'wb') as f:
        pickle.dump(save_data, f)

    print(f"✓ 保存完了: {output_file}")
    print(f"  - 新規Lead: {len(results['new_leads']):,}件")
    print(f"  - Lead更新: {len(results['lead_updates']):,}件")
    print(f"  - Account更新: {len(results['account_updates']):,}件")
    print(f"  - 成約先除外: {len(excluded_contract):,}件")
    print(f"  - 架電済み除外: {len(excluded_called):,}件")
    print()


def main():
    print("=" * 60)
    print("dodaデータ処理パイプライン")
    print("=" * 60)
    print()

    # ファイル存在チェック
    if not INPUT_FILE.exists():
        print(f"❌ エラー: ファイルが見つかりません: {INPUT_FILE}")
        return

    # STEP 1: CSVロード + 職種フィルタ
    df = pd.read_csv(INPUT_FILE, encoding='utf-8-sig', dtype=str)
    filtered_df = filter_by_job_keywords(df)

    # STEP 2: 電話番号抽出・重複排除
    records = deduplicate_by_phone(filtered_df)

    stats = {
        'total_filtered': len(filtered_df),
        'after_dedup': len(records)
    }

    # STEP 3: 成約先除外
    records, excluded_contract = exclude_contract_accounts(records)
    stats['excluded_contract'] = len(excluded_contract)

    # STEP 4: 架電済み除外
    records, excluded_called = exclude_called_list(records)
    stats['excluded_called'] = len(excluded_called)

    # STEP 5: Salesforce突合
    results = match_salesforce(records)

    # STEP 6: サマリー出力
    print_summary(results, stats)

    # STEP 7: pickle保存
    save_results(results, excluded_contract, excluded_called)

    print("=" * 60)
    print("処理完了")
    print("=" * 60)


if __name__ == '__main__':
    main()
