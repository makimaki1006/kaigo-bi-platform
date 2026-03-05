"""
DODA媒体データ準備スクリプト

Step 1: Salesforce認証
Step 2: 成約先電話番号抽出
Step 3: 既存Lead電話番号抽出
Step 4: 既存Account電話番号抽出
Step 5: 既存Contact電話番号抽出
Step 6: 媒体掲載中リスト読み込み
Step 7: サマリー出力とPickle保存
"""

import sys
import re
import pickle
from pathlib import Path
from typing import Set, Dict, List, Any
import pandas as pd

# UTF-8出力設定
sys.stdout.reconfigure(encoding='utf-8')

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / 'src'))

from src.api.salesforce_client import SalesforceClient


def normalize_phone(phone: str) -> str:
    """
    電話番号を正規化（ハイフン・スペース除去、0始まり10-11桁のみ）

    Args:
        phone: 電話番号文字列

    Returns:
        正規化された電話番号（不正な場合は空文字列）
    """
    if not phone or not isinstance(phone, str):
        return ''

    # ハイフン、スペース、括弧を除去
    cleaned = re.sub(r'[-\s()（）]', '', phone)

    # 数字のみ抽出
    digits = re.sub(r'\D', '', cleaned)

    # 0始まり10-11桁チェック
    if digits and digits[0] == '0' and len(digits) in [10, 11]:
        return digits

    return ''


def extract_phones_from_record(record: Dict[str, Any], phone_fields: List[str]) -> Set[str]:
    """
    レコードから複数の電話番号フィールドを抽出して正規化

    Args:
        record: Salesforceレコード
        phone_fields: 電話番号フィールド名リスト

    Returns:
        正規化された電話番号のセット
    """
    phones = set()
    for field in phone_fields:
        phone = record.get(field)
        if phone:
            normalized = normalize_phone(str(phone))
            if normalized:
                phones.add(normalized)
    return phones


def query_with_pagination(client: SalesforceClient, query: str) -> List[Dict[str, Any]]:
    """
    ページネーション付きSOQLクエリ実行

    Args:
        client: SalesforceClient
        query: SOQLクエリ

    Returns:
        全レコードのリスト
    """
    import requests

    all_records = []

    # 初回クエリ
    url = f"{client.instance_url}/services/data/{client.api_version}/query"
    headers = client._get_headers()
    params = {'q': query}

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    result = response.json()

    all_records.extend(result['records'])
    print(f"  取得: {len(result['records'])} 件")

    # ページネーション
    while not result['done']:
        next_url = f"{client.instance_url}{result['nextRecordsUrl']}"
        response = requests.get(next_url, headers=headers)
        response.raise_for_status()
        result = response.json()

        all_records.extend(result['records'])
        print(f"  追加取得: {len(result['records'])} 件（累計: {len(all_records)} 件）")

    return all_records


def main():
    print("=" * 80)
    print("DODA媒体データ準備スクリプト")
    print("=" * 80)

    # 出力ディレクトリ作成
    output_dir = Path('data/output/media_matching')
    output_dir.mkdir(parents=True, exist_ok=True)

    # Step 1: Salesforce認証
    print("\n[Step 1] Salesforce認証")
    print("-" * 80)
    client = SalesforceClient()
    client.authenticate()
    print("✅ 認証成功")

    # Step 2: 成約先電話番号抽出
    print("\n[Step 2] 成約先（契約アカウント）電話番号抽出")
    print("-" * 80)

    contract_query = """
    SELECT Id, Name, Phone, Phone2__c
    FROM Account
    WHERE Status__c LIKE '%商談中%'
       OR Status__c LIKE '%プロジェクト進行中%'
       OR Status__c LIKE '%深耕対象%'
       OR Status__c LIKE '%過去客%'
       OR RelatedAccountFlg__c = 'グループ案件進行中'
       OR RelatedAccountFlg__c = 'グループ過去案件実績あり'
    """

    contract_accounts = query_with_pagination(client, contract_query)
    print(f"成約先Account件数: {len(contract_accounts)} 件")

    contract_phones = set()
    for acc in contract_accounts:
        phones = extract_phones_from_record(acc, ['Phone', 'Phone2__c'])
        contract_phones.update(phones)

    print(f"成約先電話番号数: {len(contract_phones)} 件")

    # CSV保存
    contract_csv = Path('data/output/contract_accounts_20260203.csv')
    pd.DataFrame(contract_accounts).to_csv(contract_csv, index=False, encoding='utf-8-sig')
    print(f"✅ 成約先Account保存: {contract_csv}")

    # Step 3: 既存Lead電話番号抽出
    print("\n[Step 3] 既存Lead電話番号抽出")
    print("-" * 80)

    lead_query = """
    SELECT Id, Phone, MobilePhone, Phone2__c, MobilePhone2__c,
           Company, LastName, Status
    FROM Lead
    WHERE IsConverted = false
    """

    leads = query_with_pagination(client, lead_query)
    print(f"Lead件数: {len(leads)} 件")

    lead_phones = {}  # normalized_phone -> [lead_record, ...]
    for lead in leads:
        phones = extract_phones_from_record(
            lead,
            ['Phone', 'MobilePhone', 'Phone2__c', 'MobilePhone2__c']
        )
        for phone in phones:
            if phone not in lead_phones:
                lead_phones[phone] = []
            lead_phones[phone].append(lead)

    print(f"Lead電話番号数（ユニーク）: {len(lead_phones)} 件")

    # Step 4: 既存Account電話番号抽出
    print("\n[Step 4] 既存Account電話番号抽出")
    print("-" * 80)

    account_query = """
    SELECT Id, Name, Phone, Phone2__c
    FROM Account
    """

    accounts = query_with_pagination(client, account_query)
    print(f"Account件数: {len(accounts)} 件")

    account_phones = {}  # normalized_phone -> [account_record, ...]
    for acc in accounts:
        phones = extract_phones_from_record(acc, ['Phone', 'Phone2__c'])
        for phone in phones:
            if phone not in account_phones:
                account_phones[phone] = []
            account_phones[phone].append(acc)

    print(f"Account電話番号数（ユニーク）: {len(account_phones)} 件")

    # Step 5: 既存Contact電話番号抽出
    print("\n[Step 5] 既存Contact電話番号抽出")
    print("-" * 80)

    contact_query = """
    SELECT Id, Name, Phone, MobilePhone, Phone2__c, MobilePhone2__c, AccountId
    FROM Contact
    """

    contacts = query_with_pagination(client, contact_query)
    print(f"Contact件数: {len(contacts)} 件")

    contact_phones = {}  # normalized_phone -> [contact_record, ...]
    for contact in contacts:
        phones = extract_phones_from_record(
            contact,
            ['Phone', 'MobilePhone', 'Phone2__c', 'MobilePhone2__c']
        )
        for phone in phones:
            if phone not in contact_phones:
                contact_phones[phone] = []
            contact_phones[phone].append(contact)

    print(f"Contact電話番号数（ユニーク）: {len(contact_phones)} 件")

    # Step 6: 媒体掲載中リスト（電話済みリスト）読み込み
    print("\n[Step 6] 媒体掲載中リスト（電話済みリスト）読み込み")
    print("-" * 80)

    called_list_path = Path(r'C:\Users\fuji1\Downloads\媒体掲載中のリスト.xlsx')

    called_phones = set()

    if called_list_path.exists():
        try:
            # 全シート読み込み
            xlsx = pd.ExcelFile(called_list_path)
            print(f"シート数: {len(xlsx.sheet_names)}")

            for sheet_name in xlsx.sheet_names:
                print(f"  処理中: {sheet_name}")
                df = pd.read_excel(xlsx, sheet_name=sheet_name, dtype=str)

                # 全列から電話番号を抽出
                for col in df.columns:
                    for value in df[col].dropna():
                        phone = normalize_phone(str(value))
                        if phone:
                            called_phones.add(phone)

            print(f"✅ 電話済み電話番号数: {len(called_phones)} 件")

        except Exception as e:
            print(f"⚠️ 警告: 媒体掲載中リスト読み込みエラー: {e}")
            print("  電話済みリストなしで続行します")

    else:
        print(f"⚠️ 警告: 媒体掲載中リストが見つかりません: {called_list_path}")
        print("  電話済みリストなしで続行します")

    # Step 7: サマリー出力とPickle保存
    print("\n[Step 7] サマリー出力")
    print("=" * 80)
    print(f"成約先Account件数:        {len(contract_accounts):,} 件")
    print(f"成約先電話番号数:         {len(contract_phones):,} 件")
    print(f"Lead件数:                 {len(leads):,} 件")
    print(f"Lead電話番号数:           {len(lead_phones):,} 件")
    print(f"Account件数:              {len(accounts):,} 件")
    print(f"Account電話番号数:        {len(account_phones):,} 件")
    print(f"Contact件数:              {len(contacts):,} 件")
    print(f"Contact電話番号数:        {len(contact_phones):,} 件")
    print(f"電話済み電話番号数:       {len(called_phones):,} 件")
    print("=" * 80)

    # Pickle保存
    print("\n[Pickle保存]")
    print("-" * 80)

    pickle_files = {
        'contract_phones.pkl': contract_phones,
        'called_phones.pkl': called_phones,
        'lead_phones.pkl': lead_phones,
        'account_phones.pkl': account_phones,
        'contact_phones.pkl': contact_phones
    }

    for filename, data in pickle_files.items():
        pkl_path = output_dir / filename
        with open(pkl_path, 'wb') as f:
            pickle.dump(data, f)
        print(f"✅ 保存: {pkl_path}")

    print("\n" + "=" * 80)
    print("データ準備完了")
    print("=" * 80)


if __name__ == '__main__':
    main()
