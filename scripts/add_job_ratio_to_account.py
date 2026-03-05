"""
Account.Description に有効求人倍率を追記するスクリプト
MECEマッピングデータを使用して、住所から市区町村を特定し求人倍率を付与
"""

import io
import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

# Windowsコンソールのエンコーディング対応
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.api.salesforce_client import SalesforceClient


def extract_municipality_from_address(address: str, prefecture: str = None) -> tuple:
    """
    住所文字列から都道府県と市区町村を抽出

    Args:
        address: 住所文字列
        prefecture: 都道府県（既知の場合）

    Returns:
        tuple: (都道府県, 市区町村)
    """
    if not address or pd.isna(address):
        return (prefecture, None)

    address = str(address).strip()

    # 都道府県抽出
    pref_pattern = r'^(北海道|東京都|大阪府|京都府|.+?県)'
    pref_match = re.match(pref_pattern, address)

    if pref_match:
        extracted_pref = pref_match.group(1)
        remaining = address[len(extracted_pref):]
    else:
        extracted_pref = prefecture
        remaining = address

    if not remaining:
        return (extracted_pref, None)

    # 市区町村抽出（政令市の区も含む）
    # 優先順位: 〇〇市〇〇区 > 〇〇市 > 〇〇区 > 〇〇郡〇〇町 > 〇〇町 > 〇〇村

    # 政令指定都市の区パターン（例: 横浜市中区）
    seirei_pattern = r'^(.+?市)(.+?区)'
    seirei_match = re.match(seirei_pattern, remaining)
    if seirei_match:
        city = seirei_match.group(1)
        ward = seirei_match.group(2)
        return (extracted_pref, f"{city}{ward}")

    # 市パターン
    city_pattern = r'^(.+?市)'
    city_match = re.match(city_pattern, remaining)
    if city_match:
        return (extracted_pref, city_match.group(1))

    # 東京23区パターン
    ku_pattern = r'^(.+?区)'
    ku_match = re.match(ku_pattern, remaining)
    if ku_match:
        return (extracted_pref, ku_match.group(1))

    # 郡+町村パターン
    gun_pattern = r'^(.+?郡)(.+?[町村])'
    gun_match = re.match(gun_pattern, remaining)
    if gun_match:
        return (extracted_pref, gun_match.group(2))

    # 町村パターン
    town_pattern = r'^(.+?[町村])'
    town_match = re.match(town_pattern, remaining)
    if town_match:
        return (extracted_pref, town_match.group(1))

    return (extracted_pref, None)


def load_mece_mapping() -> pd.DataFrame:
    """MECEマッピングデータを読み込み"""
    mapping_path = project_root / 'data' / 'job_openings_ratio' / 'complete_mece_municipality_hellowork_mapping.csv'

    df = pd.read_csv(mapping_path, encoding='utf-8-sig')
    print(f"✅ MECEマッピング読み込み: {len(df)}件")
    print(f"   求人倍率あり: {df['ratio'].notna().sum()}件")

    return df


def export_account_data(client: SalesforceClient) -> pd.DataFrame:
    """
    Accountデータをエクスポート（REST API使用 - ページネーション対応）
    必要フィールド: Id, Name, Description, Address__c, Prefectures__c, BillingState, BillingCity
    """
    print("📥 Accountデータをエクスポート中...")

    import requests

    # SOQL作成
    fields = [
        'Id', 'Name', 'Description',
        'Address__c', 'Prefectures__c',
        'BillingState', 'BillingCity', 'BillingStreet'
    ]
    soql = f"SELECT {','.join(fields)} FROM Account"

    # 全レコード取得（ページネーション対応）
    all_records = []
    url = f"{client.instance_url}/services/data/{client.api_version}/query?q={soql}"

    while url:
        headers = client._get_headers()
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()

        records = data.get('records', [])
        all_records.extend(records)

        # 次のページ
        next_url = data.get('nextRecordsUrl')
        if next_url:
            url = f"{client.instance_url}{next_url}"
            print(f"  取得中: {len(all_records)}件...")
        else:
            url = None

    # DataFrameに変換
    df = pd.DataFrame(all_records)
    # attributes列を削除
    if 'attributes' in df.columns:
        df = df.drop(columns=['attributes'])

    print(f"✅ Accountデータ取得: {len(df)}件")

    return df


def match_and_add_ratio(account_df: pd.DataFrame, mapping_df: pd.DataFrame) -> pd.DataFrame:
    """
    Accountの住所からMECEマッピングと突合し、求人倍率を追加
    """
    print("\n🔍 住所から市区町村を特定してマッチング中...")

    # マッピング用辞書作成（都道府県+市区町村 → ratio, hellowork_name）
    mapping_dict = {}
    for _, row in mapping_df.iterrows():
        key = (row['prefecture'], row['municipality'])
        mapping_dict[key] = {
            'ratio': row['ratio'],
            'hellowork_name': row['hellowork_name']
        }

    # 政令指定都市の区から市へのフォールバック用
    # 例: 横浜市中区 → 横浜市
    seirei_cities = [
        '札幌市', '仙台市', '新潟市', 'さいたま市', '千葉市', '横浜市', '川崎市', '相模原市',
        '静岡市', '浜松市', '名古屋市', '京都市', '大阪市', '堺市', '神戸市',
        '岡山市', '広島市', '北九州市', '福岡市', '熊本市'
    ]

    results = []
    match_count = 0
    no_match_count = 0
    no_address_count = 0

    for idx, row in account_df.iterrows():
        account_id = row['Id']
        name = row['Name']
        description = row.get('Description', '')

        # 住所から都道府県・市区町村を抽出
        # 優先順位: Address__c > BillingStreet
        address = row.get('Address__c') or row.get('BillingStreet') or ''
        prefecture = row.get('Prefectures__c') or row.get('BillingState') or ''

        extracted_pref, municipality = extract_municipality_from_address(address, prefecture)

        if not municipality:
            no_address_count += 1
            continue

        # マッチング
        ratio_info = None

        # 完全一致
        key = (extracted_pref, municipality)
        if key in mapping_dict:
            ratio_info = mapping_dict[key]
        else:
            # 政令指定都市の区の場合、市でマッチング試行
            for seirei in seirei_cities:
                if municipality.startswith(seirei):
                    city_key = (extracted_pref, seirei)
                    if city_key in mapping_dict:
                        ratio_info = mapping_dict[city_key]
                        break

        if ratio_info and pd.notna(ratio_info['ratio']):
            match_count += 1
            results.append({
                'Id': account_id,
                'Name': name,
                'Description': description,
                'prefecture': extracted_pref,
                'municipality': municipality,
                'ratio': ratio_info['ratio'],
                'hellowork_name': ratio_info['hellowork_name']
            })
        else:
            no_match_count += 1

    print(f"✅ マッチング完了:")
    print(f"   マッチ成功: {match_count}件")
    print(f"   マッチ失敗（求人倍率なし）: {no_match_count}件")
    print(f"   住所なし: {no_address_count}件")

    return pd.DataFrame(results)


def generate_update_csv(matched_df: pd.DataFrame, output_dir: Path) -> Path:
    """
    Salesforce更新用CSVを生成
    Descriptionに求人倍率情報を追記
    """
    print("\n📝 更新用CSV生成中...")

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    updates = []
    for _, row in matched_df.iterrows():
        account_id = row['Id']
        current_desc = row['Description'] if pd.notna(row['Description']) else ''
        ratio = row['ratio']
        hellowork = row['hellowork_name']

        # 既に求人倍率情報がある場合はスキップ
        if '有効求人倍率' in str(current_desc):
            continue

        # 追記テキスト
        ratio_text = f"\n\n【有効求人倍率】{ratio}倍（{hellowork}ハローワーク管轄）"
        new_desc = current_desc + ratio_text

        updates.append({
            'Id': account_id,
            'Description': new_desc
        })

    if not updates:
        print("⚠️ 更新対象なし（全て既に求人倍率情報あり）")
        return None

    update_df = pd.DataFrame(updates)

    # CSV保存
    output_path = output_dir / f'account_job_ratio_updates_{timestamp}.csv'
    update_df.to_csv(output_path, index=False, encoding='utf-8-sig')

    print(f"✅ 更新用CSV保存: {output_path}")
    print(f"   更新対象: {len(update_df)}件")

    return output_path


def main():
    """メイン処理"""
    print("=" * 60)
    print("Account.Description に有効求人倍率を追記")
    print("=" * 60)

    # 出力ディレクトリ
    output_dir = project_root / 'data' / 'output' / 'job_ratio'
    output_dir.mkdir(parents=True, exist_ok=True)

    # 1. Salesforce認証
    client = SalesforceClient()
    client.authenticate()

    # 2. MECEマッピング読み込み
    mapping_df = load_mece_mapping()

    # 3. Accountデータエクスポート
    account_df = export_account_data(client)

    # 4. マッチングと求人倍率付与
    matched_df = match_and_add_ratio(account_df, mapping_df)

    if matched_df.empty:
        print("\n⚠️ マッチするAccountがありません")
        return

    # 中間ファイル保存（デバッグ用）
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    matched_path = output_dir / f'account_matched_{timestamp}.csv'
    matched_df.to_csv(matched_path, index=False, encoding='utf-8-sig')
    print(f"\n📊 マッチ結果保存: {matched_path}")

    # 5. 更新用CSV生成
    update_path = generate_update_csv(matched_df, output_dir)

    if update_path:
        print("\n" + "=" * 60)
        print("📋 サマリー")
        print("=" * 60)
        print(f"更新対象件数: {len(pd.read_csv(update_path))}件")
        print(f"更新用CSV: {update_path}")
        print("\n⚠️ Salesforceへのインポートはユーザー確認後に実行してください")


if __name__ == '__main__':
    main()
