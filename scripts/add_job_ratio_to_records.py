#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Lead/Accountにハローワーク別有効求人倍率データを付与するスクリプト

MECEマッピングテーブルを使用して、全市区町村を漏れなくカバー

使用方法:
    python scripts/add_job_ratio_to_records.py [--object Lead|Account] [--dry-run]

フィールドマッピング:
    - JobOpeningsRatio__c: 有効求人倍率
    - HelloWorkName__c: 管轄ハローワーク名
    - JobRatioDate__c: データ時点（年月）
"""

import argparse
import csv
import re
import sys
from datetime import datetime
from pathlib import Path
import pandas as pd

# プロジェクトルートをパスに追加
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.api.salesforce_client import SalesforceClient


def load_mece_mapping(data_dir: Path) -> dict:
    """
    MECEマッピングテーブルを読み込み

    Returns:
        dict: {(都道府県名, 市区町村名): {ratio, hellowork_name, year, month}}
    """
    mapping_file = data_dir / "complete_mece_municipality_hellowork_mapping.csv"

    if not mapping_file.exists():
        print(f"エラー: MECEマッピングファイルが見つかりません: {mapping_file}")
        print("  先に build_complete_mece_mapping.py を実行してください")
        return {}

    df = pd.read_csv(mapping_file, encoding='utf-8-sig')
    print(f"MECEマッピング読み込み: {len(df)}件")

    mapping = {}
    for _, row in df.iterrows():
        pref = row['prefecture']
        muni = row['municipality']
        key = (pref, muni)

        mapping[key] = {
            'ratio': row['ratio'] if pd.notna(row['ratio']) else None,
            'hellowork_name': row['hellowork_name'],
            'year': int(row['year']) if pd.notna(row['year']) else None,
            'month': int(row['month']) if pd.notna(row['month']) else None,
            'match_type': row['match_type'],
        }

        # 市区町村名のみでも検索できるようにする（都道府県なしのケース用）
        if muni not in mapping:
            mapping[muni] = mapping[key]

    # 政令指定都市の区名のみでも検索できるようにする
    # 例: "中央区" → 最も人口が多い都市の値を使用（フォールバック）
    ward_fallbacks = {}
    for key, data in mapping.items():
        if isinstance(key, tuple) and len(key) == 2:
            pref, muni = key
            if isinstance(pref, str) and isinstance(muni, str):
                # 政令指定都市の区を抽出（例: 札幌市中央区 → 中央区）
                ward_match = re.match(r'.+市(.+区)$', muni)
                if ward_match:
                    ward_name = ward_match.group(1)
                    if ward_name not in ward_fallbacks:
                        ward_fallbacks[ward_name] = data

    mapping.update(ward_fallbacks)

    return mapping


def extract_prefecture(address: str) -> str:
    """住所から都道府県名を抽出"""
    if not address:
        return ''

    patterns = [
        r'^(北海道)',
        r'^(東京都)',
        r'^(京都府|大阪府)',
        r'^(.{2,3}県)',
    ]

    for pattern in patterns:
        match = re.match(pattern, address)
        if match:
            return match.group(1)

    return ''


def extract_municipality_from_address(address: str) -> tuple[str, str]:
    """
    住所文字列から都道府県と市区町村名を抽出

    Returns:
        tuple: (都道府県名, 市区町村名)
    """
    if not address:
        return '', ''

    address = address.strip()

    # 都道府県を抽出
    prefecture = extract_prefecture(address)
    if prefecture:
        address = address[len(prefecture):]

    # パターン1: 政令指定都市の区（〇〇市〇〇区）
    match = re.match(r'^(.+市.+区)', address)
    if match:
        return prefecture, match.group(1)

    # パターン2: 東京23区
    if prefecture == "東京都":
        match = re.match(r'^(.+区)', address)
        if match:
            return prefecture, match.group(1)

    # パターン3: 通常の市
    match = re.match(r'^(.+市)', address)
    if match:
        return prefecture, match.group(1)

    # パターン4: 町（郡なし）
    match = re.match(r'^(.+町)', address)
    if match:
        return prefecture, match.group(1)

    # パターン5: 村（郡なし）
    match = re.match(r'^(.+村)', address)
    if match:
        return prefecture, match.group(1)

    return prefecture, ''


def get_address_from_record(record: dict, object_type: str) -> str:
    """レコードから住所を取得"""
    if object_type == 'Lead':
        # 優先順位: Address__c > Street > City
        address_c = record.get('Address__c', '') or ''
        street = record.get('Street', '') or ''
        city = record.get('City', '') or ''
        state = record.get('State', '') or ''
        pref = record.get('Prefecture__c', '') or ''

        if address_c and re.search(r'[市区町村]', address_c):
            return address_c
        if street and re.search(r'[市区町村]', street):
            return street
        if city:
            return (state or pref) + city

        return ''

    else:  # Account
        billing_city = record.get('BillingCity', '') or ''
        billing_state = record.get('BillingState', '') or ''
        billing_street = record.get('BillingStreet', '') or ''

        if billing_street and re.search(r'[市区町村]', billing_street):
            return billing_street
        if billing_city:
            return billing_state + billing_city

        return ''


def lookup_job_ratio(mapping: dict, prefecture: str, municipality: str) -> dict:
    """
    MECEマッピングから有効求人倍率を検索

    検索優先順位:
    1. (都道府県, 市区町村) の完全一致
    2. 市区町村名のみの一致
    3. 政令指定都市の区名のみの一致（フォールバック）
    """
    # 完全一致
    key = (prefecture, municipality)
    if key in mapping:
        return mapping[key]

    # 市区町村名のみ
    if municipality in mapping:
        return mapping[municipality]

    # 政令指定都市の市名から区を除去して検索
    # 例: "横浜市中区" → "横浜市" で検索
    city_match = re.match(r'^(.+市)', municipality)
    if city_match:
        city_only = city_match.group(1)
        key = (prefecture, city_only)
        if key in mapping:
            return mapping[key]
        if city_only in mapping:
            return mapping[city_only]

    return None


def test_mapping_only(mapping: dict, output_dir: Path) -> dict:
    """マッピングのテスト（Salesforce接続なし）"""
    print("\n[テストモード] サンプル住所でマッピングをテスト")

    test_addresses = [
        "東京都新宿区西新宿1-1-1",
        "東京都港区六本木1-1-1",
        "東京都八王子市元本郷町1-1-1",
        "神奈川県横浜市中区山下町1",
        "大阪府大阪市北区梅田1-1-1",
        "北海道札幌市中央区北1条西1丁目",
        "北海道神恵内村字神恵内",  # 小規模村（フォールバックテスト）
        "鹿児島県与論町茶花",  # 離島（フォールバックテスト）
    ]

    matched = 0
    for addr in test_addresses:
        pref, muni = extract_municipality_from_address(addr)
        data = lookup_job_ratio(mapping, pref, muni)

        if data:
            ratio_str = f"{data['ratio']:.2f}" if data['ratio'] else "N/A"
            match_type = data.get('match_type', 'unknown')
            print(f"  {addr}")
            print(f"    -> {pref} {muni} -> {data['hellowork_name']}: {ratio_str}倍 [{match_type}]")
            matched += 1
        else:
            print(f"  {addr}")
            print(f"    -> {pref} {muni} -> マッチなし")

    print(f"\nテスト結果: {matched}/{len(test_addresses)} マッチ")

    # 統計情報
    total_mapped = len([v for v in mapping.values() if isinstance(v, dict) and 'ratio' in v])
    with_ratio = len([v for v in mapping.values() if isinstance(v, dict) and v.get('ratio') is not None])
    print(f"\nマッピング統計:")
    print(f"  総マッピング件数: {total_mapped}")
    print(f"  有効求人倍率あり: {with_ratio}")

    return {
        'total': len(test_addresses),
        'matched': matched,
        'unmatched': len(test_addresses) - matched,
        'output_file': None
    }


def add_job_ratio_to_records(
    object_type: str,
    mapping: dict,
    output_dir: Path,
    dry_run: bool = False
) -> dict:
    """
    Salesforceレコードに有効求人倍率を付与

    Returns:
        dict: 処理結果サマリー
    """
    client = SalesforceClient()
    try:
        client.authenticate()
    except Exception as e:
        if dry_run:
            print(f"注意: 認証スキップ（dry-run モード）: {e}")
            return test_mapping_only(mapping, output_dir)
        raise

    # 取得フィールド
    if object_type == 'Lead':
        fields = ['Id', 'Company', 'Address__c', 'Street', 'City', 'State', 'Prefecture__c']
    else:
        fields = ['Id', 'Name', 'BillingStreet', 'BillingCity', 'BillingState']

    # レコード取得
    query = f"SELECT {', '.join(fields)} FROM {object_type} WHERE IsDeleted = false"

    print(f"\n{object_type}レコードを取得中...")
    records = client.query_all(query)
    print(f"取得件数: {len(records)}")

    # マッチング処理
    updates = []
    matched_count = 0
    unmatched_count = 0
    match_type_counts = {'direct': 0, 'gun_removed': 0, 'seirei_expansion': 0, 'fallback': 0}

    for record in records:
        address = get_address_from_record(record, object_type)
        pref, muni = extract_municipality_from_address(address)

        data = lookup_job_ratio(mapping, pref, muni)

        if data:
            date_str = None
            if data['year'] and data['month']:
                date_str = f"{data['year']}-{data['month']:02d}-01"

            updates.append({
                'Id': record['Id'],
                'JobOpeningsRatio__c': data['ratio'],
                'HelloWorkName__c': data['hellowork_name'],
                'JobRatioDate__c': date_str
            })
            matched_count += 1
            match_type = data.get('match_type', 'unknown')
            if match_type in match_type_counts:
                match_type_counts[match_type] += 1
        else:
            unmatched_count += 1

    print(f"\nマッチング結果:")
    print(f"  マッチ成功: {matched_count}")
    print(f"  マッチ失敗: {unmatched_count}")
    print(f"  マッチ率: {matched_count / len(records) * 100:.1f}%" if len(records) > 0 else "  マッチ率: N/A")
    print(f"\nマッチタイプ別:")
    for match_type, count in match_type_counts.items():
        if count > 0:
            print(f"    - {match_type}: {count}")

    # CSV出力
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"{object_type.lower()}_job_ratio_updates_{timestamp}.csv"

    if updates:
        with open(output_file, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['Id', 'JobOpeningsRatio__c', 'HelloWorkName__c', 'JobRatioDate__c'])
            writer.writeheader()
            writer.writerows(updates)
        print(f"\n更新用CSV出力: {output_file}")

    # Salesforce更新（dry_run=Falseの場合）
    if not dry_run and updates:
        print(f"\nSalesforceへの更新を開始...")
        # TODO: Bulk API 2.0で更新
        print("※ 現在はCSV出力のみ。Bulk更新は別途実装予定。")

    return {
        'total': len(records),
        'matched': matched_count,
        'unmatched': unmatched_count,
        'output_file': str(output_file) if updates else None
    }


def main():
    parser = argparse.ArgumentParser(description="Lead/Accountに有効求人倍率を付与（MECEマッピング版）")
    parser.add_argument(
        "--object", "-o",
        type=str,
        choices=['Lead', 'Account'],
        default='Lead',
        help="対象オブジェクト（デフォルト: Lead）"
    )
    parser.add_argument(
        "--data-dir", "-d",
        type=str,
        default="data/job_openings_ratio",
        help="有効求人倍率データディレクトリ"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="data/output/job_ratio",
        help="出力ディレクトリ"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="テスト実行（Salesforce更新なし）"
    )
    args = parser.parse_args()

    # ディレクトリ設定
    data_dir = Path(args.data_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # MECEマッピング読み込み
    print("=" * 60)
    print("MECEマッピングテーブルを読み込み中...")
    print("=" * 60)
    mapping = load_mece_mapping(data_dir)

    if not mapping:
        print("エラー: MECEマッピングが読み込めません")
        sys.exit(1)

    # サンプル表示
    print("\nサンプルデータ:")
    sample_count = 0
    for key, data in mapping.items():
        if isinstance(key, tuple) and sample_count < 5:
            pref, muni = key
            ratio_str = f"{data['ratio']:.2f}" if data['ratio'] else "N/A"
            print(f"  {pref} {muni}: {ratio_str}倍 ({data['hellowork_name']}) [{data['match_type']}]")
            sample_count += 1

    # レコード更新
    print("\n" + "=" * 60)
    print(f"{args.object}レコードの更新処理")
    print("=" * 60)

    result = add_job_ratio_to_records(
        object_type=args.object,
        mapping=mapping,
        output_dir=output_dir,
        dry_run=args.dry_run
    )

    # サマリー
    print("\n" + "=" * 60)
    print("処理完了サマリー")
    print("=" * 60)
    print(f"対象オブジェクト: {args.object}")
    print(f"総レコード数: {result['total']}")
    print(f"マッチ成功: {result['matched']}")
    print(f"マッチ失敗: {result['unmatched']}")
    if result['output_file']:
        print(f"出力ファイル: {result['output_file']}")


if __name__ == "__main__":
    main()
