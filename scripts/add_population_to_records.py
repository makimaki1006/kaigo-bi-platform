#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Lead/Accountに市区町村人口データを付与するスクリプト

使用方法:
    python scripts/add_population_to_records.py [--object Lead|Account] [--dry-run]
"""

import argparse
import csv
import json
import re
import sys
from pathlib import Path
from datetime import datetime

# プロジェクトルートをパスに追加
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.api.salesforce_client import SalesforceClient


def load_population_mapping(mapping_file: Path) -> dict:
    """人口マッピングJSONを読み込み"""
    with open(mapping_file, 'r', encoding='utf-8') as f:
        return json.load(f)


def extract_municipality_from_address(address: str) -> tuple[str, str]:
    """
    住所文字列から市区町村名を抽出

    Returns:
        tuple: (抽出した市区町村名, 人口検索用キー)
    """
    if not address:
        return '', ''

    # 都道府県を除去
    address = address.strip()
    pref_patterns = [
        r'^(東京都|北海道|(?:京都|大阪)府|.{2,3}県)',
    ]
    for pattern in pref_patterns:
        match = re.match(pattern, address)
        if match:
            address = address[len(match.group(1)):]
            break

    # パターン1: 政令指定都市（〇〇市〇〇区）
    match = re.match(r'^(.+市)(.+区)', address)
    if match:
        city = match.group(1)
        full = match.group(1) + match.group(2)
        return full, city

    # パターン2: 東京23区
    match = re.match(r'^(.+区)', address)
    if match:
        ward = match.group(1)
        return ward, ward

    # パターン3: 通常の市町村
    match = re.match(r'^(.+?[市町村])', address)
    if match:
        return match.group(1), match.group(1)

    # パターン4: 郡（〇〇郡〇〇町）
    match = re.match(r'^(.+郡)(.+?[町村])', address)
    if match:
        town = match.group(2)
        full = match.group(1) + town
        return full, town

    return '', ''


def get_address_from_record(record: dict, object_type: str) -> str:
    """レコードから住所を取得（最も詳細な住所を優先）"""
    if object_type == 'Lead':
        # 優先順位: Address__c > Street > City+Prefecture__c
        address_c = record.get('Address__c', '') or ''
        street = record.get('Street', '') or ''
        city = record.get('City', '') or ''
        state = record.get('State', '') or ''
        pref = record.get('Prefecture__c', '') or ''

        # Address__c に詳細住所がある場合（最優先）
        if address_c and re.search(r'[市区町村]', address_c):
            return address_c

        # Street に住所がある場合
        if street and re.search(r'[市区町村]', street):
            return street

        # City がある場合
        if city:
            return (state or pref) + city

        # Prefecture__c に市区町村が含まれている場合
        if pref and re.search(r'[市区町村]', pref):
            pref = re.sub(r'^(東京都|北海道|(?:京都|大阪)府|.{2,3}県)\1', r'\1', pref)
            return pref

        # 都道府県のみ
        return pref or state

    else:  # Account
        # 優先順位: Address__c > HJBG_Address__c > BillingStreet > BillingCity
        address_c = record.get('Address__c', '') or ''
        hjbg = record.get('HJBG_Address__c', '') or ''
        state = record.get('BillingState', '') or ''
        city = record.get('BillingCity', '') or ''
        street = record.get('BillingStreet', '') or ''

        # Address__c に詳細住所がある場合（最優先）
        if address_c and re.search(r'[市区町村]', address_c):
            return address_c

        # HJBG_Address__c に住所がある場合
        if hjbg and re.search(r'[市区町村]', hjbg):
            return hjbg

        # BillingStreet に住所がある場合
        if street and re.search(r'[市区町村]', street):
            return street

        # BillingCity がある場合
        if city:
            return state + city

        return state
    return ''


def query_records(client: SalesforceClient, object_type: str, limit: int = None) -> list:
    """Salesforceからレコードを取得"""
    if object_type == 'Lead':
        # Address__c に詳細住所が入っている場合が多い
        fields = ['Id', 'Company', 'State', 'City', 'Street', 'Prefecture__c', 'Address__c', 'Phone']
    else:  # Account
        # Address__c, HJBG_Address__c も確認
        fields = ['Id', 'Name', 'BillingState', 'BillingCity', 'BillingStreet', 'Address__c', 'HJBG_Address__c', 'Phone']

    soql = f"SELECT {','.join(fields)} FROM {object_type}"
    if limit:
        soql += f" LIMIT {limit}"

    url = f"{client.instance_url}/services/data/{client.api_version}/query"
    headers = client._get_headers()

    all_records = []
    params = {'q': soql}

    while True:
        response = client._get_headers()  # リフレッシュ
        response = __import__('requests').get(url, headers=client._get_headers(), params=params)
        response.raise_for_status()
        data = response.json()

        all_records.extend(data['records'])
        print(f"  取得: {len(all_records)} 件...")

        if data.get('nextRecordsUrl'):
            url = client.instance_url + data['nextRecordsUrl']
            params = None
        else:
            break

    return all_records


def process_records(records: list, mapping: dict, object_type: str) -> list:
    """レコードに人口データを付与"""
    results = []
    matched = 0
    unmatched = 0

    for rec in records:
        address = get_address_from_record(rec, object_type)
        municipality, search_key = extract_municipality_from_address(address)
        population = mapping.get(search_key, 0)

        if population > 0:
            matched += 1
        else:
            unmatched += 1

        results.append({
            'Id': rec['Id'],
            'Name': rec.get('Company') or rec.get('Name', ''),
            'Address': address,
            'Municipality': municipality,
            'SearchKey': search_key,
            'Population': population
        })

    print(f"\n  マッチ成功: {matched} 件")
    print(f"  マッチ失敗: {unmatched} 件")

    return results


def export_results(results: list, output_file: Path):
    """結果をCSVに出力"""
    with open(output_file, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['Id', 'Name', 'Address', 'Municipality', 'SearchKey', 'Population'])
        writer.writeheader()
        writer.writerows(results)
    print(f"\n出力: {output_file}")


def show_population_distribution(results: list):
    """人口分布を表示"""
    # 人口帯別に集計
    brackets = [
        (0, 0, '人口不明'),
        (1, 50000, '〜5万'),
        (50001, 100000, '5〜10万'),
        (100001, 300000, '10〜30万'),
        (300001, 500000, '30〜50万'),
        (500001, 1000000, '50〜100万'),
        (1000001, float('inf'), '100万〜'),
    ]

    counts = {label: 0 for _, _, label in brackets}

    for rec in results:
        pop = rec['Population']
        for low, high, label in brackets:
            if low <= pop <= high:
                counts[label] += 1
                break

    print("\n=== 人口分布 ===")
    total = len(results)
    for _, _, label in brackets:
        count = counts[label]
        pct = (count / total * 100) if total > 0 else 0
        bar = '█' * int(pct / 2)
        print(f"{label:>10}: {count:>6} ({pct:>5.1f}%) {bar}")


def main():
    parser = argparse.ArgumentParser(description='Lead/Accountに人口データを付与')
    parser.add_argument('--object', choices=['Lead', 'Account'], default='Lead',
                        help='対象オブジェクト (default: Lead)')
    parser.add_argument('--limit', type=int, default=None,
                        help='取得件数制限（テスト用）')
    parser.add_argument('--dry-run', action='store_true',
                        help='実行確認のみ（Salesforce更新なし）')
    args = parser.parse_args()

    base_dir = Path(__file__).parent.parent
    mapping_file = base_dir / 'data' / 'population' / 'population_mapping.json'
    output_dir = base_dir / 'data' / 'output' / 'population'
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = output_dir / f'{args.object.lower()}_population_{timestamp}.csv'

    print(f"=== {args.object} 人口データ付与 ===\n")

    # 1. 人口マッピング読み込み
    print("1. 人口マッピング読み込み中...")
    mapping = load_population_mapping(mapping_file)
    print(f"  → {len(mapping)} エントリ\n")

    # 2. Salesforceからデータ取得
    print(f"2. Salesforceから{args.object}を取得中...")
    client = SalesforceClient()
    client.authenticate()
    records = query_records(client, args.object, args.limit)
    print(f"  → {len(records)} 件取得\n")

    # 3. 人口データ付与
    print("3. 人口データ付与中...")
    results = process_records(records, mapping, args.object)

    # 4. 結果出力
    print("\n4. 結果出力...")
    export_results(results, output_file)

    # 5. 人口分布表示
    show_population_distribution(results)

    print(f"\n完了！")
    print(f"出力ファイル: {output_file}")

    if args.dry_run:
        print("\n[DRY-RUN] Salesforceへの更新は行いませんでした")


if __name__ == '__main__':
    main()
