# -*- coding: utf-8 -*-
"""
2026/01/16作成のハローワーク新規リードに人口データを付与し、1万人以下を削除
"""
import pandas as pd
import json
import re
import sys
import io
from pathlib import Path
from datetime import datetime

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.api.salesforce_client import SalesforceClient
import requests

# 設定
BASE_DIR = Path(__file__).parent.parent
MAPPING_FILE = BASE_DIR / 'data' / 'population' / 'population_mapping.json'
LEAD_IDS_FILE = BASE_DIR / 'data' / 'output' / 'hellowork' / 'created_lead_ids_20260116.csv'

def load_population_mapping():
    """人口マッピングJSONを読み込み"""
    with open(MAPPING_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)

def extract_municipality(address):
    """住所から市区町村を抽出して人口検索キーを返す"""
    if not address:
        return '', ''

    address = str(address).strip()

    # 都道府県を除去
    pref_patterns = [r'^(東京都|北海道|(?:京都|大阪)府|.{2,3}県)']
    for pattern in pref_patterns:
        match = re.match(pattern, address)
        if match:
            address = address[len(match.group(1)):]
            break

    # パターン1: 政令指定都市（〇〇市〇〇区）
    match = re.match(r'^(.+市)(.+区)', address)
    if match:
        city = match.group(1)
        return match.group(1) + match.group(2), city

    # パターン2: 東京23区
    match = re.match(r'^(.+区)', address)
    if match:
        return match.group(1), match.group(1)

    # パターン3: 通常の市町村
    match = re.match(r'^(.+?[市町村])', address)
    if match:
        return match.group(1), match.group(1)

    # パターン4: 郡（〇〇郡〇〇町）
    match = re.match(r'^(.+郡)(.+?[町村])', address)
    if match:
        return match.group(1) + match.group(2), match.group(2)

    return '', ''

def main():
    print('=== ハローワーク新規リード 人口データ付与 ===')
    print()

    # 1. 人口マッピング読み込み
    print('1. 人口マッピング読み込み...')
    mapping = load_population_mapping()
    print(f'   → {len(mapping):,} エントリ')
    print()

    # 2. 作成済みリードIDを読み込み
    print('2. 作成済みリードID読み込み...')
    df_ids = pd.read_csv(LEAD_IDS_FILE, dtype=str)
    lead_ids = df_ids['sf__Id'].dropna().tolist()
    print(f'   → {len(lead_ids):,} 件')
    print()

    # 3. SalesforceからリードのStreet（住所）を取得
    print('3. Salesforceからリード情報取得...')
    client = SalesforceClient()
    client.authenticate()

    # IDをチャンク分割してクエリ
    chunk_size = 200
    all_records = []

    for i in range(0, len(lead_ids), chunk_size):
        chunk_ids = lead_ids[i:i+chunk_size]
        ids_str = "','".join(chunk_ids)
        soql = f"SELECT Id, Company, Street, Prefecture__c FROM Lead WHERE Id IN ('{ids_str}')"

        url = f"{client.instance_url}/services/data/{client.api_version}/query"
        response = requests.get(url, headers=client._get_headers(), params={'q': soql})
        response.raise_for_status()
        data = response.json()
        all_records.extend(data['records'])
        print(f'   → 取得: {len(all_records):,} 件...')

    print(f'   → 合計: {len(all_records):,} 件取得完了')
    print()

    # 4. 人口データを付与
    print('4. 人口データ付与...')
    results = []
    matched = 0
    unmatched = 0

    for rec in all_records:
        # Street（住所）から市区町村を抽出
        address = rec.get('Street', '') or ''
        if not address or not re.search(r'[市区町村]', address):
            # Prefecture__cから試行
            address = rec.get('Prefecture__c', '') or ''

        municipality, search_key = extract_municipality(address)
        population = mapping.get(search_key, 0)

        if population > 0:
            matched += 1
        else:
            unmatched += 1

        results.append({
            'Id': rec['Id'],
            'Company': rec.get('Company', ''),
            'Street': rec.get('Street', ''),
            'Municipality': municipality,
            'SearchKey': search_key,
            'Population__c': population
        })

    print(f'   マッチ成功: {matched:,} 件')
    print(f'   マッチ失敗: {unmatched:,} 件')
    print()

    # 5. 人口分布確認
    print('5. 人口分布:')
    df_results = pd.DataFrame(results)

    # 1万人以下の件数
    below_10k = (df_results['Population__c'] <= 10000).sum()
    above_10k = (df_results['Population__c'] > 10000).sum()
    unknown = (df_results['Population__c'] == 0).sum()

    print(f'   人口不明（0）: {unknown:,} 件')
    print(f'   1万人以下: {below_10k:,} 件')
    print(f'   1万人超: {above_10k:,} 件')
    print()

    # 人口帯別
    brackets = [
        (0, 0, '人口不明'),
        (1, 10000, '〜1万'),
        (10001, 50000, '1〜5万'),
        (50001, 100000, '5〜10万'),
        (100001, 300000, '10〜30万'),
        (300001, 500000, '30〜50万'),
        (500001, 1000000, '50〜100万'),
        (1000001, float('inf'), '100万〜'),
    ]

    for low, high, label in brackets:
        count = ((df_results['Population__c'] >= low) & (df_results['Population__c'] <= high)).sum()
        pct = count / len(df_results) * 100
        print(f'   {label:>10}: {count:>5} 件 ({pct:>5.1f}%)')
    print()

    # 6. 1万人以下を除外対象として出力
    df_to_delete = df_results[df_results['Population__c'] <= 10000]
    df_to_keep = df_results[df_results['Population__c'] > 10000]

    output_dir = BASE_DIR / 'data' / 'output' / 'hellowork'

    # 更新用CSV（Population__c付与）
    df_update = df_results[['Id', 'Population__c']]
    update_file = output_dir / 'hw_202601_population_update.csv'
    df_update.to_csv(update_file, index=False, encoding='utf-8-sig')
    print(f'更新用CSV保存: {update_file}')

    # 削除対象CSV
    delete_file = output_dir / 'hw_202601_leads_to_delete.csv'
    df_to_delete[['Id', 'Company', 'Municipality', 'Population__c']].to_csv(
        delete_file, index=False, encoding='utf-8-sig'
    )
    print(f'削除対象CSV保存: {delete_file} ({len(df_to_delete):,} 件)')

    print()
    print('=' * 50)
    print(f'次のステップ:')
    print(f'  1. Population__cを更新: {len(df_results):,} 件')
    print(f'  2. 人口1万人以下を削除: {len(df_to_delete):,} 件')
    print(f'  3. 残り件数: {len(df_to_keep):,} 件')
    print('=' * 50)

if __name__ == '__main__':
    main()
