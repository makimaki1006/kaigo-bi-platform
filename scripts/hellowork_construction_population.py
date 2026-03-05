"""
建設系リードに人口データを付与し、分布を分析するスクリプト

Usage:
    python scripts/hellowork_construction_population.py
"""

import sys
import json
import re
import pandas as pd
from pathlib import Path

# UTF-8出力設定
sys.stdout.reconfigure(encoding='utf-8')

# ベースパス
base = Path(r'C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List')


def extract_municipality(address):
    """住所から市区町村を抽出"""
    if not address or pd.isna(address):
        return None

    # 東京23区パターン
    match = re.search(r'(千代田|中央|港|新宿|文京|台東|墨田|江東|品川|目黒|大田|世田谷|渋谷|中野|杉並|豊島|北|荒川|板橋|練馬|足立|葛飾|江戸川)区', address)
    if match:
        return match.group(0)

    # 政令指定都市パターン (〇〇市〇〇区 → 〇〇市)
    match = re.search(r'([^\s　]+市)[^\s　]*区', address)
    if match:
        return match.group(1)

    # 通常の市
    match = re.search(r'([^\s　]+市)', address)
    if match:
        return match.group(1)

    # 町村
    match = re.search(r'([^\s　]+[町村])', address)
    if match:
        return match.group(1)

    return None


def get_population(address, pop_mapping):
    """住所から人口を取得"""
    municipality = extract_municipality(address)
    if not municipality:
        return None, None

    # Direct match
    if municipality in pop_mapping:
        return municipality, pop_mapping[municipality]

    # Try without 市/区/町/村
    for suffix in ['市', '区', '町', '村']:
        base_name = municipality.replace(suffix, '')
        for key in pop_mapping:
            if base_name in key:
                return key, pop_mapping[key]

    return municipality, None


def get_pop_band(pop):
    """人口帯を取得"""
    if pd.isna(pop):
        return '不明'
    elif pop < 50000:
        return '〜5万'
    elif pop < 100000:
        return '5〜10万'
    elif pop < 300000:
        return '10〜30万'
    elif pop < 500000:
        return '30〜50万'
    elif pop < 1000000:
        return '50〜100万'
    else:
        return '100万〜'


def main():
    print("=== STEP 1: データ読み込み ===\n")

    # Load construction leads CSV
    leads_path = base / 'data/output/hellowork/construction_new_leads_20260203.csv'
    leads = pd.read_csv(leads_path, encoding='utf-8-sig', dtype=str)
    print(f'建設系リード: {len(leads):,}件')

    # Load population mapping
    pop_mapping_path = base / 'data/population/population_mapping.json'
    with open(pop_mapping_path, 'r', encoding='utf-8') as f:
        pop_mapping = json.load(f)
    print(f'人口マッピング: {len(pop_mapping):,}エントリ')

    print("\n=== STEP 2: 市区町村抽出と人口マッチング ===\n")

    # Identify address column
    address_col = 'Street' if 'Street' in leads.columns else None
    if not address_col:
        for col in leads.columns:
            if '住所' in col or 'Street' in col or 'Address' in col:
                address_col = col
                break

    print(f'住所カラム: {address_col}')

    # Apply population matching
    leads['municipality'] = None
    leads['population'] = None

    for idx, row in leads.iterrows():
        address = row.get(address_col, '') or ''
        prefecture = row.get('Prefecture__c', '') or ''
        full_address = f"{prefecture}{address}"

        muni, pop = get_population(full_address, pop_mapping)
        leads.at[idx, 'municipality'] = muni
        leads.at[idx, 'population'] = pop

    # Convert population to numeric
    leads['population'] = pd.to_numeric(leads['population'], errors='coerce')

    print("\n=== STEP 3: 人口分布分析 ===\n")

    # Population bands
    leads['pop_band'] = leads['population'].apply(get_pop_band)

    # Print distribution
    print('=== 人口帯別分布 ===')
    pop_dist = leads['pop_band'].value_counts()
    band_order = ['100万〜', '50〜100万', '30〜50万', '10〜30万', '5〜10万', '〜5万', '不明']
    for band in band_order:
        count = pop_dist.get(band, 0)
        pct = count / len(leads) * 100 if len(leads) > 0 else 0
        print(f'{band}: {count:,}件 ({pct:.1f}%)')

    # Match rate
    matched = leads['population'].notna().sum()
    print(f'\nマッチ率: {matched:,} / {len(leads):,} = {matched/len(leads)*100:.1f}%')

    # Top municipalities by count
    print('\n=== 市区町村別TOP20 ===')
    muni_counts = leads[leads['municipality'].notna()]['municipality'].value_counts().head(20)
    for muni, count in muni_counts.items():
        pop = pop_mapping.get(muni, 'N/A')
        if isinstance(pop, int):
            print(f'{muni}: {count:,}件 (人口: {pop:,}人)')
        else:
            print(f'{muni}: {count:,}件')

    print("\n=== STEP 4: フィルタオプション分析 ===\n")

    # Calculate counts for different thresholds
    print('=== 人口しきい値別の件数 ===')
    thresholds = [
        ('10万以上', 100000),
        ('20万以上', 200000),
        ('30万以上', 300000),
        ('50万以上', 500000),
        ('100万以上', 1000000),
    ]

    for label, threshold in thresholds:
        count = (leads['population'] >= threshold).sum()
        print(f'{label}: {count:,}件')

    # Combination filters
    print('\n=== フィルタ組み合わせ候補 ===')
    mobile_col = 'MobilePhone' if 'MobilePhone' in leads.columns else None

    combos = [
        ('人口30万以上', leads['population'] >= 300000),
        ('人口50万以上', leads['population'] >= 500000),
        ('人口10万以上', leads['population'] >= 100000),
    ]

    if mobile_col:
        combos.insert(1, ('人口30万以上 + 携帯あり', (leads['population'] >= 300000) & (leads[mobile_col].fillna('') != '')))

    for label, mask in combos:
        count = mask.sum()
        print(f'{label}: {count:,}件')

    print("\n=== STEP 5: データ保存 ===\n")

    # Save with population data
    output_path = base / 'data/output/hellowork/construction_new_leads_with_pop_20260203.csv'
    leads.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f'保存: {output_path}')
    print(f'総件数: {len(leads):,}件')
    print('\n完了!')


if __name__ == '__main__':
    main()
