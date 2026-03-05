# -*- coding: utf-8 -*-
"""人口・人口密度データをSalesforce Account/Leadに更新するスクリプト

e-Statの市区町村別人口データとSalesforceの住所をマッチングし、
Population__c と PopulationDensity__c フィールドを更新する。
"""

import pandas as pd
import re
from pathlib import Path
from datetime import datetime

# パス設定
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / 'data'
OUTPUT_DIR = DATA_DIR / 'output' / 'population'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# 都道府県リスト（正規化用）
PREFECTURES = [
    '北海道', '青森県', '岩手県', '宮城県', '秋田県', '山形県', '福島県',
    '茨城県', '栃木県', '群馬県', '埼玉県', '千葉県', '東京都', '神奈川県',
    '新潟県', '富山県', '石川県', '福井県', '山梨県', '長野県',
    '岐阜県', '静岡県', '愛知県', '三重県',
    '滋賀県', '京都府', '大阪府', '兵庫県', '奈良県', '和歌山県',
    '鳥取県', '島根県', '岡山県', '広島県', '山口県',
    '徳島県', '香川県', '愛媛県', '高知県',
    '福岡県', '佐賀県', '長崎県', '熊本県', '大分県', '宮崎県', '鹿児島県', '沖縄県'
]

# 都道府県の短縮形マッピング
PREFECTURE_NORMALIZE = {
    '北海道': '北海道',
    '青森': '青森県', '岩手': '岩手県', '宮城': '宮城県', '秋田': '秋田県',
    '山形': '山形県', '福島': '福島県', '茨城': '茨城県', '栃木': '栃木県',
    '群馬': '群馬県', '埼玉': '埼玉県', '千葉': '千葉県', '東京': '東京都',
    '神奈川': '神奈川県', '新潟': '新潟県', '富山': '富山県', '石川': '石川県',
    '福井': '福井県', '山梨': '山梨県', '長野': '長野県', '岐阜': '岐阜県',
    '静岡': '静岡県', '愛知': '愛知県', '三重': '三重県', '滋賀': '滋賀県',
    '京都': '京都府', '大阪': '大阪府', '兵庫': '兵庫県', '奈良': '奈良県',
    '和歌山': '和歌山県', '鳥取': '鳥取県', '島根': '島根県', '岡山': '岡山県',
    '広島': '広島県', '山口': '山口県', '徳島': '徳島県', '香川': '香川県',
    '愛媛': '愛媛県', '高知': '高知県', '福岡': '福岡県', '佐賀': '佐賀県',
    '長崎': '長崎県', '熊本': '熊本県', '大分': '大分県', '宮崎': '宮崎県',
    '鹿児島': '鹿児島県', '沖縄': '沖縄県'
}

# 完全な都道府県名も追加
for pref in PREFECTURES:
    PREFECTURE_NORMALIZE[pref] = pref


def normalize_prefecture(pref):
    """都道府県名を正規化（県/都/府なしでも対応）"""
    if pd.isna(pref) or not pref:
        return None
    pref = str(pref).strip().replace('　', '').replace(' ', '')
    return PREFECTURE_NORMALIZE.get(pref, None)


def extract_city_from_address(address, prefecture=None):
    """住所から市区町村を抽出

    Args:
        address: 住所文字列
        prefecture: 都道府県（既知の場合）

    Returns:
        tuple: (都道府県, 市区町村) or (None, None)
    """
    if pd.isna(address) or not address:
        return None, None

    address = str(address).strip().replace('　', '').replace(' ', '')

    # 都道府県を住所から抽出
    detected_pref = None
    for pref in PREFECTURES:
        if address.startswith(pref):
            detected_pref = pref
            address = address[len(pref):]
            break

    # 【修正】住所内の都道府県を優先（矛盾防止）
    # 住所に都道府県があればそれを使用、なければ入力値を使用
    if detected_pref:
        final_pref = detected_pref
    elif prefecture and not pd.isna(prefecture):
        final_pref = normalize_prefecture(prefecture)
    else:
        final_pref = None

    if not final_pref:
        return None, None

    # 市区町村を抽出
    # パターン1: 政令指定都市の区（横浜市中区、大阪市北区など）
    designated_city_pattern = re.compile(r'^(.+?市)(.+?区)')
    match = designated_city_pattern.match(address)
    if match:
        city = match.group(1) + match.group(2)
        return final_pref, city

    # パターン2: 東京23区（〇〇区）
    if final_pref == '東京都':
        tokyo_ward_pattern = re.compile(r'^(.+?区)')
        match = tokyo_ward_pattern.match(address)
        if match:
            return final_pref, match.group(1)

    # パターン3: 通常の市
    city_pattern = re.compile(r'^(.+?市)')
    match = city_pattern.match(address)
    if match:
        return final_pref, match.group(1)

    # パターン4: 郡+町村 → 【修正】町村名のみ抽出（郡名は除去）
    # e-Statデータは「岐南町」形式で「羽島郡岐南町」ではない
    gun_pattern = re.compile(r'^.+?郡(.+?[町村])')
    match = gun_pattern.match(address)
    if match:
        return final_pref, match.group(1)  # 町村名のみ返す

    # パターン5: 町村（郡なし）
    town_pattern = re.compile(r'^(.+?[町村])')
    match = town_pattern.match(address)
    if match:
        return final_pref, match.group(1)

    return final_pref, None


def load_population_lookup():
    """人口データルックアップテーブルを読み込み"""
    lookup_file = OUTPUT_DIR / 'municipality_population_density.csv'
    df = pd.read_csv(lookup_file, dtype=str, encoding='utf-8-sig')

    # 数値変換
    df['population'] = pd.to_numeric(df['population'], errors='coerce')
    df['population_density_km2'] = pd.to_numeric(df['population_density_km2'], errors='coerce')
    df['population_density_m2'] = pd.to_numeric(df['population_density_m2'], errors='coerce')

    # keyでインデックス作成
    lookup = {}
    for _, row in df.iterrows():
        key = row['key']
        lookup[key] = {
            'population': row['population'],
            'population_density_km2': row['population_density_km2'],
            'population_density_m2': row['population_density_m2'],
            'city': row['city']
        }

    print(f"人口ルックアップテーブル: {len(lookup)}件")
    return lookup


def process_accounts(lookup):
    """Accountデータを処理"""
    print("\n=== Account処理 ===")

    # 最新のAccountファイルを取得
    account_files = sorted(DATA_DIR.glob('output/Account_*.csv'), reverse=True)
    if not account_files:
        print("  エラー: Accountファイルが見つかりません")
        return None

    account_file = account_files[0]
    print(f"  ファイル: {account_file.name}")

    # 必要な列のみ読み込み
    cols = ['Id', 'Prefectures__c', 'Address__c']
    df = pd.read_csv(account_file, usecols=lambda c: c in cols, dtype=str, encoding='utf-8')
    print(f"  読み込み件数: {len(df)}")

    # マッチング処理
    updates = []
    matched = 0
    unmatched = 0
    no_address = 0

    for _, row in df.iterrows():
        account_id = row['Id']
        prefecture = row.get('Prefectures__c', '')
        address = row.get('Address__c', '')

        if pd.isna(address) or not address:
            no_address += 1
            continue

        # 市区町村を抽出
        pref, city = extract_city_from_address(address, prefecture)

        if not pref or not city:
            unmatched += 1
            continue

        # ルックアップ
        key = pref + city
        if key in lookup:
            pop_data = lookup[key]
            updates.append({
                'Id': account_id,
                'Population__c': int(pop_data['population']) if pd.notna(pop_data['population']) else '',
                'PopulationDensity__c': pop_data['population_density_m2'] if pd.notna(pop_data['population_density_m2']) else ''
            })
            matched += 1
        else:
            unmatched += 1

    print(f"  マッチ: {matched}件")
    print(f"  アンマッチ: {unmatched}件")
    print(f"  住所なし: {no_address}件")

    return pd.DataFrame(updates) if updates else None


def process_leads(lookup):
    """Leadデータを処理"""
    print("\n=== Lead処理 ===")

    # 最新のLeadファイルを取得
    lead_files = sorted(DATA_DIR.glob('output/Lead_*.csv'), reverse=True)
    if not lead_files:
        print("  エラー: Leadファイルが見つかりません")
        return None

    lead_file = lead_files[0]
    print(f"  ファイル: {lead_file.name}")

    # 必要な列のみ読み込み
    cols = ['Id', 'Prefecture__c', 'Address__c', 'Street']
    df = pd.read_csv(lead_file, usecols=lambda c: c in cols, dtype=str, encoding='utf-8')
    print(f"  読み込み件数: {len(df)}")

    # マッチング処理
    updates = []
    matched = 0
    unmatched = 0
    no_address = 0

    for _, row in df.iterrows():
        lead_id = row['Id']
        prefecture = row.get('Prefecture__c', '')
        address = row.get('Address__c', '')
        street = row.get('Street', '')

        # Address__c優先、なければStreet
        addr = address if pd.notna(address) and address else street

        if pd.isna(addr) or not addr:
            no_address += 1
            continue

        # 市区町村を抽出
        pref, city = extract_city_from_address(addr, prefecture)

        if not pref or not city:
            unmatched += 1
            continue

        # ルックアップ
        key = pref + city
        if key in lookup:
            pop_data = lookup[key]
            updates.append({
                'Id': lead_id,
                'Population__c': int(pop_data['population']) if pd.notna(pop_data['population']) else '',
                'PopulationDensity__c': pop_data['population_density_m2'] if pd.notna(pop_data['population_density_m2']) else ''
            })
            matched += 1
        else:
            unmatched += 1

    print(f"  マッチ: {matched}件")
    print(f"  アンマッチ: {unmatched}件")
    print(f"  住所なし: {no_address}件")

    return pd.DataFrame(updates) if updates else None


def main():
    print("=" * 60)
    print("人口・人口密度データ更新")
    print("=" * 60)

    # ルックアップテーブル読み込み
    lookup = load_population_lookup()

    # Account処理
    account_updates = process_accounts(lookup)

    # Lead処理
    lead_updates = process_leads(lookup)

    # CSV出力
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    if account_updates is not None and len(account_updates) > 0:
        account_file = OUTPUT_DIR / f'account_population_updates_{timestamp}.csv'
        account_updates.to_csv(account_file, index=False, encoding='utf-8-sig')
        print(f"\nAccount更新CSV: {account_file}")
        print(f"  件数: {len(account_updates)}")

    if lead_updates is not None and len(lead_updates) > 0:
        lead_file = OUTPUT_DIR / f'lead_population_updates_{timestamp}.csv'
        lead_updates.to_csv(lead_file, index=False, encoding='utf-8-sig')
        print(f"\nLead更新CSV: {lead_file}")
        print(f"  件数: {len(lead_updates)}")

    # サンプル表示
    if account_updates is not None and len(account_updates) > 0:
        print("\n=== Account更新サンプル（5件） ===")
        for _, row in account_updates.head(5).iterrows():
            pop = row['Population__c']
            density = row['PopulationDensity__c']
            print(f"  {row['Id']}: 人口={pop:,}人, 密度={density:.6f}人/m2" if density else f"  {row['Id']}: 人口={pop}")

    if lead_updates is not None and len(lead_updates) > 0:
        print("\n=== Lead更新サンプル（5件） ===")
        for _, row in lead_updates.head(5).iterrows():
            pop = row['Population__c']
            density = row['PopulationDensity__c']
            print(f"  {row['Id']}: 人口={pop:,}人, 密度={density:.6f}人/m2" if density else f"  {row['Id']}: 人口={pop}")


if __name__ == '__main__':
    main()
