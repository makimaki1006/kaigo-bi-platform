# -*- coding: utf-8 -*-
"""人口・人口密度データの準備スクリプト

e-Stat「統計でみる市区町村のすがた2024」から人口・面積データを読み込み、
市区町村別の人口・人口密度CSVを作成する。

データソース:
- A 人口・世帯: 総人口（2020年国勢調査）
- B 自然環境: 可住地面積（2022年）
"""

import pandas as pd
import re
from pathlib import Path

# パス設定
BASE_DIR = Path(__file__).parent.parent
INPUT_DIR = BASE_DIR / 'data' / 'input' / 'population'
OUTPUT_DIR = BASE_DIR / 'data' / 'output' / 'population'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ファイルパス
POPULATION_FILE = INPUT_DIR / 'A_jinko_setai_2024.xlsx'
AREA_FILE = INPUT_DIR / 'B_shizen_kankyo_2024.xlsx'

# 都道府県リスト
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


def clean_municipality_name(name):
    """市区町村名をクリーンアップ"""
    if pd.isna(name):
        return None
    name = str(name).strip()
    # 全角・半角スペースを除去
    name = name.replace('　', '').replace(' ', '')
    return name


def clean_municipality_code(code):
    """市区町村コードをクリーンアップ（5桁の文字列に）"""
    if pd.isna(code):
        return None
    code_str = str(code).strip()
    # .0を除去
    if code_str.endswith('.0'):
        code_str = code_str[:-2]
    # 数字のみ
    code_str = re.sub(r'[^0-9]', '', code_str)
    if not code_str:
        return None
    # 5桁に正規化（先頭0埋め）
    if len(code_str) < 5:
        code_str = code_str.zfill(5)
    return code_str


def is_prefecture_code(code):
    """都道府県コードかどうかを判定（2桁）"""
    if not code:
        return False
    return len(code) <= 2 or (len(code) == 5 and code.endswith('000'))


def load_population_data():
    """人口データを読み込み"""
    print("=== 人口データ読み込み ===")
    df = pd.read_excel(POPULATION_FILE, sheet_name='A', header=None)

    # データ部分を抽出（行10以降）
    data_df = df.iloc[10:].copy()
    data_df.columns = range(len(data_df.columns))

    # 必要な列を抽出
    result = pd.DataFrame({
        'municipality_code': data_df[1],  # 市区町村コード（列1）
        'municipality_raw': data_df[8],   # 市区町村名（日本語）
        'municipality_en': data_df[9],    # 市区町村名（英語）
        'population': data_df[10],        # 総人口
    })

    # クリーンアップ
    result['municipality_code'] = result['municipality_code'].apply(clean_municipality_code)
    result['municipality_raw'] = result['municipality_raw'].apply(clean_municipality_name)

    # 空行を除去
    result = result.dropna(subset=['municipality_raw', 'municipality_code'])
    result = result[result['municipality_raw'] != '']

    print(f"  読み込み件数: {len(result)}")
    return result


def load_area_data():
    """面積データを読み込み"""
    print("\n=== 面積データ読み込み ===")
    df = pd.read_excel(AREA_FILE, sheet_name=0, header=None)

    # データ部分を抽出（行10以降）
    data_df = df.iloc[10:].copy()
    data_df.columns = range(len(data_df.columns))

    # 必要な列を抽出
    result = pd.DataFrame({
        'municipality_raw': data_df[8],           # 市区町村名（日本語）
        'total_area_km2': data_df[10],            # 総面積（km²）
        'inhabitable_area_km2': data_df[11],      # 可住地面積（km²）
        'municipality_code': data_df[12],         # 市区町村コード
    })

    # クリーンアップ
    result['municipality_code'] = result['municipality_code'].apply(clean_municipality_code)
    result['municipality_raw'] = result['municipality_raw'].apply(clean_municipality_name)

    # 空行を除去
    result = result.dropna(subset=['municipality_raw', 'municipality_code'])
    result = result[result['municipality_raw'] != '']

    print(f"  読み込み件数: {len(result)}")
    return result


def determine_prefecture_city(df):
    """データフレームに都道府県・市区町村名（フル）列を追加"""
    current_prefecture = None
    current_city = None  # 政令指定都市

    prefectures = []
    cities = []

    for _, row in df.iterrows():
        name = row['municipality_raw']
        code = row.get('municipality_code', '')

        # 都道府県判定
        if name in PREFECTURES:
            current_prefecture = name
            current_city = None
            prefectures.append(name)
            cities.append(None)
        # 政令指定都市判定（コードが5桁で末尾00）
        elif code and len(code) == 5 and code.endswith('00') and name.endswith('市'):
            current_city = name
            prefectures.append(current_prefecture)
            cities.append(name)
        # 政令指定都市の区判定（コードが5桁で末尾が01-99）
        elif code and len(code) == 5 and current_city and name.endswith('区'):
            prefectures.append(current_prefecture)
            cities.append(f"{current_city}{name}")  # 横浜市中区 など
        else:
            # 通常の市町村
            if name.endswith('市') or name.endswith('町') or name.endswith('村'):
                current_city = None
            prefectures.append(current_prefecture)
            cities.append(name)

    df = df.copy()
    df['prefecture'] = prefectures
    df['city'] = cities

    return df


def merge_and_calculate():
    """人口と面積データを統合し、人口密度を計算"""
    print("\n=== データ統合・人口密度計算 ===")

    pop_df = load_population_data()
    area_df = load_area_data()

    # 都道府県・市区町村を判定
    pop_df = determine_prefecture_city(pop_df)
    area_df = determine_prefecture_city(area_df)

    # 市区町村コードでマージ（ユニークキー）
    merged = pd.merge(
        pop_df[['municipality_code', 'prefecture', 'city', 'population', 'municipality_raw']],
        area_df[['municipality_code', 'total_area_km2', 'inhabitable_area_km2']],
        on='municipality_code',
        how='inner'  # 両方にあるもののみ
    )

    print(f"  統合後件数: {len(merged)}")

    # 数値変換
    merged['population'] = pd.to_numeric(merged['population'], errors='coerce')
    merged['total_area_km2'] = pd.to_numeric(merged['total_area_km2'], errors='coerce')
    merged['inhabitable_area_km2'] = pd.to_numeric(merged['inhabitable_area_km2'], errors='coerce')

    # 人口密度を計算（人/km²）- 可住地面積ベース
    merged['population_density_km2'] = merged['population'] / merged['inhabitable_area_km2']

    # 人/m²に変換（1km² = 1,000,000m²）
    merged['population_density_m2'] = merged['population_density_km2'] / 1000000

    # 都道府県レコードを除外（市区町村のみ）
    municipalities = merged[merged['city'].notna()].copy()
    prefectures_df = merged[merged['city'].isna()].copy()

    print(f"  市区町村数: {len(municipalities)}")
    print(f"  都道府県数: {len(prefectures_df)}")

    return merged, municipalities, prefectures_df


def create_lookup_table(municipalities):
    """Salesforceマッチング用のルックアップテーブルを作成"""
    print("\n=== ルックアップテーブル作成 ===")

    lookup = municipalities[['municipality_code', 'prefecture', 'city', 'population',
                            'inhabitable_area_km2', 'population_density_km2', 'population_density_m2']].copy()

    # 検索キー作成（都道府県+市区町村）
    lookup['key'] = lookup['prefecture'] + lookup['city']

    # 重複チェック
    duplicates = lookup[lookup.duplicated(subset=['key'], keep=False)]
    if len(duplicates) > 0:
        print(f"  警告: 重複キーあり {len(duplicates)}件")
        print(duplicates[['prefecture', 'city', 'key']].head(10))

    print(f"  ルックアップテーブル件数: {len(lookup)}")

    return lookup


def main():
    print("=" * 60)
    print("人口・人口密度データ準備")
    print("e-Stat「統計でみる市区町村のすがた2024」")
    print("=" * 60)

    # データ統合
    all_data, municipalities, prefectures_df = merge_and_calculate()

    # ルックアップテーブル作成
    lookup = create_lookup_table(municipalities)

    # CSV出力
    output_file = OUTPUT_DIR / 'municipality_population_density.csv'
    lookup.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"\n出力: {output_file}")

    # 都道府県別データも出力
    pref_output = OUTPUT_DIR / 'prefecture_population_density.csv'
    prefectures_df[['municipality_raw', 'population', 'total_area_km2', 'inhabitable_area_km2',
                    'population_density_km2']].to_csv(
        pref_output, index=False, encoding='utf-8-sig'
    )
    print(f"出力: {pref_output}")

    # サンプル表示
    print("\n=== サンプルデータ（人口密度上位10） ===")
    top10 = lookup.nlargest(10, 'population_density_km2')
    for _, row in top10.iterrows():
        print(f"  {row['prefecture']}{row['city']}: {row['population']:,.0f}人, {row['population_density_km2']:,.1f}人/km²")

    print("\n=== サンプルデータ（人口密度下位10） ===")
    bottom10 = lookup.nsmallest(10, 'population_density_km2')
    for _, row in bottom10.iterrows():
        print(f"  {row['prefecture']}{row['city']}: {row['population']:,.0f}人, {row['population_density_km2']:,.1f}人/km²")

    # 統計サマリー
    print("\n=== 統計サマリー ===")
    print(f"  市区町村数: {len(lookup)}")
    print(f"  総人口: {lookup['population'].sum():,.0f}人")
    print(f"  平均人口密度: {lookup['population_density_km2'].mean():,.1f}人/km²")
    print(f"  中央値人口密度: {lookup['population_density_km2'].median():,.1f}人/km²")

    # 東京都の区を確認
    print("\n=== 東京都23区サンプル ===")
    tokyo_wards = lookup[lookup['prefecture'] == '東京都']
    tokyo_wards = tokyo_wards[tokyo_wards['city'].str.endswith('区')]
    for _, row in tokyo_wards.head(5).iterrows():
        print(f"  {row['city']}: {row['population']:,.0f}人, {row['population_density_km2']:,.1f}人/km²")


if __name__ == '__main__':
    main()
