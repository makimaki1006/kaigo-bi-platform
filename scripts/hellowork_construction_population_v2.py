import sys, json, re
import pandas as pd
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')

base = Path(r'C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List')

# Load construction leads CSV
leads = pd.read_csv(base / 'data/output/hellowork/construction_new_leads_20260203.csv',
                    encoding='utf-8-sig', dtype=str)
print(f'建設系リード: {len(leads)}件')

# Load population mapping
with open(base / 'data/population/population_mapping.json', 'r', encoding='utf-8') as f:
    pop_mapping = json.load(f)
print(f'人口マッピング: {len(pop_mapping)}エントリ')

# Also load the municipality CSV for more matching options
muni_df = pd.read_csv(base / 'data/population/municipality_population.csv', encoding='utf-8-sig', dtype=str)
muni_df['population_2010'] = pd.to_numeric(muni_df['population_2010'], errors='coerce')
print(f'市区町村マスタ: {len(muni_df)}件')

# Create more flexible matching dict
# Key variations: "横浜市", "神奈川県横浜市", "横浜"
flex_pop = {}
for _, row in muni_df.iterrows():
    name = row.get('municipality')
    pref = row.get('prefecture')
    pop = row['population_2010']
    if pd.isna(name) or pd.isna(pop):
        continue

    # Add various keys
    flex_pop[name] = pop
    if pref and not pd.isna(pref):
        flex_pop[f'{pref}{name}'] = pop

    # Remove suffix variations
    for suffix in ['市', '区', '町', '村']:
        if name.endswith(suffix):
            base_name = name[:-1]
            flex_pop[base_name] = pop

# Add all pop_mapping entries too
flex_pop.update({k: v for k, v in pop_mapping.items() if isinstance(v, (int, float))})

print(f'フレキシブルマッピング: {len(flex_pop)}エントリ')

def clean_address(address):
    """Clean duplicate prefecture names"""
    if not address or pd.isna(address):
        return ''

    address = str(address)

    # Remove duplicate prefecture pattern: "愛知県愛知県" -> "愛知県"
    prefectures = ['北海道', '青森県', '岩手県', '宮城県', '秋田県', '山形県', '福島県',
                   '茨城県', '栃木県', '群馬県', '埼玉県', '千葉県', '東京都', '神奈川県',
                   '新潟県', '富山県', '石川県', '福井県', '山梨県', '長野県', '岐阜県',
                   '静岡県', '愛知県', '三重県', '滋賀県', '京都府', '大阪府', '兵庫県',
                   '奈良県', '和歌山県', '鳥取県', '島根県', '岡山県', '広島県', '山口県',
                   '徳島県', '香川県', '愛媛県', '高知県', '福岡県', '佐賀県', '長崎県',
                   '熊本県', '大分県', '宮崎県', '鹿児島県', '沖縄県']

    for pref in prefectures:
        # Replace duplicate
        address = address.replace(f'{pref}{pref}', pref)

    return address

def extract_municipality_v2(address):
    """Improved municipality extraction"""
    if not address or pd.isna(address):
        return None

    address = clean_address(str(address))

    # 東京23区パターン
    tokyo_wards = ['千代田区', '中央区', '港区', '新宿区', '文京区', '台東区', '墨田区',
                   '江東区', '品川区', '目黒区', '大田区', '世田谷区', '渋谷区', '中野区',
                   '杉並区', '豊島区', '北区', '荒川区', '板橋区', '練馬区', '足立区',
                   '葛飾区', '江戸川区']
    for ward in tokyo_wards:
        if ward in address:
            return ward

    # 政令指定都市パターン (〇〇市〇〇区 → 〇〇市を返す)
    seirei_cities = ['札幌市', '仙台市', 'さいたま市', '千葉市', '横浜市', '川崎市',
                     '相模原市', '新潟市', '静岡市', '浜松市', '名古屋市', '京都市',
                     '大阪市', '堺市', '神戸市', '岡山市', '広島市', '北九州市', '福岡市', '熊本市']
    for city in seirei_cities:
        if city in address:
            return city

    # 通常の市 (最初にマッチした市を返す)
    match = re.search(r'([^\s　県都府道]+市)', address)
    if match:
        city = match.group(1)
        # Exclude prefecture names that end with 市 (none in Japan, but safety)
        if city not in ['東京市']:  # safety check
            return city

    # 町
    match = re.search(r'([^\s　県都府道郡]+町)', address)
    if match:
        return match.group(1)

    # 村
    match = re.search(r'([^\s　県都府道郡]+村)', address)
    if match:
        return match.group(1)

    return None

def get_population_v2(address, flex_pop):
    """Get population with improved matching"""
    municipality = extract_municipality_v2(address)
    if not municipality:
        return None, None

    # Direct match
    if municipality in flex_pop:
        return municipality, int(flex_pop[municipality])

    # Try partial matches
    for key, pop in flex_pop.items():
        if municipality in key or key in municipality:
            return key, int(pop) if not pd.isna(pop) else None

    return municipality, None

# Use Street column directly (don't prepend Prefecture - it causes duplication)
address_col = 'Street'

leads['municipality'] = None
leads['population'] = None

for idx, row in leads.iterrows():
    # Use only Street, not Prefecture (it's already included or causes duplication)
    address = row.get(address_col, '') or ''

    muni, pop = get_population_v2(address, flex_pop)
    leads.at[idx, 'municipality'] = muni
    leads.at[idx, 'population'] = pop

# Convert population to numeric
leads['population'] = pd.to_numeric(leads['population'], errors='coerce')

# Check match rate
matched = leads['population'].notna().sum()
print(f'\n改善後マッチ率: {matched:,} / {len(leads):,} = {matched/len(leads)*100:.1f}%')

def get_pop_band(pop):
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

leads['pop_band'] = leads['population'].apply(get_pop_band)

print('\n=== 人口帯別分布 ===')
band_order = ['100万〜', '50〜100万', '30〜50万', '10〜30万', '5〜10万', '〜5万', '不明']
for band in band_order:
    count = (leads['pop_band'] == band).sum()
    pct = count / len(leads) * 100
    print(f'{band}: {count:,}件 ({pct:.1f}%)')

print('\n=== 人口しきい値別の件数 ===')
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

print('\n=== TOP20市区町村 ===')
muni_counts = leads[leads['municipality'].notna()]['municipality'].value_counts().head(20)
for muni, count in muni_counts.items():
    pop = flex_pop.get(muni, 'N/A')
    if isinstance(pop, (int, float)) and not pd.isna(pop):
        print(f'{muni}: {count:,}件 (人口: {int(pop):,}人)')
    else:
        print(f'{muni}: {count:,}件')

# Save enriched CSV
output_path = base / 'data/output/hellowork/construction_new_leads_with_pop_v2_20260203.csv'
leads.to_csv(output_path, index=False, encoding='utf-8-sig')
print(f'\n保存: {output_path}')
