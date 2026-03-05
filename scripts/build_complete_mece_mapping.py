#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
完全なMECEハローワーク管轄マッピングを構築するスクリプト

戦略:
1. 管轄区域情報から直接マッピング
2. 政令指定都市の区を市にマッピング
3. 漏れた市区町村には都道府県代表ハローワークをフォールバック
"""

import pandas as pd
import re
from pathlib import Path
from collections import defaultdict
import warnings
warnings.filterwarnings('ignore')


# 都道府県の代表ハローワーク（県庁所在地）
PREFECTURE_MAIN_HW = {
    "北海道": "札幌",
    "青森県": "青森",
    "岩手県": "盛岡",
    "宮城県": "仙台",
    "秋田県": "秋田",
    "山形県": "やまがた",
    "福島県": "福島",
    "茨城県": "水戸",
    "栃木県": "宇都宮",
    "群馬県": "前橋",
    "埼玉県": "大宮",
    "千葉県": "千葉",
    "東京都": "新宿",
    "神奈川県": "横浜",
    "新潟県": "新潟",
    "富山県": "富山",
    "石川県": "金沢",
    "福井県": "福井",
    "山梨県": "甲府",
    "長野県": "長野",
    "岐阜県": "岐阜",
    "静岡県": "静岡",
    "愛知県": "名古屋中",
    "三重県": "津",
    "滋賀県": "大津",
    "京都府": "西陣",
    "大阪府": "梅田",
    "兵庫県": "神戸",
    "奈良県": "奈良",
    "和歌山県": "和歌山",
    "鳥取県": "鳥取",
    "島根県": "松江",
    "岡山県": "岡山",
    "広島県": "広島",
    "山口県": "山口",
    "徳島県": "徳島",
    "香川県": "高松",
    "愛媛県": "松山",
    "高知県": "高知",
    "福岡県": "福岡中央",
    "佐賀県": "佐賀",
    "長崎県": "長崎",
    "熊本県": "熊本",
    "大分県": "大分",
    "宮崎県": "宮崎",
    "鹿児島県": "鹿児島",
    "沖縄県": "那覇",
}

# 政令指定都市と区の対応
SEIREI_CITIES = {
    "札幌市": ["中央区", "北区", "東区", "白石区", "厚別区", "豊平区", "清田区", "南区", "西区", "手稲区"],
    "仙台市": ["青葉区", "宮城野区", "若林区", "太白区", "泉区"],
    "さいたま市": ["西区", "北区", "大宮区", "見沼区", "中央区", "桜区", "浦和区", "南区", "緑区", "岩槻区"],
    "千葉市": ["中央区", "花見川区", "稲毛区", "若葉区", "緑区", "美浜区"],
    "横浜市": ["鶴見区", "神奈川区", "西区", "中区", "南区", "港南区", "保土ケ谷区", "旭区", "磯子区", "金沢区", "港北区", "緑区", "青葉区", "都筑区", "戸塚区", "栄区", "泉区", "瀬谷区"],
    "川崎市": ["川崎区", "幸区", "中原区", "高津区", "宮前区", "多摩区", "麻生区"],
    "相模原市": ["緑区", "中央区", "南区"],
    "新潟市": ["北区", "東区", "中央区", "江南区", "秋葉区", "南区", "西区", "西蒲区"],
    "静岡市": ["葵区", "駿河区", "清水区"],
    "浜松市": ["中央区", "浜名区", "天竜区"],
    "名古屋市": ["千種区", "東区", "北区", "西区", "中村区", "中区", "昭和区", "瑞穂区", "熱田区", "中川区", "港区", "南区", "守山区", "緑区", "名東区", "天白区"],
    "京都市": ["北区", "上京区", "左京区", "中京区", "東山区", "下京区", "南区", "右京区", "伏見区", "山科区", "西京区"],
    "大阪市": ["都島区", "福島区", "此花区", "西区", "港区", "大正区", "天王寺区", "浪速区", "西淀川区", "東淀川区", "東成区", "生野区", "旭区", "城東区", "阿倍野区", "住吉区", "東住吉区", "西成区", "淀川区", "鶴見区", "住之江区", "平野区", "北区", "中央区"],
    "堺市": ["堺区", "中区", "東区", "西区", "南区", "北区", "美原区"],
    "神戸市": ["東灘区", "灘区", "兵庫区", "長田区", "須磨区", "垂水区", "北区", "中央区", "西区"],
    "岡山市": ["北区", "中区", "東区", "南区"],
    "広島市": ["中区", "東区", "南区", "西区", "安佐南区", "安佐北区", "安芸区", "佐伯区"],
    "北九州市": ["門司区", "若松区", "戸畑区", "小倉北区", "小倉南区", "八幡東区", "八幡西区"],
    "福岡市": ["東区", "博多区", "中央区", "南区", "西区", "城南区", "早良区"],
    "熊本市": ["中央区", "東区", "西区", "南区", "北区"],
}

# 手動マッピング: 管轄区域に明記されていないが調査で判明したマッピング
# キー: (都道府県, 市区町村名), 値: ハローワーク名
MANUAL_MAPPING = {
    # 北海道
    ("北海道", "神恵内村"): "小樽",  # 後志地域
    ("北海道", "美瑛町"): "旭川",  # 上川地域
    ("北海道", "幌加内町"): "旭川",  # 上川地域
    ("北海道", "幌延町"): "稚内",  # 宗谷地域
    ("北海道", "弟子屈町"): "釧路",  # 釧路地域
    ("北海道", "白糠町"): "釧路",  # 釧路地域
    ("北海道", "蘂取村"): "根室",  # 北方領土（便宜上）
    # 青森県
    ("青森県", "六ヶ所村"): "三沢",  # 上北地域
    # 福島県
    ("福島県", "鏡石町"): "須賀川",  # 岩瀬地域
    # 群馬県
    ("群馬県", "上野村"): "藤岡",  # 多野地域
    ("群馬県", "中之条町"): "中之条",  # 吾妻地域
    # 埼玉県
    ("埼玉県", "桶川市"): "大宮",  # 県央地域
    # 千葉県
    ("千葉県", "白井市"): "船橋",  # 東葛地域
    ("千葉県", "東庄町"): "銚子",  # 海匝地域
    # 東京都
    ("東京都", "青ヶ島村"): "八王子",  # 島嶼部（便宜上）
    # 山梨県
    ("山梨県", "早川町"): "甲府",  # 南巨摩地域
    # 長野県
    ("長野県", "根羽村"): "飯田",  # 下伊那地域
    ("長野県", "大鹿村"): "飯田",  # 下伊那地域
    ("長野県", "大桑村"): "木曽福島",  # 木曽地域
    ("長野県", "木曽町"): "木曽福島",  # 木曽地域
    # 静岡県
    ("静岡県", "川根本町"): "島田",  # 榛原地域
    # 愛知県
    ("愛知県", "東浦町"): "半田",  # 知多地域
    # 三重県
    ("三重県", "川越町"): "四日市",  # 三泗地域
    ("三重県", "御浜町"): "尾鷲",  # 東紀州地域
    # 京都府
    ("京都府", "与謝野町"): "峰山",  # 丹後地域
    # 兵庫県
    ("兵庫県", "稲美町"): "明石",  # 東播地域
    # 鳥取県
    ("鳥取県", "琴浦町"): "倉吉",  # 中部地域
    # 香川県
    ("香川県", "土庄町"): "小豆",  # 小豆地域
    ("香川県", "琴平町"): "丸亀",  # 仲多度地域
    ("香川県", "まんのう町"): "丸亀",  # 仲多度地域
    # 高知県
    ("高知県", "馬路村"): "安芸",  # 安芸地域
    ("高知県", "梼原町"): "四万十",  # 高幡地域
    ("高知県", "津野町"): "四万十",  # 高幡地域
    # 福岡県
    ("福岡県", "須恵町"): "福岡東",  # 糟屋地域
    ("福岡県", "筑前町"): "甘木",  # 朝倉地域
    # 佐賀県
    ("佐賀県", "みやき町"): "鳥栖",  # 三神地域
    # 熊本県
    ("熊本県", "和水町"): "玉名",  # 玉名地域
    ("熊本県", "菊陽町"): "菊池",  # 菊池地域
    # 鹿児島県
    ("鹿児島県", "南種子町"): "熊毛",  # 熊毛地域
    ("鹿児島県", "喜界町"): "名瀬",  # 奄美地域
    ("鹿児島県", "与論町"): "名瀬",  # 奄美地域
}

# 政令指定都市の区→ハローワーク対応（区ごとの詳細マッピング）
# 同じ市の他の区と同じハローワークが管轄するケースが多い
SEIREI_WARD_HW = {
    # 横浜市（横浜、港北、戸塚）
    ("神奈川県", "横浜市鶴見区"): "横浜",
    # 川崎市（川崎、川崎北）
    ("神奈川県", "川崎市宮前区"): "川崎北",
    # 浜松市（浜松）
    ("静岡県", "浜松市中央区"): "浜松",
    # 名古屋市（名古屋中、名古屋東、名古屋南、名古屋北、熱田）
    ("愛知県", "名古屋市名東区"): "名古屋東",
    # 京都市（西陣、園部、伏見）
    ("京都府", "京都市北区"): "西陣",
    ("京都府", "京都市中京区"): "西陣",
    # 大阪市（梅田、大阪東、阿倍野、大阪西、淀川）
    ("大阪府", "大阪市住之江区"): "阿倍野",
    # 神戸市（神戸、三宮、灘、西神）
    ("兵庫県", "神戸市須磨区"): "神戸",
    # 福岡市（福岡中央、福岡東、福岡南、福岡西）
    ("福岡県", "福岡市南区"): "福岡南",
}


def load_additional_manual_mapping(file_path: Path) -> dict:
    """追加の手動マッピングをCSVから読み込み"""
    if not file_path.exists():
        return {}

    df = pd.read_csv(file_path, encoding='utf-8-sig')
    mapping = {}
    for _, row in df.iterrows():
        key = (row['prefecture'], row['municipality'])
        mapping[key] = row['hellowork_name']
    return mapping


def load_soumu_master(file_path: Path) -> pd.DataFrame:
    """総務省の市区町村コードマスタを読み込み"""
    df_all = pd.read_excel(file_path, sheet_name=0)
    df_seirei = pd.read_excel(file_path, sheet_name=1)

    df_all.columns = ['団体コード', '都道府県名', '市区町村名', '都道府県名カナ', '市区町村名カナ']
    df_seirei.columns = ['団体コード', '都道府県名', '市区町村名', '都道府県名カナ', '市区町村名カナ']

    df_municipalities = df_all[df_all['市区町村名'].notna()].copy()
    df_municipalities = pd.concat([df_municipalities, df_seirei], ignore_index=True)
    df_municipalities = df_municipalities.drop_duplicates(subset=['団体コード'])

    return df_municipalities


def load_hellowork_data(data_dir: Path) -> pd.DataFrame:
    """ハローワークデータを読み込み"""
    csv_files = list(data_dir.glob("job_ratio_*.csv"))
    csv_files = [f for f in csv_files if "_all_" not in f.name and "_analysis" not in f.name and "mece" not in f.name and "missing" not in f.name]

    all_data = []
    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file, encoding='utf-8-sig')
            all_data.append(df)
        except Exception as e:
            print(f"  読み込みエラー: {csv_file.name} - {e}")

    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return pd.DataFrame()


def load_supplementary_ratio_data(file_path: Path) -> dict:
    """補完用求人倍率データを読み込み"""
    if not file_path.exists():
        return {}

    df = pd.read_csv(file_path, encoding='utf-8-sig')
    supplementary = {}
    for _, row in df.iterrows():
        pref = row['prefecture']
        hw_name = row['hellowork_name']
        # 北海道の場合は正規化
        if '北海道' in pref:
            pref = '北海道'
        key = (pref, hw_name)
        supplementary[key] = {
            'ratio': row['ratio'],
            'year': row.get('year', 2025),
            'month': row.get('month', 11),
            'source': row.get('source', 'supplementary'),
        }
    return supplementary


def apply_supplementary_ratios(hw_df: pd.DataFrame, supplementary: dict) -> pd.DataFrame:
    """補完データをハローワークデータに適用"""
    if not supplementary:
        return hw_df

    hw_df = hw_df.copy()
    updated_count = 0

    for idx, row in hw_df.iterrows():
        pref = normalize_prefecture(row['prefecture'])
        hw_name = row['hellowork_name']
        key = (pref, hw_name)

        # 既存のratioが空で、補完データがある場合に適用
        if pd.isna(row['ratio']) and key in supplementary:
            supp = supplementary[key]
            hw_df.at[idx, 'ratio'] = supp['ratio']
            hw_df.at[idx, 'year'] = supp['year']
            hw_df.at[idx, 'month'] = supp['month']
            updated_count += 1

    print(f"  補完データ適用: {updated_count}件の求人倍率を補完")
    return hw_df


def normalize_prefecture(pref: str) -> str:
    """都道府県名を正規化"""
    if pd.isna(pref):
        return ""
    if "北海道" in pref:
        return "北海道"
    return pref.strip()


def extract_municipalities_advanced(jurisdiction: str, prefecture: str) -> list[dict]:
    """管轄区域から市区町村を抽出（改善版）"""
    if pd.isna(jurisdiction) or not jurisdiction:
        return []

    results = []

    # 1. 政令指定都市の区を展開（例: 札幌市（中央区、南区））
    city_ward_pattern = r'([^、（）]+市)（([^）]+)）'
    for match in re.finditer(city_ward_pattern, jurisdiction):
        city = match.group(1)
        wards_str = match.group(2)
        if 'を除く' in wards_str:
            continue
        for ward in re.split(r'[、,・]', wards_str):
            ward = ward.strip()
            if ward and re.search(r'区$', ward):
                full_name = f"{city}{ward}"
                results.append({'name': full_name, 'type': 'ward', 'city': city})

    # 2. 郡（町、村）形式を展開（例: 上水内郡（信濃町、小川村、飯綱町））
    gun_pattern = r'([^、（）]+郡)（([^）]+)）'
    for match in re.finditer(gun_pattern, jurisdiction):
        gun = match.group(1)
        towns_str = match.group(2)
        if 'を除く' in towns_str:
            continue
        for town in re.split(r'[、,・]', towns_str):
            town = town.strip()
            if town and re.search(r'(町|村)$', town):
                # 総務省マスタでは「町名」のみ（郡名なし）なので町名のみを追加
                results.append({'name': town, 'type': 'town' if '町' in town else 'village', 'city': None})

    # 3. 「郡町名」形式（括弧なし）を処理（例: 埴科郡坂城町）
    gun_town_pattern = r'([^、（）]+郡)([^、（）]+(?:町|村))'
    for match in re.finditer(gun_town_pattern, jurisdiction):
        gun = match.group(1)
        town = match.group(2)
        # 町名のみを追加
        results.append({'name': town, 'type': 'town' if '町' in town else 'village', 'city': None})

    # 4. 括弧を除去してから市・町・村を抽出
    clean_jurisdiction = re.sub(r'（[^）]+）', '', jurisdiction)
    clean_jurisdiction = re.sub(r'を除く', '', clean_jurisdiction)
    parts = re.split(r'[、,・]', clean_jurisdiction)

    for part in parts:
        part = part.strip()
        if not part:
            continue

        # 市で終わるもの
        if re.search(r'市$', part):
            results.append({'name': part, 'type': 'city', 'city': None})
        # 町で終わるもの（郡名付きは既に処理済みなのでスキップ）
        elif re.search(r'町$', part) and '郡' not in part:
            results.append({'name': part, 'type': 'town', 'city': None})
        # 村で終わるもの（郡名付きは既に処理済みなのでスキップ）
        elif re.search(r'村$', part) and '郡' not in part:
            results.append({'name': part, 'type': 'village', 'city': None})
        # 東京23区
        elif re.search(r'区$', part) and prefecture == "東京都":
            results.append({'name': part, 'type': 'special_ward', 'city': None})

    # 重複削除
    seen = set()
    unique_results = []
    for r in results:
        if r['name'] not in seen:
            seen.add(r['name'])
            unique_results.append(r)

    return unique_results


def get_prefecture_hw_data(hw_df: pd.DataFrame) -> dict:
    """都道府県別のハローワークデータを取得"""
    pref_hw = {}

    for _, row in hw_df.iterrows():
        pref = normalize_prefecture(row['prefecture'])
        hw_name = row['hellowork_name']
        ratio = row['ratio']
        year = row.get('year')
        month = row.get('month')

        if pref not in pref_hw:
            pref_hw[pref] = []

        pref_hw[pref].append({
            'hellowork_name': hw_name,
            'ratio': ratio,
            'year': year,
            'month': month,
        })

    return pref_hw


def find_main_hw_for_prefecture(pref: str, hw_df: pd.DataFrame) -> dict:
    """都道府県の代表ハローワークを見つける"""
    main_hw_name = PREFECTURE_MAIN_HW.get(pref)

    if main_hw_name:
        matches = hw_df[
            (hw_df['prefecture'].apply(normalize_prefecture) == pref) &
            (hw_df['hellowork_name'] == main_hw_name)
        ]
        if len(matches) > 0:
            row = matches.iloc[0]
            return {
                'hellowork_name': row['hellowork_name'],
                'ratio': row['ratio'],
                'year': row.get('year'),
                'month': row.get('month'),
            }

    # フォールバック: その都道府県の最初のハローワーク
    matches = hw_df[hw_df['prefecture'].apply(normalize_prefecture) == pref]
    if len(matches) > 0:
        # 求人倍率があるものを優先
        valid_matches = matches[matches['ratio'].notna()]
        if len(valid_matches) > 0:
            row = valid_matches.iloc[0]
        else:
            row = matches.iloc[0]
        return {
            'hellowork_name': row['hellowork_name'],
            'ratio': row['ratio'],
            'year': row.get('year'),
            'month': row.get('month'),
        }

    return None


def get_hw_data_by_name(hw_name: str, hw_df: pd.DataFrame) -> dict:
    """ハローワーク名からデータを取得"""
    matches = hw_df[hw_df['hellowork_name'] == hw_name]
    if len(matches) > 0:
        row = matches.iloc[0]
        return {
            'hellowork_name': row['hellowork_name'],
            'ratio': row['ratio'],
            'year': row.get('year'),
            'month': row.get('month'),
        }
    return None


def build_complete_mece_mapping(hw_df: pd.DataFrame, master_df: pd.DataFrame, additional_mapping: dict = None) -> tuple[pd.DataFrame, dict]:
    """完全なMECEマッピングを構築"""
    if additional_mapping is None:
        additional_mapping = {}
    
    mapping_rows = []
    stats = {
        'total_master': len(master_df),
        'direct_mapped': 0,
        'seirei_mapped': 0,
        'manual_mapped': 0,
        'seirei_ward_mapped': 0,
        'fallback_mapped': 0,
        'match_types': defaultdict(int),
    }

    # 市区町村→ハローワーク直接マッピング
    direct_mapping = {}  # municipality_code → hw_data

    for _, hw_row in hw_df.iterrows():
        prefecture = hw_row['prefecture']
        hellowork_name = hw_row['hellowork_name']
        jurisdiction = hw_row['jurisdiction']
        ratio = hw_row['ratio']
        year = hw_row.get('year')
        month = hw_row.get('month')

        municipalities = extract_municipalities_advanced(jurisdiction, prefecture)

        for muni in municipalities:
            muni_name = muni['name']
            norm_pref = normalize_prefecture(prefecture)

            # マスタ照合
            matches = master_df[
                (master_df['市区町村名'] == muni_name) &
                (master_df['都道府県名'] == norm_pref)
            ]

            if len(matches) == 1:
                code = matches.iloc[0]['団体コード']
                if code not in direct_mapping:
                    direct_mapping[code] = {
                        'municipality_code': code,
                        'prefecture': norm_pref,
                        'municipality': muni_name,
                        'hellowork_name': hellowork_name,
                        'ratio': ratio,
                        'year': year,
                        'month': month,
                        'match_type': 'direct',
                    }
                continue

            # 郡名を除去して再照合
            if '郡' in muni_name:
                gun_match = re.match(r'.+郡(.+)', muni_name)
                if gun_match:
                    town_only = gun_match.group(1)
                    matches = master_df[
                        (master_df['市区町村名'] == town_only) &
                        (master_df['都道府県名'] == norm_pref)
                    ]
                    if len(matches) == 1:
                        code = matches.iloc[0]['団体コード']
                        if code not in direct_mapping:
                            direct_mapping[code] = {
                                'municipality_code': code,
                                'prefecture': norm_pref,
                                'municipality': matches.iloc[0]['市区町村名'],
                                'hellowork_name': hellowork_name,
                                'ratio': ratio,
                                'year': year,
                                'month': month,
                                'match_type': 'gun_removed',
                            }

            # 政令指定都市の市名から区を展開
            if muni['type'] == 'city' and muni_name in SEIREI_CITIES:
                wards = SEIREI_CITIES[muni_name]
                for ward in wards:
                    full_ward_name = f"{muni_name}{ward}"
                    ward_matches = master_df[
                        (master_df['市区町村名'] == full_ward_name) &
                        (master_df['都道府県名'] == norm_pref)
                    ]
                    if len(ward_matches) == 1:
                        code = ward_matches.iloc[0]['団体コード']
                        if code not in direct_mapping:
                            direct_mapping[code] = {
                                'municipality_code': code,
                                'prefecture': norm_pref,
                                'municipality': full_ward_name,
                                'hellowork_name': hellowork_name,
                                'ratio': ratio,
                                'year': year,
                                'month': month,
                                'match_type': 'seirei_expansion',
                            }

    stats['direct_mapped'] = len([m for m in direct_mapping.values() if m['match_type'] in ['direct', 'gun_removed']])
    stats['seirei_mapped'] = len([m for m in direct_mapping.values() if m['match_type'] == 'seirei_expansion'])

    # 全マスタ市区町村に対してマッピング
    for _, master_row in master_df.iterrows():
        code = master_row['団体コード']
        pref = master_row['都道府県名']
        muni = master_row['市区町村名']

        if code in direct_mapping:
            mapping_rows.append(direct_mapping[code])
            stats['match_types'][direct_mapping[code]['match_type']] += 1
        else:
            # 優先度1: 手動マッピング
            key = (pref, muni)
            if key in MANUAL_MAPPING:
                hw_name = MANUAL_MAPPING[key]
                hw_data = get_hw_data_by_name(hw_name, hw_df)
                if hw_data:
                    mapping_rows.append({
                        'municipality_code': code,
                        'prefecture': pref,
                        'municipality': muni,
                        'hellowork_name': hw_data['hellowork_name'],
                        'ratio': hw_data['ratio'],
                        'year': hw_data['year'],
                        'month': hw_data['month'],
                        'match_type': 'manual',
                    })
                    stats['manual_mapped'] += 1
                    stats['match_types']['manual'] += 1
                    continue

            # 優先度2: 政令指定都市区マッピング
            if key in SEIREI_WARD_HW:
                hw_name = SEIREI_WARD_HW[key]
                hw_data = get_hw_data_by_name(hw_name, hw_df)
                if hw_data:
                    mapping_rows.append({
                        'municipality_code': code,
                        'prefecture': pref,
                        'municipality': muni,
                        'hellowork_name': hw_data['hellowork_name'],
                        'ratio': hw_data['ratio'],
                        'year': hw_data['year'],
                        'month': hw_data['month'],
                        'match_type': 'seirei_ward',
                    })
                    stats['seirei_ward_mapped'] += 1
                    stats['match_types']['seirei_ward'] += 1
                    continue

            # 優先度3: 政令指定都市の区で同じ市の他の区からマッピングを借りる
            borrowed = False
            for city_name, wards in SEIREI_CITIES.items():
                for ward in wards:
                    full_ward_name = f"{city_name}{ward}"
                    if muni == full_ward_name:
                        # 同じ市の他の区を探す
                        for other_ward in wards:
                            other_full_name = f"{city_name}{other_ward}"
                            other_match = master_df[
                                (master_df['市区町村名'] == other_full_name) &
                                (master_df['都道府県名'] == pref)
                            ]
                            if len(other_match) > 0:
                                other_code = other_match.iloc[0]['団体コード']
                                if other_code in direct_mapping:
                                    hw_data = direct_mapping[other_code]
                                    mapping_rows.append({
                                        'municipality_code': code,
                                        'prefecture': pref,
                                        'municipality': muni,
                                        'hellowork_name': hw_data['hellowork_name'],
                                        'ratio': hw_data['ratio'],
                                        'year': hw_data['year'],
                                        'month': hw_data['month'],
                                        'match_type': 'seirei_borrowed',
                                    })
                                    stats['match_types']['seirei_borrowed'] += 1
                                    borrowed = True
                                    break
                        if borrowed:
                            break
                if borrowed:
                    break

            if borrowed:
                continue

            # 優先度4: 追加手動マッピング（CSVから読み込み）
            if key in additional_mapping:
                hw_name = additional_mapping[key]
                hw_data = get_hw_data_by_name(hw_name, hw_df)
                if hw_data:
                    mapping_rows.append({
                        'municipality_code': code,
                        'prefecture': pref,
                        'municipality': muni,
                        'hellowork_name': hw_data['hellowork_name'],
                        'ratio': hw_data['ratio'],
                        'year': hw_data['year'],
                        'month': hw_data['month'],
                        'match_type': 'additional_manual',
                    })
                    stats['match_types']['additional_manual'] += 1
                    continue

            # 北方領土チェック（ハローワークが存在しない地域）
            NORTHERN_TERRITORIES = ['色丹村', '留夜別村', '留別村', '紗那村', '蘂取村']
            if muni in NORTHERN_TERRITORIES:
                # 北方領土は空欄（未対応）として登録
                mapping_rows.append({
                    'municipality_code': code,
                    'prefecture': pref,
                    'municipality': muni,
                    'hellowork_name': None,
                    'ratio': None,
                    'year': None,
                    'month': None,
                    'match_type': 'no_hellowork',
                })
                stats['match_types']['no_hellowork'] += 1
                continue

            # フォールバック: 都道府県代表ハローワーク
            main_hw = find_main_hw_for_prefecture(pref, hw_df)
            if main_hw:
                mapping_rows.append({
                    'municipality_code': code,
                    'prefecture': pref,
                    'municipality': muni,
                    'hellowork_name': main_hw['hellowork_name'],
                    'ratio': main_hw['ratio'],
                    'year': main_hw['year'],
                    'month': main_hw['month'],
                    'match_type': 'fallback',
                })
                stats['fallback_mapped'] += 1
                stats['match_types']['fallback'] += 1

    mapping_df = pd.DataFrame(mapping_rows)

    return mapping_df, stats


def main():
    """メイン処理"""
    data_dir = Path(__file__).parent.parent / "data" / "job_openings_ratio"
    soumu_file = data_dir / "soumu_municipality_codes.xls"

    print("=" * 70)
    print("完全なMECEハローワーク管轄マッピング構築")
    print("=" * 70)

    # 総務省マスタ読み込み
    print("\n[1] 総務省市区町村マスタ読み込み")
    master_df = load_soumu_master(soumu_file)
    print(f"  市区町村数: {len(master_df)}")

    # ハローワークデータ読み込み
    print("\n[2] ハローワーク管轄データ読み込み")
    hw_df = load_hellowork_data(data_dir)
    print(f"  ハローワーク事業所数: {len(hw_df)}")
    print(f"  求人倍率取得済み（補完前）: {len(hw_df[hw_df['ratio'].notna()])}")

    # 補完用求人倍率データ読み込み・適用
    print("\n[2.5] 補完用求人倍率データ読み込み")
    supplementary_file = data_dir / "supplementary_ratio_data.csv"
    if supplementary_file.exists():
        supplementary = load_supplementary_ratio_data(supplementary_file)
        print(f"  補完データ件数: {len(supplementary)}")
        hw_df = apply_supplementary_ratios(hw_df, supplementary)
        print(f"  求人倍率取得済み（補完後）: {len(hw_df[hw_df['ratio'].notna()])}")
    else:
        print("  補完データファイルなし")

    # 追加手動マッピング読み込み
    print("\n[2.6] 追加手動マッピング読み込み")
    additional_mapping_file = data_dir / "additional_manual_mapping.csv"
    additional_mapping = load_additional_manual_mapping(additional_mapping_file)
    print(f"  追加マッピング件数: {len(additional_mapping)}")

    # 完全なMECEマッピング構築
    print("\n[3] 完全なMECEマッピング構築")
    mapping_df, stats = build_complete_mece_mapping(hw_df, master_df, additional_mapping)

    print(f"\n  マッピング結果:")
    print(f"    - 総務省マスタ市区町村: {stats['total_master']}")
    print(f"    - 直接マッピング: {stats['direct_mapped']}")
    print(f"    - 政令指定都市展開: {stats['seirei_mapped']}")
    print(f"    - 手動マッピング: {stats['manual_mapped']}")
    print(f"    - 政令指定都市区マッピング: {stats['seirei_ward_mapped']}")
    print(f"    - 都道府県フォールバック: {stats['fallback_mapped']}")
    print(f"    - 合計: {len(mapping_df)}")

    print(f"\n  マッチタイプ別:")
    for match_type, count in stats['match_types'].items():
        print(f"    - {match_type}: {count}")

    # カバレッジ計算
    coverage = len(mapping_df) / stats['total_master'] * 100 if stats['total_master'] > 0 else 0
    print(f"\n  カバレッジ率: {coverage:.1f}%")

    # 求人倍率の分布
    if len(mapping_df) > 0:
        print(f"\n  求人倍率の分布:")
        valid_ratios = mapping_df[mapping_df['ratio'].notna()]['ratio']
        print(f"    - 有効データ: {len(valid_ratios)}件")
        print(f"    - 最小: {valid_ratios.min():.2f}倍")
        print(f"    - 最大: {valid_ratios.max():.2f}倍")
        print(f"    - 平均: {valid_ratios.mean():.2f}倍")
        print(f"    - 中央値: {valid_ratios.median():.2f}倍")

    # CSV出力
    print("\n[4] マッピングテーブル出力")
    output_path = data_dir / "complete_mece_municipality_hellowork_mapping.csv"
    mapping_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"  出力先: {output_path}")
    print(f"  レコード数: {len(mapping_df)}")

    # サンプル出力
    print("\n[5] マッピングサンプル（各マッチタイプ）")
    for match_type in ['direct', 'gun_removed', 'seirei_expansion', 'manual', 'seirei_ward', 'seirei_borrowed', 'additional_manual', 'fallback', 'no_hellowork']:
        sample = mapping_df[mapping_df['match_type'] == match_type].head(3)
        if len(sample) > 0:
            print(f"\n  {match_type}:")
            for _, row in sample.iterrows():
                print(f"    {row['prefecture']} {row['municipality']} → {row['hellowork_name']} ({row['ratio']}倍)")

    # MECE検証
    print("\n[6] MECE検証")
    # 漏れチェック
    missing = stats['total_master'] - len(mapping_df)
    print(f"  漏れ: {missing}件")

    # 重複チェック
    duplicates = mapping_df[mapping_df.duplicated(subset=['municipality_code'], keep=False)]
    print(f"  重複: {len(duplicates)}件")

    if missing == 0 and len(duplicates) == 0:
        print("\n  [OK] MECEが達成されました（漏れなく・ダブりなく）")
    else:
        print("\n  [NG] MECE未達成")

    print("\n" + "=" * 70)
    print("構築完了")
    print("=" * 70)

    return mapping_df


if __name__ == "__main__":
    main()
