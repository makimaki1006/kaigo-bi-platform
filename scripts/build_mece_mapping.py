#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
MECEなハローワーク管轄マッピングを構築するスクリプト

1. 総務省の全国市区町村マスタを読み込み
2. 収集したハローワーク管轄区域データと照合
3. 漏れ・重複をチェック
4. Salesforce連携用のMECEマッピングテーブルを作成
"""

import pandas as pd
import re
from pathlib import Path
from collections import defaultdict
import warnings
warnings.filterwarnings('ignore')


def load_soumu_master(file_path: Path) -> pd.DataFrame:
    """総務省の市区町村コードマスタを読み込み"""
    # シート1: 全団体
    df_all = pd.read_excel(file_path, sheet_name=0)
    # シート2: 政令指定都市の区
    df_seirei = pd.read_excel(file_path, sheet_name=1)

    # カラム名を正規化
    df_all.columns = ['団体コード', '都道府県名', '市区町村名', '都道府県名カナ', '市区町村名カナ']
    df_seirei.columns = ['団体コード', '都道府県名', '市区町村名', '都道府県名カナ', '市区町村名カナ']

    # 都道府県のみの行（市区町村名がNaN）を除外
    df_municipalities = df_all[df_all['市区町村名'].notna()].copy()

    # 政令指定都市の区を追加
    df_municipalities = pd.concat([df_municipalities, df_seirei], ignore_index=True)

    # 重複削除（団体コードベース）
    df_municipalities = df_municipalities.drop_duplicates(subset=['団体コード'])

    return df_municipalities


def load_hellowork_data(data_dir: Path) -> pd.DataFrame:
    """収集したハローワークデータを読み込み"""
    csv_files = list(data_dir.glob("job_ratio_*.csv"))
    # 統合ファイル（all_*.csv）を除外
    csv_files = [f for f in csv_files if "_all_" not in f.name and "_analysis" not in f.name]

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


def normalize_prefecture(pref: str) -> str:
    """都道府県名を正規化"""
    if pd.isna(pref):
        return ""
    # 北海道の地域表記を正規化
    if "北海道" in pref:
        return "北海道"
    return pref.strip()


def extract_municipalities_advanced(jurisdiction: str, prefecture: str) -> list[dict]:
    """
    管轄区域文字列から市区町村を詳細に抽出

    返り値: [{'name': '市区町村名', 'type': 'city/ward/town/village'}]
    """
    if pd.isna(jurisdiction) or not jurisdiction:
        return []

    results = []

    # 政令指定都市の区を展開
    # パターン1: "札幌市（中央区、南区、西区）"
    city_ward_pattern = r'([^、（）]+市)（([^）]+)）'
    for match in re.finditer(city_ward_pattern, jurisdiction):
        city = match.group(1)
        wards_str = match.group(2)
        # 「を除く」を含む場合は除外情報も保持
        if 'を除く' in wards_str:
            # 除外パターンを処理（複雑なので基本的にスキップ）
            continue
        # 区を分割
        for ward in re.split(r'[、,]', wards_str):
            ward = ward.strip()
            if ward and '区' in ward:
                # 「区」で終わるものだけ
                if re.search(r'区$', ward):
                    full_name = f"{city}{ward}"
                    results.append({'name': full_name, 'type': 'ward'})

    # 括弧なしの市区町村を抽出
    clean_jurisdiction = re.sub(r'（[^）]+）', '', jurisdiction)  # 括弧内を削除
    clean_jurisdiction = re.sub(r'を除く', '', clean_jurisdiction)  # 「を除く」を削除
    parts = re.split(r'[、,]', clean_jurisdiction)

    for part in parts:
        part = part.strip()
        if not part:
            continue

        # 市で終わるもの
        if re.search(r'市$', part):
            results.append({'name': part, 'type': 'city'})
        # 町で終わるもの（郡を含む場合は郡ごと）
        elif re.search(r'町$', part):
            results.append({'name': part, 'type': 'town'})
        # 村で終わるもの
        elif re.search(r'村$', part):
            results.append({'name': part, 'type': 'village'})
        # 特別区（東京23区）
        elif re.search(r'区$', part) and prefecture == "東京都":
            results.append({'name': part, 'type': 'special_ward'})
        # 郡（町名、町名）パターン
        elif '郡' in part:
            gun_match = re.match(r'(.+郡)(.+)', part)
            if gun_match:
                gun = gun_match.group(1)
                town = gun_match.group(2)
                if re.search(r'(町|村)$', town):
                    results.append({'name': part, 'type': 'town' if '町' in town else 'village'})

    # 重複削除
    seen = set()
    unique_results = []
    for r in results:
        if r['name'] not in seen:
            seen.add(r['name'])
            unique_results.append(r)

    return unique_results


def match_municipality_to_master(muni_name: str, master_df: pd.DataFrame, prefecture: str) -> dict:
    """市区町村名をマスタと照合"""
    # 完全一致を試みる
    matches = master_df[
        (master_df['市区町村名'] == muni_name) &
        (master_df['都道府県名'] == normalize_prefecture(prefecture))
    ]

    if len(matches) == 1:
        row = matches.iloc[0]
        return {
            'code': row['団体コード'],
            'prefecture': row['都道府県名'],
            'municipality': row['市区町村名'],
            'match_type': 'exact'
        }

    # 部分一致を試みる（政令指定都市の区）
    # "札幌市中央区" vs "中央区" のケース
    if '区' in muni_name:
        for _, row in master_df[master_df['都道府県名'] == normalize_prefecture(prefecture)].iterrows():
            master_muni = row['市区町村名']
            if pd.notna(master_muni):
                # マスタが "札幌市中央区" で、検索が "中央区" の場合
                if muni_name in master_muni or master_muni.endswith(muni_name):
                    return {
                        'code': row['団体コード'],
                        'prefecture': row['都道府県名'],
                        'municipality': row['市区町村名'],
                        'match_type': 'partial'
                    }
                # 逆パターン
                if master_muni in muni_name:
                    return {
                        'code': row['団体コード'],
                        'prefecture': row['都道府県名'],
                        'municipality': row['市区町村名'],
                        'match_type': 'partial'
                    }

    # 郡名を除去して再照合
    # "東津軽郡平内町" vs "平内町" のケース
    if '郡' in muni_name:
        gun_match = re.match(r'.+郡(.+)', muni_name)
        if gun_match:
            town_only = gun_match.group(1)
            matches = master_df[
                (master_df['市区町村名'] == town_only) &
                (master_df['都道府県名'] == normalize_prefecture(prefecture))
            ]
            if len(matches) == 1:
                row = matches.iloc[0]
                return {
                    'code': row['団体コード'],
                    'prefecture': row['都道府県名'],
                    'municipality': row['市区町村名'],
                    'match_type': 'gun_removed'
                }

    return None


def build_mece_mapping(hw_df: pd.DataFrame, master_df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """MECEマッピングを構築"""
    mapping_rows = []
    stats = {
        'total_hw_offices': len(hw_df),
        'mapped_municipalities': 0,
        'unmapped_municipalities': [],
        'duplicates': defaultdict(list),
        'match_types': defaultdict(int),
    }

    # 市区町村→ハローワーク の逆引きマップ（重複チェック用）
    muni_to_hw = defaultdict(list)

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

            # マスタ照合
            match_result = match_municipality_to_master(muni_name, master_df, prefecture)

            if match_result:
                mapping_rows.append({
                    'municipality_code': match_result['code'],
                    'prefecture': match_result['prefecture'],
                    'municipality': match_result['municipality'],
                    'hellowork_name': hellowork_name,
                    'ratio': ratio,
                    'year': year,
                    'month': month,
                    'match_type': match_result['match_type'],
                    'original_name': muni_name,
                })
                stats['match_types'][match_result['match_type']] += 1

                # 重複チェック用
                key = (match_result['code'], match_result['municipality'])
                muni_to_hw[key].append(hellowork_name)
            else:
                stats['unmapped_municipalities'].append({
                    'name': muni_name,
                    'prefecture': prefecture,
                    'hellowork': hellowork_name
                })

    # 重複チェック
    for key, hw_list in muni_to_hw.items():
        if len(hw_list) > 1:
            stats['duplicates'][key] = hw_list

    mapping_df = pd.DataFrame(mapping_rows)

    # 重複を除去（最初のハローワークを優先）
    if len(mapping_df) > 0:
        mapping_df = mapping_df.drop_duplicates(subset=['municipality_code'], keep='first')

    stats['mapped_municipalities'] = len(mapping_df)

    return mapping_df, stats


def check_coverage(mapping_df: pd.DataFrame, master_df: pd.DataFrame) -> dict:
    """カバレッジをチェック"""
    # マスタの市区町村（都道府県を除く）
    master_municipalities = set(master_df['団体コード'].astype(str))

    # マッピング済み市区町村
    if len(mapping_df) > 0:
        mapped_municipalities = set(mapping_df['municipality_code'].astype(str))
    else:
        mapped_municipalities = set()

    # 漏れ
    missing = master_municipalities - mapped_municipalities

    coverage = {
        'total_master': len(master_municipalities),
        'total_mapped': len(mapped_municipalities),
        'coverage_rate': len(mapped_municipalities) / len(master_municipalities) * 100 if len(master_municipalities) > 0 else 0,
        'missing_count': len(missing),
        'missing_codes': list(missing)[:50],  # 最初の50件
    }

    return coverage


def main():
    """メイン処理"""
    data_dir = Path(__file__).parent.parent / "data" / "job_openings_ratio"
    soumu_file = data_dir / "soumu_municipality_codes.xls"

    print("=" * 70)
    print("MECEハローワーク管轄マッピング構築")
    print("=" * 70)

    # 総務省マスタ読み込み
    print("\n[1] 総務省市区町村マスタ読み込み")
    master_df = load_soumu_master(soumu_file)
    print(f"  市区町村数: {len(master_df)}")

    # 都道府県別集計
    pref_counts = master_df['都道府県名'].value_counts()
    print(f"  都道府県数: {len(pref_counts)}")

    # ハローワークデータ読み込み
    print("\n[2] ハローワーク管轄データ読み込み")
    hw_df = load_hellowork_data(data_dir)
    print(f"  ハローワーク事業所数: {len(hw_df)}")

    # MECEマッピング構築
    print("\n[3] MECEマッピング構築")
    mapping_df, stats = build_mece_mapping(hw_df, master_df)
    print(f"  マッピング済み市区町村: {stats['mapped_municipalities']}")
    print(f"  マッチタイプ別:")
    for match_type, count in stats['match_types'].items():
        print(f"    - {match_type}: {count}")

    # 未マッピング
    if stats['unmapped_municipalities']:
        print(f"\n  未マッピング市区町村: {len(stats['unmapped_municipalities'])}件")
        for item in stats['unmapped_municipalities'][:20]:
            print(f"    - {item['prefecture']} {item['name']} ({item['hellowork']})")
        if len(stats['unmapped_municipalities']) > 20:
            print(f"    ... 他 {len(stats['unmapped_municipalities']) - 20}件")

    # 重複
    if stats['duplicates']:
        print(f"\n  重複マッピング: {len(stats['duplicates'])}件")
        for key, hw_list in list(stats['duplicates'].items())[:10]:
            print(f"    - {key[1]}: {', '.join(hw_list)}")

    # カバレッジチェック
    print("\n[4] カバレッジチェック")
    coverage = check_coverage(mapping_df, master_df)
    print(f"  総務省マスタ市区町村数: {coverage['total_master']}")
    print(f"  マッピング済み: {coverage['total_mapped']}")
    print(f"  カバレッジ率: {coverage['coverage_rate']:.1f}%")
    print(f"  漏れ: {coverage['missing_count']}件")

    # 漏れの詳細（都道府県別）
    if coverage['missing_codes']:
        print("\n  漏れ市区町村の例:")
        missing_df = master_df[master_df['団体コード'].astype(str).isin(coverage['missing_codes'][:20])]
        for _, row in missing_df.iterrows():
            print(f"    - {row['都道府県名']} {row['市区町村名']}")

    # CSVに出力
    print("\n[5] マッピングテーブル出力")
    output_path = data_dir / "mece_municipality_hellowork_mapping.csv"
    if len(mapping_df) > 0:
        mapping_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"  出力先: {output_path}")
        print(f"  レコード数: {len(mapping_df)}")

    # 漏れ市区町村リスト出力
    missing_output = data_dir / "missing_municipalities.csv"
    missing_df = master_df[master_df['団体コード'].astype(str).isin(coverage['missing_codes'])]
    missing_df.to_csv(missing_output, index=False, encoding='utf-8-sig')
    print(f"  漏れリスト出力先: {missing_output}")

    print("\n" + "=" * 70)
    print("構築完了")
    print("=" * 70)

    return mapping_df, stats, coverage


if __name__ == "__main__":
    main()
