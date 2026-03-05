#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ハローワーク管轄区域マッピング分析スクリプト

収集したハローワークデータを分析し、MECEなマッピング作成に必要な情報を抽出する。
"""

import pandas as pd
import re
from pathlib import Path
from collections import defaultdict


def load_all_hellowork_data(data_dir: Path) -> pd.DataFrame:
    """収集した全ハローワークCSVデータを読み込み"""
    csv_files = list(data_dir.glob("job_ratio_*.csv"))
    # 統合ファイル（all_*.csv）を除外し、個別県ファイルのみ使用
    csv_files = [f for f in csv_files if "_all_" not in f.name]

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


def extract_municipalities_from_jurisdiction(jurisdiction: str, prefecture: str) -> list[str]:
    """
    管轄区域文字列から市区町村名を抽出

    例: "札幌市（中央区、南区、西区、手稲区）" → ["札幌市中央区", "札幌市南区", ...]
    例: "江別市、新篠津村" → ["江別市", "新篠津村"]
    """
    if pd.isna(jurisdiction) or not jurisdiction:
        return []

    municipalities = []

    # 括弧内の区情報を展開
    # パターン: "札幌市（中央区、南区、西区）"
    city_ward_pattern = r'([^、（）]+市)（([^）]+)）'
    for match in re.finditer(city_ward_pattern, jurisdiction):
        city = match.group(1)
        wards = match.group(2)
        # 区を分割
        for ward in re.split(r'[、,]', wards):
            ward = ward.strip()
            if ward and '区' in ward:
                municipalities.append(f"{city}{ward}")

    # 政令指定都市の区表記を除去したバージョンも追加
    # 括弧なしの市区町村を抽出
    # パターン: カンマ区切りの市町村
    clean_jurisdiction = re.sub(r'（[^）]+）', '', jurisdiction)  # 括弧内を削除
    parts = re.split(r'[、,]', clean_jurisdiction)

    for part in parts:
        part = part.strip()
        if not part:
            continue
        # 市・町・村・区で終わるものを抽出
        if re.search(r'(市|町|村|区)$', part):
            # 「を除く」などの除外表現をスキップ
            if 'を除く' not in part:
                municipalities.append(part)
        # 郡名付き（例: 東津軽郡平内町）
        elif '郡' in part:
            # 郡（町名、町名）パターン
            gun_match = re.match(r'([^郡]+郡)(.+)', part)
            if gun_match:
                municipalities.append(part)

    return list(set(municipalities))  # 重複排除


def analyze_coverage(df: pd.DataFrame) -> dict:
    """データカバレッジを分析"""
    analysis = {
        'total_hellowork_offices': len(df),
        'offices_with_ratio': len(df[df['ratio'].notna()]),
        'offices_with_jurisdiction': len(df[df['jurisdiction'].notna() & (df['jurisdiction'] != '')]),
        'offices_by_prefecture': df.groupby('prefecture').size().to_dict(),
        'missing_ratio_offices': df[df['ratio'].isna()][['prefecture', 'hellowork_name', 'url']].to_dict('records'),
        'missing_jurisdiction_offices': df[df['jurisdiction'].isna() | (df['jurisdiction'] == '')][['prefecture', 'hellowork_name', 'url']].to_dict('records'),
    }
    return analysis


def build_municipality_mapping(df: pd.DataFrame) -> dict:
    """市区町村→ハローワークマッピングを構築"""
    mapping = defaultdict(list)  # 市区町村 → [(ハローワーク名, 都道府県, 求人倍率)]

    for _, row in df.iterrows():
        prefecture = row['prefecture']
        hellowork_name = row['hellowork_name']
        jurisdiction = row['jurisdiction']
        ratio = row['ratio']

        municipalities = extract_municipalities_from_jurisdiction(jurisdiction, prefecture)

        for muni in municipalities:
            mapping[muni].append({
                'hellowork_name': hellowork_name,
                'prefecture': prefecture,
                'ratio': ratio
            })

    return dict(mapping)


def check_mece(mapping: dict) -> dict:
    """MECEチェック：漏れ・重複を検出"""
    duplicates = {}  # 複数のハローワークにマッピングされている市区町村

    for muni, helloworks in mapping.items():
        if len(helloworks) > 1:
            duplicates[muni] = helloworks

    return {
        'total_municipalities_mapped': len(mapping),
        'duplicate_mappings': duplicates,
        'duplicate_count': len(duplicates),
    }


def main():
    """メイン処理"""
    data_dir = Path(__file__).parent.parent / "data" / "job_openings_ratio"

    print("=" * 60)
    print("ハローワーク管轄区域マッピング分析")
    print("=" * 60)

    # データ読み込み
    print("\n[1] データ読み込み")
    df = load_all_hellowork_data(data_dir)
    print(f"  読み込み完了: {len(df)} レコード")

    # カバレッジ分析
    print("\n[2] データカバレッジ分析")
    coverage = analyze_coverage(df)
    print(f"  ハローワーク事業所数: {coverage['total_hellowork_offices']}")
    print(f"  求人倍率取得成功: {coverage['offices_with_ratio']} ({coverage['offices_with_ratio']/coverage['total_hellowork_offices']*100:.1f}%)")
    print(f"  管轄区域取得成功: {coverage['offices_with_jurisdiction']} ({coverage['offices_with_jurisdiction']/coverage['total_hellowork_offices']*100:.1f}%)")

    if coverage['missing_ratio_offices']:
        print(f"\n  求人倍率未取得 ({len(coverage['missing_ratio_offices'])}件):")
        for office in coverage['missing_ratio_offices'][:10]:
            print(f"    - {office['prefecture']} {office['hellowork_name']}")
        if len(coverage['missing_ratio_offices']) > 10:
            print(f"    ... 他 {len(coverage['missing_ratio_offices']) - 10}件")

    # 市区町村マッピング構築
    print("\n[3] 市区町村→ハローワークマッピング構築")
    mapping = build_municipality_mapping(df)
    print(f"  マッピング構築完了: {len(mapping)} 市区町村")

    # MECEチェック
    print("\n[4] MECEチェック（漏れなく・ダブりなく）")
    mece_result = check_mece(mapping)
    print(f"  マッピング済み市区町村: {mece_result['total_municipalities_mapped']}")
    print(f"  重複マッピング: {mece_result['duplicate_count']}")

    if mece_result['duplicate_mappings']:
        print("\n  重複マッピング詳細:")
        for muni, helloworks in list(mece_result['duplicate_mappings'].items())[:20]:
            hw_names = [hw['hellowork_name'] for hw in helloworks]
            print(f"    - {muni}: {', '.join(hw_names)}")
        if len(mece_result['duplicate_mappings']) > 20:
            print(f"    ... 他 {len(mece_result['duplicate_mappings']) - 20}件")

    # サンプル出力
    print("\n[5] マッピングサンプル（最初の30件）")
    for i, (muni, helloworks) in enumerate(list(mapping.items())[:30]):
        hw = helloworks[0]
        print(f"  {muni} → {hw['hellowork_name']} ({hw['ratio']}倍)")

    # 都道府県別統計
    print("\n[6] 都道府県別ハローワーク数")
    for pref, count in sorted(coverage['offices_by_prefecture'].items()):
        print(f"  {pref}: {count}事業所")

    # マッピングをCSV出力
    output_path = data_dir / "municipality_hellowork_mapping_analysis.csv"
    mapping_rows = []
    for muni, helloworks in mapping.items():
        for hw in helloworks:
            mapping_rows.append({
                'municipality': muni,
                'hellowork_name': hw['hellowork_name'],
                'prefecture': hw['prefecture'],
                'ratio': hw['ratio'],
                'is_duplicate': len(helloworks) > 1
            })

    mapping_df = pd.DataFrame(mapping_rows)
    mapping_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"\n[7] マッピング分析結果を出力: {output_path}")

    print("\n" + "=" * 60)
    print("分析完了")
    print("=" * 60)


if __name__ == "__main__":
    main()
