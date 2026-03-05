"""
doda CSVファイルから職種情報を抽出・分析するスクリプト

目的:
- 列index 5-15の列名を確認
- 職種情報が含まれる列を特定
- 全ユニーク職種とその出現頻度を表示
- 作業員系のみを抽出するためのフィルタ検討
"""

import sys
import pandas as pd
from pathlib import Path
from collections import Counter
import re

# UTF-8出力設定
sys.stdout.reconfigure(encoding='utf-8')

# CSVファイルパス
csv_path = Path(r'C:\Users\fuji1\Downloads\doda_google_search_urls.csv')

print("=" * 80)
print("doda 職種情報分析")
print("=" * 80)
print()

# CSVファイル読み込み
print(f"読み込み中: {csv_path}")
df = pd.read_csv(csv_path, encoding='utf-8-sig', dtype=str)
print(f"総レコード数: {len(df):,}")
print()

# 全列名表示
print("-" * 80)
print("全列名（インデックス付き）")
print("-" * 80)
for idx, col in enumerate(df.columns):
    print(f"{idx:3d}: {col}")
print()

# index 5-15の列を詳細表示
print("-" * 80)
print("列 index 5-15 の詳細")
print("-" * 80)
for idx in range(5, min(16, len(df.columns))):
    col = df.columns[idx]
    unique_count = df[col].nunique()
    sample_values = df[col].dropna().head(3).tolist()
    print(f"\nIndex {idx}: {col}")
    print(f"  ユニーク数: {unique_count:,}")
    print(f"  サンプル値:")
    for val in sample_values:
        val_str = str(val)[:100]  # 最初の100文字まで
        print(f"    - {val_str}")
print()

# index 7（会社名+職種）の分析
print("=" * 80)
print("列 index 7 (PageTitle-module_title__2RYke) の分析")
print("=" * 80)

if len(df.columns) > 7:
    title_col = df.columns[7]
    print(f"列名: {title_col}")
    print()

    titles = df[title_col].dropna()
    print(f"非NULL値: {len(titles):,} 件")
    print()

    # 全ユニーク値とその頻度
    title_counts = Counter(titles)
    print(f"ユニーク職種数: {len(title_counts):,}")
    print()

    print("-" * 80)
    print("全職種（頻度順 Top 100）")
    print("-" * 80)
    for idx, (title, count) in enumerate(title_counts.most_common(100), 1):
        print(f"{idx:4d}. [{count:4d}件] {title}")
    print()

    if len(title_counts) > 100:
        print(f"（残り {len(title_counts) - 100} 件の職種は省略）")
        print()

# index 8 の分析
print("=" * 80)
print("列 index 8 の分析")
print("=" * 80)

if len(df.columns) > 8:
    col8 = df.columns[8]
    print(f"列名: {col8}")
    print()

    values8 = df[col8].dropna()
    print(f"非NULL値: {len(values8):,} 件")
    print()

    counts8 = Counter(values8)
    print(f"ユニーク値数: {len(counts8):,}")
    print()

    print("-" * 80)
    print("上位50値")
    print("-" * 80)
    for idx, (val, count) in enumerate(counts8.most_common(50), 1):
        print(f"{idx:4d}. [{count:4d}件] {val}")
    print()

# index 9 の分析
print("=" * 80)
print("列 index 9 の分析")
print("=" * 80)

if len(df.columns) > 9:
    col9 = df.columns[9]
    print(f"列名: {col9}")
    print()

    values9 = df[col9].dropna()
    print(f"非NULL値: {len(values9):,} 件")
    print()

    counts9 = Counter(values9)
    print(f"ユニーク値数: {len(counts9):,}")
    print()

    print("-" * 80)
    print("上位50値")
    print("-" * 80)
    for idx, (val, count) in enumerate(counts9.most_common(50), 1):
        print(f"{idx:4d}. [{count:4d}件] {val}")
    print()

# キーワード分析（index 7の職種名から）
print("=" * 80)
print("キーワード頻度分析（職種名から抽出）")
print("=" * 80)

if len(df.columns) > 7:
    title_col = df.columns[7]
    titles = df[title_col].dropna()

    # キーワードリスト（作業員系判定用）
    worker_keywords = [
        '作業員', '作業スタッフ', '製造スタッフ', '工場', '倉庫',
        '軽作業', 'ピッキング', '検品', '梱包', '組立',
        'ライン', '製造', '生産', '加工',
        '運転手', 'ドライバー', '配送', '物流',
        '清掃', '警備', '施設管理',
        '調理補助', '調理スタッフ', '厨房'
    ]

    # 除外キーワード（高度職種、リモート等）
    exclude_keywords = [
        'リモート', '在宅', 'テレワーク',
        'エンジニア', 'プログラマ', 'SE', 'IT',
        'コンサルタント', 'マネージャー', '営業',
        'デザイナー', 'ディレクター',
        '企画', 'マーケティング', '広報'
    ]

    keyword_counts = Counter()

    for title in titles:
        title_lower = str(title).lower()
        for keyword in worker_keywords + exclude_keywords:
            if keyword.lower() in title_lower or keyword in title:
                keyword_counts[keyword] += 1

    print("作業員系キーワード:")
    print("-" * 40)
    for keyword in worker_keywords:
        count = keyword_counts.get(keyword, 0)
        print(f"  {keyword:15s}: {count:5d} 件")
    print()

    print("除外候補キーワード:")
    print("-" * 40)
    for keyword in exclude_keywords:
        count = keyword_counts.get(keyword, 0)
        print(f"  {keyword:15s}: {count:5d} 件")
    print()

print("=" * 80)
print("分析完了")
print("=" * 80)
