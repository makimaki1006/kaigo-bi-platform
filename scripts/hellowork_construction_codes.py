#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ハローワークデータから建設・工事関連の職業分類コードを分析
"""
import sys
import pandas as pd
from pathlib import Path
from collections import Counter

# 標準出力をUTF-8に設定
sys.stdout.reconfigure(encoding='utf-8')

def main():
    csv_path = Path(r'C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List\data\input\hellowork.csv')
    output_path = Path(r'C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List\data\output\hellowork_construction_analysis.txt')
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"ファイルパス: {csv_path}")
    print(f"ファイル存在確認: {csv_path.exists()}\n")

    # エンコーディングを試行
    encodings = ['utf-8-sig', 'cp932', 'shift-jis']
    df = None

    for encoding in encodings:
        try:
            print(f"エンコーディング {encoding} で読み込み中...")
            df = pd.read_csv(csv_path, encoding=encoding, dtype=str)
            print(f"✅ {encoding} で読み込み成功")
            print(f"行数: {len(df):,} 行")
            print(f"列数: {len(df.columns)} 列\n")
            break
        except Exception as e:
            print(f"❌ {encoding} 失敗")
            continue

    if df is None:
        print("全てのエンコーディングで読み込み失敗")
        return

    # 詳細な出力はファイルに保存
    with open(output_path, 'w', encoding='utf-8') as f:
        # カラム一覧を書き込み
        f.write("=" * 80 + "\n")
        f.write("カラム一覧:\n")
        f.write("=" * 80 + "\n")
        for i, col in enumerate(df.columns, 1):
            f.write(f"{i:3d}. {col}\n")
        f.write("\n")

        # 職業分類関連のカラムを探す
        job_code_cols = [col for col in df.columns if '職業分類' in col and 'コード' in col]
        industry_code_cols = [col for col in df.columns if '産業分類' in col and 'コード' in col]
        industry_name_cols = [col for col in df.columns if '産業分類' in col and '名称' in col]
        job_title_cols = [col for col in df.columns if '職種' in col or '仕事の内容' in col or '仕事内容' in col]

        f.write("=" * 80 + "\n")
        f.write("分析対象カラム:\n")
        f.write("=" * 80 + "\n")
        f.write(f"職業分類コード列: {job_code_cols}\n")
        f.write(f"産業分類コード列: {industry_code_cols}\n")
        f.write(f"産業分類名称列: {industry_name_cols}\n")
        f.write(f"職種関連列: {job_title_cols}\n\n")

        # 職業分類コードの全値（トップ50のみ）
        if job_code_cols:
            f.write("=" * 80 + "\n")
            f.write("【職業分類コード】TOP50（件数順）\n")
            f.write("=" * 80 + "\n")

            for col in job_code_cols:
                f.write(f"\n■ {col}\n")
                value_counts = df[col].value_counts(dropna=False)
                f.write(f"ユニーク値数: {len(value_counts)}\n")
                f.write("-" * 40 + "\n")
                for i, (value, count) in enumerate(value_counts.head(50).items(), 1):
                    value_str = str(value) if pd.notna(value) else 'NaN'
                    f.write(f"{i:2d}. {value_str:>10s} : {count:>8,} 件\n")
                f.write("\n")

        # 産業分類（名称）全値
        if industry_name_cols:
            f.write("=" * 80 + "\n")
            f.write("【産業分類（名称）】全ユニーク値（件数順）\n")
            f.write("=" * 80 + "\n")

            for col in industry_name_cols:
                f.write(f"\n■ {col}\n")
                value_counts = df[col].value_counts(dropna=False)
                f.write(f"ユニーク値数: {len(value_counts)}\n")
                f.write("-" * 40 + "\n")
                for value, count in value_counts.items():
                    f.write(f"{str(value)[:50]:50s} : {count:>8,} 件\n")
                f.write("\n")

        # 産業分類（コード）全値
        if industry_code_cols:
            f.write("=" * 80 + "\n")
            f.write("【産業分類（コード）】全ユニーク値（件数順）\n")
            f.write("=" * 80 + "\n")

            for col in industry_code_cols:
                f.write(f"\n■ {col}\n")
                value_counts = df[col].value_counts(dropna=False)
                f.write(f"ユニーク値数: {len(value_counts)}\n")
                f.write("-" * 40 + "\n")
                for value, count in value_counts.items():
                    value_str = str(value) if pd.notna(value) else 'NaN'
                    f.write(f"{value_str:>10s} : {count:>8,} 件\n")
                f.write("\n")

    print(f"詳細な分析結果を {output_path} に保存しました\n")

    # 建設・工事関連キーワード
    construction_keywords = [
        '建設', '建築', '土木', '工事', '配管', '電気工',
        '塗装', '左官', '鳶', '大工', '解体', '施工',
        '設備工', '内装', '外装', '防水', '屋根', '基礎',
        '造園', '舗装', 'とび', 'とび工', '足場'
    ]

    print("=" * 80)
    print("【建設・工事関連分析】")
    print("=" * 80)

    construction_codes = set()

    # 職種や仕事内容から建設関連を判定
    if job_title_cols:
        for col in job_title_cols:
            print(f"\n■ {col} から建設関連を検索中...")

            # 建設キーワードを含む行を抽出
            mask = df[col].fillna('').str.contains('|'.join(construction_keywords), case=False, na=False)
            construction_df = df[mask]

            print(f"建設関連キーワードにヒットした件数: {len(construction_df):,} 件")
            print(f"全体に対する割合: {len(construction_df) / len(df) * 100:.2f}%")

            if len(construction_df) > 0 and job_code_cols:
                # その行の職業分類コードを収集（職業分類１のみ）
                code_col = '職業分類１（コード）'
                if code_col in construction_df.columns:
                    codes = construction_df[code_col].dropna().unique()
                    construction_codes.update(codes)

                    print(f"\n職業分類コード TOP20:")
                    code_counts = construction_df[code_col].value_counts()
                    for i, (code, count) in enumerate(code_counts.head(20).items(), 1):
                        print(f"  {i:2d}. {code:>10s} : {count:>6,} 件")

    # 産業分類で建設業を検索
    print("\n" + "=" * 80)
    print("【産業分類で「建設」を含むもの】")
    print("=" * 80)

    if industry_name_cols:
        for col in industry_name_cols:
            mask = df[col].fillna('').str.contains('建設', case=False, na=False)
            construction_industry_df = df[mask]

            print(f"\n■ {col}")
            print(f"建設業の件数: {len(construction_industry_df):,} 件")
            print(f"全体に対する割合: {len(construction_industry_df) / len(df) * 100:.2f}%")

            if len(construction_industry_df) > 0:
                # 産業分類名称の内訳 TOP10
                print("\n産業分類名称 TOP10:")
                for i, (name, count) in enumerate(construction_industry_df[col].value_counts().head(10).items(), 1):
                    print(f"  {i:2d}. {str(name):50s} : {count:>6,} 件")

                # 対応する産業分類コード TOP10
                if industry_code_cols:
                    for code_col in industry_code_cols:
                        print(f"\n産業分類コード TOP10:")
                        for i, (code, count) in enumerate(construction_industry_df[code_col].value_counts().head(10).items(), 1):
                            print(f"  {i:2d}. {code:>10s} : {count:>6,} 件")

                # 対応する職業分類コード TOP20
                code_col = '職業分類１（コード）'
                if code_col in construction_industry_df.columns:
                    codes = construction_industry_df[code_col].dropna().unique()
                    construction_codes.update(codes)

                    print(f"\n建設業における職業分類コード TOP20:")
                    code_counts = construction_industry_df[code_col].value_counts()
                    for i, (code, count) in enumerate(code_counts.head(20).items(), 1):
                        print(f"  {i:2d}. {code:>10s} : {count:>6,} 件")

    # 建設関連コードのサマリー
    print("\n" + "=" * 80)
    print("【建設・工事関連職業分類コード 統合サマリー】")
    print("=" * 80)
    print(f"特定された建設関連コード数: {len(construction_codes)}")
    print(f"コード一覧（ソート済み）: {sorted(construction_codes)}\n")

    if construction_codes:
        code_col = '職業分類１（コード）'
        if code_col in df.columns:
            mask = df[code_col].isin(construction_codes)
            count = mask.sum()
            print(f"{code_col} で建設関連コードにヒットする件数: {count:,} 件")
            print(f"全体に対する割合: {count / len(df) * 100:.2f}%")

            # 各コードの件数
            print(f"\n建設関連コードの内訳:")
            code_counts = df[code_col].value_counts()
            construction_code_counts = {code: code_counts.get(code, 0) for code in sorted(construction_codes)}
            for code, count in sorted(construction_code_counts.items(), key=lambda x: x[1], reverse=True):
                print(f"  {code:>10s} : {count:>6,} 件")

    # 数値範囲でのコード分析（070-099など）
    print("\n" + "=" * 80)
    print("【職業分類コードの数値範囲分析】")
    print("=" * 80)

    code_col = '職業分類１（コード）'
    if code_col in df.columns:
        print(f"\n■ {code_col}")

        numeric_codes = df[code_col].dropna()

        # 070-099の範囲をチェック（建設関連の一般的な範囲）
        def in_construction_range(code_str):
            try:
                code_int = int(str(code_str).strip().replace('-', ''))
                return 70 <= code_int <= 99 or (700 <= code_int <= 999)
            except:
                return False

        mask = numeric_codes.apply(in_construction_range)
        range_count = mask.sum()

        print(f"\nコード範囲 070-099 の件数: {range_count:,} 件")
        print(f"全体に対する割合: {range_count / len(df) * 100:.2f}%")

        if range_count > 0:
            range_codes = numeric_codes[mask].value_counts()
            print("\n該当コードの内訳 TOP30:")
            for i, (code, count) in enumerate(range_codes.head(30).items(), 1):
                print(f"  {i:2d}. {code:>10s} : {count:>6,} 件")

    print(f"\n\n詳細な分析結果は {output_path} を参照してください")

if __name__ == '__main__':
    main()
