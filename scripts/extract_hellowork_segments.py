"""ハローワークCSVからセグメント別ターゲット抽出スクリプト

マスタールール: claudedocs/master_rules/MASTER_RULE_hellowork.md
方式: パターン①（職種分類）OR パターン②（産業分類）の2軸抽出
"""
import pandas as pd
import sys
import os
from pathlib import Path

# エンコーディング設定
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

# --- セグメント定義 ---
SEGMENTS = {
    'C_工事': {
        'industry_codes': ['06', '07', '08'],
        'industry_keywords': ['総合工事', '職別工事', '設備工事', '建設'],
        'industry_names_whitelist': [],
        'industry_names_exclude': [],
        'job_codes': [
            '007-03', '048-10', '080-01', '080-04', '089-04', '089-05',
        ],
        'job_major_prefixes': [
            '008',  # 建築・土木・測量技術者
            '090', '091', '092', '093', '094',  # 大分類14全体
        ],
        'exclude_job_codes': [],
        'exclude_job_keywords': [],
        'keep_job_keywords': [],
    },
    'D_ホテル旅館': {
        'industry_codes': ['75'],
        'industry_keywords': ['宿泊', 'ホテル', '旅館'],
        'industry_names_whitelist': [],
        'industry_names_exclude': [],
        'job_codes': ['056-02', '056-04', '056-05', '096-03'],
        'job_major_prefixes': [],
        'exclude_job_codes': [],
        'exclude_job_keywords': [],
        'keep_job_keywords': [],
    },
    'E_葬儀': {
        'industry_codes': [],  # 79は範囲が広いためキーワードのみ
        'industry_keywords': ['葬儀', '葬祭'],
        'industry_names_whitelist': [],
        'industry_names_exclude': [],
        'job_codes': ['058-06'],
        'job_major_prefixes': [],
        'exclude_job_codes': [],
        'exclude_job_keywords': [],
        'keep_job_keywords': [],
    },
    'F_産業廃棄物': {
        'industry_codes': ['88'],
        'industry_keywords': ['廃棄物'],
        'industry_names_whitelist': [],
        'industry_names_exclude': [],
        'job_codes': ['096-05', '096-06'],
        'job_major_prefixes': [],
        'exclude_job_codes': [],
        'exclude_job_keywords': [],
        'keep_job_keywords': [],
    },
}


def extract_segment(df, segment_config):
    """指定セグメントに該当する求人を抽出（パターン① OR ②）

    ベクトル化処理で高速抽出。
    """
    industry_codes = segment_config.get('industry_codes', [])
    industry_keywords = segment_config.get('industry_keywords', [])
    industry_whitelist = segment_config.get('industry_names_whitelist', [])
    industry_exclude = segment_config.get('industry_names_exclude', [])
    job_codes = segment_config.get('job_codes', [])
    job_major_prefixes = segment_config.get('job_major_prefixes', [])
    exclude_job_codes = segment_config.get('exclude_job_codes', [])
    exclude_job_kw = segment_config.get('exclude_job_keywords', [])
    keep_job_kw = segment_config.get('keep_job_keywords', [])

    # カラム名マッピング
    col_ind_code = '産業分類（コード）'
    col_ind_name = '産業分類（名称）'
    col_job1 = '職業分類１（コード）'
    col_job2 = '職業分類２（コード）'
    col_job3 = '職業分類３（コード）'
    col_job_title = '職種'

    # 産業分類コード先頭2桁
    ind_code_2 = df[col_ind_code].fillna('').astype(str).str[:2]
    ind_name = df[col_ind_name].fillna('').astype(str).str.strip()

    # --- 産業分類の明示除外 ---
    mask_ind_exclude = ind_name.isin(industry_exclude) if industry_exclude else pd.Series(False, index=df.index)

    # --- パターン②: 産業分類マッチ ---
    mask_p2 = pd.Series(False, index=df.index)

    # ホワイトリスト方式（セグメントA/B用）
    if industry_whitelist:
        mask_p2 = mask_p2 | ind_name.isin(industry_whitelist)

    # コード方式
    if industry_codes:
        mask_p2 = mask_p2 | ind_code_2.isin(industry_codes)

    # キーワード方式
    for kw in industry_keywords:
        mask_p2 = mask_p2 | ind_name.str.contains(kw, na=False)

    # パターン②から除外を引く
    mask_p2 = mask_p2 & ~mask_ind_exclude

    # --- パターン①: 職種分類マッチ ---
    # 3つの職業分類コード列を統合して判定
    job_cols = [col_job1, col_job2, col_job3]
    job_values = [df[c].fillna('').astype(str).str.strip() for c in job_cols]

    mask_p1 = pd.Series(False, index=df.index)

    # 個別コード一致
    if job_codes:
        job_code_set = set(job_codes)
        for jv in job_values:
            mask_p1 = mask_p1 | jv.isin(job_code_set)

    # 大分類プレフィックス一致
    for prefix in job_major_prefixes:
        for jv in job_values:
            mask_p1 = mask_p1 | jv.str.startswith(prefix)

    # 除外職種コード
    if exclude_job_codes:
        exc_set = set(exclude_job_codes)
        mask_exc_code = pd.Series(False, index=df.index)
        for jv in job_values:
            mask_exc_code = mask_exc_code | jv.isin(exc_set)
        mask_p1 = mask_p1 & ~mask_exc_code

    # 職種キーワード除外（keep優先）
    if exclude_job_kw:
        job_title = df[col_job_title].fillna('').astype(str)
        mask_kw_exclude = pd.Series(False, index=df.index)
        for kw in exclude_job_kw:
            mask_kw_exclude = mask_kw_exclude | job_title.str.contains(kw, na=False)
        if keep_job_kw:
            mask_kw_keep = pd.Series(False, index=df.index)
            for kw in keep_job_kw:
                mask_kw_keep = mask_kw_keep | job_title.str.contains(kw, na=False)
            mask_kw_exclude = mask_kw_exclude & ~mask_kw_keep
        mask_p1 = mask_p1 & ~mask_kw_exclude

    # パターン① OR パターン②
    final_mask = mask_p1 | mask_p2
    return df[final_mask]


def main():
    input_csv = r'C:\Users\fuji1\OneDrive\デスクトップ\RCMEB002002_M100.csv'
    output_dir = Path(r'C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List\data\output\hellowork_segments')
    output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("ハローワーク セグメント別ターゲット抽出")
    print("=" * 60)
    print(f"入力ファイル: {input_csv}")

    # CSV読み込み
    print("\nCSV読み込み中...")
    df = pd.read_csv(input_csv, encoding='cp932', dtype=str, low_memory=False)
    print(f"  全求人数: {len(df):,}件")

    # 抽出対象セグメント
    target_segments = ['C_工事', 'D_ホテル旅館', 'E_葬儀', 'F_産業廃棄物']

    results = {}
    total = 0

    # 出力カラム定義（重要フィールドのみ）
    output_cols = [
        '求人番号', '事業所番号',
        '受付年月日（西暦）', '求人有効年月日（西暦）',
        '事業所名漢字', '事業所名カナ',
        '事業所郵便番号', '事業所所在地',
        '事業所ホームページ',
        '産業分類（コード）', '産業分類（大分類コード）', '産業分類（名称）',
        '職業分類１（コード）', '職業分類２（コード）', '職業分類３（コード）',
        '職業分類１（大分類コード）',
        '職種', '仕事内容',
        '雇用形態',
    ]

    # 電話番号系カラムを探す
    phone_cols = [c for c in df.columns if '電話' in c or 'ＴＥＬ' in c or 'TEL' in c]
    # 従業員系カラム
    emp_cols = [c for c in df.columns if '従業員' in c]
    # 法人番号
    corp_cols = [c for c in df.columns if '法人番号' in c]
    # 選考担当者系
    contact_cols = [c for c in df.columns if '選考担当者' in c or '担当者' in c]

    extra_cols = phone_cols + emp_cols + corp_cols + contact_cols
    all_output_cols = output_cols + [c for c in extra_cols if c not in output_cols]
    # 存在するカラムのみ
    all_output_cols = [c for c in all_output_cols if c in df.columns]

    print(f"\n抽出開始（対象: {', '.join(target_segments)}）")
    print("-" * 60)

    for seg_name in target_segments:
        config = SEGMENTS[seg_name]
        extracted = extract_segment(df, config)
        count = len(extracted)
        results[seg_name] = extracted
        total += count

        # パターン内訳を計算
        col_ind_code = '産業分類（コード）'
        col_ind_name = '産業分類（名称）'
        col_job1 = '職業分類１（コード）'

        ind_code_2 = extracted[col_ind_code].fillna('').astype(str).str[:2]
        ind_name = extracted[col_ind_name].fillna('').astype(str).str.strip()

        # パターン②該当数
        mask_p2 = pd.Series(False, index=extracted.index)
        for code in config.get('industry_codes', []):
            mask_p2 = mask_p2 | (ind_code_2 == code)
        for kw in config.get('industry_keywords', []):
            mask_p2 = mask_p2 | ind_name.str.contains(kw, na=False)
        for name in config.get('industry_names_whitelist', []):
            mask_p2 = mask_p2 | (ind_name == name)

        p2_count = mask_p2.sum()
        p1_only_count = count - p2_count

        print(f"\n【{seg_name}】{count:,}件")
        print(f"  パターン②（産業分類）該当: {p2_count:,}件")
        print(f"  パターン①のみ（職種分類）: {p1_only_count:,}件")

        # 産業分類の内訳トップ10
        top_industries = extracted[col_ind_name].value_counts().head(10)
        print(f"  産業分類トップ10:")
        for ind, cnt in top_industries.items():
            print(f"    {ind}: {cnt:,}件")

        # CSV出力
        out_path = output_dir / f'segment_{seg_name}.csv'
        extracted[all_output_cols].to_csv(out_path, index=False, encoding='utf-8-sig')
        print(f"  → {out_path}")

    # 全セグメント結合（重複除外）
    all_extracted = pd.concat(results.values()).drop_duplicates(subset=['求人番号'])
    all_path = output_dir / 'segments_CDEF_all.csv'
    all_extracted[all_output_cols].to_csv(all_path, index=False, encoding='utf-8-sig')

    print("\n" + "=" * 60)
    print("抽出結果サマリー")
    print("=" * 60)
    for seg_name, extracted in results.items():
        print(f"  {seg_name}: {len(extracted):,}件")
    print(f"  ---")
    print(f"  合計（重複含む）: {total:,}件")
    print(f"  合計（重複除外）: {len(all_extracted):,}件")
    print(f"\n出力先: {output_dir}")
    print(f"全セグメント統合: {all_path}")


if __name__ == '__main__':
    main()
