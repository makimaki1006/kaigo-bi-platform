"""ハローワーク セグメント別ターゲット最終出力

フィルタ条件:
  - 従業員数150名以下
  - 人口: C/E/F=5万超、D=3万超
  - 優先フラグ: 代表者=選考担当者、携帯番号有無
"""
import pandas as pd
import json
import re
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

# --- 設定 ---
INPUT_CSV = r'C:\Users\fuji1\OneDrive\デスクトップ\RCMEB002002_M100.csv'
OUTPUT_DIR = Path(r'C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List\data\output\hellowork_segments')
POP_JSON = r'C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List\data\population\city_population_map.json'

POP_THRESHOLDS = {
    'C_工事': 50000,
    'D_ホテル旅館': 30000,
    'E_葬儀': 50000,
    'F_産業廃棄物': 50000,
}

# セグメント定義（マスタールール準拠）
SEGMENTS = {
    'C_工事': {
        'industry_codes': ['06', '07', '08'],
        'industry_keywords': ['総合工事', '職別工事', '設備工事', '建設'],
        'industry_names_whitelist': [],
        'industry_names_exclude': [],
        'job_codes': ['007-03', '048-10', '080-01', '080-04', '089-04', '089-05'],
        'job_major_prefixes': ['008', '090', '091', '092', '093', '094'],
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
        'industry_codes': [],
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

# --- ユーティリティ ---
CITY_PATTERN = re.compile(r'^(北海道|東京都|(?:大阪|京都)府|.{2,3}県)(.+?[市区町村郡](?:.+?[町村])?)')


def load_population():
    with open(POP_JSON, 'r', encoding='utf-8') as f:
        return json.load(f)


def extract_pref_city(addr, pop_map):
    if pd.isna(addr):
        return None
    m = CITY_PATTERN.match(str(addr).strip())
    if m:
        pref, city = m.group(1), m.group(2)
        city_base = re.match(r'(.+市)', city)
        if city_base:
            key = f'{pref} {city_base.group(1)}'
            if key in pop_map:
                return key
        key = f'{pref} {city}'
        if key in pop_map:
            return key
    return None


def parse_emp(val):
    if pd.isna(val) or str(val).strip() == '':
        return None
    s = str(val).replace('人', '').replace(',', '').replace('，', '').strip()
    try:
        return int(s)
    except ValueError:
        return None


def normalize_name(name):
    if pd.isna(name) or str(name).strip() == '':
        return None
    return str(name).strip().replace('\u3000', '').replace(' ', '') or None


def is_mobile(tel):
    """携帯番号判定（090/080/070始まり）"""
    if pd.isna(tel):
        return False
    s = str(tel).replace('-', '').replace('－', '').replace('ー', '').strip()
    return bool(re.match(r'^0[789]0\d{8}$', s))


def extract_segment(df, config):
    """2軸抽出（ベクトル化）"""
    industry_codes = config.get('industry_codes', [])
    industry_keywords = config.get('industry_keywords', [])
    industry_whitelist = config.get('industry_names_whitelist', [])
    industry_exclude = config.get('industry_names_exclude', [])
    job_codes = config.get('job_codes', [])
    job_major_prefixes = config.get('job_major_prefixes', [])
    exclude_job_codes = config.get('exclude_job_codes', [])
    exclude_job_kw = config.get('exclude_job_keywords', [])
    keep_job_kw = config.get('keep_job_keywords', [])

    ind_code_2 = df['産業分類（コード）'].fillna('').astype(str).str[:2]
    ind_name = df['産業分類（名称）'].fillna('').astype(str).str.strip()

    mask_ind_exclude = ind_name.isin(industry_exclude) if industry_exclude else pd.Series(False, index=df.index)

    mask_p2 = pd.Series(False, index=df.index)
    if industry_whitelist:
        mask_p2 = mask_p2 | ind_name.isin(industry_whitelist)
    if industry_codes:
        mask_p2 = mask_p2 | ind_code_2.isin(industry_codes)
    for kw in industry_keywords:
        mask_p2 = mask_p2 | ind_name.str.contains(kw, na=False)
    mask_p2 = mask_p2 & ~mask_ind_exclude

    job_cols = ['職業分類１（コード）', '職業分類２（コード）', '職業分類３（コード）']
    job_values = [df[c].fillna('').astype(str).str.strip() for c in job_cols]

    mask_p1 = pd.Series(False, index=df.index)
    if job_codes:
        jset = set(job_codes)
        for jv in job_values:
            mask_p1 = mask_p1 | jv.isin(jset)
    for prefix in job_major_prefixes:
        for jv in job_values:
            mask_p1 = mask_p1 | jv.str.startswith(prefix)
    if exclude_job_codes:
        eset = set(exclude_job_codes)
        mask_exc = pd.Series(False, index=df.index)
        for jv in job_values:
            mask_exc = mask_exc | jv.isin(eset)
        mask_p1 = mask_p1 & ~mask_exc
    if exclude_job_kw:
        jt = df['職種'].fillna('').astype(str)
        mask_kw_exc = pd.Series(False, index=df.index)
        for kw in exclude_job_kw:
            mask_kw_exc = mask_kw_exc | jt.str.contains(kw, na=False)
        if keep_job_kw:
            mask_kw_keep = pd.Series(False, index=df.index)
            for kw in keep_job_kw:
                mask_kw_keep = mask_kw_keep | jt.str.contains(kw, na=False)
            mask_kw_exc = mask_kw_exc & ~mask_kw_keep
        mask_p1 = mask_p1 & ~mask_kw_exc

    return df[mask_p1 | mask_p2]


def main():
    pop_map = load_population()

    print("=" * 70)
    print("ハローワーク セグメント別ターゲット 最終出力")
    print("=" * 70)

    # CSV読み込み
    print("\n元CSV読み込み中...")
    df = pd.read_csv(INPUT_CSV, encoding='cp932', dtype=str, low_memory=False)
    print(f"  全求人数: {len(df):,}件")

    # 出力カラム定義
    output_cols = [
        # 基本情報
        '求人番号', '事業所番号',
        '受付年月日（西暦）', '求人有効年月日（西暦）',
        # 事業所
        '事業所名漢字', '事業所名カナ',
        '事業所郵便番号', '事業所所在地',
        '事業所ホームページ',
        # 分類
        '産業分類（コード）', '産業分類（大分類コード）', '産業分類（名称）',
        '職業分類１（コード）', '職業分類２（コード）', '職業分類３（コード）',
        '職種', '仕事内容',
        '雇用形態',
        # 従業員
        '従業員数企業全体', '従業員数就業場所',
        # 代表者
        '代表者役職', '代表者名',
        # 選考担当者
        '選考担当者課係名／役職名', '選考担当者氏名漢字', '選考担当者氏名フリガナ',
        '選考担当者ＴＥＬ', '選考担当者内線', '選考担当者ＦＡＸ', '選考担当者Ｅメール',
        # 法人番号
        '法人番号',
        # 設立年
        '設立年',
    ]
    # 存在するカラムのみ
    output_cols = [c for c in output_cols if c in df.columns]

    results = {}

    for seg_name, config in SEGMENTS.items():
        pop_th = POP_THRESHOLDS[seg_name]

        print(f"\n--- {seg_name} ---")

        # STEP 1: 2軸抽出
        seg_df = extract_segment(df, config)
        print(f"  2軸抽出: {len(seg_df):,}件")

        # STEP 2: 従業員150名以下
        seg_df = seg_df.copy()
        seg_df['_emp'] = seg_df['従業員数企業全体'].apply(parse_emp)
        seg_df = seg_df[(seg_df['_emp'].notna()) & (seg_df['_emp'] <= 150)]
        print(f"  150名以下: {len(seg_df):,}件")

        # STEP 3: 人口フィルタ
        seg_df = seg_df.copy()
        seg_df['_city_key'] = seg_df['事業所所在地'].apply(lambda a: extract_pref_city(a, pop_map))
        seg_df['_pop'] = seg_df['_city_key'].apply(lambda k: pop_map.get(k) if k else None)
        seg_df = seg_df[seg_df['_pop'] > pop_th]
        print(f"  人口{pop_th // 10000}万超: {len(seg_df):,}件")

        # 付加カラム作成
        seg_df = seg_df.copy()

        # 代表者=担当者フラグ
        rep = seg_df['代表者名'].apply(normalize_name)
        contact = seg_df['選考担当者氏名漢字'].apply(normalize_name)
        seg_df['代表者直通'] = ((rep.notna()) & (contact.notna()) & (rep == contact)).map({True: '○', False: ''})

        # 携帯番号フラグ
        seg_df['担当者携帯'] = seg_df['選考担当者ＴＥＬ'].apply(lambda x: '○' if is_mobile(x) else '')

        # 優先度（代表者直通 AND/OR 携帯）
        def priority(row):
            if row['代表者直通'] == '○' and row['担当者携帯'] == '○':
                return 'S'
            elif row['代表者直通'] == '○':
                return 'A'
            elif row['担当者携帯'] == '○':
                return 'B'
            else:
                return 'C'
        seg_df['優先度'] = seg_df.apply(priority, axis=1)

        # 市区町村・人口
        seg_df['市区町村'] = seg_df['_city_key'].fillna('')
        seg_df['市区町村人口'] = seg_df['_pop'].fillna(0).astype(int)

        # セグメント名
        seg_df['セグメント'] = seg_name

        # 出力カラム
        extra_cols = ['セグメント', '優先度', '代表者直通', '担当者携帯', '市区町村', '市区町村人口']
        final_cols = extra_cols + output_cols
        final_cols = [c for c in final_cols if c in seg_df.columns]

        # 優先度順ソート
        priority_order = {'S': 0, 'A': 1, 'B': 2, 'C': 3}
        seg_df['_sort'] = seg_df['優先度'].map(priority_order)
        seg_df = seg_df.sort_values('_sort')

        # CSV出力
        out_path = OUTPUT_DIR / f'final_{seg_name}.csv'
        seg_df[final_cols].to_csv(out_path, index=False, encoding='utf-8-sig')
        print(f"  → {out_path}")

        # 優先度内訳
        pri_counts = seg_df['優先度'].value_counts().reindex(['S', 'A', 'B', 'C'], fill_value=0)
        print(f"  優先度内訳:")
        print(f"    S（代表直通+携帯）: {pri_counts['S']:,}件")
        print(f"    A（代表直通）:       {pri_counts['A']:,}件")
        print(f"    B（携帯あり）:       {pri_counts['B']:,}件")
        print(f"    C（その他）:         {pri_counts['C']:,}件")

        results[seg_name] = seg_df

    # 全セグメント統合
    all_df = pd.concat(results.values()).drop_duplicates(subset=['求人番号'])
    extra_cols_out = ['セグメント', '優先度', '代表者直通', '担当者携帯', '市区町村', '市区町村人口']
    final_all_cols = extra_cols_out + output_cols
    final_all_cols = [c for c in final_all_cols if c in all_df.columns]
    all_path = OUTPUT_DIR / 'final_CDEF_all.csv'
    all_df[final_all_cols].to_csv(all_path, index=False, encoding='utf-8-sig')

    print("\n" + "=" * 70)
    print("最終結果サマリー")
    print("=" * 70)
    print(f"\n{'セグメント':>15} | {'件数':>7} | {'S':>5} | {'A':>5} | {'B':>5} | {'C':>5}")
    print("-" * 60)
    grand_total = 0
    grand_pri = {'S': 0, 'A': 0, 'B': 0, 'C': 0}
    for seg_name, seg_df in results.items():
        total = len(seg_df)
        grand_total += total
        pri = seg_df['優先度'].value_counts().reindex(['S', 'A', 'B', 'C'], fill_value=0)
        for p in ['S', 'A', 'B', 'C']:
            grand_pri[p] += pri[p]
        print(f"{seg_name:>15} | {total:>7,} | {pri['S']:>5,} | {pri['A']:>5,} | {pri['B']:>5,} | {pri['C']:>5,}")
    print("-" * 60)
    print(f"{'合計（重複含）':>15} | {grand_total:>7,} | {grand_pri['S']:>5,} | {grand_pri['A']:>5,} | {grand_pri['B']:>5,} | {grand_pri['C']:>5,}")
    print(f"{'合計（重複除外）':>15} | {len(all_df):>7,}")
    print(f"\n統合CSV: {all_path}")
    print(f"\n優先度定義:")
    print(f"  S = 代表者直通 + 携帯番号あり（最強セグメント）")
    print(f"  A = 代表者直通（代表者=選考担当者）")
    print(f"  B = 携帯番号あり（090/080/070）")
    print(f"  C = その他")


if __name__ == '__main__':
    main()
