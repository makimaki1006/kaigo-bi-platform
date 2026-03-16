"""
ハローワーク セグメントC-F 新規リード作成パイプライン
====================================================================================
対象セグメント:
  C_工事: 総合工事・職別工事・設備工事・建設
  D_ホテル旅館: 宿泊・ホテル・旅館
  E_葬儀: 葬儀・葬祭
  F_産業廃棄物: 廃棄物処理

処理フロー:
  STEP 1: データ読み込み・結合
  STEP 2: セグメントC/D/E/F抽出（パターン① OR ②）
  STEP 3: 品質フィルタ（パート除外・従業員数・人口）
  STEP 4: 電話番号重複排除 + 法人番号重複排除 + 他職種情報集約
  STEP 5: Salesforce突合（Account/Contact/Lead）
  STEP 6: 成約先除外（電話番号+法人番号）
  STEP 7: 決裁者近接スコア付与（★1-5）
  STEP 8: Salesforceインポート用CSV生成

A/Bパイプラインとの主な差異:
  - STEP 2b（多角的職種フィルタ）なし: C-Fは産業内の全職種を含む
  - STEP 3 人口閾値がセグメント別: C=50000, D=30000, E=50000, F=50000
  - STEP 6.5（訪問看護除外・大分類Pフィルタ）なし: A/B固有のため不要
"""

import pandas as pd
import numpy as np
import re
import json
import sys
import os
from pathlib import Path
from datetime import datetime

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# --- パス設定 ---
BASE_DIR = Path(r'C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List')
DATA_DIR = BASE_DIR / 'data' / 'output' / 'hellowork_segments'
OUTPUT_DIR = DATA_DIR  # 既存と同じ場所に出力
POP_FILE = BASE_DIR / 'data' / 'population' / 'city_population_map.json'

CSV_FILE_1 = Path(r'C:\Users\fuji1\OneDrive\デスクトップ\RCMEB002002_M300.csv')
CSV_FILE_2 = Path(r'C:\Users\fuji1\OneDrive\デスクトップ\RCMEB002002_M100 (4).csv')

TODAY = datetime.now().strftime('%Y%m%d')
TODAY_ISO = datetime.now().strftime('%Y-%m-%d')

# --- セグメント定義（C-F） ---
SEGMENTS = {
    'C_工事': {
        'industry_codes': ['06', '07', '08'],
        'industry_keywords': ['総合工事', '職別工事', '設備工事', '建設'],
        'job_codes': ['007-03', '048-10', '080-01', '080-04', '089-04', '089-05'],
        'job_major_prefixes': ['008', '090', '091', '092', '093', '094'],
        'pop_threshold': 50000,
    },
    'D_ホテル旅館': {
        'industry_codes': ['75'],
        'industry_keywords': ['宿泊', 'ホテル', '旅館'],
        'job_codes': ['056-02', '056-04', '056-05', '096-03'],
        'job_major_prefixes': [],
        'pop_threshold': 30000,  # 観光地は低人口エリアが多い
    },
    'E_葬儀': {
        'industry_codes': [],  # 79は広すぎるためキーワードのみ
        'industry_keywords': ['葬儀', '葬祭'],
        'job_codes': ['058-06'],
        'job_major_prefixes': [],
        'pop_threshold': 50000,
    },
    'F_産業廃棄物': {
        'industry_codes': ['88'],
        'industry_keywords': ['廃棄物'],
        'job_codes': ['096-05', '096-06'],
        'job_major_prefixes': [],
        'pop_threshold': 50000,
    },
}

# --- 品質フィルタ設定 ---
EXCLUDE_EMPLOYMENT_TYPES = ['パート労働者', '無期雇用派遣パート', '有期雇用派遣パート']
EMP_MIN = 11
EMP_MAX = 150

# 雇用形態優先度（重複排除用）
EMPLOYMENT_PRIORITY = {
    '正社員': 1,
    '正社員以外': 2,
    '無期雇用派遣労働者': 3,
    '有期雇用派遣労働者': 4,
}

# --- 決裁者役職キーワード ---
DECISION_MAKER_TITLES = [
    '代表取締役', '社長', '代表', '取締役', '役員',
    '理事長', '理事', '施設長', '院長', '園長',
    '所長', '会長', '専務', '常務', '部長',
    '支配人', '統括', 'オーナー', '経営者',
]
MANAGER_WALL_KEYWORDS = ['総務', '人事', '管理部', '管理課', '事務局', '労務']


# =====================================================================
# ユーティリティ関数
# =====================================================================

def normalize_phone(val):
    """電話番号正規化: 数字以外除去、10-11桁で0始まりのみ有効"""
    if pd.isna(val):
        return ''
    digits = re.sub(r'[^\d]', '', str(val))
    if 10 <= len(digits) <= 11 and digits.startswith('0'):
        return digits
    return ''


def parse_emp(val):
    """従業員数を整数に変換"""
    if pd.isna(val):
        return 0
    s = str(val).replace(',', '').replace('，', '').replace('人', '').strip()
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return 0


def extract_pref_city(address):
    """住所から都道府県+市区町村を抽出"""
    if pd.isna(address):
        return '', ''
    addr = str(address)
    pref_match = re.match(r'^(北海道|東京都|(?:京都|大阪)府|.{2,3}県)', addr)
    if not pref_match:
        return '', ''
    pref = pref_match.group(1)
    rest = addr[len(pref):]
    city_match = re.match(r'^(.+?[市区町村])', rest)
    city = city_match.group(1) if city_match else ''
    return pref, city


def clean_corporate_number(val):
    """法人番号クリーニング: .0除去、13桁チェック"""
    if pd.isna(val) or not val:
        return ''
    s = str(val).strip()
    if '.' in s:
        try:
            s = str(int(float(s)))
        except (ValueError, OverflowError):
            return ''
    s = re.sub(r'[^\d]', '', s)
    return s if len(s) == 13 else ''


def clean_date(val):
    """日付フォーマット: YYYY/MM/DD -> YYYY-MM-DD"""
    if pd.isna(val) or not val:
        return ''
    s = str(val).strip()[:10]
    s = s.replace('/', '-')
    if re.match(r'^\d{4}-\d{2}-\d{2}$', s):
        return s
    return ''


def validate_email(email):
    """メールバリデーション"""
    if pd.isna(email) or not email:
        return ''
    email = str(email).strip()
    if not re.match(r'^[^@]+@[^@]+\.[^@]+$', email):
        return ''
    return email


def normalize_name(name):
    """名前の正規化（空白除去）"""
    if pd.isna(name):
        return ''
    return re.sub(r'\s+', '', str(name).strip())


def get_surname(name):
    """苗字を抽出"""
    n = normalize_name(name)
    if not n:
        return ''
    parts = re.split(r'[\s　]+', str(name).strip())
    return parts[0] if parts else n


# =====================================================================
# STEP 1: データ読み込み・結合
# =====================================================================

def normalize_column_names(df):
    """M300/M100(4)のカラム名差異を統一（M100(4)形式に合わせる）"""
    rename_map = {
        # M300 -> M100(4) 統一名
        '担当者電話番号': '選考担当者ＴＥＬ',
        '担当者氏名（漢字）': '選考担当者氏名漢字',
        '担当者氏名（カナ）': '選考担当者氏名フリガナ',
        '担当者課係名／役職名': '選考担当者課係名／役職名',
        '担当者Ｅメール': '選考担当者Ｅメール',
        '担当者ＦＡＸ番号': '選考担当者ＦＡＸ',
        '担当者内線': '選考担当者内線',
        '役職': '代表者役職',
    }
    existing = {k: v for k, v in rename_map.items() if k in df.columns and v not in df.columns}
    if existing:
        df = df.rename(columns=existing)
    return df


def step1_load_data():
    """両CSVを読み込み、カラム名統一後に求人番号で重複除去して結合"""
    print('=' * 60)
    print('STEP 1: データ読み込み・結合')
    print('=' * 60)

    df1 = pd.read_csv(CSV_FILE_1, encoding='cp932', dtype=str)
    df2 = pd.read_csv(CSV_FILE_2, encoding='cp932', dtype=str)
    print(f'  ファイル(M300): {len(df1):,}件 ({len(df1.columns)}カラム)')
    print(f'  ファイル(M100(4)): {len(df2):,}件 ({len(df2.columns)}カラム)')

    # カラム名統一（M300のカラム名をM100(4)形式にリネーム）
    df1 = normalize_column_names(df1)
    df2 = normalize_column_names(df2)
    print(f'  カラム名統一完了')

    # M100(4)固有カラム確認
    for col in ['募集理由区分', '採用人数']:
        in1 = col in df1.columns
        in2 = col in df2.columns
        print(f'  {col}: M300={in1}, M100(4)={in2}')

    # 必要カラムのみ残してメモリ節約
    KEEP_COLS = [
        '求人番号', '事業所名漢字', '事業所名カナ', '事業所郵便番号', '事業所所在地',
        '事業所ホームページ', '産業分類（コード）', '産業分類（名称）',
        '職業分類１（コード）', '職業分類２（コード）', '職業分類３（コード）',
        '職種', '雇用形態', '従業員数企業全体（コード）', '法人番号', '代表者名',
        '代表者役職', '選考担当者ＴＥＬ', '選考担当者氏名漢字', '選考担当者氏名フリガナ',
        '選考担当者課係名／役職名', '選考担当者Ｅメール', '選考担当者ＦＡＸ',
        '受付年月日（西暦）', '求人有効年月日（西暦）', '設立年月日（西暦）',
        '募集理由区分', '採用人数', '採用人数（コード）', '従業員数就業場所（コード）',
    ]
    keep1 = [c for c in KEEP_COLS if c in df1.columns]
    keep2 = [c for c in KEEP_COLS if c in df2.columns]
    df1 = df1[keep1]
    df2 = df2[keep2]

    df = pd.concat([df1, df2], ignore_index=True)
    del df1, df2
    before = len(df)
    df = df.drop_duplicates(subset=['求人番号'], keep='first')
    print(f'  重複除去: {before:,} -> {len(df):,}件 (-{before - len(df):,})')

    # 電話番号正規化
    df['電話_正規化'] = df['選考担当者ＴＥＬ'].apply(normalize_phone)
    df['従業員数_数値'] = df['従業員数企業全体（コード）'].apply(parse_emp)

    # 電話番号あり件数
    has_phone = (df['電話_正規化'] != '').sum()
    print(f'  電話番号あり: {has_phone:,}件')

    return df


# =====================================================================
# STEP 2: セグメント抽出（パターン(1) OR (2)）
# =====================================================================

def extract_segment(df, seg_config):
    """セグメント定義に基づいてデータを抽出（パターン(1)職種 OR パターン(2)産業）

    C-Fセグメントはシンプルな構造:
    - industry_names_whitelist / industry_names_exclude は使用しない
    - industry_codes / industry_keywords でパターン(2)判定
    - job_codes / job_major_prefixes でパターン(1)判定
    """
    industry_codes = seg_config.get('industry_codes', [])
    industry_keywords = seg_config.get('industry_keywords', [])
    job_codes = seg_config.get('job_codes', [])
    job_major_prefixes = seg_config.get('job_major_prefixes', [])

    def matches(row):
        ind_name = str(row.get('産業分類（名称）', ''))
        ind_code = str(row.get('産業分類（コード）', ''))

        # パターン(2): 産業分類コード or キーワード
        pattern2 = False
        if industry_codes:
            code_2 = ind_code[:2] if len(ind_code) >= 2 else ind_code
            if code_2 in industry_codes:
                pattern2 = True
        if not pattern2 and industry_keywords:
            if any(kw in ind_name for kw in industry_keywords):
                pattern2 = True

        # パターン(1): 職種分類コード
        pattern1 = False
        job_code_cols = ['職業分類１（コード）', '職業分類２（コード）', '職業分類３（コード）']

        for col in job_code_cols:
            jc = str(row.get(col, ''))
            if pd.isna(row.get(col)) or jc == 'nan':
                continue

            # コード完全一致
            if jc in job_codes:
                pattern1 = True
                break
            # 大分類プレフィックス一致
            for prefix in job_major_prefixes:
                if jc.startswith(prefix):
                    pattern1 = True
                    break
            if pattern1:
                break

        return pattern1 or pattern2

    return df[df.apply(matches, axis=1)]


def step2_extract(df):
    """セグメントC/D/E/Fを抽出"""
    print('\n' + '=' * 60)
    print('STEP 2: セグメント抽出（C/D/E/F）')
    print('=' * 60)

    results = {}
    for seg_name, seg_config in SEGMENTS.items():
        extracted = extract_segment(df, seg_config)
        results[seg_name] = extracted
        print(f'  {seg_name}: {len(extracted):,}件')

        # 産業分類内訳
        top5 = extracted['産業分類（名称）'].value_counts().head(5)
        for ind, cnt in top5.items():
            print(f'    - {ind}: {cnt:,}')

    # セグメント間の重複チェック
    all_job_nums = {}
    for seg_name, seg_df in results.items():
        for jn in seg_df['求人番号']:
            if jn in all_job_nums:
                all_job_nums[jn].append(seg_name)
            else:
                all_job_nums[jn] = [seg_name]
    overlap_count = sum(1 for v in all_job_nums.values() if len(v) > 1)
    if overlap_count > 0:
        print(f'\n  セグメント間重複: {overlap_count:,}件（先に該当したセグメントに帰属）')

    # 結合（重複除去: 求人番号ベース）
    all_cf = pd.concat(results.values(), ignore_index=True)
    before_dedup = len(all_cf)
    all_cf = all_cf.drop_duplicates(subset=['求人番号'], keep='first')
    print(f'\n  C+D+E+F結合（重複除去後）: {len(all_cf):,}件 (重複除去 -{before_dedup - len(all_cf):,})')

    # セグメントラベル付与（後続で使用）
    seg_label_map = {}
    for seg_name, seg_df in results.items():
        for jn in seg_df['求人番号']:
            if jn not in seg_label_map:
                seg_label_map[jn] = seg_name
    all_cf['_segment'] = all_cf['求人番号'].map(seg_label_map)

    return all_cf, results


# =====================================================================
# STEP 3: 品質フィルタ（セグメント別人口閾値）
# =====================================================================

def step3_quality_filter(df):
    """パート除外・従業員数フィルタ・人口フィルタ（セグメント別閾値）"""
    print('\n' + '=' * 60)
    print('STEP 3: 品質フィルタ')
    print('=' * 60)

    initial = len(df)

    # 3-1: パート除外
    df_filtered = df[~df['雇用形態'].str.contains('パート', na=False)]
    print(f'  パート除外: {initial:,} -> {len(df_filtered):,} (-{initial - len(df_filtered):,})')

    # 3-2: 従業員数フィルタ (11-150)
    before = len(df_filtered)
    df_filtered = df_filtered[
        (df_filtered['従業員数_数値'] >= EMP_MIN) &
        (df_filtered['従業員数_数値'] <= EMP_MAX)
    ]
    print(f'  従業員数 {EMP_MIN}-{EMP_MAX}: {before:,} -> {len(df_filtered):,} (-{before - len(df_filtered):,})')

    # 3-3: 人口フィルタ（セグメント別閾値）
    if POP_FILE.exists():
        with open(POP_FILE, 'r', encoding='utf-8') as f:
            pop_map = json.load(f)

        before = len(df_filtered)
        pref_city = df_filtered['事業所所在地'].apply(lambda x: extract_pref_city(x))
        df_filtered = df_filtered.copy()
        df_filtered['_pref'] = pref_city.apply(lambda x: x[0])
        df_filtered['_city'] = pref_city.apply(lambda x: x[1])
        df_filtered['_pop_key'] = df_filtered['_pref'] + df_filtered['_city']
        df_filtered['_population'] = df_filtered['_pop_key'].map(pop_map).fillna(0).astype(int)

        # セグメント別人口閾値を適用
        pop_thresholds = {seg: cfg['pop_threshold'] for seg, cfg in SEGMENTS.items()}
        df_filtered['_pop_threshold'] = df_filtered['_segment'].map(pop_thresholds).fillna(50000).astype(int)
        pop_pass = df_filtered['_population'] >= df_filtered['_pop_threshold']
        df_filtered = df_filtered[pop_pass]

        # セグメント別除外状況
        print(f'  人口フィルタ（セグメント別閾値）: {before:,} -> {len(df_filtered):,} (-{before - len(df_filtered):,})')
        for seg, threshold in pop_thresholds.items():
            seg_count = (df_filtered['_segment'] == seg).sum()
            print(f'    {seg} (>={threshold:,}人): {seg_count:,}件')
    else:
        print(f'  警告: 人口データなし: {POP_FILE}')

    # 3-4: 電話番号なしを除外
    before = len(df_filtered)
    df_filtered = df_filtered[df_filtered['電話_正規化'] != '']
    print(f'  電話番号あり: {before:,} -> {len(df_filtered):,} (-{before - len(df_filtered):,})')

    print(f'\n  品質フィルタ後: {len(df_filtered):,}件 (除外率: {(1 - len(df_filtered)/initial)*100:.1f}%)')
    return df_filtered


# =====================================================================
# STEP 4: 重複排除
# =====================================================================

def step4_dedup(df):
    """電話番号重複 + 法人番号重複排除"""
    print('\n' + '=' * 60)
    print('STEP 4: 重複排除')
    print('=' * 60)

    initial = len(df)

    # 4-0: 同一電話番号の募集職種を事前集約（dedup前に全職種を記録）
    _job_agg = df[df['電話_正規化'] != ''].groupby('電話_正規化')['職種'].apply(
        lambda x: list(x.unique())
    ).to_dict()

    # 雇用形態優先度
    df = df.copy()
    df['_emp_pri'] = df['雇用形態'].map(EMPLOYMENT_PRIORITY).fillna(5)

    # 4-1: 電話番号重複排除（正社員優先）
    before = len(df)
    df = df.sort_values(['_emp_pri'], ascending=True)
    df = df.drop_duplicates(subset=['電話_正規化'], keep='first')
    print(f'  電話番号重複除去: {before:,} -> {len(df):,} (-{before - len(df):,})')

    # 4-2: 法人番号重複排除
    df['法人番号_clean'] = df['法人番号'].apply(clean_corporate_number)
    has_corp = df[df['法人番号_clean'] != '']
    no_corp = df[df['法人番号_clean'] == '']

    before_corp = len(has_corp)

    def phone_score(phone):
        """本社番号（一般的市外局番）を優先"""
        digits = str(phone)
        if re.match(r'^0[3-9]\d{8}$', digits):
            return 3
        if re.match(r'^0\d{9}$', digits):
            return 2
        if re.match(r'^0\d{10}$', digits):
            return 1
        return 0

    has_corp = has_corp.copy()
    has_corp['_phone_score'] = has_corp['電話_正規化'].apply(phone_score)
    has_corp = has_corp.sort_values(['_phone_score'], ascending=False)
    has_corp = has_corp.drop_duplicates(subset=['法人番号_clean'], keep='first')
    print(f'  法人番号重複除去: {before_corp:,} -> {len(has_corp):,} (-{before_corp - len(has_corp):,})')

    df = pd.concat([has_corp, no_corp], ignore_index=True)
    print(f'\n  重複排除後: {len(df):,}件 (初期比 -{initial - len(df):,})')

    # 4-3: 同一事業所の他募集職種情報を付与
    def get_other_jobs(row):
        phone = row['電話_正規化']
        my_job = str(row.get('職種', ''))
        all_jobs = _job_agg.get(phone, [])
        others = [str(j)[:30] for j in all_jobs if str(j) != my_job][:4]
        return '／'.join(others) if others else ''

    df['_other_jobs'] = df.apply(get_other_jobs, axis=1)
    multi = (df['_other_jobs'] != '').sum()
    print(f'  複数職種募集中の事業所: {multi:,}件')

    return df


# =====================================================================
# STEP 5: Salesforce突合
# =====================================================================

def step5_sf_matching(df):
    """Salesforceの既存レコードと電話番号で突合"""
    print('\n' + '=' * 60)
    print('STEP 5: Salesforce突合')
    print('=' * 60)

    # SFバックアップデータ読み込み
    acc_file = sorted(DATA_DIR.glob('Account_*.csv'), key=lambda p: p.stat().st_mtime, reverse=True)
    con_file = sorted(DATA_DIR.glob('Contact_*.csv'), key=lambda p: p.stat().st_mtime, reverse=True)
    lead_file = sorted(DATA_DIR.glob('Lead_*.csv'), key=lambda p: p.stat().st_mtime, reverse=True)

    sf_phones = set()

    if acc_file:
        acc = pd.read_csv(acc_file[0], encoding='utf-8-sig', dtype=str)
        for col in ['Phone', 'Phone2__c']:
            if col in acc.columns:
                phones = acc[col].apply(normalize_phone)
                sf_phones.update(phones[phones != ''])
        print(f'  Account: {len(acc):,}件 ({acc_file[0].name})')

    if con_file:
        con = pd.read_csv(con_file[0], encoding='utf-8-sig', dtype=str)
        for col in ['Phone', 'Phone2__c', 'MobilePhone', 'MobilePhone2__c']:
            if col in con.columns:
                phones = con[col].apply(normalize_phone)
                sf_phones.update(phones[phones != ''])
        print(f'  Contact: {len(con):,}件 ({con_file[0].name})')

    if lead_file:
        lead = pd.read_csv(lead_file[0], encoding='utf-8-sig', dtype=str)
        for col in ['Phone', 'MobilePhone', 'Phone2__c', 'MobilePhone2__c']:
            if col in lead.columns:
                phones = lead[col].apply(normalize_phone)
                sf_phones.update(phones[phones != ''])
        converted_count = (lead['Status'] == '取引開始済').sum() if 'Status' in lead.columns else 0
        print(f'  Lead: {len(lead):,}件（うち取引開始済 {converted_count:,}件も突合対象に含む）({lead_file[0].name})')

    print(f'  SF電話番号セット: {len(sf_phones):,}件')

    # 突合
    df['_is_existing'] = df['電話_正規化'].isin(sf_phones)
    existing = df[df['_is_existing']]
    new_leads = df[~df['_is_existing']]

    print(f'\n  既存マッチ: {len(existing):,}件')
    print(f'  新規リード候補: {len(new_leads):,}件')

    return new_leads, existing


# =====================================================================
# STEP 6: 成約先除外
# =====================================================================

def step6_contract_exclusion(df):
    """成約先の電話番号+法人番号で除外"""
    print('\n' + '=' * 60)
    print('STEP 6: 成約先除外')
    print('=' * 60)

    contract_file = sorted(DATA_DIR.glob('contract_accounts_*.csv'),
                           key=lambda p: p.stat().st_mtime, reverse=True)

    if not contract_file:
        print('  警告: 成約先データなし - Salesforceから取得が必要')
        return df, 0

    contracts = pd.read_csv(contract_file[0], encoding='utf-8-sig', dtype=str)
    print(f'  成約先データ: {len(contracts):,}件 ({contract_file[0].name})')

    # 成約先電話番号セット
    contract_phones = set()
    for col in ['Phone', 'Phone2__c']:
        if col in contracts.columns:
            phones = contracts[col].apply(normalize_phone)
            contract_phones.update(phones[phones != ''])

    # 成約先法人番号セット
    contract_corps = set()
    for col in ['CorporateIdentificationNumber__c', 'CorporateNumber__c']:
        if col in contracts.columns:
            corps = contracts[col].apply(clean_corporate_number)
            contract_corps.update(corps[corps != ''])

    print(f'  成約先電話番号: {len(contract_phones):,}件')
    print(f'  成約先法人番号: {len(contract_corps):,}件')

    # 除外
    phone_match = df['電話_正規化'].isin(contract_phones)
    corp_match = df['法人番号_clean'].isin(contract_corps)
    is_contract = phone_match | corp_match

    excluded_count = is_contract.sum()
    df_safe = df[~is_contract]

    print(f'\n  成約先除外: {excluded_count:,}件')
    print(f'  安全な新規リード: {len(df_safe):,}件')

    return df_safe, excluded_count


# =====================================================================
# STEP 7: 決裁者近接スコア
# =====================================================================

def step7_proximity_score(df):
    """決裁者近接スコア（★1-5）を計算"""
    print('\n' + '=' * 60)
    print('STEP 7: 決裁者近接スコア')
    print('=' * 60)

    df = df.copy()

    def calc_indicators(row):
        rep_name = normalize_name(row.get('代表者名', ''))
        contact_name = normalize_name(row.get('選考担当者氏名漢字', ''))
        contact_title = str(row.get('選考担当者課係名／役職名', ''))
        company = str(row.get('事業所名漢字', ''))
        phone = str(row.get('電話_正規化', ''))
        emp = parse_emp(row.get('従業員数企業全体（コード）', '0'))

        # 代表者直通: 代表者名 = 選考担当者名
        is_direct = (rep_name != '' and contact_name != '' and rep_name == contact_name)

        # 携帯判定
        is_mobile = bool(re.match(r'^0[789]0', phone))

        # 同姓親族: 苗字が同じだが異なる人
        rep_surname = get_surname(row.get('代表者名', ''))
        con_surname = get_surname(row.get('選考担当者氏名漢字', ''))
        is_family = (rep_surname != '' and con_surname != '' and
                     rep_surname == con_surname and rep_name != contact_name)

        # 決裁者役職
        is_dm_title = any(kw in contact_title for kw in DECISION_MAKER_TITLES)

        # 管理部門壁
        is_wall = any(kw in contact_title for kw in MANAGER_WALL_KEYWORDS)

        # 小規模判定
        is_small = emp <= 10

        # 家族経営名: 社名に代表者の苗字が含まれる
        is_family_biz = (rep_surname != '' and rep_surname in company)

        # HP有無
        has_hp = bool(row.get('事業所ホームページ', '')) and str(row.get('事業所ホームページ', '')) != 'nan'

        return pd.Series({
            '代表者直通': '○' if is_direct else '',
            '携帯判定': '携帯' if is_mobile else '固定',
            '同姓親族': '○' if is_family else '',
            '決裁者役職': '○' if is_dm_title else '',
            '管理部門壁': '○' if is_wall else '',
            '家族経営名': '○' if is_family_biz else '',
            'HP有無': '○' if has_hp else '',
        })

    indicators = df.apply(calc_indicators, axis=1)
    for col in indicators.columns:
        df[col] = indicators[col]

    # スコア計算
    def calc_score(row):
        is_direct = row['代表者直通'] == '○'
        is_mobile = row['携帯判定'] == '携帯'
        is_family = row['同姓親族'] == '○'
        is_dm_title = row['決裁者役職'] == '○'
        is_wall = row['管理部門壁'] == '○'
        is_small = parse_emp(row.get('従業員数企業全体（コード）', '0')) <= 10
        is_family_biz = row['家族経営名'] == '○'

        if is_direct and is_mobile:
            return 5
        if is_direct:
            return 4
        if is_family and is_small:
            return 4
        if is_dm_title and is_mobile:
            return 4
        if is_family:
            return 3
        if is_dm_title:
            return 3
        if is_family_biz and is_small:
            return 3
        if is_wall:
            return 1
        return 2

    df['近接スコア'] = df.apply(calc_score, axis=1)

    stars = {5: '★★★★★', 4: '★★★★☆', 3: '★★★☆☆', 2: '★★☆☆☆', 1: '★☆☆☆☆'}
    df['近接スコア_星'] = df['近接スコア'].map(stars)

    # 分布表示
    dist = df['近接スコア'].value_counts().sort_index()
    for score, count in dist.items():
        print(f'  {stars[score]} ({score}): {count:,}件 ({count/len(df)*100:.1f}%)')

    return df


# =====================================================================
# STEP 8: インポートCSV生成
# =====================================================================

def step8_generate_import_csv(df):
    """Salesforceインポート用CSVを生成"""
    print('\n' + '=' * 60)
    print('STEP 8: インポートCSV生成')
    print('=' * 60)

    output_dir = DATA_DIR / 'import_ready'
    output_dir.mkdir(parents=True, exist_ok=True)

    records = []
    skipped_no_company = 0
    skipped_no_phone = 0

    for _, row in df.iterrows():
        company = str(row.get('事業所名漢字', '')).strip()
        if not company or company == 'nan':
            skipped_no_company += 1
            continue

        phone_norm = str(row.get('電話_正規化', ''))
        if not phone_norm:
            skipped_no_phone += 1
            continue

        # 担当者名
        contact = str(row.get('選考担当者氏名漢字', '')).strip()
        if not contact or contact == 'nan':
            contact = '担当者'

        # 電話番号フィールド設定
        is_mobile = bool(re.match(r'^0[789]0', phone_norm))
        phone_field = phone_norm
        mobile_field = phone_norm if is_mobile else ''

        # 都道府県
        pref = str(row.get('_pref', ''))
        if not pref or pref == 'nan':
            pref_city = extract_pref_city(row.get('事業所所在地', ''))
            pref = pref_city[0]

        # 法人番号
        corp_num = clean_corporate_number(row.get('法人番号', ''))

        # 日付
        pub_date = clean_date(row.get('受付年月日（西暦）', ''))
        exp_date = clean_date(row.get('求人有効年月日（西暦）', ''))

        # メール
        email = validate_email(row.get('選考担当者Ｅメール', ''))

        # セグメント・メモ情報
        seg = str(row.get('_segment', 'CF'))
        stars = str(row.get('近接スコア_星', '★★☆☆☆'))
        job = str(row.get('職種', ''))
        emp_type = str(row.get('雇用形態', ''))
        ind = str(row.get('産業分類（名称）', ''))
        emp_count = parse_emp(row.get('従業員数企業全体（コード）', '0'))

        # 同一事業所の他募集職種
        other_jobs = str(row.get('_other_jobs', ''))
        other_jobs_line = ''
        if other_jobs and other_jobs != 'nan' and other_jobs != '':
            other_jobs_line = f'\n同事業所の他募集: {other_jobs}'

        # 募集理由・採用人数
        recruit_reason = str(row.get('募集理由区分', '')).replace('nan', '')
        recruit_num = str(row.get('採用人数', '')).replace('nan', '')
        recruit_reason_line = f'\n募集理由: {recruit_reason}' if recruit_reason else ''
        recruit_num_line = f'\n採用人数: {recruit_num}' if recruit_num else ''

        # Publish_ImportText__c: 産業分類を追加（C-F固有）
        publish_text = (
            f'[{TODAY_ISO} ハロワ新規_{seg}]\n'
            f'セグメント: {seg}\n'
            f'産業分類: {ind}\n'
            f'職種: {job}\n'
            f'雇用形態: {emp_type}\n'
            f'従業員数: {emp_count}\n'
            f'近接スコア: {stars}'
            f'{recruit_reason_line}'
            f'{recruit_num_line}'
            f'{other_jobs_line}'
        )

        lead_source_memo = f'{TODAY}_ハロワ_{seg}_{job}【{emp_type}】'

        # 従業員数
        emp_val = emp_count if emp_count > 0 else ''

        # 設立年
        establish = clean_date(row.get('設立年月日（西暦）', ''))

        # Webサイト
        website = str(row.get('事業所ホームページ', ''))
        if website == 'nan':
            website = ''

        record = {
            'Company': company,
            'LastName': contact,
            'Phone': phone_field,
            'MobilePhone': mobile_field,
            'PostalCode': str(row.get('事業所郵便番号', '')),
            'Street': str(row.get('事業所所在地', '')),
            'Prefecture__c': pref,
            'NumberOfEmployees': emp_val,
            'CorporateNumber__c': corp_num,
            'Establish__c': establish,
            'Website': website,
            'Email': email,
            'Title': str(row.get('選考担当者課係名／役職名', '')).replace('nan', ''),
            'Name_Kana__c': str(row.get('事業所名カナ', '')).replace('nan', ''),
            'PresidentName__c': str(row.get('代表者名', '')).replace('nan', ''),
            'PresidentTitle__c': str(row.get('代表者役職', '')).replace('nan', ''),
            'Hellowork_JobPublicationDate__c': pub_date,
            'Hellowork_JobClosedDate__c': exp_date,
            'Hellowork_Industry__c': ind,
            'Hellowork_RecuritmentType__c': job[:255] if len(job) > 255 else job,
            'Hellowork_EmploymentType__c': emp_type,
            'Hellowork_RecruitmentReasonCategory__c': recruit_reason,
            'Hellowork_NumberOfRecruitment__c': parse_emp(row.get('採用人数（コード）', '0')) or '',
            'Hellowork_NumberOfEmployee_Office__c': parse_emp(row.get('従業員数就業場所（コード）', '0')) or '',
            'Hellowork_DataImportDate__c': TODAY_ISO,
            'Publish_ImportText__c': publish_text,
            'LeadSourceMemo__c': lead_source_memo[:255] if len(lead_source_memo) > 255 else lead_source_memo,
            'LeadSource': 'ハローワーク',
            'Status': '未架電',
            # 参考用メタデータ（インポート時に除外可能）
            '_segment': seg,
            '_proximity_score': row.get('近接スコア', 2),
            '_proximity_stars': stars,
        }
        records.append(record)

    result_df = pd.DataFrame(records)
    # CorporateNumber__cがfloat化しないようstring型を保証
    if 'CorporateNumber__c' in result_df.columns:
        result_df['CorporateNumber__c'] = result_df['CorporateNumber__c'].astype(str).replace('nan', '')
    print(f'  有効レコード: {len(result_df):,}件')
    print(f'  スキップ（Company空）: {skipped_no_company}')
    print(f'  スキップ（Phone空）: {skipped_no_phone}')

    # セグメント別件数
    seg_dist = result_df['_segment'].value_counts()
    for seg, cnt in seg_dist.items():
        print(f'  {seg}: {cnt:,}件')

    # 近接スコア分布
    score_dist = result_df['_proximity_score'].value_counts().sort_index()
    stars_map = {5: '★★★★★', 4: '★★★★☆', 3: '★★★☆☆', 2: '★★☆☆☆', 1: '★☆☆☆☆'}
    print(f'\n  近接スコア分布:')
    for score, cnt in score_dist.items():
        star_label = stars_map.get(int(score), '?')
        print(f'    {star_label}: {cnt:,}件')

    # 出力
    output_path = output_dir / f'import_CF_{TODAY}.csv'
    result_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f'\n  出力: {output_path}')

    return result_df


# =====================================================================
# メイン実行
# =====================================================================

def main():
    print('━' * 60)
    print(f'ハローワーク セグメントC-F 新規リード作成パイプライン')
    print(f'実行日: {TODAY_ISO}')
    print(f'対象: C_工事 / D_ホテル旅館 / E_葬儀 / F_産業廃棄物')
    print('━' * 60)

    # STEP 1
    df = step1_load_data()

    # STEP 2（多角的職種フィルタなし: C-Fは産業内の全職種を含む）
    all_cf, seg_results = step2_extract(df)

    # STEP 3
    filtered = step3_quality_filter(all_cf)

    # STEP 4
    deduped = step4_dedup(filtered)

    # STEP 5
    new_leads, existing = step5_sf_matching(deduped)

    # STEP 6
    safe_leads, excluded = step6_contract_exclusion(new_leads)

    # STEP 7
    scored = step7_proximity_score(safe_leads)

    # STEP 8
    import_df = step8_generate_import_csv(scored)

    # サマリー
    print('\n' + '━' * 60)
    print('パイプライン完了サマリー')
    print('━' * 60)
    print(f'  入力データ: {len(df):,}件')
    print(f'  C-F抽出: {len(all_cf):,}件')
    print(f'  品質フィルタ後: {len(filtered):,}件')
    print(f'  重複排除後: {len(deduped):,}件')
    print(f'  SF既存マッチ: {len(existing):,}件')
    print(f'  新規リード候補: {len(new_leads):,}件')
    print(f'  成約先除外: {excluded}件')
    print(f'  最終インポート候補: {len(import_df):,}件')

    # セグメント別サマリー
    if len(import_df) > 0:
        print(f'\n  セグメント別内訳:')
        for seg in SEGMENTS.keys():
            cnt = (import_df['_segment'] == seg).sum()
            if cnt > 0:
                print(f'    {seg}: {cnt:,}件')

    # 中間ファイル保存
    matched_dir = DATA_DIR / 'matched'
    matched_dir.mkdir(parents=True, exist_ok=True)

    safe_leads.to_csv(matched_dir / f'new_leads_CF_all.csv', index=False, encoding='utf-8-sig')
    existing.to_csv(matched_dir / f'existing_CF_all.csv', index=False, encoding='utf-8-sig')
    print(f'\n  中間ファイル:')
    print(f'    {matched_dir / "new_leads_CF_all.csv"}')
    print(f'    {matched_dir / "existing_CF_all.csv"}')

    return import_df


if __name__ == '__main__':
    main()
