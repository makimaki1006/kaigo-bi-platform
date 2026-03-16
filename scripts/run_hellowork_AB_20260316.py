"""
ハローワーク セグメントA/B 2026-03-16
=============================================
STEP 0: SFデータリフレッシュ → パイプライン実行
入力: RCMEB002002_M100 (5).csv のみ
教訓反映: ご担当者統一、分割、完全重複排除
所有者割り当て: 件数確定後にユーザー指示を待つ
"""
import pandas as pd
import numpy as np
import re
import json
import sys
import time
import requests
from io import StringIO
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'src'))
sys.stdout.reconfigure(encoding='utf-8')

from api.salesforce_client import SalesforceClient

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / 'data' / 'output' / 'hellowork_segments'
OUTPUT_DIR = DATA_DIR
POP_FILE = BASE_DIR / 'data' / 'population' / 'city_population_map.json'

CSV_FILE = Path(r'C:\Users\fuji1\OneDrive\デスクトップ\RCMEB002002_M100 (5).csv')

TODAY = datetime.now().strftime('%Y%m%d')
TODAY_ISO = datetime.now().strftime('%Y-%m-%d')

# =====================================================================
# pipeline_hellowork_AB.py から定義をインポート
# =====================================================================
sys.path.insert(0, str(Path(__file__).resolve().parent))
from pipeline_hellowork_AB import (
    SEGMENTS, EXCLUDE_EMPLOYMENT_TYPES, EMP_MIN, EMP_MAX,
    EMPLOYMENT_PRIORITY, DECISION_MAKER_TITLES, MANAGER_WALL_KEYWORDS,
    MEDICAL_CARE_PREFIXES, DIVERSE_JOBS_ALLOWED,
    normalize_phone, parse_emp, extract_pref_city, clean_corporate_number,
    clean_date, validate_email, normalize_name, get_surname,
    normalize_column_names, extract_segment,
)

# =====================================================================
# 教訓反映: LastName処理
# =====================================================================
GENERIC_LASTNAMES = [
    '担当者', '採用担当者', '採用担当', '中途採用担当',
    '人事担当', '人事担当者', '総務担当', '総務担当者',
    '事務担当', '経理担当', '管理部', '人事部',
]
LASTNAME_SEPARATORS = re.compile(r'[／/・、,]')

def clean_lastname(name):
    """LastName処理: 分割→汎用名統一→空チェック"""
    if pd.isna(name) or not name or str(name).strip() in ('', 'nan'):
        return 'ご担当者'
    s = str(name).strip()
    # 分割: 複数名の場合は先頭1名のみ
    if LASTNAME_SEPARATORS.search(s):
        s = LASTNAME_SEPARATORS.split(s)[0].strip()
    # スペース・記号除去
    s = re.sub(r'[\s　]+', '', s)
    if not s:
        return 'ご担当者'
    # 汎用名統一
    if s in GENERIC_LASTNAMES:
        return 'ご担当者'
    return s


# =====================================================================
# STEP 0: SFデータリフレッシュ
# =====================================================================
def export_sf_data(client):
    """Salesforceから最新データをエクスポート"""
    print('=' * 60)
    print('STEP 0: Salesforceデータ リフレッシュ')
    print('=' * 60)

    headers = {**client._get_headers(), 'Content-Type': 'application/json'}
    api_version = client.api_version
    job_url = f'{client.instance_url}/services/data/{api_version}/jobs/query'

    objects = {
        'Lead': "SELECT Id,Phone,MobilePhone,Phone2__c,MobilePhone2__c,Company,Status,CorporateNumber__c FROM Lead",
        'Account': "SELECT Id,Phone,Phone2__c,Name,CorporateNumber__c FROM Account",
        'Contact': "SELECT Id,Phone,Phone2__c,MobilePhone,MobilePhone2__c,AccountId FROM Contact",
    }

    for obj_name, soql in objects.items():
        print(f'\n  {obj_name} エクスポート中...')
        resp = requests.post(job_url, headers=headers, json={
            'operation': 'query', 'query': soql, 'contentType': 'CSV'
        })
        resp.raise_for_status()
        job_id = resp.json()['id']

        for _ in range(120):
            time.sleep(5)
            sr = requests.get(f'{job_url}/{job_id}', headers=client._get_headers())
            state = sr.json()['state']
            if state == 'JobComplete':
                break
            if state in ('Failed', 'Aborted'):
                raise Exception(f'{obj_name} export failed: {sr.json()}')
            print(f'    {state}...')

        result = requests.get(f'{job_url}/{job_id}/results', headers=client._get_headers())
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        out_path = DATA_DIR / f'{obj_name}_{ts}.csv'
        with open(out_path, 'wb') as f:
            f.write(result.content)
        lines = result.text.count('\n')
        print(f'  {obj_name}: {lines:,}件 → {out_path.name}')

    # 成約先エクスポート
    print(f'\n  成約先エクスポート中...')
    contract_soql = (
        "SELECT Id,Name,Phone,Phone2__c,CorporateIdentificationNumber__c,CorporateNumber__c,Status__c,RelatedAccountFlg__c "
        "FROM Account WHERE Status__c LIKE '%商談中%' "
        "OR Status__c LIKE '%プロジェクト進行中%' "
        "OR Status__c LIKE '%深耕対象%' "
        "OR Status__c LIKE '%過去客%' "
        "OR RelatedAccountFlg__c = 'グループ案件進行中' "
        "OR RelatedAccountFlg__c = 'グループ過去案件実績あり'"
    )
    resp = requests.post(job_url, headers=headers, json={
        'operation': 'query', 'query': contract_soql, 'contentType': 'CSV'
    })
    resp.raise_for_status()
    job_id = resp.json()['id']

    for _ in range(120):
        time.sleep(5)
        sr = requests.get(f'{job_url}/{job_id}', headers=client._get_headers())
        state = sr.json()['state']
        if state == 'JobComplete':
            break
        if state in ('Failed', 'Aborted'):
            raise Exception(f'Contract export failed: {sr.json()}')
        print(f'    {state}...')

    result = requests.get(f'{job_url}/{job_id}/results', headers=client._get_headers())
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    out_path = DATA_DIR / f'contract_accounts_{ts}.csv'
    with open(out_path, 'wb') as f:
        f.write(result.content)
    lines = result.text.count('\n')
    print(f'  成約先: {lines:,}件 → {out_path.name}')

    print('\n  SFデータ リフレッシュ完了')


# =====================================================================
# STEP 1: データ読み込み（M100(5)のみ）
# =====================================================================
def step1_load_data():
    print('=' * 60)
    print('STEP 1: データ読み込み（M100(5)のみ）')
    print('=' * 60)

    df = pd.read_csv(CSV_FILE, encoding='cp932', dtype=str)
    print(f'  ファイル: {CSV_FILE.name}')
    print(f'  件数: {len(df):,}件 ({len(df.columns)}カラム)')

    df = normalize_column_names(df)
    print(f'  カラム名統一完了')

    for col in ['募集理由区分', '採用人数', '選考担当者ＴＥＬ', '産業分類（名称）', '職業分類１（コード）']:
        print(f'  {col}: {"あり" if col in df.columns else "なし"}')

    df['電話_正規化'] = df['選考担当者ＴＥＬ'].apply(normalize_phone)
    df['従業員数_数値'] = df['従業員数企業全体（コード）'].apply(parse_emp)

    has_phone = (df['電話_正規化'] != '').sum()
    print(f'  電話番号あり: {has_phone:,}件')

    return df


# =====================================================================
# STEP 2-7: パイプラインのSTEP（pipeline_hellowork_AB.pyから流用）
# =====================================================================
# step2_extract, step2b, step3, step4, step5, step6, step6b, step7 は
# pipeline_hellowork_AB.py のロジックをそのまま使うが、
# step5のSFファイル検索は最新データを使うようDATA_DIRを参照

def step2_extract(df):
    print('\n' + '=' * 60)
    print('STEP 2: セグメント抽出（2軸）')
    print('=' * 60)

    results = {}
    for seg_name, seg_config in SEGMENTS.items():
        extracted = extract_segment(df, seg_config)
        results[seg_name] = extracted
        print(f'  {seg_name}: {len(extracted):,}件')
        top5 = extracted['産業分類（名称）'].value_counts().head(5)
        for ind, cnt in top5.items():
            print(f'    - {ind}: {cnt:,}')

    all_ab = pd.concat(results.values(), ignore_index=True)
    all_ab = all_ab.drop_duplicates(subset=['求人番号'], keep='first')
    print(f'\n  A+B結合（重複除去後）: {len(all_ab):,}件')
    return all_ab, results


def step2b_diverse_job_filter(df):
    print('\n' + '=' * 60)
    print('STEP 2b: 多角的職種フィルタ')
    print('=' * 60)

    initial = len(df)
    job_code = df['職業分類１（コード）'].fillna('')
    mid_code = job_code.str[:3]
    is_medical = mid_code.isin(MEDICAL_CARE_PREFIXES)
    is_diverse_allowed = job_code.isin(DIVERSE_JOBS_ALLOWED)
    keep = is_medical | is_diverse_allowed
    df_filtered = df[keep].copy()
    excluded = df[~keep]

    print(f'  医療介護専門職: {is_medical.sum():,}件')
    print(f'  許可多角的職種: {(is_diverse_allowed & ~is_medical).sum():,}件')
    print(f'  除外: {len(excluded):,}件')

    if len(excluded) > 0:
        excluded_codes = excluded['職業分類１（コード）'].value_counts().head(10)
        print(f'  除外職種TOP10:')
        for code, cnt in excluded_codes.items():
            sample = excluded[excluded['職業分類１（コード）'] == code]['職種'].iloc[0]
            print(f'    {code}: {cnt:,}件 ({sample[:30]})')

    print(f'\n  フィルタ後: {len(df_filtered):,}件 (除外率: {len(excluded)/initial*100:.1f}%)')
    return df_filtered


def step3_quality_filter(df):
    print('\n' + '=' * 60)
    print('STEP 3: 品質フィルタ')
    print('=' * 60)

    initial = len(df)

    df_f = df[~df['雇用形態'].str.contains('パート', na=False)]
    print(f'  パート除外: {initial:,} → {len(df_f):,} (-{initial - len(df_f):,})')

    before = len(df_f)
    df_f = df_f[(df_f['従業員数_数値'] >= EMP_MIN) & (df_f['従業員数_数値'] <= EMP_MAX)]
    print(f'  従業員数 {EMP_MIN}-{EMP_MAX}: {before:,} → {len(df_f):,} (-{before - len(df_f):,})')

    if POP_FILE.exists():
        with open(POP_FILE, 'r', encoding='utf-8') as f:
            pop_map = json.load(f)
        before = len(df_f)
        pref_city = df_f['事業所所在地'].apply(lambda x: extract_pref_city(x))
        df_f = df_f.copy()
        df_f['_pref'] = pref_city.apply(lambda x: x[0])
        df_f['_city'] = pref_city.apply(lambda x: x[1])
        df_f['_pop_key'] = df_f['_pref'] + df_f['_city']
        df_f['_population'] = df_f['_pop_key'].map(pop_map).fillna(0).astype(int)
        pop_threshold = 30000
        df_f = df_f[df_f['_population'] >= pop_threshold]
        print(f'  人口 ≥ {pop_threshold:,}: {before:,} → {len(df_f):,} (-{before - len(df_f):,})')
    else:
        print(f'  ⚠️ 人口データなし: {POP_FILE}')

    before = len(df_f)
    df_f = df_f[df_f['電話_正規化'] != '']
    print(f'  電話番号あり: {before:,} → {len(df_f):,} (-{before - len(df_f):,})')

    print(f'\n  品質フィルタ後: {len(df_f):,}件 (除外率: {(1 - len(df_f)/initial)*100:.1f}%)')
    return df_f


def step4_dedup(df):
    """電話番号重複 + 法人番号重複排除（教訓: 葬儀で重複インポートした反省を反映）"""
    print('\n' + '=' * 60)
    print('STEP 4: 重複排除 ※葬儀教訓: 完全な重複排除を保証')
    print('=' * 60)

    initial = len(df)

    # 事前に全職種情報を集約
    _job_agg = df[df['電話_正規化'] != ''].groupby('電話_正規化')['職種'].apply(
        lambda x: list(x.unique())
    ).to_dict()

    df = df.copy()
    df['_emp_pri'] = df['雇用形態'].map(EMPLOYMENT_PRIORITY).fillna(5)

    # 電話番号重複排除（正社員優先）
    before = len(df)
    df = df.sort_values(['_emp_pri'], ascending=True)
    df = df.drop_duplicates(subset=['電話_正規化'], keep='first')
    print(f'  電話番号重複除去: {before:,} → {len(df):,} (-{before - len(df):,})')

    # 法人番号重複排除
    df['法人番号_clean'] = df['法人番号'].apply(clean_corporate_number)
    has_corp = df[df['法人番号_clean'] != '']
    no_corp = df[df['法人番号_clean'] == '']

    before_corp = len(has_corp)
    def phone_score(phone):
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
    print(f'  法人番号重複除去: {before_corp:,} → {len(has_corp):,} (-{before_corp - len(has_corp):,})')

    df = pd.concat([has_corp, no_corp], ignore_index=True)

    # 自己レビュー: 電話番号のユニーク数を検証
    phone_unique = df['電話_正規化'].nunique()
    print(f'\n  ✅ 自己レビュー: 電話番号ユニーク数 = {phone_unique:,}（レコード数 {len(df):,}と一致すべき）')
    if phone_unique != len(df):
        print(f'  ⚠️ 電話番号重複あり！ 差分: {len(df) - phone_unique:,}件')
        # 追加で重複除去
        df = df.drop_duplicates(subset=['電話_正規化'], keep='first')
        print(f'  追加重複除去後: {len(df):,}件')

    print(f'\n  重複排除後: {len(df):,}件 (初期比 -{initial - len(df):,})')

    # 同一事業所の他募集職種情報を付与
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


def step5_sf_matching(df):
    print('\n' + '=' * 60)
    print('STEP 5: Salesforce突合')
    print('=' * 60)

    sf_phones = set()

    acc_file = sorted(DATA_DIR.glob('Account_*.csv'), key=lambda p: p.stat().st_mtime, reverse=True)
    con_file = sorted(DATA_DIR.glob('Contact_*.csv'), key=lambda p: p.stat().st_mtime, reverse=True)
    lead_file = sorted(DATA_DIR.glob('Lead_*.csv'), key=lambda p: p.stat().st_mtime, reverse=True)

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

    df['_is_existing'] = df['電話_正規化'].isin(sf_phones)
    existing = df[df['_is_existing']]
    new_leads = df[~df['_is_existing']]

    print(f'\n  既存マッチ: {len(existing):,}件')
    print(f'  新規リード候補: {len(new_leads):,}件')

    return new_leads, existing


def step6_contract_exclusion(df):
    print('\n' + '=' * 60)
    print('STEP 6: 成約先除外')
    print('=' * 60)

    contract_file = sorted(DATA_DIR.glob('contract_accounts_*.csv'),
                           key=lambda p: p.stat().st_mtime, reverse=True)

    if not contract_file:
        print('  ⚠️ 成約先データなし')
        return df, 0

    contracts = pd.read_csv(contract_file[0], encoding='utf-8-sig', dtype=str)
    print(f'  成約先データ: {len(contracts):,}件 ({contract_file[0].name})')

    contract_phones = set()
    for col in ['Phone', 'Phone2__c']:
        if col in contracts.columns:
            phones = contracts[col].apply(normalize_phone)
            contract_phones.update(phones[phones != ''])

    contract_corps = set()
    for col in ['CorporateIdentificationNumber__c', 'CorporateNumber__c']:
        if col in contracts.columns:
            corps = contracts[col].apply(clean_corporate_number)
            contract_corps.update(corps[corps != ''])

    print(f'  成約先電話番号: {len(contract_phones):,}件')
    print(f'  成約先法人番号: {len(contract_corps):,}件')

    phone_match = df['電話_正規化'].isin(contract_phones)
    corp_match = df['法人番号_clean'].isin(contract_corps) & (df['法人番号_clean'] != '')
    is_contract = phone_match | corp_match

    excluded_count = is_contract.sum()
    df_safe = df[~is_contract]

    print(f'\n  🔴 成約先除外: {excluded_count:,}件')
    print(f'  安全な新規リード: {len(df_safe):,}件')

    return df_safe, excluded_count


def step6b_additional_exclusion(df):
    """STEP 6.5: 訪問看護除外 + 労働者派遣業除外 + 非P産業×非医療職種ノイズ除外

    ルール:
    - 訪問看護 → 常に除外
    - 労働者派遣業 → 常に除外（競合）
    - 非P産業 + 医療介護系職種コード(Pattern①) → 残す
    - 非P産業 + 非医療介護系職種 → 除外（ノイズ）
    """
    print('\n' + '=' * 60)
    print('STEP 6.5: 追加除外フィルタ（セグメントA/B固有）')
    print('=' * 60)

    before = len(df)

    # --- 訪問看護除外 ---
    VISITING_NURSE_KW = ['訪問看護']
    VISITING_NURSE_COMPANY_KW = ['訪問看護', 'ナーシング']

    mask_job = df['職種'].str.contains('|'.join(VISITING_NURSE_KW), na=False)
    mask_company = df['事業所名漢字'].str.contains('|'.join(VISITING_NURSE_COMPANY_KW), na=False)
    visiting_nurse_mask = mask_job | mask_company
    vn_count = visiting_nurse_mask.sum()

    if vn_count > 0:
        print(f'  訪問看護除外: {vn_count}件')

    df = df[~visiting_nurse_mask]

    # --- 労働者派遣業除外（競合） ---
    dispatch_mask = df['産業分類（名称）'].str.contains('労働者派遣業', na=False)
    dispatch_count = dispatch_mask.sum()
    if dispatch_count > 0:
        print(f'  労働者派遣業除外（競合）: {dispatch_count}件')
    df = df[~dispatch_mask]

    # --- 非P産業 × 非医療介護系職種 → ノイズ除外 ---
    # P産業（83/84/85）ならそのまま残す
    # 非P産業でも医療介護系職種コード（Pattern①）なら残す
    ind_code_col = '産業分類（コード）'
    if ind_code_col in df.columns:
        codes = df[ind_code_col].astype(str)
        is_p = codes.str.startswith(('83', '84', '85'))

        # 医療介護系職種コード判定
        from pipeline_hellowork_AB import MEDICAL_CARE_PREFIXES
        job_code = df['職業分類１（コード）'].fillna('')
        is_medical_job = job_code.str[:3].isin(MEDICAL_CARE_PREFIXES)

        # 非P産業 かつ 非医療職種 → ノイズ
        noise_mask = (~is_p) & (~is_medical_job)
        noise_count = noise_mask.sum()

        if noise_count > 0:
            print(f'  非P産業×非医療職種ノイズ除外: {noise_count}件')
            noise_df = df[noise_mask]
            for _, r in noise_df.head(5).iterrows():
                print(f'    - [{r["産業分類（名称）"]}] {r["職種"][:40]}')

        # 非P産業 かつ 医療職種 → 残す（ログ出力）
        kept_non_p = (~is_p) & is_medical_job
        kept_count = kept_non_p.sum()
        if kept_count > 0:
            print(f'  非P産業×医療職種（Pattern①）→ 残す: {kept_count}件')

        df = df[~noise_mask]

    after = len(df)
    print(f'\n  除外前: {before}件 → 除外後: {after}件（-{before - after}件）')

    return df, before - after


def step7_proximity_score(df):
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

        is_direct = (rep_name != '' and contact_name != '' and rep_name == contact_name)
        is_mobile = bool(re.match(r'^0[789]0', phone))

        rep_surname = get_surname(row.get('代表者名', ''))
        con_surname = get_surname(row.get('選考担当者氏名漢字', ''))
        is_family = (rep_surname != '' and con_surname != '' and
                     rep_surname == con_surname and rep_name != contact_name)

        is_dm_title = any(kw in contact_title for kw in DECISION_MAKER_TITLES)
        is_wall = any(kw in contact_title for kw in MANAGER_WALL_KEYWORDS)
        is_small = emp <= 10
        is_family_biz = (rep_surname != '' and rep_surname in company)

        return pd.Series({
            '代表者直通': '○' if is_direct else '',
            '携帯判定': '携帯' if is_mobile else '固定',
            '同姓親族': '○' if is_family else '',
            '決裁者役職': '○' if is_dm_title else '',
            '管理部門壁': '○' if is_wall else '',
            '家族経営名': '○' if is_family_biz else '',
        })

    indicators = df.apply(calc_indicators, axis=1)
    for col in indicators.columns:
        df[col] = indicators[col]

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

    dist = df['近接スコア'].value_counts().sort_index()
    for score, count in dist.items():
        print(f'  {stars[score]} ({score}): {count:,}件 ({count/len(df)*100:.1f}%)')

    return df


# =====================================================================
# STEP 8: インポートCSV生成（所有者なし版）
# =====================================================================
def step8_generate_import_csv(df):
    """所有者割り当てなしのインポートCSV生成"""
    print('\n' + '=' * 60)
    print('STEP 8: インポートCSV生成（所有者割り当てなし）')
    print('=' * 60)

    output_dir = DATA_DIR / 'import_ready'
    output_dir.mkdir(parents=True, exist_ok=True)

    def classify_segment(row):
        ind = str(row.get('産業分類（名称）', ''))
        if ind in SEGMENTS['A_医療看護保健']['industry_names_whitelist']:
            return 'A_医療看護保健'
        if ind in SEGMENTS['B_介護福祉']['industry_names_whitelist']:
            return 'B_介護福祉'
        return 'AB_その他'

    df['_segment'] = df.apply(classify_segment, axis=1)

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

        # 教訓反映: LastName処理（ご担当者統一 + 分割）
        contact = clean_lastname(row.get('選考担当者氏名漢字', ''))

        # 電話番号フィールド
        is_mobile = bool(re.match(r'^0[789]0', phone_norm))
        phone_field = phone_norm
        # Phone先頭ゼロ補完（9桁→10桁）
        if len(phone_field) == 9:
            phone_field = '0' + phone_field
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

        # メモ
        seg = row.get('_segment', 'AB')
        stars_str = str(row.get('近接スコア_星', '★★☆☆☆'))
        job = str(row.get('職種', ''))
        emp_type = str(row.get('雇用形態', ''))
        ind = str(row.get('産業分類（名称）', ''))
        emp_count = parse_emp(row.get('従業員数企業全体（コード）', '0'))

        other_jobs = str(row.get('_other_jobs', ''))
        other_jobs_line = f'\n同事業所の他募集: {other_jobs}' if other_jobs and other_jobs not in ('nan', '') else ''

        recruit_reason = str(row.get('募集理由区分', '')).replace('nan', '')
        recruit_num = str(row.get('採用人数', '')).replace('nan', '')
        recruit_reason_line = f'\n募集理由: {recruit_reason}' if recruit_reason else ''
        recruit_num_line = f'\n採用人数: {recruit_num}' if recruit_num else ''

        publish_text = (
            f'[{TODAY_ISO} ハロワ新規_{seg}]\n'
            f'セグメント: {seg}\n'
            f'業界: {ind}\n'
            f'職種: {job}\n'
            f'雇用形態: {emp_type}\n'
            f'従業員数: {emp_count}\n'
            f'近接スコア: {stars_str}'
            f'{recruit_reason_line}'
            f'{recruit_num_line}'
            f'{other_jobs_line}'
        )

        lead_source_memo = f'{TODAY}_ハロワ_{seg}_{job}【{emp_type}】'

        emp_val = emp_count if emp_count > 0 else ''
        establish = clean_date(row.get('設立年月日（西暦）', ''))
        website = str(row.get('事業所ホームページ', ''))
        if website == 'nan':
            website = ''

        record = {
            'Company': company,
            'LastName': contact,
            'Phone': phone_field,
            'MobilePhone': mobile_field,
            'PostalCode': str(row.get('事業所郵便番号', '')).replace('nan', ''),
            'Street': str(row.get('事業所所在地', '')).replace('nan', ''),
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
            'LeadSourceMemo__c': lead_source_memo[:255],
            'LeadSource': 'ハローワーク',
            'Status': '未架電',
            '_segment': seg,
            '_proximity_score': row.get('近接スコア', 2),
            '_proximity_stars': stars_str,
        }
        records.append(record)

    result_df = pd.DataFrame(records)
    if 'CorporateNumber__c' in result_df.columns:
        result_df['CorporateNumber__c'] = result_df['CorporateNumber__c'].astype(str).replace('nan', '')

    print(f'  有効レコード: {len(result_df):,}件')
    print(f'  スキップ（Company空）: {skipped_no_company}')
    print(f'  スキップ（Phone空）: {skipped_no_phone}')

    # セグメント別
    seg_dist = result_df['_segment'].value_counts()
    for seg, cnt in seg_dist.items():
        print(f'  {seg}: {cnt:,}件')

    # メールあり件数
    has_email = (result_df['Email'] != '').sum()
    print(f'  メールあり: {has_email:,}件')

    # 近接スコア分布
    score_dist = result_df['_proximity_score'].value_counts().sort_index()
    stars = {5: '★★★★★', 4: '★★★★☆', 3: '★★★☆☆', 2: '★★☆☆☆', 1: '★☆☆☆☆'}
    for score, cnt in score_dist.items():
        print(f'  {stars.get(score, "?")} ({score}): {cnt:,}件')

    # 自己レビュー: 電話番号ユニーク検証（最終CSV）
    phone_unique = result_df['Phone'].nunique()
    print(f'\n  ✅ 自己レビュー（最終CSV）: Phone ユニーク数 = {phone_unique:,} / レコード数 = {len(result_df):,}')
    if phone_unique != len(result_df):
        print(f'  ⚠️ 電話番号重複あり！ → 追加dedup実行')
        result_df = result_df.drop_duplicates(subset=['Phone'], keep='first')
        print(f'  dedup後: {len(result_df):,}件')

    # LastName分布確認
    ln_dist = result_df['LastName'].value_counts().head(10)
    print(f'\n  LastName TOP10:')
    for name, cnt in ln_dist.items():
        print(f'    {name}: {cnt:,}件')

    output_path = output_dir / f'import_AB_{TODAY}.csv'
    result_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f'\n  出力: {output_path}')

    return result_df


# =====================================================================
# メイン実行
# =====================================================================
def main():
    print('━' * 60)
    print(f'ハローワーク セグメントA/B パイプライン {TODAY_ISO}')
    print(f'入力: {CSV_FILE.name}')
    print(f'教訓反映: ご担当者統一、分割、完全重複排除')
    print('━' * 60)

    # STEP 0: SFデータリフレッシュ
    client = SalesforceClient()
    client.authenticate()
    export_sf_data(client)

    # STEP 1
    df = step1_load_data()

    # STEP 2
    all_ab, seg_results = step2_extract(df)

    # STEP 2b
    all_ab = step2b_diverse_job_filter(all_ab)

    # STEP 3
    filtered = step3_quality_filter(all_ab)

    # STEP 4
    deduped = step4_dedup(filtered)

    # STEP 5
    new_leads, existing = step5_sf_matching(deduped)

    # STEP 6
    safe_leads, excluded = step6_contract_exclusion(new_leads)

    # STEP 6.5
    clean_leads, additional_excluded = step6b_additional_exclusion(safe_leads)

    # STEP 7
    scored = step7_proximity_score(clean_leads)

    # STEP 8
    import_df = step8_generate_import_csv(scored)

    # 中間ファイル保存
    matched_dir = DATA_DIR / 'matched'
    matched_dir.mkdir(parents=True, exist_ok=True)
    safe_leads.to_csv(matched_dir / 'new_leads_AB_all.csv', index=False, encoding='utf-8-sig')
    existing.to_csv(matched_dir / 'existing_AB_all.csv', index=False, encoding='utf-8-sig')

    # サマリー
    print('\n' + '━' * 60)
    print('パイプライン完了サマリー')
    print('━' * 60)
    print(f'  入力データ: {len(df):,}件')
    print(f'  A/B抽出（職種フィルタ後）: {len(all_ab):,}件')
    print(f'  品質フィルタ後: {len(filtered):,}件')
    print(f'  重複排除後: {len(deduped):,}件')
    print(f'  SF既存マッチ: {len(existing):,}件')
    print(f'  新規リード候補: {len(new_leads):,}件')
    print(f'  成約先除外: {excluded}件')
    print(f'  追加除外（訪問看護+大分類P以外）: {additional_excluded}件')
    print(f'  ━━━━━━━━━━━━━━━━━━━━━')
    print(f'  ✅ 最終インポート候補: {len(import_df):,}件')
    print(f'  ')
    print(f'  ⏳ 所有者割り当て待ち（件数確定後にユーザーに確認）')
    print(f'  出力CSV: {DATA_DIR / "import_ready" / f"import_AB_{TODAY}.csv"}')

    return import_df


if __name__ == '__main__':
    main()
