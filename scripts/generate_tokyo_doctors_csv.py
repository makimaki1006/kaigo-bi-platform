# -*- coding: utf-8 -*-
"""
東京ドクターズ データCSV生成スクリプト

処理内容:
1. tokyo-doctors.com スクレイピングCSVを読み込み
2. 電話番号正規化（応募用＋クリニック代表の2系統）
3. 成約先除外（電話番号突合＋会社名突合＋住所名前突合）
4. Salesforce突合（Lead/Account）
5. 新規リード/更新CSVを生成

媒体名: 東京ドクターズ
"""

import pandas as pd
import numpy as np
import re
from datetime import datetime
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

OUTPUT_DIR = Path("data/output/media_matching")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
TODAY = datetime.now().strftime('%Y-%m-%d')
TIMESTAMP = datetime.now().strftime('%Y%m%d_%H%M%S')
MEDIA_NAME = '東京ドクターズ'

# 入力ファイル
INPUT_FILE = r'C:\Users\fuji1\Downloads\tokyo-doctors.com-から詳細をスクレイピングします-2026-02-10.csv'

# Salesforceエクスポートファイル（最新）
LEAD_FILE = Path('data/output/hellowork/Lead_20260210.csv')
ACCOUNT_FILE = Path('data/output/hellowork/Account_20260210_094201.csv')
CONTRACT_FILE = Path('data/output/hellowork/contract_accounts_20260210_094412.csv')

# 都道府県リスト
PREFECTURES = [
    '北海道', '青森県', '岩手県', '宮城県', '秋田県', '山形県', '福島県',
    '茨城県', '栃木県', '群馬県', '埼玉県', '千葉県', '東京都', '神奈川県',
    '新潟県', '富山県', '石川県', '福井県', '山梨県', '長野県',
    '岐阜県', '静岡県', '愛知県', '三重県',
    '滋賀県', '京都府', '大阪府', '兵庫県', '奈良県', '和歌山県',
    '鳥取県', '島根県', '岡山県', '広島県', '山口県',
    '徳島県', '香川県', '愛媛県', '高知県',
    '福岡県', '佐賀県', '長崎県', '熊本県', '大分県', '宮崎県', '鹿児島県', '沖縄県'
]

# 一般名称リスト（担当者名がこれらの場合はバイネームではない）
GENERIC_NAMES = {
    '担当者', '採用担当', '採用担当者', '採用担当者（名前を聞けたら変更）',
    '人事担当', '人事担当者', '採用係', '採用担当係', '店長', '院長', '事務長',
    '総務担当', '総務担当者', '総務課', '管理者', '責任者', '代表者'
}


def normalize_phone(phone):
    """電話番号を正規化（数字のみ10-11桁）"""
    if pd.isna(phone) or phone == '' or str(phone) == 'nan':
        return None
    phone_str = str(phone).strip()
    if phone_str.endswith('.0'):
        phone_str = phone_str[:-2]
    digits = re.sub(r'\D', '', phone_str)
    if len(digits) == 10 and not digits.startswith('0'):
        digits = '0' + digits
    if 10 <= len(digits) <= 11:
        return digits
    return None


def is_mobile_phone(phone_digits):
    """携帯電話番号かどうか判定（070/080/090始まり）"""
    if not phone_digits:
        return False
    return phone_digits.startswith(('070', '080', '090'))


def is_freephone(phone_digits):
    """フリーダイヤル/ナビダイヤルかどうか判定"""
    if not phone_digits:
        return False
    return phone_digits.startswith(('0120', '0570', '0800'))


def extract_prefecture(address):
    """住所から都道府県を抽出"""
    if pd.isna(address) or not address:
        return None, None
    address = str(address).strip()
    for pref in PREFECTURES:
        if address.startswith(pref):
            return pref, address[len(pref):].strip()
    return None, address


def extract_city(address):
    """住所から市区町村を抽出（都道府県+市区町村）"""
    if pd.isna(address) or not address:
        return None, None
    address = str(address).strip().replace('　', ' ').replace(' ', '')

    pref = None
    for p in PREFECTURES:
        if address.startswith(p):
            pref = p
            address = address[len(p):]
            break

    if not pref:
        return None, None

    city_pattern = re.compile(r'^(.+?[市区])|(.*?郡.+?[町村])')
    match = city_pattern.match(address)

    if match:
        city = match.group(0)
        return pref, city

    return pref, None


def normalize_company_name(name):
    """会社名を正規化（比較用）"""
    if pd.isna(name) or not name:
        return ''
    name = str(name).strip()
    name = name.replace('　', '').replace(' ', '')
    for prefix in ['医療法人社団', '医療法人財団', '医療法人', '社会福祉法人',
                    '株式会社', '有限会社', '合同会社', '一般社団法人', '公益社団法人']:
        name = name.replace(prefix, '')
    return name


def is_similar_name(name1, name2, threshold=0.85):
    """名前が類似しているか判定"""
    if not name1 or not name2:
        return False

    n1 = normalize_company_name(name1)
    n2 = normalize_company_name(name2)

    if not n1 or not n2:
        return False

    if n1 == n2:
        return True

    if len(n1) >= 5 and len(n2) >= 5:
        shorter = n1 if len(n1) <= len(n2) else n2
        longer = n2 if len(n1) <= len(n2) else n1
        if shorter in longer and len(shorter) >= 5:
            return True

    if len(n1) <= 5 or len(n2) <= 5:
        return n1 == n2

    set1 = set(n1)
    set2 = set(n2)
    intersection = len(set1 & set2)
    union = len(set1 | set2)
    similarity = intersection / union if union > 0 else 0

    return similarity >= threshold


def is_valid_value(val):
    """有効な値かどうか判定"""
    if pd.isna(val) or val is None:
        return False
    str_val = str(val).strip()
    if str_val == '' or str_val.lower() == 'nan':
        return False
    return True


def is_generic_contact_name(name):
    """担当者名が一般名称かどうか判定"""
    if not is_valid_value(name):
        return True
    name = str(name).strip()
    return name in GENERIC_NAMES


def clean_doctor_name(doctor_name_raw):
    """院長名から役職を除去してクリーンな名前を返す"""
    if not is_valid_value(doctor_name_raw):
        return '', ''
    name = str(doctor_name_raw).strip()

    # 役職部分を抽出して分離
    roles = ['分院長', '副院長', '院長', '理事長', '副理事長', '所長', '副所長',
             '園長', '施設長', 'センター長', '部長', '科長', '室長', '主任']
    role = ''
    clean_name = name
    for r in roles:
        if r in name:
            role = r
            clean_name = name.replace(r, '').strip()
            break

    return clean_name, role


def extract_job_types(job_text1, job_text2=''):
    """求人情報から職種・雇用形態・給与を抽出"""
    jobs = []
    for text in [job_text1, job_text2]:
        if not is_valid_value(text):
            continue
        text = str(text).strip()
        # パターン: ［N］職種名／雇用形態／給与
        match = re.match(r'[［\[]\d[］\]]\s*(.+?)／(.+?)／(.+)', text)
        if match:
            jobs.append({
                'job_type': match.group(1).strip(),
                'employment_type': match.group(2).strip(),
                'salary': match.group(3).strip()
            })
        else:
            jobs.append({'raw': text})
    return jobs


# ============================================================
# データ読み込み
# ============================================================

def load_tokyo_doctors_data():
    """東京ドクターズのスクレイピングデータを読み込み"""
    print("=== 東京ドクターズ データ読み込み ===")
    df = pd.read_csv(INPUT_FILE, dtype=str, encoding='utf-8-sig')
    print(f"  読み込み件数: {len(df)}")

    records = []
    skipped_no_company = 0
    skipped_no_phone = 0

    for _, row in df.iterrows():
        # クリニック名（Company）
        company = row.get('p-job__title', '')
        if not is_valid_value(company):
            skipped_no_company += 1
            continue

        # 電話番号（応募用 = 主キー、クリニック代表 = 補助）
        recruitment_phone = normalize_phone(row.get('p-job-apply__tel', ''))
        clinic_phone = normalize_phone(row.get('c-clinicList-info (2)', ''))

        # いずれかの電話番号が必要
        if not recruitment_phone and not clinic_phone:
            skipped_no_phone += 1
            continue

        # 応募用電話を優先、なければクリニック代表電話を使用
        primary_phone = recruitment_phone or clinic_phone
        # クリニック代表電話が応募用と異なる場合のみ保持（フリーダイヤルは除外）
        secondary_phone = None
        if clinic_phone and clinic_phone != recruitment_phone and not is_freephone(clinic_phone):
            secondary_phone = clinic_phone

        # 担当者名
        contact_name_raw = row.get('p-job-body__table (7)', '')
        if is_generic_contact_name(contact_name_raw):
            contact_name = '担当者'
        else:
            contact_name = str(contact_name_raw).strip()

        # 院長・代表者名
        doctor_name_raw = row.get('c-clinicList-interview-banner__name', '')
        doctor_name, doctor_role = clean_doctor_name(doctor_name_raw)

        # 住所（完全住所から都道府県と残りを分離）
        full_address = row.get('c-clinicList-info__body-address', '')
        pref, street = extract_prefecture(full_address)

        # 求人情報
        job1 = row.get('p-job-body__list01', '')
        job2 = row.get('p-job-body__list01 (2)', '')
        jobs = extract_job_types(job1, job2)

        # 求人職種（最初の求人から）
        job_type_str = ''
        employment_type_str = ''
        salary_str = ''
        if jobs:
            if 'job_type' in jobs[0]:
                job_type_str = jobs[0]['job_type']
                employment_type_str = jobs[0]['employment_type']
                salary_str = jobs[0]['salary']
            if len(jobs) > 1 and 'job_type' in jobs[1]:
                job_type_str += f" / {jobs[1]['job_type']}"

        # 求人URL
        url = row.get('c-jobList-card__link href', '')

        # ウェブサイト（SNSリンクから実サイトを探す）
        website = ''
        for sns_col in ['c-clinicList-info__body-sns-list href', 'c-clinicList-info__body-sns-list href (2)',
                        'c-clinicList-info__body-sns-list href (3)', 'c-clinicList-info__body-sns-list href (4)']:
            sns_val = row.get(sns_col, '')
            if is_valid_value(sns_val):
                sns_str = str(sns_val).strip()
                # SNS以外のURLをウェブサイトとして使用
                if not any(domain in sns_str for domain in ['line.me', 'instagram.com', 'youtube.com',
                                                             'x.com', 'twitter.com', 'facebook.com',
                                                             'tiktok.com']):
                    website = sns_str
                    break

        # メモ欄構築
        memo_parts = [f"【{MEDIA_NAME}】"]
        if is_valid_value(full_address):
            memo_parts.append(f"住所: {full_address}")
        if doctor_name:
            role_suffix = f"（{doctor_role}）" if doctor_role else ''
            memo_parts.append(f"代表者: {doctor_name}{role_suffix}")
        if contact_name != '担当者':
            memo_parts.append(f"担当者: {contact_name}")
        if job_type_str:
            memo_parts.append(f"募集職種: {job_type_str}")
        if employment_type_str:
            memo_parts.append(f"雇用形態: {employment_type_str}")
        if salary_str:
            memo_parts.append(f"給与: {salary_str}")
        if secondary_phone:
            memo_parts.append(f"代表電話: {secondary_phone}")
        if is_valid_value(url):
            memo_parts.append(f"URL: {url}")
        memo_parts.append(f"取得日: {TODAY}")

        records.append({
            'source': MEDIA_NAME,
            'company_name': str(company).strip(),
            'contact_name': contact_name,
            'phone_normalized': primary_phone,
            'secondary_phone': secondary_phone,
            'phone': primary_phone if not is_mobile_phone(primary_phone) else primary_phone,
            'mobile_phone': primary_phone if is_mobile_phone(primary_phone) else None,
            'prefecture': pref,
            'street': street,
            'full_address': full_address,
            'doctor_name': doctor_name,
            'doctor_role': doctor_role,
            'job_type': job_type_str,
            'employment_type': employment_type_str,
            'salary': salary_str,
            'url': url,
            'website': website,
            'memo': '\n'.join(memo_parts),
        })

    print(f"  有効レコード: {len(records)}件")
    if skipped_no_company > 0:
        print(f"  ※クリニック名空でスキップ: {skipped_no_company}件")
    if skipped_no_phone > 0:
        print(f"  ※電話番号空でスキップ: {skipped_no_phone}件")
    return records


# ============================================================
# 除外・突合データ読み込み
# ============================================================

def load_exclusion_phones():
    """成約先電話番号を読み込み"""
    print("\n=== 除外リスト読み込み ===")
    contract_phones = set()

    if CONTRACT_FILE.exists():
        df = pd.read_csv(CONTRACT_FILE, dtype=str, encoding='utf-8')
        for _, row in df.iterrows():
            normalized = normalize_phone(row.get('Phone', ''))
            if normalized:
                contract_phones.add(normalized)
        print(f"  成約先電話番号: {len(contract_phones)}件")
    else:
        print(f"  ⚠️ 成約先ファイルなし: {CONTRACT_FILE}")

    return contract_phones


def load_contract_company_names():
    """成約先の会社名を読み込み"""
    company_names = set()

    if CONTRACT_FILE.exists():
        df = pd.read_csv(CONTRACT_FILE, dtype=str, encoding='utf-8')
        for _, row in df.iterrows():
            name = row.get('Name', '')
            if pd.notna(name) and name:
                company_names.add(str(name).strip())
        print(f"  成約先会社名: {len(company_names)}件")

    return company_names


def load_contract_location_index():
    """成約先の住所+名前インデックスを構築"""
    location_index = {}

    if CONTRACT_FILE.exists():
        df = pd.read_csv(CONTRACT_FILE, dtype=str, encoding='utf-8')
        valid_count = 0
        for _, row in df.iterrows():
            name = row.get('Name', '')
            address = row.get('Address__c', '')

            if not pd.notna(name) or not name:
                continue

            pref, city = extract_city(address)
            if pref and city:
                key = (pref, city)
                if key not in location_index:
                    location_index[key] = []
                location_index[key].append((normalize_company_name(name), str(name).strip()))
                valid_count += 1

        print(f"  成約先住所インデックス: {len(location_index)}地域, {valid_count}件")

    return location_index


def load_salesforce_phone_index():
    """Salesforceの電話番号インデックスを構築"""
    print("\n=== Salesforce電話番号インデックス構築 ===")

    phone_to_records = {}

    # Lead
    if LEAD_FILE.exists():
        lead_cols = ['Id', 'Company', 'LastName', 'Status', 'Phone', 'MobilePhone',
                     'Phone2__c', 'MobilePhone2__c', 'LeadSourceMemo__c']
        df_lead = pd.read_csv(LEAD_FILE, usecols=lambda c: c in lead_cols, dtype=str, encoding='utf-8')

        for _, row in df_lead.iterrows():
            for col in ['Phone', 'MobilePhone', 'Phone2__c', 'MobilePhone2__c']:
                if col in row:
                    normalized = normalize_phone(row[col])
                    if normalized:
                        if normalized not in phone_to_records:
                            phone_to_records[normalized] = []
                        phone_to_records[normalized].append(('Lead', row['Id'], row))
                        break
        lead_count = len([k for k, v in phone_to_records.items() if any(x[0] == 'Lead' for x in v)])
        print(f"  Lead電話番号: {lead_count}件")
    else:
        print(f"  ⚠️ Leadファイルなし: {LEAD_FILE}")
        df_lead = pd.DataFrame()

    # Account
    if ACCOUNT_FILE.exists():
        acc_cols = ['Id', 'Name', 'Phone', 'Phone2__c', 'BillingStreet', 'BillingCity', 'BillingState']
        df_acc = pd.read_csv(ACCOUNT_FILE, usecols=lambda c: c in acc_cols, dtype=str, encoding='utf-8')

        for _, row in df_acc.iterrows():
            for col in ['Phone', 'Phone2__c']:
                if col in row:
                    normalized = normalize_phone(row[col])
                    if normalized:
                        if normalized not in phone_to_records:
                            phone_to_records[normalized] = []
                        phone_to_records[normalized].append(('Account', row['Id'], row))
                        break
        print(f"  電話番号インデックス合計: {len(phone_to_records)}件")
    else:
        print(f"  ⚠️ Accountファイルなし: {ACCOUNT_FILE}")
        df_acc = pd.DataFrame()

    return phone_to_records, df_lead, df_acc


def load_account_location_index():
    """取引先の住所+名前インデックスを構築"""
    location_index = {}

    if ACCOUNT_FILE.exists():
        df = pd.read_csv(ACCOUNT_FILE, dtype=str, encoding='utf-8')
        valid_count = 0

        for _, row in df.iterrows():
            name = row.get('Name', '')
            # BillingState + BillingCity + BillingStreet で住所を構成
            state = row.get('BillingState', '') or ''
            city = row.get('BillingCity', '') or ''
            street = row.get('BillingStreet', '') or ''
            address = f"{state}{city}{street}".strip()

            if not pd.notna(name) or not name or not address:
                continue

            pref, city_name = extract_city(address)
            if pref and city_name:
                key = (pref, city_name)
                if key not in location_index:
                    location_index[key] = []
                location_index[key].append((
                    normalize_company_name(name),
                    str(name).strip(),
                    row['Id'],
                    row
                ))
                valid_count += 1

        print(f"  取引先住所インデックス: {len(location_index)}地域, {valid_count}件")

    return location_index


# ============================================================
# データ処理（突合・分類）
# ============================================================

def process_data(all_records, contract_phones, contract_names,
                 phone_index, contract_location_index, account_location_index):
    """データを処理して分類"""
    print("\n=== データ処理 ===")

    new_leads = []
    lead_updates = []
    account_updates = []
    excluded = []
    account_location_match_count = 0

    for rec in all_records:
        phone = rec['phone_normalized']
        secondary = rec.get('secondary_phone')
        company = rec['company_name']

        # 媒体データの住所を取得
        media_pref = rec.get('prefecture', '')
        full_address = rec.get('full_address', '')
        if media_pref and full_address:
            _, media_city = extract_city(full_address)
        else:
            media_city = None

        # === 成約先除外 ===

        # 1. 成約先電話番号チェック（応募用 + 代表の両方）
        if phone in contract_phones:
            excluded.append({**rec, 'reason': '成約先（応募用電話番号一致）'})
            continue
        if secondary and secondary in contract_phones:
            excluded.append({**rec, 'reason': '成約先（代表電話番号一致）'})
            continue

        # 2. 成約先 住所+名前チェック
        location_matched = False
        if media_pref and media_city and company:
            location_key = (media_pref, media_city)
            if location_key in contract_location_index:
                company_normalized = normalize_company_name(company)
                for contract_normalized, contract_original in contract_location_index[location_key]:
                    if is_similar_name(company_normalized, contract_normalized):
                        excluded.append({**rec, 'reason': f'成約先（住所+名前一致: {contract_original}）'})
                        location_matched = True
                        break
        if location_matched:
            continue

        # 3. 成約先会社名チェック（住所なしでも名前だけで完全一致）
        name_matched = False
        if company and is_valid_value(company):
            company_normalized = normalize_company_name(company)
            for contract_name in contract_names:
                if contract_name and pd.notna(contract_name):
                    contract_normalized = normalize_company_name(contract_name)
                    if company_normalized == contract_normalized and len(company_normalized) >= 5:
                        excluded.append({**rec, 'reason': f'成約先（会社名完全一致: {contract_name}）'})
                        name_matched = True
                        break
        if name_matched:
            continue

        # === Salesforce突合 ===

        # 4. 電話番号で突合（応募用 → 代表の順に検索）
        matched = False
        for check_phone in [phone, secondary]:
            if not check_phone:
                continue
            if check_phone in phone_index:
                matches = phone_index[check_phone]
                for obj_type, obj_id, obj_row in matches:
                    if obj_type == 'Lead':
                        lead_updates.append({**rec, 'sf_id': obj_id, 'sf_row': obj_row})
                        matched = True
                        break
                    elif obj_type == 'Account':
                        account_updates.append({**rec, 'sf_id': obj_id, 'sf_row': obj_row})
                        matched = True
                        break
            if matched:
                break

        if matched:
            continue

        # 5. 電話番号でマッチしなかった場合、取引先を住所+名前で検索
        account_found = False
        if media_pref and media_city and company:
            location_key = (media_pref, media_city)
            if location_key in account_location_index:
                company_normalized = normalize_company_name(company)
                for acc_normalized, acc_original, acc_id, acc_row in account_location_index[location_key]:
                    if is_similar_name(company_normalized, acc_normalized):
                        account_updates.append({
                            **rec,
                            'sf_id': acc_id,
                            'sf_row': acc_row,
                            'match_reason': f'住所+名前一致: {acc_original}'
                        })
                        account_found = True
                        account_location_match_count += 1
                        break

        if not account_found:
            new_leads.append(rec)

    print(f"  新規リード: {len(new_leads)}件")
    print(f"  Lead更新: {len(lead_updates)}件")
    print(f"  Account更新: {len(account_updates)}件")
    print(f"    └ うち住所+名前マッチ: {account_location_match_count}件")
    print(f"  除外: {len(excluded)}件")

    return new_leads, lead_updates, account_updates, excluded


# ============================================================
# CSV生成
# ============================================================

def safe_str(val):
    """NoneやNaNを空文字に変換"""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ''
    s = str(val)
    if s.lower() == 'nan':
        return ''
    return s


def generate_new_lead_csv(new_leads):
    """新規リード作成用CSV生成"""
    if not new_leads:
        print("  新規リードなし")
        return None

    records = []
    skipped_no_phone = 0
    skipped_no_company = 0

    for row in new_leads:
        company_name = row.get('company_name', '')
        if not company_name or not is_valid_value(company_name):
            skipped_no_company += 1
            continue

        phone = row.get('phone_normalized', '')
        if not phone:
            skipped_no_phone += 1
            print(f"  警告: Phone空のためスキップ: {company_name}")
            continue

        # Phone必須: 携帯のみでもPhoneに値を設定
        if is_mobile_phone(phone):
            phone_field = phone
            mobile_field = phone
        else:
            phone_field = phone
            mobile_field = ''

        # 担当者名
        contact_name = row.get('contact_name', '担当者')
        if not is_valid_value(contact_name):
            contact_name = '担当者'

        record = {
            'Company': company_name,
            'LastName': contact_name,
            'Phone': phone_field,
            'MobilePhone': mobile_field,
            'Prefecture__c': safe_str(row.get('prefecture', '')),
            'Street': safe_str(row.get('street', '')),
            'Website': safe_str(row.get('website', '')),
            'PresidentName__c': safe_str(row.get('doctor_name', '')),
            'PresidentTitle__c': safe_str(row.get('doctor_role', '')),
            'LeadSource': 'Other',
            'Paid_Media__c': MEDIA_NAME,
            'Paid_DataSource__c': MEDIA_NAME,
            'Paid_DataExportDate__c': TODAY,
            'Paid_RecruitmentType__c': safe_str(row.get('job_type', '')),
            'Paid_EmploymentType__c': safe_str(row.get('employment_type', '')),
            'Paid_URL__c': safe_str(row.get('url', '')),
            'Paid_Memo__c': safe_str(row.get('memo', '')),
            'LeadSourceMemo__c': f"【新規作成】有料媒体突合 {TODAY}",
        }
        records.append(record)

    df = pd.DataFrame(records)
    df = df.astype(str)
    df = df.replace('nan', '')
    df = df.replace('None', '')

    output_path = OUTPUT_DIR / f'tokyo_doctors_new_leads_{TIMESTAMP}.csv'
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"  出力: {output_path} ({len(df)}件)")
    if skipped_no_company > 0:
        print(f"  ※Company空でスキップ: {skipped_no_company}件")
    if skipped_no_phone > 0:
        print(f"  ※Phone空でスキップ: {skipped_no_phone}件")
    return output_path


def generate_lead_update_csv(lead_updates):
    """Lead更新用CSV生成"""
    if not lead_updates:
        print("  Lead更新なし")
        return None

    records = []
    for row in lead_updates:
        sf_row = row.get('sf_row', {})
        existing_memo = sf_row.get('LeadSourceMemo__c', '') or ''

        new_memo = f"【既存更新】有料媒体突合 {TODAY}\n{row.get('memo', '')}"
        if is_valid_value(existing_memo):
            new_memo = f"{new_memo}\n---\n{existing_memo}"

        record = {
            'Id': row['sf_id'],
            'LeadSourceMemo__c': new_memo,
            'Paid_Media__c': MEDIA_NAME,
            'Paid_DataExportDate__c': TODAY,
            'Paid_DataSource__c': MEDIA_NAME,
        }

        # PresidentName__cが空の場合のみ補完
        if is_valid_value(row.get('doctor_name')) and not is_valid_value(sf_row.get('PresidentName__c', '')):
            record['PresidentName__c'] = row['doctor_name']

        records.append(record)

    df = pd.DataFrame(records)
    df = df.astype(str)
    df = df.replace('nan', '')
    df = df.replace('None', '')

    output_path = OUTPUT_DIR / f'tokyo_doctors_lead_updates_{TIMESTAMP}.csv'
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"  出力: {output_path} ({len(df)}件)")
    return output_path


def generate_account_update_csv(account_updates):
    """Account更新用CSV生成"""
    if not account_updates:
        print("  Account更新なし")
        return None

    records = []
    for row in account_updates:
        record = {
            'Id': row['sf_id'],
            'Paid_Media__c': MEDIA_NAME,
            'Paid_DataExportDate__c': TODAY,
            'Paid_DataSource__c': MEDIA_NAME,
        }
        records.append(record)

    df = pd.DataFrame(records)
    df = df.astype(str)
    df = df.replace('nan', '')
    df = df.replace('None', '')

    output_path = OUTPUT_DIR / f'tokyo_doctors_account_updates_{TIMESTAMP}.csv'
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"  出力: {output_path} ({len(df)}件)")
    return output_path


def generate_excluded_csv(excluded):
    """除外リストCSV生成"""
    if not excluded:
        return None

    records = []
    for row in excluded:
        records.append({
            'company_name': row.get('company_name', ''),
            'phone': row.get('phone_normalized', ''),
            'secondary_phone': row.get('secondary_phone', ''),
            'reason': row.get('reason', ''),
        })

    df = pd.DataFrame(records)
    output_path = OUTPUT_DIR / f'tokyo_doctors_excluded_{TIMESTAMP}.csv'
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"  除外リスト: {output_path} ({len(df)}件)")
    return output_path


# ============================================================
# セグメント分析
# ============================================================

def analyze_segments(new_leads):
    """新規リードのセグメント分析"""
    if not new_leads:
        return

    print("\n=== セグメント分析（新規リード） ===")

    # 代表者名の有無
    has_doctor = [r for r in new_leads if is_valid_value(r.get('doctor_name'))]
    no_doctor = [r for r in new_leads if not is_valid_value(r.get('doctor_name'))]

    # バイネーム担当者
    has_byname = [r for r in new_leads if r.get('contact_name', '担当者') != '担当者']
    no_byname = [r for r in new_leads if r.get('contact_name', '担当者') == '担当者']

    # 代表者直通（担当者名に代表者名が含まれるか）
    direct_to_doctor = []
    for r in new_leads:
        doctor = r.get('doctor_name', '')
        contact = r.get('contact_name', '担当者')
        if doctor and contact != '担当者':
            # 姓が一致するか
            doctor_surname = doctor.split()[0] if doctor else ''
            contact_surname = contact.split()[0] if contact else ''
            if doctor_surname and contact_surname and doctor_surname == contact_surname:
                direct_to_doctor.append(r)

    # 携帯電話あり
    has_mobile = [r for r in new_leads if is_mobile_phone(r.get('phone_normalized', ''))]

    print(f"  合計: {len(new_leads)}件")
    print(f"  代表者名あり: {len(has_doctor)}件 ({len(has_doctor)/len(new_leads)*100:.1f}%)")
    print(f"  バイネーム担当者: {len(has_byname)}件 ({len(has_byname)/len(new_leads)*100:.1f}%)")
    print(f"  代表者直通: {len(direct_to_doctor)}件")
    print(f"  携帯電話あり: {len(has_mobile)}件")

    # 職種別内訳
    job_types = {}
    for r in new_leads:
        jt = r.get('job_type', '')
        if is_valid_value(jt):
            # 最初の職種のみカウント
            first_job = jt.split(' / ')[0] if ' / ' in jt else jt
            job_types[first_job] = job_types.get(first_job, 0) + 1

    if job_types:
        print(f"\n  職種別内訳（上位10）:")
        for jt, count in sorted(job_types.items(), key=lambda x: -x[1])[:10]:
            print(f"    {jt}: {count}件")


# ============================================================
# メイン
# ============================================================

def main():
    print("=" * 60)
    print(f"東京ドクターズ データ処理 ({TODAY})")
    print("=" * 60)

    # 1. データ読み込み
    all_records = load_tokyo_doctors_data()

    # 2. 電話番号ベースで重複除去
    print("\n=== 重複除去 ===")
    seen_phones = set()
    unique_records = []
    dup_count = 0
    for rec in all_records:
        phone = rec['phone_normalized']
        if phone not in seen_phones:
            seen_phones.add(phone)
            unique_records.append(rec)
        else:
            dup_count += 1

    print(f"  重複除去前: {len(all_records)}件")
    print(f"  重複除去後: {len(unique_records)}件 (重複: {dup_count}件)")

    # 3. 除外リスト読み込み
    contract_phones = load_exclusion_phones()
    contract_names = load_contract_company_names()
    contract_location_index = load_contract_location_index()

    # 4. Salesforce電話番号インデックス
    phone_index, df_lead, df_acc = load_salesforce_phone_index()

    # 5. 取引先住所+名前インデックス
    account_location_index = load_account_location_index()

    # 6. データ処理（突合・分類）
    new_leads, lead_updates, account_updates, excluded = process_data(
        unique_records, contract_phones, contract_names,
        phone_index, contract_location_index, account_location_index
    )

    # 7. セグメント分析
    analyze_segments(new_leads)

    # 8. CSV生成
    print("\n=== CSV生成 ===")
    generate_new_lead_csv(new_leads)
    generate_lead_update_csv(lead_updates)
    generate_account_update_csv(account_updates)
    generate_excluded_csv(excluded)

    # 9. サマリー
    print("\n" + "=" * 60)
    print("処理完了サマリー")
    print("=" * 60)
    print(f"入力: {len(all_records)}件 → 重複除去後: {len(unique_records)}件")
    print(f"\n分類結果:")
    print(f"  新規リード:    {len(new_leads)}件")
    print(f"  Lead更新:      {len(lead_updates)}件")
    print(f"  Account更新:   {len(account_updates)}件")
    print(f"  除外:          {len(excluded)}件")
    total = len(new_leads) + len(lead_updates) + len(account_updates) + len(excluded)
    print(f"  合計:          {total}件")

    if excluded:
        print(f"\n除外理由内訳:")
        reasons = {}
        for ex in excluded:
            reason = ex.get('reason', 'Unknown')
            if '住所+名前一致' in reason:
                reason = '成約先（住所+名前一致）'
            elif '会社名完全一致' in reason:
                reason = '成約先（会社名完全一致）'
            reasons[reason] = reasons.get(reason, 0) + 1
        for reason, count in sorted(reasons.items(), key=lambda x: -x[1]):
            print(f"    {reason}: {count}件")

    print(f"\n出力ディレクトリ: {OUTPUT_DIR}")


if __name__ == '__main__':
    main()
