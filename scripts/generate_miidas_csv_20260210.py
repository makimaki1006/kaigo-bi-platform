# -*- coding: utf-8 -*-
"""
ミイダス NDJSON → Salesforce突合・CSV生成スクリプト (2026-02-10版)
- NDJSONファイルから読み込み
- 成約先・電話済み除外
- Salesforce既存データ（Lead/Account/Contact）と電話番号突合
- 新規リード・更新用CSV生成
- セグメント分析
"""

import json
import pandas as pd
import numpy as np
import re
import sys
from datetime import datetime
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')

# パス設定
BASE_DIR = Path(r"C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List")
OUTPUT_DIR = BASE_DIR / "data" / "output" / "media_matching"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

SF_DATA_DIR = BASE_DIR / "data" / "output" / "hellowork"
NDJSON_PATH = OUTPUT_DIR / "miidas_unique_phone_20260209.ndjson"
CONTRACT_PATH = SF_DATA_DIR / "contract_accounts_20260210_094412.csv"
CALLED_LIST_PATH = Path(r"C:\Users\fuji1\Downloads\媒体掲載中のリスト.xlsx")
LEAD_PATH = SF_DATA_DIR / "Lead_20260210.csv"
ACCOUNT_PATH = SF_DATA_DIR / "Account_20260210_094201.csv"
CONTACT_PATH = SF_DATA_DIR / "Contact_20260210_094224.csv"

TODAY = datetime.now().strftime('%Y-%m-%d')
TIMESTAMP = datetime.now().strftime('%Y%m%d_%H%M%S')

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

# 一般名称リスト
GENERIC_NAMES = [
    '担当者', '採用担当', '採用担当者', '人事担当', '人事担当者',
    '採用係', '採用担当係', '店長', '院長', '事務長',
    '総務担当', '総務担当者', '総務課', '管理者', '責任者', '代表者',
]

# 役職プレフィックス
TITLE_PREFIXES = [
    '理事長', '院長', '事務長', '園長', '施設長', '所長', '部長', '課長', '係長', '主任',
    '代表取締役', '取締役', '代表', '社長', '副社長', '専務', '常務', '監査役',
    '総務部', '人事部', '採用担当', '人事担当', '総務課', '法人事務局',
    '統括マネージャー', 'マネージャー', 'チーフ', 'リーダー', 'ディレクター',
    '看護部長', '看護師長', '介護部長', '事務部長', '経理部長', '営業部長',
    '店長', '支店長', '工場長', '本部長', '次長', '顧問', '相談役',
]

INVALID_NAME_LABELS = [
    '連絡先', 'TEL', 'tel', 'Tel', 'FAX', 'fax', 'Fax',
    '電話番号', '携帯番号', 'メール', 'E-mail', 'email', 'Email',
    '担当', '採用', '人事', '総務', '事務局',
    '電話', '問い合わせ', 'お問い合わせ', '問合せ',
]

UNCONTACTED_STATUSES = ['未架電', '00 架電OK - 接触なし']


def normalize_phone(phone):
    if pd.isna(phone) or phone == '' or str(phone) == 'nan':
        return None
    digits = re.sub(r'\D', '', str(phone).strip())
    if 10 <= len(digits) <= 11:
        return digits
    return None


def is_mobile_phone(phone_digits):
    if not phone_digits:
        return False
    return phone_digits.startswith(('070', '080', '090'))


def extract_prefecture(address):
    if pd.isna(address) or not address:
        return None, address
    address = str(address).strip()
    lines = address.split('\n')
    for line in lines:
        line = line.strip()
        for pref in PREFECTURES:
            if pref in line:
                idx = line.find(pref)
                remaining = line[idx + len(pref):].strip()
                return pref, remaining if remaining else address
    return None, address


def extract_job_title(full_text):
    if pd.isna(full_text) or not full_text:
        return None
    text = str(full_text).strip()
    first_line = text.split('\n')[0].strip()
    first_line = re.sub(r'[【】《》]', '', first_line)
    return first_line[:100] if first_line else None


def extract_recruitment_number(full_text):
    if pd.isna(full_text) or not full_text:
        return None
    text = str(full_text)
    fw = '０１２３４５６７８９'
    hw = '0123456789'
    for f, h in zip(fw, hw):
        text = text.replace(f, h)
    patterns = [
        r'募集人数[：:]*\s*(\d+)\s*名', r'採用人数[：:]*\s*(\d+)\s*名',
        r'(\d+)\s*名募集', r'(\d+)\s*名以上募集', r'(\d+)\s*人募集',
        r'若干名',
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            if pattern == r'若干名':
                return '若干名'
            return match.group(1) + '名'
    return None


def clean_name_with_title(name):
    if not name:
        return None, None
    name = str(name).strip()
    extracted_title = None
    for label in INVALID_NAME_LABELS:
        if name.strip() == label:
            return None, None
    name = re.sub(r'0\d{1,4}[-－ー−‐\s]?\d{1,4}[-－ー−‐\s]?\d{3,4}', '', name)
    name = re.sub(r'0\d{9,10}', '', name)
    name = re.sub(r'^[】【\[\]「」『』《》〈〉（）\(\)\s：:・]+', '', name)
    name = re.sub(r'[】【\[\]「」『』《》〈〉（）\(\)\s：:・]+$', '', name)
    extracted_titles = []
    changed = True
    while changed:
        changed = False
        for prefix in TITLE_PREFIXES:
            if name.startswith(prefix):
                extracted_titles.append(prefix)
                name = name[len(prefix):].strip()
                name = re.sub(r'^[：:\s]+', '', name)
                changed = True
                break
    if extracted_titles:
        extracted_title = ' '.join(extracted_titles)
    name = name.replace('　', ' ')
    name = re.sub(r'^[：:・\s]+', '', name)
    name = re.sub(r'[：:・\s]+$', '', name)
    if name.startswith('（') and name.endswith('）'):
        name = name[1:-1].strip()
    if name.startswith('(') and name.endswith(')'):
        name = name[1:-1].strip()
    for label in INVALID_NAME_LABELS:
        if name == label:
            return None, extracted_title
    if len(name) < 2:
        return None, extracted_title
    return name.strip() if name.strip() else None, extracted_title


def extract_contact_name_with_title(contact_text, representative=None, role_field=None):
    name = None
    title = None
    if pd.notna(role_field) and role_field:
        role_str = str(role_field).strip()
        invalid_roles = ['役職なし', '役職無し', 'なし', '無し', '-', '−', '―', 'ー', '']
        if role_str not in invalid_roles and not role_str.startswith('役職なし'):
            if '\n' in role_str:
                role_str = role_str.split('\n')[0].strip()
            title = role_str
    if pd.notna(contact_text) and contact_text:
        text = str(contact_text)
        patterns = [
            r'担当者[：:\s]*([^\n\r@0-9]{2,20})',
            r'採用担当[：:\s]*([^\n\r@0-9]{2,20})',
            r'人事担当[：:\s]*([^\n\r@0-9]{2,20})',
            r'担当[：:\s]*([^\n\r@0-9]{2,20})',
            r'([^\s\n@0-9]{2,10})宛',
            r'([^\s\n@0-9]{2,10})まで(?:ご連絡)?',
        ]
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                candidate = match.group(1).strip()
                candidate, extracted_title = clean_name_with_title(candidate)
                if candidate and len(candidate) >= 2:
                    name = candidate
                    if not title and extracted_title:
                        title = extracted_title
                    break
    if not name and pd.notna(representative) and representative:
        name, extracted_title = clean_name_with_title(str(representative))
        if not title and extracted_title:
            title = extracted_title
    return name, title


def is_generic_name(name):
    if pd.isna(name) or not name:
        return True
    name = str(name).strip()
    if '\n' in name:
        name = name.split('\n')[0].strip()
    for generic in GENERIC_NAMES:
        if generic in name:
            return True
    return name == ''


def is_real_name(name):
    if pd.isna(name) or not name:
        return False
    name = str(name).strip()
    if '\n' in name:
        name = name.split('\n')[0].strip()
    if not name or len(name) < 2:
        return False
    for generic in GENERIC_NAMES:
        if generic in name:
            return False
    return True


def is_uncontacted_status(status):
    if pd.isna(status) or not status:
        return True
    return str(status).strip() in UNCONTACTED_STATUSES


# ========================================
# STEP 1: ミイダスデータ読み込み
# ========================================
def load_miidas_data():
    print("=" * 70)
    print("[STEP 1] ミイダスデータ読み込み")
    print("=" * 70)

    records = []
    with open(NDJSON_PATH, 'r', encoding='utf-8') as f:
        for line in f:
            obj = json.loads(line.strip())

            primary_phone = obj['_primary_phone']
            landline = obj.get('_landline')
            mobile = obj.get('_mobile')

            # 住所から都道府県抽出
            address = obj.get('勤務地', '') or obj.get('本社住所', '')
            prefecture, street = extract_prefecture(address)

            # 職種・募集人数を抽出
            job_title = extract_job_title(obj.get('全本文ダンプ', ''))
            num_recruitment = extract_recruitment_number(obj.get('全本文ダンプ', ''))

            # 担当者名と役職を抽出
            contact_name, contact_title = extract_contact_name_with_title(
                obj.get('連絡先', ''),
                obj.get('代表者', ''),
                obj.get('役職', '')
            )

            # 代表者名
            president_name = obj.get('代表者', '')
            if president_name:
                # 「代表取締役 XXX」等から名前を抽出
                pn_cleaned, pn_title = clean_name_with_title(president_name)
                if pn_cleaned:
                    president_name = pn_cleaned

            # Phone必須チェック: 携帯のみの場合もPhoneに値を設定
            phone_field = landline
            mobile_field = mobile
            if not phone_field and mobile_field:
                phone_field = mobile_field  # 携帯番号をPhoneにも設定

            records.append({
                'source': 'ミイダス',
                'company_name': obj.get('企業名', ''),
                'contact_name': contact_name,
                'contact_title': contact_title,
                'president_name': president_name if president_name else '',
                'phone': phone_field,
                'mobile_phone': mobile_field,
                'phone_normalized': primary_phone,
                'prefecture': prefecture,
                'street': street,
                'job_type': job_title,
                'employment_type': None,
                'industry': str(obj.get('企業規模', '')).split('\n')[0].strip() if obj.get('企業規模') else '',
                'num_recruitment': num_recruitment,
                'memo': f"設立: {str(obj.get('設立', 'N/A')).split(chr(10))[0]}",
                'url': obj.get('url', ''),
                'website': obj.get('企業サイトURL', ''),
            })

    df = pd.DataFrame(records)
    print(f"読み込み件数: {len(df)}")
    return df


# ========================================
# STEP 2: 除外リスト読み込み
# ========================================
def load_exclusion_phones():
    print("\n" + "=" * 70)
    print("[STEP 2] 除外リスト読み込み")
    print("=" * 70)

    # 成約先
    df_contract = pd.read_csv(CONTRACT_PATH, dtype=str, encoding='utf-8')
    contract_phones = set()
    for _, row in df_contract.iterrows():
        normalized = normalize_phone(row.get('Phone', ''))
        if normalized:
            contract_phones.add(normalized)
    print(f"成約先電話番号: {len(contract_phones)}件")

    # 電話済みリスト
    called_phones = set()
    if CALLED_LIST_PATH.exists():
        xlsx = pd.ExcelFile(CALLED_LIST_PATH)
        phone_pattern = re.compile(r'0\d{1,4}[-\s]?\d{1,4}[-\s]?\d{3,4}|0\d{9,10}')
        for sheet in xlsx.sheet_names:
            df = pd.read_excel(xlsx, sheet_name=sheet, header=None)
            for col in df.columns:
                for val in df[col].astype(str):
                    matches = phone_pattern.findall(val)
                    for m in matches:
                        digits = re.sub(r'\D', '', m)
                        if 10 <= len(digits) <= 11:
                            called_phones.add(digits)
        print(f"電話済み電話番号: {len(called_phones)}件")
    else:
        print("電話済みリスト: ファイルなし（スキップ）")

    return contract_phones, called_phones


# ========================================
# STEP 3: Salesforce電話番号インデックス構築
# ========================================
def load_salesforce_phone_index():
    print("\n" + "=" * 70)
    print("[STEP 3] Salesforce電話番号インデックス構築")
    print("=" * 70)

    phone_to_records = {}

    # Lead
    lead_cols = ['Id', 'Company', 'LastName', 'Title', 'Status',
                 'Phone', 'MobilePhone', 'Phone2__c', 'MobilePhone2__c',
                 'PresidentName__c', 'LeadSourceMemo__c']
    df_lead = pd.read_csv(LEAD_PATH,
                           usecols=lambda c: c in lead_cols,
                           dtype=str, encoding='utf-8')
    print(f"Lead読み込み: {len(df_lead)}件")

    lead_phone_cols = ['Phone', 'MobilePhone', 'Phone2__c', 'MobilePhone2__c']
    lead_phone_count = 0
    for _, row in df_lead.iterrows():
        for col in lead_phone_cols:
            if col in row.index:
                normalized = normalize_phone(row[col])
                if normalized:
                    if normalized not in phone_to_records:
                        phone_to_records[normalized] = []
                    phone_to_records[normalized].append(('Lead', row['Id'], row))
                    lead_phone_count += 1
                    break
    print(f"  Lead電話インデックス: {lead_phone_count}件")

    # Account
    acc_cols = ['Id', 'Name', 'Phone', 'Phone2__c', 'Website']
    df_acc = pd.read_csv(ACCOUNT_PATH,
                          usecols=lambda c: c in acc_cols,
                          dtype=str, encoding='utf-8')
    print(f"Account読み込み: {len(df_acc)}件")

    acc_phone_count = 0
    for _, row in df_acc.iterrows():
        for col in ['Phone', 'Phone2__c']:
            if col in row.index:
                normalized = normalize_phone(row[col])
                if normalized:
                    if normalized not in phone_to_records:
                        phone_to_records[normalized] = []
                    phone_to_records[normalized].append(('Account', row['Id'], row))
                    acc_phone_count += 1
                    break
    print(f"  Account電話インデックス: {acc_phone_count}件")

    # Contact
    con_cols = ['Id', 'AccountId', 'Phone', 'MobilePhone', 'Phone2__c', 'MobilePhone2__c']
    df_con = pd.read_csv(CONTACT_PATH,
                          usecols=lambda c: c in con_cols,
                          dtype=str, encoding='utf-8')
    print(f"Contact読み込み: {len(df_con)}件")

    con_phone_count = 0
    for _, row in df_con.iterrows():
        for col in ['Phone', 'MobilePhone', 'Phone2__c', 'MobilePhone2__c']:
            if col in row.index:
                normalized = normalize_phone(row[col])
                if normalized:
                    if normalized not in phone_to_records:
                        phone_to_records[normalized] = []
                    phone_to_records[normalized].append(('Contact', row['Id'], row))
                    con_phone_count += 1
                    break
    print(f"  Contact電話インデックス: {con_phone_count}件")

    print(f"ユニーク電話番号総数: {len(phone_to_records)}件")
    return phone_to_records, df_lead, df_acc, df_con


# ========================================
# STEP 4: 突合処理
# ========================================
def match_records(df_miidas, contract_phones, called_phones, phone_to_records):
    print("\n" + "=" * 70)
    print("[STEP 4] 突合処理")
    print("=" * 70)

    matched = []
    new_leads = []
    excluded_contract = []
    excluded_called = []

    for _, row in df_miidas.iterrows():
        phone = row['phone_normalized']

        # 成約先チェック
        if phone in contract_phones:
            excluded_contract.append({**row.to_dict(), 'reason': '成約先電話番号'})
            continue

        # 電話済みチェック
        if phone in called_phones:
            excluded_called.append({**row.to_dict(), 'reason': '電話済み'})
            continue

        # Salesforce突合
        if phone in phone_to_records:
            records = phone_to_records[phone]
            # Lead優先
            best_match = None
            for obj_type, obj_id, record in records:
                if obj_type == 'Lead' and (best_match is None or best_match[0] != 'Lead'):
                    best_match = (obj_type, obj_id, record)
                elif best_match is None:
                    best_match = (obj_type, obj_id, record)
            if best_match:
                matched.append({
                    **row.to_dict(),
                    'match_object': best_match[0],
                    'match_id': best_match[1],
                })
        else:
            new_leads.append(row.to_dict())

    excluded = excluded_contract + excluded_called

    print(f"既存マッチ: {len(matched)}件")
    print(f"  - Lead: {sum(1 for m in matched if m['match_object']=='Lead')}件")
    print(f"  - Account: {sum(1 for m in matched if m['match_object']=='Account')}件")
    print(f"  - Contact: {sum(1 for m in matched if m['match_object']=='Contact')}件")
    print(f"新規リード候補: {len(new_leads)}件")
    print(f"除外: {len(excluded)}件")
    print(f"  - 成約先: {len(excluded_contract)}件")
    print(f"  - 電話済み: {len(excluded_called)}件")

    return matched, new_leads, excluded


# ========================================
# STEP 5: セグメント分析
# ========================================
def analyze_segments(new_leads):
    print("\n" + "=" * 70)
    print("[STEP 5] セグメント分析（新規リードのみ）")
    print("=" * 70)

    segments = {
        '代表者直通_携帯あり': [],
        '代表者直通_携帯なし': [],
        '担当者経由_携帯あり': [],
        '担当者経由_携帯なし': [],
        'バイネームなし_携帯あり': [],
        'バイネームなし_携帯なし': [],
    }

    for row in new_leads:
        contact_name = row.get('contact_name', '')
        president_name = row.get('president_name', '')
        has_mobile = bool(row.get('mobile_phone'))

        # セグメント判定
        if is_real_name(contact_name) and president_name:
            # 代表者名と担当者名の一致チェック（部分一致）
            cn = str(contact_name).replace(' ', '').replace('　', '')
            pn = str(president_name).replace(' ', '').replace('　', '')
            if cn in pn or pn in cn:
                key = '代表者直通_携帯あり' if has_mobile else '代表者直通_携帯なし'
            else:
                key = '担当者経由_携帯あり' if has_mobile else '担当者経由_携帯なし'
        elif is_real_name(contact_name):
            key = '担当者経由_携帯あり' if has_mobile else '担当者経由_携帯なし'
        else:
            key = 'バイネームなし_携帯あり' if has_mobile else 'バイネームなし_携帯なし'

        segments[key].append(row)

    print(f"\n{'セグメント':<25} {'件数':>6}")
    print("-" * 35)
    for seg, items in segments.items():
        print(f"  {seg:<23} {len(items):>4}件")
    print("-" * 35)
    best = segments['代表者直通_携帯あり']
    print(f"  最強セグメント（代表者直通×携帯あり）: {len(best)}件")

    return segments


# ========================================
# STEP 6: CSV生成
# ========================================
def generate_new_lead_csv(new_leads):
    print("\n" + "=" * 70)
    print("[STEP 6-1] 新規リード作成CSV生成")
    print("=" * 70)

    records = []
    skipped_no_company = 0
    skipped_no_phone = 0

    for row in new_leads:
        company = row.get('company_name', '')
        if not company or str(company) == 'nan':
            skipped_no_company += 1
            continue

        phone = row.get('phone', '')
        if not phone or str(phone) == 'nan':
            skipped_no_phone += 1
            print(f"  警告: Phone空のためスキップ: {company}")
            continue

        last_name = row.get('contact_name', '') or '担当者'
        if str(last_name) == 'nan':
            last_name = '担当者'

        title = row.get('contact_title', '')
        if pd.isna(title) or str(title) == 'nan':
            title = ''

        president_name = row.get('president_name', '')
        if pd.isna(president_name) or str(president_name) == 'nan':
            president_name = ''

        record = {
            'Company': company,
            'LastName': last_name,
            'Title': title,
            'PresidentName__c': president_name,
            'Phone': phone,
            'MobilePhone': row.get('mobile_phone', '') or '',
            'Prefecture__c': row.get('prefecture', '') or '',
            'Street': row.get('street', '') or '',
            'Website': row.get('website', '') or '',
            'LeadSource': 'Other',
            'Paid_Media__c': 'ミイダス',
            'Paid_DataSource__c': 'ミイダス',
            'Paid_JobTitle__c': row.get('job_type', '') or '',
            'Paid_RecruitmentType__c': row.get('job_type', '') or '',
            'Paid_EmploymentType__c': '',
            'Paid_Industry__c': row.get('industry', '') or '',
            'Paid_NumberOfRecruitment__c': row.get('num_recruitment', '') or '',
            'Paid_Memo__c': row.get('memo', '') or '',
            'Paid_URL__c': row.get('url', '') or '',
            'Paid_DataExportDate__c': TODAY,
        }

        # 空文字をNaNに変換
        record = {k: (v if v != '' else np.nan) for k, v in record.items()}
        records.append(record)

    if skipped_no_company > 0:
        print(f"Companyなしスキップ: {skipped_no_company}件")
    if skipped_no_phone > 0:
        print(f"Phoneなしスキップ: {skipped_no_phone}件")

    df = pd.DataFrame(records)
    # 電話番号カラムをstring型に強制（先頭0欠落防止）
    for col in ['Phone', 'MobilePhone']:
        if col in df.columns:
            df[col] = df[col].astype(str).replace('nan', '')
            # 先頭0が欠落している場合は復元（10桁未満の場合）
            def fix_phone(v):
                if not v or v == 'nan' or v == '':
                    return ''
                digits = re.sub(r'\D', '', v)
                if len(digits) == 9:
                    return '0' + digits
                elif len(digits) == 10:
                    return digits
                return v
            df[col] = df[col].apply(fix_phone)
            df[col] = df[col].replace('', np.nan)

    output_path = OUTPUT_DIR / f"miidas_new_leads_{TIMESTAMP}.csv"
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"新規リードCSV: {output_path}")
    print(f"件数: {len(df)}")

    return df, output_path


def generate_update_csv(matched, df_lead, df_acc, df_con):
    print("\n" + "=" * 70)
    print("[STEP 6-2] 更新用CSV生成")
    print("=" * 70)

    # Lead更新
    lead_updates = []
    for row in matched:
        if row['match_object'] != 'Lead':
            continue

        lead_id = row['match_id']
        lead_row = df_lead[df_lead['Id'] == lead_id]
        if len(lead_row) == 0:
            continue
        lead_row = lead_row.iloc[0]

        update = {'Id': lead_id}

        # LastName更新ロジック
        existing_name = lead_row.get('LastName', '')
        new_name = row.get('contact_name', '')
        lead_status = lead_row.get('Status', '')
        if is_real_name(new_name):
            if is_generic_name(existing_name):
                update['LastName'] = str(new_name).split('\n')[0].strip()
            elif is_real_name(existing_name) and is_uncontacted_status(lead_status):
                update['LastName'] = str(new_name).split('\n')[0].strip()

        # Title更新（空欄のみ）
        new_title = row.get('contact_title', '')
        if new_title and str(new_title) != 'nan':
            existing_title = lead_row.get('Title', '')
            if pd.isna(existing_title) or existing_title == '' or str(existing_title) == 'nan':
                update['Title'] = new_title

        # PresidentName__c更新（空欄のみ）
        new_president = row.get('president_name', '')
        if new_president and str(new_president) != 'nan':
            existing_president = lead_row.get('PresidentName__c', '')
            if pd.isna(existing_president) or existing_president == '' or str(existing_president) == 'nan':
                update['PresidentName__c'] = new_president

        # Paid_*フィールド（常に更新 - 最新情報で上書き）
        update['Paid_Media__c'] = 'ミイダス'
        update['Paid_DataSource__c'] = 'ミイダス'
        update['Paid_DataExportDate__c'] = TODAY

        if row.get('job_type') and str(row.get('job_type')) != 'nan':
            update['Paid_JobTitle__c'] = row['job_type']
            update['Paid_RecruitmentType__c'] = row['job_type']
        if row.get('industry') and str(row.get('industry')) != 'nan':
            update['Paid_Industry__c'] = row['industry']
        if row.get('num_recruitment') and str(row.get('num_recruitment')) != 'nan':
            update['Paid_NumberOfRecruitment__c'] = row['num_recruitment']
        if row.get('url') and str(row.get('url')) != 'nan':
            update['Paid_URL__c'] = row['url']
        if row.get('memo') and str(row.get('memo')) != 'nan':
            update['Paid_Memo__c'] = row['memo']

        # LeadSourceMemo__c: 【既存更新】追記
        existing_memo = lead_row.get('LeadSourceMemo__c', '')
        if pd.isna(existing_memo):
            existing_memo = ''
        batch_tag = f"【既存更新】有料媒体突合 {TODAY} ミイダス"
        if batch_tag not in str(existing_memo):
            new_memo = f"{existing_memo}\n{batch_tag}".strip() if existing_memo else batch_tag
            update['LeadSourceMemo__c'] = new_memo

        if len(update) > 1:  # Id以外のフィールドがある場合のみ
            lead_updates.append(update)

    df_lead_updates = pd.DataFrame(lead_updates)
    lead_path = OUTPUT_DIR / f"miidas_lead_updates_{TIMESTAMP}.csv"
    df_lead_updates.to_csv(lead_path, index=False, encoding='utf-8-sig')
    print(f"Lead更新CSV: {lead_path} ({len(df_lead_updates)}件)")

    # Account更新
    acc_updates = []
    acc_ids_done = set()
    for row in matched:
        if row['match_object'] == 'Account':
            acc_id = row['match_id']
        elif row['match_object'] == 'Contact':
            con_row = df_con[df_con['Id'] == row['match_id']]
            if len(con_row) == 0:
                continue
            acc_id = con_row.iloc[0].get('AccountId', '')
            if pd.isna(acc_id) or not acc_id:
                continue
        else:
            continue

        if acc_id in acc_ids_done:
            continue
        acc_ids_done.add(acc_id)

        acc_row = df_acc[df_acc['Id'] == acc_id]
        if len(acc_row) == 0:
            continue
        acc_row = acc_row.iloc[0]

        update = {
            'Id': acc_id,
            'Paid_Media__c': 'ミイダス',
            'Paid_DataSource__c': 'ミイダス',
            'Paid_DataExportDate__c': TODAY,
        }
        if row.get('job_type') and str(row.get('job_type')) != 'nan':
            update['Paid_JobTitle__c'] = row['job_type']
        if row.get('url') and str(row.get('url')) != 'nan':
            update['Paid_URL__c'] = row['url']

        acc_updates.append(update)

    df_acc_updates = pd.DataFrame(acc_updates)
    acc_path = OUTPUT_DIR / f"miidas_account_updates_{TIMESTAMP}.csv"
    df_acc_updates.to_csv(acc_path, index=False, encoding='utf-8-sig')
    print(f"Account更新CSV: {acc_path} ({len(df_acc_updates)}件)")

    return df_lead_updates, df_acc_updates, lead_path, acc_path


def generate_excluded_csv(excluded):
    if not excluded:
        return
    df = pd.DataFrame(excluded)
    # 内部フィールドを除去
    drop_cols = [c for c in df.columns if c.startswith('_')]
    df = df.drop(columns=drop_cols, errors='ignore')
    path = OUTPUT_DIR / f"miidas_excluded_{TIMESTAMP}.csv"
    df.to_csv(path, index=False, encoding='utf-8-sig')
    print(f"除外リスト: {path} ({len(df)}件)")


# ========================================
# メイン
# ========================================
def main():
    print("=" * 70)
    print(f"ミイダス データCSV生成 ({TODAY})")
    print("=" * 70)

    # データ読み込み
    df_miidas = load_miidas_data()
    contract_phones, called_phones = load_exclusion_phones()
    phone_to_records, df_lead, df_acc, df_con = load_salesforce_phone_index()

    # 突合処理
    matched, new_leads, excluded = match_records(
        df_miidas, contract_phones, called_phones, phone_to_records)

    # セグメント分析
    segments = analyze_segments(new_leads)

    # CSV生成
    df_new, new_path = generate_new_lead_csv(new_leads)
    df_lead_upd, df_acc_upd, lead_path, acc_path = generate_update_csv(
        matched, df_lead, df_acc, df_con)
    generate_excluded_csv(excluded)

    # 最終サマリー
    print("\n" + "=" * 70)
    print("処理完了サマリー")
    print("=" * 70)
    print(f"入力: {len(df_miidas)}件（ユニーク電話番号）")
    print(f"除外: {len(excluded)}件")
    print(f"  - 成約先: {sum(1 for e in excluded if e['reason']=='成約先電話番号')}件")
    print(f"  - 電話済み: {sum(1 for e in excluded if e['reason']=='電話済み')}件")
    print(f"既存マッチ: {len(matched)}件")
    print(f"新規リード: {len(df_new)}件")
    print(f"Lead更新: {len(df_lead_upd)}件")
    print(f"Account更新: {len(df_acc_upd)}件")

    best_seg = segments['代表者直通_携帯あり']
    print(f"\n最強セグメント（代表者直通×携帯あり）: {len(best_seg)}件")

    print(f"\n出力ファイル:")
    print(f"  新規リード: {new_path}")
    print(f"  Lead更新: {lead_path}")
    print(f"  Account更新: {acc_path}")

    return new_path, lead_path, acc_path, segments


if __name__ == "__main__":
    main()
