# -*- coding: utf-8 -*-
"""
ミイダス NDJSON パイプライン
- NDJSONデータを読み込み
- Salesforce最新データをBulk APIで取得
- 電話番号抽出・突合・除外
- CSV生成
"""

import sys
import json
import re
import pickle
from pathlib import Path
from datetime import datetime

import pandas as pd
import numpy as np

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

OUTPUT_DIR = Path("data/output/media_matching")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
TODAY = datetime.now().strftime('%Y-%m-%d')
TIMESTAMP = datetime.now().strftime('%Y%m%d_%H%M%S')

NDJSON_PATH = Path(r"C:/Users/fuji1/OneDrive/デスクトップ/pythonスクリプト置き場/miidas_night_result_20260302.ndjson")
CONTRACT_PATH = Path("data/output/contract_accounts_20260303_112903.csv")
CALLED_LIST_PATH = Path(r"C:/Users/fuji1/Downloads/媒体掲載中のリスト.xlsx")

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

GENERIC_NAMES = [
    '担当者', '採用担当', '採用担当者', '採用担当者（名前を聞けたら変更）',
    '人事担当', '人事担当者', '採用係', '採用担当係', '店長', '院長', '事務長',
    '総務担当', '総務担当者', '総務課', '管理者', '責任者', '代表者',
]

INVALID_NAME_LABELS = [
    '連絡先', 'TEL', 'tel', 'Tel', 'FAX', 'fax', 'Fax',
    '電話番号', '携帯番号', 'メール', 'E-mail', 'email', 'Email',
    '担当', '採用', '人事', '総務', '事務局',
    '電話', '問い合わせ', 'お問い合わせ', '問合せ',
]

TITLE_PREFIXES = [
    '理事長', '院長', '事務長', '園長', '施設長', '所長', '部長', '課長', '係長', '主任',
    '代表取締役', '取締役', '代表', '社長', '副社長', '専務', '常務', '監査役',
    '総務部', '人事部', '採用担当', '人事担当', '総務課', '法人事務局',
    '統括マネージャー', 'マネージャー', 'チーフ', 'リーダー', 'ディレクター',
    '看護部長', '看護師長', '介護部長', '事務部長', '経理部長', '営業部長',
    '店長', '支店長', '工場長', '本部長', '次長', '顧問', '相談役',
]

UNCONTACTED_STATUSES = ['未架電', '00 架電OK - 接触なし']


def normalize_phone(phone):
    if pd.isna(phone) or phone == '' or str(phone) == 'nan':
        return None
    digits = re.sub(r'\D', '', str(phone).strip())
    if 10 <= len(digits) <= 11:
        return digits
    return None


def convert_fullwidth_to_halfwidth(text):
    fw_digits = '０１２３４５６７８９'
    hw_digits = '0123456789'
    for fw, hw in zip(fw_digits, hw_digits):
        text = text.replace(fw, hw)
    hyphen_variants = [
        '\u2010', '\u2011', '\u2012', '\u2013', '\u2014', '\u2015',
        '\u2212', '\u30FC', '\uFF0D', '\uFF70', '－', '−', '―', 'ー', '–'
    ]
    for h in hyphen_variants:
        text = text.replace(h, '-')
    return text


def extract_phones_from_contact(contact_text):
    if not contact_text or contact_text == 'N/A' or pd.isna(contact_text):
        return [], []
    text = convert_fullwidth_to_halfwidth(str(contact_text))

    # FAX番号を特定
    fax_phones = set()
    fax_matches = re.findall(r'FAX[:\s]*([0-9][\d\-\s]{8,})', text)
    for fm in fax_matches:
        digits = re.sub(r'\D', '', fm)
        if 10 <= len(digits) <= 11:
            fax_phones.add(digits)

    # 全電話番号を抽出
    all_phones = []
    patterns = [
        r'0\d{1,4}[-\s]?\d{1,4}[-\s]?\d{3,4}',
        r'0\d{9,10}',
    ]
    for pattern in patterns:
        matches = re.findall(pattern, text)
        for m in matches:
            digits = re.sub(r'\D', '', m)
            if 10 <= len(digits) <= 11 and digits not in all_phones:
                all_phones.append(digits)

    # FAX以外
    actual_phones = [p for p in all_phones if p not in fax_phones]
    return actual_phones, list(fax_phones)


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
    fw_digits = '０１２３４５６７８９'
    hw_digits = '0123456789'
    for fw, hw in zip(fw_digits, hw_digits):
        text = text.replace(fw, hw)
    patterns = [
        r'募集人数[：:]*\s*(\d+)\s*名', r'採用人数[：:]*\s*(\d+)\s*名',
        r'採用予定[：:]*\s*(\d+)\s*名', r'(\d+)\s*名募集',
        r'(\d+)\s*名以上募集', r'(\d+)\s*人募集',
        r'(\d+)\s*名採用', r'若干名',
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
    extracted_title = ' '.join(extracted_titles) if extracted_titles else None
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

    if role_field and str(role_field).strip():
        role_str = str(role_field).strip()
        invalid_roles = ['役職なし', '役職無し', 'なし', '無し', '-', '−', '―', 'ー', '']
        if role_str not in invalid_roles and not role_str.startswith('役職なし'):
            if '\n' in role_str:
                role_str = role_str.split('\n')[0].strip()
            title = role_str

    if contact_text and str(contact_text).strip() and contact_text != 'N/A':
        text = str(contact_text)
        patterns = [
            r'担当[：:\s]*([^\n\r@0-9]{2,20})',
            r'担当者[：:\s]*([^\n\r@0-9]{2,20})',
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

    if not name and representative and str(representative).strip():
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


# ============================================================
# STEP 1: Salesforceデータ取得（Bulk API）
# ============================================================
def export_salesforce_data():
    """最新のLead/Account/ContactをBulk APIでエクスポート"""
    from scripts.bulk_export import BulkExporter

    output_dir = Path("data/output")
    exporter = BulkExporter()
    exporter.authenticate()

    # Lead
    print("\n[Lead] エクスポート中...")
    lead_fields = [
        "Id", "Company", "LastName", "Title", "Status", "Phone", "MobilePhone",
        "Phone2__c", "MobilePhone2__c", "LeadSourceMemo__c",
        "PresidentName__c",
        "Paid_Media__c", "Paid_JobTitle__c", "Paid_RecruitmentType__c",
        "Paid_EmploymentType__c", "Paid_Industry__c", "Paid_NumberOfRecruitment__c",
        "Paid_Memo__c", "Paid_DataExportDate__c", "Paid_DataSource__c", "Paid_URL__c",
    ]
    lead_path = exporter.export_object_bulk("Lead", output_dir, fields=lead_fields)
    print(f"  -> {lead_path}")

    # Account
    print("\n[Account] エクスポート中...")
    acc_fields = [
        "Id", "Name", "Phone", "PersonMobilePhone", "Phone2__c", "Description",
        "Paid_Media__c", "Paid_JobTitle__c", "Paid_URL__c",
        "Paid_DataExportDate__c", "Paid_DataSource__c",
    ]
    acc_path = exporter.export_object_bulk("Account", output_dir, fields=acc_fields)
    print(f"  -> {acc_path}")

    # Contact
    print("\n[Contact] エクスポート中...")
    con_fields = [
        "Id", "AccountId", "Phone", "MobilePhone", "Phone2__c", "MobilePhone2__c",
    ]
    con_path = exporter.export_object_bulk("Contact", output_dir, fields=con_fields)
    print(f"  -> {con_path}")

    return str(lead_path), str(acc_path), str(con_path)


# ============================================================
# STEP 2: NDJSONデータ読み込み・電話番号抽出
# ============================================================
def load_ndjson_data():
    print("\n=== ミイダスNDJSONデータ読み込み ===")

    records = []
    with open(NDJSON_PATH, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    print(f"NDJSONレコード数: {len(records)}")

    parsed = []
    no_phone = 0

    for rec in records:
        contact = rec.get('連絡先', '')
        phones, fax_phones = extract_phones_from_contact(contact)

        if not phones:
            no_phone += 1
            continue

        # 住所
        address = rec.get('勤務地', '') or rec.get('本社住所', '')
        prefecture, street = extract_prefecture(address)

        # 職種
        job_title = extract_job_title(rec.get('全本文ダンプ', ''))
        num_recruitment = extract_recruitment_number(rec.get('全本文ダンプ', ''))

        # 担当者名・役職
        contact_name, contact_title = extract_contact_name_with_title(
            contact, rec.get('代表者', ''), rec.get('役職', '')
        )

        president_name = rec.get('代表者', '')
        if not president_name:
            president_name = ''

        # 固定電話と携帯電話を分類
        landline_phones = [p for p in phones if not is_mobile_phone(p)]
        mobile_phones_list = [p for p in phones if is_mobile_phone(p)]

        # 代表電話番号を決定（固定電話優先）
        main_phone = landline_phones[0] if landline_phones else None
        main_mobile = mobile_phones_list[0] if mobile_phones_list else None
        # 突合用の正規化番号（固定電話優先、なければ携帯）
        primary_phone = main_phone or main_mobile

        if primary_phone:
            parsed.append({
                'source': 'ミイダス',
                'company_name': rec.get('企業名', ''),
                'contact_name': contact_name,
                'contact_title': contact_title,
                'president_name': president_name,
                'phone': main_phone,
                'mobile_phone': main_mobile,
                'phone_normalized': primary_phone,
                'prefecture': prefecture,
                'street': street,
                'job_type': job_title,
                'employment_type': None,
                'industry': rec.get('企業規模', ''),
                'num_recruitment': num_recruitment,
                'memo': f"設立: {rec.get('設立', 'N/A')}",
                'url': rec.get('url', ''),
                'website': rec.get('企業サイトURL', ''),
            })

    # 電話番号で重複除去
    seen = set()
    unique = []
    for r in parsed:
        if r['phone_normalized'] not in seen:
            seen.add(r['phone_normalized'])
            unique.append(r)

    print(f"電話番号なし: {no_phone}件")
    print(f"電話番号レコード（重複前）: {len(parsed)}件")
    print(f"電話番号レコード（ユニーク）: {len(unique)}件")

    return unique


# ============================================================
# STEP 3: 除外処理
# ============================================================
def load_exclusion_data():
    print("\n=== 除外リスト読み込み ===")

    # 成約先
    df_contract = pd.read_csv(CONTRACT_PATH, dtype=str)
    contract_phones = set()
    phone_cols = [c for c in df_contract.columns if 'Phone' in c or 'phone' in c.lower()]
    for col in phone_cols:
        for val in df_contract[col]:
            n = normalize_phone(val)
            if n:
                contract_phones.add(n)
    print(f"成約先電話番号: {len(contract_phones)}件")

    # 成約先会社名
    contract_names = set()
    if 'Name' in df_contract.columns:
        for name in df_contract['Name']:
            if pd.notna(name) and len(str(name)) >= 4:
                normalized = str(name).replace(' ', '').replace('\u3000', '')
                for suffix in ['株式会社', '有限会社', '医療法人', '社会福祉法人', '合同会社', '一般社団法人', '特定非営利活動法人']:
                    normalized = normalized.replace(suffix, '')
                if len(normalized) >= 3:
                    contract_names.add(normalized)
    print(f"成約先会社名: {len(contract_names)}件")

    # 電話済み
    xlsx = pd.ExcelFile(CALLED_LIST_PATH)
    phone_pattern = re.compile(r'0\d{1,4}[-\s]?\d{1,4}[-\s]?\d{3,4}|0\d{9,10}')
    called_phones = set()
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

    return contract_phones, contract_names, called_phones


def apply_exclusions(records, contract_phones, contract_names, called_phones):
    print("\n=== 除外処理 ===")

    excluded_contract = []
    excluded_name = []
    excluded_called = []
    remaining = []

    for r in records:
        phone = r['phone_normalized']
        company = r['company_name']

        # 成約先電話番号
        if phone in contract_phones:
            excluded_contract.append({**r, 'reason': '成約先電話番号'})
            continue

        # 成約先会社名
        company_normalized = company.replace(' ', '').replace('\u3000', '')
        for suffix in ['株式会社', '有限会社', '医療法人', '社会福祉法人', '合同会社', '一般社団法人', '特定非営利活動法人']:
            company_normalized = company_normalized.replace(suffix, '')

        is_contract = False
        for cn in contract_names:
            if len(cn) >= 4 and (cn in company_normalized or company_normalized in cn):
                excluded_name.append({**r, 'reason': f'成約先会社名一致: {cn}'})
                is_contract = True
                break
        if is_contract:
            continue

        # 電話済み
        if phone in called_phones:
            excluded_called.append({**r, 'reason': '電話済み'})
            continue

        remaining.append(r)

    print(f"成約先電話番号一致: {len(excluded_contract)}件")
    print(f"成約先会社名一致: {len(excluded_name)}件")
    if excluded_name:
        for e in excluded_name:
            print(f"  -> {e['company_name']} ({e['reason']})")
    print(f"電話済み: {len(excluded_called)}件")
    print(f"残り: {len(remaining)}件")

    return remaining, excluded_contract + excluded_name + excluded_called


# ============================================================
# STEP 4: Salesforce突合
# ============================================================
def load_sf_phone_index(lead_path, acc_path, con_path):
    print("\n=== Salesforce電話番号インデックス構築 ===")

    phone_to_records = {}

    # Lead
    lead_cols = [
        'Id', 'Company', 'LastName', 'Title', 'Status', 'Phone', 'MobilePhone',
        'Phone2__c', 'MobilePhone2__c', 'LeadSourceMemo__c',
        'PresidentName__c',
        'Paid_Media__c', 'Paid_JobTitle__c', 'Paid_RecruitmentType__c',
        'Paid_EmploymentType__c', 'Paid_Industry__c', 'Paid_NumberOfRecruitment__c',
        'Paid_Memo__c', 'Paid_DataExportDate__c', 'Paid_DataSource__c', 'Paid_URL__c',
    ]
    df_lead = pd.read_csv(lead_path, usecols=lambda c: c in lead_cols, dtype=str, encoding='utf-8')
    lead_phone_cols = ['Phone', 'MobilePhone', 'Phone2__c', 'MobilePhone2__c']
    for _, row in df_lead.iterrows():
        for col in lead_phone_cols:
            if col in row:
                n = normalize_phone(row[col])
                if n:
                    if n not in phone_to_records:
                        phone_to_records[n] = []
                    phone_to_records[n].append(('Lead', row['Id'], row))
                    break
    print(f"Lead: {len(df_lead)}件")

    # Account
    acc_cols = [
        'Id', 'Name', 'Phone', 'PersonMobilePhone', 'Phone2__c', 'Description',
        'Paid_Media__c', 'Paid_JobTitle__c', 'Paid_URL__c',
        'Paid_DataExportDate__c', 'Paid_DataSource__c',
    ]
    df_acc = pd.read_csv(acc_path, usecols=lambda c: c in acc_cols, dtype=str, encoding='utf-8')
    for _, row in df_acc.iterrows():
        for col in ['Phone', 'PersonMobilePhone', 'Phone2__c']:
            if col in row:
                n = normalize_phone(row[col])
                if n:
                    if n not in phone_to_records:
                        phone_to_records[n] = []
                    phone_to_records[n].append(('Account', row['Id'], row))
                    break
    print(f"Account: {len(df_acc)}件")

    # Contact
    df_con = pd.read_csv(con_path, usecols=lambda c: c in [
        'Id', 'AccountId', 'Phone', 'MobilePhone', 'Phone2__c', 'MobilePhone2__c'
    ], dtype=str, encoding='utf-8')
    for _, row in df_con.iterrows():
        for col in ['Phone', 'MobilePhone', 'Phone2__c', 'MobilePhone2__c']:
            if col in row:
                n = normalize_phone(row[col])
                if n:
                    if n not in phone_to_records:
                        phone_to_records[n] = []
                    phone_to_records[n].append(('Contact', row['Id'], row))
                    break
    print(f"Contact: {len(df_con)}件")
    print(f"ユニーク電話番号: {len(phone_to_records)}件")

    return phone_to_records, df_lead, df_acc, df_con


def match_salesforce(records, phone_to_records):
    print("\n=== Salesforce突合 ===")

    matched = []
    new_leads = []

    for r in records:
        phone = r['phone_normalized']
        if phone in phone_to_records:
            sf_records = phone_to_records[phone]
            best = None
            for obj_type, obj_id, record in sf_records:
                if obj_type == 'Lead' and (best is None or best[0] != 'Lead'):
                    best = (obj_type, obj_id, record)
                elif obj_type == 'Account' and best is None:
                    best = (obj_type, obj_id, record)
                elif obj_type == 'Contact' and best is None:
                    best = (obj_type, obj_id, record)
            if best:
                matched.append({**r, 'match_object': best[0], 'match_id': best[1]})
            else:
                new_leads.append(r)
        else:
            new_leads.append(r)

    print(f"既存マッチ: {len(matched)}件")
    lead_match = sum(1 for m in matched if m['match_object'] == 'Lead')
    acc_match = sum(1 for m in matched if m['match_object'] == 'Account')
    con_match = sum(1 for m in matched if m['match_object'] == 'Contact')
    print(f"  Lead: {lead_match}, Account: {acc_match}, Contact: {con_match}")
    print(f"新規リード候補: {len(new_leads)}件")

    return matched, new_leads


# ============================================================
# STEP 5: セグメント分析
# ============================================================
def analyze_segments(records):
    print("\n=== セグメント分析 ===")

    segments = {
        '代表者直通_携帯あり': [],
        '代表者直通_携帯なし': [],
        '担当者経由_携帯あり': [],
        '担当者経由_携帯なし': [],
        'バイネームなし_携帯あり': [],
        'バイネームなし_携帯なし': [],
    }

    for r in records:
        contact_name = r.get('contact_name', '')
        president_name = r.get('president_name', '')
        has_mobile = r.get('mobile_phone') is not None

        # セグメント判定
        if not contact_name or contact_name in GENERIC_NAMES:
            seg = 'バイネームなし'
        elif president_name and contact_name:
            # 代表者直通判定（部分一致）
            pn = president_name.replace(' ', '').replace('\u3000', '')
            cn = contact_name.replace(' ', '').replace('\u3000', '')
            if pn in cn or cn in pn:
                seg = '代表者直通'
            else:
                seg = '担当者経由'
        else:
            seg = '担当者経由'

        mobile_key = '携帯あり' if has_mobile else '携帯なし'
        segments[f'{seg}_{mobile_key}'].append(r)

    print(f"\n{'セグメント':<25} {'件数':>6}")
    print("-" * 35)
    total = 0
    for seg_name, seg_records in segments.items():
        print(f"  {seg_name:<23} {len(seg_records):>6}")
        total += len(seg_records)
    print("-" * 35)
    print(f"  {'合計':<23} {total:>6}")

    strongest = segments['代表者直通_携帯あり']
    print(f"\n最強セグメント（代表者直通×携帯あり）: {len(strongest)}件")

    return segments


# ============================================================
# STEP 6: CSV生成
# ============================================================
def generate_csvs(new_leads, matched, excluded, segments, df_lead, df_acc, df_con):
    print("\n=== CSV生成 ===")

    # 新規リードCSV
    new_records = []
    for row in new_leads:
        last_name = row.get('contact_name', '')
        if not last_name or str(last_name) == 'nan':
            last_name = '担当者'
        title = row.get('contact_title', '') or ''
        president_name = row.get('president_name', '') or ''

        # Phone必須チェック
        phone_val = row.get('phone', '') or ''
        mobile_val = row.get('mobile_phone', '') or ''

        # 携帯のみの場合はPhoneにも設定（Salesforce必須対応）
        if not phone_val and mobile_val:
            phone_val = mobile_val

        if not phone_val:
            continue

        record = {
            'Company': row.get('company_name', ''),
            'LastName': last_name,
            'Title': title,
            'PresidentName__c': president_name,
            'Phone': phone_val,
            'MobilePhone': mobile_val,
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
        new_records.append(record)

    df_new = pd.DataFrame(new_records)
    new_path = OUTPUT_DIR / f"miidas_new_leads_{TIMESTAMP}.csv"
    df_new.to_csv(new_path, index=False, encoding='utf-8-sig')
    print(f"新規リードCSV: {new_path} ({len(df_new)}件)")

    # Lead更新CSV
    df_matched = pd.DataFrame(matched)
    lead_matched = df_matched[df_matched['match_object'] == 'Lead'] if len(df_matched) > 0 else pd.DataFrame()
    lead_updates = []

    for _, row in lead_matched.iterrows():
        lead_id = row['match_id']
        lead_row = df_lead[df_lead['Id'] == lead_id]
        if len(lead_row) == 0:
            continue
        lead_row = lead_row.iloc[0]

        update = {'Id': lead_id}

        # LastName更新
        existing_name = lead_row.get('LastName', '')
        new_name = row.get('contact_name', '')
        lead_status = lead_row.get('Status', '')

        if is_real_name(new_name):
            if is_generic_name(existing_name):
                update['LastName'] = str(new_name).strip()
            elif is_real_name(existing_name) and str(lead_status).strip() in UNCONTACTED_STATUSES:
                update['LastName'] = str(new_name).strip()

        # Title
        new_title = row.get('contact_title', '')
        if new_title and str(new_title) != 'nan':
            existing_title = lead_row.get('Title', '')
            if pd.isna(existing_title) or existing_title == '' or str(existing_title) == 'nan':
                update['Title'] = new_title

        # Paid_*フィールド
        field_mapping = {
            'Paid_Media__c': 'ミイダス',
            'Paid_DataSource__c': 'ミイダス',
            'Paid_JobTitle__c': row.get('job_type'),
            'Paid_RecruitmentType__c': row.get('job_type'),
            'Paid_Industry__c': row.get('industry'),
            'Paid_NumberOfRecruitment__c': row.get('num_recruitment'),
            'Paid_URL__c': row.get('url'),
            'Paid_Memo__c': row.get('memo'),
        }
        for sf_field, value in field_mapping.items():
            if pd.notna(value) and value != '' and str(value) != 'nan':
                existing = lead_row.get(sf_field, '')
                if pd.isna(existing) or existing == '' or str(existing) == 'nan':
                    update[sf_field] = value

        update['Paid_DataExportDate__c'] = TODAY

        lead_updates.append(update)

    df_lead_upd = pd.DataFrame(lead_updates) if lead_updates else pd.DataFrame()
    lead_upd_path = OUTPUT_DIR / f"miidas_lead_updates_{TIMESTAMP}.csv"
    df_lead_upd.to_csv(lead_upd_path, index=False, encoding='utf-8-sig')
    print(f"Lead更新CSV: {lead_upd_path} ({len(df_lead_upd)}件)")

    # Account更新CSV
    acc_con_matched = df_matched[df_matched['match_object'].isin(['Account', 'Contact'])] if len(df_matched) > 0 else pd.DataFrame()
    acc_to_source = {}
    for _, row in acc_con_matched.iterrows():
        if row['match_object'] == 'Account':
            acc_to_source[row['match_id']] = row
        elif row['match_object'] == 'Contact':
            con_row = df_con[df_con['Id'] == row['match_id']]
            if len(con_row) > 0 and pd.notna(con_row.iloc[0].get('AccountId')):
                acc_id = con_row.iloc[0]['AccountId']
                if acc_id not in acc_to_source:
                    acc_to_source[acc_id] = row

    acc_updates = []
    for acc_id, source_row in acc_to_source.items():
        acc_row = df_acc[df_acc['Id'] == acc_id]
        if len(acc_row) == 0:
            continue
        acc_row = acc_row.iloc[0]

        update = {'Id': acc_id}
        field_mapping = {
            'Paid_Media__c': 'ミイダス',
            'Paid_DataSource__c': 'ミイダス',
            'Paid_JobTitle__c': source_row.get('job_type'),
            'Paid_URL__c': source_row.get('url'),
        }
        for sf_field, value in field_mapping.items():
            if pd.notna(value) and value != '' and str(value) != 'nan':
                existing = acc_row.get(sf_field, '')
                if pd.isna(existing) or existing == '' or str(existing) == 'nan':
                    update[sf_field] = value
        update['Paid_DataExportDate__c'] = TODAY
        acc_updates.append(update)

    df_acc_upd = pd.DataFrame(acc_updates) if acc_updates else pd.DataFrame()
    acc_upd_path = OUTPUT_DIR / f"miidas_account_updates_{TIMESTAMP}.csv"
    df_acc_upd.to_csv(acc_upd_path, index=False, encoding='utf-8-sig')
    print(f"Account更新CSV: {acc_upd_path} ({len(df_acc_upd)}件)")

    # 除外リスト
    if excluded:
        df_excluded = pd.DataFrame(excluded)
        exc_path = OUTPUT_DIR / f"miidas_excluded_{TIMESTAMP}.csv"
        df_excluded.to_csv(exc_path, index=False, encoding='utf-8-sig')
        print(f"除外リスト: {exc_path} ({len(df_excluded)}件)")

    return df_new, new_path, df_lead_upd, lead_upd_path, df_acc_upd, acc_upd_path


# ============================================================
# メイン
# ============================================================
def main():
    print("=" * 70)
    print(f"ミイダス NDJSON パイプライン ({TODAY})")
    print("=" * 70)

    # STEP 1: Salesforce最新データ取得（既存ファイルがあればスキップ）
    print("\n[STEP 1] Salesforceデータエクスポート")
    # 直近エクスポート済みファイルを検索
    existing_leads = sorted(Path("data/output").glob("Lead_20260303_*.csv"), reverse=True)
    existing_accs = sorted(Path("data/output").glob("Account_20260303_*.csv"), reverse=True)
    existing_cons = sorted(Path("data/output").glob("Contact_20260303_*.csv"), reverse=True)

    if existing_leads and existing_accs and existing_cons:
        lead_path = str(existing_leads[0])
        acc_path = str(existing_accs[0])
        con_path = str(existing_cons[0])
        print(f"  既存データを使用:")
        print(f"    Lead: {lead_path}")
        print(f"    Account: {acc_path}")
        print(f"    Contact: {con_path}")
    else:
        lead_path, acc_path, con_path = export_salesforce_data()

    # STEP 2: NDJSONデータ読み込み
    print("\n[STEP 2] NDJSONデータ読み込み")
    miidas_records = load_ndjson_data()

    # STEP 3: 除外処理
    print("\n[STEP 3] 除外処理")
    contract_phones, contract_names, called_phones = load_exclusion_data()
    remaining, excluded = apply_exclusions(miidas_records, contract_phones, contract_names, called_phones)

    # STEP 4: Salesforce突合
    print("\n[STEP 4] Salesforce突合")
    phone_to_records, df_lead, df_acc, df_con = load_sf_phone_index(lead_path, acc_path, con_path)
    matched, new_leads = match_salesforce(remaining, phone_to_records)

    # STEP 5: セグメント分析（新規リードのみ）
    print("\n[STEP 5] セグメント分析（新規リード）")
    segments = analyze_segments(new_leads)

    # STEP 6: CSV生成
    print("\n[STEP 6] CSV生成")
    df_new, new_path, df_lead_upd, lead_upd_path, df_acc_upd, acc_upd_path = generate_csvs(
        new_leads, matched, excluded, segments, df_lead, df_acc, df_con
    )

    # サマリー
    print("\n" + "=" * 70)
    print("処理結果サマリー")
    print("=" * 70)
    print(f"  NDJSONレコード: 339件")
    print(f"  電話番号抽出 → ユニーク: {len(miidas_records)}件")
    print(f"  除外合計: {len(excluded)}件")
    print(f"  既存マッチ: {len(matched)}件")
    print(f"  新規リード: {len(new_leads)}件 → CSV: {len(df_new)}件")
    print(f"  Lead更新: {len(df_lead_upd)}件")
    print(f"  Account更新: {len(df_acc_upd)}件")
    print("=" * 70)


if __name__ == "__main__":
    main()
