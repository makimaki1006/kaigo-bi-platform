# -*- coding: utf-8 -*-
"""
ミイダス データCSV生成スクリプト
- 連絡先から電話番号を抽出
- 全本文ダンプから職種を抽出
- 電話番号ベースで既存Salesforceデータと突合
"""

import pandas as pd
import numpy as np
import re
from datetime import datetime
from pathlib import Path

OUTPUT_DIR = Path("data/output/media_matching")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
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

# 一般名称リスト（これらは名前として扱わない）
GENERIC_NAMES = [
    '担当者', '採用担当', '採用担当者', '採用担当者（名前を聞けたら変更）',
    '人事担当', '人事担当者', '採用係', '採用担当係', '店長', '院長', '事務長',
    '総務担当', '総務担当者', '総務課', '管理者', '責任者', '代表者',
]

# 名前ではないラベル（完全一致で除外）
INVALID_NAME_LABELS = [
    '連絡先', 'TEL', 'tel', 'Tel', 'FAX', 'fax', 'Fax',
    '電話番号', '携帯番号', 'メール', 'E-mail', 'email', 'Email',
    '担当', '採用', '人事', '総務', '事務局',
    '電話', '問い合わせ', 'お問い合わせ', '問合せ',
]

# 役職プレフィックス（これらで始まる場合は除去）
TITLE_PREFIXES = [
    '理事長', '院長', '事務長', '園長', '施設長', '所長', '部長', '課長', '係長', '主任',
    '代表取締役', '取締役', '代表', '社長', '副社長', '専務', '常務', '監査役',
    '総務部', '人事部', '採用担当', '人事担当', '総務課', '法人事務局',
    '統括マネージャー', 'マネージャー', 'チーフ', 'リーダー', 'ディレクター',
    '看護部長', '看護師長', '介護部長', '事務部長', '経理部長', '営業部長',
    '店長', '支店長', '工場長', '本部長', '次長', '顧問', '相談役',
]

# 未接触ステータス
UNCONTACTED_STATUSES = [
    '未架電',
    '00 架電OK - 接触なし',
]

def normalize_phone(phone):
    """電話番号を正規化（数字のみ10-11桁）"""
    if pd.isna(phone) or phone == '' or str(phone) == 'nan':
        return None
    phone_str = str(phone).strip()
    digits = re.sub(r'\D', '', phone_str)
    if len(digits) >= 10 and len(digits) <= 11:
        return digits
    return None

def convert_fullwidth_to_halfwidth(text):
    """全角数字・記号を半角に変換"""
    # 全角数字 → 半角数字
    fullwidth_digits = '０１２３４５６７８９'
    halfwidth_digits = '0123456789'
    for fw, hw in zip(fullwidth_digits, halfwidth_digits):
        text = text.replace(fw, hw)

    # 各種ハイフン・ダッシュ系 → 半角ハイフン
    # U+002D: HYPHEN-MINUS (標準)
    # U+2010: HYPHEN
    # U+2011: NON-BREAKING HYPHEN
    # U+2012: FIGURE DASH
    # U+2013: EN DASH
    # U+2014: EM DASH
    # U+2015: HORIZONTAL BAR
    # U+2212: MINUS SIGN
    # U+30FC: KATAKANA-HIRAGANA PROLONGED SOUND MARK
    # U+FF0D: FULLWIDTH HYPHEN-MINUS
    # U+FF70: HALFWIDTH KATAKANA-HIRAGANA PROLONGED SOUND MARK
    hyphen_variants = [
        '\u2010', '\u2011', '\u2012', '\u2013', '\u2014', '\u2015',
        '\u2212', '\u30FC', '\uFF0D', '\uFF70',
        '－', '−', '―', 'ー', '–'
    ]
    for h in hyphen_variants:
        text = text.replace(h, '-')

    return text

def extract_phones_from_contact(contact_text):
    """連絡先テキストから電話番号を抽出"""
    if pd.isna(contact_text) or not contact_text:
        return []

    text = str(contact_text)

    # 全角を半角に変換
    text = convert_fullwidth_to_halfwidth(text)

    phones = []

    # 電話番号パターン（ハイフンあり/なし対応）
    patterns = [
        r'0\d{1,4}[-\s]?\d{1,4}[-\s]?\d{3,4}',  # ハイフン区切り（2-3セグメント）
        r'0\d{9,10}',  # 連続数字
    ]

    for pattern in patterns:
        matches = re.findall(pattern, text)
        for m in matches:
            normalized = normalize_phone(m)
            if normalized and normalized not in phones:
                phones.append(normalized)

    return phones

def is_mobile_phone(phone_digits):
    """携帯電話番号かどうか判定（070/080/090始まり）"""
    if not phone_digits:
        return False
    return phone_digits.startswith(('070', '080', '090'))

def extract_prefecture(address):
    """住所から都道府県を抽出"""
    if pd.isna(address) or not address:
        return None, address
    address = str(address).strip()
    # 改行があれば最初の行で都道府県を探す
    lines = address.split('\n')
    for line in lines:
        line = line.strip()
        for pref in PREFECTURES:
            if pref in line:
                # 都道府県以降を抽出
                idx = line.find(pref)
                remaining = line[idx + len(pref):].strip()
                return pref, remaining if remaining else address
    return None, address

def extract_job_title(full_text):
    """全本文ダンプから職種を抽出（最初の行）"""
    if pd.isna(full_text) or not full_text:
        return None
    text = str(full_text).strip()
    # 最初の行を取得
    first_line = text.split('\n')[0].strip()
    # 【】や《》を除去してタイトルを取得
    first_line = re.sub(r'[【】《》]', '', first_line)
    return first_line[:100] if first_line else None


def extract_recruitment_number(full_text):
    """全本文ダンプから募集人数を抽出"""
    if pd.isna(full_text) or not full_text:
        return None

    text = str(full_text)

    # 全角数字を半角に変換
    fullwidth_digits = '０１２３４５６７８９'
    halfwidth_digits = '0123456789'
    for fw, hw in zip(fullwidth_digits, halfwidth_digits):
        text = text.replace(fw, hw)

    # 募集人数のパターン（優先順）
    patterns = [
        r'募集人数[：:]*\s*(\d+)\s*名',
        r'採用人数[：:]*\s*(\d+)\s*名',
        r'採用予定[：:]*\s*(\d+)\s*名',
        r'(\d+)\s*名募集',
        r'(\d+)\s*名以上募集',
        r'(\d+)\s*名程度募集',
        r'(\d+)\s*人募集',
        r'(\d+)\s*名の募集',
        r'(\d+)\s*名採用',
        r'(\d+)\s*人採用',
        r'若干名',  # 特殊ケース
    ]

    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            if pattern == r'若干名':
                return '若干名'
            return match.group(1) + '名'

    return None

def clean_name_with_title(name):
    """名前から役職・電話番号などを除去してクリーンアップ（役職も返す）

    Returns:
        tuple: (クリーンアップした名前, 抽出した役職)
    """
    if not name:
        return None, None

    name = str(name).strip()
    extracted_title = None

    # 無効なラベルを完全一致でチェック
    name_stripped = name.strip()
    for label in INVALID_NAME_LABELS:
        if name_stripped == label:
            return None, None

    # 電話番号パターンを除去
    name = re.sub(r'0\d{1,4}[-－ー−‐\s]?\d{1,4}[-－ー−‐\s]?\d{3,4}', '', name)
    name = re.sub(r'0\d{9,10}', '', name)

    # 先頭・末尾の不要な記号を除去（】【（）など）
    name = re.sub(r'^[】【\[\]「」『』《》〈〉（）\(\)\s：:・]+', '', name)
    name = re.sub(r'[】【\[\]「」『』《》〈〉（）\(\)\s：:・]+$', '', name)

    # 役職プレフィックスを除去（繰り返し処理で複数役職に対応）
    extracted_titles = []
    changed = True
    while changed:
        changed = False
        for prefix in TITLE_PREFIXES:
            if name.startswith(prefix):
                extracted_titles.append(prefix)
                name = name[len(prefix):].strip()
                # 続く「：」や空白も除去
                name = re.sub(r'^[：:\s]+', '', name)
                changed = True
                break

    # 抽出した役職を結合
    if extracted_titles:
        extracted_title = ' '.join(extracted_titles)

    # 全角スペースを半角に
    name = name.replace('　', ' ')

    # 記号を除去
    name = re.sub(r'^[：:・\s]+', '', name)
    name = re.sub(r'[：:・\s]+$', '', name)

    # 括弧で囲まれた名前から括弧を除去（例:（今泉）→ 今泉）
    if name.startswith('（') and name.endswith('）'):
        name = name[1:-1].strip()
    if name.startswith('(') and name.endswith(')'):
        name = name[1:-1].strip()

    # 最終的に無効なラベルと一致したらNone
    for label in INVALID_NAME_LABELS:
        if name == label:
            return None, extracted_title

    # 1文字以下は無効
    if len(name) < 2:
        return None, extracted_title

    return name.strip() if name.strip() else None, extracted_title


def clean_name(name):
    """名前から役職・電話番号などを除去してクリーンアップ（後方互換性用）"""
    cleaned, _ = clean_name_with_title(name)
    return cleaned

def extract_contact_name_with_title(contact_text, representative=None, role_field=None):
    """連絡先テキストから担当者名と役職を抽出

    Returns:
        tuple: (担当者名, 役職)
    """
    name = None
    title = None

    # 役職フィールドがあればそれを優先使用
    if pd.notna(role_field) and role_field:
        role_str = str(role_field).strip()
        # 「役職なし」等の無効な値を除外
        invalid_roles = ['役職なし', '役職無し', 'なし', '無し', '-', '−', '―', 'ー', '']
        if role_str not in invalid_roles and not role_str.startswith('役職なし'):
            # 改行があれば最初の行のみ使用
            if '\n' in role_str:
                role_str = role_str.split('\n')[0].strip()
            title = role_str

    if pd.notna(contact_text) and contact_text:
        text = str(contact_text)

        # 担当者名のパターン（優先順）
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
                # クリーンアップ（役職も抽出）
                candidate, extracted_title = clean_name_with_title(candidate)
                if candidate and len(candidate) >= 2:
                    name = candidate
                    # 役職がまだなければ抽出した役職を使用
                    if not title and extracted_title:
                        title = extracted_title
                    break

    # 連絡先から取れなければ代表者名を使用
    if not name and pd.notna(representative) and representative:
        name, extracted_title = clean_name_with_title(str(representative))
        if not title and extracted_title:
            title = extracted_title

    return name, title


def extract_contact_name(contact_text, representative=None):
    """連絡先テキストから担当者名を抽出（後方互換性用）"""
    name, _ = extract_contact_name_with_title(contact_text, representative)
    return name

def is_uncontacted_status(status):
    """未接触ステータスかどうか判定"""
    if pd.isna(status) or not status:
        return True
    return str(status).strip() in UNCONTACTED_STATUSES

def clean_contact_name(name):
    """担当者名をクリーンアップ"""
    if pd.isna(name) or not name:
        return None
    name = str(name).strip()
    if '\n' in name:
        name = name.split('\n')[0].strip()
    return name if name else None

def is_generic_name(name):
    """一般名称かどうか判定"""
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
    """バイネーム（個人名）かどうか判定"""
    if pd.isna(name) or not name:
        return False
    name = clean_contact_name(name)
    if not name:
        return False
    for generic in GENERIC_NAMES:
        if generic in name:
            return False
    return True

def load_miidas_data():
    """ミイダスデータを読み込み"""
    print("=== ミイダスデータ読み込み ===")

    df = pd.read_csv(r'C:\Users\fuji1\OneDrive\デスクトップ\pythonスクリプト置き場\miidas_structured_data.csv',
                     encoding='utf-8', dtype=str, low_memory=False)
    print(f"総レコード数: {len(df)}")

    records = []
    no_phone_count = 0

    # 電話番号を抽出するフィールド（優先順）
    phone_fields = ['連絡先', '全本文ダンプ', '探索_連絡先']

    for idx, row in df.iterrows():
        # 複数フィールドから電話番号を抽出
        phones = []
        for field in phone_fields:
            if field in df.columns:
                field_phones = extract_phones_from_contact(row.get(field, ''))
                for p in field_phones:
                    if p not in phones:
                        phones.append(p)

        if not phones:
            no_phone_count += 1
            continue

        # 住所から都道府県抽出
        address = row.get('勤務地', '') or row.get('本社住所', '')
        prefecture, street = extract_prefecture(address)

        # 職種を抽出
        job_title = extract_job_title(row.get('全本文ダンプ', ''))

        # 募集人数を抽出
        num_recruitment = extract_recruitment_number(row.get('全本文ダンプ', ''))

        # 担当者名と役職を抽出（役職フィールドがあれば優先使用）
        contact_name, contact_title = extract_contact_name_with_title(
            row.get('連絡先', ''),
            row.get('代表者', ''),
            row.get('役職', '')  # 元データの役職フィールド
        )

        # 各電話番号ごとにレコードを作成（より多くの電話番号を活用）
        for phone in phones:
            # 固定/携帯を判定
            if is_mobile_phone(phone):
                main_phone = None
                main_mobile = phone
            else:
                main_phone = phone
                main_mobile = None

            # 代表者名を取得（担当者と比較用）
            president_name = row.get('代表者', '')
            if pd.isna(president_name):
                president_name = ''

            records.append({
                'source': 'ミイダス',
                'company_name': row.get('企業名', ''),
                'contact_name': contact_name,
                'contact_title': contact_title,  # 役職情報を追加
                'president_name': president_name,  # 代表者名を追加
                'phone': main_phone,
                'mobile_phone': main_mobile,
                'phone_normalized': phone,
                'prefecture': prefecture,
                'street': street,
                'job_type': job_title,
                'employment_type': None,
                'industry': row.get('企業規模', ''),
                'num_recruitment': num_recruitment,  # 募集人数を追加
                'memo': f"設立: {row.get('設立', 'N/A')}",
                'url': row.get('url', ''),
                'website': row.get('企業サイトURL', ''),
            })

    df_records = pd.DataFrame(records)

    # 電話番号で重複除去
    df_records = df_records.drop_duplicates(subset=['phone_normalized'], keep='first')

    print(f"電話番号なし: {no_phone_count}件")
    print(f"電話番号あり（ユニーク）: {len(df_records)}件")

    return df_records

def load_exclusion_phones():
    """除外対象電話番号を読み込み（成約先＋電話済み）"""
    print("\n=== 除外リスト読み込み ===")

    # 成約先
    df_contract = pd.read_csv('data/output/contract_accounts_20260305_114315.csv',
                              dtype=str, encoding='utf-8')
    contract_phones = set()
    for _, row in df_contract.iterrows():
        normalized = normalize_phone(row.get('Phone', ''))
        if normalized:
            contract_phones.add(normalized)
    print(f"成約先電話番号: {len(contract_phones)}件")

    # 電話済みリスト（全シート）
    xlsx = pd.ExcelFile('C:/Users/fuji1/Downloads/媒体掲載中のリスト.xlsx')
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

    return contract_phones, called_phones

def load_salesforce_phone_index():
    """Salesforceの電話番号インデックスを構築"""
    print("\n=== Salesforce電話番号インデックス構築 ===")

    phone_to_records = {}

    # Lead
    lead_cols = ['Id', 'Company', 'LastName', 'Title', 'Status', 'Phone', 'MobilePhone', 'Phone2__c', 'MobilePhone2__c', 'Description',
                 'Paid_Media__c', 'Paid_JobTitle__c', 'Paid_RecruitmentType__c', 'Paid_EmploymentType__c',
                 'Paid_Industry__c', 'Paid_NumberOfRecruitment__c', 'Paid_Memo__c',
                 'Paid_DataExportDate__c', 'Paid_DataSource__c', 'Paid_URL__c']
    df_lead = pd.read_csv('data/output/Lead_20260305_115825.csv',
                          usecols=lambda c: c in lead_cols, dtype=str, encoding='utf-8')

    lead_phone_cols = ['Phone', 'MobilePhone', 'Phone2__c', 'MobilePhone2__c']
    for _, row in df_lead.iterrows():
        for col in lead_phone_cols:
            if col in row:
                normalized = normalize_phone(row[col])
                if normalized:
                    if normalized not in phone_to_records:
                        phone_to_records[normalized] = []
                    phone_to_records[normalized].append(('Lead', row['Id'], row))
                    break
    print(f"Lead電話番号: {len([k for k, v in phone_to_records.items() if any(x[0]=='Lead' for x in v)])}件")

    # Account
    acc_cols = ['Id', 'Name', 'Phone', 'PersonMobilePhone', 'Phone2__c', 'Description',
                'Paid_Media__c', 'Paid_JobTitle__c', 'Paid_URL__c', 'Paid_DataExportDate__c', 'Paid_DataSource__c']
    df_acc = pd.read_csv('data/output/Account_20260305_115035.csv',
                         usecols=lambda c: c in acc_cols, dtype=str, encoding='utf-8')

    acc_phone_cols = ['Phone', 'PersonMobilePhone', 'Phone2__c']
    for _, row in df_acc.iterrows():
        for col in acc_phone_cols:
            if col in row:
                normalized = normalize_phone(row[col])
                if normalized:
                    if normalized not in phone_to_records:
                        phone_to_records[normalized] = []
                    phone_to_records[normalized].append(('Account', row['Id'], row))
                    break

    # Contact
    con_cols = ['Id', 'AccountId', 'Phone', 'MobilePhone', 'Phone2__c', 'MobilePhone2__c']
    df_con = pd.read_csv('data/output/Contact_20260305_115454.csv',
                         usecols=lambda c: c in con_cols, dtype=str, encoding='utf-8')

    for _, row in df_con.iterrows():
        for col in ['Phone', 'MobilePhone', 'Phone2__c', 'MobilePhone2__c']:
            if col in row:
                normalized = normalize_phone(row[col])
                if normalized:
                    if normalized not in phone_to_records:
                        phone_to_records[normalized] = []
                    phone_to_records[normalized].append(('Contact', row['Id'], row))
                    break

    print(f"ユニーク電話番号総数: {len(phone_to_records)}件")
    return phone_to_records, df_lead, df_acc, df_con

def generate_new_lead_csv(new_leads):
    """新規リード作成用CSV生成"""
    print("\n=== 新規リード作成CSV生成 ===")

    records = []
    for row in new_leads:
        last_name = row.get('contact_name', '')
        if pd.isna(last_name) or not last_name or str(last_name) == 'nan':
            last_name = '担当者'

        # 役職を取得
        title = row.get('contact_title', '')
        if pd.isna(title) or str(title) == 'nan':
            title = ''

        # 代表者名を取得
        president_name = row.get('president_name', '')
        if pd.isna(president_name) or str(president_name) == 'nan':
            president_name = ''

        record = {
            'Company': row.get('company_name', ''),
            'LastName': last_name,
            'Title': title,  # 役職フィールドを追加
            'PresidentName__c': president_name,  # 代表者名を追加
            'Phone': row.get('phone', ''),
            'MobilePhone': row.get('mobile_phone', ''),
            'Prefecture__c': row.get('prefecture', ''),
            'Street': row.get('street', ''),
            'Website': row.get('website', '') if pd.notna(row.get('website')) else '',
            'LeadSource': 'Other',
            'Paid_Media__c': 'ミイダス',
            'Paid_DataSource__c': 'ミイダス',
            'Paid_JobTitle__c': row.get('job_type', ''),
            'Paid_RecruitmentType__c': row.get('job_type', ''),
            'Paid_EmploymentType__c': '',
            'Paid_Industry__c': row.get('industry', '') if pd.notna(row.get('industry')) else '',
            'Paid_NumberOfRecruitment__c': row.get('num_recruitment', '') if pd.notna(row.get('num_recruitment')) else '',
            'Paid_Memo__c': row.get('memo', '') if pd.notna(row.get('memo')) else '',
            'Paid_URL__c': row.get('url', ''),
            'Paid_DataExportDate__c': TODAY,
        }

        record = {k: (v if v != '' else np.nan) for k, v in record.items()}
        records.append(record)

    df = pd.DataFrame(records)
    output_path = OUTPUT_DIR / f"miidas_new_leads_{TIMESTAMP}.csv"
    df.to_csv(output_path, index=False, encoding='utf-8-sig')

    print(f"新規リードCSV: {output_path}")
    print(f"件数: {len(df)}")

    return df, output_path

def generate_update_csv(matched, df_lead, df_acc, df_con):
    """更新用CSV生成"""
    print("\n=== 更新用CSV生成 ===")

    df_matched = pd.DataFrame(matched)

    # Lead更新
    lead_matched = df_matched[df_matched['match_object'] == 'Lead']
    lead_updates = []

    for _, row in lead_matched.iterrows():
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
                update['LastName'] = clean_contact_name(new_name)
            elif is_real_name(existing_name) and is_uncontacted_status(lead_status):
                update['LastName'] = clean_contact_name(new_name)

        # Title更新ロジック（空欄の場合のみ補完）
        new_title = row.get('contact_title', '')
        if pd.notna(new_title) and new_title != '' and str(new_title) != 'nan':
            existing_title = lead_row.get('Title', '')
            if pd.isna(existing_title) or existing_title == '' or str(existing_title) == 'nan':
                update['Title'] = new_title

        # 空欄のみ補完するフィールド
        field_mapping = {
            'Paid_Media__c': 'ミイダス',
            'Paid_DataSource__c': 'ミイダス',
            'Paid_JobTitle__c': row.get('job_type'),
            'Paid_RecruitmentType__c': row.get('job_type'),
            'Paid_Industry__c': row.get('industry'),
            'Paid_NumberOfRecruitment__c': row.get('num_recruitment'),  # 募集人数
            'Paid_URL__c': row.get('url'),
            'Paid_Memo__c': row.get('memo'),
        }

        for sf_field, value in field_mapping.items():
            if pd.notna(value) and value != '' and str(value) != 'nan':
                existing = lead_row.get(sf_field, '')
                if pd.isna(existing) or existing == '' or str(existing) == 'nan':
                    update[sf_field] = value

        update['Paid_DataExportDate__c'] = TODAY

        # Description追記（検索対策ワード＋データ品質フラグ）
        quality_flags = []
        # 携帯電話
        if row.get('mobile_phone') and pd.notna(row.get('mobile_phone')) and str(row.get('mobile_phone')) != 'nan':
            quality_flags.append('携帯電話あり')
        # 固定電話
        if row.get('phone') and pd.notna(row.get('phone')) and str(row.get('phone')) != 'nan':
            quality_flags.append('固定電話あり')
        # 代表者名
        if row.get('president_name') and pd.notna(row.get('president_name')) and str(row.get('president_name')) != 'nan':
            quality_flags.append('代表者名あり')
        # 担当者名（バイネーム）
        contact_name = row.get('contact_name', '')
        if contact_name and pd.notna(contact_name) and str(contact_name) != 'nan' and contact_name not in GENERIC_NAMES:
            quality_flags.append('担当者名あり')
        # 役職
        if row.get('contact_title') and pd.notna(row.get('contact_title')) and str(row.get('contact_title')) != 'nan':
            quality_flags.append('役職あり')
        # 募集人数
        if row.get('num_recruitment') and pd.notna(row.get('num_recruitment')):
            quality_flags.append('募集人数あり')

        quality_text = ' / '.join(quality_flags) if quality_flags else ''
        new_desc = f"""★
[{TODAY} 有料媒体突合]
【検索用】有料媒体 有料求人 ミイダス miidas 求人媒体 {quality_text}
媒体: ミイダス
URL: {row.get('url', 'N/A')}"""

        existing_desc = lead_row.get('Description', '')
        if pd.isna(existing_desc):
            existing_desc = ''
        if 'ミイダス' not in str(existing_desc) and '有料媒体突合' not in str(existing_desc):
            update['Description'] = f"{existing_desc}\n\n{new_desc}".strip()

        lead_updates.append(update)

    df_lead_updates = pd.DataFrame(lead_updates)
    lead_path = OUTPUT_DIR / f"miidas_lead_updates_{TIMESTAMP}.csv"
    df_lead_updates.to_csv(lead_path, index=False, encoding='utf-8-sig')
    print(f"Lead更新CSV: {lead_path} ({len(df_lead_updates)}件)")

    # Account更新
    acc_con_matched = df_matched[df_matched['match_object'].isin(['Account', 'Contact'])]

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
            'Paid_RecruitmentType__c': source_row.get('job_type'),
            'Paid_EmploymentType__c': '',  # ミイダスにはなし
            'Paid_Industry__c': source_row.get('industry'),
            'Paid_NumberOfRecruitment__c': source_row.get('num_recruitment'),
            'Paid_Memo__c': source_row.get('memo'),
            'Paid_URL__c': source_row.get('url'),
        }

        for sf_field, value in field_mapping.items():
            if pd.notna(value) and value != '' and str(value) != 'nan':
                existing = acc_row.get(sf_field, '')
                if pd.isna(existing) or existing == '' or str(existing) == 'nan':
                    update[sf_field] = value

        update['Paid_DataExportDate__c'] = TODAY

        # Description追記（検索対策ワード＋データ品質フラグ）
        quality_flags = []
        if source_row.get('mobile_phone') and pd.notna(source_row.get('mobile_phone')) and str(source_row.get('mobile_phone')) != 'nan':
            quality_flags.append('携帯電話あり')
        if source_row.get('phone') and pd.notna(source_row.get('phone')) and str(source_row.get('phone')) != 'nan':
            quality_flags.append('固定電話あり')
        if source_row.get('president_name') and pd.notna(source_row.get('president_name')) and str(source_row.get('president_name')) != 'nan':
            quality_flags.append('代表者名あり')
        contact_name = source_row.get('contact_name', '')
        if contact_name and pd.notna(contact_name) and str(contact_name) != 'nan' and contact_name not in GENERIC_NAMES:
            quality_flags.append('担当者名あり')
        if source_row.get('contact_title') and pd.notna(source_row.get('contact_title')) and str(source_row.get('contact_title')) != 'nan':
            quality_flags.append('役職あり')
        if source_row.get('num_recruitment') and pd.notna(source_row.get('num_recruitment')):
            quality_flags.append('募集人数あり')

        quality_text = ' / '.join(quality_flags) if quality_flags else ''
        new_desc = f"""★
[{TODAY} 有料媒体突合]
【検索用】有料媒体 有料求人 ミイダス miidas 求人媒体 {quality_text}
媒体: ミイダス
URL: {source_row.get('url', 'N/A')}"""

        existing_desc = acc_row.get('Description', '')
        if pd.isna(existing_desc):
            existing_desc = ''
        if 'ミイダス' not in str(existing_desc) and '有料媒体突合' not in str(existing_desc):
            update['Description'] = f"{existing_desc}\n\n{new_desc}".strip()

        acc_updates.append(update)

    df_acc_updates = pd.DataFrame(acc_updates)
    acc_path = OUTPUT_DIR / f"miidas_account_updates_{TIMESTAMP}.csv"
    df_acc_updates.to_csv(acc_path, index=False, encoding='utf-8-sig')
    print(f"Account更新CSV: {acc_path} ({len(df_acc_updates)}件)")

    return df_lead_updates, df_acc_updates, lead_path, acc_path

def main():
    print("=" * 70)
    print("ミイダス データCSV生成")
    print("=" * 70)

    # データ読み込み
    df_miidas = load_miidas_data()
    contract_phones, called_phones = load_exclusion_phones()
    phone_to_records, df_lead, df_acc, df_con = load_salesforce_phone_index()

    # 突合処理
    print("\n=== 突合処理 ===")
    matched = []
    new_leads = []
    excluded = []

    for _, row in df_miidas.iterrows():
        phone = row['phone_normalized']

        # 除外チェック
        if phone in contract_phones:
            excluded.append({**row.to_dict(), 'reason': '成約先電話番号'})
            continue
        if phone in called_phones:
            excluded.append({**row.to_dict(), 'reason': '電話済み'})
            continue

        # 既存レコードとの突合
        if phone in phone_to_records:
            records = phone_to_records[phone]
            best_match = None
            for obj_type, obj_id, record in records:
                if obj_type == 'Lead' and (best_match is None or best_match[0] != 'Lead'):
                    best_match = (obj_type, obj_id, record)
                elif obj_type == 'Account' and best_match is None:
                    best_match = (obj_type, obj_id, record)
                elif obj_type == 'Contact' and best_match is None:
                    best_match = (obj_type, obj_id, record)

            if best_match:
                matched.append({
                    **row.to_dict(),
                    'match_object': best_match[0],
                    'match_id': best_match[1],
                })
        else:
            new_leads.append(row.to_dict())

    print(f"既存マッチ: {len(matched)}件")
    print(f"新規リード候補: {len(new_leads)}件")
    print(f"除外: {len(excluded)}件")

    # CSV生成
    df_new, new_path = generate_new_lead_csv(new_leads)
    df_lead_upd, df_acc_upd, lead_path, acc_path = generate_update_csv(matched, df_lead, df_acc, df_con)

    # 除外リスト出力
    if excluded:
        df_excluded = pd.DataFrame(excluded)
        exc_path = OUTPUT_DIR / f"miidas_excluded_{TIMESTAMP}.csv"
        df_excluded.to_csv(exc_path, index=False, encoding='utf-8-sig')
        print(f"除外リスト: {exc_path} ({len(df_excluded)}件)")

    print("\n" + "=" * 70)
    print("生成完了")
    print("=" * 70)

    return new_path, lead_path, acc_path

if __name__ == "__main__":
    main()
