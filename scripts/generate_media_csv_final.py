# -*- coding: utf-8 -*-
"""
有料媒体データCSV生成スクリプト（最終版）
- 全Paid_*フィールド対応
- 電話番号の動的マッピング（固定/携帯判定）
- 都道府県分割
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

def normalize_phone(phone):
    """電話番号を正規化（数字のみ10-11桁）"""
    if pd.isna(phone) or phone == '' or str(phone) == 'nan':
        return None
    phone_str = str(phone).strip()
    digits = re.sub(r'\D', '', phone_str)
    if len(digits) >= 10 and len(digits) <= 11:
        return digits
    return None

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
    for pref in PREFECTURES:
        if address.startswith(pref):
            return pref, address[len(pref):].strip()
    return None, address

def extract_number(text):
    """テキストから数値を抽出（採用人数用）"""
    if pd.isna(text) or not text:
        return None
    text = str(text)
    match = re.search(r'(\d+)', text)
    if match:
        return int(match.group(1))
    return None

# 一般名称リスト（これらの場合はバイネームで上書き可能）
GENERIC_NAMES = [
    '担当者', '採用担当', '採用担当者', '採用担当者（名前を聞けたら変更）',
    '人事担当', '人事担当者', '採用係', '採用担当係', '店長', '院長', '事務長',
    '総務担当', '総務担当者', '総務課', '管理者', '責任者', '代表者',
]

# 未接触ステータス（バイネーム→バイネームの更新が許可されるステータス）
UNCONTACTED_STATUSES = [
    '未架電',
    '00 架電OK - 接触なし',
]

def is_uncontacted_status(status):
    """未接触ステータスかどうか判定"""
    if pd.isna(status) or not status:
        return True  # ステータスなしは未接触扱い
    return str(status).strip() in UNCONTACTED_STATUSES

def clean_contact_name(name):
    """担当者名をクリーンアップ（改行でカナを分離、最初の部分のみ返す）"""
    if pd.isna(name) or not name:
        return None
    name = str(name).strip()
    # 改行で分割して最初の部分（名前）のみ取得
    if '\n' in name:
        name = name.split('\n')[0].strip()
    return name if name else None

def is_generic_name(name):
    """一般名称かどうか判定"""
    if pd.isna(name) or not name:
        return True
    name = str(name).strip()
    # 改行があれば最初の部分のみで判定
    if '\n' in name:
        name = name.split('\n')[0].strip()
    # 一般名称を含むかチェック
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
    # 一般名称を含まなければバイネーム
    for generic in GENERIC_NAMES:
        if generic in name:
            return False
    return True

def load_scraping_data_full():
    """スクレイピングデータを全フィールドで読み込み"""
    print("=== スクレイピングデータ読み込み ===")

    # PT・OT・STネット
    df_pt = pd.read_excel('PT・OT・STネット_スクレイピングデータ.xlsx')
    print(f"PT・OT・STネット: {len(df_pt)}件")

    phone_cols = [c for c in df_pt.columns if '電話番号' in c]
    pt_records = []

    for idx, row in df_pt.iterrows():
        # 電話番号を収集（固定/携帯を分類）
        fixed_phones = []
        mobile_phones = []

        for col in phone_cols:
            normalized = normalize_phone(row[col])
            if normalized:
                if is_mobile_phone(normalized):
                    mobile_phones.append(normalized)
                else:
                    fixed_phones.append(normalized)

        # メイン電話番号を決定
        main_phone = fixed_phones[0] if fixed_phones else (mobile_phones[0] if mobile_phones else None)
        main_mobile = mobile_phones[0] if mobile_phones else None

        if main_phone or main_mobile:
            # 都道府県分割
            address = row.get('所在地・勤務地', '')
            prefecture, street = extract_prefecture(address)

            # 採用人数を数値変換
            num_recruit = extract_number(row.get('採用人数', ''))

            # メモ欄に掲載日＋閲覧数＋連絡先を結合
            publication_date = row.get('掲載日', '')
            view_count = row.get('求人情報閲覧数', '')
            contact_info = row.get('連絡先', '')
            memo_parts = []
            if pd.notna(publication_date) and publication_date:
                memo_parts.append(f"掲載日: {publication_date}")
            if pd.notna(view_count) and str(view_count) != 'nan':
                memo_parts.append(f"閲覧数: {view_count}")
            if pd.notna(contact_info) and contact_info:
                memo_parts.append(f"連絡先: {contact_info}")
            memo_text = '\n'.join(memo_parts)

            pt_records.append({
                'source': 'PT・OT・STネット',
                'company_name': row.get('事業所名', ''),
                'contact_name': row.get('担当者', ''),
                'phone': main_phone,
                'mobile_phone': main_mobile,
                'phone_normalized': main_phone or main_mobile,  # 突合用
                'prefecture': prefecture,
                'street': street,
                'job_type': row.get('募集職種', ''),
                'employment_type': row.get('雇用形態', ''),
                'industry': row.get('リハビリ分類', ''),
                'num_recruitment': num_recruit,
                'memo': memo_text,
                'url': row.get('URL', ''),
            })

    # ジョブポスター
    df_jp = pd.read_excel('ジョブポスター.xlsx')
    print(f"ジョブポスター: {len(df_jp)}件")

    jp_records = []
    for idx, row in df_jp.iterrows():
        normalized = normalize_phone(row.get('電話番号', ''))
        if normalized:
            # 固定/携帯判定
            if is_mobile_phone(normalized):
                main_phone = None
                main_mobile = normalized
            else:
                main_phone = normalized
                main_mobile = None

            # 都道府県分割
            address = row.get('勤務地', '')
            prefecture, street = extract_prefecture(address)

            # 会社名（改行前）
            company = str(row.get('会社情報', '')).split('\n')[0].strip()

            jp_records.append({
                'source': 'ジョブポスター',
                'company_name': company,
                'contact_name': row.get('応募担当者名', ''),
                'phone': main_phone,
                'mobile_phone': main_mobile,
                'phone_normalized': normalized,  # 突合用
                'prefecture': prefecture,
                'street': street,
                'job_type': row.get('職 種', ''),
                'employment_type': None,
                'industry': None,
                'num_recruitment': None,
                'memo': row.get('掲載期間', ''),
                'url': row.get('URL', ''),
                'website': row.get('ホームページ', ''),
            })

    # 統合
    all_records = pt_records + jp_records
    df_all = pd.DataFrame(all_records)

    # 電話番号で重複除去
    df_all = df_all.drop_duplicates(subset=['phone_normalized'], keep='first')

    print(f"統合後（電話番号ユニーク）: {len(df_all)}件")
    return df_all

def load_exclusion_phones():
    """除外対象電話番号を読み込み（成約先＋電話済み）"""
    print("\n=== 除外リスト読み込み ===")

    # 成約先
    df_contract = pd.read_csv('data/output/contract_accounts_20260107_125913.csv',
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
    lead_cols = ['Id', 'Company', 'LastName', 'Status', 'Phone', 'MobilePhone', 'Phone2__c', 'MobilePhone2__c', 'Description',
                 'Paid_Media__c', 'Paid_JobTitle__c', 'Paid_RecruitmentType__c', 'Paid_EmploymentType__c',
                 'Paid_Industry__c', 'Paid_NumberOfRecruitment__c', 'Paid_Memo__c',
                 'Paid_DataExportDate__c', 'Paid_DataSource__c', 'Paid_URL__c']
    df_lead = pd.read_csv('data/output/Lead_20260107_003026.csv',
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
    df_acc = pd.read_csv('data/output/Account_20260107_003958.csv',
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
    df_con = pd.read_csv('data/output/Contact_20260107_004329.csv',
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
        # 担当者名が空の場合は「担当者」
        last_name = row.get('contact_name', '')
        if pd.isna(last_name) or not last_name or str(last_name) == 'nan':
            last_name = '担当者'

        record = {
            # 基本情報
            'Company': row.get('company_name', ''),
            'LastName': last_name,
            'Phone': row.get('phone', ''),
            'MobilePhone': row.get('mobile_phone', ''),
            'Prefecture__c': row.get('prefecture', ''),
            'Street': row.get('street', ''),
            'Website': row.get('website', '') if pd.notna(row.get('website')) else '',
            'LeadSource': 'Other',

            # 有料媒体情報
            'Paid_Media__c': row.get('source', ''),
            'Paid_DataSource__c': row.get('source', ''),
            'Paid_JobTitle__c': row.get('job_type', ''),
            'Paid_RecruitmentType__c': row.get('job_type', ''),
            'Paid_EmploymentType__c': row.get('employment_type', '') if pd.notna(row.get('employment_type')) else '',
            'Paid_Industry__c': row.get('industry', '') if pd.notna(row.get('industry')) else '',
            'Paid_NumberOfRecruitment__c': row.get('num_recruitment', ''),
            'Paid_Memo__c': row.get('memo', '') if pd.notna(row.get('memo')) else '',
            'Paid_URL__c': row.get('url', ''),
            'Paid_DataExportDate__c': TODAY,
        }

        # 空文字列をNaNに変換（CSVで空欄にするため）
        record = {k: (v if v != '' else np.nan) for k, v in record.items()}
        records.append(record)

    df = pd.DataFrame(records)

    # NaN列を削除せず保持
    output_path = OUTPUT_DIR / f"new_leads_final_{TIMESTAMP}.csv"
    df.to_csv(output_path, index=False, encoding='utf-8-sig')

    print(f"新規リードCSV: {output_path}")
    print(f"件数: {len(df)}")
    print(f"列: {list(df.columns)}")

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
        # - 一般名称 → バイネーム: すべてのステータスで更新OK
        # - バイネーム → バイネーム: 未接触ステータスの場合のみ更新
        existing_name = lead_row.get('LastName', '')
        new_name = row.get('contact_name', '')
        lead_status = lead_row.get('Status', '')

        if is_real_name(new_name):
            if is_generic_name(existing_name):
                # 一般名称 → バイネーム: すべてのステータスで更新
                update['LastName'] = clean_contact_name(new_name)
            elif is_real_name(existing_name) and is_uncontacted_status(lead_status):
                # バイネーム → バイネーム: 未接触の場合のみ更新
                update['LastName'] = clean_contact_name(new_name)

        # 空欄のみ補完するフィールド
        field_mapping = {
            'Paid_Media__c': row.get('source'),
            'Paid_DataSource__c': row.get('source'),
            'Paid_JobTitle__c': row.get('job_type'),
            'Paid_RecruitmentType__c': row.get('job_type'),
            'Paid_EmploymentType__c': row.get('employment_type'),
            'Paid_Industry__c': row.get('industry'),
            'Paid_URL__c': row.get('url'),
            'Paid_Memo__c': row.get('memo'),
        }

        for sf_field, value in field_mapping.items():
            if pd.notna(value) and value != '' and str(value) != 'nan':
                existing = lead_row.get(sf_field, '')
                if pd.isna(existing) or existing == '' or str(existing) == 'nan':
                    update[sf_field] = value

        # 採用人数（数値）
        if row.get('num_recruitment') and pd.notna(row.get('num_recruitment')):
            existing = lead_row.get('Paid_NumberOfRecruitment__c', '')
            if pd.isna(existing) or existing == '' or str(existing) == 'nan':
                update['Paid_NumberOfRecruitment__c'] = row['num_recruitment']

        # 常に更新
        update['Paid_DataExportDate__c'] = TODAY

        # Description追記（検索対策ワード＋データ品質フラグ）
        media_name = row['source']
        media_keyword = 'ptotst' if 'PT' in media_name else 'jobposter ジョブポスター'

        quality_flags = []
        # 携帯電話
        if row.get('mobile_phone') and pd.notna(row.get('mobile_phone')) and str(row.get('mobile_phone')) != 'nan':
            quality_flags.append('携帯電話あり')
        # 固定電話
        if row.get('phone') and pd.notna(row.get('phone')) and str(row.get('phone')) != 'nan':
            quality_flags.append('固定電話あり')
        # 担当者名（バイネーム）
        contact_name = row.get('contact_name', '')
        if contact_name and pd.notna(contact_name) and str(contact_name) != 'nan' and contact_name != '担当者':
            quality_flags.append('担当者名あり')
        # 募集人数
        if row.get('num_recruitment') and pd.notna(row.get('num_recruitment')):
            quality_flags.append('募集人数あり')
        # 雇用形態
        if row.get('employment_type') and pd.notna(row.get('employment_type')):
            quality_flags.append('雇用形態あり')

        quality_text = ' / '.join(quality_flags) if quality_flags else ''
        new_desc = f"""★
[{TODAY} 有料媒体突合]
【検索用】有料媒体 有料求人 {media_name} {media_keyword} 求人媒体 {quality_text}
媒体: {media_name}
URL: {row.get('url', 'N/A')}"""

        existing_desc = lead_row.get('Description', '')
        if pd.isna(existing_desc):
            existing_desc = ''
        if media_name not in str(existing_desc) and '有料媒体突合' not in str(existing_desc):
            update['Description'] = f"{existing_desc}\n\n{new_desc}".strip()

        lead_updates.append(update)

    df_lead_updates = pd.DataFrame(lead_updates)
    lead_path = OUTPUT_DIR / f"lead_updates_final_{TIMESTAMP}.csv"
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
            'Paid_Media__c': source_row.get('source'),
            'Paid_DataSource__c': source_row.get('source'),
            'Paid_JobTitle__c': source_row.get('job_type'),
            'Paid_RecruitmentType__c': source_row.get('job_type'),
            'Paid_EmploymentType__c': source_row.get('employment_type'),
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
        media_name = source_row['source']
        media_keyword = 'ptotst' if 'PT' in media_name else 'jobposter ジョブポスター'

        quality_flags = []
        if source_row.get('mobile_phone') and pd.notna(source_row.get('mobile_phone')) and str(source_row.get('mobile_phone')) != 'nan':
            quality_flags.append('携帯電話あり')
        if source_row.get('phone') and pd.notna(source_row.get('phone')) and str(source_row.get('phone')) != 'nan':
            quality_flags.append('固定電話あり')
        contact_name = source_row.get('contact_name', '')
        if contact_name and pd.notna(contact_name) and str(contact_name) != 'nan' and contact_name != '担当者':
            quality_flags.append('担当者名あり')
        if source_row.get('num_recruitment') and pd.notna(source_row.get('num_recruitment')):
            quality_flags.append('募集人数あり')
        if source_row.get('employment_type') and pd.notna(source_row.get('employment_type')):
            quality_flags.append('雇用形態あり')

        quality_text = ' / '.join(quality_flags) if quality_flags else ''
        new_desc = f"""★
[{TODAY} 有料媒体突合]
【検索用】有料媒体 有料求人 {media_name} {media_keyword} 求人媒体 {quality_text}
媒体: {media_name}
URL: {source_row.get('url', 'N/A')}"""

        existing_desc = acc_row.get('Description', '')
        if pd.isna(existing_desc):
            existing_desc = ''
        if media_name not in str(existing_desc) and '有料媒体突合' not in str(existing_desc):
            update['Description'] = f"{existing_desc}\n\n{new_desc}".strip()

        acc_updates.append(update)

    df_acc_updates = pd.DataFrame(acc_updates)
    acc_path = OUTPUT_DIR / f"account_updates_final_{TIMESTAMP}.csv"
    df_acc_updates.to_csv(acc_path, index=False, encoding='utf-8-sig')
    print(f"Account更新CSV: {acc_path} ({len(df_acc_updates)}件)")

    return df_lead_updates, df_acc_updates, lead_path, acc_path

def main():
    print("=" * 70)
    print("有料媒体データCSV生成（最終版）")
    print("=" * 70)

    # データ読み込み
    df_scraping = load_scraping_data_full()
    contract_phones, called_phones = load_exclusion_phones()
    phone_to_records, df_lead, df_acc, df_con = load_salesforce_phone_index()

    # 突合処理
    print("\n=== 突合処理 ===")
    matched = []
    new_leads = []
    excluded = []

    for _, row in df_scraping.iterrows():
        phone = row['phone_normalized']

        # 除外チェック
        if phone in contract_phones:
            excluded.append({**row.to_dict(), 'reason': '成約先'})
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
        exc_path = OUTPUT_DIR / f"excluded_final_{TIMESTAMP}.csv"
        df_excluded.to_csv(exc_path, index=False, encoding='utf-8-sig')
        print(f"除外リスト: {exc_path} ({len(df_excluded)}件)")

    print("\n" + "=" * 70)
    print("生成完了")
    print("=" * 70)

    return new_path, lead_path, acc_path

if __name__ == "__main__":
    main()
