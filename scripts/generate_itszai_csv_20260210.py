# -*- coding: utf-8 -*-
"""
itszai データ → Salesforce突合・CSV生成スクリプト (2026-02-10)
- Excelファイルから読み込み
- 成約先・電話済み除外
- Salesforce既存データ（Lead/Account/Contact）と電話番号突合
- 新規リード・更新用CSV生成
"""

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
INPUT_PATH = Path(r"C:\Users\fuji1\Downloads\itszai_抽出結果.xlsx")
CONTRACT_PATH = SF_DATA_DIR / "contract_accounts_20260210_094412.csv"
CALLED_LIST_PATH = Path(r"C:\Users\fuji1\Downloads\媒体掲載中のリスト.xlsx")
LEAD_PATH = SF_DATA_DIR / "Lead_20260210.csv"
ACCOUNT_PATH = SF_DATA_DIR / "Account_20260210_094201.csv"
CONTACT_PATH = SF_DATA_DIR / "Contact_20260210_094224.csv"

TODAY = datetime.now().strftime('%Y-%m-%d')
TIMESTAMP = datetime.now().strftime('%Y%m%d_%H%M%S')

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
    '担当者', '採用担当', '採用担当者', '人事担当', '人事担当者',
    '採用係', '店長', '院長', '事務長', '総務担当', '総務担当者',
    '総務課', '管理者', '責任者', '代表者',
]

UNCONTACTED_STATUSES = ['未架電', '00 架電OK - 接触なし']

# 会社名として不正なパターン（求人タイトル等）
INVALID_COMPANY_PATTERNS = ['求人', '正社員', '年休', '月給', '未経験', '募集', '施工管理技士', '急募']


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


def is_generic_name(name):
    if pd.isna(name) or not name:
        return True
    name = str(name).strip()
    for generic in GENERIC_NAMES:
        if generic in name:
            return True
    return name == ''


def is_real_name(name):
    if pd.isna(name) or not name:
        return False
    name = str(name).strip()
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


def is_invalid_company(name):
    """会社名が求人タイトル等の不正値かチェック"""
    if not name or pd.isna(name):
        return True
    name = str(name)
    for pattern in INVALID_COMPANY_PATTERNS:
        if pattern in name:
            return True
    return False


# ========================================
# STEP 1: itszaiデータ読み込み
# ========================================
def load_itszai_data():
    print("=" * 70)
    print("[STEP 1] itszaiデータ読み込み")
    print("=" * 70)

    df = pd.read_excel(INPUT_PATH, sheet_name='全件（627件）', dtype=str)
    print(f"読み込み行数: {len(df)}")

    records = []
    skipped_no_phone = 0
    skipped_invalid_company = 0
    seen_phones = set()

    for _, row in df.iterrows():
        company = str(row.get('会社名', '')).strip()

        # 会社名チェック
        if is_invalid_company(company):
            skipped_invalid_company += 1
            continue

        # 電話番号正規化
        phone_raw = row.get('電話番号', '')
        phone_normalized = normalize_phone(phone_raw)
        if not phone_normalized:
            skipped_no_phone += 1
            continue

        # 電話番号重複チェック
        if phone_normalized in seen_phones:
            continue
        seen_phones.add(phone_normalized)

        # 固定/携帯分類
        if is_mobile_phone(phone_normalized):
            phone_field = phone_normalized  # 携帯のみでもPhoneに設定（必須フィールド）
            mobile_field = phone_normalized
        else:
            phone_field = phone_normalized
            mobile_field = None

        # 担当者名
        contact_name = row.get('担当者名', '')
        if pd.isna(contact_name) or not str(contact_name).strip() or str(contact_name) == 'nan':
            contact_name = '担当者'

        # 代表者名
        president_name = row.get('代表者名', '')
        if pd.isna(president_name) or str(president_name) == 'nan':
            president_name = ''

        # メールアドレス
        email = row.get('メールアドレス', '')
        if pd.isna(email) or str(email) == 'nan':
            email = ''

        records.append({
            'source': 'itszai',
            'company_name': company,
            'contact_name': str(contact_name).strip(),
            'president_name': str(president_name).strip() if president_name else '',
            'phone': phone_field,
            'mobile_phone': mobile_field,
            'phone_normalized': phone_normalized,
            'email': email,
            'url': row.get('URL', ''),
        })

    df_records = pd.DataFrame(records)
    print(f"不正会社名スキップ: {skipped_invalid_company}件")
    print(f"電話番号なしスキップ: {skipped_no_phone}件")
    print(f"ユニーク電話番号レコード: {len(df_records)}件")

    # 携帯/固定内訳
    mobile_count = sum(1 for r in records if r['mobile_phone'])
    landline_only = sum(1 for r in records if not r['mobile_phone'])
    print(f"  固定電話のみ: {landline_only}件")
    print(f"  携帯電話あり: {mobile_count}件")

    return df_records


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
                 'LeadSourceMemo__c']
    df_lead = pd.read_csv(LEAD_PATH, usecols=lambda c: c in lead_cols,
                           dtype=str, encoding='utf-8')
    print(f"Lead読み込み: {len(df_lead)}件")

    for _, row in df_lead.iterrows():
        for col in ['Phone', 'MobilePhone', 'Phone2__c', 'MobilePhone2__c']:
            if col in row.index:
                normalized = normalize_phone(row[col])
                if normalized:
                    if normalized not in phone_to_records:
                        phone_to_records[normalized] = []
                    phone_to_records[normalized].append(('Lead', row['Id'], row))
                    break

    # Account
    df_acc = pd.read_csv(ACCOUNT_PATH, usecols=lambda c: c in ['Id', 'Name', 'Phone', 'Phone2__c'],
                          dtype=str, encoding='utf-8')
    print(f"Account読み込み: {len(df_acc)}件")

    for _, row in df_acc.iterrows():
        for col in ['Phone', 'Phone2__c']:
            if col in row.index:
                normalized = normalize_phone(row[col])
                if normalized:
                    if normalized not in phone_to_records:
                        phone_to_records[normalized] = []
                    phone_to_records[normalized].append(('Account', row['Id'], row))
                    break

    # Contact
    df_con = pd.read_csv(CONTACT_PATH, usecols=lambda c: c in ['Id', 'AccountId', 'Phone', 'MobilePhone', 'Phone2__c', 'MobilePhone2__c'],
                          dtype=str, encoding='utf-8')
    print(f"Contact読み込み: {len(df_con)}件")

    for _, row in df_con.iterrows():
        for col in ['Phone', 'MobilePhone', 'Phone2__c', 'MobilePhone2__c']:
            if col in row.index:
                normalized = normalize_phone(row[col])
                if normalized:
                    if normalized not in phone_to_records:
                        phone_to_records[normalized] = []
                    phone_to_records[normalized].append(('Contact', row['Id'], row))
                    break

    print(f"ユニーク電話番号総数: {len(phone_to_records)}件")
    return phone_to_records, df_lead, df_acc, df_con


# ========================================
# STEP 4: 突合処理
# ========================================
def match_records(df_data, contract_phones, called_phones, phone_to_records):
    print("\n" + "=" * 70)
    print("[STEP 4] 突合処理")
    print("=" * 70)

    matched = []
    new_leads = []
    excluded_contract = []
    excluded_called = []

    for _, row in df_data.iterrows():
        phone = row['phone_normalized']

        if phone in contract_phones:
            excluded_contract.append({**row.to_dict(), 'reason': '成約先電話番号'})
            continue
        if phone in called_phones:
            excluded_called.append({**row.to_dict(), 'reason': '電話済み'})
            continue

        if phone in phone_to_records:
            records = phone_to_records[phone]
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
# STEP 5: CSV生成
# ========================================
def generate_new_lead_csv(new_leads):
    print("\n" + "=" * 70)
    print("[STEP 5-1] 新規リード作成CSV生成")
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
            continue

        last_name = row.get('contact_name', '') or '担当者'
        if str(last_name) == 'nan':
            last_name = '担当者'

        record = {
            'Company': company,
            'LastName': last_name,
            'Phone': phone,
            'MobilePhone': row.get('mobile_phone', '') or '',
            'Email': row.get('email', '') or '',
            'Website': '',
            'LeadSource': 'Other',
            'Paid_Media__c': 'itszai',
            'Paid_DataSource__c': 'itszai',
            'Paid_JobTitle__c': '',
            'Paid_RecruitmentType__c': '',
            'Paid_EmploymentType__c': '',
            'Paid_Industry__c': '',
            'Paid_NumberOfRecruitment__c': '',
            'Paid_Memo__c': '',
            'Paid_URL__c': row.get('url', '') or '',
            'Paid_DataExportDate__c': TODAY,
        }

        record = {k: (v if v != '' else np.nan) for k, v in record.items()}
        records.append(record)

    if skipped_no_company > 0:
        print(f"Companyなしスキップ: {skipped_no_company}件")
    if skipped_no_phone > 0:
        print(f"Phoneなしスキップ: {skipped_no_phone}件")

    df = pd.DataFrame(records)
    # 電話番号の先頭0保護
    for col in ['Phone', 'MobilePhone']:
        if col in df.columns:
            df[col] = df[col].astype(str).replace('nan', '')
            def fix_phone(v):
                if not v or v == 'nan' or v == '':
                    return ''
                digits = re.sub(r'\D', '', v)
                if len(digits) == 9:
                    return '0' + digits
                elif len(digits) >= 10:
                    return digits
                return v
            df[col] = df[col].apply(fix_phone)
            df[col] = df[col].replace('', np.nan)

    output_path = OUTPUT_DIR / f"itszai_new_leads_{TIMESTAMP}.csv"
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"新規リードCSV: {output_path}")
    print(f"件数: {len(df)}")

    return df, output_path


def generate_update_csv(matched, df_lead, df_acc, df_con):
    print("\n" + "=" * 70)
    print("[STEP 5-2] 更新用CSV生成")
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

        # LastName更新
        existing_name = lead_row.get('LastName', '')
        new_name = row.get('contact_name', '')
        lead_status = lead_row.get('Status', '')
        if is_real_name(new_name):
            if is_generic_name(existing_name):
                update['LastName'] = str(new_name).strip()
            elif is_real_name(existing_name) and is_uncontacted_status(lead_status):
                update['LastName'] = str(new_name).strip()

        # Paid_*フィールド
        update['Paid_Media__c'] = 'itszai'
        update['Paid_DataSource__c'] = 'itszai'
        update['Paid_DataExportDate__c'] = TODAY
        if row.get('url') and str(row.get('url')) != 'nan':
            update['Paid_URL__c'] = row['url']

        # LeadSourceMemo__c
        existing_memo = lead_row.get('LeadSourceMemo__c', '')
        if pd.isna(existing_memo):
            existing_memo = ''
        batch_tag = f"【既存更新】有料媒体突合 {TODAY} itszai"
        if batch_tag not in str(existing_memo):
            new_memo = f"{existing_memo}\n{batch_tag}".strip() if existing_memo else batch_tag
            update['LeadSourceMemo__c'] = new_memo

        if len(update) > 1:
            lead_updates.append(update)

    df_lead_updates = pd.DataFrame(lead_updates)
    lead_path = OUTPUT_DIR / f"itszai_lead_updates_{TIMESTAMP}.csv"
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

        update = {
            'Id': acc_id,
            'Paid_Media__c': 'itszai',
            'Paid_DataSource__c': 'itszai',
            'Paid_DataExportDate__c': TODAY,
        }
        if row.get('url') and str(row.get('url')) != 'nan':
            update['Paid_URL__c'] = row['url']
        acc_updates.append(update)

    df_acc_updates = pd.DataFrame(acc_updates)
    acc_path = OUTPUT_DIR / f"itszai_account_updates_{TIMESTAMP}.csv"
    df_acc_updates.to_csv(acc_path, index=False, encoding='utf-8-sig')
    print(f"Account更新CSV: {acc_path} ({len(df_acc_updates)}件)")

    return df_lead_updates, df_acc_updates, lead_path, acc_path


# ========================================
# メイン
# ========================================
def main():
    print("=" * 70)
    print(f"itszai データCSV生成 ({TODAY})")
    print("=" * 70)

    df_data = load_itszai_data()
    contract_phones, called_phones = load_exclusion_phones()
    phone_to_records, df_lead, df_acc, df_con = load_salesforce_phone_index()

    matched, new_leads, excluded = match_records(
        df_data, contract_phones, called_phones, phone_to_records)

    df_new, new_path = generate_new_lead_csv(new_leads)
    df_lead_upd, df_acc_upd, lead_path, acc_path = generate_update_csv(
        matched, df_lead, df_acc, df_con)

    # 除外リスト
    if excluded:
        df_exc = pd.DataFrame(excluded)
        exc_path = OUTPUT_DIR / f"itszai_excluded_{TIMESTAMP}.csv"
        df_exc.to_csv(exc_path, index=False, encoding='utf-8-sig')
        print(f"除外リスト: {exc_path} ({len(df_exc)}件)")

    # サマリー
    print("\n" + "=" * 70)
    print("処理完了サマリー")
    print("=" * 70)
    print(f"入力: {len(df_data)}件")
    print(f"除外: {len(excluded)}件")
    print(f"  - 成約先: {sum(1 for e in excluded if e['reason']=='成約先電話番号')}件")
    print(f"  - 電話済み: {sum(1 for e in excluded if e['reason']=='電話済み')}件")
    print(f"既存マッチ: {len(matched)}件")
    print(f"新規リード: {len(df_new)}件")
    print(f"Lead更新: {len(df_lead_upd)}件")
    print(f"Account更新: {len(df_acc_upd)}件")
    print(f"\n出力ファイル:")
    print(f"  新規リード: {new_path}")
    print(f"  Lead更新: {lead_path}")
    print(f"  Account更新: {acc_path}")


if __name__ == "__main__":
    main()
