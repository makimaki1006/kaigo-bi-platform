# -*- coding: utf-8 -*-
"""
看護roo・ナース専科 データCSV生成スクリプト

処理内容:
1. 両媒体のExcelを読み込み
2. 電話番号正規化
3. 成約先・電話済み除外
4. Salesforce突合
5. 新規リード/更新CSVを生成
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

# ファイルパス
KANGO_FILE = r'C:\Users\fuji1\OneDrive\デスクトップ\pythonスクリプト置き場\final_kango_with_google_v2.xlsx'
NURSE_FILE = r'C:\Users\fuji1\OneDrive\デスクトップ\pythonスクリプト置き場\final_fallback_nursejinzaibank_final_structured_v3.xlsx'

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
    # .0を除去（float変換対策）
    if phone_str.endswith('.0'):
        phone_str = phone_str[:-2]
    digits = re.sub(r'\D', '', phone_str)
    # 先頭0を補完
    if len(digits) == 10 and not digits.startswith('0'):
        digits = '0' + digits
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

    # 都道府県を抽出
    pref = None
    for p in PREFECTURES:
        if address.startswith(p):
            pref = p
            address = address[len(p):]
            break

    if not pref:
        return None, None

    # 市区町村を抽出（正規表現）
    # パターン: 市、区（東京23区）、郡+町/村
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
    # 全角→半角、空白除去
    name = name.replace('　', '').replace(' ', '')
    # 法人格を除去
    for prefix in ['医療法人社団', '医療法人財団', '医療法人', '社会福祉法人',
                   '株式会社', '有限会社', '合同会社', '一般社団法人', '公益社団法人']:
        name = name.replace(prefix, '')
    return name


def is_similar_name(name1, name2, threshold=0.85):
    """名前が類似しているか判定（編集距離ベース）"""
    if not name1 or not name2:
        return False

    n1 = normalize_company_name(name1)
    n2 = normalize_company_name(name2)

    if not n1 or not n2:
        return False

    # 完全一致
    if n1 == n2:
        return True

    # 片方がもう片方に含まれる（6文字以上、かつ含まれる方が5文字以上）
    if len(n1) >= 5 and len(n2) >= 5:
        shorter = n1 if len(n1) <= len(n2) else n2
        longer = n2 if len(n1) <= len(n2) else n1
        if shorter in longer and len(shorter) >= 5:
            return True

    # 短い方の名前が5文字以下の場合は完全一致のみ
    if len(n1) <= 5 or len(n2) <= 5:
        return n1 == n2

    # Jaccard類似度（文字単位）- 閾値を高めに
    set1 = set(n1)
    set2 = set(n2)
    intersection = len(set1 & set2)
    union = len(set1 | set2)
    similarity = intersection / union if union > 0 else 0

    return similarity >= threshold


def clean_facility_type(facility_type):
    """施設形態をクリーンアップ（改行を「/」に置換）"""
    if pd.isna(facility_type) or not facility_type:
        return ''
    val = str(facility_type).strip()
    if val.lower() == 'nan':
        return ''
    return val.replace('\n', ' / ').strip()


def is_valid_value(val):
    """有効な値かどうか判定（nan, None, 空文字を除外）"""
    if pd.isna(val) or val is None:
        return False
    str_val = str(val).strip()
    if str_val == '' or str_val.lower() == 'nan':
        return False
    return True


def clean_job_info(job_text):
    """募集内容をクリーンアップ（最初の募集情報のみ抽出）"""
    if pd.isna(job_text) or not job_text:
        return ''
    text = str(job_text).replace('この施設の求人一覧', '').strip()
    # 最初の募集情報のみ（改行で分割して最初の2-3行）
    lines = [l.strip() for l in text.split('\n') if l.strip()]
    if len(lines) >= 2:
        return f"{lines[0]} / {lines[1]}"
    elif len(lines) == 1:
        return lines[0]
    return ''


def clean_diagnosis(diagnosis_text):
    """診療科目をクリーンアップ（改行・空白除去）"""
    if pd.isna(diagnosis_text) or not diagnosis_text:
        return ''
    text = str(diagnosis_text).strip()
    # 改行を除去して、カンマ区切りに
    text = re.sub(r'\s+', '', text)
    return text


def load_kango_data():
    """看護rooデータを読み込み"""
    print("=== 看護roo データ読み込み ===")
    df = pd.read_excel(KANGO_FILE, dtype=str)
    print(f"  読み込み件数: {len(df)}")

    records = []
    skipped_no_name = 0
    for _, row in df.iterrows():
        # 電話番号を取得（phone_cleaned優先、なければgoogle_phone）
        phone = normalize_phone(row.get('phone_cleaned', ''))
        if not phone:
            phone = normalize_phone(row.get('google_phone', ''))

        if not phone:
            continue

        # 施設名（Company）が空の場合はスキップ
        company_name = row.get('shisetsu_name', '')
        if not is_valid_value(company_name):
            skipped_no_name += 1
            continue

        # 住所（font-xs列）
        address = row.get('font-xs', '') or ''
        # NEWなどの場合は住所ではない
        if address in ['NEW', '']:
            address = ''
            pref, street = None, None
        else:
            pref, street = extract_prefecture(address)

        # 施設形態をクリーンアップ
        facility_type = clean_facility_type(row.get('施設形態', ''))

        # 診療科目
        diagnosis = clean_diagnosis(row.get('診療科目', ''))

        # 設立年
        establishment = row.get('設立', '') or ''

        # 更新日
        update_date = row.get('datetime', '') or ''
        update_date = update_date.replace('更新', '')

        # 募集内容
        job_info = clean_job_info(row.get('Unnamed: 9', ''))

        # メモ欄（is_valid_valueで空・nan判定）
        memo_parts = [f"【看護roo】"]
        if is_valid_value(address) and address not in ['NEW']:
            memo_parts.append(f"住所: {address}")
        if is_valid_value(facility_type):
            memo_parts.append(f"施設形態: {facility_type}")
        if is_valid_value(establishment):
            memo_parts.append(f"設立: {establishment}")
        if is_valid_value(diagnosis):
            memo_parts.append(f"診療科目: {diagnosis}")
        if is_valid_value(update_date):
            memo_parts.append(f"更新日: {update_date}")
        if is_valid_value(job_info):
            memo_parts.append(f"募集: {job_info}")
        memo_parts.append(f"取得日: {TODAY}")

        records.append({
            'source': '看護roo',
            'company_name': company_name,
            'phone_normalized': phone,
            'phone': phone if not is_mobile_phone(phone) else None,
            'mobile_phone': phone if is_mobile_phone(phone) else None,
            'prefecture': pref,
            'street': street,
            'facility_type': facility_type,
            'memo': '\n'.join(memo_parts),
        })

    print(f"  電話番号あり: {len(records)}件")
    if skipped_no_name > 0:
        print(f"  ※施設名空でスキップ: {skipped_no_name}件")
    return records


def extract_headcount(headcount_raw):
    """募集人数から数字のみを抽出"""
    if pd.isna(headcount_raw) or not headcount_raw:
        return ''
    text = str(headcount_raw).strip()
    # 数字のみ抽出
    match = re.search(r'(\d+)', text)
    if match:
        return match.group(1)
    return ''


def load_nurse_data():
    """ナース専科データを読み込み"""
    print("\n=== ナース専科 データ読み込み ===")
    df = pd.read_excel(NURSE_FILE, dtype=str)
    print(f"  読み込み件数: {len(df)}")

    records = []
    skipped_no_name = 0
    for _, row in df.iterrows():
        phone = normalize_phone(row.get('phone_cleaned', ''))
        if not phone:
            continue

        # 名称（Company）が空の場合はスキップ
        company_name = row.get('名称', '')
        if not is_valid_value(company_name):
            skipped_no_name += 1
            continue

        # 住所から都道府県を抽出
        address = row.get('所在地', '')
        prefecture, street = extract_prefecture(address)

        # 施設形態をクリーンアップ
        facility_type = clean_facility_type(row.get('施設形態', ''))

        # 各フィールド
        job_type = row.get('募集職種', '') or ''
        headcount = extract_headcount(row.get('headcount_raw', ''))
        employment_type = row.get('雇用形態', '') or ''
        assignment = row.get('配属先', '') or ''
        if assignment == '-':
            assignment = ''
        diagnosis = clean_diagnosis(row.get('診療科目', ''))
        valid_until = row.get('求人有効期限日', '') or ''

        # メモ欄（is_valid_valueで空・nan判定）
        memo_parts = [f"【ナース専科】"]
        if is_valid_value(facility_type):
            memo_parts.append(f"施設形態: {facility_type}")
        if is_valid_value(job_type):
            memo_parts.append(f"募集職種: {job_type}")
        if is_valid_value(headcount):
            memo_parts.append(f"募集人数: {headcount}")
        if is_valid_value(employment_type):
            memo_parts.append(f"雇用形態: {employment_type}")
        if is_valid_value(assignment):
            memo_parts.append(f"配属先: {assignment}")
        if is_valid_value(diagnosis):
            memo_parts.append(f"診療科目: {diagnosis}")
        if is_valid_value(valid_until):
            memo_parts.append(f"求人有効期限: {valid_until}")
        memo_parts.append(f"取得日: {TODAY}")

        records.append({
            'source': 'ナース専科',
            'company_name': company_name,
            'phone_normalized': phone,
            'phone': phone if not is_mobile_phone(phone) else None,
            'mobile_phone': phone if is_mobile_phone(phone) else None,
            'prefecture': prefecture,
            'street': street,
            'facility_type': facility_type,
            'memo': '\n'.join(memo_parts),
        })

    print(f"  電話番号あり: {len(records)}件")
    if skipped_no_name > 0:
        print(f"  ※名称空でスキップ: {skipped_no_name}件")
    return records


def load_exclusion_phones():
    """除外対象電話番号を読み込み（成約先＋電話済み）"""
    print("\n=== 除外リスト読み込み ===")

    # 成約先（最新ファイルを探す）
    contract_files = sorted(Path('data/output').glob('contract_accounts_*.csv'), reverse=True)
    contract_phones = set()

    if contract_files:
        df_contract = pd.read_csv(contract_files[0], dtype=str, encoding='utf-8')
        for _, row in df_contract.iterrows():
            normalized = normalize_phone(row.get('Phone', ''))
            if normalized:
                contract_phones.add(normalized)
        print(f"  成約先電話番号: {len(contract_phones)}件 ({contract_files[0].name})")
    else:
        print("  成約先ファイルなし → エクスポートが必要")

    # 電話済みリスト（全シート）
    called_phones = set()
    called_list_path = Path('C:/Users/fuji1/Downloads/媒体掲載中のリスト.xlsx')

    if called_list_path.exists():
        xlsx = pd.ExcelFile(called_list_path)
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
        print(f"  電話済み電話番号: {len(called_phones)}件")
    else:
        print(f"  電話済みリストなし: {called_list_path}")

    return contract_phones, called_phones


def load_contract_company_names():
    """成約先の会社名を読み込み（電話番号が異なる成約先を検出するため）"""
    contract_files = sorted(Path('data/output').glob('contract_accounts_*.csv'), reverse=True)
    company_names = set()

    if contract_files:
        df = pd.read_csv(contract_files[0], dtype=str, encoding='utf-8')
        for _, row in df.iterrows():
            name = row.get('Name', '')
            if pd.notna(name) and name:
                # 正規化: 全角半角統一、空白除去
                name = str(name).strip()
                company_names.add(name)
        print(f"  成約先会社名: {len(company_names)}件")

    return company_names


def load_contract_location_index():
    """成約先の住所+名前インデックスを構築（都道府県+市区町村 → 名前リスト）"""
    contract_files = sorted(Path('data/output').glob('contract_accounts_*.csv'), reverse=True)
    location_index = {}  # {(都道府県, 市区町村): [(名前, 元の名前), ...]}

    if contract_files:
        df = pd.read_csv(contract_files[0], dtype=str, encoding='utf-8')
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


def load_account_location_index():
    """取引先の住所+名前インデックスを構築（都道府県+市区町村 → Account情報リスト）"""
    acc_files = sorted(Path('data/output').glob('Account_*.csv'), reverse=True)
    location_index = {}  # {(都道府県, 市区町村): [(名前normalized, Id, row), ...]}

    if acc_files:
        # Address__c列を追加で読み込み
        acc_cols = ['Id', 'Name', 'Phone', 'PersonMobilePhone', 'Phone2__c', 'Description', 'Address__c']
        df = pd.read_csv(acc_files[0], usecols=lambda c: c in acc_cols, dtype=str, encoding='utf-8')
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
                location_index[key].append((
                    normalize_company_name(name),
                    str(name).strip(),
                    row['Id'],
                    row
                ))
                valid_count += 1

        print(f"  取引先住所インデックス: {len(location_index)}地域, {valid_count}件")

    return location_index


def load_salesforce_phone_index():
    """Salesforceの電話番号インデックスを構築"""
    print("\n=== Salesforce電話番号インデックス構築 ===")

    phone_to_records = {}

    # 最新のエクスポートファイルを探す
    lead_files = sorted(Path('data/output').glob('Lead_*.csv'), reverse=True)
    acc_files = sorted(Path('data/output').glob('Account_*.csv'), reverse=True)

    # Lead
    if lead_files:
        lead_cols = ['Id', 'Company', 'LastName', 'Status', 'Phone', 'MobilePhone', 'Phone2__c', 'MobilePhone2__c', 'Description']
        df_lead = pd.read_csv(lead_files[0], usecols=lambda c: c in lead_cols, dtype=str, encoding='utf-8')

        for _, row in df_lead.iterrows():
            for col in ['Phone', 'MobilePhone', 'Phone2__c', 'MobilePhone2__c']:
                if col in row:
                    normalized = normalize_phone(row[col])
                    if normalized:
                        if normalized not in phone_to_records:
                            phone_to_records[normalized] = []
                        phone_to_records[normalized].append(('Lead', row['Id'], row))
                        break
        print(f"  Lead電話番号: {len([k for k, v in phone_to_records.items() if any(x[0]=='Lead' for x in v)])}件")
    else:
        print("  Leadファイルなし")
        df_lead = pd.DataFrame()

    # Account
    if acc_files:
        acc_cols = ['Id', 'Name', 'Phone', 'PersonMobilePhone', 'Phone2__c', 'Description', 'Address__c']
        df_acc = pd.read_csv(acc_files[0], usecols=lambda c: c in acc_cols, dtype=str, encoding='utf-8')

        for _, row in df_acc.iterrows():
            for col in ['Phone', 'PersonMobilePhone', 'Phone2__c']:
                if col in row:
                    normalized = normalize_phone(row[col])
                    if normalized:
                        if normalized not in phone_to_records:
                            phone_to_records[normalized] = []
                        phone_to_records[normalized].append(('Account', row['Id'], row))
                        break
        print(f"  Account電話番号追加後: {len(phone_to_records)}件")
    else:
        print("  Accountファイルなし")
        df_acc = pd.DataFrame()

    return phone_to_records, df_lead, df_acc


def process_data(all_records, contract_phones, called_phones, contract_names, phone_index, contract_location_index, account_location_index):
    """データを処理して分類"""
    print("\n=== データ処理 ===")

    new_leads = []
    lead_updates = []
    account_updates = []
    excluded = []

    # 統計用カウンター
    account_location_match_count = 0

    for rec in all_records:
        phone = rec['phone_normalized']
        company = rec['company_name']

        # 媒体データの住所を取得
        media_pref = rec.get('prefecture', '')
        media_street = rec.get('street', '')
        # streetから市区町村を抽出（prefecture + streetで完全住所を再構成）
        if media_pref and media_street:
            full_address = media_pref + media_street
            _, media_city = extract_city(full_address)
        else:
            media_city = None

        # 1. 成約先電話番号チェック
        if phone in contract_phones:
            excluded.append({**rec, 'reason': '成約先（電話番号一致）'})
            continue

        # 2. 成約先 住所+名前チェック（都道府県+市区町村が一致 AND 名前が類似）
        location_matched = False
        if media_pref and media_city and company:
            location_key = (media_pref, media_city)
            if location_key in contract_location_index:
                company_normalized = normalize_company_name(company)
                for contract_normalized, contract_original in contract_location_index[location_key]:
                    if is_similar_name(company_normalized, contract_normalized):
                        excluded.append({**rec, 'reason': f'成約先（住所+名前一致: {contract_original}@{media_pref}{media_city}）'})
                        location_matched = True
                        break
        if location_matched:
            continue

        # 3. 成約先会社名チェック（住所なしでも名前だけで完全一致の場合）
        name_matched = False
        if company and pd.notna(company) and str(company) != 'nan':
            company_str = str(company).strip()
            company_normalized = normalize_company_name(company_str)

            for contract_name in contract_names:
                if contract_name and pd.notna(contract_name) and str(contract_name) != 'nan':
                    contract_str = str(contract_name).strip()
                    contract_normalized = normalize_company_name(contract_str)

                    # 完全一致のみ（住所がない場合は厳格に）
                    if company_normalized == contract_normalized and len(company_normalized) >= 5:
                        excluded.append({**rec, 'reason': f'成約先（会社名完全一致: {contract_str}）'})
                        name_matched = True
                        break
        if name_matched:
            continue

        # 4. 電話済みチェック
        if phone in called_phones:
            excluded.append({**rec, 'reason': '電話済み'})
            continue

        # 5. Salesforce突合（電話番号）
        if phone in phone_index:
            matches = phone_index[phone]
            for obj_type, obj_id, obj_row in matches:
                if obj_type == 'Lead':
                    lead_updates.append({**rec, 'sf_id': obj_id, 'sf_row': obj_row})
                elif obj_type == 'Account':
                    account_updates.append({**rec, 'sf_id': obj_id, 'sf_row': obj_row})
        else:
            # 6. 電話番号でマッチしなかった場合、取引先を住所+名前で検索
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
                # 新規リード
                new_leads.append(rec)

    print(f"  新規リード: {len(new_leads)}件")
    print(f"  Lead更新: {len(lead_updates)}件")
    print(f"  Account更新: {len(account_updates)}件")
    print(f"    └ うち住所+名前マッチ: {account_location_match_count}件")
    print(f"  除外: {len(excluded)}件")

    return new_leads, lead_updates, account_updates, excluded


def generate_new_lead_csv(new_leads):
    """新規リード作成用CSV生成"""
    if not new_leads:
        print("  新規リードなし")
        return None

    records = []
    skipped_no_phone = 0
    skipped_no_company = 0
    for row in new_leads:
        # Company必須チェック（空の場合はスキップ）
        company_name = row.get('company_name', '')
        if not company_name or not is_valid_value(company_name):
            skipped_no_company += 1
            continue

        # 電話番号を正規化済み形式で取得（先頭0付き）
        # Phone(固定電話)が空だとエラーになるため、携帯のみでも両方に入れる
        phone = row.get('phone_normalized', '')

        # Phone必須チェック（空の場合はスキップ）
        if not phone:
            skipped_no_phone += 1
            print(f"  警告: Phone空のためスキップ: {company_name}")
            continue

        if is_mobile_phone(phone):
            phone_field = phone  # 携帯番号をPhoneにも設定
            mobile_field = phone
        else:
            phone_field = phone
            mobile_field = ''

        # NoneやNaNを空文字に変換するヘルパー
        def safe_str(val):
            if val is None or pd.isna(val):
                return ''
            return str(val)

        record = {
            'Company': company_name,  # 事前にバリデーション済み
            'LastName': '担当者',
            'Phone': phone_field,  # 事前にバリデーション済み（必ず値あり）
            'MobilePhone': mobile_field if mobile_field else '',
            'Prefecture__c': safe_str(row.get('prefecture', '')),
            'Street': safe_str(row.get('street', '')),
            'Paid_Memo__c': safe_str(row.get('memo', '')),  # LeadにはDescriptionがないためPaid_Memo__c使用
            'LeadSource': safe_str(row.get('source', '')),
            'Paid_Media__c': safe_str(row.get('source', '')),
            'Paid_DataExportDate__c': TODAY,
            'Paid_DataSource__c': safe_str(row.get('source', '')),
        }
        records.append(record)

    df = pd.DataFrame(records)
    # 全列を文字列型として保存（電話番号のfloat変換を防ぐ）
    df = df.astype(str)
    df = df.replace('nan', '')
    df = df.replace('None', '')
    output_path = OUTPUT_DIR / f'kango_nurse_new_leads_{TIMESTAMP}.csv'
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
        existing_desc = sf_row.get('Description', '') or ''

        # 既存メモに追記
        new_memo = f"【既存更新】\n{row.get('memo', '')}\n---\n{existing_desc}"

        record = {
            'Id': row['sf_id'],
            'Description': new_memo,
            'Paid_Media__c': row.get('source', ''),
            'Paid_DataExportDate__c': TODAY,
            'Paid_DataSource__c': row.get('source', ''),
        }
        records.append(record)

    df = pd.DataFrame(records)
    df = df.astype(str)
    df = df.replace('nan', '')
    df = df.replace('None', '')
    output_path = OUTPUT_DIR / f'kango_nurse_lead_updates_{TIMESTAMP}.csv'
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
        sf_row = row.get('sf_row', {})
        existing_desc = sf_row.get('Description', '') or ''

        # 既存メモに追記
        new_memo = f"【既存更新】\n{row.get('memo', '')}\n---\n{existing_desc}"

        record = {
            'Id': row['sf_id'],
            'Description': new_memo,
            'Paid_Media__c': row.get('source', ''),
            'Paid_DataExportDate__c': TODAY,
            'Paid_DataSource__c': row.get('source', ''),
        }
        records.append(record)

    df = pd.DataFrame(records)
    df = df.astype(str)
    df = df.replace('nan', '')
    df = df.replace('None', '')
    output_path = OUTPUT_DIR / f'kango_nurse_account_updates_{TIMESTAMP}.csv'
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"  出力: {output_path} ({len(df)}件)")
    return output_path


def generate_excluded_csv(excluded):
    """除外リストCSV生成"""
    if not excluded:
        return None

    records = []
    for row in excluded:
        record = {
            'source': row.get('source', ''),
            'company_name': row.get('company_name', ''),
            'phone': row.get('phone_normalized', ''),
            'reason': row.get('reason', ''),
        }
        records.append(record)

    df = pd.DataFrame(records)
    output_path = OUTPUT_DIR / f'kango_nurse_excluded_{TIMESTAMP}.csv'
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"  除外リスト: {output_path} ({len(df)}件)")
    return output_path


def main():
    print("=" * 60)
    print("看護roo・ナース専科 データ処理")
    print("=" * 60)

    # 1. データ読み込み
    kango_records = load_kango_data()
    nurse_records = load_nurse_data()

    # 2. 統合・重複除去（電話番号ベース、看護roo優先）
    print("\n=== データ統合 ===")
    all_records = kango_records + nurse_records
    seen_phones = set()
    unique_records = []
    for rec in all_records:
        phone = rec['phone_normalized']
        if phone not in seen_phones:
            seen_phones.add(phone)
            unique_records.append(rec)

    print(f"  統合後（電話番号ユニーク）: {len(unique_records)}件")
    print(f"    - 看護roo由来: {len([r for r in unique_records if r['source'] == '看護roo'])}件")
    print(f"    - ナース専科由来: {len([r for r in unique_records if r['source'] == 'ナース専科'])}件")

    # 3. 除外リスト読み込み
    contract_phones, called_phones = load_exclusion_phones()
    contract_names = load_contract_company_names()
    contract_location_index = load_contract_location_index()

    # 4. Salesforce電話番号インデックス
    phone_index, df_lead, df_acc = load_salesforce_phone_index()

    # 5. 取引先住所+名前インデックス（電話番号でマッチしない場合の補完用）
    account_location_index = load_account_location_index()

    # 6. データ処理
    new_leads, lead_updates, account_updates, excluded = process_data(
        unique_records, contract_phones, called_phones, contract_names, phone_index, contract_location_index, account_location_index
    )

    # 6. CSV生成
    print("\n=== CSV生成 ===")
    generate_new_lead_csv(new_leads)
    generate_lead_update_csv(lead_updates)
    generate_account_update_csv(account_updates)
    generate_excluded_csv(excluded)

    # 7. サマリー
    print("\n" + "=" * 60)
    print("処理完了サマリー")
    print("=" * 60)
    print(f"入力データ:")
    print(f"  看護roo: {len(kango_records)}件")
    print(f"  ナース専科: {len(nurse_records)}件")
    print(f"  統合後: {len(unique_records)}件")
    print(f"\n出力:")
    print(f"  新規リード: {len(new_leads)}件")
    print(f"  Lead更新: {len(lead_updates)}件")
    print(f"  Account更新: {len(account_updates)}件")
    print(f"  除外: {len(excluded)}件")

    # 除外理由の内訳
    if excluded:
        print(f"\n除外理由内訳:")
        reasons = {}
        for ex in excluded:
            reason = ex.get('reason', 'Unknown')
            # 会社名一致の場合は「成約先（会社名一致）」に集約
            if '会社名一致' in reason:
                reason = '成約先（会社名一致）'
            reasons[reason] = reasons.get(reason, 0) + 1
        for reason, count in sorted(reasons.items(), key=lambda x: -x[1]):
            print(f"  {reason}: {count}件")


if __name__ == '__main__':
    main()
