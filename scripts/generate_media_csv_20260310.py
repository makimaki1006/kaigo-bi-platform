"""
有料媒体インポート用CSV生成（PT・OT・STネット + ジョブポスター）
2026-03-10 マスタールール準拠

処理フロー:
1. スクレイピングデータ読み込み
2. 電話番号正規化・抽出
3. 成約先除外
4. 電話済み除外
5. SF既存突合（Lead/Account/Contact）
6. 新規リード / 既存更新の分類
7. 所有者割り当て
8. CSV出力
"""

import pandas as pd
import re
import sys
import os
from pathlib import Path
from datetime import datetime

sys.stdout.reconfigure(encoding='utf-8')

BASE_DIR = Path(__file__).parent.parent
PROCESS_DATE = '2026-03-10'
TIMESTAMP = datetime.now().strftime('%Y%m%d_%H%M%S')

# === 入力ファイル ===
PT_OT_ST_FILE = Path(r'C:\Users\fuji1\Downloads\extracted_phone_and_name.csv')
JOBPOSTER_FILE = Path(r'C:\Users\fuji1\Downloads\job-poster.com-から詳細をスクレイピングします--2--2026-03-10.csv')

# === SF既存データ（最新 2026-03-10） ===
CONTRACT_FILE = BASE_DIR / 'data/output/contract_accounts_20260310_174114.csv'
LEAD_FILE = BASE_DIR / 'data/output/Lead_20260310_174622.csv'
ACCOUNT_FILE = BASE_DIR / 'data/output/Account_20260310_175404.csv'
CONTACT_FILE = BASE_DIR / 'data/output/Contact_20260310_175813.csv'

# === 電話済みリスト ===
MEDIA_LIST_FILE = Path(r'C:\Users\fuji1\Downloads\媒体掲載中のリスト.xlsx')

# === 出力 ===
OUTPUT_DIR = BASE_DIR / 'data/output/media_matching'

# === 所有者ID ===
OWNERS = {
    '市来': '005dc00000FwuKXAAZ',
    '嶋谷': '005dc000001dryLAAQ',
    '小林': '005J3000000ERz4IAG',
    '熊谷': '0055i00000CDtTOAA1',
    '松風': '0055i00000CwGDpAAN',
    '篠木': '005dc00000HgmfxAAB',
    '澤田': '005dc00000IwKTpAAN',
}
FUKAHORI = ('深堀', '0055i00000CwKEhAAN')
HATTORI = ('服部', '005J3000000EYYjIAO')

# === 一般名称リスト ===
GENERIC_NAMES = {
    '担当者', '採用担当', '採用担当者', '採用担当者（名前を聞けたら変更）',
    '人事担当', '人事担当者', '採用係', '採用担当係', '店長', '院長', '事務長',
    '総務担当', '総務担当者', '総務課', '管理者', '責任者', '代表者',
}

UNTOUCHED_STATUSES = {'未架電', '00 架電OK - 接触なし'}


def normalize_phone(p):
    """電話番号を正規化（10-11桁の数字列に）"""
    if not p or pd.isna(p):
        return None
    p = str(p).strip()
    # .0除去
    p = p.replace('.0', '')
    # 数字以外を除去
    digits = re.sub(r'[^\d]', '', p)
    # 先頭0補完
    if digits and digits[0] != '0':
        digits = '0' + digits
    if 10 <= len(digits) <= 11:
        return digits
    return None


def is_mobile(phone):
    """携帯電話判定"""
    return phone and phone[:3] in ('070', '080', '090')


def is_generic_name(name):
    """一般名称かどうか"""
    if not name or pd.isna(name):
        return True
    name = str(name).strip()
    return name in GENERIC_NAMES or name == '' or name == 'nan'


def extract_prefecture(addr):
    """住所から都道府県を抽出"""
    if not addr or pd.isna(addr) or str(addr) == 'nan':
        return ''
    match = re.search(r'(東京都|北海道|(?:京都|大阪)府|.{2,3}県)', str(addr))
    return match.group(1) if match else ''


def extract_street(addr):
    """住所から都道府県を除いた部分"""
    if not addr or pd.isna(addr) or str(addr) == 'nan':
        return ''
    addr = str(addr)
    addr = re.sub(r'^(東京都|北海道|(?:京都|大阪)府|.{2,3}県)', '', addr)
    return addr.strip()


def safe_str(val):
    if val is None or pd.isna(val) or str(val) == 'nan':
        return ''
    return str(val).strip()


def extract_phones_from_text(text):
    """テキストから電話番号を抽出"""
    if not text or pd.isna(text):
        return []
    phones = re.findall(r'[\d\-]{10,13}', str(text))
    result = []
    for p in phones:
        n = normalize_phone(p)
        if n:
            result.append(n)
    return result


# ======================================================
# STEP 1: データ読み込み
# ======================================================
def load_pt_ot_st():
    """PT・OT・STネットデータ読み込み"""
    print('\n--- PT・OT・STネット データ読み込み ---')
    df = pd.read_csv(PT_OT_ST_FILE, encoding='utf-8-sig', dtype=str)
    print(f'  件数: {len(df)}')

    records = []
    for _, row in df.iterrows():
        # 電話番号: 専用列から取得
        phone_raw = safe_str(row.get('電話番号'))
        phones = []
        if phone_raw:
            # カンマ区切りの場合
            for p in phone_raw.replace('、', ',').split(','):
                n = normalize_phone(p.strip())
                if n:
                    phones.append(n)

        if not phones:
            continue

        # 固定電話/携帯電話を分類
        landline = None
        mobile = None
        for p in phones:
            if is_mobile(p):
                if not mobile:
                    mobile = p
            else:
                if not landline:
                    landline = p

        phone_field = landline or mobile or phones[0]

        # 会社名
        company = safe_str(row.get('recruit_detail_name'))
        if not company:
            continue

        # 担当者名（1行目のみ）
        contact_name = safe_str(row.get('担当者名'))
        if contact_name:
            contact_name = contact_name.split('\n')[0].strip()
            # カナ除去
            contact_name = contact_name.split('\u3000')[0].strip()

        # 住所
        address = safe_str(row.get('clearfix (2)'))

        # 募集職種
        job_title = safe_str(row.get('recruit_detailHead'))

        # 掲載日
        posting_date = safe_str(row.get('recruit_detailHead (2)'))

        # リハビリ分類
        rehab_class = safe_str(row.get('clearfix (4)'))

        # 雇用形態
        employment_type = safe_str(row.get('clearfix (10)'))

        # 採用人数
        recruitment_num = safe_str(row.get('clearfix (6)'))
        # 数値のみ抽出
        if recruitment_num:
            num_match = re.search(r'(\d+)', recruitment_num)
            recruitment_num_val = num_match.group(1) if num_match else ''
        else:
            recruitment_num_val = ''

        # HP
        hp_raw = safe_str(row.get('clearfix (12)'))
        homepage = ''
        if hp_raw:
            # URL抽出
            url_match = re.search(r'https?://[^\s]+', hp_raw)
            if url_match:
                homepage = url_match.group(0)

        # URL
        url = safe_str(row.get('recruit_listDetail href'))

        # 閲覧数
        view_count = safe_str(row.get('clearfix (24)'))

        # メモ構築
        memo_parts = [f'掲載日: {posting_date}']
        if view_count:
            memo_parts.append(f'閲覧数: {view_count}')
        if rehab_class:
            memo_parts.append(f'リハビリ分類: {rehab_class}')
        if employment_type:
            memo_parts.append(f'雇用形態: {employment_type}')
        if recruitment_num:
            memo_parts.append(f'採用人数: {recruitment_num}')

        records.append({
            'media': 'PT・OT・STネット',
            'company': company,
            'contact_name': contact_name if contact_name else '担当者',
            'phone': phone_field,
            'mobile': mobile or '',
            'address': address,
            'job_title': job_title,
            'rehab_class': rehab_class,
            'employment_type': employment_type,
            'recruitment_num': recruitment_num_val,
            'homepage': homepage,
            'url': url,
            'memo': '\n'.join(memo_parts),
            'posting_date': posting_date,
        })

    print(f'  有効レコード: {len(records)}件')
    return records


def load_jobposter():
    """ジョブポスターデータ読み込み"""
    print('\n--- ジョブポスター データ読み込み ---')
    df = pd.read_csv(JOBPOSTER_FILE, encoding='utf-8-sig', dtype=str)
    print(f'  件数: {len(df)}')

    records = []
    for _, row in df.iterrows():
        # 電話番号
        phone_raw = safe_str(row.get('en'))
        phone = normalize_phone(phone_raw)
        if not phone:
            continue

        # 会社名（会社情報の1行目）
        company_info = safe_str(row.get('even (6)'))
        if not company_info:
            continue
        company = company_info.split('\n')[0].strip()
        if not company:
            continue

        # 担当者名
        contact_name = safe_str(row.get('first (6)'))
        if contact_name:
            # 「採用担当／河原」→「河原」のようなパターン
            if '／' in contact_name:
                contact_name = contact_name.split('／')[-1].strip()
            elif '/' in contact_name:
                contact_name = contact_name.split('/')[-1].strip()

        # 住所
        address = safe_str(row.get('even (4)'))

        # 職種
        job_title = safe_str(row.get('first (2)'))

        # HP
        homepage = safe_str(row.get('first (4)'))

        # 掲載期間
        app_period = safe_str(row.get('appPireod'))

        # URL
        url = safe_str(row.get('hvBtn href'))

        # メモ
        memo_parts = []
        if app_period:
            memo_parts.append(f'掲載期間: {app_period}')

        mobile = phone if is_mobile(phone) else ''

        records.append({
            'media': 'ジョブポスター',
            'company': company,
            'contact_name': contact_name if contact_name and not is_generic_name(contact_name) else '担当者',
            'phone': phone,
            'mobile': mobile,
            'address': address,
            'job_title': job_title,
            'rehab_class': '',
            'employment_type': '',
            'recruitment_num': '',
            'homepage': homepage,
            'url': url,
            'memo': '\n'.join(memo_parts),
            'posting_date': '',
        })

    print(f'  有効レコード: {len(records)}件')
    return records


# ======================================================
# STEP 2-3: 除外処理
# ======================================================
def load_exclusion_phones():
    """成約先 + 電話済みリストの電話番号セットを構築"""
    print('\n--- 除外用電話番号読み込み ---')

    # 成約先
    contract_phones = set()
    df_contract = pd.read_csv(CONTRACT_FILE, encoding='utf-8-sig', dtype=str)
    for col in ['Phone', 'Phone2__c', 'PersonMobilePhone']:
        if col in df_contract.columns:
            for v in df_contract[col].dropna():
                p = normalize_phone(v)
                if p:
                    contract_phones.add(p)
    print(f'  成約先電話番号: {len(contract_phones)}件')

    # 成約先会社名（会社名突合用）
    contract_names = set()
    if 'Name' in df_contract.columns:
        for v in df_contract['Name'].dropna():
            name = str(v).replace(' ', '').replace('　', '')
            for suffix in ['株式会社', '有限会社', '医療法人', '社会福祉法人',
                           '医療法人社団', '医療法人財団', '社会福祉法人']:
                name = name.replace(suffix, '')
            if len(name) >= 4:
                contract_names.add(name)
    print(f'  成約先会社名: {len(contract_names)}件')

    # 電話済みリスト
    called_phones = set()
    if MEDIA_LIST_FILE.exists():
        xls = pd.ExcelFile(MEDIA_LIST_FILE)
        for sheet in xls.sheet_names:
            df_sheet = pd.read_excel(xls, sheet_name=sheet, dtype=str)
            for col in df_sheet.columns:
                if '電話' in col or 'phone' in col.lower():
                    for v in df_sheet[col].dropna():
                        p = normalize_phone(v)
                        if p:
                            called_phones.add(p)
        print(f'  電話済み電話番号: {len(called_phones)}件')
    else:
        print(f'  電話済みリスト: ファイルなし（スキップ）')

    return contract_phones, contract_names, called_phones


# ======================================================
# STEP 4: SF既存データ突合
# ======================================================
def build_sf_phone_index():
    """SF既存データから電話番号→レコードのインデックスを構築"""
    print('\n--- SF既存データ読み込み ---')

    # Lead
    phone_to_lead = {}
    df_lead = pd.read_csv(LEAD_FILE, encoding='utf-8-sig', dtype=str, low_memory=False)
    print(f'  Lead: {len(df_lead)}件')
    for _, row in df_lead.iterrows():
        lead_id = row.get('Id', '')
        for col in ['Phone', 'MobilePhone', 'Phone2__c', 'MobilePhone2__c']:
            if col in df_lead.columns:
                p = normalize_phone(row.get(col))
                if p and p not in phone_to_lead:
                    phone_to_lead[p] = row

    # Account
    phone_to_account = {}
    df_account = pd.read_csv(ACCOUNT_FILE, encoding='utf-8-sig', dtype=str, low_memory=False)
    print(f'  Account: {len(df_account)}件')
    for _, row in df_account.iterrows():
        for col in ['Phone', 'PersonMobilePhone', 'Phone2__c']:
            if col in df_account.columns:
                p = normalize_phone(row.get(col))
                if p and p not in phone_to_account:
                    phone_to_account[p] = row

    # Contact
    phone_to_contact = {}
    df_contact = pd.read_csv(CONTACT_FILE, encoding='utf-8-sig', dtype=str, low_memory=False)
    print(f'  Contact: {len(df_contact)}件')
    for _, row in df_contact.iterrows():
        for col in ['Phone', 'MobilePhone', 'Phone2__c', 'MobilePhone2__c']:
            if col in df_contact.columns:
                p = normalize_phone(row.get(col))
                if p and p not in phone_to_contact:
                    phone_to_contact[p] = row

    print(f'  Lead電話番号インデックス: {len(phone_to_lead)}件')
    print(f'  Account電話番号インデックス: {len(phone_to_account)}件')
    print(f'  Contact電話番号インデックス: {len(phone_to_contact)}件')

    return phone_to_lead, phone_to_account, phone_to_contact, df_lead


def check_contract_by_name(company_name, contract_names):
    """会社名で成約先チェック"""
    normalized = company_name.replace(' ', '').replace('　', '')
    for suffix in ['株式会社', '有限会社', '医療法人', '社会福祉法人',
                   '医療法人社団', '医療法人財団']:
        normalized = normalized.replace(suffix, '')

    for contract_name in contract_names:
        if len(contract_name) >= 4:
            if contract_name in normalized or normalized in contract_name:
                return True
    return False


def should_update_lastname(existing_name, new_name, status):
    """担当者名更新判定"""
    if not new_name or is_generic_name(new_name):
        return False
    if is_generic_name(existing_name):
        return True
    if status in UNTOUCHED_STATUSES:
        return True
    return False


# ======================================================
# メイン処理
# ======================================================
def main():
    print('=' * 70)
    print(f'有料媒体インポート用CSV生成 {PROCESS_DATE}')
    print(f'対象: PT・OT・STネット + ジョブポスター')
    print('=' * 70)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # STEP 1: データ読み込み
    pt_records = load_pt_ot_st()
    jp_records = load_jobposter()
    all_records = pt_records + jp_records
    print(f'\n合計: {len(all_records)}件（PT: {len(pt_records)}, JP: {len(jp_records)}）')

    # 電話番号で重複除去
    seen_phones = set()
    unique_records = []
    dup_count = 0
    for r in all_records:
        if r['phone'] not in seen_phones:
            seen_phones.add(r['phone'])
            unique_records.append(r)
        else:
            dup_count += 1
    print(f'電話番号重複除去: {dup_count}件 → ユニーク: {len(unique_records)}件')

    # STEP 2-3: 除外処理
    contract_phones, contract_names, called_phones = load_exclusion_phones()

    excluded_contract = []
    excluded_called = []
    excluded_contract_name = []
    remaining = []

    for r in unique_records:
        if r['phone'] in contract_phones:
            excluded_contract.append(r)
        elif r['phone'] in called_phones:
            excluded_called.append(r)
        elif check_contract_by_name(r['company'], contract_names):
            excluded_contract_name.append(r)
        else:
            remaining.append(r)

    print(f'\n--- 除外結果 ---')
    print(f'  成約先電話番号一致: {len(excluded_contract)}件')
    print(f'  成約先会社名一致: {len(excluded_contract_name)}件')
    print(f'  電話済み: {len(excluded_called)}件')
    print(f'  残り: {len(remaining)}件')

    # STEP 4: SF突合
    phone_to_lead, phone_to_account, phone_to_contact, df_lead = build_sf_phone_index()

    new_leads = []
    lead_updates = []
    account_updates = []

    for r in remaining:
        phone = r['phone']

        # Lead突合（最優先）
        if phone in phone_to_lead:
            lead_row = phone_to_lead[phone]
            lead_updates.append({
                'record': r,
                'sf_lead': lead_row,
            })
        # Account突合
        elif phone in phone_to_account:
            account_row = phone_to_account[phone]
            account_updates.append({
                'record': r,
                'sf_account': account_row,
            })
        # Contact突合
        elif phone in phone_to_contact:
            contact_row = phone_to_contact[phone]
            # ContactのAccountIdでAccount更新
            account_id = safe_str(contact_row.get('AccountId'))
            account_updates.append({
                'record': r,
                'sf_contact': contact_row,
                'account_id': account_id,
            })
        else:
            new_leads.append(r)

    print(f'\n--- SF突合結果 ---')
    print(f'  新規リード: {len(new_leads)}件')
    print(f'  既存Lead更新: {len(lead_updates)}件')
    print(f'  既存Account更新: {len(account_updates)}件')

    # 媒体別内訳
    new_pt = sum(1 for r in new_leads if r['media'] == 'PT・OT・STネット')
    new_jp = sum(1 for r in new_leads if r['media'] == 'ジョブポスター')
    print(f'  新規内訳: PT・OT・STネット {new_pt}件, ジョブポスター {new_jp}件')

    # ======================================================
    # STEP 5: 新規リードCSV生成
    # ======================================================
    print(f'\n--- 新規リードCSV生成 ---')

    new_lead_records = []
    for r in new_leads:
        phone = r['phone']
        mobile = r['mobile'] if r['mobile'] else (phone if is_mobile(phone) else '')
        phone_field = phone  # Phoneは必ず値を入れる

        # メモ欄
        memo = f"【新規作成】有料媒体突合 {PROCESS_DATE} {r['media']}\n{r['memo']}"

        record = {
            'Company': r['company'],
            'LastName': r['contact_name'] if r['contact_name'] else '担当者',
            'Phone': phone_field,
            'MobilePhone': mobile,
            'Prefecture__c': extract_prefecture(r['address']),
            'Street': extract_street(r['address']),
            'Website': r['homepage'],
            'LeadSource': 'Other',
            'Paid_Media__c': r['media'],
            'Paid_DataSource__c': r['media'],
            'Paid_JobTitle__c': r['job_title'],
            'Paid_RecruitmentType__c': r['job_title'],
            'Paid_EmploymentType__c': r['employment_type'],
            'Paid_Industry__c': r['rehab_class'],
            'Paid_NumberOfRecruitment__c': r['recruitment_num'],
            'Paid_Memo__c': memo,
            'Paid_URL__c': r['url'],
            'Paid_DataExportDate__c': PROCESS_DATE,
            'LeadSourceMemo__c': memo,
        }
        new_lead_records.append(record)

    df_new = pd.DataFrame(new_lead_records)

    # セグメント分析
    mobile_count = sum(1 for r in new_leads if r['mobile'] or is_mobile(r['phone']))
    byname_count = sum(1 for r in new_leads if not is_generic_name(r['contact_name']))
    print(f'  携帯電話あり: {mobile_count}件')
    print(f'  バイネームあり: {byname_count}件')

    # 最強セグメント（携帯 + バイネーム）- PT/OT/STには代表者なし
    strongest = [r for r in new_leads if (r['mobile'] or is_mobile(r['phone'])) and not is_generic_name(r['contact_name'])]
    print(f'  最強セグメント（携帯+バイネーム）: {len(strongest)}件')

    # ======================================================
    # STEP 6: 所有者割り当て
    # ======================================================
    print(f'\n--- 所有者割り当て ---')
    n = len(df_new)
    if n == 0:
        print('  新規リードなし、スキップ')
    else:
        # 深堀30%、服部30%、人材開発40%
        fukahori_n = n * 30 // 100
        hattori_n = n * 30 // 100
        jinzai_n = n - fukahori_n - hattori_n

        # 最強セグメントを市来に優先
        strongest_phones = set(r['phone'] for r in strongest)

        # 市来の最強セグメント件数
        ichiki_strongest = 0
        owners_list = []
        owner_names_list = []

        # まず最強セグメントを市来に
        for i, r in enumerate(new_leads):
            if r['phone'] in strongest_phones:
                owners_list.append(('市来', OWNERS['市来'], i))
                ichiki_strongest += 1

        # 残りのインデックス
        remaining_indices = [i for i in range(n) if i not in set(x[2] for x in owners_list)]

        # 深堀分
        for idx in remaining_indices[:fukahori_n]:
            owners_list.append((FUKAHORI[0], FUKAHORI[1], idx))
        remaining_indices = remaining_indices[fukahori_n:]

        # 服部分
        for idx in remaining_indices[:hattori_n]:
            owners_list.append((HATTORI[0], HATTORI[1], idx))
        remaining_indices = remaining_indices[hattori_n:]

        # 人材開発（市来以外の6名 + 市来の均等分）
        member_list = list(OWNERS.items())
        jinzai_remaining = len(remaining_indices)
        per_member = jinzai_remaining // 7
        remainder = jinzai_remaining % 7

        idx_pos = 0
        for j, (name, uid) in enumerate(member_list):
            count = per_member + (1 if j < remainder else 0)
            for _ in range(count):
                if idx_pos < len(remaining_indices):
                    owners_list.append((name, uid, remaining_indices[idx_pos]))
                    idx_pos += 1

        # ソートしてDataFrameに適用
        owners_list.sort(key=lambda x: x[2])
        owner_ids = [x[1] for x in owners_list]
        owner_name_list = [x[0] for x in owners_list]

        df_new['OwnerId'] = owner_ids[:n]

        # 所有者別集計
        owner_counts = pd.Series(owner_name_list[:n]).value_counts()
        for name, cnt in owner_counts.items():
            suffix = f' (最強セグメント{ichiki_strongest}件含む)' if name == '市来' and ichiki_strongest > 0 else ''
            print(f'  {name}: {cnt}件{suffix}')

    # ======================================================
    # STEP 7: 既存Lead更新CSV生成
    # ======================================================
    print(f'\n--- 既存Lead更新CSV生成 ---')
    lead_update_records = []
    for item in lead_updates:
        r = item['record']
        sf = item['sf_lead']

        lead_id = safe_str(sf.get('Id'))
        if not lead_id:
            continue

        update = {'Id': lead_id}

        # 担当者名更新判定
        existing_name = safe_str(sf.get('LastName'))
        new_name = r['contact_name']
        status = safe_str(sf.get('Status__c'))
        if should_update_lastname(existing_name, new_name, status):
            update['LastName'] = new_name

        # Paid_* フィールド（空欄のみ補完）
        field_map = {
            'Paid_Media__c': r['media'],
            'Paid_DataSource__c': r['media'],
            'Paid_JobTitle__c': r['job_title'],
            'Paid_RecruitmentType__c': r['job_title'],
            'Paid_EmploymentType__c': r['employment_type'],
            'Paid_Industry__c': r['rehab_class'],
            'Paid_URL__c': r['url'],
        }
        for sf_field, value in field_map.items():
            if value and not safe_str(sf.get(sf_field)):
                update[sf_field] = value

        # Paid_NumberOfRecruitment__c（空欄のみ、数値型）
        if r['recruitment_num'] and not safe_str(sf.get('Paid_NumberOfRecruitment__c')):
            update['Paid_NumberOfRecruitment__c'] = r['recruitment_num']

        # Website（空欄のみ、ジョブポスターのみ）
        if r['homepage'] and not safe_str(sf.get('Website')):
            update['Website'] = r['homepage']

        # 常に更新
        update['Paid_DataExportDate__c'] = PROCESS_DATE

        # メモ欄
        existing_memo = safe_str(sf.get('LeadSourceMemo__c'))
        new_memo = f"【既存更新】有料媒体突合 {PROCESS_DATE} {r['media']}"
        if existing_memo:
            update['LeadSourceMemo__c'] = f"{new_memo}\n{existing_memo}"
        else:
            update['LeadSourceMemo__c'] = new_memo

        lead_update_records.append(update)

    print(f'  更新レコード: {len(lead_update_records)}件')

    # ======================================================
    # STEP 8: 既存Account更新CSV生成
    # ======================================================
    print(f'\n--- 既存Account更新CSV生成 ---')
    account_update_records = []
    for item in account_updates:
        r = item['record']
        sf_account = item.get('sf_account')
        account_id = item.get('account_id', '')

        if sf_account is not None:
            account_id = safe_str(sf_account.get('Id'))

        if not account_id:
            continue

        update = {'Id': account_id}
        update['Paid_DataExportDate__c'] = PROCESS_DATE

        # メモ差別化
        if sf_account is not None:
            existing_desc = safe_str(sf_account.get('Description'))
            new_desc = f"【既存更新】有料媒体突合 {PROCESS_DATE} {r['media']}"
            if existing_desc:
                update['Description'] = f"{new_desc}\n{existing_desc}"
            else:
                update['Description'] = new_desc

        account_update_records.append(update)

    print(f'  更新レコード: {len(account_update_records)}件')

    # ======================================================
    # STEP 9: CSV出力
    # ======================================================
    print(f'\n--- CSV出力 ---')

    # 新規リード
    if len(df_new) > 0:
        new_file = OUTPUT_DIR / f'new_leads_{TIMESTAMP}.csv'
        # owner_name列は除外
        df_new.to_csv(new_file, index=False, encoding='utf-8-sig')
        print(f'  新規リード: {new_file} ({len(df_new)}件)')
    else:
        print('  新規リード: なし')

    # Lead更新
    if lead_update_records:
        df_lu = pd.DataFrame(lead_update_records)
        lu_file = OUTPUT_DIR / f'lead_updates_{TIMESTAMP}.csv'
        df_lu.to_csv(lu_file, index=False, encoding='utf-8-sig')
        print(f'  Lead更新: {lu_file} ({len(df_lu)}件)')

    # Account更新
    if account_update_records:
        df_au = pd.DataFrame(account_update_records)
        au_file = OUTPUT_DIR / f'account_updates_{TIMESTAMP}.csv'
        df_au.to_csv(au_file, index=False, encoding='utf-8-sig')
        print(f'  Account更新: {au_file} ({len(df_au)}件)')

    # 除外リスト
    all_excluded = (
        [{'phone': r['phone'], 'company': r['company'], 'reason': '成約先電話番号'} for r in excluded_contract] +
        [{'phone': r['phone'], 'company': r['company'], 'reason': '成約先会社名'} for r in excluded_contract_name] +
        [{'phone': r['phone'], 'company': r['company'], 'reason': '電話済み'} for r in excluded_called]
    )
    if all_excluded:
        df_ex = pd.DataFrame(all_excluded)
        ex_file = OUTPUT_DIR / f'excluded_{TIMESTAMP}.csv'
        df_ex.to_csv(ex_file, index=False, encoding='utf-8-sig')
        print(f'  除外リスト: {ex_file} ({len(df_ex)}件)')

    # ======================================================
    # サマリー
    # ======================================================
    print('\n' + '=' * 70)
    print('処理サマリー')
    print('=' * 70)
    print(f'  入力: PT・OT・STネット {len(pt_records)}件 + ジョブポスター {len(jp_records)}件 = {len(all_records)}件')
    print(f'  重複除去後: {len(unique_records)}件')
    print(f'  除外: 成約先(電話){len(excluded_contract)} + 成約先(会社名){len(excluded_contract_name)} + 電話済み{len(excluded_called)} = {len(excluded_contract) + len(excluded_contract_name) + len(excluded_called)}件')
    print(f'  新規リード: {len(new_leads)}件')
    print(f'  既存Lead更新: {len(lead_updates)}件')
    print(f'  既存Account更新: {len(account_updates)}件')

    # サンプル
    if len(df_new) > 0:
        print(f'\n--- 新規リードサンプル（5件） ---')
        for _, row in df_new.head(5).iterrows():
            print(f"  {row['Company']:30s} | {row['LastName']:8s} | {row['Phone']:12s} | {row['Paid_Media__c']}")

    return {
        'new_leads': len(new_leads),
        'lead_updates': len(lead_updates),
        'account_updates': len(account_updates),
        'excluded': len(excluded_contract) + len(excluded_contract_name) + len(excluded_called),
    }


if __name__ == '__main__':
    main()
