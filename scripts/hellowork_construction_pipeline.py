"""
ハローワーク建設系リード抽出パイプライン

建設・土木関連の職種をハローワークデータから抽出し、
Salesforceとの突合を行い、新規リードCSVを生成する。

処理フロー:
1. ハローワークCSV読み込み
2. 建設系フィルタ（職業分類コード + キーワード）
3. 電話番号正規化・重複排除
4. 成約先・電話済み除外
5. Salesforce突合（新規/既存判定）
6. 新規リードCSV生成
7. サマリー出力
"""

import sys
import re
import pickle
import requests
import pandas as pd
from pathlib import Path
from datetime import date
from collections import defaultdict

# 標準出力をUTF-8に設定
sys.stdout.reconfigure(encoding='utf-8')

# パスを追加
import os
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / 'src'))

# パス設定
base = Path(r'C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List')
today = date.today().isoformat()

print("="*80)
print("ハローワーク建設系リード抽出パイプライン")
print(f"実行日: {today}")
print("="*80)

# ========================================
# STEP 1: ハローワークCSV読み込み
# ========================================
print("\n[STEP 1] ハローワークCSV読み込み")
hw_path = base / 'data/input/hellowork.csv'

try:
    hw = pd.read_csv(hw_path, encoding='cp932', dtype=str, low_memory=False)
    print(f"✅ 読み込み成功: {len(hw)}件, {len(hw.columns)}列")
except Exception as e:
    print(f"❌ エラー: {e}")
    sys.exit(1)

# 列名確認（デバッグ用）
print(f"\n主要列名サンプル:")
for i, col in enumerate(hw.columns[:20]):
    print(f"  {i+1}. {col}")

# ========================================
# STEP 2: 建設系フィルタ
# ========================================
print("\n[STEP 2] 建設系フィルタ適用")

# 建設関連職業分類コード
construction_codes = []
# 007系 (建築・土木・測量技術者等)
for i in ['01','02','03','04','05','06','07','08','09','99']:
    construction_codes.append(f'007-{i}')
# 008系 (建築・土木技術者)
for i in ['01','02','03','04','05','06','07','08','09','99']:
    construction_codes.append(f'008-{i}')
# 009系 (建設関連技術者)
for i in ['01','02','03','04','05','06','07','08','09','99']:
    construction_codes.append(f'009-{i}')
# 070-099系 (建設・土木作業員)
for major in range(70, 100):
    for minor in ['01','02','03','04','05','06','07','08','09','10','99']:
        construction_codes.append(f'{major:03d}-{minor}')

construction_codes_set = set(construction_codes)

# 職業分類コード列を検索
code_cols = []
for col in hw.columns:
    if '職業分類' in col and 'コード' in col:
        code_cols.append(col)

print(f"職業分類コード列: {code_cols}")

# コードマッチ
code_match = pd.Series(False, index=hw.index)
for col in code_cols:
    code_match = code_match | hw[col].isin(construction_codes_set)

# キーワードマッチ
keywords = [
    '建設', '建築', '土木', '工事', '配管', '電気工', '塗装', '左官',
    '鳶', '大工', '解体', '施工', '設備工', '内装', '外装', '防水',
    '屋根', '基礎', '造園', '舗装', 'とび', 'とび工', '足場',
    '型枠', '鉄筋', '溶接', 'クレーン', '重機', 'ユンボ'
]
keyword_pattern = '|'.join(keywords)

# 職種・仕事内容列を検索
keyword_cols = []
for col in hw.columns:
    if col in ['職種', '仕事内容'] or '職種' in col or '仕事内容' in col:
        keyword_cols.append(col)

print(f"キーワード検索列: {keyword_cols}")

keyword_match = pd.Series(False, index=hw.index)
for col in keyword_cols:
    if col in hw.columns:
        keyword_match = keyword_match | hw[col].fillna('').str.contains(keyword_pattern, na=False, regex=True)

# ハイブリッドフィルタ（コード OR キーワード）
construction_mask = code_match | keyword_match
hw_construction = hw[construction_mask].copy()

print(f"\n建設系フィルタ結果:")
print(f"  コードマッチ: {code_match.sum()}件")
print(f"  キーワードマッチ: {keyword_match.sum()}件")
print(f"  ハイブリッド(OR): {len(hw_construction)}件")

# ========================================
# STEP 3: 電話番号正規化・重複排除
# ========================================
print("\n[STEP 3] 電話番号正規化・重複排除")

def normalize_phone(val):
    """電話番号を正規化（10-11桁、0始まり）"""
    if pd.isna(val) or not val:
        return ''
    digits = re.sub(r'[^\d]', '', str(val))
    # .0除去
    if digits.endswith('0') and '.' in str(val):
        digits = digits[:-1]
    if len(digits) >= 10 and len(digits) <= 11 and digits.startswith('0'):
        return digits
    return ''

# 電話番号列を検索
phone_col = None
for col in hw_construction.columns:
    if '選考担当者' in col and 'ＴＥＬ' in col and '携帯' not in col:
        phone_col = col
        break

if not phone_col:
    # 代替列名を検索
    for col in hw_construction.columns:
        if 'TEL' in col.upper() or '電話' in col:
            phone_col = col
            break

if not phone_col:
    print("❌ エラー: 電話番号列が見つかりません")
    sys.exit(1)

print(f"電話番号列: {phone_col}")

hw_construction['phone_norm'] = hw_construction[phone_col].apply(normalize_phone)

# 電話番号必須
has_phone = hw_construction['phone_norm'] != ''
hw_with_phone = hw_construction[has_phone].copy()
print(f"電話番号あり: {len(hw_with_phone)}件（元: {len(hw_construction)}件）")

# 電話番号重複排除（最初のレコードを保持）
hw_deduped = hw_with_phone.drop_duplicates(subset='phone_norm', keep='first')
print(f"重複排除後: {len(hw_deduped)}件")

# ========================================
# STEP 4: 成約先・電話済み除外
# ========================================
print("\n[STEP 4] 成約先・電話済み除外")

# Pickle読み込み
with open(base / 'data/output/media_matching/contract_phones.pkl', 'rb') as f:
    contract_phones = pickle.load(f)
with open(base / 'data/output/media_matching/called_phones.pkl', 'rb') as f:
    called_phones = pickle.load(f)

# 法人番号でも成約先チェック
from api.salesforce_client import SalesforceClient
client = SalesforceClient()
client.authenticate()

# SOQL クエリで成約先法人番号を取得
contract_query = """
SELECT CorporateNumber__c, CorporateIdentificationNumber__c
FROM Account
WHERE Status__c LIKE '%商談中%'
   OR Status__c LIKE '%プロジェクト進行中%'
   OR Status__c LIKE '%深耕対象%'
   OR Status__c LIKE '%過去客%'
   OR RelatedAccountFlg__c = 'グループ案件進行中'
   OR RelatedAccountFlg__c = 'グループ過去案件実績あり'
"""

# REST API で直接クエリを実行
query_url = f"{client.instance_url}/services/data/{client.api_version}/query"
params = {'q': contract_query}
headers = client._get_headers()

contract_corps = set()
response = requests.get(query_url, headers=headers, params=params)
response.raise_for_status()
result = response.json()

for r in result.get('records', []):
    for field in ['CorporateNumber__c', 'CorporateIdentificationNumber__c']:
        val = r.get(field)
        if val:
            num = re.sub(r'[^0-9]', '', str(val))
            if len(num) >= 10:
                contract_corps.add(num)

# 次のページがあれば取得
while not result.get('done', True):
    next_url = f"{client.instance_url}{result['nextRecordsUrl']}"
    response = requests.get(next_url, headers=headers)
    response.raise_for_status()
    result = response.json()

    for r in result.get('records', []):
        for field in ['CorporateNumber__c', 'CorporateIdentificationNumber__c']:
            val = r.get(field)
            if val:
                num = re.sub(r'[^0-9]', '', str(val))
                if len(num) >= 10:
                    contract_corps.add(num)

print(f"成約先電話番号: {len(contract_phones)}件")
print(f"成約先法人番号: {len(contract_corps)}件")
print(f"電話済みリスト: {len(called_phones)}件")

# 法人番号列を検索
corp_col = None
for col in hw_deduped.columns:
    if '法人番号' in col:
        corp_col = col
        break

def normalize_corp(val):
    """法人番号を正規化"""
    if pd.isna(val) or not val:
        return ''
    num = re.sub(r'[^0-9]', '', str(val))
    # .0除去
    if '.' in str(val):
        num = num.rstrip('0')
    return num if len(num) >= 10 else ''

if corp_col:
    hw_deduped = hw_deduped.copy()
    hw_deduped['corp_norm'] = hw_deduped[corp_col].apply(normalize_corp)
    print(f"法人番号列: {corp_col}")
else:
    hw_deduped = hw_deduped.copy()
    hw_deduped['corp_norm'] = ''
    print("法人番号列: なし")

# 成約先除外（電話番号 OR 法人番号）
is_contract_phone = hw_deduped['phone_norm'].isin(contract_phones)
is_contract_corp = hw_deduped['corp_norm'].isin(contract_corps) & (hw_deduped['corp_norm'] != '')
is_contract = is_contract_phone | is_contract_corp

excluded_contract = hw_deduped[is_contract]
hw_deduped = hw_deduped[~is_contract]
print(f"\n成約先除外: {len(excluded_contract)}件")
print(f"  - 電話番号一致: {is_contract_phone.sum()}件")
print(f"  - 法人番号一致: {is_contract_corp.sum()}件")

# 電話済み除外
is_called = hw_deduped['phone_norm'].isin(called_phones)
excluded_called = hw_deduped[is_called]
hw_deduped = hw_deduped[~is_called]
print(f"電話済み除外: {len(excluded_called)}件")

print(f"\n除外後: {len(hw_deduped)}件")

# ========================================
# STEP 5: Salesforce突合
# ========================================
print("\n[STEP 5] Salesforce突合")

# SF電話番号辞書読み込み
with open(base / 'data/output/media_matching/lead_phones.pkl', 'rb') as f:
    lead_phones = pickle.load(f)
with open(base / 'data/output/media_matching/account_phones.pkl', 'rb') as f:
    account_phones = pickle.load(f)
with open(base / 'data/output/media_matching/contact_phones.pkl', 'rb') as f:
    contact_phones = pickle.load(f)

new_leads = []
lead_updates = []
account_updates = []

for idx, row in hw_deduped.iterrows():
    phone = row['phone_norm']

    if phone in lead_phones:
        lead_updates.append({'hw_row': row, 'matched': lead_phones[phone]})
    elif phone in account_phones:
        account_updates.append({'hw_row': row, 'matched': account_phones[phone]})
    elif phone in contact_phones:
        account_updates.append({'hw_row': row, 'matched': contact_phones[phone]})
    else:
        new_leads.append(row)

print(f"\nSalesforce突合結果:")
print(f"  新規Lead候補: {len(new_leads)}件")
print(f"  既存Lead更新: {len(lead_updates)}件")
print(f"  既存Account更新: {len(account_updates)}件")

# ========================================
# STEP 6: 新規リードCSV生成
# ========================================
print("\n[STEP 6] 新規リードCSV生成")

# 所有者割り当て（建設系: 3名均等）
owners = [
    {'name': '佐藤丈太郎', 'id': '0055i00000CwGDGAA3'},
    {'name': '志村亮介', 'id': '0055i00000CwGCrAAN'},
    {'name': '小林幸太', 'id': '005J3000000ERz4IAG'}
]

# 列名マッピング検索
def find_column(keywords, columns):
    """キーワードで列を検索"""
    for keyword in keywords:
        for col in columns:
            if keyword in col:
                return col
    return None

# 主要列を検索（完全一致）
company_col = '事業所名漢字'
lastname_col = '選考担当者氏名漢字'
mobile_col = '選考担当者ＴＥＬ(携帯)'
postal_col = '事業所郵便番号'
street_col = '事業所所在地'
employees_col = '従業員数企業全体'
establish_col = '創業設立年（西暦）'
website_col = '事業所ホームページ'
title_col = '選考担当者課係名／役職名'
name_kana_col = '事業所名カナ'
president_name_col = '代表者名'
president_title_col = '代表者役職'
industry_col = '産業分類（名称）'
job_type_col = '職種'
employment_col = '雇用形態'
recruitment_col = '採用人数'

print(f"\nフィールドマッピング:")
print(f"  会社名: {company_col}")
print(f"  担当者: {lastname_col}")
print(f"  携帯電話: {mobile_col}")
print(f"  従業員数: {employees_col}")
print(f"  法人番号: {corp_col}")

def extract_prefecture(address):
    """住所から都道府県を抽出"""
    if pd.isna(address) or not address:
        return ''
    prefectures = [
        '北海道', '青森県', '岩手県', '宮城県', '秋田県', '山形県', '福島県',
        '茨城県', '栃木県', '群馬県', '埼玉県', '千葉県', '東京都', '神奈川県',
        '新潟県', '富山県', '石川県', '福井県', '山梨県', '長野県', '岐阜県',
        '静岡県', '愛知県', '三重県', '滋賀県', '京都府', '大阪府', '兵庫県',
        '奈良県', '和歌山県', '鳥取県', '島根県', '岡山県', '広島県', '山口県',
        '徳島県', '香川県', '愛媛県', '高知県', '福岡県', '佐賀県', '長崎県',
        '熊本県', '大分県', '宮崎県', '鹿児島県', '沖縄県'
    ]
    for pref in prefectures:
        if address.startswith(pref):
            return pref
    return ''

def is_mobile_phone(phone):
    """携帯電話番号判定（070/080/090始まり）"""
    return phone.startswith('070') or phone.startswith('080') or phone.startswith('090')

def clean_value(val, max_length=None):
    """値のクリーニング（.0除去、長さ制限）"""
    if pd.isna(val) or not val:
        return ''
    val_str = str(val)
    # .0除去
    if val_str.endswith('.0'):
        val_str = val_str[:-2]
    # 長さ制限
    if max_length and len(val_str) > max_length:
        val_str = val_str[:max_length]
    return val_str

# CSV生成
sf_leads = []
skipped_no_company = 0
skipped_no_phone = 0

for idx, row in enumerate(new_leads):
    # 必須フィールドチェック
    company = clean_value(row.get(company_col))
    phone = row['phone_norm']

    if not company:
        skipped_no_company += 1
        continue

    if not phone:
        skipped_no_phone += 1
        continue

    # 所有者割り当て（ラウンドロビン）
    owner = owners[idx % len(owners)]

    # LastName
    lastname = clean_value(row.get(lastname_col))
    if not lastname:
        lastname = '担当者'

    # Phone/MobilePhone（携帯の場合は両方に入れる）
    mobile = clean_value(row.get(mobile_col))
    if is_mobile_phone(phone):
        phone_field = phone
        mobile_field = phone
    else:
        phone_field = phone
        mobile_field = mobile if is_mobile_phone(mobile) else ''

    # 住所・都道府県
    street = clean_value(row.get(street_col))
    prefecture = extract_prefecture(street)

    # 従業員数（整数）
    employees = clean_value(row.get(employees_col))
    if employees:
        try:
            employees = str(int(float(employees)))
        except:
            employees = ''

    # 法人番号（13文字制限）
    corp_num = clean_value(row.get('corp_norm'), max_length=13)

    # メモ欄
    memo = f"【新規作成】ハローワーク建設系突合 {today}"

    sf_lead = {
        'Company': company,
        'LastName': lastname,
        'Phone': phone_field,
        'MobilePhone': mobile_field,
        'PostalCode': clean_value(row.get(postal_col)),
        'Street': street,
        'Prefecture__c': prefecture,
        'NumberOfEmployees': employees,
        'CorporateNumber__c': corp_num,
        'Establish__c': clean_value(row.get(establish_col)),
        'Website': clean_value(row.get(website_col)),
        'Title': clean_value(row.get(title_col)),
        'Name_Kana__c': clean_value(row.get(name_kana_col)),
        'PresidentName__c': clean_value(row.get(president_name_col)),
        'PresidentTitle__c': clean_value(row.get(president_title_col)),
        'LeadSource': 'Other',
        'Hellowork_DataImportDate__c': today,
        'Hellowork_Industry__c': clean_value(row.get(industry_col)),
        'Hellowork_RecuritmentType__c': clean_value(row.get(job_type_col)),
        'Hellowork_EmploymentType__c': clean_value(row.get(employment_col)),
        'Hellowork_NumberOfRecruitment__c': clean_value(row.get(recruitment_col)),
        'LeadSourceMemo__c': memo,
        'OwnerId': owner['id']
    }

    sf_leads.append(sf_lead)

print(f"\n新規リード生成結果:")
print(f"  生成成功: {len(sf_leads)}件")
print(f"  スキップ（会社名なし）: {skipped_no_company}件")
print(f"  スキップ（電話番号なし）: {skipped_no_phone}件")

# 所有者別内訳
owner_counts = defaultdict(int)
for lead in sf_leads:
    owner_id = lead['OwnerId']
    owner_name = next(o['name'] for o in owners if o['id'] == owner_id)
    owner_counts[owner_name] += 1

print(f"\n所有者別内訳:")
for owner_name, count in sorted(owner_counts.items()):
    print(f"  {owner_name}: {count}件")

# CSV保存
if sf_leads:
    df_leads = pd.DataFrame(sf_leads)

    # 電話番号フィールドを文字列として保持（先頭0を保護）
    phone_fields = ['Phone', 'MobilePhone', 'PostalCode', 'CorporateNumber__c']
    for field in phone_fields:
        if field in df_leads.columns:
            df_leads[field] = df_leads[field].astype(str).replace('nan', '')

    output_path = base / f'data/output/hellowork/construction_new_leads_{today.replace("-", "")}.csv'
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df_leads.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"\n✅ CSV保存: {output_path}")

    # サンプル表示
    print(f"\nサンプル（先頭5件）:")
    for i, lead in enumerate(sf_leads[:5]):
        print(f"  {i+1}. {lead['Company']} / {lead['Phone']} / {lead['OwnerId']}")
else:
    print("\n⚠️ 新規リードなし")

# ========================================
# STEP 7: サマリー出力
# ========================================
print("\n" + "="*80)
print("処理サマリー")
print("="*80)
print(f"元データ: {len(hw)}件")
print(f"建設系フィルタ後: {len(hw_construction)}件")
print(f"電話番号あり: {len(hw_with_phone)}件")
print(f"重複排除後: {len(hw_deduped)}件")
print(f"成約先除外: {len(excluded_contract)}件")
print(f"電話済み除外: {len(excluded_called)}件")
print(f"除外後: {len(hw_deduped)}件")
print(f"\nSalesforce突合:")
print(f"  新規Lead: {len(sf_leads)}件（スキップ: {skipped_no_company + skipped_no_phone}件）")
print(f"  既存Lead更新: {len(lead_updates)}件")
print(f"  既存Account更新: {len(account_updates)}件")
print("="*80)

# 結果をPickle保存
results = {
    'total_records': len(hw),
    'construction_filtered': len(hw_construction),
    'with_phone': len(hw_with_phone),
    'after_dedup': len(hw_deduped),
    'excluded_contract': len(excluded_contract),
    'excluded_called': len(excluded_called),
    'new_leads': len(sf_leads),
    'lead_updates': len(lead_updates),
    'account_updates': len(account_updates),
    'skipped': {
        'no_company': skipped_no_company,
        'no_phone': skipped_no_phone
    },
    'owner_counts': dict(owner_counts),
    'date': today
}

pickle_path = base / f'data/output/hellowork/construction_results_{today.replace("-", "")}.pkl'
with open(pickle_path, 'wb') as f:
    pickle.dump(results, f)
print(f"\n✅ 結果保存: {pickle_path}")

print("\n✅ パイプライン完了")
