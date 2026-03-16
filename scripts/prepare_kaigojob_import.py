"""
介護求人 従業員数11-100名の101件をSalesforceインポート用CSVに変換する。
"""
import pandas as pd
import re
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')

BASE_DIR = Path(r'C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List')
INPUT_FILE = BASE_DIR / 'data/output/google_scraping/kaigojob_final_leads_20260309.csv'
OUTPUT_DIR = BASE_DIR / 'data/output/google_scraping'

# 所有者割り当て
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


def parse_emp(val):
    if not val or pd.isna(val) or str(val).strip() == '' or val == 'nan':
        return None
    digits = re.sub(r'[^\d]', '', str(val))
    return int(digits) if digits else None


def normalize_phone(p):
    if not p or pd.isna(p):
        return None
    p = str(p)
    if p[0] != '0':
        p = '0' + p
    digits = re.sub(r'[^\d]', '', p)
    return digits if 10 <= len(digits) <= 11 else None


def is_mobile(phone):
    if not phone:
        return False
    return phone[:3] in ('070', '080', '090')


def extract_prefecture(addr):
    """住所から都道府県を抽出"""
    if not addr or pd.isna(addr) or str(addr) == 'nan':
        return ''
    match = re.search(r'(東京都|北海道|(?:京都|大阪)府|.{2,3}県)', str(addr))
    return match.group(1) if match else ''


def extract_postal(addr):
    """住所から郵便番号を抽出"""
    if not addr or pd.isna(addr) or str(addr) == 'nan':
        return ''
    match = re.search(r'〒?(\d{3}[-\-]?\d{4})', str(addr))
    if match:
        return match.group(1).replace('−', '-').replace('ー', '-')
    return ''


def extract_street(addr):
    """住所から都道府県を除いた部分を抽出"""
    if not addr or pd.isna(addr) or str(addr) == 'nan':
        return ''
    addr = str(addr)
    addr = re.sub(r'〒?\d{3}[-\-ー]?\d{4}\s*', '', addr)
    addr = re.sub(r'^(東京都|北海道|(?:京都|大阪)府|.{2,3}県)', '', addr)
    return addr.strip()


def build_memo(row):
    """メモ欄を構築"""
    parts = ['【新規作成】介護求人媒体（kaigojob）突合 2026-03-09']
    parts.append(f'法人名: {row.get("法人名", "")}')
    parts.append(f'事業所名: {row.get("事業所名", "")}')
    parts.append(f'サービス種別: {row.get("サービス種別", "")}')

    emp = row.get('hw_employees_total', '')
    if emp and str(emp) != 'nan':
        parts.append(f'従業員数: {emp}')

    industry = row.get('hw_industry', '')
    if industry and str(industry) != 'nan':
        parts.append(f'産業分類: {industry}')

    biz = row.get('hw_business_content', '')
    if biz and str(biz) != 'nan':
        parts.append(f'事業内容: {str(biz)[:100]}')

    capital = row.get('hw_capital', '')
    if capital and str(capital) != 'nan':
        parts.append(f'資本金: {capital}')

    return '\n'.join(parts)


def main():
    print("=" * 70)
    print("介護求人 Salesforceインポート用CSV生成")
    print("=" * 70)

    df = pd.read_csv(INPUT_FILE, encoding='utf-8-sig', dtype=str)
    df['emp_num'] = df['hw_employees_total'].apply(parse_emp)

    # 従業員数11-100のみ
    target = df[(df['emp_num'] >= 11) & (df['emp_num'] <= 100)].copy()
    print(f"\n対象: {len(target)}件（従業員数11-100名）")

    # === フィールドマッピング ===
    records = []
    skipped_no_phone = 0
    skipped_no_company = 0

    for _, row in target.iterrows():
        phone = normalize_phone(row.get('phone'))
        if not phone:
            skipped_no_phone += 1
            continue

        company = str(row.get('法人名', ''))
        if not company or company == 'nan':
            company = str(row.get('企業・施設名', ''))
        if not company or company == 'nan':
            skipped_no_company += 1
            continue

        # 住所ソース（Google住所 or HW住所）
        addr = str(row.get('address', ''))
        if not addr or addr == 'nan':
            addr = str(row.get('hw_address', ''))

        # 電話番号分類
        phone_field = phone
        mobile_field = ''
        if is_mobile(phone):
            mobile_field = phone

        # 担当者名（HW代表者名があれば使用、なければ「担当者」）
        last_name = '担当者'
        title = ''
        president = str(row.get('hw_president_name', ''))
        if president and president != 'nan':
            # スペース含む場合は姓のみ
            name_parts = president.strip().split()
            if len(name_parts) >= 1:
                last_name = name_parts[0]
            president_title = str(row.get('hw_president_title', ''))
            if president_title and president_title != 'nan':
                title = president_title

        # 従業員数
        emp_str = str(row.get('hw_employees_total', ''))
        emp_num = parse_emp(emp_str)

        # 法人番号
        corp_num = str(row.get('hw_corporate_number', '')).replace('.0', '').strip()
        if corp_num == 'nan' or len(corp_num) < 10:
            corp_num = ''

        # 設立年
        established = str(row.get('hw_established', '')).replace('.0', '').strip()
        if established == 'nan':
            established = ''

        record = {
            'Company': company,
            'LastName': last_name,
            'Title': title,
            'Phone': phone_field,
            'MobilePhone': mobile_field,
            'PostalCode': extract_postal(addr),
            'Prefecture__c': extract_prefecture(addr),
            'Street': extract_street(addr),
            'NumberOfEmployees': emp_num if emp_num else '',
            'CorporateNumber__c': corp_num,
            'Establish__c': established,
            'PresidentName__c': president if president != 'nan' else '',
            'PresidentTitle__c': title,
            'Website': '',
            'Name_Kana__c': str(row.get('hw_name_kana', '')) if str(row.get('hw_name_kana', '')) != 'nan' else '',
            'Description': build_memo(row),
            'LeadSource': '有料媒体',
        }
        records.append(record)

    print(f"  有効レコード: {len(records)}件")
    print(f"  スキップ（Phone空）: {skipped_no_phone}件")
    print(f"  スキップ（Company空）: {skipped_no_company}件")

    df_import = pd.DataFrame(records)

    # === 所有者割り当て ===
    print("\n--- 所有者割り当て ---")
    n = len(df_import)
    # 101件 → 深堀・服部は少数、人材開発7名に均等分配
    # 深堀30%、服部30%、人材開発40%くらい
    fukahori_n = n * 30 // 100
    hattori_n = n * 30 // 100
    jinzai_n = n - fukahori_n - hattori_n

    per_member = jinzai_n // 7
    remainder = jinzai_n % 7

    owners = []
    owner_names = []

    # 深堀
    for _ in range(fukahori_n):
        owners.append(FUKAHORI[1])
        owner_names.append(FUKAHORI[0])

    # 服部
    for _ in range(hattori_n):
        owners.append(HATTORI[1])
        owner_names.append(HATTORI[0])

    # 人材開発（均等）
    member_list = list(OWNERS.items())
    for i, (name, uid) in enumerate(member_list):
        count = per_member + (1 if i < remainder else 0)
        for _ in range(count):
            owners.append(uid)
            owner_names.append(name)

    df_import['OwnerId'] = owners[:n]
    df_import['owner_name'] = owner_names[:n]

    # 所有者別集計
    print(f"  総件数: {n}件")
    owner_dist = pd.Series(owner_names[:n]).value_counts()
    for name, cnt in owner_dist.items():
        print(f"    {name}: {cnt}件")

    # === CSV出力 ===
    # Salesforceインポート用（owner_name除外）
    import_file = OUTPUT_DIR / 'kaigojob_sf_import_20260309.csv'
    df_import.drop(columns=['owner_name']).to_csv(import_file, index=False, encoding='utf-8-sig')
    print(f"\nSFインポート用CSV: {import_file}")

    # 確認用（owner_name付き）
    review_file = OUTPUT_DIR / 'kaigojob_sf_import_review_20260309.csv'
    df_import.to_csv(review_file, index=False, encoding='utf-8-sig')
    print(f"確認用CSV: {review_file}")

    # === プレビュー ===
    print("\n" + "=" * 70)
    print("インポートプレビュー")
    print("=" * 70)
    print(f"\n対象: Lead（リード）新規作成")
    print(f"件数: {len(df_import)}件")
    print(f"更新フィールド: Company, LastName, Title, Phone, MobilePhone,")
    print(f"  PostalCode, Prefecture__c, Street, NumberOfEmployees,")
    print(f"  CorporateNumber__c, Establish__c, PresidentName__c,")
    print(f"  PresidentTitle__c, Name_Kana__c, Description, LeadSource, OwnerId")

    print(f"\nサンプル（5件）:")
    for _, row in df_import.head(5).iterrows():
        print(f"  {row['Company']:30s} | {row['LastName']:6s} | {row['Phone']:12s} | 従業員:{row['NumberOfEmployees']} | {row['owner_name']}")

    return df_import


if __name__ == '__main__':
    main()
