"""
介護求人（ウェルミージョブ）従業員数11-100名 → Salesforceインポート用CSV生成 v2
- 所有者: 藤巻 真弥
- メモ欄: ウェルミージョブ、求人入稿日、スクレイピングデータ全項目、ハローワーク突合済み
- フィールド: ハローワークと同等（従業員数、代表者名、役職等）
"""
import pandas as pd
import re
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')

BASE_DIR = Path(r'C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List')
FINAL_FILE = BASE_DIR / 'data/output/google_scraping/kaigojob_final_leads_20260309.csv'
DETAIL_FILE = Path(r'C:\Users\fuji1\OneDrive\デスクトップ\kaigojob_data.csv')
OUTPUT_DIR = BASE_DIR / 'data/output/google_scraping'

FUJIMAKI_ID = '0055i00000BeOKbAAN'
SCRAPING_DATE = '2026-03-09'


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
    return phone and phone[:3] in ('070', '080', '090')


def extract_prefecture(addr):
    if not addr or pd.isna(addr) or str(addr) == 'nan':
        return ''
    match = re.search(r'(東京都|北海道|(?:京都|大阪)府|.{2,3}県)', str(addr))
    return match.group(1) if match else ''


def extract_postal(addr):
    if not addr or pd.isna(addr) or str(addr) == 'nan':
        return ''
    match = re.search(r'〒?(\d{3}[-\-−]?\d{4})', str(addr))
    if match:
        return match.group(1).replace('−', '-').replace('ー', '-')
    return ''


def extract_street(addr):
    if not addr or pd.isna(addr) or str(addr) == 'nan':
        return ''
    addr = str(addr)
    addr = re.sub(r'〒?\d{3}[-\-ー−]?\d{4}\s*', '', addr)
    addr = re.sub(r'^(東京都|北海道|(?:京都|大阪)府|.{2,3}県)', '', addr)
    return addr.strip()


def safe_str(val):
    if val is None or pd.isna(val) or str(val) == 'nan':
        return ''
    return str(val).strip()


def build_memo(row):
    """メモ欄構築 - ウェルミージョブ情報 + スクレイピングデータ + HW突合情報"""
    parts = []

    # ヘッダ
    parts.append(f'【新規作成】ウェルミージョブ 求人入稿日: {SCRAPING_DATE}')
    parts.append(f'ハローワーク突合済み')
    parts.append('')

    # === スクレイピングデータ（求人情報）===
    parts.append('■ 求人情報（ウェルミージョブ）')
    parts.append(f'法人名: {safe_str(row.get("法人名"))}')
    parts.append(f'事業所名: {safe_str(row.get("事業所名"))}')
    parts.append(f'サービス区分: {safe_str(row.get("サービス区分"))}')
    parts.append(f'サービス種別: {safe_str(row.get("サービス種別"))}')
    parts.append(f'募集職種: {safe_str(row.get("募集職種"))}')
    parts.append(f'給与: {safe_str(row.get("給与"))}')
    parts.append(f'雇用形態: {safe_str(row.get("雇用形態"))}')
    parts.append(f'勤務地: {safe_str(row.get("勤務地"))}')

    quals = safe_str(row.get('応募資格'))
    if quals:
        parts.append(f'応募資格: {quals[:200]}')

    job_content = safe_str(row.get('仕事内容'))
    if job_content:
        parts.append(f'仕事内容: {job_content[:300]}')

    work_hours = safe_str(row.get('勤務時間'))
    if work_hours:
        parts.append(f'勤務時間: {work_hours[:200]}')

    benefits = safe_str(row.get('福利厚生'))
    if benefits:
        parts.append(f'福利厚生: {benefits[:200]}')

    url = safe_str(row.get('URL'))
    if url:
        parts.append(f'求人URL: {url}')

    parts.append('')

    # === ハローワーク突合情報 ===
    parts.append('■ ハローワーク突合データ')
    hw_method = safe_str(row.get('hw_match_method'))
    parts.append(f'突合方法: {hw_method}')

    emp_total = safe_str(row.get('hw_employees_total'))
    if emp_total:
        parts.append(f'従業員数（企業全体）: {emp_total}')

    emp_loc = safe_str(row.get('hw_employees_location'))
    if emp_loc:
        parts.append(f'従業員数（就業場所）: {emp_loc}')

    corp_num = safe_str(row.get('hw_corporate_number'))
    if corp_num:
        parts.append(f'法人番号: {corp_num}')

    president = safe_str(row.get('hw_president_name'))
    if president:
        parts.append(f'代表者名: {president}')

    pres_title = safe_str(row.get('hw_president_title'))
    if pres_title:
        parts.append(f'代表者役職: {pres_title}')

    capital = safe_str(row.get('hw_capital'))
    if capital:
        parts.append(f'資本金: {capital}')

    established = safe_str(row.get('hw_established'))
    if established:
        parts.append(f'設立年: {established}')

    industry = safe_str(row.get('hw_industry'))
    if industry:
        parts.append(f'産業分類: {industry}')

    biz = safe_str(row.get('hw_business_content'))
    if biz:
        parts.append(f'事業内容: {biz[:200]}')

    return '\n'.join(parts)


def main():
    print("=" * 70)
    print("ウェルミージョブ Salesforceインポート用CSV生成 v2")
    print("所有者: 藤巻 真弥（全件）")
    print("=" * 70)

    # 最終リードデータ読み込み
    df = pd.read_csv(FINAL_FILE, encoding='utf-8-sig', dtype=str)
    df['emp_num'] = df['hw_employees_total'].apply(parse_emp)

    # 従業員数11-100のみ
    target = df[(df['emp_num'] >= 11) & (df['emp_num'] <= 100)].copy()
    print(f"\n対象: {len(target)}件（従業員数11-100名）")

    # 元の求人詳細データを結合（応募資格、仕事内容、勤務時間、福利厚生）
    df_detail = pd.read_csv(DETAIL_FILE, encoding='utf-8-sig', dtype=str)
    # URLで結合
    detail_cols = ['URL', '応募資格', '仕事内容', '勤務時間', '福利厚生']
    # URL重複は最初のレコード
    df_detail_unique = df_detail[detail_cols].drop_duplicates(subset='URL', keep='first')
    target = target.merge(df_detail_unique, on='URL', how='left', suffixes=('', '_detail'))
    print(f"  求人詳細結合後: {len(target)}件")

    # === インポートレコード生成 ===
    records = []

    for _, row in target.iterrows():
        phone = normalize_phone(row.get('phone'))
        if not phone:
            continue

        company = safe_str(row.get('法人名'))
        if not company:
            company = safe_str(row.get('企業・施設名'))
        if not company:
            continue

        # 住所（Google住所 → HW住所）
        addr = safe_str(row.get('address'))
        if not addr:
            addr = safe_str(row.get('hw_address'))

        # 電話番号分類
        phone_field = phone
        mobile_field = phone if is_mobile(phone) else ''

        # 担当者名（HW代表者名）
        last_name = '担当者'
        title = ''
        president = safe_str(row.get('hw_president_name'))
        if president:
            name_parts = president.strip().split()
            if name_parts:
                last_name = name_parts[0]
            title = safe_str(row.get('hw_president_title'))

        # 従業員数
        emp_num = parse_emp(row.get('hw_employees_total'))

        # 法人番号
        corp_num = safe_str(row.get('hw_corporate_number')).replace('.0', '')
        if len(corp_num) < 10:
            corp_num = ''

        # 設立年
        established = safe_str(row.get('hw_established')).replace('.0', '')

        # カナ
        name_kana = safe_str(row.get('hw_name_kana'))

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
            'PresidentName__c': president,
            'PresidentTitle__c': title,
            'Name_Kana__c': name_kana,
            'Description': build_memo(row),
            'LeadSource': '有料媒体',
            'OwnerId': FUJIMAKI_ID,
        }
        records.append(record)

    df_import = pd.DataFrame(records)
    print(f"\n有効レコード: {len(df_import)}件")

    # === CSV出力 ===
    import_file = OUTPUT_DIR / 'kaigojob_sf_import_20260309.csv'
    df_import.to_csv(import_file, index=False, encoding='utf-8-sig')
    print(f"\nSFインポート用CSV: {import_file}")

    # === プレビュー ===
    print("\n" + "=" * 70)
    print("Salesforceインポート確認")
    print("=" * 70)
    print(f"\n対象: Lead（リード）新規作成")
    print(f"件数: {len(df_import)}件")
    print(f"所有者: 藤巻 真弥（全件）")
    print(f"LeadSource: 有料媒体")

    print(f"\nフィールド一覧:")
    for c in df_import.columns:
        non_empty = (df_import[c].notna() & (df_import[c] != '') & (df_import[c].astype(str) != 'nan')).sum()
        print(f"  {c}: {non_empty}/{len(df_import)}件")

    print(f"\nサンプル（5件）:")
    for _, row in df_import.head(5).iterrows():
        print(f"  {row['Company']:30s} | {row['LastName']:6s} | {row['Phone']:12s} | 従業員:{row['NumberOfEmployees']}")

    # メモ欄サンプル（1件）
    print(f"\n=== メモ欄サンプル ===")
    print(df_import.iloc[0]['Description'])

    return df_import


if __name__ == '__main__':
    main()
