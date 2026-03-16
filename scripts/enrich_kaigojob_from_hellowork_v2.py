"""
介護求人新規リードデータをハローワークデータと突合し、従業員数・法人番号等を補完する。
突合キー:
  1. 電話番号一致（最優先）
  2. 事業所名一致 + 住所部分一致（検証付き）
"""
import pandas as pd
import re
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')

BASE_DIR = Path(r'C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List')
KAIGOJOB_FILE = BASE_DIR / 'data/output/google_scraping/kaigojob_new_leads_20260309.csv'
OUTPUT_DIR = BASE_DIR / 'data/output/google_scraping'

HW_FILES = [
    Path(r'C:\Users\fuji1\OneDrive\デスクトップ\RCMEB002002_M100.csv'),
    Path(r'C:\Users\fuji1\OneDrive\デスクトップ\RCMEB002002_M100 (2).csv'),
    Path(r'C:\Users\fuji1\OneDrive\デスクトップ\RCMEB002002_M100 (3).csv'),
    Path(r'C:\Users\fuji1\OneDrive\デスクトップ\ハローワーク2025.02.25\RCMEB002002_M100.csv'),
    Path(r'C:\Users\fuji1\OneDrive\デスクトップ\ハローワーク2025.02.25\RCMEB002002_M100 (2).csv'),
    Path(r'C:\Users\fuji1\OneDrive\デスクトップ\ハローワーク2025.02.25\RCMEB002002_M100 (3).csv'),
    Path(r'C:\Users\fuji1\OneDrive\デスクトップ\ハローワーク2025.02.25\RCMEB002002_M100 (4).csv'),
    Path(r'C:\Users\fuji1\OneDrive\デスクトップ\ハローワーク2025.02.25\RCMEB002002_M100①.csv'),
]

HW_USECOLS = [
    '事業所名漢字', '事業所所在地', '事業所郵便番号', '選考担当者ＴＥＬ',
    '従業員数企業全体', '従業員数就業場所', '法人番号',
    '代表者名', '代表者役職', '資本金', '創業設立年（西暦）',
    '事業内容', '事業所名カナ', '産業分類（名称）',
]


def normalize_phone(phone_str):
    if not phone_str or pd.isna(phone_str):
        return None
    digits = re.sub(r'[^\d]', '', str(phone_str))
    if 10 <= len(digits) <= 11:
        return digits
    return None


def normalize_name(name):
    if not name or pd.isna(name):
        return ''
    name = str(name).strip()
    name = name.translate(str.maketrans(
        'ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ０１２３４５６７８９',
        'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'
    ))
    name = re.sub(r'[\s　・\-\.\(\)（）【】「」\u3000]+', '', name)
    return name.lower()


def extract_city_from_address(addr):
    """住所から市区町村を抽出"""
    if not addr or pd.isna(addr):
        return ''
    addr = str(addr)
    # 〒除去
    addr = re.sub(r'〒?\d{3}[-\-]?\d{4}\s*', '', addr)
    # 都道府県除去
    addr = re.sub(r'^(東京都|北海道|(?:京都|大阪)府|.{2,3}県)', '', addr)
    # 市区町村を抽出
    match = re.match(r'(.+?[市区町村郡])', addr)
    if match:
        return match.group(1).strip()
    return addr[:6].strip()


def address_match(kaigo_addr, kaigo_location, hw_addr):
    """住所の部分一致チェック"""
    # kaigo側の市区町村
    kaigo_city = ''
    if kaigo_addr and pd.notna(kaigo_addr) and str(kaigo_addr) != 'nan':
        kaigo_city = extract_city_from_address(str(kaigo_addr))
    if not kaigo_city and kaigo_location and pd.notna(kaigo_location):
        kaigo_city = str(kaigo_location).strip()

    # HW側の市区町村
    hw_city = extract_city_from_address(hw_addr) if hw_addr and pd.notna(hw_addr) else ''

    if not kaigo_city or not hw_city:
        return False

    # 正規化
    kaigo_city = re.sub(r'[\s　]+', '', kaigo_city)
    hw_city = re.sub(r'[\s　]+', '', hw_city)

    # 部分一致チェック
    if kaigo_city in hw_city or hw_city in kaigo_city:
        return True

    # 市名だけで比較（「札幌市中央区」→「札幌市」）
    kaigo_base = re.match(r'(.+?市)', kaigo_city)
    hw_base = re.match(r'(.+?市)', hw_city)
    if kaigo_base and hw_base and kaigo_base.group(1) == hw_base.group(1):
        return True

    return False


def load_hellowork_data():
    all_records = []
    phone_index = {}
    name_index = {}

    for hw_file in HW_FILES:
        if not hw_file.exists():
            print(f"  スキップ: {hw_file.name}")
            continue
        print(f"  読み込み中: {hw_file.name}...", end='', flush=True)
        try:
            chunks = pd.read_csv(hw_file, encoding='cp932', usecols=HW_USECOLS,
                                 dtype=str, chunksize=50000)
            file_count = 0
            for chunk in chunks:
                for _, row in chunk.iterrows():
                    idx = len(all_records)
                    record = row.to_dict()
                    all_records.append(record)

                    phone = normalize_phone(record.get('選考担当者ＴＥＬ'))
                    if phone:
                        if phone not in phone_index:
                            phone_index[phone] = []
                        phone_index[phone].append(idx)

                    name = normalize_name(record.get('事業所名漢字'))
                    if name and len(name) >= 3:
                        if name not in name_index:
                            name_index[name] = []
                        name_index[name].append(idx)

                    file_count += 1
            print(f" {file_count:,}件")
        except Exception as e:
            print(f" エラー: {e}")

    print(f"  合計: {len(all_records):,}件")
    print(f"  電話番号インデックス: {len(phone_index):,}件")
    print(f"  事業所名インデックス: {len(name_index):,}件")
    return all_records, phone_index, name_index


def main():
    print("=" * 70)
    print("介護求人新規リード × ハローワーク 従業員数補完 v2")
    print("（事業所名+住所部分一致で検証）")
    print("=" * 70)

    df_kaigo = pd.read_csv(KAIGOJOB_FILE, encoding='utf-8-sig', dtype={'phone_normalized': str})
    df_kaigo['phone_normalized'] = df_kaigo['phone_normalized'].apply(
        lambda x: '0' + str(x) if pd.notna(x) and str(x)[0] != '0' else str(x) if pd.notna(x) else x
    )
    print(f"\n介護求人新規リード: {len(df_kaigo)}件")

    print("\n--- ハローワークデータ読み込み ---")
    hw_records, phone_index, name_index = load_hellowork_data()

    print("\n--- 突合処理 ---")
    enriched = []
    match_stats = {'phone': 0, 'name_addr': 0, 'name_only_rejected': 0, 'none': 0}

    for idx, row in df_kaigo.iterrows():
        phone = normalize_phone(row.get('phone_normalized'))
        company = str(row.get('company_name', ''))
        facility = str(row.get('facility_name', ''))
        kaigo_addr = row.get('address', '')
        kaigo_loc = row.get('location', '')

        hw_match = None
        match_method = 'none'

        # 1. 電話番号一致
        if phone and phone in phone_index:
            hw_idx = phone_index[phone][0]
            hw_match = hw_records[hw_idx]
            match_method = 'phone'

        # 2. 事業所名一致 + 住所検証
        if hw_match is None:
            for name_candidate in [company, facility]:
                norm = normalize_name(name_candidate)
                if norm and len(norm) >= 3 and norm in name_index:
                    # 候補が複数ある場合、住所一致するものを探す
                    candidates = name_index[norm]
                    matched_candidate = None
                    for c_idx in candidates:
                        c_record = hw_records[c_idx]
                        hw_addr = c_record.get('事業所所在地', '')
                        if address_match(kaigo_addr, kaigo_loc, hw_addr):
                            matched_candidate = c_record
                            break

                    if matched_candidate:
                        hw_match = matched_candidate
                        match_method = 'name_addr'
                        break
                    else:
                        match_stats['name_only_rejected'] += 1

        match_stats[match_method] += 1

        result = row.to_dict()
        if hw_match:
            result['hw_match_method'] = match_method
            result['hw_company_name'] = hw_match.get('事業所名漢字', '')
            result['hw_address'] = hw_match.get('事業所所在地', '')
            result['hw_employees_total'] = hw_match.get('従業員数企業全体', '')
            result['hw_employees_location'] = hw_match.get('従業員数就業場所', '')
            result['hw_corporate_number'] = hw_match.get('法人番号', '')
            result['hw_president_name'] = hw_match.get('代表者名', '')
            result['hw_president_title'] = hw_match.get('代表者役職', '')
            result['hw_capital'] = hw_match.get('資本金', '')
            result['hw_established'] = hw_match.get('創業設立年（西暦）', '')
            result['hw_business_content'] = hw_match.get('事業内容', '')
            result['hw_industry'] = hw_match.get('産業分類（名称）', '')
            result['hw_name_kana'] = hw_match.get('事業所名カナ', '')
        else:
            result['hw_match_method'] = 'none'
            for field in ['hw_company_name', 'hw_address', 'hw_employees_total',
                          'hw_employees_location', 'hw_corporate_number',
                          'hw_president_name', 'hw_president_title', 'hw_capital',
                          'hw_established', 'hw_business_content', 'hw_industry',
                          'hw_name_kana']:
                result[field] = ''

        enriched.append(result)

    df_enriched = pd.DataFrame(enriched)

    # === 結果 ===
    print("\n" + "=" * 70)
    print("突合結果")
    print("=" * 70)
    print(f"  入力: {len(df_kaigo)}件")
    print(f"  電話番号一致: {match_stats['phone']}件")
    print(f"  事業所名+住所一致: {match_stats['name_addr']}件")
    total_match = match_stats['phone'] + match_stats['name_addr']
    print(f"  合計一致: {total_match}件 ({total_match/len(df_kaigo)*100:.1f}%)")
    print(f"  事業所名一致したが住所不一致で除外: {match_stats['name_only_rejected']}件")
    print(f"  未一致: {match_stats['none']}件")

    # 従業員数分布
    matched = df_enriched[df_enriched['hw_match_method'] != 'none']
    if len(matched) > 0:
        emp_raw = matched['hw_employees_total'].copy()
        emp_numeric = emp_raw.str.replace(r'[^\d]', '', regex=True)
        emp_numeric = pd.to_numeric(emp_numeric, errors='coerce')
        valid_emp = emp_numeric.dropna()
        print(f"\n--- 従業員数分布（{len(valid_emp)}件に有効値） ---")
        if len(valid_emp) > 0:
            bins = [0, 10, 30, 50, 100, 300, 1000, float('inf')]
            labels = ['1-10', '11-30', '31-50', '51-100', '101-300', '301-1000', '1001+']
            emp_dist = pd.cut(valid_emp, bins=bins, labels=labels).value_counts().sort_index()
            for label, cnt in emp_dist.items():
                if cnt > 0:
                    print(f"  {label}名: {cnt}件")

    # 出力
    output_file = OUTPUT_DIR / 'kaigojob_new_leads_enriched_20260309.csv'
    df_enriched.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"\n出力: {output_file}")

    unmatched = df_enriched[df_enriched['hw_match_method'] == 'none']
    if len(unmatched) > 0:
        unmatched_file = OUTPUT_DIR / 'kaigojob_new_leads_no_hw_match_20260309.csv'
        unmatched.to_csv(unmatched_file, index=False, encoding='utf-8-sig')
        print(f"未一致: {unmatched_file} ({len(unmatched)}件)")

    # サンプル表示
    if len(matched) > 0:
        print(f"\n=== 一致サンプル ===")
        print(f"--- 電話番号一致 ---")
        phone_matched = df_enriched[df_enriched['hw_match_method'] == 'phone']
        for i, row in phone_matched.head(5).iterrows():
            print(f"  {row.get('company_name',''):30s} | HW: {row.get('hw_company_name',''):30s} | 従業員: {row.get('hw_employees_total','')}")

        print(f"\n--- 事業所名+住所一致 ---")
        name_matched = df_enriched[df_enriched['hw_match_method'] == 'name_addr']
        for i, row in name_matched.head(10).iterrows():
            print(f"  {row.get('company_name',''):30s} | HW: {row.get('hw_company_name',''):30s} | 従業員: {row.get('hw_employees_total','')} | 勤務地: {row.get('location','')}")

    return df_enriched


if __name__ == '__main__':
    main()
