"""
介護求人新規リードデータをハローワークデータと突合し、
従業員数・法人番号等を補完する。
突合キー: 電話番号 → 事業所名+住所（部分一致）
"""
import pandas as pd
import re
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')

# === 設定 ===
BASE_DIR = Path(r'C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List')
KAIGOJOB_FILE = BASE_DIR / 'data/output/google_scraping/kaigojob_new_leads_20260309.csv'
OUTPUT_DIR = BASE_DIR / 'data/output/google_scraping'

# ハローワークCSVファイル一覧
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

# 必要カラムのみ読み込む
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
    """施設名を正規化（比較用）"""
    if not name or pd.isna(name):
        return ''
    name = str(name).strip()
    # 全角→半角変換（数字・英字）
    name = name.translate(str.maketrans(
        'ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ０１２３４５６７８９',
        'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'
    ))
    # スペース・記号除去
    name = re.sub(r'[\s　・\-\.\(\)（）【】「」\u3000]+', '', name)
    return name.lower()


def load_hellowork_data():
    """全ハローワークファイルから必要データを読み込み、電話番号・名前インデックスを構築"""
    all_records = []
    phone_index = {}  # phone -> list of record indices
    name_index = {}   # normalized_name -> list of record indices

    for hw_file in HW_FILES:
        if not hw_file.exists():
            print(f"  スキップ（ファイルなし）: {hw_file.name}")
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

                    # 電話番号インデックス
                    phone = normalize_phone(record.get('選考担当者ＴＥＬ'))
                    if phone:
                        if phone not in phone_index:
                            phone_index[phone] = []
                        phone_index[phone].append(idx)

                    # 事業所名インデックス
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
    print("介護求人新規リード × ハローワーク 従業員数補完")
    print("=" * 70)

    # === 介護求人データ読み込み ===
    df_kaigo = pd.read_csv(KAIGOJOB_FILE, encoding='utf-8-sig', dtype={'phone_normalized': str})
    # 先頭0復元
    df_kaigo['phone_normalized'] = df_kaigo['phone_normalized'].apply(
        lambda x: '0' + str(x) if pd.notna(x) and str(x)[0] != '0' else str(x) if pd.notna(x) else x
    )
    print(f"\n介護求人新規リード: {len(df_kaigo)}件")

    # === ハローワークデータ読み込み ===
    print("\n--- ハローワークデータ読み込み ---")
    hw_records, phone_index, name_index = load_hellowork_data()

    # === 突合処理 ===
    print("\n--- 突合処理 ---")
    enriched = []
    match_stats = {'phone': 0, 'name': 0, 'none': 0}

    for idx, row in df_kaigo.iterrows():
        phone = normalize_phone(row.get('phone_normalized'))
        company = str(row.get('company_name', ''))
        facility = str(row.get('facility_name', ''))

        hw_match = None
        match_method = 'none'

        # 1. 電話番号で突合
        if phone and phone in phone_index:
            hw_idx = phone_index[phone][0]
            hw_match = hw_records[hw_idx]
            match_method = 'phone'

        # 2. 事業所名で突合（電話番号で見つからない場合）
        if hw_match is None:
            for name_candidate in [company, facility]:
                norm = normalize_name(name_candidate)
                if norm and len(norm) >= 3 and norm in name_index:
                    hw_idx = name_index[norm][0]
                    hw_match = hw_records[hw_idx]
                    match_method = 'name'
                    break

        match_stats[match_method] += 1

        result = row.to_dict()
        if hw_match:
            result['hw_match_method'] = match_method
            result['hw_company_name'] = hw_match.get('事業所名漢字', '')
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
            for field in ['hw_company_name', 'hw_employees_total', 'hw_employees_location',
                          'hw_corporate_number', 'hw_president_name', 'hw_president_title',
                          'hw_capital', 'hw_established', 'hw_business_content',
                          'hw_industry', 'hw_name_kana']:
                result[field] = ''

        enriched.append(result)

    df_enriched = pd.DataFrame(enriched)

    # === 結果サマリー ===
    print("\n" + "=" * 70)
    print("突合結果")
    print("=" * 70)
    print(f"  入力: {len(df_kaigo)}件")
    print(f"  電話番号一致: {match_stats['phone']}件")
    print(f"  事業所名一致: {match_stats['name']}件")
    print(f"  合計一致: {match_stats['phone'] + match_stats['name']}件 ({(match_stats['phone'] + match_stats['name'])/len(df_kaigo)*100:.1f}%)")
    print(f"  未一致: {match_stats['none']}件")

    # 従業員数の分布（一致した分）
    matched = df_enriched[df_enriched['hw_match_method'] != 'none']
    if len(matched) > 0:
        employees = pd.to_numeric(matched['hw_employees_total'], errors='coerce')
        valid_emp = employees.dropna()
        print(f"\n--- 従業員数分布（{len(valid_emp)}件） ---")
        if len(valid_emp) > 0:
            bins = [0, 10, 30, 50, 100, 300, 1000, float('inf')]
            labels = ['1-10', '11-30', '31-50', '51-100', '101-300', '301-1000', '1001+']
            emp_dist = pd.cut(valid_emp, bins=bins, labels=labels).value_counts().sort_index()
            for label, cnt in emp_dist.items():
                print(f"  {label}名: {cnt}件")

    # === 出力 ===
    output_file = OUTPUT_DIR / 'kaigojob_new_leads_enriched_20260309.csv'
    df_enriched.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"\n出力: {output_file}")

    # 未一致分も別途出力
    unmatched = df_enriched[df_enriched['hw_match_method'] == 'none']
    if len(unmatched) > 0:
        unmatched_file = OUTPUT_DIR / 'kaigojob_new_leads_no_hw_match_20260309.csv'
        unmatched.to_csv(unmatched_file, index=False, encoding='utf-8-sig')
        print(f"未一致: {unmatched_file} ({len(unmatched)}件)")

    # サンプル表示
    if len(matched) > 0:
        print(f"\n=== 一致サンプル（先頭10件） ===")
        for i, row in matched.head(10).iterrows():
            print(f"  {row.get('company_name',''):30s} | HW:{row.get('hw_company_name',''):30s} | 従業員:{row.get('hw_employees_total','')} | 法人番号:{row.get('hw_corporate_number','')}")

    return df_enriched


if __name__ == '__main__':
    main()
