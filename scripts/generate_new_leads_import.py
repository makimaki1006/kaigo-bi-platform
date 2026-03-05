"""新規リードインポートCSV生成スクリプト（フィルタ適用済み）"""
import sys
import io
import re
import pandas as pd
from datetime import datetime

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# === 設定 ===
INPUT_PATH = 'data/output/hellowork/true_new_leads.csv'
OUTPUT_PATH = 'data/output/hellowork/new_leads_import_20260227.csv'
TODAY = datetime.now().strftime('%Y-%m-%d')
BATCH_ID = 'BATCH_20260227_HW'

# === フィルタ定義 ===
TARGET_REASONS = ['欠員補充', '増員', '新規事業所設立']

TARGET_INDUSTRIES = [
    # 医療（歯科・薬局除く）
    '一般診療所', '病院', '助産・看護業',
    '医療に附帯するサービス業',
    'その他の保健衛生',
    # 介護・福祉
    '老人福祉・介護事業', '障害者福祉事業',
    'その他の社会保険・社会福祉・介護事業',
    '社会保険事業団体', '福祉事務所',
    # 児童福祉（放課後デイ含む）
    '児童福祉事業',
    # 施術（整骨院、鍼灸等）
    '施術業',
]

# 職業分類コード（中分類: 先頭3桁）でターゲット職種を絞り込み
TARGET_JOB_CODES = [
    '022',  # 保健師
    '023',  # 看護師・准看護師
    '024',  # 理学療法士・作業療法士等（歯科衛生士混在→キーワードで除外）
    '025',  # 栄養士・管理栄養士
    '026',  # 柔道整復師・鍼灸師
    '027',  # 臨床検査技師・放射線技師等
    '028',  # 看護助手（歯科助手混在→キーワードで除外）
    '030',  # 児童支援員・学童保育
    '037',  # 医療事務
    '049',  # 福祉相談・指導専門員
    '050',  # 介護職員
    '051',  # 訪問介護員
]

# コードフィルタ後に残ったNG職種をキーワードで除外
EXCLUDE_JOB_KW = ['歯科衛生士', '歯科助手', '歯科技工', '保育士', '保育教諭',
                  '保育補助', '幼稚園教諭', '薬剤師']
KEEP_JOB_KW = ['児童指導員', '放課後', '児童発達']

# 営業お断りキーワード（役職欄・メモ欄等に含まれる場合除外）
EXCLUDE_SALES_KW = ['お断り', 'ご相談ください', 'ハローワークへ', 'ハローワーク以外']

PREFECTURES = [
    '北海道','青森県','岩手県','宮城県','秋田県','山形県','福島県',
    '茨城県','栃木県','群馬県','埼玉県','千葉県','東京都','神奈川県',
    '新潟県','富山県','石川県','福井県','山梨県','長野県','岐阜県',
    '静岡県','愛知県','三重県','滋賀県','京都府','大阪府','兵庫県',
    '奈良県','和歌山県','鳥取県','島根県','岡山県','広島県','山口県',
    '徳島県','香川県','愛媛県','高知県','福岡県','佐賀県','長崎県',
    '熊本県','大分県','宮崎県','鹿児島県','沖縄県'
]


# === ユーティリティ関数 ===
def normalize_phone(phone):
    if pd.isna(phone) or not phone:
        return ''
    phone = re.sub(r'[^\d]', '', str(phone).strip())
    if phone.startswith('0') and 10 <= len(phone) <= 11:
        return phone
    return ''


def is_mobile(phone):
    return bool(phone) and phone[:3] in ('090', '080', '070')


def extract_prefecture(address):
    if pd.isna(address) or not address:
        return ''
    for pref in PREFECTURES:
        if str(address).startswith(pref):
            return pref
    return ''


def normalize_corp_number(val):
    if pd.isna(val) or not val:
        return ''
    val = str(val).replace('.0', '').strip()
    if len(val) <= 13 and val.isdigit():
        return val
    return ''


def parse_employees(val):
    if pd.isna(val) or not val:
        return ''
    val = str(val).replace('.0', '').strip()
    try:
        n = int(float(val))
        return str(n) if n > 0 else ''
    except Exception:
        return ''


def safe_str(val):
    if pd.isna(val) or str(val).strip() in ('', 'nan'):
        return ''
    return str(val).strip()


def parse_date(val):
    if pd.isna(val) or not val or str(val).strip() in ('', 'nan'):
        return ''
    val = str(val).strip()
    if '/' in val:
        parts = val.split('/')
        if len(parts) == 3:
            return f'{parts[0]}-{parts[1].zfill(2)}-{parts[2].zfill(2)}'
    return val


def should_exclude_nursery(job):
    """保育士・幼稚園教諭を除外（児童指導員・放課後は残す）"""
    if pd.isna(job):
        return False
    job = str(job)
    for kw in KEEP_JOB_KW:
        if kw in job:
            return False
    for kw in EXCLUDE_JOB_KW:
        if kw in job:
            return True
    return False


# === メイン処理 ===
def main():
    # データ読み込み
    df = pd.read_csv(INPUT_PATH, encoding='utf-8-sig', dtype=str)
    print(f'読み込み: {len(df)}件')

    # フィルタ1: 産業分類
    df = df[df['産業分類（名称）'].isin(TARGET_INDUSTRIES)].copy()
    print(f'産業分類フィルタ後: {len(df)}件')

    # フィルタ2: 職業分類コード（中分類）
    df['中分類'] = df['職業分類１（コード）'].str[:3]
    df = df[df['中分類'].isin(TARGET_JOB_CODES)].copy()
    print(f'職業分類コードフィルタ後: {len(df)}件')

    # フィルタ3: NGキーワード除外（コード内の混在職種を除去）
    df = df[~df['職種'].apply(should_exclude_nursery)].copy()
    print(f'NGキーワード除外後: {len(df)}件')

    # フィルタ4: 募集理由
    df = df[df['募集理由区分'].isin(TARGET_REASONS)].copy()
    print(f'募集理由フィルタ後: {len(df)}件')

    # フィルタ5: 従業員数 >= 200 を除外（大企業は対象外）
    df['_emp_num'] = pd.to_numeric(df['従業員数企業全体（コード）'], errors='coerce')
    large_mask = df['_emp_num'] >= 200
    print(f'従業員数200以上除外: {large_mask.sum()}件')
    df = df[~large_mask].copy()
    df.drop(columns=['_emp_num'], inplace=True)
    print(f'従業員数フィルタ後: {len(df)}件')

    # フィルタ6: 営業お断り除外（役職欄等に記載）
    def has_sales_rejection(row):
        text = ' '.join([
            str(row.get('選考担当者課係名／役職名', '')),
            str(row.get('職種', '')),
        ])
        return any(kw in text for kw in EXCLUDE_SALES_KW)

    reject_mask = df.apply(has_sales_rejection, axis=1)
    print(f'営業お断り除外: {reject_mask.sum()}件')
    df = df[~reject_mask].copy()
    print(f'営業お断りフィルタ後: {len(df)}件')

    # CSV生成
    rows = []
    skipped_no_company = 0
    skipped_no_phone = 0

    for _, r in df.iterrows():
        company = safe_str(r.get('事業所名漢字'))
        if not company:
            skipped_no_company += 1
            continue

        phone = normalize_phone(safe_str(r.get('選考担当者ＴＥＬ')))
        if not phone:
            skipped_no_phone += 1
            continue

        mobile_phone = phone if is_mobile(phone) else ''
        last_name = safe_str(r.get('選考担当者氏名漢字')) or '担当者'
        address = safe_str(r.get('事業所所在地'))
        website = safe_str(r.get('事業所ホームページ'))
        hw_job_title = safe_str(r.get('職種'))
        hw_emp_type = safe_str(r.get('雇用形態'))
        hw_recruit_reason = safe_str(r.get('募集理由区分'))
        hw_recruit_count = parse_employees(r.get('採用人数（コード）'))

        # メモ欄
        memo_parts = [
            f'【新規作成】ハローワーク求人情報（{TODAY}）',
            f'職種: {hw_job_title}' if hw_job_title else '',
            f'募集理由: {hw_recruit_reason}' if hw_recruit_reason else '',
            f'募集人員: {hw_recruit_count}名' if hw_recruit_count else '',
            f'雇用形態: {hw_emp_type}' if hw_emp_type else '',
            f'バッチID: {BATCH_ID}',
        ]
        memo = '\n'.join([p for p in memo_parts if p])

        row = {
            # 基本フィールド
            'Company': company,
            'LastName': last_name,
            'Phone': phone,
            'MobilePhone': mobile_phone,
            'PostalCode': safe_str(r.get('事業所郵便番号')),
            'Street': address,
            'Prefecture__c': extract_prefecture(address),
            'Website': website,
            'NumberOfEmployees': parse_employees(r.get('従業員数企業全体（コード）')),
            'CorporateNumber__c': normalize_corp_number(r.get('法人番号')),
            'Establish__c': safe_str(r.get('創業設立年（西暦）')),
            'PresidentName__c': safe_str(r.get('代表者名')),
            'PresidentTitle__c': safe_str(r.get('代表者役職')),
            'Title': safe_str(r.get('選考担当者課係名／役職名')),
            'Name_Kana__c': safe_str(r.get('選考担当者氏名フリガナ')),
            'LeadSource': 'ハローワーク',
            'Status': '未架電',
            'Publish_ImportText__c': memo,
            # ハローワーク専用フィールド
            'Hellowork_JobPublicationDate__c': parse_date(r.get('受付年月日（西暦）')),
            'Hellowork_JobClosedDate__c': parse_date(r.get('求人有効年月日（西暦）')),
            'Hellowork_Industry__c': safe_str(r.get('産業分類（名称）')),
            'Hellowork_RecuritmentType__c': hw_job_title[:255] if hw_job_title else '',
            'Hellowork_EmploymentType__c': hw_emp_type,
            'Hellowork_RecruitmentReasonCategory__c': hw_recruit_reason,
            'Hellowork_NumberOfRecruitment__c': hw_recruit_count,
            'Hellowork_NumberOfEmployee_Office__c': parse_employees(r.get('従業員数就業場所（コード）')),
            'Hellowork_DataImportDate__c': TODAY,
            'Hellowork_URL__c': website,
        }
        rows.append(row)

    import_df = pd.DataFrame(rows)
    import_df.to_csv(OUTPUT_PATH, index=False, encoding='utf-8-sig')

    # === レポート出力 ===
    print(f'\n{"="*50}')
    print(f'生成完了: {len(import_df)}件')
    print(f'カラム数: {len(import_df.columns)}')
    print(f'スキップ（Company空）: {skipped_no_company}件')
    print(f'スキップ（Phone空）: {skipped_no_phone}件')
    print(f'出力: {OUTPUT_PATH}')
    print(f'{"="*50}')

    print(f'\n--- フィールド充填率 ---')
    for col in import_df.columns:
        filled = (import_df[col].notna() & (import_df[col] != '')).sum()
        pct = filled * 100 // len(import_df)
        print(f'  {col}: {filled}/{len(import_df)} ({pct}%)')

    print(f'\n--- 産業分類 ---')
    for name, cnt in import_df['Hellowork_Industry__c'].value_counts().items():
        print(f'  {name}: {cnt}件')

    print(f'\n--- 職種 上位15 ---')
    for name, cnt in import_df['Hellowork_RecuritmentType__c'].value_counts().head(15).items():
        print(f'  {name}: {cnt}件')

    print(f'\n--- 募集理由 ---')
    for name, cnt in import_df['Hellowork_RecruitmentReasonCategory__c'].value_counts().items():
        print(f'  {name}: {cnt}件')

    print(f'\n--- 雇用形態 ---')
    total = len(import_df)
    for name, cnt in import_df['Hellowork_EmploymentType__c'].value_counts().items():
        print(f'  {name}: {cnt}件 ({cnt*100/total:.1f}%)')


if __name__ == '__main__':
    main()
