# -*- coding: utf-8 -*-
"""
ハローワーク新規リードのSalesforceインポート用CSV作成
マスタールール: claudedocs/master_rules/MASTER_RULE_hellowork.md に準拠
"""
import pandas as pd
import sys
import io
import re
from datetime import datetime

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

def normalize_phone(val):
    """電話番号を正規化（数字以外を除去）"""
    if pd.isna(val) or val == '':
        return ''
    digits = re.sub(r'[^\d]', '', str(val))
    return digits if len(digits) >= 10 else ''

def is_mobile_phone(phone):
    """携帯電話番号かどうか判定"""
    if not phone:
        return False
    return phone.startswith(('090', '080', '070'))

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

def safe_int(val):
    """安全に整数変換（.0除去、「人」「名」等の接尾辞、カンマ対応）"""
    if pd.isna(val) or val == '':
        return ''
    try:
        # 文字列に変換
        val = str(val).strip()
        # 「人」「名」等を除去
        val = val.replace('人', '').replace('名', '')
        # カンマを除去
        val = val.replace(',', '')
        # 数字以外を除去
        val = re.sub(r'[^\d.]', '', val)
        if not val:
            return ''
        return str(int(float(val)))
    except:
        return ''

def normalize_corp_num(val):
    """法人番号を正規化（13桁制限、.0除去）"""
    if pd.isna(val) or val == '':
        return ''
    val = str(val).strip()
    if val.endswith('.0'):
        val = val[:-2]
    val = re.sub(r'[^0-9]', '', val)
    if len(val) > 13:
        val = val[:13]
    return val if len(val) >= 10 else ''

def format_date(val):
    """日付をSalesforce形式（YYYY-MM-DD）に変換"""
    if pd.isna(val) or val == '':
        return ''
    try:
        val = str(val).strip()
        if '/' in val:
            parts = val.split('/')
            if len(parts) == 3:
                return f"{parts[0]}-{parts[1].zfill(2)}-{parts[2].zfill(2)}"
        return val
    except:
        return ''

# ========================================
# メイン処理
# ========================================
print('=== ハローワーク新規リード インポート準備 ===')
print()

# データ読み込み
df = pd.read_csv('data/output/hellowork/hw_202601_true_new_leads.csv', dtype=str)
print(f'読み込み: {len(df):,}件')

# 電話番号で重複除去
df['phone_normalized'] = df['選考担当者ＴＥＬ'].apply(normalize_phone)
df_dedup = df.drop_duplicates(subset=['phone_normalized'], keep='first')
print(f'電話番号重複除去後: {len(df_dedup):,}件')

# インポート用DataFrame作成
import_df = pd.DataFrame()

# === 必須フィールド ===
import_df['Company'] = df_dedup['事業所名漢字']
# LastName: 空の場合は「担当者」を設定
import_df['LastName'] = df_dedup['選考担当者氏名漢字'].fillna('担当者').replace('', '担当者')

# === 電話番号 ===
# Phone必須: 携帯のみの場合でもPhoneに値を入れる
import_df['Phone'] = df_dedup['phone_normalized']
import_df['MobilePhone'] = df_dedup['phone_normalized'].apply(lambda x: x if is_mobile_phone(x) else '')

# === 住所関連 ===
import_df['PostalCode'] = df_dedup['事業所郵便番号']
import_df['Street'] = df_dedup['事業所所在地']
import_df['Prefecture__c'] = df_dedup['事業所所在地'].apply(extract_prefecture)

# === 会社情報 ===
import_df['NumberOfEmployees'] = df_dedup['従業員数企業全体'].apply(safe_int)
import_df['CorporateNumber__c'] = df_dedup['法人番号'].apply(normalize_corp_num)
import_df['Website'] = df_dedup['事業所ホームページ']
import_df['Name_Kana__c'] = df_dedup['事業所名カナ']

# === 代表者情報 ===
import_df['PresidentName__c'] = df_dedup['代表者名']
import_df['PresidentTitle__c'] = df_dedup['代表者役職']

# === 担当者情報 ===
import_df['Title'] = df_dedup['選考担当者課係名／役職名']

# === ハローワーク固有フィールド（マスタールール準拠）===
import_df['Hellowork_JobPublicationDate__c'] = df_dedup['受付年月日（西暦）'].apply(format_date)
import_df['Hellowork_JobClosedDate__c'] = df_dedup['求人有効年月日（西暦）'].apply(format_date)
import_df['Hellowork_Industry__c'] = df_dedup['産業分類（名称）']
import_df['Hellowork_RecuritmentType__c'] = df_dedup['職種']
import_df['Hellowork_EmploymentType__c'] = df_dedup['雇用形態']
import_df['Hellowork_RecruitmentReasonCategory__c'] = df_dedup['募集理由区分']
import_df['Hellowork_NumberOfRecruitment__c'] = df_dedup['採用人数'].apply(safe_int)
import_df['Hellowork_NumberOfEmployee_Office__c'] = df_dedup['従業員数就業場所'].apply(safe_int)
import_df['Hellowork_DataImportDate__c'] = datetime.now().strftime('%Y-%m-%d')

# === リードソース ===
import_df['LeadSource'] = 'ハローワーク'

# === メモフィールド（マスタールール準拠）===
today = datetime.now().strftime('%Y-%m-%d')

def create_memo(row):
    """Publish_ImportText__c / LeadSourceMemo__c 用メモ作成"""
    lines = [
        f'【{today} ハローワーク新規作成】',
        f'産業分類: {row.get("産業分類（名称）", "")}',
        f'職種: {row.get("職種", "")}',
        f'雇用形態: {row.get("雇用形態", "")}',
        f'採用人数: {row.get("採用人数", "")}',
        f'募集理由: {row.get("募集理由区分", "")}',
        f'従業員数: {row.get("従業員数企業全体", "")}',
    ]
    return '\n'.join(lines)

import_df['Publish_ImportText__c'] = df_dedup.apply(create_memo, axis=1)
import_df['LeadSourceMemo__c'] = df_dedup.apply(create_memo, axis=1)

# === バリデーション ===
# Company必須チェック
valid_company = import_df['Company'].notna() & (import_df['Company'] != '')
# Phone必須チェック
valid_phone = import_df['Phone'].notna() & (import_df['Phone'] != '')

valid_mask = valid_company & valid_phone
skipped_no_company = (~valid_company).sum()
skipped_no_phone = (~valid_phone).sum()
skipped_total = (~valid_mask).sum()

import_df = import_df[valid_mask]

print(f'バリデーション後: {len(import_df):,}件')
print(f'  - Company空でスキップ: {skipped_no_company:,}件')
print(f'  - Phone空でスキップ: {skipped_no_phone:,}件')

# 保存
output_path = 'data/output/hellowork/hw_202601_new_leads_import.csv'
import_df.to_csv(output_path, index=False, encoding='utf-8-sig')
print(f'保存完了: {output_path}')

# ========================================
# サマリー出力
# ========================================
print()
print('=' * 50)
print('インポートデータサマリー')
print('=' * 50)
print(f'総件数: {len(import_df):,}件')
print()
print('フィールド有効件数:')
print(f'  Phone: {(import_df["Phone"] != "").sum():,}件')
print(f'  MobilePhone: {(import_df["MobilePhone"] != "").sum():,}件')
print(f'  CorporateNumber__c: {(import_df["CorporateNumber__c"] != "").sum():,}件')
print(f'  Website: {import_df["Website"].notna().sum():,}件')
print(f'  NumberOfEmployees: {(import_df["NumberOfEmployees"] != "").sum():,}件')
print()
print('産業分類分布（上位10）:')
print(import_df['Hellowork_Industry__c'].value_counts().head(10).to_string())
