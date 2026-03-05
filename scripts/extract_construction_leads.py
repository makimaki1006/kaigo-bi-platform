# -*- coding: utf-8 -*-
"""建設・リフォーム系ハローワーク求人抽出"""
import sys
import io
import re
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import pandas as pd

project_root = Path(__file__).parent.parent
input_csv = project_root / 'data' / 'input' / 'hellowork.csv'
output_dir = project_root / 'data' / 'output' / 'hellowork'

df = pd.read_csv(input_csv, encoding='cp932', dtype=str, low_memory=False)
print(f"全件: {len(df):,}")

# カラム名取得
ind_code_col = [c for c in df.columns if '産業分類' in c and 'コード' in c and '大' not in c][0]
ind_name_col = [c for c in df.columns if '産業分類' in c and '名称' in c][0]
job1_col = [c for c in df.columns if '職業分類１' in c and 'コード' in c and '大' not in c][0]
job2_col = [c for c in df.columns if '職業分類２' in c and 'コード' in c and '大' not in c][0]
job3_col = [c for c in df.columns if '職業分類３' in c and 'コード' in c and '大' not in c][0]
tel_col = [c for c in df.columns if '選考担当者' in c and 'ＴＥＬ' in c and '携帯' not in c][0]
office_col = [c for c in df.columns if c == '事業所番号'][0]
company_col = df.columns[40]  # 事業所名漢字

# 産業分類: 建設業系
target_industry = ['066', '077', '079', '081', '083']

# 職業分類: 設備/電気/建築/リフォーム/土木
target_job_codes = [
    '601-01', '602-01', '593-01', '593-02',
    '598-01', '581-01', '597-01', '599-99',
    '585-01', '611-01'
]

ind_match = df[ind_code_col].isin(target_industry)
job_match = (
    df[job1_col].isin(target_job_codes) |
    df[job2_col].isin(target_job_codes) |
    df[job3_col].isin(target_job_codes)
)

combined = ind_match | job_match
print(f"産業分類マッチ: {ind_match.sum():,}")
print(f"職業分類マッチ: {job_match.sum():,}")
print(f"OR結合: {combined.sum():,}")
print(f"AND結合: {(ind_match & job_match).sum():,}")

# 内訳
print("\n--- 産業分類コード別 ---")
for code in target_industry:
    mask = df[ind_code_col] == code
    n = mask.sum()
    name = df[mask][ind_name_col].iloc[0] if n > 0 else '-'
    print(f"  {code} {name}: {n:,}")

print("\n--- 職業分類コード別 ---")
for code in target_job_codes:
    n = ((df[job1_col] == code).sum() +
         (df[job2_col] == code).sum() +
         (df[job3_col] == code).sum())
    print(f"  {code}: {n:,}")

# フィルタ
filtered = df[combined].copy()
has_phone = filtered[tel_col].notna() & (filtered[tel_col].str.strip() != '')
print(f"\n電話番号あり: {has_phone.sum():,} / {len(filtered):,}")
print(f"ユニーク事業所: {filtered[office_col].nunique():,}")

# 電話番号正規化
def normalize_phone(val):
    if pd.isna(val):
        return ''
    digits = re.sub(r'[^\d]', '', str(val))
    return digits if digits else ''

filtered['phone_norm'] = filtered[tel_col].apply(normalize_phone)

# 既存SF突合済み電話番号を読み込んで除外（既にSFにあるもの）
merged_account = output_dir / 'merged_取引先.csv'
merged_contact = output_dir / 'merged_責任者.csv'

existing_phones = set()
for f in [merged_account, merged_contact]:
    if f.exists():
        m = pd.read_csv(f, encoding='utf-8-sig', dtype=str, low_memory=False)
        tel_candidates = [c for c in m.columns if 'ＴＥＬ' in c or '加工' in c]
        for tc in tel_candidates:
            phones = m[tc].dropna().apply(normalize_phone)
            existing_phones.update(phones[phones != ''].tolist())

# Lead突合ファイルがあればそれも
lead_file = output_dir / 'merged_リード.csv'
if lead_file.exists():
    m = pd.read_csv(lead_file, encoding='utf-8-sig', dtype=str, low_memory=False)
    tel_candidates = [c for c in m.columns if 'ＴＥＬ' in c or '加工' in c]
    for tc in tel_candidates:
        phones = m[tc].dropna().apply(normalize_phone)
        existing_phones.update(phones[phones != ''].tolist())

print(f"既存SF電話番号: {len(existing_phones):,}")

# 成約先除外
contract_file = sorted(output_dir.glob('contract_accounts_*.csv'))[-1] if list(output_dir.glob('contract_accounts_*.csv')) else None
contract_phones = set()
if contract_file:
    cf = pd.read_csv(contract_file, encoding='utf-8-sig', dtype=str, low_memory=False)
    for col in ['Phone', 'Phone2__c']:
        if col in cf.columns:
            phones = cf[col].dropna().apply(normalize_phone)
            contract_phones.update(phones[phones != ''].tolist())
    print(f"成約先電話番号: {len(contract_phones):,}")

# フィルタリング
filtered_with_phone = filtered[has_phone].copy()
is_existing = filtered_with_phone['phone_norm'].isin(existing_phones)
is_contract = filtered_with_phone['phone_norm'].isin(contract_phones)

print(f"\n--- フィルタ結果 ---")
print(f"  電話番号あり: {len(filtered_with_phone):,}")
print(f"  既存SF一致: {is_existing.sum():,}")
print(f"  成約先一致: {is_contract.sum():,}")

# 新規のみ（SF未登録 & 成約先でない）
new_only = filtered_with_phone[~is_existing & ~is_contract].copy()

# 事業所番号で重複除去（最新求人を残す）
date_col = [c for c in df.columns if '受付年月日（西暦）' in c][0]
new_only = new_only.sort_values(date_col, ascending=False).drop_duplicates(subset=[office_col], keep='first')

print(f"  新規（SF未登録 & 非成約先）: {new_only[office_col].nunique():,} 事業所")

# 保存
out_path = output_dir / 'construction_new_leads.csv'
new_only.to_csv(out_path, index=False, encoding='utf-8-sig')
print(f"\n保存: {out_path}")
print(f"  件数: {len(new_only):,}")

# SF既存との一致分も保存（更新候補）
existing_only = filtered_with_phone[is_existing & ~is_contract].copy()
existing_only = existing_only.sort_values(date_col, ascending=False).drop_duplicates(subset=[office_col], keep='first')
out_existing = output_dir / 'construction_existing_matches.csv'
existing_only.to_csv(out_existing, index=False, encoding='utf-8-sig')
print(f"\n既存SF一致（更新候補）: {out_existing}")
print(f"  件数: {len(existing_only):,}")
