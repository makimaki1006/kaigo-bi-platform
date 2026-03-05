import sys
import pandas as pd
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')

base = Path(r'C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List')

# Load the CSV with population data
leads = pd.read_csv(base / 'data/output/hellowork/construction_new_leads_with_pop_v2_20260203.csv',
                    encoding='utf-8-sig', dtype=str)
leads['population'] = pd.to_numeric(leads['population'], errors='coerce')

print(f'読み込み: {len(leads)}件')

# Filter: 5万以上 AND 100万未満
mask = (leads['population'] >= 50000) & (leads['population'] < 1000000)
filtered = leads[mask].copy()

print(f'人口5万〜100万フィルタ後: {len(filtered)}件')

# Show breakdown by band
def get_pop_band(pop):
    if pd.isna(pop):
        return '不明'
    elif pop < 100000:
        return '5〜10万'
    elif pop < 300000:
        return '10〜30万'
    elif pop < 500000:
        return '30〜50万'
    else:
        return '50〜100万'

filtered['pop_band'] = filtered['population'].apply(get_pop_band)

print('\n=== セグメント内訳 ===')
for band in ['50〜100万', '30〜50万', '10〜30万', '5〜10万']:
    count = (filtered['pop_band'] == band).sum()
    print(f'{band}: {count:,}件')

# Owner assignment - 3 people equal distribution
owners = [
    ('佐藤丈太郎', '0055i00000CwGDGAA3'),
    ('志村亮介', '0055i00000CwGCrAAN'),
    ('小林幸太', '005J3000000ERz4IAG'),
]

# Assign owners round-robin
filtered = filtered.reset_index(drop=True)
owner_ids = [owners[i % 3][1] for i in range(len(filtered))]
filtered['OwnerId'] = owner_ids

print('\n=== 所有者別件数 ===')
for name, oid in owners:
    count = (filtered['OwnerId'] == oid).sum()
    print(f'{name}: {count:,}件')

# Select only the columns needed for SF import (remove population analysis columns)
# Keep the original SF field columns
sf_columns = [
    'Company', 'LastName', 'Phone', 'MobilePhone', 'PostalCode', 'Street',
    'Prefecture__c', 'NumberOfEmployees', 'CorporateNumber__c', 'Establish__c',
    'Website', 'Title', 'Name_Kana__c', 'PresidentName__c', 'PresidentTitle__c',
    'LeadSource', 'Hellowork_DataImportDate__c', 'Hellowork_Industry__c',
    'Hellowork_RecuritmentType__c', 'Hellowork_EmploymentType__c',
    'Hellowork_NumberOfRecruitment__c', 'LeadSourceMemo__c', 'OwnerId'
]

# Check which columns exist
existing_cols = [c for c in sf_columns if c in filtered.columns]
missing_cols = [c for c in sf_columns if c not in filtered.columns]
if missing_cols:
    print(f'\n注意: 以下の列がCSVにありません: {missing_cols}')

# Also add municipality and population for reference (can be removed before import)
extra_cols = ['municipality', 'population', 'pop_band']
export_cols = existing_cols + [c for c in extra_cols if c in filtered.columns]

export_df = filtered[export_cols].copy()

# Save filtered CSV
output_path = base / 'data/output/hellowork/construction_new_leads_pop5_100_20260203.csv'
export_df.to_csv(output_path, index=False, encoding='utf-8-sig')

print(f'\n保存: {output_path}')
print(f'総件数: {len(export_df):,}件')

# Show sample
print('\n=== サンプル5件 ===')
sample_cols = ['Company', 'Phone', 'municipality', 'population', 'OwnerId']
sample_cols = [c for c in sample_cols if c in export_df.columns]
print(export_df[sample_cols].head())
