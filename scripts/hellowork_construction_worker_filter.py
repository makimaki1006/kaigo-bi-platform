import sys
import pandas as pd
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')

base = Path(r'C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List')

# Load the filtered CSV
leads = pd.read_csv(base / 'data/output/hellowork/construction_new_leads_pop5_100_20260203.csv',
                    encoding='utf-8-sig', dtype=str)

print(f'総件数: {len(leads):,}件')

job_col = 'Hellowork_RecuritmentType__c'

# Step 1: Filter to 建設業コア categories
core_keywords = [
    '施工管理', '現場監督', '監理',
    '設備', '空調', '給排水', '衛生',
    '電気', '電工', '電設',
    '建築', '建設',
    '土木',
    '配管', 'プラント',
    '大工', '左官', '内装', '造作',
    '塗装', '防水',
    '鳶', 'とび', '足場',
    '解体',
    '型枠', '鉄筋',
    '重機', 'オペレーター', 'クレーン', 'ユンボ',
    '測量', '設計', 'CAD',
]

core_pattern = '|'.join(core_keywords)
core_mask = leads[job_col].fillna('').str.contains(core_pattern, na=False)
core_leads = leads[core_mask].copy()

print(f'\n建設業コアフィルタ後: {len(core_leads):,}件')

# Step 2: Filter to "作業員" OR "スタッフ"
worker_pattern = '作業員|スタッフ'
worker_mask = core_leads[job_col].fillna('').str.contains(worker_pattern, na=False)
worker_leads = core_leads[worker_mask].copy()

print(f'「作業員」or「スタッフ」フィルタ後: {len(worker_leads):,}件')

# Show breakdown
print('\n=== 「作業員」のみ ===')
sagyouin_count = core_leads[job_col].fillna('').str.contains('作業員', na=False).sum()
print(f'件数: {sagyouin_count:,}件')

print('\n=== 「スタッフ」のみ ===')
staff_count = core_leads[job_col].fillna('').str.contains('スタッフ', na=False).sum()
print(f'件数: {staff_count:,}件')

print('\n=== 両方含む（重複） ===')
both_count = core_leads[job_col].fillna('').str.contains('作業員', na=False) & core_leads[job_col].fillna('').str.contains('スタッフ', na=False)
print(f'件数: {both_count.sum():,}件')

# Show sample job titles that match
print('\n=== マッチした職種サンプル TOP30 ===')
matched_jobs = worker_leads[job_col].value_counts().head(30)
for job, count in matched_jobs.items():
    print(f'{job}: {count:,}件')

# Show population band distribution for matched
print('\n=== 人口帯別分布（マッチ分） ===')
if 'pop_band' in worker_leads.columns:
    for band in ['50〜100万', '30〜50万', '10〜30万', '5〜10万']:
        count = (worker_leads['pop_band'] == band).sum()
        print(f'{band}: {count:,}件')
