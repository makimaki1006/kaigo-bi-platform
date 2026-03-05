import sys
import pandas as pd
from pathlib import Path
from collections import Counter

sys.stdout.reconfigure(encoding='utf-8')

base = Path(r'C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List')

# Load the filtered CSV
leads = pd.read_csv(base / 'data/output/hellowork/construction_new_leads_pop5_100_20260203.csv',
                    encoding='utf-8-sig', dtype=str)

print(f'総件数: {len(leads):,}件')

# Check job-related columns
job_col = 'Hellowork_RecuritmentType__c'  # This should be 職種

print(f'\n=== 職種カラム: {job_col} ===')

# Get all unique job types and their counts
job_counts = leads[job_col].fillna('不明').value_counts()

print(f'\nユニーク職種数: {len(job_counts)}')
print(f'\n=== 職種別件数 TOP50 ===')
for job, count in job_counts.head(50).items():
    print(f'{job}: {count:,}件')

# Also analyze by keywords in job title
print('\n\n=== キーワード別件数 ===')

keywords = {
    '施工管理': ['施工管理', '現場監督', '監理'],
    '設備': ['設備', '空調', '給排水', '衛生'],
    '電気': ['電気', '電工', '電設'],
    '建築': ['建築', '建設'],
    '土木': ['土木'],
    '配管': ['配管', 'プラント'],
    '大工・左官': ['大工', '左官', '内装', '造作'],
    '塗装・防水': ['塗装', '防水'],
    '鳶・足場': ['鳶', 'とび', '足場'],
    '解体': ['解体'],
    '型枠・鉄筋': ['型枠', '鉄筋'],
    '重機オペ': ['重機', 'オペレーター', 'クレーン', 'ユンボ'],
    '測量・設計': ['測量', '設計', 'CAD'],
    '営業': ['営業'],
    'メンテナンス': ['メンテナンス', '保守', '点検'],
    '清掃': ['清掃', 'ビル管理'],
    '警備': ['警備'],
    '運転・配送': ['運転', 'ドライバー', '配送', '運搬'],
    '製造・工場': ['製造', '工場', 'ライン'],
}

for category, kws in keywords.items():
    pattern = '|'.join(kws)
    count = leads[job_col].fillna('').str.contains(pattern, na=False).sum()
    print(f'{category}: {count:,}件')

# Show what's NOT matching any category
print('\n\n=== カテゴリ外の職種 TOP30 ===')
all_patterns = '|'.join([kw for kws in keywords.values() for kw in kws])
unmatched = leads[~leads[job_col].fillna('').str.contains(all_patterns, na=False)]
unmatched_jobs = unmatched[job_col].fillna('不明').value_counts().head(30)
for job, count in unmatched_jobs.items():
    print(f'{job}: {count:,}件')
