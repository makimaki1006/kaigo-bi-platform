"""応募獲得情報の総合ランキング分析スクリプト
直近1年間（2025年3月～2026年2月）の応募獲得数を
地域・職種・雇用形態別にランキング分析する
"""
import pandas as pd
import os

pd.set_option('display.max_rows', 100)
pd.set_option('display.width', 200)
pd.set_option('display.max_colwidth', 50)

df = pd.read_csv('data/output/apply_management/ApplyManagement_all.csv', encoding='utf-8-sig', dtype=str)

# 応募日をパース（tz-naiveに統一）
df['ApplyDate_parsed'] = pd.to_datetime(df['ApplyDate__c'], errors='coerce')
df['CreatedDate_parsed'] = pd.to_datetime(df['CreatedDate'], errors='coerce', utc=True).dt.tz_localize(None)

# 応募日がない場合はCreatedDateで代替
df['AnalysisDate'] = df['ApplyDate_parsed'].fillna(df['CreatedDate_parsed'])

# 直近1年（2025-03-01 ～ 2026-02-28）
start_date = pd.Timestamp('2025-03-01')
end_date = pd.Timestamp('2026-02-28')
df_year = df[(df['AnalysisDate'] >= start_date) & (df['AnalysisDate'] <= end_date)].copy()

# 無効を除外
df_year = df_year[df_year['InvalidFlg__c'] != 'true']

print(f'分析対象期間: {start_date.date()} ~ {end_date.date()}')
print(f'分析対象レコード数: {len(df_year)} 件（無効除外済み）')
print(f'（うち Prefecture 有: {(df_year["Prefecture__c"].notna() & (df_year["Prefecture__c"] != "")).sum()} 件）')
print(f'（うち Job 有: {(df_year["Job__c"].notna() & (df_year["Job__c"] != "")).sum()} 件）')
print(f'（うち JobStyle 有: {(df_year["JobStyle__c"].notna() & (df_year["JobStyle__c"] != "")).sum()} 件）')
print()

# 月別推移
df_year['YearMonth'] = df_year['AnalysisDate'].dt.to_period('M')
monthly = df_year.groupby('YearMonth').size()
print('=== 月別応募獲得数 ===')
for ym, cnt in monthly.items():
    bar = '#' * (cnt // 50)
    print(f'  {ym}  {cnt:>5} 件 {bar}')
print(f'  合計: {monthly.sum()} 件')
print()

# ----- 地域ランキング -----
print('=' * 60)
print('[1] 地域（都道府県）別 応募獲得ランキング TOP20')
print('=' * 60)
pref_data = df_year[df_year['Prefecture__c'].notna() & (df_year['Prefecture__c'] != '')]
pref_rank = pref_data['Prefecture__c'].value_counts()
total_pref = pref_rank.sum()
for i, (pref, cnt) in enumerate(pref_rank.head(20).items(), 1):
    pct = cnt / total_pref * 100
    bar = '#' * int(pct / 2)
    print(f'  {i:>2}. {pref:<8} {cnt:>5} 件 ({pct:>5.1f}%) {bar}')
print(f'  --- 合計: {total_pref} 件 / {pref_rank.nunique()} 都道府県')
print()

# ----- 職種ランキング -----
print('=' * 60)
print('[2] 職種別 応募獲得ランキング')
print('=' * 60)
job_data = df_year[df_year['Job__c'].notna() & (df_year['Job__c'] != '')]
job_rank = job_data['Job__c'].value_counts()
total_job = job_rank.sum()
for i, (job, cnt) in enumerate(job_rank.items(), 1):
    pct = cnt / total_job * 100
    bar = '#' * int(pct / 2)
    print(f'  {i:>2}. {job:<20} {cnt:>5} 件 ({pct:>5.1f}%) {bar}')
print(f'  --- 合計: {total_job} 件 / {job_rank.nunique()} 職種')
print()

# ----- 雇用形態ランキング -----
print('=' * 60)
print('[3] 雇用形態別 応募獲得ランキング')
print('=' * 60)
style_data = df_year[df_year['JobStyle__c'].notna() & (df_year['JobStyle__c'] != '')]
style_rank = style_data['JobStyle__c'].value_counts()
total_style = style_rank.sum()
for i, (style, cnt) in enumerate(style_rank.items(), 1):
    pct = cnt / total_style * 100
    bar = '#' * int(pct / 2)
    print(f'  {i:>2}. {style:<20} {cnt:>5} 件 ({pct:>5.1f}%) {bar}')
print(f'  --- 合計: {total_style} 件 / {style_rank.nunique()} 雇用形態')
print()

# ----- 応募ソースランキング -----
print('=' * 60)
print('[4] 応募ソース別 応募獲得ランキング')
print('=' * 60)
src_rank = df_year['EntrySource__c'].value_counts()
total_src = src_rank.sum()
for i, (src, cnt) in enumerate(src_rank.head(15).items(), 1):
    pct = cnt / total_src * 100
    bar = '#' * int(pct / 2)
    print(f'  {i:>2}. {src:<45} {cnt:>5} 件 ({pct:>5.1f}%) {bar}')
print(f'  --- 合計: {total_src} 件 / {src_rank.nunique()} ソース')
print()

# ----- クロス分析: 地域 x 職種 TOP20 -----
print('=' * 60)
print('[5] 地域 x 職種 クロスランキング TOP20')
print('=' * 60)
cross_data = df_year[(df_year['Prefecture__c'].notna() & (df_year['Prefecture__c'] != '')) &
                     (df_year['Job__c'].notna() & (df_year['Job__c'] != ''))]
cross = cross_data.groupby(['Prefecture__c', 'Job__c']).size().reset_index(name='count')
cross = cross.sort_values('count', ascending=False).head(20)
for i, row in enumerate(cross.itertuples(), 1):
    print(f'  {i:>2}. {row.Prefecture__c} x {row.Job__c:<15} {row.count:>5} 件')
print()

# ----- 職種別 面接率・採用率 -----
print('=' * 60)
print('[6] 職種別 面接率・採用率（応募10件以上）')
print('=' * 60)
job_analysis = df_year[df_year['Job__c'].notna() & (df_year['Job__c'] != '')].copy()
job_stats = job_analysis.groupby('Job__c').agg(
    apply_count=('Id', 'count'),
    interview_count=('InterviewFlg__c', lambda x: (x == 'true').sum()),
    recruit_count=('SuccessRecruitment__c', lambda x: (x == 'true').sum())
).reset_index()
job_stats['interview_rate'] = (job_stats['interview_count'] / job_stats['apply_count'] * 100).round(1)
job_stats['recruit_rate'] = (job_stats['recruit_count'] / job_stats['apply_count'] * 100).round(1)
job_stats = job_stats[job_stats['apply_count'] >= 10].sort_values('apply_count', ascending=False)
print(f'  {"職種":<20} {"応募数":>6} {"面接数":>6} {"面接率":>7} {"採用数":>6} {"採用率":>7}')
print('  ' + '-' * 68)
for _, row in job_stats.iterrows():
    print(f'  {row["Job__c"]:<20} {int(row["apply_count"]):>6} {int(row["interview_count"]):>6} {row["interview_rate"]:>5.1f}%  {int(row["recruit_count"]):>6} {row["recruit_rate"]:>5.1f}%')
print()

# ----- 地域別 面接率・採用率 -----
print('=' * 60)
print('[7] 地域別 面接率・採用率（応募30件以上）')
print('=' * 60)
pref_analysis = df_year[df_year['Prefecture__c'].notna() & (df_year['Prefecture__c'] != '')].copy()
pref_stats = pref_analysis.groupby('Prefecture__c').agg(
    apply_count=('Id', 'count'),
    interview_count=('InterviewFlg__c', lambda x: (x == 'true').sum()),
    recruit_count=('SuccessRecruitment__c', lambda x: (x == 'true').sum())
).reset_index()
pref_stats['interview_rate'] = (pref_stats['interview_count'] / pref_stats['apply_count'] * 100).round(1)
pref_stats['recruit_rate'] = (pref_stats['recruit_count'] / pref_stats['apply_count'] * 100).round(1)
pref_stats = pref_stats[pref_stats['apply_count'] >= 30].sort_values('apply_count', ascending=False)
print(f'  {"都道府県":<10} {"応募数":>6} {"面接数":>6} {"面接率":>7} {"採用数":>6} {"採用率":>7}')
print('  ' + '-' * 60)
for _, row in pref_stats.iterrows():
    print(f'  {row["Prefecture__c"]:<10} {int(row["apply_count"]):>6} {int(row["interview_count"]):>6} {row["interview_rate"]:>5.1f}%  {int(row["recruit_count"]):>6} {row["recruit_rate"]:>5.1f}%')
print()

# ----- 雇用形態別 面接率・採用率 -----
print('=' * 60)
print('[8] 雇用形態別 面接率・採用率')
print('=' * 60)
style_analysis = df_year[df_year['JobStyle__c'].notna() & (df_year['JobStyle__c'] != '')].copy()
style_stats = style_analysis.groupby('JobStyle__c').agg(
    apply_count=('Id', 'count'),
    interview_count=('InterviewFlg__c', lambda x: (x == 'true').sum()),
    recruit_count=('SuccessRecruitment__c', lambda x: (x == 'true').sum())
).reset_index()
style_stats['interview_rate'] = (style_stats['interview_count'] / style_stats['apply_count'] * 100).round(1)
style_stats['recruit_rate'] = (style_stats['recruit_count'] / style_stats['apply_count'] * 100).round(1)
style_stats = style_stats.sort_values('apply_count', ascending=False)
print(f'  {"雇用形態":<20} {"応募数":>6} {"面接数":>6} {"面接率":>7} {"採用数":>6} {"採用率":>7}')
print('  ' + '-' * 68)
for _, row in style_stats.iterrows():
    print(f'  {row["JobStyle__c"]:<20} {int(row["apply_count"]):>6} {int(row["interview_count"]):>6} {row["interview_rate"]:>5.1f}%  {int(row["recruit_count"]):>6} {row["recruit_rate"]:>5.1f}%')
