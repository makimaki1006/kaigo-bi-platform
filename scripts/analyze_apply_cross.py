"""応募獲得情報 クロス集計分析スクリプト
地域 x 職種 x 事業形態 x 法人格 のクロス集計ランキング
"""
import pandas as pd
import os

pd.set_option('display.max_rows', 200)
pd.set_option('display.width', 250)
pd.set_option('display.max_colwidth', 40)

df = pd.read_csv('data/output/apply_management/ApplyManagement_with_project.csv', encoding='utf-8-sig', dtype=str)

# 日付パース
df['ApplyDate_parsed'] = pd.to_datetime(df['ApplyDate__c'], errors='coerce')
df['CreatedDate_parsed'] = pd.to_datetime(df['CreatedDate'], errors='coerce', utc=True).dt.tz_localize(None)
df['AnalysisDate'] = df['ApplyDate_parsed'].fillna(df['CreatedDate_parsed'])

# 直近1年
start_date = pd.Timestamp('2025-03-01')
end_date = pd.Timestamp('2026-02-28')
df_year = df[(df['AnalysisDate'] >= start_date) & (df['AnalysisDate'] <= end_date)].copy()
df_year = df_year[df_year['InvalidFlg__c'] != 'true']

# カラム名短縮
df_year.rename(columns={
    'Project__r.LegalPersonality__c': 'LegalPersonality',
    'Project__r.FacilityType_Large__c': 'FacilityLarge',
    'Project__r.FacilityType_Middle__c': 'FacilityMiddle',
    'Project__r.FacilityType_Small__c': 'FacilitySmall',
}, inplace=True)

# 空文字をNaNに統一
for col in ['LegalPersonality', 'FacilityLarge', 'FacilityMiddle', 'FacilitySmall']:
    df_year[col] = df_year[col].replace('', pd.NA)

print(f'分析対象期間: {start_date.date()} ~ {end_date.date()}')
print(f'分析対象レコード数: {len(df_year)} 件')
print()

# 充填率
print('=== フィールド充填率 ===')
for col in ['Prefecture__c', 'Job__c', 'JobStyle__c', 'LegalPersonality', 'FacilityLarge', 'FacilityMiddle', 'FacilitySmall']:
    filled = df_year[col].notna().sum()
    print(f'  {col:<30} {filled:>6}/{len(df_year)} ({filled/len(df_year)*100:.1f}%)')
print()

# ===========================================================
# 法人格ランキング
# ===========================================================
print('=' * 70)
print('[1] 法人格別 応募獲得ランキング')
print('=' * 70)
lp_data = df_year[df_year['LegalPersonality'].notna()]
lp_rank = lp_data['LegalPersonality'].value_counts()
total_lp = lp_rank.sum()
for i, (lp, cnt) in enumerate(lp_rank.head(15).items(), 1):
    pct = cnt / total_lp * 100
    # 面接率・採用率
    subset = lp_data[lp_data['LegalPersonality'] == lp]
    iv_rate = (subset['InterviewFlg__c'] == 'true').sum() / len(subset) * 100
    rc_rate = (subset['SuccessRecruitment__c'] == 'true').sum() / len(subset) * 100
    print(f'  {i:>2}. {lp:<16} {cnt:>5} 件 ({pct:>5.1f}%)  面接率{iv_rate:>5.1f}%  採用率{rc_rate:>5.1f}%')
print(f'  --- 合計: {total_lp} 件 / {lp_rank.nunique()} 法人格')
print()

# ===========================================================
# 事業形態（大項目）ランキング
# ===========================================================
print('=' * 70)
print('[2] 事業形態（大項目）別 応募獲得ランキング')
print('=' * 70)
fl_data = df_year[df_year['FacilityLarge'].notna()]
fl_rank = fl_data['FacilityLarge'].value_counts()
total_fl = fl_rank.sum()
for i, (fl, cnt) in enumerate(fl_rank.items(), 1):
    pct = cnt / total_fl * 100
    subset = fl_data[fl_data['FacilityLarge'] == fl]
    iv_rate = (subset['InterviewFlg__c'] == 'true').sum() / len(subset) * 100
    rc_rate = (subset['SuccessRecruitment__c'] == 'true').sum() / len(subset) * 100
    print(f'  {i:>2}. {fl:<25} {cnt:>5} 件 ({pct:>5.1f}%)  面接率{iv_rate:>5.1f}%  採用率{rc_rate:>5.1f}%')
print(f'  --- 合計: {total_fl} 件 / {fl_rank.nunique()} 事業形態')
print()

# ===========================================================
# 事業形態（中項目）ランキング TOP20
# ===========================================================
print('=' * 70)
print('[3] 事業形態（中項目）別 応募獲得ランキング TOP20')
print('=' * 70)
fm_data = df_year[df_year['FacilityMiddle'].notna()]
fm_rank = fm_data['FacilityMiddle'].value_counts()
total_fm = fm_rank.sum()
for i, (fm, cnt) in enumerate(fm_rank.head(20).items(), 1):
    pct = cnt / total_fm * 100
    subset = fm_data[fm_data['FacilityMiddle'] == fm]
    iv_rate = (subset['InterviewFlg__c'] == 'true').sum() / len(subset) * 100
    rc_rate = (subset['SuccessRecruitment__c'] == 'true').sum() / len(subset) * 100
    print(f'  {i:>2}. {fm:<30} {cnt:>5} 件 ({pct:>5.1f}%)  面接率{iv_rate:>5.1f}%  採用率{rc_rate:>5.1f}%')
print(f'  --- 合計: {total_fm} 件 / {fm_rank.nunique()} 事業形態')
print()

# ===========================================================
# クロス集計: 法人格 x 職種 TOP20
# ===========================================================
print('=' * 70)
print('[4] 法人格 x 職種 クロスランキング TOP20')
print('=' * 70)
c1 = df_year[df_year['LegalPersonality'].notna() & df_year['Job__c'].notna()]
cross1 = c1.groupby(['LegalPersonality', 'Job__c']).agg(
    count=('Id', 'count'),
    interview=('InterviewFlg__c', lambda x: (x == 'true').sum()),
    recruit=('SuccessRecruitment__c', lambda x: (x == 'true').sum())
).reset_index()
cross1['iv_rate'] = (cross1['interview'] / cross1['count'] * 100).round(1)
cross1['rc_rate'] = (cross1['recruit'] / cross1['count'] * 100).round(1)
cross1 = cross1.sort_values('count', ascending=False).head(20)
print(f'  {"法人格":<12} {"職種":<18} {"応募":>5} {"面接率":>7} {"採用率":>7}')
print('  ' + '-' * 60)
for _, r in cross1.iterrows():
    print(f'  {r["LegalPersonality"]:<12} {r["Job__c"]:<18} {int(r["count"]):>5}  {r["iv_rate"]:>5.1f}%  {r["rc_rate"]:>5.1f}%')
print()

# ===========================================================
# クロス集計: 事業形態（大） x 職種 TOP20
# ===========================================================
print('=' * 70)
print('[5] 事業形態（大項目）x 職種 クロスランキング TOP20')
print('=' * 70)
c2 = df_year[df_year['FacilityLarge'].notna() & df_year['Job__c'].notna()]
cross2 = c2.groupby(['FacilityLarge', 'Job__c']).agg(
    count=('Id', 'count'),
    interview=('InterviewFlg__c', lambda x: (x == 'true').sum()),
    recruit=('SuccessRecruitment__c', lambda x: (x == 'true').sum())
).reset_index()
cross2['iv_rate'] = (cross2['interview'] / cross2['count'] * 100).round(1)
cross2['rc_rate'] = (cross2['recruit'] / cross2['count'] * 100).round(1)
cross2 = cross2.sort_values('count', ascending=False).head(20)
print(f'  {"事業形態(大)":<20} {"職種":<18} {"応募":>5} {"面接率":>7} {"採用率":>7}')
print('  ' + '-' * 65)
for _, r in cross2.iterrows():
    print(f'  {r["FacilityLarge"]:<20} {r["Job__c"]:<18} {int(r["count"]):>5}  {r["iv_rate"]:>5.1f}%  {r["rc_rate"]:>5.1f}%')
print()

# ===========================================================
# クロス集計: 地域 x 事業形態（大） TOP20
# ===========================================================
print('=' * 70)
print('[6] 地域 x 事業形態（大項目） クロスランキング TOP20')
print('=' * 70)
c3 = df_year[df_year['Prefecture__c'].notna() & df_year['FacilityLarge'].notna()]
cross3 = c3.groupby(['Prefecture__c', 'FacilityLarge']).agg(
    count=('Id', 'count'),
    interview=('InterviewFlg__c', lambda x: (x == 'true').sum()),
    recruit=('SuccessRecruitment__c', lambda x: (x == 'true').sum())
).reset_index()
cross3['iv_rate'] = (cross3['interview'] / cross3['count'] * 100).round(1)
cross3['rc_rate'] = (cross3['recruit'] / cross3['count'] * 100).round(1)
cross3 = cross3.sort_values('count', ascending=False).head(20)
print(f'  {"地域":<8} {"事業形態(大)":<20} {"応募":>5} {"面接率":>7} {"採用率":>7}')
print('  ' + '-' * 55)
for _, r in cross3.iterrows():
    print(f'  {r["Prefecture__c"]:<8} {r["FacilityLarge"]:<20} {int(r["count"]):>5}  {r["iv_rate"]:>5.1f}%  {r["rc_rate"]:>5.1f}%')
print()

# ===========================================================
# クロス集計: 地域 x 法人格 TOP20
# ===========================================================
print('=' * 70)
print('[7] 地域 x 法人格 クロスランキング TOP20')
print('=' * 70)
c4 = df_year[df_year['Prefecture__c'].notna() & df_year['LegalPersonality'].notna()]
cross4 = c4.groupby(['Prefecture__c', 'LegalPersonality']).agg(
    count=('Id', 'count'),
    interview=('InterviewFlg__c', lambda x: (x == 'true').sum()),
    recruit=('SuccessRecruitment__c', lambda x: (x == 'true').sum())
).reset_index()
cross4['iv_rate'] = (cross4['interview'] / cross4['count'] * 100).round(1)
cross4['rc_rate'] = (cross4['recruit'] / cross4['count'] * 100).round(1)
cross4 = cross4.sort_values('count', ascending=False).head(20)
print(f'  {"地域":<8} {"法人格":<16} {"応募":>5} {"面接率":>7} {"採用率":>7}')
print('  ' + '-' * 55)
for _, r in cross4.iterrows():
    print(f'  {r["Prefecture__c"]:<8} {r["LegalPersonality"]:<16} {int(r["count"]):>5}  {r["iv_rate"]:>5.1f}%  {r["rc_rate"]:>5.1f}%')
print()

# ===========================================================
# クロス集計: 法人格 x 事業形態（大） TOP20
# ===========================================================
print('=' * 70)
print('[8] 法人格 x 事業形態（大項目） クロスランキング TOP20')
print('=' * 70)
c5 = df_year[df_year['LegalPersonality'].notna() & df_year['FacilityLarge'].notna()]
cross5 = c5.groupby(['LegalPersonality', 'FacilityLarge']).agg(
    count=('Id', 'count'),
    interview=('InterviewFlg__c', lambda x: (x == 'true').sum()),
    recruit=('SuccessRecruitment__c', lambda x: (x == 'true').sum())
).reset_index()
cross5['iv_rate'] = (cross5['interview'] / cross5['count'] * 100).round(1)
cross5['rc_rate'] = (cross5['recruit'] / cross5['count'] * 100).round(1)
cross5 = cross5.sort_values('count', ascending=False).head(20)
print(f'  {"法人格":<12} {"事業形態(大)":<20} {"応募":>5} {"面接率":>7} {"採用率":>7}')
print('  ' + '-' * 60)
for _, r in cross5.iterrows():
    print(f'  {r["LegalPersonality"]:<12} {r["FacilityLarge"]:<20} {int(r["count"]):>5}  {r["iv_rate"]:>5.1f}%  {r["rc_rate"]:>5.1f}%')
print()

# ===========================================================
# 4軸クロス: 地域 x 職種 x 事業形態 x 法人格 TOP30
# ===========================================================
print('=' * 70)
print('[9] 4軸クロス: 地域 x 職種 x 事業形態(大) x 法人格 TOP30')
print('=' * 70)
c6 = df_year[
    df_year['Prefecture__c'].notna() &
    df_year['Job__c'].notna() &
    df_year['FacilityLarge'].notna() &
    df_year['LegalPersonality'].notna()
]
cross6 = c6.groupby(['Prefecture__c', 'Job__c', 'FacilityLarge', 'LegalPersonality']).agg(
    count=('Id', 'count'),
    interview=('InterviewFlg__c', lambda x: (x == 'true').sum()),
    recruit=('SuccessRecruitment__c', lambda x: (x == 'true').sum())
).reset_index()
cross6['iv_rate'] = (cross6['interview'] / cross6['count'] * 100).round(1)
cross6['rc_rate'] = (cross6['recruit'] / cross6['count'] * 100).round(1)
cross6 = cross6.sort_values('count', ascending=False).head(30)
print(f'  {"地域":<6} {"職種":<14} {"事業形態":<16} {"法人格":<10} {"応募":>5} {"面接率":>6} {"採用率":>6}')
print('  ' + '-' * 80)
for _, r in cross6.iterrows():
    print(f'  {r["Prefecture__c"]:<6} {r["Job__c"]:<14} {r["FacilityLarge"]:<16} {r["LegalPersonality"]:<10} {int(r["count"]):>5} {r["iv_rate"]:>5.1f}% {r["rc_rate"]:>5.1f}%')
