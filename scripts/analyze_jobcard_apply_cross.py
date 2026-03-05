"""案件数（JobCard）と応募数のクロス集計分析
地域 x 職種 x 事業形態 x 法人格 の4軸で
案件ボリュームゾーンを特定し、応募数を対応付ける
"""
import pandas as pd

pd.set_option('display.max_rows', 200)
pd.set_option('display.width', 250)

# データ読込
jc = pd.read_csv('data/output/apply_management/JobCard_all.csv', encoding='utf-8-sig', dtype=str)
jc['Apply__c'] = pd.to_numeric(jc['Apply__c'], errors='coerce').fillna(0).astype(int)
jc['NumberOfRecruitment__c'] = pd.to_numeric(jc['NumberOfRecruitment__c'], errors='coerce').fillna(0).astype(int)

# カラム短縮
jc.rename(columns={
    'Project__r.LegalPersonality__c': 'LegalPersonality',
    'Project__r.FacilityType_Large__c': 'FacilityLarge',
    'Project__r.FacilityType_Middle__c': 'FacilityMiddle',
    'Project__r.FacilityType_Small__c': 'FacilitySmall',
    'Occupation__c': 'Job',
    'EmploymentStatus__c': 'JobStyle',
}, inplace=True)

# 空文字をNaNに
for col in ['LegalPersonality', 'FacilityLarge', 'FacilityMiddle', 'Prefecture__c', 'Job', 'JobStyle']:
    jc[col] = jc[col].replace('', pd.NA)

print(f'求人(JobCard)総数: {len(jc)} 件')
print(f'  掲載中: {(jc["Status__c"]=="掲載中").sum()} 件')
print(f'  掲載終了: {(jc["Status__c"]=="掲載終了").sum()} 件')
print(f'  掲載前: {(jc["Status__c"]=="掲載前").sum()} 件')
print(f'  応募合計: {jc["Apply__c"].sum()} 件')
print(f'  採用合計: {jc["NumberOfRecruitment__c"].sum()} 件')
print()

# 充填率
print('=== フィールド充填率 ===')
for col in ['Prefecture__c', 'Job', 'JobStyle', 'LegalPersonality', 'FacilityLarge']:
    filled = jc[col].notna().sum()
    print(f'  {col:<25} {filled:>6}/{len(jc)} ({filled/len(jc)*100:.1f}%)')
print()

# ヘルパー関数
def cross_analysis(df, group_cols, title, top_n=20):
    """クロス集計: 案件数・応募数・応募/案件・採用数"""
    valid = df.dropna(subset=group_cols)
    stats = valid.groupby(group_cols).agg(
        案件数=('Id', 'count'),
        応募数=('Apply__c', 'sum'),
        採用数=('NumberOfRecruitment__c', 'sum'),
    ).reset_index()
    stats['応募/案件'] = (stats['応募数'] / stats['案件数']).round(1)
    stats['採用/案件'] = (stats['採用数'] / stats['案件数']).round(1)
    stats['採用率'] = (stats['採用数'] / stats['応募数'].replace(0, pd.NA) * 100).round(1).fillna(0)
    stats = stats.sort_values('案件数', ascending=False).head(top_n)

    print('=' * 90)
    print(title)
    print('=' * 90)

    # ヘッダー
    col_labels = [c for c in group_cols]
    header = '  '
    for c in col_labels:
        # 短い表示名
        display = c.replace('__c','').replace('Prefecture','地域').replace('FacilityLarge','事業形態')
        header += f'{display:<16}'
    header += f'{"案件数":>6} {"応募数":>6} {"応募/案件":>8} {"採用数":>6} {"採用率":>7}'
    print(header)
    print('  ' + '-' * (len(header) - 2))

    for _, row in stats.iterrows():
        line = '  '
        for c in group_cols:
            line += f'{str(row[c]):<16}'
        line += f'{int(row["案件数"]):>6} {int(row["応募数"]):>6} {row["応募/案件"]:>7.1f}  {int(row["採用数"]):>6} {row["採用率"]:>5.1f}%'
        print(line)
    print()
    return stats


# ===========================================================
# 単軸ランキング
# ===========================================================
cross_analysis(jc, ['Prefecture__c'], '[1] 地域別 案件数・応募数ランキング TOP20')
cross_analysis(jc, ['Job'], '[2] 職種別 案件数・応募数ランキング', top_n=25)
cross_analysis(jc, ['FacilityLarge'], '[3] 事業形態（大項目）別 案件数・応募数ランキング', top_n=10)
cross_analysis(jc, ['LegalPersonality'], '[4] 法人格別 案件数・応募数ランキング', top_n=15)

# ===========================================================
# 2軸クロス
# ===========================================================
cross_analysis(jc, ['FacilityLarge', 'Job'], '[5] 事業形態 x 職種 クロス TOP20')
cross_analysis(jc, ['LegalPersonality', 'FacilityLarge'], '[6] 法人格 x 事業形態 クロス TOP20')
cross_analysis(jc, ['Prefecture__c', 'FacilityLarge'], '[7] 地域 x 事業形態 クロス TOP20')

# ===========================================================
# 4軸クロス: 地域 x 職種 x 事業形態 x 法人格 TOP30
# ===========================================================
print('=' * 110)
print('[8] 4軸クロス: 地域 x 職種 x 事業形態 x 法人格  案件ボリューム TOP30')
print('=' * 110)

valid4 = jc.dropna(subset=['Prefecture__c', 'Job', 'FacilityLarge', 'LegalPersonality'])
stats4 = valid4.groupby(['Prefecture__c', 'Job', 'FacilityLarge', 'LegalPersonality']).agg(
    案件数=('Id', 'count'),
    応募数=('Apply__c', 'sum'),
    採用数=('NumberOfRecruitment__c', 'sum'),
).reset_index()
stats4['応募/案件'] = (stats4['応募数'] / stats4['案件数']).round(1)
stats4['採用率'] = (stats4['採用数'] / stats4['応募数'].replace(0, pd.NA) * 100).round(1).fillna(0)
stats4 = stats4.sort_values('案件数', ascending=False).head(30)

print(f'  {"地域":<8} {"職種":<16} {"事業形態":<16} {"法人格":<12} {"案件数":>6} {"応募数":>6} {"応募/案件":>8} {"採用数":>6} {"採用率":>7}')
print('  ' + '-' * 100)
for _, r in stats4.iterrows():
    print(f'  {r["Prefecture__c"]:<8} {r["Job"]:<16} {r["FacilityLarge"]:<16} {r["LegalPersonality"]:<12} {int(r["案件数"]):>6} {int(r["応募数"]):>6} {r["応募/案件"]:>7.1f}  {int(r["採用数"]):>6} {r["採用率"]:>5.1f}%')
print()

# ===========================================================
# 応募効率（応募/案件）が高いセグメント TOP20（案件3件以上）
# ===========================================================
print('=' * 110)
print('[9] 応募効率（応募/案件）が高いセグメント TOP20（案件3件以上）')
print('=' * 110)

valid_eff = jc.dropna(subset=['Prefecture__c', 'Job', 'FacilityLarge', 'LegalPersonality'])
stats_eff = valid_eff.groupby(['Prefecture__c', 'Job', 'FacilityLarge', 'LegalPersonality']).agg(
    案件数=('Id', 'count'),
    応募数=('Apply__c', 'sum'),
    採用数=('NumberOfRecruitment__c', 'sum'),
).reset_index()
stats_eff['応募/案件'] = (stats_eff['応募数'] / stats_eff['案件数']).round(1)
stats_eff['採用率'] = (stats_eff['採用数'] / stats_eff['応募数'].replace(0, pd.NA) * 100).round(1).fillna(0)
stats_eff = stats_eff[stats_eff['案件数'] >= 3].sort_values('応募/案件', ascending=False).head(20)

print(f'  {"地域":<8} {"職種":<16} {"事業形態":<16} {"法人格":<12} {"案件数":>6} {"応募数":>6} {"応募/案件":>8} {"採用数":>6} {"採用率":>7}')
print('  ' + '-' * 100)
for _, r in stats_eff.iterrows():
    print(f'  {r["Prefecture__c"]:<8} {r["Job"]:<16} {r["FacilityLarge"]:<16} {r["LegalPersonality"]:<12} {int(r["案件数"]):>6} {int(r["応募数"]):>6} {r["応募/案件"]:>7.1f}  {int(r["採用数"]):>6} {r["採用率"]:>5.1f}%')
print()

# ===========================================================
# 案件あるが応募ゼロのセグメント（機会損失）TOP20
# ===========================================================
print('=' * 110)
print('[10] 案件はあるが応募ゼロ（機会損失）セグメント TOP20')
print('=' * 110)

valid_zero = jc.dropna(subset=['Prefecture__c', 'Job', 'FacilityLarge', 'LegalPersonality'])
stats_zero = valid_zero.groupby(['Prefecture__c', 'Job', 'FacilityLarge', 'LegalPersonality']).agg(
    案件数=('Id', 'count'),
    応募数=('Apply__c', 'sum'),
).reset_index()
stats_zero = stats_zero[stats_zero['応募数'] == 0].sort_values('案件数', ascending=False).head(20)

print(f'  {"地域":<8} {"職種":<16} {"事業形態":<16} {"法人格":<12} {"案件数":>6}')
print('  ' + '-' * 65)
for _, r in stats_zero.iterrows():
    print(f'  {r["Prefecture__c"]:<8} {r["Job"]:<16} {r["FacilityLarge"]:<16} {r["LegalPersonality"]:<12} {int(r["案件数"]):>6}')
