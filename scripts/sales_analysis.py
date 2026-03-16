"""
プロのセールス視点での商談分析スクリプト
新規営業チームのみを対象に、2月の改善ポイントを中心に分析
"""
import pandas as pd
import re
import warnings
warnings.filterwarnings('ignore')

# === データ読み込み ===
BASE = 'C:/Users/fuji1/OneDrive/デスクトップ/Salesforce_List'
df_main = pd.read_csv(f'{BASE}/data/output/analysis/opportunities_with_units.csv')
df_sub = pd.read_csv(f'{BASE}/data/output/analysis/opportunities_20260305_115216.csv', usecols=['Id', 'CreatedDate'])

# CreatedDateをマージ
df = df_main.merge(df_sub[['Id', 'CreatedDate']], on='Id', how='left')

# === チーム名抽出（正規表現 r'】(.+)$'） ===
def extract_team(val):
    if pd.isna(val):
        return None
    m = re.search(r'】(.+)$', str(val))
    return m.group(1) if m else None

df['Team'] = df['AppointUnit__r.Name'].apply(extract_team)
df['FirstTeam'] = df['FirstOpportunityUnit__r.Name'].apply(extract_team)

# === 除外チーム ===
EXCLUDE_TEAMS = ['マーケ', 'エンタープライズ', 'プロダクト', 'MEO', 'セールス部', '株式会社CyXen', 'マーケ兼AI']

# チームがNoneまたは除外対象を除外
df_filtered = df[
    df['Team'].notna() &
    ~df['Team'].isin(EXCLUDE_TEAMS) &
    (df['Team'] != '')
].copy()

# CloseDate→月
df_filtered['CloseDate'] = pd.to_datetime(df_filtered['CloseDate'])
df_filtered['CloseMonth'] = df_filtered['CloseDate'].dt.to_period('M')
df_filtered['CloseYM'] = df_filtered['CloseDate'].dt.strftime('%Y-%m')

# CreatedDate
df_filtered['CreatedDate'] = pd.to_datetime(df_filtered['CreatedDate'], utc=True)
df_filtered['CreatedMonth'] = df_filtered['CreatedDate'].dt.to_period('M')

print('=' * 80)
print('セールス分析レポート（新規営業チームのみ）')
print(f'対象期間: {df_filtered["CloseDate"].min().date()} ~ {df_filtered["CloseDate"].max().date()}')
print(f'対象商談数: {len(df_filtered)}件（除外後）')
print(f'除外チーム: {", ".join(EXCLUDE_TEAMS)}')
print('=' * 80)

# ========================================
# 1. ステージ遷移分析
# ========================================
print('\n' + '=' * 80)
print('【分析1】ステージ遷移分析 - 各月でどのステージで脱落しているか')
print('=' * 80)

# ステージの順序定義
stage_order = ['01 初回商談前', '02 社内検討中', '03 担当者の価値合意',
               '04 上席担当者の価値合意', '05 決裁者の価値合意', '受注']

# 失注・キャンセル・無効を「脱落」として、最終ステージと組み合わせて分析
# 現在のステージ分布を月別に見る
months = sorted(df_filtered['CloseYM'].unique())

print('\n■ 月別ステージ分布（件数）')
stage_month = pd.crosstab(df_filtered['StageName'], df_filtered['CloseYM'])
# ステージの表示順序
all_stages = ['01 初回商談前', '02 社内検討中', '03 担当者の価値合意',
              '04 上席担当者の価値合意', '05 決裁者の価値合意',
              '契約手続き', '受注', '失注', '商談キャンセル', '無効商談']
stage_month = stage_month.reindex([s for s in all_stages if s in stage_month.index])
print(stage_month.to_string())

print('\n■ 月別ステージ分布（構成比%）')
stage_month_pct = stage_month.div(stage_month.sum()) * 100
print(stage_month_pct.round(1).to_string())

# 受注率の月別推移
print('\n■ 月別受注率（受注 / クローズ済み商談）')
for m in months:
    month_data = df_filtered[df_filtered['CloseYM'] == m]
    closed = month_data[month_data['IsClosed'] == True]
    won = closed[closed['IsWon'] == True]
    total = len(month_data)
    closed_cnt = len(closed)
    won_cnt = len(won)
    lost = len(closed[closed['StageName'] == '失注'])
    cancel = len(closed[closed['StageName'] == '商談キャンセル'])
    invalid = len(closed[closed['StageName'] == '無効商談'])

    win_rate = (won_cnt / closed_cnt * 100) if closed_cnt > 0 else 0
    print(f'  {m}: 全{total}件 | クローズ{closed_cnt}件 | 受注{won_cnt}件 | '
          f'失注{lost}件 | キャンセル{cancel}件 | 無効{invalid}件 | 受注率{win_rate:.1f}%')

# 商談進行中（未クローズ）を除いた、クローズ済み商談の最終到達ステージ分析
print('\n■ 失注商談の最終到達ステージ分析（月別）')
lost_df = df_filtered[df_filtered['StageName'] == '失注'].copy()
# 失注はStageName='失注'だが、どのステージまで進んだか知りたい
# → 今あるデータではStageName=失注なので、進行度は不明
# → 代わりに、全商談のステージ到達分布を見る

# クローズ済み商談（受注+失注）でのステージ到達率
print('\n■ 有効商談（受注+失注）のステージ到達推計（月別）')
print('  ※受注=全ステージ通過、失注=最終ステージで脱落と仮定')
for m in months:
    month_data = df_filtered[df_filtered['CloseYM'] == m]
    won = len(month_data[month_data['IsWon'] == True])
    lost = len(month_data[month_data['StageName'] == '失注'])
    effective = won + lost
    if effective > 0:
        win_rate_eff = won / effective * 100
        print(f'  {m}: 有効商談{effective}件 | 受注{won}件 | 失注{lost}件 | 有効受注率{win_rate_eff:.1f}%')

# ========================================
# 2. アポランク × 受注率
# ========================================
print('\n' + '=' * 80)
print('【分析2】アポランク × 受注率（月別比較）')
print('=' * 80)

ranked = df_filtered[df_filtered['AppointRank__c'].notna()].copy()

print('\n■ アポランク別 月別 受注率')
for rank in ['A', 'B', 'C', 'D']:
    rank_data = ranked[ranked['AppointRank__c'] == rank]
    if len(rank_data) == 0:
        continue
    print(f'\n  ランク{rank}:')
    for m in months:
        md = rank_data[rank_data['CloseYM'] == m]
        if len(md) == 0:
            continue
        closed = md[md['IsClosed'] == True]
        won = closed[closed['IsWon'] == True]
        total = len(md)
        closed_cnt = len(closed)
        won_cnt = len(won)
        win_rate = (won_cnt / closed_cnt * 100) if closed_cnt > 0 else 0
        print(f'    {m}: 全{total}件 | クローズ{closed_cnt}件 | 受注{won_cnt}件 | 受注率{win_rate:.1f}%')

print('\n■ アポランク別 受注率サマリ（クローズ済みベース）')
rank_summary = []
for rank in ['A', 'B', 'C', 'D']:
    rank_data = ranked[ranked['AppointRank__c'] == rank]
    for m in months:
        md = rank_data[(rank_data['CloseYM'] == m) & (rank_data['IsClosed'] == True)]
        won = len(md[md['IsWon'] == True])
        total = len(md)
        if total > 0:
            rank_summary.append({'Rank': rank, 'Month': m, 'Won': won, 'Closed': total,
                               'WinRate': won/total*100})

rank_df = pd.DataFrame(rank_summary)
if len(rank_df) > 0:
    pivot = rank_df.pivot_table(index='Rank', columns='Month', values='WinRate', aggfunc='first')
    print(pivot.round(1).to_string())

# ========================================
# 3. 商談実施者 vs アポ獲得者（分業体制分析）
# ========================================
print('\n' + '=' * 80)
print('【分析3】アポ獲得者 vs 商談実施者 - 分業体制の効果')
print('=' * 80)

# Appointer__cはID形式なので、Owner.Name（商談実施者）とTeam（アポ獲得チーム）で比較
# AppointUnit__r.Name = アポ獲得ユニット、FirstOpportunityUnit__r.Name = 初回商談ユニット
# これらが異なる = 分業

df_filtered['IsDivision'] = df_filtered['Team'] != df_filtered['FirstTeam']
# 両方notnaの場合のみ比較
both_teams = df_filtered[df_filtered['Team'].notna() & df_filtered['FirstTeam'].notna()].copy()

print(f'\n対象商談数（両チーム情報あり）: {len(both_teams)}件')
print(f'同一チーム: {len(both_teams[both_teams["IsDivision"]==False])}件')
print(f'異なるチーム（分業）: {len(both_teams[both_teams["IsDivision"]==True])}件')

print('\n■ 分業 vs 同一チームの受注率比較')
for label, subset in [('同一チーム', both_teams[both_teams['IsDivision']==False]),
                       ('分業（異なるチーム）', both_teams[both_teams['IsDivision']==True])]:
    closed = subset[subset['IsClosed'] == True]
    won = closed[closed['IsWon'] == True]
    won_cnt = len(won)
    closed_cnt = len(closed)
    win_rate = (won_cnt / closed_cnt * 100) if closed_cnt > 0 else 0
    print(f'  {label}: クローズ{closed_cnt}件 | 受注{won_cnt}件 | 受注率{win_rate:.1f}%')

print('\n■ 月別 分業効果')
for m in months:
    md = both_teams[both_teams['CloseYM'] == m]
    for label, is_div in [('同一', False), ('分業', True)]:
        subset = md[md['IsDivision'] == is_div]
        closed = subset[subset['IsClosed'] == True]
        won = closed[closed['IsWon'] == True]
        closed_cnt = len(closed)
        won_cnt = len(won)
        win_rate = (won_cnt / closed_cnt * 100) if closed_cnt > 0 else 0
        print(f'  {m} [{label}]: クローズ{closed_cnt}件 | 受注{won_cnt}件 | 受注率{win_rate:.1f}%')

# 商談実施者（Owner.Name）別の受注率
print('\n■ 商談実施者（Owner.Name）別 受注率 TOP15')
owner_stats = []
for owner in df_filtered['Owner.Name'].unique():
    od = df_filtered[df_filtered['Owner.Name'] == owner]
    closed = od[od['IsClosed'] == True]
    won = closed[closed['IsWon'] == True]
    if len(closed) >= 5:  # 5件以上のみ
        owner_stats.append({
            'Owner': owner, 'Total': len(od), 'Closed': len(closed),
            'Won': len(won), 'WinRate': len(won)/len(closed)*100
        })

owner_df = pd.DataFrame(owner_stats).sort_values('WinRate', ascending=False)
print(owner_df.head(15).to_string(index=False))

# ========================================
# 4. サービス形態（FacilityType_Large__c）別分析
# ========================================
print('\n' + '=' * 80)
print('【分析4】サービス形態（FacilityType_Large__c）× 月別 受注件数・受注率')
print('=' * 80)

facility = df_filtered[df_filtered['FacilityType_Large__c'].notna()].copy()

print('\n■ 施設形態別 月別 受注件数')
for ft in facility['FacilityType_Large__c'].unique():
    ft_data = facility[facility['FacilityType_Large__c'] == ft]
    print(f'\n  {ft}:')
    for m in months:
        md = ft_data[ft_data['CloseYM'] == m]
        if len(md) == 0:
            continue
        closed = md[md['IsClosed'] == True]
        won = closed[closed['IsWon'] == True]
        lost = closed[closed['StageName'] == '失注']
        closed_cnt = len(closed)
        won_cnt = len(won)
        win_rate = (won_cnt / closed_cnt * 100) if closed_cnt > 0 else 0
        print(f'    {m}: 全{len(md)}件 | クローズ{closed_cnt}件 | 受注{won_cnt}件 | 失注{len(lost)}件 | 受注率{win_rate:.1f}%')

print('\n■ 施設形態別 受注率クロス集計')
fac_summary = []
for ft in facility['FacilityType_Large__c'].unique():
    for m in months:
        md = facility[(facility['FacilityType_Large__c'] == ft) & (facility['CloseYM'] == m)]
        closed = md[md['IsClosed'] == True]
        won = closed[closed['IsWon'] == True]
        if len(closed) > 0:
            fac_summary.append({'Facility': ft, 'Month': m, 'Won': len(won),
                              'Closed': len(closed), 'WinRate': len(won)/len(closed)*100})

fac_df = pd.DataFrame(fac_summary)
if len(fac_df) > 0:
    pivot_fac = fac_df.pivot_table(index='Facility', columns='Month', values='WinRate', aggfunc='first')
    print(pivot_fac.round(1).to_string())

# ========================================
# 5. 失注理由の小項目分析
# ========================================
print('\n' + '=' * 80)
print('【分析5】失注理由の小項目分析（LostReason_Small__c）')
print('=' * 80)

lost_all = df_filtered[df_filtered['StageName'] == '失注'].copy()

print('\n■ 月別 失注理由小項目（上位15）')
for m in months:
    md = lost_all[lost_all['CloseYM'] == m]
    if len(md) == 0:
        continue
    print(f'\n  {m}（失注{len(md)}件）:')
    reasons = md['LostReason_Small__c'].value_counts().head(15)
    for reason, cnt in reasons.items():
        print(f'    {cnt:3d}件 | {reason}')

# 1月→2月の変化
print('\n■ 1月→2月の失注理由小項目 変化')
jan = lost_all[lost_all['CloseYM'] == '2026-01']
feb = lost_all[lost_all['CloseYM'] == '2026-02']

jan_reasons = jan['LostReason_Small__c'].value_counts()
feb_reasons = feb['LostReason_Small__c'].value_counts()

all_reasons = set(jan_reasons.index.tolist() + feb_reasons.index.tolist())
changes = []
for r in all_reasons:
    j = jan_reasons.get(r, 0)
    f = feb_reasons.get(r, 0)
    changes.append({'Reason': r, 'Jan': j, 'Feb': f, 'Diff': f - j})

change_df = pd.DataFrame(changes).sort_values('Diff', ascending=False)
print('\n  増加順:')
for _, row in change_df.head(10).iterrows():
    print(f'    {row["Diff"]:+3d} | 1月{row["Jan"]:3d}件→2月{row["Feb"]:3d}件 | {row["Reason"]}')
print('\n  減少順:')
for _, row in change_df.tail(10).iterrows():
    print(f'    {row["Diff"]:+3d} | 1月{row["Jan"]:3d}件→2月{row["Feb"]:3d}件 | {row["Reason"]}')

# 「サービスに価値を感じていない」の深掘り
print('\n■ 「サービスに価値を感じていない」の深掘り')
no_value = lost_all[lost_all['LostReason_Large__c'] == 'サービスに価値を感じていない'].copy()
print(f'  全期間 該当件数: {len(no_value)}件')

print('\n  月別件数:')
for m in months:
    md = no_value[no_value['CloseYM'] == m]
    if len(md) == 0:
        continue
    print(f'    {m}: {len(md)}件')
    # 小項目内訳
    small = md['LostReason_Small__c'].value_counts()
    for reason, cnt in small.items():
        print(f'      {cnt:3d}件 | {reason}')

# 「サービスに価値を感じていない」のチーム別
print('\n  ■ 1月→2月「サービスに価値を感じていない」チーム別変化')
nv_jan = no_value[no_value['CloseYM'] == '2026-01']
nv_feb = no_value[no_value['CloseYM'] == '2026-02']
jan_teams = nv_jan['Team'].value_counts()
feb_teams = nv_feb['Team'].value_counts()
all_t = set(jan_teams.index.tolist() + feb_teams.index.tolist())
t_changes = []
for t in all_t:
    j = jan_teams.get(t, 0)
    f = feb_teams.get(t, 0)
    t_changes.append({'Team': t, 'Jan': j, 'Feb': f, 'Diff': f - j})
t_df = pd.DataFrame(t_changes).sort_values('Diff', ascending=False)
for _, row in t_df.iterrows():
    if row['Diff'] != 0:
        print(f'    {row["Diff"]:+3d} | 1月{row["Jan"]:2d}件→2月{row["Feb"]:2d}件 | {row["Team"]}')

# 「サービスに価値を感じていない」の施設形態別
print('\n  ■ 1月→2月「サービスに価値を感じていない」施設形態別変化')
jan_fac = nv_jan['FacilityType_Large__c'].value_counts()
feb_fac = nv_feb['FacilityType_Large__c'].value_counts()
all_f = set(list(jan_fac.index) + list(feb_fac.index))
for f in sorted(all_f):
    j = jan_fac.get(f, 0)
    ff = feb_fac.get(f, 0)
    diff = ff - j
    if diff != 0:
        print(f'    {diff:+3d} | 1月{j:2d}件→2月{ff:2d}件 | {f}')

# ========================================
# 6. 営業日数効率
# ========================================
print('\n' + '=' * 80)
print('【分析6】商談の営業日数効率 - 1営業日あたり受注件数')
print('=' * 80)

# 営業日数定義
biz_days = {
    '2025-11': 20,
    '2025-12': 21,
    '2026-01': 19,
    '2026-02': 19,
}

print('\n■ 月別 1営業日あたり受注件数')
print(f'  {"月":>8s} | {"営業日":>4s} | {"受注件数":>6s} | {"1日あたり":>8s} | {"受注金額(万)":>10s} | {"1日あたり金額(万)":>14s}')
print('  ' + '-' * 70)

efficiency_data = []
for m in ['2025-11', '2025-12', '2026-01', '2026-02']:
    md = df_filtered[(df_filtered['CloseYM'] == m) & (df_filtered['IsWon'] == True)]
    won_cnt = len(md)
    amount = md['Amount'].sum()
    days = biz_days.get(m, 20)
    per_day = won_cnt / days
    amount_per_day = amount / days / 10000
    amount_total = amount / 10000
    print(f'  {m:>8s} | {days:>4d}日 | {won_cnt:>6d}件 | {per_day:>8.2f}件 | {amount_total:>10.0f}万 | {amount_per_day:>14.1f}万')
    efficiency_data.append({'Month': m, 'Days': days, 'Won': won_cnt, 'PerDay': per_day,
                           'Amount': amount, 'AmountPerDay': amount_per_day})

# 前月比
print('\n■ 前月比較')
for i in range(1, len(efficiency_data)):
    curr = efficiency_data[i]
    prev = efficiency_data[i-1]
    cnt_change = (curr['PerDay'] - prev['PerDay']) / prev['PerDay'] * 100 if prev['PerDay'] > 0 else 0
    amt_change = (curr['AmountPerDay'] - prev['AmountPerDay']) / prev['AmountPerDay'] * 100 if prev['AmountPerDay'] > 0 else 0
    print(f'  {prev["Month"]}→{curr["Month"]}: '
          f'受注件数/日 {cnt_change:+.1f}% | 受注金額/日 {amt_change:+.1f}%')

# チーム別の営業日数効率（2月）
print('\n■ チーム別 2月の1営業日あたり受注件数')
feb_won = df_filtered[(df_filtered['CloseYM'] == '2026-02') & (df_filtered['IsWon'] == True)]
team_eff = feb_won.groupby('Team').agg(
    Won=('Id', 'count'),
    Amount=('Amount', 'sum')
).reset_index()
team_eff['PerDay'] = team_eff['Won'] / 19
team_eff['AmountPerDay'] = team_eff['Amount'] / 19 / 10000
team_eff = team_eff.sort_values('Won', ascending=False)
print(team_eff.to_string(index=False))

# ========================================
# 追加: チーム別総合サマリ
# ========================================
print('\n' + '=' * 80)
print('【補足】チーム別 月別 受注率サマリ')
print('=' * 80)

team_monthly = []
for team in df_filtered['Team'].unique():
    for m in months:
        md = df_filtered[(df_filtered['Team'] == team) & (df_filtered['CloseYM'] == m)]
        closed = md[md['IsClosed'] == True]
        won = closed[closed['IsWon'] == True]
        if len(closed) >= 3:  # 3件以上
            team_monthly.append({
                'Team': team, 'Month': m,
                'Total': len(md), 'Closed': len(closed), 'Won': len(won),
                'WinRate': len(won)/len(closed)*100
            })

tm_df = pd.DataFrame(team_monthly)
if len(tm_df) > 0:
    pivot_tm = tm_df.pivot_table(index='Team', columns='Month', values='WinRate', aggfunc='first')
    print('\n受注率（クローズ済みベース、3件以上のチームのみ）:')
    print(pivot_tm.round(1).to_string())

    # 件数
    pivot_won = tm_df.pivot_table(index='Team', columns='Month', values='Won', aggfunc='first')
    print('\n受注件数:')
    print(pivot_won.to_string())

print('\n' + '=' * 80)
print('分析完了')
print('=' * 80)
