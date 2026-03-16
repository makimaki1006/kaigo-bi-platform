"""
2月200%達成の深層分析
- 外れ値検出・影響度分析
- 統計的有意性検定
- パイプライン速度分析
- 集中度リスク分析
- ステージ遷移分析
- 担当者パフォーマンス分解
"""
import pandas as pd
import numpy as np
from scipy import stats
import re
import warnings
warnings.filterwarnings('ignore')

# データ読み込み
df_units = pd.read_csv('data/output/analysis/opportunities_with_units.csv', dtype=str)
df_base = pd.read_csv('data/output/analysis/opportunities_20260305_115216.csv', dtype=str)[['Id', 'CreatedDate']]
df = df_units.merge(df_base, on='Id', how='left')

df['CloseDate'] = pd.to_datetime(df['CloseDate'])
df['CreatedDate'] = pd.to_datetime(df['CreatedDate'], utc=True).dt.tz_localize(None)
df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce').fillna(0)
df['IsWon'] = df['IsWon'].str.lower() == 'true'
df['IsClosed'] = df['IsClosed'].str.lower() == 'true'
df['Month'] = df['CloseDate'].dt.to_period('M')
df['LT'] = (df['CloseDate'] - df['CreatedDate']).dt.days

def extract_team(u):
    if pd.isna(u): return '(空)'
    m = re.search(r'】(.+)$', u)
    return m.group(1) if m else u

df['Team'] = df['AppointUnit__r.Name'].apply(extract_team)

# 新規営業チームのみ
exclude = ['(空)', 'マーケ', 'マーケ兼AI', 'エンタープライズ', 'プロダクト', 'MEO', 'セールス部', '株式会社CyXen']
df = df[~df['Team'].isin(exclude)].copy()

target = ['2025-11', '2025-12', '2026-01', '2026-02']
df_t = df[df['Month'].astype(str).isin(target)]
won = df_t[df_t['IsWon']].copy()

print('=' * 100)
print('  深層分析: 2月200%達成の要因（新規営業チームのみ）')
print('=' * 100)

# ============================================================
# 1. 外れ値分析
# ============================================================
print('\n' + '=' * 100)
print('  1. 外れ値分析')
print('=' * 100)

for m in target:
    m_won = won[won['Month'].astype(str) == m]
    amounts = m_won['Amount']
    if len(amounts) == 0:
        continue

    q1 = amounts.quantile(0.25)
    q3 = amounts.quantile(0.75)
    iqr = q3 - q1
    upper = q3 + 1.5 * iqr
    lower = q1 - 1.5 * iqr

    outliers = m_won[(amounts > upper) | (amounts < lower)]
    non_outliers = m_won[(amounts <= upper) & (amounts >= lower)]

    print(f'\n  [{m}] 受注{len(m_won)}件')
    print(f'    分布: 平均{amounts.mean():,.0f} / 中央値{amounts.median():,.0f} / 標準偏差{amounts.std():,.0f}')
    print(f'    四分位: Q1={q1:,.0f} / Q3={q3:,.0f} / IQR={iqr:,.0f}')
    print(f'    外れ値閾値: 上限{upper:,.0f} / 下限{lower:,.0f}')
    print(f'    外れ値: {len(outliers)}件 (合計{outliers["Amount"].sum():,.0f})')
    if len(outliers) > 0:
        for _, row in outliers.sort_values('Amount', ascending=False).iterrows():
            print(f'      {row["Amount"]:>12,.0f} {row["Team"]:12s} {row["Name"][:45]}')
    print(f'    外れ値除外後: {len(non_outliers)}件 / 平均{non_outliers["Amount"].mean():,.0f} / 合計{non_outliers["Amount"].sum():,.0f}')

# 外れ値除外後の月別比較
print('\n  --- 外れ値除外後の月別比較 ---')
for m in target:
    m_won = won[won['Month'].astype(str) == m]
    amounts = m_won['Amount']
    q1, q3 = amounts.quantile(0.25), amounts.quantile(0.75)
    iqr = q3 - q1
    normal = m_won[(amounts <= q3 + 1.5 * iqr) & (amounts >= q1 - 1.5 * iqr)]
    print(f'    {m}: {len(normal)}件 / 平均{normal["Amount"].mean():,.0f} / 合計{normal["Amount"].sum():,.0f}')

# ============================================================
# 2. 統計的有意性検定
# ============================================================
print('\n' + '=' * 100)
print('  2. 統計的有意性検定')
print('=' * 100)

jan_won = won[won['Month'].astype(str) == '2026-01']['Amount']
feb_won = won[won['Month'].astype(str) == '2026-02']['Amount']
nov_won = won[won['Month'].astype(str) == '2025-11']['Amount']
dec_won = won[won['Month'].astype(str) == '2025-12']['Amount']

# Mann-Whitney U検定（受注金額の分布差）
print('\n  --- Mann-Whitney U検定（受注金額分布）---')
for label, a, b in [('1月 vs 2月', jan_won, feb_won),
                      ('11月 vs 2月', nov_won, feb_won),
                      ('12月 vs 2月', dec_won, feb_won)]:
    if len(a) > 0 and len(b) > 0:
        stat, p = stats.mannwhitneyu(a, b, alternative='two-sided')
        sig = '有意' if p < 0.05 else '有意でない'
        print(f'    {label}: U={stat:.0f}, p={p:.4f} → {sig}（α=0.05）')

# 受注率のカイ二乗検定
print('\n  --- 受注率のカイ二乗検定 ---')
for m1, m2, label in [('2026-01', '2026-02', '1月 vs 2月'),
                        ('2025-11', '2026-02', '11月 vs 2月')]:
    m1_all = df_t[df_t['Month'].astype(str) == m1]
    m2_all = df_t[df_t['Month'].astype(str) == m2]
    m1_closed = m1_all[m1_all['StageName'].isin(['受注', '失注', '商談キャンセル', '無効商談'])]
    m2_closed = m2_all[m2_all['StageName'].isin(['受注', '失注', '商談キャンセル', '無効商談'])]

    m1_won = m1_closed[m1_closed['IsWon']].shape[0]
    m1_lost = len(m1_closed) - m1_won
    m2_won = m2_closed[m2_closed['IsWon']].shape[0]
    m2_lost = len(m2_closed) - m2_won

    contingency = [[m1_won, m1_lost], [m2_won, m2_lost]]
    chi2, p, dof, expected = stats.chi2_contingency(contingency)
    sig = '有意' if p < 0.05 else '有意でない'
    print(f'    {label}: χ²={chi2:.2f}, p={p:.4f} → {sig}')
    print(f'      {m1}: {m1_won}/{len(m1_closed)} ({m1_won/len(m1_closed)*100:.1f}%)')
    print(f'      {m2}: {m2_won}/{len(m2_closed)} ({m2_won/len(m2_closed)*100:.1f}%)')

# ============================================================
# 3. 集中度・偏り分析
# ============================================================
print('\n' + '=' * 100)
print('  3. 集中度・偏り分析（リスク評価）')
print('=' * 100)

for m in target:
    m_won = won[won['Month'].astype(str) == m].copy()
    if len(m_won) == 0:
        continue

    total_amount = m_won['Amount'].sum()

    # 上位3件の集中度
    top3 = m_won.nlargest(3, 'Amount')
    top3_pct = top3['Amount'].sum() / total_amount * 100 if total_amount > 0 else 0

    # 上位1件の集中度
    top1_pct = m_won['Amount'].max() / total_amount * 100 if total_amount > 0 else 0

    # 担当者集中度（上位3人）
    owner_totals = m_won.groupby('Owner.Name')['Amount'].sum().sort_values(ascending=False)
    top3_owners = owner_totals.head(3)
    owner_top3_pct = top3_owners.sum() / total_amount * 100 if total_amount > 0 else 0

    # チーム集中度
    team_totals = m_won.groupby('Team')['Amount'].sum().sort_values(ascending=False)
    top2_teams = team_totals.head(2)
    team_top2_pct = top2_teams.sum() / total_amount * 100 if total_amount > 0 else 0

    # ジニ係数（金額の不均等度）
    amounts_sorted = np.sort(m_won['Amount'].values)
    n = len(amounts_sorted)
    index = np.arange(1, n + 1)
    gini = (2 * np.sum(index * amounts_sorted) - (n + 1) * np.sum(amounts_sorted)) / (n * np.sum(amounts_sorted)) if np.sum(amounts_sorted) > 0 else 0

    print(f'\n  [{m}] 受注{len(m_won)}件 / {total_amount:,.0f}円')
    print(f'    案件集中: Top1={top1_pct:.1f}% / Top3={top3_pct:.1f}%')
    print(f'    担当者集中(Top3): {owner_top3_pct:.1f}% ({", ".join(f"{n}:{v:,.0f}" for n, v in top3_owners.items())})')
    print(f'    チーム集中(Top2): {team_top2_pct:.1f}% ({", ".join(f"{n}:{v:,.0f}" for n, v in top2_teams.items())})')
    print(f'    ジニ係数: {gini:.3f} (0=完全均等, 1=完全集中)')

# ============================================================
# 4. パイプライン速度（Velocity）分析
# ============================================================
print('\n' + '=' * 100)
print('  4. パイプライン速度分析')
print('=' * 100)

print('\n  Pipeline Velocity = 商談数 × 受注率 × 平均単価 / 平均リードタイム')
for m in target:
    m_all = df_t[df_t['Month'].astype(str) == m]
    m_closed = m_all[m_all['StageName'].isin(['受注', '失注', '商談キャンセル', '無効商談'])]
    m_won = won[won['Month'].astype(str) == m]

    n_opps = len(m_closed)
    win_rate = len(m_won) / n_opps if n_opps > 0 else 0
    avg_deal = m_won['Amount'].mean() if len(m_won) > 0 else 0
    avg_lt = m_won['LT'].mean() if len(m_won) > 0 else 1
    avg_lt = max(avg_lt, 1)

    velocity = n_opps * win_rate * avg_deal / avg_lt

    print(f'\n  [{m}]')
    print(f'    商談数={n_opps} × 受注率={win_rate:.3f} × 平均単価={avg_deal:,.0f} / LT={avg_lt:.1f}日')
    print(f'    Velocity = {velocity:,.0f} 円/日')

# ============================================================
# 5. 受注・失注のパターン分析（ロジスティック的）
# ============================================================
print('\n' + '=' * 100)
print('  5. 受注 vs 失注の特徴比較（2月）')
print('=' * 100)

feb_all = df_t[df_t['Month'].astype(str) == '2026-02']
feb_closed = feb_all[feb_all['StageName'].isin(['受注', '失注'])]
feb_w = feb_closed[feb_closed['IsWon']]
feb_l = feb_closed[feb_closed['StageName'] == '失注']

print(f'\n  受注{len(feb_w)}件 vs 失注{len(feb_l)}件')

# チーム別受注率
print('\n  --- チーム別受注率（2月）---')
team_wr = feb_closed.groupby('Team').apply(
    lambda x: pd.Series({
        '受注': x['IsWon'].sum(),
        '失注': (~x['IsWon']).sum(),
        '受注率': x['IsWon'].mean() * 100,
        '商談数': len(x)
    })
).sort_values('受注率', ascending=False)
team_wr_filtered = team_wr[team_wr['商談数'] >= 5]
print(team_wr_filtered.to_string())

# サービス形態別受注率
print('\n  --- サービス形態別受注率（2月）---')
fac_wr = feb_closed.groupby('FacilityType_Large__c').apply(
    lambda x: pd.Series({
        '受注': x['IsWon'].sum(),
        '失注': (~x['IsWon']).sum(),
        '受注率': x['IsWon'].mean() * 100,
        '商談数': len(x)
    })
).sort_values('受注率', ascending=False)
print(fac_wr.to_string())

# アポランク別受注率
print('\n  --- アポランク別受注率（2月）---')
rank_wr = feb_closed.groupby('AppointRank__c').apply(
    lambda x: pd.Series({
        '受注': x['IsWon'].sum(),
        '失注': (~x['IsWon']).sum(),
        '受注率': x['IsWon'].mean() * 100,
        '商談数': len(x)
    })
).sort_values('受注率', ascending=False)
print(rank_wr.to_string())

# ============================================================
# 6. 担当者パフォーマンス分解
# ============================================================
print('\n' + '=' * 100)
print('  6. 担当者パフォーマンス分解（1月→2月）')
print('=' * 100)

# 受注者ごとの変化を分解
jan_all = df_t[df_t['Month'].astype(str) == '2026-01']
feb_all_data = df_t[df_t['Month'].astype(str) == '2026-02']

jan_closed = jan_all[jan_all['StageName'].isin(['受注', '失注', '商談キャンセル', '無効商談'])]
feb_closed_all = feb_all_data[feb_all_data['StageName'].isin(['受注', '失注', '商談キャンセル', '無効商談'])]

print('\n  --- 主要担当者の1月→2月変化 ---')
all_owners = set(jan_closed['Owner.Name'].unique()) | set(feb_closed_all['Owner.Name'].unique())

owner_data = []
for owner in all_owners:
    jan_o = jan_closed[jan_closed['Owner.Name'] == owner]
    feb_o = feb_closed_all[feb_closed_all['Owner.Name'] == owner]

    jan_w = jan_o[jan_o['IsWon']].shape[0]
    feb_w = feb_o[feb_o['IsWon']].shape[0]
    jan_total = len(jan_o)
    feb_total = len(feb_o)
    jan_wr = jan_w / jan_total * 100 if jan_total > 0 else 0
    feb_wr = feb_w / feb_total * 100 if feb_total > 0 else 0
    jan_amt = jan_o[jan_o['IsWon']]['Amount'].sum()
    feb_amt = feb_o[feb_o['IsWon']]['Amount'].sum()

    if jan_total + feb_total >= 5:
        owner_data.append({
            'name': owner, 'jan_w': jan_w, 'feb_w': feb_w,
            'jan_total': jan_total, 'feb_total': feb_total,
            'jan_wr': jan_wr, 'feb_wr': feb_wr,
            'jan_amt': jan_amt, 'feb_amt': feb_amt,
            'diff_w': feb_w - jan_w, 'diff_wr': feb_wr - jan_wr
        })

owner_df = pd.DataFrame(owner_data).sort_values('diff_w', ascending=False)
print(f'  {"担当者":12s} {"1月受注":>6} {"1月商談":>6} {"1月率":>6} {"2月受注":>6} {"2月商談":>6} {"2月率":>6} {"受注差":>6} {"率差":>7}')
for _, r in owner_df.head(15).iterrows():
    print(f'  {r["name"]:12s} {r["jan_w"]:>6.0f} {r["jan_total"]:>6.0f} {r["jan_wr"]:>5.1f}% {r["feb_w"]:>6.0f} {r["feb_total"]:>6.0f} {r["feb_wr"]:>5.1f}% {r["diff_w"]:>+5.0f} {r["diff_wr"]:>+6.1f}%')

# ============================================================
# 7. 効果の分解（Decomposition）
# ============================================================
print('\n' + '=' * 100)
print('  7. 受注増+18件の効果分解（Decomposition）')
print('=' * 100)

jan_won_data = won[won['Month'].astype(str) == '2026-01']
feb_won_data = won[won['Month'].astype(str) == '2026-02']

# 商談名パターンで分類
def classify(name):
    is_re = '再商談' in str(name)
    is_dai = '代表者' in str(name)
    if is_re and is_dai: return '再商談×代表者'
    if is_re and not is_dai: return '再商談×担当者'
    if not is_re and is_dai: return '新規×代表者'
    if '新規' in str(name): return '新規×担当者'
    return 'その他'

jan_won_data = jan_won_data.copy()
feb_won_data = feb_won_data.copy()
jan_won_data['Pattern'] = jan_won_data['Name'].apply(classify)
feb_won_data['Pattern'] = feb_won_data['Name'].apply(classify)

print('\n  --- パターン別の件数・金額・LT ---')
patterns = ['再商談×代表者', '再商談×担当者', '新規×代表者', '新規×担当者', 'その他']
print(f'  {"パターン":14s} │{"1月件数":>6} {"1月金額":>10} {"1月LT":>6} │{"2月件数":>6} {"2月金額":>10} {"2月LT":>6} │{"件数差":>5} {"金額差":>10}')
print(f'  {"─"*14}─┼{"─"*24}─┼{"─"*24}─┼{"─"*17}')

total_diff_count = 0
total_diff_amount = 0
for p in patterns:
    jan_p = jan_won_data[jan_won_data['Pattern'] == p]
    feb_p = feb_won_data[feb_won_data['Pattern'] == p]

    jan_c, feb_c = len(jan_p), len(feb_p)
    jan_a = jan_p['Amount'].sum()
    feb_a = feb_p['Amount'].sum()
    jan_lt = jan_p['LT'].median() if len(jan_p) > 0 else 0
    feb_lt = feb_p['LT'].median() if len(feb_p) > 0 else 0

    diff_c = feb_c - jan_c
    diff_a = feb_a - jan_a
    total_diff_count += diff_c
    total_diff_amount += diff_a

    print(f'  {p:14s} │{jan_c:>6} {jan_a:>10,.0f} {jan_lt:>5.0f}日 │{feb_c:>6} {feb_a:>10,.0f} {feb_lt:>5.0f}日 │{diff_c:>+4} {diff_a:>+10,.0f}')

print(f'  {"合計":14s} │{len(jan_won_data):>6} {jan_won_data["Amount"].sum():>10,.0f}       │{len(feb_won_data):>6} {feb_won_data["Amount"].sum():>10,.0f}       │{total_diff_count:>+4} {total_diff_amount:>+10,.0f}')

# ============================================================
# 8. 金額帯別分析
# ============================================================
print('\n' + '=' * 100)
print('  8. 金額帯別の受注分布変化')
print('=' * 100)

bins = [0, 300000, 600000, 900000, 1200000, 1500000, float('inf')]
labels = ['~30万', '30~60万', '60~90万', '90~120万', '120~150万', '150万~']

for m in target:
    m_won = won[won['Month'].astype(str) == m].copy()
    m_won['AmountBin'] = pd.cut(m_won['Amount'], bins=bins, labels=labels, right=True)
    dist = m_won['AmountBin'].value_counts().sort_index()
    print(f'\n  [{m}] 受注{len(m_won)}件')
    for b in labels:
        cnt = dist.get(b, 0)
        bar = '#' * cnt
        print(f'    {b:10s}: {cnt:>3}件 {bar}')

# ============================================================
# 9. 商談区分（SFの公式区分）で再検証
# ============================================================
print('\n' + '=' * 100)
print('  9. OpportunityCategory__c（SF公式区分）での検証')
print('=' * 100)

print('\n  商談名の「再商談」とSFの「OpportunityCategory__c = 再商談」は一致するか？')
for m in target:
    m_won = won[won['Month'].astype(str) == m].copy()
    name_re = m_won['Name'].str.contains('再商談', na=False)
    sf_re = m_won['OpportunityCategory__c'] == '再商談'

    both = (name_re & sf_re).sum()
    name_only = (name_re & ~sf_re).sum()
    sf_only = (~name_re & sf_re).sum()
    neither = (~name_re & ~sf_re).sum()

    print(f'  [{m}] 名前「再商談」={name_re.sum()} / SF区分「再商談」={sf_re.sum()} / 一致={both} / 名前のみ={name_only} / SF区分のみ={sf_only}')

# ============================================================
# 10. 月ごとの新規参入チームの影響
# ============================================================
print('\n' + '=' * 100)
print('  10. チーム構成変化の影響（同一チーム比較）')
print('=' * 100)

# 4ヶ月全てに存在するチームだけで比較
all_month_teams = set()
for m in target:
    m_won = won[won['Month'].astype(str) == m]
    if m == target[0]:
        all_month_teams = set(m_won['Team'].unique())
    else:
        all_month_teams &= set(m_won['Team'].unique())

print(f'\n  4ヶ月全てで受注があるチーム: {all_month_teams}')

if all_month_teams:
    common_won = won[won['Team'].isin(all_month_teams)]
    print('\n  --- 共通チームのみの受注推移 ---')
    for m in target:
        m_common = common_won[common_won['Month'].astype(str) == m]
        print(f'    {m}: {len(m_common)}件 / {m_common["Amount"].sum():,.0f}円')

# 1月と2月の両方に存在するチーム
jan_teams = set(won[won['Month'].astype(str) == '2026-01']['Team'].unique())
feb_teams = set(won[won['Month'].astype(str) == '2026-02']['Team'].unique())
common_jan_feb = jan_teams & feb_teams
only_feb = feb_teams - jan_teams

print(f'\n  1月にも2月にも受注があるチーム: {common_jan_feb}')
print(f'  2月のみ受注があるチーム（1月=0件）: {only_feb}')

# 共通チームだけでの増減
common_jan = won[(won['Month'].astype(str) == '2026-01') & won['Team'].isin(common_jan_feb)]
common_feb = won[(won['Month'].astype(str) == '2026-02') & won['Team'].isin(common_jan_feb)]
new_feb = won[(won['Month'].astype(str) == '2026-02') & won['Team'].isin(only_feb)]

print(f'\n  共通チームの受注: 1月{len(common_jan)}件 → 2月{len(common_feb)}件 (差{len(common_feb)-len(common_jan):+d}件)')
print(f'  2月のみチームの寄与: {len(new_feb)}件 / {new_feb["Amount"].sum():,.0f}円')

# ============================================================
# 11. Bootstrap信頼区間
# ============================================================
print('\n' + '=' * 100)
print('  11. Bootstrap信頼区間（受注金額の真の平均）')
print('=' * 100)

np.random.seed(42)
n_bootstrap = 10000

for m in target:
    m_won = won[won['Month'].astype(str) == m]['Amount'].values
    if len(m_won) < 3:
        continue

    boot_means = []
    for _ in range(n_bootstrap):
        sample = np.random.choice(m_won, size=len(m_won), replace=True)
        boot_means.append(sample.mean())

    ci_low = np.percentile(boot_means, 2.5)
    ci_high = np.percentile(boot_means, 97.5)

    print(f'  [{m}] 平均{np.mean(m_won):,.0f} / 95%CI: [{ci_low:,.0f} - {ci_high:,.0f}]')

print('\n  → 信頼区間が重なっていれば「統計的に差があるとは言えない」')

print('\n' + '=' * 100)
print('  分析完了')
print('=' * 100)
