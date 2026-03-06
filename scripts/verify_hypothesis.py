"""仮説検証: 投入量 vs 架電率 vs 成績の関係を4ヶ月分で検証"""
import pandas as pd

df_lead = pd.read_csv('data/output/analysis/leads_20260305_115216.csv', dtype=str)
df_opp = pd.read_csv('data/output/analysis/opportunities_20260305_115146.csv', dtype=str)

df_lead['CreatedDate'] = pd.to_datetime(df_lead['CreatedDate'], utc=True).dt.tz_localize(None)
df_lead['IsConverted'] = df_lead['IsConverted'].str.lower() == 'true'
df_lead['CreateMonth'] = df_lead['CreatedDate'].dt.to_period('M')

df_opp['CloseDate'] = pd.to_datetime(df_opp['CloseDate'])
df_opp['Amount'] = pd.to_numeric(df_opp['Amount'], errors='coerce').fillna(0)
df_opp['IsWon'] = df_opp['IsWon'].str.lower() == 'true'

target = ['2025-11', '2025-12', '2026-01', '2026-02']

print('=' * 110)
print('  仮説検証: 「投入量が少ない → 架電率が上がる → 成績が良くなる」は成り立つか？')
print('=' * 110)

print()
header = f'{"月":<8} {"投入数":>8} {"架電済":>8} {"架電率":>8} {"未架電":>8} {"追客募集有":>8} {"CVR":>8} {"受注":>6} {"受注額":>12} {"受注率":>8}'
print(header)
print('-' * 110)

data = []
for m in target:
    leads = df_lead[df_lead['CreateMonth'].astype(str) == m]
    total = len(leads)
    uncalled = (leads['Status'] == '未架電').sum()
    called = total - uncalled
    call_rate = called / total * 100 if total > 0 else 0
    tsuikyaku = (leads['Status'] == '01 追客 - 募集あり').sum()
    converted = leads['IsConverted'].sum()

    y, mo = m.split('-')
    won = df_opp[(df_opp['CloseDate'].dt.year == int(y)) & (df_opp['CloseDate'].dt.month == int(mo)) & df_opp['IsWon']]
    won_count = len(won)
    won_amount = won['Amount'].sum()

    all_month = df_opp[(df_opp['CloseDate'].dt.year == int(y)) & (df_opp['CloseDate'].dt.month == int(mo))]
    closed = all_month[all_month['StageName'].isin(['受注', '失注', '商談キャンセル', '無効商談'])]
    win_rate = won_count / len(closed) * 100 if len(closed) > 0 else 0

    print(f'{m:<8} {total:>8,} {called:>8,} {call_rate:>7.1f}% {uncalled:>8,} {tsuikyaku:>8} {converted:>8} {won_count:>6} {won_amount:>12,.0f} {win_rate:>7.1f}%')

    data.append({
        'month': m, 'total': total, 'called': called,
        'call_rate': call_rate, 'tsuikyaku': tsuikyaku,
        'converted': converted, 'won': won_count, 'won_amount': won_amount,
        'win_rate': win_rate
    })


print()
print('=' * 110)
print('  検証1: 投入数 vs 架電率（逆相関があるか？）')
print('=' * 110)
print()
for d in sorted(data, key=lambda x: x['total']):
    bar = '#' * int(d['call_rate'] / 2)
    print(f'  {d["month"]}  投入{d["total"]:>8,}件  →  架電率 {d["call_rate"]:5.1f}%  {bar}')

print()
print('  判定: ', end='')
# 投入数の昇順で架電率が単調増加するか
sorted_by_total = sorted(data, key=lambda x: x['total'])
rates = [d['call_rate'] for d in sorted_by_total]
if all(rates[i] >= rates[i+1] for i in range(len(rates)-1)):
    print('完全に成立（投入が少ないほど架電率が高い）')
else:
    # 12月除外で確認
    data_no_dec = [d for d in data if d['month'] != '2025-12']
    sorted_no_dec = sorted(data_no_dec, key=lambda x: x['total'])
    rates_no_dec = [d['call_rate'] for d in sorted_no_dec]
    if all(rates_no_dec[i] >= rates_no_dec[i+1] for i in range(len(rates_no_dec)-1)):
        print('12月を除けば成立（12月は5.2万件の大量投入で異常値）')
    else:
        print('単純な逆相関は認められない')


print()
print('=' * 110)
print('  検証2: 架電「件数」（絶対量）vs 成績')
print('=' * 110)
print()
for d in sorted(data, key=lambda x: x['called']):
    bar_called = '#' * int(d['called'] / 200)
    bar_won = '*' * d['won']
    print(f'  {d["month"]}  架電{d["called"]:>6,}件  受注{d["won"]:>3}件  コンバート{d["converted"]:>3}件')

print()
print('  判定: ', end='')
sorted_by_called = sorted(data, key=lambda x: x['called'])
if all(sorted_by_called[i]['won'] <= sorted_by_called[i+1]['won'] for i in range(len(sorted_by_called)-1)):
    print('完全に成立（架電が多いほど受注が多い）')
else:
    print('単純な正相関は認められない')
    print('  → 12月が反例: 架電4,335件だが受注52件（最多）')


print()
print('=' * 110)
print('  検証3: 追客-募集あり（架電の質）vs 成績')
print('=' * 110)
print()
for d in sorted(data, key=lambda x: x['tsuikyaku']):
    print(f'  {d["month"]}  追客募集有{d["tsuikyaku"]:>4}件  コンバート{d["converted"]:>3}件  受注{d["won"]:>3}件  受注率{d["win_rate"]:5.1f}%')


print()
print('=' * 110)
print('  検証4: 12月の特殊性（5.2万件インポートの内訳）')
print('=' * 110)
print()

dec_leads = df_lead[df_lead['CreateMonth'].astype(str) == '2025-12']
print(f'  12月リード総数: {len(dec_leads):,}')
print()
print('  Status分布:')
status_dist = dec_leads['Status'].fillna('(空)').value_counts()
for st, cnt in status_dist.items():
    pct = cnt / len(dec_leads) * 100
    bar = '#' * int(pct / 2)
    print(f'    {st:<25} {cnt:>8,}件 ({pct:5.1f}%) {bar}')

print()
print('  LeadSource分布:')
source_dist = dec_leads['LeadSource'].fillna('(空/未設定)').value_counts()
for src, cnt in source_dist.head(10).items():
    print(f'    {src}: {cnt:,}件')

print()
print('  Owner分布（上位10名）:')
owner_dist = dec_leads['Owner.Name'].fillna('(空)').value_counts()
for owner, cnt in owner_dist.head(10).items():
    print(f'    {owner}: {cnt:,}件')


print()
print('=' * 110)
print('  検証5: コンバート元リードの作成月を確認（受注は本当にその月のリードから来たか？）')
print('=' * 110)
print()

converted = df_lead[df_lead['IsConverted']].copy()
converted['ConvertedDate'] = pd.to_datetime(converted['ConvertedDate'], utc=True).dt.tz_localize(None)
converted['ConvertMonth'] = converted['ConvertedDate'].dt.to_period('M')

for m in target:
    month_conv = converted[converted['ConvertMonth'].astype(str) == m]
    if len(month_conv) == 0:
        continue
    print(f'  [{m}] コンバート{len(month_conv)}件の作成月内訳:')
    create_dist = month_conv['CreateMonth'].astype(str).value_counts().sort_index()
    for cm, cnt in create_dist.items():
        pct = cnt / len(month_conv) * 100
        label = '← 当月' if cm == m else ''
        print(f'    {cm}作成: {cnt}件 ({pct:.0f}%) {label}')
    print()


print()
print('=' * 110)
print('  総合判定')
print('=' * 110)
print()
print('  仮説「投入量が少ない → 架電率が上がる → 成績が良くなる」')
print()
print('  [投入量 → 架電率] ')
print('    11月(7,730件→52.6%), 1月(9,543件→38.1%), 2月(6,795件→78.4%)')
print('    → 11月と2月は成立するが、1月は11月より投入多く架電率も低い = 成立')
print('    → ただし12月(51,984件→8.3%)は大量投入の特殊ケース')
print()
print('  [架電率 → 受注] ')
print('    2月(78.4%→45件), 11月(52.6%→45件), 12月(8.3%→52件), 1月(38.1%→27件)')
print('    → 12月が反例（架電率最低だが受注最多）')
print('    → 12月の受注は当月以前のリードからも来ている可能性')
