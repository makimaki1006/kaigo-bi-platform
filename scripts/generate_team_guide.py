"""
現場向けレポート検索キーワードガイドを生成
"""
import sys
import json
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# 最新のJSONファイルを取得
json_dir = project_root / 'data' / 'output' / 'keyword_dictionary'
json_files = sorted(json_dir.glob('keyword_dictionary_*.json'))
json_path = json_files[-1]

with open(json_path, 'r', encoding='utf-8') as f:
    data = json.load(f)

lines = []

# ===== ヘッダー =====
lines.append('# Salesforce レポート検索キーワード一覧')
lines.append('')
lines.append('リード・取引先のメモ系フィールドに入っているキーワードの一覧です。')
lines.append('レポート作成やリストビューのフィルタ条件として活用してください。')
lines.append('')
lines.append('最終更新: 2026-02-08')
lines.append('')
lines.append('---')
lines.append('')

# ===== 1. LeadSource =====
lines.append('## 1. リードソース（LeadSource）')
lines.append('')
lines.append('**対象**: リード | **用途**: リードの流入元大分類')
lines.append('')
lead_source = data['Lead']['field_keywords'].get('LeadSource', {})
lines.append('| キーワード | 件数 | 説明 |')
lines.append('|-----------|------|------|')
desc_map = {
    'ハローワーク': 'ハローワーク求人からの取込',
    'Other': 'その他（分類未設定）',
    '看護roo': '看護roo媒体経由',
    'ナース専科': 'ナース専科媒体経由',
    'きらケア': 'きらケア媒体経由',
    '看護のお仕事': '看護のお仕事媒体経由',
    '発注ナビ': '発注ナビ経由',
}
for val, count in sorted(lead_source.items(), key=lambda x: x[1], reverse=True):
    desc = desc_map.get(val, '')
    if not desc:
        if 'ハロワ_' in val and 'medica' in val:
            desc = 'ハロワ×medica対象リスト'
        elif 'ハロワ_' in val:
            desc = 'ハローワーク（バッチ別）'
        elif 'チョディカ' in val and '厨房' in val:
            desc = 'チョディカ_厨房委託業者限定'
        elif 'チョディカ' in val:
            desc = 'チョディカ（メディカル系）'
        elif 'リハプライド' in val:
            desc = 'リハプライド媒体経由'
        elif '担当者代表管理者級' in val:
            desc = 'ハロワ新規_決裁者リスト'
    lines.append(f'| {val} | {count:,} | {desc} |')
lines.append('')

# ===== 2. LeadSourceMemo =====
lines.append('## 2. リードソースメモ（LeadSourceMemo）')
lines.append('')
lines.append('**対象**: リード | **用途**: バッチ処理ごとの詳細識別。「含む」検索で絞込可能')
lines.append('')
lines.append('### 命名パターン')
lines.append('')
lines.append('| パターン | 例 | 意味 |')
lines.append('|---------|-----|------|')
lines.append('| `YYYYMMDD_ハロワ_職種` | 20260203_ハロワ_介護 | ハロワ定例更新（日付・職種別） |')
lines.append('| `有料媒体新規_媒体名_月` | 有料媒体新規_看護るー_11月 | 有料媒体からの新規リード |')
lines.append('| `【新規作成】有料媒体突合 日付` | 【新規作成】有料媒体突合 2026-02-03 | 突合パイプラインで作成された新規 |')
lines.append('| `【TW新規_固定】タウンワーク突合...` | 【TW新規_固定】タウンワーク突合 2026-01-21 | タウンワーク突合バッチ |')
lines.append('| `新規 クリニックYYYYMMDD` | 新規 クリニック20151212 | 初期クリニックリスト |')
lines.append('| `対象拡大_チョディカ_...` | 対象拡大_チョディカ_10月新規リスト | チョディカ拡大リスト |')
lines.append('| `[YYYY-MM-DD ハロワセグメント]` | [2026-01-07 ハロワセグメント] | ハロワセグメント処理タグ |')
lines.append('')
lines.append('### 上位30キーワード')
lines.append('')
memo = data['Lead']['field_keywords'].get('LeadSourceMemo__c', {})
lines.append('| 検索キーワード（「含む」で使用） | 件数 |')
lines.append('|-------------------------------|------|')
for val, count in sorted(memo.items(), key=lambda x: x[1], reverse=True)[:30]:
    if chr(0x2605) in val:  # ★
        val = val.split(chr(0x2605))[0].strip() + ' ★ ...(履歴)'
    display = val[:80].replace('|', '/').replace('\n', ' ')
    lines.append(f'| {display} | {count:,} |')
lines.append('')
lines.append('> **Tips**: 「含む」検索で `ハロワ_介護` `看護るー` のように部分一致で絞込可能')
lines.append('')

# ===== 3. Paid_DataSource =====
lines.append('## 3. 有料媒体データ取得元（Paid_DataSource）')
lines.append('')
lines.append('**対象**: リード / 取引先 | **用途**: どの有料媒体から取得したデータか')
lines.append('')
lines.append('| データ取得元 | Lead件数 | Account件数 | カテゴリ |')
lines.append('|------------|---------|------------|---------|')

lead_paid = data['Lead']['field_keywords'].get('Paid_DataSource__c', {})
acct_paid = data['Account']['field_keywords'].get('Paid_DataSource__c', {})
all_sources = set(list(lead_paid.keys()) + list(acct_paid.keys()))

cat_map = {
    '看護roo': '看護系', '看護るー': '看護系', 'ナース専科': '看護系',
    'レバウェル看護': '看護系', '看護のお仕事': '看護系',
    'きらケア': '介護系', 'タウンワーク': '総合求人',
    'タウンワーク_ Indeed PLUS': '総合求人', 'doda': '総合求人',
    'ミイダス': '転職/スカウト', 'ジョブポスター': '求人掲載',
    'PT・OT・STネット': 'リハビリ系',
    'Baseconnect_本社のみ': '企業DB', 'Baseconnect_spilt': '企業DB',
    'Baseconnect_沢山': '企業DB', 'HRog': 'HR系',
}

for src in sorted(all_sources, key=lambda x: lead_paid.get(x, 0), reverse=True):
    l_cnt = lead_paid.get(src, 0)
    a_cnt = acct_paid.get(src, 0)
    cat = cat_map.get(src, '')
    l_str = f'{l_cnt:,}' if l_cnt else '-'
    a_str = f'{a_cnt:,}' if a_cnt else '-'
    lines.append(f'| {src} | {l_str} | {a_str} | {cat} |')
lines.append('')

# ===== 4. Lead Status =====
lines.append('## 4. リード状況（Status）')
lines.append('')
lines.append('**対象**: リード')
lines.append('')
lead_status = data['Lead']['field_keywords'].get('Status', {})
lines.append('| ステータス | 件数 |')
lines.append('|----------|------|')
for val, count in sorted(lead_status.items(), key=lambda x: x[1], reverse=True):
    lines.append(f'| {val} | {count:,} |')
lines.append('')

# ===== 5. Account Status =====
lines.append('## 5. 取引先ステータス（Status）')
lines.append('')
lines.append('**対象**: 取引先')
lines.append('')
acct_status = data['Account']['field_keywords'].get('Status__c', {})
lines.append('| ステータス | 件数 |')
lines.append('|----------|------|')
for val, count in sorted(acct_status.items(), key=lambda x: x[1], reverse=True):
    lines.append(f'| {val} | {count:,} |')
lines.append('')

# ===== 6. AppointNGReason =====
lines.append('## 6. アポNG理由（AppointNGReason）')
lines.append('')
lines.append('**対象**: リード / 取引先')
lines.append('')
lines.append('| アポNG理由 | Lead件数 | Account件数 |')
lines.append('|-----------|---------|------------|')
lead_appt = data['Lead']['field_keywords'].get('AppointNGReason__c', {})
acct_appt = data['Account']['field_keywords'].get('AppointNGReason__c', {})
all_reasons = set(list(lead_appt.keys()) + list(acct_appt.keys()))
for r in sorted(all_reasons, key=lambda x: lead_appt.get(x, 0) + acct_appt.get(x, 0), reverse=True):
    l = lead_appt.get(r, 0)
    a = acct_appt.get(r, 0)
    l_str = f'{l:,}' if l else '-'
    a_str = f'{a:,}' if a else '-'
    lines.append(f'| {r} | {l_str} | {a_str} |')
lines.append('')

# ===== 7. Hellowork =====
lines.append('## 7. ハローワーク募集理由区分')
lines.append('')
lines.append('**対象**: リード / 取引先')
lines.append('')
lines.append('| 募集理由区分 | Lead件数 | Account件数 |')
lines.append('|------------|---------|------------|')
lead_hw = data['Lead']['field_keywords'].get('Hellowork_RecruitmentReasonCategory__c', {})
acct_hw = data['Account']['field_keywords'].get('Hellowork_RecruitmentReasonCategory__c', {})
all_hw = set(list(lead_hw.keys()) + list(acct_hw.keys()))
for r in sorted(all_hw, key=lambda x: lead_hw.get(x, 0), reverse=True):
    l = lead_hw.get(r, 0)
    a = acct_hw.get(r, 0)
    lines.append(f'| {r} | {l:,} | {a:,} |')
lines.append('')

# ===== 8. 架電対象外 =====
lines.append('## 8. 架電対象外の理由（Account）')
lines.append('')
lines.append('**対象**: 取引先')
lines.append('')
call_na = data['Account']['field_keywords'].get('CallNotApplicableReason__c', {})
lines.append('| 理由 | 件数 |')
lines.append('|------|------|')
for val, count in sorted(call_na.items(), key=lambda x: x[1], reverse=True)[:20]:
    lines.append(f'| {val} | {count:,} |')
if len(call_na) > 20:
    lines.append(f'| *(他{len(call_na)-20}種類)* | |')
lines.append('')

# ===== 9. 失注理由 =====
lines.append('## 9. 失注理由（Account）')
lines.append('')
lines.append('**対象**: 取引先')
lines.append('')
lost = data['Account']['field_keywords'].get('Lost_LostReason__c', {})
lines.append('| 失注理由 | 件数 |')
lines.append('|---------|------|')
for val, count in sorted(lost.items(), key=lambda x: x[1], reverse=True)[:20]:
    display = val[:80].replace('|', '/').replace('\n', ' ')
    if len(val) > 80:
        display += '...'
    lines.append(f'| {display} | {count:,} |')
if len(lost) > 20:
    lines.append(f'| *(他{len(lost)-20}種類)* | |')
lines.append('')

# ===== 10. 解約理由 =====
lines.append('## 10. 解約理由（Account）')
lines.append('')
lines.append('**対象**: 取引先')
lines.append('')
churn = data['Account']['field_keywords'].get('Churn_CancellationReason__c', {})
lines.append('| 解約理由 | 件数 |')
lines.append('|---------|------|')
for val, count in sorted(churn.items(), key=lambda x: x[1], reverse=True)[:20]:
    display = val[:80].replace('|', '/').replace('\n', ' ')
    if len(val) > 80:
        display += '...'
    lines.append(f'| {display} | {count:,} |')
if len(churn) > 20:
    lines.append(f'| *(他{len(churn)-20}種類)* | |')
lines.append('')

# ===== 11. 架電リスト割振メモ =====
lines.append('## 11. 架電リスト割振メモ（Account）')
lines.append('')
lines.append('**対象**: 取引先')
lines.append('')
clm = data['Account']['field_keywords'].get('CallListMemo__c', {})
if clm:
    lines.append('| キーワード | 件数 |')
    lines.append('|-----------|------|')
    for val, count in sorted(clm.items(), key=lambda x: x[1], reverse=True):
        lines.append(f'| {val} | {count:,} |')
else:
    lines.append('現在データなし')
lines.append('')

# ===== 12. 次回アクション =====
lines.append('## 12. 次回アクション内容')
lines.append('')
lines.append('**対象**: リード / 取引先 | **用途**: 架電結果の記録（ISメンバー入力）')
lines.append('')
lines.append('### Lead（上位20）')
lines.append('')
lead_next = data['Lead']['field_keywords'].get('NextAction__c', {})
lines.append('| アクション内容 | 件数 |')
lines.append('|--------------|------|')
for val, count in sorted(lead_next.items(), key=lambda x: x[1], reverse=True)[:20]:
    display = val[:60].replace('|', '/').replace('\n', ' ')
    lines.append(f'| {display} | {count:,} |')
lines.append('')

lines.append('### Account（上位20）')
lines.append('')
acct_next = data['Account']['field_keywords'].get('NextAction__c', {})
lines.append('| アクション内容 | 件数 |')
lines.append('|--------------|------|')
for val, count in sorted(acct_next.items(), key=lambda x: x[1], reverse=True)[:20]:
    display = val[:60].replace('|', '/').replace('\n', ' ')
    lines.append(f'| {display} | {count:,} |')
lines.append('')

lines.append('---')
lines.append('')
lines.append('以上')

report = '\n'.join(lines)
output_path = project_root / 'claudedocs' / 'keyword_guide_for_team.md'
with open(output_path, 'w', encoding='utf-8') as f:
    f.write(report)

print(f'生成完了: {output_path} ({len(lines)}行)')
