"""
keyword_dictionary JSONからclaudedocs向け辞書レポートを生成
"""
import sys
import json
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# 最新のJSONファイルを取得
json_dir = project_root / 'data' / 'output' / 'keyword_dictionary'
json_files = sorted(json_dir.glob('keyword_dictionary_*.json'))
if not json_files:
    print('JSONファイルが見つかりません')
    sys.exit(1)

json_path = json_files[-1]  # 最新ファイル
print(f'読み込み: {json_path.name}')

with open(json_path, 'r', encoding='utf-8') as f:
    data = json.load(f)

# 長文テキストフィールド（レポートフィルタには使わない）
skip_fields = [
    'Street', 'BillingStreet', 'ShippingStreet', 'PersonMailingStreet',
    'CallLog__c', 'CallLog__pc', 'CallLogHubSpot__pc',
    'Hearing_Character__c', 'Hearing_RecuritmentType_EmploymentStatus__c',
    'Hearing_RecruitingMethod_Detail__c', 'Hearing_Assignment__c',
    'Hearing_Infomation__c', 'Paid_JobTitle__c', 'Paid_Memo__c',
    'Publish_ImportText__c', 'Publish_BusinessHour__c',
    'Description', 'CompanyOverview__c',
    'Churn_CancellationReasonDetail__c',
    'EmailBouncedReason', 'PersonEmailBouncedReason', 'SicDesc',
    'ContactMethodRemarks__c', 'ContactMethodRemarks__pc',
]

lines = []
lines.append('# Salesforce メモ系フィールド キーワード辞書')
lines.append('')
lines.append('生成日時: 2026-02-08')
lines.append('データソース: Bulk API 2.0 によるLead 147,069件 / Account 287,715件 全件抽出')
lines.append('')
lines.append('---')
lines.append('')

for obj_name in ['Lead', 'Account']:
    obj = data[obj_name]
    lines.append(f'## {obj_name} オブジェクト')
    lines.append('')

    # フィールド一覧
    lines.append(f'### メモ系フィールド一覧 ({len(obj["fields"])}件)')
    lines.append('')
    lines.append('| API名 | ラベル | 型 | 長さ |')
    lines.append('|-------|--------|-----|------|')
    for f in obj['fields']:
        lines.append(f'| `{f["name"]}` | {f["label"]} | {f["type"]} | {f["length"]} |')
    lines.append('')

    # キーワード辞書
    lines.append('### レポート検索用キーワード辞書')
    lines.append('')

    for field_name, keywords in obj['field_keywords'].items():
        if field_name in skip_fields:
            continue

        field_info = next((fi for fi in obj['fields'] if fi['name'] == field_name), None)
        if not field_info:
            continue

        total = sum(keywords.values())
        lines.append(f'#### `{field_name}` ({field_info["label"]})')
        lines.append('')
        lines.append(f'種類数: {len(keywords)} | 該当レコード数: {total:,}')
        lines.append('')
        lines.append('| キーワード | 件数 | 用途/備考 |')
        lines.append('|-----------|------|---------|')

        sorted_kw = sorted(keywords.items(), key=lambda x: x[1], reverse=True)
        for val, count in sorted_kw[:50]:
            display = val[:120].replace('|', '/').replace('\n', ' ')
            if len(val) > 120:
                display += '...'

            # 用途推定
            purpose = ''
            if 'ハロワ' in val or 'ハローワーク' in val:
                purpose = 'ハローワーク'
            elif 'medica' in val.lower():
                purpose = 'medica関連'
            elif '看護' in val or 'ナース' in val:
                purpose = '看護媒体'
            elif 'きらケア' in val:
                purpose = 'きらケア'
            elif 'PT・OT' in val or 'PT・OT・ST' in val:
                purpose = 'PT/OT/ST媒体'
            elif 'ミイダス' in val:
                purpose = 'ミイダス'
            elif 'ジョブポスター' in val:
                purpose = 'ジョブポスター'
            elif 'タウンワーク' in val:
                purpose = 'タウンワーク'
            elif 'doda' in val.lower():
                purpose = 'doda'
            elif 'Baseconnect' in val:
                purpose = 'Baseconnect'
            elif 'クリニック' in val:
                purpose = 'クリニック'
            elif 'BATCH' in val:
                purpose = 'バッチ処理'
            elif '新規作成' in val or '新規' in val:
                purpose = '新規'
            elif '既存更新' in val:
                purpose = '既存更新'
            elif 'チョディカ' in val:
                purpose = 'チョディカ(メディカル)'
            elif 'レバウェル' in val:
                purpose = 'レバウェル看護'
            elif 'マイナビ' in val:
                purpose = 'マイナビ'
            elif 'リハプライド' in val:
                purpose = 'リハプライド'
            elif 'HubSpot' in val:
                purpose = 'HubSpot移管'
            elif 'オート入稿' in val or 'オート' in val:
                purpose = 'オート入稿'
            elif 'TW' in val:
                purpose = 'タウンワーク'

            lines.append(f'| {display} | {count:,} | {purpose} |')

        if len(sorted_kw) > 50:
            lines.append(f'| *(他{len(sorted_kw)-50}種類省略)* | | |')
        lines.append('')

    lines.append('---')
    lines.append('')

# サマリー
lines.append('## サマリー')
lines.append('')
lines.append('### 処理統計')
lines.append('')
lines.append('| オブジェクト | メモ系フィールド数 | キーワード種類数 |')
lines.append('|------------|-----------------|--------------|')
for obj_name in ['Lead', 'Account']:
    obj = data[obj_name]
    field_count = len(obj['fields'])
    kw_count = sum(len(v) for v in obj['field_keywords'].values())
    lines.append(f'| {obj_name} | {field_count} | {kw_count:,} |')
lines.append('')

lines.append('### 主要フィルタフィールド（レポート検索でよく使うもの）')
lines.append('')
lines.append('| フィールド | オブジェクト | 用途 |')
lines.append('|-----------|------------|------|')
lines.append('| `LeadSource` | Lead | リードソース（ハロワ/媒体の大分類） |')
lines.append('| `LeadSourceMemo__c` | Lead | リードソースメモ（バッチ別の詳細識別） |')
lines.append('| `Status` | Lead | リード状況（架電結果） |')
lines.append('| `Status__c` | Account | 取引先ステータス |')
lines.append('| `Paid_DataSource__c` | Lead/Account | 有料媒体のデータ取得元 |')
lines.append('| `AppointNGReason__c` | Lead/Account | アポNG理由 |')
lines.append('| `Hellowork_RecruitmentReasonCategory__c` | Lead/Account | ハローワーク募集理由区分 |')
lines.append('| `CallListMemo__c` | Account | 架電リスト割り振りメモ |')
lines.append('| `CallNotApplicableReason__c` | Account | 架電対象外の理由 |')
lines.append('| `Lost_LostReason__c` | Account | 失注理由 |')
lines.append('| `Churn_CancellationReason__c` | Account | 解約理由 |')
lines.append('')

lines.append('### 出力ファイル一覧')
lines.append('')
lines.append('| パス | 内容 |')
lines.append('|------|------|')
lines.append(f'| `{json_path.relative_to(project_root)}` | JSON形式辞書（プログラム利用用） |')
lines.append('| `data/output/keyword_dictionary/*_values_*.csv` | フィールド別キーワードCSV |')
lines.append('| `data/output/keyword_dictionary/*_memo_fields_*.csv` | メモ系フィールド一覧CSV |')

report = '\n'.join(lines)
output_path = project_root / 'claudedocs' / 'keyword_dictionary_report.md'
with open(output_path, 'w', encoding='utf-8') as f:
    f.write(report)

print(f'レポート生成完了: {output_path}')
