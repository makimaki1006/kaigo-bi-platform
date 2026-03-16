"""
既存リード3,846件のPublish_ImportText__cメモ再更新
==================================================
前回の不完全メモ（近接スコア・インジケーターなし）を
新規リードと同じフォーマットで再生成して更新する
"""
import pandas as pd
import numpy as np
import re
import sys
import time
import requests
from io import StringIO
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'src'))
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.stdout.reconfigure(encoding='utf-8')

from api.salesforce_client import SalesforceClient
from pipeline_hellowork_AB import (
    SEGMENTS, normalize_phone, parse_emp, normalize_name, get_surname,
    DECISION_MAKER_TITLES, MANAGER_WALL_KEYWORDS,
)

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / 'data' / 'output' / 'hellowork_segments'
TODAY_ISO = datetime.now().strftime('%Y-%m-%d')

# =====================================================================
# ファイル読み込み
# =====================================================================
print('=' * 60)
print('既存リード メモ再更新（近接スコア・インジケーター追加）')
print('=' * 60)

target_file = DATA_DIR / 'matched' / 'lead_update_target_20260316.csv'
existing_file = DATA_DIR / 'matched' / 'lead_existing_fields_20260316.csv'

print(f'\n読み込み: {target_file.name}')
hw = pd.read_csv(target_file, encoding='utf-8-sig', dtype=str, low_memory=False)
print(f'  HWレコード数: {len(hw):,}')

print(f'読み込み: {existing_file.name}')
sf = pd.read_csv(existing_file, encoding='utf-8-sig', dtype=str, low_memory=False)
print(f'  SF既存レコード数: {len(sf):,}')

# LeadIdで結合
hw['LeadId'] = hw['LeadId'].astype(str).str.strip()
sf['Id'] = sf['Id'].astype(str).str.strip()

# =====================================================================
# セグメント分類
# =====================================================================
def classify_segment(row):
    ind = str(row.get('産業分類（名称）', ''))
    if ind in SEGMENTS['A_医療看護保健']['industry_names_whitelist']:
        return 'A_医療看護保健'
    if ind in SEGMENTS['B_介護福祉']['industry_names_whitelist']:
        return 'B_介護福祉'
    return 'AB_その他'

hw['_segment'] = hw.apply(classify_segment, axis=1)
seg_dist = hw['_segment'].value_counts()
print(f'\nセグメント分布:')
for seg, cnt in seg_dist.items():
    print(f'  {seg}: {cnt:,}')

# =====================================================================
# 近接スコア・インジケーター計算
# =====================================================================
print('\n近接スコア計算中...')

def calc_indicators(row):
    rep_name = normalize_name(str(row.get('代表者名', '')))
    contact_name = normalize_name(str(row.get('選考担当者氏名漢字', '')))
    contact_title = str(row.get('選考担当者課係名／役職名', ''))
    company = str(row.get('事業所名漢字', ''))
    phone = str(row.get('電話_正規化', ''))
    emp = parse_emp(row.get('従業員数企業全体（コード）', '0'))

    is_direct = (rep_name != '' and contact_name != '' and rep_name == contact_name)
    is_mobile = bool(re.match(r'^0[789]0', phone))
    rep_surname = get_surname(str(row.get('代表者名', '')))
    con_surname = get_surname(str(row.get('選考担当者氏名漢字', '')))
    is_family = (rep_surname != '' and con_surname != '' and
                 rep_surname == con_surname and rep_name != contact_name)
    is_dm_title = any(kw in contact_title for kw in DECISION_MAKER_TITLES)
    is_wall = any(kw in contact_title for kw in MANAGER_WALL_KEYWORDS)
    is_small = emp <= 10
    is_family_biz = (rep_surname != '' and rep_surname in company)

    return pd.Series({
        '代表者直通': '○' if is_direct else '',
        '携帯判定': '携帯' if is_mobile else '固定',
        '同姓親族': '○' if is_family else '',
        '決裁者役職': '○' if is_dm_title else '',
        '管理部門壁': '○' if is_wall else '',
        '家族経営名': '○' if is_family_biz else '',
    })

indicators = hw.apply(calc_indicators, axis=1)
for col in indicators.columns:
    hw[col] = indicators[col]

def calc_score(row):
    is_direct = row['代表者直通'] == '○'
    is_mobile = row['携帯判定'] == '携帯'
    is_family = row['同姓親族'] == '○'
    is_dm_title = row['決裁者役職'] == '○'
    is_wall = row['管理部門壁'] == '○'
    emp = parse_emp(row.get('従業員数企業全体（コード）', '0'))
    is_small = emp <= 10
    is_family_biz = row['家族経営名'] == '○'

    if is_direct and is_mobile:
        return 5
    if is_direct:
        return 4
    if is_family and is_small:
        return 4
    if is_dm_title and is_mobile:
        return 4
    if is_family:
        return 3
    if is_dm_title:
        return 3
    if is_family_biz and is_small:
        return 3
    if is_wall:
        return 1
    return 2

hw['近接スコア'] = hw.apply(calc_score, axis=1)
stars_map = {5: '★★★★★', 4: '★★★★☆', 3: '★★★☆☆', 2: '★★☆☆☆', 1: '★☆☆☆☆'}
hw['近接スコア_星'] = hw['近接スコア'].map(stars_map)

dist = hw['近接スコア'].value_counts().sort_index()
print('近接スコア分布:')
for score, count in dist.items():
    print(f'  {stars_map[score]} ({score}): {count:,}件')

# 代表者直通件数
direct_count = (hw['代表者直通'] == '○').sum()
print(f'\n代表者直通: {direct_count:,}件')

# =====================================================================
# メモ生成（新規リードと同一フォーマット）
# =====================================================================
print('\nメモ生成中...')

update_records = []
for _, row in hw.iterrows():
    lead_id = str(row.get('LeadId', '')).strip()
    if not lead_id or lead_id == 'nan':
        continue

    seg = str(row.get('_segment', 'AB'))
    ind = str(row.get('産業分類（名称）', '')).replace('nan', '')
    job = str(row.get('職種', '')).replace('nan', '')
    emp_type = str(row.get('雇用形態', '')).replace('nan', '')
    emp_count = parse_emp(row.get('従業員数企業全体（コード）', '0'))
    stars_str = str(row.get('近接スコア_星', '★★☆☆☆'))

    # インジケーター行を構築
    indicator_parts = []
    if row.get('代表者直通') == '○':
        indicator_parts.append('代表者直通: ○')
    if row.get('携帯判定') == '携帯':
        indicator_parts.append('携帯: ○')
    if row.get('同姓親族') == '○':
        indicator_parts.append('同姓親族: ○')
    if row.get('決裁者役職') == '○':
        indicator_parts.append('決裁者役職: ○')
    if row.get('管理部門壁') == '○':
        indicator_parts.append('管理部門壁: ○')
    if row.get('家族経営名') == '○':
        indicator_parts.append('家族経営名: ○')
    indicators_line = '\n' + ', '.join(indicator_parts) if indicator_parts else ''

    recruit_reason = str(row.get('募集理由区分', '')).replace('nan', '')
    recruit_num = str(row.get('採用人数', '')).replace('nan', '')
    recruit_reason_line = f'\n募集理由: {recruit_reason}' if recruit_reason else ''
    recruit_num_line = f'\n採用人数: {recruit_num}' if recruit_num else ''

    other_jobs = str(row.get('_other_jobs', '')).replace('nan', '')
    other_jobs_line = f'\n同事業所の他募集: {other_jobs}' if other_jobs else ''

    new_memo = (
        f'[{TODAY_ISO} ハロワ突合_{seg}]\n'
        f'セグメント: {seg}\n'
        f'業界: {ind}\n'
        f'職種: {job}\n'
        f'雇用形態: {emp_type}\n'
        f'従業員数: {emp_count}\n'
        f'近接スコア: {stars_str}'
        f'{indicators_line}'
        f'{recruit_reason_line}'
        f'{recruit_num_line}'
        f'{other_jobs_line}'
    )

    # 既存のPublish_ImportText__cを取得
    sf_row = sf[sf['Id'] == lead_id]
    existing_memo = ''
    if len(sf_row) > 0:
        existing_memo = str(sf_row.iloc[0].get('Publish_ImportText__c', ''))
        if existing_memo == 'nan':
            existing_memo = ''

    # 前回の不完全メモ [2026-03-16 ハロワ突合更新] を除去
    if existing_memo:
        # 前回の不完全メモブロックを削除（[2026-03-16 ハロワ突合更新] で始まるブロック）
        pattern = r'\[2026-03-16 ハロワ突合更新\].*?(?=\n\[|\Z)'
        existing_memo = re.sub(pattern, '', existing_memo, flags=re.DOTALL).strip()

    # 新しいメモを先頭に配置
    if existing_memo:
        combined = new_memo + '\n\n' + existing_memo
    else:
        combined = new_memo

    # 10,000文字制限
    if len(combined) > 10000:
        combined = combined[:9997] + '...'

    update_records.append({
        'Id': lead_id,
        'Publish_ImportText__c': combined,
    })

print(f'更新対象: {len(update_records):,}件')

# サンプル表示
if update_records:
    print(f'\n--- サンプル（1件目）---')
    sample = update_records[0]
    memo_lines = sample['Publish_ImportText__c'].split('\n')
    for line in memo_lines[:15]:
        print(f'  {line}')
    if len(memo_lines) > 15:
        print(f'  ... (以下省略、計{len(memo_lines)}行)')

# 代表者直通メモ含有チェック
direct_in_memo = sum(1 for r in update_records if '代表者直通: ○' in r['Publish_ImportText__c'])
print(f'\nメモに「代表者直通: ○」含有: {direct_in_memo:,}件')

# =====================================================================
# Bulk API 2.0 アップロード
# =====================================================================
print('\n' + '=' * 60)
print('Salesforce Bulk API 2.0 更新')
print('=' * 60)

update_df = pd.DataFrame(update_records)
csv_str = update_df.to_csv(index=False, encoding='utf-8')

client = SalesforceClient()
client.authenticate()
headers = client._get_headers()
instance_url = client.instance_url
api_version = 'v59.0'
base_url = f'{instance_url}/services/data/{api_version}'

# バッチ分割（5,000件ずつ）
BATCH_SIZE = 5000
total = len(update_df)
batch_count = (total + BATCH_SIZE - 1) // BATCH_SIZE

all_success = 0
all_failed = 0
failed_records = []

for batch_idx in range(batch_count):
    start = batch_idx * BATCH_SIZE
    end = min(start + BATCH_SIZE, total)
    batch_df = update_df.iloc[start:end]
    batch_csv = batch_df.to_csv(index=False, encoding='utf-8')

    print(f'\nバッチ {batch_idx+1}/{batch_count}: {len(batch_df):,}件')

    # ジョブ作成
    job_payload = {
        'object': 'Lead',
        'operation': 'update',
        'contentType': 'CSV',
        'lineEnding': 'CRLF',
    }
    resp = requests.post(f'{base_url}/jobs/ingest', headers=headers, json=job_payload)
    if resp.status_code != 200:
        print(f'  ジョブ作成失敗: {resp.status_code} {resp.text[:200]}')
        continue
    job_id = resp.json()['id']

    # CSV アップロード
    upload_headers = {**headers, 'Content-Type': 'text/csv; charset=UTF-8'}
    resp = requests.put(
        f'{base_url}/jobs/ingest/{job_id}/batches',
        headers=upload_headers,
        data=batch_csv.encode('utf-8')
    )
    if resp.status_code != 201:
        print(f'  アップロード失敗: {resp.status_code} {resp.text[:200]}')
        continue

    # ジョブ開始
    resp = requests.patch(
        f'{base_url}/jobs/ingest/{job_id}',
        headers=headers,
        json={'state': 'UploadComplete'}
    )

    # 完了待ち
    print(f'  ジョブID: {job_id}')
    for i in range(120):
        time.sleep(5)
        resp = requests.get(f'{base_url}/jobs/ingest/{job_id}', headers=headers)
        job_info = resp.json()
        state = job_info.get('state', '')
        processed = job_info.get('numberRecordsProcessed', 0)
        failed = job_info.get('numberRecordsFailed', 0)
        if i % 6 == 0:
            print(f'  ポーリング {i}: state={state} processed={processed} failed={failed}')
        if state in ('JobComplete', 'Failed', 'Aborted'):
            break

    processed = job_info.get('numberRecordsProcessed', 0)
    failed = job_info.get('numberRecordsFailed', 0)
    success = processed - failed

    print(f'  成功: {success:,} / 失敗: {failed:,}')
    all_success += success
    all_failed += failed

    # 失敗レコード取得
    if failed > 0:
        resp = requests.get(
            f'{base_url}/jobs/ingest/{job_id}/failedResults',
            headers={**headers, 'Accept': 'text/csv'}
        )
        if resp.status_code == 200 and resp.text.strip():
            fail_df = pd.read_csv(StringIO(resp.text))
            for _, frow in fail_df.head(5).iterrows():
                print(f'    エラー: {frow.get("sf__Error", "")}')
            failed_records.extend(fail_df.to_dict('records'))

print(f'\n=== 更新結果 ===')
print(f'成功: {all_success:,}')
print(f'失敗: {all_failed:,}')

# 失敗レコード保存
if failed_records:
    fail_path = DATA_DIR / 'import_ready' / 'failed_reupdate_20260316.csv'
    pd.DataFrame(failed_records).to_csv(fail_path, index=False, encoding='utf-8-sig')
    print(f'失敗レコード: {fail_path}')

# =====================================================================
# 検証: メモ内「代表者直通: ○」含有数（ローカルデータで確認）
# =====================================================================
print('\n' + '=' * 60)
print('検証: 代表者直通含有数')
print('=' * 60)

# ローカルデータから検証（SOQLではlong text areaはLIKE不可）
recruit_num_col = '採用人数（コード）'
direct_and_recruit2 = hw[
    (hw['代表者直通'] == '○') &
    (hw[recruit_num_col].apply(lambda x: parse_emp(x) >= 2))
]
print(f'代表者直通 × 募集2名以上: {len(direct_and_recruit2):,}件（ローカルデータ）')
print('※レポート③の表示はSFレポートフィルタ（従業員11-150）適用後の件数')

print('\n完了')
