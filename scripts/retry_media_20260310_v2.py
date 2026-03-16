"""
有料媒体インポート リトライ v2 2026-03-10
- 新規リード18件: Prefecture__c無効値クリア + フィールド切り詰め
- Lead更新残り: 全角数字変換 + LastName除外
- Accountレポート作成（カラム名調査含む）
"""
import pandas as pd
import sys
import io
import re
import time
import requests
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))
sys.stdout.reconfigure(encoding='utf-8')

from api.salesforce_client import SalesforceClient

BASE_DIR = Path(__file__).parent.parent
OUTPUT_DIR = BASE_DIR / 'data/output/media_matching'
PROCESS_DATE = '2026-03-10'
CREATED_IDS_FILE = OUTPUT_DIR / 'created_lead_ids_20260310.csv'

STRING_FIELDS_255 = [
    'LeadSourceMemo__c', 'Paid_EmploymentType__c', 'Paid_Industry__c',
    'Paid_RecruitmentType__c', 'Paid_JobTitle__c', 'Website'
]


def zen_to_han(text):
    if not isinstance(text, str):
        return text
    # 全角数字→半角
    text = re.sub(r'[０-９]', lambda m: chr(ord(m.group()) - 0xFEE0), text)
    # 全角カンマ→半角
    text = text.replace('，', ',').replace('、', ',')
    return text


def truncate_fields(df):
    for col in STRING_FIELDS_255:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda x: (x[:252] + '...') if isinstance(x, str) and len(x) > 255 else x
            )
    return df


def clean_numeric_fields(df):
    """数値フィールドの全角文字を半角に変換し、数値以外を除去"""
    if 'Paid_NumberOfRecruitment__c' in df.columns:
        def clean_number(val):
            if not isinstance(val, str) or pd.isna(val) or val.strip() == '':
                return val
            val = zen_to_han(val)
            # 数字とドットのみ抽出
            nums = re.findall(r'[\d.]+', val)
            if nums:
                return nums[0]
            return ''
        df['Paid_NumberOfRecruitment__c'] = df['Paid_NumberOfRecruitment__c'].apply(clean_number)
    return df


def bulk_op(client, object_name, operation, df, label=''):
    headers = client._get_headers()
    api_version = 'v59.0'
    job_url = f'{client.instance_url}/services/data/{api_version}/jobs/ingest'

    resp = requests.post(job_url, headers=headers, json={
        'object': object_name, 'operation': operation,
        'contentType': 'CSV', 'lineEnding': 'CRLF',
    })
    resp.raise_for_status()
    job_id = resp.json()['id']
    print(f'  ジョブ: {job_id}')

    csv_buf = io.StringIO()
    df.to_csv(csv_buf, index=False, lineterminator='\r\n')
    upload_headers = {**headers, 'Content-Type': 'text/csv'}
    resp = requests.put(f'{job_url}/{job_id}/batches', headers=upload_headers,
                        data=csv_buf.getvalue().encode('utf-8'))
    resp.raise_for_status()

    resp = requests.patch(f'{job_url}/{job_id}', headers=headers, json={'state': 'UploadComplete'})
    resp.raise_for_status()

    for _ in range(60):
        time.sleep(5)
        resp = requests.get(f'{job_url}/{job_id}', headers=headers)
        resp.raise_for_status()
        state = resp.json()['state']
        proc = resp.json().get('numberRecordsProcessed', 0)
        fail = resp.json().get('numberRecordsFailed', 0)
        print(f'  {label}: {state} (処理: {proc}, 失敗: {fail})')
        if state in ('JobComplete', 'Failed', 'Aborted'):
            break

    result = resp.json()
    success_ids = []
    r = requests.get(f'{job_url}/{job_id}/successfulResults', headers=headers)
    if r.text.strip() and len(r.text.strip().split('\n')) > 1:
        success_ids = pd.read_csv(io.StringIO(r.text), dtype=str)['sf__Id'].tolist()

    failed_records = []
    r = requests.get(f'{job_url}/{job_id}/failedResults', headers=headers)
    if r.text.strip() and len(r.text.strip().split('\n')) > 1:
        failed_records = pd.read_csv(io.StringIO(r.text), dtype=str).to_dict('records')

    return {
        'success_ids': success_ids,
        'failed': result.get('numberRecordsFailed', 0),
        'failed_records': failed_records,
        'state': result['state'],
    }


def get_valid_prefectures(client):
    """Describe APIからPrefecture__cの有効値リスト取得"""
    headers = client._get_headers()
    url = f'{client.instance_url}/services/data/v59.0/sobjects/Lead/describe'
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    for field in resp.json()['fields']:
        if field['name'] == 'Prefecture__c':
            return {pv['value'] for pv in field['picklistValues'] if pv['active']}
    return set()


def main():
    print('=' * 70)
    print(f'有料媒体 リトライ v2 {PROCESS_DATE}')
    print('=' * 70)

    client = SalesforceClient()
    client.authenticate()
    print('SF認証成功')

    # Prefecture有効値取得
    valid_prefs = get_valid_prefectures(client)
    print(f'有効な都道府県: {len(valid_prefs)}件')

    # === Step 1: 新規リード18件 ===
    print('\n--- Step 1: 新規リード リトライ ---')
    df_new = pd.read_csv(OUTPUT_DIR / 'failed_new_leads_20260310.csv', encoding='utf-8-sig', dtype=str)
    df_new = df_new.drop(columns=[c for c in ['sf__Id', 'sf__Error'] if c in df_new.columns])

    # Prefecture__c無効値クリア
    if 'Prefecture__c' in df_new.columns:
        invalid_mask = ~df_new['Prefecture__c'].isin(valid_prefs) & df_new['Prefecture__c'].notna()
        invalid_count = invalid_mask.sum()
        if invalid_count > 0:
            print(f'  無効な都道府県: {invalid_count}件 → クリア')
            for idx in df_new[invalid_mask].index:
                print(f'    "{df_new.loc[idx, "Prefecture__c"]}" → 空')
            df_new.loc[invalid_mask, 'Prefecture__c'] = ''

    df_new = truncate_fields(df_new)
    df_new = clean_numeric_fields(df_new)

    print(f'  対象: {len(df_new)}件')
    r1 = bulk_op(client, 'Lead', 'insert', df_new, '新規リード')
    print(f'  成功: {len(r1["success_ids"])}件, 失敗: {r1["failed"]}件')

    if r1['success_ids']:
        existing = pd.read_csv(CREATED_IDS_FILE, dtype=str)
        combined = pd.concat([existing, pd.DataFrame({'Id': r1['success_ids']})], ignore_index=True)
        combined.to_csv(CREATED_IDS_FILE, index=False, encoding='utf-8-sig')
        print(f'  IDファイル更新: {len(combined)}件')

    if r1['failed_records']:
        print(f'  まだ失敗:')
        for rec in r1['failed_records'][:5]:
            err = rec.get('sf__Error', 'unknown')
            print(f'    {err[:120]}')

    # === Step 2: Lead更新 残り(全角数字問題) ===
    print('\n--- Step 2: Lead更新 残り分 リトライ ---')
    # 前回のv1リトライで失敗した分を直接対処
    # 14件(LastNameあり) + 12件(全角数字) = まだ約26件
    # 元のfailed_lead_updatesから、コンバート済み・LastNameエラー以外で再処理
    df_lu = pd.read_csv(OUTPUT_DIR / 'failed_lead_updates_20260310.csv', encoding='utf-8-sig', dtype=str)

    # コンバート済み除外
    df_lu = df_lu[~df_lu['sf__Error'].str.contains('CANNOT_UPDATE_CONVERTED', na=False)]
    # v1で成功した21件(LastNameバリデーション)も含まれるが、冪等なので再実行OK

    df_lu = df_lu.drop(columns=[c for c in ['sf__Id', 'sf__Error'] if c in df_lu.columns])

    # LastName除外（バリデーション回避）
    if 'LastName' in df_lu.columns:
        df_lu = df_lu.drop(columns=['LastName'])

    df_lu = truncate_fields(df_lu)
    df_lu = clean_numeric_fields(df_lu)

    print(f'  対象: {len(df_lu)}件（コンバート済み除外後、LastName除外）')
    r2 = bulk_op(client, 'Lead', 'update', df_lu, 'Lead更新')
    print(f'  成功: {len(r2["success_ids"])}件, 失敗: {r2["failed"]}件')

    if r2['failed_records']:
        print(f'  まだ失敗:')
        for rec in r2['failed_records'][:5]:
            err = rec.get('sf__Error', 'unknown')
            print(f'    {err[:120]}')

    # === Step 3: Accountレポート ===
    print('\n--- Step 3: Accountレポート作成 ---')

    # まずAccountListのレポートタイプメタデータを確認
    headers = client._get_headers()
    try:
        rt_url = f'{client.instance_url}/services/data/v59.0/analytics/reportTypes/AccountList'
        rt_resp = requests.get(rt_url, headers=headers)
        rt_resp.raise_for_status()
        rt_data = rt_resp.json()

        # カラム名を探す
        print('  AccountListカラム:')
        for cat in rt_data.get('reportTypeColumnCategories', []):
            for col in cat.get('columns', {}).values():
                name = col.get('name', '')
                label = col.get('label', '')
                if any(kw in name.lower() or kw in label for kw in
                       ['name', 'phone', 'paid', 'owner', 'user']):
                    print(f'    {name}: {label}')
    except Exception as e:
        print(f'  メタデータ取得エラー: {e}')

    # レポート作成試行
    try:
        report_configs = [
            # 試行1: Paid_DataExportDateフィルタ
            {
                'columns': ['ACCOUNT_NAME', 'PHONE1', 'USERS.NAME'],
                'filters': [
                    {'column': 'Account.Paid_DataExportDate__c', 'operator': 'equals', 'value': PROCESS_DATE}
                ]
            },
            # 試行2: フィルタなし（直近更新）
            {
                'columns': ['ACCOUNT_NAME', 'PHONE1'],
                'filters': []
            },
        ]

        for i, cfg in enumerate(report_configs):
            try:
                url = f'{client.instance_url}/services/data/v59.0/analytics/reports'
                body = {
                    'reportMetadata': {
                        'name': f'有料媒体_既存更新Account_{PROCESS_DATE}',
                        'reportFormat': 'TABULAR',
                        'reportType': {'type': 'AccountList'},
                        'detailColumns': cfg['columns'],
                    }
                }
                if cfg['filters']:
                    body['reportMetadata']['reportFilters'] = cfg['filters']

                resp = requests.post(url, headers=headers, json=body)
                if resp.status_code == 200:
                    r3_id = resp.json()['reportMetadata']['id']
                    r3_url = f'{client.instance_url}/lightning/r/Report/{r3_id}/view'
                    print(f'  Account更新レポート: {r3_url}')
                    break
                else:
                    print(f'  試行{i+1}失敗: {resp.status_code} {resp.text[:200]}')
            except Exception as e:
                print(f'  試行{i+1}エラー: {e}')
    except Exception as e:
        print(f'  レポート作成全体エラー: {e}')

    print('\n' + '=' * 70)
    print('リトライ v2 完了')
    print('=' * 70)


if __name__ == '__main__':
    main()
