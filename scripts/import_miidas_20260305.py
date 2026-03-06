# -*- coding: utf-8 -*-
"""ミイダス保育園リスト インポート・更新スクリプト 2026-03-05"""
import pandas as pd
import sys
import os
import time
import json
import requests

project_root = os.path.join(os.path.dirname(__file__), '..')
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, 'src'))
sys.stdout.reconfigure(encoding='utf-8')

from src.api.salesforce_client import SalesforceClient


def import_new_leads(client):
    """新規リード作成 (66件)"""
    print('\n' + '=' * 60)
    print('【STEP 1】新規リード作成')
    print('=' * 60)

    df = pd.read_csv('data/output/media_matching/miidas_new_leads_20260305_final.csv',
                     dtype=str, encoding='utf-8-sig')
    # nan文字列をクリーンアップ
    df = df.fillna('')
    df = df.replace('nan', '')

    print(f'対象件数: {len(df)}件')

    # Bulk API 2.0 Insert
    instance_url = client.instance_url
    headers = client._get_headers()
    api_version = 'v59.0'

    # ジョブ作成
    job_url = f'{instance_url}/services/data/{api_version}/jobs/ingest'
    job_payload = {
        'object': 'Lead',
        'operation': 'insert',
        'contentType': 'CSV',
        'lineEnding': 'CRLF'
    }
    resp = requests.post(job_url, headers=headers, json=job_payload)
    resp.raise_for_status()
    job_id = resp.json()['id']
    print(f'ジョブ作成: {job_id}')

    # CSVデータアップロード
    csv_data = df.to_csv(index=False, encoding='utf-8')
    upload_url = f'{job_url}/{job_id}/batches'
    upload_headers = {**headers, 'Content-Type': 'text/csv'}
    resp = requests.put(upload_url, headers=upload_headers, data=csv_data.encode('utf-8'))
    resp.raise_for_status()

    # ジョブ完了通知
    close_url = f'{job_url}/{job_id}'
    resp = requests.patch(close_url, headers=headers, json={'state': 'UploadComplete'})
    resp.raise_for_status()

    # 完了待ち
    for i in range(30):
        time.sleep(3)
        resp = requests.get(close_url, headers=headers)
        state = resp.json().get('state')
        if state in ('JobComplete', 'Failed', 'Aborted'):
            break
        print(f'  ... {state}')

    result = resp.json()
    print(f'状態: {result["state"]}')
    print(f'成功: {result.get("numberRecordsProcessed", 0)}件')
    print(f'失敗: {result.get("numberRecordsFailed", 0)}件')

    # 失敗レコード取得
    if int(result.get('numberRecordsFailed', 0)) > 0:
        fail_url = f'{job_url}/{job_id}/failedResults'
        resp = requests.get(fail_url, headers={**headers, 'Accept': 'text/csv'})
        fail_path = 'data/output/media_matching/miidas_new_leads_failed_20260305.csv'
        with open(fail_path, 'w', encoding='utf-8-sig') as f:
            f.write(resp.text)
        print(f'失敗レコード: {fail_path}')

    # 成功レコードID取得
    success_url = f'{job_url}/{job_id}/successfulResults'
    resp = requests.get(success_url, headers={**headers, 'Accept': 'text/csv'})
    success_path = 'data/output/media_matching/miidas_new_leads_success_20260305.csv'
    with open(success_path, 'w', encoding='utf-8-sig') as f:
        f.write(resp.text)
    print(f'成功レコード: {success_path}')

    return result


def update_leads(client):
    """既存Lead更新 (85件)"""
    print('\n' + '=' * 60)
    print('【STEP 2】既存Lead更新')
    print('=' * 60)

    df = pd.read_csv('data/output/media_matching/miidas_lead_updates_20260305_final.csv',
                     dtype=str, encoding='utf-8-sig')
    df = df.fillna('')
    df = df.replace('nan', '')

    print(f'対象件数: {len(df)}件')

    instance_url = client.instance_url
    headers = client._get_headers()
    api_version = 'v59.0'

    job_url = f'{instance_url}/services/data/{api_version}/jobs/ingest'
    job_payload = {
        'object': 'Lead',
        'operation': 'update',
        'contentType': 'CSV',
        'lineEnding': 'CRLF'
    }
    resp = requests.post(job_url, headers=headers, json=job_payload)
    resp.raise_for_status()
    job_id = resp.json()['id']
    print(f'ジョブ作成: {job_id}')

    csv_data = df.to_csv(index=False, encoding='utf-8')
    upload_url = f'{job_url}/{job_id}/batches'
    upload_headers = {**headers, 'Content-Type': 'text/csv'}
    resp = requests.put(upload_url, headers=upload_headers, data=csv_data.encode('utf-8'))
    resp.raise_for_status()

    close_url = f'{job_url}/{job_id}'
    resp = requests.patch(close_url, headers=headers, json={'state': 'UploadComplete'})
    resp.raise_for_status()

    for i in range(30):
        time.sleep(3)
        resp = requests.get(close_url, headers=headers)
        state = resp.json().get('state')
        if state in ('JobComplete', 'Failed', 'Aborted'):
            break
        print(f'  ... {state}')

    result = resp.json()
    print(f'状態: {result["state"]}')
    print(f'成功: {result.get("numberRecordsProcessed", 0)}件')
    print(f'失敗: {result.get("numberRecordsFailed", 0)}件')

    if int(result.get('numberRecordsFailed', 0)) > 0:
        fail_url = f'{job_url}/{job_id}/failedResults'
        resp = requests.get(fail_url, headers={**headers, 'Accept': 'text/csv'})
        fail_path = 'data/output/media_matching/miidas_lead_updates_failed_20260305.csv'
        with open(fail_path, 'w', encoding='utf-8-sig') as f:
            f.write(resp.text)
        print(f'失敗レコード: {fail_path}')

    return result


def update_accounts(client):
    """既存Account更新 (39件)"""
    print('\n' + '=' * 60)
    print('【STEP 3】既存Account更新')
    print('=' * 60)

    df = pd.read_csv('data/output/media_matching/miidas_account_updates_20260305_164456.csv',
                     dtype=str, encoding='utf-8-sig')
    df = df.fillna('')
    df = df.replace('nan', '')

    print(f'対象件数: {len(df)}件')

    instance_url = client.instance_url
    headers = client._get_headers()
    api_version = 'v59.0'

    job_url = f'{instance_url}/services/data/{api_version}/jobs/ingest'
    job_payload = {
        'object': 'Account',
        'operation': 'update',
        'contentType': 'CSV',
        'lineEnding': 'CRLF'
    }
    resp = requests.post(job_url, headers=headers, json=job_payload)
    resp.raise_for_status()
    job_id = resp.json()['id']
    print(f'ジョブ作成: {job_id}')

    csv_data = df.to_csv(index=False, encoding='utf-8')
    upload_url = f'{job_url}/{job_id}/batches'
    upload_headers = {**headers, 'Content-Type': 'text/csv'}
    resp = requests.put(upload_url, headers=upload_headers, data=csv_data.encode('utf-8'))
    resp.raise_for_status()

    close_url = f'{job_url}/{job_id}'
    resp = requests.patch(close_url, headers=headers, json={'state': 'UploadComplete'})
    resp.raise_for_status()

    for i in range(30):
        time.sleep(3)
        resp = requests.get(close_url, headers=headers)
        state = resp.json().get('state')
        if state in ('JobComplete', 'Failed', 'Aborted'):
            break
        print(f'  ... {state}')

    result = resp.json()
    print(f'状態: {result["state"]}')
    print(f'成功: {result.get("numberRecordsProcessed", 0)}件')
    print(f'失敗: {result.get("numberRecordsFailed", 0)}件')

    if int(result.get('numberRecordsFailed', 0)) > 0:
        fail_url = f'{job_url}/{job_id}/failedResults'
        resp = requests.get(fail_url, headers={**headers, 'Accept': 'text/csv'})
        fail_path = 'data/output/media_matching/miidas_account_updates_failed_20260305.csv'
        with open(fail_path, 'w', encoding='utf-8-sig') as f:
            f.write(resp.text)
        print(f'失敗レコード: {fail_path}')

    return result


def create_reports(client):
    """レポート作成"""
    print('\n' + '=' * 60)
    print('【STEP 4】レポート作成')
    print('=' * 60)

    instance_url = client.instance_url
    headers = client._get_headers()
    api_version = 'v59.0'
    report_url = f'{instance_url}/services/data/{api_version}/analytics/reports'

    reports = []

    # レポート1: 新規作成リード
    meta1 = {
        'reportMetadata': {
            'name': 'ミイダス保育園_新規作成リード_20260305',
            'reportFormat': 'TABULAR',
            'reportType': {'type': 'LeadList'},
            'detailColumns': [
                'OWNER', 'COMPANY', 'LAST_NAME', 'PHONE', 'MOBILE_PHONE',
                'STREET', 'Lead.Prefecture__c', 'Lead.PresidentName__c',
                'Lead.Paid_Media__c', 'Lead.Paid_JobTitle__c',
                'Lead.Paid_URL__c', 'CREATED_DATE'
            ],
            'reportFilters': [
                {
                    'column': 'Lead.LeadSourceMemo__c',
                    'operator': 'contains',
                    'value': '新規作成】有料媒体突合 2026-03-05 ミイダス保育園'
                }
            ]
        }
    }

    resp = requests.post(report_url, headers=headers, json=meta1)
    if resp.status_code == 200:
        r1 = resp.json()
        r1_id = r1['reportMetadata']['id']
        print(f'レポート1（新規リード）: {r1_id}')
        reports.append(('新規作成リード', r1_id))
    else:
        print(f'レポート1 エラー: {resp.status_code} {resp.text[:200]}')

    # レポート2: 既存更新リード
    meta2 = {
        'reportMetadata': {
            'name': 'ミイダス保育園_既存更新リード_20260305',
            'reportFormat': 'TABULAR',
            'reportType': {'type': 'LeadList'},
            'detailColumns': [
                'OWNER', 'COMPANY', 'LAST_NAME', 'PHONE', 'MOBILE_PHONE',
                'STREET', 'Lead.Prefecture__c', 'Lead.PresidentName__c',
                'Lead.Paid_Media__c', 'Lead.Paid_JobTitle__c',
                'Lead.Paid_URL__c', 'Lead.Paid_DataExportDate__c'
            ],
            'reportFilters': [
                {
                    'column': 'Lead.LeadSourceMemo__c',
                    'operator': 'contains',
                    'value': '既存更新】有料媒体突合 2026-03-05 ミイダス保育園'
                }
            ]
        }
    }

    resp = requests.post(report_url, headers=headers, json=meta2)
    if resp.status_code == 200:
        r2 = resp.json()
        r2_id = r2['reportMetadata']['id']
        print(f'レポート2（既存更新リード）: {r2_id}')
        reports.append(('既存更新リード', r2_id))
    else:
        print(f'レポート2 エラー: {resp.status_code} {resp.text[:200]}')

    # レポート3: Account更新
    meta3 = {
        'reportMetadata': {
            'name': 'ミイダス保育園_既存更新Account_20260305',
            'reportFormat': 'TABULAR',
            'reportType': {'type': 'AccountList'},
            'detailColumns': [
                'ACCOUNT.NAME', 'PHONE1', 'Account.Paid_Media__c',
                'Account.Paid_DataSource__c', 'Account.Paid_JobTitle__c',
                'Account.Paid_DataExportDate__c', 'Account.Paid_URL__c'
            ],
            'reportFilters': [
                {
                    'column': 'Account.Paid_DataSource__c',
                    'operator': 'equals',
                    'value': 'ミイダス'
                },
                {
                    'column': 'Account.Paid_DataExportDate__c',
                    'operator': 'equals',
                    'value': '2026-03-05'
                }
            ]
        }
    }

    resp = requests.post(report_url, headers=headers, json=meta3)
    if resp.status_code == 200:
        r3 = resp.json()
        r3_id = r3['reportMetadata']['id']
        print(f'レポート3（Account更新）: {r3_id}')
        reports.append(('既存更新Account', r3_id))
    else:
        print(f'レポート3 エラー: {resp.status_code} {resp.text[:200]}')

    return reports


def main():
    print('=' * 70)
    print('ミイダス保育園リスト Salesforceインポート 2026-03-05')
    print('=' * 70)

    client = SalesforceClient()
    client.authenticate()
    print('Salesforce認証OK')

    # STEP 1: 新規リード作成
    r1 = import_new_leads(client)

    # STEP 2: 既存Lead更新
    r2 = update_leads(client)

    # STEP 3: 既存Account更新
    r3 = update_accounts(client)

    # STEP 4: レポート作成
    reports = create_reports(client)

    # 最終サマリー
    print('\n' + '=' * 70)
    print('【最終結果サマリー】')
    print('=' * 70)
    print(f'新規リード: {r1.get("numberRecordsProcessed", "?")}件成功 / {r1.get("numberRecordsFailed", "?")}件失敗')
    print(f'Lead更新: {r2.get("numberRecordsProcessed", "?")}件成功 / {r2.get("numberRecordsFailed", "?")}件失敗')
    print(f'Account更新: {r3.get("numberRecordsProcessed", "?")}件成功 / {r3.get("numberRecordsFailed", "?")}件失敗')
    print()
    print('レポート:')
    for name, rid in reports:
        print(f'  {name}: https://fora-career6.my.salesforce.com/lightning/r/Report/{rid}/view')


if __name__ == '__main__':
    main()
