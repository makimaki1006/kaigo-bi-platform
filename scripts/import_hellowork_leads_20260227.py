"""ハローワーク新規リード インポート + レポート作成（2026-02-27）"""
import sys
import io
import csv
import time
from pathlib import Path
from datetime import datetime

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.utils.config import sf_config

OUTPUT_DIR = project_root / "data" / "output" / "hellowork"
INPUT_CSV = OUTPUT_DIR / "new_leads_import_20260227_assigned.csv"
TIMESTAMP = datetime.now().strftime('%Y%m%d_%H%M%S')

OWNERS = {
    '小林幸太': '005J3000000ERz4IAG',
    '藤巻真弥': '0055i00000BeOKbAAN',
    '服部翔太郎': '005J3000000EYYjIAO',
    '深堀勇侍': '0055i00000CwKEhAAN',
}


class BulkInserter:
    def __init__(self):
        self.instance_url = sf_config.INSTANCE_URL
        self.api_version = sf_config.API_VERSION
        self.access_token = None
        self.session = requests.Session()
        retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
        self.session.mount("https://", adapter)

    def authenticate(self):
        url = f"{self.instance_url}/services/oauth2/token"
        payload = {
            'grant_type': 'refresh_token',
            'client_id': sf_config.CLIENT_ID,
            'client_secret': sf_config.CLIENT_SECRET,
            'refresh_token': sf_config.REFRESH_TOKEN,
        }
        resp = self.session.post(url, data=payload)
        resp.raise_for_status()
        self.access_token = resp.json()['access_token']
        print("認証成功")

    def _headers(self, content_type=None):
        h = {'Authorization': f'Bearer {self.access_token}'}
        if content_type:
            h['Content-Type'] = content_type
        return h

    def create_insert_job(self, object_name):
        url = f"{self.instance_url}/services/data/{self.api_version}/jobs/ingest"
        payload = {
            'operation': 'insert',
            'object': object_name,
            'contentType': 'CSV',
            'lineEnding': 'CRLF',
        }
        resp = self.session.post(url, headers=self._headers('application/json'), json=payload)
        if resp.status_code not in (200, 201):
            raise Exception(f"Job作成失敗: {resp.text}")
        job_id = resp.json()['id']
        print(f"  Insert Job作成: {job_id}")
        return job_id

    def upload_data(self, job_id, df):
        url = f"{self.instance_url}/services/data/{self.api_version}/jobs/ingest/{job_id}/batches"
        buf = io.StringIO()
        df.to_csv(buf, index=False, quoting=csv.QUOTE_ALL)
        resp = self.session.put(url, headers=self._headers('text/csv'), data=buf.getvalue().encode('utf-8'))
        if resp.status_code not in (200, 201):
            raise Exception(f"アップロード失敗: {resp.text}")
        print(f"  アップロード完了: {len(df)}件")

    def close_job(self, job_id):
        url = f"{self.instance_url}/services/data/{self.api_version}/jobs/ingest/{job_id}"
        resp = self.session.patch(url, headers=self._headers('application/json'), json={'state': 'UploadComplete'})
        if resp.status_code not in (200, 201):
            raise Exception(f"Jobクローズ失敗: {resp.text}")
        print("  Jobクローズ（処理開始）")

    def wait_for_job(self, job_id, timeout=600):
        url = f"{self.instance_url}/services/data/{self.api_version}/jobs/ingest/{job_id}"
        start = time.time()
        while True:
            resp = self.session.get(url, headers=self._headers())
            resp.raise_for_status()
            info = resp.json()
            state = info['state']
            processed = info.get('numberRecordsProcessed', 0)
            failed = info.get('numberRecordsFailed', 0)
            if state == 'JobComplete':
                print(f"  完了: 処理={processed}件, 失敗={failed}件")
                return info
            elif state in ('Failed', 'Aborted'):
                raise Exception(f"Job失敗: {state} - {info.get('errorMessage', '')}")
            if time.time() - start > timeout:
                raise Exception(f"タイムアウト: {timeout}秒")
            print(f"  処理中... ({processed}件)")
            time.sleep(5)

    def get_successful_records(self, job_id):
        url = f"{self.instance_url}/services/data/{self.api_version}/jobs/ingest/{job_id}/successfulResults"
        resp = self.session.get(url, headers=self._headers())
        if resp.status_code == 200 and resp.content:
            return pd.read_csv(io.StringIO(resp.text))
        return pd.DataFrame()

    def get_failed_records(self, job_id):
        url = f"{self.instance_url}/services/data/{self.api_version}/jobs/ingest/{job_id}/failedResults"
        resp = self.session.get(url, headers=self._headers())
        if resp.status_code == 200 and resp.content:
            return pd.read_csv(io.StringIO(resp.text))
        return pd.DataFrame()

    def create_report(self, name, filters, description=''):
        """公開レポートを作成（unfiled$public）"""
        url = f"{self.instance_url}/services/data/{self.api_version}/analytics/reports"

        detail_columns = [
            'LEAD.OWNER_FULL_NAME',
            'COMPANY',
            'Lead.LastName',
            'PHONE1',
            'Lead.MobilePhone',
            'Lead.Street',
            'Lead.Prefecture__c',
            'Lead.NumberOfEmployees',
            'Lead.Website',
            'Lead.Hellowork_Industry__c',
            'Lead.Hellowork_RecuritmentType__c',
            'Lead.Hellowork_EmploymentType__c',
            'Lead.Hellowork_RecruitmentReasonCategory__c',
            'Lead.Hellowork_NumberOfRecruitment__c',
            'Lead.Hellowork_DataImportDate__c',
            'Lead.Status',
            'Lead.PresidentName__c',
            'Lead.PresidentTitle__c',
        ]

        report_metadata = {
            'reportMetadata': {
                'name': name,
                'description': description,
                'reportFormat': 'TABULAR',
                'reportType': {'type': 'LeadList'},
                'detailColumns': detail_columns,
                'reportFilters': filters,
                'folderId': 'unfiled$public',
            }
        }

        resp = self.session.post(url, headers=self._headers('application/json'), json=report_metadata)
        if resp.status_code in (200, 201):
            rid = resp.json()['reportMetadata']['id']
            rurl = f"{self.instance_url}/lightning/r/Report/{rid}/view"
            print(f"  OK: {name}")
            print(f"      {rurl}")
            return rid, rurl
        else:
            print(f"  NG: {name} -> {resp.status_code} {resp.text[:300]}")
            return None, None


def main():
    print("=" * 60)
    print("ハローワーク新規リード インポート (2026-02-27)")
    print("=" * 60)

    inserter = BulkInserter()
    inserter.authenticate()

    # === STEP 1: インポート ===
    print(f"\n--- STEP 1: 新規リード作成 ---")
    df = pd.read_csv(INPUT_CSV, dtype=str, encoding='utf-8-sig')
    df = df.drop(columns=['Id'], errors='ignore').fillna('')
    print(f"  対象: {len(df)}件")

    # 所有者別内訳表示
    owner_map = {v: k for k, v in OWNERS.items()}
    for oid, cnt in df['OwnerId'].value_counts().items():
        print(f"    {owner_map.get(oid, oid)}: {cnt}件")

    job_id = inserter.create_insert_job('Lead')
    inserter.upload_data(job_id, df)
    inserter.close_job(job_id)
    info = inserter.wait_for_job(job_id)

    # 成功レコード保存
    success_df = inserter.get_successful_records(job_id)
    if len(success_df) > 0:
        id_path = OUTPUT_DIR / f"created_lead_ids_{TIMESTAMP}.csv"
        success_df.to_csv(id_path, index=False, encoding='utf-8-sig')
        print(f"  作成済みID保存: {id_path} ({len(success_df)}件)")

    # 失敗レコード
    failed_count = info.get('numberRecordsFailed', 0)
    if failed_count > 0:
        failed_df = inserter.get_failed_records(job_id)
        failed_path = OUTPUT_DIR / f"failed_leads_{TIMESTAMP}.csv"
        failed_df.to_csv(failed_path, index=False, encoding='utf-8-sig')
        print(f"  失敗レコード: {failed_path} ({failed_count}件)")

    # === STEP 2: レポート作成（公開） ===
    print(f"\n--- STEP 2: レポート作成（公開） ---")

    base_filters = [
        {'column': 'Lead.Hellowork_DataImportDate__c', 'operator': 'equals', 'value': '2026-02-27'},
        {'column': 'Lead.LeadSource', 'operator': 'equals', 'value': 'ハローワーク'},
        {'column': 'Lead.Status', 'operator': 'equals', 'value': '未架電'},
    ]

    report_results = []

    # 各所有者レポート（4件）
    for owner_name in OWNERS:
        name = f'HW新規リード_{owner_name}_20260227'
        filters = base_filters + [
            {'column': 'LEAD.OWNER_FULL_NAME', 'operator': 'equals', 'value': owner_name},
        ]
        rid, rurl = inserter.create_report(name, filters, f'ハローワーク新規リード（{owner_name}担当分）')
        report_results.append((name, rid, rurl))

    # 代表携帯レポート（服部・深堀分）
    name = 'HW新規_代表携帯_服部深堀_20260227'
    filters = base_filters + [
        {'column': 'Lead.MobilePhone', 'operator': 'notEqual', 'value': ''},
        {'column': 'LEAD.OWNER_FULL_NAME', 'operator': 'equals', 'value': '服部翔太郎,深堀勇侍'},
    ]
    rid, rurl = inserter.create_report(name, filters, 'ハローワーク新規リード（代表直通+携帯 服部・深堀分）')
    report_results.append((name, rid, rurl))

    # === サマリー ===
    print(f"\n{'='*60}")
    print("完了サマリー")
    print(f"{'='*60}")
    print(f"  インポート: {info.get('numberRecordsProcessed',0)}件処理, {failed_count}件失敗")
    print(f"\n  レポート:")
    for rname, rid, rurl in report_results:
        status = 'OK' if rid else 'NG'
        print(f"    [{status}] {rname}")
        if rurl:
            print(f"         {rurl}")


if __name__ == '__main__':
    main()
