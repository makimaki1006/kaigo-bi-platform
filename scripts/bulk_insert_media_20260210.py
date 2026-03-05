# -*- coding: utf-8 -*-
"""
有料媒体 新規リード Bulk Insert + レポート作成 (2026-02-10)
ミイダス + itszai の新規リードをSalesforceにインポートし、レポートを作成
"""

import sys
import time
import io
import csv
from pathlib import Path
from datetime import datetime

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.stdout.reconfigure(encoding='utf-8')

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.utils.config import sf_config

OUTPUT_DIR = project_root / "data" / "output" / "media_matching"
TODAY = datetime.now().strftime('%Y-%m-%d')
TIMESTAMP = datetime.now().strftime('%Y%m%d_%H%M%S')

# 新規リードCSVファイル
MIIDAS_CSV = OUTPUT_DIR / "miidas_new_leads_20260210_101354.csv"
ITSZAI_CSV = OUTPUT_DIR / "itszai_new_leads_20260210_113048.csv"


class BulkInserter:
    """Salesforce Bulk API 2.0 Insert クライアント"""

    def __init__(self):
        self.instance_url = sf_config.INSTANCE_URL
        self.api_version = sf_config.API_VERSION
        self.access_token = None
        self.session = requests.Session()
        retry_strategy = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
        self.session.mount("https://", adapter)

    def authenticate(self):
        token_url = f"{self.instance_url}/services/oauth2/token"
        payload = {
            'grant_type': 'refresh_token',
            'client_id': sf_config.CLIENT_ID,
            'client_secret': sf_config.CLIENT_SECRET,
            'refresh_token': sf_config.REFRESH_TOKEN
        }
        response = self.session.post(token_url, data=payload)
        response.raise_for_status()
        self.access_token = response.json()['access_token']
        print("認証成功")

    def _headers(self, content_type=None):
        headers = {'Authorization': f'Bearer {self.access_token}'}
        if content_type:
            headers['Content-Type'] = content_type
        return headers

    def create_insert_job(self, object_name):
        url = f"{self.instance_url}/services/data/{self.api_version}/jobs/ingest"
        payload = {
            'operation': 'insert',
            'object': object_name,
            'contentType': 'CSV',
            'lineEnding': 'CRLF',
        }
        response = self.session.post(url, headers=self._headers('application/json'), json=payload)
        if response.status_code not in [200, 201]:
            raise Exception(f"Job作成失敗: {response.text}")
        job_id = response.json()['id']
        print(f"  Insert Job作成: {job_id}")
        return job_id

    def upload_data(self, job_id, df):
        url = f"{self.instance_url}/services/data/{self.api_version}/jobs/ingest/{job_id}/batches"
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, quoting=csv.QUOTE_ALL)
        csv_data = csv_buffer.getvalue()
        response = self.session.put(url, headers=self._headers('text/csv'), data=csv_data.encode('utf-8'))
        if response.status_code not in [200, 201]:
            raise Exception(f"アップロード失敗: {response.text}")
        print(f"  アップロード完了: {len(df)}件")

    def close_job(self, job_id):
        url = f"{self.instance_url}/services/data/{self.api_version}/jobs/ingest/{job_id}"
        response = self.session.patch(url, headers=self._headers('application/json'), json={'state': 'UploadComplete'})
        if response.status_code not in [200, 201]:
            raise Exception(f"Jobクローズ失敗: {response.text}")
        print("  Jobクローズ（処理開始）")

    def wait_for_job(self, job_id, timeout=600):
        url = f"{self.instance_url}/services/data/{self.api_version}/jobs/ingest/{job_id}"
        start = time.time()
        while True:
            response = self.session.get(url, headers=self._headers())
            response.raise_for_status()
            info = response.json()
            state = info['state']
            processed = info.get('numberRecordsProcessed', 0)
            failed = info.get('numberRecordsFailed', 0)
            if state == 'JobComplete':
                print(f"  完了: 処理={processed}件, 失敗={failed}件")
                return info
            elif state in ['Failed', 'Aborted']:
                raise Exception(f"Job失敗: {state} - {info.get('errorMessage', '')}")
            if time.time() - start > timeout:
                raise Exception(f"タイムアウト: {timeout}秒")
            print(f"  処理中... ({processed}件)")
            time.sleep(5)

    def get_successful_records(self, job_id):
        url = f"{self.instance_url}/services/data/{self.api_version}/jobs/ingest/{job_id}/successfulResults"
        response = self.session.get(url, headers=self._headers())
        if response.status_code == 200 and response.content:
            return pd.read_csv(io.StringIO(response.text))
        return pd.DataFrame()

    def get_failed_records(self, job_id):
        url = f"{self.instance_url}/services/data/{self.api_version}/jobs/ingest/{job_id}/failedResults"
        response = self.session.get(url, headers=self._headers())
        if response.status_code == 200 and response.content:
            return pd.read_csv(io.StringIO(response.text))
        return pd.DataFrame()

    def insert_leads(self, csv_path, label):
        print(f"\n{'='*60}")
        print(f"新規リード作成: {label}")
        print(f"{'='*60}")

        df = pd.read_csv(csv_path, dtype=str, encoding='utf-8-sig')
        # NaN を空文字に変換し、Id列があれば除去
        df = df.drop(columns=['Id'], errors='ignore')
        df = df.fillna('')
        print(f"  対象: {len(df)}件")

        # Job作成 → アップロード → クローズ → 待機
        job_id = self.create_insert_job('Lead')
        self.upload_data(job_id, df)
        self.close_job(job_id)
        info = self.wait_for_job(job_id)

        # 成功レコード取得（作成されたIDを保存）
        success_df = self.get_successful_records(job_id)
        if len(success_df) > 0:
            id_path = OUTPUT_DIR / f"created_lead_ids_{label}_{TIMESTAMP}.csv"
            success_df.to_csv(id_path, index=False, encoding='utf-8-sig')
            print(f"  作成済みID保存: {id_path} ({len(success_df)}件)")

        # 失敗レコード
        failed = info.get('numberRecordsFailed', 0)
        if failed > 0:
            failed_df = self.get_failed_records(job_id)
            failed_path = OUTPUT_DIR / f"failed_leads_{label}_{TIMESTAMP}.csv"
            failed_df.to_csv(failed_path, index=False, encoding='utf-8-sig')
            print(f"  失敗レコード: {failed_path} ({failed}件)")

        return {
            'label': label,
            'total': len(df),
            'processed': info.get('numberRecordsProcessed', 0),
            'failed': failed,
            'job_id': job_id,
            'success_ids': success_df['sf__Id'].tolist() if len(success_df) > 0 and 'sf__Id' in success_df.columns else [],
        }

    def create_report(self, name, report_type, detail_columns, filters, description=''):
        """Salesforce Analytics APIでレポートを作成"""
        url = f"{self.instance_url}/services/data/{self.api_version}/analytics/reports"
        report_metadata = {
            'reportMetadata': {
                'name': name,
                'reportFormat': 'TABULAR',
                'reportType': {'type': report_type},
                'detailColumns': detail_columns,
                'reportFilters': filters,
                'description': description,
            }
        }
        response = self.session.post(url, headers=self._headers('application/json'), json=report_metadata)
        if response.status_code not in [200, 201]:
            print(f"  レポート作成失敗: {response.text[:200]}")
            return None
        report_id = response.json()['reportMetadata']['id']
        report_url = f"{self.instance_url}/lightning/r/Report/{report_id}/view"
        print(f"  レポート作成成功: {name}")
        print(f"  URL: {report_url}")
        return report_id, report_url


def main():
    print("=" * 60)
    print(f"有料媒体 新規リード一括インポート ({TODAY})")
    print("=" * 60)

    inserter = BulkInserter()
    inserter.authenticate()

    results = []

    # ミイダス
    if MIIDAS_CSV.exists():
        result = inserter.insert_leads(MIIDAS_CSV, 'miidas')
        results.append(result)
    else:
        print(f"ファイルなし: {MIIDAS_CSV}")

    # itszai
    if ITSZAI_CSV.exists():
        result = inserter.insert_leads(ITSZAI_CSV, 'itszai')
        results.append(result)
    else:
        print(f"ファイルなし: {ITSZAI_CSV}")

    # サマリー
    print(f"\n{'='*60}")
    print("インポート結果サマリー")
    print(f"{'='*60}")
    all_ids = []
    for r in results:
        print(f"  {r['label']}: 処理={r['processed']}件, 失敗={r['failed']}件")
        all_ids.extend(r.get('success_ids', []))
    print(f"  合計作成ID: {len(all_ids)}件")

    # レポート作成
    print(f"\n{'='*60}")
    print("レポート作成")
    print(f"{'='*60}")

    # レポート1: 新規作成リード（ミイダス + itszai）
    report_result = inserter.create_report(
        name=f'有料媒体 新規リード {TODAY}（ミイダス・itszai）',
        report_type='LeadList',
        detail_columns=[
            'FULL_NAME',
            'COMPANY',
            'LEAD.PHONE',
            'LEAD.MOBILE_PHONE',
            'LEAD.EMAIL',
            'Lead.Paid_Media__c',
            'Lead.Paid_JobTitle__c',
            'Lead.Paid_URL__c',
            'Lead.Paid_DataExportDate__c',
            'Lead.PresidentName__c',
            'LEAD.STATUS',
            'OWNER_FULL_NAME',
        ],
        filters=[
            {
                'column': 'Lead.Paid_DataExportDate__c',
                'operator': 'equals',
                'value': TODAY,
            },
            {
                'column': 'Lead.Paid_Media__c',
                'operator': 'equals',
                'value': 'ミイダス,itszai',
            },
        ],
        description=f'{TODAY} 有料媒体突合で新規作成されたリード（ミイダス + itszai）',
    )

    print(f"\n{'='*60}")
    print("全処理完了")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
