"""
Salesforce Opportunity サービス
- Zoom商談分析結果をOpportunityに書き込む
- Bulk API 2.0 使用
"""

import sys
import time
import io
import csv
from pathlib import Path
from datetime import datetime
from typing import Optional

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.utils.config import sf_config


class OpportunityService:
    """
    Salesforce Opportunity 操作サービス

    主な機能:
    - Zoom分析結果の書き込み
    - Bulk API 2.0 によるバッチ更新
    - Opportunityデータの取得
    """

    # Zoom分析結果フィールドマッピング
    ZOOM_FIELD_MAP = {
        'prediction': 'Zoom_Prediction__c',           # Won予測/Lost予測
        'analysis_score': 'Zoom_Analysis_Score__c',   # 総合スコア 0-100
        'risk_level': 'Zoom_Risk_Level__c',           # 高/中/低
        'temperature_check': 'Zoom_Temperature_Check__c',  # Boolean
        'temperature_value': 'Zoom_Temperature_Value__c',  # 1-10
        'customer_next_step': 'Zoom_Customer_Next_Step__c',  # Boolean
        'hearing_ratio': 'Zoom_Hearing_Ratio__c',     # パーセント
        'objection_ratio': 'Zoom_Objection_Ratio__c', # パーセント
        'applied_rule': 'Zoom_Applied_Rule__c',       # ルール名
        'last_analyzed': 'Zoom_Last_Analyzed__c',     # 日時
        'meeting_id': 'Zoom_Meeting_ID__c',           # Zoom UUID
    }

    def __init__(self):
        self.instance_url = sf_config.INSTANCE_URL
        self.api_version = sf_config.API_VERSION
        self.access_token = None

        # セッション設定（リトライ付き）
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
        self.session.mount("https://", adapter)

    def authenticate(self) -> str:
        """OAuth認証"""
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
        print("Salesforce認証成功")
        return self.access_token

    def _headers(self, content_type: str = None) -> dict:
        headers = {'Authorization': f'Bearer {self.access_token}'}
        if content_type:
            headers['Content-Type'] = content_type
        return headers

    # ========================================
    # Bulk API 2.0 Query メソッド
    # ========================================

    def create_query_job(self, soql: str) -> str:
        """Bulk API 2.0 Query Job作成"""
        url = f"{self.instance_url}/services/data/{self.api_version}/jobs/query"

        payload = {
            'operation': 'query',
            'query': soql,
        }

        response = self.session.post(
            url,
            headers=self._headers('application/json'),
            json=payload
        )

        if response.status_code not in [200, 201]:
            error_detail = response.json() if response.content else response.text
            raise Exception(f"Query Job作成失敗: {error_detail}")

        return response.json()['id']

    def wait_for_query_job(self, job_id: str, timeout: int = 600) -> dict:
        """Query Job完了待機"""
        url = f"{self.instance_url}/services/data/{self.api_version}/jobs/query/{job_id}"
        start_time = time.time()

        while True:
            response = self.session.get(url, headers=self._headers())
            response.raise_for_status()

            job_info = response.json()
            state = job_info['state']

            if state == 'JobComplete':
                return job_info
            elif state in ['Failed', 'Aborted']:
                error_msg = job_info.get('errorMessage', 'Unknown error')
                raise Exception(f"Query Job失敗: {state} - {error_msg}")

            if time.time() - start_time > timeout:
                raise Exception(f"Query Jobタイムアウト: {timeout}秒")

            time.sleep(2)

    def get_query_results(self, job_id: str) -> pd.DataFrame:
        """Query Job結果をDataFrameで取得"""
        all_data = []
        locator = None

        while True:
            url = f"{self.instance_url}/services/data/{self.api_version}/jobs/query/{job_id}/results"
            params = {}
            if locator:
                params['locator'] = locator

            response = self.session.get(url, headers=self._headers(), params=params)
            response.raise_for_status()

            # Salesforce Bulk APIはUTF-8でデータを返す
            response.encoding = 'utf-8'
            if response.text:
                df_chunk = pd.read_csv(io.StringIO(response.text), dtype=str)
                all_data.append(df_chunk)

            locator = response.headers.get('Sforce-Locator')
            if not locator or locator == 'null':
                break

        if all_data:
            return pd.concat(all_data, ignore_index=True)
        return pd.DataFrame()

    def bulk_query(self, soql: str, description: str = "") -> pd.DataFrame:
        """Bulk API 2.0 Queryを実行（統合メソッド）"""
        if description:
            print(f"  [Bulk Query] {description}")

        job_id = self.create_query_job(soql)
        print(f"    Job作成: {job_id}")

        print("    処理中...", end="", flush=True)
        job_info = self.wait_for_query_job(job_id)
        records = job_info.get('numberRecordsProcessed', 0)
        print(f" 完了 ({records:,} 件)")

        df = self.get_query_results(job_id)
        return df

    # ========================================
    # Bulk API 2.0 Ingest (Update) メソッド
    # ========================================

    def create_update_job(self, object_name: str = 'Opportunity') -> str:
        """Bulk API 2.0 Update Job作成"""
        url = f"{self.instance_url}/services/data/{self.api_version}/jobs/ingest"

        payload = {
            'operation': 'update',
            'object': object_name,
            'contentType': 'CSV',
            'lineEnding': 'CRLF',
        }

        response = self.session.post(
            url,
            headers=self._headers('application/json'),
            json=payload
        )

        if response.status_code not in [200, 201]:
            error_detail = response.json() if response.content else response.text
            raise Exception(f"Update Job作成失敗: {error_detail}")

        return response.json()['id']

    def upload_csv_data(self, job_id: str, csv_data: str) -> None:
        """CSVデータをJobにアップロード"""
        url = f"{self.instance_url}/services/data/{self.api_version}/jobs/ingest/{job_id}/batches"

        response = self.session.put(
            url,
            headers=self._headers('text/csv'),
            data=csv_data.encode('utf-8')
        )

        if response.status_code not in [200, 201]:
            error_detail = response.text
            raise Exception(f"CSVアップロード失敗: {error_detail}")

    def close_job(self, job_id: str) -> None:
        """Jobをクローズして処理開始"""
        url = f"{self.instance_url}/services/data/{self.api_version}/jobs/ingest/{job_id}"

        payload = {'state': 'UploadComplete'}

        response = self.session.patch(
            url,
            headers=self._headers('application/json'),
            json=payload
        )

        if response.status_code not in [200, 201]:
            error_detail = response.text
            raise Exception(f"Jobクローズ失敗: {error_detail}")

    def wait_for_ingest_job(self, job_id: str, timeout: int = 600) -> dict:
        """Ingest Job完了待機"""
        url = f"{self.instance_url}/services/data/{self.api_version}/jobs/ingest/{job_id}"
        start_time = time.time()

        while True:
            response = self.session.get(url, headers=self._headers())
            response.raise_for_status()

            job_info = response.json()
            state = job_info['state']

            if state == 'JobComplete':
                return job_info
            elif state in ['Failed', 'Aborted']:
                error_msg = job_info.get('errorMessage', 'Unknown error')
                raise Exception(f"Ingest Job失敗: {state} - {error_msg}")

            if time.time() - start_time > timeout:
                raise Exception(f"Ingest Jobタイムアウト: {timeout}秒")

            time.sleep(2)

    def get_failed_results(self, job_id: str) -> pd.DataFrame:
        """失敗レコードを取得"""
        url = f"{self.instance_url}/services/data/{self.api_version}/jobs/ingest/{job_id}/failedResults"

        response = self.session.get(url, headers=self._headers())
        response.raise_for_status()

        if response.text:
            return pd.read_csv(io.StringIO(response.text), dtype=str)
        return pd.DataFrame()

    # ========================================
    # Opportunity 取得メソッド
    # ========================================

    def get_opportunities(
        self,
        fields: list[str] = None,
        where_clause: str = None,
        limit: int = None
    ) -> pd.DataFrame:
        """
        Opportunityデータを取得

        Args:
            fields: 取得フィールドリスト（デフォルト: 基本フィールド）
            where_clause: WHERE句（例: "StageName = '受注'"）
            limit: 取得件数制限

        Returns:
            pd.DataFrame: Opportunityデータ
        """
        if fields is None:
            fields = [
                'Id', 'Name', 'AccountId', 'Account.Name',
                'StageName', 'CloseDate', 'Amount',
                'OwnerId', 'Owner.Name',
                'CreatedDate', 'LastModifiedDate'
            ]

        # Zoomフィールドが存在する場合は追加
        zoom_fields = list(self.ZOOM_FIELD_MAP.values())

        field_list = ', '.join(fields)
        soql = f"SELECT {field_list} FROM Opportunity"

        if where_clause:
            soql += f" WHERE {where_clause}"

        if limit:
            soql += f" LIMIT {limit}"

        return self.bulk_query(soql, "Opportunity取得")

    def get_opportunities_for_matching(
        self,
        from_date: str = None,
        to_date: str = None
    ) -> pd.DataFrame:
        """
        マッチング用のOpportunityデータを取得

        Args:
            from_date: 開始日（YYYY-MM-DD）
            to_date: 終了日（YYYY-MM-DD）

        Returns:
            pd.DataFrame: マッチング用Opportunityデータ
        """
        fields = [
            'Id', 'Name', 'AccountId', 'Account.Name',
            'StageName', 'CloseDate', 'Amount',
            'OwnerId', 'Owner.Name',
        ]

        # カスタムフィールドが存在する場合のみ追加
        zoom_fields_exist = self.check_zoom_fields_exist()
        if zoom_fields_exist.get('Zoom_Meeting_ID__c', False):
            fields.append('Zoom_Meeting_ID__c')

        field_list = ', '.join(fields)
        soql = f"SELECT {field_list} FROM Opportunity"

        conditions = []
        if from_date:
            conditions.append(f"CloseDate >= {from_date}")
        if to_date:
            conditions.append(f"CloseDate <= {to_date}")

        if conditions:
            soql += " WHERE " + " AND ".join(conditions)

        return self.bulk_query(soql, f"マッチング用Opportunity取得 ({from_date}〜{to_date})")

    # ========================================
    # Zoom分析結果書き込みメソッド
    # ========================================

    def update_zoom_analysis(
        self,
        updates: list[dict],
        dry_run: bool = False
    ) -> dict:
        """
        Zoom分析結果をOpportunityに書き込む

        Args:
            updates: 更新データリスト
                [{
                    'Id': 'OpportunityId',
                    'prediction': 'Won予測',
                    'analysis_score': 75,
                    'risk_level': '中',
                    'temperature_check': True,
                    'temperature_value': 7,
                    'customer_next_step': True,
                    'hearing_ratio': 0.25,
                    'objection_ratio': 0.08,
                    'applied_rule': 'rule_2a_next_step_positive',
                    'meeting_id': 'zoom_uuid'
                }, ...]
            dry_run: Trueなら実際の更新を行わない

        Returns:
            dict: 処理結果
        """
        if not updates:
            return {'success': 0, 'failed': 0, 'errors': []}

        print(f"\n[Zoom分析結果更新] {len(updates):,} 件")

        # 更新日時を設定
        now = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.000Z')

        # DataFrame作成
        rows = []
        for update in updates:
            row = {'Id': update['Id']}

            # フィールドマッピング
            for key, sf_field in self.ZOOM_FIELD_MAP.items():
                if key in update:
                    value = update[key]

                    # Boolean変換
                    if key in ['temperature_check', 'customer_next_step']:
                        row[sf_field] = 'true' if value else 'false'
                    # パーセント変換
                    elif key in ['hearing_ratio', 'objection_ratio']:
                        row[sf_field] = str(float(value) * 100) if value is not None else ''
                    # 日時
                    elif key == 'last_analyzed':
                        row[sf_field] = now
                    else:
                        row[sf_field] = str(value) if value is not None else ''

            # 最終分析日時を必ず設定
            row['Zoom_Last_Analyzed__c'] = now

            rows.append(row)

        df = pd.DataFrame(rows)

        if dry_run:
            print("  [DRY RUN] 実際の更新は行いません")
            print(f"  更新予定フィールド: {list(df.columns)}")
            print(f"  サンプル:")
            print(df.head(3).to_string())
            return {'success': len(updates), 'failed': 0, 'errors': [], 'dry_run': True}

        # CSV生成
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, lineterminator='\r\n')
        csv_data = csv_buffer.getvalue()

        # Bulk API 2.0 Update実行
        try:
            job_id = self.create_update_job('Opportunity')
            print(f"    Update Job作成: {job_id}")

            self.upload_csv_data(job_id, csv_data)
            print("    CSVアップロード完了")

            self.close_job(job_id)
            print("    Job実行開始")

            print("    処理中...", end="", flush=True)
            job_info = self.wait_for_ingest_job(job_id)

            success = int(job_info.get('numberRecordsProcessed', 0))
            failed = int(job_info.get('numberRecordsFailed', 0))

            print(f" 完了 (成功: {success:,}, 失敗: {failed:,})")

            # 失敗レコード取得
            errors = []
            if failed > 0:
                df_failed = self.get_failed_results(job_id)
                if not df_failed.empty:
                    errors = df_failed.to_dict('records')
                    print(f"  失敗レコード:")
                    for err in errors[:5]:
                        print(f"    - {err}")

            return {
                'success': success,
                'failed': failed,
                'errors': errors,
                'job_id': job_id
            }

        except Exception as e:
            print(f"  エラー: {e}")
            return {
                'success': 0,
                'failed': len(updates),
                'errors': [str(e)]
            }

    # ========================================
    # ユーティリティ
    # ========================================

    def check_zoom_fields_exist(self) -> dict:
        """
        Zoom分析フィールドが存在するか確認

        Returns:
            dict: {フィールド名: 存在するか}
        """
        print("\n[Zoomフィールド確認]")

        url = f"{self.instance_url}/services/data/{self.api_version}/sobjects/Opportunity/describe"

        response = self.session.get(url, headers=self._headers())
        response.raise_for_status()

        describe = response.json()
        existing_fields = {f['name'] for f in describe['fields']}

        result = {}
        for display_name, api_name in self.ZOOM_FIELD_MAP.items():
            exists = api_name in existing_fields
            result[api_name] = exists
            status = "✅" if exists else "❌"
            print(f"  {status} {api_name}")

        missing = [k for k, v in result.items() if not v]
        if missing:
            print(f"\n  ⚠️ 未作成のフィールド: {len(missing)} 件")
            print("    Salesforce設定画面でカスタムフィールドを追加してください")
        else:
            print(f"\n  ✅ 全フィールド存在確認済み")

        return result


# ========================================
# CLI
# ========================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Opportunity Service")
    parser.add_argument("--check-fields", action="store_true", help="Zoomフィールドの存在確認")
    parser.add_argument("--get-opps", action="store_true", help="Opportunityデータ取得テスト")
    parser.add_argument("--from-date", type=str, help="開始日 (YYYY-MM-DD)")
    parser.add_argument("--to-date", type=str, help="終了日 (YYYY-MM-DD)")

    args = parser.parse_args()

    service = OpportunityService()
    service.authenticate()

    if args.check_fields:
        service.check_zoom_fields_exist()

    if args.get_opps:
        df = service.get_opportunities_for_matching(args.from_date, args.to_date)
        print(f"\n取得件数: {len(df):,} 件")
        print(df.head(10))
