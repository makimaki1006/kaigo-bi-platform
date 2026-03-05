"""
Salesforce API クライアント
認証、データ抽出、レポート取得を行う
"""

import time
from pathlib import Path
from typing import List, Optional

import requests

from utils.config import sf_config, output_config


class SalesforceClient:
    """Salesforce API クライアントクラス"""

    def __init__(self):
        self.instance_url = sf_config.INSTANCE_URL
        self.api_version = sf_config.API_VERSION
        self.access_token: Optional[str] = None

    def authenticate(self) -> str:
        """
        リフレッシュトークンを使用してアクセストークンを取得

        Returns:
            str: アクセストークン
        """
        token_url = f"{self.instance_url}/services/oauth2/token"
        payload = {
            'grant_type': 'refresh_token',
            'client_id': sf_config.CLIENT_ID,
            'client_secret': sf_config.CLIENT_SECRET,
            'refresh_token': sf_config.REFRESH_TOKEN
        }

        response = requests.post(token_url, data=payload)
        response.raise_for_status()

        self.access_token = response.json()['access_token']
        print("[OK] 認証成功")
        return self.access_token

    def _get_headers(self) -> dict:
        """認証ヘッダーを取得"""
        if not self.access_token:
            self.authenticate()
        return {'Authorization': f'Bearer {self.access_token}'}

    def get_all_fields(self, object_name: str) -> List[str]:
        """
        オブジェクトの全フィールド名を取得（Describe API）

        Args:
            object_name: Salesforceオブジェクト名（例: Account, Contact）

        Returns:
            List[str]: フィールド名のリスト
        """
        url = f"{self.instance_url}/services/data/{self.api_version}/sobjects/{object_name}/describe"
        headers = self._get_headers()

        response = requests.get(url, headers=headers)
        response.raise_for_status()

        fields = [field['name'] for field in response.json()['fields']]
        print(f"  - {object_name}: {len(fields)}フィールド取得")
        return fields

    def export_object(self, object_name: str, output_dir: Optional[Path] = None) -> Path:
        """
        Bulk API 2.0 を使用してオブジェクトの全データを抽出

        Args:
            object_name: Salesforceオブジェクト名
            output_dir: 出力ディレクトリ（省略時はデフォルト）

        Returns:
            Path: 保存されたCSVファイルのパス
        """
        print(f"🚀 {object_name} の抽出を開始...")

        # 出力先を確保
        if output_dir is None:
            output_dir = output_config.ensure_dir()

        # 全フィールドを取得
        fields = self.get_all_fields(object_name)
        soql = f"SELECT {','.join(fields)} FROM {object_name}"

        # Query Job 作成
        job_url = f"{self.instance_url}/services/data/{self.api_version}/jobs/query"
        headers = {
            **self._get_headers(),
            'Content-Type': 'application/json'
        }
        payload = {
            'operation': 'query',
            'query': soql,
            'contentType': 'CSV'
        }

        response = requests.post(job_url, headers=headers, json=payload)
        response.raise_for_status()
        job_id = response.json()['id']

        # ジョブ完了を待機
        while True:
            status_response = requests.get(
                f"{job_url}/{job_id}",
                headers=self._get_headers()
            )
            state = status_response.json()['state']
            print(f"  - ステータス: {state}")

            if state == 'JobComplete':
                break
            elif state in ['Failed', 'Aborted']:
                raise Exception(f"Job {state}: {status_response.json()}")

            time.sleep(10)

        # 結果をCSVとして保存
        result_response = requests.get(
            f"{job_url}/{job_id}/results",
            headers=self._get_headers()
        )

        file_path = output_dir / f"{object_name}_all_fields.csv"
        with open(file_path, 'wb') as f:
            f.write(result_response.content)

        print(f"✅ {file_path} 保存完了")
        return file_path

    def export_report(self, report_id: str, report_name: str,
                      output_dir: Optional[Path] = None) -> Path:
        """
        Salesforceレポートをエクスポート

        Args:
            report_id: レポートID
            report_name: 保存ファイル名
            output_dir: 出力ディレクトリ（省略時はデフォルト）

        Returns:
            Path: 保存されたCSVファイルのパス
        """
        print(f"🚀 レポート '{report_name}' を抽出中...")

        if output_dir is None:
            output_dir = output_config.ensure_dir()

        url = f"{self.instance_url}/{report_id}?export=1&enc=UTF-8&isdtp=p1"
        headers = self._get_headers()

        response = requests.get(url, headers=headers)
        response.raise_for_status()

        file_path = output_dir / f"{report_name}.csv"
        with open(file_path, 'wb') as f:
            f.write(response.content)

        print(f"✅ {file_path} 保存完了")
        return file_path

    def export_multiple_objects(self, object_names: List[str],
                                 output_dir: Optional[Path] = None) -> List[Path]:
        """
        複数オブジェクトを一括抽出

        Args:
            object_names: オブジェクト名のリスト
            output_dir: 出力ディレクトリ

        Returns:
            List[Path]: 保存されたCSVファイルパスのリスト
        """
        results = []
        for obj_name in object_names:
            try:
                path = self.export_object(obj_name, output_dir)
                results.append(path)
            except Exception as e:
                print(f"❌ {obj_name} の抽出に失敗: {e}")
        return results
