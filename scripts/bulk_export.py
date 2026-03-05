"""
Salesforce Bulk API 2.0 エクスポートスクリプト
全項目をAPI参照名ヘッダーでCSV出力する
"""

import sys
import time
import argparse
from pathlib import Path
from datetime import datetime

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.utils.config import sf_config


class BulkExporter:
    """
    Salesforce Bulk API 2.0 エクスポーター

    効率的な接続プールとリトライ機能を備えた実装
    """

    def __init__(self):
        self.instance_url = sf_config.INSTANCE_URL
        self.api_version = sf_config.API_VERSION
        self.access_token = None

        # セッション設定（接続プール + リトライ）
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
        self.session.mount("https://", adapter)

    def authenticate(self) -> str:
        """認証してアクセストークンを取得"""
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
        return self.access_token

    def _headers(self, content_type: str = None) -> dict:
        """リクエストヘッダーを取得"""
        headers = {'Authorization': f'Bearer {self.access_token}'}
        if content_type:
            headers['Content-Type'] = content_type
        return headers

    def get_queryable_fields(self, object_name: str) -> list[str]:
        """
        クエリ可能なフィールドのみを取得

        Args:
            object_name: オブジェクト名

        Returns:
            list[str]: クエリ可能なフィールド名のリスト
        """
        url = f"{self.instance_url}/services/data/{self.api_version}/sobjects/{object_name}/describe"
        response = self.session.get(url, headers=self._headers())
        response.raise_for_status()

        fields = []
        for field in response.json()['fields']:
            # クエリ可能なフィールドのみ（計算フィールド等を除外）
            # 複合フィールド（Address等）も除外（個別フィールドで取得）
            if field.get('type') != 'address' and field.get('type') != 'location':
                fields.append(field['name'])

        print(f"  {object_name}: {len(fields)} フィールド（クエリ可能）")
        return fields

    def export_object_bulk(
        self,
        object_name: str,
        output_dir: Path,
        fields: list[str] = None,
        where_clause: str = None,
    ) -> Path:
        """
        Bulk API 2.0でオブジェクトをエクスポート

        Args:
            object_name: オブジェクト名
            output_dir: 出力ディレクトリ
            fields: 取得フィールド（Noneで全項目）
            where_clause: WHERE句（オプション）

        Returns:
            Path: 出力ファイルパス
        """
        print(f"\n[{object_name}] エクスポート開始")

        # フィールド取得
        if fields is None:
            fields = self.get_queryable_fields(object_name)

        # フィールドが多すぎる場合は分割（SOQLの長さ制限対策）
        # 1クエリあたりの推奨フィールド数は約100-150
        if len(fields) > 100:
            print(f"  フィールド数が多いため分割エクスポート: {len(fields)}フィールド")
            return self._export_chunked(object_name, output_dir, fields, where_clause)

        # SOQL作成
        soql = f"SELECT {', '.join(fields)} FROM {object_name}"
        if where_clause:
            soql += f" WHERE {where_clause}"

        # Bulk Job作成
        job_id = self._create_bulk_job(soql)

        # ジョブ完了待機
        self._wait_for_job(job_id)

        # 結果取得
        output_path = output_dir / f"{object_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        self._download_results(job_id, output_path)

        print(f"  出力: {output_path}")
        return output_path

    def _export_chunked(
        self,
        object_name: str,
        output_dir: Path,
        fields: list[str],
        where_clause: str = None,
    ) -> Path:
        """
        フィールドを分割してエクスポートし、後で結合

        Args:
            object_name: オブジェクト名
            output_dir: 出力ディレクトリ
            fields: 全フィールド
            where_clause: WHERE句

        Returns:
            Path: 結合後の出力ファイルパス
        """
        import pandas as pd

        chunk_size = 80  # 1回あたりのフィールド数
        id_field = 'Id'

        # Idは必ず含める
        if id_field in fields:
            fields.remove(id_field)

        chunks = [fields[i:i + chunk_size] for i in range(0, len(fields), chunk_size)]
        print(f"  {len(chunks)} 分割で実行")

        dfs = []

        for i, chunk_fields in enumerate(chunks):
            # Idを先頭に追加
            query_fields = [id_field] + chunk_fields

            soql = f"SELECT {', '.join(query_fields)} FROM {object_name}"
            if where_clause:
                soql += f" WHERE {where_clause}"

            print(f"  チャンク {i+1}/{len(chunks)}: {len(query_fields)} フィールド")

            job_id = self._create_bulk_job(soql)
            self._wait_for_job(job_id)

            # 一時ファイルに保存
            temp_path = output_dir / f"_temp_{object_name}_{i}.csv"
            self._download_results(job_id, temp_path)

            # DataFrameに読み込み
            df = pd.read_csv(temp_path, dtype=str, encoding='utf-8')
            dfs.append(df)

            # 一時ファイル削除
            temp_path.unlink()

        # 結合（Id列をキーに）
        result_df = dfs[0]
        for df in dfs[1:]:
            # Id列以外のカラムを取得
            merge_cols = [c for c in df.columns if c != id_field]
            result_df = result_df.merge(
                df[[id_field] + merge_cols],
                on=id_field,
                how='outer'
            )

        # 出力
        output_path = output_dir / f"{object_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        result_df.to_csv(output_path, index=False, encoding='utf-8-sig')

        print(f"  結合完了: {len(result_df)} 件, {len(result_df.columns)} カラム")
        return output_path

    def _create_bulk_job(self, soql: str) -> str:
        """Bulk Query Jobを作成"""
        url = f"{self.instance_url}/services/data/{self.api_version}/jobs/query"
        payload = {
            'operation': 'query',
            'query': soql,
            'contentType': 'CSV',
            'columnDelimiter': 'COMMA',
            'lineEnding': 'CRLF',
        }

        response = self.session.post(
            url,
            headers=self._headers('application/json'),
            json=payload
        )

        if response.status_code != 200:
            error_detail = response.json() if response.content else response.text
            raise Exception(f"Job作成失敗: {error_detail}")

        job_id = response.json()['id']
        print(f"  Job作成: {job_id}")
        return job_id

    def _wait_for_job(self, job_id: str, timeout: int = 600) -> None:
        """ジョブ完了を待機"""
        url = f"{self.instance_url}/services/data/{self.api_version}/jobs/query/{job_id}"
        start_time = time.time()

        while True:
            response = self.session.get(url, headers=self._headers())
            response.raise_for_status()

            state = response.json()['state']
            records = response.json().get('numberRecordsProcessed', 0)

            if state == 'JobComplete':
                print(f"  完了: {records} レコード")
                return
            elif state in ['Failed', 'Aborted']:
                error_msg = response.json().get('errorMessage', 'Unknown error')
                raise Exception(f"Job失敗: {state} - {error_msg}")

            # タイムアウトチェック
            if time.time() - start_time > timeout:
                raise Exception(f"タイムアウト: {timeout}秒")

            print(f"  処理中... ({records} レコード)")
            time.sleep(5)

    def _download_results(self, job_id: str, output_path: Path) -> None:
        """結果をダウンロード"""
        url = f"{self.instance_url}/services/data/{self.api_version}/jobs/query/{job_id}/results"

        response = self.session.get(url, headers=self._headers())
        response.raise_for_status()

        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, 'wb') as f:
            f.write(response.content)

    def export_multiple(
        self,
        object_names: list[str],
        output_dir: Path,
    ) -> list[Path]:
        """複数オブジェクトをエクスポート"""
        results = []

        for obj_name in object_names:
            try:
                path = self.export_object_bulk(obj_name, output_dir)
                results.append(path)
            except Exception as e:
                print(f"  エラー [{obj_name}]: {e}")

        return results


def main():
    parser = argparse.ArgumentParser(
        description='Salesforce Bulk API 2.0 エクスポート'
    )
    parser.add_argument(
        '--objects',
        nargs='+',
        default=['Lead'],
        help='エクスポートするオブジェクト（デフォルト: Lead）'
    )
    parser.add_argument(
        '--output-dir',
        default='data/output',
        help='出力ディレクトリ（デフォルト: data/output）'
    )
    parser.add_argument(
        '--where',
        default=None,
        help='WHERE句（例: "CreatedDate > 2024-01-01T00:00:00Z"）'
    )

    args = parser.parse_args()

    output_dir = project_root / args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 50)
    print("Salesforce Bulk API 2.0 エクスポート")
    print("=" * 50)
    print(f"対象: {', '.join(args.objects)}")
    print(f"出力: {output_dir}")
    print()

    exporter = BulkExporter()
    exporter.authenticate()

    results = exporter.export_multiple(args.objects, output_dir)

    print("\n" + "=" * 50)
    print("完了")
    print("=" * 50)
    for path in results:
        print(f"  - {path}")


if __name__ == "__main__":
    main()
