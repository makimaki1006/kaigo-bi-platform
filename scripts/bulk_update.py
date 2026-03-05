"""
Salesforce Bulk API 2.0 更新スクリプト
既存レコードの一括更新（Description/Memo フィールド追記）
"""

import sys
import time
import io
import csv
from pathlib import Path
from datetime import datetime

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.utils.config import sf_config


class BulkUpdater:
    """
    Salesforce Bulk API 2.0 一括更新クライアント

    Description/Memo フィールドへの追記更新に特化
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

    def get_existing_values(self, object_name: str, ids: list[str], field_name: str) -> dict[str, str]:
        """
        既存レコードのフィールド値を取得（追記用）

        Args:
            object_name: オブジェクト名
            ids: レコードIDリスト
            field_name: 取得するフィールド名

        Returns:
            dict[str, str]: {Id: 現在の値} のマッピング
        """
        print(f"[INFO] 既存値を取得中: {object_name}.{field_name} ({len(ids):,} 件)")

        # IDをチャンクに分割（SOQLの長さ制限対策）
        chunk_size = 200
        id_chunks = [ids[i:i + chunk_size] for i in range(0, len(ids), chunk_size)]

        result = {}

        for i, id_chunk in enumerate(id_chunks):
            # IN句用にIDをフォーマット
            id_list = "', '".join(id_chunk)
            soql = f"SELECT Id, {field_name} FROM {object_name} WHERE Id IN ('{id_list}')"

            url = f"{self.instance_url}/services/data/{self.api_version}/query"
            response = self.session.get(url, headers=self._headers(), params={'q': soql})
            response.raise_for_status()

            records = response.json().get('records', [])
            for rec in records:
                result[rec['Id']] = rec.get(field_name, '') or ''

            if (i + 1) % 10 == 0:
                print(f"  進捗: {(i + 1) * chunk_size:,} / {len(ids):,}")

        print(f"  取得完了: {len(result):,} 件")
        return result

    def prepare_update_data(
        self,
        update_df: pd.DataFrame,
        object_name: str,
        target_field: str,
        append_mode: bool = True
    ) -> pd.DataFrame:
        """
        更新用データを準備（追記モード対応）

        Args:
            update_df: 更新データ（Id, Description_Addition カラム必須）
            object_name: オブジェクト名
            target_field: 更新対象フィールド名
            append_mode: True=追記 / False=上書き

        Returns:
            pd.DataFrame: Bulk API用のデータ（Id, target_field）
        """
        if append_mode:
            # 既存値を取得
            ids = update_df['Id'].tolist()
            existing_values = self.get_existing_values(object_name, ids, target_field)

            # 追記データを作成
            def append_value(row):
                existing = existing_values.get(row['Id'], '') or ''
                addition = row['Description_Addition']

                if pd.isna(addition) or not str(addition).strip():
                    return existing

                addition = str(addition).strip()

                if existing:
                    # 重複チェック: 同じ日付のログが既にあればスキップ
                    first_line = addition.split('\n')[0] if addition else ''
                    if first_line and first_line in existing:
                        return existing
                    # ★区切りで追記（ユーザー要件）
                    return existing + '\n★\n' + addition
                return addition

            update_df[target_field] = update_df.apply(append_value, axis=1)
        else:
            # 上書きモード
            update_df[target_field] = update_df['Description_Addition']

        return update_df[['Id', target_field]]

    def create_update_job(self, object_name: str) -> str:
        """
        Bulk Update Job を作成

        Args:
            object_name: オブジェクト名

        Returns:
            str: Job ID
        """
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
            raise Exception(f"Job作成失敗: {error_detail}")

        job_id = response.json()['id']
        print(f"  Job作成: {job_id}")
        return job_id

    def upload_data(self, job_id: str, df: pd.DataFrame) -> None:
        """
        CSV データをアップロード

        Args:
            job_id: Job ID
            df: アップロードするデータ
        """
        url = f"{self.instance_url}/services/data/{self.api_version}/jobs/ingest/{job_id}/batches"

        # DataFrameをCSV文字列に変換
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, quoting=csv.QUOTE_ALL)
        csv_data = csv_buffer.getvalue()

        response = self.session.put(
            url,
            headers=self._headers('text/csv'),
            data=csv_data.encode('utf-8')
        )

        if response.status_code not in [200, 201]:
            error_detail = response.text
            raise Exception(f"データアップロード失敗: {error_detail}")

        print(f"  データアップロード完了: {len(df):,} 件")

    def close_job(self, job_id: str) -> None:
        """Job をクローズして処理を開始"""
        url = f"{self.instance_url}/services/data/{self.api_version}/jobs/ingest/{job_id}"

        payload = {'state': 'UploadComplete'}

        response = self.session.patch(
            url,
            headers=self._headers('application/json'),
            json=payload
        )

        if response.status_code not in [200, 201]:
            error_detail = response.text
            raise Exception(f"Job クローズ失敗: {error_detail}")

        print("  Job クローズ（処理開始）")

    def wait_for_job(self, job_id: str, timeout: int = 600) -> dict:
        """
        ジョブ完了を待機

        Returns:
            dict: ジョブ結果情報
        """
        url = f"{self.instance_url}/services/data/{self.api_version}/jobs/ingest/{job_id}"
        start_time = time.time()

        while True:
            response = self.session.get(url, headers=self._headers())
            response.raise_for_status()

            job_info = response.json()
            state = job_info['state']
            processed = job_info.get('numberRecordsProcessed', 0)
            failed = job_info.get('numberRecordsFailed', 0)

            if state == 'JobComplete':
                print(f"  完了: 処理={processed:,} 件, 失敗={failed:,} 件")
                return job_info
            elif state in ['Failed', 'Aborted']:
                error_msg = job_info.get('errorMessage', 'Unknown error')
                raise Exception(f"Job失敗: {state} - {error_msg}")

            # タイムアウトチェック
            if time.time() - start_time > timeout:
                raise Exception(f"タイムアウト: {timeout}秒")

            print(f"  処理中... ({processed:,} 件)")
            time.sleep(5)

    def get_failed_records(self, job_id: str) -> pd.DataFrame:
        """失敗レコードを取得"""
        url = f"{self.instance_url}/services/data/{self.api_version}/jobs/ingest/{job_id}/failedResults"

        response = self.session.get(url, headers=self._headers())

        if response.status_code == 200 and response.content:
            return pd.read_csv(io.StringIO(response.text))
        return pd.DataFrame()

    def update_records(
        self,
        object_name: str,
        update_csv: Path,
        target_field: str,
        append_mode: bool = True,
        dry_run: bool = True
    ) -> dict:
        """
        レコードを一括更新

        Args:
            object_name: オブジェクト名
            update_csv: 更新データCSV（Id, Description_Addition カラム）
            target_field: 更新対象フィールド名
            append_mode: True=追記 / False=上書き
            dry_run: True=実行せず確認のみ

        Returns:
            dict: 実行結果
        """
        print(f"\n{'='*60}")
        print(f"{object_name} 更新処理")
        print(f"{'='*60}")
        print(f"  対象フィールド: {target_field}")
        print(f"  モード: {'追記' if append_mode else '上書き'}")
        print(f"  Dry Run: {dry_run}")

        # データ読み込み
        df = pd.read_csv(update_csv, dtype=str, encoding='utf-8-sig')
        print(f"  更新対象: {len(df):,} 件")

        if dry_run:
            print("\n[DRY RUN] 実行せずに終了します")
            print("  実際に更新するには dry_run=False を指定してください")
            return {
                'object': object_name,
                'field': target_field,
                'total': len(df),
                'dry_run': True
            }

        # 更新データ準備（追記モードの場合は既存値を取得）
        print("\n[STEP 1] 更新データ準備")
        update_df = self.prepare_update_data(df, object_name, target_field, append_mode)

        # Bulk Job 作成
        print("\n[STEP 2] Bulk Job 作成")
        job_id = self.create_update_job(object_name)

        # データアップロード
        print("\n[STEP 3] データアップロード")
        self.upload_data(job_id, update_df)

        # Job クローズ
        print("\n[STEP 4] Job クローズ")
        self.close_job(job_id)

        # 完了待機
        print("\n[STEP 5] 処理完了待機")
        job_info = self.wait_for_job(job_id)

        # 結果確認
        result = {
            'object': object_name,
            'field': target_field,
            'job_id': job_id,
            'total': len(df),
            'processed': job_info.get('numberRecordsProcessed', 0),
            'failed': job_info.get('numberRecordsFailed', 0),
            'dry_run': False
        }

        # 失敗レコードがあれば取得
        if result['failed'] > 0:
            failed_df = self.get_failed_records(job_id)
            failed_path = update_csv.parent / f"failed_{object_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            failed_df.to_csv(failed_path, index=False, encoding='utf-8-sig')
            result['failed_file'] = str(failed_path)
            print(f"\n[WARNING] 失敗レコード: {failed_path}")

        print(f"\n[完了] {object_name}: {result['processed']:,} 件更新")
        return result


def main():
    """テスト実行"""
    import argparse

    parser = argparse.ArgumentParser(description='Salesforce Bulk API 2.0 更新')
    parser.add_argument('--object', required=True, help='オブジェクト名')
    parser.add_argument('--csv', required=True, help='更新データCSVパス')
    parser.add_argument('--field', required=True, help='更新対象フィールド名')
    parser.add_argument('--overwrite', action='store_true', help='上書きモード（デフォルト: 追記）')
    parser.add_argument('--execute', action='store_true', help='実際に実行（デフォルト: dry-run）')

    args = parser.parse_args()

    updater = BulkUpdater()
    updater.authenticate()

    result = updater.update_records(
        object_name=args.object,
        update_csv=Path(args.csv),
        target_field=args.field,
        append_mode=not args.overwrite,
        dry_run=not args.execute
    )

    print("\n" + "=" * 60)
    print("結果サマリ")
    print("=" * 60)
    for k, v in result.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
