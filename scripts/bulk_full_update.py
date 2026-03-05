"""
Salesforce Bulk API 2.0 全フィールド一括更新スクリプト
- 全フィールド一括更新
- メモフィールドは★区切りで追記
- 更新前にバックアップCSV作成
- Bulk API 2.0 Query使用でAPI使用量削減
"""

import sys
import time
import io
import csv
from pathlib import Path
from datetime import datetime
from typing import Optional

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.utils.config import sf_config


class BulkFullUpdater:
    """
    Salesforce Bulk API 2.0 全フィールド一括更新

    - 通常フィールド: 上書き更新
    - メモフィールド（*_Addition）: ★区切りで追記
    - 更新前バックアップ機能付き
    """

    # メモフィールドのマッピング（Addition列 → 実際のSFフィールド）
    MEMO_FIELD_MAP = {
        'Publish_ImportText_Addition': 'Publish_ImportText__c',
        'Description_Addition': 'Description',
        'LeadSourceMemo_Addition': 'LeadSourceMemo__c',
    }

    # 上書きモードのフィールド（追記ではなく最新値で置換）
    OVERWRITE_MEMO_FIELDS = {'LeadSourceMemo__c'}

    def __init__(self):
        self.instance_url = sf_config.INSTANCE_URL
        self.api_version = sf_config.API_VERSION
        self.access_token = None

        # セッション設定
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
        self.session.mount("https://", adapter)

    def authenticate(self) -> str:
        """認証"""
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
        headers = {'Authorization': f'Bearer {self.access_token}'}
        if content_type:
            headers['Content-Type'] = content_type
        return headers

    # ========================================
    # Bulk API 2.0 Query メソッド
    # ========================================

    def create_query_job(self, soql: str) -> str:
        """
        Bulk API 2.0 Query Job作成

        Args:
            soql: SOQLクエリ文字列

        Returns:
            str: Job ID
        """
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

        job_id = response.json()['id']
        return job_id

    def wait_for_query_job(self, job_id: str, timeout: int = 600) -> dict:
        """
        Query Job完了待機

        Returns:
            dict: ジョブ情報
        """
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
        """
        Query Job結果をDataFrameで取得

        Args:
            job_id: Query Job ID

        Returns:
            pd.DataFrame: クエリ結果
        """
        all_data = []
        locator = None

        while True:
            url = f"{self.instance_url}/services/data/{self.api_version}/jobs/query/{job_id}/results"
            params = {}
            if locator:
                params['locator'] = locator

            response = self.session.get(url, headers=self._headers(), params=params)
            response.raise_for_status()

            # CSVデータをパース
            if response.text:
                df_chunk = pd.read_csv(io.StringIO(response.text), dtype=str)
                all_data.append(df_chunk)

            # 次のページがあるかチェック
            locator = response.headers.get('Sforce-Locator')
            if not locator or locator == 'null':
                break

        if all_data:
            return pd.concat(all_data, ignore_index=True)
        return pd.DataFrame()

    def bulk_query(self, soql: str, description: str = "") -> pd.DataFrame:
        """
        Bulk API 2.0 Queryを実行（統合メソッド）

        Args:
            soql: SOQLクエリ
            description: ログ用の説明

        Returns:
            pd.DataFrame: クエリ結果
        """
        if description:
            print(f"  [Bulk Query] {description}")

        # Job作成
        job_id = self.create_query_job(soql)
        print(f"    Job作成: {job_id}")

        # 完了待機
        print("    処理中...", end="", flush=True)
        job_info = self.wait_for_query_job(job_id)
        records = job_info.get('numberRecordsProcessed', 0)
        print(f" 完了 ({records:,} 件)")

        # 結果取得
        df = self.get_query_results(job_id)
        return df

    # ========================================
    # バックアップ・既存値取得（Bulk Query使用）
    # ========================================

    def backup_records(
        self,
        object_name: str,
        ids: list[str],
        fields: list[str],
        backup_dir: Path
    ) -> Path:
        """
        更新対象レコードの現在値をバックアップ（Bulk API 2.0 Query使用）

        Args:
            object_name: オブジェクト名
            ids: レコードIDリスト
            fields: バックアップ対象フィールド
            backup_dir: バックアップ保存先ディレクトリ

        Returns:
            Path: バックアップファイルパス
        """
        print(f"\n[バックアップ] {object_name} ({len(ids):,} 件)")

        # フィールドリストにIdを追加
        query_fields = ['Id'] + [f for f in fields if f != 'Id']
        field_list = ', '.join(query_fields)

        # Bulk API 2.0 Queryで全件取得（WHERE条件でID指定は非効率なので全件取得）
        # Lead: IsConverted=false で絞り込み、Account/Contact: 全件取得
        if object_name == 'Lead':
            soql = f"SELECT {field_list} FROM {object_name} WHERE IsConverted = false"
        else:
            soql = f"SELECT {field_list} FROM {object_name}"

        df_all = self.bulk_query(soql, f"{object_name} バックアップ取得")

        # 更新対象IDでフィルタリング
        id_set = set(ids)
        df_backup = df_all[df_all['Id'].isin(id_set)].copy()

        print(f"    フィルタ後: {len(df_backup):,} 件 (全体: {len(df_all):,} 件)")

        # バックアップCSV保存
        backup_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_path = backup_dir / f"backup_{object_name}_{timestamp}.csv"

        df_backup.to_csv(backup_path, index=False, encoding='utf-8-sig')

        print(f"  バックアップ完了: {backup_path}")
        print(f"  レコード数: {len(df_backup):,} 件")

        return backup_path

    def get_existing_memo_values(
        self,
        object_name: str,
        ids: list[str],
        memo_fields: list[str]
    ) -> dict[str, dict[str, str]]:
        """
        メモフィールドの既存値を取得（Bulk API 2.0 Query使用）

        Returns:
            dict[str, dict[str, str]]: {Id: {field: value}} のマッピング
        """
        if not memo_fields:
            return {}

        print(f"\n[メモ既存値取得] {', '.join(memo_fields)}")

        # Bulk API 2.0 Queryで取得
        field_list = 'Id, ' + ', '.join(memo_fields)

        # Lead: IsConverted=false で絞り込み、Account/Contact: 全件取得
        if object_name == 'Lead':
            soql = f"SELECT {field_list} FROM {object_name} WHERE IsConverted = false"
        else:
            soql = f"SELECT {field_list} FROM {object_name}"

        df_all = self.bulk_query(soql, f"{object_name} メモ既存値取得")

        # 更新対象IDでフィルタリング
        id_set = set(ids)
        df_filtered = df_all[df_all['Id'].isin(id_set)]

        # 辞書形式に変換
        result = {id_: {f: '' for f in memo_fields} for id_ in ids}

        for _, row in df_filtered.iterrows():
            rec_id = row['Id']
            if rec_id in result:
                for field in memo_fields:
                    val = row.get(field, '') or ''
                    if pd.notna(val):
                        result[rec_id][field] = str(val).strip()

        print(f"    フィルタ後: {len(df_filtered):,} 件 (全体: {len(df_all):,} 件)")
        return result

    # 整数フィールド（.0 を除去する対象）
    INTEGER_FIELDS = [
        'NumberOfEmployees',
        'Hellowork_NumberOfRecruitment__c',
        'Hellowork_NumberOfEmployee_Office__c',
    ]

    def prepare_update_data(
        self,
        df: pd.DataFrame,
        object_name: str,
        existing_memo: dict[str, dict[str, str]]
    ) -> pd.DataFrame:
        """
        更新データを準備

        - OVERWRITE_MEMO_FIELDS: 古い値を捨てて最新値で上書き
        - それ以外のメモ: ★区切りで追記（最新が上）

        Args:
            df: 更新データ
            object_name: オブジェクト名
            existing_memo: 既存メモ値

        Returns:
            pd.DataFrame: Bulk API用データ
        """
        # メモフィールドを特定
        memo_additions = [col for col in df.columns if col in self.MEMO_FIELD_MAP]

        # 結果用DataFrame
        result_df = df.copy()

        # 整数フィールドの .0 を除去
        for field in self.INTEGER_FIELDS:
            if field in result_df.columns:
                result_df[field] = result_df[field].apply(
                    lambda x: str(int(float(x))) if pd.notna(x) and str(x).strip() else ''
                )

        # メモフィールドの処理
        for addition_col in memo_additions:
            actual_field = self.MEMO_FIELD_MAP[addition_col]
            is_overwrite = actual_field in self.OVERWRITE_MEMO_FIELDS

            def process_memo(row, field=actual_field, add_col=addition_col, overwrite=is_overwrite):
                rec_id = row['Id']
                existing = existing_memo.get(rec_id, {}).get(field, '') or ''
                addition = row.get(add_col, '') or ''

                if pd.isna(addition) or not str(addition).strip():
                    return existing

                addition = str(addition).strip()

                # 上書きモード: 古い値を捨てて最新値のみ
                if overwrite:
                    return addition

                # 追記モード: ★区切りで追記（最新が上）
                if existing:
                    first_line = addition.split('\n')[0] if addition else ''
                    if first_line and first_line in existing:
                        return existing
                    return addition + '\n★\n' + existing
                return addition

            result_df[actual_field] = result_df.apply(process_memo, axis=1)
            # Addition列は削除
            result_df = result_df.drop(columns=[addition_col])

        return result_df

    def create_update_job(self, object_name: str) -> str:
        """Bulk Update Job作成"""
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
        """CSVデータアップロード（1ジョブ1PUT）"""
        url = f"{self.instance_url}/services/data/{self.api_version}/jobs/ingest/{job_id}/batches"

        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, quoting=csv.QUOTE_ALL)
        csv_data = csv_buffer.getvalue().encode('utf-8')

        response = self.session.put(
            url,
            headers=self._headers('text/csv'),
            data=csv_data
        )
        if response.status_code not in [200, 201]:
            raise Exception(f"データアップロード失敗: {response.text}")
        print(f"  データアップロード完了: {len(df):,} 件 ({len(csv_data):,} bytes)")

    def close_job(self, job_id: str) -> None:
        """Jobクローズ"""
        url = f"{self.instance_url}/services/data/{self.api_version}/jobs/ingest/{job_id}"

        response = self.session.patch(
            url,
            headers=self._headers('application/json'),
            json={'state': 'UploadComplete'}
        )

        if response.status_code not in [200, 201]:
            raise Exception(f"Jobクローズ失敗: {response.text}")

        print("  Job クローズ（処理開始）")

    def wait_for_job(self, job_id: str, timeout: int = 600) -> dict:
        """ジョブ完了待機"""
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

            if time.time() - start_time > timeout:
                raise Exception(f"タイムアウト: {timeout}秒")

            print(f"  処理中... ({processed:,} 件)")
            time.sleep(5)

    def get_failed_records(self, job_id: str) -> pd.DataFrame:
        """失敗レコード取得"""
        url = f"{self.instance_url}/services/data/{self.api_version}/jobs/ingest/{job_id}/failedResults"

        response = self.session.get(url, headers=self._headers())

        if response.status_code == 200 and response.content:
            return pd.read_csv(io.StringIO(response.text))
        return pd.DataFrame()

    def update_records(
        self,
        object_name: str,
        update_csv: Path,
        backup_dir: Path = None,
        dry_run: bool = True
    ) -> dict:
        """
        全フィールド一括更新

        Args:
            object_name: オブジェクト名
            update_csv: 更新データCSVパス
            backup_dir: バックアップ保存先（Noneの場合はupdate_csvと同じディレクトリ）
            dry_run: True=実行せず確認のみ

        Returns:
            dict: 実行結果
        """
        print(f"\n{'='*60}")
        print(f"{object_name} 全フィールド一括更新")
        print(f"{'='*60}")
        print(f"  入力CSV: {update_csv}")
        print(f"  Dry Run: {dry_run}")

        # データ読み込み
        df = pd.read_csv(update_csv, dtype=str, encoding='utf-8-sig')
        print(f"  更新対象: {len(df):,} 件")

        # カラム確認
        memo_additions = [col for col in df.columns if col in self.MEMO_FIELD_MAP]
        regular_fields = [col for col in df.columns if col not in self.MEMO_FIELD_MAP and col != 'Id']

        print(f"\n  通常フィールド: {len(regular_fields)} 個")
        print(f"  メモフィールド（追記）: {len(memo_additions)} 個")
        for col in memo_additions:
            print(f"    - {col} → {self.MEMO_FIELD_MAP[col]}")

        if dry_run:
            print("\n[DRY RUN] 実行せずに終了します")
            print("  実際に更新するには dry_run=False を指定してください")
            return {
                'object': object_name,
                'total': len(df),
                'regular_fields': len(regular_fields),
                'memo_fields': len(memo_additions),
                'dry_run': True
            }

        # バックアップ
        backup_dir = backup_dir or update_csv.parent
        ids = df['Id'].tolist()

        # バックアップ対象フィールド（更新対象 + メモの実フィールド）
        backup_fields = regular_fields.copy()
        for col in memo_additions:
            actual_field = self.MEMO_FIELD_MAP[col]
            if actual_field not in backup_fields:
                backup_fields.append(actual_field)

        backup_path = self.backup_records(object_name, ids, backup_fields, backup_dir)

        # メモフィールドの既存値取得
        memo_actual_fields = [self.MEMO_FIELD_MAP[col] for col in memo_additions]
        existing_memo = self.get_existing_memo_values(object_name, ids, memo_actual_fields)

        # 更新データ準備（メモ追記処理）
        print("\n[STEP 1] 更新データ準備")
        update_df = self.prepare_update_data(df, object_name, existing_memo)
        print(f"  更新カラム: {list(update_df.columns)}")

        # データサイズを確認してチャンク分割判定
        csv_buffer = io.StringIO()
        update_df.to_csv(csv_buffer, index=False, quoting=csv.QUOTE_ALL)
        total_bytes = len(csv_buffer.getvalue().encode('utf-8'))
        max_bytes = 90_000_000  # 90MB（余裕を持たせる）

        if total_bytes <= max_bytes:
            chunks = [update_df]
        else:
            # 平均行サイズから初期チャンクサイズを推定
            avg_row_bytes = total_bytes / len(update_df)
            initial_chunk_size = max(1, int(max_bytes / avg_row_bytes * 0.85))  # 安全マージン15%
            chunks = []
            for i in range(0, len(update_df), initial_chunk_size):
                chunk = update_df.iloc[i:i + initial_chunk_size]
                # 実際のCSVサイズを検証
                buf = io.StringIO()
                chunk.to_csv(buf, index=False, quoting=csv.QUOTE_ALL)
                chunk_bytes = len(buf.getvalue().encode('utf-8'))
                if chunk_bytes > max_bytes and len(chunk) > 1:
                    # サイズ超過の場合、さらに分割
                    half = len(chunk) // 2
                    chunks.append(chunk.iloc[:half])
                    chunks.append(chunk.iloc[half:])
                else:
                    chunks.append(chunk)
            print(f"  データサイズ {total_bytes:,} bytes → {len(chunks)} チャンクに分割")

        total_processed = 0
        total_failed = 0
        job_ids = []

        for chunk_idx, chunk_df in enumerate(chunks):
            chunk_label = f" (チャンク {chunk_idx+1}/{len(chunks)})" if len(chunks) > 1 else ""

            # Bulk Job作成
            print(f"\n[STEP 2] Bulk Job 作成{chunk_label}")
            job_id = self.create_update_job(object_name)
            job_ids.append(job_id)

            # データアップロード
            print(f"\n[STEP 3] データアップロード{chunk_label}")
            self.upload_data(job_id, chunk_df)

            # Jobクローズ
            print(f"\n[STEP 4] Job クローズ{chunk_label}")
            self.close_job(job_id)

            # 完了待機
            print(f"\n[STEP 5] 処理完了待機{chunk_label}")
            job_info = self.wait_for_job(job_id)

            processed = int(job_info.get('numberRecordsProcessed', 0))
            failed = int(job_info.get('numberRecordsFailed', 0))
            total_processed += processed
            total_failed += failed

            print(f"  処理: {processed:,} 件, 失敗: {failed:,} 件")

            # 失敗レコードがあれば保存
            if failed > 0:
                failed_df = self.get_failed_records(job_id)
                failed_path = backup_dir / f"failed_{object_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_chunk{chunk_idx+1}.csv"
                failed_df.to_csv(failed_path, index=False, encoding='utf-8-sig')
                print(f"  [WARNING] 失敗レコード: {failed_path}")

        # 結果
        result = {
            'object': object_name,
            'job_id': ','.join(job_ids),
            'total': len(df),
            'processed': total_processed,
            'failed': total_failed,
            'backup_file': str(backup_path),
            'dry_run': False
        }

        print(f"\n[完了] {object_name}: {total_processed:,} 件更新 (失敗: {total_failed:,} 件)")
        return result


def main():
    """メイン処理"""
    import argparse

    parser = argparse.ArgumentParser(description='Salesforce 全フィールド一括更新')
    parser.add_argument('--object', required=True, help='オブジェクト名')
    parser.add_argument('--csv', required=True, help='更新データCSVパス')
    parser.add_argument('--backup-dir', help='バックアップ保存先（デフォルト: CSVと同じディレクトリ）')
    parser.add_argument('--execute', action='store_true', help='実際に実行（デフォルト: dry-run）')

    args = parser.parse_args()

    updater = BulkFullUpdater()
    updater.authenticate()

    backup_dir = Path(args.backup_dir) if args.backup_dir else None

    result = updater.update_records(
        object_name=args.object,
        update_csv=Path(args.csv),
        backup_dir=backup_dir,
        dry_run=not args.execute
    )

    print("\n" + "=" * 60)
    print("結果サマリ")
    print("=" * 60)
    for k, v in result.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
