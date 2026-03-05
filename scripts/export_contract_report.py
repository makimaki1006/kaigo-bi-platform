"""
契約先レポートエクスポートスクリプト
Salesforce Report API を使用してレポートを取得
"""

import sys
from pathlib import Path
from datetime import datetime
import csv
import json

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import requests
from src.utils.config import sf_config, output_config


# 契約先レポートID
CONTRACT_REPORT_ID = "00Odc000005FHs5EAG"


class ReportExporter:
    """Salesforce Report API クライアント"""

    def __init__(self):
        self.instance_url = sf_config.INSTANCE_URL
        self.api_version = sf_config.API_VERSION
        self.access_token = None

    def authenticate(self) -> str:
        """アクセストークンを取得"""
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
        print("認証成功")
        return self.access_token

    def _get_headers(self) -> dict:
        """認証ヘッダーを取得"""
        if not self.access_token:
            self.authenticate()
        return {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }

    def get_report_metadata(self, report_id: str) -> dict:
        """レポートのメタデータを取得"""
        url = f"{self.instance_url}/services/data/{self.api_version}/analytics/reports/{report_id}/describe"
        response = requests.get(url, headers=self._get_headers())
        response.raise_for_status()
        return response.json()

    def execute_report(self, report_id: str, include_details: bool = True) -> dict:
        """
        レポートを実行して結果を取得（同期）

        Args:
            report_id: レポートID
            include_details: 詳細行を含めるか

        Returns:
            dict: レポート実行結果
        """
        url = f"{self.instance_url}/services/data/{self.api_version}/analytics/reports/{report_id}"

        # レポート実行オプション
        params = {
            "includeDetails": str(include_details).lower()
        }

        response = requests.get(url, headers=self._get_headers(), params=params)
        response.raise_for_status()
        return response.json()

    def execute_report_async(self, report_id: str) -> tuple:
        """
        レポートを非同期実行して結果を取得

        Args:
            report_id: レポートID

        Returns:
            tuple: (インスタンスID, 結果dict or None)
        """
        import time

        url = f"{self.instance_url}/services/data/{self.api_version}/analytics/reports/{report_id}/instances"

        # 非同期実行を開始
        response = requests.post(url, headers=self._get_headers(), json={})
        response.raise_for_status()
        instance_id = response.json()["id"]
        print(f"  非同期レポート開始: {instance_id}")

        # 完了を待機
        status_url = f"{url}/{instance_id}"
        while True:
            status_response = requests.get(status_url, headers=self._get_headers())
            status_response.raise_for_status()
            response_json = status_response.json()

            # ステータスを取得
            status = response_json.get("status")

            # 結果が直接返ってきている場合（レポート結果の構造をチェック）
            if "factMap" in response_json or "reportMetadata" in response_json:
                print(f"  ステータス: Complete")
                return instance_id, response_json

            if status is None:
                print(f"  レスポンス構造: {list(response_json.keys())[:10]}")
                status = "Unknown"

            print(f"  ステータス: {status}")

            if status in ["Success", "Complete"]:
                return instance_id, None
            elif status in ["Error", "Failed"]:
                raise Exception(f"レポート実行失敗: {response_json}")

            time.sleep(2)

    def get_async_report_result(self, report_id: str, instance_id: str) -> dict:
        """
        非同期レポートの結果を取得

        Args:
            report_id: レポートID
            instance_id: インスタンスID

        Returns:
            dict: レポート結果
        """
        url = f"{self.instance_url}/services/data/{self.api_version}/analytics/reports/{report_id}/instances/{instance_id}"
        response = requests.get(url, headers=self._get_headers())
        response.raise_for_status()
        return response.json()

    def export_report_to_csv(self, report_id: str, output_path: Path, use_async: bool = True) -> Path:
        """
        レポートをCSVにエクスポート

        Args:
            report_id: レポートID
            output_path: 出力ファイルパス
            use_async: 非同期実行を使用するか（大量データ対応）

        Returns:
            Path: 保存されたファイルパス
        """
        print(f"レポート {report_id} を取得中...")

        # レポート実行
        if use_async:
            instance_id, result = self.execute_report_async(report_id)
            if result is None:
                result = self.get_async_report_result(report_id, instance_id)
        else:
            result = self.execute_report(report_id, include_details=True)

        # レポート情報
        report_metadata = result.get("reportMetadata", {})
        report_name = report_metadata.get("name", "Unknown")
        print(f"  レポート名: {report_name}")

        # カラム情報を取得
        report_extended_metadata = result.get("reportExtendedMetadata", {})
        detail_column_info = report_extended_metadata.get("detailColumnInfo", {})

        # カラム順序
        detail_columns = report_metadata.get("detailColumns", [])

        # ヘッダー行（表示ラベル）
        headers = []
        api_names = []
        for col in detail_columns:
            col_info = detail_column_info.get(col, {})
            headers.append(col_info.get("label", col))
            api_names.append(col)

        print(f"  カラム数: {len(headers)}")

        # データ行を取得
        fact_map = result.get("factMap", {})
        rows = []

        for key, value in fact_map.items():
            if "rows" in value:
                for row in value["rows"]:
                    data_cells = row.get("dataCells", [])
                    row_data = []
                    for cell in data_cells:
                        # labelがあればそれを使用、なければvalueを使用
                        cell_value = cell.get("label", cell.get("value", ""))
                        row_data.append(cell_value)
                    rows.append(row_data)

        print(f"  レコード数: {len(rows)}")

        # 全データ取得できたか確認
        all_data = result.get("allData", True)
        if not all_data:
            print(f"  警告: データが2000件制限で切り捨てられています。全件取得にはフィルタリングが必要です。")

        # CSVに書き込み
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            # API名をヘッダーとして使用（表示ラベルは2行目にコメントとして）
            writer.writerow(api_names)
            writer.writerows(rows)

        print(f"保存完了: {output_path}")
        return output_path


def export_via_bulk_api(output_dir: Path, timestamp: str) -> Path:
    """
    Bulk API を使用して契約先データを全件取得

    レポートと同等のフィルタ条件を使用:
    - Status__c が「商談中,プロジェクト進行中,深耕対象,過去客」のいずれかを含む OR
    - RelatedAccountFlg__c が「グループ案件進行中,グループ過去案件実績あり」のいずれか
    """
    from scripts.bulk_export import BulkExporter

    print("\nBulk API で契約先データを取得中...")

    # SOQL WHERE句（レポートと同等のフィルタ）
    where_clause = """(
        Status__c LIKE '%商談中%'
        OR Status__c LIKE '%プロジェクト進行中%'
        OR Status__c LIKE '%深耕対象%'
        OR Status__c LIKE '%過去客%'
        OR RelatedAccountFlg__c = 'グループ案件進行中'
        OR RelatedAccountFlg__c = 'グループ過去案件実績あり'
    )"""

    # 必要なフィールド（レポートのカラムに対応）
    fields = [
        "Id",
        "Name",
        "Phone",
        "Email__c",
        "CompanyName__c",
        "CorporateNumber__c",
        "CorporateIdentificationNumber__c",
        "Address__c",
        "PresidentName__c",
        "PresidentTitle__c",
        "Status__c",
        "RelatedAccountFlg__c",
    ]

    exporter = BulkExporter()
    exporter.authenticate()

    output_path = exporter.export_object_bulk(
        "Account",
        output_dir,
        fields=fields,
        where_clause=where_clause,
    )

    # ファイル名を契約先用にリネーム
    new_path = output_dir / f"contract_accounts_{timestamp}.csv"
    output_path.rename(new_path)

    return new_path


def main():
    """メイン処理"""
    print("=" * 60)
    print("契約先レポートエクスポート")
    print("=" * 60)

    # 出力ディレクトリ
    output_dir = output_config.ensure_dir()
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    # 方法1: Report API（2000件制限あり）
    # 方法2: Bulk API（全件取得可能）

    use_bulk_api = True  # 全件取得するには True

    if use_bulk_api:
        try:
            output_path = export_via_bulk_api(output_dir, timestamp)
            print(f"\n保存完了: {output_path}")
        except Exception as e:
            print(f"\nBulk APIエラー: {e}")
            print("Report API にフォールバックします...")
            use_bulk_api = False

    if not use_bulk_api:
        exporter = ReportExporter()
        exporter.authenticate()

        try:
            # メタデータ取得
            print("\nレポートメタデータを取得中...")
            metadata = exporter.get_report_metadata(CONTRACT_REPORT_ID)
            report_meta = metadata.get('reportMetadata', {})
            print(f"  レポート名: {report_meta.get('name', 'Unknown')}")
            print(f"  レポートタイプ: {report_meta.get('reportType', {}).get('label', 'Unknown')}")

            # レポートフィルタを確認
            filters = report_meta.get('reportFilters', [])
            if filters:
                print(f"  フィルタ条件:")
                for f in filters:
                    print(f"    - {f.get('column', 'Unknown')}: {f.get('operator', '')} {f.get('value', '')}")

            # レポートをCSVにエクスポート（同期モード: 2000件制限あり）
            output_path = output_dir / f"contract_accounts_{timestamp}.csv"
            exporter.export_report_to_csv(CONTRACT_REPORT_ID, output_path, use_async=False)

        except requests.exceptions.HTTPError as e:
            print(f"\nAPIエラー: {e}")
            print(f"レスポンス: {e.response.text if hasattr(e, 'response') else 'N/A'}")
            sys.exit(1)
        except Exception as e:
            print(f"\nエラー: {e}")
            sys.exit(1)

    print("\n" + "=" * 60)
    print("エクスポート完了")
    print("=" * 60)


if __name__ == "__main__":
    main()
