"""
Salesforce同期サービス
突合結果をSalesforceに同期（新規作成・更新）する
"""

import time
from typing import Optional
from datetime import datetime

import requests
from loguru import logger

from src.models.lead import LeadData, MatchResult, MatchStatus
from src.utils.config import sf_config


class SyncService:
    """
    Salesforce同期サービス

    突合結果に基づいてSalesforceのリードを作成・更新する
    """

    def __init__(
        self,
        batch_size: int = 200,
        dry_run: bool = False,
    ):
        """
        初期化

        Args:
            batch_size: バッチサイズ
            dry_run: テスト実行モード（実際にはSalesforceを更新しない）
        """
        self.batch_size = batch_size
        self.dry_run = dry_run
        self.access_token: Optional[str] = None
        self.instance_url = sf_config.INSTANCE_URL
        self.api_version = sf_config.API_VERSION

    def authenticate(self) -> str:
        """
        Salesforce認証を行う

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
        logger.info("✅ Salesforce認証成功")
        return self.access_token

    def _get_headers(self) -> dict:
        """認証ヘッダーを取得"""
        if not self.access_token:
            self.authenticate()
        return {'Authorization': f'Bearer {self.access_token}'}

    def sync_leads(
        self,
        match_results: list[MatchResult],
        update_existing: bool = True,
        create_new: bool = True,
    ) -> dict:
        """
        突合結果をSalesforceに同期する

        Args:
            match_results: 突合結果のリスト
            update_existing: 既存リードを更新するか
            create_new: 新規リードを作成するか

        Returns:
            dict: 同期結果の統計情報
        """
        logger.info(f"Salesforce同期開始 (dry_run={self.dry_run})")

        results = {
            "created": 0,
            "updated": 0,
            "skipped": 0,
            "errors": [],
            "created_ids": [],
            "updated_ids": [],
        }

        # 既存リードの更新
        if update_existing:
            matched_results = [
                r for r in match_results
                if r.status == MatchStatus.MATCHED
            ]
            if matched_results:
                update_result = self._update_existing_leads(matched_results)
                results["updated"] = update_result["success"]
                results["updated_ids"] = update_result["ids"]
                results["errors"].extend(update_result["errors"])

        # 新規リードの作成
        if create_new:
            new_results = [
                r for r in match_results
                if r.status == MatchStatus.NEW
            ]
            if new_results:
                create_result = self._create_new_leads(new_results)
                results["created"] = create_result["success"]
                results["created_ids"] = create_result["ids"]
                results["errors"].extend(create_result["errors"])

        # スキップ
        results["skipped"] = len(match_results) - results["created"] - results["updated"]

        logger.info(f"✅ 同期完了:")
        logger.info(f"   - 作成: {results['created']}件")
        logger.info(f"   - 更新: {results['updated']}件")
        logger.info(f"   - スキップ: {results['skipped']}件")
        if results["errors"]:
            logger.warning(f"   - エラー: {len(results['errors'])}件")

        return results

    def _update_existing_leads(
        self,
        match_results: list[MatchResult]
    ) -> dict:
        """
        既存リードを更新する

        Args:
            match_results: 一致した結果

        Returns:
            dict: 更新結果
        """
        logger.info(f"既存リード更新: {len(match_results)}件")

        result = {
            "success": 0,
            "ids": [],
            "errors": [],
        }

        if self.dry_run:
            logger.info("  [DRY RUN] 実際の更新はスキップ")
            result["success"] = len(match_results)
            result["ids"] = [r.matched_sf_id for r in match_results]
            return result

        # バッチ処理
        for i in range(0, len(match_results), self.batch_size):
            batch = match_results[i:i + self.batch_size]
            batch_result = self._update_batch(batch)
            result["success"] += batch_result["success"]
            result["ids"].extend(batch_result["ids"])
            result["errors"].extend(batch_result["errors"])

        return result

    def _update_batch(self, batch: list[MatchResult]) -> dict:
        """
        バッチで更新する

        Args:
            batch: 更新対象のバッチ

        Returns:
            dict: 更新結果
        """
        result = {
            "success": 0,
            "ids": [],
            "errors": [],
        }

        for match_result in batch:
            try:
                sf_id = match_result.matched_sf_id
                csv_data = match_result.csv_data

                # 更新データを構築
                update_data = self._build_update_data(csv_data, match_result)

                if not update_data:
                    continue

                # Salesforce API呼び出し
                url = f"{self.instance_url}/services/data/{self.api_version}/sobjects/Lead/{sf_id}"
                headers = {
                    **self._get_headers(),
                    'Content-Type': 'application/json'
                }

                response = requests.patch(url, headers=headers, json=update_data)

                if response.status_code == 204:
                    result["success"] += 1
                    result["ids"].append(sf_id)
                else:
                    error_msg = f"更新失敗 (ID: {sf_id}): {response.text}"
                    result["errors"].append(error_msg)
                    logger.warning(f"  {error_msg}")

            except Exception as e:
                error_msg = f"更新エラー: {str(e)}"
                result["errors"].append(error_msg)
                logger.error(f"  {error_msg}")

        return result

    def _build_update_data(
        self,
        csv_data: LeadData,
        match_result: MatchResult
    ) -> dict:
        """
        更新データを構築する（空でないフィールドのみ）

        Args:
            csv_data: CSVデータ
            match_result: 突合結果

        Returns:
            dict: 更新データ
        """
        update_data = {}

        # 空でないフィールドのみ更新
        field_mapping = {
            "phone": "Phone",
            "address": "Street",
            "website": "Website",
            "industry": "Industry",
            "email": "Email",
            "title": "Title",
        }

        for csv_field, sf_field in field_mapping.items():
            value = getattr(csv_data, csv_field, None)
            if value:
                # 既存データと異なる場合のみ更新
                sf_data = match_result.matched_sf_data
                if sf_data:
                    existing = getattr(sf_data, csv_field.lower(), None)
                    if existing != value:
                        update_data[sf_field] = value

        return update_data

    def _create_new_leads(
        self,
        match_results: list[MatchResult]
    ) -> dict:
        """
        新規リードを作成する

        Args:
            match_results: 新規の結果

        Returns:
            dict: 作成結果
        """
        logger.info(f"新規リード作成: {len(match_results)}件")

        result = {
            "success": 0,
            "ids": [],
            "errors": [],
        }

        if self.dry_run:
            logger.info("  [DRY RUN] 実際の作成はスキップ")
            result["success"] = len(match_results)
            return result

        # バッチ処理
        for i in range(0, len(match_results), self.batch_size):
            batch = match_results[i:i + self.batch_size]
            batch_result = self._create_batch(batch)
            result["success"] += batch_result["success"]
            result["ids"].extend(batch_result["ids"])
            result["errors"].extend(batch_result["errors"])

        return result

    def _create_batch(self, batch: list[MatchResult]) -> dict:
        """
        バッチで作成する

        Args:
            batch: 作成対象のバッチ

        Returns:
            dict: 作成結果
        """
        result = {
            "success": 0,
            "ids": [],
            "errors": [],
        }

        for match_result in batch:
            try:
                csv_data = match_result.csv_data

                # 作成データを構築
                create_data = self._build_create_data(csv_data)

                # Salesforce API呼び出し
                url = f"{self.instance_url}/services/data/{self.api_version}/sobjects/Lead"
                headers = {
                    **self._get_headers(),
                    'Content-Type': 'application/json'
                }

                response = requests.post(url, headers=headers, json=create_data)

                if response.status_code == 201:
                    new_id = response.json().get('id')
                    result["success"] += 1
                    result["ids"].append(new_id)
                else:
                    error_msg = f"作成失敗 ({csv_data.company_name}): {response.text}"
                    result["errors"].append(error_msg)
                    logger.warning(f"  {error_msg}")

            except Exception as e:
                error_msg = f"作成エラー: {str(e)}"
                result["errors"].append(error_msg)
                logger.error(f"  {error_msg}")

        return result

    def _build_create_data(self, csv_data: LeadData) -> dict:
        """
        新規作成データを構築する

        Args:
            csv_data: CSVデータ

        Returns:
            dict: 作成データ
        """
        # 必須フィールド
        create_data = {
            "Company": csv_data.company_name,
            "LastName": csv_data.last_name or csv_data.company_name[:40],  # 姓がない場合は会社名
        }

        # オプションフィールド
        optional_mapping = {
            "phone": "Phone",
            "address": "Street",
            "website": "Website",
            "industry": "Industry",
            "first_name": "FirstName",
            "email": "Email",
            "title": "Title",
        }

        for csv_field, sf_field in optional_mapping.items():
            value = getattr(csv_data, csv_field, None)
            if value:
                create_data[sf_field] = value

        # リードソース
        create_data["LeadSource"] = "CSV Import"

        # 説明欄にメタ情報を追加
        description_parts = []
        if csv_data.source_url:
            description_parts.append(f"取得元: {csv_data.source_url}")
        if csv_data.scraped_at:
            description_parts.append(f"取得日: {csv_data.scraped_at.strftime('%Y-%m-%d')}")
        if csv_data.notes:
            description_parts.append(f"備考: {csv_data.notes}")

        if description_parts:
            create_data["Description"] = "\n".join(description_parts)

        return create_data

    def get_sf_leads(
        self,
        limit: Optional[int] = None,
        fields: Optional[list[str]] = None,
    ) -> list[dict]:
        """
        SalesforceからリードデータをSOQLで取得する

        Args:
            limit: 取得件数制限
            fields: 取得フィールド

        Returns:
            list[dict]: リードデータのリスト
        """
        if fields is None:
            fields = [
                "Id", "Company", "Phone", "Street", "City", "State",
                "PostalCode", "Country", "Website", "Industry",
                "LastName", "FirstName", "Email", "Title", "Status",
                "CreatedDate", "LastModifiedDate"
            ]

        soql = f"SELECT {', '.join(fields)} FROM Lead"

        if limit:
            soql += f" LIMIT {limit}"

        url = f"{self.instance_url}/services/data/{self.api_version}/query"
        headers = self._get_headers()
        params = {"q": soql}

        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()

        result = response.json()
        records = result.get("records", [])

        logger.info(f"Salesforceから{len(records)}件のリードを取得")

        return records

    def export_results(
        self,
        match_results: list[MatchResult],
        output_path: str,
    ) -> str:
        """
        突合結果をCSVにエクスポートする

        Args:
            match_results: 突合結果
            output_path: 出力パス

        Returns:
            str: 出力パス
        """
        import csv
        from pathlib import Path

        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)

            # ヘッダー
            writer.writerow([
                "ステータス", "会社名", "電話番号", "メール",
                "一致SF_ID", "一致キー", "スコア", "備考"
            ])

            # データ
            for r in match_results:
                writer.writerow([
                    r.status.value,
                    r.csv_data.company_name,
                    r.csv_data.phone or "",
                    r.csv_data.email or "",
                    r.matched_sf_id or "",
                    r.match_key or "",
                    f"{r.match_score:.2f}",
                    r.notes or "",
                ])

        logger.info(f"✅ 結果エクスポート: {output_path}")
        return str(output_path)
