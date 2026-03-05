"""
CSVインポーター
スクレイピング済みCSVファイルをLeadDataモデルに変換する
"""

import csv
from pathlib import Path
from typing import Optional
from datetime import datetime

import pandas as pd
from loguru import logger

from src.models.lead import LeadData


# デフォルトのカラムマッピング
# CSVのカラム名 → LeadDataのフィールド名
DEFAULT_COLUMN_MAPPING = {
    # 会社情報
    "会社名": "company_name",
    "company_name": "company_name",
    "Company": "company_name",
    "企業名": "company_name",

    "電話番号": "phone",
    "phone": "phone",
    "Phone": "phone",
    "TEL": "phone",
    "tel": "phone",

    "住所": "address",
    "address": "address",
    "Address": "address",
    "所在地": "address",

    "URL": "website",
    "url": "website",
    "Website": "website",
    "website": "website",
    "ホームページ": "website",

    "業種": "industry",
    "industry": "industry",
    "Industry": "industry",

    # 担当者情報
    "姓": "last_name",
    "last_name": "last_name",
    "LastName": "last_name",
    "氏名": "last_name",  # 氏名は姓として扱う

    "名": "first_name",
    "first_name": "first_name",
    "FirstName": "first_name",

    "メールアドレス": "email",
    "email": "email",
    "Email": "email",
    "E-mail": "email",
    "mail": "email",

    "役職": "title",
    "title": "title",
    "Title": "title",

    "部署": "department",
    "department": "department",
    "Department": "department",

    # メタ情報
    "取得元URL": "source_url",
    "source_url": "source_url",
    "ソースURL": "source_url",

    "取得日時": "scraped_at",
    "scraped_at": "scraped_at",

    "備考": "notes",
    "notes": "notes",
    "Notes": "notes",
}


class CSVImporter:
    """
    CSVインポーターサービス

    スクレイピング済みCSVファイルをLeadDataのリストに変換する
    """

    def __init__(
        self,
        column_mapping: Optional[dict[str, str]] = None,
        encoding: str = "utf-8-sig",
    ):
        """
        初期化

        Args:
            column_mapping: カスタムカラムマッピング（CSVカラム名 → LeadDataフィールド名）
            encoding: CSVファイルのエンコーディング
        """
        self.column_mapping = {
            **DEFAULT_COLUMN_MAPPING,
            **(column_mapping or {})
        }
        self.encoding = encoding

    def import_csv(self, file_path: str | Path) -> list[LeadData]:
        """
        CSVファイルをインポートしてLeadDataのリストを返す

        Args:
            file_path: CSVファイルパス

        Returns:
            list[LeadData]: インポートしたリードデータのリスト
        """
        file_path = Path(file_path)

        if not file_path.exists():
            raise FileNotFoundError(f"CSVファイルが見つかりません: {file_path}")

        logger.info(f"CSVインポート開始: {file_path}")

        # pandasでCSV読み込み
        df = pd.read_csv(
            file_path,
            encoding=self.encoding,
            dtype=str,  # 全て文字列として読み込み
            na_values=["", "NA", "N/A", "null", "NULL", "None"],
        )

        logger.info(f"  - 読み込み件数: {len(df)}")
        logger.info(f"  - カラム: {list(df.columns)}")

        # カラムマッピングを適用
        mapped_df = self._apply_column_mapping(df)

        # LeadDataに変換
        leads = self._convert_to_leads(mapped_df)

        logger.info(f"✅ インポート完了: {len(leads)}件")
        return leads

    def _apply_column_mapping(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        カラムマッピングを適用する

        Args:
            df: 元のDataFrame

        Returns:
            pd.DataFrame: マッピング適用後のDataFrame
        """
        rename_map = {}

        for csv_col in df.columns:
            # マッピングを探す
            if csv_col in self.column_mapping:
                rename_map[csv_col] = self.column_mapping[csv_col]
            else:
                # スペースや全角を正規化して再検索
                normalized_col = csv_col.strip().replace("　", " ")
                if normalized_col in self.column_mapping:
                    rename_map[csv_col] = self.column_mapping[normalized_col]

        # マッピングされなかったカラムをログ出力
        unmapped = [c for c in df.columns if c not in rename_map]
        if unmapped:
            logger.warning(f"  - マッピングされなかったカラム: {unmapped}")

        # カラム名を変更
        if rename_map:
            df = df.rename(columns=rename_map)

        return df

    def _convert_to_leads(self, df: pd.DataFrame) -> list[LeadData]:
        """
        DataFrameをLeadDataのリストに変換する

        Args:
            df: マッピング適用済みDataFrame

        Returns:
            list[LeadData]: LeadDataのリスト
        """
        leads = []
        errors = []

        for idx, row in df.iterrows():
            try:
                # 必須フィールドのチェック
                company_name = self._get_value(row, "company_name")
                if not company_name:
                    errors.append(f"行{idx + 2}: 会社名が空です")
                    continue

                # LeadDataを作成
                lead = LeadData(
                    # 会社情報
                    company_name=company_name,
                    phone=self._get_value(row, "phone"),
                    address=self._get_value(row, "address"),
                    website=self._get_value(row, "website"),
                    industry=self._get_value(row, "industry"),
                    # 担当者情報
                    last_name=self._get_value(row, "last_name"),
                    first_name=self._get_value(row, "first_name"),
                    email=self._get_value(row, "email"),
                    title=self._get_value(row, "title"),
                    department=self._get_value(row, "department"),
                    # メタ情報
                    source_url=self._get_value(row, "source_url"),
                    scraped_at=self._parse_datetime(
                        self._get_value(row, "scraped_at")
                    ),
                    notes=self._get_value(row, "notes"),
                )
                leads.append(lead)

            except Exception as e:
                errors.append(f"行{idx + 2}: {str(e)}")

        # エラーがあればログ出力
        if errors:
            logger.warning(f"  - 変換エラー: {len(errors)}件")
            for err in errors[:10]:  # 最初の10件のみ表示
                logger.warning(f"    {err}")
            if len(errors) > 10:
                logger.warning(f"    ... 他{len(errors) - 10}件")

        return leads

    def _get_value(self, row: pd.Series, field: str) -> Optional[str]:
        """
        行から値を取得（NaNはNoneに変換）

        Args:
            row: DataFrameの行
            field: フィールド名

        Returns:
            Optional[str]: 値（NaNの場合はNone）
        """
        if field not in row.index:
            return None

        value = row[field]
        if pd.isna(value):
            return None

        # 文字列に変換して空白をトリム
        value = str(value).strip()
        return value if value else None

    def _parse_datetime(self, value: Optional[str]) -> Optional[datetime]:
        """
        日時文字列をdatetimeに変換

        Args:
            value: 日時文字列

        Returns:
            Optional[datetime]: datetimeオブジェクト
        """
        if not value:
            return None

        # 複数のフォーマットを試す
        formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y/%m/%d %H:%M:%S",
            "%Y-%m-%d",
            "%Y/%m/%d",
        ]

        for fmt in formats:
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue

        logger.warning(f"日時パースエラー: {value}")
        return None

    def preview_csv(
        self,
        file_path: str | Path,
        rows: int = 5
    ) -> dict:
        """
        CSVファイルのプレビューを返す

        Args:
            file_path: CSVファイルパス
            rows: プレビュー行数

        Returns:
            dict: プレビュー情報
        """
        file_path = Path(file_path)

        df = pd.read_csv(
            file_path,
            encoding=self.encoding,
            dtype=str,
            nrows=rows,
        )

        return {
            "columns": list(df.columns),
            "row_count": len(df),
            "sample_data": df.to_dict(orient="records"),
            "column_mapping_preview": {
                col: self.column_mapping.get(col, "(未マッピング)")
                for col in df.columns
            }
        }
