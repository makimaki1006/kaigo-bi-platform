"""
リードデータモデル
CSVインポート、Salesforce連携、突合処理で使用するデータ構造を定義
"""

from datetime import datetime
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field, field_validator
import re


class MatchStatus(str, Enum):
    """突合ステータス"""
    MATCHED = "matched"           # 既存リードと一致
    NEW = "new"                   # 新規リード（Salesforceに存在しない）
    DUPLICATE = "duplicate"       # 重複データ
    UNPROCESSED = "unprocessed"   # 未処理


class DataSource(str, Enum):
    """データソース"""
    CSV_IMPORT = "csv_import"     # CSVインポート
    SALESFORCE = "salesforce"     # Salesforce


class LeadData(BaseModel):
    """
    リードデータモデル（CSVインポート用）

    会社情報と担当者情報を保持する
    """
    # 会社情報
    company_name: str = Field(..., description="会社名")
    phone: Optional[str] = Field(None, description="電話番号（突合キー）")
    address: Optional[str] = Field(None, description="住所")
    website: Optional[str] = Field(None, description="Webサイト")
    industry: Optional[str] = Field(None, description="業種")

    # 担当者情報
    last_name: Optional[str] = Field(None, description="姓")
    first_name: Optional[str] = Field(None, description="名")
    email: Optional[str] = Field(None, description="メールアドレス")
    title: Optional[str] = Field(None, description="役職")
    department: Optional[str] = Field(None, description="部署")

    # メタ情報
    source_url: Optional[str] = Field(None, description="取得元URL")
    scraped_at: Optional[datetime] = Field(None, description="スクレイピング日時")
    notes: Optional[str] = Field(None, description="備考")

    # 処理ステータス
    match_status: MatchStatus = Field(
        default=MatchStatus.UNPROCESSED,
        description="突合ステータス"
    )

    @field_validator('phone', mode='before')
    @classmethod
    def normalize_phone_basic(cls, v: Optional[str]) -> Optional[str]:
        """電話番号の基本的な正規化（詳細はCleansingServiceで行う）"""
        if v is None or v == "":
            return None
        # 文字列に変換
        return str(v).strip()

    @field_validator('email', mode='before')
    @classmethod
    def normalize_email(cls, v: Optional[str]) -> Optional[str]:
        """メールアドレスの正規化"""
        if v is None or v == "":
            return None
        return str(v).strip().lower()


class SalesforceLeadData(BaseModel):
    """
    Salesforceリードデータモデル

    SalesforceのLeadオブジェクトに対応
    """
    # Salesforce固有
    id: str = Field(..., description="Salesforce ID")

    # 会社情報
    company: str = Field(..., description="会社名")
    phone: Optional[str] = Field(None, description="電話番号")
    street: Optional[str] = Field(None, description="住所（番地）")
    city: Optional[str] = Field(None, description="市区町村")
    state: Optional[str] = Field(None, description="都道府県")
    postal_code: Optional[str] = Field(None, description="郵便番号")
    country: Optional[str] = Field(None, description="国")
    website: Optional[str] = Field(None, description="Webサイト")
    industry: Optional[str] = Field(None, description="業種")

    # 担当者情報
    last_name: str = Field(..., description="姓")
    first_name: Optional[str] = Field(None, description="名")
    email: Optional[str] = Field(None, description="メールアドレス")
    title: Optional[str] = Field(None, description="役職")

    # ステータス
    status: Optional[str] = Field(None, description="リードステータス")

    # メタ情報
    created_date: Optional[datetime] = Field(None, description="作成日")
    last_modified_date: Optional[datetime] = Field(None, description="最終更新日")

    @property
    def full_address(self) -> str:
        """完全な住所を返す"""
        parts = [
            self.postal_code,
            self.state,
            self.city,
            self.street
        ]
        return " ".join(p for p in parts if p)


class MatchResult(BaseModel):
    """
    突合結果モデル

    CSVデータとSalesforceデータの突合結果を保持
    """
    # 突合対象
    csv_data: LeadData = Field(..., description="CSVから読み込んだデータ")

    # 突合結果
    status: MatchStatus = Field(..., description="突合ステータス")
    matched_sf_id: Optional[str] = Field(None, description="一致したSalesforce ID")
    matched_sf_data: Optional[SalesforceLeadData] = Field(
        None,
        description="一致したSalesforceデータ"
    )

    # スコア情報
    match_score: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="突合スコア（0.0-1.0）"
    )
    match_key: Optional[str] = Field(None, description="一致したキー（phone, company_name等）")

    # 処理情報
    processed_at: datetime = Field(
        default_factory=datetime.now,
        description="処理日時"
    )
    notes: Optional[str] = Field(None, description="備考")


class CleansingResult(BaseModel):
    """
    クレンジング結果モデル

    データクレンジング処理の結果を保持
    """
    original_data: LeadData = Field(..., description="元データ")
    cleansed_data: LeadData = Field(..., description="クレンジング後データ")

    # 変更情報
    changes: dict = Field(default_factory=dict, description="変更内容")
    is_valid: bool = Field(default=True, description="有効なデータかどうか")
    validation_errors: list[str] = Field(
        default_factory=list,
        description="バリデーションエラー"
    )

    # 重複情報
    is_duplicate: bool = Field(default=False, description="重複フラグ")
    duplicate_of: Optional[int] = Field(
        None,
        description="重複元のインデックス"
    )
