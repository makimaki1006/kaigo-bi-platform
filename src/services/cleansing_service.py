"""
データクレンジングサービス
リードデータの正規化、重複排除、バリデーションを行う
"""

import re
from typing import Optional
from collections import defaultdict

from loguru import logger

from src.models.lead import LeadData, CleansingResult, MatchStatus


class CleansingService:
    """
    データクレンジングサービス

    電話番号の正規化、会社名の正規化、重複排除などを行う
    """

    def __init__(self):
        """初期化"""
        # 電話番号正規化用のパターン
        self._phone_pattern = re.compile(r"[\d０-９]+")

        # 会社名から削除する文字列
        self._company_suffixes = [
            "株式会社", "（株）", "(株)", "㈱",
            "有限会社", "（有）", "(有)", "㈲",
            "合同会社", "（同）", "(同)",
            "一般社団法人", "一般財団法人",
            "公益社団法人", "公益財団法人",
            "NPO法人", "特定非営利活動法人",
        ]

        # 全角→半角変換テーブル
        self._zenkaku_table = str.maketrans(
            "０１２３４５６７８９ａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺ",
            "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
        )

    def cleanse_leads(
        self,
        leads: list[LeadData],
        remove_duplicates: bool = True,
    ) -> list[CleansingResult]:
        """
        リードデータのリストをクレンジングする

        Args:
            leads: リードデータのリスト
            remove_duplicates: 重複を排除するかどうか

        Returns:
            list[CleansingResult]: クレンジング結果のリスト
        """
        logger.info(f"クレンジング開始: {len(leads)}件")

        results = []
        phone_index: dict[str, int] = {}  # 正規化電話番号 → インデックス

        for idx, lead in enumerate(leads):
            # クレンジング処理
            cleansed, changes = self._cleanse_single_lead(lead)

            # バリデーション
            is_valid, errors = self._validate_lead(cleansed)

            # 重複チェック
            is_duplicate = False
            duplicate_of = None

            if remove_duplicates and cleansed.phone:
                normalized_phone = self.normalize_phone(cleansed.phone)
                if normalized_phone:
                    if normalized_phone in phone_index:
                        is_duplicate = True
                        duplicate_of = phone_index[normalized_phone]
                        cleansed.match_status = MatchStatus.DUPLICATE
                    else:
                        phone_index[normalized_phone] = idx

            result = CleansingResult(
                original_data=lead,
                cleansed_data=cleansed,
                changes=changes,
                is_valid=is_valid,
                validation_errors=errors,
                is_duplicate=is_duplicate,
                duplicate_of=duplicate_of,
            )
            results.append(result)

        # 統計情報
        valid_count = sum(1 for r in results if r.is_valid and not r.is_duplicate)
        invalid_count = sum(1 for r in results if not r.is_valid)
        duplicate_count = sum(1 for r in results if r.is_duplicate)

        logger.info(f"✅ クレンジング完了:")
        logger.info(f"   - 有効: {valid_count}件")
        logger.info(f"   - 無効: {invalid_count}件")
        logger.info(f"   - 重複: {duplicate_count}件")

        return results

    def _cleanse_single_lead(
        self,
        lead: LeadData
    ) -> tuple[LeadData, dict]:
        """
        単一のリードデータをクレンジングする

        Args:
            lead: リードデータ

        Returns:
            tuple[LeadData, dict]: クレンジング後のデータと変更内容
        """
        changes = {}

        # 電話番号の正規化
        if lead.phone:
            original_phone = lead.phone
            normalized_phone = self.normalize_phone(lead.phone)
            if normalized_phone != original_phone:
                changes["phone"] = {
                    "before": original_phone,
                    "after": normalized_phone
                }

        # 会社名の正規化
        original_company = lead.company_name
        normalized_company = self.normalize_company_name(lead.company_name)
        if normalized_company != original_company:
            changes["company_name"] = {
                "before": original_company,
                "after": normalized_company
            }

        # 住所の正規化
        if lead.address:
            original_address = lead.address
            normalized_address = self.normalize_address(lead.address)
            if normalized_address != original_address:
                changes["address"] = {
                    "before": original_address,
                    "after": normalized_address
                }

        # メールの正規化（既にモデルで正規化済みだが念のため）
        if lead.email:
            original_email = lead.email
            normalized_email = lead.email.strip().lower()
            if normalized_email != original_email:
                changes["email"] = {
                    "before": original_email,
                    "after": normalized_email
                }

        # クレンジング後のデータを作成
        cleansed = LeadData(
            # 会社情報
            company_name=normalized_company,
            phone=self.normalize_phone(lead.phone) if lead.phone else None,
            address=self.normalize_address(lead.address) if lead.address else None,
            website=lead.website,
            industry=lead.industry,
            # 担当者情報
            last_name=self._normalize_text(lead.last_name) if lead.last_name else None,
            first_name=self._normalize_text(lead.first_name) if lead.first_name else None,
            email=lead.email.strip().lower() if lead.email else None,
            title=lead.title,
            department=lead.department,
            # メタ情報
            source_url=lead.source_url,
            scraped_at=lead.scraped_at,
            notes=lead.notes,
            # ステータス
            match_status=lead.match_status,
        )

        return cleansed, changes

    def normalize_phone(self, phone: Optional[str]) -> Optional[str]:
        """
        電話番号を正規化する

        - 全角数字を半角に変換
        - ハイフン、スペース、括弧を削除
        - 数字のみを抽出

        Args:
            phone: 電話番号文字列

        Returns:
            Optional[str]: 正規化された電話番号（数字のみ）
        """
        if not phone:
            return None

        # 全角を半角に変換
        phone = phone.translate(self._zenkaku_table)

        # 数字のみを抽出
        digits = re.sub(r"[^\d]", "", phone)

        # 空なら None
        if not digits:
            return None

        # 先頭の0が抜けている場合は追加（日本の電話番号）
        if len(digits) == 9 and not digits.startswith("0"):
            digits = "0" + digits

        return digits

    def normalize_company_name(self, company_name: str) -> str:
        """
        会社名を正規化する

        - 前後の空白を削除
        - 全角スペースを半角に
        - 法人格を末尾に統一

        Args:
            company_name: 会社名

        Returns:
            str: 正規化された会社名
        """
        if not company_name:
            return company_name

        # 前後の空白を削除
        name = company_name.strip()

        # 全角スペースを半角に
        name = name.replace("　", " ")

        # 連続するスペースを1つに
        name = re.sub(r"\s+", " ", name)

        return name

    def normalize_address(self, address: Optional[str]) -> Optional[str]:
        """
        住所を正規化する

        Args:
            address: 住所文字列

        Returns:
            Optional[str]: 正規化された住所
        """
        if not address:
            return None

        # 全角を半角に（数字のみ）
        result = address.translate(self._zenkaku_table)

        # 前後の空白を削除
        result = result.strip()

        # 全角スペースを半角に
        result = result.replace("　", " ")

        return result

    def _normalize_text(self, text: Optional[str]) -> Optional[str]:
        """
        一般的なテキストの正規化

        Args:
            text: テキスト

        Returns:
            Optional[str]: 正規化されたテキスト
        """
        if not text:
            return None

        # 前後の空白を削除
        result = text.strip()

        # 全角スペースを半角に
        result = result.replace("　", " ")

        return result if result else None

    def _validate_lead(self, lead: LeadData) -> tuple[bool, list[str]]:
        """
        リードデータのバリデーション

        Args:
            lead: リードデータ

        Returns:
            tuple[bool, list[str]]: 有効かどうかとエラーメッセージのリスト
        """
        errors = []

        # 必須: 会社名
        if not lead.company_name:
            errors.append("会社名は必須です")

        # 推奨: 電話番号（突合キーなので重要）
        if not lead.phone:
            # エラーではなく警告扱い
            pass

        # メールアドレスの形式チェック
        if lead.email:
            if not re.match(r"^[\w\.-]+@[\w\.-]+\.\w+$", lead.email):
                errors.append(f"メールアドレスの形式が不正: {lead.email}")

        # 電話番号の桁数チェック（日本の電話番号）
        if lead.phone:
            digits_only = re.sub(r"[^\d]", "", lead.phone)
            if len(digits_only) < 10 or len(digits_only) > 11:
                errors.append(f"電話番号の桁数が不正: {lead.phone}")

        return len(errors) == 0, errors

    def get_valid_leads(
        self,
        results: list[CleansingResult]
    ) -> list[LeadData]:
        """
        クレンジング結果から有効なリードのみを抽出する

        Args:
            results: クレンジング結果のリスト

        Returns:
            list[LeadData]: 有効なリードデータのリスト
        """
        return [
            r.cleansed_data
            for r in results
            if r.is_valid and not r.is_duplicate
        ]

    def get_statistics(
        self,
        results: list[CleansingResult]
    ) -> dict:
        """
        クレンジング結果の統計情報を取得する

        Args:
            results: クレンジング結果のリスト

        Returns:
            dict: 統計情報
        """
        total = len(results)
        valid = sum(1 for r in results if r.is_valid and not r.is_duplicate)
        invalid = sum(1 for r in results if not r.is_valid)
        duplicates = sum(1 for r in results if r.is_duplicate)

        # 変更があったフィールドの集計
        change_counts = defaultdict(int)
        for r in results:
            for field in r.changes:
                change_counts[field] += 1

        # エラーの集計
        error_counts = defaultdict(int)
        for r in results:
            for error in r.validation_errors:
                error_counts[error] += 1

        return {
            "total": total,
            "valid": valid,
            "invalid": invalid,
            "duplicates": duplicates,
            "valid_rate": valid / total * 100 if total > 0 else 0,
            "changes_by_field": dict(change_counts),
            "error_counts": dict(error_counts),
        }
