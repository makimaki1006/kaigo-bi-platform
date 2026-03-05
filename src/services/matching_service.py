"""
マッチング（突合）サービス
CSVデータとSalesforceデータを突合し、一致・新規を判定する
"""

import re
from typing import Optional
from difflib import SequenceMatcher

from loguru import logger

from src.models.lead import (
    LeadData,
    SalesforceLeadData,
    MatchResult,
    MatchStatus,
)
from src.services.cleansing_service import CleansingService


class MatchingService:
    """
    マッチング（突合）サービス

    CSVからインポートしたリードデータとSalesforceのリードデータを
    電話番号をメインキーとして突合する
    """

    def __init__(
        self,
        similarity_threshold: float = 0.85,
    ):
        """
        初期化

        Args:
            similarity_threshold: 類似度の閾値（0.0-1.0）
        """
        self.similarity_threshold = similarity_threshold
        self.cleansing_service = CleansingService()

    def match_leads(
        self,
        csv_leads: list[LeadData],
        sf_leads: list[SalesforceLeadData],
    ) -> list[MatchResult]:
        """
        CSVリードとSalesforceリードを突合する

        Args:
            csv_leads: CSVからインポートしたリードデータ
            sf_leads: Salesforceから取得したリードデータ

        Returns:
            list[MatchResult]: 突合結果のリスト
        """
        logger.info(f"突合開始: CSV {len(csv_leads)}件 vs SF {len(sf_leads)}件")

        # Salesforceデータのインデックスを構築
        sf_phone_index = self._build_phone_index(sf_leads)
        sf_company_index = self._build_company_index(sf_leads)

        results = []

        for csv_lead in csv_leads:
            result = self._match_single_lead(
                csv_lead,
                sf_leads,
                sf_phone_index,
                sf_company_index,
            )
            results.append(result)

        # 統計情報
        matched = sum(1 for r in results if r.status == MatchStatus.MATCHED)
        new = sum(1 for r in results if r.status == MatchStatus.NEW)

        logger.info(f"✅ 突合完了:")
        logger.info(f"   - 一致: {matched}件")
        logger.info(f"   - 新規: {new}件")

        return results

    def _build_phone_index(
        self,
        sf_leads: list[SalesforceLeadData]
    ) -> dict[str, list[SalesforceLeadData]]:
        """
        電話番号インデックスを構築する

        Args:
            sf_leads: Salesforceリードデータ

        Returns:
            dict: 正規化電話番号 → Salesforceリードのリスト
        """
        index: dict[str, list[SalesforceLeadData]] = {}

        for sf_lead in sf_leads:
            if sf_lead.phone:
                normalized = self.cleansing_service.normalize_phone(sf_lead.phone)
                if normalized:
                    if normalized not in index:
                        index[normalized] = []
                    index[normalized].append(sf_lead)

        return index

    def _build_company_index(
        self,
        sf_leads: list[SalesforceLeadData]
    ) -> dict[str, list[SalesforceLeadData]]:
        """
        会社名インデックスを構築する（補助キー用）

        Args:
            sf_leads: Salesforceリードデータ

        Returns:
            dict: 正規化会社名 → Salesforceリードのリスト
        """
        index: dict[str, list[SalesforceLeadData]] = {}

        for sf_lead in sf_leads:
            if sf_lead.company:
                normalized = self._normalize_for_matching(sf_lead.company)
                if normalized not in index:
                    index[normalized] = []
                index[normalized].append(sf_lead)

        return index

    def _match_single_lead(
        self,
        csv_lead: LeadData,
        sf_leads: list[SalesforceLeadData],
        sf_phone_index: dict[str, list[SalesforceLeadData]],
        sf_company_index: dict[str, list[SalesforceLeadData]],
    ) -> MatchResult:
        """
        単一のCSVリードを突合する

        Args:
            csv_lead: CSVリード
            sf_leads: 全Salesforceリード
            sf_phone_index: 電話番号インデックス
            sf_company_index: 会社名インデックス

        Returns:
            MatchResult: 突合結果
        """
        # 1. 電話番号での完全一致を試みる
        if csv_lead.phone:
            normalized_phone = self.cleansing_service.normalize_phone(csv_lead.phone)
            if normalized_phone and normalized_phone in sf_phone_index:
                sf_matches = sf_phone_index[normalized_phone]

                # 複数一致の場合は会社名で絞り込み
                best_match = self._select_best_match(csv_lead, sf_matches)

                csv_lead.match_status = MatchStatus.MATCHED
                return MatchResult(
                    csv_data=csv_lead,
                    status=MatchStatus.MATCHED,
                    matched_sf_id=best_match.id,
                    matched_sf_data=best_match,
                    match_score=1.0,
                    match_key="phone",
                )

        # 2. 会社名 + 住所での類似マッチングを試みる（補助）
        if csv_lead.company_name:
            normalized_company = self._normalize_for_matching(csv_lead.company_name)

            # 完全一致
            if normalized_company in sf_company_index:
                sf_matches = sf_company_index[normalized_company]

                # 住所で絞り込み
                best_match = self._select_best_match_by_address(
                    csv_lead, sf_matches
                )

                if best_match:
                    csv_lead.match_status = MatchStatus.MATCHED
                    return MatchResult(
                        csv_data=csv_lead,
                        status=MatchStatus.MATCHED,
                        matched_sf_id=best_match.id,
                        matched_sf_data=best_match,
                        match_score=0.9,
                        match_key="company_name",
                        notes="電話番号一致なし、会社名で一致",
                    )

            # 類似マッチング
            similar_match = self._find_similar_company(
                csv_lead.company_name,
                sf_leads,
            )
            if similar_match:
                sf_lead, score = similar_match
                if score >= self.similarity_threshold:
                    csv_lead.match_status = MatchStatus.MATCHED
                    return MatchResult(
                        csv_data=csv_lead,
                        status=MatchStatus.MATCHED,
                        matched_sf_id=sf_lead.id,
                        matched_sf_data=sf_lead,
                        match_score=score,
                        match_key="company_name_similar",
                        notes=f"会社名類似度: {score:.2f}",
                    )

        # 3. 一致なし → 新規
        csv_lead.match_status = MatchStatus.NEW
        return MatchResult(
            csv_data=csv_lead,
            status=MatchStatus.NEW,
            match_score=0.0,
        )

    def _select_best_match(
        self,
        csv_lead: LeadData,
        sf_matches: list[SalesforceLeadData],
    ) -> SalesforceLeadData:
        """
        複数の候補から最適な一致を選択する

        Args:
            csv_lead: CSVリード
            sf_matches: 候補のSalesforceリード

        Returns:
            SalesforceLeadData: 最適な一致
        """
        if len(sf_matches) == 1:
            return sf_matches[0]

        # 会社名の類似度でスコアリング
        best_match = sf_matches[0]
        best_score = 0.0

        for sf_lead in sf_matches:
            score = self._calculate_similarity(
                csv_lead.company_name,
                sf_lead.company
            )
            if score > best_score:
                best_score = score
                best_match = sf_lead

        return best_match

    def _select_best_match_by_address(
        self,
        csv_lead: LeadData,
        sf_matches: list[SalesforceLeadData],
    ) -> Optional[SalesforceLeadData]:
        """
        住所を考慮して最適な一致を選択する

        Args:
            csv_lead: CSVリード
            sf_matches: 候補のSalesforceリード

        Returns:
            Optional[SalesforceLeadData]: 最適な一致（なければNone）
        """
        if not csv_lead.address:
            # 住所がない場合は会社名一致のみで判定
            return sf_matches[0] if sf_matches else None

        best_match = None
        best_score = 0.0

        for sf_lead in sf_matches:
            sf_address = sf_lead.full_address
            if not sf_address:
                continue

            score = self._calculate_similarity(csv_lead.address, sf_address)
            if score > best_score:
                best_score = score
                best_match = sf_lead

        # 住所の類似度が閾値以上の場合のみ返す
        if best_score >= self.similarity_threshold:
            return best_match

        return None

    def _find_similar_company(
        self,
        company_name: str,
        sf_leads: list[SalesforceLeadData],
    ) -> Optional[tuple[SalesforceLeadData, float]]:
        """
        類似した会社名を持つSalesforceリードを探す

        Args:
            company_name: 検索対象の会社名
            sf_leads: Salesforceリード

        Returns:
            Optional[tuple]: (Salesforceリード, 類似度スコア)
        """
        best_match = None
        best_score = 0.0

        normalized_target = self._normalize_for_matching(company_name)

        for sf_lead in sf_leads:
            normalized_sf = self._normalize_for_matching(sf_lead.company)
            score = self._calculate_similarity(normalized_target, normalized_sf)

            if score > best_score:
                best_score = score
                best_match = sf_lead

        if best_match and best_score >= self.similarity_threshold:
            return (best_match, best_score)

        return None

    def _normalize_for_matching(self, text: str) -> str:
        """
        マッチング用にテキストを正規化する

        Args:
            text: テキスト

        Returns:
            str: 正規化されたテキスト
        """
        if not text:
            return ""

        # 小文字化
        result = text.lower()

        # 法人格を削除
        patterns = [
            r"株式会社", r"（株）", r"\(株\)", r"㈱",
            r"有限会社", r"（有）", r"\(有\)", r"㈲",
            r"合同会社", r"（同）", r"\(同\)",
        ]
        for pattern in patterns:
            result = re.sub(pattern, "", result)

        # スペースを削除
        result = re.sub(r"\s+", "", result)

        return result

    def _calculate_similarity(self, str1: str, str2: str) -> float:
        """
        2つの文字列の類似度を計算する

        Args:
            str1: 文字列1
            str2: 文字列2

        Returns:
            float: 類似度（0.0-1.0）
        """
        if not str1 or not str2:
            return 0.0

        return SequenceMatcher(None, str1, str2).ratio()

    def get_matched_leads(
        self,
        results: list[MatchResult]
    ) -> list[MatchResult]:
        """
        一致したリードのみを抽出する

        Args:
            results: 突合結果

        Returns:
            list[MatchResult]: 一致したリードの結果
        """
        return [r for r in results if r.status == MatchStatus.MATCHED]

    def get_new_leads(
        self,
        results: list[MatchResult]
    ) -> list[MatchResult]:
        """
        新規リードのみを抽出する

        Args:
            results: 突合結果

        Returns:
            list[MatchResult]: 新規リードの結果
        """
        return [r for r in results if r.status == MatchStatus.NEW]

    def get_statistics(self, results: list[MatchResult]) -> dict:
        """
        突合結果の統計情報を取得する

        Args:
            results: 突合結果

        Returns:
            dict: 統計情報
        """
        total = len(results)
        matched = sum(1 for r in results if r.status == MatchStatus.MATCHED)
        new = sum(1 for r in results if r.status == MatchStatus.NEW)

        # マッチキーの集計
        match_keys: dict[str, int] = {}
        for r in results:
            if r.match_key:
                match_keys[r.match_key] = match_keys.get(r.match_key, 0) + 1

        # スコア分布
        scores = [r.match_score for r in results if r.match_score > 0]
        avg_score = sum(scores) / len(scores) if scores else 0

        return {
            "total": total,
            "matched": matched,
            "new": new,
            "match_rate": matched / total * 100 if total > 0 else 0,
            "match_keys": match_keys,
            "average_match_score": avg_score,
        }
