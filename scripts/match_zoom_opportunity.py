"""
Zoom録画とSalesforce Opportunityをマッチングするスクリプト

マッチングキー（優先順位）:
1. Zoom Topic ⇔ Opportunity.Name - 会社名部分一致
2. 日付 - 商談実施日 ±1日許容
3. 担当者 - Zoom User ⇔ Opportunity.Owner
4. 取引先名 - Zoom Topic ⇔ AccountName

信頼度スコア:
- 3つ以上一致 → High（自動更新）
- 2つ一致 → Medium（確認推奨）
- 1つのみ → Low（手動確認必須）
"""

import sys
import re
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd

from src.services.opportunity_service import OpportunityService


class ZoomOpportunityMatcher:
    """
    Zoom録画とSalesforce Opportunityをマッチング

    信頼度レベル:
    - HIGH: 3つ以上のキーが一致
    - MEDIUM: 2つのキーが一致
    - LOW: 1つのみ一致
    - NONE: マッチなし
    """

    # 担当者名マッピング（Salesforce名 → Zoomメールドメイン/ユーザー名の一部）
    # 必要に応じて追加
    OWNER_MAPPING = {
        # 例: "田中 太郎": ["tanaka", "taro.tanaka"],
    }

    def __init__(self, opp_service: OpportunityService = None):
        self.opp_service = opp_service or OpportunityService()
        self.opportunities_df = None

    def load_opportunities(
        self,
        from_date: str = None,
        to_date: str = None
    ) -> pd.DataFrame:
        """
        Salesforceから Opportunity データを取得

        Args:
            from_date: 開始日 (YYYY-MM-DD)
            to_date: 終了日 (YYYY-MM-DD)
        """
        if not self.opp_service.access_token:
            self.opp_service.authenticate()

        self.opportunities_df = self.opp_service.get_opportunities_for_matching(
            from_date, to_date
        )

        print(f"Opportunity読み込み: {len(self.opportunities_df):,} 件")
        return self.opportunities_df

    def load_opportunities_from_csv(self, csv_path: str) -> pd.DataFrame:
        """
        CSVからOpportunityデータを読み込み（オフライン用）
        """
        self.opportunities_df = pd.read_csv(csv_path, dtype=str)
        print(f"Opportunity読み込み (CSV): {len(self.opportunities_df):,} 件")
        return self.opportunities_df

    def _normalize_company_name(self, name: str) -> str:
        """
        会社名を正規化（マッチング用）

        - 空白除去
        - 株式会社、有限会社等を除去
        - カタカナ/ひらがな統一（将来的に）
        """
        if not name or pd.isna(name):
            return ""

        name = str(name).strip()

        # 法人格を除去
        patterns = [
            r'株式会社', r'有限会社', r'合同会社', r'一般社団法人',
            r'社会福祉法人', r'医療法人', r'特定非営利活動法人',
            r'㈱', r'㈲', r'\(株\)', r'\(有\)',
        ]
        for pattern in patterns:
            name = re.sub(pattern, '', name)

        # 空白を除去
        name = re.sub(r'\s+', '', name)

        return name.lower()

    def _normalize_date(self, date_str: str) -> Optional[datetime]:
        """日付を正規化"""
        if not date_str or pd.isna(date_str):
            return None

        date_str = str(date_str).strip()

        # 複数のフォーマットに対応
        formats = [
            '%Y-%m-%d',
            '%Y/%m/%d',
            '%Y%m%d',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%dT%H:%M:%SZ',
            '%Y-%m-%dT%H:%M:%S.%fZ',
        ]

        for fmt in formats:
            try:
                return datetime.strptime(date_str[:len(fmt.replace('%', '').replace('.f', '000'))+3], fmt)
            except (ValueError, IndexError):
                continue

        return None

    def _extract_company_from_topic(self, topic: str) -> list[str]:
        """
        Zoomトピックから会社名候補を抽出

        例:
        - "株式会社ABC_田中様" → ["ABC", "田中"]
        - "ABC株式会社 商談" → ["ABC"]
        - "20250601_ABC_初回商談" → ["ABC"]
        """
        if not topic or pd.isna(topic):
            return []

        topic = str(topic).strip()
        candidates = []

        # アンダースコアや空白で分割
        parts = re.split(r'[_\s　]+', topic)

        for part in parts:
            # 日付パターンを除外
            if re.match(r'^\d{6,8}$', part):
                continue
            # 「様」「商談」「初回」等を除外
            if part in ['様', '商談', '初回', '再商談', '新規', 'MTG', 'meeting']:
                continue
            # 短すぎる文字列を除外
            if len(part) < 2:
                continue

            # 法人格を除去して追加
            normalized = self._normalize_company_name(part)
            if normalized and len(normalized) >= 2:
                candidates.append(normalized)

        return candidates

    def _match_owner(self, zoom_user_email: str, zoom_user_name: str, sf_owner_name: str) -> bool:
        """担当者名のマッチング"""
        if not sf_owner_name or pd.isna(sf_owner_name):
            return False

        sf_owner_name = str(sf_owner_name).strip()
        sf_parts = sf_owner_name.split()

        # メールアドレスに名前の一部が含まれるか
        if zoom_user_email:
            email_lower = zoom_user_email.lower()
            for part in sf_parts:
                if part.lower() in email_lower:
                    return True

        # Zoomユーザー名にSF担当者名の一部が含まれるか
        if zoom_user_name:
            zoom_name_lower = zoom_user_name.lower()
            for part in sf_parts:
                if part.lower() in zoom_name_lower:
                    return True

        return False

    def match_single(
        self,
        zoom_topic: str,
        zoom_date: str,
        zoom_user_email: str = None,
        zoom_user_name: str = None,
        date_tolerance_days: int = 1
    ) -> dict:
        """
        単一のZoom録画をOpportunityとマッチング

        Args:
            zoom_topic: Zoom会議のトピック
            zoom_date: Zoom会議の日付（YYYY-MM-DD等）
            zoom_user_email: Zoomユーザーのメールアドレス
            zoom_user_name: Zoomユーザーの名前
            date_tolerance_days: 日付の許容誤差（日）

        Returns:
            dict: マッチング結果
                {
                    'opportunity_id': str,
                    'opportunity_name': str,
                    'confidence': 'HIGH' | 'MEDIUM' | 'LOW' | 'NONE',
                    'match_score': int,
                    'match_reasons': list[str],
                    'candidates': list[dict]  # 複数候補がある場合
                }
        """
        if self.opportunities_df is None or self.opportunities_df.empty:
            return {
                'opportunity_id': None,
                'confidence': 'NONE',
                'match_score': 0,
                'match_reasons': [],
                'candidates': [],
                'error': 'Opportunityデータが読み込まれていません'
            }

        # Zoomデータの正規化
        zoom_date_parsed = self._normalize_date(zoom_date)
        zoom_company_candidates = self._extract_company_from_topic(zoom_topic)

        candidates = []

        for _, opp_row in self.opportunities_df.iterrows():
            score = 0
            reasons = []

            # 1. 会社名マッチング（Opportunity.Name）
            opp_name = str(opp_row.get('Name', ''))
            opp_name_normalized = self._normalize_company_name(opp_name)

            for zoom_company in zoom_company_candidates:
                if zoom_company in opp_name_normalized or opp_name_normalized in zoom_company:
                    score += 3
                    reasons.append(f"商談名一致: {zoom_company}")
                    break

            # 2. 取引先名マッチング（Account.Name）
            account_name = str(opp_row.get('Account.Name', '') or '')
            account_name_normalized = self._normalize_company_name(account_name)

            if account_name_normalized:
                for zoom_company in zoom_company_candidates:
                    if zoom_company in account_name_normalized or account_name_normalized in zoom_company:
                        score += 2
                        reasons.append(f"取引先名一致: {zoom_company}")
                        break

            # 3. 日付マッチング
            opp_close_date = self._normalize_date(opp_row.get('CloseDate', ''))
            if zoom_date_parsed and opp_close_date:
                date_diff = abs((zoom_date_parsed - opp_close_date).days)
                if date_diff <= date_tolerance_days:
                    score += 2
                    reasons.append(f"日付一致 (差: {date_diff}日)")

            # 4. 担当者マッチング
            owner_name = str(opp_row.get('Owner.Name', '') or '')
            if self._match_owner(zoom_user_email, zoom_user_name, owner_name):
                score += 2
                reasons.append(f"担当者一致: {owner_name}")

            # スコアが1以上なら候補に追加
            if score > 0:
                candidates.append({
                    'opportunity_id': opp_row['Id'],
                    'opportunity_name': opp_name,
                    'account_name': account_name,
                    'owner_name': owner_name,
                    'close_date': str(opp_row.get('CloseDate', '')),
                    'stage_name': str(opp_row.get('StageName', '')),
                    'match_score': score,
                    'match_reasons': reasons,
                })

        # スコア順にソート
        candidates.sort(key=lambda x: x['match_score'], reverse=True)

        if not candidates:
            return {
                'opportunity_id': None,
                'confidence': 'NONE',
                'match_score': 0,
                'match_reasons': [],
                'candidates': [],
            }

        # ベストマッチを取得
        best = candidates[0]

        # 信頼度を判定
        if best['match_score'] >= 5:
            confidence = 'HIGH'
        elif best['match_score'] >= 3:
            confidence = 'MEDIUM'
        else:
            confidence = 'LOW'

        return {
            'opportunity_id': best['opportunity_id'],
            'opportunity_name': best['opportunity_name'],
            'account_name': best['account_name'],
            'confidence': confidence,
            'match_score': best['match_score'],
            'match_reasons': best['match_reasons'],
            'candidates': candidates[:5],  # 上位5件を返す
        }

    def match_batch(
        self,
        zoom_records: list[dict],
        date_tolerance_days: int = 1
    ) -> pd.DataFrame:
        """
        複数のZoom録画をバッチでマッチング

        Args:
            zoom_records: Zoom録画データのリスト
                [{
                    'meeting_id': str,
                    'topic': str,
                    'date': str,
                    'user_email': str,
                    'user_name': str,
                }, ...]
            date_tolerance_days: 日付の許容誤差（日）

        Returns:
            pd.DataFrame: マッチング結果
        """
        results = []

        print(f"\n[バッチマッチング] {len(zoom_records):,} 件")

        for i, zoom_rec in enumerate(zoom_records, 1):
            if i % 100 == 0:
                print(f"  進捗: {i:,}/{len(zoom_records):,}")

            match_result = self.match_single(
                zoom_topic=zoom_rec.get('topic', ''),
                zoom_date=zoom_rec.get('date', ''),
                zoom_user_email=zoom_rec.get('user_email', ''),
                zoom_user_name=zoom_rec.get('user_name', ''),
                date_tolerance_days=date_tolerance_days,
            )

            results.append({
                'zoom_meeting_id': zoom_rec.get('meeting_id', ''),
                'zoom_topic': zoom_rec.get('topic', ''),
                'zoom_date': zoom_rec.get('date', ''),
                'zoom_user_email': zoom_rec.get('user_email', ''),
                'opportunity_id': match_result['opportunity_id'],
                'opportunity_name': match_result.get('opportunity_name', ''),
                'account_name': match_result.get('account_name', ''),
                'confidence': match_result['confidence'],
                'match_score': match_result['match_score'],
                'match_reasons': '; '.join(match_result['match_reasons']),
            })

        df = pd.DataFrame(results)

        # 統計表示
        print(f"\n[マッチング結果]")
        print(f"  HIGH:   {(df['confidence'] == 'HIGH').sum():,} 件")
        print(f"  MEDIUM: {(df['confidence'] == 'MEDIUM').sum():,} 件")
        print(f"  LOW:    {(df['confidence'] == 'LOW').sum():,} 件")
        print(f"  NONE:   {(df['confidence'] == 'NONE').sum():,} 件")

        return df


# ========================================
# CLI
# ========================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Zoom-Opportunity マッチング")
    parser.add_argument("--zoom-csv", type=str, help="Zoom録画データCSVパス")
    parser.add_argument("--opp-csv", type=str, help="Opportunity CSVパス（オフライン用）")
    parser.add_argument("--from-date", type=str, help="開始日 (YYYY-MM-DD)")
    parser.add_argument("--to-date", type=str, help="終了日 (YYYY-MM-DD)")
    parser.add_argument("--output", type=str, default="data/output/zoom_opp_matching.csv", help="出力CSVパス")
    parser.add_argument("--date-tolerance", type=int, default=1, help="日付許容誤差（日）")

    args = parser.parse_args()

    matcher = ZoomOpportunityMatcher()

    # Opportunity読み込み
    if args.opp_csv:
        matcher.load_opportunities_from_csv(args.opp_csv)
    else:
        matcher.load_opportunities(args.from_date, args.to_date)

    # Zoom録画読み込み
    if args.zoom_csv:
        zoom_df = pd.read_csv(args.zoom_csv, dtype=str)
        print(f"Zoom録画読み込み: {len(zoom_df):,} 件")

        # カラム名を正規化
        column_mapping = {
            'meeting_id': 'meeting_id',
            'zoom_uuid': 'meeting_id',
            'uuid': 'meeting_id',
            'topic': 'topic',
            'zoom_topic': 'topic',
            'date': 'date',
            'zoom_date': 'date',
            'start_time': 'date',
            'user_email': 'user_email',
            'zoom_user_email': 'user_email',
            'user_name': 'user_name',
            'zoom_user_name': 'user_name',
        }

        zoom_records = []
        for _, row in zoom_df.iterrows():
            record = {}
            for target_key, source_keys in column_mapping.items():
                if isinstance(source_keys, str):
                    source_keys = [source_keys]
                for source_key in source_keys:
                    if source_key in row and pd.notna(row[source_key]):
                        record[target_key] = row[source_key]
                        break

            zoom_records.append(record)

        # バッチマッチング
        result_df = matcher.match_batch(zoom_records, args.date_tolerance)

        # 結果保存
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        result_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"\n結果保存: {output_path}")
    else:
        print("--zoom-csv を指定してください")
