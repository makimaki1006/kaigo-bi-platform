"""
Zoom商談分析 → Salesforce自動同期パイプライン

処理フロー:
1. 新規Zoom録画の検出
2. VTT取得 → TSV変換
3. Gemini v8分析 → JSON
4. 特徴量抽出
5. 81.4%ルール予測
6. Opportunityマッチング
7. Salesforce更新

使用方法:
    # 単発実行
    python zoom_sf_sync_pipeline.py --from-date 2026-01-01 --to-date 2026-01-15

    # 差分実行（処理済みをスキップ）
    python zoom_sf_sync_pipeline.py --incremental

    # ドライラン
    python zoom_sf_sync_pipeline.py --dry-run
"""

import sys
import os
import json
import time
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# ZoomMTG_analyzer_autoをパスに追加
zoom_project = Path("C:/Users/fuji1/OneDrive/デスクトップ/ZoomMTG_analyzer_auto")
sys.path.insert(0, str(zoom_project))

import pandas as pd

from src.services.opportunity_service import OpportunityService
from scripts.match_zoom_opportunity import ZoomOpportunityMatcher


class ZoomSFSyncPipeline:
    """
    Zoom商談分析 → Salesforce同期パイプライン

    処理時間目安:
    - VTT取得: 1-2秒/件
    - TSV変換: 0.1秒/件
    - Gemini分析: 8-12秒/件 (ボトルネック)
    - 特徴量抽出: 0.3秒/件
    - 予測: 0.1秒/件
    - SF更新: 0.5秒/件
    合計: 10-15秒/件
    """

    def __init__(self, config_path: str = None):
        """
        Args:
            config_path: 設定ファイルパス（オプション）
        """
        self.project_root = project_root
        self.zoom_project = zoom_project
        self.output_dir = project_root / "data" / "output" / "zoom_sync"
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # 処理済みUUID管理ファイル
        self.processed_file = self.output_dir / "processed_uuids.csv"
        self.processed_uuids = self._load_processed_uuids()

        # サービス初期化
        self.opp_service = OpportunityService()
        self.matcher = ZoomOpportunityMatcher(self.opp_service)

        # Gemini API設定
        self.gemini_api_key = os.environ.get("GEMINI_API_KEY", "")

    def _load_processed_uuids(self) -> set:
        """処理済みUUIDを読み込み"""
        if self.processed_file.exists():
            df = pd.read_csv(self.processed_file, dtype=str)
            return set(df['uuid'].tolist())
        return set()

    def _save_processed_uuid(self, uuid: str, status: str = "success"):
        """処理済みUUIDを保存"""
        self.processed_uuids.add(uuid)

        new_row = pd.DataFrame([{
            'uuid': uuid,
            'status': status,
            'processed_at': datetime.now().isoformat()
        }])

        if self.processed_file.exists():
            df = pd.read_csv(self.processed_file, dtype=str)
            df = pd.concat([df, new_row], ignore_index=True)
        else:
            df = new_row

        df.to_csv(self.processed_file, index=False, encoding='utf-8-sig')

    def authenticate(self):
        """Salesforce認証"""
        self.opp_service.authenticate()
        print("Salesforce認証完了")

    def check_zoom_fields(self) -> bool:
        """Salesforce側のZoomフィールド存在確認"""
        result = self.opp_service.check_zoom_fields_exist()
        missing = [k for k, v in result.items() if not v]

        if missing:
            print(f"\n⚠️ 以下のカスタムフィールドを先に作成してください:")
            for field in missing:
                print(f"  - {field}")
            return False
        return True

    def _predict_won_lost(self, features: Dict[str, Any]) -> tuple:
        """
        81.4%精度ルールで予測

        v7ルール:
        1. 温度確認で数値回答あり → Lost
        2. 顧客コミットメントあり → Won（条件付き）
        3. hearing高い & objection低い → Won
        4. 次ステップ合意あり → Won
        5. デフォルト → Lost
        """
        # 閾値設定
        HEARING_THRESHOLD_HIGH = 0.20  # 20%以上
        HEARING_THRESHOLD_LOW = 0.10   # 10%以下
        OBJECTION_THRESHOLD_HIGH = 0.15
        OBJECTION_THRESHOLD_LOW = 0.10

        # 特徴量取得（実際のカラム名に合わせる）
        temp_score = features.get('temperature_score', 0) or 0
        has_temp_check = features.get('has_temperature_check', 0) == 1

        # 顧客コミットメント（closing_customer_commitment または n_act_next_step_client > 0）
        customer_commit = features.get('closing_customer_commitment', 0) == 1
        next_step_client = (features.get('n_act_next_step_client', 0) or 0) > 0

        # ヒアリング比率と反論比率
        hearing_ratio = features.get('phase_hearing_ratio', 0.15) or 0.15
        obj_ratio = features.get('phase_objection_ratio', 0.1) or 0.1

        # 次ステップ合意
        has_next_step = features.get('has_next_step_agreed', 0) == 1
        next_step_specificity = features.get('next_step_specificity', 0) or 0
        commitment_level = features.get('commitment_level', 0) or 0

        # ルール適用
        # Rule 1: 温度確認で具体的な数値回答あり → Lost
        if has_temp_check and temp_score > 0:
            return 'Lost予測', 'rule_1_temperature', '温度確認で数値回答あり'

        # Rule 2: 顧客コミットメントあり → Won（ただし条件付き）
        if customer_commit or next_step_client:
            if hearing_ratio < HEARING_THRESHOLD_LOW and obj_ratio >= OBJECTION_THRESHOLD_HIGH:
                return 'Lost予測', 'rule_2b_concerns', 'ヒアリング不足かつ反論多い'
            return 'Won予測', 'rule_2a_customer_commit', '顧客からコミットメントあり'

        # Rule 3: 十分なヒアリング & 反論少ない → Won
        if hearing_ratio >= HEARING_THRESHOLD_HIGH and obj_ratio < OBJECTION_THRESHOLD_LOW:
            return 'Won予測', 'rule_3_good_hearing', '十分なヒアリングかつ反論少ない'

        # Rule 4: 次ステップ合意あり & 具体的 → Won
        if has_next_step and (next_step_specificity >= 5 or commitment_level >= 2):
            return 'Won予測', 'rule_4_next_step_agreed', '具体的な次ステップ合意あり'

        return 'Lost予測', 'rule_default', '明確なWonシグナルなし'

    def _calculate_risk_level(self, prediction: str, features: Dict[str, Any]) -> str:
        """リスクレベルを計算"""
        if prediction == 'Won予測':
            return '低'

        # Lost予測の場合、特徴量でリスクレベルを判定
        obj_ratio = features.get('phase_objection_ratio', 0) or 0
        has_temp_check = features.get('has_temperature_check', 0) == 1

        if has_temp_check or obj_ratio >= 0.2:
            return '高'
        elif obj_ratio >= 0.1:
            return '中'
        else:
            return '低'

    def _calculate_analysis_score(self, features: Dict[str, Any]) -> int:
        """総合分析スコアを計算（0-100）"""
        score = 50  # ベーススコア

        # プラス要素
        if features.get('closing_customer_commitment', 0) == 1:
            score += 20

        if features.get('has_next_step_agreed', 0) == 1:
            score += 10

        hearing_ratio = features.get('phase_hearing_ratio', 0) or 0
        if hearing_ratio > 0.2:
            score += 10

        # マイナス要素
        if features.get('has_temperature_check', 0) == 1:
            score -= 15

        obj_ratio = features.get('phase_objection_ratio', 0) or 0
        if obj_ratio >= 0.15:
            score -= 10

        return max(0, min(100, score))

    def process_single(
        self,
        zoom_meeting_id: str,
        zoom_topic: str,
        zoom_date: str,
        zoom_user_email: str = None,
        features: Dict[str, Any] = None,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """
        単一の商談を処理

        Args:
            zoom_meeting_id: Zoom Meeting UUID
            zoom_topic: Zoom会議トピック
            zoom_date: 会議日付
            zoom_user_email: Zoomユーザーメール
            features: 分析特徴量（既にGemini分析済みの場合）
            dry_run: ドライラン

        Returns:
            dict: 処理結果
        """
        result = {
            'zoom_meeting_id': zoom_meeting_id,
            'zoom_topic': zoom_topic,
            'zoom_date': zoom_date,
            'status': 'pending',
        }

        # 処理済みチェック
        if zoom_meeting_id in self.processed_uuids:
            result['status'] = 'skipped_already_processed'
            return result

        # 1. Opportunityマッチング
        match_result = self.matcher.match_single(
            zoom_topic=zoom_topic,
            zoom_date=zoom_date,
            zoom_user_email=zoom_user_email,
        )

        result['opportunity_id'] = match_result['opportunity_id']
        result['opportunity_name'] = match_result.get('opportunity_name', '')
        result['match_confidence'] = match_result['confidence']
        result['match_score'] = match_result['match_score']

        if match_result['confidence'] == 'NONE':
            result['status'] = 'no_match'
            return result

        # 2. 予測実行
        if features:
            prediction, applied_rule, explanation = self._predict_won_lost(features)
            risk_level = self._calculate_risk_level(prediction, features)
            analysis_score = self._calculate_analysis_score(features)

            result['prediction'] = prediction
            result['applied_rule'] = applied_rule
            result['explanation'] = explanation
            result['risk_level'] = risk_level
            result['analysis_score'] = analysis_score
            result['temperature_check'] = features.get('has_temperature_check', False)
            result['temperature_value'] = features.get('temperature_numeric_value')
            result['customer_next_step'] = features.get('customer_proposes_next_step', 0) == 1
            result['hearing_ratio'] = features.get('phase_hearing_ratio')
            result['objection_ratio'] = features.get('objection_phase_ratio')
        else:
            # 特徴量がない場合はスキップ
            result['status'] = 'no_features'
            return result

        # 3. Salesforce更新
        if match_result['confidence'] == 'HIGH' and not dry_run:
            update_data = {
                'Id': match_result['opportunity_id'],
                'prediction': result['prediction'],
                'analysis_score': result['analysis_score'],
                'risk_level': result['risk_level'],
                'temperature_check': result.get('temperature_check', False),
                'temperature_value': result.get('temperature_value'),
                'customer_next_step': result.get('customer_next_step', False),
                'hearing_ratio': result.get('hearing_ratio'),
                'objection_ratio': result.get('objection_ratio'),
                'applied_rule': result['applied_rule'],
                'meeting_id': zoom_meeting_id,
            }

            update_result = self.opp_service.update_zoom_analysis([update_data], dry_run=dry_run)
            result['sf_update'] = update_result
            result['status'] = 'success' if update_result['success'] > 0 else 'sf_update_failed'
        elif dry_run:
            result['status'] = 'dry_run'
        else:
            result['status'] = 'low_confidence_match'

        # 処理済み記録
        if result['status'] == 'success':
            self._save_processed_uuid(zoom_meeting_id, 'success')

        return result

    def process_from_features_csv(
        self,
        features_csv: str,
        from_date: str = None,
        to_date: str = None,
        dry_run: bool = False,
        incremental: bool = True
    ) -> pd.DataFrame:
        """
        特徴量CSVから一括処理

        Args:
            features_csv: 特徴量CSVパス（features_combined_627等）
            from_date: 開始日
            to_date: 終了日
            dry_run: ドライラン
            incremental: 処理済みをスキップ

        Returns:
            pd.DataFrame: 処理結果
        """
        print(f"\n[パイプライン開始]")
        print(f"  特徴量CSV: {features_csv}")
        print(f"  ドライラン: {dry_run}")
        print(f"  差分処理: {incremental}")

        # 特徴量読み込み
        features_df = pd.read_csv(features_csv, dtype=str)
        print(f"  特徴量レコード: {len(features_df):,} 件")

        # 数値カラムを変換
        numeric_cols = [
            'phase_hearing', 'objection_phase_ratio', 'phase_hearing_ratio',
            'temperature_numeric_value', 'customer_proposes_next_step',
            'customer_asks_implementation', 'hedge_words_count'
        ]
        for col in numeric_cols:
            if col in features_df.columns:
                features_df[col] = pd.to_numeric(features_df[col], errors='coerce')

        # Opportunity読み込み
        self.matcher.load_opportunities(from_date, to_date)

        # 処理
        results = []
        for i, row in features_df.iterrows():
            if (i + 1) % 50 == 0:
                print(f"  進捗: {i + 1:,}/{len(features_df):,}")

            meeting_id = row.get('meeting_id', '')

            # 差分チェック
            if incremental and meeting_id in self.processed_uuids:
                continue

            # Zoomトピック抽出（meeting_idから）
            zoom_topic = meeting_id  # 実際はトピック情報が必要
            zoom_date = row.get('meeting_date', '')  # カラム名は要調整

            features = row.to_dict()

            result = self.process_single(
                zoom_meeting_id=meeting_id,
                zoom_topic=zoom_topic,
                zoom_date=zoom_date,
                features=features,
                dry_run=dry_run
            )

            results.append(result)

        results_df = pd.DataFrame(results)

        # 統計
        print(f"\n[処理結果]")
        if not results_df.empty and 'status' in results_df.columns:
            print(results_df['status'].value_counts().to_string())

        # 結果保存
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_path = self.output_dir / f"sync_results_{timestamp}.csv"
        results_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"\n結果保存: {output_path}")

        return results_df

    def run_scheduled(
        self,
        from_date: str = None,
        to_date: str = None,
        dry_run: bool = False
    ):
        """
        スケジュール実行用エントリポイント

        Args:
            from_date: 開始日（デフォルト: 7日前）
            to_date: 終了日（デフォルト: 今日）
            dry_run: ドライラン
        """
        print(f"\n{'=' * 60}")
        print(f"Zoom-Salesforce同期パイプライン")
        print(f"実行日時: {datetime.now().isoformat()}")
        print(f"{'=' * 60}")

        # デフォルト日付設定
        if not to_date:
            to_date = datetime.now().strftime('%Y-%m-%d')
        if not from_date:
            from_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

        print(f"対象期間: {from_date} 〜 {to_date}")

        # 認証
        self.authenticate()

        # フィールド確認
        if not self.check_zoom_fields():
            print("\n⛔ カスタムフィールドが不足しています。処理を中断します。")
            return

        # 特徴量ファイル確認
        features_csv = self.zoom_project / "features_combined_627_with_sf_attributes.csv"
        if not features_csv.exists():
            print(f"\n⛔ 特徴量ファイルが見つかりません: {features_csv}")
            return

        # 処理実行
        results_df = self.process_from_features_csv(
            features_csv=str(features_csv),
            from_date=from_date,
            to_date=to_date,
            dry_run=dry_run,
            incremental=True
        )

        print(f"\n{'=' * 60}")
        print(f"処理完了")
        print(f"{'=' * 60}")

        return results_df


# ========================================
# CLI
# ========================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Zoom-Salesforce同期パイプライン")
    parser.add_argument("--from-date", type=str, help="開始日 (YYYY-MM-DD)")
    parser.add_argument("--to-date", type=str, help="終了日 (YYYY-MM-DD)")
    parser.add_argument("--features-csv", type=str, help="特徴量CSVパス")
    parser.add_argument("--dry-run", action="store_true", help="ドライラン（実際の更新を行わない）")
    parser.add_argument("--incremental", action="store_true", default=True, help="差分処理（処理済みをスキップ）")
    parser.add_argument("--check-fields", action="store_true", help="Salesforceフィールド確認のみ")

    args = parser.parse_args()

    pipeline = ZoomSFSyncPipeline()
    pipeline.authenticate()

    if args.check_fields:
        pipeline.check_zoom_fields()
    elif args.features_csv:
        pipeline.process_from_features_csv(
            features_csv=args.features_csv,
            from_date=args.from_date,
            to_date=args.to_date,
            dry_run=args.dry_run,
            incremental=args.incremental
        )
    else:
        pipeline.run_scheduled(
            from_date=args.from_date,
            to_date=args.to_date,
            dry_run=args.dry_run
        )
