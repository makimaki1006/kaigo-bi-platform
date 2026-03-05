"""
メイン実行スクリプト
CSVインポート → クレンジング → 突合 → Salesforce同期のパイプラインを実行
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from loguru import logger

from src.scrapers.csv_importer import CSVImporter
from src.services.cleansing_service import CleansingService
from src.services.matching_service import MatchingService
from src.services.sync_service import SyncService
from src.models.lead import SalesforceLeadData


def setup_logger(log_dir: Path) -> None:
    """ロガーをセットアップ"""
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    logger.add(
        log_file,
        rotation="10 MB",
        retention="7 days",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    )


def parse_args() -> argparse.Namespace:
    """コマンドライン引数をパース"""
    parser = argparse.ArgumentParser(
        description="CSV → Salesforce 同期パイプライン"
    )

    parser.add_argument(
        "csv_file",
        type=str,
        help="インポートするCSVファイルのパス"
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="テスト実行（Salesforceへの書き込みをスキップ）"
    )

    parser.add_argument(
        "--no-sync",
        action="store_true",
        help="Salesforce同期をスキップ（クレンジングと突合のみ）"
    )

    parser.add_argument(
        "--skip-update",
        action="store_true",
        help="既存リードの更新をスキップ"
    )

    parser.add_argument(
        "--skip-create",
        action="store_true",
        help="新規リードの作成をスキップ"
    )

    parser.add_argument(
        "--output-dir",
        type=str,
        default="data/output",
        help="出力ディレクトリ（デフォルト: data/output）"
    )

    parser.add_argument(
        "--similarity-threshold",
        type=float,
        default=0.85,
        help="会社名類似度の閾値（デフォルト: 0.85）"
    )

    parser.add_argument(
        "--encoding",
        type=str,
        default="utf-8-sig",
        help="CSVファイルのエンコーディング（デフォルト: utf-8-sig）"
    )

    return parser.parse_args()


def main():
    """メイン処理"""
    args = parse_args()

    # ログ設定
    log_dir = project_root / "logs"
    setup_logger(log_dir)

    print("=" * 60)
    print("CSV → Salesforce 同期パイプライン")
    print("=" * 60)

    csv_path = Path(args.csv_file)
    if not csv_path.exists():
        logger.error(f"CSVファイルが見つかりません: {csv_path}")
        sys.exit(1)

    output_dir = project_root / args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        # ========================================
        # Step 1: CSVインポート
        # ========================================
        print("\n📥 Step 1: CSVインポート")
        print("-" * 40)

        importer = CSVImporter(encoding=args.encoding)
        leads = importer.import_csv(csv_path)

        print(f"  インポート件数: {len(leads)}件")

        if not leads:
            logger.warning("インポートされたデータがありません")
            sys.exit(0)

        # ========================================
        # Step 2: データクレンジング
        # ========================================
        print("\n🧹 Step 2: データクレンジング")
        print("-" * 40)

        cleansing_service = CleansingService()
        cleansing_results = cleansing_service.cleanse_leads(leads)

        # 統計表示
        stats = cleansing_service.get_statistics(cleansing_results)
        print(f"  有効データ: {stats['valid']}件")
        print(f"  無効データ: {stats['invalid']}件")
        print(f"  重複データ: {stats['duplicates']}件")
        print(f"  有効率: {stats['valid_rate']:.1f}%")

        # 有効なリードのみ抽出
        valid_leads = cleansing_service.get_valid_leads(cleansing_results)

        if not valid_leads:
            logger.warning("有効なデータがありません")
            sys.exit(0)

        # ========================================
        # Step 3: Salesforceデータ取得 & 突合
        # ========================================
        print("\n🔍 Step 3: Salesforce突合")
        print("-" * 40)

        sync_service = SyncService(dry_run=args.dry_run)
        sync_service.authenticate()

        # Salesforceからリードを取得
        print("  Salesforceからリードを取得中...")
        sf_records = sync_service.get_sf_leads()

        # SalesforceLeadDataに変換
        sf_leads = [
            SalesforceLeadData(
                id=r["Id"],
                company=r.get("Company", ""),
                phone=r.get("Phone"),
                street=r.get("Street"),
                city=r.get("City"),
                state=r.get("State"),
                postal_code=r.get("PostalCode"),
                country=r.get("Country"),
                website=r.get("Website"),
                industry=r.get("Industry"),
                last_name=r.get("LastName", ""),
                first_name=r.get("FirstName"),
                email=r.get("Email"),
                title=r.get("Title"),
                status=r.get("Status"),
            )
            for r in sf_records
        ]

        print(f"  Salesforceリード: {len(sf_leads)}件")

        # 突合実行
        matching_service = MatchingService(
            similarity_threshold=args.similarity_threshold
        )
        match_results = matching_service.match_leads(valid_leads, sf_leads)

        # 統計表示
        match_stats = matching_service.get_statistics(match_results)
        print(f"  一致: {match_stats['matched']}件")
        print(f"  新規: {match_stats['new']}件")
        print(f"  一致率: {match_stats['match_rate']:.1f}%")

        # 結果をCSVにエクスポート
        result_file = output_dir / f"match_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        sync_service.export_results(match_results, str(result_file))

        # ========================================
        # Step 4: Salesforce同期
        # ========================================
        if not args.no_sync:
            print("\n📤 Step 4: Salesforce同期")
            print("-" * 40)

            if args.dry_run:
                print("  [DRY RUN モード]")

            sync_result = sync_service.sync_leads(
                match_results,
                update_existing=not args.skip_update,
                create_new=not args.skip_create,
            )

            print(f"  作成: {sync_result['created']}件")
            print(f"  更新: {sync_result['updated']}件")
            print(f"  スキップ: {sync_result['skipped']}件")

            if sync_result["errors"]:
                print(f"  エラー: {len(sync_result['errors'])}件")
                for err in sync_result["errors"][:5]:
                    print(f"    - {err}")
        else:
            print("\n⏭️ Salesforce同期はスキップされました")

        # ========================================
        # 完了
        # ========================================
        print("\n" + "=" * 60)
        print("✅ パイプライン完了")
        print("=" * 60)
        print(f"  結果ファイル: {result_file}")

    except Exception as e:
        logger.exception(f"パイプラインエラー: {e}")
        print(f"\n❌ エラー: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
