"""
ハローワークデータ突合パイプライン
CSVファイルまたはSalesforce APIからデータを取得して突合処理を実行
"""

import argparse
import sys
import io
from pathlib import Path
from datetime import datetime

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from loguru import logger

from src.services.hellowork_service import HelloWorkService


def setup_logger(log_dir: Path) -> None:
    """ロガーをセットアップ"""
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"hellowork_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    logger.add(
        log_file,
        rotation="10 MB",
        retention="7 days",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    )


def parse_args() -> argparse.Namespace:
    """コマンドライン引数をパース"""
    parser = argparse.ArgumentParser(
        description="ハローワークデータ突合パイプライン"
    )

    parser.add_argument(
        "hellowork_csv",
        type=str,
        help="ハローワークCSVファイルのパス"
    )

    parser.add_argument(
        "--accounts-csv",
        type=str,
        help="取引先CSVファイルのパス（指定しない場合はSalesforceから取得）"
    )

    parser.add_argument(
        "--contacts-csv",
        type=str,
        help="責任者CSVファイルのパス（指定しない場合はSalesforceから取得）"
    )

    parser.add_argument(
        "--leads-csv",
        type=str,
        help="リードCSVファイルのパス（指定しない場合はSalesforceから取得）"
    )

    parser.add_argument(
        "--output-dir",
        type=str,
        default="data/output/hellowork",
        help="出力ディレクトリ（デフォルト: data/output/hellowork）"
    )

    parser.add_argument(
        "--phone-column",
        type=str,
        default="選考担当者ＴＥＬ",
        help="ハローワークCSVの電話番号カラム名（デフォルト: 選考担当者ＴＥＬ）"
    )

    parser.add_argument(
        "--normalize-only",
        action="store_true",
        help="電話番号正規化のみ実行（突合は行わない）"
    )

    parser.add_argument(
        "--fetch-from-sf",
        action="store_true",
        help="取引先・責任者をSalesforce APIから取得"
    )

    parser.add_argument(
        "--contract-csv",
        type=str,
        help="契約先CSVファイルのパス（指定時は契約先を除外）"
    )

    parser.add_argument(
        "--contract-phone-column",
        type=str,
        default="Phone",
        help="契約先CSVの電話番号カラム名（デフォルト: Phone）"
    )

    parser.add_argument(
        "--fetch-contract",
        action="store_true",
        help="契約先データをSalesforce APIから取得"
    )

    return parser.parse_args()


def fetch_salesforce_data(output_dir: Path) -> tuple[Path, Path, Path]:
    """
    Salesforce APIから取引先・責任者・リードデータを取得

    Returns:
        tuple[Path, Path, Path]: (取引先CSVパス, 責任者CSVパス, リードCSVパス)
    """
    from scripts.bulk_export import BulkExporter

    logger.info("Salesforceからデータを取得中...")

    exporter = BulkExporter()
    exporter.authenticate()

    # 取引先
    logger.info("  取引先(Account)を取得...")
    account_path = exporter.export_object_bulk(
        "Account",
        output_dir,
        fields=["Id", "Name", "Phone", "Phone2__c", "BillingStreet", "BillingCity", "BillingState", "Website"],
    )

    # 責任者
    logger.info("  責任者(Contact)を取得...")
    contact_path = exporter.export_object_bulk(
        "Contact",
        output_dir,
        fields=["Id", "AccountId", "Name", "Phone", "Phone2__c", "MobilePhone", "MobilePhone2__c", "Email"],
    )

    # リード
    logger.info("  リード(Lead)を取得...")
    lead_path = exporter.export_object_bulk(
        "Lead",
        output_dir,
        fields=["Id", "Company", "Name", "Phone", "MobilePhone", "Phone2__c", "MobilePhone2__c", "Status"],
    )

    return account_path, contact_path, lead_path


def fetch_contract_accounts(output_dir: Path) -> Path:
    """
    Salesforce APIから契約先データを取得

    Returns:
        Path: 契約先CSVパス
    """
    from scripts.export_contract_report import export_via_bulk_api
    from datetime import datetime

    logger.info("契約先データをSalesforceから取得中...")
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    contract_path = export_via_bulk_api(output_dir, timestamp)
    logger.info(f"  契約先取得完了: {contract_path}")

    return contract_path


def main():
    """メイン処理"""
    args = parse_args()

    # ログ設定
    log_dir = project_root / "logs"
    setup_logger(log_dir)

    print("=" * 60)
    print("ハローワークデータ突合パイプライン")
    print("=" * 60)

    # パス設定
    hellowork_csv = Path(args.hellowork_csv)
    output_dir = project_root / args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    if not hellowork_csv.exists():
        logger.error(f"ハローワークCSVが見つかりません: {hellowork_csv}")
        sys.exit(1)

    # サービス初期化
    service = HelloWorkService(phone_column=args.phone_column)

    try:
        # ========================================
        # 電話番号正規化
        # ========================================
        print("\n📞 電話番号正規化")
        print("-" * 40)

        hw_df = service.normalize_hellowork_csv(
            hellowork_csv,
            output_dir / f"processed_{hellowork_csv.name}"
        )

        print(f"  入力: {len(hw_df)} 件")

        if args.normalize_only:
            print("\n✅ 正規化完了（--normalize-only指定のため突合はスキップ）")
            return

        # ========================================
        # 取引先・責任者・リードデータ取得
        # ========================================
        leads_csv = None
        if args.fetch_from_sf:
            print("\n📥 Salesforceからデータ取得")
            print("-" * 40)
            accounts_csv, contacts_csv, leads_csv = fetch_salesforce_data(output_dir)
        else:
            if not args.accounts_csv or not args.contacts_csv:
                logger.error(
                    "取引先・責任者CSVを指定するか、--fetch-from-sf オプションを使用してください"
                )
                sys.exit(1)

            accounts_csv = Path(args.accounts_csv)
            contacts_csv = Path(args.contacts_csv)

            if not accounts_csv.exists():
                logger.error(f"取引先CSVが見つかりません: {accounts_csv}")
                sys.exit(1)
            if not contacts_csv.exists():
                logger.error(f"責任者CSVが見つかりません: {contacts_csv}")
                sys.exit(1)

            # リードCSVがある場合
            if args.leads_csv:
                leads_csv = Path(args.leads_csv)
                if not leads_csv.exists():
                    logger.error(f"リードCSVが見つかりません: {leads_csv}")
                    sys.exit(1)

        # ========================================
        # 取引先との突合
        # ========================================
        print("\n🏢 取引先との突合")
        print("-" * 40)

        accounts_df = service.read_csv_auto(accounts_csv)
        print(f"  取引先: {len(accounts_df)} 件")

        merged_accounts, diff_accounts = service.match_with_accounts(
            hw_df, accounts_df, output_dir
        )

        # ========================================
        # 責任者との突合
        # ========================================
        print("\n👤 責任者との突合")
        print("-" * 40)

        contacts_df = service.read_csv_auto(contacts_csv)
        print(f"  責任者: {len(contacts_df)} 件")

        merged_contacts, diff_contacts = service.match_with_contacts(
            hw_df, contacts_df, output_dir
        )

        # ========================================
        # リードとの突合
        # ========================================
        diff_leads = None
        if leads_csv:
            print("\n📋 リードとの突合")
            print("-" * 40)

            leads_df = service.read_csv_auto(leads_csv)
            print(f"  リード: {len(leads_df)} 件")

            merged_leads, diff_leads = service.match_with_leads(
                hw_df, leads_df, output_dir
            )
        else:
            print("\n⚠️ リードCSVなし → リード突合スキップ")

        # ========================================
        # 差分結合（新規リード候補）
        # ========================================
        print("\n🆕 差分結合（新規リード候補）")
        print("-" * 40)

        new_leads = service.combine_diffs(
            diff_accounts,
            diff_contacts,
            diff_leads,
            output_dir / "combined_diff_new_leads.csv"
        )

        # ========================================
        # 契約先フィルタ（オプション）
        # ========================================
        contract_csv = None
        excluded_count = 0

        if args.fetch_contract:
            print("\n📋 契約先データ取得（Salesforce）")
            print("-" * 40)
            contract_csv = fetch_contract_accounts(output_dir)

        elif args.contract_csv:
            contract_csv = Path(args.contract_csv)
            if not contract_csv.exists():
                logger.error(f"契約先CSVが見つかりません: {contract_csv}")
                sys.exit(1)

        if contract_csv:
            print("\n🔍 契約先フィルタ")
            print("-" * 40)

            contract_df = service.read_csv_auto(contract_csv)
            print(f"  契約先: {len(contract_df)} 件")

            new_leads, excluded = service.filter_by_contract_accounts(
                new_leads,
                contract_df,
                args.contract_phone_column,
                output_dir / "final_new_leads.csv"
            )
            excluded_count = len(excluded)

            # 除外データも保存
            excluded.to_csv(
                output_dir / "excluded_contract_matches.csv",
                index=False,
                encoding="utf-8-sig"
            )
            print(f"  契約先除外: {excluded_count} 件")
            print(f"  最終新規リード候補: {len(new_leads)} 件")

        # ========================================
        # 完了
        # ========================================
        print("\n" + "=" * 60)
        print("パイプライン完了")
        print("=" * 60)

        print(f"\n出力ファイル:")
        print(f"  - processed_{hellowork_csv.name}")
        print(f"  - merged_取引先.csv")
        print(f"  - merged_責任者.csv")
        if leads_csv:
            print(f"  - merged_リード.csv")
        print(f"  - diff_取引先_not_matched.csv")
        print(f"  - diff_責任者_not_matched.csv")
        if leads_csv:
            print(f"  - diff_リード_not_matched.csv")
        print(f"  - combined_diff_new_leads.csv（差分結合）")
        if contract_csv:
            print(f"  - final_new_leads.csv（最終新規リード候補: {len(new_leads)} 件）")
            print(f"  - excluded_contract_matches.csv（契約先除外: {excluded_count} 件）")
        else:
            print(f"  新規リード候補: {len(new_leads)} 件")

        print(f"\n出力先: {output_dir}")

    except Exception as e:
        logger.exception(f"パイプラインエラー: {e}")
        print(f"\n❌ エラー: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
