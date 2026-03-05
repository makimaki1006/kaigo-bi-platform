"""
ハローワークデータ処理サービス
既存の突合ロジックを踏襲した専用処理モジュール
"""

import re
import gzip
import io
import zipfile
from pathlib import Path
from typing import Optional

import pandas as pd
from loguru import logger


# エンコーディング試行順
ENCODINGS = ["utf-8-sig", "utf-8", "cp932", "shift_jis", "euc_jp"]


class HelloWorkService:
    """
    ハローワークデータ処理サービス

    既存ロジックを踏襲:
    - 電話番号正規化（数字のみ + 先頭に'）
    - 取引先・責任者との突合（melt方式、複数電話番号フィールド対応）
    - 差分結合（新規リード候補抽出）
    """

    # 取引先の電話番号フィールド（優先順）
    ACCOUNT_PHONE_FIELDS = ["Phone", "Phone2__c"]

    # 責任者の電話番号フィールド（優先順）
    CONTACT_PHONE_FIELDS = ["Phone", "Phone2__c", "MobilePhone", "MobilePhone2__c"]

    # リードの電話番号フィールド（優先順）
    LEAD_PHONE_FIELDS = ["Phone", "MobilePhone", "Phone2__c", "MobilePhone2__c"]

    def __init__(
        self,
        phone_column: str = "選考担当者ＴＥＬ",
        normalized_phone_column: str = None,
    ):
        """
        初期化

        Args:
            phone_column: ハローワークCSVの電話番号カラム名
            normalized_phone_column: 正規化後のカラム名（Noneで自動生成）
        """
        self.phone_column = phone_column
        self.normalized_phone_column = normalized_phone_column or f"{phone_column}_加工"

    # =========================================================================
    # CSV読み込みユーティリティ
    # =========================================================================

    @staticmethod
    def read_csv_auto(path: Path) -> pd.DataFrame:
        """
        圧縮・文字コードに頑強なCSV読み込み

        Args:
            path: CSVファイルパス

        Returns:
            pd.DataFrame: 読み込んだデータ（全て文字列型）
        """
        path = Path(path)

        # ファイル読み込み（圧縮対応）
        try:
            if path.suffix.lower() == ".gz":
                with gzip.open(path, "rb") as gz:
                    raw = gz.read()
            elif path.suffix.lower() == ".zip":
                with zipfile.ZipFile(path) as zf:
                    raw = zf.read(zf.namelist()[0])
            else:
                raw = path.read_bytes()
        except FileNotFoundError:
            raise RuntimeError(f"ファイルが見つかりません: {path}")
        except PermissionError:
            raise RuntimeError(f"ファイルを開けません（Excel等で開いている可能性）: {path}")
        except Exception as e:
            raise RuntimeError(f"ファイル読み込み失敗: {e}")

        # 文字コード自動判定
        last_err = None
        for enc in ENCODINGS:
            try:
                return pd.read_csv(
                    io.BytesIO(raw),
                    encoding=enc,
                    dtype=str,
                    low_memory=False,
                    on_bad_lines="warn",
                )
            except Exception as e:
                last_err = e

        raise RuntimeError(f"全エンコーディングで読み込み失敗: {last_err}")

    # =========================================================================
    # 電話番号正規化
    # =========================================================================

    @staticmethod
    def normalize_phone(val) -> Optional[str]:
        """
        電話番号を正規化（既存ロジック踏襲）

        - 数字以外を全て除去
        - 先頭に ' を付与（Excel数値化防止）

        Args:
            val: 電話番号値

        Returns:
            Optional[str]: 正規化された電話番号
        """
        if pd.isna(val):
            return pd.NA
        digits = re.sub(r"[^\d]", "", str(val))
        return f"'{digits}" if digits else pd.NA

    @staticmethod
    def normalize_phone_digits_only(val) -> Optional[str]:
        """
        電話番号を数字のみに正規化（'なし）

        Args:
            val: 電話番号値

        Returns:
            Optional[str]: 数字のみの電話番号
        """
        if pd.isna(val):
            return None
        digits = re.sub(r"[^\d]", "", str(val))
        return digits if digits else None

    def normalize_hellowork_csv(
        self,
        input_path: Path,
        output_path: Optional[Path] = None,
    ) -> pd.DataFrame:
        """
        ハローワークCSVの電話番号を正規化

        Args:
            input_path: 入力CSVパス
            output_path: 出力CSVパス（Noneで保存しない）

        Returns:
            pd.DataFrame: 正規化後のデータ
        """
        logger.info(f"ハローワークCSV読み込み: {input_path}")

        df = self.read_csv_auto(input_path)

        # 列名クリーンアップ
        df.columns = df.columns.str.strip().str.replace(r"[\r\n]", "", regex=True)

        logger.info(f"  読み込み: {len(df)} 行 × {len(df.columns)} 列")

        # 電話番号カラム確認
        if self.phone_column not in df.columns:
            raise ValueError(
                f"電話番号カラム '{self.phone_column}' が見つかりません。\n"
                f"列名一覧: {list(df.columns)}"
            )

        # 正規化
        df[self.normalized_phone_column] = df[self.phone_column].apply(self.normalize_phone)

        logger.info(f"  電話番号正規化完了 → '{self.normalized_phone_column}'")

        # 保存
        if output_path:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(output_path, index=False, encoding="utf-8-sig")
            logger.info(f"  保存: {output_path}")

        return df

    # =========================================================================
    # 突合処理（melt方式）
    # =========================================================================

    def match_with_salesforce(
        self,
        hellowork_df: pd.DataFrame,
        salesforce_df: pd.DataFrame,
        sf_phone_fields: list[str],
        output_merged_path: Optional[Path] = None,
        output_diff_path: Optional[Path] = None,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        ハローワークデータとSalesforceデータを電話番号で突合（melt方式）

        Args:
            hellowork_df: ハローワークデータ（正規化済み）
            salesforce_df: Salesforceデータ
            sf_phone_fields: Salesforceの電話番号フィールド（優先順）
            output_merged_path: マージ結果出力パス
            output_diff_path: 差分出力パス

        Returns:
            tuple[pd.DataFrame, pd.DataFrame]: (マージ結果, 差分)
        """
        logger.info(f"突合開始: ハロワ {len(hellowork_df)} 件 vs SF {len(salesforce_df)} 件")
        logger.info(f"  SF電話フィールド: {sf_phone_fields}")

        # 列名クリーンアップ
        salesforce_df.columns = salesforce_df.columns.str.strip().str.replace(r"[\r\n]", "", regex=True)

        # キー列確認
        proc_key = self.normalized_phone_column
        if proc_key not in hellowork_df.columns:
            raise ValueError(f"ハローワークデータに '{proc_key}' がありません")

        for field in sf_phone_fields:
            if field not in salesforce_df.columns:
                logger.warning(f"  SFデータに '{field}' がありません → スキップ")
                sf_phone_fields = [f for f in sf_phone_fields if f != field]

        if not sf_phone_fields:
            raise ValueError("有効な電話番号フィールドがありません")

        # ハローワーク側の重複キー除去
        dup_cnt = hellowork_df[proc_key].duplicated().sum()
        if dup_cnt:
            logger.warning(f"  ハローワーク重複キー {dup_cnt} 件 → 先頭行のみ採用")
            hellowork_df = hellowork_df.drop_duplicates(subset=[proc_key], keep="first")

        # Salesforceデータを縦持ち（melt）に変換
        sf_long = (
            salesforce_df.reset_index()
            .melt(
                id_vars="index",
                value_vars=sf_phone_fields,
                var_name="phone_field",
                value_name="raw_phone"
            )
            .assign(phone_norm=lambda d: d["raw_phone"].apply(self.normalize_phone))
            .dropna(subset=["phone_norm"])
        )

        # 優先順位マップ
        priority_map = {name: idx for idx, name in enumerate(sf_phone_fields)}
        sf_long["priority"] = sf_long["phone_field"].map(priority_map)

        # マージ
        df_merged = pd.merge(
            sf_long,
            hellowork_df,
            left_on="phone_norm",
            right_on=proc_key,
            how="left",
            indicator=True
        )

        # マッチした行を抽出（同一indexで優先順位の高いものを採用）
        matched = (
            df_merged[df_merged["_merge"] == "both"]
            .sort_values(["index", "priority"])
            .drop_duplicates(subset="index", keep="first")
            .set_index("index")
        )

        # 元のSFデータにハローワークデータを結合
        df_result = salesforce_df.copy()
        hw_cols_to_add = [c for c in hellowork_df.columns if c != proc_key]
        add_cols_map = {c: f"{c}_hw" if c in df_result.columns else c for c in hw_cols_to_add}
        df_result = df_result.join(
            matched[hw_cols_to_add].rename(columns=add_cols_map),
            how="left"
        )

        # 差分（マッチしなかったハローワークデータ）
        matched_keys = set(matched[proc_key])
        df_diff = hellowork_df[~hellowork_df[proc_key].isin(matched_keys)]

        logger.info(f"  一致: {len(matched)} 件")
        logger.info(f"  差分（未マッチ）: {len(df_diff)} 件")

        # 保存
        if output_merged_path:
            output_merged_path = Path(output_merged_path)
            output_merged_path.parent.mkdir(parents=True, exist_ok=True)
            df_result.to_csv(output_merged_path, index=False, encoding="utf-8-sig")
            logger.info(f"  マージ結果保存: {output_merged_path}")

        if output_diff_path:
            output_diff_path = Path(output_diff_path)
            output_diff_path.parent.mkdir(parents=True, exist_ok=True)
            df_diff.to_csv(output_diff_path, index=False, encoding="utf-8-sig")
            logger.info(f"  差分保存: {output_diff_path}")

        return df_result, df_diff

    def match_with_accounts(
        self,
        hellowork_df: pd.DataFrame,
        accounts_df: pd.DataFrame,
        output_dir: Optional[Path] = None,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        取引先（Account）との突合

        Args:
            hellowork_df: ハローワークデータ
            accounts_df: 取引先データ
            output_dir: 出力ディレクトリ

        Returns:
            tuple: (マージ結果, 差分)
        """
        logger.info("=== 取引先との突合 ===")

        merged_path = Path(output_dir) / "merged_取引先.csv" if output_dir else None
        diff_path = Path(output_dir) / "diff_取引先_not_matched.csv" if output_dir else None

        return self.match_with_salesforce(
            hellowork_df,
            accounts_df,
            self.ACCOUNT_PHONE_FIELDS,
            merged_path,
            diff_path,
        )

    def match_with_contacts(
        self,
        hellowork_df: pd.DataFrame,
        contacts_df: pd.DataFrame,
        output_dir: Optional[Path] = None,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        責任者（Contact）との突合

        Args:
            hellowork_df: ハローワークデータ
            contacts_df: 責任者データ
            output_dir: 出力ディレクトリ

        Returns:
            tuple: (マージ結果, 差分)
        """
        logger.info("=== 責任者との突合 ===")

        merged_path = Path(output_dir) / "merged_責任者.csv" if output_dir else None
        diff_path = Path(output_dir) / "diff_責任者_not_matched.csv" if output_dir else None

        return self.match_with_salesforce(
            hellowork_df,
            contacts_df,
            self.CONTACT_PHONE_FIELDS,
            merged_path,
            diff_path,
        )

    def match_with_leads(
        self,
        hellowork_df: pd.DataFrame,
        leads_df: pd.DataFrame,
        output_dir: Optional[Path] = None,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        リード（Lead）との突合

        Args:
            hellowork_df: ハローワークデータ
            leads_df: リードデータ
            output_dir: 出力ディレクトリ

        Returns:
            tuple: (マージ結果, 差分)
        """
        logger.info("=== リードとの突合 ===")

        merged_path = Path(output_dir) / "merged_リード.csv" if output_dir else None
        diff_path = Path(output_dir) / "diff_リード_not_matched.csv" if output_dir else None

        return self.match_with_salesforce(
            hellowork_df,
            leads_df,
            self.LEAD_PHONE_FIELDS,
            merged_path,
            diff_path,
        )

    # =========================================================================
    # 差分結合
    # =========================================================================

    def combine_diffs(
        self,
        diff_account: pd.DataFrame,
        diff_contact: pd.DataFrame,
        diff_lead: Optional[pd.DataFrame] = None,
        output_path: Optional[Path] = None,
    ) -> pd.DataFrame:
        """
        取引先・責任者・リードの差分を結合（新規リード候補）

        Account/Contact/Leadいずれにもマッチしなかったレコードのみ抽出

        Args:
            diff_account: 取引先差分
            diff_contact: 責任者差分
            diff_lead: リード差分（オプション）
            output_path: 出力パス

        Returns:
            pd.DataFrame: 結合後の差分（重複除去済み）
        """
        logger.info("=== 差分結合 ===")

        proc_key = self.normalized_phone_column

        # 各差分の電話番号キーセットを取得
        account_keys = set(diff_account[proc_key].dropna())
        contact_keys = set(diff_contact[proc_key].dropna())

        if diff_lead is not None:
            lead_keys = set(diff_lead[proc_key].dropna())
            # 3つ全てでマッチしなかった電話番号 = 真の新規リード候補
            common_keys = account_keys & contact_keys & lead_keys
            logger.info(f"  取引先差分: {len(account_keys)} 件")
            logger.info(f"  責任者差分: {len(contact_keys)} 件")
            logger.info(f"  リード差分: {len(lead_keys)} 件")
        else:
            # Lead差分なし（後方互換）
            common_keys = account_keys & contact_keys
            logger.info(f"  取引先差分: {len(account_keys)} 件")
            logger.info(f"  責任者差分: {len(contact_keys)} 件")
            logger.info("  リード差分: なし（Lead突合スキップ）")

        # 共通キーでフィルタ（Account差分を基準にする）
        df_combined = diff_account[diff_account[proc_key].isin(common_keys)].copy()

        # 重複キー除去
        dup_cnt = df_combined[proc_key].duplicated().sum()
        if dup_cnt:
            logger.warning(f"  重複キー {dup_cnt} 件 → 先頭行のみ採用")
            df_combined = df_combined.drop_duplicates(subset=[proc_key], keep="first")

        logger.info(f"  結合後: {len(df_combined)} 件（新規リード候補）")

        # 保存
        if output_path:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            df_combined.to_csv(output_path, index=False, encoding="utf-8-sig")
            logger.info(f"  保存: {output_path}")

        return df_combined

    # =========================================================================
    # 契約先フィルタ
    # =========================================================================

    def filter_by_contract_accounts(
        self,
        df: pd.DataFrame,
        contract_accounts_df: pd.DataFrame,
        contract_phone_column: str = "Phone",
        output_path: Optional[Path] = None,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        契約先（成約済み取引先）を除外

        契約先の電話番号と照合し、該当するデータを除外することで
        新規リード候補から既存契約先を除外する

        Args:
            df: フィルタリング対象データ（新規リード候補など）
            contract_accounts_df: 契約先データ
            contract_phone_column: 契約先の電話番号カラム名
            output_path: 除外後のデータ出力パス

        Returns:
            tuple[pd.DataFrame, pd.DataFrame]: (フィルタ後データ, 除外されたデータ)
        """
        logger.info("=== 契約先フィルタ ===")
        logger.info(f"  対象: {len(df)} 件")
        logger.info(f"  契約先: {len(contract_accounts_df)} 件")

        proc_key = self.normalized_phone_column

        # 契約先電話番号を正規化
        contract_phones = (
            contract_accounts_df[contract_phone_column]
            .apply(self.normalize_phone)
            .dropna()
            .unique()
        )
        contract_phone_set = set(contract_phones)

        logger.info(f"  契約先電話番号（有効）: {len(contract_phone_set)} 件")

        # フィルタリング
        is_contract = df[proc_key].isin(contract_phone_set)
        df_excluded = df[is_contract]
        df_filtered = df[~is_contract]

        logger.info(f"  除外（契約先に該当）: {len(df_excluded)} 件")
        logger.info(f"  残り（新規リード候補）: {len(df_filtered)} 件")

        # 保存
        if output_path:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            df_filtered.to_csv(output_path, index=False, encoding="utf-8-sig")
            logger.info(f"  保存: {output_path}")

        return df_filtered, df_excluded

    # =========================================================================
    # 一括処理パイプライン
    # =========================================================================

    def run_full_pipeline(
        self,
        hellowork_csv: Path,
        accounts_csv: Path,
        contacts_csv: Path,
        output_dir: Path,
        leads_csv: Optional[Path] = None,
        contract_accounts_csv: Optional[Path] = None,
        contract_phone_column: str = "Phone",
    ) -> dict:
        """
        ハローワーク突合パイプラインを一括実行

        Args:
            hellowork_csv: ハローワークCSVパス
            accounts_csv: 取引先CSVパス
            contacts_csv: 責任者CSVパス
            output_dir: 出力ディレクトリ
            contract_accounts_csv: 契約先CSVパス（オプション、指定時は契約先を除外）
            contract_phone_column: 契約先の電話番号カラム名

        Returns:
            dict: 処理結果統計
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        logger.info("=" * 60)
        logger.info("ハローワーク突合パイプライン開始")
        logger.info("=" * 60)

        # Step 1: 電話番号正規化
        logger.info("\n[Step 1] 電話番号正規化")
        hw_df = self.normalize_hellowork_csv(
            hellowork_csv,
            output_dir / "processed_hellowork.csv"
        )

        # Step 2: 取引先との突合
        logger.info("\n[Step 2] 取引先との突合")
        accounts_df = self.read_csv_auto(accounts_csv)
        merged_accounts, diff_accounts = self.match_with_accounts(
            hw_df, accounts_df, output_dir
        )

        # Step 3: 責任者との突合
        logger.info("\n[Step 3] 責任者との突合")
        contacts_df = self.read_csv_auto(contacts_csv)
        merged_contacts, diff_contacts = self.match_with_contacts(
            hw_df, contacts_df, output_dir
        )

        # Step 4: リードとの突合（オプション）
        diff_leads = None
        if leads_csv:
            logger.info("\n[Step 4] リードとの突合")
            leads_df = self.read_csv_auto(leads_csv)
            merged_leads, diff_leads = self.match_with_leads(
                hw_df, leads_df, output_dir
            )

        # Step 5: 差分結合
        logger.info("\n[Step 5] 差分結合")
        new_leads = self.combine_diffs(
            diff_accounts,
            diff_contacts,
            diff_leads,
            output_dir / "combined_diff_new_leads.csv"
        )

        # Step 5: 契約先フィルタ（オプション）
        excluded_count = 0
        if contract_accounts_csv:
            logger.info("\n[Step 5] 契約先フィルタ")
            contract_df = self.read_csv_auto(contract_accounts_csv)
            new_leads, excluded = self.filter_by_contract_accounts(
                new_leads,
                contract_df,
                contract_phone_column,
                output_dir / "final_new_leads.csv"
            )
            excluded_count = len(excluded)

            # 除外データも保存
            excluded.to_csv(
                output_dir / "excluded_contract_matches.csv",
                index=False,
                encoding="utf-8-sig"
            )
            logger.info(f"  除外データ保存: excluded_contract_matches.csv")

        # 統計
        stats = {
            "hellowork_total": len(hw_df),
            "accounts_matched": len(merged_accounts) - merged_accounts.iloc[:, -1].isna().sum(),
            "contacts_matched": len(merged_contacts) - merged_contacts.iloc[:, -1].isna().sum(),
            "new_leads_before_contract_filter": len(new_leads) + excluded_count,
            "contract_excluded": excluded_count,
            "new_leads": len(new_leads),
        }

        logger.info("\n" + "=" * 60)
        logger.info("パイプライン完了")
        logger.info("=" * 60)
        logger.info(f"  ハローワーク入力: {stats['hellowork_total']} 件")
        logger.info(f"  取引先マッチ: {stats['accounts_matched']} 件")
        logger.info(f"  責任者マッチ: {stats['contacts_matched']} 件")
        if contract_accounts_csv:
            logger.info(f"  契約先除外: {stats['contract_excluded']} 件")
        logger.info(f"  最終新規リード候補: {stats['new_leads']} 件")

        return stats
