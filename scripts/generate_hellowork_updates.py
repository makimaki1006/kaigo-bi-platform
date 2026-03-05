"""
ハローワークデータ更新用CSV生成スクリプト
突合結果から各オブジェクト用の更新データを生成

更新内容:
- 基本フィールド（従業員数、郵便番号、住所、Webサイト等）
- ハローワーク専用フィールド（求人公開日、産業区分、募集職種等）
- メモフィールド（更新ログ形式、★区切りで追記）
"""

import sys
import re
from pathlib import Path
from datetime import datetime
from typing import Optional

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
from loguru import logger


class HelloWorkFieldMapper:
    """
    ハローワークデータ → Salesforceフィールドマッピング

    突合結果から各オブジェクト用の更新データを生成
    """

    # 今日の日付
    TODAY = datetime.now().strftime('%Y%m%d')
    TODAY_DATE = datetime.now().strftime('%Y-%m-%d')

    # ハローワークCSVカラム名 → 内部キー
    HW_COLS = {
        'job_number': '求人番号',
        'office_number': '事業所番号',
        'office_name': '事業所名漢字',
        'postal_code': '事業所郵便番号',
        'address': '事業所所在地',
        'website': '事業所ホームページ',
        'job_title': '職種',
        'job_description': '仕事内容',
        'employment_type': '雇用形態',
        'industry': '産業分類（名称）',
        'reception_date': '受付年月日（西暦）',
        'valid_date': '求人有効年月日（西暦）',
        'employee_count_company': '従業員数企業全体（コード）',
        'employee_count_office': '従業員数就業場所（コード）',
        'president_name': '代表者名',
        'president_title': '代表者役職',
        'contact_name': '選考担当者氏名漢字',
        'contact_name_kana': '選考担当者氏名フリガナ',
        'contact_dept_title': '選考担当者課係名／役職名',
        'contact_phone': '選考担当者ＴＥＬ',
        'contact_email': '選考担当者Ｅメール',
        'corporate_number': '法人番号',
        'recruitment_reason': '募集理由区分',
        'recruitment_count': '採用人数（コード）',
    }

    # Salesforceフィールド → UI名（ログ用）
    FIELD_LABELS = {
        # Account/Lead 共通基本フィールド
        'CorporateIdentificationNumber__c': '法人番号（入力用）',
        'NumberOfEmployees': '従業員数',
        'BillingPostalCode': '郵便番号(請求先)',
        'BillingStreet': '町名・番地(請求先)',
        'Website': 'Web サイト',
        'ContactName__c': '担当者名',
        'ContactTitle__c': '担当者役職',
        'PresidentName__c': '代表者氏名',
        'PresidentTitle__c': '代表者役職',
        # Lead専用
        'MobilePhone': '携帯電話',
        'Company': '事業所名 / 法人名',
        'PostalCode': '郵便番号',
        'Street': '町名・番地',
        'Email': 'メール',
        # ハローワーク専用フィールド
        'Hellowork_JobPublicationDate__c': '【ハローワーク】求人公開日',
        'Hellowork_JobClosedDate__c': '【ハローワーク】求人掲載終了日',
        'Hellowork_Industry__c': '【ハローワーク】産業区分',
        'Hellowork_RecuritmentType__c': '【ハローワーク】募集職種',
        'Hellowork_EmploymentType__c': '【ハローワーク】雇用形態',
        'Hellowork_RecruitmentReasonCategory__c': '【ハローワーク】募集理由区分',
        'Hellowork_NumberOfRecruitment__c': '【ハローワーク】採用人数',
        'Hellowork_NumberOfEmployee_Office__c': '【ハローワーク】従業員数（就業場所）',
        'Hellowork_DataImportDate__c': '【ハローワーク】データ入稿日',
        'Hellowork_URL__c': '【ハローワーク】求人URL',
        # メモ
        'Publish_ImportText__c': '【掲載情報】入稿データメモ',
        'LeadSourceMemo__c': 'リードソースメモ',
    }

    def __init__(self, merged_df: pd.DataFrame, sf_df: pd.DataFrame, object_type: str):
        """
        初期化

        Args:
            merged_df: 突合結果DataFrame（Salesforce + ハローワーク結合済み）
            sf_df: 元のSalesforceデータ（既存値比較用）
            object_type: オブジェクトタイプ（'Account', 'Contact', 'Lead'）
        """
        self.merged_df = merged_df.copy()
        self.sf_df = sf_df.copy() if sf_df is not None else None
        self.object_type = object_type

        # Salesforce既存値をIDでインデックス
        if self.sf_df is not None and 'Id' in self.sf_df.columns:
            self.sf_df = self.sf_df.set_index('Id')

    def _get_hw_col(self, key: str) -> Optional[str]:
        """ハローワークカラム名を取得"""
        base_name = self.HW_COLS.get(key)
        if not base_name:
            return None
        if base_name in self.merged_df.columns:
            return base_name
        return None

    def _safe_get(self, row: pd.Series, key: str, default: str = '') -> str:
        """安全に値を取得"""
        col = self._get_hw_col(key)
        if col and col in row.index:
            val = row[col]
            if pd.notna(val) and str(val).strip():
                return str(val).strip()
        return default

    def _get_sf_existing_value(self, record_id: str, field: str) -> str:
        """Salesforce既存値を取得"""
        if self.sf_df is None or record_id not in self.sf_df.index:
            return ''
        val = self.sf_df.loc[record_id].get(field, '')
        if pd.isna(val):
            return ''
        return str(val).strip()

    @staticmethod
    def normalize_postal_code(val) -> Optional[str]:
        """郵便番号を正規化（ハイフン付き形式）"""
        if pd.isna(val) or not str(val).strip():
            return None
        digits = re.sub(r'[^\d]', '', str(val))
        if len(digits) == 7:
            return f"{digits[:3]}-{digits[3:]}"
        return None

    @staticmethod
    def validate_url(val) -> Optional[str]:
        """URLを検証"""
        if pd.isna(val) or not str(val).strip():
            return None
        url = str(val).strip()
        if url.startswith('http://') or url.startswith('https://'):
            return url
        return None

    @staticmethod
    def validate_email(val) -> Optional[str]:
        """メールアドレスを検証"""
        if pd.isna(val) or not str(val).strip():
            return None
        email = str(val).strip()
        if '@' in email and '.' in email:
            return email
        return None

    @staticmethod
    def parse_number(val) -> Optional[int]:
        """数値を整数に変換"""
        if pd.isna(val):
            return None
        try:
            return int(float(val))
        except (ValueError, TypeError):
            return None

    @staticmethod
    def normalize_phone(val) -> Optional[str]:
        """電話番号を正規化（数字のみ10-11桁）"""
        if pd.isna(val) or not str(val).strip():
            return None
        digits = re.sub(r'[^\d]', '', str(val))
        if digits.startswith('0') and 10 <= len(digits) <= 11:
            return digits
        return None

    @staticmethod
    def is_mobile_phone(phone: str) -> bool:
        """携帯番号判定（090/080/070始まり）"""
        return bool(phone) and phone[:3] in ('090', '080', '070')

    @staticmethod
    def parse_date(val) -> Optional[str]:
        """日付をYYYY-MM-DD形式に変換"""
        if pd.isna(val) or not str(val).strip():
            return None
        val_str = str(val).strip()
        # YYYY/MM/DD 形式
        match = re.match(r'(\d{4})/(\d{1,2})/(\d{1,2})', val_str)
        if match:
            y, m, d = match.groups()
            return f"{y}-{int(m):02d}-{int(d):02d}"
        return None

    def generate_change_log(self, record_id: str, changes: dict) -> str:
        """
        変更ログを生成

        Args:
            record_id: レコードID
            changes: {フィールド名: 新しい値} の辞書

        Returns:
            str: 変更ログテキスト
        """
        log_lines = [f"[{self.TODAY_DATE} ハロワ突合]"]

        for field, new_value in changes.items():
            if field in ('Id', 'Publish_ImportText__c', 'LeadSourceMemo__c'):
                continue

            # 既存値を取得
            old_value = self._get_sf_existing_value(record_id, field)

            # 変更があった場合のみログに記録
            new_value_str = str(new_value) if new_value is not None else ''
            if old_value != new_value_str and new_value_str:
                # UI名を取得
                label = self.FIELD_LABELS.get(field, field)
                old_display = old_value if old_value else '空'
                log_lines.append(f"{label}: {old_display}→{new_value_str}")

        if len(log_lines) == 1:
            # 変更がない場合
            return ''

        return '\n'.join(log_lines)

    def generate_lead_source_memo(self, row: pd.Series) -> str:
        """
        LeadSourceMemo用のキーワードを生成

        形式: YYYYMMDD_ハロワ_カテゴリ
        """
        # カテゴリは募集職種から抽出（最初の主要キーワード）
        job_title = self._safe_get(row, 'job_title')

        # 職種から主要カテゴリを抽出
        category = ''
        keywords = ['介護', '看護', '医療', '清掃', '調理', '飲食', '事務', '営業', '製造', '配送', '運転']
        for kw in keywords:
            if kw in job_title:
                category = kw
                break

        if not category:
            # カテゴリが見つからない場合は職種の先頭10文字
            category = job_title[:10] if job_title else '不明'

        return f"{self.TODAY}_ハロワ_{category}"

    def generate_account_updates(self) -> pd.DataFrame:
        """Account用更新データを生成"""
        logger.info(f"Account更新データ生成: {len(self.merged_df)} 件")

        # マッチしたデータのみ抽出
        hw_col = self._get_hw_col('job_number')
        if hw_col:
            matched = self.merged_df[self.merged_df[hw_col].notna()].copy()
        else:
            logger.warning("求人番号カラムが見つかりません")
            return pd.DataFrame()

        logger.info(f"  マッチ済み: {len(matched)} 件")

        updates = []
        for idx, row in matched.iterrows():
            record_id = row['Id']
            update = {'Id': record_id}

            # 基本フィールド
            corp_num = self._safe_get(row, 'corporate_number')
            if corp_num:
                update['CorporateIdentificationNumber__c'] = corp_num

            emp = self.parse_number(self._safe_get(row, 'employee_count_company'))
            if emp:
                update['NumberOfEmployees'] = emp

            postal = self.normalize_postal_code(self._safe_get(row, 'postal_code'))
            if postal:
                update['BillingPostalCode'] = postal

            address = self._safe_get(row, 'address')
            if address:
                update['BillingStreet'] = address

            website = self.validate_url(self._safe_get(row, 'website'))
            if website:
                update['Website'] = website

            contact_name = self._safe_get(row, 'contact_name')
            if contact_name:
                update['ContactName__c'] = contact_name

            contact_title = self._safe_get(row, 'contact_dept_title')
            if contact_title:
                update['ContactTitle__c'] = contact_title

            president_name = self._safe_get(row, 'president_name')
            if president_name:
                update['PresidentName__c'] = president_name

            president_title = self._safe_get(row, 'president_title')
            if president_title:
                update['PresidentTitle__c'] = president_title

            # ハローワーク専用フィールド
            pub_date = self.parse_date(self._safe_get(row, 'reception_date'))
            if pub_date:
                update['Hellowork_JobPublicationDate__c'] = pub_date

            closed_date = self.parse_date(self._safe_get(row, 'valid_date'))
            if closed_date:
                update['Hellowork_JobClosedDate__c'] = closed_date

            industry = self._safe_get(row, 'industry')
            if industry:
                update['Hellowork_Industry__c'] = industry

            job_title = self._safe_get(row, 'job_title')
            if job_title:
                update['Hellowork_RecuritmentType__c'] = job_title[:255]

            emp_type = self._safe_get(row, 'employment_type')
            if emp_type:
                update['Hellowork_EmploymentType__c'] = emp_type

            recruit_reason = self._safe_get(row, 'recruitment_reason')
            if recruit_reason:
                update['Hellowork_RecruitmentReasonCategory__c'] = recruit_reason

            recruit_count = self.parse_number(self._safe_get(row, 'recruitment_count'))
            if recruit_count:
                update['Hellowork_NumberOfRecruitment__c'] = recruit_count

            emp_office = self.parse_number(self._safe_get(row, 'employee_count_office'))
            if emp_office:
                update['Hellowork_NumberOfEmployee_Office__c'] = emp_office

            # データ入稿日は今日
            update['Hellowork_DataImportDate__c'] = self.TODAY_DATE

            # 求人URL（事業所ホームページ）
            hw_url = self.validate_url(self._safe_get(row, 'website'))
            if hw_url:
                update['Hellowork_URL__c'] = hw_url

            # 更新ログ生成
            change_log = self.generate_change_log(record_id, update)
            if change_log:
                update['Publish_ImportText_Addition'] = change_log

            updates.append(update)

        df_updates = pd.DataFrame(updates)
        logger.info(f"  生成完了: {len(df_updates)} 件")
        return df_updates

    def generate_contact_updates(self) -> pd.DataFrame:
        """Contact用更新データを生成（メールとメモのみ）"""
        logger.info(f"Contact更新データ生成: {len(self.merged_df)} 件")

        hw_col = self._get_hw_col('job_number')
        if hw_col:
            matched = self.merged_df[self.merged_df[hw_col].notna()].copy()
        else:
            logger.warning("求人番号カラムが見つかりません")
            return pd.DataFrame()

        logger.info(f"  マッチ済み: {len(matched)} 件")

        updates = []
        for idx, row in matched.iterrows():
            record_id = row['Id']
            update = {'Id': record_id}

            # メールアドレスのみ
            email = self.validate_email(self._safe_get(row, 'contact_email'))
            if email:
                update['Email'] = email

            # 更新ログ生成
            change_log = self.generate_change_log(record_id, update)
            if change_log:
                update['Description_Addition'] = change_log

            updates.append(update)

        df_updates = pd.DataFrame(updates)
        logger.info(f"  生成完了: {len(df_updates)} 件")
        return df_updates

    # 更新対象外のLeadステータス
    EXCLUDED_LEAD_STATUSES = ['取引開始済']

    def generate_lead_updates(self) -> pd.DataFrame:
        """Lead用更新データを生成"""
        logger.info(f"Lead更新データ生成: {len(self.merged_df)} 件")

        hw_col = self._get_hw_col('job_number')
        if hw_col:
            matched = self.merged_df[self.merged_df[hw_col].notna()].copy()
        else:
            logger.warning("求人番号カラムが見つかりません")
            return pd.DataFrame()

        logger.info(f"  マッチ済み: {len(matched)} 件")

        # 取引開始済Leadを除外
        if 'Status' in matched.columns:
            before_count = len(matched)
            matched = matched[~matched['Status'].isin(self.EXCLUDED_LEAD_STATUSES)]
            excluded_count = before_count - len(matched)
            if excluded_count > 0:
                logger.info(f"  取引開始済を除外: {excluded_count} 件")
                logger.info(f"  除外後: {len(matched)} 件")

        updates = []
        for idx, row in matched.iterrows():
            record_id = row['Id']
            update = {'Id': record_id}

            # 基本フィールド
            company = self._safe_get(row, 'office_name')
            if company:
                update['Company'] = company

            emp = self.parse_number(self._safe_get(row, 'employee_count_company'))
            if emp:
                update['NumberOfEmployees'] = emp

            postal = self.normalize_postal_code(self._safe_get(row, 'postal_code'))
            if postal:
                update['PostalCode'] = postal

            address = self._safe_get(row, 'address')
            if address:
                update['Street'] = address

            website = self.validate_url(self._safe_get(row, 'website'))
            if website:
                update['Website'] = website

            email = self.validate_email(self._safe_get(row, 'contact_email'))
            if email:
                update['Email'] = email

            president_name = self._safe_get(row, 'president_name')
            if president_name:
                update['PresidentName__c'] = president_name

            president_title = self._safe_get(row, 'president_title')
            if president_title:
                update['PresidentTitle__c'] = president_title

            # 携帯番号更新（SF既存MobilePhone空 かつ HW電話番号が携帯の場合）
            hw_phone = self.normalize_phone(self._safe_get(row, 'contact_phone'))
            if hw_phone and self.is_mobile_phone(hw_phone):
                existing_mobile = self._get_sf_existing_value(record_id, 'MobilePhone')
                if not existing_mobile:
                    update['MobilePhone'] = hw_phone

            # ハローワーク専用フィールド
            pub_date = self.parse_date(self._safe_get(row, 'reception_date'))
            if pub_date:
                update['Hellowork_JobPublicationDate__c'] = pub_date

            closed_date = self.parse_date(self._safe_get(row, 'valid_date'))
            if closed_date:
                update['Hellowork_JobClosedDate__c'] = closed_date

            industry = self._safe_get(row, 'industry')
            if industry:
                update['Hellowork_Industry__c'] = industry

            job_title = self._safe_get(row, 'job_title')
            if job_title:
                update['Hellowork_RecuritmentType__c'] = job_title[:255]

            emp_type = self._safe_get(row, 'employment_type')
            if emp_type:
                update['Hellowork_EmploymentType__c'] = emp_type

            recruit_reason = self._safe_get(row, 'recruitment_reason')
            if recruit_reason:
                update['Hellowork_RecruitmentReasonCategory__c'] = recruit_reason

            recruit_count = self.parse_number(self._safe_get(row, 'recruitment_count'))
            if recruit_count:
                update['Hellowork_NumberOfRecruitment__c'] = recruit_count

            emp_office = self.parse_number(self._safe_get(row, 'employee_count_office'))
            if emp_office:
                update['Hellowork_NumberOfEmployee_Office__c'] = emp_office

            # データ入稿日は今日
            update['Hellowork_DataImportDate__c'] = self.TODAY_DATE

            # 求人URL（事業所ホームページ）
            hw_url = self.validate_url(self._safe_get(row, 'website'))
            if hw_url:
                update['Hellowork_URL__c'] = hw_url

            # LeadSourceMemo（キーワード形式）
            update['LeadSourceMemo_Addition'] = self.generate_lead_source_memo(row)

            # 更新ログ生成
            change_log = self.generate_change_log(record_id, update)
            if change_log:
                update['Publish_ImportText_Addition'] = change_log

            updates.append(update)

        df_updates = pd.DataFrame(updates)
        logger.info(f"  生成完了: {len(df_updates)} 件")
        return df_updates


def load_sf_data(output_dir: Path, object_name: str) -> Optional[pd.DataFrame]:
    """Salesforce元データを読み込み"""
    # 最新のエクスポートファイルを探す
    pattern = f"{object_name}_*.csv"
    parent_dir = output_dir.parent
    files = list(parent_dir.glob(pattern))
    if not files:
        logger.warning(f"  {object_name}の元データが見つかりません")
        return None
    latest = max(files, key=lambda p: p.stat().st_mtime)
    logger.info(f"  SF元データ: {latest.name}")
    return pd.read_csv(latest, dtype=str, encoding='utf-8-sig', low_memory=False)


def generate_all_updates(output_dir: Path) -> dict:
    """
    全オブジェクトの更新データを生成

    Args:
        output_dir: 突合結果が格納されているディレクトリ

    Returns:
        dict: 生成結果統計
    """
    output_dir = Path(output_dir)

    logger.info("=" * 60)
    logger.info("ハローワーク更新データ生成")
    logger.info("=" * 60)

    results = {}

    # Account
    account_merged_path = output_dir / "merged_取引先.csv"
    if account_merged_path.exists():
        logger.info(f"\n[Account] {account_merged_path}")
        df_merged = pd.read_csv(account_merged_path, dtype=str, encoding='utf-8-sig', low_memory=False)
        df_sf = load_sf_data(output_dir, 'Account')
        mapper = HelloWorkFieldMapper(df_merged, df_sf, 'Account')
        df_updates = mapper.generate_account_updates()

        if len(df_updates) > 0:
            output_path = output_dir / "account_full_updates.csv"
            df_updates.to_csv(output_path, index=False, encoding='utf-8-sig')
            logger.info(f"  保存: {output_path}")
            results['Account'] = len(df_updates)

    # Contact
    contact_merged_path = output_dir / "merged_責任者.csv"
    if contact_merged_path.exists():
        logger.info(f"\n[Contact] {contact_merged_path}")
        df_merged = pd.read_csv(contact_merged_path, dtype=str, encoding='utf-8-sig', low_memory=False)
        df_sf = load_sf_data(output_dir, 'Contact')
        mapper = HelloWorkFieldMapper(df_merged, df_sf, 'Contact')
        df_updates = mapper.generate_contact_updates()

        if len(df_updates) > 0:
            output_path = output_dir / "contact_full_updates.csv"
            df_updates.to_csv(output_path, index=False, encoding='utf-8-sig')
            logger.info(f"  保存: {output_path}")
            results['Contact'] = len(df_updates)

    # Lead
    lead_merged_path = output_dir / "merged_リード.csv"
    if lead_merged_path.exists():
        logger.info(f"\n[Lead] {lead_merged_path}")
        df_merged = pd.read_csv(lead_merged_path, dtype=str, encoding='utf-8-sig', low_memory=False)
        df_sf = load_sf_data(output_dir, 'Lead')
        mapper = HelloWorkFieldMapper(df_merged, df_sf, 'Lead')
        df_updates = mapper.generate_lead_updates()

        if len(df_updates) > 0:
            output_path = output_dir / "lead_full_updates.csv"
            df_updates.to_csv(output_path, index=False, encoding='utf-8-sig')
            logger.info(f"  保存: {output_path}")
            results['Lead'] = len(df_updates)

    logger.info("\n" + "=" * 60)
    logger.info("生成完了")
    logger.info("=" * 60)
    for obj, count in results.items():
        logger.info(f"  {obj}: {count} 件")

    return results


def main():
    """メイン処理"""
    import argparse

    parser = argparse.ArgumentParser(description='ハローワーク更新データ生成')
    parser.add_argument(
        '--output-dir',
        type=str,
        default='data/output/hellowork',
        help='突合結果ディレクトリ（デフォルト: data/output/hellowork）'
    )

    args = parser.parse_args()

    output_dir = project_root / args.output_dir

    if not output_dir.exists():
        logger.error(f"ディレクトリが見つかりません: {output_dir}")
        sys.exit(1)

    results = generate_all_updates(output_dir)

    if not results:
        logger.warning("更新データが生成されませんでした")
        sys.exit(1)


if __name__ == "__main__":
    main()
