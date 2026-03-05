"""
成約先除外スクリプト
電話番号 + 法人番号で成約先を除外する

使用方法:
    # 更新データから成約先を除外
    python scripts/exclude_contract_accounts.py --updates

    # 新規リード候補から成約先を除外
    python scripts/exclude_contract_accounts.py --new-leads

    # 両方実行
    python scripts/exclude_contract_accounts.py --all
"""

import sys
import re
import argparse
from pathlib import Path
from datetime import datetime

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
from loguru import logger


def normalize_phone(phone):
    """電話番号を正規化"""
    if pd.isna(phone) or not phone:
        return ''
    phone = str(phone).strip()
    phone = re.sub(r'[^0-9]', '', phone)
    return phone


def normalize_corp_num(num):
    """法人番号を正規化"""
    if pd.isna(num) or not num:
        return ''
    num = str(num).strip()
    num = re.sub(r'[^0-9]', '', num)
    return num


def load_contract_data(contract_path: Path) -> tuple[set, set]:
    """
    成約先データを読み込み、電話番号と法人番号のセットを返す

    Returns:
        tuple[set, set]: (電話番号セット, 法人番号セット)
    """
    contract_df = pd.read_csv(contract_path, dtype=str, encoding='utf-8-sig')
    logger.info(f"成約先データ読み込み: {len(contract_df):,} 件")

    # 電話番号セット
    contract_phones = set()
    if 'Phone' in contract_df.columns:
        for phone in contract_df['Phone'].dropna():
            norm = normalize_phone(phone)
            if norm:
                contract_phones.add(norm)

    # 法人番号セット
    contract_corps = set()
    for col in ['CorporateNumber__c', 'CorporateIdentificationNumber__c']:
        if col in contract_df.columns:
            for corp in contract_df[col].dropna():
                norm = normalize_corp_num(corp)
                if norm and len(norm) >= 10:
                    contract_corps.add(norm)

    logger.info(f"  電話番号: {len(contract_phones):,} 件")
    logger.info(f"  法人番号: {len(contract_corps):,} 件")

    return contract_phones, contract_corps


def exclude_from_updates(
    output_dir: Path,
    contract_phones: set,
    contract_corps: set,
) -> dict:
    """
    更新データから成約先を除外

    Args:
        output_dir: 出力ディレクトリ
        contract_phones: 成約先電話番号セット
        contract_corps: 成約先法人番号セット

    Returns:
        dict: 処理結果
    """
    config = {
        'Account': {
            'merged': 'merged_取引先.csv',
            'update': 'account_full_updates.csv',
            'phone_cols': ['Phone'],
            'corp_cols': ['CorporateIdentificationNumber__c', 'CorporateNumber__c', '法人番号'],
        },
        'Contact': {
            'merged': 'merged_責任者.csv',
            'update': 'contact_full_updates.csv',
            'phone_cols': ['Phone'],
            'corp_cols': ['Account_CorporateNumber__c', '法人番号'],
        },
        'Lead': {
            'merged': 'merged_リード.csv',
            'update': 'lead_full_updates.csv',
            'phone_cols': ['Phone'],
            'corp_cols': ['CorporateNumber__c', 'HJBG_CorporateNumber__c', '法人番号'],
        },
    }

    results = {}

    for obj, cfg in config.items():
        merged_path = output_dir / cfg['merged']
        update_path = output_dir / cfg['update']

        if not merged_path.exists():
            logger.warning(f"{obj}: {cfg['merged']} が見つかりません")
            continue
        if not update_path.exists():
            logger.warning(f"{obj}: {cfg['update']} が見つかりません")
            continue

        logger.info(f"\n=== {obj} 成約先除外 ===")

        merged_df = pd.read_csv(merged_path, dtype=str, encoding='utf-8-sig')
        update_df = pd.read_csv(update_path, dtype=str, encoding='utf-8-sig')

        before_count = len(update_df)
        logger.info(f"  更新前: {before_count:,} 件")

        # 電話番号でマッチするId
        phone_match_ids = set()
        for col in cfg['phone_cols']:
            if col in merged_df.columns:
                merged_df[f'{col}_norm'] = merged_df[col].apply(normalize_phone)
                matched = merged_df[merged_df[f'{col}_norm'].isin(contract_phones)]['Id'].dropna()
                phone_match_ids.update(matched)

        # 法人番号でマッチするId
        corp_match_ids = set()
        for col in cfg['corp_cols']:
            if col in merged_df.columns:
                merged_df[f'{col}_norm'] = merged_df[col].apply(normalize_corp_num)
                matched = merged_df[merged_df[f'{col}_norm'].isin(contract_corps)]['Id'].dropna()
                corp_match_ids.update(matched)

        # 電話番号 OR 法人番号でマッチしたIdを除外
        contract_ids = phone_match_ids | corp_match_ids

        excluded_df = update_df[update_df['Id'].isin(contract_ids)]
        filtered_df = update_df[~update_df['Id'].isin(contract_ids)]

        after_count = len(filtered_df)
        excluded_count = len(excluded_df)

        logger.info(f"  電話番号マッチ: {len(phone_match_ids):,} 件")
        logger.info(f"  法人番号マッチ: {len(corp_match_ids):,} 件")
        logger.info(f"  成約先除外: {excluded_count:,} 件")
        logger.info(f"  更新後: {after_count:,} 件")

        # 保存
        filtered_path = output_dir / f"{cfg['update'].replace('.csv', '_filtered.csv')}"
        excluded_path = output_dir / f"{cfg['update'].replace('.csv', '_excluded_contract.csv')}"

        filtered_df.to_csv(filtered_path, index=False, encoding='utf-8-sig')
        if excluded_count > 0:
            excluded_df.to_csv(excluded_path, index=False, encoding='utf-8-sig')

        logger.info(f"  保存: {filtered_path.name}")

        results[obj] = {
            'before': before_count,
            'excluded': excluded_count,
            'after': after_count,
        }

    return results


def exclude_from_new_leads(
    output_dir: Path,
    contract_phones: set,
    contract_corps: set,
) -> dict:
    """
    新規リード候補から成約先を除外

    Args:
        output_dir: 出力ディレクトリ
        contract_phones: 成約先電話番号セット
        contract_corps: 成約先法人番号セット

    Returns:
        dict: 処理結果
    """
    # final_new_leads.csv から除外
    final_path = output_dir / 'final_new_leads.csv'

    if not final_path.exists():
        logger.warning(f"final_new_leads.csv が見つかりません")
        return {}

    logger.info(f"\n=== 新規リード候補 成約先除外 ===")

    final_df = pd.read_csv(final_path, dtype=str, encoding='utf-8-sig')
    before_count = len(final_df)
    logger.info(f"  入力: {before_count:,} 件")

    # 電話番号カラム
    if '選考担当者ＴＥＬ_加工' in final_df.columns:
        phone_col = '選考担当者ＴＥＬ_加工'
    else:
        phone_col = '選考担当者ＴＥＬ'

    final_df['phone_norm'] = final_df[phone_col].apply(normalize_phone)
    final_df['corp_norm'] = final_df['法人番号'].apply(normalize_corp_num)

    # 電話番号 OR 法人番号でマッチ
    phone_match = final_df['phone_norm'].isin(contract_phones)
    corp_match = final_df['corp_norm'].isin(contract_corps)
    is_contract = phone_match | corp_match

    excluded_df = final_df[is_contract]
    true_new_df = final_df[~is_contract]

    excluded_count = len(excluded_df)
    after_count = len(true_new_df)

    logger.info(f"  電話番号マッチ: {phone_match.sum():,} 件")
    logger.info(f"  法人番号マッチ: {corp_match.sum():,} 件")
    logger.info(f"  成約先除外: {excluded_count:,} 件")
    logger.info(f"  真の新規リード: {after_count:,} 件")

    # 作業用カラムを削除
    true_new_df = true_new_df.drop(columns=['phone_norm', 'corp_norm'], errors='ignore')
    excluded_df = excluded_df.drop(columns=['phone_norm', 'corp_norm'], errors='ignore')

    # 保存
    true_new_path = output_dir / 'true_new_leads.csv'
    excluded_path = output_dir / 'excluded_by_contract.csv'

    true_new_df.to_csv(true_new_path, index=False, encoding='utf-8-sig')
    if excluded_count > 0:
        excluded_df.to_csv(excluded_path, index=False, encoding='utf-8-sig')

    logger.info(f"  保存: {true_new_path.name}")

    return {
        'NewLeads': {
            'before': before_count,
            'excluded': excluded_count,
            'after': after_count,
        }
    }


def main():
    parser = argparse.ArgumentParser(description='成約先除外スクリプト')
    parser.add_argument('--updates', action='store_true', help='更新データから成約先を除外')
    parser.add_argument('--new-leads', action='store_true', help='新規リード候補から成約先を除外')
    parser.add_argument('--all', action='store_true', help='すべて実行')
    parser.add_argument('--contract-csv', type=str, help='成約先CSVパス（デフォルト: 最新のcontract_accounts_*.csv）')
    parser.add_argument('--output-dir', type=str, default='data/output/hellowork', help='出力ディレクトリ')

    args = parser.parse_args()

    if not (args.updates or args.new_leads or args.all):
        parser.print_help()
        return

    output_dir = project_root / args.output_dir

    # 成約先CSVを探す
    if args.contract_csv:
        contract_path = Path(args.contract_csv)
    else:
        # 最新のcontract_accounts_*.csvを探す
        contract_files = list((project_root / 'data/output').glob('contract_accounts_*.csv'))
        if not contract_files:
            logger.error("成約先CSVが見つかりません。--contract-csv で指定するか、export_contract_report.py を実行してください。")
            return
        contract_path = max(contract_files, key=lambda p: p.stat().st_mtime)

    logger.info(f"成約先CSV: {contract_path}")

    # 成約先データ読み込み
    contract_phones, contract_corps = load_contract_data(contract_path)

    results = {}

    # 更新データから除外
    if args.updates or args.all:
        update_results = exclude_from_updates(output_dir, contract_phones, contract_corps)
        results.update(update_results)

    # 新規リード候補から除外
    if args.new_leads or args.all:
        new_lead_results = exclude_from_new_leads(output_dir, contract_phones, contract_corps)
        results.update(new_lead_results)

    # サマリ出力
    print("\n" + "=" * 60)
    print("成約先除外 完了")
    print("=" * 60)

    total_before = sum(r['before'] for r in results.values())
    total_excluded = sum(r['excluded'] for r in results.values())
    total_after = sum(r['after'] for r in results.values())

    for obj, r in results.items():
        print(f"  {obj}: {r['before']:,} → {r['after']:,} (除外: {r['excluded']:,})")

    print(f"\n  合計: {total_before:,} → {total_after:,} (除外: {total_excluded:,})")


if __name__ == "__main__":
    main()
