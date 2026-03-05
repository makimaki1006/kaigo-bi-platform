"""
代表者名入力と決裁者到達率の関係分析

目的: Account.PresidentName__c を持つ商談の決裁者到達率・成約率を検証
"""

import sys
from pathlib import Path
from datetime import datetime

# プロジェクトルート追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
from src.services.opportunity_service import OpportunityService


def extract_corporate_type(account_name: str) -> str:
    """
    会社名から法人格を抽出

    Args:
        account_name: 会社名

    Returns:
        法人格カテゴリ（株式会社/有限会社/社福/医療法人/その他）
    """
    if not account_name or pd.isna(account_name):
        return "その他"

    name = str(account_name)

    if "株式会社" in name or "(株)" in name or "（株）" in name:
        return "株式会社"
    elif "有限会社" in name or "(有)" in name or "（有）" in name:
        return "有限会社"
    elif "社会福祉法人" in name or "社福" in name:
        return "社会福祉法人"
    elif "医療法人" in name or "医法" in name:
        return "医療法人"
    elif "合同会社" in name or "(同)" in name or "（同）" in name:
        return "合同会社"
    elif "一般社団" in name or "公益社団" in name or "社団法人" in name:
        return "社団法人"
    elif "NPO" in name or "特定非営利" in name:
        return "NPO法人"
    else:
        return "その他"


def main():
    print("=" * 60)
    print("代表者名入力と決裁者到達率の関係分析")
    print(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Salesforce認証
    service = OpportunityService()
    service.authenticate()

    # 分析期間: 2025年4月1日以降のクローズ済み商談
    print("\n[STEP 1] Opportunity データ取得")
    print("  条件: IsClosed=true, CloseDate >= 2025-04-01")

    soql = """
    SELECT
        Id,
        Name,
        AccountId,
        Account.Name,
        Account.PresidentName__c,
        StageName,
        IsClosed,
        IsWon,
        CloseDate,
        Hearing_Authority__c
    FROM Opportunity
    WHERE IsClosed = true
      AND CloseDate >= 2025-04-01
    """

    df = service.bulk_query(soql, "クローズ済みOpportunity取得")
    print(f"  取得件数: {len(df):,} 件")

    if df.empty:
        print("  [ERROR] データが取得できませんでした")
        return

    # カラム名を正規化（ドット区切り対応）
    # Bulk API 2.0はフラットな名前で返す
    print("\n[STEP 2] データ前処理")

    # カラム名確認
    print(f"  取得カラム: {list(df.columns)}")

    # Account.PresidentName__c の存在確認
    president_col = None
    for col in df.columns:
        if 'PresidentName' in col:
            president_col = col
            break

    if president_col is None:
        print("  [ERROR] PresidentName__c カラムが見つかりません")
        return

    print(f"  代表者名カラム: {president_col}")

    # Account.Name カラム確認
    account_name_col = None
    for col in df.columns:
        if col == 'Account.Name' or col == 'AccountName':
            account_name_col = col
            break

    if account_name_col is None:
        # Account.Name が取れない場合はName（商談名）から推定しない
        print("  [WARN] Account.Name カラムが見つかりません。法人格分析はスキップします。")
    else:
        print(f"  取引先名カラム: {account_name_col}")

    # Hearing_Authority__c カラム確認
    authority_col = None
    for col in df.columns:
        if 'Hearing_Authority' in col or 'Authority' in col:
            authority_col = col
            break

    if authority_col is None:
        print("  [ERROR] Hearing_Authority__c カラムが見つかりません")
        # 実際のカラム名を確認
        print(f"  利用可能なカラム: {list(df.columns)}")
        return

    print(f"  決裁者到達カラム: {authority_col}")

    # データ変換
    # 代表者名あり/なし判定
    df['has_president'] = df[president_col].apply(
        lambda x: 'あり' if pd.notna(x) and str(x).strip() != '' else 'なし'
    )

    # 決裁者到達判定（'あり' = 到達）
    df['reached_authority'] = df[authority_col].apply(
        lambda x: True if str(x).strip() == 'あり' else False
    )

    # 成約判定（IsWon）
    df['is_won'] = df['IsWon'].apply(
        lambda x: True if str(x).lower() == 'true' else False
    )

    # 法人格抽出
    if account_name_col:
        df['corporate_type'] = df[account_name_col].apply(extract_corporate_type)

    print(f"\n  代表者名入力状況:")
    print(f"    あり: {(df['has_president'] == 'あり').sum():,} 件")
    print(f"    なし: {(df['has_president'] == 'なし').sum():,} 件")

    # ========================================
    # 分析1: 代表者名あり/なし × 決裁者到達率・成約率
    # ========================================
    print("\n" + "=" * 60)
    print("[分析1] 代表者名入力と決裁者到達率・成約率")
    print("=" * 60)

    results = []

    for has_pres in ['あり', 'なし']:
        subset = df[df['has_president'] == has_pres]
        n = len(subset)

        if n == 0:
            continue

        authority_rate = subset['reached_authority'].sum() / n * 100
        won_rate = subset['is_won'].sum() / n * 100

        results.append({
            '条件': f'代表者名{has_pres}',
            '決裁者到達率': f'{authority_rate:.1f}%',
            '成約率': f'{won_rate:.1f}%',
            'N数': n,
            '決裁者到達数': subset['reached_authority'].sum(),
            '成約数': subset['is_won'].sum()
        })

    df_result1 = pd.DataFrame(results)
    print("\n" + df_result1.to_string(index=False))

    # ========================================
    # 分析2: 代表者名 × 法人格別
    # ========================================
    if account_name_col:
        print("\n" + "=" * 60)
        print("[分析2] 代表者名 × 法人格別分析")
        print("=" * 60)

        # 主要な法人格のみ
        main_types = ['株式会社', '有限会社', '社会福祉法人', '医療法人', '合同会社']

        results2 = []

        for corp_type in main_types:
            for has_pres in ['あり', 'なし']:
                subset = df[(df['corporate_type'] == corp_type) & (df['has_president'] == has_pres)]
                n = len(subset)

                if n == 0:
                    continue

                if n < 5:
                    # サンプル数少なすぎる場合
                    authority_rate_str = f'({subset["reached_authority"].sum()}/{n})'
                    won_rate_str = f'({subset["is_won"].sum()}/{n})'
                else:
                    authority_rate = subset['reached_authority'].sum() / n * 100
                    won_rate = subset['is_won'].sum() / n * 100
                    authority_rate_str = f'{authority_rate:.1f}%'
                    won_rate_str = f'{won_rate:.1f}%'

                results2.append({
                    '法人格': corp_type,
                    '代表者名': has_pres,
                    '決裁者到達率': authority_rate_str,
                    '成約率': won_rate_str,
                    'N数': n
                })

        df_result2 = pd.DataFrame(results2)

        # ピボット表示用に整形
        print("\n" + df_result2.to_string(index=False))

        # 法人格ごとのサマリ
        print("\n[法人格別サマリ]")
        for corp_type in main_types:
            subset = df[df['corporate_type'] == corp_type]
            if len(subset) == 0:
                continue

            pres_yes = subset[subset['has_president'] == 'あり']
            pres_no = subset[subset['has_president'] == 'なし']

            if len(pres_yes) > 0 and len(pres_no) > 0:
                auth_diff = (
                    pres_yes['reached_authority'].sum() / len(pres_yes) * 100 -
                    pres_no['reached_authority'].sum() / len(pres_no) * 100
                )
                won_diff = (
                    pres_yes['is_won'].sum() / len(pres_yes) * 100 -
                    pres_no['is_won'].sum() / len(pres_no) * 100
                )

                print(f"  {corp_type}: 代表者名あり vs なし")
                print(f"    決裁者到達率差: {auth_diff:+.1f}pt")
                print(f"    成約率差: {won_diff:+.1f}pt")

    # ========================================
    # 結果の保存
    # ========================================
    output_dir = project_root / 'data' / 'output' / 'analysis'
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_path = output_dir / f'president_authority_analysis_{timestamp}.csv'

    # 詳細データを保存
    df_export = df[['Id', 'Name', president_col, 'has_president',
                    authority_col, 'reached_authority', 'IsWon', 'is_won',
                    'CloseDate', 'StageName']]

    if account_name_col:
        df_export = df[['Id', 'Name', account_name_col, president_col, 'has_president',
                        'corporate_type', authority_col, 'reached_authority',
                        'IsWon', 'is_won', 'CloseDate', 'StageName']]

    df_export.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"\n[保存完了] {output_path}")

    print("\n" + "=" * 60)
    print("分析完了")
    print("=" * 60)


if __name__ == "__main__":
    main()
