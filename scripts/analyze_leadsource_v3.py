# -*- coding: utf-8 -*-
"""
LeadSource別成約率分析スクリプト v3

Lead.LeadSourceの分布確認と、OpportunityのType/Stage等代替フィールドでの分析
"""

import sys
import io
from pathlib import Path
from datetime import datetime

# Windows環境でのUTF-8出力設定
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
from src.services.opportunity_service import OpportunityService


# 九州沖縄の都道府県
KYUSHU_OKINAWA = [
    '福岡県', '佐賀県', '長崎県', '熊本県', '大分県', '宮崎県', '鹿児島県', '沖縄県',
    '福岡', '佐賀', '長崎', '熊本', '大分', '宮崎', '鹿児島', '沖縄',
]


def is_kyushu_okinawa(prefecture: str) -> bool:
    """九州沖縄かどうか判定"""
    if pd.isna(prefecture) or prefecture == '':
        return False

    for region in KYUSHU_OKINAWA:
        if region in str(prefecture):
            return True

    return False


def calculate_conversion_rate(df: pd.DataFrame, group_col: str) -> pd.DataFrame:
    """成約率を計算"""
    df_calc = df.copy()
    df_calc['IsWon_bool'] = df_calc['IsWon'].apply(lambda x: str(x).lower() == 'true')
    df_calc['IsClosed_bool'] = df_calc['IsClosed'].apply(lambda x: str(x).lower() == 'true')

    grouped = df_calc.groupby(group_col, dropna=False).agg(
        総件数=('Id', 'count'),
        クローズ済み=('IsClosed_bool', 'sum'),
        成約件数=('IsWon_bool', 'sum'),
    ).reset_index()

    grouped['成約率'] = (grouped['成約件数'] / grouped['クローズ済み'] * 100).round(2)
    grouped['成約率_全体基準'] = (grouped['成約件数'] / grouped['総件数'] * 100).round(2)

    # NaNを0に置換
    grouped = grouped.fillna(0)

    # ソート（成約件数降順）
    grouped = grouped.sort_values('成約件数', ascending=False)

    return grouped


def main():
    print("=" * 70)
    print("LeadSource/Type別成約率分析 v3")
    print("=" * 70)
    print(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # OpportunityService初期化・認証
    service = OpportunityService()
    service.authenticate()
    print()

    # ========================================
    # 1. Lead.LeadSourceの分布確認
    # ========================================
    print("-" * 70)
    print("1. Lead.LeadSourceの分布確認")
    print("-" * 70)

    lead_soql = """
    SELECT
        Id,
        LeadSource,
        Status,
        IsConverted,
        CreatedDate
    FROM Lead
    WHERE CreatedDate >= 2025-04-01T00:00:00Z
    """

    df_leads = service.bulk_query(lead_soql, "Lead取得（2025年4月～）")
    print(f"\nLead件数: {len(df_leads):,} 件")

    print(f"\nLead.LeadSource分布:")
    lead_source_dist = df_leads['LeadSource'].value_counts(dropna=False)
    print(lead_source_dist.head(30))
    print()

    # ========================================
    # 2. Opportunity.Typeの分布確認
    # ========================================
    print("-" * 70)
    print("2. Opportunity.Type/StageName/Owner分布確認")
    print("-" * 70)

    opp_soql = """
    SELECT
        Id,
        Name,
        Type,
        StageName,
        IsClosed,
        IsWon,
        CloseDate,
        Amount,
        AccountId,
        Account.Name,
        Account.Prefectures__c,
        OwnerId,
        Owner.Name,
        CreatedDate
    FROM Opportunity
    WHERE CloseDate >= 2025-04-01
    """

    df_opps = service.bulk_query(opp_soql, "Opportunity取得（2025年4月～）")
    print(f"\nOpportunity件数: {len(df_opps):,} 件")

    # カラム名を整理
    if 'Account.Prefectures__c' in df_opps.columns:
        df_opps.rename(columns={'Account.Prefectures__c': 'Prefecture'}, inplace=True)
    elif 'Prefectures__c' in df_opps.columns:
        df_opps.rename(columns={'Prefectures__c': 'Prefecture'}, inplace=True)
    else:
        df_opps['Prefecture'] = ''

    if 'Owner.Name' in df_opps.columns:
        df_opps.rename(columns={'Owner.Name': 'OwnerName'}, inplace=True)

    print(f"\nOpportunity.Type分布:")
    type_dist = df_opps['Type'].value_counts(dropna=False)
    print(type_dist.head(20))

    print(f"\nOpportunity.StageName分布:")
    stage_dist = df_opps['StageName'].value_counts(dropna=False)
    print(stage_dist.head(20))

    print(f"\nOpportunity.Owner分布（上位15名）:")
    owner_dist = df_opps['OwnerName'].value_counts(dropna=False)
    print(owner_dist.head(15))
    print()

    # ========================================
    # 3. Type別成約率（全国）
    # ========================================
    print("-" * 70)
    print("3. Type別成約率（全国、2025年4月～）")
    print("-" * 70)

    df_opps['Type_filled'] = df_opps['Type'].fillna('(未設定)').replace('', '(未設定)')
    type_stats = calculate_conversion_rate(df_opps.copy(), 'Type_filled')
    type_stats.rename(columns={'Type_filled': 'Type'}, inplace=True)

    print("\n【Type別成約率】")
    print(type_stats.to_string(index=False))
    print()

    # ========================================
    # 4. Owner別成約率（全国）
    # ========================================
    print("-" * 70)
    print("4. Owner別成約率（全国、2025年4月～）")
    print("-" * 70)

    df_opps['OwnerName_filled'] = df_opps['OwnerName'].fillna('(未設定)').replace('', '(未設定)')
    owner_stats = calculate_conversion_rate(df_opps.copy(), 'OwnerName_filled')
    owner_stats.rename(columns={'OwnerName_filled': 'Owner'}, inplace=True)

    print("\n【Owner別成約率（上位20名）】")
    print(owner_stats.head(20).to_string(index=False))
    print()

    # ========================================
    # 5. 九州沖縄限定のType別成約率
    # ========================================
    print("-" * 70)
    print("5. 九州沖縄限定のType/Owner別成約率")
    print("-" * 70)

    df_kyushu = df_opps[df_opps['Prefecture'].apply(is_kyushu_okinawa)].copy()
    print(f"\n九州沖縄のOpportunity件数: {len(df_kyushu):,} 件")

    if len(df_kyushu) > 0:
        # Type別
        df_kyushu['Type_filled'] = df_kyushu['Type'].fillna('(未設定)').replace('', '(未設定)')
        kyushu_type_stats = calculate_conversion_rate(df_kyushu.copy(), 'Type_filled')
        kyushu_type_stats.rename(columns={'Type_filled': 'Type'}, inplace=True)

        print("\n【九州沖縄 Type別成約率】")
        print(kyushu_type_stats.to_string(index=False))
        print()

        # Owner別
        df_kyushu['OwnerName_filled'] = df_kyushu['OwnerName'].fillna('(未設定)').replace('', '(未設定)')
        kyushu_owner_stats = calculate_conversion_rate(df_kyushu.copy(), 'OwnerName_filled')
        kyushu_owner_stats.rename(columns={'OwnerName_filled': 'Owner'}, inplace=True)

        print("\n【九州沖縄 Owner別成約率（上位15名）】")
        print(kyushu_owner_stats.head(15).to_string(index=False))
        print()

        # 都道府県別内訳
        print("\n【九州沖縄 都道府県別件数】")
        print(df_kyushu['Prefecture'].value_counts())
    print()

    # ========================================
    # 6. サマリー
    # ========================================
    print("=" * 70)
    print("サマリー")
    print("=" * 70)

    # Lead.LeadSource活用状況
    lead_source_set_count = df_leads['LeadSource'].notna().sum()
    print(f"\n【Lead.LeadSource活用状況】")
    print(f"  LeadSource設定済み: {lead_source_set_count:,} 件 / {len(df_leads):,} 件 ({lead_source_set_count/len(df_leads)*100:.1f}%)")

    # 全国成約率
    total_closed = (df_opps['IsClosed'].apply(lambda x: str(x).lower() == 'true')).sum()
    total_won = (df_opps['IsWon'].apply(lambda x: str(x).lower() == 'true')).sum()
    overall_rate = total_won / total_closed * 100 if total_closed > 0 else 0

    print(f"\n【全国成約率（2025年4月～）】")
    print(f"  全体: {overall_rate:.1f}% ({total_won}/{total_closed}) [総数: {len(df_opps)}]")

    # Type別成約率
    print(f"\n【Type別成約率】")
    for _, row in type_stats.iterrows():
        t = row['Type']
        closed = int(row['クローズ済み'])
        won = int(row['成約件数'])
        rate = row['成約率']
        print(f"  {t}: {rate:.1f}% ({won}/{closed})")

    # 九州沖縄
    if len(df_kyushu) > 0:
        kyushu_closed = (df_kyushu['IsClosed'].apply(lambda x: str(x).lower() == 'true')).sum()
        kyushu_won = (df_kyushu['IsWon'].apply(lambda x: str(x).lower() == 'true')).sum()
        kyushu_rate = kyushu_won / kyushu_closed * 100 if kyushu_closed > 0 else 0

        print(f"\n【九州沖縄成約率】")
        print(f"  全体: {kyushu_rate:.1f}% ({kyushu_won}/{kyushu_closed}) [総数: {len(df_kyushu)}]")

        print(f"\n【九州沖縄 Type別成約率】")
        for _, row in kyushu_type_stats.iterrows():
            t = row['Type']
            closed = int(row['クローズ済み'])
            won = int(row['成約件数'])
            rate = row['成約率']
            print(f"  {t}: {rate:.1f}% ({won}/{closed})")

    print("\n" + "=" * 70)
    print("分析完了")
    print("=" * 70)

    # CSVに保存
    output_dir = project_root / 'data' / 'output' / 'analysis'
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    type_stats.to_csv(output_dir / f'type_stats_{timestamp}.csv', index=False, encoding='utf-8-sig')
    owner_stats.to_csv(output_dir / f'owner_stats_{timestamp}.csv', index=False, encoding='utf-8-sig')
    lead_source_dist.to_frame().to_csv(output_dir / f'lead_leadsource_dist_{timestamp}.csv', encoding='utf-8-sig')

    if len(df_kyushu) > 0:
        kyushu_type_stats.to_csv(output_dir / f'kyushu_type_stats_{timestamp}.csv', index=False, encoding='utf-8-sig')
        kyushu_owner_stats.to_csv(output_dir / f'kyushu_owner_stats_{timestamp}.csv', index=False, encoding='utf-8-sig')

    print(f"\nCSV出力先: {output_dir}")

    return {
        'lead_source_dist': lead_source_dist,
        'type_stats': type_stats,
        'owner_stats': owner_stats,
    }


if __name__ == "__main__":
    main()
