# -*- coding: utf-8 -*-
"""
LeadSource別成約率分析スクリプト Final版

Lead.LeadSource → Lead.ConvertedAccountId → Account → Opportunity の紐づけで分析

分析内容:
1. LeadSource別の成約率（2025年4月〜、全国）
2. インバウンド vs アウトバウンドの比較
3. 九州沖縄限定でのLeadSource別成約率
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


# インバウンドに該当するLeadSource
INBOUND_SOURCES = [
    'Web',
    '問い合わせ',
    '資料請求',
    'ウェブサイト',
    'HP',
    'ホームページ',
    '紹介',
    '口コミ',
    'セミナー',
    '展示会',
    'イベント',
    'SNS',
    '広告',
    'リスティング',
    '自然検索',
    'Inbound',
    'Marketing',
    'インバウンド',
    '問合せ',
    '反響',
    'ナース専科',
    '看護roo',
    '看護のお仕事',
    'きらケア',
]

# アウトバウンド（ハローワーク系）に該当するLeadSource
OUTBOUND_SOURCES = [
    'ハローワーク',
    'ハロワ',
    'Other',
]

# 九州沖縄の都道府県
KYUSHU_OKINAWA = [
    '福岡県', '佐賀県', '長崎県', '熊本県', '大分県', '宮崎県', '鹿児島県', '沖縄県',
    '福岡', '佐賀', '長崎', '熊本', '大分', '宮崎', '鹿児島', '沖縄',
]


def classify_inbound_outbound(lead_source: str) -> str:
    """LeadSourceをインバウンド/アウトバウンドに分類"""
    if pd.isna(lead_source) or lead_source == '' or str(lead_source).strip() == '':
        return '未設定'

    lead_source_str = str(lead_source)

    # インバウンドチェック
    for inbound in INBOUND_SOURCES:
        if inbound.lower() in lead_source_str.lower():
            return 'インバウンド'

    # アウトバウンドチェック
    for outbound in OUTBOUND_SOURCES:
        if outbound.lower() in lead_source_str.lower():
            return 'アウトバウンド'

    return 'その他'


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
    print("LeadSource別成約率分析 Final版")
    print("（Lead→Account→Opportunity紐づけ）")
    print("=" * 70)
    print(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # OpportunityService初期化・認証
    service = OpportunityService()
    service.authenticate()
    print()

    # ========================================
    # 1. コンバート済みLeadデータ取得（AccountId紐づけあり）
    # ========================================
    print("-" * 70)
    print("1. コンバート済みLead取得（Account紐づけ）")
    print("-" * 70)

    # ConvertedAccountIdがあるLeadを取得
    lead_soql = """
    SELECT
        Id,
        LeadSource,
        ConvertedAccountId,
        ConvertedDate,
        Status,
        IsConverted
    FROM Lead
    WHERE IsConverted = true
    AND ConvertedAccountId != null
    """

    df_leads = service.bulk_query(lead_soql, "コンバート済みLead取得")
    print(f"\nコンバート済みLead件数: {len(df_leads):,} 件")

    print(f"\nLead.LeadSource分布:")
    print(df_leads['LeadSource'].value_counts(dropna=False).head(20))
    print()

    # ========================================
    # 2. Opportunityデータ取得（2025年4月～）
    # ========================================
    print("-" * 70)
    print("2. Opportunityデータ取得")
    print("-" * 70)

    opp_soql = """
    SELECT
        Id,
        Name,
        StageName,
        IsClosed,
        IsWon,
        CloseDate,
        Amount,
        AccountId,
        Account.Name,
        Account.Prefectures__c
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

    # ========================================
    # 3. Lead→Account→Opportunityマージ
    # ========================================
    print("-" * 70)
    print("3. Lead→Account→Opportunityマージ")
    print("-" * 70)

    # LeadのLeadSourceをAccountId経由でマージ
    # Account毎に最初のLeadSourceを採用（1Account複数Leadの場合）
    df_lead_source = df_leads.groupby('ConvertedAccountId').agg({
        'LeadSource': 'first'  # 最初のLeadSourceを採用
    }).reset_index()
    df_lead_source.rename(columns={'ConvertedAccountId': 'AccountId'}, inplace=True)

    print(f"AccountId毎のLeadSource件数: {len(df_lead_source):,} 件")

    # Opportunityとマージ
    df_merged = df_opps.merge(df_lead_source, on='AccountId', how='left')

    # マージ結果確認
    has_leadsource = df_merged['LeadSource'].notna().sum()
    print(f"\nLeadSource紐づけ成功: {has_leadsource:,} 件 / {len(df_merged):,} 件")
    print(f"  紐づけ率: {has_leadsource/len(df_merged)*100:.1f}%")

    # LeadSource分布
    print(f"\nマージ後LeadSource分布:")
    print(df_merged['LeadSource'].value_counts(dropna=False).head(20))
    print()

    # ========================================
    # 4. LeadSource別成約率（全国）
    # ========================================
    print("-" * 70)
    print("4. LeadSource別成約率（全国、2025年4月～）")
    print("-" * 70)

    df_merged['LeadSource_filled'] = df_merged['LeadSource'].fillna('(未設定)').replace('', '(未設定)')

    leadsource_stats = calculate_conversion_rate(df_merged.copy(), 'LeadSource_filled')
    leadsource_stats.rename(columns={'LeadSource_filled': 'LeadSource'}, inplace=True)

    print("\n【LeadSource別成約率】")
    print(leadsource_stats.to_string(index=False))
    print()

    # ========================================
    # 5. インバウンド vs アウトバウンド比較
    # ========================================
    print("-" * 70)
    print("5. インバウンド vs アウトバウンド比較")
    print("-" * 70)

    df_merged['InboundOutbound'] = df_merged['LeadSource'].apply(classify_inbound_outbound)

    inbound_stats = calculate_conversion_rate(df_merged.copy(), 'InboundOutbound')

    print("\n【インバウンド/アウトバウンド別成約率】")
    print(inbound_stats.to_string(index=False))
    print()

    # LeadSourceの分類内訳
    print("\n【LeadSource分類内訳】")
    for category in ['インバウンド', 'アウトバウンド', 'その他', '未設定']:
        sources = df_merged[df_merged['InboundOutbound'] == category]['LeadSource'].dropna().unique()
        if len(sources) > 0:
            sources_str = ', '.join([str(s) for s in sources[:5]])
            if len(sources) > 5:
                sources_str += f' ... 他{len(sources)-5}件'
            print(f"  {category}: {sources_str}")
        else:
            print(f"  {category}: (該当LeadSourceなし)")
    print()

    # ========================================
    # 6. 九州沖縄限定のLeadSource別成約率
    # ========================================
    print("-" * 70)
    print("6. 九州沖縄限定のLeadSource別成約率")
    print("-" * 70)

    df_kyushu = df_merged[df_merged['Prefecture'].apply(is_kyushu_okinawa)].copy()
    print(f"\n九州沖縄のOpportunity件数: {len(df_kyushu):,} 件")

    kyushu_leadsource_stats = None
    kyushu_inbound_stats = None

    if len(df_kyushu) > 0:
        # LeadSource別
        df_kyushu['LeadSource_filled'] = df_kyushu['LeadSource'].fillna('(未設定)').replace('', '(未設定)')
        kyushu_leadsource_stats = calculate_conversion_rate(df_kyushu.copy(), 'LeadSource_filled')
        kyushu_leadsource_stats.rename(columns={'LeadSource_filled': 'LeadSource'}, inplace=True)

        print("\n【九州沖縄 LeadSource別成約率】")
        print(kyushu_leadsource_stats.to_string(index=False))
        print()

        # インバウンド/アウトバウンド別
        kyushu_inbound_stats = calculate_conversion_rate(df_kyushu.copy(), 'InboundOutbound')
        print("\n【九州沖縄 インバウンド/アウトバウンド別成約率】")
        print(kyushu_inbound_stats.to_string(index=False))
        print()

        # 都道府県別内訳
        print("\n【九州沖縄 都道府県別件数】")
        print(df_kyushu['Prefecture'].value_counts())
    print()

    # ========================================
    # 7. サマリーテーブル出力
    # ========================================
    print("=" * 70)
    print("分析結果サマリー")
    print("=" * 70)

    # 全国成約率
    print("\n┌" + "─" * 68 + "┐")
    print("│ 1. 全国 LeadSource別成約率（2025年4月～）" + " " * 27 + "│")
    print("├" + "─" * 68 + "┤")
    print("│ LeadSource                        │ 成約率 │ 成約数/クローズ │ 総数   │")
    print("├" + "─" * 68 + "┤")
    for _, row in leadsource_stats.iterrows():
        source = str(row['LeadSource'])[:32].ljust(32)
        rate = f"{row['成約率']:.1f}%".rjust(6)
        won_closed = f"{int(row['成約件数'])}/{int(row['クローズ済み'])}".rjust(14)
        total = f"{int(row['総件数'])}".rjust(6)
        print(f"│ {source} │ {rate} │ {won_closed} │ {total} │")
    print("└" + "─" * 68 + "┘")

    print("\n┌" + "─" * 68 + "┐")
    print("│ 2. 全国 インバウンド vs アウトバウンド" + " " * 30 + "│")
    print("├" + "─" * 68 + "┤")
    print("│ カテゴリ         │ 成約率 │ 成約数/クローズ │ 総数   │")
    print("├" + "─" * 68 + "┤")
    for _, row in inbound_stats.iterrows():
        category = str(row['InboundOutbound'])[:16].ljust(16)
        rate = f"{row['成約率']:.1f}%".rjust(6)
        won_closed = f"{int(row['成約件数'])}/{int(row['クローズ済み'])}".rjust(14)
        total = f"{int(row['総件数'])}".rjust(6)
        print(f"│ {category} │ {rate} │ {won_closed} │ {total} │")
    print("└" + "─" * 68 + "┘")

    if len(df_kyushu) > 0 and kyushu_inbound_stats is not None:
        print("\n┌" + "─" * 68 + "┐")
        print("│ 3. 九州沖縄 インバウンド vs アウトバウンド" + " " * 26 + "│")
        print("├" + "─" * 68 + "┤")
        print("│ カテゴリ         │ 成約率 │ 成約数/クローズ │ 総数   │")
        print("├" + "─" * 68 + "┤")
        for _, row in kyushu_inbound_stats.iterrows():
            category = str(row['InboundOutbound'])[:16].ljust(16)
            rate = f"{row['成約率']:.1f}%".rjust(6)
            won_closed = f"{int(row['成約件数'])}/{int(row['クローズ済み'])}".rjust(14)
            total = f"{int(row['総件数'])}".rjust(6)
            print(f"│ {category} │ {rate} │ {won_closed} │ {total} │")
        print("└" + "─" * 68 + "┘")

    print("\n" + "=" * 70)
    print("分析完了")
    print("=" * 70)

    # CSVに保存
    output_dir = project_root / 'data' / 'output' / 'analysis'
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    leadsource_stats.to_csv(output_dir / f'leadsource_stats_final_{timestamp}.csv', index=False, encoding='utf-8-sig')
    inbound_stats.to_csv(output_dir / f'inbound_stats_final_{timestamp}.csv', index=False, encoding='utf-8-sig')

    if kyushu_leadsource_stats is not None:
        kyushu_leadsource_stats.to_csv(output_dir / f'kyushu_leadsource_stats_final_{timestamp}.csv', index=False, encoding='utf-8-sig')
    if kyushu_inbound_stats is not None:
        kyushu_inbound_stats.to_csv(output_dir / f'kyushu_inbound_stats_final_{timestamp}.csv', index=False, encoding='utf-8-sig')

    print(f"\nCSV出力先: {output_dir}")

    return {
        'leadsource_stats': leadsource_stats,
        'inbound_stats': inbound_stats,
        'kyushu_leadsource_stats': kyushu_leadsource_stats,
        'kyushu_inbound_stats': kyushu_inbound_stats,
    }


if __name__ == "__main__":
    main()
