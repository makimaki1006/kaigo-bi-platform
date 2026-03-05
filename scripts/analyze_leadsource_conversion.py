# -*- coding: utf-8 -*-
"""
LeadSource別成約率分析スクリプト

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
    'Webサイト',
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

    lead_source_str = str(lead_source).lower()

    for inbound in INBOUND_SOURCES:
        if inbound.lower() in lead_source_str:
            return 'インバウンド'

    return 'アウトバウンド'


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
    # StageName = '受注' または IsWon = true を成約とする
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
    print("=" * 60)
    print("LeadSource別成約率分析")
    print("=" * 60)
    print(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # OpportunityService初期化・認証
    service = OpportunityService()
    service.authenticate()
    print()

    # ========================================
    # 0. Opportunityフィールド確認
    # ========================================
    print("-" * 60)
    print("0. Opportunityフィールド確認")
    print("-" * 60)

    # Describeでフィールド確認
    url = f"{service.instance_url}/services/data/{service.api_version}/sobjects/Opportunity/describe"
    response = service.session.get(url, headers=service._headers())
    response.raise_for_status()
    describe = response.json()

    # LeadSource関連フィールドを検索
    lead_source_fields = [f for f in describe['fields'] if 'lead' in f['name'].lower() or 'source' in f['name'].lower()]
    print(f"LeadSource関連フィールド:")
    for f in lead_source_fields:
        print(f"  - {f['name']} ({f['label']}): {f['type']}")
    print()

    # ========================================
    # 1. Opportunityデータ取得（2025年4月〜）
    # ========================================
    print("-" * 60)
    print("1. Opportunityデータ取得")
    print("-" * 60)

    # LeadSourceフィールドの実際の値を確認
    soql = """
    SELECT
        Id,
        Name,
        LeadSource,
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

    df = service.bulk_query(soql, "Opportunity取得（2025年4月～）")
    print(f"\n取得件数: {len(df):,} 件")

    # カラム名を整理
    if 'Account.Prefectures__c' in df.columns:
        df.rename(columns={'Account.Prefectures__c': 'Prefecture'}, inplace=True)
    elif 'Prefectures__c' in df.columns:
        df.rename(columns={'Prefectures__c': 'Prefecture'}, inplace=True)
    else:
        df['Prefecture'] = ''

    # LeadSourceの実際の値を確認
    print(f"\nLeadSourceのユニーク値:")
    unique_sources = df['LeadSource'].dropna().unique()
    print(f"  ユニーク値の数: {len(unique_sources)}")
    if len(unique_sources) > 0:
        for s in unique_sources[:30]:
            count = len(df[df['LeadSource'] == s])
            print(f"  - '{s}': {count} 件")
    else:
        print("  LeadSourceが設定されているレコードがありません")

    print(f"\nLeadSource分布（NULL含む）:")
    print(df['LeadSource'].value_counts(dropna=False).head(20))
    print()

    # LeadSourceが空の場合、他のソースフィールドを探す
    if df['LeadSource'].isna().all() or (df['LeadSource'] == '').all():
        print("LeadSourceが全て空です。代替フィールドを確認します...")

        # Lead関連のカスタムフィールドを確認
        lead_related = [f['name'] for f in describe['fields']
                       if 'source' in f['name'].lower() or 'channel' in f['name'].lower()
                       or 'origin' in f['name'].lower() or 'media' in f['name'].lower()]
        print(f"代替候補フィールド: {lead_related}")

        # 代替フィールドでデータ再取得
        if lead_related:
            alt_fields = ', '.join(lead_related[:5])
            alt_soql = f"""
            SELECT Id, {alt_fields}
            FROM Opportunity
            WHERE CloseDate >= 2025-04-01
            LIMIT 100
            """
            try:
                df_alt = service.bulk_query(alt_soql, "代替フィールド確認")
                print("\n代替フィールドサンプル:")
                print(df_alt.head(10))
            except Exception as e:
                print(f"代替フィールド取得エラー: {e}")

    # ========================================
    # 2. LeadSource別成約率（全国）
    # ========================================
    print("-" * 60)
    print("2. LeadSource別成約率（全国、2025年4月～）")
    print("-" * 60)

    # LeadSourceを文字列に変換（NaNを'(未設定)'に）
    df['LeadSource_filled'] = df['LeadSource'].fillna('(未設定)').replace('', '(未設定)')

    leadsource_stats = calculate_conversion_rate(df.copy(), 'LeadSource_filled')
    leadsource_stats.rename(columns={'LeadSource_filled': 'LeadSource'}, inplace=True)

    print("\n【LeadSource別成約率】")
    print(leadsource_stats.to_string(index=False))
    print()

    # ========================================
    # 3. インバウンド vs アウトバウンド比較
    # ========================================
    print("-" * 60)
    print("3. インバウンド vs アウトバウンド比較")
    print("-" * 60)

    df['InboundOutbound'] = df['LeadSource'].apply(classify_inbound_outbound)

    inbound_stats = calculate_conversion_rate(df.copy(), 'InboundOutbound')

    print("\n【インバウンド/アウトバウンド別成約率】")
    print(inbound_stats.to_string(index=False))
    print()

    # LeadSourceの分類内訳
    print("\n【LeadSource分類内訳】")
    for category in ['インバウンド', 'アウトバウンド', '未設定']:
        sources = df[df['InboundOutbound'] == category]['LeadSource'].dropna().unique()
        if len(sources) > 0:
            sources_str = ', '.join([str(s) for s in sources[:10]])
            if len(sources) > 10:
                sources_str += f' ... 他{len(sources)-10}件'
            print(f"  {category}: {sources_str}")
        else:
            print(f"  {category}: (該当LeadSourceなし)")
    print()

    # ========================================
    # 4. 九州沖縄限定のLeadSource別成約率
    # ========================================
    print("-" * 60)
    print("4. 九州沖縄限定のLeadSource別成約率")
    print("-" * 60)

    df_kyushu = df[df['Prefecture'].apply(is_kyushu_okinawa)].copy()
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
    else:
        print("  九州沖縄のデータがありません。")
        print("  Account.Prefectures__c フィールドが設定されていない可能性があります。")
    print()

    # ========================================
    # 5. サマリー出力
    # ========================================
    print("=" * 60)
    print("サマリー")
    print("=" * 60)

    print("\n【全国 LeadSource別 成約率】")
    for _, row in leadsource_stats.iterrows():
        source = row['LeadSource']
        closed = int(row['クローズ済み'])
        won = int(row['成約件数'])
        rate = row['成約率']
        print(f"  {source}: {rate:.1f}% ({won}/{closed})")

    print("\n【インバウンド vs アウトバウンド】")
    for _, row in inbound_stats.iterrows():
        category = row['InboundOutbound']
        closed = int(row['クローズ済み'])
        won = int(row['成約件数'])
        rate = row['成約率']
        print(f"  {category}: 成約率 {rate:.1f}% ({won}/{closed})")

    if kyushu_inbound_stats is not None and len(df_kyushu) > 0:
        print("\n【九州沖縄 インバウンド vs アウトバウンド】")
        for _, row in kyushu_inbound_stats.iterrows():
            category = row['InboundOutbound']
            closed = int(row['クローズ済み'])
            won = int(row['成約件数'])
            rate = row['成約率']
            print(f"  {category}: 成約率 {rate:.1f}% ({won}/{closed})")

    print("\n" + "=" * 60)
    print("分析完了")
    print("=" * 60)

    return {
        'leadsource_stats': leadsource_stats,
        'inbound_stats': inbound_stats,
        'kyushu_leadsource_stats': kyushu_leadsource_stats,
        'kyushu_inbound_stats': kyushu_inbound_stats,
    }


if __name__ == "__main__":
    main()
