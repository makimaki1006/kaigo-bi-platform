"""
サービス種別（訪問系）× 代表者名 クロス分析スクリプト

目的: 訪問系×代表者名ありで14%超セグメントを特定
対象: 2025年4月〜のクローズ商談
"""

import sys
from pathlib import Path

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
from src.services.opportunity_service import OpportunityService


def analyze_service_president_cross():
    """訪問系×代表者名のクロス分析を実行"""

    print("=" * 60)
    print("サービス種別（訪問系）× 代表者名 クロス分析")
    print("=" * 60)

    # サービス初期化・認証
    service = OpportunityService()
    service.authenticate()

    # 2025年4月以降のクローズ商談を取得
    # ServiceType__c と PresidentName__c はAccountの項目
    soql = """
    SELECT
        Id,
        Name,
        StageName,
        CloseDate,
        AccountId,
        Account.Name,
        Account.ServiceType__c,
        Account.PresidentName__c
    FROM Opportunity
    WHERE CloseDate >= 2025-04-01
      AND (StageName = '受注' OR StageName = '失注' OR StageName = 'Closed Won' OR StageName = 'Closed Lost')
    """

    print("\n[1] データ取得中...")
    df = service.bulk_query(soql, "2025年4月以降のクローズ商談")

    if df.empty:
        print("  データなし")
        return

    print(f"  取得件数: {len(df):,} 件")

    # カラム名確認
    print(f"\n  カラム: {list(df.columns)}")

    # StageName確認
    print(f"\n  StageNameの値: {df['StageName'].unique()}")

    # 成約判定（受注/Closed Wonを成約とする）
    won_stages = ['受注', 'Closed Won', '成約']
    df['is_won'] = df['StageName'].isin(won_stages)

    # 訪問系判定（ServiceType__cに「訪問」を含む）
    df['is_houmon'] = df['Account.ServiceType__c'].fillna('').str.contains('訪問')

    # 代表者名あり判定
    df['has_president'] = df['Account.PresidentName__c'].notna() & (df['Account.PresidentName__c'] != '')

    # 法人格抽出
    def extract_corp_type(name):
        if pd.isna(name):
            return 'その他'
        name = str(name)
        if '株式会社' in name:
            return '株式会社'
        elif '有限会社' in name:
            return '有限会社'
        elif '社会福祉法人' in name or '社福' in name:
            return '社会福祉法人'
        elif '合同会社' in name:
            return '合同会社'
        elif '医療法人' in name:
            return '医療法人'
        elif 'NPO法人' in name or '特定非営利' in name:
            return 'NPO法人'
        else:
            return 'その他'

    df['corp_type'] = df['Account.Name'].apply(extract_corp_type)

    # ========================================
    # 分析1: 訪問系の成約率
    # ========================================
    print("\n" + "=" * 60)
    print("[分析1] 訪問系の成約率")
    print("=" * 60)

    result1 = df.groupby('is_houmon').agg(
        total=('Id', 'count'),
        won=('is_won', 'sum')
    ).reset_index()
    result1['win_rate'] = (result1['won'] / result1['total'] * 100).round(2)
    result1['is_houmon'] = result1['is_houmon'].map({True: '訪問系', False: '訪問系以外'})

    print("\n" + result1.to_string(index=False))

    # ========================================
    # 分析2: 訪問系 × 代表者名あり/なし
    # ========================================
    print("\n" + "=" * 60)
    print("[分析2] 訪問系 × 代表者名あり/なし")
    print("=" * 60)

    # 訪問系のみフィルタ
    df_houmon = df[df['is_houmon'] == True].copy()

    if df_houmon.empty:
        print("  訪問系データなし")
    else:
        result2 = df_houmon.groupby('has_president').agg(
            total=('Id', 'count'),
            won=('is_won', 'sum')
        ).reset_index()
        result2['win_rate'] = (result2['won'] / result2['total'] * 100).round(2)
        result2['has_president'] = result2['has_president'].map({True: '代表者名あり', False: '代表者名なし'})

        print("\n" + result2.to_string(index=False))

    # ========================================
    # 分析3: 訪問系 × 代表者名あり × 法人格別
    # ========================================
    print("\n" + "=" * 60)
    print("[分析3] 訪問系 × 代表者名あり × 法人格別")
    print("=" * 60)

    # 訪問系かつ代表者名ありのみフィルタ
    df_target = df[(df['is_houmon'] == True) & (df['has_president'] == True)].copy()

    if df_target.empty:
        print("  該当データなし")
    else:
        result3 = df_target.groupby('corp_type').agg(
            total=('Id', 'count'),
            won=('is_won', 'sum')
        ).reset_index()
        result3['win_rate'] = (result3['won'] / result3['total'] * 100).round(2)
        result3 = result3.sort_values('win_rate', ascending=False)

        print("\n法人格別成約率（訪問系 × 代表者名あり）:")
        print("-" * 40)
        print(result3.to_string(index=False))

    # ========================================
    # サマリー
    # ========================================
    print("\n" + "=" * 60)
    print("サマリー")
    print("=" * 60)

    # 全体成約率
    total_count = len(df)
    total_won = df['is_won'].sum()
    total_rate = total_won / total_count * 100 if total_count > 0 else 0

    print(f"\n全体: {total_count:,}件中 {total_won:,}件成約 ({total_rate:.2f}%)")

    # 14%超セグメント特定
    print("\n" + "-" * 40)
    print("成約率14%超セグメント:")
    print("-" * 40)

    if not df_target.empty:
        for _, row in result3.iterrows():
            if row['win_rate'] > 14:
                print(f"  [OK] 訪問系 x 代表者名あり x {row['corp_type']}: {row['win_rate']:.2f}% ({row['won']}/{row['total']}件)")

    # ServiceType__cの内訳確認
    print("\n" + "-" * 40)
    print("ServiceType__c の値一覧（参考）:")
    print("-" * 40)
    service_counts = df['Account.ServiceType__c'].value_counts(dropna=False).head(20)
    print(service_counts.to_string())

    return df


if __name__ == "__main__":
    analyze_service_president_cross()
