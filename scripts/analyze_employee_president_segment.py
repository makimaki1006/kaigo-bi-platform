"""
従業員数 × 代表者名 クロス分析スクリプト

目的: 代表者名あり×従業員規模で最適セグメントを特定
対象: 2025年4月以降のクローズ商談
"""

import sys
from pathlib import Path

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
from src.services.opportunity_service import OpportunityService


def classify_employee_count(val):
    """従業員数を帯域に分類"""
    if pd.isna(val) or val == '' or val == 'None':
        return '不明'
    try:
        n = int(float(val))
        if n <= 0:
            return '不明'
        elif n <= 10:
            return '1-10'
        elif n <= 30:
            return '11-30'
        elif n <= 50:
            return '31-50'
        elif n <= 100:
            return '51-100'
        elif n <= 200:
            return '101-200'
        else:
            return '201+'
    except (ValueError, TypeError):
        return '不明'


def classify_company_type(name):
    """法人格を分類"""
    if pd.isna(name) or name == '':
        return 'その他'
    name = str(name)
    if '株式会社' in name:
        return '株式会社'
    elif '有限会社' in name:
        return '有限会社'
    elif '社会福祉法人' in name or '社福' in name:
        return '社会福祉法人'
    elif '医療法人' in name:
        return '医療法人'
    elif '合同会社' in name:
        return '合同会社'
    elif '一般社団法人' in name:
        return '一般社団法人'
    elif 'NPO法人' in name or '特定非営利活動法人' in name:
        return 'NPO法人'
    else:
        return 'その他'


def has_president_name(val):
    """代表者名があるかどうか判定"""
    if pd.isna(val) or val == '' or val == 'None':
        return 'なし'
    return 'あり'


def main():
    print("=" * 60)
    print("従業員数 × 代表者名 クロス分析")
    print("対象: 2025年4月〜のクローズ商談")
    print("=" * 60)

    # Salesforce認証
    service = OpportunityService()
    service.authenticate()

    # 商談データ取得（Accountの従業員数、代表者名を含む）
    # CloseDate >= 2025-04-01 AND (StageName = '受注' OR StageName = '失注')
    soql = """
    SELECT
        Id,
        Name,
        StageName,
        CloseDate,
        Amount,
        AccountId,
        Account.Name,
        Account.NumberOfEmployees,
        Account.PresidentName__c,
        Account.Industry
    FROM Opportunity
    WHERE CloseDate >= 2025-04-01
      AND (StageName = '受注' OR StageName = '失注')
    """

    print("\n商談データ取得中...")
    df = service.bulk_query(soql, "クローズ商談取得")

    print(f"\n取得件数: {len(df):,} 件")
    print(f"カラム: {list(df.columns)}")

    if df.empty:
        print("データがありません。終了します。")
        return

    # データ変換
    df['employee_band'] = df['Account.NumberOfEmployees'].apply(classify_employee_count)
    df['president_flag'] = df['Account.PresidentName__c'].apply(has_president_name)
    df['company_type'] = df['Account.Name'].apply(classify_company_type)
    df['is_won'] = df['StageName'] == '受注'

    # ====================================
    # 1. 従業員数帯域別の成約率
    # ====================================
    print("\n" + "=" * 60)
    print("1. 従業員数帯域別の成約率")
    print("=" * 60)

    band_order = ['1-10', '11-30', '31-50', '51-100', '101-200', '201+', '不明']

    band_stats = df.groupby('employee_band').agg(
        total=('Id', 'count'),
        won=('is_won', 'sum')
    ).reindex(band_order).fillna(0)
    band_stats['won'] = band_stats['won'].astype(int)
    band_stats['lost'] = (band_stats['total'] - band_stats['won']).astype(int)
    band_stats['win_rate'] = (band_stats['won'] / band_stats['total'] * 100).round(1)

    print("\n| 従業員数 | 合計 | 受注 | 失注 | 成約率 |")
    print("|----------|------|------|------|--------|")
    for band in band_order:
        if band in band_stats.index:
            row = band_stats.loc[band]
            print(f"| {band:8} | {int(row['total']):>4} | {int(row['won']):>4} | {int(row['lost']):>4} | {row['win_rate']:>5.1f}% |")

    # ====================================
    # 2. 従業員数 × 代表者名 クロス分析
    # ====================================
    print("\n" + "=" * 60)
    print("2. 従業員数 × 代表者名あり/なし クロス分析")
    print("=" * 60)

    cross_stats = df.groupby(['employee_band', 'president_flag']).agg(
        total=('Id', 'count'),
        won=('is_won', 'sum')
    ).reset_index()
    cross_stats['win_rate'] = (cross_stats['won'] / cross_stats['total'] * 100).round(1)

    print("\n| 従業員数 | 代表者名 | 合計 | 受注 | 成約率 |")
    print("|----------|----------|------|------|--------|")
    for band in band_order:
        for flag in ['あり', 'なし']:
            row = cross_stats[(cross_stats['employee_band'] == band) & (cross_stats['president_flag'] == flag)]
            if not row.empty:
                r = row.iloc[0]
                print(f"| {band:8} | {flag:8} | {int(r['total']):>4} | {int(r['won']):>4} | {r['win_rate']:>5.1f}% |")

    # ====================================
    # 3. 法人格別の成約率
    # ====================================
    print("\n" + "=" * 60)
    print("3. 法人格別の成約率")
    print("=" * 60)

    type_stats = df.groupby('company_type').agg(
        total=('Id', 'count'),
        won=('is_won', 'sum')
    )
    type_stats['win_rate'] = (type_stats['won'] / type_stats['total'] * 100).round(1)
    type_stats = type_stats.sort_values('total', ascending=False)

    print("\n| 法人格 | 合計 | 受注 | 成約率 |")
    print("|--------|------|------|--------|")
    for idx, row in type_stats.iterrows():
        print(f"| {idx:12} | {int(row['total']):>4} | {int(row['won']):>4} | {row['win_rate']:>5.1f}% |")

    # ====================================
    # 4. 法人格 × 従業員数 × 代表者名 詳細分析
    # ====================================
    print("\n" + "=" * 60)
    print("4. 法人格 × 従業員数 × 代表者名 詳細分析（上位法人格のみ）")
    print("=" * 60)

    # 上位法人格（株式会社、有限会社、社会福祉法人、医療法人）
    top_types = ['株式会社', '有限会社', '社会福祉法人', '医療法人']

    for ctype in top_types:
        df_type = df[df['company_type'] == ctype]
        if df_type.empty:
            continue

        print(f"\n### {ctype}")
        print("| 従業員数 | 代表者名 | 合計 | 受注 | 成約率 |")
        print("|----------|----------|------|------|--------|")

        detail_stats = df_type.groupby(['employee_band', 'president_flag']).agg(
            total=('Id', 'count'),
            won=('is_won', 'sum')
        ).reset_index()
        detail_stats['win_rate'] = (detail_stats['won'] / detail_stats['total'] * 100).round(1)

        for band in band_order:
            for flag in ['あり', 'なし']:
                row = detail_stats[(detail_stats['employee_band'] == band) & (detail_stats['president_flag'] == flag)]
                if not row.empty:
                    r = row.iloc[0]
                    print(f"| {band:8} | {flag:8} | {int(r['total']):>4} | {int(r['won']):>4} | {r['win_rate']:>5.1f}% |")

    # ====================================
    # 5. 最適セグメント特定
    # ====================================
    print("\n" + "=" * 60)
    print("5. 最適セグメント（成約率上位）")
    print("=" * 60)

    # 全組み合わせ
    all_segments = df.groupby(['company_type', 'employee_band', 'president_flag']).agg(
        total=('Id', 'count'),
        won=('is_won', 'sum')
    ).reset_index()
    all_segments['win_rate'] = (all_segments['won'] / all_segments['total'] * 100).round(1)

    # N>=5 のセグメントのみで成約率上位10
    filtered = all_segments[all_segments['total'] >= 5].sort_values('win_rate', ascending=False).head(15)

    print("\n（N>=5 のセグメントで成約率上位15）")
    print("| 法人格 | 従業員数 | 代表者名 | 合計 | 受注 | 成約率 |")
    print("|--------|----------|----------|------|------|--------|")
    for _, row in filtered.iterrows():
        print(f"| {row['company_type']:12} | {row['employee_band']:8} | {row['president_flag']:8} | {int(row['total']):>4} | {int(row['won']):>4} | {row['win_rate']:>5.1f}% |")

    # 成約率下位（要注意セグメント）
    worst = all_segments[all_segments['total'] >= 5].sort_values('win_rate', ascending=True).head(10)

    print("\n（N>=5 のセグメントで成約率下位10 - 要注意）")
    print("| 法人格 | 従業員数 | 代表者名 | 合計 | 受注 | 成約率 |")
    print("|--------|----------|----------|------|------|--------|")
    for _, row in worst.iterrows():
        print(f"| {row['company_type']:12} | {row['employee_band']:8} | {row['president_flag']:8} | {int(row['total']):>4} | {int(row['won']):>4} | {row['win_rate']:>5.1f}% |")

    # ====================================
    # 6. サマリー
    # ====================================
    print("\n" + "=" * 60)
    print("6. サマリー")
    print("=" * 60)

    total_opps = len(df)
    total_won = df['is_won'].sum()
    overall_rate = total_won / total_opps * 100

    print(f"\n全体統計:")
    print(f"  - 商談総数: {total_opps:,} 件")
    print(f"  - 受注: {int(total_won):,} 件")
    print(f"  - 全体成約率: {overall_rate:.1f}%")

    # 代表者名あり/なし比較
    pres_yes = df[df['president_flag'] == 'あり']
    pres_no = df[df['president_flag'] == 'なし']

    print(f"\n代表者名の効果:")
    print(f"  - 代表者名あり: {len(pres_yes):,}件 → 成約率 {pres_yes['is_won'].mean()*100:.1f}%")
    print(f"  - 代表者名なし: {len(pres_no):,}件 → 成約率 {pres_no['is_won'].mean()*100:.1f}%")

    print("\n" + "=" * 60)
    print("分析完了")
    print("=" * 60)


if __name__ == "__main__":
    main()
