"""
株式会社 × 代表者名あり の詳細分析 v2
- 出力を整形して可読性向上
- サマリーレポート自動生成
"""

import sys
from pathlib import Path
from datetime import datetime

# プロジェクトルート追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
from src.services.opportunity_service import OpportunityService


def run_analysis():
    """株式会社×代表者名ありの詳細分析"""

    print("=" * 70)
    print("株式会社 × 代表者名あり 詳細分析")
    print("=" * 70)
    print(f"分析日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # Salesforce認証
    service = OpportunityService()
    service.authenticate()

    # 分析に必要なフィールドを取得
    soql = """
    SELECT
        Id,
        Name,
        StageName,
        CloseDate,
        Amount,
        Account.Id,
        Account.Name,
        Account.Type,
        Account.LegalPersonality__c,
        Account.PresidentName__c,
        Account.ServiceType__c,
        Account.NumberOfEmployees,
        Account.BillingState,
        Account.BillingCity,
        Account.Prefectures__c,
        Hearing_Authority__c,
        OwnerId,
        Owner.Name
    FROM Opportunity
    WHERE CloseDate >= 2025-04-01
    AND StageName IN ('受注', '失注')
    """

    print("商談データ取得中...")
    df = service.bulk_query(soql, "2025年4月以降のクローズ商談")

    print(f"\n取得件数: {len(df):,} 件")

    # 成約フラグ作成
    df['is_won'] = df['StageName'] == '受注'

    # 株式会社判定（LegalPersonality__c = 法人格）
    df['is_kabushiki'] = df['Account.LegalPersonality__c'].fillna('').str.contains('株式会社', na=False)

    # 代表者名あり判定
    df['has_president'] = df['Account.PresidentName__c'].notna() & (df['Account.PresidentName__c'] != '')

    # 九州地域判定
    kyushu_prefs = ['福岡', '佐賀', '長崎', '熊本', '大分', '宮崎', '鹿児島', '沖縄']

    def check_kyushu(row):
        pref = str(row.get('Account.Prefectures__c', '') or '')
        state = str(row.get('Account.BillingState', '') or '')
        combined = pref + state
        return any(p in combined for p in kyushu_prefs)

    df['is_kyushu'] = df.apply(check_kyushu, axis=1)

    # 県名を抽出
    def extract_pref(row):
        pref = str(row.get('Account.Prefectures__c', '') or '')
        if pref and pref != 'nan':
            return pref
        state = str(row.get('Account.BillingState', '') or '')
        return state if state and state != 'nan' else ''

    df['prefecture'] = df.apply(extract_pref, axis=1)

    # 従業員数帯域
    def get_emp_band(emp):
        try:
            emp_num = float(emp) if pd.notna(emp) else 0
            if emp_num <= 0:
                return '不明'
            elif emp_num <= 10:
                return '1-10'
            elif emp_num <= 30:
                return '11-30'
            elif emp_num <= 50:
                return '31-50'
            elif emp_num <= 100:
                return '51-100'
            else:
                return '101+'
        except:
            return '不明'

    df['emp_band'] = df['Account.NumberOfEmployees'].apply(get_emp_band)

    # ================================
    # レポート作成
    # ================================
    report_lines = []

    def add_line(text=""):
        report_lines.append(text)
        print(text)

    add_line("=" * 70)
    add_line("株式会社 × 代表者名あり 詳細分析レポート")
    add_line(f"分析日: {datetime.now().strftime('%Y-%m-%d')}")
    add_line(f"対象期間: 2025年4月1日以降のクローズ商談")
    add_line("=" * 70)

    # 全体サマリー
    add_line("\n【全体サマリー】")
    add_line("-" * 50)

    total = len(df)
    won = df['is_won'].sum()
    rate = won / total * 100 if total > 0 else 0
    add_line(f"全クローズ商談: {total:,}件, 受注: {won:,}件, 成約率: {rate:.1f}%")

    df_kb = df[df['is_kabushiki']]
    kb_total = len(df_kb)
    kb_won = df_kb['is_won'].sum()
    kb_rate = kb_won / kb_total * 100 if kb_total > 0 else 0
    add_line(f"株式会社: {kb_total:,}件, 受注: {kb_won:,}件, 成約率: {kb_rate:.1f}%")

    df_kb_pres = df[(df['is_kabushiki']) & (df['has_president'])]
    kbp_total = len(df_kb_pres)
    kbp_won = df_kb_pres['is_won'].sum()
    kbp_rate = kbp_won / kbp_total * 100 if kbp_total > 0 else 0
    add_line(f"株式会社×代表者名あり: {kbp_total:,}件, 受注: {kbp_won:,}件, 成約率: {kbp_rate:.1f}%")

    # 1. サービス種別別
    add_line("\n" + "=" * 70)
    add_line("【1. サービス種別別】株式会社 × 代表者名あり")
    add_line("=" * 70)

    service_analysis = []
    for service_type in df_kb_pres['Account.ServiceType__c'].dropna().unique():
        subset = df_kb_pres[df_kb_pres['Account.ServiceType__c'] == service_type]
        s_total = len(subset)
        s_won = subset['is_won'].sum()
        s_rate = s_won / s_total * 100 if s_total > 0 else 0
        service_analysis.append({
            'サービス種別': service_type,
            '件数': s_total,
            '受注': s_won,
            '成約率': s_rate
        })

    # 空の場合も追加
    subset_null = df_kb_pres[df_kb_pres['Account.ServiceType__c'].isna()]
    if len(subset_null) > 0:
        s_total = len(subset_null)
        s_won = subset_null['is_won'].sum()
        s_rate = s_won / s_total * 100 if s_total > 0 else 0
        service_analysis.append({
            'サービス種別': '(未設定)',
            '件数': s_total,
            '受注': s_won,
            '成約率': s_rate
        })

    df_service = pd.DataFrame(service_analysis)
    df_service = df_service.sort_values('成約率', ascending=False)

    # 10件以上で14%超えのサービス種別
    add_line("\n【14%超え（10件以上）】")
    for _, row in df_service.iterrows():
        if row['件数'] >= 10 and row['成約率'] >= 14:
            add_line(f"  ★ {row['サービス種別']}: {row['成約率']:.1f}% ({row['受注']}/{row['件数']}件)")

    # 全サービス種別
    add_line("\n【全サービス種別一覧】")
    add_line(f"{'サービス種別':<40} {'件数':>6} {'受注':>6} {'成約率':>8} {'14%超':>6}")
    add_line("-" * 70)
    for _, row in df_service.iterrows():
        marker = "★" if row['成約率'] >= 14 else ""
        add_line(f"{row['サービス種別']:<40} {row['件数']:>6} {row['受注']:>6} {row['成約率']:>7.1f}% {marker:>6}")

    # 2. 従業員数別
    add_line("\n" + "=" * 70)
    add_line("【2. 従業員数別】株式会社 × 代表者名あり")
    add_line("=" * 70)

    emp_order = ['1-10', '11-30', '31-50', '51-100', '101+', '不明']
    add_line(f"{'従業員数':<10} {'件数':>8} {'受注':>8} {'成約率':>10} {'14%超':>8}")
    add_line("-" * 50)

    for band in emp_order:
        subset = df_kb_pres[df_kb_pres['emp_band'] == band]
        e_total = len(subset)
        e_won = subset['is_won'].sum()
        e_rate = e_won / e_total * 100 if e_total > 0 else 0
        marker = "★" if e_rate >= 14 else ""
        add_line(f"{band:<10} {e_total:>8} {e_won:>8} {e_rate:>9.1f}% {marker:>8}")

    # 3. 決裁者到達有無別
    add_line("\n" + "=" * 70)
    add_line("【3. 決裁者到達有無別】株式会社 × 代表者名あり")
    add_line("=" * 70)

    add_line(f"{'決裁者到達':<12} {'件数':>8} {'受注':>8} {'成約率':>10} {'14%超':>8}")
    add_line("-" * 50)

    for auth_val, label in [('あり', 'あり'), ('なし', 'なし')]:
        subset = df_kb_pres[df_kb_pres['Hearing_Authority__c'] == auth_val]
        a_total = len(subset)
        a_won = subset['is_won'].sum()
        a_rate = a_won / a_total * 100 if a_total > 0 else 0
        marker = "★" if a_rate >= 14 else ""
        add_line(f"{label:<12} {a_total:>8} {a_won:>8} {a_rate:>9.1f}% {marker:>8}")

    # 未設定
    subset_null = df_kb_pres[df_kb_pres['Hearing_Authority__c'].isna() | (df_kb_pres['Hearing_Authority__c'] == '')]
    null_total = len(subset_null)
    null_won = subset_null['is_won'].sum()
    null_rate = null_won / null_total * 100 if null_total > 0 else 0
    marker = "★" if null_rate >= 14 else ""
    add_line(f"{'(未設定)':<12} {null_total:>8} {null_won:>8} {null_rate:>9.1f}% {marker:>8}")

    # 4. 地域別（九州）
    add_line("\n" + "=" * 70)
    add_line("【4. 地域別】株式会社 × 代表者名あり × 九州")
    add_line("=" * 70)

    df_kb_pres_kyushu = df_kb_pres[df_kb_pres['is_kyushu']]
    ky_total = len(df_kb_pres_kyushu)
    ky_won = df_kb_pres_kyushu['is_won'].sum()
    ky_rate = ky_won / ky_total * 100 if ky_total > 0 else 0
    marker = "★" if ky_rate >= 14 else ""
    add_line(f"九州全体: {ky_total:,}件, 受注: {ky_won:,}件, 成約率: {ky_rate:.1f}% {marker}")

    add_line(f"\n{'県':<12} {'件数':>8} {'受注':>8} {'成約率':>10} {'14%超':>8}")
    add_line("-" * 50)

    for pref in kyushu_prefs:
        subset = df_kb_pres[df_kb_pres['prefecture'].str.contains(pref, na=False)]
        r_total = len(subset)
        r_won = subset['is_won'].sum()
        r_rate = r_won / r_total * 100 if r_total > 0 else 0
        marker = "★" if r_rate >= 14 else ""
        add_line(f"{pref:<12} {r_total:>8} {r_won:>8} {r_rate:>9.1f}% {marker:>8}")

    # 5. 14%超えセグメント一覧
    add_line("\n" + "=" * 70)
    add_line("【5. 14%超え複合セグメント一覧】(10件以上)")
    add_line("=" * 70)

    high_segments = []

    # サービス種別 × 従業員数
    for service_type in df_kb_pres['Account.ServiceType__c'].dropna().unique():
        for band in ['1-10', '11-30', '31-50', '51-100', '101+']:
            subset = df_kb_pres[
                (df_kb_pres['Account.ServiceType__c'] == service_type) &
                (df_kb_pres['emp_band'] == band)
            ]
            if len(subset) >= 10:
                s_won = subset['is_won'].sum()
                s_rate = s_won / len(subset) * 100
                if s_rate >= 14:
                    high_segments.append({
                        '条件': f"{service_type} × {band}人",
                        '件数': len(subset),
                        '受注': s_won,
                        '成約率': s_rate
                    })

    # 決裁者到達 × サービス種別
    for service_type in df_kb_pres['Account.ServiceType__c'].dropna().unique():
        subset = df_kb_pres[
            (df_kb_pres['Account.ServiceType__c'] == service_type) &
            (df_kb_pres['Hearing_Authority__c'] == 'あり')
        ]
        if len(subset) >= 10:
            s_won = subset['is_won'].sum()
            s_rate = s_won / len(subset) * 100
            if s_rate >= 14:
                high_segments.append({
                    '条件': f"{service_type} × 決裁者到達あり",
                    '件数': len(subset),
                    '受注': s_won,
                    '成約率': s_rate
                })

    # 九州 × サービス種別
    for service_type in df_kb_pres['Account.ServiceType__c'].dropna().unique():
        subset = df_kb_pres[
            (df_kb_pres['Account.ServiceType__c'] == service_type) &
            (df_kb_pres['is_kyushu'])
        ]
        if len(subset) >= 5:
            s_won = subset['is_won'].sum()
            s_rate = s_won / len(subset) * 100
            if s_rate >= 14:
                high_segments.append({
                    '条件': f"{service_type} × 九州",
                    '件数': len(subset),
                    '受注': s_won,
                    '成約率': s_rate
                })

    if high_segments:
        df_high = pd.DataFrame(high_segments)
        df_high = df_high.drop_duplicates(subset=['条件'])
        df_high = df_high.sort_values('成約率', ascending=False)

        add_line(f"{'条件':<50} {'件数':>6} {'受注':>6} {'成約率':>8}")
        add_line("-" * 75)
        for _, row in df_high.iterrows():
            add_line(f"{row['条件']:<50} {row['件数']:>6} {row['受注']:>6} {row['成約率']:>7.1f}%")
    else:
        add_line("14%以上のセグメントは見つかりませんでした（10件以上）")

    # まとめ
    add_line("\n" + "=" * 70)
    add_line("【まとめ: 14%超えの条件】")
    add_line("=" * 70)

    summary_segments = []

    # 単独条件で14%超え
    if kbp_rate >= 14:
        summary_segments.append(f"株式会社×代表者名あり: {kbp_rate:.1f}% ({kbp_won}/{kbp_total}件)")

    # サービス種別（10件以上）
    for _, row in df_service.iterrows():
        if row['件数'] >= 10 and row['成約率'] >= 14:
            summary_segments.append(f"株式会社×代表者名×{row['サービス種別']}: {row['成約率']:.1f}% ({row['受注']}/{row['件数']}件)")

    # 決裁者到達あり
    subset = df_kb_pres[df_kb_pres['Hearing_Authority__c'] == 'あり']
    a_total = len(subset)
    a_won = subset['is_won'].sum()
    a_rate = a_won / a_total * 100 if a_total > 0 else 0
    if a_rate >= 14 and a_total >= 10:
        summary_segments.append(f"株式会社×代表者名×決裁者到達あり: {a_rate:.1f}% ({a_won}/{a_total}件)")

    for seg in summary_segments:
        add_line(f"  ★ {seg}")

    add_line("\n" + "=" * 70)
    add_line("分析完了")
    add_line("=" * 70)

    # レポート保存
    output_dir = project_root / 'claudedocs'
    output_dir.mkdir(parents=True, exist_ok=True)

    report_file = output_dir / f"kabushiki_president_analysis_{datetime.now().strftime('%Y%m%d')}.md"
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report_lines))
    print(f"\nレポート保存: {report_file}")

    # データ保存
    data_dir = project_root / 'data' / 'output' / 'analysis'
    data_dir.mkdir(parents=True, exist_ok=True)
    data_file = data_dir / f"kabushiki_president_data_{datetime.now().strftime('%Y%m%d')}.csv"
    df.to_csv(data_file, index=False, encoding='utf-8-sig')
    print(f"データ保存: {data_file}")

    return df, df_kb_pres


if __name__ == "__main__":
    run_analysis()
