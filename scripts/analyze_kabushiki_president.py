"""
株式会社 × 代表者名あり の詳細分析
- サービス種別別
- 従業員数別
- 決裁者到達有無別
- 地域別（九州）
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

    print("=" * 60)
    print("株式会社 × 代表者名あり 詳細分析")
    print("=" * 60)
    print(f"分析日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # Salesforce認証
    service = OpportunityService()
    service.authenticate()

    # 分析に必要なフィールドを取得
    # Account経由で会社情報、商談情報を取得
    # LegalPersonality__c = 法人格（株式会社等）
    # PresidentName__c = 代表者名
    # ServiceType__c = サービス形態
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
    print(f"カラム: {list(df.columns)}")

    # 成約フラグ作成
    df['is_won'] = df['StageName'] == '受注'

    # 株式会社判定（LegalPersonality__c = 法人格）
    df['is_kabushiki'] = df['Account.LegalPersonality__c'].fillna('').str.contains('株式会社', na=False)

    # 代表者名あり判定
    df['has_president'] = df['Account.PresidentName__c'].notna() & (df['Account.PresidentName__c'] != '')

    # 九州地域判定（Prefectures__c または BillingState）
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
    # 分析結果
    # ================================
    results = []

    print("\n" + "=" * 60)
    print("【全体サマリー】")
    print("=" * 60)

    # 全体
    total = len(df)
    won = df['is_won'].sum()
    rate = won / total * 100 if total > 0 else 0
    print(f"全クローズ商談: {total:,}件, 受注: {won:,}件, 成約率: {rate:.1f}%")

    # 株式会社全体
    df_kb = df[df['is_kabushiki']]
    kb_total = len(df_kb)
    kb_won = df_kb['is_won'].sum()
    kb_rate = kb_won / kb_total * 100 if kb_total > 0 else 0
    print(f"株式会社: {kb_total:,}件, 受注: {kb_won:,}件, 成約率: {kb_rate:.1f}%")

    # 株式会社×代表者名あり
    df_kb_pres = df[(df['is_kabushiki']) & (df['has_president'])]
    kbp_total = len(df_kb_pres)
    kbp_won = df_kb_pres['is_won'].sum()
    kbp_rate = kbp_won / kbp_total * 100 if kbp_total > 0 else 0
    print(f"株式会社×代表者名あり: {kbp_total:,}件, 受注: {kbp_won:,}件, 成約率: {kbp_rate:.1f}%")

    # ================================
    # 1. サービス種別別分析
    # ================================
    print("\n" + "=" * 60)
    print("【1. サービス種別別】株式会社 × 代表者名あり")
    print("=" * 60)

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

    # 14%以上マーク
    df_service['14%超'] = df_service['成約率'].apply(lambda x: '★' if x >= 14 else '')

    print(df_service.to_string(index=False))

    # 14%超えのサービス種別
    high_rate_services = df_service[df_service['成約率'] >= 14]
    if len(high_rate_services) > 0:
        print(f"\n【14%超えサービス種別】")
        for _, row in high_rate_services.iterrows():
            print(f"  ★ {row['サービス種別']}: {row['成約率']:.1f}% ({row['受注']}/{row['件数']}件)")

    # ================================
    # 2. 従業員数別分析
    # ================================
    print("\n" + "=" * 60)
    print("【2. 従業員数別】株式会社 × 代表者名あり")
    print("=" * 60)

    emp_order = ['1-10', '11-30', '31-50', '51-100', '101+', '不明']
    emp_analysis = []

    for band in emp_order:
        subset = df_kb_pres[df_kb_pres['emp_band'] == band]
        e_total = len(subset)
        e_won = subset['is_won'].sum()
        e_rate = e_won / e_total * 100 if e_total > 0 else 0
        emp_analysis.append({
            '従業員数': band,
            '件数': e_total,
            '受注': e_won,
            '成約率': e_rate,
            '14%超': '★' if e_rate >= 14 else ''
        })

    df_emp = pd.DataFrame(emp_analysis)
    print(df_emp.to_string(index=False))

    # 14%超え
    high_rate_emp = df_emp[df_emp['成約率'] >= 14]
    if len(high_rate_emp) > 0:
        print(f"\n【14%超え従業員数帯域】")
        for _, row in high_rate_emp.iterrows():
            print(f"  ★ {row['従業員数']}人: {row['成約率']:.1f}% ({row['受注']}/{row['件数']}件)")

    # ================================
    # 3. 決裁者到達有無別分析
    # ================================
    print("\n" + "=" * 60)
    print("【3. 決裁者到達有無別】株式会社 × 代表者名あり")
    print("=" * 60)

    auth_analysis = []

    # 決裁者到達あり
    subset_auth = df_kb_pres[df_kb_pres['Hearing_Authority__c'] == 'あり']
    a_total = len(subset_auth)
    a_won = subset_auth['is_won'].sum()
    a_rate = a_won / a_total * 100 if a_total > 0 else 0
    auth_analysis.append({
        '決裁者到達': 'あり',
        '件数': a_total,
        '受注': a_won,
        '成約率': a_rate,
        '14%超': '★' if a_rate >= 14 else ''
    })

    # 決裁者到達なし
    subset_no_auth = df_kb_pres[df_kb_pres['Hearing_Authority__c'] == 'なし']
    na_total = len(subset_no_auth)
    na_won = subset_no_auth['is_won'].sum()
    na_rate = na_won / na_total * 100 if na_total > 0 else 0
    auth_analysis.append({
        '決裁者到達': 'なし',
        '件数': na_total,
        '受注': na_won,
        '成約率': na_rate,
        '14%超': '★' if na_rate >= 14 else ''
    })

    # 未設定
    subset_null_auth = df_kb_pres[df_kb_pres['Hearing_Authority__c'].isna() | (df_kb_pres['Hearing_Authority__c'] == '')]
    null_total = len(subset_null_auth)
    null_won = subset_null_auth['is_won'].sum()
    null_rate = null_won / null_total * 100 if null_total > 0 else 0
    auth_analysis.append({
        '決裁者到達': '(未設定)',
        '件数': null_total,
        '受注': null_won,
        '成約率': null_rate,
        '14%超': '★' if null_rate >= 14 else ''
    })

    df_auth = pd.DataFrame(auth_analysis)
    print(df_auth.to_string(index=False))

    # ================================
    # 4. 地域別分析（九州）
    # ================================
    print("\n" + "=" * 60)
    print("【4. 地域別】株式会社 × 代表者名あり × 九州")
    print("=" * 60)

    # 九州全体
    df_kb_pres_kyushu = df_kb_pres[df_kb_pres['is_kyushu']]
    ky_total = len(df_kb_pres_kyushu)
    ky_won = df_kb_pres_kyushu['is_won'].sum()
    ky_rate = ky_won / ky_total * 100 if ky_total > 0 else 0
    print(f"九州全体: {ky_total:,}件, 受注: {ky_won:,}件, 成約率: {ky_rate:.1f}% {'★' if ky_rate >= 14 else ''}")

    # 県別
    region_analysis = []
    for pref in kyushu_prefs:
        subset = df_kb_pres[df_kb_pres['prefecture'].str.contains(pref, na=False)]
        r_total = len(subset)
        r_won = subset['is_won'].sum()
        r_rate = r_won / r_total * 100 if r_total > 0 else 0
        region_analysis.append({
            '県': pref,
            '件数': r_total,
            '受注': r_won,
            '成約率': r_rate,
            '14%超': '★' if r_rate >= 14 else ''
        })

    df_region = pd.DataFrame(region_analysis)
    df_region = df_region.sort_values('成約率', ascending=False)
    print("\n【県別内訳】")
    print(df_region.to_string(index=False))

    # ================================
    # 5. クロス分析（高成約率セグメント特定）
    # ================================
    print("\n" + "=" * 60)
    print("【5. 14%超えセグメント一覧】複合条件")
    print("=" * 60)

    high_segments = []

    # サービス種別 × 従業員数
    for service_type in df_kb_pres['Account.ServiceType__c'].dropna().unique():
        for band in ['1-10', '11-30', '31-50', '51-100', '101+']:
            subset = df_kb_pres[
                (df_kb_pres['Account.ServiceType__c'] == service_type) &
                (df_kb_pres['emp_band'] == band)
            ]
            if len(subset) >= 10:  # 10件以上のみ
                s_won = subset['is_won'].sum()
                s_rate = s_won / len(subset) * 100
                if s_rate >= 14:
                    high_segments.append({
                        '条件': f"株式会社×代表者名×{service_type}×{band}人",
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
                    '条件': f"株式会社×代表者名×{service_type}×決裁者到達あり",
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
        if len(subset) >= 5:  # 九州は5件以上
            s_won = subset['is_won'].sum()
            s_rate = s_won / len(subset) * 100
            if s_rate >= 14:
                high_segments.append({
                    '条件': f"株式会社×代表者名×{service_type}×九州",
                    '件数': len(subset),
                    '受注': s_won,
                    '成約率': s_rate
                })

    if high_segments:
        df_high = pd.DataFrame(high_segments)
        df_high = df_high.sort_values('成約率', ascending=False)
        print(df_high.to_string(index=False))
    else:
        print("14%以上のセグメントは見つかりませんでした")

    # ================================
    # 結果出力
    # ================================
    output_dir = project_root / 'data' / 'output' / 'analysis'
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / f"kabushiki_president_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    # 全データ保存
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"\n分析データ保存: {output_file}")

    print("\n" + "=" * 60)
    print("分析完了")
    print("=" * 60)


if __name__ == "__main__":
    run_analysis()
