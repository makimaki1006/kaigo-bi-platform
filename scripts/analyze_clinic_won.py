"""
成約企業のクリニック分析
- クリニック数と内訳
- 整形外科の実績
- 受注推移（月次）
- 地域セグメント
"""

import sys
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# プロジェクトルート追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
from src.services.opportunity_service import OpportunityService


def run_analysis():
    """成約企業のクリニック分析"""

    print("=" * 70)
    print("成約企業 クリニック分析")
    print("=" * 70)
    print(f"分析日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # Salesforce認証
    service = OpportunityService()
    service.authenticate()

    # 受注商談 + Account情報を取得（全期間）
    soql = """
    SELECT
        Id,
        Name,
        StageName,
        CloseDate,
        Amount,
        Account.Id,
        Account.Name,
        Account.LegalPersonality__c,
        Account.ServiceType__c,
        Account.ServiceType2__c,
        Account.NumberOfEmployees,
        Account.BillingState,
        Account.Prefectures__c,
        Account.Phone,
        OwnerId,
        Owner.Name
    FROM Opportunity
    WHERE StageName = '受注'
    """

    print("受注商談データ取得中...")
    df = service.bulk_query(soql, "全期間の受注商談")
    print(f"\n全受注商談: {len(df):,} 件")

    # クリニック判定: Account.Name に「クリニック」を含む
    df['is_clinic'] = df['Account.Name'].fillna('').str.contains('クリニック', na=False)

    # 整形外科判定: Account.Name に「整形」を含む
    df['is_orthopedic'] = df['Account.Name'].fillna('').str.contains('整形', na=False)

    # ServiceType にクリニック関連を含む
    df['service_has_clinic'] = df['Account.ServiceType__c'].fillna('').str.contains('クリニック', na=False)

    # 広義のクリニック（名前 OR サービス種別）
    df['is_clinic_broad'] = df['is_clinic'] | df['service_has_clinic']

    # CloseDate を datetime に変換
    df['close_date'] = pd.to_datetime(df['CloseDate'], errors='coerce')
    df['close_month'] = df['close_date'].dt.to_period('M')
    df['close_ym'] = df['close_date'].dt.strftime('%Y-%m')

    # 県名抽出
    def extract_pref(row):
        pref = str(row.get('Account.Prefectures__c', '') or '')
        if pref and pref != 'nan':
            return pref
        state = str(row.get('Account.BillingState', '') or '')
        return state if state and state != 'nan' else '(不明)'

    df['prefecture'] = df.apply(extract_pref, axis=1)

    # クリニックのみ抽出
    df_clinic = df[df['is_clinic_broad']].copy()

    # ================================
    # レポート作成
    # ================================
    report_lines = []

    def add_line(text=""):
        report_lines.append(text)
        print(text)

    add_line("=" * 70)
    add_line("成約企業 クリニック分析レポート")
    add_line(f"分析日: {datetime.now().strftime('%Y-%m-%d')}")
    add_line(f"対象: 全期間の受注商談")
    add_line("=" * 70)

    # ================================
    # 1. 全体サマリー
    # ================================
    add_line("\n【1. 全体サマリー】")
    add_line("-" * 50)
    add_line(f"全受注商談数: {len(df):,} 件")
    add_line(f"うちクリニック（名前に「クリニック」含む）: {df['is_clinic'].sum():,} 件")
    add_line(f"うちサービス種別に「クリニック」含む: {df['service_has_clinic'].sum():,} 件")
    add_line(f"広義のクリニック（名前 OR サービス種別）: {len(df_clinic):,} 件")
    add_line(f"うち整形外科（名前に「整形」含む）: {df['is_orthopedic'].sum():,} 件")

    # 整形外科クリニック
    df_ortho = df[df['is_orthopedic']]
    add_line(f"\n整形外科の受注商談: {len(df_ortho):,} 件")
    if len(df_ortho) > 0:
        add_line(f"  金額合計: {df_ortho['Amount'].astype(float, errors='ignore').sum():,.0f} 円") if df_ortho['Amount'].notna().any() else None

    # ================================
    # 2. サービス種別内訳（クリニック）
    # ================================
    add_line("\n" + "=" * 70)
    add_line("【2. サービス種別内訳】クリニック成約企業")
    add_line("=" * 70)

    service_counts = df_clinic['Account.ServiceType__c'].fillna('(未設定)').value_counts()
    add_line(f"\n{'サービス種別':<40} {'件数':>8}")
    add_line("-" * 50)
    for stype, count in service_counts.items():
        add_line(f"{stype:<40} {count:>8}")

    # ServiceType2 の内訳もあれば
    if 'Account.ServiceType2__c' in df_clinic.columns:
        service2_counts = df_clinic['Account.ServiceType2__c'].fillna('(未設定)').value_counts()
        if len(service2_counts[service2_counts.index != '(未設定)']) > 0:
            add_line(f"\n{'サービス種別2':<40} {'件数':>8}")
            add_line("-" * 50)
            for stype, count in service2_counts.items():
                if stype != '(未設定)':
                    add_line(f"{stype:<40} {count:>8}")

    # ================================
    # 3. 整形外科の実績詳細
    # ================================
    add_line("\n" + "=" * 70)
    add_line("【3. 整形外科の実績詳細】")
    add_line("=" * 70)

    if len(df_ortho) > 0:
        # 金額情報
        df_ortho_amount = df_ortho.copy()
        df_ortho_amount['Amount'] = pd.to_numeric(df_ortho_amount['Amount'], errors='coerce')

        amount_valid = df_ortho_amount['Amount'].dropna()
        if len(amount_valid) > 0:
            add_line(f"  受注件数: {len(df_ortho):,} 件")
            add_line(f"  金額合計: {amount_valid.sum():,.0f} 円")
            add_line(f"  平均金額: {amount_valid.mean():,.0f} 円")
            add_line(f"  中央値:   {amount_valid.median():,.0f} 円")

        # 整形外科の企業一覧（Account重複排除）
        add_line(f"\n  【整形外科 成約企業一覧】")
        ortho_accounts = df_ortho.drop_duplicates(subset='Account.Id')[
            ['Account.Name', 'Account.ServiceType__c', 'prefecture']
        ].reset_index(drop=True)
        add_line(f"  ユニーク企業数: {len(ortho_accounts):,} 社")
        add_line(f"\n  {'企業名':<35} {'サービス種別':<25} {'地域':<10}")
        add_line("  " + "-" * 70)
        for _, row in ortho_accounts.iterrows():
            name = str(row['Account.Name'])[:33]
            stype = str(row['Account.ServiceType__c'] or '(未設定)')[:23]
            pref = str(row['prefecture'])[:8]
            add_line(f"  {name:<35} {stype:<25} {pref:<10}")
    else:
        add_line("  整形外科の受注実績はありません")

    # ================================
    # 4. 受注推移（月次）
    # ================================
    add_line("\n" + "=" * 70)
    add_line("【4. 受注推移（月次）】クリニック成約")
    add_line("=" * 70)

    if len(df_clinic) > 0:
        monthly = df_clinic.groupby('close_ym').agg(
            件数=('Id', 'count'),
            金額合計=('Amount', lambda x: pd.to_numeric(x, errors='coerce').sum()),
        ).reset_index().rename(columns={'close_ym': '年月'})
        monthly = monthly.sort_values('年月')

        add_line(f"\n{'年月':<12} {'件数':>8} {'金額合計':>15}")
        add_line("-" * 40)
        for _, row in monthly.iterrows():
            ym = row['年月'] if pd.notna(row['年月']) else '(不明)'
            amount_str = f"{row['金額合計']:,.0f}" if pd.notna(row['金額合計']) else '-'
            add_line(f"{ym:<12} {row['件数']:>8} {amount_str:>15}")

        add_line(f"\n合計: {len(df_clinic):,} 件")

        # 直近12ヶ月のトレンド
        recent_months = monthly.tail(12)
        if len(recent_months) > 1:
            avg_recent = recent_months['件数'].mean()
            add_line(f"直近12ヶ月平均: {avg_recent:.1f} 件/月")

    # ================================
    # 5. 地域セグメント
    # ================================
    add_line("\n" + "=" * 70)
    add_line("【5. 地域セグメント】クリニック成約")
    add_line("=" * 70)

    if len(df_clinic) > 0:
        region_counts = df_clinic['prefecture'].value_counts()

        add_line(f"\n{'都道府県':<15} {'件数':>8} {'割合':>10}")
        add_line("-" * 35)
        total_clinic = len(df_clinic)
        for pref, count in region_counts.items():
            pct = count / total_clinic * 100
            add_line(f"{pref:<15} {count:>8} {pct:>9.1f}%")

        # 地方別集計
        add_line("\n【地方別集計】")

        region_map = {
            '北海道': '北海道',
            '青森': '東北', '岩手': '東北', '宮城': '東北', '秋田': '東北',
            '山形': '東北', '福島': '東北',
            '茨城': '関東', '栃木': '関東', '群馬': '関東', '埼玉': '関東',
            '千葉': '関東', '東京': '関東', '神奈川': '関東',
            '新潟': '中部', '富山': '中部', '石川': '中部', '福井': '中部',
            '山梨': '中部', '長野': '中部', '岐阜': '中部', '静岡': '中部', '愛知': '中部',
            '三重': '近畿', '滋賀': '近畿', '京都': '近畿', '大阪': '近畿',
            '兵庫': '近畿', '奈良': '近畿', '和歌山': '近畿',
            '鳥取': '中国', '島根': '中国', '岡山': '中国', '広島': '中国', '山口': '中国',
            '徳島': '四国', '香川': '四国', '愛媛': '四国', '高知': '四国',
            '福岡': '九州', '佐賀': '九州', '長崎': '九州', '熊本': '九州',
            '大分': '九州', '宮崎': '九州', '鹿児島': '九州', '沖縄': '九州',
        }

        def get_region(pref_name):
            for key, region in region_map.items():
                if key in str(pref_name):
                    return region
            return '(不明)'

        df_clinic['region'] = df_clinic['prefecture'].apply(get_region)
        region_agg = df_clinic['region'].value_counts()

        add_line(f"{'地方':<12} {'件数':>8} {'割合':>10}")
        add_line("-" * 32)
        for region, count in region_agg.items():
            pct = count / total_clinic * 100
            add_line(f"{region:<12} {count:>8} {pct:>9.1f}%")

    # ================================
    # 6. 整形外科 × 地域
    # ================================
    if len(df_ortho) > 0:
        add_line("\n" + "=" * 70)
        add_line("【6. 整形外科 × 地域】")
        add_line("=" * 70)

        ortho_region = df_ortho['prefecture'].value_counts()
        add_line(f"\n{'都道府県':<15} {'件数':>8}")
        add_line("-" * 25)
        for pref, count in ortho_region.items():
            add_line(f"{pref:<15} {count:>8}")

    # まとめ
    add_line("\n" + "=" * 70)
    add_line("【まとめ】")
    add_line("=" * 70)
    add_line(f"  全受注商談: {len(df):,} 件")
    add_line(f"  クリニック成約: {len(df_clinic):,} 件 ({len(df_clinic)/len(df)*100:.1f}%)")
    add_line(f"  整形外科成約: {len(df_ortho):,} 件")

    if len(df_clinic) > 0:
        top_region = df_clinic['prefecture'].value_counts().head(3)
        add_line(f"  クリニック成約 上位地域: {', '.join(f'{p}({c}件)' for p, c in top_region.items())}")

    add_line("\n" + "=" * 70)
    add_line("分析完了")
    add_line("=" * 70)

    # レポート保存
    output_dir = project_root / 'claudedocs'
    output_dir.mkdir(parents=True, exist_ok=True)
    report_file = output_dir / f"clinic_won_analysis_{datetime.now().strftime('%Y%m%d')}.md"
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report_lines))
    print(f"\nレポート保存: {report_file}")

    # データ保存
    data_dir = project_root / 'data' / 'output' / 'analysis'
    data_dir.mkdir(parents=True, exist_ok=True)
    clinic_file = data_dir / f"clinic_won_data_{datetime.now().strftime('%Y%m%d')}.csv"
    df_clinic.to_csv(clinic_file, index=False, encoding='utf-8-sig')
    print(f"クリニックデータ保存: {clinic_file}")

    if len(df_ortho) > 0:
        ortho_file = data_dir / f"orthopedic_won_data_{datetime.now().strftime('%Y%m%d')}.csv"
        df_ortho.to_csv(ortho_file, index=False, encoding='utf-8-sig')
        print(f"整形外科データ保存: {ortho_file}")

    return df, df_clinic, df_ortho


if __name__ == "__main__":
    run_analysis()
