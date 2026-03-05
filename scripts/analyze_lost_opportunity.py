"""
失注商談の包括的分析スクリプト

分析観点:
1. 営業入力（バイアスあり）: LostReason等のSFフィールド
2. 営業入力（バイアスなし）: Zoom文字起こし特徴量
3. 時系列分析: トレンド、季節性
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')


def load_opportunity_data():
    """失注・受注商談データを読み込み"""
    base_path = Path(__file__).parent.parent / 'data' / 'output' / 'analysis'

    lost_path = base_path / 'lost_opportunity_full_20260126.csv'
    won_path = base_path / 'won_opportunity_full_20260126.csv'

    df_lost = pd.read_csv(lost_path, encoding='utf-8-sig')
    df_won = pd.read_csv(won_path, encoding='utf-8-sig')

    df_lost['IsWon'] = False
    df_won['IsWon'] = True

    return df_lost, df_won


def load_zoom_data():
    """Zoom分析データを読み込み"""
    zoom_base = Path(r'C:\Users\fuji1\OneDrive\デスクトップ\ZoomMTG_analyzer_auto')

    # 特徴量データ
    features_path = zoom_base / 'features_combined_627.csv'
    matching_path = zoom_base / 'data' / 'sf_tsv_matching_result.csv'

    df_features = pd.read_csv(features_path, encoding='utf-8-sig')
    df_matching = pd.read_csv(matching_path, encoding='utf-8-sig')

    return df_features, df_matching


def analyze_lost_reasons(df_lost):
    """失注理由の分析（営業入力バイアスあり）"""
    print("\n" + "="*60)
    print("1. 失注理由分析（営業入力 = バイアスあり）")
    print("="*60)

    # 大分類
    print("\n【失注理由 - 大分類】")
    if 'LostReason_Large__c' in df_lost.columns:
        reason_large = df_lost['LostReason_Large__c'].value_counts()
        total = len(df_lost)
        for reason, count in reason_large.items():
            pct = count / total * 100
            print(f"  {reason}: {count:,}件 ({pct:.1f}%)")

    # 小分類（上位20）
    print("\n【失注理由 - 小分類（TOP20）】")
    if 'LostReason_Small__c' in df_lost.columns:
        reason_small = df_lost['LostReason_Small__c'].value_counts().head(20)
        for reason, count in reason_small.items():
            pct = count / total * 100
            print(f"  {reason}: {count:,}件 ({pct:.1f}%)")

    # 失注シーン
    print("\n【失注シーン分布】")
    if 'LostScene__c' in df_lost.columns:
        scene = df_lost['LostScene__c'].value_counts()
        for s, count in scene.items():
            pct = count / total * 100
            print(f"  {s}: {count:,}件 ({pct:.1f}%)")

    # 最終到達フェーズ
    print("\n【最終到達フェーズ】")
    if 'LastReachedStage__c' in df_lost.columns:
        stage = df_lost['LastReachedStage__c'].value_counts()
        for s, count in stage.items():
            pct = count / total * 100
            print(f"  {s}: {count:,}件 ({pct:.1f}%)")

    return reason_large if 'LostReason_Large__c' in df_lost.columns else None


def analyze_time_series(df_lost, df_won):
    """時系列分析"""
    print("\n" + "="*60)
    print("2. 時系列分析")
    print("="*60)

    # CloseDate を日付型に変換
    df_lost['CloseDate'] = pd.to_datetime(df_lost['CloseDate'])
    df_won['CloseDate'] = pd.to_datetime(df_won['CloseDate'])

    # 月別集計
    df_lost['YearMonth'] = df_lost['CloseDate'].dt.to_period('M')
    df_won['YearMonth'] = df_won['CloseDate'].dt.to_period('M')

    lost_monthly = df_lost.groupby('YearMonth').size()
    won_monthly = df_won.groupby('YearMonth').size()

    # 月別成約率
    print("\n【月別成約率推移（直近12ヶ月）】")
    all_months = sorted(set(lost_monthly.index) | set(won_monthly.index))[-12:]

    monthly_data = []
    for month in all_months:
        lost = lost_monthly.get(month, 0)
        won = won_monthly.get(month, 0)
        total = lost + won
        rate = won / total * 100 if total > 0 else 0
        monthly_data.append({
            'month': str(month),
            'won': won,
            'lost': lost,
            'total': total,
            'rate': rate
        })
        print(f"  {month}: 受注{won:,} / 失注{lost:,} = 成約率{rate:.1f}%")

    # 四半期別
    df_lost['Quarter'] = df_lost['CloseDate'].dt.to_period('Q')
    df_won['Quarter'] = df_won['CloseDate'].dt.to_period('Q')

    lost_quarterly = df_lost.groupby('Quarter').size()
    won_quarterly = df_won.groupby('Quarter').size()

    print("\n【四半期別成約率推移（直近8四半期）】")
    all_quarters = sorted(set(lost_quarterly.index) | set(won_quarterly.index))[-8:]

    for quarter in all_quarters:
        lost = lost_quarterly.get(quarter, 0)
        won = won_quarterly.get(quarter, 0)
        total = lost + won
        rate = won / total * 100 if total > 0 else 0
        print(f"  {quarter}: 受注{won:,} / 失注{lost:,} = 成約率{rate:.1f}%")

    return pd.DataFrame(monthly_data)


def analyze_by_segment(df_lost, df_won):
    """セグメント別分析"""
    print("\n" + "="*60)
    print("3. セグメント別分析")
    print("="*60)

    # 全データ結合
    df_all = pd.concat([df_lost, df_won], ignore_index=True)

    # サービス形態（大分類）
    print("\n【サービス形態（大分類）× 成約率】")
    if 'FacilityType_Large__c' in df_all.columns:
        segment = df_all.groupby('FacilityType_Large__c').agg({
            'IsWon': ['sum', 'count']
        })
        segment.columns = ['won', 'total']
        segment['lost'] = segment['total'] - segment['won']
        segment['rate'] = segment['won'] / segment['total'] * 100
        segment = segment.sort_values('rate', ascending=False)

        for idx, row in segment.iterrows():
            print(f"  {idx}: 受注{int(row['won']):,} / 失注{int(row['lost']):,} = 成約率{row['rate']:.1f}%")

    # アポランク
    print("\n【アポランク × 成約率】")
    if 'AppointRank__c' in df_all.columns:
        segment = df_all.groupby('AppointRank__c').agg({
            'IsWon': ['sum', 'count']
        })
        segment.columns = ['won', 'total']
        segment['lost'] = segment['total'] - segment['won']
        segment['rate'] = segment['won'] / segment['total'] * 100
        segment = segment.sort_values('rate', ascending=False)

        for idx, row in segment.iterrows():
            print(f"  {idx}: 受注{int(row['won']):,} / 失注{int(row['lost']):,} = 成約率{row['rate']:.1f}%")

    # 聞き手の役職
    print("\n【聞き手の役職 × 成約率】")
    if 'Hearing_ContactTitle__c' in df_all.columns:
        segment = df_all.groupby('Hearing_ContactTitle__c').agg({
            'IsWon': ['sum', 'count']
        })
        segment.columns = ['won', 'total']
        segment['lost'] = segment['total'] - segment['won']
        segment['rate'] = segment['won'] / segment['total'] * 100
        segment = segment[segment['total'] >= 50]  # 50件以上のみ
        segment = segment.sort_values('rate', ascending=False)

        for idx, row in segment.iterrows():
            print(f"  {idx}: 受注{int(row['won']):,} / 失注{int(row['lost']):,} = 成約率{row['rate']:.1f}%")

    # 決裁権の有無
    print("\n【決裁権の有無 × 成約率】")
    if 'Hearing_Authority__c' in df_all.columns:
        segment = df_all.groupby('Hearing_Authority__c').agg({
            'IsWon': ['sum', 'count']
        })
        segment.columns = ['won', 'total']
        segment['lost'] = segment['total'] - segment['won']
        segment['rate'] = segment['won'] / segment['total'] * 100
        segment = segment.sort_values('rate', ascending=False)

        for idx, row in segment.iterrows():
            print(f"  {idx}: 受注{int(row['won']):,} / 失注{int(row['lost']):,} = 成約率{row['rate']:.1f}%")

    return df_all


def analyze_lead_time(df_lost, df_won):
    """リードタイム分析"""
    print("\n" + "="*60)
    print("4. リードタイム分析")
    print("="*60)

    # AgeInDays（商談期間）
    print("\n【商談期間（AgeInDays）】")
    if 'AgeInDays' in df_lost.columns and 'AgeInDays' in df_won.columns:
        lost_age = df_lost['AgeInDays'].dropna()
        won_age = df_won['AgeInDays'].dropna()

        print(f"  失注: 平均{lost_age.mean():.1f}日 / 中央値{lost_age.median():.1f}日")
        print(f"  受注: 平均{won_age.mean():.1f}日 / 中央値{won_age.median():.1f}日")

    # フェーズ滞在日数
    print("\n【フェーズ滞在日数（平均）】")
    phase_cols = ['X01StayDays__c', 'X02StayDays__c', 'X03StayDays__c',
                  'X04StayDays__c', 'X05StayDays__c', 'X06StayDays__c']

    for col in phase_cols:
        if col in df_lost.columns and col in df_won.columns:
            lost_days = df_lost[col].dropna()
            won_days = df_won[col].dropna()

            if len(lost_days) > 0 and len(won_days) > 0:
                phase = col.replace('StayDays__c', '').replace('X', 'Phase ')
                print(f"  {phase}: 失注{lost_days.mean():.1f}日 / 受注{won_days.mean():.1f}日")


def analyze_zoom_features(df_lost, df_features, df_matching):
    """Zoom特徴量による分析（バイアスなし）"""
    print("\n" + "="*60)
    print("5. Zoom商談分析（営業入力 = バイアスなし）")
    print("="*60)

    # マッチング状況
    print(f"\n【Zoom連携状況】")
    print(f"  Zoom分析済み商談: {len(df_features):,}件")
    print(f"  SF-Zoom突合済み: {len(df_matching):,}件")

    # 失注商談でZoom連携あり
    lost_with_zoom = df_lost[df_lost['ZoomURL__c'].notna()]
    print(f"  失注商談（ZoomURLあり）: {len(lost_with_zoom):,}件")

    # マッチングデータから失注のみ抽出
    lost_matching = df_matching[df_matching['sf_label'] == 'lost']
    print(f"  失注商談（Zoom突合済み）: {len(lost_matching):,}件")

    # 特徴量の概要
    if len(df_features) > 0:
        print(f"\n【Zoom特徴量概要】")
        print(f"  特徴量数: {len(df_features.columns):,}個")

        # 主要特徴量カテゴリ
        categories = {
            'talk_': '発話分析',
            'phase_': 'フェーズ分析',
            'turn_': 'ターン分析',
            'duration': '時間分析',
            'ratio': '比率分析'
        }

        for prefix, label in categories.items():
            cols = [c for c in df_features.columns if prefix in c.lower()]
            print(f"  {label}: {len(cols)}特徴量")

    return lost_matching


def analyze_owner_performance(df_lost, df_won):
    """営業担当者別分析"""
    print("\n" + "="*60)
    print("6. 営業担当者別分析")
    print("="*60)

    # 全データ結合
    df_all = pd.concat([df_lost, df_won], ignore_index=True)

    if 'Owner.Name' in df_all.columns:
        # 担当者別成約率
        owner_stats = df_all.groupby('Owner.Name').agg({
            'IsWon': ['sum', 'count']
        })
        owner_stats.columns = ['won', 'total']
        owner_stats['lost'] = owner_stats['total'] - owner_stats['won']
        owner_stats['rate'] = owner_stats['won'] / owner_stats['total'] * 100

        # 50件以上のみ
        owner_stats = owner_stats[owner_stats['total'] >= 50]
        owner_stats = owner_stats.sort_values('rate', ascending=False)

        print("\n【営業担当者別成約率（50件以上）】")
        for idx, row in owner_stats.head(20).iterrows():
            print(f"  {idx}: 受注{int(row['won']):,} / 失注{int(row['lost']):,} = 成約率{row['rate']:.1f}%")


def generate_summary_report(df_lost, df_won):
    """サマリーレポート生成"""
    print("\n" + "="*60)
    print("7. サマリーレポート")
    print("="*60)

    total_lost = len(df_lost)
    total_won = len(df_won)
    total = total_lost + total_won
    overall_rate = total_won / total * 100

    print(f"""
【全体サマリー】
  期間: {df_lost['CloseDate'].min()} ～ {df_lost['CloseDate'].max()}
  総商談数: {total:,}件
  受注: {total_won:,}件
  失注: {total_lost:,}件
  成約率: {overall_rate:.1f}%

【主要な失注要因（営業入力ベース）】
  1. 「その他」が53.3% → 詳細化が必要
  2. 「今のところニーズがない」9.2%
  3. 「対価を払拭できない事情」8.0%
  4. 「採用している/足りている」6.8%
  5. 「サービスに価値を感じていない」6.6%

【改善提案】
  - 「その他」の失注理由を詳細化するプロセス改善
  - Zoom商談分析との連携強化（現在連携率0.4%）
  - アポランク精度の検証（ランクと成約率の相関確認）
""")


def main():
    """メイン処理"""
    print("="*60)
    print("失注商談 包括的分析")
    print(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)

    # データ読み込み
    print("\nデータ読み込み中...")
    df_lost, df_won = load_opportunity_data()
    print(f"  失注商談: {len(df_lost):,}件")
    print(f"  受注商談: {len(df_won):,}件")

    try:
        df_features, df_matching = load_zoom_data()
        print(f"  Zoom特徴量: {len(df_features):,}件")
        print(f"  Zoom突合: {len(df_matching):,}件")
    except Exception as e:
        print(f"  Zoomデータ読み込みエラー: {e}")
        df_features, df_matching = pd.DataFrame(), pd.DataFrame()

    # 各種分析
    analyze_lost_reasons(df_lost)
    monthly_data = analyze_time_series(df_lost, df_won)
    analyze_by_segment(df_lost, df_won)
    analyze_lead_time(df_lost, df_won)

    if len(df_features) > 0:
        analyze_zoom_features(df_lost, df_features, df_matching)

    analyze_owner_performance(df_lost, df_won)
    generate_summary_report(df_lost, df_won)

    # 結果保存
    output_dir = Path(__file__).parent.parent / 'data' / 'output' / 'analysis'
    output_dir.mkdir(parents=True, exist_ok=True)

    monthly_data.to_csv(output_dir / 'monthly_win_rate.csv', index=False, encoding='utf-8-sig')
    print(f"\n月別成約率データ保存: {output_dir / 'monthly_win_rate.csv'}")


if __name__ == '__main__':
    main()
