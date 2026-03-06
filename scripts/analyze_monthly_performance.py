"""
月次営業成績分析スクリプト
11月〜2月の商談データを比較し、2月200%達成の要因を分析する
"""

import sys
import os
from pathlib import Path
from datetime import datetime

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np

from src.services.opportunity_service import OpportunityService


def export_opportunity_data(service):
    """11月〜2月の商談データを取得"""

    # 広めにデータ取得（10月〜3月初旬）
    fields = [
        'Id', 'Name', 'AccountId', 'Account.Name',
        'StageName', 'CloseDate', 'Amount',
        'OwnerId', 'Owner.Name',
        'CreatedDate', 'LastModifiedDate',
        'LeadSource', 'Type',
        'Probability', 'IsClosed', 'IsWon',
        'ForecastCategory',
    ]

    field_list = ', '.join(fields)
    soql = f"""SELECT {field_list} FROM Opportunity
        WHERE CloseDate >= 2025-10-01 AND CloseDate <= 2026-03-05
        ORDER BY CloseDate ASC"""

    df = service.bulk_query(soql, "商談データ取得（10月〜3月）")
    return df


def export_lead_data(service):
    """リード作成データを取得（リスト施策の効果を見る）"""

    soql = """SELECT Id, CreatedDate, LeadSource, Status,
        ConvertedDate, ConvertedOpportunityId, OwnerId, Owner.Name,
        IsConverted
        FROM Lead
        WHERE CreatedDate >= 2025-10-01T00:00:00Z AND CreatedDate <= 2026-03-05T00:00:00Z
        ORDER BY CreatedDate ASC"""

    df = service.bulk_query(soql, "リードデータ取得（10月〜3月）")
    return df


def analyze_opportunities(df_opp):
    """商談データを月次で分析"""

    print("\n" + "=" * 80)
    print("  商談（Opportunity）月次分析")
    print("=" * 80)

    # 型変換（タイムゾーン統一）
    df_opp['CloseDate'] = pd.to_datetime(df_opp['CloseDate'], utc=True).dt.tz_localize(None)
    df_opp['CreatedDate'] = pd.to_datetime(df_opp['CreatedDate'], utc=True).dt.tz_localize(None)
    df_opp['Amount'] = pd.to_numeric(df_opp['Amount'], errors='coerce').fillna(0)
    df_opp['IsWon'] = df_opp['IsWon'].astype(str).str.lower() == 'true'
    df_opp['IsClosed'] = df_opp['IsClosed'].astype(str).str.lower() == 'true'

    # 月カラム追加
    df_opp['CloseMonth'] = df_opp['CloseDate'].dt.to_period('M')
    df_opp['CreateMonth'] = df_opp['CreatedDate'].dt.to_period('M')

    # 対象月に絞り込み
    target_months = ['2025-11', '2025-12', '2026-01', '2026-02']
    df_target = df_opp[df_opp['CloseMonth'].astype(str).isin(target_months)].copy()

    # --- 1. 月別サマリー ---
    print("\n--- 1. 月別受注サマリー ---")
    won = df_target[df_target['IsWon']].copy()

    monthly_won = won.groupby('CloseMonth').agg(
        受注件数=('Id', 'count'),
        受注金額合計=('Amount', 'sum'),
        受注金額平均=('Amount', 'mean'),
        受注金額中央値=('Amount', 'median'),
    ).reset_index()

    print(monthly_won.to_string(index=False))

    # --- 2. 月別全商談（受注率分析） ---
    print("\n--- 2. 月別受注率 ---")
    monthly_all = df_target.groupby('CloseMonth').agg(
        全商談数=('Id', 'count'),
        受注数=('IsWon', 'sum'),
        受注金額=('Amount', lambda x: x[df_target.loc[x.index, 'IsWon']].sum()),
    ).reset_index()

    monthly_all['受注率'] = (monthly_all['受注数'] / monthly_all['全商談数'] * 100).round(1)
    print(monthly_all.to_string(index=False))

    # --- 3. StageName別分布 ---
    print("\n--- 3. 月別ステージ分布 ---")
    stage_dist = df_target.groupby(['CloseMonth', 'StageName']).size().unstack(fill_value=0)
    print(stage_dist.to_string())

    # --- 4. 担当者別受注実績 ---
    print("\n--- 4. 担当者別受注実績（月別） ---")
    owner_monthly = won.groupby(['CloseMonth', 'Owner.Name']).agg(
        件数=('Id', 'count'),
        金額=('Amount', 'sum'),
    ).reset_index()

    # ピボット: 担当者 × 月
    owner_pivot = owner_monthly.pivot_table(
        index='Owner.Name', columns='CloseMonth',
        values=['件数', '金額'], fill_value=0, aggfunc='sum'
    )
    print(owner_pivot.to_string())

    # --- 5. LeadSource別分析 ---
    print("\n--- 5. LeadSource別受注分析（月別） ---")
    source_monthly = won.groupby(['CloseMonth', 'LeadSource']).agg(
        件数=('Id', 'count'),
        金額=('Amount', 'sum'),
    ).reset_index()

    source_pivot = source_monthly.pivot_table(
        index='LeadSource', columns='CloseMonth',
        values=['件数', '金額'], fill_value=0, aggfunc='sum'
    )
    print(source_pivot.to_string())

    # --- 6. Type別分析 ---
    print("\n--- 6. Type（商談種別）別分析 ---")
    type_monthly = won.groupby(['CloseMonth', 'Type']).agg(
        件数=('Id', 'count'),
        金額=('Amount', 'sum'),
    ).reset_index()

    type_pivot = type_monthly.pivot_table(
        index='Type', columns='CloseMonth',
        values=['件数', '金額'], fill_value=0, aggfunc='sum'
    )
    print(type_pivot.to_string())

    # --- 7. 商談作成→受注までのリードタイム ---
    print("\n--- 7. 受注リードタイム（作成日→クローズ日） ---")
    won['LeadTime'] = (won['CloseDate'] - won['CreatedDate']).dt.days

    leadtime_monthly = won.groupby('CloseMonth').agg(
        平均リードタイム=('LeadTime', 'mean'),
        中央値リードタイム=('LeadTime', 'median'),
        最短=('LeadTime', 'min'),
        最長=('LeadTime', 'max'),
    ).reset_index()
    leadtime_monthly['平均リードタイム'] = leadtime_monthly['平均リードタイム'].round(1)
    leadtime_monthly['中央値リードタイム'] = leadtime_monthly['中央値リードタイム'].round(1)
    print(leadtime_monthly.to_string(index=False))

    # --- 8. 2月の受注案件詳細 ---
    print("\n--- 8. 2月受注案件の詳細 ---")
    feb_won = won[won['CloseMonth'].astype(str) == '2026-02'].copy()
    feb_won_sorted = feb_won.sort_values('Amount', ascending=False)

    display_cols = ['Name', 'Account.Name', 'Amount', 'Owner.Name', 'LeadSource', 'Type', 'CreatedDate', 'CloseDate', 'LeadTime']
    print(f"2月受注: {len(feb_won)} 件, 合計金額: {feb_won['Amount'].sum():,.0f}")
    if len(feb_won) > 0:
        print(feb_won_sorted[display_cols].head(20).to_string(index=False))

    # --- 9. 商談作成月と受注月のクロス分析 ---
    print("\n--- 9. 商談作成月→受注月 クロス分析 ---")
    cross = won.groupby(['CreateMonth', 'CloseMonth']).agg(
        件数=('Id', 'count'),
        金額=('Amount', 'sum'),
    ).reset_index()

    cross_pivot = cross.pivot_table(
        index='CreateMonth', columns='CloseMonth',
        values='件数', fill_value=0, aggfunc='sum'
    )
    print("（行: 作成月, 列: 受注月, 値: 件数）")
    print(cross_pivot.to_string())

    return df_opp, won


def analyze_leads(df_lead):
    """リードデータの分析"""

    print("\n" + "=" * 80)
    print("  リード（Lead）月次分析")
    print("=" * 80)

    df_lead['CreatedDate'] = pd.to_datetime(df_lead['CreatedDate'], utc=True).dt.tz_localize(None)
    df_lead['CreateMonth'] = df_lead['CreatedDate'].dt.to_period('M')
    df_lead['IsConverted'] = df_lead['IsConverted'].astype(str).str.lower() == 'true'

    target_months = ['2025-11', '2025-12', '2026-01', '2026-02']
    df_target = df_lead[df_lead['CreateMonth'].astype(str).isin(target_months)].copy()

    # --- 1. 月別リード作成数 ---
    print("\n--- 1. 月別リード作成数 ---")
    monthly_leads = df_target.groupby('CreateMonth').agg(
        作成数=('Id', 'count'),
        コンバート数=('IsConverted', 'sum'),
    ).reset_index()
    monthly_leads['コンバート率'] = (monthly_leads['コンバート数'] / monthly_leads['作成数'] * 100).round(1)
    print(monthly_leads.to_string(index=False))

    # --- 2. LeadSource別 ---
    print("\n--- 2. LeadSource別リード作成数（月別） ---")
    source_leads = df_target.groupby(['CreateMonth', 'LeadSource']).size().unstack(fill_value=0)
    print(source_leads.to_string())

    # --- 3. Status別 ---
    print("\n--- 3. Status別分布（月別） ---")
    status_leads = df_target.groupby(['CreateMonth', 'Status']).size().unstack(fill_value=0)
    print(status_leads.to_string())

    return df_lead


def generate_summary(df_opp, won, df_lead):
    """総合サマリーを生成"""

    print("\n" + "=" * 80)
    print("  総合分析サマリー: 2月200%達成の要因分析")
    print("=" * 80)

    target_months = ['2025-11', '2025-12', '2026-01', '2026-02']

    df_opp['CloseMonth'] = df_opp['CloseDate'].dt.to_period('M')
    won['CloseMonth'] = won['CloseDate'].dt.to_period('M')

    print("\n--- 月別KPI比較 ---")
    for month in target_months:
        month_won = won[won['CloseMonth'].astype(str) == month]
        month_all = df_opp[df_opp['CloseMonth'].astype(str) == month]
        closed = month_all[month_all['IsClosed']]

        total = len(month_all)
        won_count = len(month_won)
        won_amount = month_won['Amount'].sum()
        win_rate = (won_count / len(closed) * 100) if len(closed) > 0 else 0
        avg_deal = month_won['Amount'].mean() if won_count > 0 else 0

        print(f"\n  [{month}]")
        print(f"    商談数: {total}, 受注数: {won_count}, 受注率: {win_rate:.1f}%")
        print(f"    受注金額合計: {won_amount:,.0f}, 平均単価: {avg_deal:,.0f}")

    # 2月 vs 11-1月平均の比較
    print("\n--- 2月 vs 11-1月平均 比較 ---")
    feb_won = won[won['CloseMonth'].astype(str) == '2026-02']
    prev_months = ['2025-11', '2025-12', '2026-01']
    prev_won = won[won['CloseMonth'].astype(str).isin(prev_months)]

    feb_count = len(feb_won)
    feb_amount = feb_won['Amount'].sum()

    prev_count_avg = len(prev_won) / 3 if len(prev_won) > 0 else 1
    prev_amount_avg = prev_won['Amount'].sum() / 3 if len(prev_won) > 0 else 1

    print(f"  2月受注件数: {feb_count} (前3ヶ月平均: {prev_count_avg:.1f}) -> {feb_count/prev_count_avg*100:.0f}%")
    print(f"  2月受注金額: {feb_amount:,.0f} (前3ヶ月平均: {prev_amount_avg:,.0f}) -> {feb_amount/prev_amount_avg*100:.0f}%")


def main():
    print("=" * 80)
    print("  月次営業成績分析: 11月〜2月")
    print("  目的: 2月200%達成の要因・因果関係を特定する")
    print("=" * 80)

    output_dir = project_root / 'data' / 'output' / 'analysis'
    output_dir.mkdir(parents=True, exist_ok=True)

    # 既存CSVがあればそこから読み込み
    existing_opp = sorted(output_dir.glob('opportunities_*.csv'))
    existing_lead = sorted(output_dir.glob('leads_*.csv'))

    if existing_opp and existing_lead:
        print("\n[STEP 1] 既存データから読み込み")
        opp_path = existing_opp[-1]
        lead_path = existing_lead[-1]
        df_opp = pd.read_csv(opp_path, dtype=str)
        df_lead = pd.read_csv(lead_path, dtype=str)
        print(f"  商談データ: {len(df_opp):,} 件 <- {opp_path.name}")
        print(f"  リードデータ: {len(df_lead):,} 件 <- {lead_path.name}")
    else:
        print("\n[STEP 1] Salesforceからデータ取得")
        service = OpportunityService()
        service.authenticate()

        df_opp = export_opportunity_data(service)
        df_lead = export_lead_data(service)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        opp_path = output_dir / f'opportunities_{timestamp}.csv'
        lead_path = output_dir / f'leads_{timestamp}.csv'

        df_opp.to_csv(opp_path, index=False, encoding='utf-8-sig')
        df_lead.to_csv(lead_path, index=False, encoding='utf-8-sig')
        print(f"  商談データ: {len(df_opp):,} 件 -> {opp_path}")
        print(f"  リードデータ: {len(df_lead):,} 件 -> {lead_path}")

    # 分析
    print("\n[STEP 2] 商談分析")
    df_opp, won = analyze_opportunities(df_opp)

    print("\n[STEP 3] リード分析")
    df_lead_analyzed = analyze_leads(df_lead)

    print("\n[STEP 4] 総合サマリー")
    generate_summary(df_opp, won, df_lead)

    print("\n\n分析完了。データファイル:")
    print(f"  {opp_path}")
    print(f"  {lead_path}")


if __name__ == "__main__":
    main()
