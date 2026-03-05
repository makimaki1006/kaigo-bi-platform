# -*- coding: utf-8 -*-
"""
都道府県別 複数年受注率分析
- 毎年安定して受注率が高い/低い都道府県を特定
- リスト偏り（バイアス）検出も実施
"""
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / 'src'))

import os
os.environ['PYTHONIOENCODING'] = 'utf-8'

import pandas as pd
import numpy as np
from src.services.opportunity_service import OpportunityService

# 施設形態補完マッピング
INDUSTRY_MAP = {
    '医療': '医療', '介護': '介護（高齢者）', '障害福祉': '障がい福祉',
    '保育': '保育', '障害者福祉': '障がい福祉', '高齢者福祉': '介護（高齢者）',
}
SERVICE_TYPE_MAP = {
    '訪問介護': '介護（高齢者）', '通所介護': '介護（高齢者）', '訪問看護': '介護（高齢者）',
    '居宅介護支援': '介護（高齢者）', '短期入所生活介護': '介護（高齢者）',
    '介護老人福祉施設': '介護（高齢者）', '介護老人保健施設': '介護（高齢者）',
    '認知症対応型共同生活介護': '介護（高齢者）', '地域密着型通所介護': '介護（高齢者）',
    '特定施設入居者生活介護（有料老人ホーム）': '介護（高齢者）',
    '小規模多機能型居宅介護': '介護（高齢者）', '通所リハビリテーション': '介護（高齢者）',
    '訪問リハビリテーション': '介護（高齢者）', '福祉用具貸与': '介護（高齢者）',
    '放課後等デイサービス': '障がい福祉', '就労継続支援Ａ型': '障がい福祉',
    '就労継続支援Ｂ型': '障がい福祉', '共同生活援助（グループホーム）': '障がい福祉',
    '生活介護': '障がい福祉', '児童発達支援': '障がい福祉',
    '保育所': '保育', '認定こども園': '保育',
}

def get_facility_type(row):
    """施設形態の補完ロジック"""
    ft = row.get('FacilityType_Large__c')
    if pd.notna(ft) and ft:
        return ft
    ic = row.get('IndustryCategory__c')
    if pd.notna(ic) and ic:
        for key, val in INDUSTRY_MAP.items():
            if key in str(ic):
                return val
    st = row.get('ServiceType__c')
    if pd.notna(st) and st:
        return SERVICE_TYPE_MAP.get(st, '不明')
    return '不明'

def main():
    print('=' * 80)
    print('都道府県別 複数年受注率分析（バイアス検出付き）')
    print('=' * 80)

    svc = OpportunityService()
    svc.authenticate()
    print('Salesforce認証成功')

    # 全商談（初回商談のみ）を年度別に取得
    query = """
    SELECT Id, AccountId, CloseDate, IsWon, IsClosed,
           FacilityType_Large__c, OpportunityCategory__c,
           Account.Prefectures__c, Account.IndustryCategory__c,
           Account.ServiceType__c, Account.NumberOfEmployees,
           Account.WonOpportunityies__c
    FROM Opportunity
    WHERE IsClosed = true
      AND OpportunityCategory__c = '初回商談'
      AND Account.WonOpportunityies__c < 2
    """
    print('\n全期間の初回商談を取得...')
    df = svc.bulk_query(query, '全初回商談')

    # Bulk APIのカラム名を確認
    print(f'  カラム: {list(df.columns)}')

    df['CloseDate'] = pd.to_datetime(df['CloseDate'])
    df['is_won'] = df['IsWon'].astype(str).str.lower() == 'true'

    # Bulk API 2.0のリレーション項目カラム名を解決
    pref_col = next((c for c in df.columns if 'Prefectures' in c), None)
    ic_col = next((c for c in df.columns if 'IndustryCategory' in c), None)
    st_col = next((c for c in df.columns if 'ServiceType__c' in c and 'ServiceType2' not in c and 'ServiceType3' not in c), None)
    emp_col = next((c for c in df.columns if 'NumberOfEmployees' in c), None)
    won_opp_col = next((c for c in df.columns if 'WonOpportunityies' in c), None)

    print(f'  都道府県カラム: {pref_col}')
    print(f'  業界カラム: {ic_col}')
    print(f'  サービスカラム: {st_col}')

    df['prefecture'] = df[pref_col].fillna('不明') if pref_col else '不明'

    # get_facility_type用にカラム名を統一
    if ic_col and ic_col != 'IndustryCategory__c':
        df['IndustryCategory__c'] = df[ic_col]
    if st_col and st_col != 'ServiceType__c':
        df['ServiceType__c'] = df[st_col]
    df['facility_type'] = df.apply(get_facility_type, axis=1)

    # 年度を付与（4月始まり）
    df['fiscal_year'] = df['CloseDate'].apply(
        lambda d: f'FY{d.year}' if d.month >= 4 else f'FY{d.year - 1}'
    )

    # 年度一覧
    fy_list = sorted(df['fiscal_year'].unique())
    print(f'\n年度一覧: {", ".join(fy_list)}')
    for fy in fy_list:
        sub = df[df['fiscal_year'] == fy]
        print(f'  {fy}: {len(sub)}件, 受注{sub["is_won"].sum()}件, 受注率{sub["is_won"].mean()*100:.1f}%')

    # 十分なデータがある年度のみ（100件以上）
    valid_fys = [fy for fy in fy_list if len(df[df['fiscal_year'] == fy]) >= 100]
    print(f'\n分析対象年度（100件以上）: {", ".join(valid_fys)}')

    # === 1. 都道府県×年度のクロス集計 ===
    print(f'\n{"=" * 80}')
    print('1. 都道府県×年度 受注率マトリクス')
    print(f'{"=" * 80}')

    # 年度別・都道府県別集計
    pref_fy = df[df['fiscal_year'].isin(valid_fys)].groupby(
        ['prefecture', 'fiscal_year']
    ).agg(
        total=('Id', 'count'),
        won=('is_won', 'sum')
    ).reset_index()
    pref_fy['rate'] = pref_fy['won'] / pref_fy['total']

    # ピボット
    pivot_total = pref_fy.pivot(index='prefecture', columns='fiscal_year', values='total').fillna(0)
    pivot_rate = pref_fy.pivot(index='prefecture', columns='fiscal_year', values='rate')

    # 全年度にデータがある都道府県
    all_year_prefs = pivot_total[pivot_total.min(axis=1) >= 10].index.tolist()
    print(f'\n全年度で10件以上ある都道府県: {len(all_year_prefs)}')

    # 各都道府県の年度別受注率
    results = []
    for pref in all_year_prefs:
        row = {'prefecture': pref}
        rates = []
        totals = []
        for fy in valid_fys:
            sub = pref_fy[(pref_fy['prefecture'] == pref) & (pref_fy['fiscal_year'] == fy)]
            if len(sub) > 0:
                row[f'{fy}_件数'] = int(sub['total'].values[0])
                row[f'{fy}_受注率'] = sub['rate'].values[0]
                rates.append(sub['rate'].values[0])
                totals.append(int(sub['total'].values[0]))
            else:
                row[f'{fy}_件数'] = 0
                row[f'{fy}_受注率'] = None
                rates.append(None)
                totals.append(0)

        valid_rates = [r for r in rates if r is not None]
        if len(valid_rates) >= 2:
            row['平均受注率'] = np.mean(valid_rates)
            row['最低受注率'] = min(valid_rates)
            row['最高受注率'] = max(valid_rates)
            row['標準偏差'] = np.std(valid_rates)
            row['年度数'] = len(valid_rates)
            row['合計件数'] = sum(totals)
            # 安定度 = 最低が平均の何%か（高いほど安定）
            if row['平均受注率'] > 0:
                row['安定度'] = row['最低受注率'] / row['平均受注率']
            else:
                row['安定度'] = 0
            # 全年度で全体平均を上回った回数
            above_avg_count = 0
            for i, fy in enumerate(valid_fys):
                if rates[i] is not None:
                    fy_avg = df[df['fiscal_year'] == fy]['is_won'].mean()
                    if rates[i] > fy_avg:
                        above_avg_count += 1
            row['平均超え回数'] = above_avg_count
            row['平均超え率'] = above_avg_count / len(valid_rates)
            results.append(row)

    df_results = pd.DataFrame(results)

    # === 2. 安定して高い都道府県 ===
    print(f'\n{"=" * 80}')
    print('2. 毎年安定して受注率が高い都道府県')
    print('   条件: 平均受注率8%超 & 全年度で全体平均超え率50%以上 & 合計50件以上')
    print(f'{"=" * 80}')

    good_prefs = df_results[
        (df_results['平均受注率'] > 0.08) &
        (df_results['平均超え率'] >= 0.5) &
        (df_results['合計件数'] >= 50)
    ].sort_values('平均受注率', ascending=False)

    for _, row in good_prefs.iterrows():
        print(f'\n  {row["prefecture"]}:')
        print(f'    平均受注率: {row["平均受注率"]*100:.1f}%  最低: {row["最低受注率"]*100:.1f}%  最高: {row["最高受注率"]*100:.1f}%')
        print(f'    合計: {row["合計件数"]:.0f}件  平均超え: {row["平均超え回数"]:.0f}/{row["年度数"]:.0f}年度')
        fy_detail = '    '
        for fy in valid_fys:
            n = row.get(f'{fy}_件数', 0)
            r = row.get(f'{fy}_受注率')
            if r is not None and n > 0:
                fy_detail += f'{fy}: {r*100:.1f}%({n:.0f}件)  '
        print(fy_detail)

    # === 3. 安定して低い都道府県 ===
    print(f'\n{"=" * 80}')
    print('3. 毎年安定して受注率が低い都道府県')
    print('   条件: 平均受注率5%未満 & 全体平均超え率50%未満 & 合計50件以上')
    print(f'{"=" * 80}')

    bad_prefs = df_results[
        (df_results['平均受注率'] < 0.05) &
        (df_results['平均超え率'] < 0.5) &
        (df_results['合計件数'] >= 50)
    ].sort_values('平均受注率', ascending=True)

    for _, row in bad_prefs.iterrows():
        print(f'\n  {row["prefecture"]}:')
        print(f'    平均受注率: {row["平均受注率"]*100:.1f}%  最低: {row["最低受注率"]*100:.1f}%  最高: {row["最高受注率"]*100:.1f}%')
        print(f'    合計: {row["合計件数"]:.0f}件  平均超え: {row["平均超え回数"]:.0f}/{row["年度数"]:.0f}年度')
        fy_detail = '    '
        for fy in valid_fys:
            n = row.get(f'{fy}_件数', 0)
            r = row.get(f'{fy}_受注率')
            if r is not None and n > 0:
                fy_detail += f'{fy}: {r*100:.1f}%({n:.0f}件)  '
        print(fy_detail)

    # === 4. バイアス検出: リスト偏り ===
    print(f'\n{"=" * 80}')
    print('4. バイアス検出: 都道府県別の商談件数シェア推移')
    print('   特定年度にだけ集中 → リストが作為的に偏っている可能性')
    print(f'{"=" * 80}')

    # 年度別の件数シェア
    for pref in all_year_prefs:
        shares = []
        for fy in valid_fys:
            sub_pref = pref_fy[(pref_fy['prefecture'] == pref) & (pref_fy['fiscal_year'] == fy)]
            sub_total = pref_fy[pref_fy['fiscal_year'] == fy]['total'].sum()
            if len(sub_pref) > 0 and sub_total > 0:
                shares.append(sub_pref['total'].values[0] / sub_total)
            else:
                shares.append(0)

        # シェアの変動が大きい都道府県（CV > 0.5）を検出
        if len(shares) >= 2 and np.mean(shares) > 0:
            cv = np.std(shares) / np.mean(shares)
            if cv > 0.5:
                print(f'\n  ⚠️ {pref}: シェア変動が大きい（CV={cv:.2f}）')
                for i, fy in enumerate(valid_fys):
                    print(f'    {fy}: シェア{shares[i]*100:.1f}%')

    # === 5. バイアス検出: 施設形態の構成比 ===
    print(f'\n{"=" * 80}')
    print('5. バイアス検出: 高受注率の都道府県の施設形態構成')
    print('   保育や介護×訪問看護が多いだけ → 地域効果ではなく施設形態効果')
    print(f'{"=" * 80}')

    if len(good_prefs) > 0:
        for _, row in good_prefs.head(10).iterrows():
            pref = row['prefecture']
            pref_data = df[df['prefecture'] == pref]
            ft_dist = pref_data.groupby('facility_type').agg(
                total=('Id', 'count'),
                won=('is_won', 'sum')
            ).reset_index()
            ft_dist['rate'] = ft_dist['won'] / ft_dist['total']
            ft_dist = ft_dist.sort_values('total', ascending=False)

            print(f'\n  {pref}（平均受注率{row["平均受注率"]*100:.1f}%）:')
            for _, ft_row in ft_dist.iterrows():
                print(f'    {ft_row["facility_type"]}: {ft_row["total"]}件, 受注率{ft_row["rate"]*100:.1f}%')

    # 全体の施設形態構成比と比較
    print(f'\n  【参考】全体の施設形態構成:')
    overall_ft = df.groupby('facility_type').agg(
        total=('Id', 'count'),
        won=('is_won', 'sum')
    ).reset_index()
    overall_ft['rate'] = overall_ft['won'] / overall_ft['total']
    overall_ft['share'] = overall_ft['total'] / overall_ft['total'].sum()
    overall_ft = overall_ft.sort_values('total', ascending=False)
    for _, ft_row in overall_ft.iterrows():
        print(f'    {ft_row["facility_type"]}: {ft_row["total"]}件({ft_row["share"]*100:.1f}%), 受注率{ft_row["rate"]*100:.1f}%')

    # === 6. 純粋な地域効果の推定 ===
    print(f'\n{"=" * 80}')
    print('6. 施設形態を統制した地域効果（施設形態別の受注率差分で評価）')
    print('   同じ施設形態でも特定の地域で受注率が高いか？')
    print(f'{"=" * 80}')

    # 施設形態別の全国平均受注率
    ft_avg = df.groupby('facility_type')['is_won'].mean().to_dict()

    # 各都道府県×施設形態の受注率と全国平均との差分
    pref_adjusted = {}
    for pref in all_year_prefs:
        pref_data = df[df['prefecture'] == pref]
        diffs = []
        weights = []
        for ft, ft_data in pref_data.groupby('facility_type'):
            if len(ft_data) >= 10 and ft in ft_avg:
                local_rate = ft_data['is_won'].mean()
                national_rate = ft_avg[ft]
                diff = local_rate - national_rate
                diffs.append(diff)
                weights.append(len(ft_data))
        if len(diffs) > 0:
            weighted_diff = np.average(diffs, weights=weights)
            pref_adjusted[pref] = {
                'weighted_diff': weighted_diff,
                'n_types': len(diffs),
                'total': sum(weights)
            }

    # ソートして表示
    sorted_prefs = sorted(pref_adjusted.items(), key=lambda x: x[1]['weighted_diff'], reverse=True)

    print('\n  施設形態を統制した後の受注率差分（加重平均）:')
    print(f'  {"都道府県":<8} {"差分":>8} {"施設形態数":>10} {"件数":>8}')
    print(f'  {"-"*40}')
    for pref, vals in sorted_prefs:
        sign = '+' if vals['weighted_diff'] >= 0 else ''
        print(f'  {pref:<8} {sign}{vals["weighted_diff"]*100:.1f}%pt  {vals["n_types"]:>8}形態  {vals["total"]:>6}件')

    print(f'\n{"=" * 80}')
    print('分析完了')
    print(f'{"=" * 80}')

if __name__ == '__main__':
    main()
