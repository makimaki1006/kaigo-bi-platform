# -*- coding: utf-8 -*-
"""
決裁者商談（Hearing_Authority__c）の詳細分析
1. 決裁者区分別の成約率（2025年4月〜、九州沖縄）
2. 決裁者区分×法人格のクロス分析（全国、2025年4月〜）
3. 決裁者到達の時間帯・曜日分析
"""

import sys
import io
from pathlib import Path
from datetime import datetime

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

from src.services.opportunity_service import OpportunityService


# 九州沖縄の都道府県リスト
KYUSHU_OKINAWA = ['福岡県', '佐賀県', '長崎県', '熊本県', '大分県', '宮崎県', '鹿児島県', '沖縄県']


def main():
    print('='*80)
    print('決裁者商談（Hearing_Authority__c）詳細分析')
    print('='*80)

    opp_service = OpportunityService()
    opp_service.authenticate()

    # データ取得（2025年4月以降のクローズ済み商談）
    soql = """
        SELECT Id, IsWon, IsClosed, CreatedDate, CloseDate,
               Account.Name,
               Account.NumberOfEmployees,
               Account.LegalPersonality__c,
               Account.ServiceType__c,
               Account.Prefectures__c,
               Account.IndustryCategory__c,
               Account.WonOpportunityies__c,
               OpportunityType__c,
               Hearing_Authority__c,
               OwnerId,
               Owner.Name
        FROM Opportunity
        WHERE IsClosed = true AND CreatedDate >= 2025-04-01T00:00:00Z
    """

    df = opp_service.bulk_query(soql, '決裁者詳細分析データ取得')

    print(f'\n取得件数: {len(df):,}件')

    # データ前処理
    df['is_won'] = df['IsWon'].map({'true': 1, 'false': 0, True: 1, False: 0}).astype(int)
    df['is_closed'] = df['IsClosed'].map({'true': 1, 'false': 0, True: 1, False: 0}).astype(int)
    df['won_count'] = pd.to_numeric(df.get('Account.WonOpportunityies__c', 0), errors='coerce').fillna(0)

    # 初回商談のみ抽出（過去成約なし）
    df['past_won_count'] = df.apply(lambda row: row['won_count'] - 1 if row['is_won'] == 1 else row['won_count'], axis=1)
    df_first = df[df['past_won_count'] == 0].copy().reset_index(drop=True)

    print(f'初回商談: {len(df_first):,}件')

    # CreatedDate を datetime に変換
    df_first['created_datetime'] = pd.to_datetime(df_first['CreatedDate'], errors='coerce', utc=True)
    df_first['created_hour'] = df_first['created_datetime'].dt.hour
    df_first['created_weekday'] = df_first['created_datetime'].dt.weekday  # 0=月曜 ... 6=日曜
    df_first['created_weekday_name'] = df_first['created_datetime'].dt.day_name()

    # 日本語曜日マッピング
    weekday_jp = {
        0: '月曜', 1: '火曜', 2: '水曜', 3: '木曜', 4: '金曜', 5: '土曜', 6: '日曜'
    }
    df_first['曜日'] = df_first['created_weekday'].map(weekday_jp)

    # Hearing_Authority__c の値確認
    print('\n--- Hearing_Authority__c 値分布 ---')
    print(df_first['Hearing_Authority__c'].value_counts(dropna=False))

    # ========================================
    # 1. 決裁者区分別の成約率（九州沖縄、2025年4月〜）
    # ========================================
    print('\n' + '='*80)
    print('1. 決裁者区分別の成約率（九州沖縄、2025年4月〜）')
    print('='*80)

    # 九州沖縄フィルタ
    df_kyushu = df_first[df_first['Account.Prefectures__c'].isin(KYUSHU_OKINAWA)].copy()
    print(f'\n九州沖縄商談数: {len(df_kyushu):,}件')

    if len(df_kyushu) > 0:
        # 県別分布
        print('\n■ 県別分布:')
        pref_dist = df_kyushu['Account.Prefectures__c'].value_counts()
        for pref, cnt in pref_dist.items():
            print(f'  {pref}: {cnt}件')

        # 決裁者区分別成約率
        print('\n■ Hearing_Authority__c 別成約率:')
        authority_stats = df_kyushu.groupby('Hearing_Authority__c', dropna=False).agg(
            商談数=('is_won', 'count'),
            成約数=('is_won', 'sum'),
            成約率=('is_won', 'mean')
        ).round(4)
        authority_stats['構成比'] = (authority_stats['商談数'] / len(df_kyushu) * 100).round(1)
        authority_stats = authority_stats.sort_values('商談数', ascending=False)

        print(authority_stats.to_string())

        # 県×決裁者区分クロス
        print('\n■ 県×決裁者区分 成約率クロス:')
        cross_kyushu = pd.crosstab(
            df_kyushu['Account.Prefectures__c'],
            df_kyushu['Hearing_Authority__c'],
            values=df_kyushu['is_won'],
            aggfunc=['count', 'mean']
        ).round(3)
        print(cross_kyushu.to_string())

    else:
        print('九州沖縄のデータがありません')

    # ========================================
    # 2. 決裁者区分×法人格のクロス分析（全国、2025年4月〜）
    # ========================================
    print('\n' + '='*80)
    print('2. 決裁者区分×法人格のクロス分析（全国、2025年4月〜）')
    print('='*80)

    print(f'\n全国商談数: {len(df_first):,}件')

    # 決裁者区分別（全国）
    print('\n■ Hearing_Authority__c 別成約率（全国）:')
    authority_all = df_first.groupby('Hearing_Authority__c', dropna=False).agg(
        商談数=('is_won', 'count'),
        成約数=('is_won', 'sum'),
        成約率=('is_won', 'mean')
    ).round(4)
    authority_all['構成比'] = (authority_all['商談数'] / len(df_first) * 100).round(1)
    authority_all = authority_all.sort_values('商談数', ascending=False)
    print(authority_all.to_string())

    # 法人格別（全国）
    print('\n■ LegalPersonality__c 別成約率（全国）:')
    legal_all = df_first.groupby('Account.LegalPersonality__c', dropna=False).agg(
        商談数=('is_won', 'count'),
        成約数=('is_won', 'sum'),
        成約率=('is_won', 'mean')
    ).query('商談数 >= 30').round(4)
    legal_all['構成比'] = (legal_all['商談数'] / len(df_first) * 100).round(1)
    legal_all = legal_all.sort_values('成約率', ascending=False)
    print(legal_all.to_string())

    # 決裁者区分×法人格クロス（件数）
    print('\n■ 決裁者区分×法人格 件数クロス:')
    cross_count = pd.crosstab(
        df_first['Hearing_Authority__c'],
        df_first['Account.LegalPersonality__c']
    )
    # 上位10法人格のみ
    top_legals = df_first['Account.LegalPersonality__c'].value_counts().head(10).index.tolist()
    cross_count_top = cross_count[[c for c in top_legals if c in cross_count.columns]]
    print(cross_count_top.to_string())

    # 決裁者区分×法人格クロス（成約率）
    print('\n■ 決裁者区分×法人格 成約率クロス:')

    # 詳細クロス集計
    cross_detail = df_first.groupby(['Hearing_Authority__c', 'Account.LegalPersonality__c']).agg(
        商談数=('is_won', 'count'),
        成約数=('is_won', 'sum'),
        成約率=('is_won', 'mean')
    ).query('商談数 >= 20').round(4)
    cross_detail = cross_detail.sort_values(['Hearing_Authority__c', '成約率'], ascending=[True, False])
    print(cross_detail.to_string())

    # ピボット形式で成約率
    cross_rate = df_first[df_first['Account.LegalPersonality__c'].isin(top_legals)].pivot_table(
        values='is_won',
        index='Hearing_Authority__c',
        columns='Account.LegalPersonality__c',
        aggfunc='mean'
    ).round(3)
    print('\n■ 成約率ピボット（上位10法人格）:')
    print(cross_rate.to_string())

    # ========================================
    # 3. 決裁者到達の時間帯・曜日分析
    # ========================================
    print('\n' + '='*80)
    print('3. 決裁者到達の時間帯・曜日分析')
    print('='*80)

    # 時間帯分析
    print('\n■ 時間帯別商談作成数と成約率:')

    # 時間帯カテゴリ
    def categorize_hour(hour):
        if pd.isna(hour):
            return 'Unknown'
        hour = int(hour)
        if 0 <= hour < 9:
            return '早朝(0-8時)'
        elif 9 <= hour < 12:
            return '午前(9-11時)'
        elif 12 <= hour < 14:
            return '昼(12-13時)'
        elif 14 <= hour < 18:
            return '午後(14-17時)'
        elif 18 <= hour < 21:
            return '夕方(18-20時)'
        else:
            return '深夜(21-23時)'

    df_first['時間帯'] = df_first['created_hour'].apply(categorize_hour)

    # 時間帯別集計
    hour_stats = df_first.groupby('時間帯').agg(
        商談数=('is_won', 'count'),
        成約数=('is_won', 'sum'),
        成約率=('is_won', 'mean')
    ).round(4)
    hour_stats['構成比'] = (hour_stats['商談数'] / len(df_first) * 100).round(1)

    # 時間帯を順序付け
    hour_order = ['早朝(0-8時)', '午前(9-11時)', '昼(12-13時)', '午後(14-17時)', '夕方(18-20時)', '深夜(21-23時)', 'Unknown']
    hour_stats = hour_stats.reindex([h for h in hour_order if h in hour_stats.index])
    print(hour_stats.to_string())

    # 詳細時間別
    print('\n■ 1時間単位の商談作成数と成約率:')
    hourly = df_first.groupby('created_hour', dropna=False).agg(
        商談数=('is_won', 'count'),
        成約数=('is_won', 'sum'),
        成約率=('is_won', 'mean')
    ).round(4)
    hourly = hourly.sort_index()
    print(hourly.to_string())

    # 曜日別分析
    print('\n■ 曜日別商談作成数と成約率:')
    weekday_stats = df_first.groupby('曜日').agg(
        商談数=('is_won', 'count'),
        成約数=('is_won', 'sum'),
        成約率=('is_won', 'mean')
    ).round(4)
    weekday_stats['構成比'] = (weekday_stats['商談数'] / len(df_first) * 100).round(1)

    # 曜日順序付け
    weekday_order = ['月曜', '火曜', '水曜', '木曜', '金曜', '土曜', '日曜']
    weekday_stats = weekday_stats.reindex([w for w in weekday_order if w in weekday_stats.index])
    print(weekday_stats.to_string())

    # 時間帯×決裁者区分クロス
    print('\n■ 時間帯×Hearing_Authority__c クロス（成約率）:')
    time_auth_cross = df_first.pivot_table(
        values='is_won',
        index='時間帯',
        columns='Hearing_Authority__c',
        aggfunc='mean'
    ).round(3)
    time_auth_cross = time_auth_cross.reindex([h for h in hour_order if h in time_auth_cross.index])
    print(time_auth_cross.to_string())

    # 時間帯×決裁者区分クロス（件数）
    print('\n■ 時間帯×Hearing_Authority__c クロス（件数）:')
    time_auth_count = pd.crosstab(df_first['時間帯'], df_first['Hearing_Authority__c'])
    time_auth_count = time_auth_count.reindex([h for h in hour_order if h in time_auth_count.index])
    print(time_auth_count.to_string())

    # 曜日×決裁者区分クロス
    print('\n■ 曜日×Hearing_Authority__c クロス（成約率）:')
    weekday_auth_cross = df_first.pivot_table(
        values='is_won',
        index='曜日',
        columns='Hearing_Authority__c',
        aggfunc='mean'
    ).round(3)
    weekday_auth_cross = weekday_auth_cross.reindex([w for w in weekday_order if w in weekday_auth_cross.index])
    print(weekday_auth_cross.to_string())

    # 曜日×決裁者区分クロス（件数）
    print('\n■ 曜日×Hearing_Authority__c クロス（件数）:')
    weekday_auth_count = pd.crosstab(df_first['曜日'], df_first['Hearing_Authority__c'])
    weekday_auth_count = weekday_auth_count.reindex([w for w in weekday_order if w in weekday_auth_count.index])
    print(weekday_auth_count.to_string())

    # ========================================
    # 追加分析: 担当者別（九州沖縄）
    # ========================================
    print('\n' + '='*80)
    print('追加: 担当者別成約率（九州沖縄）')
    print('='*80)

    if len(df_kyushu) > 0:
        owner_kyushu = df_kyushu.groupby('Owner.Name').agg(
            商談数=('is_won', 'count'),
            成約数=('is_won', 'sum'),
            成約率=('is_won', 'mean')
        ).query('商談数 >= 10').sort_values('成約率', ascending=False).round(4)
        print(owner_kyushu.to_string())

    # ========================================
    # サマリー出力
    # ========================================
    print('\n' + '='*80)
    print('サマリー')
    print('='*80)

    # 九州沖縄の最高成約率Hearing_Authority
    if len(df_kyushu) > 0:
        best_auth_kyushu = authority_stats[authority_stats['商談数'] >= 10].nlargest(1, '成約率')
        if len(best_auth_kyushu) > 0:
            print(f'\n■ 九州沖縄ベスト決裁者区分:')
            print(f'  {best_auth_kyushu.index[0]}: 成約率 {best_auth_kyushu["成約率"].values[0]:.1%} (n={int(best_auth_kyushu["商談数"].values[0])})')

    # 全国の最高成約率組み合わせ
    if len(cross_detail) > 0:
        best_combo = cross_detail.nlargest(5, '成約率')
        print(f'\n■ 全国ベスト決裁者区分×法人格 TOP5:')
        for idx, row in best_combo.iterrows():
            print(f'  {idx[0]} × {idx[1]}: 成約率 {row["成約率"]:.1%} (n={int(row["商談数"])})')

    # 最高成約率時間帯
    best_hour = hour_stats[hour_stats['商談数'] >= 50].nlargest(1, '成約率')
    if len(best_hour) > 0:
        print(f'\n■ ベスト時間帯:')
        print(f'  {best_hour.index[0]}: 成約率 {best_hour["成約率"].values[0]:.1%} (n={int(best_hour["商談数"].values[0])})')

    # 最高成約率曜日
    best_weekday = weekday_stats[weekday_stats['商談数'] >= 50].nlargest(1, '成約率')
    if len(best_weekday) > 0:
        print(f'\n■ ベスト曜日:')
        print(f'  {best_weekday.index[0]}: 成約率 {best_weekday["成約率"].values[0]:.1%} (n={int(best_weekday["商談数"].values[0])})')

    print('\n' + '='*80)
    print('分析完了')
    print('='*80)

    return df_first, authority_stats, cross_detail


if __name__ == "__main__":
    df_first, authority_stats, cross_detail = main()
