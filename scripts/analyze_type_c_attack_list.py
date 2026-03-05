# -*- coding: utf-8 -*-
"""
タイプC アタックリスト用セグメント分析
- ローラー架電に使える優先度別セグメント
- ボリュームを確保しつつ優先度を付ける
"""

import sys
import io
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

from src.services.opportunity_service import OpportunityService


def main():
    opp_service = OpportunityService()
    opp_service.authenticate()

    soql = """
        SELECT Id, IsWon, CreatedDate,
               Account.NumberOfEmployees,
               Account.Population__c,
               Account.LegalPersonality__c,
               Account.ServiceType__c,
               Account.Prefectures__c,
               Account.WonOpportunityies__c,
               OpportunityType__c,
               Hearing_Authority__c
        FROM Opportunity
        WHERE IsClosed = true AND CreatedDate >= 2025-04-01T00:00:00Z
    """
    df = opp_service.bulk_query(soql, 'アタックリスト分析')

    df['is_won'] = df['IsWon'].map({'true': 1, 'false': 0, True: 1, False: 0})
    df['won_count'] = pd.to_numeric(df.get('Account.WonOpportunityies__c', 0), errors='coerce').fillna(0)
    df['past_won_count'] = df.apply(lambda row: row['won_count'] - 1 if row['is_won'] == 1 else row['won_count'], axis=1)
    df = df[df['past_won_count'] == 0].copy().reset_index(drop=True)

    df['employees'] = pd.to_numeric(df['Account.NumberOfEmployees'], errors='coerce')
    df['population'] = pd.to_numeric(df['Account.Population__c'], errors='coerce')

    # 決裁者タイプ分類
    def classify_decision_type(row):
        opp_type = str(row['OpportunityType__c'])
        authority = str(row['Hearing_Authority__c'])
        if '決裁者' in opp_type and '代表者' in opp_type:
            return 'A: 代表者商談（決裁者）'
        elif '決裁者' in opp_type and '担当者' in opp_type:
            return 'B: 担当者商談（決裁者）'
        elif authority == 'あり':
            return 'C: 担当者商談（決裁権あり）'
        else:
            return 'D: 決裁権なし'

    df['decision_type'] = df.apply(classify_decision_type, axis=1)
    df['is_type_c'] = (df['decision_type'] == 'C: 担当者商談（決裁権あり）').astype(int)

    # 大分類を作成
    df['法人格'] = df['Account.LegalPersonality__c'].apply(
        lambda x: '株式会社' if x == '株式会社' else 'その他法人'
    )

    def classify_service(x):
        x = str(x)
        if '訪問' in x:
            return '訪問系'
        elif '通所' in x or 'デイ' in x:
            return '通所系'
        elif '入所' in x or '入居' in x or 'ホーム' in x or '共同生活' in x or '短期' in x:
            return '入所系'
        else:
            return 'その他'

    df['サービス'] = df['Account.ServiceType__c'].apply(classify_service)

    def classify_region(pref):
        pref = str(pref)
        if pref in ['北海道']:
            return '北海道'
        elif pref in ['青森県', '岩手県', '宮城県', '秋田県', '山形県', '福島県']:
            return '東北'
        elif pref in ['東京都', '神奈川県', '埼玉県', '千葉県', '茨城県', '栃木県', '群馬県']:
            return '関東'
        elif pref in ['新潟県', '富山県', '石川県', '福井県', '山梨県', '長野県']:
            return '中部北陸'
        elif pref in ['岐阜県', '静岡県', '愛知県', '三重県']:
            return '東海'
        elif pref in ['滋賀県', '京都府', '大阪府', '兵庫県', '奈良県', '和歌山県']:
            return '関西'
        elif pref in ['鳥取県', '島根県', '岡山県', '広島県', '山口県']:
            return '中国'
        elif pref in ['徳島県', '香川県', '愛媛県', '高知県']:
            return '四国'
        elif pref in ['福岡県', '佐賀県', '長崎県', '熊本県', '大分県', '宮崎県', '鹿児島県', '沖縄県']:
            return '九州沖縄'
        else:
            return 'その他'

    df['地域'] = df['Account.Prefectures__c'].apply(classify_region)

    print('='*80)
    print('タイプC アタックリスト用セグメント分析')
    print('='*80)
    print(f'\n全体: {len(df):,}件, 成約率: {df["is_won"].mean():.1%}')
    print(f'タイプC発生率: {df["is_type_c"].mean():.1%}')

    # ========================================
    # 1. 法人格 × サービス（4セグメント）
    # ========================================
    print('\n' + '='*80)
    print('【セグメント1】法人格 × サービス（4分類）')
    print('='*80)

    seg1 = df.groupby(['法人格', 'サービス']).agg(
        件数=('is_won', 'count'),
        成約数=('is_won', 'sum'),
        成約率=('is_won', 'mean'),
        タイプC発生率=('is_type_c', 'mean')
    ).round(4)

    # 期待値計算
    df_c = df[df['is_type_c'] == 1]
    seg1_c = df_c.groupby(['法人格', 'サービス']).agg(
        タイプC成約率=('is_won', 'mean')
    ).round(4)
    seg1 = seg1.join(seg1_c, how='left').fillna(0)
    seg1['スコア'] = (seg1['成約率'] * 100).round(1)
    seg1 = seg1.sort_values('スコア', ascending=False)

    print(seg1.to_string())

    # ========================================
    # 2. 優先度別に分ける
    # ========================================
    print('\n' + '='*80)
    print('【優先度別セグメント】')
    print('='*80)

    # 優先度ロジックを作成
    def assign_priority(row):
        legal = row['法人格']
        service = row['サービス']

        # S: 株式会社 × 訪問系
        if legal == '株式会社' and service == '訪問系':
            return 'S'
        # A: 株式会社 × その他サービス
        elif legal == '株式会社':
            return 'A'
        # B: その他法人 × 訪問系/通所系
        elif service in ['訪問系', '通所系']:
            return 'B'
        # C: その他法人 × 入所系/その他
        else:
            return 'C'

    df['優先度'] = df.apply(assign_priority, axis=1)

    priority_stats = df.groupby('優先度').agg(
        件数=('is_won', 'count'),
        成約数=('is_won', 'sum'),
        成約率=('is_won', 'mean'),
        タイプC発生率=('is_type_c', 'mean')
    ).round(4)

    priority_c = df_c.groupby(df[df['is_type_c']==1]['優先度']).agg(
        タイプC成約率=('is_won', 'mean')
    ).round(4)
    priority_stats = priority_stats.join(priority_c, how='left').fillna(0)

    print('\n【優先度サマリー】')
    for priority in ['S', 'A', 'B', 'C']:
        if priority in priority_stats.index:
            row = priority_stats.loc[priority]
            print(f'\n優先度{priority}: {int(row["件数"]):,}件')
            print(f'  成約率: {row["成約率"]*100:.1f}%')
            print(f'  タイプC発生率: {row["タイプC発生率"]*100:.1f}%')
            print(f'  タイプC成約率: {row["タイプC成約率"]*100:.1f}%')

    # ========================================
    # 3. 地域を加えた詳細セグメント
    # ========================================
    print('\n' + '='*80)
    print('【地域別の詳細セグメント】')
    print('='*80)

    for priority in ['S', 'A', 'B', 'C']:
        df_p = df[df['優先度'] == priority]
        if len(df_p) == 0:
            continue

        print(f'\n■ 優先度{priority}（全体{len(df_p):,}件）の地域内訳:')

        region_stats = df_p.groupby('地域').agg(
            件数=('is_won', 'count'),
            成約数=('is_won', 'sum'),
            成約率=('is_won', 'mean')
        ).sort_values('件数', ascending=False).round(4)

        for region, row in region_stats.iterrows():
            print(f'  {region}: {int(row["件数"]):,}件, 成約{int(row["成約数"])}件, 成約率{row["成約率"]*100:.1f}%')

    # ========================================
    # 4. アタックリスト用の最終セグメント
    # ========================================
    print('\n' + '='*80)
    print('【アタックリスト用 最終セグメント】')
    print('='*80)

    # 優先度 × 地域
    attack_list = df.groupby(['優先度', '地域']).agg(
        件数=('is_won', 'count'),
        成約数=('is_won', 'sum'),
        成約率=('is_won', 'mean')
    ).round(4)

    attack_list = attack_list.reset_index()
    attack_list['順位スコア'] = attack_list['優先度'].map({'S': 4, 'A': 3, 'B': 2, 'C': 1}) * 100 + attack_list['成約率'] * 100
    attack_list = attack_list.sort_values('順位スコア', ascending=False)

    print('\n【ローラー架電順序（推奨）】\n')
    print(f'{"順位":<4} {"優先度":<4} {"地域":<10} {"件数":>8} {"成約数":>6} {"成約率":>8}')
    print('-' * 50)

    for i, (_, row) in enumerate(attack_list.iterrows(), 1):
        print(f'{i:<4} {row["優先度"]:<4} {row["地域"]:<10} {int(row["件数"]):>8,} {int(row["成約数"]):>6} {row["成約率"]*100:>7.1f}%')

    # ========================================
    # 5. セグメント定義の明確化
    # ========================================
    print('\n' + '='*80)
    print('【セグメント定義】')
    print('='*80)

    print('''
┌─────────────────────────────────────────────────────────────────────────────┐
│ 優先度 │ 定義                         │ 狙う理由                          │
├────────┼──────────────────────────────┼───────────────────────────────────┤
│   S    │ 株式会社 × 訪問系            │ タイプC成約率35.7%（最高）        │
│        │ (訪問介護、訪問看護等)       │                                   │
├────────┼──────────────────────────────┼───────────────────────────────────┤
│   A    │ 株式会社 × その他サービス    │ 株式会社はタイプC成約率21%        │
│        │ (通所介護、有料老人ホーム等) │ 法人格だけで2倍の差               │
├────────┼──────────────────────────────┼───────────────────────────────────┤
│   B    │ その他法人 × 訪問系/通所系   │ サービス種別で救える              │
│        │ (医療法人、社福の訪問・通所) │ タイプC発生率5-6%                 │
├────────┼──────────────────────────────┼───────────────────────────────────┤
│   C    │ その他法人 × 入所系/その他   │ ベースライン                      │
│        │ (社福の入所施設等)           │ 母数確保のため架電                │
└─────────────────────────────────────────────────────────────────────────────┘

【サービス種別の分類】
  訪問系: 訪問介護、訪問看護、訪問リハビリテーション、訪問入浴
  通所系: 通所介護、地域密着型通所介護、通所リハビリテーション
  入所系: 特養、老健、グループホーム、有料老人ホーム、短期入所
  その他: 居宅介護支援、小規模多機能、その他

【法人格の分類】
  株式会社: 株式会社のみ
  その他法人: 医療法人、社会福祉法人、有限会社、合同会社、NPO法人、その他
''')

    # ========================================
    # 6. 件数サマリー
    # ========================================
    print('\n' + '='*80)
    print('【件数サマリー（アタックリスト規模感）】')
    print('='*80)

    summary = df.groupby('優先度').agg(
        件数=('is_won', 'count'),
        成約率=('is_won', 'mean')
    ).round(4)

    total = len(df)
    print(f'\n全体: {total:,}件\n')

    cumulative = 0
    for priority in ['S', 'A', 'B', 'C']:
        if priority in summary.index:
            row = summary.loc[priority]
            cumulative += row['件数']
            pct = row['件数'] / total * 100
            cum_pct = cumulative / total * 100
            print(f'優先度{priority}: {int(row["件数"]):>5,}件 ({pct:>5.1f}%)  累計: {int(cumulative):>5,}件 ({cum_pct:>5.1f}%)  成約率: {row["成約率"]*100:.1f}%')

    return df, attack_list


if __name__ == "__main__":
    df, attack_list = main()
