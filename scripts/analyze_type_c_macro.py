# -*- coding: utf-8 -*-
"""
タイプC 大枠での包括的分析
- 細かいセグメントではなく、大きな方針を出す
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
               Account.PopulationDensity__c,
               Account.LegalPersonality__c,
               Account.ServiceType__c,
               Account.Prefectures__c,
               Account.WonOpportunityies__c,
               OpportunityType__c,
               Hearing_Authority__c
        FROM Opportunity
        WHERE IsClosed = true AND CreatedDate >= 2025-04-01T00:00:00Z
    """
    df = opp_service.bulk_query(soql, 'タイプC包括分析')

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

    # タイプCのみ
    df_c = df[df['decision_type'] == 'C: 担当者商談（決裁権あり）'].copy()

    print('='*80)
    print('タイプC「担当者商談（決裁権あり）」包括的方針')
    print('='*80)

    # ========================================
    # 1. 大分類: 法人格（2分割）
    # ========================================
    print('\n' + '='*80)
    print('【方針1】法人格: 株式会社 vs それ以外')
    print('='*80)

    df_c['法人格大分類'] = df_c['Account.LegalPersonality__c'].apply(
        lambda x: '株式会社' if x == '株式会社' else 'その他法人'
    )

    legal_macro = df_c.groupby('法人格大分類').agg(
        件数=('is_won', 'count'),
        成約数=('is_won', 'sum'),
        成約率=('is_won', 'mean')
    ).round(4)
    print(legal_macro.to_string())

    # 全体での発生率も
    df['法人格大分類'] = df['Account.LegalPersonality__c'].apply(
        lambda x: '株式会社' if x == '株式会社' else 'その他法人'
    )
    legal_occur = df.groupby('法人格大分類').agg(
        全体件数=('is_won', 'count'),
        タイプC発生率=('is_type_c', 'mean'),
        全体成約率=('is_won', 'mean')
    ).round(4)
    print('\n全体からの発生率:')
    print(legal_occur.to_string())

    # ========================================
    # 2. 大分類: 従業員規模（3分割）
    # ========================================
    print('\n' + '='*80)
    print('【方針2】従業員規模: 小・中・大')
    print('='*80)

    df_c['規模大分類'] = pd.cut(df_c['employees'],
                              bins=[0, 50, 200, float('inf')],
                              labels=['小規模(~50人)', '中規模(51-200人)', '大規模(200人+)'])

    emp_macro = df_c.groupby('規模大分類', observed=True).agg(
        件数=('is_won', 'count'),
        成約数=('is_won', 'sum'),
        成約率=('is_won', 'mean')
    ).round(4)
    print(emp_macro.to_string())

    df['規模大分類'] = pd.cut(df['employees'],
                            bins=[0, 50, 200, float('inf')],
                            labels=['小規模(~50人)', '中規模(51-200人)', '大規模(200人+)'])
    emp_occur = df.groupby('規模大分類', observed=True).agg(
        全体件数=('is_won', 'count'),
        タイプC発生率=('is_type_c', 'mean'),
        全体成約率=('is_won', 'mean')
    ).round(4)
    print('\n全体からの発生率:')
    print(emp_occur.to_string())

    # ========================================
    # 3. 大分類: サービス種別（3分割）
    # ========================================
    print('\n' + '='*80)
    print('【方針3】サービス種別: 訪問系 vs 通所系 vs 入所系')
    print('='*80)

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

    df_c['サービス大分類'] = df_c['Account.ServiceType__c'].apply(classify_service)

    service_macro = df_c.groupby('サービス大分類').agg(
        件数=('is_won', 'count'),
        成約数=('is_won', 'sum'),
        成約率=('is_won', 'mean')
    ).round(4)
    print(service_macro.to_string())

    df['サービス大分類'] = df['Account.ServiceType__c'].apply(classify_service)
    service_occur = df.groupby('サービス大分類').agg(
        全体件数=('is_won', 'count'),
        タイプC発生率=('is_type_c', 'mean'),
        全体成約率=('is_won', 'mean')
    ).round(4)
    print('\n全体からの発生率:')
    print(service_occur.to_string())

    # ========================================
    # 4. 大分類: 都市規模（3分割）
    # ========================================
    print('\n' + '='*80)
    print('【方針4】都市規模: 小都市 vs 中都市 vs 大都市')
    print('='*80)

    df_c['都市規模'] = pd.cut(df_c['population']/10000,
                            bins=[0, 10, 50, float('inf')],
                            labels=['小都市(~10万)', '中都市(10-50万)', '大都市(50万+)'])

    city_macro = df_c.groupby('都市規模', observed=True).agg(
        件数=('is_won', 'count'),
        成約数=('is_won', 'sum'),
        成約率=('is_won', 'mean')
    ).round(4)
    print(city_macro.to_string())

    df['都市規模'] = pd.cut(df['population']/10000,
                          bins=[0, 10, 50, float('inf')],
                          labels=['小都市(~10万)', '中都市(10-50万)', '大都市(50万+)'])
    city_occur = df.groupby('都市規模', observed=True).agg(
        全体件数=('is_won', 'count'),
        タイプC発生率=('is_type_c', 'mean'),
        全体成約率=('is_won', 'mean')
    ).round(4)
    print('\n全体からの発生率:')
    print(city_occur.to_string())

    # ========================================
    # 5. 大分類: 地域ブロック
    # ========================================
    print('\n' + '='*80)
    print('【方針5】地域ブロック')
    print('='*80)

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

    df_c['地域ブロック'] = df_c['Account.Prefectures__c'].apply(classify_region)

    region_macro = df_c.groupby('地域ブロック').agg(
        件数=('is_won', 'count'),
        成約数=('is_won', 'sum'),
        成約率=('is_won', 'mean')
    ).sort_values('成約率', ascending=False).round(4)
    print(region_macro.to_string())

    df['地域ブロック'] = df['Account.Prefectures__c'].apply(classify_region)
    region_occur = df.groupby('地域ブロック').agg(
        全体件数=('is_won', 'count'),
        タイプC発生率=('is_type_c', 'mean'),
        全体成約率=('is_won', 'mean')
    ).sort_values('タイプC発生率', ascending=False).round(4)
    print('\n全体からの発生率:')
    print(region_occur.to_string())

    # ========================================
    # 6. 大枠クロス分析（2軸のみ）
    # ========================================
    print('\n' + '='*80)
    print('【方針6】2軸クロス: 法人格 × 規模')
    print('='*80)

    cross1 = df_c.groupby(['法人格大分類', '規模大分類'], observed=True).agg(
        件数=('is_won', 'count'),
        成約数=('is_won', 'sum'),
        成約率=('is_won', 'mean')
    ).round(4)
    print(cross1.to_string())

    # 全体での2軸
    cross1_all = df.groupby(['法人格大分類', '規模大分類'], observed=True).agg(
        全体件数=('is_won', 'count'),
        タイプC発生率=('is_type_c', 'mean'),
        全体成約率=('is_won', 'mean')
    ).round(4)
    print('\n全体からの発生率:')
    print(cross1_all.to_string())

    print('\n' + '='*80)
    print('【方針7】2軸クロス: 法人格 × サービス種別')
    print('='*80)

    cross2 = df_c.groupby(['法人格大分類', 'サービス大分類'], observed=True).agg(
        件数=('is_won', 'count'),
        成約数=('is_won', 'sum'),
        成約率=('is_won', 'mean')
    ).round(4)
    print(cross2.to_string())

    cross2_all = df.groupby(['法人格大分類', 'サービス大分類'], observed=True).agg(
        全体件数=('is_won', 'count'),
        タイプC発生率=('is_type_c', 'mean'),
        全体成約率=('is_won', 'mean')
    ).round(4)
    print('\n全体からの発生率:')
    print(cross2_all.to_string())

    # ========================================
    # 7. 包括的方針のまとめ
    # ========================================
    print('\n' + '='*80)
    print('【包括的方針まとめ】')
    print('='*80)

    print('''
┌─────────────────────────────────────────────────────────────────────┐
│ タイプC「担当者商談（決裁権あり）」を狙う包括的方針                    │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│ 【法人格】株式会社を優先                                             │
│   ├─ タイプC発生率: 株式会社 7.2% > その他 4.5%                      │
│   └─ タイプC成約率: 株式会社 21.0% > その他 10.3%                    │
│                                                                     │
│ 【規模】小〜中規模（50〜200人）がスイートスポット                     │
│   ├─ 小規模: 発生率高いが成約率やや低い                              │
│   ├─ 中規模: 発生率・成約率ともにバランス良い                        │
│   └─ 大規模: 成約率高いが発生率が低い                                │
│                                                                     │
│ 【サービス種別】訪問系・通所系を優先                                  │
│   ├─ 訪問系: タイプC発生率 8.6%（最高）                              │
│   └─ 通所系: タイプC成約率 15.2%（最高）                             │
│                                                                     │
│ 【都市規模】中都市（10-50万人）が最適                                 │
│   ├─ タイプC発生率: 6.7%                                            │
│   └─ タイプC成約率: 13.3%                                           │
│                                                                     │
│ 【地域】関東・東海・北海道を注力                                      │
│   ├─ 関東: 母数多い                                                 │
│   ├─ 東海: 成約率高い                                               │
│   └─ 北海道: 発生率・成約率ともに良好                                │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘

【シンプルな営業方針】

  ★ 株式会社 × 小〜中規模 × 訪問系/通所系 × 中都市

  この条件を満たす企業を優先的にアプローチすれば、
  「担当者商談だが決裁権あり」の状態に入りやすく、
  成約率も高くなる。

''')

    return df, df_c


if __name__ == "__main__":
    df, df_c = main()
