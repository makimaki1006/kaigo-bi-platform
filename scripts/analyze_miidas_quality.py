# -*- coding: utf-8 -*-
"""ミイダスデータ品質分析スクリプト"""

import pandas as pd
import re

def analyze_quality():
    df = pd.read_csv('data/output/media_matching/miidas_new_leads_20260108_093737.csv', encoding='utf-8-sig')

    print('=' * 60)
    print('ミイダス 新規リード データ品質レポート')
    print('=' * 60)
    print(f'総件数: {len(df)}件')
    print()

    # 担当者名の分析
    print('【担当者名（LastName）】')
    has_name = df[(df['LastName'].notna()) & (df['LastName'] != '') & (df['LastName'] != '担当者')]
    generic_name = df[df['LastName'] == '担当者']
    print(f'  バイネーム（個人名）: {len(has_name)}件')
    print(f'  一般名称（担当者）: {len(generic_name)}件')
    print()

    # 役職の分析
    print('【役職（Title）】')
    has_title = df[(df['Title'].notna()) & (df['Title'] != '')]
    print(f'  役職あり: {len(has_title)}件')
    print(f'  役職なし: {len(df) - len(has_title)}件')
    print()

    # 電話番号の分析
    print('【電話番号タイプ別】')
    has_phone = df[(df['Phone'].notna()) & (df['Phone'] != '')]
    has_mobile = df[(df['MobilePhone'].notna()) & (df['MobilePhone'] != '')]
    has_both = df[(df['Phone'].notna()) & (df['Phone'] != '') & (df['MobilePhone'].notna()) & (df['MobilePhone'] != '')]
    fixed_only = len(has_phone) - len(has_both)
    mobile_only = len(has_mobile) - len(has_both)
    print(f'  固定電話のみ: {fixed_only}件')
    print(f'  携帯電話のみ: {mobile_only}件')
    print(f'  両方あり: {len(has_both)}件')
    print()

    # クロス集計: 担当者名 × 電話番号タイプ
    print('【クロス集計: 担当者名 × 電話番号タイプ】')

    # バイネームの場合
    byname_fixed = has_name[(has_name['Phone'].notna()) & (has_name['Phone'] != '') &
                            ((has_name['MobilePhone'].isna()) | (has_name['MobilePhone'] == ''))]
    byname_mobile = has_name[(has_name['MobilePhone'].notna()) & (has_name['MobilePhone'] != '') &
                             ((has_name['Phone'].isna()) | (has_name['Phone'] == ''))]
    byname_both = has_name[(has_name['Phone'].notna()) & (has_name['Phone'] != '') &
                           (has_name['MobilePhone'].notna()) & (has_name['MobilePhone'] != '')]

    print(f'  バイネーム + 固定電話: {len(byname_fixed)}件')
    print(f'  バイネーム + 携帯電話: {len(byname_mobile)}件')
    print(f'  バイネーム + 両方: {len(byname_both)}件')
    print(f'  一般名称 + 固定電話: {fixed_only - len(byname_fixed)}件')
    print(f'  一般名称 + 携帯電話: {mobile_only - len(byname_mobile)}件')
    print()

    # 募集人数の抽出可能性を確認
    print('【募集人数の抽出可能性】')
    # Paid_JobTitle__cやPaid_Memo__cから募集人数を探す
    num_patterns = [
        r'(\d+)名募集',
        r'(\d+)名以上',
        r'(\d+)名程度',
        r'募集人数[：:]\s*(\d+)',
        r'(\d+)人募集',
    ]

    count_found = 0
    examples = []
    for idx, row in df.iterrows():
        job_title = str(row.get('Paid_JobTitle__c', ''))
        memo = str(row.get('Paid_Memo__c', ''))
        industry = str(row.get('Paid_Industry__c', ''))

        for pattern in num_patterns:
            match = re.search(pattern, job_title + memo)
            if match:
                count_found += 1
                if len(examples) < 5:
                    examples.append((row['Company'], match.group(0)))
                break

    print(f'  募集人数記載あり: {count_found}件')
    if examples:
        print('  例:')
        for company, text in examples:
            print(f'    - {company}: {text}')
    print()

    # 企業規模（従業員数）の分析
    print('【企業規模（従業員数）】')
    industry_counts = df['Paid_Industry__c'].value_counts()
    print('  内訳:')
    for val, count in industry_counts.head(10).items():
        if pd.notna(val) and val != '':
            # 改行を除去して表示
            val_clean = str(val).split('\n')[0][:30]
            print(f'    {val_clean}: {count}件')

if __name__ == "__main__":
    analyze_quality()
