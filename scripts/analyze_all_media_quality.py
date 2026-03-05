# -*- coding: utf-8 -*-
"""全媒体データ品質分析スクリプト"""

import pandas as pd

def analyze_media(df, media_name):
    """媒体ごとのデータ品質を分析"""
    total = len(df)

    # 担当者名
    has_byname = len(df[(df['LastName'].notna()) & (df['LastName'] != '') & (df['LastName'] != '担当者')])
    generic_name = len(df[df['LastName'] == '担当者'])

    # 役職
    if 'Title' in df.columns:
        has_title = len(df[(df['Title'].notna()) & (df['Title'] != '')])
    else:
        has_title = 0

    # 募集人数
    if 'Paid_NumberOfRecruitment__c' in df.columns:
        has_num = len(df[(df['Paid_NumberOfRecruitment__c'].notna()) & (df['Paid_NumberOfRecruitment__c'] != '')])
    else:
        has_num = 0

    # 電話番号
    has_phone = len(df[(df['Phone'].notna()) & (df['Phone'] != '')])
    has_mobile = len(df[(df['MobilePhone'].notna()) & (df['MobilePhone'] != '')])
    has_both = len(df[(df['Phone'].notna()) & (df['Phone'] != '') &
                      (df['MobilePhone'].notna()) & (df['MobilePhone'] != '')])

    fixed_only = has_phone - has_both
    mobile_only = has_mobile - has_both

    print(f'\n{"=" * 60}')
    print(f'{media_name}')
    print(f'{"=" * 60}')
    print(f'新規リード件数: {total}件')
    print()
    print(f'| 項目       | あり    | なし    | 補完率  |')
    print(f'|------------|---------|---------|---------|')
    print(f'| バイネーム | {has_byname}件    | {generic_name}件    | {has_byname/total*100:.1f}%   |')
    print(f'| 役職       | {has_title}件    | {total - has_title}件   | {has_title/total*100:.1f}%   |')
    print(f'| 募集人数   | {has_num}件     | {total - has_num}件   | {has_num/total*100:.1f}%    |')
    print(f'| 固定電話   | {fixed_only}件   | -       | {fixed_only/total*100:.1f}%   |')
    print(f'| 携帯電話   | {mobile_only}件    | -       | {mobile_only/total*100:.1f}%   |')
    print(f'| 両方       | {has_both}件     | -       | {has_both/total*100:.1f}%    |')

    return {
        'media': media_name,
        'total': total,
        'byname': has_byname,
        'generic': generic_name,
        'title': has_title,
        'num_recruitment': has_num,
        'fixed_only': fixed_only,
        'mobile_only': mobile_only,
        'both': has_both
    }

def main():
    # ミイダス
    df_miidas = pd.read_csv('data/output/media_matching/miidas_new_leads_20260108_100403.csv', encoding='utf-8-sig')
    miidas_stats = analyze_media(df_miidas, 'ミイダス')

    # PT・OT・STネット、ジョブポスター
    df_others = pd.read_csv('data/output/media_matching/new_leads_final_20260108_011052.csv', encoding='utf-8-sig')

    df_ptot = df_others[df_others['Paid_Media__c'] == 'PT・OT・STネット']
    ptot_stats = analyze_media(df_ptot, 'PT・OT・STネット')

    df_jp = df_others[df_others['Paid_Media__c'] == 'ジョブポスター']
    jp_stats = analyze_media(df_jp, 'ジョブポスター')

    # 合計
    print(f'\n{"=" * 60}')
    print('【合計】')
    print(f'{"=" * 60}')
    total_all = miidas_stats['total'] + ptot_stats['total'] + jp_stats['total']
    byname_all = miidas_stats['byname'] + ptot_stats['byname'] + jp_stats['byname']
    generic_all = miidas_stats['generic'] + ptot_stats['generic'] + jp_stats['generic']
    title_all = miidas_stats['title'] + ptot_stats['title'] + jp_stats['title']
    num_all = miidas_stats['num_recruitment'] + ptot_stats['num_recruitment'] + jp_stats['num_recruitment']
    fixed_all = miidas_stats['fixed_only'] + ptot_stats['fixed_only'] + jp_stats['fixed_only']
    mobile_all = miidas_stats['mobile_only'] + ptot_stats['mobile_only'] + jp_stats['mobile_only']
    both_all = miidas_stats['both'] + ptot_stats['both'] + jp_stats['both']

    print(f'新規リード合計: {total_all}件')
    print()
    print(f'| 項目       | あり    | なし    | 補完率  |')
    print(f'|------------|---------|---------|---------|')
    print(f'| バイネーム | {byname_all}件   | {generic_all}件    | {byname_all/total_all*100:.1f}%   |')
    print(f'| 役職       | {title_all}件    | {total_all - title_all}件   | {title_all/total_all*100:.1f}%   |')
    print(f'| 募集人数   | {num_all}件     | {total_all - num_all}件   | {num_all/total_all*100:.1f}%    |')
    print(f'| 固定電話   | {fixed_all}件   | -       | {fixed_all/total_all*100:.1f}%   |')
    print(f'| 携帯電話   | {mobile_all}件   | -       | {mobile_all/total_all*100:.1f}%   |')
    print(f'| 両方       | {both_all}件     | -       | {both_all/total_all*100:.1f}%    |')

if __name__ == "__main__":
    main()
