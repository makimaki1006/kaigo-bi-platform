# -*- coding: utf-8 -*-
"""担当者・代表者・電話番号タイプ別セグメント分析"""

import pandas as pd

def analyze_segments(df, media_name, has_president_field=True):
    """媒体ごとのセグメント分析"""
    total = len(df)

    print(f'\n{"=" * 70}')
    print(f'{media_name}（{total}件）')
    print(f'{"=" * 70}')

    # 電話番号タイプ判定
    def get_phone_type(row):
        has_fixed = pd.notna(row.get('Phone')) and row.get('Phone') != ''
        has_mobile = pd.notna(row.get('MobilePhone')) and row.get('MobilePhone') != ''
        if has_fixed and has_mobile:
            return '両方'
        elif has_mobile:
            return '携帯'
        elif has_fixed:
            return '固定'
        return 'なし'

    df = df.copy()
    df['phone_type'] = df.apply(get_phone_type, axis=1)

    # 担当者=代表者 判定
    # PresidentName__c がある場合は比較、ない場合はLastNameが代表者的かどうかで判断
    def is_president_contact(row):
        last_name = str(row.get('LastName', '')).strip()
        president = str(row.get('PresidentName__c', '')).strip() if 'PresidentName__c' in row.index else ''

        if last_name == '担当者' or last_name == '':
            return 'バイネームなし'

        if president and president != 'nan' and president != '':
            # 代表者名と担当者名を比較（部分一致も考慮）
            if last_name in president or president in last_name:
                return '代表者直通'
            else:
                return '担当者経由'
        else:
            # 代表者情報がない場合は「不明」
            return '代表者情報なし'

    df['contact_type'] = df.apply(is_president_contact, axis=1)

    # セグメント集計
    print()
    print('【セグメント別内訳】')
    print()
    print('| セグメント | 固定電話 | 携帯電話 | 両方 | 合計 |')
    print('|------------|----------|----------|------|------|')

    segments = ['代表者直通', '担当者経由', '代表者情報なし', 'バイネームなし']
    segment_totals = {'固定': 0, '携帯': 0, '両方': 0}

    best_segment_count = 0  # 代表者直通 + 携帯

    for seg in segments:
        seg_df = df[df['contact_type'] == seg]
        fixed = len(seg_df[seg_df['phone_type'] == '固定'])
        mobile = len(seg_df[seg_df['phone_type'] == '携帯'])
        both = len(seg_df[seg_df['phone_type'] == '両方'])
        seg_total = fixed + mobile + both

        segment_totals['固定'] += fixed
        segment_totals['携帯'] += mobile
        segment_totals['両方'] += both

        if seg == '代表者直通':
            best_segment_count = mobile + both  # 携帯または両方

        print(f'| {seg:<10} | {fixed:>8} | {mobile:>8} | {both:>4} | {seg_total:>4} |')

    print(f'|------------|----------|----------|------|------|')
    print(f'| 合計       | {segment_totals["固定"]:>8} | {segment_totals["携帯"]:>8} | {segment_totals["両方"]:>4} | {total:>4} |')
    print()

    # 最強セグメント（代表者直通 + 携帯）
    best_df = df[(df['contact_type'] == '代表者直通') &
                 ((df['phone_type'] == '携帯') | (df['phone_type'] == '両方'))]
    print(f'★ 最強セグメント（代表者直通 × 携帯）: {len(best_df)}件 ({len(best_df)/total*100:.1f}%)')

    # サンプル表示
    if len(best_df) > 0:
        print()
        print('  サンプル（先頭5件）:')
        for idx, row in best_df.head(5).iterrows():
            company = str(row.get('Company', ''))[:20]
            name = str(row.get('LastName', ''))
            mobile = str(row.get('MobilePhone', ''))[:15] if pd.notna(row.get('MobilePhone')) else ''
            print(f'    - {company}: {name} ({mobile})')

    return {
        'media': media_name,
        'total': total,
        'best_count': len(best_df),
        'best_rate': len(best_df)/total*100 if total > 0 else 0
    }


def main():
    # ミイダス
    print('\n' + '=' * 70)
    print('全媒体 担当者・代表者・電話番号タイプ別セグメント分析')
    print('=' * 70)

    df_miidas = pd.read_csv('data/output/media_matching/miidas_new_leads_20260108_101935.csv', encoding='utf-8-sig')
    miidas_stats = analyze_segments(df_miidas, 'ミイダス')

    # PT・OT・STネット、ジョブポスター
    df_others = pd.read_csv('data/output/media_matching/new_leads_final_20260108_011052.csv', encoding='utf-8-sig')

    df_ptot = df_others[df_others['Paid_Media__c'] == 'PT・OT・STネット'].copy()
    ptot_stats = analyze_segments(df_ptot, 'PT・OT・STネット')

    df_jp = df_others[df_others['Paid_Media__c'] == 'ジョブポスター'].copy()
    jp_stats = analyze_segments(df_jp, 'ジョブポスター')

    # サマリー
    print('\n' + '=' * 70)
    print('【最強セグメント（代表者直通 × 携帯）サマリー】')
    print('=' * 70)
    print()
    print('| 媒体 | 該当件数 | 割合 |')
    print('|------|----------|------|')
    for stats in [miidas_stats, ptot_stats, jp_stats]:
        print(f'| {stats["media"]:<14} | {stats["best_count"]:>8}件 | {stats["best_rate"]:>5.1f}% |')

    total_best = miidas_stats['best_count'] + ptot_stats['best_count'] + jp_stats['best_count']
    total_all = miidas_stats['total'] + ptot_stats['total'] + jp_stats['total']
    print(f'|----------------|----------|------|')
    print(f'| 合計           | {total_best:>8}件 | {total_best/total_all*100:>5.1f}% |')


if __name__ == "__main__":
    main()
