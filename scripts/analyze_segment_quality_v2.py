# -*- coding: utf-8 -*-
"""担当者・代表者・電話番号タイプ別セグメント分析（修正版）"""

import pandas as pd

def analyze_segments(df, media_name):
    """媒体ごとのセグメント分析"""
    total = len(df)

    print(f'\n{"=" * 70}')
    print(f'{media_name}（{total}件）')
    print(f'{"=" * 70}')

    # 電話番号の有無を判定
    df = df.copy()
    df['has_fixed'] = df['Phone'].notna() & (df['Phone'] != '')
    df['has_mobile'] = df['MobilePhone'].notna() & (df['MobilePhone'] != '')

    # 担当者=代表者 判定
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
            return '代表者情報なし'

    df['contact_type'] = df.apply(is_president_contact, axis=1)

    # セグメント集計（携帯電話ありで集計）
    print()
    print('【セグメント別内訳】')
    print()
    print('| セグメント     | 携帯あり | 携帯なし | 合計 | 携帯率 |')
    print('|----------------|----------|----------|------|--------|')

    segments = ['代表者直通', '担当者経由', '代表者情報なし', 'バイネームなし']
    total_mobile = 0
    total_no_mobile = 0

    results = {}
    for seg in segments:
        seg_df = df[df['contact_type'] == seg]
        with_mobile = len(seg_df[seg_df['has_mobile']])
        without_mobile = len(seg_df[~seg_df['has_mobile']])
        seg_total = with_mobile + without_mobile
        mobile_rate = with_mobile / seg_total * 100 if seg_total > 0 else 0

        total_mobile += with_mobile
        total_no_mobile += without_mobile

        results[seg] = {'mobile': with_mobile, 'no_mobile': without_mobile, 'total': seg_total}

        print(f'| {seg:<14} | {with_mobile:>8} | {without_mobile:>8} | {seg_total:>4} | {mobile_rate:>5.1f}% |')

    print(f'|----------------|----------|----------|------|--------|')
    total_rate = total_mobile / total * 100 if total > 0 else 0
    print(f'| 合計           | {total_mobile:>8} | {total_no_mobile:>8} | {total:>4} | {total_rate:>5.1f}% |')
    print()

    # 最強セグメント（代表者直通 + 携帯あり）
    best_df = df[(df['contact_type'] == '代表者直通') & (df['has_mobile'])]
    print(f'★ 最強セグメント（代表者直通 × 携帯あり）: {len(best_df)}件 ({len(best_df)/total*100:.1f}%)')

    # 固定電話の内訳も表示
    print()
    print('【電話番号タイプ詳細】')
    fixed_only = len(df[df['has_fixed'] & ~df['has_mobile']])
    mobile_only = len(df[~df['has_fixed'] & df['has_mobile']])
    both = len(df[df['has_fixed'] & df['has_mobile']])
    print(f'  固定のみ: {fixed_only}件')
    print(f'  携帯のみ: {mobile_only}件')
    print(f'  両方あり: {both}件')
    print(f'  → 携帯あり計: {mobile_only + both}件')

    # サンプル表示
    if len(best_df) > 0:
        print()
        print('  最強セグメント サンプル（先頭5件）:')
        for idx, row in best_df.head(5).iterrows():
            company = str(row.get('Company', ''))[:20]
            name = str(row.get('LastName', ''))
            mobile = str(row.get('MobilePhone', ''))
            # 電話番号のフォーマット
            if mobile and len(mobile) >= 10:
                mobile = mobile.replace('.0', '')
                if len(mobile) == 11:
                    mobile = f"{mobile[:3]}-{mobile[3:7]}-{mobile[7:]}"
            print(f'    - {company}: {name} ({mobile})')

    return {
        'media': media_name,
        'total': total,
        'best_count': len(best_df),
        'best_rate': len(best_df)/total*100 if total > 0 else 0,
        'mobile_count': total_mobile,
        'mobile_rate': total_rate
    }


def main():
    print('\n' + '=' * 70)
    print('全媒体 担当者・代表者・電話番号タイプ別セグメント分析')
    print('=' * 70)

    # ミイダス
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
    print('【サマリー】')
    print('=' * 70)
    print()
    print('| 媒体             | 総件数 | 携帯あり | 携帯率 | 最強セグメント | 最強率 |')
    print('|------------------|--------|----------|--------|----------------|--------|')
    for stats in [miidas_stats, ptot_stats, jp_stats]:
        print(f'| {stats["media"]:<16} | {stats["total"]:>6} | {stats["mobile_count"]:>8} | {stats["mobile_rate"]:>5.1f}% | {stats["best_count"]:>14} | {stats["best_rate"]:>5.1f}% |')

    total_all = miidas_stats['total'] + ptot_stats['total'] + jp_stats['total']
    total_mobile = miidas_stats['mobile_count'] + ptot_stats['mobile_count'] + jp_stats['mobile_count']
    total_best = miidas_stats['best_count'] + ptot_stats['best_count'] + jp_stats['best_count']
    print(f'|------------------|--------|----------|--------|----------------|--------|')
    print(f'| 合計             | {total_all:>6} | {total_mobile:>8} | {total_mobile/total_all*100:>5.1f}% | {total_best:>14} | {total_best/total_all*100:>5.1f}% |')


if __name__ == "__main__":
    main()
