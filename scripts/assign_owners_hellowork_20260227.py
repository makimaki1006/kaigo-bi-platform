"""ハローワーク新規リード所有者割り当て（2026-02-27）"""
import sys
import io
import pandas as pd

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# === 設定 ===
INPUT_PATH = 'data/output/hellowork/new_leads_import_20260227.csv'
OUTPUT_PATH = 'data/output/hellowork/new_leads_import_20260227_assigned.csv'

OWNERS = {
    '小林': '005J3000000ERz4IAG',
    '藤巻': '0055i00000BeOKbAAN',
    '服部': '005J3000000EYYjIAO',
    '深堀': '0055i00000CwKEhAAN',
}

KOBAYASHI_COUNT = 90


def is_president_direct(row):
    """代表直通判定"""
    last_name = str(row.get('LastName', ''))
    president = str(row.get('PresidentName__c', ''))
    title = str(row.get('Title', ''))
    if last_name == '担当者':
        name_match = False
    else:
        name_match = last_name == president
    title_match = '代表' in title
    return name_match or title_match


def main():
    df = pd.read_csv(INPUT_PATH, encoding='utf-8-sig', dtype=str)
    print(f'読み込み: {len(df)}件')

    # セグメント判定
    df['_is_pres'] = df.apply(is_president_direct, axis=1)
    mobile = df['MobilePhone'].fillna('')
    df['_has_mobile'] = mobile.str.len() > 0

    seg_pres_mobile = df['_is_pres'] & df['_has_mobile']       # 代表直通（携帯）
    seg_pres_landline = df['_is_pres'] & ~df['_has_mobile']    # 代表（固定電話）
    seg_non_pres = ~df['_is_pres']                              # 非代表

    print(f'\n--- 3セグメント ---')
    print(f'  代表直通（携帯）: {seg_pres_mobile.sum()}件')
    print(f'  代表（固定電話）: {seg_pres_landline.sum()}件')
    print(f'  非代表:           {seg_non_pres.sum()}件')

    # === 割り当て ===

    # 1. 代表直通（携帯）36件 → 服部・深堀に半分ずつ
    pres_mobile_df = df[seg_pres_mobile].sample(frac=1, random_state=42).reset_index(drop=True)
    half = len(pres_mobile_df) // 2
    pm_hattori = pres_mobile_df.iloc[:half].copy()
    pm_fukabori = pres_mobile_df.iloc[half:].copy()
    pm_hattori['OwnerId'] = OWNERS['服部']
    pm_fukabori['OwnerId'] = OWNERS['深堀']

    print(f'\n--- 代表携帯 割り当て ---')
    print(f'  服部: {len(pm_hattori)}件')
    print(f'  深堀: {len(pm_fukabori)}件')

    # 2. 残り376件（代表固定 + 非代表）からシャッフル
    rest_df = df[seg_pres_landline | seg_non_pres].sample(frac=1, random_state=42).reset_index(drop=True)
    print(f'\n残り（代表固定+非代表）: {len(rest_df)}件')

    # 3. 小林: 90件
    kobayashi_df = rest_df.iloc[:KOBAYASHI_COUNT].copy()
    kobayashi_df['OwnerId'] = OWNERS['小林']

    # 4. 残り286件を藤巻・服部・深堀に配分（合計を均等にする）
    remaining = rest_df.iloc[KOBAYASHI_COUNT:].reset_index(drop=True)

    # 3名の合計目標を均等化（代表携帯含む）
    total_three = len(pres_mobile_df) + len(remaining)  # 322件
    target = total_three // 3       # 107
    extra = total_three % 3         # 1

    # 目標件数: 108, 107, 107（端数は藤巻に）
    targets = [target, target, target]
    for i in range(extra):
        targets[i] += 1

    # 代表携帯分を引いた残り配分
    fujimaki_count = targets[0]                         # 藤巻は代表携帯0なので全てここから
    hattori_count = targets[1] - len(pm_hattori)        # 服部は代表携帯分を差し引き
    fukabori_count = targets[2] - len(pm_fukabori)      # 深堀は代表携帯分を差し引き

    fujimaki_df = remaining.iloc[:fujimaki_count].copy()
    hattori_rest = remaining.iloc[fujimaki_count:fujimaki_count+hattori_count].copy()
    fukabori_rest = remaining.iloc[fujimaki_count+hattori_count:].copy()

    fujimaki_df['OwnerId'] = OWNERS['藤巻']
    hattori_rest['OwnerId'] = OWNERS['服部']
    fukabori_rest['OwnerId'] = OWNERS['深堀']

    print(f'\n--- 残り配分（合計均等化） ---')
    print(f'  藤巻: {len(fujimaki_df)}件（残りから）')
    print(f'  服部: {len(hattori_rest)}件（残りから）+ {len(pm_hattori)}件（代表携帯）= {len(hattori_rest)+len(pm_hattori)}件')
    print(f'  深堀: {len(fukabori_rest)}件（残りから）+ {len(pm_fukabori)}件（代表携帯）= {len(fukabori_rest)+len(pm_fukabori)}件')

    # 結合
    result = pd.concat([
        kobayashi_df, fujimaki_df,
        pm_hattori, hattori_rest,
        pm_fukabori, fukabori_rest,
    ], ignore_index=True)

    # 作業列削除
    result.drop(columns=['_is_pres', '_has_mobile'], inplace=True)

    result.to_csv(OUTPUT_PATH, index=False, encoding='utf-8-sig')

    # === サマリー ===
    owner_map = {v: k for k, v in OWNERS.items()}

    print(f'\n{"="*50}')
    print(f'最終割り当て結果')
    print(f'{"="*50}')
    for oid, cnt in result['OwnerId'].value_counts().sort_values(ascending=False).items():
        print(f'  {owner_map.get(oid, oid)}: {cnt}件')
    print(f'  合計: {len(result)}件')

    # 各所有者のセグメント内訳
    result['_is_pres'] = result.apply(is_president_direct, axis=1)
    mob = result['MobilePhone'].fillna('')
    result['_has_mob'] = mob.str.len() > 0

    print(f'\n--- 所有者別セグメント内訳 ---')
    for owner_name, oid in OWNERS.items():
        subset = result[result['OwnerId'] == oid]
        pm = ((subset['_is_pres']) & (subset['_has_mob'])).sum()
        pl = ((subset['_is_pres']) & (~subset['_has_mob'])).sum()
        np_ = (~subset['_is_pres']).sum()
        print(f'  {owner_name} ({len(subset)}件): 代表携帯={pm} / 代表固定={pl} / 非代表={np_}')

    print(f'\n出力: {OUTPUT_PATH}')


if __name__ == '__main__':
    main()
