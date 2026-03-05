"""ハローワーク セグメント別 Salesforceインポート前処理

- 従業員数を数値化
- 市区町村人口を付与
- 電話番号正規化
- 決裁者近接スコア（★1〜5）
- Salesforceフィールドマッピング用カラム整備
"""
import pandas as pd
import json
import re
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

INPUT_DIR = Path(r'C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List\data\output\hellowork_segments')
OUTPUT_DIR = Path(r'C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List\data\output\hellowork_segments\import_ready')
POP_JSON = r'C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List\data\population\city_population_map.json'
ORIGINAL_CSV = r'C:\Users\fuji1\OneDrive\デスクトップ\RCMEB002002_M100.csv'

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# --- セグメント定義（業界分類用） ---
SEGMENTS = {
    'C_工事': {
        'industry_codes': ['06', '07', '08'],
        'industry_keywords': ['総合工事', '職別工事', '設備工事', '建設'],
        'industry_names_whitelist': [],
        'job_codes': ['007-03', '048-10', '080-01', '080-04', '089-04', '089-05'],
        'job_major_prefixes': ['008', '090', '091', '092', '093', '094'],
        'sub_industries': {
            '総合工事': '総合工事業',
            '土木工事': '土木工事業',
            '舗装': '舗装工事業',
            '建築工事': '建築工事業',
            '木造建築': '木造建築工事業',
            '建築リフォーム': 'リフォーム工事業',
            '電気工事': '電気工事業',
            '電気通信': '電気通信工事業',
            '管工事': '管工事・設備工事業',
            '機械器具設置': '管工事・設備工事業',
            '塗装': '塗装工事業',
            '防水': '防水工事業',
            'とび': 'とび・土工業',
            '鉄骨': '鉄骨・鉄筋工事業',
            '鉄筋': '鉄骨・鉄筋工事業',
            '解体': '解体工事業',
        },
    },
    'D_ホテル旅館': {
        'industry_codes': ['75'],
        'industry_keywords': ['宿泊', 'ホテル', '旅館'],
        'industry_names_whitelist': [],
        'job_codes': ['056-02', '056-04', '056-05', '096-03'],
        'job_major_prefixes': [],
        'sub_industries': {
            '旅館': 'ホテル・旅館',
            'ホテル': 'ホテル・旅館',
            '簡易宿所': '簡易宿所・民泊',
            '下宿': '簡易宿所・民泊',
            '宿泊': 'その他宿泊業',
        },
    },
    'E_葬儀': {
        'industry_codes': [],
        'industry_keywords': ['葬儀', '葬祭'],
        'industry_names_whitelist': [],
        'job_codes': ['058-06'],
        'job_major_prefixes': [],
        'sub_industries': {
            '冠婚葬祭': '冠婚葬祭業',
            '火葬': '火葬・墓地管理',
            '墓地': '火葬・墓地管理',
        },
    },
    'F_産業廃棄物': {
        'industry_codes': ['88'],
        'industry_keywords': ['廃棄物'],
        'industry_names_whitelist': [],
        'job_codes': ['096-05', '096-06'],
        'job_major_prefixes': [],
        'sub_industries': {
            '産業廃棄物': '産業廃棄物処理業',
            '一般廃棄物': '一般廃棄物処理業',
            '再生資源': 'リサイクル業',
        },
    },
}


def classify_industry(row, config):
    """産業分類名称からサブカテゴリを判定"""
    ind_name = str(row.get('産業分類（名称）', '')) if pd.notna(row.get('産業分類（名称）')) else ''
    ind_code_2 = str(row.get('産業分類（コード）', ''))[:2] if pd.notna(row.get('産業分類（コード）')) else ''
    seg = str(row.get('セグメント', ''))

    sub_industries = config.get('sub_industries', {})

    # サブカテゴリのキーワードマッチ
    for kw, label in sub_industries.items():
        if kw in ind_name:
            return label

    # 産業コードがセグメントの対象コードに一致 → セグメント業界
    industry_codes = config.get('industry_codes', [])
    if ind_code_2 in industry_codes:
        return seg.split('_', 1)[1] + '（その他）'

    # パターン①（職種）で引っかかった他業種
    return '他業種（職種マッチ）'


def match_route(row, config):
    """パターン①/②のどちらで抽出されたかを判定"""
    ind_code_2 = str(row.get('産業分類（コード）', ''))[:2] if pd.notna(row.get('産業分類（コード）')) else ''
    ind_name = str(row.get('産業分類（名称）', '')) if pd.notna(row.get('産業分類（名称）')) else ''

    industry_codes = config.get('industry_codes', [])
    industry_keywords = config.get('industry_keywords', [])
    industry_whitelist = config.get('industry_names_whitelist', [])

    # パターン②チェック
    if ind_name in industry_whitelist:
        return 'パターン②（産業分類）'
    if ind_code_2 in industry_codes:
        return 'パターン②（産業分類）'
    for kw in industry_keywords:
        if kw in ind_name:
            return 'パターン②（産業分類）'

    return 'パターン①（職種分類）'


# --- ユーティリティ ---
CITY_PATTERN = re.compile(r'^(北海道|東京都|(?:大阪|京都)府|.{2,3}県)(.+?[市区町村郡](?:.+?[町村])?)')
PREF_PATTERN = re.compile(r'^(北海道|東京都|(?:大阪|京都)府|.{2,3}県)')


def load_population():
    with open(POP_JSON, 'r', encoding='utf-8') as f:
        return json.load(f)


def extract_pref_city(addr, pop_map):
    if pd.isna(addr):
        return None, None, None
    addr = str(addr).strip()
    # 都道府県
    pm = PREF_PATTERN.match(addr)
    pref = pm.group(1) if pm else None
    # 市区町村
    m = CITY_PATTERN.match(addr)
    if m:
        p, city = m.group(1), m.group(2)
        city_base = re.match(r'(.+市)', city)
        if city_base:
            key = f'{p} {city_base.group(1)}'
            if key in pop_map:
                return pref, city_base.group(1), pop_map[key]
        key = f'{p} {city}'
        if key in pop_map:
            return pref, city, pop_map[key]
    return pref, None, None


def normalize_phone(tel):
    """電話番号を正規化（ハイフン除去、10-11桁）"""
    if pd.isna(tel) or str(tel).strip() == '':
        return ''
    s = str(tel).replace('-', '').replace('－', '').replace('ー', '').replace('（', '').replace('）', '').replace(' ', '').replace('\u3000', '').strip()
    s = re.sub(r'[^0-9]', '', s)
    if len(s) >= 10 and len(s) <= 11 and s.startswith('0'):
        return s
    return ''


def is_mobile(normalized_phone):
    return bool(re.match(r'^0[789]0\d{8}$', normalized_phone))


def parse_emp(val):
    if pd.isna(val) or str(val).strip() == '':
        return None
    s = str(val).replace('人', '').replace(',', '').replace('，', '').strip()
    try:
        return int(s)
    except ValueError:
        return None


def normalize_name(name):
    if pd.isna(name) or str(name).strip() == '':
        return ''
    return str(name).strip().replace('\u3000', '').replace(' ', '')


def extract_prefecture(addr):
    if pd.isna(addr):
        return ''
    m = PREF_PATTERN.match(str(addr).strip())
    return m.group(1) if m else ''


def clean_corporate_number(val):
    """法人番号: .0除去、13桁に"""
    if pd.isna(val) or str(val).strip() == '':
        return ''
    s = str(val).strip()
    if s.endswith('.0'):
        s = s[:-2]
    s = re.sub(r'[^0-9]', '', s)
    return s if len(s) == 13 else ''


def extract_surname(name):
    """名前から苗字を抽出（全角/半角スペース区切り、またはカタカナ/漢字の境界）"""
    if not name:
        return ''
    # スペース区切りがある場合
    parts = re.split(r'[\s\u3000]+', name)
    if len(parts) >= 2:
        return parts[0]
    # 2文字以上の場合、先頭2文字を苗字とみなす（最低限の推定）
    return name[:2] if len(name) >= 2 else name


def is_family_business_name(company_name, rep_name):
    """社名に代表者の苗字が含まれるか（家族経営の指標）"""
    if not company_name or not rep_name:
        return False
    surname = extract_surname(rep_name)
    if not surname or len(surname) < 2:
        return False
    # 法人格を除去して比較
    clean_company = re.sub(r'(株式会社|有限会社|合同会社|合資会社|合名会社|一般社団法人|一般財団法人|医療法人|社会福祉法人|（株）|（有）|\(株\)|\(有\))', '', str(company_name))
    return surname in clean_company


# 決裁者役職キーワード
DECISION_MAKER_TITLES = ['代表取締役', '社長', '代表', '取締役', '役員', '理事長', '理事', '会長', '所長', '院長', '園長', '施設長', '支配人', 'オーナー', '経営者']
MANAGER_WALL_KEYWORDS = ['総務', '人事', '管理部', '管理課', '事務局', '労務']


def detect_decision_maker_title(title_str):
    """担当者の役職が決裁者レベルかどうか"""
    if not title_str:
        return False
    for kw in DECISION_MAKER_TITLES:
        if kw in title_str:
            return True
    return False


def detect_manager_wall(dept_title_str):
    """管理部門（壁）の検出"""
    if not dept_title_str:
        return False
    for kw in MANAGER_WALL_KEYWORDS:
        if kw in dept_title_str:
            return True
    return False


def calc_proximity_score(row):
    """決裁者近接スコア（★1〜5）

    ★5: 代表者直通 + 携帯（最強、代表が直接出る + すぐ繋がる）
    ★4: 代表者直通 OR 同姓親族(小規模) OR 決裁者役職+携帯
    ★3: 同姓親族 OR 決裁者役職（固定電話）
    ★2: 小規模企業（近接シグナルあり）
    ★1: 管理部門が壁（決裁者到達困難）
    """
    is_direct = row.get('代表者直通') == '○'
    is_mobile = row.get('電話番号_携帯判定') == '携帯'
    is_family = row.get('同姓親族') == '○'
    is_dm_title = row.get('決裁者役職') == '○'
    is_wall = row.get('管理部門壁') == '○'
    emp = row.get('従業員数_数値')
    is_small = emp is not None and not pd.isna(emp) and emp <= 10
    is_family_biz = row.get('家族経営名') == '○'
    has_hp = row.get('HP有無') == '有'

    # ★5: 代表直通 + 携帯
    if is_direct and is_mobile:
        return 5

    # ★4: 代表直通 / 同姓親族+小規模 / 決裁者役職+携帯
    if is_direct:
        return 4
    if is_family and is_small:
        return 4
    if is_dm_title and is_mobile:
        return 4

    # ★3: 同姓親族 / 決裁者役職 / 家族経営名+小規模
    if is_family:
        return 3
    if is_dm_title:
        return 3
    if is_family_biz and is_small:
        return 3

    # ★1: 管理部門が壁（★2より先に判定）
    if is_wall:
        return 1

    # ★2: その他（小規模でシグナルなし含む）
    return 2


def main():
    pop_map = load_population()

    print("=" * 70)
    print("ハローワーク セグメント Salesforceインポート前処理")
    print("=" * 70)

    # 元CSVから追加カラムを取得
    print("\n元CSV読み込み中...")
    extra_cols = [
        '求人番号',
        '事業内容', '創業設立年（西暦）', '資本金', '法人番号',
    ]
    df_extra = pd.read_csv(ORIGINAL_CSV, encoding='cp932', dtype=str, usecols=extra_cols, low_memory=False)
    df_extra = df_extra.set_index('求人番号')
    print(f"  元CSV追加カラム読み込み完了")

    seg_names = ['C_工事', 'D_ホテル旅館', 'E_葬儀', 'F_産業廃棄物']

    all_results = []

    for seg_name in seg_names:
        config = SEGMENTS[seg_name]
        print(f"\n--- {seg_name} ---")
        df = pd.read_csv(INPUT_DIR / f'final_{seg_name}.csv', encoding='utf-8-sig', dtype=str)
        print(f"  入力: {len(df):,}件")

        # 元CSVから追加カラムをマージ
        df = df.set_index('求人番号').join(df_extra, rsuffix='_orig').reset_index()

        # --- 業界カラム（フィルタ用） ---
        df['業界'] = df.apply(lambda r: classify_industry(r, config), axis=1)

        # --- マッチ経路（パターン①/②判定） ---
        df['マッチ経路'] = df.apply(lambda r: match_route(r, config), axis=1)

        # --- 従業員数（数値） ---
        df['従業員数_数値'] = df['従業員数企業全体'].apply(parse_emp)
        df['従業員数就業場所_数値'] = df['従業員数就業場所'].apply(parse_emp)

        # --- 市区町村・人口 ---
        city_data = df['事業所所在地'].apply(lambda a: extract_pref_city(a, pop_map))
        df['都道府県'] = city_data.apply(lambda x: x[0] if x[0] else '')
        df['市区町村名'] = city_data.apply(lambda x: x[1] if x[1] else '')
        df['市区町村人口_数値'] = city_data.apply(lambda x: x[2] if x[2] else '')

        # --- 電話番号正規化 ---
        df['電話番号_正規化'] = df['選考担当者ＴＥＬ'].apply(normalize_phone)
        df['電話番号_携帯判定'] = df['電話番号_正規化'].apply(lambda x: '携帯' if is_mobile(x) else ('固定' if x else ''))

        # --- FAX正規化 ---
        df['FAX_正規化'] = df['選考担当者ＦＡＸ'].apply(normalize_phone)

        # --- 担当者名正規化 ---
        df['担当者名_正規化'] = df['選考担当者氏名漢字'].apply(normalize_name)
        df['代表者名_正規化'] = df['代表者名'].apply(normalize_name)

        # --- 代表者直通判定 ---
        df['代表者直通'] = ((df['代表者名_正規化'] != '') &
                         (df['担当者名_正規化'] != '') &
                         (df['代表者名_正規化'] == df['担当者名_正規化'])).map({True: '○', False: ''})

        # --- 同姓親族判定 ---
        rep_surname = df['代表者名_正規化'].apply(extract_surname)
        contact_surname = df['担当者名_正規化'].apply(extract_surname)
        df['同姓親族'] = (
            (df['代表者直通'] != '○') &  # 代表直通でない
            (rep_surname != '') &
            (contact_surname != '') &
            (rep_surname == contact_surname)
        ).map({True: '○', False: ''})

        # --- 決裁者役職判定 ---
        dept_title = df['選考担当者課係名／役職名'].fillna('').astype(str)
        df['決裁者役職'] = dept_title.apply(
            lambda x: '○' if detect_decision_maker_title(x) else ''
        )

        # --- 管理部門（壁）判定 ---
        df['管理部門壁'] = dept_title.apply(
            lambda x: '○' if detect_manager_wall(x) else ''
        )

        # --- 家族経営名判定 ---
        df['家族経営名'] = df.apply(
            lambda r: '○' if is_family_business_name(
                r.get('事業所名漢字', ''),
                r.get('代表者名_正規化', '')
            ) else '', axis=1
        )

        # --- HP有無 ---
        df['HP有無'] = df['事業所ホームページ'].apply(
            lambda x: '有' if pd.notna(x) and str(x).strip() != '' else '無'
        )

        # --- 法人番号クリーニング ---
        # 元CSVの法人番号を優先（final CSVの法人番号は既にある場合もある）
        law_col = '法人番号_orig' if '法人番号_orig' in df.columns else '法人番号'
        df['法人番号_cleaned'] = df[law_col].apply(clean_corporate_number)

        # --- 設立年 ---
        est_col = '創業設立年（西暦）'
        if est_col in df.columns:
            df['設立年'] = df[est_col].apply(lambda x: str(x).strip() if pd.notna(x) and str(x).strip() != '' else '')
        else:
            df['設立年'] = ''

        # --- 優先度再計算 ---
        def calc_priority(row):
            is_direct = row['代表者直通'] == '○'
            is_mobile_flag = row['電話番号_携帯判定'] == '携帯'
            if is_direct and is_mobile_flag:
                return 'S'
            elif is_direct:
                return 'A'
            elif is_mobile_flag:
                return 'B'
            return 'C'
        df['優先度'] = df.apply(calc_priority, axis=1)

        # --- 決裁者近接スコア ---
        df['近接スコア'] = df.apply(calc_proximity_score, axis=1)
        df['近接ランク'] = df['近接スコア'].map({
            5: '★★★★★', 4: '★★★★☆', 3: '★★★☆☆', 2: '★★☆☆☆', 1: '★☆☆☆☆'
        })

        # --- 出力カラム定義 ---
        output_cols = [
            # メタ
            'セグメント', '業界', 'マッチ経路', '優先度', '近接スコア', '近接ランク',
            '代表者直通', '同姓親族', '決裁者役職', '管理部門壁', '家族経営名', 'HP有無',
            '電話番号_携帯判定',
            # 基本
            '求人番号', '事業所番号',
            '受付年月日（西暦）', '求人有効年月日（西暦）',
            # 事業所
            '事業所名漢字', '事業所名カナ',
            '事業所郵便番号',
            '都道府県', '事業所所在地',
            '市区町村名', '市区町村人口_数値',
            '事業所ホームページ',
            # 分類
            '産業分類（コード）', '産業分類（大分類コード）', '産業分類（名称）',
            '職業分類１（コード）', '職業分類２（コード）', '職業分類３（コード）',
            '職種', '仕事内容',
            '雇用形態',
            # 従業員数
            '従業員数_数値', '従業員数就業場所_数値',
            # 代表者
            '代表者役職', '代表者名',
            # 選考担当者
            '選考担当者課係名／役職名',
            '選考担当者氏名漢字', '選考担当者氏名フリガナ',
            '電話番号_正規化', '選考担当者ＴＥＬ',
            'FAX_正規化',
            '選考担当者Ｅメール',
            # 法人情報
            '法人番号_cleaned', '設立年', '資本金', '事業内容',
        ]
        output_cols = [c for c in output_cols if c in df.columns]

        # 優先度ソート
        priority_order = {'S': 0, 'A': 1, 'B': 2, 'C': 3}
        df['_sort'] = df['優先度'].map(priority_order)
        df = df.sort_values('_sort')

        # 出力
        out_path = OUTPUT_DIR / f'import_{seg_name}.csv'
        df[output_cols].to_csv(out_path, index=False, encoding='utf-8-sig')

        # 統計
        total = len(df)
        emp_filled = df['従業員数_数値'].notna().sum()
        pop_filled = (df['市区町村人口_数値'] != '').sum()
        phone_filled = (df['電話番号_正規化'] != '').sum()
        mobile_cnt = (df['電話番号_携帯判定'] == '携帯').sum()
        fixed_cnt = (df['電話番号_携帯判定'] == '固定').sum()
        corp_filled = (df['法人番号_cleaned'] != '').sum()
        direct_cnt = (df['代表者直通'] == '○').sum()

        print(f"  出力: {out_path}")
        print(f"  --- データ充足率 ---")
        print(f"    従業員数:     {emp_filled:>6,}/{total:,} ({emp_filled/total*100:.1f}%)")
        print(f"    市区町村人口:  {pop_filled:>6,}/{total:,} ({pop_filled/total*100:.1f}%)")
        print(f"    電話番号:     {phone_filled:>6,}/{total:,} ({phone_filled/total*100:.1f}%)")
        print(f"      携帯:      {mobile_cnt:>6,}")
        print(f"      固定:      {fixed_cnt:>6,}")
        print(f"    法人番号:     {corp_filled:>6,}/{total:,} ({corp_filled/total*100:.1f}%)")
        print(f"    代表者直通:   {direct_cnt:>6,}/{total:,} ({direct_cnt/total*100:.1f}%)")

        # 優先度内訳
        pri = df['優先度'].value_counts().reindex(['S', 'A', 'B', 'C'], fill_value=0)
        print(f"  --- 優先度 ---")
        print(f"    S: {pri['S']:,}  A: {pri['A']:,}  B: {pri['B']:,}  C: {pri['C']:,}")

        # 業界内訳
        print(f"  --- 業界 ---")
        ind_counts = df['業界'].value_counts()
        for ind, cnt in ind_counts.items():
            print(f"    {ind}: {cnt:,}件")

        # マッチ経路
        route_counts = df['マッチ経路'].value_counts()
        print(f"  --- マッチ経路 ---")
        for route, cnt in route_counts.items():
            print(f"    {route}: {cnt:,}件")

        # 近接スコア
        prox = df['近接スコア'].value_counts().reindex([5, 4, 3, 2, 1], fill_value=0)
        print(f"  --- 決裁者近接スコア ---")
        print(f"    ★5（代表直通+携帯）:   {prox[5]:>6,}件")
        print(f"    ★4（代表直通/親族小規模）: {prox[4]:>6,}件")
        print(f"    ★3（同姓親族/決裁者役職）: {prox[3]:>6,}件")
        print(f"    ★2（その他）:          {prox[2]:>6,}件")
        print(f"    ★1（管理部門壁）:       {prox[1]:>6,}件")
        wl = prox[5] + prox[4]
        bl = prox[1]
        print(f"    → ホワイト(★4-5): {wl:,}件 ({wl/total*100:.1f}%)")
        print(f"    → ブラック(★1):   {bl:,}件 ({bl/total*100:.1f}%)")

        # 近接指標の充足率
        family_cnt = (df['同姓親族'] == '○').sum()
        dm_cnt = (df['決裁者役職'] == '○').sum()
        wall_cnt = (df['管理部門壁'] == '○').sum()
        fam_biz_cnt = (df['家族経営名'] == '○').sum()
        hp_cnt = (df['HP有無'] == '有').sum()
        print(f"  --- 近接指標 ---")
        print(f"    同姓親族:     {family_cnt:>6,}件 ({family_cnt/total*100:.1f}%)")
        print(f"    決裁者役職:   {dm_cnt:>6,}件 ({dm_cnt/total*100:.1f}%)")
        print(f"    管理部門壁:   {wall_cnt:>6,}件 ({wall_cnt/total*100:.1f}%)")
        print(f"    家族経営名:   {fam_biz_cnt:>6,}件 ({fam_biz_cnt/total*100:.1f}%)")
        print(f"    HP有:        {hp_cnt:>6,}件 ({hp_cnt/total*100:.1f}%)")

        all_results.append(df[output_cols])

    # 統合CSV
    all_df = pd.concat(all_results).drop_duplicates(subset=['求人番号'])
    all_path = OUTPUT_DIR / 'import_CDEF_all.csv'
    all_df.to_csv(all_path, index=False, encoding='utf-8-sig')

    print("\n" + "=" * 70)
    print("最終サマリー")
    print("=" * 70)
    print(f"  C_工事:        {len(all_results[0]):>7,}件")
    print(f"  D_ホテル旅館:   {len(all_results[1]):>7,}件")
    print(f"  E_葬儀:        {len(all_results[2]):>7,}件")
    print(f"  F_産業廃棄物:   {len(all_results[3]):>7,}件")
    print(f"  統合（重複除外）: {len(all_df):>7,}件")
    print(f"\n出力先: {OUTPUT_DIR}")


if __name__ == '__main__':
    main()
