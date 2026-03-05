"""ハローワーク セグメント分析レポート生成

3つのレポートCSVを生成:
  1. 業界別レポート
  2. 職種別レポート
  3. 決裁者近接スコア別レポート
"""
import pandas as pd
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

INPUT_DIR = Path(r'C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List\data\output\hellowork_segments\import_ready')
OUTPUT_DIR = Path(r'C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List\data\output\hellowork_segments\reports')
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

SEG_FILES = ['import_C_工事.csv', 'import_D_ホテル旅館.csv', 'import_E_葬儀.csv', 'import_F_産業廃棄物.csv']


def load_all():
    frames = []
    for f in SEG_FILES:
        df = pd.read_csv(INPUT_DIR / f, encoding='utf-8-sig', dtype=str)
        df['近接スコア'] = pd.to_numeric(df['近接スコア'], errors='coerce')
        df['従業員数_数値'] = pd.to_numeric(df['従業員数_数値'], errors='coerce')
        df['市区町村人口_数値'] = pd.to_numeric(df['市区町村人口_数値'], errors='coerce')
        frames.append(df)
    return pd.concat(frames, ignore_index=True)


def report_industry(df):
    """レポート1: 業界別"""
    print("\n" + "=" * 70)
    print("レポート1: 業界別サマリー")
    print("=" * 70)

    rows = []
    for seg_name in df['セグメント'].unique():
        seg_df = df[df['セグメント'] == seg_name]
        for ind_name in seg_df['業界'].value_counts().index:
            ind_df = seg_df[seg_df['業界'] == ind_name]
            n = len(ind_df)
            emp_med = ind_df['従業員数_数値'].median()
            emp_mean = ind_df['従業員数_数値'].mean()
            emp_10 = (ind_df['従業員数_数値'] <= 10).sum()

            direct = (ind_df['代表者直通'] == '○').sum()
            family = (ind_df['同姓親族'] == '○').sum()
            mobile = (ind_df['電話番号_携帯判定'] == '携帯').sum()
            dm_title = (ind_df['決裁者役職'] == '○').sum()
            wall = (ind_df['管理部門壁'] == '○').sum()

            white = ((ind_df['近接スコア'] >= 4)).sum()
            black = ((ind_df['近接スコア'] == 1)).sum()

            pri_s = (ind_df['優先度'] == 'S').sum()
            pri_a = (ind_df['優先度'] == 'A').sum()
            pri_b = (ind_df['優先度'] == 'B').sum()
            pri_c = (ind_df['優先度'] == 'C').sum()

            # マッチ経路
            route_p1 = (ind_df['マッチ経路'].str.contains('パターン①', na=False)).sum()
            route_p2 = (ind_df['マッチ経路'].str.contains('パターン②', na=False)).sum()

            rows.append({
                'セグメント': seg_name,
                '業界': ind_name,
                '件数': n,
                '従業員数_中央値': round(emp_med, 1) if pd.notna(emp_med) else '',
                '従業員数_平均': round(emp_mean, 1) if pd.notna(emp_mean) else '',
                '10名以下': emp_10,
                '10名以下率': f'{emp_10/n*100:.1f}%',
                '代表者直通': direct,
                '代表者直通率': f'{direct/n*100:.1f}%',
                '同姓親族': family,
                '同姓親族率': f'{family/n*100:.1f}%',
                '携帯あり': mobile,
                '決裁者役職': dm_title,
                '管理部門壁': wall,
                'ホワイト(★4-5)': white,
                'ホワイト率': f'{white/n*100:.1f}%',
                'ブラック(★1)': black,
                'ブラック率': f'{black/n*100:.1f}%',
                '優先度S': pri_s,
                '優先度A': pri_a,
                '優先度B': pri_b,
                '優先度C': pri_c,
                'パターン①件数': route_p1,
                'パターン②件数': route_p2,
            })

    report_df = pd.DataFrame(rows)
    out_path = OUTPUT_DIR / 'report_01_業界別.csv'
    report_df.to_csv(out_path, index=False, encoding='utf-8-sig')
    print(f"  → {out_path}")

    # コンソール表示
    for seg_name in df['セグメント'].unique():
        seg_rows = [r for r in rows if r['セグメント'] == seg_name]
        print(f"\n  [{seg_name}]")
        print(f"  {'業界':<20} {'件数':>6} {'従業員中央':>6} {'直通率':>6} {'親族率':>6} {'WH率':>6} {'BL率':>6}")
        print(f"  {'-'*70}")
        for r in seg_rows:
            print(f"  {r['業界']:<20} {r['件数']:>6,} {str(r['従業員数_中央値']):>6} {r['代表者直通率']:>6} {r['同姓親族率']:>6} {r['ホワイト率']:>6} {r['ブラック率']:>6}")

    return report_df


def report_job_type(df):
    """レポート2: 職種別"""
    print("\n" + "=" * 70)
    print("レポート2: 職種別サマリー")
    print("=" * 70)

    # 職種を大分類にまとめる（上位N件 + その他）
    def categorize_job(job_title):
        if pd.isna(job_title) or str(job_title).strip() == '':
            return 'その他'
        j = str(job_title).strip()

        # 工事系
        if any(kw in j for kw in ['施工管理', '現場監督', '工事管理', '主任技術者']):
            return '施工管理・現場監督'
        if any(kw in j for kw in ['土木', '道路', '河川', '造成', '法面', '橋梁']):
            return '土木作業'
        if any(kw in j for kw in ['大工', '型枠', '木工']):
            return '大工・型枠'
        if any(kw in j for kw in ['とび', '鳶', '足場', '仮設']):
            return 'とび・足場'
        if any(kw in j for kw in ['塗装', 'ペンキ', '吹付']):
            return '塗装'
        if any(kw in j for kw in ['電気工事', '電気設備', '配線']):
            return '電気工事'
        if any(kw in j for kw in ['配管', '水道', '給排水', '管工事', '設備工事']):
            return '設備・配管工事'
        if any(kw in j for kw in ['鉄骨', '鉄筋', '溶接']):
            return '鉄骨・鉄筋・溶接'
        if any(kw in j for kw in ['解体', '取壊']):
            return '解体工事'
        if any(kw in j for kw in ['左官', 'タイル', 'ブロック']):
            return '左官・タイル'
        if any(kw in j for kw in ['防水', 'シーリング']):
            return '防水工事'
        if any(kw in j for kw in ['内装', 'クロス', '床', '天井']):
            return '内装仕上'
        if any(kw in j for kw in ['リフォーム', '改修', '改装', 'リノベ']):
            return 'リフォーム・改修'
        if any(kw in j for kw in ['建築', '建設', '工事']):
            return '建築・建設（一般）'

        # ホテル・旅館系
        if any(kw in j for kw in ['フロント', '受付', 'レセプション']):
            return 'フロント・受付'
        if any(kw in j for kw in ['調理', '料理', 'シェフ', 'コック', '板前', '厨房']):
            return '調理・厨房'
        if any(kw in j for kw in ['客室', '清掃', 'ハウスキーピング', 'ベッドメイク']):
            return '客室・清掃'
        if any(kw in j for kw in ['仲居', '接客', 'サービス', 'ホール']):
            return '接客・サービス'
        if any(kw in j for kw in ['支配人', 'マネージャー', '管理者']):
            return '支配人・管理'

        # 葬儀系
        if any(kw in j for kw in ['葬儀', '葬祭', 'セレモニー', '式典']):
            return '葬儀・セレモニー'
        if any(kw in j for kw in ['納棺', '湯灌', 'エンバーミング']):
            return '納棺・湯灌'

        # 廃棄物系
        if any(kw in j for kw in ['収集', '回収', '運搬']):
            return '収集・運搬'
        if any(kw in j for kw in ['処理', '処分', '焼却', '中間処理']):
            return '処理・処分'
        if any(kw in j for kw in ['リサイクル', '選別', '分別', '再生']):
            return 'リサイクル・選別'
        if any(kw in j for kw in ['重機', 'ショベル', 'ユンボ', 'クレーン', 'オペレータ']):
            return '重機オペレータ'

        # 共通系
        if any(kw in j for kw in ['営業', '販売', 'ルート']):
            return '営業・販売'
        if any(kw in j for kw in ['事務', '経理', '総務', '人事']):
            return '事務・管理'
        if any(kw in j for kw in ['ドライバー', '運転', '配送', '運送', '輸送']):
            return 'ドライバー・運転'
        if any(kw in j for kw in ['CAD', '設計', '積算']):
            return '設計・CAD・積算'

        return 'その他'

    df = df.copy()
    df['職種カテゴリ'] = df['職種'].apply(categorize_job)

    rows = []
    for seg_name in df['セグメント'].unique():
        seg_df = df[df['セグメント'] == seg_name]
        for cat in seg_df['職種カテゴリ'].value_counts().index:
            cat_df = seg_df[seg_df['職種カテゴリ'] == cat]
            n = len(cat_df)
            emp_med = cat_df['従業員数_数値'].median()

            direct = (cat_df['代表者直通'] == '○').sum()
            family = (cat_df['同姓親族'] == '○').sum()
            mobile = (cat_df['電話番号_携帯判定'] == '携帯').sum()
            wall = (cat_df['管理部門壁'] == '○').sum()

            white = (cat_df['近接スコア'] >= 4).sum()
            black = (cat_df['近接スコア'] == 1).sum()

            pri_s = (cat_df['優先度'] == 'S').sum()
            pri_a = (cat_df['優先度'] == 'A').sum()

            rows.append({
                'セグメント': seg_name,
                '職種カテゴリ': cat,
                '件数': n,
                '従業員数_中央値': round(emp_med, 1) if pd.notna(emp_med) else '',
                '代表者直通': direct,
                '代表者直通率': f'{direct/n*100:.1f}%',
                '同姓親族': family,
                '同姓親族率': f'{family/n*100:.1f}%',
                '携帯あり': mobile,
                '管理部門壁': wall,
                'ホワイト(★4-5)': white,
                'ホワイト率': f'{white/n*100:.1f}%',
                'ブラック(★1)': black,
                'ブラック率': f'{black/n*100:.1f}%',
                '優先度S': pri_s,
                '優先度A': pri_a,
            })

    report_df = pd.DataFrame(rows)
    out_path = OUTPUT_DIR / 'report_02_職種別.csv'
    report_df.to_csv(out_path, index=False, encoding='utf-8-sig')
    print(f"  → {out_path}")

    # コンソール表示
    for seg_name in df['セグメント'].unique():
        seg_rows = [r for r in rows if r['セグメント'] == seg_name]
        print(f"\n  [{seg_name}]")
        print(f"  {'職種カテゴリ':<20} {'件数':>6} {'従業員中央':>6} {'直通率':>6} {'親族率':>6} {'WH率':>6} {'BL率':>6}")
        print(f"  {'-'*70}")
        for r in seg_rows:
            print(f"  {r['職種カテゴリ']:<20} {r['件数']:>6,} {str(r['従業員数_中央値']):>6} {r['代表者直通率']:>6} {r['同姓親族率']:>6} {r['ホワイト率']:>6} {r['ブラック率']:>6}")

    return report_df


def report_proximity(df):
    """レポート3: 決裁者近接スコア別"""
    print("\n" + "=" * 70)
    print("レポート3: 決裁者近接スコア別サマリー")
    print("=" * 70)

    score_labels = {
        5: '★5 代表直通+携帯',
        4: '★4 代表直通/親族小規模/決裁者+携帯',
        3: '★3 同姓親族/決裁者役職',
        2: '★2 その他',
        1: '★1 管理部門壁',
    }

    rows = []
    for seg_name in df['セグメント'].unique():
        seg_df = df[df['セグメント'] == seg_name]
        seg_total = len(seg_df)
        for score in [5, 4, 3, 2, 1]:
            sc_df = seg_df[seg_df['近接スコア'] == score]
            n = len(sc_df)
            if n == 0:
                continue
            emp_med = sc_df['従業員数_数値'].median()
            emp_mean = sc_df['従業員数_数値'].mean()
            emp_10 = (sc_df['従業員数_数値'] <= 10).sum()
            emp_30 = (sc_df['従業員数_数値'] <= 30).sum()

            direct = (sc_df['代表者直通'] == '○').sum()
            family = (sc_df['同姓親族'] == '○').sum()
            mobile = (sc_df['電話番号_携帯判定'] == '携帯').sum()
            dm_title = (sc_df['決裁者役職'] == '○').sum()
            wall = (sc_df['管理部門壁'] == '○').sum()
            fam_biz = (sc_df['家族経営名'] == '○').sum()
            hp = (sc_df['HP有無'] == '有').sum()

            # 業界top3
            ind_top3 = sc_df['業界'].value_counts().head(3)
            ind_str = ' / '.join([f'{k}({v:,})' for k, v in ind_top3.items()])

            rows.append({
                'セグメント': seg_name,
                '近接スコア': score,
                'ランク': score_labels[score],
                '件数': n,
                '構成比': f'{n/seg_total*100:.1f}%',
                '従業員数_中央値': round(emp_med, 1) if pd.notna(emp_med) else '',
                '従業員数_平均': round(emp_mean, 1) if pd.notna(emp_mean) else '',
                '10名以下': emp_10,
                '10名以下率': f'{emp_10/n*100:.1f}%',
                '30名以下': emp_30,
                '30名以下率': f'{emp_30/n*100:.1f}%',
                '代表者直通': direct,
                '同姓親族': family,
                '携帯あり': mobile,
                '決裁者役職': dm_title,
                '管理部門壁': wall,
                '家族経営名': fam_biz,
                'HP有': hp,
                '上位業界': ind_str,
            })

    report_df = pd.DataFrame(rows)
    out_path = OUTPUT_DIR / 'report_03_近接スコア別.csv'
    report_df.to_csv(out_path, index=False, encoding='utf-8-sig')
    print(f"  → {out_path}")

    # コンソール表示
    for seg_name in df['セグメント'].unique():
        seg_rows = [r for r in rows if r['セグメント'] == seg_name]
        seg_total = sum(r['件数'] for r in seg_rows)
        print(f"\n  [{seg_name}] 合計: {seg_total:,}件")
        print(f"  {'ランク':<35} {'件数':>6} {'構成比':>6} {'従業員中央':>6} {'10名以下':>7} {'直通':>5} {'親族':>5} {'携帯':>5} {'壁':>5}")
        print(f"  {'-'*95}")
        for r in seg_rows:
            print(f"  {r['ランク']:<35} {r['件数']:>6,} {r['構成比']:>6} {str(r['従業員数_中央値']):>6} {r['10名以下率']:>7} {r['代表者直通']:>5,} {r['同姓親族']:>5,} {r['携帯あり']:>5,} {r['管理部門壁']:>5,}")

    # 全セグメント合計
    print(f"\n  [全セグメント合計]")
    grand_total = len(df)
    print(f"  {'ランク':<35} {'件数':>6} {'構成比':>6}")
    print(f"  {'-'*55}")
    for score in [5, 4, 3, 2, 1]:
        n = (df['近接スコア'] == score).sum()
        print(f"  {score_labels[score]:<35} {n:>6,} {n/grand_total*100:>5.1f}%")
    print(f"  {'─'*55}")
    wh = (df['近接スコア'] >= 4).sum()
    gr = ((df['近接スコア'] >= 2) & (df['近接スコア'] <= 3)).sum()
    bl = (df['近接スコア'] == 1).sum()
    print(f"  {'ホワイトリスト(★4-5)':<35} {wh:>6,} {wh/grand_total*100:>5.1f}%")
    print(f"  {'グレーゾーン(★2-3)':<35} {gr:>6,} {gr/grand_total*100:>5.1f}%")
    print(f"  {'ブラックリスト(★1)':<35} {bl:>6,} {bl/grand_total*100:>5.1f}%")

    return report_df


def main():
    print("=" * 70)
    print("ハローワーク セグメント分析レポート生成")
    print("=" * 70)

    df = load_all()
    print(f"\n全データ: {len(df):,}件")

    r1 = report_industry(df)
    r2 = report_job_type(df)
    r3 = report_proximity(df)

    print("\n" + "=" * 70)
    print("レポート生成完了")
    print("=" * 70)
    print(f"  1. {OUTPUT_DIR / 'report_01_業界別.csv'}")
    print(f"  2. {OUTPUT_DIR / 'report_02_職種別.csv'}")
    print(f"  3. {OUTPUT_DIR / 'report_03_近接スコア別.csv'}")


if __name__ == '__main__':
    main()
