"""
求人データの相関分析スクリプト

分析対象:
- 職種
- 人口数
- 人口密度
- 有効求人倍率
- 給与下限・給与上限（HRハッカーデータ）
と「応募数/掲載日数」の相関
"""

import io
import sys
import re
from pathlib import Path
from datetime import datetime

import pandas as pd
import numpy as np

# Windowsコンソール対応
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# プロジェクトルート
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# HRハッカーCSVパス
HRHACKER_CSV_PATH = Path(r'C:\Users\fuji1\Downloads\HRハッカー_求人情報.csv')

from src.api.salesforce_client import SalesforceClient


def export_jobcard_data(client):
    """JobCard__cデータをエクスポート"""
    print("📥 JobCard__cデータ取得中...")

    import requests

    fields = [
        'Id', 'Name', 'Apply__c', 'StartDate__c', 'EndDate__c',
        'Occupation__c', 'Prefecture__c', 'City__c',
        'HRHackerID__c', 'HRHackerID2__c'
    ]
    soql = f"SELECT {','.join(fields)} FROM JobCard__c"

    all_records = []
    url = f"{client.instance_url}/services/data/{client.api_version}/query?q={soql}"

    while url:
        resp = requests.get(url, headers=client._get_headers())
        data = resp.json()
        all_records.extend(data.get('records', []))
        next_url = data.get('nextRecordsUrl')
        url = f"{client.instance_url}{next_url}" if next_url else None

    df = pd.DataFrame(all_records)
    if 'attributes' in df.columns:
        df = df.drop(columns=['attributes'])

    print(f"✅ JobCard__c取得: {len(df)}件")
    return df


def load_population_data():
    """人口・人口密度データを読み込み"""
    pop_path = project_root / 'data' / 'output' / 'population' / 'municipality_population_density.csv'
    df = pd.read_csv(pop_path, encoding='utf-8-sig')
    print(f"✅ 人口データ読み込み: {len(df)}件")
    return df


def load_job_ratio_data():
    """有効求人倍率データを読み込み"""
    ratio_path = project_root / 'data' / 'job_openings_ratio' / 'complete_mece_municipality_hellowork_mapping.csv'
    df = pd.read_csv(ratio_path, encoding='utf-8-sig')
    print(f"✅ 求人倍率データ読み込み: {len(df)}件")
    return df


def load_hrhacker_salary_data():
    """HRハッカー給与データを読み込み（給与形態含む）"""
    if not HRHACKER_CSV_PATH.exists():
        print(f"⚠️ HRハッカーCSVが見つかりません: {HRHACKER_CSV_PATH}")
        return None

    df = pd.read_csv(HRHACKER_CSV_PATH, encoding='cp932')
    print(f"✅ HRハッカー給与データ読み込み: {len(df)}件")

    # 必要カラムのみ抽出（給与形態も含む）
    salary_df = df[['求人id', '基本給与 最小', '基本給与 最大', '給与形態']].copy()
    salary_df = salary_df.rename(columns={
        '求人id': 'hrhacker_id',
        '基本給与 最小': 'salary_min',
        '基本給与 最大': 'salary_max',
        '給与形態': 'salary_type'
    })

    # 給与データ統計
    valid_min = salary_df['salary_min'].notna().sum()
    valid_max = salary_df['salary_max'].notna().sum()
    print(f"   給与下限あり: {valid_min}件")
    print(f"   給与上限あり: {valid_max}件")
    print(f"   給与形態分布:")
    for stype, cnt in salary_df['salary_type'].value_counts().items():
        print(f"     {stype}: {cnt}件")

    return salary_df


def normalize_city(city):
    """市区町村名を正規化"""
    if not city or pd.isna(city):
        return None
    city = str(city).strip()
    # 郡名を除去
    gun_match = re.match(r'^(.+郡)(.+[町村])$', city)
    if gun_match:
        return gun_match.group(2)
    return city


def merge_data(jobcard_df, population_df, ratio_df, salary_df=None):
    """全データを結合（給与データ含む）"""
    print("\n🔄 データ結合中...")

    # 人口データのキー作成
    population_df['key'] = population_df['prefecture'] + population_df['city']
    pop_dict = {}
    for _, row in population_df.iterrows():
        pop_dict[row['key']] = {
            'population': row['population'],
            'population_density': row['population_density_km2']
        }

    # 求人倍率データの辞書作成
    ratio_dict = {}
    for _, row in ratio_df.iterrows():
        key = (row['prefecture'], row['municipality'])
        ratio_dict[key] = row['ratio']

    # 政令指定都市
    seirei_cities = [
        '札幌市', '仙台市', '新潟市', 'さいたま市', '千葉市', '横浜市', '川崎市', '相模原市',
        '静岡市', '浜松市', '名古屋市', '京都市', '大阪市', '堺市', '神戸市',
        '岡山市', '広島市', '北九州市', '福岡市', '熊本市'
    ]

    # 給与データ辞書作成（給与形態含む）
    salary_dict = {}
    if salary_df is not None:
        for _, row in salary_df.iterrows():
            hrid = row['hrhacker_id']
            if pd.notna(hrid):
                salary_dict[int(hrid)] = {
                    'salary_min': row['salary_min'],
                    'salary_max': row['salary_max'],
                    'salary_type': row['salary_type']
                }
        print(f"   給与データ辞書: {len(salary_dict)}件")

    results = []
    salary_matched = 0

    for _, row in jobcard_df.iterrows():
        pref = row.get('Prefecture__c')
        city = row.get('City__c')
        name = row.get('Name', '')

        # ////はスキップ
        if '////' in str(name):
            continue

        if not pref or pd.isna(pref) or not city or pd.isna(city):
            continue

        # 掲載期間計算
        start_date = row.get('StartDate__c')
        end_date = row.get('EndDate__c')

        # StartDateは必須
        if not start_date or pd.isna(start_date):
            continue

        try:
            start = pd.to_datetime(start_date)
            # EndDateがない場合は今日の日付を使用
            if not end_date or pd.isna(end_date):
                end = pd.Timestamp.now()
            else:
                end = pd.to_datetime(end_date)
            days = (end - start).days
            if days <= 0:
                days = 1  # 最低1日
        except:
            continue

        # 応募数
        apply_count = row.get('Apply__c', 0)
        if pd.isna(apply_count):
            apply_count = 0

        # 応募数/掲載日数（正規化）
        apply_per_day = apply_count / days

        # 職種
        occupation = row.get('Occupation__c', '')

        # 市区町村正規化
        normalized_city = normalize_city(city)

        # 人口データ取得
        pop_key = pref + normalized_city
        pop_info = pop_dict.get(pop_key, {})

        # 政令指定都市の区の場合、市でも試す
        if not pop_info:
            for seirei in seirei_cities:
                if normalized_city and normalized_city.startswith(seirei):
                    seirei_key = pref + seirei
                    pop_info = pop_dict.get(seirei_key, {})
                    if pop_info:
                        break

        population = pop_info.get('population')
        population_density = pop_info.get('population_density')

        # 求人倍率取得
        ratio_key = (pref, normalized_city)
        ratio = ratio_dict.get(ratio_key)

        # 政令指定都市の区の場合
        if pd.isna(ratio) or ratio is None:
            for seirei in seirei_cities:
                if normalized_city and normalized_city.startswith(seirei):
                    seirei_ratio_key = (pref, seirei)
                    ratio = ratio_dict.get(seirei_ratio_key)
                    if ratio:
                        break

        # 給与データ取得（HRHackerID__c または HRHackerID2__c）
        salary_min = None
        salary_max = None
        salary_type = None
        hrhacker_id1 = row.get('HRHackerID__c')
        hrhacker_id2 = row.get('HRHackerID2__c')

        # HRHackerID__c を優先（文字列型の可能性があるため変換）
        def try_get_int_id(val):
            if pd.isna(val) or val is None:
                return None
            try:
                return int(float(str(val).strip()))
            except (ValueError, TypeError):
                return None

        id1_int = try_get_int_id(hrhacker_id1)
        id2_int = try_get_int_id(hrhacker_id2)

        if id1_int is not None and id1_int in salary_dict:
            salary_info = salary_dict[id1_int]
            salary_min = salary_info['salary_min']
            salary_max = salary_info['salary_max']
            salary_type = salary_info['salary_type']
            salary_matched += 1
        elif id2_int is not None and id2_int in salary_dict:
            salary_info = salary_dict[id2_int]
            salary_min = salary_info['salary_min']
            salary_max = salary_info['salary_max']
            salary_type = salary_info['salary_type']
            salary_matched += 1

        results.append({
            'id': row['Id'],
            'name': name,
            'occupation': occupation,
            'prefecture': pref,
            'city': city,
            'apply_count': apply_count,
            'days': days,
            'apply_per_day': apply_per_day,
            'population': population,
            'population_density': population_density,
            'job_ratio': ratio,
            'salary_min': salary_min,
            'salary_max': salary_max,
            'salary_type': salary_type
        })

    result_df = pd.DataFrame(results)
    print(f"✅ 結合完了: {len(result_df)}件")
    if salary_df is not None:
        print(f"   給与データマッチ: {salary_matched}件")

    return result_df


def analyze_correlation(df):
    """相関分析を実行（給与データ含む）"""
    print("\n" + "=" * 60)
    print("📊 相関分析（給与データ含む）")
    print("=" * 60)

    from scipy import stats as scipy_stats

    # 基本変数
    base_cols = ['apply_per_day', 'population', 'population_density', 'job_ratio']

    # 給与カラムの存在チェック
    has_salary = 'salary_min' in df.columns and 'salary_max' in df.columns
    if has_salary:
        salary_cols = ['salary_min', 'salary_max']
        all_cols = base_cols + salary_cols
    else:
        all_cols = base_cols

    # ======================================
    # 1. 基本分析（地理・求人倍率のみ）
    # ======================================
    print("\n" + "-" * 40)
    print("【1. 基本分析（地理・求人倍率）】")
    print("-" * 40)

    analysis_df_base = df.dropna(subset=base_cols)
    print(f"\n分析対象レコード数: {len(analysis_df_base)}件（基本変数の欠損値除外後）")

    # 基本統計量
    print("\n【基本統計量】")
    stats = analysis_df_base[base_cols].describe()
    print(stats.to_string())

    # ピアソン相関
    print("\n【ピアソン相関係数】")
    corr_matrix_base = analysis_df_base[base_cols].corr()
    print(corr_matrix_base.to_string())

    # スピアマン相関
    print("\n【スピアマン順位相関係数】")
    target = 'apply_per_day'
    for col in ['population', 'population_density', 'job_ratio']:
        valid_data = analysis_df_base[[target, col]].dropna()
        if len(valid_data) > 2:
            corr, pvalue = scipy_stats.spearmanr(valid_data[target], valid_data[col])
            sig = "***" if pvalue < 0.001 else "**" if pvalue < 0.01 else "*" if pvalue < 0.05 else ""
            print(f"  {col}: r={corr:.4f}, p={pvalue:.4e} {sig}")

    # ======================================
    # 2. 給与形態別・給与帯別の応募分析
    # ======================================
    corr_matrix_full = None
    has_salary_type = 'salary_type' in df.columns

    if has_salary and has_salary_type:
        print("\n" + "-" * 40)
        print("【2. 給与形態別・給与帯別の応募分析】")
        print("-" * 40)

        # 給与データありのレコードのみ
        salary_subset = df.dropna(subset=['salary_min', 'salary_type'])
        print(f"\n給与データあり: {len(salary_subset)}件")

        # 給与形態別の件数
        print("\n【給与形態別 件数】")
        print(salary_subset['salary_type'].value_counts().to_string())

        # ======================================
        # 給与形態別・給与帯別の応募数分析
        # ======================================
        print("\n" + "=" * 50)
        print("【給与形態別・給与帯別の応募数分析】")
        print("=" * 50)

        for salary_type in ['時給', '月給', '日給', '年俸']:
            type_df = salary_subset[salary_subset['salary_type'] == salary_type].copy()
            if len(type_df) < 5:
                continue

            print(f"\n■ {salary_type} ({len(type_df)}件)")

            # 給与帯の設定（給与形態別）
            if salary_type == '時給':
                bins = [0, 1000, 1100, 1200, 1300, 1500, 2000, 100000]
                labels = ['~1000', '1001-1100', '1101-1200', '1201-1300', '1301-1500', '1501-2000', '2001~']
            elif salary_type == '月給':
                bins = [0, 180000, 200000, 220000, 250000, 280000, 300000, 1000000]
                labels = ['~18万', '18-20万', '20-22万', '22-25万', '25-28万', '28-30万', '30万~']
            elif salary_type == '日給':
                bins = [0, 8000, 10000, 12000, 15000, 20000, 100000]
                labels = ['~8千', '8千-1万', '1万-1.2万', '1.2万-1.5万', '1.5万-2万', '2万~']
            elif salary_type == '年俸':
                bins = [0, 3000000, 3500000, 4000000, 5000000, 10000000]
                labels = ['~300万', '300-350万', '350-400万', '400-500万', '500万~']
            else:
                continue

            # 給与帯を作成
            type_df['salary_band'] = pd.cut(type_df['salary_min'], bins=bins, labels=labels, right=True)

            # 給与帯別の統計
            band_stats = type_df.groupby('salary_band', observed=True).agg({
                'apply_per_day': ['mean', 'median', 'sum'],
                'apply_count': 'sum',
                'id': 'count'
            }).round(4)
            band_stats.columns = ['応募/日_平均', '応募/日_中央値', '応募/日_合計', '応募数_合計', '求人数']

            # 応募ありの割合も計算
            apply_exists = type_df.groupby('salary_band', observed=True)['apply_count'].apply(lambda x: (x > 0).sum())
            band_stats['応募あり件数'] = apply_exists
            band_stats['応募あり率'] = (band_stats['応募あり件数'] / band_stats['求人数'] * 100).round(1)

            print(band_stats.to_string())

            # この給与形態でどの帯が最も応募を集めているか
            if len(band_stats) > 0:
                best_band = band_stats['応募/日_平均'].idxmax()
                best_rate = band_stats.loc[best_band, '応募/日_平均']
                best_apply_rate = band_stats.loc[best_band, '応募あり率']
                print(f"\n  → 最も応募が多い帯: {best_band} (応募/日={best_rate:.4f}, 応募あり率={best_apply_rate}%)")

        # ======================================
        # 応募がある求人の給与分布
        # ======================================
        print("\n" + "=" * 50)
        print("【応募がある求人 vs ない求人の給与比較】")
        print("=" * 50)

        for salary_type in ['時給', '月給']:
            type_df = salary_subset[salary_subset['salary_type'] == salary_type].copy()
            if len(type_df) < 10:
                continue

            with_apply = type_df[type_df['apply_count'] > 0]
            without_apply = type_df[type_df['apply_count'] == 0]

            print(f"\n■ {salary_type}")
            print(f"  応募あり ({len(with_apply)}件):")
            print(f"    給与下限 平均: {with_apply['salary_min'].mean():,.0f}円")
            print(f"    給与下限 中央値: {with_apply['salary_min'].median():,.0f}円")
            if len(without_apply) > 0:
                print(f"  応募なし ({len(without_apply)}件):")
                print(f"    給与下限 平均: {without_apply['salary_min'].mean():,.0f}円")
                print(f"    給与下限 中央値: {without_apply['salary_min'].median():,.0f}円")

                # 統計的検定（Mann-Whitney U検定）
                if len(with_apply) >= 5 and len(without_apply) >= 5:
                    stat, pvalue = scipy_stats.mannwhitneyu(
                        with_apply['salary_min'].dropna(),
                        without_apply['salary_min'].dropna(),
                        alternative='two-sided'
                    )
                    sig = "***" if pvalue < 0.001 else "**" if pvalue < 0.01 else "*" if pvalue < 0.05 else ""
                    print(f"  → Mann-Whitney U検定: p={pvalue:.4e} {sig}")

    # ======================================
    # 3. 職種別分析
    # ======================================
    print("\n" + "-" * 40)
    print("【3. 職種別 応募数/日 平均】")
    print("-" * 40)
    occupation_stats = df.groupby('occupation')['apply_per_day'].agg(['mean', 'std', 'count'])
    occupation_stats = occupation_stats.sort_values('mean', ascending=False)
    print(occupation_stats.head(15).to_string())

    # 給与付き職種別分析
    if has_salary:
        print("\n【職種別 給与平均（給与データありのみ）】")
        salary_by_occ = df.dropna(subset=['salary_min']).groupby('occupation').agg({
            'salary_min': 'mean',
            'salary_max': 'mean',
            'apply_per_day': 'mean'
        }).round(0)
        salary_by_occ = salary_by_occ.sort_values('apply_per_day', ascending=False)
        print(salary_by_occ.head(15).to_string())

    return analysis_df_base, corr_matrix_full if corr_matrix_full is not None else corr_matrix_base


def cross_tabulation_top30(df, exclude_occupations=None):
    """
    クロス集計 TOP30
    都道府県×市町村×職種×給与形態 + 人口・人口密度・有効求人倍率
    """
    print("\n" + "=" * 80)
    print("📊 クロス集計 TOP30（都道府県×市町村×職種×給与形態）")
    print("=" * 80)

    # 除外職種
    if exclude_occupations is None:
        exclude_occupations = ['調理', 'ドライバー', '事務']

    print(f"除外職種: {exclude_occupations}")

    # 給与データあり、かつ除外職種を除く
    subset = df.dropna(subset=['salary_type', 'salary_min']).copy()
    for occ in exclude_occupations:
        subset = subset[~subset['occupation'].str.contains(occ, na=False)]

    print(f"分析対象: {len(subset)}件（給与データあり、除外職種除く）")

    # クロス集計：都道府県×市町村×職種×給与形態
    cross_tab = subset.groupby(
        ['prefecture', 'city', 'occupation', 'salary_type'],
        as_index=False
    ).agg({
        'id': 'count',
        'apply_per_day': 'mean',
        'apply_count': 'sum',
        'salary_min': 'mean',
        'salary_max': 'mean',
        'population': 'first',
        'population_density': 'first',
        'job_ratio': 'first'
    })

    cross_tab.columns = [
        '都道府県', '市町村', '職種', '給与形態',
        '求人数', '応募/日平均', '応募計', '給与下限平均', '給与上限平均',
        '人口', '人口密度', '有効求人倍率'
    ]

    # 応募/日平均でソート（降順）してTOP30
    cross_tab_sorted = cross_tab.sort_values('応募/日平均', ascending=False)
    top30 = cross_tab_sorted.head(30)

    # 表示用に整形
    print("\n【TOP30 応募/日平均順】")
    print("-" * 140)

    # カラム幅を調整して表示
    display_cols = [
        '都道府県', '市町村', '職種', '給与形態', '求人数',
        '応募/日平均', '応募計', '給与下限平均',
        '人口', '人口密度', '有効求人倍率'
    ]

    # 数値フォーマット
    top30_display = top30[display_cols].copy()
    top30_display['応募/日平均'] = top30_display['応募/日平均'].round(4)
    top30_display['給与下限平均'] = top30_display['給与下限平均'].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else '-')
    top30_display['人口'] = top30_display['人口'].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else '-')
    top30_display['人口密度'] = top30_display['人口密度'].apply(lambda x: f"{x:,.1f}" if pd.notna(x) else '-')
    top30_display['有効求人倍率'] = top30_display['有効求人倍率'].apply(lambda x: f"{x:.2f}" if pd.notna(x) else '-')

    # 一覧表示
    for i, row in top30_display.iterrows():
        print(f"{top30_display.index.get_loc(i)+1:2}. "
              f"{row['都道府県']:4s} {row['市町村']:10s} "
              f"{row['職種']:15s} {row['給与形態']:3s} "
              f"求人{row['求人数']:3d}件 "
              f"応募/日{row['応募/日平均']:.4f} "
              f"給与{row['給与下限平均']:>10s}円 "
              f"人口{row['人口']:>12s} "
              f"密度{row['人口密度']:>8s} "
              f"倍率{row['有効求人倍率']}")

    return cross_tab_sorted


def main():
    print("=" * 60)
    print("求人データ相関分析（給与データ含む）")
    print("=" * 60)

    # Salesforce認証
    client = SalesforceClient()
    client.authenticate()

    # データ取得
    jobcard_df = export_jobcard_data(client)
    population_df = load_population_data()
    ratio_df = load_job_ratio_data()
    salary_df = load_hrhacker_salary_data()

    # データ結合（給与データ含む）
    merged_df = merge_data(jobcard_df, population_df, ratio_df, salary_df)

    # 相関分析
    analysis_df, corr_matrix = analyze_correlation(merged_df)

    # クロス集計TOP30（調理・ドライバー・事務除外）
    cross_tab = cross_tabulation_top30(merged_df, exclude_occupations=['調理', 'ドライバー', '事務'])

    # 結果保存
    output_dir = project_root / 'data' / 'output' / 'analysis'
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    merged_path = output_dir / f'jobcard_correlation_data_{timestamp}.csv'
    merged_df.to_csv(merged_path, index=False, encoding='utf-8-sig')
    print(f"\n📁 分析用データ保存: {merged_path}")

    corr_path = output_dir / f'correlation_matrix_{timestamp}.csv'
    corr_matrix.to_csv(corr_path, encoding='utf-8-sig')
    print(f"📁 相関行列保存: {corr_path}")

    # クロス集計結果保存
    cross_path = output_dir / f'cross_tabulation_top30_{timestamp}.csv'
    cross_tab.to_csv(cross_path, index=False, encoding='utf-8-sig')
    print(f"📁 クロス集計TOP30保存: {cross_path}")


if __name__ == '__main__':
    main()
