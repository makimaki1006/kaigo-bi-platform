"""
介護サービス情報公表システム kihon（詳細）ページ差分スクレイピング
================================================================
既存CSV（kaigo_fast_*.csv）の施設リストを元に、kihonページの
tableGroup-4（従業者）とtableGroup-5（サービス内容）から
職種別人数・資格・認知症研修・全加算項目を追加取得する。

既存CSVに含まれない詳細データのみを抽出し、事業所番号で結合可能な
差分CSVを出力する。

使い方:
  # フル実行（15並列、推定18-24時間）
  python scripts/scrape_kihon_delta.py

  # 特定都道府県のみ
  python scripts/scrape_kihon_delta.py --pref 13

  # 件数制限（テスト用）
  python scripts/scrape_kihon_delta.py --limit 10

  # 並列数変更
  python scripts/scrape_kihon_delta.py --workers 20

出力:
  data/output/kaigo_scraping/kihon_delta_{date}.csv
"""

import sys
import os
import json
import time
import re
import argparse
import traceback
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# コンソール出力のエンコーディング設定
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
os.environ['PYTHONIOENCODING'] = 'utf-8'

import requests
from bs4 import BeautifulSoup
import pandas as pd

# =====================================================================
# 設定
# =====================================================================
BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE_DIR / "data" / "output" / "kaigo_scraping"
PROGRESS_DIR = OUTPUT_DIR / "delta_progress"

TODAY = datetime.now().strftime("%Y%m%d")
TODAY_ISO = datetime.now().strftime("%Y-%m-%d")

DEFAULT_WORKERS = 15
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_DELAY = 5
SAVE_INTERVAL = 100       # 100件ごとに中間保存
PROGRESS_INTERVAL = 50    # 50件ごとに進捗表示

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'ja,en-US;q=0.7,en;q=0.3',
}

# =====================================================================
# action_code・サービス名マッピング（scrape_kaigo_full.pyと同一）
# =====================================================================
ACTION_CODE_MAP = {
    110: '001', 120: '002', 130: '004', 140: '005', 150: '001',
    160: '003', 170: '006', 210: '007', 220: '008', 230: '009',
    320: '022', 331: '014', 332: '015', 334: '016', 335: '017',
    336: '001', 337: '001', 361: '018', 362: '019', 364: '020',
    410: '010', 430: '023', 510: '011', 520: '012', 530: '013',
    540: '001', 550: '001', 551: '001',
    710: '024', 720: '025', 730: '021', 760: '026', 770: '027', 780: '028',
}

SERVICE_NAMES = {
    110: '訪問介護', 120: '訪問入浴介護', 130: '訪問看護',
    140: '訪問リハビリテーション', 150: '通所介護', 155: '指定療養通所介護',
    160: '通所リハビリテーション', 170: '福祉用具貸与',
    210: '短期入所生活介護', 220: '短期入所療養介護_老健',
    230: '短期入所療養介護_病院', 320: '認知症対応型共同生活介護',
    331: '特定施設_有料老人ホーム', 332: '特定施設_軽費老人ホーム',
    334: '特定施設_サ高住', 335: '特定施設_有料_外部',
    336: '特定施設_軽費_外部', 337: '特定施設_サ高住_外部',
    361: '地域密着型特定施設_有料', 362: '地域密着型特定施設_軽費',
    364: '地域密着型特定施設_サ高住',
    410: '特定福祉用具販売', 430: '居宅介護支援',
    510: '介護老人福祉施設', 520: '介護老人保健施設',
    530: '介護療養型医療施設', 540: '地域密着型特養',
    550: '介護医療院', 551: '短期入所_介護医療院',
    710: '夜間対応型訪問介護', 720: '認知症対応型通所介護',
    730: '小規模多機能型居宅介護', 760: '定期巡回随時対応',
    770: '看護小規模多機能', 780: '地域密着型通所介護',
}

# =====================================================================
# 出力カラム定義
# =====================================================================
DELTA_COLUMNS = [
    '事業所番号',
    'サービスコード',
    '都道府県コード',
    # 職種別人数（tableGroup-4）
    '介護職員_常勤', '介護職員_非常勤', '介護職員_合計',
    '看護職員_常勤', '看護職員_非常勤', '看護職員_合計',
    '生活相談員_常勤', '生活相談員_非常勤', '生活相談員_合計',
    '機能訓練指導員_常勤', '機能訓練指導員_非常勤', '機能訓練指導員_合計',
    '管理栄養士_常勤', '管理栄養士_非常勤', '管理栄養士_合計',
    '事務員_常勤', '事務員_非常勤', '事務員_合計',
    # 資格（tableGroup-4）
    '介護福祉士数', '実務者研修数', '初任者研修数', '介護支援専門員数',
    # 夜勤（tableGroup-4）
    '夜勤人数', '宿直人数',
    # 認知症研修（tableGroup-4）
    '認知症指導者研修数', '認知症リーダー研修数', '認知症実践者研修数',
    # 全加算（tableGroup-5、JSON）
    '加算_全項目',
    # メタ
    'スクレイピング日',
]

# 職種名→カラム名プレフィックスのマッピング
# kihonページの従業者テーブルでは職種名が行見出しに含まれる
STAFF_TYPE_MAP = {
    '介護職員': '介護職員',
    '看護職員': '看護職員',
    '看護師': '看護職員',
    '准看護師': '看護職員',
    '生活相談員': '生活相談員',
    '支援相談員': '生活相談員',
    '機能訓練指導員': '機能訓練指導員',
    '理学療法士': '機能訓練指導員',
    '作業療法士': '機能訓練指導員',
    '言語聴覚士': '機能訓練指導員',
    '管理栄養士': '管理栄養士',
    '栄養士': '管理栄養士',
    '事務員': '事務員',
    '事務職員': '事務員',
}


# =====================================================================
# ユーティリティ
# =====================================================================
def parse_num(text):
    """テキストから数値を抽出"""
    if not text:
        return ''
    m = re.search(r'(\d+)', str(text).replace(',', ''))
    return m.group(1) if m else ''


def find_table_by_keyword(tables, keywords, exclude_keywords=None):
    """キーワードでテーブルを検索"""
    if isinstance(keywords, str):
        keywords = [keywords]
    exclude_keywords = exclude_keywords or []
    for i, table in enumerate(tables):
        text = table.get_text()
        if all(kw in text for kw in keywords):
            if not any(ekw in text for ekw in exclude_keywords):
                return i, table
    return -1, None


# =====================================================================
# URL構築（scrape_kaigo_full.pyと同一パターン）
# =====================================================================
def build_kihon_url(pref_code, action_code, jigyosyo_cd, service_code):
    """kihonページのURLを構築"""
    base = f"https://www.kaigokensaku.mhlw.go.jp/{pref_code}/index.php"
    return (
        f"{base}?action_kouhyou_detail_{action_code}_kihon=true"
        f"&JigyosyoCd={jigyosyo_cd}-00&ServiceCd={service_code}"
    )


# =====================================================================
# ページ取得（リトライ付き、スレッドセーフ）
# =====================================================================
# スレッドローカルストレージ（セッション管理用）
_thread_local = threading.local()


def get_session():
    """スレッドごとにrequests.Sessionを管理"""
    if not hasattr(_thread_local, 'session'):
        _thread_local.session = requests.Session()
        _thread_local.session.headers.update(HEADERS)
    return _thread_local.session


def fetch_page(url):
    """1ページを取得（リトライ付き）"""
    session = get_session()
    for attempt in range(MAX_RETRIES):
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
            else:
                raise


# =====================================================================
# kihon差分パーサー
# =====================================================================
def parse_kihon_delta(html, result):
    """kihonページのtableGroup-4（従業者）とtableGroup-5（サービス内容）をパース

    既存のparse_kihonと同じキーワードベースの検出手法を使用。
    tableGroupのIDではなく、テーブル内容のキーワードで対象テーブルを特定する。
    """
    soup = BeautifulSoup(html, 'html.parser')
    tables = soup.find_all('table')

    if len(tables) < 3:
        return False

    parsed_anything = False

    # =================================================================
    # tableGroup-4: 従業者情報
    # =================================================================

    # --- 職種別人数テーブル ---
    # キーワード: 「従業者の数」「常勤」を含み「賃金」を含まないテーブル
    idx, t_emp = find_table_by_keyword(
        tables, ['従業者の数', '常勤'], exclude_keywords=['賃金']
    )
    if not t_emp:
        idx, t_emp = find_table_by_keyword(tables, ['実人数', '常勤'])

    if t_emp:
        # 職種名→人数の累積（同じカテゴリに複数職種が属する場合を考慮）
        staff_totals = {}

        for row in t_emp.find_all('tr'):
            cells = row.find_all(['th', 'td'])
            texts = [c.get_text(strip=True) for c in cells]

            # 「（うち」「うち」で始まる行はサブカテゴリなのでスキップ
            if texts and (texts[0].startswith('（うち') or texts[0].startswith('うち')):
                continue

            # 7セルパターン: [職種名, 常勤専従, 常勤兼務, 非常勤専従, 非常勤兼務, 合計, 人]
            if len(texts) == 7 and '人' in texts[5]:
                job_name = texts[0]
                jokin = int(parse_num(texts[1]) or '0') + int(parse_num(texts[2]) or '0')
                hijokin = int(parse_num(texts[3]) or '0') + int(parse_num(texts[4]) or '0')
                goukei = int(parse_num(texts[5]) or '0')
                _accumulate_staff(staff_totals, job_name, jokin, hijokin, goukei)
                parsed_anything = True

            # 6セルパターン: [職種名, 常勤専従, 常勤兼務, 非常勤, 合計, 人]
            elif len(texts) == 6 and '人' in texts[4]:
                job_name = texts[0]
                jokin = int(parse_num(texts[1]) or '0') + int(parse_num(texts[2]) or '0')
                hijokin = int(parse_num(texts[3]) or '0')
                goukei = int(parse_num(texts[4]) or '0')
                _accumulate_staff(staff_totals, job_name, jokin, hijokin, goukei)
                parsed_anything = True

        # 職種別人数をresultに格納
        for prefix, vals in staff_totals.items():
            result[f'{prefix}_常勤'] = str(vals['jokin']) if vals['jokin'] > 0 else ''
            result[f'{prefix}_非常勤'] = str(vals['hijokin']) if vals['hijokin'] > 0 else ''
            result[f'{prefix}_合計'] = str(vals['goukei']) if vals['goukei'] > 0 else ''

    # --- 資格保有者数テーブル ---
    # キーワード: 「介護福祉士」を含むテーブル
    for table in tables:
        table_text = table.get_text()
        if '介護福祉士' not in table_text:
            continue

        for row in table.find_all('tr'):
            cells = row.find_all(['th', 'td'])
            texts = [c.get_text(strip=True) for c in cells]
            joined = ''.join(texts)

            # 介護福祉士
            if '介護福祉士' in joined and '実務者' not in joined and '初任者' not in joined:
                num = _extract_person_count(texts)
                if num:
                    result['介護福祉士数'] = num
                    parsed_anything = True

            # 実務者研修
            if '実務者研修' in joined:
                num = _extract_person_count(texts)
                if num:
                    result['実務者研修数'] = num
                    parsed_anything = True

            # 初任者研修（介護職員初任者研修）
            if '初任者研修' in joined:
                num = _extract_person_count(texts)
                if num:
                    result['初任者研修数'] = num
                    parsed_anything = True

            # 介護支援専門員（ケアマネ）
            if '介護支援専門員' in joined or 'ケアマネ' in joined:
                num = _extract_person_count(texts)
                if num:
                    result['介護支援専門員数'] = num
                    parsed_anything = True

    # --- 夜勤・宿直テーブル ---
    for table in tables:
        table_text = table.get_text()
        if '夜勤' not in table_text and '宿直' not in table_text:
            continue

        for row in table.find_all('tr'):
            cells = row.find_all(['th', 'td'])
            texts = [c.get_text(strip=True) for c in cells]
            joined = ''.join(texts)

            if '夜勤' in joined and '人' in joined:
                num = _extract_person_count(texts)
                if num:
                    result['夜勤人数'] = num
                    parsed_anything = True

            if '宿直' in joined and '人' in joined:
                num = _extract_person_count(texts)
                if num:
                    result['宿直人数'] = num
                    parsed_anything = True

    # --- 認知症研修修了者数テーブル ---
    for table in tables:
        table_text = table.get_text()
        if '認知症' not in table_text or '研修' not in table_text:
            continue

        for row in table.find_all('tr'):
            cells = row.find_all(['th', 'td'])
            texts = [c.get_text(strip=True) for c in cells]
            joined = ''.join(texts)

            if '指導者養成' in joined or '指導者研修' in joined:
                num = _extract_person_count(texts)
                if num:
                    result['認知症指導者研修数'] = num
                    parsed_anything = True

            if 'リーダー' in joined or '実践リーダー' in joined:
                num = _extract_person_count(texts)
                if num:
                    result['認知症リーダー研修数'] = num
                    parsed_anything = True

            if '実践者研修' in joined and 'リーダー' not in joined:
                num = _extract_person_count(texts)
                if num:
                    result['認知症実践者研修数'] = num
                    parsed_anything = True

    # =================================================================
    # tableGroup-5: サービス内容（加算項目の網羅的抽出）
    # =================================================================
    # 全加算の動的抽出（scrape_kaigo_full.pyの加算パースロジックと同一）
    all_kasan = {}
    for table in tables:
        table_text = table.get_text()
        if '加算' not in table_text:
            continue

        for row in table.find_all('tr'):
            ths = row.find_all('th')
            tds = row.find_all('td')
            if not ths or not tds:
                continue

            kasan_name = ths[-1].get_text(strip=True)

            # 加算名のフィルタ
            if '加算' not in kasan_name:
                continue
            if kasan_name in ('加算状況', '介護報酬の加算状況', '加算の状況'):
                continue
            if '加算状況' in kasan_name and len(kasan_name) > 20:
                continue

            # 値の検出（○/×/img判定）
            value = ''
            has_check = False
            for cell in tds:
                cell_text = cell.get_text(strip=True)
                if cell_text in ('○', 'あり', '✓', '✔', '●'):
                    has_check = True
                    break
                if cell_text in ('×', 'なし', '―', '-'):
                    value = '×'
                    break
                inp = cell.find('input', {'checked': True})
                if inp:
                    has_check = True
                    break
                img = cell.find('img', alt=re.compile(r'チェック|あり|丸'))
                if img:
                    has_check = True
                    break
                img_alt = cell.find('img')
                if img_alt and img_alt.get('alt', '') == 'あり':
                    has_check = True
                    break
                img_no = cell.find('img', alt=re.compile(r'なし|バツ'))
                if img_no:
                    value = '×'
                    break

            if has_check:
                value = '○'

            if kasan_name and len(kasan_name) > 2:
                all_kasan[kasan_name] = value
                parsed_anything = True

    if all_kasan:
        result['加算_全項目'] = json.dumps(all_kasan, ensure_ascii=False)

    return parsed_anything


def _accumulate_staff(staff_totals, job_name, jokin, hijokin, goukei):
    """職種名をマッピングして人数を累積"""
    prefix = None
    for keyword, mapped in STAFF_TYPE_MAP.items():
        if keyword in job_name:
            prefix = mapped
            break

    if prefix is None:
        # マッピングに該当しない職種はスキップ（合計に含まれないものもある）
        return

    if prefix not in staff_totals:
        staff_totals[prefix] = {'jokin': 0, 'hijokin': 0, 'goukei': 0}

    staff_totals[prefix]['jokin'] += jokin
    staff_totals[prefix]['hijokin'] += hijokin
    staff_totals[prefix]['goukei'] += goukei


def _extract_person_count(texts):
    """テキストリストから人数を抽出（「N人」パターン）"""
    for t in texts:
        m = re.search(r'(\d+)\s*人', t)
        if m:
            return m.group(1)
    # テキスト中の数値のみの場合もある
    for t in reversed(texts):
        num = parse_num(t)
        if num and int(num) > 0:
            return num
    return ''


# =====================================================================
# 1施設のスクレイピング（スレッドから呼ばれる）
# =====================================================================
# レート制限用ロック
_rate_lock = threading.Lock()
_last_request_times = {}  # スレッドID -> 最後のリクエスト時刻


def scrape_one_facility(pref_code, facility_code, service_code):
    """1施設のkihonページをスクレイピングし、差分データを返す"""
    action_code = ACTION_CODE_MAP.get(int(service_code))
    if not action_code:
        return None

    result = {col: '' for col in DELTA_COLUMNS}
    result['事業所番号'] = facility_code
    result['サービスコード'] = str(service_code)
    result['都道府県コード'] = str(pref_code)
    result['スクレイピング日'] = TODAY_ISO

    # レート制限: スレッドごとに1秒間隔
    tid = threading.current_thread().ident
    with _rate_lock:
        now = time.time()
        last = _last_request_times.get(tid, 0)
        wait = max(0, 1.0 - (now - last))
    if wait > 0:
        time.sleep(wait)

    try:
        url = build_kihon_url(pref_code, action_code, facility_code, service_code)
        html = fetch_page(url)

        with _rate_lock:
            _last_request_times[tid] = time.time()

        if html and parse_kihon_delta(html, result):
            return result
        else:
            # HTMLは取得できたがパースに失敗
            return result
    except Exception as e:
        # リクエスト失敗
        print(f"    kihon取得エラー ({facility_code}): {e}")
        return None


# =====================================================================
# メイン処理
# =====================================================================
def main():
    parser = argparse.ArgumentParser(
        description='kihon（詳細）ページ差分スクレイピング'
    )
    parser.add_argument(
        '--pref', type=str, default=None,
        help='都道府県コード（カンマ区切り、例: 13,14）'
    )
    parser.add_argument(
        '--limit', type=int, default=0,
        help='スクレイピング件数制限（テスト用、0=無制限）'
    )
    parser.add_argument(
        '--workers', type=int, default=DEFAULT_WORKERS,
        help=f'並列ワーカー数（デフォルト: {DEFAULT_WORKERS}）'
    )
    parser.add_argument(
        '--fresh', action='store_true',
        help='進捗をリセットして最初から取得'
    )
    args = parser.parse_args()

    print("=" * 60)
    print("kihon（詳細）ページ差分スクレイピング")
    print("=" * 60)

    # 既存CSVの読み込み
    existing_csv = None
    for f in sorted(OUTPUT_DIR.glob("kaigo_fast_*.csv"), reverse=True):
        existing_csv = f
        break

    if not existing_csv:
        print("エラー: 既存CSVが見つかりません（data/output/kaigo_scraping/kaigo_fast_*.csv）")
        sys.exit(1)

    print(f"既存CSV: {existing_csv.name}")
    df = pd.read_csv(existing_csv, dtype=str, encoding='utf-8-sig', low_memory=False)
    print(f"施設数: {len(df):,}件")

    # 必須カラムの存在確認
    required_cols = ['事業所番号', 'サービスコード', '都道府県コード']
    for col in required_cols:
        if col not in df.columns:
            print(f"エラー: 既存CSVに '{col}' カラムがありません")
            sys.exit(1)

    # 都道府県フィルタ
    target_prefs = None
    if args.pref:
        target_prefs = [p.strip().zfill(2) for p in args.pref.split(',')]
        df = df[df['都道府県コード'].str.zfill(2).isin(target_prefs)]
        print(f"都道府県フィルタ後: {len(df):,}件 (コード: {', '.join(target_prefs)})")

    # 施設リスト作成
    facilities = []
    for _, row in df.iterrows():
        code = str(row['事業所番号']).strip()
        svc = str(row['サービスコード']).strip()
        pref = str(row['都道府県コード']).strip().zfill(2)
        if code and svc and pref:
            facilities.append((pref, code, svc))

    if not facilities:
        print("対象施設がありません")
        sys.exit(1)

    # 進捗ファイルの確認（再開対応）
    PROGRESS_DIR.mkdir(parents=True, exist_ok=True)

    pref_suffix = f"_pref{'_'.join(target_prefs)}" if target_prefs else ""
    progress_file = PROGRESS_DIR / f"delta_progress{pref_suffix}.csv"
    output_file = OUTPUT_DIR / f"kihon_delta_{TODAY}.csv"

    already_done = set()
    results = []

    if args.fresh and progress_file.exists():
        progress_file.unlink()
        print("進捗リセット: 最初から取得")
    elif progress_file.exists():
        try:
            prev_df = pd.read_csv(progress_file, dtype=str, encoding='utf-8-sig')
            results = prev_df.to_dict('records')
            # 事業所番号+サービスコードの組み合わせで一意キーを作る
            already_done = {
                f"{r.get('事業所番号', '')}_{r.get('サービスコード', '')}"
                for r in results
            }
            print(f"途中再開: {len(already_done):,}件済み")
        except Exception as e:
            print(f"進捗ファイル読み込みエラー: {e}")

    # スクレイピング対象をフィルタ（済みをスキップ）
    remaining = [
        (pref, code, svc) for pref, code, svc in facilities
        if f"{code}_{svc}" not in already_done
    ]

    if args.limit > 0:
        remaining = remaining[:args.limit]

    print(f"\n取得対象: {len(remaining):,}件")
    print(f"並列数: {args.workers}")
    est_hours = len(remaining) / args.workers / 3600
    print(f"推定所要時間: {est_hours:.1f}時間")
    print()

    if not remaining:
        print("全件取得済みです")
        if results:
            _save_output(results, output_file)
        return

    # スレッドプールでスクレイピング実行
    start_time = time.time()
    errors = 0
    processed = 0
    lock = threading.Lock()

    def _process_one(args_tuple):
        """1施設を処理するワーカー関数"""
        pref, code, svc = args_tuple
        return scrape_one_facility(pref, code, svc)

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(_process_one, item): item
            for item in remaining
        }

        for future in as_completed(futures):
            pref, code, svc = futures[future]
            try:
                result = future.result()
                with lock:
                    if result:
                        results.append(result)
                    else:
                        # 失敗時も最低限の情報で記録
                        fail_result = {col: '' for col in DELTA_COLUMNS}
                        fail_result['事業所番号'] = code
                        fail_result['サービスコード'] = svc
                        fail_result['都道府県コード'] = pref
                        fail_result['スクレイピング日'] = TODAY_ISO
                        results.append(fail_result)
                        errors += 1

                    processed += 1

                    # 進捗表示
                    if processed % PROGRESS_INTERVAL == 0 or processed == len(remaining):
                        elapsed = time.time() - start_time
                        rate = processed / elapsed if elapsed > 0 else 0
                        eta = (len(remaining) - processed) / rate if rate > 0 else 0
                        pct = processed / len(remaining) * 100
                        print(
                            f"  [{processed:,}/{len(remaining):,}] {pct:.1f}% | "
                            f"{rate:.1f}件/秒 | 残り{eta/60:.0f}分 | エラー{errors}"
                        )

                    # 中間保存
                    if processed % SAVE_INTERVAL == 0:
                        _save_progress(results, progress_file)

            except Exception as e:
                with lock:
                    errors += 1
                    processed += 1
                    # 失敗レコード記録
                    fail_result = {col: '' for col in DELTA_COLUMNS}
                    fail_result['事業所番号'] = code
                    fail_result['サービスコード'] = svc
                    fail_result['都道府県コード'] = pref
                    fail_result['スクレイピング日'] = TODAY_ISO
                    results.append(fail_result)

    # 最終保存
    elapsed = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"完了: {processed:,}件処理 / エラー{errors}件")
    print(f"所要時間: {elapsed/3600:.2f}時間")

    _save_output(results, output_file)

    # 進捗ファイル削除（完了時）
    if progress_file.exists():
        progress_file.unlink()
        print(f"進捗ファイル削除: {progress_file.name}")


def _save_progress(results, progress_file):
    """中間進捗を保存"""
    try:
        df = pd.DataFrame(results, columns=DELTA_COLUMNS)
        df.to_csv(progress_file, index=False, encoding='utf-8-sig')
    except Exception as e:
        print(f"  進捗保存エラー: {e}")


def _save_output(results, output_file):
    """最終結果をCSV出力"""
    df = pd.DataFrame(results, columns=DELTA_COLUMNS)
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"出力: {output_file}")
    print(f"件数: {len(df):,}件")

    # データ概要の表示
    non_empty_counts = {}
    for col in DELTA_COLUMNS:
        if col in ('事業所番号', 'サービスコード', '都道府県コード', 'スクレイピング日'):
            continue
        count = df[col].notna().sum() - (df[col] == '').sum()
        if count > 0:
            non_empty_counts[col] = int(count)

    if non_empty_counts:
        print(f"\nデータ充填状況:")
        for col, count in sorted(non_empty_counts.items(), key=lambda x: -x[1]):
            pct = count / len(df) * 100
            print(f"  {col}: {count:,}件 ({pct:.1f}%)")


if __name__ == '__main__':
    main()
