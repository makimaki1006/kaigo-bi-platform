"""
介護サービス情報公表システム 全国スクレイピング
==================================================
全都道府県×全サービス種別の施設詳細ページから
管理者名・従業者数・代表者名・電話番号等を取得する。

使い方:
  # 全件実行（数時間〜半日かかる）
  python scripts/scrape_kaigo_nationwide.py

  # 特定の都道府県だけ実行
  python scripts/scrape_kaigo_nationwide.py --pref 13        # 東京都のみ
  python scripts/scrape_kaigo_nationwide.py --pref 13,14,27  # 東京・神奈川・大阪

  # 特定のサービス種別だけ実行
  python scripts/scrape_kaigo_nationwide.py --service 150      # 通所介護のみ
  python scripts/scrape_kaigo_nationwide.py --service 150,510  # 通所介護＋特養

  # 組み合わせ
  python scripts/scrape_kaigo_nationwide.py --pref 13 --service 150

  # 中断後の再開（自動で途中から再開）
  python scripts/scrape_kaigo_nationwide.py

出力:
  data/output/kaigo_scraping/kaigo_nationwide_{date}.csv（全件統合）
  data/output/kaigo_scraping/by_service/{service_code}_{service_name}.csv（種別ごと）
"""

import sys
import os
import io
import time
import re
import argparse
import traceback
from pathlib import Path
from datetime import datetime

# UTF-8出力設定
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
SERVICE_DIR = OUTPUT_DIR / "by_service"
PROGRESS_DIR = OUTPUT_DIR / "progress"

TODAY = datetime.now().strftime("%Y%m%d")
TODAY_ISO = datetime.now().strftime("%Y-%m-%d")

REQUEST_DELAY = 1.5  # リクエスト間隔（秒）
SAVE_INTERVAL = 100  # 中間保存間隔
PROGRESS_INTERVAL = 50  # 進捗表示間隔
MAX_RETRIES = 3  # リトライ回数
RETRY_DELAY = 5  # リトライ間隔（秒）
CONSECUTIVE_ERROR_LIMIT = 10  # 連続エラーでストップ

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'ja,en-US;q=0.7,en;q=0.3',
}

# =====================================================================
# サービスコード → action_code マッピング
# =====================================================================
ACTION_CODE_MAP = {
    110: 1,    # 訪問介護
    120: 2,    # 訪問入浴介護
    130: 4,    # 訪問看護
    140: 5,    # 訪問リハビリテーション
    150: 1,    # 通所介護
    160: 3,    # 通所リハビリテーション
    170: 6,    # 福祉用具貸与
    210: 7,    # 短期入所生活介護
    220: 8,    # 短期入所療養介護（老健）
    230: 9,    # 短期入所療養介護（病院）
    320: 22,   # 認知症対応型共同生活介護
    331: 14,   # 特定施設（有料老人ホーム）
    332: 15,   # 特定施設（軽費老人ホーム）
    334: 16,   # 特定施設（サ高住）
    335: 17,   # 特定施設（有料・外部サービス利用型）
    336: 1,    # 特定施設（軽費・外部サービス利用型）
    337: 1,    # 特定施設（サ高住・外部サービス利用型）
    361: 18,   # 地域密着型特定施設（有料）
    362: 19,   # 地域密着型特定施設（軽費）
    364: 20,   # 地域密着型特定施設（サ高住）
    410: 10,   # 特定福祉用具販売
    430: 23,   # 居宅介護支援
    510: 11,   # 介護老人福祉施設（特養）
    520: 12,   # 介護老人保健施設（老健）
    530: 13,   # 介護療養型医療施設
    540: 1,    # 地域密着型特養
    550: 1,    # 介護医療院
    551: 1,    # 短期入所（介護医療院）
    710: 24,   # 夜間対応型訪問介護
    720: 25,   # 認知症対応型通所介護
    730: 21,   # 小規模多機能型居宅介護
    760: 26,   # 定期巡回・随時対応型訪問介護看護
    770: 27,   # 看護小規模多機能型居宅介護
    780: 28,   # 地域密着型通所介護
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

PREF_NAMES = {
    1: '北海道', 2: '青森県', 3: '岩手県', 4: '宮城県', 5: '秋田県',
    6: '山形県', 7: '福島県', 8: '茨城県', 9: '栃木県', 10: '群馬県',
    11: '埼玉県', 12: '千葉県', 13: '東京都', 14: '神奈川県', 15: '新潟県',
    16: '富山県', 17: '石川県', 18: '福井県', 19: '山梨県', 20: '長野県',
    21: '岐阜県', 22: '静岡県', 23: '愛知県', 24: '三重県', 25: '滋賀県',
    26: '京都府', 27: '大阪府', 28: '兵庫県', 29: '奈良県', 30: '和歌山県',
    31: '鳥取県', 32: '島根県', 33: '岡山県', 34: '広島県', 35: '山口県',
    36: '徳島県', 37: '香川県', 38: '愛媛県', 39: '高知県', 40: '福岡県',
    41: '佐賀県', 42: '長崎県', 43: '熊本県', 44: '大分県', 45: '宮崎県',
    46: '鹿児島県', 47: '沖縄県',
}

# 出力CSVカラム
OUTPUT_COLUMNS = [
    '事業所番号', 'サービスコード', 'サービス名', '都道府県コード', '都道府県名',
    '事業所名', '管理者名', '管理者職名',
    '代表者名', '代表者職名', '法人名', '法人番号',
    '電話番号', 'FAX番号', '住所', 'HP',
    '従業者_常勤', '従業者_非常勤', '従業者_合計',
    '定員', '事業開始日', '前年度採用数', '前年度退職数',
    'スクレイピング日',
]


# =====================================================================
# CSVダウンロード
# =====================================================================
def download_open_data_csv(service_code):
    """厚労省オープンデータCSVをダウンロードして事業所一覧を取得"""
    url = f"https://www.mhlw.go.jp/content/12300000/jigyosho_{service_code}.csv"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=60)
        resp.raise_for_status()
        text = resp.content.decode('utf-8-sig')
        df = pd.read_csv(io.StringIO(text), dtype=str, low_memory=False)
        return df
    except Exception as e:
        print(f"  CSV取得エラー（{service_code}）: {e}")
        return None


# =====================================================================
# HTMLパーサー（全サービス共通）
# =====================================================================
def parse_num(text):
    """テキストから数値を抽出"""
    if not text:
        return ''
    m = re.search(r'(\d+)', text.replace(',', ''))
    return m.group(1) if m else ''


def extract_facility_detail(pref_code, facility_code, service_code, action_code, session):
    """施設詳細ページから情報を抽出する（全サービス共通パーサー）

    HTMLテーブル構造（全サービス共通）:
      [0] サマリー
      [1] 法人情報（法人名、法人番号、代表者氏名・職名、法人電話番号）
      [2] サービス一覧
      [3] 事業所詳細（事業所名、住所、電話、FAX、HP、管理者名）
      [4] 従業者数（職種別常勤・非常勤）、採用・退職者数
      [5] 運営方針、利用定員
    """
    action_str = f"{action_code:03d}"
    url = (
        f"https://www.kaigokensaku.mhlw.go.jp/{pref_code}/index.php?"
        f"action_kouhyou_detail_{action_str}_kihon=true"
        f"&JigyosyoCd={facility_code}-00&ServiceCd={service_code}"
    )

    for attempt in range(MAX_RETRIES):
        try:
            resp = session.get(url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            break
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
            else:
                raise

    soup = BeautifulSoup(resp.text, 'html.parser')
    tables = soup.find_all('table')

    if len(tables) < 5:
        return None

    result = {col: '' for col in OUTPUT_COLUMNS}
    result['事業所番号'] = facility_code
    result['サービスコード'] = str(service_code)
    result['サービス名'] = SERVICE_NAMES.get(service_code, '')
    result['都道府県コード'] = str(pref_code)
    result['都道府県名'] = PREF_NAMES.get(int(pref_code), '')
    result['スクレイピング日'] = TODAY_ISO

    # === テーブル1: 法人情報 ===
    try:
        t1 = tables[1]
        t1_rows = t1.find_all('tr')
        for j, row in enumerate(t1_rows):
            ths = [th.get_text(strip=True) for th in row.find_all('th')]
            tds = [td.get_text(strip=True) for td in row.find_all('td')]
            all_text = ''.join(ths) + ''.join(tds)

            # 法人名: thなし、td1つのみの行（Row 4あたり）
            if j == 4 and len(tds) == 1 and tds[0] and not ths:
                result['法人名'] = tds[0]

            # 法人番号
            if '法人番号' in ''.join(ths) and '有無' not in ''.join(ths):
                for t in tds:
                    if re.match(r'^\d{13}$', t):
                        result['法人番号'] = t
                        break

            # 代表者氏名
            if '代表者' in all_text and '氏名' in ''.join(ths):
                if tds:
                    result['代表者名'] = tds[0]

            # 代表者職名
            if result['代表者名'] and '職名' in ''.join(ths) and '氏名' not in ''.join(ths) and not result['代表者職名']:
                if tds:
                    result['代表者職名'] = tds[0]
    except Exception:
        pass

    # === テーブル3: 事業所詳細 ===
    try:
        t3 = tables[3]
        t3_rows = t3.find_all('tr')
        _postal = ''
        _address = ''
        _building = ''

        for j, row in enumerate(t3_rows):
            ths = [th.get_text(strip=True) for th in row.find_all('th')]
            tds = [td.get_text(strip=True) for td in row.find_all('td')]
            th_text = ''.join(ths)

            # 事業所名
            if j == 2 and tds and not ths:
                result['事業所名'] = tds[0]

            # 住所
            if '所在地' in th_text and j <= 5:
                for t in tds:
                    if t.startswith('〒'):
                        _postal = t
                        break
            if '都道府県' in th_text:
                if tds:
                    _address = tds[0]
            if '建物' in th_text:
                if tds and tds[0]:
                    _building = tds[0]

            # 電話番号
            if '電話番号' in th_text and 'FAX' not in th_text and 'ＦＡＸ' not in th_text:
                for t in tds:
                    if re.search(r'\d', t):
                        result['電話番号'] = t
                        break

            # FAX番号
            if 'FAX' in th_text or 'ＦＡＸ' in th_text:
                for t in tds:
                    if re.search(r'\d', t):
                        result['FAX番号'] = t
                        break

            # HP
            if 'ホームページ' in th_text:
                link = row.find('a')
                if link and link.get('href'):
                    result['HP'] = link['href']
                else:
                    for t in tds:
                        if t.startswith('http'):
                            result['HP'] = t
                            break

            # 管理者氏名
            if '管理者' in th_text and '氏名' in th_text:
                if tds:
                    result['管理者名'] = tds[0]

            # 管理者職名
            if result['管理者名'] and '職名' in th_text and '氏名' not in th_text and '管理者' not in th_text and not result['管理者職名']:
                if tds:
                    result['管理者職名'] = tds[0]

            # 事業開始日
            if '開始' in th_text and '予定' in th_text:
                for t in tds:
                    if re.match(r'\d{4}/\d{2}/\d{2}', t):
                        result['事業開始日'] = t
                        break

        # 住所結合
        parts = [p for p in [_postal, _address, _building] if p]
        if parts:
            result['住所'] = ' '.join(parts)
    except Exception:
        pass

    # === テーブル4: 従業者数 ===
    try:
        t4 = tables[4]
        t4_rows = t4.find_all('tr')

        total_jokin = 0
        total_hijokin = 0
        total_all = 0
        emp_found = False
        total_hire = 0
        total_retire = 0

        for row in t4_rows:
            cells = row.find_all(['th', 'td'])
            texts = [c.get_text(strip=True) for c in cells]

            # 従業者行の検出
            # パターン1: 7セル（職種, 常勤専従, 常勤兼務, 非常勤専従, 非常勤兼務, 合計, 常勤換算）
            if len(texts) == 7 and '人' in texts[5]:
                job_title = texts[0]
                # 「うち〜」は内訳なのでスキップ
                if job_title.startswith('（うち') or job_title.startswith('うち'):
                    continue
                jokin = int(parse_num(texts[1]) or '0') + int(parse_num(texts[2]) or '0')
                hijokin = int(parse_num(texts[3]) or '0') + int(parse_num(texts[4]) or '0')
                goukei = int(parse_num(texts[5]) or '0')
                total_jokin += jokin
                total_hijokin += hijokin
                total_all += goukei
                emp_found = True

            # パターン2: 6セル（一部サービスで常勤換算なし）
            elif len(texts) == 6 and '人' in texts[4]:
                job_title = texts[0]
                if job_title.startswith('（うち') or job_title.startswith('うち'):
                    continue
                jokin = int(parse_num(texts[1]) or '0') + int(parse_num(texts[2]) or '0')
                hijokin = int(parse_num(texts[3]) or '0')
                goukei = int(parse_num(texts[4]) or '0')
                total_jokin += jokin
                total_hijokin += hijokin
                total_all += goukei
                emp_found = True

            # 採用・退職者数
            if '前年度の採用者数' in ''.join(texts):
                nums = [int(parse_num(t) or '0') for t in texts if '人' in t]
                total_hire += sum(nums)
            if '前年度の退職者数' in ''.join(texts):
                nums = [int(parse_num(t) or '0') for t in texts if '人' in t]
                total_retire += sum(nums)

        if emp_found:
            result['従業者_常勤'] = str(total_jokin)
            result['従業者_非常勤'] = str(total_hijokin)
            result['従業者_合計'] = str(total_all)
        if total_hire > 0:
            result['前年度採用数'] = str(total_hire)
        if total_retire > 0:
            result['前年度退職数'] = str(total_retire)
    except Exception:
        pass

    # === テーブル5: 利用定員 ===
    try:
        t5 = tables[5]
        for row in t5.find_all('tr'):
            texts = [c.get_text(strip=True) for c in row.find_all(['th', 'td'])]
            joined = ''.join(texts)
            if ('定員' in joined or '入所' in joined) and '人' in joined:
                for t in texts:
                    num = parse_num(t)
                    if num and int(num) > 0:
                        result['定員'] = num
                        break
                if result['定員']:
                    break
    except Exception:
        pass

    return result


# =====================================================================
# メインロジック
# =====================================================================
def process_service(service_code, target_prefs=None):
    """1サービス種別の全施設をスクレイピング"""
    service_name = SERVICE_NAMES.get(service_code, f'不明_{service_code}')
    action_code = ACTION_CODE_MAP.get(service_code)

    if action_code is None:
        print(f"  スキップ: {service_code} ({service_name}) - action_codeなし")
        return []

    print(f"\n{'='*60}")
    print(f"サービス: {service_code} ({service_name})")
    print(f"{'='*60}")

    # CSVダウンロード
    df = download_open_data_csv(service_code)
    if df is None or len(df) == 0:
        print(f"  データなし")
        return []

    print(f"  全国施設数: {len(df):,}件")

    # 都道府県フィルタ
    if target_prefs:
        pref_names_filter = [PREF_NAMES.get(int(p), '') for p in target_prefs]
        df = df[df['都道府県名'].isin(pref_names_filter)]
        print(f"  フィルタ後: {len(df):,}件 ({', '.join(pref_names_filter)})")

    if len(df) == 0:
        return []

    # 都道府県コードを事業所番号の先頭2桁から推定
    df['_pref_code'] = df['事業所番号'].astype(str).str[:2]

    # 進捗ファイル確認（再開対応）
    progress_file = PROGRESS_DIR / f"progress_{service_code}.csv"
    already_done = set()
    results = []
    if progress_file.exists():
        try:
            prev = pd.read_csv(progress_file, dtype=str, encoding='utf-8-sig')
            results = prev.to_dict('records')
            already_done = set(prev['事業所番号'].tolist())
            print(f"  途中再開: {len(already_done)}件済み")
        except Exception:
            pass

    # スクレイピング
    session = requests.Session()
    facility_list = list(df.iterrows())
    remaining = [(idx, row) for idx, row in facility_list if str(row['事業所番号']).strip() not in already_done]

    if not remaining:
        print(f"  全件取得済み")
        return results

    print(f"  取得対象: {len(remaining):,}件")
    est_min = len(remaining) * REQUEST_DELAY / 60
    print(f"  推定所要時間: {est_min:.0f}分")

    start_time = time.time()
    errors = []
    consecutive_errors = 0  # 連続エラーカウンタ
    bot_stopped = False  # Bot検知ストップフラグ

    for count, (idx, row) in enumerate(remaining, 1):
        code = str(row['事業所番号']).strip()
        pref_code = str(row.get('_pref_code', '13')).strip()

        # CSVから取れる基本情報（フォールバック用）
        csv_info = {
            '事業所名': str(row.get('事業所名', '')).strip() if pd.notna(row.get('事業所名')) else '',
            '電話番号': str(row.get('電話番号', '')).strip() if pd.notna(row.get('電話番号')) else '',
            'FAX番号': str(row.get('FAX番号', '')).strip() if pd.notna(row.get('FAX番号')) else '',
            '住所': str(row.get('住所', '')).strip() if pd.notna(row.get('住所')) else '',
            '法人名': str(row.get('法人の名称', '')).strip() if pd.notna(row.get('法人の名称')) else '',
            '法人番号': str(row.get('法人番号', '')).strip() if pd.notna(row.get('法人番号')) else '',
            '定員': str(row.get('定員', '')).strip() if pd.notna(row.get('定員')) else '',
            'HP': str(row.get('URL', '')).strip() if pd.notna(row.get('URL')) else '',
        }

        try:
            result = extract_facility_detail(pref_code, code, service_code, action_code, session)

            if result is None:
                # ページ取得失敗 → CSV情報で補完
                result = {col: '' for col in OUTPUT_COLUMNS}
                result['事業所番号'] = code
                result['サービスコード'] = str(service_code)
                result['サービス名'] = service_name
                result['都道府県コード'] = pref_code
                result['都道府県名'] = PREF_NAMES.get(int(pref_code), '')
                result['スクレイピング日'] = TODAY_ISO
                errors.append(code)
                consecutive_errors += 1
            else:
                consecutive_errors = 0  # 成功したらリセット

            # CSV情報で空フィールドを補完
            for key in csv_info:
                if not result.get(key) and csv_info[key]:
                    result[key] = csv_info[key]

            results.append(result)

        except Exception as e:
            result = {col: '' for col in OUTPUT_COLUMNS}
            result['事業所番号'] = code
            result['サービスコード'] = str(service_code)
            result['サービス名'] = service_name
            result['都道府県コード'] = pref_code
            result['都道府県名'] = PREF_NAMES.get(int(pref_code), '')
            result['スクレイピング日'] = TODAY_ISO
            for key in csv_info:
                if csv_info[key]:
                    result[key] = csv_info[key]
            results.append(result)
            errors.append(code)
            consecutive_errors += 1

            if 'ConnectionError' in str(type(e).__name__) or 'Timeout' in str(type(e).__name__):
                print(f"  ⚠ 接続エラー [{code}] - 10秒待機（連続{consecutive_errors}回目）")
                time.sleep(10)

        # ① 連続エラー10回でストップ
        if consecutive_errors >= CONSECUTIVE_ERROR_LIMIT:
            print(f"\n  🛑 連続{CONSECUTIVE_ERROR_LIMIT}回エラー検出 - Bot認知の可能性あり")
            print(f"  処理を中断します。取得済み{len(results)}件を保存して終了。")
            print(f"  再開するには同じコマンドを再実行してください（進捗ファイルから自動再開）。")
            bot_stopped = True
            # 中間保存して終了
            save_df = pd.DataFrame(results, columns=OUTPUT_COLUMNS)
            save_df.to_csv(progress_file, index=False, encoding='utf-8-sig')
            break

        # 進捗表示
        if count % PROGRESS_INTERVAL == 0 or count == len(remaining):
            elapsed = time.time() - start_time
            rate = count / elapsed if elapsed > 0 else 0
            eta = (len(remaining) - count) / rate if rate > 0 else 0
            name = result.get('事業所名', '')[:20] if result else ''
            print(f"  [{count}/{len(remaining)}] {count/len(remaining)*100:.1f}% "
                  f"| {rate:.1f}件/秒 | 残り{eta/60:.0f}分 | {name}")

        # 中間保存
        if count % SAVE_INTERVAL == 0:
            save_df = pd.DataFrame(results, columns=OUTPUT_COLUMNS)
            save_df.to_csv(progress_file, index=False, encoding='utf-8-sig')

        time.sleep(REQUEST_DELAY)

    # ② Bot検知ストップ時は進捗保存のみ、サービス別CSVは作らない
    if bot_stopped:
        print(f"\n  ⚠ 中断状態: 進捗ファイルに{len(results)}件保存済み")
        print(f"  進捗ファイル: {progress_file}")
        print(f"  時間を空けてから再実行してください")
        return results  # 呼び出し元でも処理を止める

    # サービス別CSV保存（正常完了時のみ）
    service_file = SERVICE_DIR / f"{service_code}_{service_name}.csv"
    save_df = pd.DataFrame(results, columns=OUTPUT_COLUMNS)
    save_df.to_csv(service_file, index=False, encoding='utf-8-sig')
    print(f"  保存: {service_file.name} ({len(results):,}件)")

    # 進捗ファイル削除（完了済み）
    if progress_file.exists():
        progress_file.unlink()

    if errors:
        print(f"  エラー: {len(errors)}件")

    return results


def print_final_summary(all_results):
    """最終サマリー"""
    print("\n" + "=" * 60)
    print("最終サマリー")
    print("=" * 60)

    total = len(all_results)
    if total == 0:
        print("結果なし")
        return

    with_manager = sum(1 for r in all_results if r.get('管理者名'))
    with_emp = sum(1 for r in all_results if r.get('従業者_合計'))
    with_ceo = sum(1 for r in all_results if r.get('代表者名'))
    with_phone = sum(1 for r in all_results if r.get('電話番号'))
    with_corp = sum(1 for r in all_results if r.get('法人番号'))

    print(f"総施設数: {total:,}")
    print(f"管理者名あり: {with_manager:,} ({with_manager/total*100:.1f}%)")
    print(f"従業者数あり: {with_emp:,} ({with_emp/total*100:.1f}%)")
    print(f"代表者名あり: {with_ceo:,} ({with_ceo/total*100:.1f}%)")
    print(f"電話番号あり: {with_phone:,} ({with_phone/total*100:.1f}%)")
    print(f"法人番号あり: {with_corp:,} ({with_corp/total*100:.1f}%)")

    # サービス別
    print("\nサービス別件数:")
    by_service = {}
    for r in all_results:
        svc = r.get('サービス名', '不明')
        by_service[svc] = by_service.get(svc, 0) + 1
    for svc, cnt in sorted(by_service.items(), key=lambda x: -x[1]):
        print(f"  {svc}: {cnt:,}件")

    # 都道府県別
    print("\n都道府県別件数:")
    by_pref = {}
    for r in all_results:
        pref = r.get('都道府県名', '不明')
        by_pref[pref] = by_pref.get(pref, 0) + 1
    for pref, cnt in sorted(by_pref.items(), key=lambda x: -x[1])[:10]:
        print(f"  {pref}: {cnt:,}件")

    # 従業者数分布
    emp_counts = []
    for r in all_results:
        if r.get('従業者_合計'):
            try:
                emp_counts.append(int(r['従業者_合計']))
            except ValueError:
                pass
    if emp_counts:
        print(f"\n従業者数分布 (N={len(emp_counts):,}):")
        ranges = [(1, 10), (11, 30), (31, 50), (51, 100), (101, 150), (151, 300), (301, 9999)]
        labels = ['1-10', '11-30', '31-50', '51-100', '101-150', '151-300', '301+']
        for (lo, hi), label in zip(ranges, labels):
            cnt = sum(1 for e in emp_counts if lo <= e <= hi)
            print(f"    {label}人: {cnt:,}件 ({cnt/len(emp_counts)*100:.1f}%)")


def main():
    parser = argparse.ArgumentParser(description='介護サービス情報公表システム 全国スクレイピング')
    parser.add_argument('--pref', type=str, help='都道府県コード（カンマ区切り）例: 13,14,27')
    parser.add_argument('--service', type=str, help='サービスコード（カンマ区切り）例: 150,510')
    args = parser.parse_args()

    # ディレクトリ作成
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    SERVICE_DIR.mkdir(parents=True, exist_ok=True)
    PROGRESS_DIR.mkdir(parents=True, exist_ok=True)

    # フィルタ解析
    target_prefs = None
    if args.pref:
        target_prefs = [p.strip() for p in args.pref.split(',')]
        print(f"対象都道府県: {[PREF_NAMES.get(int(p), p) for p in target_prefs]}")

    target_services = list(ACTION_CODE_MAP.keys())
    if args.service:
        target_services = [int(s.strip()) for s in args.service.split(',')]
        print(f"対象サービス: {[SERVICE_NAMES.get(s, s) for s in target_services]}")

    print("=" * 60)
    print("介護サービス情報公表システム 全国スクレイピング")
    print(f"実行日: {TODAY_ISO}")
    print(f"対象サービス数: {len(target_services)}")
    print(f"リクエスト間隔: {REQUEST_DELAY}秒")
    print("=" * 60)

    # 全サービスを処理
    all_results = []
    for service_code in target_services:
        results = process_service(service_code, target_prefs)
        all_results.extend(results)

    # 統合CSV保存
    if all_results:
        output_file = OUTPUT_DIR / f"kaigo_nationwide_{TODAY}.csv"
        df = pd.DataFrame(all_results, columns=OUTPUT_COLUMNS)
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"\n統合CSV: {output_file} ({len(all_results):,}件)")

    print_final_summary(all_results)

    print(f"\n完了")


if __name__ == '__main__':
    main()
