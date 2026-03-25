"""
介護サービス情報公表システム 全国スクレイピング（全ページ対応版）
================================================================
全都道府県×全サービス種別の4ページ（概要・詳細・運営・その他）から
リスト用+BI用の全データを取得する。

テーブル番号固定ではなくキーワードベースで検出するため、
サービス種別間の構造差異やテーブル数の揺れ（運営12 or 13等）に対応。

使い方:
  # 全件実行
  python scripts/scrape_kaigo_full.py

  # 特定都道府県
  python scripts/scrape_kaigo_full.py --pref 13

  # 特定サービス
  python scripts/scrape_kaigo_full.py --service 150,510

  # リスト用データのみ（高速、概要+詳細の2ページ）
  python scripts/scrape_kaigo_full.py --mode list

  # BIデータ含む全ページ（概要+詳細+運営+その他の4ページ）
  python scripts/scrape_kaigo_full.py --mode full

出力:
  data/output/kaigo_scraping/kaigo_full_{date}.csv
  data/output/kaigo_scraping/by_service/{code}_{name}.csv
"""

import sys
import os
import io
import json
import time
import re
import argparse
import traceback
from pathlib import Path
from datetime import datetime
from collections import defaultdict

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

REQUEST_DELAY = 1.5
SAVE_INTERVAL = 50
PROGRESS_INTERVAL = 25
MAX_RETRIES = 3
RETRY_DELAY = 5
CONSECUTIVE_ERROR_LIMIT = 10

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'ja,en-US;q=0.7,en;q=0.3',
}

# =====================================================================
# action_code・サービス名マッピング
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

# =====================================================================
# 出力カラム定義
# =====================================================================
# リスト用（架電リスト生成に必要な基本情報）
LIST_COLUMNS = [
    '事業所番号', 'サービスコード', 'サービス名', '都道府県コード', '都道府県名',
    '事業所名', '管理者名', '管理者職名',
    '代表者名', '代表者職名', '法人名', '法人番号',
    '電話番号', 'FAX番号', '住所', 'HP',
    '従業者_常勤', '従業者_非常勤', '従業者_合計',
    '定員', '事業開始日', '前年度採用数', '前年度退職数',
]

# BI用（経営分析に必要な追加情報）
BI_COLUMNS = [
    # 概要ページ
    '利用者総数', '利用者_都道府県平均', '経験10年以上割合', 'サービス提供地域',
    '要介護1', '要介護2', '要介護3', '要介護4', '要介護5',
    # 運営ページ - 加算
    '加算_処遇改善I', '加算_処遇改善II', '加算_処遇改善III', '加算_処遇改善IV',
    '加算_特定事業所I', '加算_特定事業所II', '加算_特定事業所III',
    '加算_特定事業所IV', '加算_特定事業所V',
    '加算_認知症ケアI', '加算_認知症ケアII',
    '加算_口腔連携', '加算_緊急時',
    # 運営ページ - 全加算（動的抽出、JSON形式）
    '加算_全項目',
    # 運営ページ - 運営品質
    '品質_BCP策定', '品質_ICT活用', '品質_第三者評価', '品質_損害賠償保険',
    # 運営ページ - 財務
    '会計種類', '財務DL_事業活動計算書', '財務DL_資金収支計算書', '財務DL_貸借対照表',
    # その他ページ - 賃金
    '賃金_職種1', '賃金_月額1', '賃金_平均年齢1', '賃金_平均勤続1',
    '賃金_職種2', '賃金_月額2', '賃金_平均年齢2', '賃金_平均勤続2',
    '賃金_職種3', '賃金_月額3', '賃金_平均年齢3', '賃金_平均勤続3',
    '賃金_職種4', '賃金_月額4', '賃金_平均年齢4', '賃金_平均勤続4',
    '賃金_職種5', '賃金_月額5', '賃金_平均年齢5', '賃金_平均勤続5',
    # その他ページ - 行政処分
    '行政処分日', '行政処分内容', '行政指導日', '行政指導内容',
]

META_COLUMNS = ['スクレイピング日']


def get_all_columns(mode):
    if mode == 'list':
        return LIST_COLUMNS + META_COLUMNS
    return LIST_COLUMNS + BI_COLUMNS + META_COLUMNS


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
    """キーワードでテーブルを検索（テーブル番号に依存しない）"""
    if isinstance(keywords, str):
        keywords = [keywords]
    exclude_keywords = exclude_keywords or []

    for i, table in enumerate(tables):
        text = table.get_text()
        if all(kw in text for kw in keywords):
            if not any(ekw in text for ekw in exclude_keywords):
                return i, table
    return -1, None


def find_all_tables_by_keyword(tables, keywords):
    """キーワードに一致する全テーブルを返す"""
    if isinstance(keywords, str):
        keywords = [keywords]
    results = []
    for i, table in enumerate(tables):
        text = table.get_text()
        if all(kw in text for kw in keywords):
            results.append((i, table))
    return results


def extract_row_value(rows, keyword, td_index=0):
    """テーブル行からキーワードに一致する行のtd値を取得"""
    for row in rows:
        ths = [th.get_text(strip=True) for th in row.find_all('th')]
        tds = [td.get_text(strip=True) for td in row.find_all('td')]
        if any(keyword in th for th in ths):
            if td_index < len(tds):
                return tds[td_index]
    return ''


def fetch_page(session, url):
    """ページ取得（リトライ付き）"""
    for attempt in range(MAX_RETRIES):
        try:
            resp = session.get(url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
            else:
                raise


# =====================================================================
# ページパーサー（キーワードベース）
# =====================================================================
def parse_kihon(html, result):
    """詳細ページ（kihon）のパース"""
    soup = BeautifulSoup(html, 'html.parser')
    tables = soup.find_all('table')
    if len(tables) < 5:
        return False

    # --- 法人情報テーブル（代表者名、法人番号）---
    idx, t_corp = find_table_by_keyword(tables, ['法人等の名称', '主たる事務所'])
    if t_corp:
        rows = t_corp.find_all('tr')
        for j, row in enumerate(rows):
            ths = [th.get_text(strip=True) for th in row.find_all('th')]
            tds = [td.get_text(strip=True) for td in row.find_all('td')]
            all_text = ''.join(ths) + ''.join(tds)

            if j == 4 and len(tds) == 1 and tds[0] and not ths:
                result['法人名'] = tds[0]
            if '法人番号' in ''.join(ths) and '有無' not in ''.join(ths):
                for t in tds:
                    if re.match(r'^\d{13}$', t):
                        result['法人番号'] = t
                        break
            if '代表者' in all_text and '氏名' in ''.join(ths) and tds:
                result['代表者名'] = tds[0]
            if result['代表者名'] and '職名' in ''.join(ths) and '氏名' not in ''.join(ths) and not result['代表者職名'] and tds:
                result['代表者職名'] = tds[0]

    # --- 事業所詳細テーブル（管理者名、電話番号、住所）---
    idx, t_detail = find_table_by_keyword(tables, ['事業所の名称', '管理者の氏名'])
    if t_detail:
        rows = t_detail.find_all('tr')
        _postal, _address, _building = '', '', ''

        for j, row in enumerate(rows):
            ths = [th.get_text(strip=True) for th in row.find_all('th')]
            tds = [td.get_text(strip=True) for td in row.find_all('td')]
            th_text = ''.join(ths)

            if j == 2 and tds and not ths:
                result['事業所名'] = tds[0]
            if '所在地' in th_text and j <= 5:
                for t in tds:
                    if t.startswith('〒'):
                        _postal = t
                        break
            if '都道府県' in th_text and tds:
                _address = tds[0]
            if '建物' in th_text and tds and tds[0]:
                _building = tds[0]
            if '電話番号' in th_text and 'FAX' not in th_text and 'ＦＡＸ' not in th_text:
                for t in tds:
                    if re.search(r'\d', t):
                        result['電話番号'] = t
                        break
            if 'FAX' in th_text or 'ＦＡＸ' in th_text:
                for t in tds:
                    if re.search(r'\d', t):
                        result['FAX番号'] = t
                        break
            if 'ホームページ' in th_text:
                link = row.find('a')
                if link and link.get('href'):
                    result['HP'] = link['href']
                else:
                    for t in tds:
                        if t.startswith('http'):
                            result['HP'] = t
                            break
            if '管理者' in th_text and '氏名' in th_text and tds:
                result['管理者名'] = tds[0]
            if result['管理者名'] and '職名' in th_text and '氏名' not in th_text and '管理者' not in th_text and not result['管理者職名'] and tds:
                result['管理者職名'] = tds[0]
            if '開始' in th_text and '予定' in th_text:
                for t in tds:
                    if re.match(r'\d{4}/\d{2}/\d{2}', t):
                        result['事業開始日'] = t
                        break

        parts = [p for p in [_postal, _address, _building] if p]
        if parts:
            result['住所'] = ' '.join(parts)

    # --- 従業者テーブル ---
    idx, t_emp = find_table_by_keyword(tables, ['従業者の数', '常勤'], exclude_keywords=['賃金'])
    if not t_emp:
        idx, t_emp = find_table_by_keyword(tables, ['実人数', '常勤'])
    if t_emp:
        rows = t_emp.find_all('tr')
        total_jokin, total_hijokin, total_all = 0, 0, 0
        emp_found = False
        total_hire, total_retire = 0, 0

        for row in rows:
            cells = row.find_all(['th', 'td'])
            texts = [c.get_text(strip=True) for c in cells]

            # 7セルパターン
            if len(texts) == 7 and '人' in texts[5]:
                if texts[0].startswith('（うち') or texts[0].startswith('うち'):
                    continue
                jokin = int(parse_num(texts[1]) or '0') + int(parse_num(texts[2]) or '0')
                hijokin = int(parse_num(texts[3]) or '0') + int(parse_num(texts[4]) or '0')
                goukei = int(parse_num(texts[5]) or '0')
                total_jokin += jokin
                total_hijokin += hijokin
                total_all += goukei
                emp_found = True
            # 6セルパターン
            elif len(texts) == 6 and '人' in texts[4]:
                if texts[0].startswith('（うち') or texts[0].startswith('うち'):
                    continue
                jokin = int(parse_num(texts[1]) or '0') + int(parse_num(texts[2]) or '0')
                hijokin = int(parse_num(texts[3]) or '0')
                goukei = int(parse_num(texts[4]) or '0')
                total_jokin += jokin
                total_hijokin += hijokin
                total_all += goukei
                emp_found = True

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

    # --- 定員テーブル ---
    idx, t_cap = find_table_by_keyword(tables, ['運営に関する方針'])
    if t_cap:
        for row in t_cap.find_all('tr'):
            texts = [c.get_text(strip=True) for c in row.find_all(['th', 'td'])]
            joined = ''.join(texts)
            if ('定員' in joined or '入所' in joined) and '人' in joined:
                for t in texts:
                    num = parse_num(t)
                    if num and int(num) > 0 and '定員' not in t:
                        result['定員'] = num
                        break
                if result.get('定員'):
                    break

    # --- 加算情報（kihonページ内のテーブルから検出）---
    # 既存13項目のマッピング（kihon/uneiどちらにあっても対応）
    # 既存13項目のマッピング（部分一致パターン）
    # サービス種別によって「介護職員処遇改善加算」「介護職員等処遇改善加算」など揺れがある
    # 注意: 「特定処遇改善加算」は別物なので除外
    kasan_patterns = [
        (re.compile(r'(?<!特定)処遇改善加算[^（(]*[（(]Ⅰ[）)]'), '加算_処遇改善I'),
        (re.compile(r'(?<!特定)処遇改善加算[^（(]*[（(]Ⅱ[）)]'), '加算_処遇改善II'),
        (re.compile(r'(?<!特定)処遇改善加算[^（(]*[（(]Ⅲ[）)]'), '加算_処遇改善III'),
        (re.compile(r'(?<!特定)処遇改善加算[^（(]*[（(]Ⅳ[）)]'), '加算_処遇改善IV'),
        (re.compile(r'特定事業所加算.*[（(]Ⅰ[）)]'), '加算_特定事業所I'),
        (re.compile(r'特定事業所加算.*[（(]Ⅱ[）)]'), '加算_特定事業所II'),
        (re.compile(r'特定事業所加算.*[（(]Ⅲ[）)]'), '加算_特定事業所III'),
        (re.compile(r'特定事業所加算.*[（(]Ⅳ[）)]'), '加算_特定事業所IV'),
        (re.compile(r'特定事業所加算.*[（(]Ⅴ[）)]'), '加算_特定事業所V'),
        (re.compile(r'認知症専門ケア加算.*[（(]Ⅰ[）)]'), '加算_認知症ケアI'),
        (re.compile(r'認知症専門ケア加算.*[（(]Ⅱ[）)]'), '加算_認知症ケアII'),
        (re.compile(r'口腔連携強化加算'), '加算_口腔連携'),
        (re.compile(r'緊急時訪問介護加算'), '加算_緊急時'),
    ]

    # 全加算の動的抽出 + 既存13項目の検出
    all_kasan = {}
    for table in tables:
        table_text = table.get_text()
        if '加算' not in table_text:
            continue
        rows = table.find_all('tr')
        for row in rows:
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

            # 値の検出
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
                img_no = cell.find('img', alt=re.compile(r'なし|バツ'))
                if img_no:
                    value = '×'
                    break
            if has_check:
                value = '○'

            if kasan_name and len(kasan_name) > 2:
                all_kasan[kasan_name] = value

                # 既存13項目へのマッピング（正規表現パターンで柔軟にマッチ）
                for pattern, col_name in kasan_patterns:
                    if pattern.search(kasan_name):
                        result[col_name] = '○' if has_check else ''
                        break

    result['加算_全項目'] = json.dumps(all_kasan, ensure_ascii=False) if all_kasan else ''

    return True


def parse_kani(html, result):
    """概要ページ（kani）のパース"""
    soup = BeautifulSoup(html, 'html.parser')
    tables = soup.find_all('table')

    # 利用者総数
    for table in tables:
        text = table.get_text()
        if '利用者総数' in text or '入所者数' in text:
            rows = table.find_all('tr')
            for row in rows:
                texts = [c.get_text(strip=True) for c in row.find_all(['th', 'td'])]
                joined = ''.join(texts)
                if '利用者総数' in joined or '入所者数' in joined:
                    # 「37人＜30.2人＞」のようなパターン
                    for t in texts:
                        m = re.match(r'(\d+)人', t)
                        if m:
                            result['利用者総数'] = m.group(1)
                        m2 = re.search(r'＜([\d.]+)人＞', t)
                        if m2:
                            result['利用者_都道府県平均'] = m2.group(1)
                    break

    # 要介護度別
    for table in tables:
        text = table.get_text()
        if '要介護' in text and '要介護度別' not in text:
            rows = table.find_all('tr')
            for row in rows:
                texts = [c.get_text(strip=True) for c in row.find_all(['th', 'td'])]
                for i, t in enumerate(texts):
                    for level in ['要介護１', '要介護２', '要介護３', '要介護４', '要介護５']:
                        if level in t and i + 1 < len(texts):
                            key = f'要介護{level[-1]}'
                            result[key] = parse_num(texts[i + 1])

    # 経験年数10年以上割合
    for table in tables:
        text = table.get_text()
        if '経験年数' in text and '１０年' in text:
            rows = table.find_all('tr')
            for row in rows:
                texts = [c.get_text(strip=True) for c in row.find_all(['th', 'td'])]
                joined = ''.join(texts)
                if '１０年以上' in joined:
                    for t in texts:
                        if '％' in t or '%' in t:
                            result['経験10年以上割合'] = t
                            break

    # サービス提供地域
    for table in tables:
        text = table.get_text()
        if 'サービス提供地域' in text:
            rows = table.find_all('tr')
            for row in rows:
                texts = [c.get_text(strip=True) for c in row.find_all(['th', 'td'])]
                if 'サービス提供地域' in ''.join(texts):
                    for t in texts:
                        if t and 'サービス提供地域' not in t:
                            result['サービス提供地域'] = t[:200]
                            break

    return True


def parse_unei(html, result):
    """運営ページ（unei）のパース"""
    soup = BeautifulSoup(html, 'html.parser')
    tables = soup.find_all('table')

    # --- 加算情報（キーワードで検出）---
    kasan_map = {
        '介護職員等処遇改善加算（Ⅰ）': '加算_処遇改善I',
        '介護職員等処遇改善加算（Ⅱ）': '加算_処遇改善II',
        '介護職員等処遇改善加算（Ⅲ）': '加算_処遇改善III',
        '介護職員等処遇改善加算（Ⅳ）': '加算_処遇改善IV',
        '特定事業所加算(Ⅰ)': '加算_特定事業所I',
        '特定事業所加算(Ⅱ)': '加算_特定事業所II',
        '特定事業所加算(Ⅲ)': '加算_特定事業所III',
        '特定事業所加算(Ⅳ)': '加算_特定事業所IV',
        '特定事業所加算(Ⅴ)': '加算_特定事業所V',
        '認知症専門ケア加算（Ⅰ）': '加算_認知症ケアI',
        '認知症専門ケア加算（Ⅱ）': '加算_認知症ケアII',
        '口腔連携強化加算': '加算_口腔連携',
        '緊急時訪問介護加算': '加算_緊急時',
    }

    # 加算テーブルを探す（複数テーブルにまたがる可能性）
    for table in tables:
        text = table.get_text()
        if '加算' not in text:
            continue
        rows = table.find_all('tr')
        for row in rows:
            cells = row.find_all(['th', 'td'])
            texts = [c.get_text(strip=True) for c in cells]
            row_text = ''.join(texts)
            for kasan_keyword, col_name in kasan_map.items():
                if kasan_keyword in row_text:
                    # チェックマークの検出（○、あり、✓等）
                    has_check = False
                    for cell in cells:
                        cell_text = cell.get_text(strip=True)
                        if cell_text in ('○', 'あり', '✓', '✔', '●'):
                            has_check = True
                        # input[checked]の検出
                        inp = cell.find('input', {'checked': True})
                        if inp:
                            has_check = True
                        # imgの検出（チェックアイコン）
                        img = cell.find('img', alt=re.compile(r'チェック|あり'))
                        if img:
                            has_check = True
                    result[col_name] = '○' if has_check else ''

    # --- 品質チェック項目 ---
    for table in tables:
        text = table.get_text()
        # BCP
        if 'BCP' in text or '業務継続計画' in text:
            result['品質_BCP策定'] = '○'
        # ICT
        if '介護ロボット' in text or 'ICT' in text:
            for row in table.find_all('tr'):
                row_text = row.get_text()
                if ('介護ロボット' in row_text or 'ICT' in row_text) and ('勤務' in row_text or '活用' in row_text):
                    result['品質_ICT活用'] = '○'
                    break
        # 第三者評価
        if '第三者評価' in text or '第三者による評価' in text:
            result['品質_第三者評価'] = '○'
        # 損害賠償
        if '損害賠償保険' in text:
            for row in table.find_all('tr'):
                if '損害賠償' in row.get_text() and '加入' in row.get_text():
                    result['品質_損害賠償保険'] = '○'
                    break

    # --- 財務情報・DLリンク ---
    for table in tables:
        text = table.get_text()
        if '会計の種類' in text or '財務状況' in text:
            rows = table.find_all('tr')
            for row in rows:
                row_text = row.get_text(strip=True)
                if '会計の種類' in row_text:
                    tds = [td.get_text(strip=True) for td in row.find_all('td')]
                    if tds:
                        result['会計種類'] = tds[0] if tds[0] != '-' else ''

            # DLリンク検出
            links = table.find_all('a')
            for link in links:
                href = link.get('href', '')
                text = link.get_text(strip=True)
                parent_text = link.parent.get_text(strip=True) if link.parent else ''
                full_text = parent_text + text

                if href and ('download' in href.lower() or 'ダウンロード' in text):
                    if '事業活動' in full_text or '損益' in full_text:
                        result['財務DL_事業活動計算書'] = href
                    elif '資金収支' in full_text or 'キャッシュ' in full_text:
                        result['財務DL_資金収支計算書'] = href
                    elif '貸借' in full_text or 'バランス' in full_text:
                        result['財務DL_貸借対照表'] = href

    # --- 全加算項目の動的抽出（uneページにも加算がある場合の補完） ---
    # kihonページで既に取得済みの場合は上書きしない
    existing_kasan = result.get('加算_全項目', '')
    all_kasan = {}
    if existing_kasan:
        try:
            all_kasan = json.loads(existing_kasan)
        except (ValueError, TypeError):
            pass

    for table in tables:
        table_text = table.get_text()
        if '加算' not in table_text:
            continue
        rows = table.find_all('tr')
        for row in rows:
            ths = row.find_all('th')
            tds = row.find_all('td')
            if not ths or not tds:
                continue
            kasan_name = ths[-1].get_text(strip=True)
            if '加算' not in kasan_name:
                continue
            if kasan_name in ('加算状況', '介護報酬の加算状況', '加算の状況'):
                continue
            if '加算状況' in kasan_name and len(kasan_name) > 20:
                continue
            # 既にkihonで取得済みの場合はスキップ
            if kasan_name in all_kasan:
                continue
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
                img_no = cell.find('img', alt=re.compile(r'なし|バツ'))
                if img_no:
                    value = '×'
                    break
            if has_check:
                value = '○'
            if kasan_name and len(kasan_name) > 2:
                all_kasan[kasan_name] = value

    result['加算_全項目'] = json.dumps(all_kasan, ensure_ascii=False) if all_kasan else ''

    return True


def parse_original(html, result):
    """その他ページ（original）のパース"""
    soup = BeautifulSoup(html, 'html.parser')
    tables = soup.find_all('table')

    # --- 賃金データ（テーブル2〜6に5職種分）---
    wage_idx = 0
    for table in tables:
        text = table.get_text()
        if '一人当たりの賃金' not in text and '平均勤続' not in text:
            continue

        rows = table.find_all('tr')
        data = {}
        for row in rows:
            cells = row.find_all(['th', 'td'])
            texts = [c.get_text(strip=True) for c in cells]
            if len(texts) >= 2:
                key = texts[0]
                val = texts[1] if texts[1] != '-' else ''
                if '具体的な職種' in key:
                    data['職種'] = val
                elif '一人当たりの賃金' in key:
                    data['賃金'] = val
                elif '平均年齢' in key:
                    data['年齢'] = val
                elif '平均勤続年数' in key:
                    data['勤続'] = val

        if data.get('職種') or data.get('賃金'):
            wage_idx += 1
            if wage_idx <= 5:
                result[f'賃金_職種{wage_idx}'] = data.get('職種', '')
                result[f'賃金_月額{wage_idx}'] = data.get('賃金', '')
                result[f'賃金_平均年齢{wage_idx}'] = data.get('年齢', '')
                result[f'賃金_平均勤続{wage_idx}'] = data.get('勤続', '')

    # --- 行政処分・指導情報 ---
    for table in tables:
        text = table.get_text()
        if '処分' not in text and '指導' not in text:
            continue
        rows = table.find_all('tr')
        for row in rows:
            texts = [c.get_text(strip=True) for c in row.find_all(['th', 'td'])]
            if len(texts) >= 2:
                key = texts[0]
                val = texts[1] if texts[1] != '-' else ''
                if '処分が行われた日' in key:
                    result['行政処分日'] = val
                elif '当該処分の内容' in key:
                    result['行政処分内容'] = val[:200]
                elif '行政指導' in key and '日' in key:
                    result['行政指導日'] = val
                elif '当該行政指導の内容' in key:
                    result['行政指導内容'] = val[:200]

    return True


# =====================================================================
# 統合スクレイピング
# =====================================================================
def build_url(pref_code, action_code, page_key, jigyosyo_cd, service_code):
    base = f"https://www.kaigokensaku.mhlw.go.jp/{pref_code}/index.php"
    if page_key == 'original':
        return f"{base}?action_kouhyou_detail_original_index=true&JigyosyoCd={jigyosyo_cd}-00&ServiceCd={service_code}"
    suffix_map = {'kani': '_kani', 'kihon': '_kihon', 'unei': '_unei'}
    suffix = suffix_map[page_key]
    return f"{base}?action_kouhyou_detail_{action_code}{suffix}=true&JigyosyoCd={jigyosyo_cd}-00&ServiceCd={service_code}"


def scrape_facility(pref_code, facility_code, service_code, action_code, session, mode):
    """1施設の全ページをスクレイピング"""
    columns = get_all_columns(mode)
    result = {col: '' for col in columns}
    result['事業所番号'] = facility_code
    result['サービスコード'] = str(service_code)
    result['サービス名'] = SERVICE_NAMES.get(service_code, '')
    result['都道府県コード'] = str(pref_code)
    result['都道府県名'] = PREF_NAMES.get(int(pref_code), '')
    result['スクレイピング日'] = TODAY_ISO

    pages_to_fetch = ['kihon']
    if mode == 'full':
        pages_to_fetch = ['kihon', 'kani', 'unei', 'original']

    parsers = {
        'kihon': parse_kihon,
        'kani': parse_kani,
        'unei': parse_unei,
        'original': parse_original,
    }

    success = False
    for page_key in pages_to_fetch:
        try:
            url = build_url(pref_code, action_code, page_key, facility_code, service_code)
            html = fetch_page(session, url)
            parser = parsers[page_key]
            if parser(html, result):
                success = True
            time.sleep(REQUEST_DELAY)
        except Exception as e:
            # 個別ページの失敗は許容（他ページのデータは保持）
            pass

    return result if success else None


# =====================================================================
# CSVダウンロード
# =====================================================================
def download_open_data_csv(service_code):
    url = f"https://www.mhlw.go.jp/content/12300000/jigyosho_{service_code}.csv"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=60)
        resp.raise_for_status()
        text = resp.content.decode('utf-8-sig')
        return pd.read_csv(io.StringIO(text), dtype=str, low_memory=False)
    except Exception as e:
        print(f"  CSV取得エラー（{service_code}）: {e}")
        return None


# =====================================================================
# メインロジック
# =====================================================================
def process_service(service_code, target_prefs, mode, limit=0):
    service_name = SERVICE_NAMES.get(service_code, f'不明_{service_code}')
    action_code = ACTION_CODE_MAP.get(service_code)
    if not action_code:
        return []

    print(f"\n{'='*60}")
    print(f"サービス: {service_code} ({service_name}) [mode={mode}]")
    print(f"{'='*60}")

    df = download_open_data_csv(service_code)
    if df is None or len(df) == 0:
        return []
    print(f"  全国施設数: {len(df):,}件")

    if target_prefs:
        pref_names_filter = [PREF_NAMES.get(int(p), '') for p in target_prefs]
        df = df[df['都道府県名'].isin(pref_names_filter)]
        print(f"  フィルタ後: {len(df):,}件")
    if len(df) == 0:
        return []

    df['_pref_code'] = df['事業所番号'].astype(str).str[:2]

    # 進捗ファイル
    progress_file = PROGRESS_DIR / f"progress_{mode}_{service_code}.csv"
    columns = get_all_columns(mode)
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

    session = requests.Session()
    remaining = [(idx, row) for idx, row in df.iterrows()
                 if str(row['事業所番号']).strip() not in already_done]
    if not remaining:
        print(f"  全件取得済み")
        return results

    # テスト用のlimit制限
    if limit and limit > 0:
        remaining = remaining[:limit]
        print(f"  --limit {limit} 適用: {len(remaining)}件に制限")

    pages_per_facility = 4 if mode == 'full' else 1
    print(f"  取得対象: {len(remaining):,}件 × {pages_per_facility}ページ")
    est_min = len(remaining) * pages_per_facility * REQUEST_DELAY / 60
    print(f"  推定所要時間: {est_min:.0f}分")

    start_time = time.time()
    errors = []
    consecutive_errors = 0
    bot_stopped = False

    for count, (idx, row) in enumerate(remaining, 1):
        code = str(row['事業所番号']).strip()
        pref_code = str(row.get('_pref_code', '13')).strip()

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
            result = scrape_facility(pref_code, code, service_code, action_code, session, mode)
            if result is None:
                result = {col: '' for col in columns}
                result['事業所番号'] = code
                result['サービスコード'] = str(service_code)
                result['サービス名'] = service_name
                result['都道府県コード'] = pref_code
                result['都道府県名'] = PREF_NAMES.get(int(pref_code), '')
                result['スクレイピング日'] = TODAY_ISO
                errors.append(code)
                consecutive_errors += 1
            else:
                consecutive_errors = 0

            for key in csv_info:
                if not result.get(key) and csv_info[key]:
                    result[key] = csv_info[key]
            results.append(result)

        except Exception as e:
            result = {col: '' for col in columns}
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
                print(f"  ⚠ 接続エラー [{code}] - 10秒待機（連続{consecutive_errors}回）")
                time.sleep(10)

        # 連続エラーストップ
        if consecutive_errors >= CONSECUTIVE_ERROR_LIMIT:
            print(f"\n  🛑 連続{CONSECUTIVE_ERROR_LIMIT}回エラー - Bot認知の可能性あり")
            print(f"  取得済み{len(results)}件を保存して中断します")
            print(f"  再開: 同じコマンドを再実行してください")
            bot_stopped = True
            save_df = pd.DataFrame(results, columns=columns)
            save_df.to_csv(progress_file, index=False, encoding='utf-8-sig')
            break

        if count % PROGRESS_INTERVAL == 0 or count == len(remaining):
            elapsed = time.time() - start_time
            rate = count / elapsed if elapsed > 0 else 0
            eta = (len(remaining) - count) / rate if rate > 0 else 0
            name = result.get('事業所名', '')[:20] if result else ''
            print(f"  [{count}/{len(remaining)}] {count/len(remaining)*100:.1f}% "
                  f"| {rate:.1f}件/秒 | 残り{eta/60:.0f}分 | {name}")

        if count % SAVE_INTERVAL == 0:
            save_df = pd.DataFrame(results, columns=columns)
            save_df.to_csv(progress_file, index=False, encoding='utf-8-sig')

    if bot_stopped:
        return results

    # 完了 → サービス別CSV保存
    service_file = SERVICE_DIR / f"{service_code}_{service_name}.csv"
    save_df = pd.DataFrame(results, columns=columns)
    save_df.to_csv(service_file, index=False, encoding='utf-8-sig')
    print(f"  保存: {service_file.name} ({len(results):,}件)")

    if progress_file.exists():
        progress_file.unlink()

    if errors:
        print(f"  エラー: {len(errors)}件")

    return results


def print_final_summary(all_results, mode):
    columns = get_all_columns(mode)
    print("\n" + "=" * 60)
    print("最終サマリー")
    print("=" * 60)

    total = len(all_results)
    if total == 0:
        print("結果なし")
        return

    checks = {
        '管理者名': 'リスト', '代表者名': 'リスト', '従業者_合計': 'リスト',
        '電話番号': 'リスト', '法人番号': 'リスト',
    }
    if mode == 'full':
        checks.update({
            '利用者総数': 'BI', '加算_処遇改善I': 'BI',
            '賃金_職種1': 'BI', '会計種類': 'BI',
        })

    for field, category in checks.items():
        if field in columns:
            cnt = sum(1 for r in all_results if r.get(field))
            print(f"  [{category}] {field}: {cnt:,}/{total} ({cnt/total*100:.1f}%)")

    # サービス別
    by_service = defaultdict(int)
    for r in all_results:
        by_service[r.get('サービス名', '不明')] += 1
    print("\nサービス別件数:")
    for svc, cnt in sorted(by_service.items(), key=lambda x: -x[1])[:10]:
        print(f"  {svc}: {cnt:,}件")

    # 従業者数分布
    emp_counts = [int(r['従業者_合計']) for r in all_results
                  if r.get('従業者_合計') and r['従業者_合計'].isdigit()]
    if emp_counts:
        print(f"\n従業者数分布 (N={len(emp_counts):,}):")
        for lo, hi, label in [(1,10,'1-10'), (11,30,'11-30'), (31,50,'31-50'),
                              (51,100,'51-100'), (101,150,'101-150'), (151,9999,'151+')]:
            cnt = sum(1 for e in emp_counts if lo <= e <= hi)
            print(f"    {label}人: {cnt:,}件 ({cnt/len(emp_counts)*100:.1f}%)")


def main():
    parser = argparse.ArgumentParser(description='介護情報公表 全国スクレイピング（全ページ対応版）')
    parser.add_argument('--pref', type=str, help='都道府県コード（カンマ区切り）')
    parser.add_argument('--service', type=str, help='サービスコード（カンマ区切り）')
    parser.add_argument('--mode', type=str, default='full', choices=['list', 'full'],
                        help='list=架電リスト用（詳細のみ）, full=BI含む全ページ（デフォルト）')
    parser.add_argument('--limit', type=int, default=0,
                        help='取得件数の上限（テスト用、0=無制限）')
    args = parser.parse_args()

    for d in [OUTPUT_DIR, SERVICE_DIR, PROGRESS_DIR]:
        d.mkdir(parents=True, exist_ok=True)

    target_prefs = [p.strip() for p in args.pref.split(',')] if args.pref else None
    target_services = [int(s.strip()) for s in args.service.split(',')] if args.service else list(ACTION_CODE_MAP.keys())

    print("=" * 60)
    print("介護サービス情報公表システム 全国スクレイピング（全ページ対応版）")
    print(f"実行日: {TODAY_ISO}")
    print(f"モード: {args.mode} ({'詳細ページのみ' if args.mode == 'list' else '概要+詳細+運営+その他の4ページ'})")
    print(f"対象サービス数: {len(target_services)}")
    if target_prefs:
        print(f"対象都道府県: {[PREF_NAMES.get(int(p), p) for p in target_prefs]}")
    print("=" * 60)

    all_results = []
    for service_code in target_services:
        results = process_service(service_code, target_prefs, args.mode, limit=args.limit)
        all_results.extend(results)

    if all_results:
        columns = get_all_columns(args.mode)
        output_file = OUTPUT_DIR / f"kaigo_full_{TODAY}.csv"
        df = pd.DataFrame(all_results, columns=columns)
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"\n統合CSV: {output_file} ({len(all_results):,}件)")

    print_final_summary(all_results, args.mode)
    print(f"\n完了")


if __name__ == '__main__':
    main()
