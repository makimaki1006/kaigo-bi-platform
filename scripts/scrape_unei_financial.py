"""
uneiページHTML取得による財務情報スクレイパー(方式B)

URL直叩きプローブはサーバの不安定応答(206空/負荷時エラー)で精度が出ないため、
uneiページのHTMLを取得し財務リンクを直接読む。判定はhrefのフォルダ名ベースで確実:
  IncomeStatementFile → 事業活動計算書(PL)
  CashFlowFile        → 資金収支計算書(CF)
  BalanceSheetsFile   → 貸借対照表(BS)
同時に会計種類も取得する。

実行方法:
  $env:TURSO_DATABASE_URL / TURSO_AUTH_TOKEN を設定して
  python scripts/scrape_unei_financial.py --pref 31          # 都道府県指定
  python scripts/scrape_unei_financial.py --all              # 全国
  python scripts/scrape_unei_financial.py --pref 31 --fresh  # 進捗リセット

出力: data/output/unei_financial/unei_results.csv (resume対応・追記式)
"""
import argparse
import csv
import logging
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from turso_helpers import execute_single, get_headers, get_turso_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BASE = "https://www.kaigokensaku.mhlw.go.jp"
OUT_DIR = Path(__file__).resolve().parent.parent / "data" / "output" / "unei_financial"
RESULTS_CSV = OUT_DIR / "unei_results.csv"

# サービスコード → action_code（scrape_kaigo_full.py より）
ACTION_CODE_MAP = {
    110: '001', 120: '002', 130: '004', 140: '005', 150: '001',
    160: '003', 170: '006', 210: '007', 220: '008', 230: '009',
    320: '022', 331: '014', 332: '015', 334: '016', 335: '017',
    336: '001', 337: '001', 361: '018', 362: '019', 364: '020',
    410: '010', 430: '023', 510: '011', 520: '012', 530: '013',
    540: '001', 550: '001', 551: '001',
    710: '024', 720: '025', 730: '021', 760: '026', 770: '027', 780: '028',
}

DOC_FOLDERS = {
    "IncomeStatementFile": "財務DL_事業活動計算書",
    "CashFlowFile": "財務DL_資金収支計算書",
    "BalanceSheetsFile": "財務DL_貸借対照表",
}

PARALLEL = 15
PER_THREAD_DELAY = 1.0
_tl = threading.local()


def get_session():
    if not hasattr(_tl, "s"):
        s = requests.Session()
        s.headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) kaigo-bi unei"
        _tl.s = s
    return _tl.s


def scrape_one(jigyosho: str, svc: str) -> dict:
    """uneiページを取得して財務リンク・会計種類を抽出"""
    time.sleep(PER_THREAD_DELAY)
    rec = {
        "jigyosho_number": jigyosho, "service_code": svc,
        "財務DL_事業活動計算書": "", "財務DL_資金収支計算書": "", "財務DL_貸借対照表": "",
        "会計種類": "", "status": "",
    }
    action = ACTION_CODE_MAP.get(int(svc)) if svc.isdigit() else None
    if not action:
        rec["status"] = f"no_action_code({svc})"
        return rec

    url = f"{BASE}/{jigyosho[:2]}/index.php?action_kouhyou_detail_{action}_unei=true&JigyosyoCd={jigyosho}-00&ServiceCd={svc}"

    for attempt in range(3):
        try:
            r = get_session().get(url, timeout=45)
            if r.status_code == 200 and r.content:
                soup = BeautifulSoup(r.content, "html.parser")
                # 財務リンク: hrefのフォルダ名で確実に分類（本文文字列に依存しない）
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    if "/upload/jigyosyofile/" not in href:
                        continue
                    for folder, col in DOC_FOLDERS.items():
                        if folder + "/" in href and not rec[col]:
                            rec[col] = href
                # 会計種類
                for row in soup.find_all("tr"):
                    rt = row.get_text(strip=True)
                    if "会計の種類" in rt:
                        tds = [td.get_text(strip=True) for td in row.find_all("td")]
                        if tds and tds[0] not in ("-", ""):
                            rec["会計種類"] = tds[0]
                        break
                rec["status"] = "ok"
                return rec
            if r.status_code == 404:
                rec["status"] = "404"
                return rec
        except requests.RequestException:
            pass
        time.sleep(2 ** attempt)
    rec["status"] = "error"
    return rec


def load_targets(pref):
    url, token = get_turso_config()
    url = url.replace("libsql://", "https://")
    headers = get_headers(token)
    where = f"WHERE \"事業所番号\" LIKE '{pref}%'" if pref else ""
    targets, offset = [], 0
    while True:
        sql = f'SELECT "事業所番号", "サービスコード" FROM facilities {where} LIMIT 5000 OFFSET {offset}'
        result = execute_single(url, headers, sql, timeout=300)
        rows = result["results"][0]["response"]["result"]["rows"]
        if not rows:
            break
        targets.extend((r[0]["value"], r[1]["value"]) for r in rows)
        offset += 5000
        if len(rows) < 5000:
            break
    return targets


def load_done():
    if not RESULTS_CSV.exists():
        return set()
    # 書き込みがutf-8-sig(BOM付き)なので読みも合わせる。でないと先頭カラム名にBOMが付く
    with open(RESULTS_CSV, encoding="utf-8-sig") as f:
        return {f"{r['jigyosho_number']}_{r['service_code']}" for r in csv.DictReader(f)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pref")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--fresh", action="store_true")
    args = ap.parse_args()
    if not args.pref and not args.all:
        logger.error("--pref XX または --all を指定"); sys.exit(1)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    if args.fresh and RESULTS_CSV.exists():
        RESULTS_CSV.unlink()

    targets = load_targets(args.pref)
    done = load_done()
    remaining = [(j, s) for j, s in targets if f"{j}_{s}" not in done]
    logger.info("対象 %s / 済み %s / 残り %s", len(targets), len(done), len(remaining))
    if not remaining:
        logger.info("完了済み"); return

    fields = ["jigyosho_number", "service_code", "財務DL_事業活動計算書",
              "財務DL_資金収支計算書", "財務DL_貸借対照表", "会計種類", "status"]
    write_header = not RESULTS_CSV.exists()
    lock = threading.Lock()
    stats = {"done": 0, "pl": 0, "bs": 0, "cf": 0, "acct": 0, "err": 0}
    start = time.time()

    # 追記モードで毎回BOMが混入するのを避けるためBOMなしutf-8で統一
    with open(RESULTS_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        if write_header:
            w.writeheader()
        with ThreadPoolExecutor(max_workers=PARALLEL) as pool:
            futures = [pool.submit(scrape_one, j, s) for j, s in remaining]
            for fut in as_completed(futures):
                rec = fut.result()
                with lock:
                    w.writerow(rec); f.flush()
                    stats["done"] += 1
                    if rec["財務DL_事業活動計算書"]: stats["pl"] += 1
                    if rec["財務DL_貸借対照表"]: stats["bs"] += 1
                    if rec["財務DL_資金収支計算書"]: stats["cf"] += 1
                    if rec["会計種類"]: stats["acct"] += 1
                    if rec["status"] in ("error", "404") and rec["status"] == "error": stats["err"] += 1
                    if stats["done"] % 200 == 0:
                        el = time.time() - start
                        rate = stats["done"] / el if el else 0
                        eta = (len(remaining) - stats["done"]) / rate / 3600 if rate else 0
                        logger.info("進捗 %s/%s (%.1f/s, 残り%.1fh) PL:%s BS:%s CF:%s 会計:%s err:%s",
                                    stats["done"], len(remaining), rate, eta,
                                    stats["pl"], stats["bs"], stats["cf"], stats["acct"], stats["err"])

    d = stats["done"]
    logger.info("=== 完了: %s施設 ===", d)
    logger.info("PL %s (%.1f%%) / BS %s (%.1f%%) / CF %s (%.1f%%) / 会計種類 %s / エラー %s",
                stats["pl"], stats["pl"]/d*100, stats["bs"], stats["bs"]/d*100,
                stats["cf"], stats["cf"]/d*100, stats["acct"], stats["err"])


if __name__ == "__main__":
    main()
