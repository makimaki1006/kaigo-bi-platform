"""
財務URLプローブの偽陰性率検証
プローブで「PDFなし」と判定した施設からランダムサンプルを取り、
実際のuneiページを取得して財務リンクの有無を照合する。

検出対象(プローブが構造上取れないパターンを含む):
  - /upload/jigyosyofile/... の (IncomeStatement|CashFlow|BalanceSheets)File リンク
    (枝番≠00 や 1.pdf以外もここで検出される)
  - /upload/jigyosyopdf/... の別スキーム(日本語ファイル名)
  - 外部URL登録(http://... へのリンクが財務セクションにある場合)

実行方法:
  python scripts/validate_probe_negatives.py [サンプル数=300]
"""
import csv
import logging
import random
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BASE = "https://www.kaigokensaku.mhlw.go.jp"
RESULTS_CSV = Path(__file__).resolve().parent.parent / "data" / "output" / "financial_probe" / "probe_results.csv"
OUT_CSV = Path(__file__).resolve().parent.parent / "data" / "output" / "financial_probe" / "validation_negatives.csv"

ACTION_CODE_MAP = {
    110: '001', 120: '002', 130: '004', 140: '005', 150: '001',
    160: '003', 170: '006', 210: '007', 220: '008', 230: '009',
    320: '022', 331: '014', 332: '015', 334: '016', 335: '017',
    336: '001', 337: '001', 361: '018', 362: '019', 364: '020',
    410: '010', 430: '023', 510: '011', 520: '012', 530: '013',
    540: '001', 550: '001', 551: '001',
    710: '024', 720: '025', 730: '021', 760: '026', 770: '027', 780: '028',
}

PARALLEL = 15
PER_THREAD_DELAY = 1.0
_tl = threading.local()


def get_session():
    if not hasattr(_tl, "s"):
        s = requests.Session()
        s.headers["User-Agent"] = "Mozilla/5.0 (kaigo-bi probe validation)"
        _tl.s = s
    return _tl.s


def check_unei(jigyosho: str, svc: str) -> dict:
    """uneiページを取得し財務リンクの有無・種類を判定"""
    time.sleep(PER_THREAD_DELAY)
    action = ACTION_CODE_MAP.get(int(svc))
    rec = {
        "jigyosho_number": jigyosho, "service_code": svc,
        "fetched": 0, "jigyosyofile_link": "", "jigyosyopdf_link": "", "note": "",
    }
    if not action:
        rec["note"] = f"action_code不明(svc={svc})"
        return rec

    url = f"{BASE}/{jigyosho[:2]}/index.php?action_kouhyou_detail_{action}_unei=true&JigyosyoCd={jigyosho}-00&ServiceCd={svc}"
    try:
        r = get_session().get(url, timeout=45)
        if r.status_code != 200:
            rec["note"] = f"HTTP {r.status_code}"
            return rec
        rec["fetched"] = 1
        soup = BeautifulSoup(r.content, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/upload/jigyosyofile/" in href and re.search(r"(IncomeStatement|CashFlow|BalanceSheets)File", href):
                rec["jigyosyofile_link"] = href[:150]
            elif "/upload/jigyosyopdf/" in href:
                rec["jigyosyopdf_link"] = href[:150]
    except requests.RequestException as e:
        rec["note"] = f"fetch error: {e}"
    return rec


def main():
    sample_n = int(sys.argv[1]) if len(sys.argv) > 1 else 300

    negatives = []
    with open(RESULTS_CSV, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if not row["PL"] and not row["BS"] and not row["CF"]:
                negatives.append((row["jigyosho_number"], row["service_code"]))
    logger.info("プローブ陰性: %s 件からランダム %s 件を検証", len(negatives), sample_n)

    random.seed(20260727)
    sample = random.sample(negatives, min(sample_n, len(negatives)))

    results = []
    with ThreadPoolExecutor(max_workers=PARALLEL) as pool:
        futures = [pool.submit(check_unei, j, s) for j, s in sample]
        for i, fut in enumerate(as_completed(futures), 1):
            results.append(fut.result())
            if i % 50 == 0:
                logger.info("進捗 %s/%s", i, len(sample))

    with open(OUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=list(results[0].keys()))
        w.writeheader()
        w.writerows(results)

    fetched = [r for r in results if r["fetched"] == 1]
    fn_file = [r for r in fetched if r["jigyosyofile_link"]]
    fn_pdf = [r for r in fetched if r["jigyosyopdf_link"] and not r["jigyosyofile_link"]]
    logger.info("=== 検証結果 ===")
    logger.info("ページ取得成功: %s / %s", len(fetched), len(results))
    logger.info("偽陰性(jigyosyofileリンクあり): %s 件 (%.1f%%)", len(fn_file), len(fn_file) / len(fetched) * 100 if fetched else 0)
    logger.info("別スキーム(jigyosyopdfのみ): %s 件 (%.1f%%)", len(fn_pdf), len(fn_pdf) / len(fetched) * 100 if fetched else 0)
    logger.info("真陰性(リンクなし): %s 件 (%.1f%%)", len(fetched) - len(fn_file) - len(fn_pdf), (len(fetched) - len(fn_file) - len(fn_pdf)) / len(fetched) * 100 if fetched else 0)
    logger.info("詳細: %s", OUT_CSV)


if __name__ == "__main__":
    main()
