"""
財務PDF URL 直接プローブ(方式A)
介護情報公表システムの財務PDF URLは決定論的に構築できる(SCRAPER_REDESIGN_20260725.md §2.1):
  /upload/jigyosyofile/{PP}/{事業所番号}_00_{サービスコード}/{Doc}File/kouhyou/1.pdf
  Doc = IncomeStatement(PL) / CashFlow(CF) / BalanceSheets(BS)

Range GET(先頭4バイト)で 200+%PDF なら有り、404なら無しを機械判定する。
PLをまず全施設でプローブし、PLが有る施設のみCF/BSを追加プローブする。

実行方法:
  $env:TURSO_DATABASE_URL / TURSO_AUTH_TOKEN を設定して
  python scripts/scrape_financial_probe.py --pref 31          # 都道府県指定(パイロット)
  python scripts/scrape_financial_probe.py --all              # 全国
  python scripts/scrape_financial_probe.py --pref 31 --fresh  # 進捗リセット

出力: data/output/financial_probe/probe_results.csv (resume対応・追記式)
"""
import argparse
import csv
import logging
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests

from turso_helpers import execute_single, get_headers, get_turso_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BASE = "https://www.kaigokensaku.mhlw.go.jp"
OUT_DIR = Path(__file__).resolve().parent.parent / "data" / "output" / "financial_probe"
RESULTS_CSV = OUT_DIR / "probe_results.csv"

DOCS = {
    "PL": "IncomeStatementFile",
    "CF": "CashFlowFile",
    "BS": "BalanceSheetsFile",
}

PARALLEL = 15
PER_THREAD_DELAY = 1.0  # 政府サイトへの負荷配慮

_thread_local = threading.local()


def get_session() -> requests.Session:
    if not hasattr(_thread_local, "session"):
        s = requests.Session()
        s.headers["User-Agent"] = "Mozilla/5.0 (kaigo-bi financial probe)"
        _thread_local.session = s
    return _thread_local.session


def build_path(jigyosho: str, svc: str, doc_folder: str) -> str:
    return f"/upload/jigyosyofile/{jigyosho[:2]}/{jigyosho}_00_{svc}/{doc_folder}/kouhyou/1.pdf"


def probe_one(path: str) -> bool:
    """PDFが存在するか(200/206 かつ %PDF 先頭)を判定"""
    session = get_session()
    for attempt in range(3):
        try:
            r = session.get(
                BASE + path,
                headers={"Range": "bytes=0-3"},
                timeout=30,
            )
            if r.status_code in (200, 206):
                return r.content[:4] == b"%PDF"
            if r.status_code == 404:
                return False
            # 一時エラー(5xx等)はリトライ
        except requests.RequestException:
            pass
        time.sleep(2 ** attempt)
    return False


def probe_facility(jigyosho: str, svc: str) -> dict:
    """PLをプローブし、有ればCF/BSも確認する"""
    time.sleep(PER_THREAD_DELAY)
    result = {"jigyosho_number": jigyosho, "service_code": svc, "PL": "", "CF": "", "BS": ""}

    pl_path = build_path(jigyosho, svc, DOCS["PL"])
    if probe_one(pl_path):
        result["PL"] = pl_path
        for doc in ("CF", "BS"):
            p = build_path(jigyosho, svc, DOCS[doc])
            if probe_one(p):
                result[doc] = p
    else:
        # PLが無くてもBSだけ公表のケースがある(充足率: BS 7,717 vs PL 8,806)
        bs_path = build_path(jigyosho, svc, DOCS["BS"])
        if probe_one(bs_path):
            result["BS"] = bs_path

    return result


def load_targets(pref: str | None):
    """Tursoから対象施設(事業所番号, サービスコード)を取得"""
    url, token = get_turso_config()
    url = url.replace("libsql://", "https://")
    headers = get_headers(token)

    where = f"WHERE \"事業所番号\" LIKE '{pref}%'" if pref else ""
    targets = []
    offset = 0
    while True:
        sql = f'SELECT "事業所番号", "サービスコード" FROM facilities {where} LIMIT 5000 OFFSET {offset}'
        result = execute_single(url, headers, sql, timeout=300)
        rows = result["results"][0]["response"]["result"]["rows"]
        if not rows:
            break
        for row in rows:
            targets.append((row[0]["value"], row[1]["value"]))
        offset += 5000
        if len(rows) < 5000:
            break
    return targets


def load_done() -> set:
    if not RESULTS_CSV.exists():
        return set()
    done = set()
    with open(RESULTS_CSV, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            done.add(f"{row['jigyosho_number']}_{row['service_code']}")
    return done


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pref", help="都道府県コード2桁(パイロット用)")
    parser.add_argument("--all", action="store_true", help="全国実行")
    parser.add_argument("--fresh", action="store_true", help="進捗をリセット")
    args = parser.parse_args()

    if not args.pref and not args.all:
        logger.error("--pref XX または --all を指定してください")
        sys.exit(1)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    if args.fresh and RESULTS_CSV.exists():
        RESULTS_CSV.unlink()

    targets = load_targets(args.pref)
    done = load_done()
    remaining = [(j, s) for j, s in targets if f"{j}_{s}" not in done]
    logger.info("対象 %s 件 / 済み %s 件 / 残り %s 件", len(targets), len(done), len(remaining))

    if not remaining:
        logger.info("すべて完了済み")
        return

    write_header = not RESULTS_CSV.exists()
    lock = threading.Lock()
    stats = {"done": 0, "pl": 0, "bs": 0, "cf": 0}
    start = time.time()

    with open(RESULTS_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["jigyosho_number", "service_code", "PL", "CF", "BS"])
        if write_header:
            writer.writeheader()

        with ThreadPoolExecutor(max_workers=PARALLEL) as pool:
            futures = {pool.submit(probe_facility, j, s): (j, s) for j, s in remaining}
            for fut in as_completed(futures):
                try:
                    rec = fut.result()
                except Exception as e:
                    j, s = futures[fut]
                    logger.warning("失敗 %s_%s: %s", j, s, e)
                    continue
                with lock:
                    writer.writerow(rec)
                    f.flush()
                    stats["done"] += 1
                    if rec["PL"]:
                        stats["pl"] += 1
                    if rec["BS"]:
                        stats["bs"] += 1
                    if rec["CF"]:
                        stats["cf"] += 1
                    if stats["done"] % 200 == 0:
                        elapsed = time.time() - start
                        rate = stats["done"] / elapsed if elapsed > 0 else 0
                        eta_h = (len(remaining) - stats["done"]) / rate / 3600 if rate > 0 else 0
                        logger.info(
                            "進捗 %s/%s (%.1f req群/s, 残り推定 %.1fh) | PL:%s BS:%s CF:%s",
                            stats["done"], len(remaining), rate, eta_h,
                            stats["pl"], stats["bs"], stats["cf"],
                        )

    logger.info(
        "完了: %s 件処理 | PL有り %s (%.1f%%) / BS有り %s / CF有り %s",
        stats["done"], stats["pl"],
        stats["pl"] / stats["done"] * 100 if stats["done"] else 0,
        stats["bs"], stats["cf"],
    )


if __name__ == "__main__":
    main()
