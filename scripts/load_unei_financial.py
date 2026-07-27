"""
uneiスクレイピング結果(unei_results.csv)をfacilitiesテーブルへ反映する

uneiページHTML方式は財務リンクの有無を確実に取得できるため、
「リンクあり=上書き、リンクなし=空にクリア」で全件同期する。
(旧プローブの偽陰性込みデータを正す)

- 財務DL_事業活動計算書/資金収支計算書/貸借対照表: uneiで見つかった相対hrefで置換、無ければ空に
- 会計種類: 取れたものだけ上書き(未公表は空のまま既存維持しない=uneiが真)
- 事業所番号+サービスコードの複合キーでマッチ

実行方法:
  $env:TURSO_DATABASE_URL / TURSO_AUTH_TOKEN を設定して
  python scripts/load_unei_financial.py
"""
import csv
import logging
from pathlib import Path

import requests

from turso_helpers import get_headers, get_turso_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

RESULTS_CSV = Path(__file__).resolve().parent.parent / "data" / "output" / "unei_financial" / "unei_results.csv"
BATCH_SIZE = 50


def main():
    url, token = get_turso_config()
    url = url.replace("libsql://", "https://")
    headers = get_headers(token)

    rows = []
    with open(RESULTS_CSV, encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            if r.get("status") == "ok":
                rows.append(r)
    logger.info("status=ok の %s 件を同期します", len(rows))

    updated = 0
    stats = {"pl": 0, "bs": 0, "cf": 0, "acct": 0}
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        stmts = []
        for r in batch:
            pl = r.get("財務DL_事業活動計算書", "") or ""
            cf = r.get("財務DL_資金収支計算書", "") or ""
            bs = r.get("財務DL_貸借対照表", "") or ""
            acct = r.get("会計種類", "") or ""
            if pl: stats["pl"] += 1
            if bs: stats["bs"] += 1
            if cf: stats["cf"] += 1
            if acct: stats["acct"] += 1
            # 会計種類は取れた時だけ上書き（空uneiで既存を消さない）
            stmts.append({
                "type": "execute",
                "stmt": {
                    "sql": """
                        UPDATE facilities SET
                            "財務DL_事業活動計算書" = ?,
                            "財務DL_資金収支計算書" = ?,
                            "財務DL_貸借対照表" = ?,
                            "会計種類" = CASE WHEN ? != '' THEN ? ELSE "会計種類" END
                        WHERE "事業所番号" = ? AND "サービスコード" = ?
                    """,
                    "args": [
                        {"type": "text", "value": pl},
                        {"type": "text", "value": cf},
                        {"type": "text", "value": bs},
                        {"type": "text", "value": acct}, {"type": "text", "value": acct},
                        {"type": "text", "value": r["jigyosho_number"]},
                        {"type": "text", "value": r["service_code"]},
                    ],
                },
            })
        stmts.append({"type": "close"})
        resp = requests.post(f"{url}/v2/pipeline", headers=headers, json={"requests": stmts}, timeout=300)
        resp.raise_for_status()
        for res in resp.json()["results"]:
            if res.get("type") == "ok" and res.get("response", {}).get("type") == "execute":
                updated += res["response"]["result"].get("affected_row_count", 0)
        if (i // BATCH_SIZE) % 40 == 0:
            logger.info("進捗 %s / %s", min(i + BATCH_SIZE, len(rows)), len(rows))

    logger.info("完了: %s 行更新 | PL %s / BS %s / CF %s / 会計種類 %s",
                updated, stats["pl"], stats["bs"], stats["cf"], stats["acct"])


if __name__ == "__main__":
    main()
