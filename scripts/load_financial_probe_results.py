"""
財務URLプローブ結果(probe_results.csv)をfacilitiesテーブルへ反映する

- PL/CF/BSのURLが1つ以上見つかった施設のみUPDATE
- 既存値は上書き(プローブ結果が最新の実在確認のため)
- 事業所番号+サービスコードの複合キーでマッチ

実行方法:
  $env:TURSO_DATABASE_URL / TURSO_AUTH_TOKEN を設定して
  python scripts/load_financial_probe_results.py
"""
import csv
import logging
from pathlib import Path

import requests

from turso_helpers import get_headers, get_turso_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

RESULTS_CSV = Path(__file__).resolve().parent.parent / "data" / "output" / "financial_probe" / "probe_results.csv"
BATCH_SIZE = 50


def main():
    url, token = get_turso_config()
    url = url.replace("libsql://", "https://")
    headers = get_headers(token)

    rows_with_data = []
    total = 0
    with open(RESULTS_CSV, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            total += 1
            if row["PL"] or row["CF"] or row["BS"]:
                rows_with_data.append(row)

    logger.info("プローブ結果 %s 件中、財務URLあり %s 件を反映します", total, len(rows_with_data))

    updated = 0
    for i in range(0, len(rows_with_data), BATCH_SIZE):
        batch = rows_with_data[i : i + BATCH_SIZE]
        stmts = []
        for row in batch:
            stmts.append({
                "type": "execute",
                "stmt": {
                    "sql": """
                        UPDATE facilities SET
                            "財務DL_事業活動計算書" = CASE WHEN ? != '' THEN ? ELSE "財務DL_事業活動計算書" END,
                            "財務DL_資金収支計算書" = CASE WHEN ? != '' THEN ? ELSE "財務DL_資金収支計算書" END,
                            "財務DL_貸借対照表" = CASE WHEN ? != '' THEN ? ELSE "財務DL_貸借対照表" END
                        WHERE "事業所番号" = ? AND "サービスコード" = ?
                    """,
                    "args": [
                        {"type": "text", "value": row["PL"]}, {"type": "text", "value": row["PL"]},
                        {"type": "text", "value": row["CF"]}, {"type": "text", "value": row["CF"]},
                        {"type": "text", "value": row["BS"]}, {"type": "text", "value": row["BS"]},
                        {"type": "text", "value": row["jigyosho_number"]},
                        {"type": "text", "value": row["service_code"]},
                    ],
                },
            })
        stmts.append({"type": "close"})
        resp = requests.post(f"{url}/v2/pipeline", headers=headers, json={"requests": stmts}, timeout=300)
        resp.raise_for_status()
        for res in resp.json()["results"]:
            if res.get("type") == "ok" and res.get("response", {}).get("type") == "execute":
                updated += res["response"]["result"].get("affected_row_count", 0)
        if (i // BATCH_SIZE) % 20 == 0:
            logger.info("進捗 %s / %s", min(i + BATCH_SIZE, len(rows_with_data)), len(rows_with_data))

    logger.info("完了: %s 行を更新しました", updated)


if __name__ == "__main__":
    main()
