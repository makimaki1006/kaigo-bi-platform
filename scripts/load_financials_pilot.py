"""
決算PDF抽出結果(extracted_*.json)をTurso financialsテーブルへロードする

前段: エージェントによる視覚抽出で data/financial_pilot/extracted_{法人種別}.json が生成済みであること
テーブルが無ければ作成する(冪等)

実行方法:
  $env:TURSO_DATABASE_URL / TURSO_AUTH_TOKEN を設定して
  python scripts/load_financials_pilot.py
"""
import csv
import json
import logging
import sys
import uuid
from pathlib import Path

from turso_helpers import execute_single, get_headers, get_turso_config, make_arg

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PILOT_DIR = Path(__file__).resolve().parent.parent / "data" / "financial_pilot"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS financials (
    id TEXT PRIMARY KEY,
    jigyosho_number TEXT NOT NULL,
    corp_number TEXT,
    doc_type TEXT NOT NULL,
    fiscal_period TEXT,
    revenue INTEGER,
    personnel_cost INTEGER,
    operating_income INTEGER,
    ordinary_income INTEGER,
    net_income INTEGER,
    prior_revenue INTEGER,
    prior_operating_income INTEGER,
    total_assets INTEGER,
    net_assets INTEGER,
    total_liabilities INTEGER,
    confidence TEXT,
    source_url TEXT,
    notes TEXT,
    extracted_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (jigyosho_number, doc_type)
)
"""


def to_int(value):
    """抽出JSONの数値をintに正規化（null/文字列対応）"""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    s = str(value).replace(",", "").replace("△", "-").strip()
    if not s:
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


def main():
    url, token = get_turso_config()
    url = url.replace("libsql://", "https://")
    headers = get_headers(token)

    # テーブル作成（冪等）
    execute_single(url, headers, CREATE_TABLE_SQL)
    execute_single(
        url, headers,
        "CREATE INDEX IF NOT EXISTS idx_financials_corp ON financials (corp_number)",
    )
    execute_single(
        url, headers,
        "CREATE INDEX IF NOT EXISTS idx_financials_jigyosho ON financials (jigyosho_number)",
    )

    # manifestから事業所番号→(法人番号, URL)のマップを作る
    meta = {}
    manifest = PILOT_DIR / "pilot_manifest.csv"
    if manifest.exists():
        with open(manifest, encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                meta[(row["jigyosho_number"], row["doc_type"])] = {
                    "corp_number": row.get("corp_number") or None,
                    "url": row.get("url") or None,
                }

    inserted = 0
    skipped = 0
    for json_path in sorted(PILOT_DIR.glob("extracted_*.json")):
        records = json.loads(json_path.read_text(encoding="utf-8"))
        logger.info("%s: %s 件", json_path.name, len(records))

        for rec in records:
            jigyosho = str(rec.get("jigyosho_number", "")).strip()
            doc_type = str(rec.get("doc_type", "")).strip()
            if not jigyosho or doc_type not in ("PL", "BS", "CF"):
                skipped += 1
                continue

            m = meta.get((jigyosho, doc_type), {})
            args = [
                make_arg(str(uuid.uuid4())),
                make_arg(jigyosho),
                make_arg(m.get("corp_number")),
                make_arg(doc_type),
                make_arg(rec.get("fiscal_period")),
                make_arg(to_int(rec.get("revenue"))),
                make_arg(to_int(rec.get("personnel_cost"))),
                make_arg(to_int(rec.get("operating_income"))),
                make_arg(to_int(rec.get("ordinary_income"))),
                make_arg(to_int(rec.get("net_income"))),
                make_arg(to_int(rec.get("prior_revenue"))),
                make_arg(to_int(rec.get("prior_operating_income"))),
                make_arg(to_int(rec.get("total_assets"))),
                make_arg(to_int(rec.get("net_assets"))),
                make_arg(to_int(rec.get("total_liabilities"))),
                make_arg(rec.get("confidence")),
                make_arg(m.get("url")),
                make_arg(rec.get("notes")),
            ]
            execute_single(
                url, headers,
                """
                INSERT INTO financials (
                    id, jigyosho_number, corp_number, doc_type, fiscal_period,
                    revenue, personnel_cost, operating_income, ordinary_income, net_income,
                    prior_revenue, prior_operating_income,
                    total_assets, net_assets, total_liabilities,
                    confidence, source_url, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (jigyosho_number, doc_type) DO UPDATE SET
                    corp_number = excluded.corp_number,
                    fiscal_period = excluded.fiscal_period,
                    revenue = excluded.revenue,
                    personnel_cost = excluded.personnel_cost,
                    operating_income = excluded.operating_income,
                    ordinary_income = excluded.ordinary_income,
                    net_income = excluded.net_income,
                    prior_revenue = excluded.prior_revenue,
                    prior_operating_income = excluded.prior_operating_income,
                    total_assets = excluded.total_assets,
                    net_assets = excluded.net_assets,
                    total_liabilities = excluded.total_liabilities,
                    confidence = excluded.confidence,
                    source_url = excluded.source_url,
                    notes = excluded.notes,
                    extracted_at = datetime('now')
                """,
                args=args,
            )
            inserted += 1

    # 検証
    result = execute_single(
        url, headers,
        "SELECT doc_type, COUNT(*), SUM(CASE WHEN revenue IS NOT NULL THEN 1 ELSE 0 END) FROM financials GROUP BY doc_type",
    )
    for row in result["results"][0]["response"]["result"]["rows"]:
        logger.info(
            "financials %s: %s 件 (revenue有り %s)",
            row[0]["value"], row[1]["value"], row[2]["value"],
        )

    logger.info("ロード完了: %s 件投入 / %s 件スキップ", inserted, skipped)


if __name__ == "__main__":
    main()
