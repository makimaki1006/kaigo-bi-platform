"""
決算PDFパイロット: サンプルダウンロード
facilitiesテーブルの財務DL URLから、法人種別のバランスを取って
PL(事業活動計算書/損益計算書)PDFをダウンロードする

実行方法:
  $env:TURSO_DATABASE_URL / TURSO_AUTH_TOKEN を設定して
  python scripts/download_financial_pdfs_pilot.py [件数/種別 デフォルト5]

出力: data/financial_pilot/{法人種別}/{事業所番号}_{PL|BS|CF}.pdf
      data/financial_pilot/pilot_manifest.csv
"""
import csv
import logging
import sys
import time
from pathlib import Path
from urllib.parse import quote

import requests

from turso_helpers import execute_single, get_headers, get_turso_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://www.kaigokensaku.mhlw.go.jp"
OUT_DIR = Path(__file__).resolve().parent.parent / "data" / "financial_pilot"

# 法人種別ごとにサンプリング（会計基準の様式差を確認するため）
CORP_TYPES = ["社会福祉法人", "医療法人", "株式会社・有限会社等", "NPO法人", "その他法人"]

DOC_COLS = {
    "PL": "財務DL_事業活動計算書",
    "CF": "財務DL_資金収支計算書",
    "BS": "財務DL_貸借対照表",
}


def to_abs_url(path: str) -> str:
    path = path.strip()
    if path.startswith("http"):
        return path
    # 日本語ファイル名などをエンコード（パス区切りは保持）
    return BASE_URL + quote(path, safe="/?=&%")


def main():
    per_type = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    url, token = get_turso_config()
    url = url.replace("libsql://", "https://")
    headers = get_headers(token)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    manifest_rows = []
    session = requests.Session()
    session.headers["User-Agent"] = "Mozilla/5.0 (kaigo-bi financial pilot)"

    for corp_type in CORP_TYPES:
        sql = f"""
            SELECT "事業所番号", "事業所名", "法人名", "法人番号", corp_type, prefecture, "サービス名", "会計種別",
                   "{DOC_COLS['PL']}", "{DOC_COLS['CF']}", "{DOC_COLS['BS']}"
            FROM facilities
            WHERE corp_type = ?
              AND COALESCE("{DOC_COLS['PL']}", '') != ''
            LIMIT {per_type}
        """
        result = execute_single(url, headers, sql, args=[{"type": "text", "value": corp_type}])
        rows = result["results"][0]["response"]["result"]["rows"]
        logger.info("%s: %s 件", corp_type, len(rows))

        type_dir = OUT_DIR / corp_type.replace("・", "_").replace("/", "_")
        type_dir.mkdir(exist_ok=True)

        for row in rows:
            vals = [c.get("value") if isinstance(c, dict) else None for c in row]
            jigyosho, name, corp_name, corp_number, ctype, pref, service, acct = vals[:8]
            doc_urls = dict(zip(["PL", "CF", "BS"], vals[8:11]))

            for doc_type, rel in doc_urls.items():
                if not rel or not str(rel).strip():
                    continue
                abs_url = to_abs_url(str(rel))
                dest = type_dir / f"{jigyosho}_{doc_type}.pdf"
                status = "skip(exists)"
                if not dest.exists():
                    try:
                        r = session.get(abs_url, timeout=60)
                        if r.status_code == 200 and r.content[:4] == b"%PDF":
                            dest.write_bytes(r.content)
                            status = f"ok({len(r.content)//1024}KB)"
                        else:
                            status = f"NG(http={r.status_code}, head={r.content[:8]!r})"
                    except Exception as e:
                        status = f"ERROR({e})"
                    time.sleep(1.0)  # サーバー負荷配慮

                logger.info("  %s %s %s: %s", jigyosho, doc_type, name, status)
                manifest_rows.append({
                    "jigyosho_number": jigyosho,
                    "facility_name": name,
                    "corp_name": corp_name,
                    "corp_number": corp_number,
                    "corp_type": ctype,
                    "prefecture": pref,
                    "service_name": service,
                    "accounting_type": acct,
                    "doc_type": doc_type,
                    "url": abs_url,
                    "status": status,
                    "file": str(dest.relative_to(OUT_DIR)) if dest.exists() else "",
                })

    manifest_path = OUT_DIR / "pilot_manifest.csv"
    with open(manifest_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=list(manifest_rows[0].keys()))
        writer.writeheader()
        writer.writerows(manifest_rows)

    ok = sum(1 for r in manifest_rows if r["status"].startswith(("ok", "skip")))
    logger.info("完了: %s/%s ダウンロード成功。manifest: %s", ok, len(manifest_rows), manifest_path)


if __name__ == "__main__":
    main()
