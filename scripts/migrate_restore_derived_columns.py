"""
3月のfacilitiesテーブル再構築で消失した派生カラムを復元する

復元対象:
  - occupancy_rate REAL   = 利用者総数 / 定員（定員>0のみ）
  - kasan_count INTEGER   = 加算_* 13フラグの合計
  - quality_score REAL    = (BCP+ICT+第三者評価+損害賠償保険) * 25
  - quality_rank TEXT     = quality_score 100:S / 75:A / 50:B / 25:C / 0:D

実行方法:
  $env:TURSO_DATABASE_URL / TURSO_AUTH_TOKEN を設定して
  python scripts/migrate_restore_derived_columns.py
"""
import logging
import sys

from turso_helpers import execute_single, get_headers, get_turso_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

KASAN_COLS = [
    "加算_処遇改善I", "加算_処遇改善II", "加算_処遇改善III", "加算_処遇改善IV",
    "加算_特定事業所I", "加算_特定事業所II", "加算_特定事業所III", "加算_特定事業所IV", "加算_特定事業所V",
    "加算_認知症ケアI", "加算_認知症ケアII", "加算_口腔連携", "加算_緊急時",
]
QUALITY_COLS = ["品質_BCP策定", "品質_ICT活用", "品質_第三者評価", "品質_損害賠償保険"]


def num(col: str) -> str:
    """TEXT/NULL混在カラムを数値化するSQL断片"""
    return f'CAST(COALESCE(NULLIF("{col}", \'\'), \'0\') AS REAL)'


def column_exists(url, headers, table, column):
    result = execute_single(url, headers, f"PRAGMA table_info({table})")
    rows = result["results"][0]["response"]["result"]["rows"]
    return column in {row[1]["value"] for row in rows}


def run(url, headers, sql, timeout=300):
    result = execute_single(url, headers, sql, timeout=timeout)
    res = result["results"][0]
    if res.get("type") != "ok":
        raise RuntimeError(f"SQL失敗: {res.get('error')}")
    return res["response"]["result"].get("affected_row_count", 0)


def main():
    url, token = get_turso_config()
    url = url.replace("libsql://", "https://")
    headers = get_headers(token)

    # 1. カラム追加（冪等）
    for col, decl in [
        ("occupancy_rate", "REAL"),
        ("kasan_count", "INTEGER"),
        ("quality_score", "REAL"),
        ("quality_rank", "TEXT"),
    ]:
        if column_exists(url, headers, "facilities", col):
            logger.info("%s は既に存在（スキップ）", col)
        else:
            run(url, headers, f"ALTER TABLE facilities ADD COLUMN {col} {decl}")
            logger.info("%s カラムを追加", col)

    # 1.5 フラグ列の正規化: '○'/NULL → 1/0
    # （再構築後のスクレイピングデータは '○' 表記。旧SQLの `= 1` 比較を全て空振りさせていた）
    for col in KASAN_COLS + QUALITY_COLS:
        affected = run(url, headers, f"""
            UPDATE facilities SET "{col}" =
                CASE WHEN "{col}" = '○' THEN 1
                     WHEN "{col}" IN ('1', 1) THEN 1
                     ELSE 0 END
            WHERE "{col}" IS NULL OR "{col}" NOT IN ('0', 0, '1', 1)
        """)
        logger.info("%s 正規化: %s 行", col, affected)

    # 2. occupancy_rate
    affected = run(url, headers, f"""
        UPDATE facilities SET occupancy_rate =
            CASE WHEN {num('定員')} > 0
                 THEN {num('利用者総数')} / {num('定員')}
                 ELSE NULL END
    """)
    logger.info("occupancy_rate 更新: %s 行", affected)

    # 3. kasan_count
    kasan_sum = " + ".join(num(c) for c in KASAN_COLS)
    affected = run(url, headers, f"UPDATE facilities SET kasan_count = CAST({kasan_sum} AS INTEGER)")
    logger.info("kasan_count 更新: %s 行", affected)

    # 4. quality_score / quality_rank
    quality_sum = " + ".join(num(c) for c in QUALITY_COLS)
    affected = run(url, headers, f"UPDATE facilities SET quality_score = ({quality_sum}) * 25.0")
    logger.info("quality_score 更新: %s 行", affected)

    affected = run(url, headers, """
        UPDATE facilities SET quality_rank =
            CASE
                WHEN quality_score >= 100 THEN 'S'
                WHEN quality_score >= 75 THEN 'A'
                WHEN quality_score >= 50 THEN 'B'
                WHEN quality_score >= 25 THEN 'C'
                ELSE 'D'
            END
    """)
    logger.info("quality_rank 更新: %s 行", affected)

    # 5. 検証
    result = execute_single(url, headers, """
        SELECT
            COUNT(*),
            SUM(CASE WHEN occupancy_rate IS NOT NULL THEN 1 ELSE 0 END),
            AVG(CASE WHEN occupancy_rate BETWEEN 0.0 AND 3.0 THEN occupancy_rate END),
            AVG(quality_score),
            AVG(kasan_count)
        FROM facilities
    """)
    row = result["results"][0]["response"]["result"]["rows"][0]
    vals = [c.get("value") for c in row]
    logger.info("検証: 総行数=%s occupancy有=%s 平均稼働率=%s 平均品質=%s 平均加算数=%s", *vals)
    logger.info("マイグレーション完了")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error("失敗: %s", e)
        sys.exit(1)
