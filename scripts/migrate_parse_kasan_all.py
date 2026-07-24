"""
加算_全項目(JSON辞書、充足94%)をパースして個別加算フラグ13列を再生成する

背景: 個別フラグ列は充足2〜29%(特定サービス種別のみ)だが、加算_全項目は
208,762件(94%)に全加算リストを保持している。json_eachで正確に展開する。

キー表記の揺れ対応: 括弧は半角(Ⅰ)/全角（Ⅰ）の両方、
処遇改善は「介護職員処遇改善加算」「介護職員等処遇改善加算」があり、
「特定処遇改善加算」(別加算)を誤マッチしないよう除外する。

実行方法:
  $env:TURSO_DATABASE_URL / TURSO_AUTH_TOKEN を設定して
  python scripts/migrate_parse_kasan_all.py
"""
import logging
import sys

from turso_helpers import execute_single, get_headers, get_turso_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# フラグ列 → (キーLIKEパターン(半角/全角), 除外パターン)
FLAG_RULES = [
    ("加算_処遇改善I",   ["%処遇改善加算（Ⅰ）%", "%処遇改善加算(Ⅰ)%"], "%特定%"),
    ("加算_処遇改善II",  ["%処遇改善加算（Ⅱ）%", "%処遇改善加算(Ⅱ)%"], "%特定%"),
    ("加算_処遇改善III", ["%処遇改善加算（Ⅲ）%", "%処遇改善加算(Ⅲ)%"], "%特定%"),
    ("加算_処遇改善IV",  ["%処遇改善加算（Ⅳ）%", "%処遇改善加算(Ⅳ)%"], "%特定%"),
    ("加算_特定事業所I",   ["%特定事業所加算（Ⅰ）%", "%特定事業所加算(Ⅰ)%"], None),
    ("加算_特定事業所II",  ["%特定事業所加算（Ⅱ）%", "%特定事業所加算(Ⅱ)%"], None),
    ("加算_特定事業所III", ["%特定事業所加算（Ⅲ）%", "%特定事業所加算(Ⅲ)%"], None),
    ("加算_特定事業所IV",  ["%特定事業所加算（Ⅳ）%", "%特定事業所加算(Ⅳ)%"], None),
    ("加算_特定事業所V",   ["%特定事業所加算（Ⅴ）%", "%特定事業所加算(Ⅴ)%"], None),
    ("加算_認知症ケアI",  ["%認知症専門ケア加算（Ⅰ）%", "%認知症専門ケア加算(Ⅰ)%"], None),
    ("加算_認知症ケアII", ["%認知症専門ケア加算（Ⅱ）%", "%認知症専門ケア加算(Ⅱ)%"], None),
    ("加算_口腔連携", ["%口腔連携%"], None),
    ("加算_緊急時",   ["%緊急時%"], None),
]


def run(url, headers, sql, timeout=600):
    result = execute_single(url, headers, sql, timeout=timeout)
    res = result["results"][0]
    if res.get("type") != "ok":
        raise RuntimeError(f"SQL失敗: {res.get('error')}")
    return res["response"]["result"].get("affected_row_count", 0)


def count_flag(url, headers, col):
    result = execute_single(url, headers, f'SELECT SUM(CASE WHEN "{col}" = 1 THEN 1 ELSE 0 END) FROM facilities')
    return int(result["results"][0]["response"]["result"]["rows"][0][0]["value"] or 0)


def main():
    url, token = get_turso_config()
    url = url.replace("libsql://", "https://")
    headers = get_headers(token)

    for col, patterns, exclude in FLAG_RULES:
        before = count_flag(url, headers, col)

        key_conds = " OR ".join(f"je.key LIKE '{p}'" for p in patterns)
        exclude_cond = f"AND je.key NOT LIKE '{exclude}'" if exclude else ""
        sql = f"""
            UPDATE facilities SET "{col}" = 1
            WHERE COALESCE("加算_全項目", '') != ''
              AND COALESCE("{col}", 0) != 1
              AND EXISTS (
                  SELECT 1 FROM json_each("加算_全項目") je
                  WHERE ({key_conds}) {exclude_cond} AND je.value = '○'
              )
        """
        added = run(url, headers, sql)
        after = count_flag(url, headers, col)
        logger.info("%s: %s → %s (+%s)", col, f"{before:,}", f"{after:,}", f"{added:,}")

    # kasan_count 再計算
    kasan_sum = " + ".join(
        f'CAST(COALESCE(NULLIF(CAST("{col}" AS TEXT), \'\'), \'0\') AS REAL)'
        for col, _, _ in FLAG_RULES
    )
    affected = run(url, headers, f"UPDATE facilities SET kasan_count = CAST({kasan_sum} AS INTEGER)")
    logger.info("kasan_count 再計算: %s 行", affected)

    result = execute_single(url, headers, "SELECT AVG(kasan_count), MAX(kasan_count) FROM facilities")
    row = result["results"][0]["response"]["result"]["rows"][0]
    logger.info("kasan_count 平均=%s 最大=%s", row[0]["value"], row[1]["value"])
    logger.info("完了")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error("失敗: %s", e)
        sys.exit(1)
