"""
パスワードリセット用テーブルのマイグレーション
password_reset_tokens テーブルを作成する（トークンはSHA256ハッシュのみ保存）

実行方法:
  $env:TURSO_DATABASE_URL = "libsql://..."
  $env:TURSO_AUTH_TOKEN = "..."
  python scripts/migrate_password_reset.py
"""
import logging
import sys

from turso_helpers import execute_single, get_headers, get_turso_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def table_exists(url, headers, table):
    result = execute_single(
        url,
        headers,
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        args=[{"type": "text", "value": table}],
    )
    return len(result["results"][0]["response"]["result"]["rows"]) > 0


def main():
    url, token = get_turso_config()
    url = url.replace("libsql://", "https://")
    headers = get_headers(token)

    if table_exists(url, headers, "password_reset_tokens"):
        logger.info("password_reset_tokens は既に存在します（スキップ）")
    else:
        execute_single(
            url,
            headers,
            """
            CREATE TABLE password_reset_tokens (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                token_hash TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                used_at TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
            """,
        )
        execute_single(
            url,
            headers,
            "CREATE INDEX idx_reset_tokens_hash ON password_reset_tokens (token_hash)",
        )
        execute_single(
            url,
            headers,
            "CREATE INDEX idx_reset_tokens_user ON password_reset_tokens (user_id)",
        )
        logger.info("password_reset_tokens テーブルとインデックスを作成しました")

    logger.info("マイグレーション完了")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error("マイグレーション失敗: %s", e)
        sys.exit(1)
