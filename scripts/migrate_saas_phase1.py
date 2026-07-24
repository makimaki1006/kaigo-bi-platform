"""
SaaS Phase 1 マイグレーション
usersテーブルにプラン・Stripe連携カラムを追加し、export_logsテーブルを作成する

追加内容:
  - users.plan                    TEXT DEFAULT 'free'（free/standard/pro/ma）
  - users.stripe_customer_id      TEXT
  - users.stripe_subscription_id  TEXT
  - export_logs テーブル（CSVエクスポートの月間クレジット管理）
  - 既存ユーザーは plan='ma' に設定（グランドファザリング: 従来の全機能アクセスを維持）

実行方法:
  $env:TURSO_DATABASE_URL = "libsql://..."
  $env:TURSO_AUTH_TOKEN = "..."
  python scripts/migrate_saas_phase1.py
"""
import logging
import sys

from turso_helpers import execute_single, get_headers, get_turso_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def column_exists(url, headers, table, column):
    """テーブルに指定カラムが存在するか確認"""
    result = execute_single(url, headers, f"PRAGMA table_info({table})")
    rows = result["results"][0]["response"]["result"]["rows"]
    existing = {row[1]["value"] for row in rows}
    return column in existing


def table_exists(url, headers, table):
    """テーブルの存在確認"""
    result = execute_single(
        url,
        headers,
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        args=[{"type": "text", "value": table}],
    )
    rows = result["results"][0]["response"]["result"]["rows"]
    return len(rows) > 0


def main():
    url, token = get_turso_config()
    url = url.replace("libsql://", "https://")
    headers = get_headers(token)

    # 1. users.plan カラム追加
    if column_exists(url, headers, "users", "plan"):
        logger.info("users.plan は既に存在します（スキップ）")
    else:
        execute_single(url, headers, "ALTER TABLE users ADD COLUMN plan TEXT NOT NULL DEFAULT 'free'")
        logger.info("users.plan カラムを追加しました")

        # 既存ユーザーをグランドファザリング（従来の全機能アクセスを維持）
        result = execute_single(url, headers, "UPDATE users SET plan = 'ma' WHERE plan = 'free'")
        affected = result["results"][0]["response"]["result"].get("affected_row_count", 0)
        logger.info("既存ユーザー %s 件を plan='ma' に設定しました", affected)

    # 2. users.stripe_customer_id カラム追加
    if column_exists(url, headers, "users", "stripe_customer_id"):
        logger.info("users.stripe_customer_id は既に存在します（スキップ）")
    else:
        execute_single(url, headers, "ALTER TABLE users ADD COLUMN stripe_customer_id TEXT")
        logger.info("users.stripe_customer_id カラムを追加しました")

    # 3. users.stripe_subscription_id カラム追加
    if column_exists(url, headers, "users", "stripe_subscription_id"):
        logger.info("users.stripe_subscription_id は既に存在します（スキップ）")
    else:
        execute_single(url, headers, "ALTER TABLE users ADD COLUMN stripe_subscription_id TEXT")
        logger.info("users.stripe_subscription_id カラムを追加しました")

    # 4. export_logs テーブル作成
    if table_exists(url, headers, "export_logs"):
        logger.info("export_logs テーブルは既に存在します（スキップ）")
    else:
        execute_single(
            url,
            headers,
            """
            CREATE TABLE export_logs (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                row_count INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
            """,
        )
        execute_single(
            url,
            headers,
            "CREATE INDEX idx_export_logs_user_month ON export_logs (user_id, created_at)",
        )
        logger.info("export_logs テーブルとインデックスを作成しました")

    # 5. 検証
    result = execute_single(url, headers, "PRAGMA table_info(users)")
    rows = result["results"][0]["response"]["result"]["rows"]
    columns = [row[1]["value"] for row in rows]
    logger.info("users カラム一覧: %s", columns)

    result = execute_single(
        url, headers, "SELECT COALESCE(plan,'(null)') AS plan, COUNT(*) FROM users GROUP BY plan"
    )
    rows = result["results"][0]["response"]["result"]["rows"]
    for row in rows:
        logger.info("plan=%s: %s 件", row[0]["value"], row[1]["value"])

    logger.info("マイグレーション完了")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error("マイグレーション失敗: %s", e)
        sys.exit(1)
