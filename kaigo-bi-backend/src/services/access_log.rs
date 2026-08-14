/// APIアクセスログ
///
/// 目的は「防ぐ」ではなく **「気づく」**。
/// kaigo-bi は公表情報を集約したBIなので、ログインさえすればAPIを素直に叩いて
/// 中身を持って行ける。完全には防げない以上、
///   誰が / いつ / どのAPIを / どれだけ叩いたか
/// を残しておかないと、全件を舐められても後から気づく手段がない。
///
/// 設計上の制約:
///   - リクエストごとに Turso へ書くと往復0.066秒がレスポンスに乗る。
///     メモリにバッファし、別タスクでまとめて流す。
///   - バッファは上限付き。溢れたら捨てる（ログのためにアプリを止めない）。
///   - 取得量は Content-Length を使う。レスポンス本文を数えると
///     ストリーミングが壊れるうえメモリを食う。
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use libsql::Database;
use std::sync::Arc;
use uuid::Uuid;

/// 1件分のアクセス記録
#[derive(Clone, Debug)]
pub struct AccessEntry {
    pub user_id: String,
    pub email: String,
    pub plan: String,
    pub method: String,
    pub path: String,
    pub query: Option<String>,
    pub status: u16,
    pub response_bytes: i64,
    pub duration_ms: i64,
    pub ip: String,
    pub user_agent: Option<String>,
}

/// 溜めすぎない。10秒ごとに流すので、通常は数百件で足りる
const MAX_BUFFER: usize = 5_000;
/// 1回のフラッシュで書く最大件数
const FLUSH_CHUNK: usize = 200;
/// フラッシュ間隔
const FLUSH_INTERVAL: Duration = Duration::from_secs(10);

static BUFFER: OnceLock<Mutex<Vec<AccessEntry>>> = OnceLock::new();

fn buffer() -> &'static Mutex<Vec<AccessEntry>> {
    BUFFER.get_or_init(|| Mutex::new(Vec::new()))
}

/// バッファへ積む。ロックが取れない・溢れている場合は黙って捨てる
pub fn record(entry: AccessEntry) {
    if let Ok(mut buf) = buffer().lock() {
        if buf.len() < MAX_BUFFER {
            buf.push(entry);
        }
    }
}

/// 現在のバッファ長（監視用）
pub fn pending() -> usize {
    buffer().lock().map(|b| b.len()).unwrap_or(0)
}

const CREATE_TABLE: &str = "
CREATE TABLE IF NOT EXISTS api_access_logs (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    email TEXT,
    plan TEXT,
    method TEXT NOT NULL,
    path TEXT NOT NULL,
    query TEXT,
    status INTEGER,
    response_bytes INTEGER,
    duration_ms INTEGER,
    ip TEXT,
    user_agent TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
)";

const CREATE_INDEX_USER: &str =
    "CREATE INDEX IF NOT EXISTS idx_aal_user_time ON api_access_logs (user_id, created_at)";
const CREATE_INDEX_TIME: &str =
    "CREATE INDEX IF NOT EXISTS idx_aal_time ON api_access_logs (created_at)";

/// テーブルを用意してから、定期フラッシュのタスクを起動する。
/// 失敗してもアプリは止めない（ログはあくまで付加機能）
pub fn spawn_flusher(db: Arc<Database>) {
    tokio::spawn(async move {
        match db.connect() {
            Ok(conn) => {
                for sql in [CREATE_TABLE, CREATE_INDEX_USER, CREATE_INDEX_TIME] {
                    if let Err(e) = conn.execute(sql, ()).await {
                        tracing::warn!("api_access_logs の作成に失敗: {}", e);
                    }
                }
                tracing::info!("APIアクセスログ: テーブル確認完了");
            }
            Err(e) => {
                tracing::warn!("APIアクセスログ: DB接続に失敗 ({}). ログは記録されません", e);
                return;
            }
        }

        loop {
            tokio::time::sleep(FLUSH_INTERVAL).await;
            flush(&db).await;
        }
    });
}

/// バッファを取り出してDBへ書く
async fn flush(db: &Arc<Database>) {
    let batch: Vec<AccessEntry> = {
        let Ok(mut buf) = buffer().lock() else { return };
        if buf.is_empty() {
            return;
        }
        let take = buf.len().min(FLUSH_CHUNK);
        buf.drain(..take).collect()
    };

    let conn = match db.connect() {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!("アクセスログのフラッシュに失敗（DB接続）: {}", e);
            return;
        }
    };

    let mut written = 0usize;
    for e in &batch {
        let res = conn
            .execute(
                "INSERT INTO api_access_logs
                   (id, user_id, email, plan, method, path, query, status,
                    response_bytes, duration_ms, ip, user_agent)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                libsql::params![
                    Uuid::new_v4().to_string(),
                    e.user_id.clone(),
                    e.email.clone(),
                    e.plan.clone(),
                    e.method.clone(),
                    e.path.clone(),
                    e.query.clone(),
                    e.status as i64,
                    e.response_bytes,
                    e.duration_ms,
                    e.ip.clone(),
                    e.user_agent.clone(),
                ],
            )
            .await;
        match res {
            Ok(_) => written += 1,
            Err(err) => {
                tracing::warn!("アクセスログの書き込みに失敗: {}", err);
                break; // DBが不調なら残りは諦める（次の周期で再開）
            }
        }
    }
    if written > 0 {
        tracing::debug!("アクセスログ {}件を記録", written);
    }
}
