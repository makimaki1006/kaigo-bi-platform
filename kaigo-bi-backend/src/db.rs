/// Turso/libSQL データベース接続管理
/// .envからURL+トークンを読み込み、接続を初期化する

use libsql::Database;

/// Tursoデータベースを初期化して返す
/// 環境変数 TURSO_DATABASE_URL と TURSO_AUTH_TOKEN を使用
pub async fn init_db() -> Result<Database, Box<dyn std::error::Error>> {
    let url = std::env::var("TURSO_DATABASE_URL")
        .expect("TURSO_DATABASE_URL が設定されていません");
    let token = std::env::var("TURSO_AUTH_TOKEN")
        .expect("TURSO_AUTH_TOKEN が設定されていません");

    let db = libsql::Builder::new_remote(url, token)
        .build()
        .await?;

    tracing::info!("Tursoデータベース接続を初期化しました");
    Ok(db)
}

/// 外部統計データベース（country-statistics）を初期化して返す
/// 環境変数 EXTERNAL_DB_URL と EXTERNAL_DB_TOKEN を使用
pub async fn init_external_db() -> Result<Database, Box<dyn std::error::Error>> {
    let url = std::env::var("EXTERNAL_DB_URL")
        .map_err(|_| "EXTERNAL_DB_URL が設定されていません")?;
    let token = std::env::var("EXTERNAL_DB_TOKEN")
        .map_err(|_| "EXTERNAL_DB_TOKEN が設定されていません")?;

    let db = libsql::Builder::new_remote(url, token)
        .build()
        .await?;

    tracing::info!("外部統計データベース接続を初期化しました");
    Ok(db)
}
