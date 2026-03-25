/// 介護事業所BIバックエンド
/// Axum + Turso による軽量APIサーバー
/// データアクセス: CacheStore（事前計算済みKPI） + Turso SQL（フィルタ付きクエリ）

mod auth;
mod config;
mod db;
mod error;
mod models;
mod routes;
mod services;
mod utils;

use std::sync::Arc;
use axum::http::HeaderValue;
use tower_http::cors::{Any, CorsLayer};
use tracing::info;

use crate::config::AppConfig;
use crate::routes::AppStateInner;
use crate::services::cache_store::CacheStore;

#[tokio::main]
async fn main() {
    // .env読み込み
    dotenvy::dotenv().ok();

    // ログ初期化
    tracing_subscriber::fmt()
        .with_target(false)
        .with_level(true)
        .init();

    // 設定読み込み
    let config = AppConfig::from_env();
    info!("ポート: {}", config.port);

    // Tursoデータベース初期化
    let database = db::init_db()
        .await
        .expect("Tursoデータベースの初期化に失敗しました");
    info!("Tursoデータベース接続完了");

    // 外部統計データベース初期化（オプション: 失敗しても起動は継続）
    let external_db = match db::init_external_db().await {
        Ok(db) => {
            info!("外部統計データベース接続完了");
            Some(Arc::new(db))
        }
        Err(e) => {
            tracing::warn!("外部統計DB接続失敗（関連機能は無効化されます）: {}", e);
            None
        }
    };

    // KPIキャッシュ読み込み（kpi_cacheテーブルから事前計算済みJSONを読み込み）
    let cache_store = CacheStore::load(&database)
        .await
        .expect("KPIキャッシュの読み込みに失敗しました");
    info!("CacheStore初期化完了: {}キー", cache_store.keys().len());

    // DataStoreは不要: 全データアクセスはCacheStoreまたはTurso SQLで処理
    // 起動時間: 数秒、メモリ使用量: ~50MB（以前: 2分、~500MB）

    // 共有状態を構築
    let state = Arc::new(AppStateInner {
        cache_store: Arc::new(cache_store),
        db: Arc::new(database),
        external_db,
    });

    // CORS設定（ALLOWED_ORIGINSが設定されていればそのオリジンのみ許可、未設定なら全オリジン許可）
    let cors = if let Ok(origins) = std::env::var("ALLOWED_ORIGINS") {
        let origin_values: Vec<HeaderValue> = origins
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        info!("CORS: 許可オリジン = {:?}", origin_values);
        CorsLayer::new()
            .allow_origin(origin_values)
            .allow_methods(Any)
            .allow_headers(Any)
    } else {
        info!("CORS: ALLOWED_ORIGINS未設定のため全オリジン許可（開発モード）");
        CorsLayer::new()
            .allow_origin(Any)
            .allow_methods(Any)
            .allow_headers(Any)
    };

    // ルーター構築
    let app = routes::create_router(state).layer(cors);

    // サーバー起動
    let addr = format!("0.0.0.0:{}", config.port);
    info!("サーバー起動: http://localhost:{}", config.port);

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .expect("ポートのバインドに失敗しました");

    axum::serve(listener, app)
        .await
        .expect("サーバーの起動に失敗しました");
}
