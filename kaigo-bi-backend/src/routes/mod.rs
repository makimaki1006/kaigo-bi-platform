/// ルーター定義
/// 全APIエンドポイントをまとめてAxumルーターに登録
/// 認証不要: /api/auth/login, /api/health
/// 認証必須: その他全API
/// admin専用: /api/users/*

pub mod auth;
pub mod benchmark;
pub mod billing;
pub mod corp_group;
pub mod dashboard;
pub mod due_diligence;
pub mod export;
pub mod external;
pub mod facilities;
pub mod growth;
pub mod ma_screening;
pub mod market;
pub mod meta;
pub mod pmi;
pub mod quality;
pub mod revenue;
pub mod salary;
pub mod users;
pub mod workforce;

use axum::{middleware, Router};
use libsql::Database;
use std::sync::Arc;

use crate::auth::middleware::auth_middleware;
use crate::auth::plan::require_plan;
use crate::services::cache_store::CacheStore;

/// アプリケーション共有状態（CacheStore + Database）
/// DataStoreは削除済み: 全データアクセスはCacheStore（事前計算）またはTurso SQL（フィルタ付き）で処理
#[derive(Clone)]
pub struct AppStateInner {
    /// KPIキャッシュストア（事前計算済みJSON）
    pub cache_store: Arc<CacheStore>,
    /// Tursoデータベース接続（フィルタ付きクエリ用）
    pub db: Arc<Database>,
    /// 外部統計データベース接続（オプション、接続失敗時はNone）
    pub external_db: Option<Arc<Database>>,
}

/// Axum State型
pub type SharedState = Arc<AppStateInner>;

/// 全ルートを結合してルーターを返す
///
/// プラン別アクセス制御（role=adminは全ゲートをバイパス）:
///   free    : dashboard, meta（全国サマリーのみ）
///   standard: + market, workforce, revenue, salary, quality, corp_group, growth,
///               facilities, benchmark, external（BI全機能）
///   pro     : + export（リストCSVダウンロード、月間クレジット制）
///   ma      : + ma_screening, due_diligence, pmi（M&A分析）
pub fn create_router(state: SharedState) -> Router {
    // 認証不要のルート（ログイン・サインアップ・ヘルスチェック・Stripe Webhook）
    let public_routes = Router::new()
        .merge(auth::public_router())
        .merge(billing::public_router())
        .merge(health_router());

    // フリープランでも閲覧可能なルート（全国サマリーダッシュボード + メタ情報）
    let free_routes = Router::new()
        .merge(dashboard::router())
        .merge(market::free_router())
        .merge(meta::router())
        .with_state(state.clone());

    // スタンダード以上: BI全機能
    let standard_routes = Router::new()
        .merge(market::router())
        .merge(workforce::router())
        .merge(revenue::router())
        .merge(salary::router())
        .merge(quality::router())
        .merge(corp_group::router())
        .merge(growth::router())
        .merge(facilities::router())
        .merge(benchmark::router())
        .with_state(state.clone())
        .layer(middleware::from_fn_with_state(state.clone(), |st, req, next| {
            require_plan("standard", st, req, next)
        }));

    // プロ以上: リストCSVエクスポート（月間クレジットはハンドラ内で制御）
    let pro_routes = Router::new()
        .merge(export::router())
        .with_state(state.clone())
        .layer(middleware::from_fn_with_state(state.clone(), |st, req, next| {
            require_plan("pro", st, req, next)
        }));

    // M&Aプラン: スクリーニング・デューデリ・PMI
    let ma_routes = Router::new()
        .merge(ma_screening::router())
        .merge(due_diligence::router())
        .merge(pmi::router())
        .with_state(state.clone())
        .layer(middleware::from_fn_with_state(state.clone(), |st, req, next| {
            require_plan("ma", st, req, next)
        }));

    // 認証必須の認証系ルート（me, logout, refresh）+ 課金操作
    let protected_auth_routes = Router::new()
        .merge(auth::protected_router())
        .merge(billing::protected_router())
        .with_state(state.clone())
        .layer(middleware::from_fn(auth_middleware));

    // 認証必須のデータルート（内側でプラン別ゲートが効く）
    let protected_data_routes = Router::new()
        .merge(free_routes)
        .merge(standard_routes)
        .merge(pro_routes)
        .merge(ma_routes)
        .layer(middleware::from_fn(auth_middleware));

    // admin専用ルート（認証ミドルウェア適用）
    let admin_routes = Router::new()
        .merge(users::router())
        .with_state(state.clone())
        .layer(middleware::from_fn(auth_middleware));

    // 外部統計データルート（認証必須 + スタンダード以上）
    let external_routes = Router::new()
        .merge(external::router())
        .with_state(state.clone())
        .layer(middleware::from_fn_with_state(state.clone(), |st, req, next| {
            require_plan("standard", st, req, next)
        }))
        .layer(middleware::from_fn(auth_middleware));

    // 全ルートを統合
    Router::new()
        .merge(public_routes.with_state(state))
        .merge(protected_auth_routes)
        .merge(protected_data_routes)
        .merge(admin_routes)
        .merge(external_routes)
}

/// ヘルスチェックルート
fn health_router() -> Router<SharedState> {
    use axum::routing::get;
    Router::new().route("/api/health", get(health_check))
}

/// GET /api/health - ヘルスチェック（認証不要）
async fn health_check() -> axum::Json<serde_json::Value> {
    axum::Json(serde_json::json!({
        "status": "ok",
        "timestamp": chrono::Utc::now().to_rfc3339(),
    }))
}
