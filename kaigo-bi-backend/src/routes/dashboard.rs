/// ダッシュボードAPIエンドポイント
/// KPI、都道府県別、サービス別の集計結果を返す
/// キャッシュファースト: フィルタなしならkpi_cacheから即座に返し、
/// フィルタありならTurso SQLフォールバック

use axum::{extract::Query, extract::State, routing::get, Json, Router};
use serde_json::Value;

use crate::error::AppError;
use crate::models::filters::FilterParams;
use crate::routes::SharedState;
use crate::services::sql_aggregator;

/// ダッシュボードルーター
pub fn router() -> Router<SharedState> {
    Router::new()
        .route("/api/dashboard/kpi", get(get_kpi))
        .route("/api/dashboard/by-prefecture", get(get_by_prefecture))
        .route("/api/dashboard/by-service", get(get_by_service))
}

/// GET /api/dashboard/kpi
/// フィルタ条件に基づくKPIを返す
async fn get_kpi(
    State(state): State<SharedState>,
    Query(params): Query<FilterParams>,
) -> Result<Json<Value>, AppError> {
    // キャッシュファースト: フィルタなしならキャッシュから返す
    if params.is_default() {
        if let Some(cached) = state.cache_store.get_global("dashboard_kpi") {
            return Ok(Json(cached.clone()));
        }
    }
    // フォールバック: Turso SQLで集計
    let result = sql_aggregator::dashboard_kpi(&state.db, &params).await?;
    Ok(Json(result))
}

/// GET /api/dashboard/by-prefecture
/// 都道府県別のサマリーを返す
async fn get_by_prefecture(
    State(state): State<SharedState>,
    Query(params): Query<FilterParams>,
) -> Result<Json<Value>, AppError> {
    if params.is_default() {
        if let Some(cached) = state.cache_store.get_global("dashboard_by_prefecture") {
            return Ok(Json(cached.clone()));
        }
    }
    let result = sql_aggregator::dashboard_by_prefecture(&state.db, &params).await?;
    Ok(Json(result))
}

/// GET /api/dashboard/by-service
/// サービス別のサマリーを返す
async fn get_by_service(
    State(state): State<SharedState>,
    Query(params): Query<FilterParams>,
) -> Result<Json<Value>, AppError> {
    if params.is_default() {
        if let Some(cached) = state.cache_store.get_global("dashboard_by_service") {
            return Ok(Json(cached.clone()));
        }
    }
    let result = sql_aggregator::dashboard_by_service(&state.db, &params).await?;
    Ok(Json(result))
}
