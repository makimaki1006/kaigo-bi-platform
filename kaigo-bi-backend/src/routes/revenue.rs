/// 収益構造APIエンドポイント
/// KPI、加算取得率、稼働率分布を返す
/// キャッシュファースト: フィルタなしならkpi_cacheから即座に返す

use axum::{extract::Query, extract::State, routing::get, Json, Router};
use serde_json::Value;

use crate::error::AppError;
use crate::models::filters::FilterParams;
use crate::routes::SharedState;
use crate::services::sql_aggregator;

/// 収益構造ルーター
pub fn router() -> Router<SharedState> {
    Router::new()
        .route("/api/revenue/kpi", get(get_revenue_kpi))
        .route("/api/revenue/kasan-rates", get(get_kasan_rates))
        .route(
            "/api/revenue/occupancy-distribution",
            get(get_occupancy_distribution),
        )
        .route("/api/revenue/kasan-all-items", get(get_kasan_all_items))
}

/// GET /api/revenue/kpi
async fn get_revenue_kpi(
    State(state): State<SharedState>,
    Query(params): Query<FilterParams>,
) -> Result<Json<Value>, AppError> {
    if params.is_default() {
        if let Some(cached) = state.cache_store.get_global("revenue_kpi") {
            return Ok(Json(cached.clone()));
        }
    }
    let result = sql_aggregator::revenue_kpi(&state.db, &params).await?;
    Ok(Json(result))
}

/// GET /api/revenue/kasan-rates
async fn get_kasan_rates(
    State(state): State<SharedState>,
    Query(params): Query<FilterParams>,
) -> Result<Json<Value>, AppError> {
    if params.is_default() {
        if let Some(cached) = state.cache_store.get_global("revenue_kasan_rates") {
            return Ok(Json(cached.clone()));
        }
    }
    let result = sql_aggregator::revenue_kasan_rates(&state.db, &params).await?;
    Ok(Json(result))
}

/// GET /api/revenue/occupancy-distribution
async fn get_occupancy_distribution(
    State(state): State<SharedState>,
    Query(params): Query<FilterParams>,
) -> Result<Json<Value>, AppError> {
    if params.is_default() {
        if let Some(cached) = state.cache_store.get_global("revenue_occupancy_dist") {
            return Ok(Json(cached.clone()));
        }
    }
    let result = sql_aggregator::revenue_occupancy_distribution(&state.db, &params).await?;
    Ok(Json(result))
}

/// GET /api/revenue/kasan-all-items
async fn get_kasan_all_items(
    State(state): State<SharedState>,
    Query(params): Query<FilterParams>,
) -> Result<Json<Value>, AppError> {
    if params.is_default() {
        if let Some(cached) = state.cache_store.get_global("kasan_all_items") {
            return Ok(Json(cached.clone()));
        }
    }
    let result = sql_aggregator::kasan_all_items(&state.db, &params).await?;
    Ok(Json(result))
}
