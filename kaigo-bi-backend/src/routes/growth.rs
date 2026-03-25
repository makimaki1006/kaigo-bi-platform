/// 成長性分析APIエンドポイント
/// 施設の設立年推移・事業年数分布を返す
/// キャッシュファースト: フィルタなしならkpi_cacheから即座に返す

use axum::{extract::Query, extract::State, routing::get, Json, Router};
use serde_json::Value;

use crate::error::AppError;
use crate::models::filters::FilterParams;
use crate::routes::SharedState;
use crate::services::sql_aggregator;

/// 成長性分析ルーター
pub fn router() -> Router<SharedState> {
    Router::new()
        .route("/api/growth/kpi", get(get_growth_kpi))
        .route(
            "/api/growth/establishment-trend",
            get(get_establishment_trend),
        )
        .route(
            "/api/growth/years-distribution",
            get(get_years_distribution),
        )
}

/// GET /api/growth/kpi
async fn get_growth_kpi(
    State(state): State<SharedState>,
    Query(params): Query<FilterParams>,
) -> Result<Json<Value>, AppError> {
    if params.is_default() {
        if let Some(cached) = state.cache_store.get_global("growth_kpi") {
            return Ok(Json(cached.clone()));
        }
    }
    let result = sql_aggregator::growth_kpi(&state.db, &params).await?;
    Ok(Json(result))
}

/// GET /api/growth/establishment-trend
async fn get_establishment_trend(
    State(state): State<SharedState>,
    Query(params): Query<FilterParams>,
) -> Result<Json<Value>, AppError> {
    if params.is_default() {
        if let Some(cached) = state.cache_store.get_global("growth_trend") {
            return Ok(Json(cached.clone()));
        }
    }
    let result = sql_aggregator::growth_establishment_trend(&state.db, &params).await?;
    Ok(Json(result))
}

/// GET /api/growth/years-distribution
async fn get_years_distribution(
    State(state): State<SharedState>,
    Query(params): Query<FilterParams>,
) -> Result<Json<Value>, AppError> {
    if params.is_default() {
        if let Some(cached) = state.cache_store.get_global("growth_years_dist") {
            return Ok(Json(cached.clone()));
        }
    }
    let result = sql_aggregator::growth_years_distribution(&state.db, &params).await?;
    Ok(Json(result))
}
