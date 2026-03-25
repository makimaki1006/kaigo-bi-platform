/// マーケット分析APIエンドポイント
/// コロプレスマップ、サービス別棒グラフ、法人種別ドーナツチャート
/// キャッシュファースト: フィルタなしならkpi_cacheから即座に返す

use axum::{extract::Query, extract::State, routing::get, Json, Router};
use serde_json::Value;

use crate::error::AppError;
use crate::models::filters::FilterParams;
use crate::routes::SharedState;
use crate::services::sql_aggregator;

/// マーケット分析ルーター
pub fn router() -> Router<SharedState> {
    Router::new()
        .route("/api/market/choropleth", get(get_choropleth))
        .route("/api/market/by-service-bar", get(get_service_bar))
        .route("/api/market/corp-type-donut", get(get_corp_type_donut))
}

/// GET /api/market/choropleth
async fn get_choropleth(
    State(state): State<SharedState>,
    Query(params): Query<FilterParams>,
) -> Result<Json<Value>, AppError> {
    if params.is_default() {
        if let Some(cached) = state.cache_store.get_global("market_choropleth") {
            return Ok(Json(cached.clone()));
        }
    }
    let result = sql_aggregator::market_choropleth(&state.db, &params).await?;
    Ok(Json(result))
}

/// GET /api/market/by-service-bar
async fn get_service_bar(
    State(state): State<SharedState>,
    Query(params): Query<FilterParams>,
) -> Result<Json<Value>, AppError> {
    if params.is_default() {
        if let Some(cached) = state.cache_store.get_global("market_by_service") {
            return Ok(Json(cached.clone()));
        }
    }
    let result = sql_aggregator::market_by_service_bar(&state.db, &params).await?;
    Ok(Json(result))
}

/// GET /api/market/corp-type-donut
async fn get_corp_type_donut(
    State(state): State<SharedState>,
    Query(params): Query<FilterParams>,
) -> Result<Json<Value>, AppError> {
    if params.is_default() {
        if let Some(cached) = state.cache_store.get_global("market_corp_donut") {
            return Ok(Json(cached.clone()));
        }
    }
    let result = sql_aggregator::market_corp_type_donut(&state.db, &params).await?;
    Ok(Json(result))
}
