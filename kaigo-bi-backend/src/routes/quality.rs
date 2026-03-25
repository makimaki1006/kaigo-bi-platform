/// 経営品質APIエンドポイント
/// KPI、スコア分布、ランク分布、レーダーチャート、都道府県別品質を返す
/// キャッシュファースト: フィルタなしならkpi_cacheから即座に返す

use axum::{extract::Query, extract::State, routing::get, Json, Router};
use serde_json::Value;

use crate::error::AppError;
use crate::models::filters::FilterParams;
use crate::routes::SharedState;
use crate::services::sql_aggregator;

/// 経営品質ルーター
pub fn router() -> Router<SharedState> {
    Router::new()
        .route("/api/quality/kpi", get(get_quality_kpi))
        .route(
            "/api/quality/score-distribution",
            get(get_quality_score_distribution),
        )
        .route(
            "/api/quality/by-prefecture",
            get(get_quality_by_prefecture),
        )
        .route(
            "/api/quality/rank-distribution",
            get(get_quality_rank_distribution),
        )
        .route(
            "/api/quality/category-radar",
            get(get_quality_category_radar),
        )
}

/// GET /api/quality/kpi
async fn get_quality_kpi(
    State(state): State<SharedState>,
    Query(params): Query<FilterParams>,
) -> Result<Json<Value>, AppError> {
    if params.is_default() {
        if let Some(cached) = state.cache_store.get_global("quality_kpi") {
            return Ok(Json(cached.clone()));
        }
    }
    let result = sql_aggregator::quality_kpi(&state.db, &params).await?;
    Ok(Json(result))
}

/// GET /api/quality/score-distribution
async fn get_quality_score_distribution(
    State(state): State<SharedState>,
    Query(params): Query<FilterParams>,
) -> Result<Json<Value>, AppError> {
    if params.is_default() {
        if let Some(cached) = state.cache_store.get_global("quality_score_dist") {
            return Ok(Json(cached.clone()));
        }
    }
    let result = sql_aggregator::quality_score_distribution(&state.db, &params).await?;
    Ok(Json(result))
}

/// GET /api/quality/by-prefecture
async fn get_quality_by_prefecture(
    State(state): State<SharedState>,
    Query(params): Query<FilterParams>,
) -> Result<Json<Value>, AppError> {
    if params.is_default() {
        if let Some(cached) = state.cache_store.get_global("quality_by_prefecture") {
            return Ok(Json(cached.clone()));
        }
    }
    let result = sql_aggregator::quality_by_prefecture(&state.db, &params).await?;
    Ok(Json(result))
}

/// GET /api/quality/rank-distribution
async fn get_quality_rank_distribution(
    State(state): State<SharedState>,
    Query(params): Query<FilterParams>,
) -> Result<Json<Value>, AppError> {
    if params.is_default() {
        if let Some(cached) = state.cache_store.get_global("quality_rank_dist") {
            return Ok(Json(cached.clone()));
        }
    }
    let result = sql_aggregator::quality_rank_distribution(&state.db, &params).await?;
    Ok(Json(result))
}

/// GET /api/quality/category-radar
async fn get_quality_category_radar(
    State(state): State<SharedState>,
    Query(params): Query<FilterParams>,
) -> Result<Json<Value>, AppError> {
    if params.is_default() {
        if let Some(cached) = state.cache_store.get_global("quality_radar") {
            return Ok(Json(cached.clone()));
        }
    }
    let result = sql_aggregator::quality_category_radar(&state.db, &params).await?;
    Ok(Json(result))
}
