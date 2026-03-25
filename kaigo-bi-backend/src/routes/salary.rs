/// 賃金分析APIエンドポイント
/// KPI、職種別賃金、都道府県別賃金を返す
/// キャッシュファースト: フィルタなしならkpi_cacheから即座に返す

use axum::{extract::Query, extract::State, routing::get, Json, Router};
use serde_json::Value;

use crate::error::AppError;
use crate::models::filters::FilterParams;
use crate::routes::SharedState;
use crate::services::sql_aggregator;

/// 賃金分析ルーター
pub fn router() -> Router<SharedState> {
    Router::new()
        .route("/api/salary/kpi", get(get_salary_kpi))
        .route("/api/salary/by-job-type", get(get_salary_by_job_type))
        .route(
            "/api/salary/by-prefecture",
            get(get_salary_by_prefecture),
        )
}

/// GET /api/salary/kpi
async fn get_salary_kpi(
    State(state): State<SharedState>,
    Query(params): Query<FilterParams>,
) -> Result<Json<Value>, AppError> {
    if params.is_default() {
        if let Some(cached) = state.cache_store.get_global("salary_kpi") {
            return Ok(Json(cached.clone()));
        }
    }
    let result = sql_aggregator::salary_kpi(&state.db, &params).await?;
    Ok(Json(result))
}

/// GET /api/salary/by-job-type
async fn get_salary_by_job_type(
    State(state): State<SharedState>,
    Query(params): Query<FilterParams>,
) -> Result<Json<Value>, AppError> {
    if params.is_default() {
        if let Some(cached) = state.cache_store.get_global("salary_by_job_type") {
            return Ok(Json(cached.clone()));
        }
    }
    let result = sql_aggregator::salary_by_job_type(&state.db, &params).await?;
    Ok(Json(result))
}

/// GET /api/salary/by-prefecture
async fn get_salary_by_prefecture(
    State(state): State<SharedState>,
    Query(params): Query<FilterParams>,
) -> Result<Json<Value>, AppError> {
    if params.is_default() {
        if let Some(cached) = state.cache_store.get_global("salary_by_prefecture") {
            return Ok(Json(cached.clone()));
        }
    }
    let result = sql_aggregator::salary_by_prefecture(&state.db, &params).await?;
    Ok(Json(result))
}
