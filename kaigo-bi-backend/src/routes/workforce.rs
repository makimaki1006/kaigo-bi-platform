/// 人材分析APIエンドポイント
/// KPI、離職率分布、都道府県別、従業者規模別の人材指標を返す
/// キャッシュファースト: フィルタなしならkpi_cacheから即座に返す

use axum::{extract::Query, extract::State, routing::get, Json, Router};
use serde_json::Value;

use crate::error::AppError;
use crate::models::filters::FilterParams;
use crate::routes::SharedState;
use crate::services::sql_aggregator;

/// 人材分析ルーター
pub fn router() -> Router<SharedState> {
    Router::new()
        .route("/api/workforce/kpi", get(get_workforce_kpi))
        .route(
            "/api/workforce/turnover-distribution",
            get(get_turnover_distribution),
        )
        .route(
            "/api/workforce/by-prefecture",
            get(get_workforce_by_prefecture),
        )
        .route("/api/workforce/by-size", get(get_workforce_by_size))
        .route(
            "/api/workforce/experience-distribution",
            get(get_experience_distribution),
        )
        .route(
            "/api/workforce/experience-vs-turnover",
            get(get_experience_vs_turnover),
        )
        .route("/api/workforce/staff-breakdown", get(get_staff_breakdown))
        .route("/api/workforce/qualifications", get(get_qualifications))
        .route("/api/workforce/night-shift", get(get_night_shift))
        .route("/api/workforce/dementia-training", get(get_dementia_training))
}

/// GET /api/workforce/kpi
async fn get_workforce_kpi(
    State(state): State<SharedState>,
    Query(params): Query<FilterParams>,
) -> Result<Json<Value>, AppError> {
    if params.is_default() {
        if let Some(cached) = state.cache_store.get_global("workforce_kpi") {
            return Ok(Json(cached.clone()));
        }
    }
    let result = sql_aggregator::workforce_kpi(&state.db, &params).await?;
    Ok(Json(result))
}

/// GET /api/workforce/turnover-distribution
async fn get_turnover_distribution(
    State(state): State<SharedState>,
    Query(params): Query<FilterParams>,
) -> Result<Json<Value>, AppError> {
    if params.is_default() {
        if let Some(cached) = state.cache_store.get_global("workforce_turnover_dist") {
            return Ok(Json(cached.clone()));
        }
    }
    let result = sql_aggregator::workforce_turnover_distribution(&state.db, &params).await?;
    Ok(Json(result))
}

/// GET /api/workforce/by-prefecture
async fn get_workforce_by_prefecture(
    State(state): State<SharedState>,
    Query(params): Query<FilterParams>,
) -> Result<Json<Value>, AppError> {
    if params.is_default() {
        if let Some(cached) = state.cache_store.get_global("workforce_by_prefecture") {
            return Ok(Json(cached.clone()));
        }
    }
    let result = sql_aggregator::workforce_by_prefecture(&state.db, &params).await?;
    Ok(Json(result))
}

/// GET /api/workforce/by-size
async fn get_workforce_by_size(
    State(state): State<SharedState>,
    Query(params): Query<FilterParams>,
) -> Result<Json<Value>, AppError> {
    if params.is_default() {
        if let Some(cached) = state.cache_store.get_global("workforce_by_size") {
            return Ok(Json(cached.clone()));
        }
    }
    let result = sql_aggregator::workforce_by_size(&state.db, &params).await?;
    Ok(Json(result))
}

/// GET /api/workforce/experience-distribution
async fn get_experience_distribution(
    State(state): State<SharedState>,
    Query(params): Query<FilterParams>,
) -> Result<Json<Value>, AppError> {
    if params.is_default() {
        if let Some(cached) = state.cache_store.get_global("workforce_exp_dist") {
            return Ok(Json(cached.clone()));
        }
    }
    let result = sql_aggregator::workforce_experience_distribution(&state.db, &params).await?;
    Ok(Json(result))
}

/// GET /api/workforce/experience-vs-turnover
async fn get_experience_vs_turnover(
    State(state): State<SharedState>,
    Query(params): Query<FilterParams>,
) -> Result<Json<Value>, AppError> {
    if params.is_default() {
        if let Some(cached) = state.cache_store.get_global("workforce_exp_turnover") {
            return Ok(Json(cached.clone()));
        }
    }
    let result = sql_aggregator::workforce_experience_vs_turnover(&state.db, &params).await?;
    Ok(Json(result))
}

/// GET /api/workforce/staff-breakdown
async fn get_staff_breakdown(
    State(state): State<SharedState>,
    Query(params): Query<FilterParams>,
) -> Result<Json<Value>, AppError> {
    if params.is_default() {
        if let Some(cached) = state.cache_store.get_global("workforce_staff_breakdown") {
            return Ok(Json(cached.clone()));
        }
    }
    Ok(Json(serde_json::json!([])))
}

/// GET /api/workforce/qualifications
async fn get_qualifications(
    State(state): State<SharedState>,
    Query(params): Query<FilterParams>,
) -> Result<Json<Value>, AppError> {
    if params.is_default() {
        if let Some(cached) = state.cache_store.get_global("workforce_qualifications") {
            return Ok(Json(cached.clone()));
        }
    }
    Ok(Json(serde_json::json!([])))
}

/// GET /api/workforce/night-shift
async fn get_night_shift(
    State(state): State<SharedState>,
    Query(params): Query<FilterParams>,
) -> Result<Json<Value>, AppError> {
    if params.is_default() {
        if let Some(cached) = state.cache_store.get_global("workforce_night_shift") {
            return Ok(Json(cached.clone()));
        }
    }
    Ok(Json(serde_json::json!({})))
}

/// GET /api/workforce/dementia-training
async fn get_dementia_training(
    State(state): State<SharedState>,
    Query(params): Query<FilterParams>,
) -> Result<Json<Value>, AppError> {
    if params.is_default() {
        if let Some(cached) = state.cache_store.get_global("workforce_dementia_training") {
            return Ok(Json(cached.clone()));
        }
    }
    Ok(Json(serde_json::json!([])))
}
