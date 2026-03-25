/// 法人グループ分析APIエンドポイント
/// 法人番号でgroupbyして法人単位の集計を行う
/// キャッシュファースト: フィルタなしならkpi_cacheから即座に返す

use axum::{extract::Query, extract::State, routing::get, Json, Router};
use serde::Deserialize;
use serde_json::Value;

use crate::error::AppError;
use crate::models::filters::FilterParams;
use crate::routes::SharedState;
use crate::services::sql_aggregator;

/// 法人グループ分析ルーター
pub fn router() -> Router<SharedState> {
    Router::new()
        .route("/api/corp-group/kpi", get(get_corp_group_kpi))
        .route(
            "/api/corp-group/size-distribution",
            get(get_size_distribution),
        )
        .route("/api/corp-group/top-corps", get(get_top_corps))
        .route(
            "/api/corp-group/kasan-heatmap",
            get(get_kasan_heatmap),
        )
}

/// top-corps用のクエリパラメータ
#[derive(Debug, Deserialize)]
pub struct TopCorpsParams {
    /// フィルタ条件（共通）
    #[serde(flatten)]
    pub filter: FilterParams,
    /// 取得件数（デフォルト: 20）
    pub limit: Option<usize>,
}

/// GET /api/corp-group/kpi
async fn get_corp_group_kpi(
    State(state): State<SharedState>,
    Query(params): Query<FilterParams>,
) -> Result<Json<Value>, AppError> {
    if params.is_default() {
        if let Some(cached) = state.cache_store.get_global("corp_group_kpi") {
            return Ok(Json(cached.clone()));
        }
    }
    let result = sql_aggregator::corp_group_kpi(&state.db, &params).await?;
    Ok(Json(result))
}

/// GET /api/corp-group/size-distribution
async fn get_size_distribution(
    State(state): State<SharedState>,
    Query(params): Query<FilterParams>,
) -> Result<Json<Value>, AppError> {
    if params.is_default() {
        if let Some(cached) = state.cache_store.get_global("corp_group_size_dist") {
            return Ok(Json(cached.clone()));
        }
    }
    let result = sql_aggregator::corp_group_size_distribution(&state.db, &params).await?;
    Ok(Json(result))
}

/// GET /api/corp-group/top-corps?limit=20
async fn get_top_corps(
    State(state): State<SharedState>,
    Query(params): Query<TopCorpsParams>,
) -> Result<Json<Value>, AppError> {
    if params.filter.is_default() && params.limit.is_none() {
        if let Some(cached) = state.cache_store.get_global("corp_group_top_corps") {
            return Ok(Json(cached.clone()));
        }
    }
    let limit = params.limit.unwrap_or(20);
    let result = sql_aggregator::corp_group_top_corps(&state.db, &params.filter, limit).await?;
    Ok(Json(result))
}

/// GET /api/corp-group/kasan-heatmap
async fn get_kasan_heatmap(
    State(state): State<SharedState>,
    Query(params): Query<FilterParams>,
) -> Result<Json<Value>, AppError> {
    if params.is_default() {
        if let Some(cached) = state.cache_store.get_global("corp_group_kasan_heatmap") {
            return Ok(Json(cached.clone()));
        }
    }
    let result = sql_aggregator::corp_group_kasan_heatmap(&state.db, &params, 10).await?;
    Ok(Json(result))
}
