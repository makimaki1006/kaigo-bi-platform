/// 施設APIエンドポイント
/// 施設検索（ページネーション+テキスト検索+ソート）と施設詳細
/// Turso SQLで直接クエリ

use axum::{
    extract::{Path, Query, State},
    routing::get,
    Json, Router,
};
use serde_json::Value;

use crate::error::AppError;
use crate::models::filters::SearchParams;
use crate::routes::SharedState;
use crate::services::sql_aggregator;

/// 施設ルーター
pub fn router() -> Router<SharedState> {
    Router::new()
        .route("/api/facilities/search", get(search_facilities))
        .route("/api/facilities/:id", get(get_facility_detail))
}

/// GET /api/facilities/search
/// テキスト検索 + フィルタ + ソート + ページネーション
async fn search_facilities(
    State(state): State<SharedState>,
    Query(params): Query<SearchParams>,
) -> Result<Json<Value>, AppError> {
    let result = sql_aggregator::search_facilities(&state.db, &params).await?;
    Ok(Json(result))
}

/// GET /api/facilities/:id
/// 事業所番号で施設詳細を取得
async fn get_facility_detail(
    State(state): State<SharedState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, AppError> {
    let result = sql_aggregator::facility_detail(&state.db, &id).await?;
    Ok(Json(result))
}
