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
use crate::models::filters::{FilterParams, SearchParams};
use crate::routes::SharedState;
use crate::services::sql_aggregator;

/// 施設ルーター
pub fn router() -> Router<SharedState> {
    Router::new()
        .route("/api/facilities/search", get(search_facilities))
        .route("/api/facilities/nearby", get(facilities_nearby))
        .route("/api/facilities/:id", get(get_facility_detail))
}

/// 周辺検索のクエリ（中心施設・半径・件数上限）
#[derive(serde::Deserialize)]
struct NearbyQuery {
    /// 中心にする事業所番号
    center: String,
    #[serde(default)]
    radius_km: Option<f64>,
    #[serde(default)]
    limit: Option<usize>,
    /// サービス名の部分一致（FilterParams は service_code しか持たないため独自に受ける）
    #[serde(default)]
    service_name: Option<String>,
}

/// GET /api/facilities/nearby?center=<事業所番号>&radius_km=3&...
/// 指定施設を中心に、半径内の施設をフィルタ付きで返す
async fn facilities_nearby(
    State(state): State<SharedState>,
    Query(nq): Query<NearbyQuery>,
    Query(filters): Query<FilterParams>,
) -> Result<Json<Value>, AppError> {
    let result = sql_aggregator::facilities_nearby(
        &state.db,
        &filters,
        &nq.center,
        nq.radius_km.unwrap_or(3.0),
        nq.limit.unwrap_or(500).min(2000),
        nq.service_name.as_deref(),
    )
    .await?;
    Ok(Json(result))
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
