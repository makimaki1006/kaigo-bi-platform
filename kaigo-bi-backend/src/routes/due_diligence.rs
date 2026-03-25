/// DD（デューデリジェンス）支援APIエンドポイント
/// 法人検索とDDレポートデータを返す
/// Turso SQLで直接クエリ

use axum::{
    extract::{Path, Query, State},
    routing::get,
    Json, Router,
};
use serde::Deserialize;
use serde_json::Value;

use crate::error::AppError;
use crate::models::filters::FilterParams;
use crate::routes::SharedState;
use crate::services::sql_aggregator;

/// DD支援ルーター
pub fn router() -> Router<SharedState> {
    Router::new()
        .route("/api/dd/search", get(search_corps))
        .route("/api/dd/report/:corp_number", get(get_dd_report))
}

/// DD検索用クエリパラメータ
#[derive(Debug, Deserialize)]
pub struct DdSearchParams {
    /// 共通フィルタ
    #[serde(flatten)]
    pub filter: FilterParams,
    /// 検索クエリ（法人名部分一致 or 法人番号完全一致）
    pub q: Option<String>,
}

/// DD レポート用パスパラメータ + フィルタ
#[derive(Debug, Deserialize)]
pub struct DdReportQuery {
    /// 共通フィルタ
    #[serde(flatten)]
    pub filter: FilterParams,
}

/// GET /api/dd/search?q=法人名or法人番号
/// 法人検索
async fn search_corps(
    State(state): State<SharedState>,
    Query(params): Query<DdSearchParams>,
) -> Result<Json<Value>, AppError> {
    let query = params.q.unwrap_or_default();
    let result = sql_aggregator::dd_search(&state.db, &params.filter, &query).await?;
    Ok(Json(result))
}

/// GET /api/dd/report/:corp_number
/// 法人番号指定でDDレポートデータを返す
async fn get_dd_report(
    State(state): State<SharedState>,
    Path(corp_number): Path<String>,
    Query(params): Query<DdReportQuery>,
) -> Result<Json<Value>, AppError> {
    let result = sql_aggregator::dd_report(&state.db, &params.filter, &corp_number).await?;
    Ok(Json(result))
}
