/// CSVエクスポートAPIエンドポイント
/// BOM付きUTF-8 CSVストリームを返す
/// Turso SQLで直接クエリ

use axum::{
    extract::{Query, State},
    http::header,
    response::IntoResponse,
    routing::get,
    Router,
};

use crate::error::AppError;
use crate::models::filters::FilterParams;
use crate::routes::SharedState;
use crate::services::sql_aggregator;

/// エクスポートルーター
pub fn router() -> Router<SharedState> {
    Router::new().route("/api/export/csv", get(export_csv))
}

/// GET /api/export/csv
/// フィルタ条件に基づいてCSVをダウンロード
/// Content-Type: text/csv; charset=utf-8
/// Content-Disposition: attachment; filename="kaigo_data.csv"
async fn export_csv(
    State(state): State<SharedState>,
    Query(params): Query<FilterParams>,
) -> Result<impl IntoResponse, AppError> {
    // Turso SQLでCSVバイト列を生成
    let csv_bytes = sql_aggregator::export_csv(&state.db, &params).await?;

    // レスポンスヘッダー設定
    let headers = [
        (
            header::CONTENT_TYPE,
            "text/csv; charset=utf-8".to_string(),
        ),
        (
            header::CONTENT_DISPOSITION,
            "attachment; filename=\"kaigo_data.csv\"".to_string(),
        ),
    ];

    Ok((headers, csv_bytes))
}
