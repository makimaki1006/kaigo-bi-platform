/// 施設ベンチマークAPIエンドポイント
/// 指定事業所の8軸レーダー + パーセンタイル + 改善提案を返す
/// Turso SQLで直接クエリ

use axum::{extract::Path, extract::State, routing::get, Json, Router};
use serde_json::Value;

use crate::error::AppError;
use crate::routes::SharedState;
use crate::services::sql_aggregator;

/// ベンチマークルーター
pub fn router() -> Router<SharedState> {
    Router::new().route(
        "/api/benchmark/:jigyosho_number",
        get(get_benchmark),
    )
}

/// GET /api/benchmark/{jigyosho_number}
/// 施設ベンチマーク（8軸レーダー + パーセンタイル + 改善提案）
/// 全データに対してベンチマークを計算（フィルタなし）
async fn get_benchmark(
    State(state): State<SharedState>,
    Path(jigyosho_number): Path<String>,
) -> Result<Json<Value>, AppError> {
    let result = sql_aggregator::benchmark(&state.db, &jigyosho_number).await?;
    Ok(Json(result))
}
