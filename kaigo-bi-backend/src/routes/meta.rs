/// メタ情報APIエンドポイント
/// 総件数、選択肢リスト（都道府県、サービスコード、法人種別）、従業者数範囲
/// キャッシュファースト: kpi_cacheにmetaキーがあればそれを返す

use axum::{extract::State, routing::get, Json, Router};
use serde_json::Value;

use crate::error::AppError;
use crate::routes::SharedState;
use crate::services::sql_aggregator;

/// メタ情報ルーター
pub fn router() -> Router<SharedState> {
    Router::new().route("/api/meta", get(get_meta))
}

/// GET /api/meta
/// フィルタなしの全体メタ情報を返す
/// フロントエンドのフィルタUIの選択肢リスト構築に使用
async fn get_meta(State(state): State<SharedState>) -> Result<Json<Value>, AppError> {
    // キャッシュからmetaを返す
    if let Some(cached) = state.cache_store.get_global("meta") {
        return Ok(Json(cached.clone()));
    }
    // フォールバック: Turso SQLから計算
    let result = sql_aggregator::meta(&state.db).await?;
    Ok(Json(result))
}
