/// PMI（Post Merger Integration）シナジー分析APIエンドポイント
/// 2法人の統合シミュレーションを実行
/// Turso SQLで直接クエリ

use axum::{extract::Query, extract::State, routing::get, Json, Router};
use serde::Deserialize;
use serde_json::Value;

use crate::error::AppError;
use crate::models::filters::FilterParams;
use crate::routes::SharedState;
use crate::services::sql_aggregator;

/// PMIシナジールーター
pub fn router() -> Router<SharedState> {
    Router::new().route("/api/pmi/simulate", get(simulate_pmi))
}

/// PMIシミュレーション用クエリパラメータ
#[derive(Debug, Deserialize)]
pub struct PmiParams {
    /// 共通フィルタ
    #[serde(flatten)]
    pub filter: FilterParams,
    /// 買収側法人番号
    pub buyer_corp: String,
    /// ターゲット法人番号
    pub target_corp: String,
}

/// GET /api/pmi/simulate?buyer_corp=123&target_corp=456
/// 2法人の統合シミュレーション
async fn simulate_pmi(
    State(state): State<SharedState>,
    Query(params): Query<PmiParams>,
) -> Result<Json<Value>, AppError> {
    let result = sql_aggregator::pmi_simulation(
        &state.db,
        &params.filter,
        &params.buyer_corp,
        &params.target_corp,
    ).await?;
    Ok(Json(result))
}
