/// M&AスクリーニングAPIエンドポイント
/// 法人単位で集計し、フィルタ条件に合致する法人をスコア付きでリストアップ
/// Turso SQLで直接クエリ

use axum::{extract::Query, extract::State, routing::get, Json, Router};
use serde::Deserialize;
use serde_json::Value;

use crate::error::AppError;
use crate::models::filters::FilterParams;
use crate::routes::SharedState;
use crate::services::sql_aggregator;

/// M&Aスクリーニングルーター
pub fn router() -> Router<SharedState> {
    Router::new().route("/api/ma/screening", get(get_ma_screening))
}

/// M&Aスクリーニング用クエリパラメータ
#[derive(Debug, Deserialize)]
pub struct MaScreeningParams {
    /// 共通フィルタ
    #[serde(flatten)]
    pub filter: FilterParams,
    /// 都道府県（カンマ区切り）
    pub prefectures: Option<String>,
    /// 法人種別（カンマ区切り）
    pub corp_types: Option<String>,
    /// 従業者数下限
    pub staff_min: Option<f64>,
    /// 従業者数上限
    pub staff_max: Option<f64>,
    /// 離職率下限
    pub turnover_min: Option<f64>,
    /// 離職率上限
    pub turnover_max: Option<f64>,
    /// 取得件数（デフォルト: 50）
    pub limit: Option<usize>,
}

/// GET /api/ma/screening
/// M&Aスクリーニング結果（魅力度スコア付き）
async fn get_ma_screening(
    State(state): State<SharedState>,
    Query(params): Query<MaScreeningParams>,
) -> Result<Json<Value>, AppError> {
    let limit = params.limit.unwrap_or(50);
    let result = sql_aggregator::ma_screening(
        &state.db,
        &params.filter,
        &params.prefectures,
        &params.corp_types,
        params.staff_min,
        params.staff_max,
        params.turnover_min,
        params.turnover_max,
        limit,
    ).await?;
    Ok(Json(result))
}
