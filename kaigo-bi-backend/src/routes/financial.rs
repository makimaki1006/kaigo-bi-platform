/// 決算書の開示状況API
///
/// 決算PDFは自由書式のアップロードで、実測（2026-08-12 / 層化サンプル518ファイル）では
///   - テキスト層があるのは 47.5%（残りはスキャン画像）
///   - PLの収益が機械抽出できたのは 24.9%
/// のため、**金額は網羅的な指標にできない**。
///
/// 一方で「決算書を出しているか / いつ出したか / どの形式か」は
/// facilities のURL列から全223,103施設で機械的に確定する。
/// このルーターはその網羅データだけを返す。金額は facility 詳細側の
/// financials（取れた分のみ・来歴付き）で扱う。
use axum::{extract::State, routing::get, Json, Router};
use serde_json::{json, Value};

use crate::error::AppError;
use crate::routes::SharedState;

pub fn router() -> Router<SharedState> {
    Router::new()
        .route("/api/financial/disclosure/kpi", get(kpi))
        .route("/api/financial/disclosure/by-prefecture", get(by_prefecture))
        .route("/api/financial/disclosure/by-corp-type", get(by_corp_type))
        .route("/api/financial/disclosure/by-service", get(by_service))
        .route("/api/financial/disclosure/freshness", get(freshness))
        .route("/api/financial/disclosure/by-acct-type", get(by_acct_type))
        .route("/api/financial/disclosure/gap", get(gap))
        .route("/api/financial/extraction-status", get(extraction_status))
        .route("/api/financial/metrics/summary", get(metrics_summary))
        .route("/api/financial/metrics/by-corp-type", get(metrics_by_corp_type))
        // 決算データ × 公表データ のクロス集計（scripts/fin_explore.py で信号を確認したもの）
        .route("/api/financial/insights/revenue-per-staff", get(revenue_per_staff))
        .route("/api/financial/insights/equity-by-corp-size", get(equity_by_corp_size))
        .route("/api/financial/insights/disclosure-vs-quality", get(disclosure_vs_quality))
        .route("/api/financial/insights/personnel-ratio-by-prefecture",
               get(personnel_ratio_by_prefecture))
}

/// kpi_cache から取り出す。未集計なら 503 ではなく空を返して画面を壊さない
fn cached(state: &SharedState, key: &str) -> Json<Value> {
    match state.cache_store.get_global(key) {
        Some(v) => Json(v.clone()),
        None => Json(json!({
            "unavailable": true,
            "key": key,
            "hint": "scripts/aggregate_financial_disclosure.py を実行してください",
        })),
    }
}

async fn kpi(State(state): State<SharedState>) -> Result<Json<Value>, AppError> {
    Ok(cached(&state, "financial_disclosure_kpi"))
}

async fn by_prefecture(State(state): State<SharedState>) -> Result<Json<Value>, AppError> {
    Ok(cached(&state, "financial_disclosure_by_prefecture"))
}

async fn by_corp_type(State(state): State<SharedState>) -> Result<Json<Value>, AppError> {
    Ok(cached(&state, "financial_disclosure_by_corp_type"))
}

async fn by_service(State(state): State<SharedState>) -> Result<Json<Value>, AppError> {
    Ok(cached(&state, "financial_disclosure_by_service"))
}

async fn freshness(State(state): State<SharedState>) -> Result<Json<Value>, AppError> {
    Ok(cached(&state, "financial_disclosure_freshness"))
}

async fn by_acct_type(State(state): State<SharedState>) -> Result<Json<Value>, AppError> {
    Ok(cached(&state, "financial_disclosure_by_acct_type"))
}

/// 未開示・更新停滞のセグメント。施設マスタの financial_status フィルタと対になる
async fn gap(State(state): State<SharedState>) -> Result<Json<Value>, AppError> {
    Ok(cached(&state, "financial_disclosure_gap"))
}

/// 金額抽出がどこまでできるかの実測値。画面に注意書きとして出すために返す
async fn extraction_status(State(state): State<SharedState>) -> Result<Json<Value>, AppError> {
    Ok(cached(&state, "financial_extraction_status"))
}

/// 抽出できた金額の中央値。全国平均ではないので n を必ず添えて返す
async fn metrics_summary(State(state): State<SharedState>) -> Result<Json<Value>, AppError> {
    Ok(cached(&state, "financial_metrics_summary"))
}

async fn metrics_by_corp_type(State(state): State<SharedState>) -> Result<Json<Value>, AppError> {
    Ok(cached(&state, "financial_metrics_by_corp_type"))
}

/// 職員1人あたり収益。集計単位が確定する施設（単一事業所・単一サービスの法人）に限る
async fn revenue_per_staff(State(state): State<SharedState>) -> Result<Json<Value>, AppError> {
    Ok(cached(&state, "financial_revenue_per_staff"))
}

/// 自己資本比率 × 法人の事業所数
async fn equity_by_corp_size(State(state): State<SharedState>) -> Result<Json<Value>, AppError> {
    Ok(cached(&state, "financial_equity_by_corp_size"))
}

/// 決算書の開示有無 × 品質スコア・離職率
async fn disclosure_vs_quality(State(state): State<SharedState>) -> Result<Json<Value>, AppError> {
    Ok(cached(&state, "financial_disclosure_vs_quality"))
}

/// 都道府県別の人件費率
async fn personnel_ratio_by_prefecture(
    State(state): State<SharedState>,
) -> Result<Json<Value>, AppError> {
    Ok(cached(&state, "financial_personnel_ratio_by_prefecture"))
}
