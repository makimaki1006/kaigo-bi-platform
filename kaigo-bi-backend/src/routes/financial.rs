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
