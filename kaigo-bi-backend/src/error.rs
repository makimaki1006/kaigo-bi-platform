/// アプリケーションエラー型
/// 全てのエラーをJSON形式でクライアントに返す

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

/// 統一エラー型
#[derive(Debug)]
pub enum AppError {
    /// 内部サーバーエラー（Polars処理失敗等）
    Internal(String),
    /// リソースが見つからない
    NotFound(String),
    /// サービス利用不可（外部DB未接続等）
    ServiceUnavailable(String),
}

impl std::fmt::Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AppError::Internal(msg) => write!(f, "Internal error: {}", msg),
            AppError::NotFound(msg) => write!(f, "Not found: {}", msg),
            AppError::ServiceUnavailable(msg) => write!(f, "Service unavailable: {}", msg),
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            AppError::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg.clone()),
            AppError::NotFound(msg) => (StatusCode::NOT_FOUND, msg.clone()),
            AppError::ServiceUnavailable(msg) => (StatusCode::SERVICE_UNAVAILABLE, msg.clone()),
        };

        let body = json!({
            "error": message,
            "status": status.as_u16(),
        });

        (status, Json(body)).into_response()
    }
}

// Polarsは不要になったため、エラー変換も削除
// DataStore/aggregator経由のPolars使用は全てTurso SQLに移行済み
