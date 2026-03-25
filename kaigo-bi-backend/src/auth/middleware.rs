/// 認証ミドルウェア
/// AuthorizationヘッダーまたはクッキーからJWTを検証し、
/// 認証済みユーザー情報をリクエストExtensionに注入する

use axum::{
    body::Body,
    extract::Request,
    http::{header, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

use super::jwt::{verify_token, Claims};

/// 認証ミドルウェア
/// Authorization: Bearer <token> またはクッキー "token" からJWTを抽出・検証する
pub async fn auth_middleware(mut req: Request<Body>, next: Next) -> Response {
    // トークンを抽出（Authorizationヘッダー → クッキーの優先順）
    let token = extract_token_from_header(&req)
        .or_else(|| extract_token_from_cookie(&req));

    let token = match token {
        Some(t) => t,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({
                    "error": "認証トークンがありません",
                    "status": 401
                })),
            )
                .into_response();
        }
    };

    // JWT検証
    match verify_token(&token) {
        Ok(claims) => {
            // 認証済みClaimsをExtensionに注入
            req.extensions_mut().insert(claims);
            next.run(req).await
        }
        Err(e) => {
            tracing::warn!("JWT検証失敗: {}", e);
            (
                StatusCode::UNAUTHORIZED,
                Json(json!({
                    "error": "無効なトークンです",
                    "status": 401
                })),
            )
                .into_response()
        }
    }
}

/// Authorizationヘッダーから "Bearer <token>" を抽出
fn extract_token_from_header(req: &Request<Body>) -> Option<String> {
    req.headers()
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
        .map(|s| s.to_string())
}

/// クッキーから "token=<value>" を抽出
fn extract_token_from_cookie(req: &Request<Body>) -> Option<String> {
    let cookie_header = req.headers().get(header::COOKIE)?.to_str().ok()?;
    for pair in cookie_header.split(';') {
        let pair = pair.trim();
        if let Some(value) = pair.strip_prefix("token=") {
            return Some(value.to_string());
        }
    }
    None
}

/// ロールチェック用ヘルパー
/// 指定されたロールのいずれかに一致すればtrueを返す
pub fn has_required_role(claims: &Claims, required_roles: &[&str]) -> bool {
    required_roles.contains(&claims.role.as_str())
}
