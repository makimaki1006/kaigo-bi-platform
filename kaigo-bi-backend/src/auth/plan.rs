/// 課金プランの階層定義とプランゲートミドルウェア
/// free < standard < pro < ma の順に権限が広がる
/// role=admin は全プランゲートをバイパスする

use axum::{
    body::Body,
    extract::Request,
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

use super::jwt::Claims;

/// プラン名を数値レベルに変換する（不明なプランはfree扱い）
pub fn plan_level(plan: &str) -> u8 {
    match plan {
        "standard" => 1,
        "pro" => 2,
        "ma" => 3,
        _ => 0, // free または不明
    }
}

/// プランの表示名（エラーメッセージ用）
pub fn plan_display_name(plan: &str) -> &'static str {
    match plan {
        "standard" => "スタンダード",
        "pro" => "プロ",
        "ma" => "M&A",
        _ => "フリー",
    }
}

/// プランゲートミドルウェア
/// auth_middleware より内側で使用する（Extension<Claims> が必要）
/// admin ロールは常に通過
pub async fn require_plan(min_plan: &'static str, req: Request<Body>, next: Next) -> Response {
    let claims = match req.extensions().get::<Claims>() {
        Some(c) => c.clone(),
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({"error": "認証情報がありません", "status": 401})),
            )
                .into_response();
        }
    };

    // adminは全機能にアクセス可能
    if claims.role == "admin" {
        return next.run(req).await;
    }

    if plan_level(&claims.plan) >= plan_level(min_plan) {
        next.run(req).await
    } else {
        (
            StatusCode::FORBIDDEN,
            Json(json!({
                "error": format!(
                    "この機能は{}プラン以上でご利用いただけます（現在: {}プラン）",
                    plan_display_name(min_plan),
                    plan_display_name(&claims.plan)
                ),
                "status": 403,
                "code": "plan_required",
                "required_plan": min_plan,
                "current_plan": claims.plan,
            })),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plan_level_ordering() {
        assert!(plan_level("free") < plan_level("standard"));
        assert!(plan_level("standard") < plan_level("pro"));
        assert!(plan_level("pro") < plan_level("ma"));
        assert_eq!(plan_level("unknown"), plan_level("free"));
    }
}
