/// 課金プランの階層定義とプランゲートミドルウェア
/// free < standard < pro < ma の順に権限が広がる
/// role=admin は全プランゲートをバイパスする

use axum::{
    body::Body,
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use super::jwt::Claims;
use crate::routes::SharedState;

/// 現行プランの短期キャッシュ(user_id → (plan, 取得時刻))
/// レビュー⑦対応: JWT内のプランは最大24時間古くなるため、課金対象APIでは
/// DBの現行プランを確認する。ただし毎回SELECTするとレイテンシが増えるので
/// 5分TTLのメモリキャッシュを噛ませる(Stripe webhookでのプラン変更は稀)。
static PLAN_CACHE: OnceLock<Mutex<HashMap<String, (String, Instant)>>> = OnceLock::new();
const PLAN_CACHE_TTL: Duration = Duration::from_secs(300);

fn plan_cache() -> &'static Mutex<HashMap<String, (String, Instant)>> {
    PLAN_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// DBから現行プランを取得(5分キャッシュ)。取得失敗時はJWTのプランにフォールバック。
async fn current_plan(state: &SharedState, user_id: &str, jwt_plan: &str) -> String {
    let now = Instant::now();
    if let Ok(cache) = plan_cache().lock() {
        if let Some((plan, at)) = cache.get(user_id) {
            if now.duration_since(*at) < PLAN_CACHE_TTL {
                return plan.clone();
            }
        }
    }
    let plan = match state.db.connect() {
        Ok(conn) => match conn
            .query(
                "SELECT COALESCE(plan, 'free') FROM users WHERE id = ?1",
                libsql::params![user_id.to_string()],
            )
            .await
        {
            Ok(mut rows) => match rows.next().await {
                Ok(Some(row)) => row.get::<String>(0).unwrap_or_else(|_| jwt_plan.to_string()),
                _ => jwt_plan.to_string(),
            },
            Err(_) => jwt_plan.to_string(),
        },
        Err(_) => jwt_plan.to_string(),
    };
    if let Ok(mut cache) = plan_cache().lock() {
        cache.insert(user_id.to_string(), (plan.clone(), now));
    }
    plan
}

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
/// admin ロールは常に通過。プランは JWT ではなく DB の現行値(5分キャッシュ)で判定。
pub async fn require_plan(
    min_plan: &'static str,
    State(state): State<SharedState>,
    req: Request<Body>,
    next: Next,
) -> Response {
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

    // JWT内のplanは古い可能性があるため、DBの現行プランで判定する
    let plan = current_plan(&state, &claims.sub, &claims.plan).await;

    if plan_level(&plan) >= plan_level(min_plan) {
        next.run(req).await
    } else {
        (
            StatusCode::FORBIDDEN,
            Json(json!({
                "error": format!(
                    "この機能は{}プラン以上でご利用いただけます（現在: {}プラン）",
                    plan_display_name(min_plan),
                    plan_display_name(&plan)
                ),
                "status": 403,
                "code": "plan_required",
                "required_plan": min_plan,
                "current_plan": plan,
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
