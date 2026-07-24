/// Stripe課金エンドポイント
/// Checkout Session作成、Customer Portal、Webhook受信
/// Stripe SDKは使わずREST APIを直接呼ぶ（依存を最小化）
///
/// 必須環境変数:
///   STRIPE_SECRET_KEY      - Stripeシークレットキー (sk_...)
///   STRIPE_WEBHOOK_SECRET  - Webhook署名シークレット (whsec_...)
///   STRIPE_PRICE_STANDARD  - スタンダードプランのPrice ID
///   STRIPE_PRICE_PRO       - プロプランのPrice ID
///   STRIPE_PRICE_MA        - M&AプランのPrice ID
///   APP_URL                - フロントエンドURL（Checkoutのリダイレクト先）

use axum::{
    body::Bytes,
    extract::{Extension, State},
    http::{HeaderMap, StatusCode},
    routing::post,
    Json, Router,
};
use hmac::{Hmac, Mac};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::Sha256;

use crate::auth::jwt::Claims;

use super::SharedState;

const STRIPE_API_BASE: &str = "https://api.stripe.com/v1";

/// 認証必須の課金ルーター
pub fn protected_router() -> Router<SharedState> {
    Router::new()
        .route("/api/billing/checkout", post(create_checkout))
        .route("/api/billing/portal", post(create_portal))
}

/// 公開ルーター（Stripe Webhookは署名で検証するため認証不要）
pub fn public_router() -> Router<SharedState> {
    Router::new().route("/api/webhooks/stripe", post(stripe_webhook))
}

/// 環境変数取得ヘルパー
fn env_var(key: &str) -> Result<String, (StatusCode, Json<Value>)> {
    std::env::var(key).map_err(|_| {
        tracing::error!("環境変数 {} が未設定です", key);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "課金機能が設定されていません", "status": 500})),
        )
    })
}

/// プラン名 → Stripe Price ID
fn price_id_for_plan(plan: &str) -> Result<String, (StatusCode, Json<Value>)> {
    match plan {
        "standard" => env_var("STRIPE_PRICE_STANDARD"),
        "pro" => env_var("STRIPE_PRICE_PRO"),
        "ma" => env_var("STRIPE_PRICE_MA"),
        _ => Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "不正なプラン指定です", "status": 400})),
        )),
    }
}

/// Stripe Price ID → プラン名（Webhook用）
fn plan_for_price_id(price_id: &str) -> Option<&'static str> {
    let check = |key: &str| std::env::var(key).ok().is_some_and(|v| v == price_id);
    if check("STRIPE_PRICE_STANDARD") {
        Some("standard")
    } else if check("STRIPE_PRICE_PRO") {
        Some("pro")
    } else if check("STRIPE_PRICE_MA") {
        Some("ma")
    } else {
        None
    }
}

#[derive(Debug, Deserialize)]
pub struct CheckoutRequest {
    pub plan: String,
}

/// ユーザーのstripe_customer_idを取得する（未設定・エラー時はNone）
async fn fetch_stripe_customer_id(state: &SharedState, user_id: &str) -> Option<String> {
    let conn = state.db.connect().ok()?;
    let mut rows = conn
        .query(
            "SELECT stripe_customer_id FROM users WHERE id = ?1",
            libsql::params![user_id.to_string()],
        )
        .await
        .ok()?;
    let row = rows.next().await.ok()??;
    row.get::<String>(0).ok().filter(|s| !s.is_empty())
}

/// POST /api/billing/checkout - Stripe Checkout Session作成
/// 成功するとStripeの決済ページURLを返す
async fn create_checkout(
    State(state): State<SharedState>,
    Extension(claims): Extension<Claims>,
    Json(payload): Json<CheckoutRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let secret_key = env_var("STRIPE_SECRET_KEY")?;
    let price_id = price_id_for_plan(&payload.plan)?;
    let app_url = std::env::var("APP_URL").unwrap_or_else(|_| "http://localhost:3000".to_string());

    // 既存のStripe顧客IDがあれば再利用する
    let existing_customer = fetch_stripe_customer_id(&state, &claims.sub).await;

    let mut params: Vec<(String, String)> = vec![
        ("mode".into(), "subscription".into()),
        ("line_items[0][price]".into(), price_id),
        ("line_items[0][quantity]".into(), "1".into()),
        (
            "success_url".into(),
            format!("{}/account?checkout=success", app_url),
        ),
        (
            "cancel_url".into(),
            format!("{}/pricing?checkout=cancel", app_url),
        ),
        ("client_reference_id".into(), claims.sub.clone()),
        ("metadata[user_id]".into(), claims.sub.clone()),
        ("metadata[plan]".into(), payload.plan.clone()),
        (
            "subscription_data[metadata][user_id]".into(),
            claims.sub.clone(),
        ),
    ];

    if let Some(customer_id) = existing_customer {
        params.push(("customer".into(), customer_id));
    } else {
        params.push(("customer_email".into(), claims.email.clone()));
    }

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/checkout/sessions", STRIPE_API_BASE))
        .bearer_auth(&secret_key)
        .form(&params)
        .send()
        .await
        .map_err(|e| {
            tracing::error!("Stripe API呼び出しエラー: {}", e);
            (
                StatusCode::BAD_GATEWAY,
                Json(json!({"error": "決済サービスへの接続に失敗しました", "status": 502})),
            )
        })?;

    let status = resp.status();
    let body: Value = resp.json().await.map_err(|e| {
        tracing::error!("Stripeレスポンスパースエラー: {}", e);
        (
            StatusCode::BAD_GATEWAY,
            Json(json!({"error": "決済サービスの応答が不正です", "status": 502})),
        )
    })?;

    if !status.is_success() {
        tracing::error!("Stripe Checkoutエラー: {}", body);
        return Err((
            StatusCode::BAD_GATEWAY,
            Json(json!({"error": "決済セッションの作成に失敗しました", "status": 502})),
        ));
    }

    let url = body["url"].as_str().unwrap_or_default();
    Ok(Json(json!({"url": url})))
}

/// POST /api/billing/portal - Stripe Customer Portal Session作成
/// プラン変更・解約・支払い方法の管理ページURLを返す
async fn create_portal(
    State(state): State<SharedState>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let secret_key = env_var("STRIPE_SECRET_KEY")?;
    let app_url = std::env::var("APP_URL").unwrap_or_else(|_| "http://localhost:3000".to_string());

    // Stripe顧客IDを取得
    let customer_id = fetch_stripe_customer_id(&state, &claims.sub).await.ok_or((
        StatusCode::BAD_REQUEST,
        Json(json!({"error": "契約中のプランがありません。まずプランをご購入ください", "status": 400})),
    ))?;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/billing_portal/sessions", STRIPE_API_BASE))
        .bearer_auth(&secret_key)
        .form(&[
            ("customer", customer_id.as_str()),
            ("return_url", &format!("{}/account", app_url)),
        ])
        .send()
        .await
        .map_err(|e| {
            tracing::error!("Stripe Portal APIエラー: {}", e);
            (
                StatusCode::BAD_GATEWAY,
                Json(json!({"error": "決済サービスへの接続に失敗しました", "status": 502})),
            )
        })?;

    let status = resp.status();
    let body: Value = resp.json().await.map_err(|_| {
        (
            StatusCode::BAD_GATEWAY,
            Json(json!({"error": "決済サービスの応答が不正です", "status": 502})),
        )
    })?;

    if !status.is_success() {
        tracing::error!("Stripe Portalエラー: {}", body);
        return Err((
            StatusCode::BAD_GATEWAY,
            Json(json!({"error": "管理ページの作成に失敗しました", "status": 502})),
        ));
    }

    Ok(Json(json!({"url": body["url"].as_str().unwrap_or_default()})))
}

/// Stripe-Signatureヘッダーを検証する
/// 形式: t=タイムスタンプ,v1=署名[,v1=署名...]
/// signed_payload = "{t}.{body}" のHMAC-SHA256がv1のいずれかと一致すればOK
fn verify_stripe_signature(
    header_value: &str,
    payload: &[u8],
    webhook_secret: &str,
) -> Result<(), String> {
    let mut timestamp: Option<&str> = None;
    let mut signatures: Vec<&str> = Vec::new();

    for part in header_value.split(',') {
        let part = part.trim();
        if let Some(t) = part.strip_prefix("t=") {
            timestamp = Some(t);
        } else if let Some(sig) = part.strip_prefix("v1=") {
            signatures.push(sig);
        }
    }

    let timestamp = timestamp.ok_or("タイムスタンプがありません")?;

    // リプレイ攻撃対策: 5分以内のイベントのみ受理
    let ts: i64 = timestamp.parse().map_err(|_| "タイムスタンプが不正です")?;
    let now = chrono::Utc::now().timestamp();
    if (now - ts).abs() > 300 {
        return Err("タイムスタンプが古すぎます".to_string());
    }

    let signed_payload = [timestamp.as_bytes(), b".", payload].concat();

    for sig in signatures {
        let expected = match hex::decode(sig) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let mut mac = Hmac::<Sha256>::new_from_slice(webhook_secret.as_bytes())
            .map_err(|_| "HMAC初期化エラー")?;
        mac.update(&signed_payload);
        if mac.verify_slice(&expected).is_ok() {
            return Ok(());
        }
    }

    Err("署名が一致しません".to_string())
}

/// POST /api/webhooks/stripe - Stripe Webhook受信
/// 署名検証後、サブスクリプション状態をusersテーブルに反映する
async fn stripe_webhook(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, (StatusCode, Json<Value>)> {
    let webhook_secret = env_var("STRIPE_WEBHOOK_SECRET")?;

    // 署名検証
    let sig_header = headers
        .get("stripe-signature")
        .and_then(|v| v.to_str().ok())
        .ok_or((
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "署名ヘッダーがありません"})),
        ))?;

    verify_stripe_signature(sig_header, &body, &webhook_secret).map_err(|e| {
        tracing::warn!("Stripe Webhook署名検証失敗: {}", e);
        (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "署名検証に失敗しました"})),
        )
    })?;

    let event: Value = serde_json::from_slice(&body).map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "JSONパースエラー"})),
        )
    })?;

    let event_type = event["type"].as_str().unwrap_or_default();
    let obj = &event["data"]["object"];

    let conn = state.db.connect().map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "データベース接続エラー"})),
        )
    })?;

    match event_type {
        // 決済完了: ユーザーにプランと顧客IDを紐付け
        "checkout.session.completed" => {
            let user_id = obj["client_reference_id"].as_str().unwrap_or_default();
            let customer_id = obj["customer"].as_str().unwrap_or_default();
            let subscription_id = obj["subscription"].as_str().unwrap_or_default();
            let plan = obj["metadata"]["plan"].as_str().unwrap_or_default();

            if user_id.is_empty() || !matches!(plan, "standard" | "pro" | "ma") {
                tracing::warn!(
                    "checkout.session.completed: user_id/planが不正 (user_id={}, plan={})",
                    user_id,
                    plan
                );
                return Ok(StatusCode::OK);
            }

            conn.execute(
                "UPDATE users SET plan = ?1, stripe_customer_id = ?2, stripe_subscription_id = ?3, updated_at = datetime('now') WHERE id = ?4",
                libsql::params![plan, customer_id, subscription_id, user_id],
            )
            .await
            .map_err(|e| {
                tracing::error!("プラン更新エラー: {}", e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "プラン更新に失敗しました"})),
                )
            })?;

            tracing::info!("プラン購入完了: user={} plan={}", user_id, plan);
        }

        // サブスク変更（プラン変更・支払い失敗等）
        "customer.subscription.updated" => {
            let subscription_id = obj["id"].as_str().unwrap_or_default();
            let status = obj["status"].as_str().unwrap_or_default();
            let price_id = obj["items"]["data"][0]["price"]["id"]
                .as_str()
                .unwrap_or_default();

            let new_plan = if matches!(status, "active" | "trialing") {
                plan_for_price_id(price_id).unwrap_or("free")
            } else if matches!(status, "canceled" | "unpaid" | "incomplete_expired") {
                "free"
            } else {
                // past_due等は現状維持（Stripeのリトライに任せる）
                tracing::info!(
                    "subscription.updated: status={} は現状維持 (sub={})",
                    status,
                    subscription_id
                );
                return Ok(StatusCode::OK);
            };

            conn.execute(
                "UPDATE users SET plan = ?1, updated_at = datetime('now') WHERE stripe_subscription_id = ?2",
                libsql::params![new_plan, subscription_id],
            )
            .await
            .map_err(|e| {
                tracing::error!("サブスク更新エラー: {}", e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "プラン更新に失敗しました"})),
                )
            })?;

            tracing::info!(
                "サブスク更新: sub={} status={} → plan={}",
                subscription_id,
                status,
                new_plan
            );
        }

        // サブスク解約: フリープランに戻す
        "customer.subscription.deleted" => {
            let subscription_id = obj["id"].as_str().unwrap_or_default();

            conn.execute(
                "UPDATE users SET plan = 'free', stripe_subscription_id = NULL, updated_at = datetime('now') WHERE stripe_subscription_id = ?1",
                libsql::params![subscription_id],
            )
            .await
            .map_err(|e| {
                tracing::error!("サブスク解約処理エラー: {}", e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "プラン更新に失敗しました"})),
                )
            })?;

            tracing::info!("サブスク解約: sub={} → free", subscription_id);
        }

        _ => {
            tracing::debug!("未処理のStripeイベント: {}", event_type);
        }
    }

    Ok(StatusCode::OK)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_verify_stripe_signature() {
        let secret = "whsec_test_secret";
        let payload = b"{\"id\":\"evt_test\"}";
        let ts = chrono::Utc::now().timestamp().to_string();

        let signed_payload = [ts.as_bytes(), b".", payload.as_slice()].concat();
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(&signed_payload);
        let sig = hex::encode(mac.finalize().into_bytes());

        let header = format!("t={},v1={}", ts, sig);
        assert!(verify_stripe_signature(&header, payload, secret).is_ok());

        // 不正な署名は拒否
        let bad_header = format!("t={},v1={}", ts, "0".repeat(64));
        assert!(verify_stripe_signature(&bad_header, payload, secret).is_err());
    }
}
