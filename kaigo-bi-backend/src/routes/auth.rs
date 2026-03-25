/// 認証エンドポイント
/// ログイン、ログアウト、ユーザー情報取得、トークンリフレッシュ

use axum::{
    extract::{Extension, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

use crate::auth::jwt::{create_token, Claims};
use crate::auth::password::verify_password;

use super::SharedState;

/// ログインリクエスト
#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    pub email: String,
    pub password: String,
}

/// ユーザー情報（パスワードハッシュを除く）
#[derive(Debug, Serialize, Clone)]
pub struct UserInfo {
    pub id: String,
    pub email: String,
    pub name: String,
    pub role: String,
    pub is_active: bool,
    pub expires_at: Option<String>,
    pub created_at: String,
}

/// 公開認証ルーター（認証不要）
pub fn public_router() -> Router<SharedState> {
    Router::new()
        .route("/api/auth/login", post(login))
}

/// 保護認証ルーター（認証必須）
pub fn protected_router() -> Router<SharedState> {
    Router::new()
        .route("/api/auth/logout", post(logout))
        .route("/api/auth/me", get(me))
        .route("/api/auth/refresh", post(refresh))
}

/// POST /api/auth/login - ログイン処理
/// 1. メールアドレスでユーザー検索
/// 2. パスワード検証（pbkdf2 or argon2）
/// 3. is_active チェック
/// 4. expires_at チェック（NULL or 未来日）
/// 5. JWT生成（24時間有効）
/// 6. セッション登録
/// 7. 監査ログ記録
async fn login(
    State(state): State<SharedState>,
    Json(payload): Json<LoginRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<serde_json::Value>)> {
    let conn = state.db.connect().map_err(|e| {
        tracing::error!("DB接続エラー: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "データベース接続エラー", "status": 500})),
        )
    })?;

    // 1. メールアドレスでユーザー検索
    let mut rows = conn
        .query(
            "SELECT id, email, name, password_hash, role, is_active, expires_at, created_at FROM users WHERE email = ?1",
            libsql::params![payload.email.clone()],
        )
        .await
        .map_err(|e| {
            tracing::error!("ユーザー検索エラー: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "データベースエラー", "status": 500})),
            )
        })?;

    let row = match rows.next().await.map_err(|e| {
        tracing::error!("行取得エラー: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "データベースエラー", "status": 500})),
        )
    })? {
        Some(row) => row,
        None => {
            return Err((
                StatusCode::UNAUTHORIZED,
                Json(json!({"error": "メールアドレスまたはパスワードが正しくありません", "status": 401})),
            ));
        }
    };

    let user_id: String = row.get(0).map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "データ取得エラー", "status": 500})),
        )
    })?;
    let email: String = row.get(1).map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "データ取得エラー", "status": 500})),
        )
    })?;
    let name: String = row.get(2).map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "データ取得エラー", "status": 500})),
        )
    })?;
    let password_hash: String = row.get(3).map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "データ取得エラー", "status": 500})),
        )
    })?;
    let role: String = row.get(4).map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "データ取得エラー", "status": 500})),
        )
    })?;
    let is_active: i64 = row.get(5).map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "データ取得エラー", "status": 500})),
        )
    })?;
    let expires_at: Option<String> = row.get::<String>(6).ok();
    let created_at: String = row.get(7).map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "データ取得エラー", "status": 500})),
        )
    })?;

    // 2. パスワード検証（pbkdf2 or argon2）
    let is_valid = verify_password(&payload.password, &password_hash).map_err(|e| {
        tracing::error!("パスワード検証エラー: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "パスワード検証エラー", "status": 500})),
        )
    })?;

    if !is_valid {
        // 監査ログ: ログイン失敗
        let _ = conn
            .execute(
                "INSERT INTO audit_logs (id, user_id, action, details, created_at) VALUES (?1, ?2, ?3, ?4, datetime('now'))",
                libsql::params![
                    Uuid::new_v4().to_string(),
                    user_id.clone(),
                    "login_failed".to_string(),
                    "パスワード不一致".to_string(),
                ],
            )
            .await;

        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "メールアドレスまたはパスワードが正しくありません", "status": 401})),
        ));
    }

    // 3. is_active チェック
    if is_active != 1 {
        return Err((
            StatusCode::FORBIDDEN,
            Json(json!({"error": "アカウントが無効化されています", "status": 403})),
        ));
    }

    // 4. expires_at チェック（NULLなら無期限、日時入りなら有効期限確認）
    if let Some(ref exp) = expires_at {
        if !exp.is_empty() {
            let now = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string();
            if *exp < now {
                return Err((
                    StatusCode::FORBIDDEN,
                    Json(json!({"error": "アカウントの有効期限が切れています", "status": 403})),
                ));
            }
        }
    }

    // 5. JWT生成（24時間有効）
    let token = create_token(&user_id, &email, &name, &role).map_err(|e| {
        tracing::error!("JWT生成エラー: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "トークン生成エラー", "status": 500})),
        )
    })?;

    // 6. セッション登録
    let session_id = Uuid::new_v4().to_string();
    // トークンハッシュ（セッション管理用、簡易SHA256）
    let token_hash = format!("{:x}", sha2::Sha256::digest(token.as_bytes()));
    let session_expires = chrono::Utc::now()
        .checked_add_signed(chrono::Duration::hours(24))
        .ok_or_else(|| (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "セッション有効期限の計算に失敗しました"}))))?
        .format("%Y-%m-%dT%H:%M:%S")
        .to_string();

    let _ = conn
        .execute(
            "INSERT INTO sessions (id, user_id, token_hash, expires_at, created_at) VALUES (?1, ?2, ?3, ?4, datetime('now'))",
            libsql::params![
                session_id,
                user_id.clone(),
                token_hash,
                session_expires,
            ],
        )
        .await;

    // 7. 監査ログ記録
    let _ = conn
        .execute(
            "INSERT INTO audit_logs (id, user_id, action, details, created_at) VALUES (?1, ?2, ?3, ?4, datetime('now'))",
            libsql::params![
                Uuid::new_v4().to_string(),
                user_id.clone(),
                "login".to_string(),
                "ログイン成功".to_string(),
            ],
        )
        .await;

    tracing::info!("ログイン成功: {} ({})", email, role);

    let user_info = UserInfo {
        id: user_id,
        email,
        name,
        role,
        is_active: is_active == 1,
        expires_at,
        created_at,
    };

    Ok((
        StatusCode::OK,
        Json(json!({
            "token": token,
            "user": user_info,
        })),
    ))
}

/// POST /api/auth/logout - ログアウト（セッション削除）
async fn logout(
    State(state): State<SharedState>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let conn = state.db.connect().map_err(|e| {
        tracing::error!("DB接続エラー: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "データベース接続エラー", "status": 500})),
        )
    })?;

    // ユーザーの全セッションを削除
    let _ = conn
        .execute(
            "DELETE FROM sessions WHERE user_id = ?1",
            libsql::params![claims.sub.clone()],
        )
        .await;

    // 監査ログ
    let _ = conn
        .execute(
            "INSERT INTO audit_logs (id, user_id, action, details, created_at) VALUES (?1, ?2, ?3, ?4, datetime('now'))",
            libsql::params![
                Uuid::new_v4().to_string(),
                claims.sub,
                "logout".to_string(),
                "ログアウト".to_string(),
            ],
        )
        .await;

    Ok(Json(json!({"message": "ログアウトしました"})))
}

/// GET /api/auth/me - 現在のユーザー情報を取得
async fn me(
    State(state): State<SharedState>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let conn = state.db.connect().map_err(|e| {
        tracing::error!("DB接続エラー: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "データベース接続エラー", "status": 500})),
        )
    })?;

    let mut rows = conn
        .query(
            "SELECT id, email, name, role, is_active, expires_at, created_at FROM users WHERE id = ?1",
            libsql::params![claims.sub.clone()],
        )
        .await
        .map_err(|e| {
            tracing::error!("ユーザー取得エラー: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "データベースエラー", "status": 500})),
            )
        })?;

    let row = match rows.next().await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("データ取得エラー: {}", e), "status": 500})),
        )
    })? {
        Some(row) => row,
        None => {
            return Err((
                StatusCode::NOT_FOUND,
                Json(json!({"error": "ユーザーが見つかりません", "status": 404})),
            ));
        }
    };

    let user = UserInfo {
        id: row.get(0).unwrap_or_default(),
        email: row.get(1).unwrap_or_default(),
        name: row.get(2).unwrap_or_default(),
        role: row.get(3).unwrap_or_default(),
        is_active: row.get::<i64>(4).unwrap_or(0) == 1,
        expires_at: row.get::<String>(5).ok(),
        created_at: row.get(6).unwrap_or_default(),
    };

    Ok(Json(json!({"user": user})))
}

/// POST /api/auth/refresh - トークンリフレッシュ
/// 現在のClaimsから新しいトークンを発行する
async fn refresh(
    Extension(claims): Extension<Claims>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let token = create_token(&claims.sub, &claims.email, &claims.name, &claims.role).map_err(
        |e| {
            tracing::error!("JWT生成エラー: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "トークン生成エラー", "status": 500})),
            )
        },
    )?;

    Ok(Json(json!({
        "token": token,
        "expires_in": 86400,
    })))
}

/// sha2::Digest トレイトを使うためのuse
use sha2::Digest;
