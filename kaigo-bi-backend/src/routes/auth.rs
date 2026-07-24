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
use crate::auth::password::{hash_password, verify_password};

use super::SharedState;

/// ログインリクエスト
#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    pub email: String,
    pub password: String,
}

/// サインアップリクエスト
#[derive(Debug, Deserialize)]
pub struct SignupRequest {
    pub email: String,
    pub password: String,
    pub name: String,
}

/// ユーザー情報（パスワードハッシュを除く）
#[derive(Debug, Serialize, Clone)]
pub struct UserInfo {
    pub id: String,
    pub email: String,
    pub name: String,
    pub role: String,
    pub plan: String,
    pub is_active: bool,
    pub expires_at: Option<String>,
    pub created_at: String,
}

/// 公開認証ルーター（認証不要）
pub fn public_router() -> Router<SharedState> {
    Router::new()
        .route("/api/auth/login", post(login))
        .route("/api/auth/signup", post(signup))
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
            "SELECT id, email, name, password_hash, role, is_active, expires_at, created_at, COALESCE(plan, 'free') FROM users WHERE email = ?1",
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
    let plan: String = row.get(8).unwrap_or_else(|_| "free".to_string());

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
    let token = create_token(&user_id, &email, &name, &role, &plan).map_err(|e| {
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
        plan,
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

/// POST /api/auth/signup - セルフサインアップ
/// 1. 入力バリデーション（メール形式、パスワード8文字以上）
/// 2. メール重複チェック
/// 3. argon2ハッシュ化して登録（role=viewer, plan=free）
/// 4. JWT発行して即ログイン状態にする
async fn signup(
    State(state): State<SharedState>,
    Json(payload): Json<SignupRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<serde_json::Value>)> {
    let email = payload.email.trim().to_lowercase();
    let name = payload.name.trim().to_string();

    // 1. 入力バリデーション
    if !email.contains('@') || email.len() < 5 || email.len() > 254 {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "メールアドレスの形式が正しくありません", "status": 400})),
        ));
    }
    if payload.password.len() < 8 {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "パスワードは8文字以上で設定してください", "status": 400})),
        ));
    }
    if name.is_empty() || name.len() > 100 {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "お名前を入力してください", "status": 400})),
        ));
    }

    let conn = state.db.connect().map_err(|e| {
        tracing::error!("DB接続エラー: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "データベース接続エラー", "status": 500})),
        )
    })?;

    // 2. メール重複チェック
    let mut rows = conn
        .query(
            "SELECT id FROM users WHERE email = ?1",
            libsql::params![email.clone()],
        )
        .await
        .map_err(|e| {
            tracing::error!("重複チェックエラー: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "データベースエラー", "status": 500})),
            )
        })?;

    if rows
        .next()
        .await
        .map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "データベースエラー", "status": 500})),
            )
        })?
        .is_some()
    {
        return Err((
            StatusCode::CONFLICT,
            Json(json!({"error": "このメールアドレスは既に登録されています", "status": 409})),
        ));
    }

    // 3. パスワードハッシュ化して登録
    let password_hash = hash_password(&payload.password).map_err(|e| {
        tracing::error!("パスワードハッシュエラー: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "登録処理に失敗しました", "status": 500})),
        )
    })?;

    let user_id = Uuid::new_v4().to_string();
    conn.execute(
        "INSERT INTO users (id, email, name, password_hash, role, plan, is_active, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, 'viewer', 'free', 1, datetime('now'), datetime('now'))",
        libsql::params![
            user_id.clone(),
            email.clone(),
            name.clone(),
            password_hash,
        ],
    )
    .await
    .map_err(|e| {
        tracing::error!("ユーザー登録エラー: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "登録処理に失敗しました", "status": 500})),
        )
    })?;

    // 監査ログ
    let _ = conn
        .execute(
            "INSERT INTO audit_logs (id, user_id, action, details, created_at) VALUES (?1, ?2, ?3, ?4, datetime('now'))",
            libsql::params![
                Uuid::new_v4().to_string(),
                user_id.clone(),
                "signup".to_string(),
                "セルフサインアップ".to_string(),
            ],
        )
        .await;

    // 4. JWT発行
    let token = create_token(&user_id, &email, &name, "viewer", "free").map_err(|e| {
        tracing::error!("JWT生成エラー: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "トークン生成エラー", "status": 500})),
        )
    })?;

    tracing::info!("サインアップ成功: {}", email);

    let user_info = UserInfo {
        id: user_id,
        email,
        name,
        role: "viewer".to_string(),
        plan: "free".to_string(),
        is_active: true,
        expires_at: None,
        created_at: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string(),
    };

    Ok((
        StatusCode::CREATED,
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
            "SELECT id, email, name, role, is_active, expires_at, created_at, COALESCE(plan, 'free') FROM users WHERE id = ?1",
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
        plan: row.get::<String>(7).unwrap_or_else(|_| "free".to_string()),
    };

    Ok(Json(json!({"user": user})))
}

/// POST /api/auth/refresh - トークンリフレッシュ
/// DBから最新のプランを読み直して新しいトークンを発行する
/// （Stripe Webhookでプランが変わった場合もリフレッシュで反映される）
async fn refresh(
    State(state): State<SharedState>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    // DBから最新プランを取得（失敗時はトークン内のプランを維持）
    let plan = match state.db.connect() {
        Ok(conn) => {
            match conn
                .query(
                    "SELECT COALESCE(plan, 'free') FROM users WHERE id = ?1",
                    libsql::params![claims.sub.clone()],
                )
                .await
            {
                Ok(mut rows) => match rows.next().await {
                    Ok(Some(row)) => row.get::<String>(0).unwrap_or_else(|_| claims.plan.clone()),
                    _ => claims.plan.clone(),
                },
                Err(_) => claims.plan.clone(),
            }
        }
        Err(_) => claims.plan.clone(),
    };

    let token = create_token(&claims.sub, &claims.email, &claims.name, &claims.role, &plan)
        .map_err(|e| {
            tracing::error!("JWT生成エラー: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "トークン生成エラー", "status": 500})),
            )
        })?;

    Ok(Json(json!({
        "token": token,
        "expires_in": 86400,
    })))
}

/// sha2::Digest トレイトを使うためのuse
use sha2::Digest;
