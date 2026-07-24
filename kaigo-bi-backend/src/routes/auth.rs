/// 認証エンドポイント
/// ログイン、ログアウト、ユーザー情報取得、トークンリフレッシュ

use axum::{
    extract::{Extension, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
    Json, Router,
};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

use crate::auth::jwt::{create_token, Claims};
use crate::auth::password::{hash_password, verify_password};
use crate::auth::rate_limit;
use crate::services::mailer;

use super::SharedState;

/// リセットトークンの有効期間（分）
const RESET_TOKEN_TTL_MINUTES: i64 = 30;

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

/// パスワードリセット要求リクエスト
#[derive(Debug, Deserialize)]
pub struct ForgotPasswordRequest {
    pub email: String,
}

/// パスワードリセット実行リクエスト
#[derive(Debug, Deserialize)]
pub struct ResetPasswordRequest {
    pub token: String,
    pub password: String,
}

/// 公開認証ルーター（認証不要）
pub fn public_router() -> Router<SharedState> {
    Router::new()
        .route("/api/auth/login", post(login))
        .route("/api/auth/signup", post(signup))
        .route("/api/auth/forgot-password", post(forgot_password))
        .route("/api/auth/reset-password", post(reset_password))
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
    headers: HeaderMap,
    Json(payload): Json<LoginRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<serde_json::Value>)> {
    // ブルートフォース対策: メール+IP単位で15分に5回まで失敗を許容
    let ip = rate_limit::client_ip(&headers);
    let limit_key = format!("{}|{}", payload.email.to_lowercase(), ip);
    if rate_limit::is_limited("login", &limit_key, &rate_limit::LOGIN_FAILURE) {
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({"error": "ログイン試行回数が上限に達しました。15分後に再度お試しください", "status": 429})),
        ));
    }

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
            // 存在しないメールへの試行もカウント（ユーザー列挙攻撃対策）
            rate_limit::record_attempt("login", &limit_key);
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
        rate_limit::record_attempt("login", &limit_key);

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

    // 成功したので失敗カウンタをクリア
    rate_limit::clear_attempts("login", &limit_key);

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
    headers: HeaderMap,
    Json(payload): Json<SignupRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<serde_json::Value>)> {
    // 大量アカウント作成対策: 同一IPで1時間に10回まで
    let ip = rate_limit::client_ip(&headers);
    if rate_limit::is_limited("signup", &ip, &rate_limit::SIGNUP) {
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({"error": "登録リクエストが多すぎます。しばらくしてから再度お試しください", "status": 429})),
        ));
    }
    rate_limit::record_attempt("signup", &ip);

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

/// POST /api/auth/forgot-password - パスワードリセット要求
/// メールの存在有無に関わらず常に200を返す（ユーザー列挙攻撃対策）
/// ユーザーが存在する場合のみトークンを発行してメール送信する
async fn forgot_password(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Json(payload): Json<ForgotPasswordRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let email = payload.email.trim().to_lowercase();
    let ip = rate_limit::client_ip(&headers);

    // レート制限: メール単位とIP単位の両方で1時間に3回まで
    if rate_limit::is_limited("forgot_email", &email, &rate_limit::PASSWORD_RESET)
        || rate_limit::is_limited("forgot_ip", &ip, &rate_limit::PASSWORD_RESET_IP)
    {
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({"error": "リセット要求が多すぎます。しばらくしてから再度お試しください", "status": 429})),
        ));
    }
    rate_limit::record_attempt("forgot_email", &email);
    rate_limit::record_attempt("forgot_ip", &ip);

    // 常に同じレスポンスを返す（存在有無を漏らさない）
    let ok_response = Json(json!({
        "message": "入力されたメールアドレスが登録されている場合、パスワード再設定のご案内を送信しました"
    }));

    let conn = match state.db.connect() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("DB接続エラー: {}", e);
            return Ok(ok_response);
        }
    };

    // ユーザー検索（存在しなければ何もせず200）
    let user_id: Option<String> = match conn
        .query(
            "SELECT id FROM users WHERE email = ?1 AND is_active = 1",
            libsql::params![email.clone()],
        )
        .await
    {
        Ok(mut rows) => match rows.next().await {
            Ok(Some(row)) => row.get::<String>(0).ok(),
            _ => None,
        },
        Err(e) => {
            tracing::error!("ユーザー検索エラー: {}", e);
            None
        }
    };

    let Some(user_id) = user_id else {
        tracing::info!("パスワードリセット要求（未登録メール）: {}", email);
        return Ok(ok_response);
    };

    // トークン生成: 32バイト乱数 → hex。DBにはSHA256ハッシュのみ保存
    let mut token_bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut token_bytes);
    let token = hex::encode(token_bytes);
    let token_hash = format!("{:x}", sha2::Sha256::digest(token.as_bytes()));

    let expires_at = chrono::Utc::now()
        + chrono::Duration::minutes(RESET_TOKEN_TTL_MINUTES);
    let expires_str = expires_at.format("%Y-%m-%dT%H:%M:%S").to_string();

    // 既存の未使用トークンを無効化してから新規発行
    let _ = conn
        .execute(
            "UPDATE password_reset_tokens SET used_at = datetime('now') WHERE user_id = ?1 AND used_at IS NULL",
            libsql::params![user_id.clone()],
        )
        .await;

    if let Err(e) = conn
        .execute(
            "INSERT INTO password_reset_tokens (id, user_id, token_hash, expires_at, created_at) VALUES (?1, ?2, ?3, ?4, datetime('now'))",
            libsql::params![
                Uuid::new_v4().to_string(),
                user_id.clone(),
                token_hash,
                expires_str,
            ],
        )
        .await
    {
        tracing::error!("リセットトークン保存エラー: {}", e);
        return Ok(ok_response);
    }

    // メール送信
    let app_url = std::env::var("APP_URL").unwrap_or_else(|_| "http://localhost:3000".to_string());
    let reset_url = format!("{}/reset-password?token={}", app_url, token);
    if let Err(e) = mailer::send_password_reset_email(&email, &reset_url).await {
        tracing::error!("リセットメール送信エラー: {}", e);
    }

    // 監査ログ
    let _ = conn
        .execute(
            "INSERT INTO audit_logs (id, user_id, action, details, created_at) VALUES (?1, ?2, ?3, ?4, datetime('now'))",
            libsql::params![
                Uuid::new_v4().to_string(),
                user_id,
                "password_reset_requested".to_string(),
                "パスワードリセット要求".to_string(),
            ],
        )
        .await;

    Ok(ok_response)
}

/// POST /api/auth/reset-password - パスワードリセット実行
/// トークン検証 → パスワード更新 → トークン使用済み化 → 全セッション削除
async fn reset_password(
    State(state): State<SharedState>,
    Json(payload): Json<ResetPasswordRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    if payload.password.len() < 8 {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "パスワードは8文字以上で設定してください", "status": 400})),
        ));
    }

    let conn = state.db.connect().map_err(|e| {
        tracing::error!("DB接続エラー: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "データベース接続エラー", "status": 500})),
        )
    })?;

    // トークンをハッシュ化して有効なレコードを検索
    let token_hash = format!("{:x}", sha2::Sha256::digest(payload.token.as_bytes()));
    let mut rows = conn
        .query(
            "SELECT id, user_id FROM password_reset_tokens WHERE token_hash = ?1 AND used_at IS NULL AND expires_at > datetime('now')",
            libsql::params![token_hash],
        )
        .await
        .map_err(|e| {
            tracing::error!("トークン検索エラー: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "データベースエラー", "status": 500})),
            )
        })?;

    let (token_id, user_id): (String, String) = match rows.next().await {
        Ok(Some(row)) => (
            row.get::<String>(0).unwrap_or_default(),
            row.get::<String>(1).unwrap_or_default(),
        ),
        _ => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "リンクが無効か有効期限が切れています。もう一度リセットを要求してください", "status": 400})),
            ));
        }
    };

    // パスワード更新
    let password_hash = hash_password(&payload.password).map_err(|e| {
        tracing::error!("パスワードハッシュエラー: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "パスワード更新に失敗しました", "status": 500})),
        )
    })?;

    conn.execute(
        "UPDATE users SET password_hash = ?1, updated_at = datetime('now') WHERE id = ?2",
        libsql::params![password_hash, user_id.clone()],
    )
    .await
    .map_err(|e| {
        tracing::error!("パスワード更新エラー: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "パスワード更新に失敗しました", "status": 500})),
        )
    })?;

    // トークンを使用済みにし、既存セッションを全て破棄
    let _ = conn
        .execute(
            "UPDATE password_reset_tokens SET used_at = datetime('now') WHERE id = ?1",
            libsql::params![token_id],
        )
        .await;
    let _ = conn
        .execute(
            "DELETE FROM sessions WHERE user_id = ?1",
            libsql::params![user_id.clone()],
        )
        .await;

    // 監査ログ
    let _ = conn
        .execute(
            "INSERT INTO audit_logs (id, user_id, action, details, created_at) VALUES (?1, ?2, ?3, ?4, datetime('now'))",
            libsql::params![
                Uuid::new_v4().to_string(),
                user_id,
                "password_reset_completed".to_string(),
                "パスワードリセット完了".to_string(),
            ],
        )
        .await;

    tracing::info!("パスワードリセット完了");

    Ok(Json(json!({
        "message": "パスワードを再設定しました。新しいパスワードでログインしてください"
    })))
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
