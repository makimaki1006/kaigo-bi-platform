/// ユーザー管理API（admin専用）
/// CRUD操作と操作ログ一覧を提供

use axum::{
    extract::{Extension, Path, Query, State},
    http::StatusCode,
    routing::{delete, get, post, put},
    Json, Router,
};
use serde::Deserialize;
use serde_json::json;
use uuid::Uuid;

use crate::auth::jwt::Claims;
use crate::auth::middleware::has_required_role;
use crate::auth::password::hash_password;

use super::SharedState;

/// ユーザー作成リクエスト
#[derive(Debug, Deserialize)]
pub struct CreateUserRequest {
    pub email: String,
    pub name: String,
    pub password: String,
    pub role: String,
    /// nullなら無期限
    pub expires_at: Option<String>,
}

/// ユーザー更新リクエスト
#[derive(Debug, Deserialize)]
pub struct UpdateUserRequest {
    pub email: Option<String>,
    pub name: Option<String>,
    pub password: Option<String>,
    pub role: Option<String>,
    pub is_active: Option<bool>,
    pub expires_at: Option<String>,
}

/// 監査ログクエリパラメータ
#[derive(Debug, Deserialize)]
pub struct AuditLogQuery {
    pub limit: Option<u32>,
    pub offset: Option<u32>,
    pub user_id: Option<String>,
    pub action: Option<String>,
}

/// ユーザー管理ルーターを構築（admin専用）
pub fn router() -> Router<SharedState> {
    Router::new()
        .route("/api/users", get(list_users))
        .route("/api/users", post(create_user))
        .route("/api/users/audit-log", get(audit_log))
        .route("/api/users/{id}", get(get_user))
        .route("/api/users/{id}", put(update_user))
        .route("/api/users/{id}", delete(delete_user))
}

/// adminロールチェックマクロ的ヘルパー
fn check_admin(claims: &Claims) -> Result<(), (StatusCode, Json<serde_json::Value>)> {
    if !has_required_role(claims, &["admin"]) {
        return Err((
            StatusCode::FORBIDDEN,
            Json(json!({"error": "管理者権限が必要です", "status": 403})),
        ));
    }
    Ok(())
}

/// GET /api/users - ユーザー一覧
async fn list_users(
    State(state): State<SharedState>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    check_admin(&claims)?;

    let conn = state.db.connect().map_err(|e| {
        tracing::error!("DB接続エラー: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "データベース接続エラー", "status": 500})),
        )
    })?;

    let mut rows = conn
        .query(
            "SELECT id, email, name, role, is_active, expires_at, created_at, updated_at FROM users ORDER BY created_at DESC",
            (),
        )
        .await
        .map_err(|e| {
            tracing::error!("ユーザー一覧取得エラー: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "データベースエラー", "status": 500})),
            )
        })?;

    let mut users = Vec::new();
    while let Some(row) = rows.next().await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("行取得エラー: {}", e), "status": 500})),
        )
    })? {
        users.push(json!({
            "id": row.get::<String>(0).unwrap_or_default(),
            "email": row.get::<String>(1).unwrap_or_default(),
            "name": row.get::<String>(2).unwrap_or_default(),
            "role": row.get::<String>(3).unwrap_or_default(),
            "is_active": row.get::<i64>(4).unwrap_or(0) == 1,
            "expires_at": row.get::<String>(5).ok(),
            "created_at": row.get::<String>(6).unwrap_or_default(),
            "updated_at": row.get::<String>(7).ok(),
        }));
    }

    Ok(Json(json!({
        "users": users,
        "total": users.len(),
    })))
}

/// POST /api/users - ユーザー作成
async fn create_user(
    State(state): State<SharedState>,
    Extension(claims): Extension<Claims>,
    Json(payload): Json<CreateUserRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<serde_json::Value>)> {
    check_admin(&claims)?;

    // ロールのバリデーション
    let valid_roles = ["admin", "consultant", "sales", "viewer"];
    if !valid_roles.contains(&payload.role.as_str()) {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": format!("無効なロールです。有効値: {:?}", valid_roles),
                "status": 400
            })),
        ));
    }

    let conn = state.db.connect().map_err(|e| {
        tracing::error!("DB接続エラー: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "データベース接続エラー", "status": 500})),
        )
    })?;

    // メールアドレスの重複チェック
    let mut existing = conn
        .query(
            "SELECT id FROM users WHERE email = ?1",
            libsql::params![payload.email.clone()],
        )
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("DB検索エラー: {}", e), "status": 500})),
            )
        })?;

    if existing.next().await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("データ取得エラー: {}", e), "status": 500})),
        )
    })?.is_some() {
        return Err((
            StatusCode::CONFLICT,
            Json(json!({"error": "このメールアドレスは既に登録されています", "status": 409})),
        ));
    }

    // パスワードハッシュ生成（argon2）
    let password_hash = hash_password(&payload.password).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("パスワードハッシュエラー: {}", e), "status": 500})),
        )
    })?;

    let user_id = Uuid::new_v4().to_string();
    let expires_at_value = payload.expires_at.clone().unwrap_or_default();

    conn.execute(
        "INSERT INTO users (id, email, name, password_hash, role, is_active, expires_at, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, 1, NULLIF(?6, ''), datetime('now'), datetime('now'))",
        libsql::params![
            user_id.clone(),
            payload.email.clone(),
            payload.name.clone(),
            password_hash,
            payload.role.clone(),
            expires_at_value,
        ],
    )
    .await
    .map_err(|e| {
        tracing::error!("ユーザー作成エラー: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("ユーザー作成エラー: {}", e), "status": 500})),
        )
    })?;

    // 監査ログ
    let _ = conn
        .execute(
            "INSERT INTO audit_logs (id, user_id, action, details, created_at) VALUES (?1, ?2, ?3, ?4, datetime('now'))",
            libsql::params![
                Uuid::new_v4().to_string(),
                claims.sub.clone(),
                "create_user".to_string(),
                format!("ユーザー作成: {} ({})", payload.email, payload.role),
            ],
        )
        .await;

    tracing::info!("ユーザー作成: {} ({})", payload.email, payload.role);

    Ok((
        StatusCode::CREATED,
        Json(json!({
            "id": user_id,
            "email": payload.email,
            "name": payload.name,
            "role": payload.role,
            "is_active": true,
            "expires_at": payload.expires_at,
        })),
    ))
}

/// GET /api/users/:id - ユーザー詳細
async fn get_user(
    State(state): State<SharedState>,
    Extension(claims): Extension<Claims>,
    Path(user_id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    check_admin(&claims)?;

    let conn = state.db.connect().map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("DB接続エラー: {}", e), "status": 500})),
        )
    })?;

    let mut rows = conn
        .query(
            "SELECT id, email, name, role, is_active, expires_at, created_at, updated_at FROM users WHERE id = ?1",
            libsql::params![user_id.clone()],
        )
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("DBエラー: {}", e), "status": 500})),
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

    Ok(Json(json!({
        "id": row.get::<String>(0).unwrap_or_default(),
        "email": row.get::<String>(1).unwrap_or_default(),
        "name": row.get::<String>(2).unwrap_or_default(),
        "role": row.get::<String>(3).unwrap_or_default(),
        "is_active": row.get::<i64>(4).unwrap_or(0) == 1,
        "expires_at": row.get::<String>(5).ok(),
        "created_at": row.get::<String>(6).unwrap_or_default(),
        "updated_at": row.get::<String>(7).ok(),
    })))
}

/// PUT /api/users/:id - ユーザー更新
async fn update_user(
    State(state): State<SharedState>,
    Extension(claims): Extension<Claims>,
    Path(user_id): Path<String>,
    Json(payload): Json<UpdateUserRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    check_admin(&claims)?;

    let conn = state.db.connect().map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("DB接続エラー: {}", e), "status": 500})),
        )
    })?;

    // 既存ユーザーの存在確認
    let mut existing = conn
        .query(
            "SELECT id FROM users WHERE id = ?1",
            libsql::params![user_id.clone()],
        )
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("DBエラー: {}", e), "status": 500})),
            )
        })?;

    if existing.next().await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("データ取得エラー: {}", e), "status": 500})),
        )
    })?.is_none() {
        return Err((
            StatusCode::NOT_FOUND,
            Json(json!({"error": "ユーザーが見つかりません", "status": 404})),
        ));
    }

    // 動的にUPDATE文を構築
    let mut set_clauses = Vec::new();
    let mut params: Vec<libsql::Value> = Vec::new();
    let mut param_idx = 1;

    if let Some(ref email) = payload.email {
        set_clauses.push(format!("email = ?{}", param_idx));
        params.push(libsql::Value::Text(email.clone()));
        param_idx += 1;
    }
    if let Some(ref name) = payload.name {
        set_clauses.push(format!("name = ?{}", param_idx));
        params.push(libsql::Value::Text(name.clone()));
        param_idx += 1;
    }
    if let Some(ref password) = payload.password {
        let hashed = hash_password(password).map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("パスワードハッシュエラー: {}", e), "status": 500})),
            )
        })?;
        set_clauses.push(format!("password_hash = ?{}", param_idx));
        params.push(libsql::Value::Text(hashed));
        param_idx += 1;
    }
    if let Some(ref role) = payload.role {
        let valid_roles = ["admin", "consultant", "sales", "viewer"];
        if !valid_roles.contains(&role.as_str()) {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({"error": format!("無効なロールです。有効値: {:?}", valid_roles), "status": 400})),
            ));
        }
        set_clauses.push(format!("role = ?{}", param_idx));
        params.push(libsql::Value::Text(role.clone()));
        param_idx += 1;
    }
    if let Some(is_active) = payload.is_active {
        set_clauses.push(format!("is_active = ?{}", param_idx));
        params.push(libsql::Value::Integer(if is_active { 1 } else { 0 }));
        param_idx += 1;
    }
    if let Some(ref expires_at) = payload.expires_at {
        if expires_at.is_empty() {
            set_clauses.push(format!("expires_at = NULL"));
        } else {
            set_clauses.push(format!("expires_at = ?{}", param_idx));
            params.push(libsql::Value::Text(expires_at.clone()));
            param_idx += 1;
        }
    }

    if set_clauses.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "更新するフィールドがありません", "status": 400})),
        ));
    }

    // updated_at も更新
    set_clauses.push("updated_at = datetime('now')".to_string());

    let sql = format!(
        "UPDATE users SET {} WHERE id = ?{}",
        set_clauses.join(", "),
        param_idx
    );
    params.push(libsql::Value::Text(user_id.clone()));

    conn.execute(&sql, params)
        .await
        .map_err(|e| {
            tracing::error!("ユーザー更新エラー: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("更新エラー: {}", e), "status": 500})),
            )
        })?;

    // 監査ログ
    let _ = conn
        .execute(
            "INSERT INTO audit_logs (id, user_id, action, details, created_at) VALUES (?1, ?2, ?3, ?4, datetime('now'))",
            libsql::params![
                Uuid::new_v4().to_string(),
                claims.sub,
                "update_user".to_string(),
                format!("ユーザー更新: {}", user_id),
            ],
        )
        .await;

    Ok(Json(json!({"message": "ユーザーを更新しました", "id": user_id})))
}

/// DELETE /api/users/:id - ユーザー削除（論理削除: is_active = 0）
async fn delete_user(
    State(state): State<SharedState>,
    Extension(claims): Extension<Claims>,
    Path(user_id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    check_admin(&claims)?;

    // 自分自身は削除不可
    if claims.sub == user_id {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "自分自身は削除できません", "status": 400})),
        ));
    }

    let conn = state.db.connect().map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("DB接続エラー: {}", e), "status": 500})),
        )
    })?;

    // 論理削除（is_active = 0 に設定）
    let affected = conn
        .execute(
            "UPDATE users SET is_active = 0, updated_at = datetime('now') WHERE id = ?1",
            libsql::params![user_id.clone()],
        )
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("削除エラー: {}", e), "status": 500})),
            )
        })?;

    if affected == 0 {
        return Err((
            StatusCode::NOT_FOUND,
            Json(json!({"error": "ユーザーが見つかりません", "status": 404})),
        ));
    }

    // セッションも削除
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
            claims.sub,
            "delete_user".to_string(),
            format!("ユーザー無効化: {}", user_id),
        ],
    )
        .await;

    Ok(Json(json!({"message": "ユーザーを無効化しました", "id": user_id})))
}

/// GET /api/users/audit-log - 操作ログ一覧
async fn audit_log(
    State(state): State<SharedState>,
    Extension(claims): Extension<Claims>,
    Query(query): Query<AuditLogQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    check_admin(&claims)?;

    let limit = query.limit.unwrap_or(50).min(200);
    let offset = query.offset.unwrap_or(0);

    let conn = state.db.connect().map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("DB接続エラー: {}", e), "status": 500})),
        )
    })?;

    // フィルタ条件を動的に構築
    let mut where_clauses = Vec::new();
    let mut params: Vec<libsql::Value> = Vec::new();
    let mut param_idx = 1;

    if let Some(ref uid) = query.user_id {
        where_clauses.push(format!("user_id = ?{}", param_idx));
        params.push(libsql::Value::Text(uid.clone()));
        param_idx += 1;
    }
    if let Some(ref action) = query.action {
        where_clauses.push(format!("action = ?{}", param_idx));
        params.push(libsql::Value::Text(action.clone()));
        param_idx += 1;
    }

    let where_sql = if where_clauses.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", where_clauses.join(" AND "))
    };

    let sql = format!(
        "SELECT id, user_id, action, details, ip_address, created_at FROM audit_logs {} ORDER BY created_at DESC LIMIT ?{} OFFSET ?{}",
        where_sql, param_idx, param_idx + 1
    );
    params.push(libsql::Value::Integer(limit as i64));
    params.push(libsql::Value::Integer(offset as i64));

    let mut rows = conn
        .query(&sql, params)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("クエリエラー: {}", e), "status": 500})),
            )
        })?;

    let mut logs = Vec::new();
    while let Some(row) = rows.next().await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("行取得エラー: {}", e), "status": 500})),
        )
    })? {
        logs.push(json!({
            "id": row.get::<String>(0).unwrap_or_default(),
            "user_id": row.get::<String>(1).ok(),
            "action": row.get::<String>(2).unwrap_or_default(),
            "details": row.get::<String>(3).ok(),
            "ip_address": row.get::<String>(4).ok(),
            "created_at": row.get::<String>(5).unwrap_or_default(),
        }));
    }

    Ok(Json(json!({
        "logs": logs,
        "limit": limit,
        "offset": offset,
    })))
}
