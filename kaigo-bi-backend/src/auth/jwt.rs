/// JWT トークンの生成と検証
/// HS256アルゴリズムを使用し、ユーザー情報をClaimsに埋め込む

use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};

/// JWTクレーム（トークンに含める情報）
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Claims {
    /// ユーザーID（subject）
    pub sub: String,
    /// メールアドレス
    pub email: String,
    /// ユーザー名
    pub name: String,
    /// ロール（admin, consultant, sales, viewer）
    pub role: String,
    /// 有効期限（UNIXタイムスタンプ）
    pub exp: usize,
    /// 発行時刻
    pub iat: usize,
}

/// JWT秘密鍵を環境変数から取得（未設定時はランダム生成）
fn get_secret() -> String {
    std::env::var("JWT_SECRET").unwrap_or_else(|_| {
        tracing::warn!("JWT_SECRET が設定されていません。ランダムな値を使用します（本番環境では必ず設定してください）");
        uuid::Uuid::new_v4().to_string()
    })
}

/// JWTトークンを生成する（有効期間24時間）
pub fn create_token(
    user_id: &str,
    email: &str,
    name: &str,
    role: &str,
) -> Result<String, jsonwebtoken::errors::Error> {
    let now = chrono::Utc::now().timestamp() as usize;
    let expiration = now + 24 * 60 * 60; // 24時間

    let claims = Claims {
        sub: user_id.to_string(),
        email: email.to_string(),
        name: name.to_string(),
        role: role.to_string(),
        exp: expiration,
        iat: now,
    };

    let secret = get_secret();
    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
}

/// JWTトークンを検証し、Claimsを返す
pub fn verify_token(token: &str) -> Result<Claims, jsonwebtoken::errors::Error> {
    let secret = get_secret();
    let token_data = decode::<Claims>(
        token,
        &DecodingKey::from_secret(secret.as_bytes()),
        &Validation::default(),
    )?;
    Ok(token_data.claims)
}
