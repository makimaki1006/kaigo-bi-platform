/// パスワードハッシュの生成と検証
/// 新規作成: argon2
/// 既存互換: pbkdf2:sha256:iterations$salt$hash 形式も検証可能

use argon2::{
    password_hash::{rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use hmac::Hmac;
use sha2::Sha256;

/// argon2でパスワードをハッシュ化（新規ユーザー用）
pub fn hash_password(password: &str) -> Result<String, String> {
    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();
    let hash = argon2
        .hash_password(password.as_bytes(), &salt)
        .map_err(|e| format!("パスワードハッシュ生成エラー: {}", e))?;
    Ok(hash.to_string())
}

/// パスワードを検証する（argon2形式とpbkdf2形式の両方に対応）
pub fn verify_password(password: &str, hash: &str) -> Result<bool, String> {
    if hash.starts_with("pbkdf2:sha256:") {
        // 既存のpbkdf2形式: pbkdf2:sha256:iterations$salt$hash
        verify_pbkdf2(password, hash)
    } else if hash.starts_with("$argon2") {
        // argon2形式
        verify_argon2(password, hash)
    } else {
        Err(format!("不明なハッシュ形式: {}", &hash[..20.min(hash.len())]))
    }
}

/// argon2形式のハッシュを検証
fn verify_argon2(password: &str, hash: &str) -> Result<bool, String> {
    let parsed_hash =
        PasswordHash::new(hash).map_err(|e| format!("argon2ハッシュパースエラー: {}", e))?;
    Ok(Argon2::default()
        .verify_password(password.as_bytes(), &parsed_hash)
        .is_ok())
}

/// pbkdf2:sha256:iterations$salt$hash 形式のハッシュを検証
/// Pythonのwerkzeug互換フォーマット
fn verify_pbkdf2(password: &str, hash: &str) -> Result<bool, String> {
    // "pbkdf2:sha256:iterations$salt$hash" をパース
    let without_prefix = hash
        .strip_prefix("pbkdf2:sha256:")
        .ok_or("pbkdf2プレフィックスが不正です")?;

    let parts: Vec<&str> = without_prefix.splitn(3, '$').collect();
    if parts.len() != 3 {
        return Err(format!(
            "pbkdf2ハッシュ形式が不正です（$区切りで3パート必要、実際: {}）",
            parts.len()
        ));
    }

    let iterations: u32 = parts[0]
        .parse()
        .map_err(|_| "イテレーション数のパースに失敗しました")?;
    let salt = parts[1];
    let expected_hash = parts[2];

    // PBKDF2-HMAC-SHA256 でハッシュを計算
    let mut derived_key = vec![0u8; expected_hash.len() / 2]; // hexデコード後のバイト長
    pbkdf2::pbkdf2::<Hmac<Sha256>>(
        password.as_bytes(),
        salt.as_bytes(),
        iterations,
        &mut derived_key,
    )
    .map_err(|e| format!("PBKDF2計算エラー: {}", e))?;

    let computed_hex = hex::encode(&derived_key);
    Ok(computed_hex == expected_hash)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_argon2_hash_and_verify() {
        let password = "test_password_123";
        let hash = hash_password(password).unwrap();
        assert!(hash.starts_with("$argon2"));
        assert!(verify_password(password, &hash).unwrap());
        assert!(!verify_password("wrong_password", &hash).unwrap());
    }
}
