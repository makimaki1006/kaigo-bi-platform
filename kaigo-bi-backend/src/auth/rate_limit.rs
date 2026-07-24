/// メモリ内レート制限（スライディングウィンドウ方式）
/// 単一インスタンス運用（Render 1台）を前提とした簡易実装。
/// 複数インスタンスにスケールする際はRedis等の共有ストアに置き換えること。

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

/// キー(用途:識別子) → 試行時刻のリスト
static ATTEMPTS: OnceLock<Mutex<HashMap<String, Vec<Instant>>>> = OnceLock::new();

fn store() -> &'static Mutex<HashMap<String, Vec<Instant>>> {
    ATTEMPTS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// 制限ポリシー
pub struct RateLimitPolicy {
    /// ウィンドウ内の最大試行回数
    pub max_attempts: usize,
    /// ウィンドウ幅
    pub window: Duration,
}

/// ログイン失敗: 同一キーで15分に5回まで
pub const LOGIN_FAILURE: RateLimitPolicy = RateLimitPolicy {
    max_attempts: 5,
    window: Duration::from_secs(15 * 60),
};

/// サインアップ: 同一IPで1時間に10回まで
pub const SIGNUP: RateLimitPolicy = RateLimitPolicy {
    max_attempts: 10,
    window: Duration::from_secs(60 * 60),
};

/// パスワードリセット要求（メール単位）: 1時間に3回まで
pub const PASSWORD_RESET: RateLimitPolicy = RateLimitPolicy {
    max_attempts: 3,
    window: Duration::from_secs(60 * 60),
};

/// パスワードリセット要求（IP単位）: 1時間に10回まで
/// 企業NAT配下で複数ユーザーが同一IPになるケースを考慮してメール単位より緩め
pub const PASSWORD_RESET_IP: RateLimitPolicy = RateLimitPolicy {
    max_attempts: 10,
    window: Duration::from_secs(60 * 60),
};

/// 試行回数が上限に達しているか確認する（記録はしない）
pub fn is_limited(scope: &str, identifier: &str, policy: &RateLimitPolicy) -> bool {
    let key = format!("{}:{}", scope, identifier);
    let now = Instant::now();
    let mut map = match store().lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };

    // 期限切れエントリの掃除（アクセスのあったキーのみ）
    if let Some(attempts) = map.get_mut(&key) {
        attempts.retain(|t| now.duration_since(*t) < policy.window);
        return attempts.len() >= policy.max_attempts;
    }
    false
}

/// 試行を記録する（失敗時・要求時に呼ぶ）
pub fn record_attempt(scope: &str, identifier: &str) {
    let key = format!("{}:{}", scope, identifier);
    let mut map = match store().lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    map.entry(key).or_default().push(Instant::now());

    // メモリ肥大防止: キー数が多すぎる場合は全体を掃除
    if map.len() > 10_000 {
        let now = Instant::now();
        let max_window = Duration::from_secs(60 * 60);
        map.retain(|_, attempts| {
            attempts.retain(|t| now.duration_since(*t) < max_window);
            !attempts.is_empty()
        });
    }
}

/// 成功時にカウンタをクリアする（ログイン成功後など）
pub fn clear_attempts(scope: &str, identifier: &str) {
    let key = format!("{}:{}", scope, identifier);
    if let Ok(mut map) = store().lock() {
        map.remove(&key);
    }
}

/// リクエストヘッダーからクライアントIPを取得する
/// Render等のプロキシ配下では X-Forwarded-For の先頭が実クライアントIP
pub fn client_ip(headers: &axum::http::HeaderMap) -> String {
    headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(',').next())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limit_flow() {
        let policy = RateLimitPolicy {
            max_attempts: 3,
            window: Duration::from_secs(60),
        };
        let id = "test-user-rate-limit-flow";

        assert!(!is_limited("test", id, &policy));
        record_attempt("test", id);
        record_attempt("test", id);
        assert!(!is_limited("test", id, &policy));
        record_attempt("test", id);
        assert!(is_limited("test", id, &policy));

        // 成功でクリア
        clear_attempts("test", id);
        assert!(!is_limited("test", id, &policy));
    }

    #[test]
    fn test_client_ip_from_forwarded_header() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("x-forwarded-for", "203.0.113.5, 10.0.0.1".parse().unwrap());
        assert_eq!(client_ip(&headers), "203.0.113.5");

        let empty = axum::http::HeaderMap::new();
        assert_eq!(client_ip(&empty), "unknown");
    }
}
