/// 認証済みリクエストをアクセスログに残すミドルウェア
///
/// auth_middleware の**内側**に置くこと。外側だと Claims がまだ入っていない。
/// routes/mod.rs では
///     .layer(from_fn(access_log_middleware))   // 内側（後に走る）
///     .layer(from_fn(auth_middleware))         // 外側（先に走る）
/// の順で積む。
use std::time::Instant;

use axum::{body::Body, extract::Request, middleware::Next, response::Response};

use crate::auth::jwt::Claims;
use crate::auth::rate_limit::client_ip;
use crate::services::access_log::{record, AccessEntry};

pub async fn access_log_middleware(req: Request<Body>, next: Next) -> Response {
    let started = Instant::now();

    // 認証済みなので Claims は入っているはずだが、無くても落とさない
    let claims = req.extensions().get::<Claims>().cloned();
    let method = req.method().to_string();
    let path = req.uri().path().to_string();
    let query = req.uri().query().map(|q| q.to_string());
    let ip = client_ip(req.headers());
    let user_agent = req
        .headers()
        .get(axum::http::header::USER_AGENT)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.chars().take(300).collect::<String>());

    let response = next.run(req).await;

    // 取得量の測り方。
    // 当初 Content-Length だけを見ていたが、axum の Json レスポンスには
    // 付かないことが多く、実測で全件 -1 になった。それでは
    // 「どれだけ持って行かれたか」が分からず、記録する意味が半減する。
    // ヘッダが無いときは本文を読んで測り、同じ内容で組み直して返す。
    let (response, response_bytes) = match response
        .headers()
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<i64>().ok())
    {
        Some(len) => (response, len),
        None => {
            let (parts, body) = response.into_parts();
            // 上限つきで読む。これを超えるレスポンスは測定を諦める（メモリ保護）
            match axum::body::to_bytes(body, 32 * 1024 * 1024).await {
                Ok(bytes) => {
                    let len = bytes.len() as i64;
                    (Response::from_parts(parts, Body::from(bytes)), len)
                }
                Err(_) => (Response::from_parts(parts, Body::empty()), -1),
            }
        }
    };

    if let Some(c) = claims {
        record(AccessEntry {
            user_id: c.sub,
            email: c.email,
            plan: c.plan,
            method,
            path,
            query,
            status: response.status().as_u16(),
            response_bytes,
            duration_ms: started.elapsed().as_millis() as i64,
            ip,
            user_agent,
        });
    }

    response
}
