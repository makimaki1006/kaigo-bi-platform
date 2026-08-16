/// データAPIのレート制限
///
/// 実証（2026-08-16、自社システムに対する検証）:
///   正規アカウント1つで /api/facilities/search を12回叩くだけで
///   1,200施設の事業所名・法人名・電話番号・住所が抜けた。抵抗はゼロ。
///   ログには残るが、能動的に見ない限り気づけず、止まる仕組みも無かった。
///
/// ここで止めるのは「大量取得」だけ。通常の画面操作は1画面あたり数リクエストで、
/// 下記の上限には遠く届かない。
///
/// 制約: カウンタはメモリ内（rate_limit.rs）。単一インスタンス前提で、
/// 再起動すると消える。複数インスタンスへ広げるときは共有ストアへ移すこと。
use std::time::Duration;

use axum::{
    body::Body,
    extract::Request,
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

use crate::auth::jwt::Claims;
use crate::auth::rate_limit::{is_limited, record_attempt, RateLimitPolicy};

/// 一覧・エクスポート系。1ページ100件なので、ここが全件取得の主経路になる
const LIST_PER_MIN: RateLimitPolicy = RateLimitPolicy {
    max_attempts: 20,
    window: Duration::from_secs(60),
};
const LIST_PER_HOUR: RateLimitPolicy = RateLimitPolicy {
    max_attempts: 200,
    window: Duration::from_secs(60 * 60),
};

/// 詳細系。IDを総当たりすれば一覧より速く抜けるので、こちらも塞ぐ。
/// 画面のクリック操作を邪魔しない範囲で緩めに置く
const DETAIL_PER_MIN: RateLimitPolicy = RateLimitPolicy {
    max_attempts: 120,
    window: Duration::from_secs(60),
};
const DETAIL_PER_HOUR: RateLimitPolicy = RateLimitPolicy {
    max_attempts: 1_200,
    window: Duration::from_secs(60 * 60),
};

/// パスから制限区分を決める
fn category(path: &str) -> Option<&'static str> {
    if path.starts_with("/api/facilities/search")
        || path.starts_with("/api/export")
        || path.starts_with("/api/ma/screening")
    {
        return Some("list");
    }
    // /api/facilities/{id} や /api/dd/report/{corp} のような単票取得
    if path.starts_with("/api/facilities/")
        || path.starts_with("/api/dd/report/")
        || path.starts_with("/api/corp/")
    {
        return Some("detail");
    }
    None
}

fn too_many(retry_after_secs: u64, detail: &str) -> Response {
    (
        StatusCode::TOO_MANY_REQUESTS,
        [("Retry-After", retry_after_secs.to_string())],
        Json(json!({
            "error": "短時間に大量のリクエストを検知したため、一時的に制限しています",
            "detail": detail,
            "hint": "大量のデータが必要な場合はCSVエクスポートをご利用ください",
            "status": 429
        })),
    )
        .into_response()
}

/// auth_middleware の内側に置く（Claims が必要なため）
pub async fn data_rate_limit_middleware(req: Request<Body>, next: Next) -> Response {
    let Some(cat) = category(req.uri().path()) else {
        return next.run(req).await;
    };
    let Some(claims) = req.extensions().get::<Claims>().cloned() else {
        return next.run(req).await;
    };

    let (per_min, per_hour, min_scope, hour_scope) = match cat {
        "list" => (&LIST_PER_MIN, &LIST_PER_HOUR, "list_min", "list_hour"),
        _ => (&DETAIL_PER_MIN, &DETAIL_PER_HOUR, "detail_min", "detail_hour"),
    };

    if is_limited(min_scope, &claims.sub, per_min) {
        tracing::warn!(
            "レート制限: user={} path={} ({}の分あたり上限)",
            claims.sub, req.uri().path(), cat
        );
        return too_many(60, "1分あたりの上限に達しました");
    }
    if is_limited(hour_scope, &claims.sub, per_hour) {
        tracing::warn!(
            "レート制限: user={} path={} ({}の時間あたり上限)",
            claims.sub, req.uri().path(), cat
        );
        return too_many(600, "1時間あたりの上限に達しました");
    }

    record_attempt(min_scope, &claims.sub);
    record_attempt(hour_scope, &claims.sub);

    next.run(req).await
}
