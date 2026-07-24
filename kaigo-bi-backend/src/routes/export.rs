/// CSVエクスポートAPIエンドポイント
/// BOM付きUTF-8 CSVストリームを返す
/// Turso SQLで直接クエリ
///
/// プラン別の月間エクスポート行数制限:
///   free/standard: 不可（proプラン以上が必要 — ルーター側のプランゲートで制御）
///   pro: 3,000行/月
///   ma: 10,000行/月
///   admin: 無制限

use axum::{
    extract::{Extension, Query, State},
    http::header,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use serde_json::json;
use uuid::Uuid;

use crate::auth::jwt::Claims;
use crate::error::AppError;
use crate::models::filters::FilterParams;
use crate::routes::SharedState;
use crate::services::sql_aggregator;

/// プラン別の月間エクスポート上限行数（adminはNone=無制限）
fn monthly_export_limit(role: &str, plan: &str) -> Option<i64> {
    if role == "admin" {
        return None;
    }
    match plan {
        "pro" => Some(3_000),
        "ma" => Some(10_000),
        _ => Some(0), // free/standard はプランゲートで弾かれる想定だが二重防御
    }
}

/// エクスポートルーター
pub fn router() -> Router<SharedState> {
    Router::new()
        .route("/api/export/csv", get(export_csv))
        .route("/api/export/usage", get(export_usage))
}

/// 当月のエクスポート済み行数を取得
async fn fetch_monthly_usage(state: &SharedState, user_id: &str) -> Result<i64, AppError> {
    let conn = state.db.connect().map_err(|e| {
        AppError::Internal(format!("DB接続エラー: {}", e))
    })?;
    let mut rows = conn
        .query(
            "SELECT COALESCE(SUM(row_count), 0) FROM export_logs WHERE user_id = ?1 AND created_at >= datetime('now', 'start of month')",
            libsql::params![user_id.to_string()],
        )
        .await
        .map_err(|e| AppError::Internal(format!("使用量取得エラー: {}", e)))?;
    let used: i64 = match rows.next().await {
        Ok(Some(row)) => row.get::<i64>(0).unwrap_or(0),
        _ => 0,
    };
    Ok(used)
}

/// GET /api/export/usage
/// 当月のエクスポート使用量と上限を返す（UI表示用）
async fn export_usage(
    State(state): State<SharedState>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<serde_json::Value>, AppError> {
    let used = fetch_monthly_usage(&state, &claims.sub).await?;
    let limit = monthly_export_limit(&claims.role, &claims.plan);
    Ok(Json(json!({
        "used": used,
        "limit": limit, // null = 無制限
        "plan": claims.plan,
    })))
}

/// GET /api/export/csv
/// フィルタ条件に基づいてCSVをダウンロード
/// Content-Type: text/csv; charset=utf-8
/// Content-Disposition: attachment; filename="kaigo_data.csv"
async fn export_csv(
    State(state): State<SharedState>,
    Extension(claims): Extension<Claims>,
    Query(params): Query<FilterParams>,
) -> Result<impl IntoResponse, AppError> {
    // 月間クレジット確認
    let limit = monthly_export_limit(&claims.role, &claims.plan);
    if let Some(limit) = limit {
        let used = fetch_monthlyusage_or_zero(&state, &claims.sub).await;
        if used >= limit {
            return Err(AppError::Forbidden(format!(
                "今月のエクスポート上限（{}行）に達しました。上位プランへのアップグレードをご検討ください",
                limit
            )));
        }
    }

    // Turso SQLでCSVバイト列を生成
    let csv_bytes = sql_aggregator::export_csv(&state.db, &params).await?;

    // 行数をカウント（ヘッダー行とBOMを除く）
    let row_count = count_csv_rows(&csv_bytes);

    // 上限の残りを超える場合は拒否（生成後の確定行数で最終判定）
    if let Some(limit) = limit {
        let used = fetch_monthlyusage_or_zero(&state, &claims.sub).await;
        if used + row_count > limit {
            return Err(AppError::Forbidden(format!(
                "この条件のエクスポート（{}行）は今月の残りクレジット（{}行）を超えています。条件を絞り込んでください",
                row_count,
                (limit - used).max(0)
            )));
        }
    }

    // 使用量を記録
    if let Ok(conn) = state.db.connect() {
        let _ = conn
            .execute(
                "INSERT INTO export_logs (id, user_id, row_count, created_at) VALUES (?1, ?2, ?3, datetime('now'))",
                libsql::params![
                    Uuid::new_v4().to_string(),
                    claims.sub.clone(),
                    row_count,
                ],
            )
            .await;
    }

    // レスポンスヘッダー設定
    let headers = [
        (
            header::CONTENT_TYPE,
            "text/csv; charset=utf-8".to_string(),
        ),
        (
            header::CONTENT_DISPOSITION,
            "attachment; filename=\"kaigo_data.csv\"".to_string(),
        ),
    ];

    Ok((headers, csv_bytes))
}

/// 使用量取得（エラー時は0扱い — エクスポートを止めないため）
async fn fetch_monthlyusage_or_zero(state: &SharedState, user_id: &str) -> i64 {
    fetch_monthly_usage(state, user_id).await.unwrap_or(0)
}

/// CSVバイト列のデータ行数を数える（ヘッダー1行を除く）
fn count_csv_rows(csv_bytes: &[u8]) -> i64 {
    let newlines = csv_bytes.iter().filter(|&&b| b == b'\n').count() as i64;
    // 最終行に改行がない場合も1行として数え、ヘッダー分を引く
    let has_trailing_newline = csv_bytes.last() == Some(&b'\n');
    let total_lines = if has_trailing_newline {
        newlines
    } else if csv_bytes.is_empty() {
        0
    } else {
        newlines + 1
    };
    (total_lines - 1).max(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count_csv_rows() {
        assert_eq!(count_csv_rows(b"h1,h2\na,b\nc,d\n"), 2);
        assert_eq!(count_csv_rows(b"h1,h2\na,b"), 1);
        assert_eq!(count_csv_rows(b"h1,h2\n"), 0);
        assert_eq!(count_csv_rows(b""), 0);
    }

    #[test]
    fn test_monthly_export_limit() {
        assert_eq!(monthly_export_limit("admin", "free"), None);
        assert_eq!(monthly_export_limit("viewer", "pro"), Some(3_000));
        assert_eq!(monthly_export_limit("viewer", "ma"), Some(10_000));
        assert_eq!(monthly_export_limit("viewer", "free"), Some(0));
    }
}
