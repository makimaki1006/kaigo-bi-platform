/// SQL集計ロジック
/// DataFrameの代わりにTurso SQLで直接集計を実行する
/// フィルタ付きリクエストのフォールバック処理用

use chrono::Datelike;
use libsql::Database;
use serde_json::{json, Value};

use crate::error::AppError;
use crate::models::filters::{FilterParams, SearchParams};

/// WHERE句とパラメータを構築するヘルパー（パラメタライズドクエリ対応）
struct WhereBuilder {
    conditions: Vec<String>,
    params: Vec<libsql::Value>,
    param_counter: usize,
}

impl WhereBuilder {
    fn new() -> Self {
        Self {
            conditions: Vec::new(),
            params: Vec::new(),
            param_counter: 0,
        }
    }

    /// 次のパラメータプレースホルダ番号を取得してインクリメント
    fn next_param(&mut self) -> usize {
        self.param_counter += 1;
        self.param_counter
    }

    /// フィルタパラメータからWHERE句を構築（パラメタライズドクエリ）
    fn from_filter_params(params: &FilterParams) -> Self {
        let mut builder = Self::new();

        if let Some(ref pref) = params.prefecture {
            let prefs: Vec<&str> = pref.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()).collect();
            if !prefs.is_empty() {
                let placeholders: Vec<String> = prefs.iter().map(|p| {
                    let idx = builder.next_param();
                    builder.params.push(libsql::Value::Text(p.to_string()));
                    format!("?{}", idx)
                }).collect();
                builder.conditions.push(format!("prefecture IN ({})", placeholders.join(",")));
            }
        }

        if let Some(ref sc) = params.service_code {
            let codes: Vec<&str> = sc.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()).collect();
            if !codes.is_empty() {
                let placeholders: Vec<String> = codes.iter().map(|c| {
                    let idx = builder.next_param();
                    builder.params.push(libsql::Value::Text(c.to_string()));
                    format!("?{}", idx)
                }).collect();
                builder.conditions.push(format!("\"サービスコード\" IN ({})", placeholders.join(",")));
            }
        }

        if let Some(ref ct) = params.corp_type {
            let types: Vec<&str> = ct.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()).collect();
            if !types.is_empty() {
                let placeholders: Vec<String> = types.iter().map(|t| {
                    let idx = builder.next_param();
                    builder.params.push(libsql::Value::Text(t.to_string()));
                    format!("?{}", idx)
                }).collect();
                builder.conditions.push(format!("corp_type IN ({})", placeholders.join(",")));
            }
        }

        if let Some(min) = params.staff_min {
            let idx = builder.next_param();
            builder.params.push(libsql::Value::Real(min));
            builder.conditions.push(format!(
                "CAST(COALESCE(NULLIF(\"従業者_合計\", ''), '0') AS REAL) >= ?{}",
                idx
            ));
        }

        if let Some(max) = params.staff_max {
            let idx = builder.next_param();
            builder.params.push(libsql::Value::Real(max));
            builder.conditions.push(format!(
                "CAST(COALESCE(NULLIF(\"従業者_合計\", ''), '0') AS REAL) <= ?{}",
                idx
            ));
        }

        if let Some(ref kw) = params.keyword {
            let kw = kw.trim();
            if !kw.is_empty() {
                let like_val = format!("%{}%", kw);
                let idx1 = builder.next_param();
                builder.params.push(libsql::Value::Text(like_val.clone()));
                let idx2 = builder.next_param();
                builder.params.push(libsql::Value::Text(like_val.clone()));
                let idx3 = builder.next_param();
                builder.params.push(libsql::Value::Text(like_val));
                builder.conditions.push(format!(
                    "(\"住所\" LIKE ?{} OR \"事業所名\" LIKE ?{} OR municipality LIKE ?{})",
                    idx1, idx2, idx3
                ));
            }
        }

        builder
    }

    /// WHERE句を生成（条件がなければ空文字列）
    fn to_where_clause(&self) -> String {
        if self.conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", self.conditions.join(" AND "))
        }
    }

    /// パラメータをlibsql::Value のVecとして返す
    fn into_params(self) -> Vec<libsql::Value> {
        self.params
    }

    /// パラメータの参照を返す（パラメータを消費しない）
    fn params_ref(&self) -> &[libsql::Value] {
        &self.params
    }

    /// パラメータをクローンして返す（複数回のクエリ実行用）
    fn clone_params(&self) -> Vec<libsql::Value> {
        self.params.clone()
    }

    /// 追加のLIKEパラメータを付与して新しいパラメータVecを返す
    fn params_with_like(&self, like_val: &str) -> (Vec<libsql::Value>, usize, usize, usize) {
        let mut params = self.params.clone();
        let idx1 = params.len() + 1;
        params.push(libsql::Value::Text(like_val.to_string()));
        let idx2 = params.len() + 1;
        params.push(libsql::Value::Text(like_val.to_string()));
        let idx3 = params.len() + 1;
        params.push(libsql::Value::Text(like_val.to_string()));
        (params, idx1, idx2, idx3)
    }

    /// 追加のテキストパラメータを付与して新しいパラメータVecを返す
    fn params_with_text(&self, val: &str) -> (Vec<libsql::Value>, usize) {
        let mut params = self.params.clone();
        let idx = params.len() + 1;
        params.push(libsql::Value::Text(val.to_string()));
        (params, idx)
    }

    /// 複数のテキストパラメータを追加
    fn params_with_texts(&self, vals: &[&str]) -> (Vec<libsql::Value>, Vec<usize>) {
        let mut params = self.params.clone();
        let mut indices = Vec::new();
        for val in vals {
            let idx = params.len() + 1;
            params.push(libsql::Value::Text(val.to_string()));
            indices.push(idx);
        }
        (params, indices)
    }
}

/// Turso接続を取得するヘルパー
async fn get_conn(db: &Database) -> Result<libsql::Connection, AppError> {
    db.connect().map_err(|e| AppError::Internal(format!("Turso接続エラー: {}", e)))
}

/// パラメタライズドクエリで単一行を取得
async fn query_single_row_params(conn: &libsql::Connection, sql: &str, params: Vec<libsql::Value>) -> Result<libsql::Row, AppError> {
    let mut rows = conn
        .query(sql, params)
        .await
        .map_err(|e| AppError::Internal(format!("SQLクエリエラー: {}\nSQL: {}", e, sql)))?;

    match rows.next().await {
        Ok(Some(row)) => Ok(row),
        Ok(None) => Err(AppError::Internal("クエリ結果が空です".into())),
        Err(e) => Err(AppError::Internal(format!("行読み込みエラー: {}", e))),
    }
}

/// パラメタライズドクエリで複数行を取得
async fn query_rows_params(conn: &libsql::Connection, sql: &str, params: Vec<libsql::Value>) -> Result<Vec<libsql::Row>, AppError> {
    let mut rows = conn
        .query(sql, params)
        .await
        .map_err(|e| AppError::Internal(format!("SQLクエリエラー: {}\nSQL: {}", e, sql)))?;

    let mut result = Vec::new();
    loop {
        match rows.next().await {
            Ok(Some(row)) => result.push(row),
            Ok(None) => break,
            Err(e) => {
                tracing::warn!("行読み込みエラー: {}", e);
                break;
            }
        }
    }
    Ok(result)
}

/// Row からf64を安全に取得（NULLは0.0にフォールバック）
fn row_f64(row: &libsql::Row, idx: i32) -> f64 {
    row.get::<f64>(idx as i32).unwrap_or(0.0)
}

/// Row からf64をOption<f64>で取得
fn row_f64_opt(row: &libsql::Row, idx: i32) -> Option<f64> {
    row.get::<f64>(idx as i32).ok()
}

/// Row からi64を取得
fn row_i64(row: &libsql::Row, idx: i32) -> i64 {
    row.get::<i64>(idx as i32).unwrap_or(0)
}

/// Row からStringを取得
fn row_str(row: &libsql::Row, idx: i32) -> String {
    row.get::<String>(idx as i32).unwrap_or_default()
}

/// Row からOption<String>を取得
fn row_str_opt(row: &libsql::Row, idx: i32) -> Option<String> {
    row.get::<String>(idx as i32).ok().filter(|s| !s.is_empty())
}

// ================================================================
// ダッシュボード系
// ================================================================

/// ダッシュボードKPI
pub async fn dashboard_kpi(db: &Database, params: &FilterParams) -> Result<Value, AppError> {
    let w = WhereBuilder::from_filter_params(params);
    let where_clause = w.to_where_clause();

    let sql = format!(
        "SELECT
            COUNT(*) as total,
            AVG(CAST(COALESCE(NULLIF(\"従業者_合計\", ''), NULL) AS REAL)) as avg_staff,
            AVG(CASE WHEN CAST(COALESCE(NULLIF(\"定員\", ''), NULL) AS REAL) BETWEEN 1 AND 500
                THEN CAST(\"定員\" AS REAL) END) as avg_capacity,
            AVG(CASE WHEN turnover_rate BETWEEN 0.0 AND 1.0 THEN turnover_rate END) as avg_turnover,
            AVG(CASE WHEN fulltime_ratio BETWEEN 0.0 AND 1.0 THEN fulltime_ratio END) as avg_fulltime,
            AVG(years_in_business) as avg_years
        FROM facilities {}",
        where_clause
    );

    let conn = get_conn(db).await?;
    let row = query_single_row_params(&conn, &sql, w.into_params()).await?;

    Ok(json!({
        "total_facilities": row_i64(&row, 0),
        "avg_staff": row_f64(&row, 1),
        "avg_capacity": row_f64(&row, 2),
        "avg_turnover_rate": row_f64(&row, 3),
        "avg_fulltime_ratio": row_f64(&row, 4),
        "avg_years_in_business": row_f64(&row, 5),
    }))
}

/// 都道府県別サマリー
pub async fn dashboard_by_prefecture(db: &Database, params: &FilterParams) -> Result<Value, AppError> {
    let w = WhereBuilder::from_filter_params(params);
    let where_clause = w.to_where_clause();

    // WHERE句の適切な結合
    let sql = if where_clause.is_empty() {
        "SELECT
                prefecture,
                COUNT(*) as facility_count,
                AVG(CAST(COALESCE(NULLIF(\"従業者_合計\", ''), NULL) AS REAL)) as avg_staff,
                AVG(CASE WHEN CAST(COALESCE(NULLIF(\"定員\", ''), NULL) AS REAL) BETWEEN 1 AND 500
                    THEN CAST(\"定員\" AS REAL) END) as avg_capacity,
                AVG(CASE WHEN turnover_rate BETWEEN 0.0 AND 1.0 THEN turnover_rate END) as avg_turnover
            FROM facilities
            WHERE prefecture IS NOT NULL AND prefecture != ''
            GROUP BY prefecture
            ORDER BY facility_count DESC".to_string()
    } else {
        format!(
            "SELECT
                prefecture,
                COUNT(*) as facility_count,
                AVG(CAST(COALESCE(NULLIF(\"従業者_合計\", ''), NULL) AS REAL)) as avg_staff,
                AVG(CASE WHEN CAST(COALESCE(NULLIF(\"定員\", ''), NULL) AS REAL) BETWEEN 1 AND 500
                    THEN CAST(\"定員\" AS REAL) END) as avg_capacity,
                AVG(CASE WHEN turnover_rate BETWEEN 0.0 AND 1.0 THEN turnover_rate END) as avg_turnover
            FROM facilities
            {} AND prefecture IS NOT NULL AND prefecture != ''
            GROUP BY prefecture
            ORDER BY facility_count DESC",
            where_clause
        )
    };

    let conn = get_conn(db).await?;
    let rows = query_rows_params(&conn, &sql, w.into_params()).await?;

    let results: Vec<Value> = rows.iter().map(|row| {
        json!({
            "prefecture": row_str(row, 0),
            "facility_count": row_i64(row, 1),
            "avg_staff": row_f64(row, 2),
            "avg_capacity": row_f64(row, 3),
            "avg_turnover_rate": row_f64(row, 4),
        })
    }).collect();

    Ok(Value::Array(results))
}

/// サービス別サマリー
pub async fn dashboard_by_service(db: &Database, params: &FilterParams) -> Result<Value, AppError> {
    let w = WhereBuilder::from_filter_params(params);
    let where_clause = w.to_where_clause();
    let extra_cond = "\"サービスコード\" IS NOT NULL AND \"サービスコード\" != ''";

    let sql = build_grouped_query(
        &["\"サービスコード\"", "\"サービス名\""],
        &[
            "COUNT(*) as facility_count",
            "AVG(CAST(COALESCE(NULLIF(\"従業者_合計\", ''), NULL) AS REAL)) as avg_staff",
        ],
        &where_clause,
        extra_cond,
        "\"サービスコード\", \"サービス名\"",
        "facility_count DESC",
    );

    let conn = get_conn(db).await?;
    let rows = query_rows_params(&conn, &sql, w.into_params()).await?;

    let results: Vec<Value> = rows.iter().map(|row| {
        json!({
            "service_code": row_str(row, 0),
            "service_name": row_str(row, 1),
            "facility_count": row_i64(row, 2),
            "avg_staff": row_f64(row, 3),
        })
    }).collect();

    Ok(Value::Array(results))
}

// ================================================================
// マーケット分析系
// ================================================================

/// コロプレスマップ用都道府県メトリクス
pub async fn market_choropleth(db: &Database, params: &FilterParams) -> Result<Value, AppError> {
    let w = WhereBuilder::from_filter_params(params);
    let where_clause = w.to_where_clause();
    let extra_cond = "prefecture IS NOT NULL AND prefecture != ''";

    let sql = build_grouped_query(
        &["prefecture"],
        &[
            "COUNT(*) as facility_count",
            "AVG(CAST(COALESCE(NULLIF(\"従業者_合計\", ''), NULL) AS REAL)) as avg_staff",
            "AVG(CASE WHEN turnover_rate BETWEEN 0.0 AND 1.0 THEN turnover_rate END) as avg_turnover",
            "AVG(CASE WHEN fulltime_ratio BETWEEN 0.0 AND 1.0 THEN fulltime_ratio END) as avg_fulltime",
        ],
        &where_clause,
        extra_cond,
        "prefecture",
        "facility_count DESC",
    );

    let conn = get_conn(db).await?;
    let rows = query_rows_params(&conn, &sql, w.into_params()).await?;

    let results: Vec<Value> = rows.iter().map(|row| {
        json!({
            "prefecture": row_str(row, 0),
            "facility_count": row_i64(row, 1),
            "avg_staff": row_f64(row, 2),
            "avg_turnover_rate": row_f64(row, 3),
            "avg_fulltime_ratio": row_f64(row, 4),
        })
    }).collect();

    Ok(Value::Array(results))
}

/// サービス別棒グラフ
pub async fn market_by_service_bar(db: &Database, params: &FilterParams) -> Result<Value, AppError> {
    let w = WhereBuilder::from_filter_params(params);
    let where_clause = w.to_where_clause();
    let extra_cond = "\"サービス名\" IS NOT NULL AND \"サービス名\" != ''";

    let sql = build_grouped_query(
        &["\"サービス名\""],
        &[
            "COUNT(*) as facility_count",
            "AVG(CAST(COALESCE(NULLIF(\"従業者_合計\", ''), NULL) AS REAL)) as avg_staff",
        ],
        &where_clause,
        extra_cond,
        "\"サービス名\"",
        "facility_count DESC",
    );

    let conn = get_conn(db).await?;
    let rows = query_rows_params(&conn, &sql, w.into_params()).await?;

    let results: Vec<Value> = rows.iter().map(|row| {
        json!({
            "service_name": row_str(row, 0),
            "facility_count": row_i64(row, 1),
            "avg_staff": row_f64(row, 2),
        })
    }).collect();

    Ok(Value::Array(results))
}

/// 法人種別ドーナツチャート
pub async fn market_corp_type_donut(db: &Database, params: &FilterParams) -> Result<Value, AppError> {
    let w = WhereBuilder::from_filter_params(params);
    let where_clause = w.to_where_clause();

    // まず総数を取得
    let count_sql = format!("SELECT COUNT(*) FROM facilities {}", where_clause);
    let conn = get_conn(db).await?;
    let total_row = query_single_row_params(&conn, &count_sql, w.clone_params()).await?;
    let total = row_i64(&total_row, 0) as f64;

    let extra_cond = "corp_type IS NOT NULL AND corp_type != ''";
    let sql = build_grouped_query(
        &["corp_type"],
        &["COUNT(*) as count"],
        &where_clause,
        extra_cond,
        "corp_type",
        "count DESC",
    );

    let rows = query_rows_params(&conn, &sql, w.into_params()).await?;

    let results: Vec<Value> = rows.iter().map(|row| {
        let count = row_i64(row, 1);
        json!({
            "corp_type": row_str(row, 0),
            "count": count,
            "ratio": if total > 0.0 { count as f64 / total } else { 0.0 },
        })
    }).collect();

    Ok(Value::Array(results))
}

// ================================================================
// 人材分析系
// ================================================================

/// 人材KPI
pub async fn workforce_kpi(db: &Database, params: &FilterParams) -> Result<Value, AppError> {
    let w = WhereBuilder::from_filter_params(params);
    let where_clause = w.to_where_clause();

    let sql = format!(
        "SELECT
            AVG(CASE WHEN turnover_rate BETWEEN 0.0 AND 1.0 THEN turnover_rate END) as avg_turnover,
            AVG(CASE WHEN CAST(COALESCE(NULLIF(\"前年度採用数\", ''), NULL) AS REAL) IS NOT NULL
                AND CAST(COALESCE(NULLIF(\"従業者_合計\", ''), NULL) AS REAL) > 0
                THEN CAST(\"前年度採用数\" AS REAL) / CAST(\"従業者_合計\" AS REAL) END) as avg_hire_rate,
            AVG(CASE WHEN fulltime_ratio BETWEEN 0.0 AND 1.0 THEN fulltime_ratio END) as avg_fulltime,
            AVG(CASE WHEN CAST(REPLACE(REPLACE(COALESCE(\"経験10年以上割合\", ''), '％', ''), '%%', '') AS REAL) BETWEEN 0 AND 100
                THEN CAST(REPLACE(REPLACE(\"経験10年以上割合\", '％', ''), '%%', '') AS REAL) / 100.0 END) as avg_exp_ratio
        FROM facilities {}",
        where_clause
    );

    let conn = get_conn(db).await?;
    let row = query_single_row_params(&conn, &sql, w.into_params()).await?;

    Ok(json!({
        "avg_turnover_rate": row_f64_opt(&row, 0),
        "avg_hire_rate": row_f64_opt(&row, 1),
        "avg_fulltime_ratio": row_f64_opt(&row, 2),
        "avg_experience_10yr_ratio": row_f64_opt(&row, 3),
    }))
}

/// 離職率分布（5%刻みヒストグラム）- 単一クエリCASE WHEN方式
pub async fn workforce_turnover_distribution(db: &Database, params: &FilterParams) -> Result<Value, AppError> {
    let w = WhereBuilder::from_filter_params(params);
    let where_clause = w.to_where_clause();
    let and_prefix = if where_clause.is_empty() { "WHERE" } else { &format!("{} AND", where_clause) };

    let sql = format!(
        "SELECT
            SUM(CASE WHEN turnover_rate >= 0 AND turnover_rate < 0.05 THEN 1 ELSE 0 END),
            SUM(CASE WHEN turnover_rate >= 0.05 AND turnover_rate < 0.10 THEN 1 ELSE 0 END),
            SUM(CASE WHEN turnover_rate >= 0.10 AND turnover_rate < 0.15 THEN 1 ELSE 0 END),
            SUM(CASE WHEN turnover_rate >= 0.15 AND turnover_rate < 0.20 THEN 1 ELSE 0 END),
            SUM(CASE WHEN turnover_rate >= 0.20 AND turnover_rate < 0.25 THEN 1 ELSE 0 END),
            SUM(CASE WHEN turnover_rate >= 0.25 AND turnover_rate < 0.30 THEN 1 ELSE 0 END),
            SUM(CASE WHEN turnover_rate >= 0.30 THEN 1 ELSE 0 END)
        FROM facilities {} turnover_rate IS NOT NULL",
        and_prefix
    );

    let conn = get_conn(db).await?;
    let row = query_single_row_params(&conn, &sql, w.into_params()).await?;

    let labels = ["0-5%", "5-10%", "10-15%", "15-20%", "20-25%", "25-30%", "30%以上"];
    let results: Vec<Value> = labels.iter().enumerate().map(|(i, label)| {
        json!({
            "range": label,
            "count": row_i64(&row, i as i32),
        })
    }).collect();

    Ok(Value::Array(results))
}

/// 都道府県別人材指標
pub async fn workforce_by_prefecture(db: &Database, params: &FilterParams) -> Result<Value, AppError> {
    let w = WhereBuilder::from_filter_params(params);
    let where_clause = w.to_where_clause();
    let extra_cond = "prefecture IS NOT NULL AND prefecture != ''";

    let sql = build_grouped_query(
        &["prefecture"],
        &[
            "AVG(CASE WHEN turnover_rate BETWEEN 0.0 AND 1.0 THEN turnover_rate END) as avg_turnover",
            "AVG(CASE WHEN fulltime_ratio BETWEEN 0.0 AND 1.0 THEN fulltime_ratio END) as avg_fulltime",
            "COUNT(*) as facility_count",
        ],
        &where_clause,
        extra_cond,
        "prefecture",
        "facility_count DESC",
    );

    let conn = get_conn(db).await?;
    let rows = query_rows_params(&conn, &sql, w.into_params()).await?;

    let results: Vec<Value> = rows.iter().map(|row| {
        json!({
            "prefecture": row_str(row, 0),
            "avg_turnover_rate": row_f64(row, 1),
            "avg_fulltime_ratio": row_f64(row, 2),
            "facility_count": row_i64(row, 3),
        })
    }).collect();

    Ok(Value::Array(results))
}

/// 従業者規模別離職率 - 単一クエリCASE WHEN方式
pub async fn workforce_by_size(db: &Database, params: &FilterParams) -> Result<Value, AppError> {
    let w = WhereBuilder::from_filter_params(params);
    let where_clause = w.to_where_clause();
    let and_prefix = if where_clause.is_empty() { "WHERE" } else { &format!("{} AND", where_clause) };

    // 各規模カテゴリのCASE WHEN: count, avg_turnover, avg_fulltime を3列ずつ（計15列）
    let sql = format!(
        "SELECT
            SUM(CASE WHEN CAST(COALESCE(NULLIF(\"従業者_合計\", ''), '0') AS REAL) BETWEEN 1 AND 10 THEN 1 ELSE 0 END),
            AVG(CASE WHEN CAST(COALESCE(NULLIF(\"従業者_合計\", ''), '0') AS REAL) BETWEEN 1 AND 10 AND turnover_rate BETWEEN 0.0 AND 1.0 THEN turnover_rate END),
            AVG(CASE WHEN CAST(COALESCE(NULLIF(\"従業者_合計\", ''), '0') AS REAL) BETWEEN 1 AND 10 AND fulltime_ratio BETWEEN 0.0 AND 1.0 THEN fulltime_ratio END),
            SUM(CASE WHEN CAST(COALESCE(NULLIF(\"従業者_合計\", ''), '0') AS REAL) BETWEEN 11 AND 30 THEN 1 ELSE 0 END),
            AVG(CASE WHEN CAST(COALESCE(NULLIF(\"従業者_合計\", ''), '0') AS REAL) BETWEEN 11 AND 30 AND turnover_rate BETWEEN 0.0 AND 1.0 THEN turnover_rate END),
            AVG(CASE WHEN CAST(COALESCE(NULLIF(\"従業者_合計\", ''), '0') AS REAL) BETWEEN 11 AND 30 AND fulltime_ratio BETWEEN 0.0 AND 1.0 THEN fulltime_ratio END),
            SUM(CASE WHEN CAST(COALESCE(NULLIF(\"従業者_合計\", ''), '0') AS REAL) BETWEEN 31 AND 50 THEN 1 ELSE 0 END),
            AVG(CASE WHEN CAST(COALESCE(NULLIF(\"従業者_合計\", ''), '0') AS REAL) BETWEEN 31 AND 50 AND turnover_rate BETWEEN 0.0 AND 1.0 THEN turnover_rate END),
            AVG(CASE WHEN CAST(COALESCE(NULLIF(\"従業者_合計\", ''), '0') AS REAL) BETWEEN 31 AND 50 AND fulltime_ratio BETWEEN 0.0 AND 1.0 THEN fulltime_ratio END),
            SUM(CASE WHEN CAST(COALESCE(NULLIF(\"従業者_合計\", ''), '0') AS REAL) BETWEEN 51 AND 100 THEN 1 ELSE 0 END),
            AVG(CASE WHEN CAST(COALESCE(NULLIF(\"従業者_合計\", ''), '0') AS REAL) BETWEEN 51 AND 100 AND turnover_rate BETWEEN 0.0 AND 1.0 THEN turnover_rate END),
            AVG(CASE WHEN CAST(COALESCE(NULLIF(\"従業者_合計\", ''), '0') AS REAL) BETWEEN 51 AND 100 AND fulltime_ratio BETWEEN 0.0 AND 1.0 THEN fulltime_ratio END),
            SUM(CASE WHEN CAST(COALESCE(NULLIF(\"従業者_合計\", ''), '0') AS REAL) >= 101 THEN 1 ELSE 0 END),
            AVG(CASE WHEN CAST(COALESCE(NULLIF(\"従業者_合計\", ''), '0') AS REAL) >= 101 AND turnover_rate BETWEEN 0.0 AND 1.0 THEN turnover_rate END),
            AVG(CASE WHEN CAST(COALESCE(NULLIF(\"従業者_合計\", ''), '0') AS REAL) >= 101 AND fulltime_ratio BETWEEN 0.0 AND 1.0 THEN fulltime_ratio END)
        FROM facilities {} CAST(COALESCE(NULLIF(\"従業者_合計\", ''), '0') AS REAL) >= 1",
        and_prefix
    );

    let conn = get_conn(db).await?;
    let row = query_single_row_params(&conn, &sql, w.into_params()).await?;

    let categories = [
        "小規模(1-10)", "中規模(11-30)", "中大規模(31-50)",
        "大規模(51-100)", "超大規模(101以上)",
    ];

    let results: Vec<Value> = categories.iter().enumerate().map(|(i, label)| {
        let base = (i * 3) as i32;
        json!({
            "size_category": label,
            "count": row_i64(&row, base),
            "avg_turnover_rate": row_f64(&row, base + 1),
            "avg_fulltime_ratio": row_f64(&row, base + 2),
        })
    }).collect();

    Ok(Value::Array(results))
}

/// 経験者割合の分布 - 単一クエリCASE WHEN方式
pub async fn workforce_experience_distribution(db: &Database, params: &FilterParams) -> Result<Value, AppError> {
    let w = WhereBuilder::from_filter_params(params);
    let where_clause = w.to_where_clause();
    let and_prefix = if where_clause.is_empty() { "WHERE" } else { &format!("{} AND", where_clause) };

    let sql = format!(
        "SELECT
            SUM(CASE WHEN CAST(REPLACE(REPLACE(COALESCE(\"経験10年以上割合\", ''), '％', ''), '%%', '') AS REAL) >= 0
                AND CAST(REPLACE(REPLACE(COALESCE(\"経験10年以上割合\", ''), '％', ''), '%%', '') AS REAL) < 20 THEN 1 ELSE 0 END),
            SUM(CASE WHEN CAST(REPLACE(REPLACE(COALESCE(\"経験10年以上割合\", ''), '％', ''), '%%', '') AS REAL) >= 20
                AND CAST(REPLACE(REPLACE(COALESCE(\"経験10年以上割合\", ''), '％', ''), '%%', '') AS REAL) < 40 THEN 1 ELSE 0 END),
            SUM(CASE WHEN CAST(REPLACE(REPLACE(COALESCE(\"経験10年以上割合\", ''), '％', ''), '%%', '') AS REAL) >= 40
                AND CAST(REPLACE(REPLACE(COALESCE(\"経験10年以上割合\", ''), '％', ''), '%%', '') AS REAL) < 60 THEN 1 ELSE 0 END),
            SUM(CASE WHEN CAST(REPLACE(REPLACE(COALESCE(\"経験10年以上割合\", ''), '％', ''), '%%', '') AS REAL) >= 60
                AND CAST(REPLACE(REPLACE(COALESCE(\"経験10年以上割合\", ''), '％', ''), '%%', '') AS REAL) < 80 THEN 1 ELSE 0 END),
            SUM(CASE WHEN CAST(REPLACE(REPLACE(COALESCE(\"経験10年以上割合\", ''), '％', ''), '%%', '') AS REAL) >= 80
                AND CAST(REPLACE(REPLACE(COALESCE(\"経験10年以上割合\", ''), '％', ''), '%%', '') AS REAL) < 100.01 THEN 1 ELSE 0 END)
        FROM facilities {} \"経験10年以上割合\" IS NOT NULL AND \"経験10年以上割合\" != ''",
        and_prefix
    );

    let conn = get_conn(db).await?;
    let row = query_single_row_params(&conn, &sql, w.into_params()).await?;

    let labels = ["0-20%", "20-40%", "40-60%", "60-80%", "80-100%"];
    let results: Vec<Value> = labels.iter().enumerate().map(|(i, label)| {
        json!({
            "range": label,
            "count": row_i64(&row, i as i32),
        })
    }).collect();

    Ok(Value::Array(results))
}

/// 経験者割合 vs 離職率（都道府県別散布図）
pub async fn workforce_experience_vs_turnover(db: &Database, params: &FilterParams) -> Result<Value, AppError> {
    let w = WhereBuilder::from_filter_params(params);
    let where_clause = w.to_where_clause();
    let extra_cond = "prefecture IS NOT NULL AND prefecture != '' AND \"経験10年以上割合\" IS NOT NULL AND \"経験10年以上割合\" != ''";

    let sql = build_grouped_query(
        &["prefecture"],
        &[
            "AVG(CAST(REPLACE(REPLACE(\"経験10年以上割合\", '％', ''), '%%', '') AS REAL)) as avg_exp",
            "AVG(CASE WHEN turnover_rate BETWEEN 0.0 AND 1.0 THEN turnover_rate * 100 END) as avg_turnover",
            "COUNT(*) as facility_count",
        ],
        &where_clause,
        extra_cond,
        "prefecture",
        "facility_count DESC",
    );

    let conn = get_conn(db).await?;
    let rows = query_rows_params(&conn, &sql, w.into_params()).await?;

    let results: Vec<Value> = rows.iter().map(|row| {
        json!({
            "prefecture": row_str(row, 0),
            "avg_experience_ratio": row_f64(row, 1),
            "avg_turnover_rate": row_f64(row, 2),
            "facility_count": row_i64(row, 3),
        })
    }).collect();

    Ok(Value::Array(results))
}

// ================================================================
// 収益構造系
// ================================================================

/// 収益KPI
pub async fn revenue_kpi(db: &Database, params: &FilterParams) -> Result<Value, AppError> {
    let w = WhereBuilder::from_filter_params(params);
    let where_clause = w.to_where_clause();

    let sql = format!(
        "SELECT
            AVG(CAST(COALESCE(NULLIF(kasan_count, ''), NULL) AS REAL)) as avg_kasan,
            (SUM(CASE WHEN \"加算_処遇改善I\" = 1 OR \"処遇改善加算フラグ\" = '○' THEN 1 ELSE 0 END) * 1.0 /
                NULLIF(COUNT(*), 0)) as syogu_rate,
            AVG(CASE WHEN occupancy_rate BETWEEN 0.0 AND 3.0 THEN occupancy_rate END) as avg_occ,
            AVG(CASE WHEN CAST(COALESCE(NULLIF(\"定員\", ''), NULL) AS REAL) BETWEEN 1 AND 500
                THEN CAST(\"定員\" AS REAL) END) as avg_cap,
            AVG(CAST(COALESCE(NULLIF(quality_score, ''), NULL) AS REAL)) as avg_quality,
            AVG(CAST(COALESCE(NULLIF(\"利用者総数\", ''), NULL) AS REAL)) as avg_users
        FROM facilities {}",
        where_clause
    );

    let conn = get_conn(db).await?;
    let row = query_single_row_params(&conn, &sql, w.into_params()).await?;

    Ok(json!({
        "avg_kasan_count": row_f64_opt(&row, 0),
        "syogu_kaizen_rate": row_f64_opt(&row, 1),
        "avg_occupancy_rate": row_f64_opt(&row, 2),
        "avg_capacity": row_f64_opt(&row, 3),
        "avg_quality_score": row_f64_opt(&row, 4),
        "avg_user_count": row_f64_opt(&row, 5),
    }))
}

/// 加算取得率
pub async fn revenue_kasan_rates(db: &Database, params: &FilterParams) -> Result<Value, AppError> {
    let w = WhereBuilder::from_filter_params(params);
    let where_clause = w.to_where_clause();

    let kasan_cols = vec![
        ("処遇改善加算I", "加算_処遇改善I"),
        ("処遇改善加算II", "加算_処遇改善II"),
        ("処遇改善加算III", "加算_処遇改善III"),
        ("処遇改善加算IV", "加算_処遇改善IV"),
        ("特定事業所加算I", "加算_特定I"),
        ("特定事業所加算II", "加算_特定II"),
        ("特定事業所加算III", "加算_特定III"),
        ("特定事業所加算IV", "加算_特定IV"),
        ("特定事業所加算V", "加算_特定V"),
        ("認知症ケア加算I", "加算_認知症I"),
        ("認知症ケア加算II", "加算_認知症II"),
        ("口腔連携加算", "加算_口腔連携"),
        ("緊急時加算", "加算_緊急時"),
    ];

    // 単一クエリで全カラムのCOUNTとtotalを取得
    let kasan_sums: Vec<String> = kasan_cols.iter().map(|(_, col)| {
        format!("SUM(CASE WHEN \"{}\" = 1 THEN 1 ELSE 0 END)", col)
    }).collect();

    let sql = format!(
        "SELECT COUNT(*), {} FROM facilities {}",
        kasan_sums.join(", "),
        where_clause
    );

    let conn = get_conn(db).await?;
    let row = query_single_row_params(&conn, &sql, w.into_params()).await?;
    let total = row_i64(&row, 0) as f64;

    let mut results = Vec::new();
    for (i, (name, _)) in kasan_cols.iter().enumerate() {
        let count = row_i64(&row, (i + 1) as i32);
        results.push(json!({
            "kasan_name": name,
            "rate": if total > 0.0 { count as f64 / total } else { 0.0 },
            "count": count,
        }));
    }

    Ok(Value::Array(results))
}

/// 稼働率分布 - 単一クエリCASE WHEN方式
pub async fn revenue_occupancy_distribution(db: &Database, params: &FilterParams) -> Result<Value, AppError> {
    let w = WhereBuilder::from_filter_params(params);
    let where_clause = w.to_where_clause();
    let and_prefix = if where_clause.is_empty() { "WHERE" } else { &format!("{} AND", where_clause) };

    let sql = format!(
        "SELECT
            SUM(CASE WHEN occupancy_rate >= 0 AND occupancy_rate < 0.50 THEN 1 ELSE 0 END),
            SUM(CASE WHEN occupancy_rate >= 0.50 AND occupancy_rate < 0.60 THEN 1 ELSE 0 END),
            SUM(CASE WHEN occupancy_rate >= 0.60 AND occupancy_rate < 0.70 THEN 1 ELSE 0 END),
            SUM(CASE WHEN occupancy_rate >= 0.70 AND occupancy_rate < 0.80 THEN 1 ELSE 0 END),
            SUM(CASE WHEN occupancy_rate >= 0.80 AND occupancy_rate < 0.90 THEN 1 ELSE 0 END),
            SUM(CASE WHEN occupancy_rate >= 0.90 AND occupancy_rate < 1.00 THEN 1 ELSE 0 END),
            SUM(CASE WHEN occupancy_rate >= 1.00 THEN 1 ELSE 0 END)
        FROM facilities {} occupancy_rate IS NOT NULL",
        and_prefix
    );

    let conn = get_conn(db).await?;
    let row = query_single_row_params(&conn, &sql, w.into_params()).await?;

    let labels = ["0-50%", "50-60%", "60-70%", "70-80%", "80-90%", "90-100%", "100%以上"];
    let results: Vec<Value> = labels.iter().enumerate().map(|(i, label)| {
        json!({
            "range": label,
            "count": row_i64(&row, i as i32),
        })
    }).collect();

    Ok(Value::Array(results))
}

// ================================================================
// 賃金分析系
// ================================================================

/// 賃金KPI
pub async fn salary_kpi(db: &Database, params: &FilterParams) -> Result<Value, AppError> {
    let w = WhereBuilder::from_filter_params(params);
    let where_clause = w.to_where_clause();

    let sql = format!(
        "SELECT
            AVG(salary_representative) as avg_salary,
            NULL as median_salary,
            MAX(salary_representative) as max_salary,
            MIN(CASE WHEN salary_representative > 0 THEN salary_representative END) as min_salary
        FROM facilities {} {} salary_representative IS NOT NULL AND salary_representative > 0",
        where_clause,
        if where_clause.is_empty() { "WHERE" } else { "AND" }
    );

    let conn = get_conn(db).await?;
    let row = query_single_row_params(&conn, &sql, w.into_params()).await?;

    Ok(json!({
        "avg_salary": row_f64_opt(&row, 0),
        "median_salary": row_f64_opt(&row, 1),
        "max_salary": row_f64_opt(&row, 2),
        "min_salary": row_f64_opt(&row, 3),
    }))
}

/// 職種別賃金（Tursoのカラム構造に基づく）
pub async fn salary_by_job_type(db: &Database, params: &FilterParams) -> Result<Value, AppError> {
    // 賃金カラムはTursoではskipされているため、salary_representativeのみ利用可能
    // 職種別の分解は難しいため、法人種別で代替
    let w = WhereBuilder::from_filter_params(params);
    let where_clause = w.to_where_clause();
    let extra_cond = "salary_representative IS NOT NULL AND salary_representative > 0 AND corp_type IS NOT NULL AND corp_type != ''";

    let sql = build_grouped_query(
        &["corp_type"],
        &[
            "AVG(salary_representative) as avg_salary",
            "COUNT(*) as cnt",
        ],
        &where_clause,
        extra_cond,
        "corp_type",
        "avg_salary DESC",
    );

    let conn = get_conn(db).await?;
    let rows = query_rows_params(&conn, &sql, w.into_params()).await?;

    let results: Vec<Value> = rows.iter().map(|row| {
        json!({
            "job_type": row_str(row, 0),
            "avg_salary": row_f64(row, 1),
            "count": row_i64(row, 2),
        })
    }).collect();

    Ok(Value::Array(results))
}

/// 都道府県別賃金
pub async fn salary_by_prefecture(db: &Database, params: &FilterParams) -> Result<Value, AppError> {
    let w = WhereBuilder::from_filter_params(params);
    let where_clause = w.to_where_clause();
    let extra_cond = "salary_representative IS NOT NULL AND salary_representative > 0 AND prefecture IS NOT NULL AND prefecture != ''";

    let sql = build_grouped_query(
        &["prefecture"],
        &[
            "AVG(salary_representative) as avg_salary",
            "COUNT(*) as cnt",
        ],
        &where_clause,
        extra_cond,
        "prefecture",
        "avg_salary DESC",
    );

    let conn = get_conn(db).await?;
    let rows = query_rows_params(&conn, &sql, w.into_params()).await?;

    let results: Vec<Value> = rows.iter().map(|row| {
        json!({
            "prefecture": row_str(row, 0),
            "avg_salary": row_f64(row, 1),
            "count": row_i64(row, 2),
        })
    }).collect();

    Ok(Value::Array(results))
}

// ================================================================
// 経営品質系
// ================================================================

/// 経営品質KPI
pub async fn quality_kpi(db: &Database, params: &FilterParams) -> Result<Value, AppError> {
    let w = WhereBuilder::from_filter_params(params);
    let where_clause = w.to_where_clause();

    let sql = format!(
        "SELECT
            COUNT(*) as facility_count,
            AVG(CAST(COALESCE(NULLIF(quality_score, ''), NULL) AS REAL)) as avg_quality_score,
            (SUM(CASE WHEN \"品質_BCP策定\" = 1 THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0)) as bcp_rate,
            (SUM(CASE WHEN \"品質_ICT活用\" = 1 THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0)) as ict_rate,
            (SUM(CASE WHEN \"品質_第三者評価\" = 1 THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0)) as third_party_rate,
            (SUM(CASE WHEN \"品質_賠償保険\" = 1 THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0)) as insurance_rate
        FROM facilities {}",
        where_clause
    );

    let conn = get_conn(db).await?;
    let row = query_single_row_params(&conn, &sql, w.into_params()).await?;

    Ok(json!({
        "avg_profit_ratio": null,
        "profitable_ratio": null,
        "avg_experienced_ratio": null,
        "facility_count": row_i64(&row, 0),
        "avg_quality_score": row_f64_opt(&row, 1),
        "bcp_rate": row_f64_opt(&row, 2),
        "ict_rate": row_f64_opt(&row, 3),
        "third_party_rate": row_f64_opt(&row, 4),
        "insurance_rate": row_f64_opt(&row, 5),
    }))
}

/// 品質スコア分布 - 単一クエリCASE WHEN方式
pub async fn quality_score_distribution(db: &Database, params: &FilterParams) -> Result<Value, AppError> {
    let w = WhereBuilder::from_filter_params(params);
    let where_clause = w.to_where_clause();
    let and_prefix = if where_clause.is_empty() { "WHERE" } else { &format!("{} AND", where_clause) };

    let sql = format!(
        "SELECT
            SUM(CASE WHEN CAST(quality_score AS REAL) >= 0 AND CAST(quality_score AS REAL) < 20 THEN 1 ELSE 0 END),
            SUM(CASE WHEN CAST(quality_score AS REAL) >= 20 AND CAST(quality_score AS REAL) < 40 THEN 1 ELSE 0 END),
            SUM(CASE WHEN CAST(quality_score AS REAL) >= 40 AND CAST(quality_score AS REAL) < 60 THEN 1 ELSE 0 END),
            SUM(CASE WHEN CAST(quality_score AS REAL) >= 60 AND CAST(quality_score AS REAL) < 80 THEN 1 ELSE 0 END),
            SUM(CASE WHEN CAST(quality_score AS REAL) >= 80 AND CAST(quality_score AS REAL) < 100.01 THEN 1 ELSE 0 END)
        FROM facilities {} quality_score IS NOT NULL",
        and_prefix
    );

    let conn = get_conn(db).await?;
    let row = query_single_row_params(&conn, &sql, w.into_params()).await?;

    let labels = ["0-20", "20-40", "40-60", "60-80", "80-100"];
    let results: Vec<Value> = labels.iter().enumerate().map(|(i, label)| {
        json!({
            "range": label,
            "count": row_i64(&row, i as i32),
        })
    }).collect();

    Ok(Value::Array(results))
}

/// 都道府県別品質
pub async fn quality_by_prefecture(db: &Database, params: &FilterParams) -> Result<Value, AppError> {
    let w = WhereBuilder::from_filter_params(params);
    let where_clause = w.to_where_clause();
    let extra_cond = "prefecture IS NOT NULL AND prefecture != ''";

    let sql = build_grouped_query(
        &["prefecture"],
        &[
            "AVG(CAST(COALESCE(NULLIF(quality_score, ''), '0') AS REAL)) as avg_profit",
            "COUNT(*) as cnt",
        ],
        &where_clause,
        extra_cond,
        "prefecture",
        "avg_profit DESC",
    );

    let conn = get_conn(db).await?;
    let rows = query_rows_params(&conn, &sql, w.into_params()).await?;

    let results: Vec<Value> = rows.iter().map(|row| {
        json!({
            "prefecture": row_str(row, 0),
            "avg_profit_ratio": row_f64(row, 1),
            "count": row_i64(row, 2),
        })
    }).collect();

    Ok(Value::Array(results))
}

/// 品質ランク分布
pub async fn quality_rank_distribution(db: &Database, params: &FilterParams) -> Result<Value, AppError> {
    let w = WhereBuilder::from_filter_params(params);
    let where_clause = w.to_where_clause();
    let extra_cond = "quality_rank IS NOT NULL AND quality_rank != ''";

    let sql = build_grouped_query(
        &["quality_rank"],
        &["COUNT(*) as cnt"],
        &where_clause,
        extra_cond,
        "quality_rank",
        "quality_rank ASC",
    );

    let conn = get_conn(db).await?;
    let rows = query_rows_params(&conn, &sql, w.into_params()).await?;

    let rank_colors = |rank: &str| -> &str {
        match rank {
            "S" => "#22c55e",
            "A" => "#3b82f6",
            "B" => "#f59e0b",
            "C" => "#f97316",
            "D" => "#ef4444",
            _ => "#6b7280",
        }
    };

    let results: Vec<Value> = rows.iter().map(|row| {
        let rank = row_str(row, 0);
        json!({
            "rank": &rank,
            "count": row_i64(row, 1),
            "color": rank_colors(&rank),
        })
    }).collect();

    Ok(Value::Array(results))
}

/// 品質カテゴリレーダー
pub async fn quality_category_radar(db: &Database, params: &FilterParams) -> Result<Value, AppError> {
    let w = WhereBuilder::from_filter_params(params);
    let where_clause = w.to_where_clause();

    let sql = format!(
        "SELECT
            (SUM(CASE WHEN \"品質_BCP策定\" = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0)) as bcp,
            (SUM(CASE WHEN \"品質_ICT活用\" = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0)) as ict,
            (SUM(CASE WHEN \"品質_第三者評価\" = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0)) as third_party,
            (SUM(CASE WHEN \"品質_賠償保険\" = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0)) as insurance,
            AVG(CASE WHEN fulltime_ratio BETWEEN 0.0 AND 1.0 THEN fulltime_ratio * 100 END) as fulltime,
            (1.0 - AVG(CASE WHEN turnover_rate BETWEEN 0.0 AND 1.0 THEN turnover_rate END)) * 100 as retention
        FROM facilities {}",
        where_clause
    );

    let conn = get_conn(db).await?;
    let row = query_single_row_params(&conn, &sql, w.into_params()).await?;

    Ok(json!([
        {"category": "BCP策定", "score": row_f64(&row, 0), "fullMark": 100.0},
        {"category": "ICT活用", "score": row_f64(&row, 1), "fullMark": 100.0},
        {"category": "第三者評価", "score": row_f64(&row, 2), "fullMark": 100.0},
        {"category": "賠償保険", "score": row_f64(&row, 3), "fullMark": 100.0},
        {"category": "常勤比率", "score": row_f64(&row, 4), "fullMark": 100.0},
        {"category": "定着率", "score": row_f64(&row, 5), "fullMark": 100.0},
    ]))
}

// ================================================================
// 法人グループ分析系
// ================================================================

/// 法人グループKPI
pub async fn corp_group_kpi(db: &Database, params: &FilterParams) -> Result<Value, AppError> {
    let w = WhereBuilder::from_filter_params(params);
    let where_clause = w.to_where_clause();
    let corp_filter = if where_clause.is_empty() {
        "WHERE \"法人番号\" IS NOT NULL AND \"法人番号\" != ''".to_string()
    } else {
        format!("{} AND \"法人番号\" IS NOT NULL AND \"法人番号\" != ''", where_clause)
    };

    let sql = format!(
        "WITH corp_stats AS (
            SELECT \"法人番号\", \"法人名\", COUNT(*) as fac_count
            FROM facilities {}
            GROUP BY \"法人番号\"
        )
        SELECT
            COUNT(*) as total_corps,
            SUM(CASE WHEN fac_count > 1 THEN 1 ELSE 0 END) as multi_fac_corps,
            AVG(fac_count) as avg_fac_per_corp,
            (SELECT \"法人名\" FROM corp_stats ORDER BY fac_count DESC LIMIT 1) as max_corp_name,
            MAX(fac_count) as max_fac_count
        FROM corp_stats",
        corp_filter
    );

    let conn = get_conn(db).await?;
    let row = query_single_row_params(&conn, &sql, w.into_params()).await?;

    Ok(json!({
        "total_corps": row_i64(&row, 0),
        "multi_facility_corps": row_i64(&row, 1),
        "avg_facilities_per_corp": row_f64(&row, 2),
        "max_facilities_corp_name": row_str_opt(&row, 3),
        "max_facilities_count": row_i64(&row, 4),
    }))
}

/// 法人規模別分布
pub async fn corp_group_size_distribution(db: &Database, params: &FilterParams) -> Result<Value, AppError> {
    let w = WhereBuilder::from_filter_params(params);
    let where_clause = w.to_where_clause();
    let corp_filter = if where_clause.is_empty() {
        "WHERE \"法人番号\" IS NOT NULL AND \"法人番号\" != ''".to_string()
    } else {
        format!("{} AND \"法人番号\" IS NOT NULL AND \"法人番号\" != ''", where_clause)
    };

    let sql = format!(
        "WITH corp_stats AS (
            SELECT \"法人番号\", COUNT(*) as fac_count
            FROM facilities {}
            GROUP BY \"法人番号\"
        )
        SELECT
            CASE
                WHEN fac_count = 1 THEN '1施設'
                WHEN fac_count BETWEEN 2 AND 5 THEN '2-5施設'
                WHEN fac_count BETWEEN 6 AND 10 THEN '6-10施設'
                WHEN fac_count BETWEEN 11 AND 20 THEN '11-20施設'
                ELSE '21施設以上'
            END as category,
            COUNT(*) as count
        FROM corp_stats
        GROUP BY category
        ORDER BY MIN(fac_count)",
        corp_filter
    );

    let conn = get_conn(db).await?;
    let rows = query_rows_params(&conn, &sql, w.into_params()).await?;

    let results: Vec<Value> = rows.iter().map(|row| {
        json!({
            "category": row_str(row, 0),
            "count": row_i64(row, 1),
        })
    }).collect();

    Ok(Value::Array(results))
}

/// 施設数上位法人
pub async fn corp_group_top_corps(db: &Database, params: &FilterParams, limit: usize) -> Result<Value, AppError> {
    let w = WhereBuilder::from_filter_params(params);
    let where_clause = w.to_where_clause();
    let corp_filter = if where_clause.is_empty() {
        "WHERE \"法人番号\" IS NOT NULL AND \"法人番号\" != ''".to_string()
    } else {
        format!("{} AND \"法人番号\" IS NOT NULL AND \"法人番号\" != ''", where_clause)
    };

    let mut query_params = w.into_params();
    let limit_idx = query_params.len() + 1;
    query_params.push(libsql::Value::Integer(limit as i64));

    let sql = format!(
        "SELECT
            COALESCE(\"法人名\", '') as corp_name,
            \"法人番号\" as corp_number,
            corp_type,
            COUNT(*) as facility_count,
            SUM(CAST(COALESCE(NULLIF(\"従業者_合計\", ''), '0') AS REAL)) as total_staff,
            AVG(CASE WHEN turnover_rate BETWEEN 0.0 AND 1.0 THEN turnover_rate END) as avg_turnover,
            GROUP_CONCAT(DISTINCT prefecture) as prefectures,
            GROUP_CONCAT(DISTINCT \"サービス名\") as service_names
        FROM facilities {}
        GROUP BY \"法人番号\", \"法人名\"
        ORDER BY facility_count DESC
        LIMIT ?{}",
        corp_filter, limit_idx
    );

    let conn = get_conn(db).await?;
    let rows = query_rows_params(&conn, &sql, query_params).await?;

    let results: Vec<Value> = rows.iter().map(|row| {
        let prefs_str = row_str(row, 6);
        let svcs_str = row_str(row, 7);
        json!({
            "corp_name": row_str(row, 0),
            "corp_number": row_str(row, 1),
            "corp_type": row_str_opt(row, 2),
            "facility_count": row_i64(row, 3),
            "total_staff": row_f64(row, 4),
            "avg_turnover_rate": row_f64_opt(row, 5),
            "prefectures": prefs_str.split(',').filter(|s| !s.is_empty()).collect::<Vec<&str>>(),
            "service_names": svcs_str.split(',').filter(|s| !s.is_empty()).collect::<Vec<&str>>(),
        })
    }).collect();

    Ok(Value::Array(results))
}

/// 法人別加算ヒートマップ - N+1クエリ解消: IN句で一括取得
pub async fn corp_group_kasan_heatmap(db: &Database, params: &FilterParams, top_n: usize) -> Result<Value, AppError> {
    let w = WhereBuilder::from_filter_params(params);
    let where_clause = w.to_where_clause();
    let corp_filter = if where_clause.is_empty() {
        "WHERE \"法人番号\" IS NOT NULL AND \"法人番号\" != ''".to_string()
    } else {
        format!("{} AND \"法人番号\" IS NOT NULL AND \"法人番号\" != ''", where_clause)
    };

    // 上位法人を取得
    let mut top_params = w.clone_params();
    let top_limit_idx = top_params.len() + 1;
    top_params.push(libsql::Value::Integer(top_n as i64));

    let top_sql = format!(
        "SELECT \"法人番号\", \"法人名\" FROM facilities {} GROUP BY \"法人番号\" ORDER BY COUNT(*) DESC LIMIT ?{}",
        corp_filter, top_limit_idx
    );

    let conn = get_conn(db).await?;
    let top_rows = query_rows_params(&conn, &top_sql, top_params).await?;

    if top_rows.is_empty() {
        return Ok(json!({ "corps": [] }));
    }

    let kasan_cols = vec![
        "加算_処遇改善I", "加算_処遇改善II", "加算_処遇改善III", "加算_処遇改善IV",
        "加算_特定I", "加算_特定II", "加算_特定III", "加算_特定IV", "加算_特定V",
        "加算_認知症I", "加算_認知症II", "加算_口腔連携", "加算_緊急時",
    ];
    let kasan_names = vec![
        "処遇改善加算I", "処遇改善加算II", "処遇改善加算III", "処遇改善加算IV",
        "特定事業所加算I", "特定事業所加算II", "特定事業所加算III", "特定事業所加算IV", "特定事業所加算V",
        "認知症ケア加算I", "認知症ケア加算II", "口腔連携加算", "緊急時加算",
    ];

    // 法人番号リストを収集
    let corp_numbers: Vec<String> = top_rows.iter().map(|r| row_str(r, 0)).collect();
    let corp_names: Vec<String> = top_rows.iter().map(|r| row_str(r, 1)).collect();

    // 単一クエリで全法人の施設データを一括取得（パラメタライズド）
    let mut fac_params: Vec<libsql::Value> = Vec::new();
    let placeholders: Vec<String> = corp_numbers.iter().enumerate().map(|(i, cn)| {
        fac_params.push(libsql::Value::Text(cn.clone()));
        format!("?{}", i + 1)
    }).collect();

    let kasan_select: Vec<String> = kasan_cols.iter().map(|c| format!("COALESCE(\"{}\", 0)", c)).collect();
    let fac_sql = format!(
        "SELECT \"法人番号\", \"事業所名\", {} FROM facilities WHERE \"法人番号\" IN ({})",
        kasan_select.join(", "),
        placeholders.join(",")
    );

    let fac_rows = query_rows_params(&conn, &fac_sql, fac_params).await.unwrap_or_default();

    // 法人番号でグルーピング
    let mut corp_facilities: std::collections::HashMap<String, Vec<&libsql::Row>> = std::collections::HashMap::new();
    for fac_row in &fac_rows {
        let corp_num = row_str(fac_row, 0);
        corp_facilities.entry(corp_num).or_default().push(fac_row);
    }

    let mut corps = Vec::new();
    for (corp_number, corp_name) in corp_numbers.iter().zip(corp_names.iter()) {
        let mut facilities = Vec::new();
        if let Some(fac_rows_for_corp) = corp_facilities.get(corp_number) {
            for fac_row in fac_rows_for_corp {
                let fac_name = row_str(fac_row, 1);
                let mut kasan_map = serde_json::Map::new();
                for (i, name) in kasan_names.iter().enumerate() {
                    let val = row_i64(fac_row, (i + 2) as i32);
                    kasan_map.insert(name.to_string(), json!(val == 1));
                }
                facilities.push(json!({
                    "facility_name": fac_name,
                    "kasan": kasan_map,
                }));
            }
        }

        corps.push(json!({
            "corp_name": corp_name,
            "facilities": facilities,
        }));
    }

    Ok(json!({ "corps": corps }))
}

// ================================================================
// 成長性分析系
// ================================================================

/// 成長性KPI
pub async fn growth_kpi(db: &Database, params: &FilterParams) -> Result<Value, AppError> {
    let w = WhereBuilder::from_filter_params(params);
    let where_clause = w.to_where_clause();

    let sql = format!(
        "SELECT
            COUNT(*) as total_with_date,
            AVG(years_in_business) as avg_years,
            SUM(CASE WHEN years_in_business <= 3 THEN 1 ELSE 0 END) as recent_3yr
        FROM facilities {} {} years_in_business IS NOT NULL",
        where_clause,
        if where_clause.is_empty() { "WHERE" } else { "AND" }
    );

    let conn = get_conn(db).await?;
    let row = query_single_row_params(&conn, &sql, w.into_params()).await?;

    let total_with_date = row_i64(&row, 0);
    let recent_3yr = row_i64(&row, 2);

    Ok(json!({
        "recent_3yr_count": recent_3yr,
        "avg_years_in_business": row_f64(&row, 1),
        "net_growth_rate": if total_with_date > 0 { recent_3yr as f64 / total_with_date as f64 } else { 0.0 },
        "total_with_start_date": total_with_date,
    }))
}

/// 設立年トレンド
pub async fn growth_establishment_trend(db: &Database, params: &FilterParams) -> Result<Value, AppError> {
    let w = WhereBuilder::from_filter_params(params);
    let where_clause = w.to_where_clause();

    let current_year = chrono::Utc::now().year();

    let mut query_params = w.into_params();
    let year_idx = query_params.len() + 1;
    query_params.push(libsql::Value::Integer(current_year as i64));

    let sql = format!(
        "SELECT
            (?{} - CAST(years_in_business AS INTEGER)) as est_year,
            COUNT(*) as cnt
        FROM facilities {} {} years_in_business IS NOT NULL AND years_in_business > 0
        GROUP BY est_year
        ORDER BY est_year ASC",
        year_idx,
        where_clause,
        if where_clause.is_empty() { "WHERE" } else { "AND" }
    );

    let conn = get_conn(db).await?;
    let rows = query_rows_params(&conn, &sql, query_params).await?;

    let results: Vec<Value> = rows.iter().map(|row| {
        json!({
            "year": row_i64(row, 0),
            "count": row_i64(row, 1),
        })
    }).collect();

    Ok(Value::Array(results))
}

/// 事業年数分布 - 単一クエリCASE WHEN方式
pub async fn growth_years_distribution(db: &Database, params: &FilterParams) -> Result<Value, AppError> {
    let w = WhereBuilder::from_filter_params(params);
    let where_clause = w.to_where_clause();
    let and_prefix = if where_clause.is_empty() { "WHERE" } else { &format!("{} AND", where_clause) };

    let sql = format!(
        "SELECT
            SUM(CASE WHEN years_in_business >= 0 AND years_in_business < 5 THEN 1 ELSE 0 END),
            SUM(CASE WHEN years_in_business >= 5 AND years_in_business < 10 THEN 1 ELSE 0 END),
            SUM(CASE WHEN years_in_business >= 10 AND years_in_business < 15 THEN 1 ELSE 0 END),
            SUM(CASE WHEN years_in_business >= 15 AND years_in_business < 20 THEN 1 ELSE 0 END),
            SUM(CASE WHEN years_in_business >= 20 AND years_in_business < 25 THEN 1 ELSE 0 END),
            SUM(CASE WHEN years_in_business >= 25 AND years_in_business < 30 THEN 1 ELSE 0 END),
            SUM(CASE WHEN years_in_business >= 30 THEN 1 ELSE 0 END)
        FROM facilities {} years_in_business IS NOT NULL",
        and_prefix
    );

    let conn = get_conn(db).await?;
    let row = query_single_row_params(&conn, &sql, w.into_params()).await?;

    let labels = ["0-5年", "5-10年", "10-15年", "15-20年", "20-25年", "25-30年", "30年以上"];
    let results: Vec<Value> = labels.iter().enumerate().map(|(i, label)| {
        json!({
            "range": label,
            "count": row_i64(&row, i as i32),
        })
    }).collect();

    Ok(Value::Array(results))
}

// ================================================================
// メタ情報
// ================================================================

/// メタ情報
pub async fn meta(db: &Database) -> Result<Value, AppError> {
    let conn = get_conn(db).await?;

    let count_row = query_single_row_params(&conn, "SELECT COUNT(*) FROM facilities", vec![]).await?;
    let total = row_i64(&count_row, 0);

    // 都道府県一覧
    let pref_rows = query_rows_params(&conn,
        "SELECT DISTINCT prefecture FROM facilities WHERE prefecture IS NOT NULL AND prefecture != '' ORDER BY prefecture",
        vec![],
    ).await?;
    let prefectures: Vec<String> = pref_rows.iter().map(|r| row_str(r, 0)).collect();

    // サービスコード一覧
    let svc_rows = query_rows_params(&conn,
        "SELECT DISTINCT \"サービスコード\" FROM facilities WHERE \"サービスコード\" IS NOT NULL AND \"サービスコード\" != '' ORDER BY \"サービスコード\"",
        vec![],
    ).await?;
    let service_codes: Vec<String> = svc_rows.iter().map(|r| row_str(r, 0)).collect();

    // 法人種別一覧
    let ct_rows = query_rows_params(&conn,
        "SELECT DISTINCT corp_type FROM facilities WHERE corp_type IS NOT NULL AND corp_type != '' ORDER BY corp_type",
        vec![],
    ).await?;
    let corp_types: Vec<String> = ct_rows.iter().map(|r| row_str(r, 0)).collect();

    // 従業者数範囲
    let range_row = query_single_row_params(&conn,
        "SELECT MIN(CAST(COALESCE(NULLIF(\"従業者_合計\", ''), '0') AS REAL)), MAX(CAST(COALESCE(NULLIF(\"従業者_合計\", ''), '0') AS REAL)) FROM facilities",
        vec![],
    ).await?;

    Ok(json!({
        "total_count": total,
        "prefectures": prefectures,
        "service_codes": service_codes,
        "corp_types": corp_types,
        "staff_range": [row_f64(&range_row, 0), row_f64(&range_row, 1)],
    }))
}

// ================================================================
// 施設検索（SQL版）
// ================================================================

/// 施設検索
pub async fn search_facilities(db: &Database, params: &SearchParams) -> Result<Value, AppError> {
    let filter_params = params.to_filter_params();
    let mut w = WhereBuilder::from_filter_params(&filter_params);

    // テキスト検索（q パラメータ）- パラメタライズド
    if let Some(ref q) = params.q {
        let q = q.trim();
        if !q.is_empty() {
            let like_val = format!("%{}%", q);
            let idx1 = w.next_param();
            w.params.push(libsql::Value::Text(like_val.clone()));
            let idx2 = w.next_param();
            w.params.push(libsql::Value::Text(like_val.clone()));
            let idx3 = w.next_param();
            w.params.push(libsql::Value::Text(like_val));
            w.conditions.push(format!(
                "(\"事業所名\" LIKE ?{} OR \"法人名\" LIKE ?{} OR \"電話番号\" LIKE ?{})",
                idx1, idx2, idx3
            ));
        }
    }

    let where_clause = w.to_where_clause();
    let page = params.page.unwrap_or(1).max(1);
    let per_page = params.per_page.unwrap_or(50).min(500).max(1);
    let offset = (page - 1) * per_page;

    // ソートカラムのマッピング（ホワイトリスト方式 - インジェクション不可）
    let sort_col = match params.sort_by.as_deref() {
        Some("jigyosho_number") => "\"事業所番号\"",
        Some("jigyosho_name") => "\"事業所名\"",
        Some("staff_total") => "CAST(COALESCE(NULLIF(\"従業者_合計\", ''), '0') AS REAL)",
        Some("capacity") => "CAST(COALESCE(NULLIF(\"定員\", ''), '0') AS REAL)",
        Some("turnover_rate") => "turnover_rate",
        Some("fulltime_ratio") => "fulltime_ratio",
        Some("years_in_business") => "years_in_business",
        Some("prefecture") => "prefecture",
        Some("corp_type") => "corp_type",
        Some("corp_name") => "\"法人名\"",
        _ => "\"事業所番号\"",
    };
    let sort_order = match params.sort_order.as_deref() {
        Some(o) if o.to_lowercase() == "desc" => "DESC",
        _ => "ASC",
    };

    // 総件数取得
    let count_sql = format!("SELECT COUNT(*) FROM facilities {}", where_clause);
    let conn = get_conn(db).await?;
    let count_row = query_single_row_params(&conn, &count_sql, w.clone_params()).await?;
    let total = row_i64(&count_row, 0) as usize;
    let total_pages = if total == 0 { 0 } else { (total + per_page - 1) / per_page };

    // データ取得（LIMIT/OFFSETもパラメタライズ）
    let mut data_params = w.into_params();
    let limit_idx = data_params.len() + 1;
    data_params.push(libsql::Value::Integer(per_page as i64));
    let offset_idx = data_params.len() + 1;
    data_params.push(libsql::Value::Integer(offset as i64));

    let data_sql = format!(
        "SELECT
            \"事業所番号\", \"事業所名\", \"管理者名\", \"管理者職名\",
            \"代表者名\", \"代表者職名\", \"法人名\", \"法人番号\",
            \"電話番号\", \"FAX番号\", \"住所\", \"HP\",
            \"従業者_常勤\", \"従業者_非常勤\", \"従業者_合計\", \"定員\",
            \"事業開始日\", \"前年度採用数\", \"前年度退職数\",
            prefecture, corp_type, turnover_rate, fulltime_ratio, years_in_business,
            \"サービスコード\", \"サービス名\"
        FROM facilities {}
        ORDER BY {} {}
        LIMIT ?{} OFFSET ?{}",
        where_clause, sort_col, sort_order, limit_idx, offset_idx
    );

    let rows = query_rows_params(&conn, &data_sql, data_params).await?;

    let items: Vec<Value> = rows.iter().map(|row| {
        json!({
            "jigyosho_number": row_str(row, 0),
            "jigyosho_name": row_str(row, 1),
            "manager_name": row_str_opt(row, 2),
            "manager_title": row_str_opt(row, 3),
            "representative_name": row_str_opt(row, 4),
            "representative_title": row_str_opt(row, 5),
            "corp_name": row_str_opt(row, 6),
            "corp_number": row_str_opt(row, 7),
            "phone": row_str_opt(row, 8),
            "fax": row_str_opt(row, 9),
            "address": row_str_opt(row, 10),
            "homepage": row_str_opt(row, 11),
            "staff_fulltime": row_f64_opt(row, 12),
            "staff_parttime": row_f64_opt(row, 13),
            "staff_total": row_f64_opt(row, 14),
            "capacity": row_f64_opt(row, 15),
            "start_date": row_str_opt(row, 16),
            "hired_last_year": row_f64_opt(row, 17),
            "left_last_year": row_f64_opt(row, 18),
            "prefecture": row_str_opt(row, 19),
            "corp_type": row_str_opt(row, 20),
            "turnover_rate": row_f64_opt(row, 21),
            "fulltime_ratio": row_f64_opt(row, 22),
            "years_in_business": row_f64_opt(row, 23),
            "service_code": row_str_opt(row, 24),
            "service_name": row_str_opt(row, 25),
        })
    }).collect();

    Ok(json!({
        "items": items,
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": total_pages,
    }))
}

/// 施設詳細 - パラメタライズドクエリ
pub async fn facility_detail(db: &Database, id: &str) -> Result<Value, AppError> {
    let conn = get_conn(db).await?;

    let sql = "SELECT
            \"事業所番号\", \"事業所名\", \"管理者名\", \"管理者職名\",
            \"代表者名\", \"代表者職名\", \"法人名\", \"法人番号\",
            \"電話番号\", \"FAX番号\", \"住所\", \"HP\",
            \"従業者_常勤\", \"従業者_非常勤\", \"従業者_合計\", \"定員\",
            \"事業開始日\", \"前年度採用数\", \"前年度退職数\",
            prefecture, corp_type, turnover_rate, fulltime_ratio, years_in_business,
            \"サービスコード\", \"サービス名\"
        FROM facilities
        WHERE \"事業所番号\" = ?1
        LIMIT 1";

    let rows = query_rows_params(&conn, sql, vec![libsql::Value::Text(id.to_string())]).await?;
    if rows.is_empty() {
        return Err(AppError::NotFound(format!("事業所番号 {} が見つかりません", id)));
    }

    let row = &rows[0];
    Ok(json!({
        "facility": {
            "jigyosho_number": row_str(row, 0),
            "jigyosho_name": row_str(row, 1),
            "manager_name": row_str_opt(row, 2),
            "manager_title": row_str_opt(row, 3),
            "representative_name": row_str_opt(row, 4),
            "representative_title": row_str_opt(row, 5),
            "corp_name": row_str_opt(row, 6),
            "corp_number": row_str_opt(row, 7),
            "phone": row_str_opt(row, 8),
            "fax": row_str_opt(row, 9),
            "address": row_str_opt(row, 10),
            "homepage": row_str_opt(row, 11),
            "staff_fulltime": row_f64_opt(row, 12),
            "staff_parttime": row_f64_opt(row, 13),
            "staff_total": row_f64_opt(row, 14),
            "capacity": row_f64_opt(row, 15),
            "start_date": row_str_opt(row, 16),
            "hired_last_year": row_f64_opt(row, 17),
            "left_last_year": row_f64_opt(row, 18),
            "prefecture": row_str_opt(row, 19),
            "corp_type": row_str_opt(row, 20),
            "turnover_rate": row_f64_opt(row, 21),
            "fulltime_ratio": row_f64_opt(row, 22),
            "years_in_business": row_f64_opt(row, 23),
            "service_code": row_str_opt(row, 24),
            "service_name": row_str_opt(row, 25),
        }
    }))
}

// ================================================================
// M&A系（SQL版）
// ================================================================

/// M&Aスクリーニング
pub async fn ma_screening(
    db: &Database,
    params: &FilterParams,
    prefectures: &Option<String>,
    corp_types: &Option<String>,
    staff_min: Option<f64>,
    staff_max: Option<f64>,
    turnover_min: Option<f64>,
    turnover_max: Option<f64>,
    limit: usize,
) -> Result<Value, AppError> {
    let mut w = WhereBuilder::from_filter_params(params);
    w.conditions.push("\"法人番号\" IS NOT NULL AND \"法人番号\" != ''".to_string());

    let where_clause = w.to_where_clause();

    // HAVING句の条件（パラメタライズド）
    let mut having_conditions = Vec::new();
    let mut extra_params: Vec<libsql::Value> = Vec::new();
    let mut extra_counter = w.param_counter;

    if let Some(ref prefs) = prefectures {
        for p in prefs.split(',') {
            let p = p.trim();
            if !p.is_empty() {
                extra_counter += 1;
                extra_params.push(libsql::Value::Text(p.to_string()));
                having_conditions.push(format!("prefectures LIKE '%' || ?{} || '%'", extra_counter));
            }
        }
    }
    if let Some(min) = staff_min {
        extra_counter += 1;
        extra_params.push(libsql::Value::Real(min));
        having_conditions.push(format!("total_staff >= ?{}", extra_counter));
    }
    if let Some(max) = staff_max {
        extra_counter += 1;
        extra_params.push(libsql::Value::Real(max));
        having_conditions.push(format!("total_staff <= ?{}", extra_counter));
    }
    if let Some(min) = turnover_min {
        extra_counter += 1;
        extra_params.push(libsql::Value::Real(min));
        having_conditions.push(format!("avg_turnover >= ?{}", extra_counter));
    }
    if let Some(max) = turnover_max {
        extra_counter += 1;
        extra_params.push(libsql::Value::Real(max));
        having_conditions.push(format!("avg_turnover <= ?{}", extra_counter));
    }

    extra_counter += 1;
    extra_params.push(libsql::Value::Integer(limit as i64));
    let limit_idx = extra_counter;

    let having = if having_conditions.is_empty() {
        String::new()
    } else {
        format!("HAVING {}", having_conditions.join(" AND "))
    };

    let sql = format!(
        "SELECT
            COALESCE(\"法人名\", '') as corp_name,
            \"法人番号\" as corp_number,
            corp_type,
            COUNT(*) as facility_count,
            SUM(CAST(COALESCE(NULLIF(\"従業者_合計\", ''), '0') AS REAL)) as total_staff,
            AVG(CASE WHEN turnover_rate BETWEEN 0.0 AND 1.0 THEN turnover_rate END) as avg_turnover,
            AVG(CAST(COALESCE(NULLIF(\"定員\", ''), '0') AS REAL)) as avg_capacity,
            GROUP_CONCAT(DISTINCT prefecture) as prefectures,
            GROUP_CONCAT(DISTINCT \"サービス名\") as service_names
        FROM facilities {}
        GROUP BY \"法人番号\", \"法人名\"
        {}
        ORDER BY facility_count DESC, total_staff DESC
        LIMIT ?{}",
        where_clause, having, limit_idx
    );

    let mut all_params = w.into_params();
    all_params.extend(extra_params);

    let conn = get_conn(db).await?;
    let rows = query_rows_params(&conn, &sql, all_params).await?;

    let items: Vec<Value> = rows.iter().enumerate().map(|(_i, row)| {
        let fac_count = row_i64(row, 3) as f64;
        let total_staff = row_f64(row, 4);
        let avg_turnover = row_f64_opt(row, 5);
        // 魅力度スコア（簡易版: 施設数 * 20 + 従業者数 / 10、上限100）
        let score = ((fac_count * 20.0 + total_staff / 10.0).min(100.0)).max(0.0);
        let prefs_str = row_str(row, 7);
        let svcs_str = row_str(row, 8);

        json!({
            "corp_name": row_str(row, 0),
            "corp_number": row_str(row, 1),
            "corp_type": row_str_opt(row, 2),
            "facility_count": row_i64(row, 3),
            "total_staff": total_staff,
            "avg_turnover_rate": avg_turnover,
            "avg_capacity": row_f64(row, 6),
            "prefectures": prefs_str.split(',').filter(|s| !s.is_empty()).collect::<Vec<&str>>(),
            "service_names": svcs_str.split(',').filter(|s| !s.is_empty()).collect::<Vec<&str>>(),
            "attractiveness_score": score,
        })
    }).collect();

    let total = items.len();

    Ok(json!({
        "items": items,
        "total": total,
        "funnel": [
            {"stage": "全法人", "count": total},
            {"stage": "条件適合", "count": total},
        ],
    }))
}

/// DD法人検索 - パラメタライズドクエリ
pub async fn dd_search(db: &Database, params: &FilterParams, query: &str) -> Result<Value, AppError> {
    let mut w = WhereBuilder::from_filter_params(params);
    w.conditions.push("\"法人番号\" IS NOT NULL AND \"法人番号\" != ''".to_string());

    if !query.is_empty() {
        let like_val = format!("%{}%", query);
        let idx1 = w.next_param();
        w.params.push(libsql::Value::Text(like_val));
        let idx2 = w.next_param();
        w.params.push(libsql::Value::Text(query.to_string()));
        w.conditions.push(format!(
            "(\"法人名\" LIKE ?{} OR \"法人番号\" = ?{})",
            idx1, idx2
        ));
    }

    let where_clause = format!("WHERE {}", w.conditions.join(" AND "));

    let sql = format!(
        "SELECT
            COALESCE(\"法人名\", '') as corp_name,
            \"法人番号\" as corp_number,
            COUNT(*) as facility_count,
            SUM(CAST(COALESCE(NULLIF(\"従業者_合計\", ''), '0') AS REAL)) as total_staff
        FROM facilities {}
        GROUP BY \"法人番号\", \"法人名\"
        ORDER BY facility_count DESC
        LIMIT 50",
        where_clause
    );

    let conn = get_conn(db).await?;
    let rows = query_rows_params(&conn, &sql, w.into_params()).await?;

    let results: Vec<Value> = rows.iter().map(|row| {
        json!({
            "corp_name": row_str(row, 0),
            "corp_number": row_str(row, 1),
            "facility_count": row_i64(row, 2),
            "total_staff": row_f64(row, 3),
        })
    }).collect();

    Ok(Value::Array(results))
}

/// DDレポート - パラメタライズドクエリ
pub async fn dd_report(db: &Database, params: &FilterParams, corp_number: &str) -> Result<Value, AppError> {
    let conn = get_conn(db).await?;

    // 法人の施設データを取得（パラメタライズド）
    let sql = "SELECT
            \"事業所名\", \"法人名\", \"代表者名\", prefecture, corp_type,
            CAST(COALESCE(NULLIF(\"従業者_合計\", ''), '0') AS REAL) as staff,
            CAST(COALESCE(NULLIF(\"定員\", ''), '0') AS REAL) as capacity,
            turnover_rate, fulltime_ratio,
            CAST(COALESCE(NULLIF(\"前年度採用数\", ''), '0') AS REAL) as hired,
            CAST(COALESCE(NULLIF(\"前年度退職数\", ''), '0') AS REAL) as left_count,
            \"サービス名\", occupancy_rate,
            COALESCE(\"品質_BCP策定\", 0) as bcp,
            COALESCE(\"品質_賠償保険\", 0) as insurance
        FROM facilities
        WHERE \"法人番号\" = ?1";

    let rows = query_rows_params(&conn, sql, vec![libsql::Value::Text(corp_number.to_string())]).await?;
    if rows.is_empty() {
        return Err(AppError::NotFound(format!("法人番号 {} が見つかりません", corp_number)));
    }

    let corp_name = row_str(&rows[0], 1);
    let representative = row_str_opt(&rows[0], 2);
    let facility_count = rows.len();

    let mut facilities = Vec::new();
    let mut service_types = std::collections::HashSet::new();
    let mut prefectures = std::collections::HashSet::new();
    let mut total_staff = 0.0f64;
    let mut total_capacity = 0.0f64;
    let mut total_hired = 0.0f64;
    let mut total_left = 0.0f64;
    let mut turnover_sum = 0.0f64;
    let mut turnover_count = 0usize;
    let mut fulltime_sum = 0.0f64;
    let mut fulltime_count = 0usize;
    let mut occupancy_sum = 0.0f64;
    let mut occupancy_count = 0usize;
    let mut bcp_count = 0usize;
    let mut insurance_count = 0usize;

    for row in &rows {
        facilities.push(row_str(row, 0));
        if let Some(svc) = row_str_opt(row, 11) { service_types.insert(svc); }
        if let Some(pref) = row_str_opt(row, 3) { prefectures.insert(pref); }
        total_staff += row_f64(row, 5);
        total_capacity += row_f64(row, 6);
        if let Some(t) = row_f64_opt(row, 7) {
            if t >= 0.0 && t <= 1.0 { turnover_sum += t; turnover_count += 1; }
        }
        if let Some(f) = row_f64_opt(row, 8) {
            if f >= 0.0 && f <= 1.0 { fulltime_sum += f; fulltime_count += 1; }
        }
        total_hired += row_f64(row, 9);
        total_left += row_f64(row, 10);
        if let Some(o) = row_f64_opt(row, 12) {
            if o >= 0.0 && o <= 3.0 { occupancy_sum += o; occupancy_count += 1; }
        }
        if row_i64(row, 13) == 1 { bcp_count += 1; }
        if row_i64(row, 14) == 1 { insurance_count += 1; }
    }

    let avg_turnover = if turnover_count > 0 { Some(turnover_sum / turnover_count as f64) } else { None };
    let avg_fulltime = if fulltime_count > 0 { Some(fulltime_sum / fulltime_count as f64) } else { None };
    let avg_occupancy = if occupancy_count > 0 { Some(occupancy_sum / occupancy_count as f64) } else { None };
    let avg_capacity = if facility_count > 0 { total_capacity / facility_count as f64 } else { 0.0 };
    let bcp_rate = if facility_count > 0 { Some(bcp_count as f64 / facility_count as f64) } else { None };
    let insurance_rate = if facility_count > 0 { Some(insurance_count as f64 / facility_count as f64) } else { None };

    // 地域ベンチマーク（パラメタライズド）
    let pref_list: Vec<&String> = prefectures.iter().collect();
    let benchmark = if !pref_list.is_empty() {
        let mut bench_params: Vec<libsql::Value> = Vec::new();
        let placeholders: Vec<String> = pref_list.iter().enumerate().map(|(i, p)| {
            bench_params.push(libsql::Value::Text((*p).clone()));
            format!("?{}", i + 1)
        }).collect();
        let bench_sql = format!(
            "SELECT
                AVG(CASE WHEN turnover_rate BETWEEN 0.0 AND 1.0 THEN turnover_rate END),
                AVG(CAST(COALESCE(NULLIF(\"従業者_合計\", ''), '0') AS REAL)),
                AVG(CAST(COALESCE(NULLIF(\"定員\", ''), '0') AS REAL))
            FROM facilities WHERE prefecture IN ({})",
            placeholders.join(",")
        );
        match query_single_row_params(&conn, &bench_sql, bench_params).await {
            Ok(row) => json!({
                "region_avg_turnover": row_f64(&row, 0),
                "region_avg_staff": row_f64(&row, 1),
                "region_avg_capacity": row_f64(&row, 2),
            }),
            Err(_) => json!({"region_avg_turnover": 0.0, "region_avg_staff": 0.0, "region_avg_capacity": 0.0}),
        }
    } else {
        json!({"region_avg_turnover": 0.0, "region_avg_staff": 0.0, "region_avg_capacity": 0.0})
    };

    // リスクフラグ生成
    let mut risk_flags = Vec::new();
    if let Some(t) = avg_turnover {
        if t > 0.25 {
            risk_flags.push(json!({"level": "red", "category": "人材", "detail": format!("離職率が高水準: {:.1}%", t * 100.0)}));
        } else if t > 0.15 {
            risk_flags.push(json!({"level": "yellow", "category": "人材", "detail": format!("離職率がやや高い: {:.1}%", t * 100.0)}));
        }
    }

    Ok(json!({
        "corp_info": {
            "corp_name": corp_name,
            "corp_number": corp_number,
            "representative": representative,
            "facility_count": facility_count,
            "prefectures": prefectures.into_iter().collect::<Vec<String>>(),
        },
        "business_dd": {
            "facilities": facilities,
            "service_types": service_types.into_iter().collect::<Vec<String>>(),
            "avg_capacity": avg_capacity,
            "avg_occupancy": avg_occupancy,
            "total_staff": total_staff,
        },
        "hr_dd": {
            "avg_turnover_rate": avg_turnover,
            "avg_fulltime_ratio": avg_fulltime,
            "total_hired": total_hired,
            "total_left": total_left,
        },
        "compliance_dd": {
            "has_violations": false,
            "bcp_rate": bcp_rate,
            "insurance_rate": insurance_rate,
        },
        "financial_dd": {
            "accounting_type": null,
            "financial_links": [],
        },
        "risk_flags": risk_flags,
        "benchmark": benchmark,
        "kasan_summary": {
            "facilities": [],
            "totals": {},
            "facility_count": facility_count,
            "has_data": false,
        },
    }))
}

/// PMIシミュレーション - パラメタライズドクエリ
pub async fn pmi_simulation(db: &Database, params: &FilterParams, buyer_corp: &str, target_corp: &str) -> Result<Value, AppError> {
    let conn = get_conn(db).await?;

    async fn get_corp_data(conn: &libsql::Connection, corp_number: &str) -> Result<Value, AppError> {
        let sql = "SELECT
                COALESCE(\"法人名\", '') as corp_name,
                \"事業所名\",
                CAST(COALESCE(NULLIF(\"従業者_合計\", ''), '0') AS REAL) as staff,
                prefecture, \"サービス名\",
                turnover_rate, fulltime_ratio
            FROM facilities WHERE \"法人番号\" = ?1";
        let rows = query_rows_params(conn, sql, vec![libsql::Value::Text(corp_number.to_string())]).await?;
        if rows.is_empty() {
            return Err(AppError::NotFound(format!("法人番号 {} が見つかりません", corp_number)));
        }

        let corp_name = row_str(&rows[0], 0);
        let mut facilities = Vec::new();
        let mut total_staff = 0.0f64;
        let mut prefectures = std::collections::HashSet::new();
        let mut services = std::collections::HashSet::new();
        let mut turnover_sum = 0.0f64;
        let mut turnover_count = 0usize;

        for row in &rows {
            facilities.push(row_str(row, 1));
            total_staff += row_f64(row, 2);
            if let Some(p) = row_str_opt(row, 3) { prefectures.insert(p); }
            if let Some(s) = row_str_opt(row, 4) { services.insert(s); }
            if let Some(t) = row_f64_opt(row, 5) {
                if t >= 0.0 && t <= 1.0 { turnover_sum += t; turnover_count += 1; }
            }
        }

        let avg_turnover = if turnover_count > 0 { turnover_sum / turnover_count as f64 } else { 0.0 };

        Ok(json!({
            "corp_name": corp_name,
            "facilities": facilities,
            "total_staff": total_staff,
            "prefectures": prefectures.into_iter().collect::<Vec<String>>(),
            "services": services.into_iter().collect::<Vec<String>>(),
            "avg_turnover": avg_turnover,
            "facility_count": rows.len(),
        }))
    }

    let buyer = get_corp_data(&conn, buyer_corp).await?;
    let target = get_corp_data(&conn, target_corp).await?;

    let buyer_prefs: Vec<String> = buyer["prefectures"].as_array().unwrap_or(&vec![]).iter()
        .filter_map(|v| v.as_str().map(|s| s.to_string())).collect();
    let target_prefs: Vec<String> = target["prefectures"].as_array().unwrap_or(&vec![]).iter()
        .filter_map(|v| v.as_str().map(|s| s.to_string())).collect();
    let buyer_svcs: Vec<String> = buyer["services"].as_array().unwrap_or(&vec![]).iter()
        .filter_map(|v| v.as_str().map(|s| s.to_string())).collect();
    let target_svcs: Vec<String> = target["services"].as_array().unwrap_or(&vec![]).iter()
        .filter_map(|v| v.as_str().map(|s| s.to_string())).collect();

    let all_prefs: std::collections::HashSet<&str> = buyer_prefs.iter().chain(target_prefs.iter()).map(|s| s.as_str()).collect();
    let all_svcs: std::collections::HashSet<&str> = buyer_svcs.iter().chain(target_svcs.iter()).map(|s| s.as_str()).collect();
    let overlap_svcs: Vec<&str> = buyer_svcs.iter().filter(|s| target_svcs.contains(s)).map(|s| s.as_str()).collect();
    let new_svcs: Vec<&str> = target_svcs.iter().filter(|s| !buyer_svcs.contains(s)).map(|s| s.as_str()).collect();
    let new_prefs: Vec<&str> = target_prefs.iter().filter(|s| !buyer_prefs.contains(s)).map(|s| s.as_str()).collect();

    let buyer_staff = buyer["total_staff"].as_f64().unwrap_or(0.0);
    let target_staff = target["total_staff"].as_f64().unwrap_or(0.0);
    let buyer_turnover = buyer["avg_turnover"].as_f64().unwrap_or(0.0);
    let target_turnover = target["avg_turnover"].as_f64().unwrap_or(0.0);

    Ok(json!({
        "buyer": {
            "corp_name": buyer["corp_name"],
            "facilities": buyer["facilities"],
            "total_staff": buyer_staff,
        },
        "target": {
            "corp_name": target["corp_name"],
            "facilities": target["facilities"],
            "total_staff": target_staff,
        },
        "combined": {
            "total_facilities": buyer["facility_count"].as_i64().unwrap_or(0) + target["facility_count"].as_i64().unwrap_or(0),
            "total_staff": buyer_staff + target_staff,
            "service_coverage": all_svcs.into_iter().collect::<Vec<&str>>(),
            "prefecture_coverage": all_prefs.into_iter().collect::<Vec<&str>>(),
            "service_overlap": overlap_svcs,
            "new_services": new_svcs,
            "new_prefectures": new_prefs,
        },
        "synergy": {
            "wage_gap": (buyer_staff - target_staff).abs(),
            "turnover_gap": (buyer_turnover - target_turnover).abs(),
            "staff_reallocation_potential": (buyer_staff - target_staff).abs() * 0.1,
        },
    }))
}

/// ベンチマーク - パラメタライズドクエリ
pub async fn benchmark(db: &Database, jigyosho_number: &str) -> Result<Value, AppError> {
    let conn = get_conn(db).await?;

    // 対象施設の情報を取得（パラメタライズド）
    let sql = "SELECT
            \"事業所番号\", \"事業所名\", \"法人名\", prefecture,
            CAST(COALESCE(NULLIF(\"従業者_合計\", ''), '0') AS REAL) as staff,
            CAST(COALESCE(NULLIF(\"定員\", ''), '0') AS REAL) as capacity,
            turnover_rate, fulltime_ratio, years_in_business,
            occupancy_rate, quality_score, kasan_count
        FROM facilities WHERE \"事業所番号\" = ?1";

    let rows = query_rows_params(&conn, sql, vec![libsql::Value::Text(jigyosho_number.to_string())]).await?;
    if rows.is_empty() {
        return Err(AppError::NotFound(format!("事業所番号 {} が見つかりません", jigyosho_number)));
    }

    let row = &rows[0];
    let pref = row_str_opt(row, 3).unwrap_or_default();
    let staff = row_f64(row, 4);
    let capacity = row_f64(row, 5);
    let turnover = row_f64_opt(row, 6).unwrap_or(0.0);
    let fulltime = row_f64_opt(row, 7).unwrap_or(0.0);
    let years = row_f64_opt(row, 8).unwrap_or(0.0);
    let occupancy = row_f64_opt(row, 9).unwrap_or(0.0);
    let quality = row_f64_opt(row, 10).unwrap_or(0.0);
    let kasan = row_f64_opt(row, 11).unwrap_or(0.0);

    // 全国平均（パラメータ不要）
    let avg_sql = "SELECT
        AVG(CAST(COALESCE(NULLIF(\"従業者_合計\", ''), '0') AS REAL)),
        AVG(CAST(COALESCE(NULLIF(\"定員\", ''), '0') AS REAL)),
        AVG(CASE WHEN turnover_rate BETWEEN 0.0 AND 1.0 THEN turnover_rate END),
        AVG(CASE WHEN fulltime_ratio BETWEEN 0.0 AND 1.0 THEN fulltime_ratio END),
        AVG(years_in_business),
        AVG(CASE WHEN occupancy_rate BETWEEN 0.0 AND 3.0 THEN occupancy_rate END),
        AVG(CAST(COALESCE(NULLIF(quality_score, ''), '0') AS REAL)),
        AVG(CAST(COALESCE(NULLIF(kasan_count, ''), '0') AS REAL))
    FROM facilities";
    let nat_row = query_single_row_params(&conn, avg_sql, vec![]).await?;

    // 都道府県平均（パラメタライズド）
    let pref_avg_sql = "SELECT
            AVG(CAST(COALESCE(NULLIF(\"従業者_合計\", ''), '0') AS REAL)),
            AVG(CAST(COALESCE(NULLIF(\"定員\", ''), '0') AS REAL)),
            AVG(CASE WHEN turnover_rate BETWEEN 0.0 AND 1.0 THEN turnover_rate END),
            AVG(CASE WHEN fulltime_ratio BETWEEN 0.0 AND 1.0 THEN fulltime_ratio END),
            AVG(years_in_business),
            AVG(CASE WHEN occupancy_rate BETWEEN 0.0 AND 3.0 THEN occupancy_rate END),
            AVG(CAST(COALESCE(NULLIF(quality_score, ''), '0') AS REAL)),
            AVG(CAST(COALESCE(NULLIF(kasan_count, ''), '0') AS REAL))
        FROM facilities WHERE prefecture = ?1";
    // 安全に取得（都道府県行が取れない場合のフォールバック）
    let (pref_staff, pref_cap, pref_turn, pref_ft, pref_years, pref_occ, pref_qual, pref_kasan) =
        match query_single_row_params(&conn, pref_avg_sql, vec![libsql::Value::Text(pref.clone())]).await {
            Ok(pr) => (
                row_f64(&pr, 0), row_f64(&pr, 1), row_f64(&pr, 2), row_f64(&pr, 3),
                row_f64(&pr, 4), row_f64(&pr, 5), row_f64(&pr, 6), row_f64(&pr, 7),
            ),
            Err(_) => (
                row_f64(&nat_row, 0), row_f64(&nat_row, 1), row_f64(&nat_row, 2), row_f64(&nat_row, 3),
                row_f64(&nat_row, 4), row_f64(&nat_row, 5), row_f64(&nat_row, 6), row_f64(&nat_row, 7),
            ),
        };

    let radar = json!([
        {"axis": "従業者数", "value": staff, "national_avg": row_f64(&nat_row, 0), "pref_avg": pref_staff},
        {"axis": "定員", "value": capacity, "national_avg": row_f64(&nat_row, 1), "pref_avg": pref_cap},
        {"axis": "定着率", "value": 1.0 - turnover, "national_avg": 1.0 - row_f64(&nat_row, 2), "pref_avg": 1.0 - pref_turn},
        {"axis": "常勤比率", "value": fulltime, "national_avg": row_f64(&nat_row, 3), "pref_avg": pref_ft},
        {"axis": "事業年数", "value": years, "national_avg": row_f64(&nat_row, 4), "pref_avg": pref_years},
        {"axis": "稼働率", "value": occupancy, "national_avg": row_f64(&nat_row, 5), "pref_avg": pref_occ},
        {"axis": "品質スコア", "value": quality, "national_avg": row_f64(&nat_row, 6), "pref_avg": pref_qual},
        {"axis": "加算取得数", "value": kasan, "national_avg": row_f64(&nat_row, 7), "pref_avg": pref_kasan},
    ]);

    // 改善提案
    let mut suggestions = Vec::new();
    let nat_turnover = row_f64(&nat_row, 2);
    if turnover > nat_turnover * 1.2 {
        suggestions.push(json!({
            "axis": "定着率",
            "current": 1.0 - turnover,
            "target": 1.0 - nat_turnover,
            "suggestion": "離職率が全国平均を上回っています。人材定着施策の強化を検討してください。",
        }));
    }
    let nat_fulltime = row_f64(&nat_row, 3);
    if fulltime < nat_fulltime * 0.8 {
        suggestions.push(json!({
            "axis": "常勤比率",
            "current": fulltime,
            "target": nat_fulltime,
            "suggestion": "常勤比率が全国平均を下回っています。常勤スタッフの採用を検討してください。",
        }));
    }

    Ok(json!({
        "facility": {
            "jigyosho_number": row_str(&rows[0], 0),
            "jigyosho_name": row_str(&rows[0], 1),
            "corp_name": row_str_opt(&rows[0], 2),
            "prefecture": pref,
        },
        "radar": radar,
        "percentiles": {},
        "improvement_suggestions": suggestions,
    }))
}

/// CSVエクスポート（SQLベース）
pub async fn export_csv(db: &Database, params: &FilterParams) -> Result<Vec<u8>, AppError> {
    let w = WhereBuilder::from_filter_params(params);
    let where_clause = w.to_where_clause();

    let sql = format!(
        "SELECT
            \"事業所番号\", \"事業所名\", \"管理者名\", \"管理者職名\",
            \"代表者名\", \"代表者職名\", \"法人名\", \"法人番号\",
            \"電話番号\", \"FAX番号\", \"住所\", \"HP\",
            \"従業者_常勤\", \"従業者_非常勤\", \"従業者_合計\", \"定員\",
            \"事業開始日\", \"前年度採用数\", \"前年度退職数\",
            prefecture as \"都道府県\", corp_type as \"法人種別\",
            turnover_rate as \"離職率\", fulltime_ratio as \"常勤比率\",
            years_in_business as \"事業年数\"
        FROM facilities {}
        ORDER BY \"事業所番号\"",
        where_clause
    );

    let conn = get_conn(db).await?;
    let rows = query_rows_params(&conn, &sql, w.into_params()).await?;

    // BOM + CSVヘッダー
    let mut csv = vec![0xEFu8, 0xBB, 0xBF]; // BOM
    let header = "事業所番号,事業所名,管理者名,管理者職名,代表者名,代表者職名,法人名,法人番号,電話番号,FAX番号,住所,HP,従業者_常勤,従業者_非常勤,従業者_合計,定員,事業開始日,前年度採用数,前年度退職数,都道府県,法人種別,離職率,常勤比率,事業年数\n";
    csv.extend_from_slice(header.as_bytes());

    for row in &rows {
        let mut fields = Vec::new();
        for i in 0..24 {
            let val = row_str(row, i);
            // CSVエスケープ: カンマや改行を含む場合はダブルクォートで囲む
            if val.contains(',') || val.contains('\n') || val.contains('"') {
                fields.push(format!("\"{}\"", val.replace('"', "\"\"")));
            } else {
                fields.push(val);
            }
        }
        csv.extend_from_slice(fields.join(",").as_bytes());
        csv.push(b'\n');
    }

    Ok(csv)
}

// ================================================================
// ヘルパー: GROUP BYクエリビルダー
// ================================================================

/// GROUP BYクエリを構築するヘルパー
fn build_grouped_query(
    group_cols: &[&str],
    agg_cols: &[&str],
    where_clause: &str,
    extra_condition: &str,
    group_by: &str,
    order_by: &str,
) -> String {
    let select_cols: Vec<&str> = group_cols.iter().chain(agg_cols.iter()).copied().collect();
    let select = select_cols.join(", ");

    let full_where = if where_clause.is_empty() {
        format!("WHERE {}", extra_condition)
    } else {
        format!("{} AND {}", where_clause, extra_condition)
    };

    format!(
        "SELECT {} FROM facilities {} GROUP BY {} ORDER BY {}",
        select, full_where, group_by, order_by
    )
}
