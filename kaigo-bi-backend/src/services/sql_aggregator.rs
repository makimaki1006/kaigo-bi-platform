/// SQL集計ロジック
/// DataFrameの代わりにTurso SQLで直接集計を実行する
/// フィルタ付きリクエストのフォールバック処理用

use chrono::Datelike;
use libsql::Database;
use serde_json::{json, Value};

use crate::error::AppError;
use crate::models::filters::{FilterParams, SearchParams};

/// 定員フィルタ上限値（異常値を除外するための閾値）
const MAX_CAPACITY_FILTER: u32 = 500;

/// 賃金の月額として妥当と見なすレンジ（円）。
/// 実データには 0 円や 5,019 万円といった明らかな誤入力・年額混入が含まれるため、
/// 集計前にこのレンジで足切りする。
/// 値は scripts/aggregate_to_cache.py（kpi_cache を作る側）と揃えている。
/// 揃えないと、キャッシュを返す既定表示とフィルタ適用時とで数字が食い違う。
const SALARY_MIN: u32 = 10_000;
const SALARY_MAX: u32 = 2_000_000;

/// 賃金の生値を数値（円）にする SQL 式を組み立てる。
///
/// 実データは表記が揃っておらず、`250000` のほかに `250,000` `25万` `25万円` が混在する。
/// 素の CAST では `250,000` が 250、`25万円` が 25 になり、いずれもレンジ外として
/// 捨てられていた（実測で 12,835 件の生値のうち妥当と判定できたのは 5,870 件）。
fn salary_value(col: &str) -> String {
    // カンマ・円を除き、全角数字を半角へ寄せた文字列を作る。
    // 全角のまま CAST すると 0 になるため、ETL 側（safe_float）と同様に正規化する。
    let mut cleaned = format!("NULLIF(\"{}\", '')", col);
    for (from, to) in [
        (",", ""), ("，", ""), ("円", ""), (" ", ""), ("　", ""),
        ("０", "0"), ("１", "1"), ("２", "2"), ("３", "3"), ("４", "4"),
        ("５", "5"), ("６", "6"), ("７", "7"), ("８", "8"), ("９", "9"),
        ("．", "."),
    ] {
        cleaned = format!("REPLACE({}, '{}', '{}')", cleaned, from, to);
    }
    format!(
        "CASE WHEN {c} LIKE '%万%' \
              THEN CAST(REPLACE({c}, '万', '') AS REAL) * 10000 \
              ELSE CAST({c} AS REAL) END",
        c = cleaned
    )
}

/// 賃金の代表値を取り出す SQL 式（1職種目）。
/// `salary_representative` というカラムは存在せず、実データは 賃金_月額1〜5。
const SALARY_EXPR: &str = "CAST(NULLIF(\"賃金_月額1\", '') AS REAL)";

/// 賃金_月額1〜5 を縦持ちに展開するサブクエリを組み立てる。
/// ETL 側も5職種すべてを対象にしているため、1職種目だけでは件数が半分以下になる。
fn salary_unpivot(where_clause: &str) -> String {
    (1..=5)
        .map(|i| {
            format!(
                "SELECT {} AS v FROM facilities {}",
                salary_value(&format!("賃金_月額{}", i)),
                where_clause
            )
        })
        .collect::<Vec<_>>()
        .join(" UNION ALL ")
}

/// 職種名と月額のペアを縦持ちに展開するサブクエリ。
fn salary_job_unpivot(where_clause: &str) -> String {
    (1..=5)
        .map(|i| {
            format!(
                "SELECT \"賃金_職種{i}\" AS job, \
                        CAST(NULLIF(\"賃金_月額{i}\", '') AS REAL) AS v, \
                        CAST(NULLIF(\"賃金_平均年齢{i}\", '') AS REAL) AS age, \
                        CAST(NULLIF(\"賃金_平均勤続{i}\", '') AS REAL) AS tenure \
                 FROM facilities {wc}",
                i = i,
                wc = where_clause
            )
        })
        .collect::<Vec<_>>()
        .join(" UNION ALL ")
}

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
// ⚠️ libsql 0.6 の row.get::<T> は期待型と実際のSQLite型が食い違うと
// unreachable!() でパニックする（例: TEXT列に get::<f64>）。
// facilitiesテーブルはCSV再ロードで数値がTEXT格納されているため、
// 必ず get_value で生値を取ってから安全に変換する。

/// Row からf64を取得（型を問わず変換、失敗時は0.0）
fn row_f64(row: &libsql::Row, idx: i32) -> f64 {
    row_f64_opt(row, idx).unwrap_or(0.0)
}

/// Row からf64をOption<f64>で取得（TEXT数値・INTEGERも変換）
fn row_f64_opt(row: &libsql::Row, idx: i32) -> Option<f64> {
    match row.get_value(idx as i32).ok()? {
        libsql::Value::Real(f) => Some(f),
        libsql::Value::Integer(i) => Some(i as f64),
        libsql::Value::Text(s) => s.trim().replace(',', "").parse::<f64>().ok(),
        _ => None,
    }
}

/// Row からi64を取得（型を問わず変換、失敗時は0）
fn row_i64(row: &libsql::Row, idx: i32) -> i64 {
    match row.get_value(idx as i32).ok() {
        Some(libsql::Value::Integer(i)) => i,
        Some(libsql::Value::Real(f)) => f as i64,
        Some(libsql::Value::Text(s)) => s.trim().parse::<f64>().map(|f| f as i64).unwrap_or(0),
        _ => 0,
    }
}

/// Row からStringを取得（数値型も文字列化）
fn row_str(row: &libsql::Row, idx: i32) -> String {
    match row.get_value(idx as i32).ok() {
        Some(libsql::Value::Text(s)) => s,
        Some(libsql::Value::Integer(i)) => i.to_string(),
        Some(libsql::Value::Real(f)) => f.to_string(),
        _ => String::new(),
    }
}

/// Row からOption<String>を取得（空文字はNone扱い）
fn row_str_opt(row: &libsql::Row, idx: i32) -> Option<String> {
    let s = row_str(row, idx);
    if s.is_empty() { None } else { Some(s) }
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
            AVG(CASE WHEN CAST(COALESCE(NULLIF(\"定員\", ''), NULL) AS REAL) BETWEEN 1 AND {cap}
                THEN CAST(\"定員\" AS REAL) END) as avg_capacity,
            AVG(CASE WHEN turnover_rate BETWEEN 0.0 AND 1.0 THEN turnover_rate END) as avg_turnover,
            AVG(CASE WHEN fulltime_ratio BETWEEN 0.0 AND 1.0 THEN fulltime_ratio END) as avg_fulltime,
            AVG(CASE WHEN years_in_business > 0 AND years_in_business <= 100 THEN years_in_business END) as avg_years
        FROM facilities {}",
        where_clause, cap = MAX_CAPACITY_FILTER
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
        format!(
            "SELECT
                prefecture,
                COUNT(*) as facility_count,
                AVG(CAST(COALESCE(NULLIF(\"従業者_合計\", ''), NULL) AS REAL)) as avg_staff,
                AVG(CASE WHEN CAST(COALESCE(NULLIF(\"定員\", ''), NULL) AS REAL) BETWEEN 1 AND {cap}
                    THEN CAST(\"定員\" AS REAL) END) as avg_capacity,
                AVG(CASE WHEN turnover_rate BETWEEN 0.0 AND 1.0 THEN turnover_rate END) as avg_turnover
            FROM facilities
            WHERE prefecture IS NOT NULL AND prefecture != ''
            GROUP BY prefecture
            ORDER BY facility_count DESC",
            cap = MAX_CAPACITY_FILTER
        )
    } else {
        format!(
            "SELECT
                prefecture,
                COUNT(*) as facility_count,
                AVG(CAST(COALESCE(NULLIF(\"従業者_合計\", ''), NULL) AS REAL)) as avg_staff,
                AVG(CASE WHEN CAST(COALESCE(NULLIF(\"定員\", ''), NULL) AS REAL) BETWEEN 1 AND {cap}
                    THEN CAST(\"定員\" AS REAL) END) as avg_capacity,
                AVG(CASE WHEN turnover_rate BETWEEN 0.0 AND 1.0 THEN turnover_rate END) as avg_turnover
            FROM facilities
            {} AND prefecture IS NOT NULL AND prefecture != ''
            GROUP BY prefecture
            ORDER BY facility_count DESC",
            where_clause, cap = MAX_CAPACITY_FILTER
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
            // フロントの「総従業者数」列が total_staff を参照するため合計も返す(全行"-"だった不具合)
            "SUM(CAST(COALESCE(NULLIF(\"従業者_合計\", ''), '0') AS REAL)) as total_staff",
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
            "total_staff": row_i64(row, 4),
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

/// 職種別スタッフ内訳（介護職員、看護職員、生活相談員、機能訓練指導員、管理栄養士、事務員）
pub async fn workforce_staff_breakdown(db: &Database, params: &FilterParams) -> Result<Value, AppError> {
    let w = WhereBuilder::from_filter_params(params);
    let where_clause = w.to_where_clause();

    let sql = format!(
        r#"SELECT
            AVG(CAST(COALESCE(NULLIF("介護職員_常勤", ''), NULL) AS REAL)) as kaigo_ft,
            AVG(CAST(COALESCE(NULLIF("介護職員_非常勤", ''), NULL) AS REAL)) as kaigo_pt,
            AVG(CAST(COALESCE(NULLIF("介護職員_合計", ''), NULL) AS REAL)) as kaigo_avg,
            SUM(CAST(COALESCE(NULLIF("介護職員_合計", ''), '0') AS INTEGER)) as kaigo_sum,
            COUNT(CASE WHEN "介護職員_合計" IS NOT NULL AND "介護職員_合計" != '' THEN 1 END) as kaigo_count,

            AVG(CAST(COALESCE(NULLIF("看護職員_常勤", ''), NULL) AS REAL)) as kango_ft,
            AVG(CAST(COALESCE(NULLIF("看護職員_非常勤", ''), NULL) AS REAL)) as kango_pt,
            AVG(CAST(COALESCE(NULLIF("看護職員_合計", ''), NULL) AS REAL)) as kango_avg,
            SUM(CAST(COALESCE(NULLIF("看護職員_合計", ''), '0') AS INTEGER)) as kango_sum,
            COUNT(CASE WHEN "看護職員_合計" IS NOT NULL AND "看護職員_合計" != '' THEN 1 END) as kango_count,

            AVG(CAST(COALESCE(NULLIF("生活相談員_常勤", ''), NULL) AS REAL)) as soudan_ft,
            AVG(CAST(COALESCE(NULLIF("生活相談員_非常勤", ''), NULL) AS REAL)) as soudan_pt,
            AVG(CAST(COALESCE(NULLIF("生活相談員_合計", ''), NULL) AS REAL)) as soudan_avg,
            SUM(CAST(COALESCE(NULLIF("生活相談員_合計", ''), '0') AS INTEGER)) as soudan_sum,
            COUNT(CASE WHEN "生活相談員_合計" IS NOT NULL AND "生活相談員_合計" != '' THEN 1 END) as soudan_count,

            AVG(CAST(COALESCE(NULLIF("機能訓練指導員_常勤", ''), NULL) AS REAL)) as kinou_ft,
            AVG(CAST(COALESCE(NULLIF("機能訓練指導員_非常勤", ''), NULL) AS REAL)) as kinou_pt,
            AVG(CAST(COALESCE(NULLIF("機能訓練指導員_合計", ''), NULL) AS REAL)) as kinou_avg,
            SUM(CAST(COALESCE(NULLIF("機能訓練指導員_合計", ''), '0') AS INTEGER)) as kinou_sum,
            COUNT(CASE WHEN "機能訓練指導員_合計" IS NOT NULL AND "機能訓練指導員_合計" != '' THEN 1 END) as kinou_count,

            AVG(CAST(COALESCE(NULLIF("管理栄養士_常勤", ''), NULL) AS REAL)) as eiyou_ft,
            AVG(CAST(COALESCE(NULLIF("管理栄養士_非常勤", ''), NULL) AS REAL)) as eiyou_pt,
            AVG(CAST(COALESCE(NULLIF("管理栄養士_合計", ''), NULL) AS REAL)) as eiyou_avg,
            SUM(CAST(COALESCE(NULLIF("管理栄養士_合計", ''), '0') AS INTEGER)) as eiyou_sum,
            COUNT(CASE WHEN "管理栄養士_合計" IS NOT NULL AND "管理栄養士_合計" != '' THEN 1 END) as eiyou_count,

            AVG(CAST(COALESCE(NULLIF("事務員_常勤", ''), NULL) AS REAL)) as jimu_ft,
            AVG(CAST(COALESCE(NULLIF("事務員_非常勤", ''), NULL) AS REAL)) as jimu_pt,
            AVG(CAST(COALESCE(NULLIF("事務員_合計", ''), NULL) AS REAL)) as jimu_avg,
            SUM(CAST(COALESCE(NULLIF("事務員_合計", ''), '0') AS INTEGER)) as jimu_sum,
            COUNT(CASE WHEN "事務員_合計" IS NOT NULL AND "事務員_合計" != '' THEN 1 END) as jimu_count
        FROM facilities {}"#,
        where_clause
    );

    let conn = get_conn(db).await?;
    let row = query_single_row_params(&conn, &sql, w.into_params()).await?;

    // 各職種を構造体配列に変換
    let job_types = [
        ("介護職員", 0),
        ("看護職員", 5),
        ("生活相談員", 10),
        ("機能訓練指導員", 15),
        ("管理栄養士", 20),
        ("事務員", 25),
    ];

    let results: Vec<Value> = job_types.iter().map(|(name, offset)| {
        let ft = row_f64_opt(&row, *offset);
        let pt = row_f64_opt(&row, offset + 1);
        let avg = row_f64_opt(&row, offset + 2);
        let total = row_i64(&row, offset + 3);
        let count = row_i64(&row, offset + 4);
        // 常勤比率: 常勤平均 / (常勤平均+非常勤平均)
        let fulltime_ratio = match (ft, pt) {
            (Some(f), Some(p)) if f + p > 0.0 => Some(f / (f + p)),
            _ => None,
        };
        json!({
            "job_type": name,
            "avg_count": avg,
            "total_count": total,
            "facility_count": count,
            "fulltime_ratio": fulltime_ratio,
        })
    }).collect();

    Ok(Value::Array(results))
}

/// 資格保有状況（介護福祉士、実務者研修、初任者研修、介護支援専門員）
pub async fn workforce_qualifications(db: &Database, params: &FilterParams) -> Result<Value, AppError> {
    let w = WhereBuilder::from_filter_params(params);
    let where_clause = w.to_where_clause();

    let sql = format!(
        r#"SELECT
            SUM(CAST(COALESCE(NULLIF("介護福祉士数", ''), '0') AS INTEGER)) as fukushi_total,
            AVG(CAST(COALESCE(NULLIF("介護福祉士数", ''), NULL) AS REAL)) as fukushi_avg,
            COUNT(CASE WHEN "介護福祉士数" IS NOT NULL AND "介護福祉士数" != '' THEN 1 END) as fukushi_count,

            SUM(CAST(COALESCE(NULLIF("実務者研修数", ''), '0') AS INTEGER)) as jitsumu_total,
            AVG(CAST(COALESCE(NULLIF("実務者研修数", ''), NULL) AS REAL)) as jitsumu_avg,
            COUNT(CASE WHEN "実務者研修数" IS NOT NULL AND "実務者研修数" != '' THEN 1 END) as jitsumu_count,

            SUM(CAST(COALESCE(NULLIF("初任者研修数", ''), '0') AS INTEGER)) as shonin_total,
            AVG(CAST(COALESCE(NULLIF("初任者研修数", ''), NULL) AS REAL)) as shonin_avg,
            COUNT(CASE WHEN "初任者研修数" IS NOT NULL AND "初任者研修数" != '' THEN 1 END) as shonin_count,

            SUM(CAST(COALESCE(NULLIF("介護支援専門員数", ''), '0') AS INTEGER)) as care_mgr_total,
            AVG(CAST(COALESCE(NULLIF("介護支援専門員数", ''), NULL) AS REAL)) as care_mgr_avg,
            COUNT(CASE WHEN "介護支援専門員数" IS NOT NULL AND "介護支援専門員数" != '' THEN 1 END) as care_mgr_count
        FROM facilities {}"#,
        where_clause
    );

    let conn = get_conn(db).await?;
    let row = query_single_row_params(&conn, &sql, w.into_params()).await?;

    let qualifications = [
        ("介護福祉士", 0),
        ("実務者研修", 3),
        ("初任者研修", 6),
        ("介護支援専門員", 9),
    ];

    let results: Vec<Value> = qualifications.iter().map(|(name, offset)| {
        json!({
            "qualification": name,
            "total_count": row_i64(&row, *offset),
            "avg_per_facility": row_f64_opt(&row, offset + 1),
            "facility_count": row_i64(&row, offset + 2),
        })
    }).collect();

    Ok(Value::Array(results))
}

/// 夜勤・宿直体制
pub async fn workforce_night_shift(db: &Database, params: &FilterParams) -> Result<Value, AppError> {
    let w = WhereBuilder::from_filter_params(params);
    let where_clause = w.to_where_clause();

    let sql = format!(
        r#"SELECT
            AVG(CAST(COALESCE(NULLIF("夜勤人数", ''), NULL) AS REAL)) as night_avg,
            COUNT(CASE WHEN "夜勤人数" IS NOT NULL AND "夜勤人数" != '' AND CAST("夜勤人数" AS INTEGER) > 0 THEN 1 END) as night_count,
            SUM(CAST(COALESCE(NULLIF("夜勤人数", ''), '0') AS INTEGER)) as night_total,
            AVG(CAST(COALESCE(NULLIF("宿直人数", ''), NULL) AS REAL)) as duty_avg,
            COUNT(CASE WHEN "宿直人数" IS NOT NULL AND "宿直人数" != '' AND CAST("宿直人数" AS INTEGER) > 0 THEN 1 END) as duty_count,
            SUM(CAST(COALESCE(NULLIF("宿直人数", ''), '0') AS INTEGER)) as duty_total,
            COUNT(*) as total_facilities
        FROM facilities {}"#,
        where_clause
    );

    let conn = get_conn(db).await?;
    let row = query_single_row_params(&conn, &sql, w.into_params()).await?;

    let total = row_i64(&row, 6);

    Ok(json!({
        "night_shift": {
            "avg_staff": row_f64_opt(&row, 0),
            "facility_count": row_i64(&row, 1),
            "total_staff": row_i64(&row, 2),
            "coverage_rate": if total > 0 { Some(row_i64(&row, 1) as f64 / total as f64) } else { None::<f64> },
        },
        "on_call_duty": {
            "avg_staff": row_f64_opt(&row, 3),
            "facility_count": row_i64(&row, 4),
            "total_staff": row_i64(&row, 5),
            "coverage_rate": if total > 0 { Some(row_i64(&row, 4) as f64 / total as f64) } else { None::<f64> },
        },
        "total_facilities": total,
    }))
}

/// 認知症関連研修受講状況（指導者研修、リーダー研修、実践者研修）
pub async fn workforce_dementia_training(db: &Database, params: &FilterParams) -> Result<Value, AppError> {
    let w = WhereBuilder::from_filter_params(params);
    let where_clause = w.to_where_clause();

    let sql = format!(
        r#"SELECT
            SUM(CAST(COALESCE(NULLIF("認知症指導者研修数", ''), '0') AS INTEGER)) as leader_total,
            AVG(CAST(COALESCE(NULLIF("認知症指導者研修数", ''), NULL) AS REAL)) as leader_avg,
            COUNT(CASE WHEN "認知症指導者研修数" IS NOT NULL AND "認知症指導者研修数" != '' AND CAST("認知症指導者研修数" AS INTEGER) > 0 THEN 1 END) as leader_count,

            SUM(CAST(COALESCE(NULLIF("認知症リーダー研修数", ''), '0') AS INTEGER)) as subleader_total,
            AVG(CAST(COALESCE(NULLIF("認知症リーダー研修数", ''), NULL) AS REAL)) as subleader_avg,
            COUNT(CASE WHEN "認知症リーダー研修数" IS NOT NULL AND "認知症リーダー研修数" != '' AND CAST("認知症リーダー研修数" AS INTEGER) > 0 THEN 1 END) as subleader_count,

            SUM(CAST(COALESCE(NULLIF("認知症実践者研修数", ''), '0') AS INTEGER)) as practice_total,
            AVG(CAST(COALESCE(NULLIF("認知症実践者研修数", ''), NULL) AS REAL)) as practice_avg,
            COUNT(CASE WHEN "認知症実践者研修数" IS NOT NULL AND "認知症実践者研修数" != '' AND CAST("認知症実践者研修数" AS INTEGER) > 0 THEN 1 END) as practice_count,

            COUNT(*) as total_facilities
        FROM facilities {}"#,
        where_clause
    );

    let conn = get_conn(db).await?;
    let row = query_single_row_params(&conn, &sql, w.into_params()).await?;

    let total = row_i64(&row, 9);

    let training_types = [
        ("認知症指導者研修", 0),
        ("認知症リーダー研修", 3),
        ("認知症実践者研修", 6),
    ];

    let results: Vec<Value> = training_types.iter().map(|(name, offset)| {
        let facility_count = row_i64(&row, offset + 2);
        json!({
            "training_type": name,
            "total_trained": row_i64(&row, *offset),
            "avg_per_facility": row_f64_opt(&row, offset + 1),
            "facility_count": facility_count,
            "coverage_rate": if total > 0 { Some(facility_count as f64 / total as f64) } else { None::<f64> },
        })
    }).collect();

    Ok(Value::Array(results))
}

/// 加算全項目（JSON列のため、フィルタ付きではキャッシュフォールバック）
/// SQLiteでのJSON解析は全行ロードが必要になるため、フィルタ時は空配列を返す
pub async fn kasan_all_items(db: &Database, _params: &FilterParams) -> Result<Value, AppError> {
    // 加算_全項目はJSON列で、SQLiteでの集計は全行スキャンが必要
    // フィルタなしリクエストはキャッシュから提供される
    let _ = db;
    Ok(serde_json::json!([]))
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
            -- \"処遇改善加算フラグ\" は存在しないカラムで、参照するとクエリ全体が
            -- no such column で失敗していた。加算列は TEXT の '0'/'1' なので文字列比較する。
            -- I〜IV のいずれかを取得していれば「処遇改善加算あり」とみなす。
            (SUM(CASE WHEN \"加算_処遇改善I\" = '1' OR \"加算_処遇改善II\" = '1'
                        OR \"加算_処遇改善III\" = '1' OR \"加算_処遇改善IV\" = '1'
                      THEN 1 ELSE 0 END) * 1.0 /
                NULLIF(COUNT(*), 0)) as syogu_rate,
            AVG(CASE WHEN occupancy_rate BETWEEN 0.0 AND 3.0 THEN occupancy_rate END) as avg_occ,
            AVG(CASE WHEN CAST(COALESCE(NULLIF(\"定員\", ''), NULL) AS REAL) BETWEEN 1 AND {cap}
                THEN CAST(\"定員\" AS REAL) END) as avg_cap,
            AVG(CAST(COALESCE(NULLIF(quality_score, ''), NULL) AS REAL)) as avg_quality,
            AVG(CAST(COALESCE(NULLIF(\"利用者総数\", ''), NULL) AS REAL)) as avg_users
        FROM facilities {}",
        where_clause, cap = MAX_CAPACITY_FILTER
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

    // 賃金は全 223,103 施設中 6,084 件(2.7%)しか登録がなく、さらに妥当レンジ外を
    // 落とすと約 2,600 件になる。母数を伏せると誤読されるので sample_count も返す。
    let sql = format!(
        "SELECT
            AVG(v) as avg_salary,
            NULL as median_salary,
            MAX(v) as max_salary,
            MIN(v) as min_salary,
            COUNT(v) as sample_count,
            (SELECT COUNT(*) FROM facilities {wc}) as population_count
        FROM ({unpivot})
        WHERE v BETWEEN {lo} AND {hi}",
        unpivot = salary_unpivot(&where_clause),
        wc = where_clause,
        lo = SALARY_MIN,
        hi = SALARY_MAX
    );

    let conn = get_conn(db).await?;
    // プレースホルダは ?1 形式の番号付きなので、where_clause が2箇所に現れても
    // バインドは1セットでよい
    let row = query_single_row_params(&conn, &sql, w.into_params()).await?;

    Ok(json!({
        "avg_salary": row_f64_opt(&row, 0),
        "median_salary": row_f64_opt(&row, 1),
        "max_salary": row_f64_opt(&row, 2),
        "min_salary": row_f64_opt(&row, 3),
        "sample_count": row_i64(&row, 4),
        "population_count": row_i64(&row, 5),
    }))
}

/// 職種別賃金（Tursoのカラム構造に基づく）
pub async fn salary_by_job_type(db: &Database, params: &FilterParams) -> Result<Value, AppError> {
    // 旧実装は存在しない salary_representative を参照し、しかも職種ではなく
    // corp_type で代替していた（「職種別」というラベルと中身が食い違っていた）。
    // 実データには 賃金_職種1 があるのでそれで本当の職種別にする。
    // ただし職種名は自由記述で、上位でも数十件しかない点に注意（count を併せて返す）。
    let w = WhereBuilder::from_filter_params(params);
    let where_clause = w.to_where_clause();
    let sql = format!(
        "SELECT job, AVG(v) as avg_salary, COUNT(*) as cnt, \
                AVG(age) as avg_age, AVG(tenure) as avg_tenure \
         FROM ({unpivot}) \
         WHERE v BETWEEN {lo} AND {hi} AND COALESCE(job, '') != '' \
         GROUP BY job ORDER BY cnt DESC",
        unpivot = salary_job_unpivot(&where_clause),
        lo = SALARY_MIN,
        hi = SALARY_MAX
    );

    let conn = get_conn(db).await?;
    let rows = query_rows_params(&conn, &sql, w.into_params()).await?;

    let results: Vec<Value> = rows.iter().map(|row| {
        json!({
            "job_type": row_str(row, 0),
            "avg_salary": row_f64(row, 1),
            "count": row_i64(row, 2),
            "avg_age": row_f64_opt(row, 3),
            "avg_tenure": row_f64_opt(row, 4),
        })
    }).collect();

    Ok(Value::Array(results))
}

/// 都道府県別賃金
pub async fn salary_by_prefecture(db: &Database, params: &FilterParams) -> Result<Value, AppError> {
    let w = WhereBuilder::from_filter_params(params);
    let where_clause = w.to_where_clause();
    // 賃金_月額1〜5 を縦持ちにして都道府県ごとに集計する
    let unpivot = (1..=5)
        .map(|i| {
            format!(
                "SELECT prefecture, CAST(NULLIF(\"賃金_月額{}\", '') AS REAL) AS v \
                 FROM facilities {}",
                i, where_clause
            )
        })
        .collect::<Vec<_>>()
        .join(" UNION ALL ");

    let sql = format!(
        "SELECT prefecture, AVG(v) as avg_salary, COUNT(*) as cnt \
         FROM ({unpivot}) \
         WHERE v BETWEEN {lo} AND {hi} AND prefecture IS NOT NULL AND prefecture != '' \
         GROUP BY prefecture ORDER BY avg_salary DESC",
        unpivot = unpivot,
        lo = SALARY_MIN,
        hi = SALARY_MAX
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
            (SUM(CASE WHEN \"品質_BCP策定\" = '1' THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0)) as bcp_rate,
            (SUM(CASE WHEN \"品質_ICT活用\" = '1' THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0)) as ict_rate,
            (SUM(CASE WHEN \"品質_第三者評価\" = '1' THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0)) as third_party_rate,
            (SUM(CASE WHEN \"品質_損害賠償保険\" = '1' THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0)) as insurance_rate
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
            (SUM(CASE WHEN \"品質_BCP策定\" = '1' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0)) as bcp,
            (SUM(CASE WHEN \"品質_ICT活用\" = '1' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0)) as ict,
            (SUM(CASE WHEN \"品質_第三者評価\" = '1' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0)) as third_party,
            (SUM(CASE WHEN \"品質_損害賠償保険\" = '1' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0)) as insurance,
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
            MAX(COALESCE(\"法人名\", '')) as corp_name,
            \"法人番号\" as corp_number,
            MAX(corp_type) as corp_type,
            COUNT(*) as facility_count,
            SUM(CAST(COALESCE(NULLIF(\"従業者_合計\", ''), '0') AS REAL)) as total_staff,
            AVG(CASE WHEN turnover_rate BETWEEN 0.0 AND 1.0 THEN turnover_rate END) as avg_turnover,
            GROUP_CONCAT(DISTINCT prefecture) as prefectures,
            GROUP_CONCAT(DISTINCT \"サービス名\") as service_names
        FROM facilities {}
        GROUP BY \"法人番号\"
        ORDER BY facility_count DESC
        LIMIT ?{}",
        corp_filter, limit_idx
    );

    let conn = get_conn(db).await?;

    // フィルタなしのときは事前集計テーブルを引く（列順は下の取り出しに合わせる）
    let rows = if params.is_default() && corp_summary_available(db).await {
        query_rows_params(
            &conn,
            "SELECT corp_name, corp_number, corp_type, facility_count, total_staff, \
                    avg_turnover, prefectures, service_names \
             FROM corp_summary ORDER BY facility_count DESC LIMIT ?1",
            vec![libsql::Value::Integer(limit as i64)],
        )
        .await?
    } else {
        query_rows_params(&conn, &sql, query_params).await?
    };

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

    let conn = get_conn(db).await?;

    // 上位法人の選定は corp_summary（1法人1行・facility_count に索引あり）から行う。
    // facilities を毎回 GROUP BY すると 190,003 グループの集約になり、
    // フィルタなしでは分単位になる。フィルタ指定時や未構築時は従来どおり集約する。
    let top_rows = if params.is_default() && corp_summary_available(db).await {
        query_rows_params(
            &conn,
            "SELECT corp_number, corp_name FROM corp_summary \
             ORDER BY facility_count DESC LIMIT ?1",
            vec![libsql::Value::Integer(top_n as i64)],
        )
        .await?
    } else {
        let top_sql = format!(
            "SELECT \"法人番号\", \"法人名\" FROM facilities {} GROUP BY \"法人番号\" ORDER BY COUNT(*) DESC LIMIT ?{}",
            corp_filter, top_limit_idx
        );
        query_rows_params(&conn, &top_sql, top_params).await?
    };

    // 実カラム名は「加算_特定事業所I」「加算_認知症ケアI」。旧定義の「加算_特定I」
    // 「加算_認知症I」は存在せず、クエリが no such column で失敗して無言で空になっていた。
    let kasan_cols = vec![
        "加算_処遇改善I", "加算_処遇改善II", "加算_処遇改善III", "加算_処遇改善IV",
        "加算_特定事業所I", "加算_特定事業所II", "加算_特定事業所III", "加算_特定事業所IV", "加算_特定事業所V",
        "加算_認知症ケアI", "加算_認知症ケアII", "加算_口腔連携", "加算_緊急時",
    ];
    let kasan_names = vec![
        "処遇改善加算I", "処遇改善加算II", "処遇改善加算III", "処遇改善加算IV",
        "特定事業所加算I", "特定事業所加算II", "特定事業所加算III", "特定事業所加算IV", "特定事業所加算V",
        "認知症ケア加算I", "認知症ケア加算II", "口腔連携加算", "緊急時加算",
    ];

    // 該当法人が無くても列定義は返す（画面が kasan_items を列に使うため）
    if top_rows.is_empty() {
        return Ok(json!({ "corps": [], "kasan_items": kasan_names }));
    }

    // 法人番号リストを収集
    let corp_numbers: Vec<String> = top_rows.iter().map(|r| row_str(r, 0)).collect();
    let corp_names: Vec<String> = top_rows.iter().map(|r| row_str(r, 1)).collect();

    // 単一クエリで全法人の施設データを一括取得（パラメタライズド）
    let mut fac_params: Vec<libsql::Value> = Vec::new();
    let placeholders: Vec<String> = corp_numbers.iter().enumerate().map(|(i, cn)| {
        fac_params.push(libsql::Value::Text(cn.clone()));
        format!("?{}", i + 1)
    }).collect();

    // 値は TEXT の '0'/'1' で入っているため、整数比較する前に CAST する
    let kasan_select: Vec<String> = kasan_cols
        .iter()
        .map(|c| format!("CAST(COALESCE(NULLIF(\"{}\", ''), '0') AS INTEGER)", c))
        .collect();
    // 上位法人には 2,700 施設を持つものがあるため件数を抑える。
    // ヒートマップは「法人ごとの傾向」を見るものなので、法人あたり数施設で足りる。
    let fac_sql = format!(
        "SELECT \"法人番号\", \"事業所名\", {} FROM facilities WHERE \"法人番号\" IN ({}) \
         ORDER BY \"法人番号\", \"事業所名\" LIMIT 400",
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

    // 1法人あたりの表示行数。上位法人をまんべんなく見せるため均等に割り当てる
    const PER_CORP: usize = 4;

    let mut corps = Vec::new();
    // 画面の HeatmapChart は「行ラベルの配列」「列ラベルの配列」「値の2次元配列」を取る。
    // corps だけを返していたため rows/values が undefined になり「データなし」になっていた。
    let mut row_labels: Vec<String> = Vec::new();
    let mut matrix: Vec<Vec<Value>> = Vec::new();

    for (corp_number, corp_name) in corp_numbers.iter().zip(corp_names.iter()) {
        let mut facilities = Vec::new();
        if let Some(fac_rows_for_corp) = corp_facilities.get(corp_number) {
            for (idx, fac_row) in fac_rows_for_corp.iter().enumerate() {
                let fac_name = row_str(fac_row, 1);
                let mut kasan_map = serde_json::Map::new();
                let mut row_vals: Vec<Value> = Vec::new();
                for (i, name) in kasan_names.iter().enumerate() {
                    let val = row_i64(fac_row, (i + 2) as i32) == 1;
                    kasan_map.insert(name.to_string(), json!(val));
                    row_vals.push(json!(val));
                }
                facilities.push(json!({
                    "facility_name": fac_name,
                    "kasan": kasan_map,
                }));
                if idx < PER_CORP {
                    // どの法人の施設か分かるようにラベルへ法人名を添える
                    let short_corp: String = corp_name.chars().take(12).collect();
                    let short_fac: String = fac_name.chars().take(18).collect();
                    row_labels.push(format!("{} / {}", short_corp, short_fac));
                    matrix.push(row_vals);
                }
            }
        }

        corps.push(json!({
            "corp_name": corp_name,
            "facilities": facilities,
        }));
    }

    Ok(json!({
        "corps": corps,
        "kasan_items": kasan_names,
        "facilities": row_labels,
        "values": matrix,
    }))
}

// ================================================================
// 成長性分析系
// ================================================================

/// 成長性KPI
pub async fn growth_kpi(db: &Database, params: &FilterParams) -> Result<Value, AppError> {
    let w = WhereBuilder::from_filter_params(params);
    let where_clause = w.to_where_clause();

    // years_in_business の異常値(パース失敗の負値・巨大値)を除外(0超〜100年)
    //
    // 画面が oldest_years / new_facilities_5yr / total_facilities を読んでいたが
    // どれも返していなかった。「最長事業年数」は常に空、「5年以内の新設」は
    // recent_3yr_count に、「総施設数」は total_with_start_date にフォールバックし、
    // ラベルと中身が食い違っていた。3つとも実際に集計して返す。
    let sql = format!(
        "SELECT
            COUNT(*) as total_with_date,
            AVG(years_in_business) as avg_years,
            SUM(CASE WHEN years_in_business <= 3 THEN 1 ELSE 0 END) as recent_3yr,
            SUM(CASE WHEN years_in_business <= 5 THEN 1 ELSE 0 END) as recent_5yr,
            MAX(years_in_business) as oldest_years,
            (SELECT COUNT(*) FROM facilities {wc}) as total_facilities
        FROM facilities {wc} {j} years_in_business IS NOT NULL AND years_in_business > 0 AND years_in_business <= 100",
        wc = where_clause,
        j = if where_clause.is_empty() { "WHERE" } else { "AND" }
    );

    let conn = get_conn(db).await?;
    // ?N 形式の番号付きプレースホルダなので where_clause が2箇所でもバインドは1セット
    let row = query_single_row_params(&conn, &sql, w.into_params()).await?;

    let total_with_date = row_i64(&row, 0);
    let recent_3yr = row_i64(&row, 2);

    Ok(json!({
        "recent_3yr_count": recent_3yr,
        "avg_years_in_business": row_f64(&row, 1),
        "net_growth_rate": if total_with_date > 0 { recent_3yr as f64 / total_with_date as f64 } else { 0.0 },
        "total_with_start_date": total_with_date,
        "new_facilities_5yr": row_i64(&row, 3),
        "oldest_years": row_f64_opt(&row, 4),
        "total_facilities": row_i64(&row, 5),
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

    // レビュー対応(2026-07-28): years_in_business に事業開始日パース失敗由来の
    // 異常値(min -2979 / max 1804)が20件混入し、設立年5005・222等が出ていた。
    // 妥当な事業年数(0超〜100年)に絞り、異常年を除外する。
    let sql = format!(
        "SELECT
            (?{} - CAST(years_in_business AS INTEGER)) as est_year,
            COUNT(*) as cnt
        FROM facilities {} {} years_in_business IS NOT NULL AND years_in_business > 0 AND years_in_business <= 100
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
/// FTS5 の MATCH に渡す文字列を作る。
///
/// trigram トークナイザではフレーズ検索（ダブルクォート囲み）が部分一致に相当する。
/// クエリ構文の記号を素通しすると MATCH がエラーになるので、内部の `"` を潰して囲む。
fn fts_quote(q: &str) -> String {
    format!("\"{}\"", q.replace('"', " "))
}

/// 指定テーブルが存在するかを1度だけ調べる（呼び出し側で結果を保持する）
async fn table_exists(db: &Database, name: &str) -> bool {
    if let Ok(conn) = get_conn(db).await {
        if let Ok(mut rows) = conn
            .query(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?1",
                vec![libsql::Value::Text(name.to_string())],
            )
            .await
        {
            return matches!(rows.next().await, Ok(Some(_)));
        }
    }
    false
}

/// corp_summary（法人単位の事前集計）が使えるか。
async fn corp_summary_available(db: &Database) -> bool {
    use std::sync::atomic::{AtomicU8, Ordering};
    static STATE: AtomicU8 = AtomicU8::new(0);
    match STATE.load(Ordering::Relaxed) {
        1 => return true,
        2 => return false,
        _ => {}
    }
    let ok = table_exists(db, "corp_summary").await;
    STATE.store(if ok { 1 } else { 2 }, Ordering::Relaxed);
    if !ok {
        tracing::warn!(
            "corp_summary が見つかりません。M&Aスクリーニングは低速な都度集計にフォールバックします。\
             scripts/build_corp_summary.py --build で作成してください"
        );
    }
    ok
}

/// facilities_fts が使えるか。結果はプロセス内で1度だけ判定して保持する。
async fn fts_available(db: &Database) -> bool {
    use std::sync::atomic::{AtomicU8, Ordering};
    // 0 = 未判定, 1 = あり, 2 = なし
    static STATE: AtomicU8 = AtomicU8::new(0);
    match STATE.load(Ordering::Relaxed) {
        1 => return true,
        2 => return false,
        _ => {}
    }
    let mut ok = false;
    if let Ok(conn) = get_conn(db).await {
        if let Ok(mut rows) = conn
            .query(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='facilities_fts'",
                (),
            )
            .await
        {
            ok = matches!(rows.next().await, Ok(Some(_)));
        }
    }
    STATE.store(if ok { 1 } else { 2 }, Ordering::Relaxed);
    if !ok {
        tracing::warn!(
            "facilities_fts が見つかりません。施設検索は低速な LIKE にフォールバックします。\
             scripts/build_search_index.py --build で索引を作成してください"
        );
    }
    ok
}

pub async fn search_facilities(db: &Database, params: &SearchParams) -> Result<Value, AppError> {
    let filter_params = params.to_filter_params();
    let mut w = WhereBuilder::from_filter_params(&filter_params);

    // テキスト検索（q パラメータ）
    //
    // 以前は 事業所名/法人名/電話番号 に LIKE '%q%' を並べていたが、223,103行の
    // 全表スキャンになり実測 20〜29 秒（COUNT と本体で 2 回走るため体感 57 秒）だった。
    // FTS5(trigram) の索引 facilities_fts を引いて事業所番号で絞る。
    // 索引が未構築の環境でも動くよう、失敗時は LIKE にフォールバックする。
    if let Some(ref q) = params.q {
        let q = q.trim();
        if !q.is_empty() {
            if fts_available(db).await {
                let idx = w.next_param();
                w.params.push(libsql::Value::Text(fts_quote(q)));
                w.conditions.push(format!(
                    "\"事業所番号\" IN (SELECT jigyosho_number FROM facilities_fts \
                     WHERE facilities_fts MATCH ?{})",
                    idx
                ));
            } else {
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
            \"サービスコード\", \"サービス名\",
            latitude, longitude
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
            "latitude": row_f64_opt(row, 26),
            "longitude": row_f64_opt(row, 27),
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

/// 周辺施設検索 — 指定施設を中心に半径内の施設を返す
///
/// 座標は国交省の位置参照情報（町丁目レベル）由来なので、同一町丁目の施設は
/// 同じ座標を持つ。距離は目安として扱うこと。
pub async fn facilities_nearby(
    db: &Database,
    params: &FilterParams,
    center_jigyosho: &str,
    radius_km: f64,
    limit: usize,
    service_name: Option<&str>,
) -> Result<Value, AppError> {
    let conn = get_conn(db).await?;

    // 中心施設
    let center_rows = query_rows_params(
        &conn,
        "SELECT \"事業所番号\", \"事業所名\", \"法人名\", prefecture, \"住所\", \
                \"サービス名\", latitude, longitude \
         FROM facilities WHERE \"事業所番号\" = ?1",
        vec![libsql::Value::Text(center_jigyosho.to_string())],
    )
    .await?;
    let c = center_rows
        .first()
        .ok_or_else(|| AppError::NotFound(format!("事業所番号 {} が見つかりません", center_jigyosho)))?;
    // 中心施設も複数サービスで複数行ありうるので、サービス名はまとめて返す
    let center_services: Vec<String> = {
        let mut v: Vec<String> = Vec::new();
        for r in &center_rows {
            if let Some(s) = row_str_opt(r, 5) {
                if !v.contains(&s) {
                    v.push(s);
                }
            }
        }
        v
    };

    let (clat, clon) = match (row_f64_opt(c, 6), row_f64_opt(c, 7)) {
        (Some(a), Some(b)) => (a, b),
        _ => {
            return Err(AppError::NotFound(
                "この施設には位置情報が付与されていません".into(),
            ))
        }
    };

    let radius_km = radius_km.clamp(0.1, 100.0);
    // 緯度1度=約111km。経度は緯度により縮むので cos で補正して矩形に絞る。
    let dlat = radius_km / 111.0;
    let dlon = radius_km / (111.0 * clat.to_radians().cos().abs().max(0.01));

    // FilterParams は service_code しか持たないため、サービス名の部分一致は
    // 周辺検索の独自パラメータとして受ける（「訪問」で訪問系をまとめて絞れる）
    let mut w = WhereBuilder::from_filter_params(params);
    if let Some(sn) = service_name.map(str::trim).filter(|s| !s.is_empty()) {
        let idx = w.next_param();
        w.params.push(libsql::Value::Text(format!("%{}%", sn)));
        w.conditions.push(format!("\"サービス名\" LIKE ?{}", idx));
    }
    let where_clause = w.to_where_clause();
    let joiner = if where_clause.is_empty() { "WHERE" } else { "AND" };
    let sql = format!(
        "SELECT \"事業所番号\", \"事業所名\", \"法人名\", corp_type, prefecture, \"住所\", \
                \"サービス名\", latitude, longitude, \
                CAST(COALESCE(NULLIF(\"従業者_合計\", ''), '0') AS REAL), \
                CAST(COALESCE(NULLIF(\"定員\", ''), '0') AS REAL), \
                turnover_rate, \"電話番号\" \
         FROM facilities {wc} {j} \
             latitude IS NOT NULL AND longitude IS NOT NULL \
             AND latitude BETWEEN {lat_lo} AND {lat_hi} \
             AND longitude BETWEEN {lon_lo} AND {lon_hi}",
        wc = where_clause,
        j = joiner,
        lat_lo = clat - dlat,
        lat_hi = clat + dlat,
        lon_lo = clon - dlon,
        lon_hi = clon + dlon,
    );

    let rows = query_rows_params(&conn, &sql, w.into_params()).await?;

    // facilities は1事業所×1サービスで1行。同じ事業所番号が最大8行あるため
    // (223,103行 / 190,003ユニーク)、事業所単位に集約しないと地図のピンも
    // 一覧も同じ施設が何度も出てしまう。
    struct Agg {
        distance: f64,
        lat: f64,
        lon: f64,
        name: String,
        corp_name: Option<String>,
        corp_type: Option<String>,
        prefecture: Option<String>,
        address: Option<String>,
        phone: Option<String>,
        services: Vec<String>,
        staff_total: f64,
        capacity: f64,
        turnover_sum: f64,
        turnover_n: usize,
    }

    let mut agg: std::collections::HashMap<String, Agg> = std::collections::HashMap::new();
    for row in &rows {
        let (lat, lon) = match (row_f64_opt(row, 7), row_f64_opt(row, 8)) {
            (Some(a), Some(b)) => (a, b),
            _ => continue,
        };
        let d = haversine_km(clat, clon, lat, lon);
        if d > radius_km {
            continue;
        }
        let jno = row_str(row, 0);
        if jno == center_jigyosho {
            continue;
        }
        let e = agg.entry(jno).or_insert_with(|| Agg {
            distance: d,
            lat,
            lon,
            // 同一事業所でも行によって全角スペースの有無が違うことがあるため最初の値を採る
            name: row_str(row, 1),
            corp_name: row_str_opt(row, 2),
            corp_type: row_str_opt(row, 3),
            prefecture: row_str_opt(row, 4),
            address: row_str_opt(row, 5),
            phone: row_str_opt(row, 12),
            services: Vec::new(),
            staff_total: 0.0,
            capacity: 0.0,
            turnover_sum: 0.0,
            turnover_n: 0,
        });
        if let Some(s) = row_str_opt(row, 6) {
            if !e.services.contains(&s) {
                e.services.push(s);
            }
        }
        // 従業者数・定員はサービスごとに計上されているので合算する
        e.staff_total += row_f64_opt(row, 9).unwrap_or(0.0);
        e.capacity += row_f64_opt(row, 10).unwrap_or(0.0);
        if let Some(t) = row_f64_opt(row, 11) {
            if (0.0..=1.0).contains(&t) {
                e.turnover_sum += t;
                e.turnover_n += 1;
            }
        }
    }

    let mut items: Vec<(f64, Value)> = agg
        .into_iter()
        .map(|(jno, a)| {
            (
                a.distance,
                json!({
                    "jigyosho_number": jno,
                    "jigyosho_name": a.name,
                    "corp_name": a.corp_name,
                    "corp_type": a.corp_type,
                    "prefecture": a.prefecture,
                    "address": a.address,
                    "service_names": a.services,
                    "latitude": a.lat,
                    "longitude": a.lon,
                    "staff_total": a.staff_total,
                    "capacity": a.capacity,
                    "turnover_rate": if a.turnover_n > 0 {
                        Some(a.turnover_sum / a.turnover_n as f64)
                    } else {
                        None
                    },
                    "phone": a.phone,
                    "distance_km": (a.distance * 100.0).round() / 100.0,
                }),
            )
        })
        .collect();

    let matched = items.len();
    items.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    let truncated = matched > limit;
    let out: Vec<Value> = items.into_iter().take(limit).map(|(_, v)| v).collect();

    Ok(json!({
        "center": {
            "jigyosho_number": row_str(c, 0),
            "jigyosho_name": row_str(c, 1),
            "corp_name": row_str_opt(c, 2),
            "prefecture": row_str_opt(c, 3),
            "address": row_str_opt(c, 4),
            "service_names": center_services,
            "latitude": clat,
            "longitude": clon,
        },
        "radius_km": radius_km,
        "items": out,
        // 半径内の該当総数。limit で切った場合に画面がその旨を出せるようにする
        "matched": matched,
        "truncated": truncated,
    }))
}

/// 2点間の大円距離（km）
fn haversine_km(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const R: f64 = 6371.0;
    let (p1, p2) = (lat1.to_radians(), lat2.to_radians());
    let dp = (lat2 - lat1).to_radians();
    let dl = (lon2 - lon1).to_radians();
    let a = (dp / 2.0).sin().powi(2) + p1.cos() * p2.cos() * (dl / 2.0).sin().powi(2);
    2.0 * R * a.sqrt().asin()
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
            \"サービスコード\", \"サービス名\",
            \"会計種類\", \"財務DL_事業活動計算書\", \"財務DL_資金収支計算書\", \"財務DL_貸借対照表\",
            \"要介護1\", \"要介護2\", \"要介護3\", \"要介護4\", \"要介護5\",
            \"利用者総数\", \"利用者_都道府県平均\", \"経験10年以上割合\",
            \"介護職員_常勤\", \"介護職員_非常勤\", \"介護職員_合計\",
            \"看護職員_常勤\", \"看護職員_非常勤\", \"看護職員_合計\",
            \"生活相談員_常勤\", \"生活相談員_非常勤\", \"生活相談員_合計\",
            \"機能訓練指導員_常勤\", \"機能訓練指導員_非常勤\", \"機能訓練指導員_合計\",
            \"管理栄養士_常勤\", \"管理栄養士_非常勤\", \"管理栄養士_合計\",
            \"事務員_常勤\", \"事務員_非常勤\", \"事務員_合計\",
            \"介護福祉士数\", \"実務者研修数\", \"初任者研修数\", \"介護支援専門員数\",
            \"夜勤人数\", \"宿直人数\",
            \"品質_BCP策定\", \"品質_ICT活用\", \"品質_第三者評価\", \"品質_損害賠償保険\",
            quality_score, quality_rank, kasan_count, occupancy_rate,
            \"加算_処遇改善I\", \"加算_処遇改善II\", \"加算_処遇改善III\", \"加算_処遇改善IV\",
            \"加算_特定事業所I\", \"加算_特定事業所II\", \"加算_特定事業所III\", \"加算_特定事業所IV\", \"加算_特定事業所V\",
            \"加算_認知症ケアI\", \"加算_認知症ケアII\", \"加算_口腔連携\", \"加算_緊急時\",
            \"行政処分日\", \"行政処分内容\", \"行政指導日\", \"行政指導内容\",
            \"スクレイピング日\"
        FROM facilities
        WHERE \"事業所番号\" = ?1
        LIMIT 1";

    let rows = query_rows_params(&conn, sql, vec![libsql::Value::Text(id.to_string())]).await?;
    if rows.is_empty() {
        return Err(AppError::NotFound(format!("事業所番号 {} が見つかりません", id)));
    }

    let row = &rows[0];

    // 抽出済み財務データ（あれば）
    let financials = fetch_financials(&conn, "jigyosho_number", id).await;

    // クロス指標（財務 × 公表データ）
    // 「なし」等の否定表記は違反扱いしない
    let sanction_detail = row_str_opt(row, 84).filter(|_| is_real_violation(&row_str_opt(row, 84)));
    let guidance_detail = row_str_opt(row, 86).filter(|_| is_real_violation(&row_str_opt(row, 86)));
    let has_violation = sanction_detail.is_some() || guidance_detail.is_some();
    let cross_metrics = build_cross_metrics(
        &financials,
        row_f64_opt(row, 14),  // 従業者_合計
        row_f64_opt(row, 35),  // 利用者総数
        row_f64_opt(row, 21),  // turnover_rate
        row_f64_opt(row, 69),  // occupancy_rate
        has_violation,
    );

    // 財務データの状態(レビュー④対応: 未公表/未取得/PDFあり抽出前/抽出済み を区別)
    let has_pdf_link = row_str_opt(row, 27).is_some()
        || row_str_opt(row, 28).is_some()
        || row_str_opt(row, 29).is_some();
    let has_extracted = !financials.is_empty();
    let financial_status = if has_extracted {
        "extracted"        // 決算PDFをAI抽出済み(財務サマリー・クロス指標あり)
    } else if has_pdf_link {
        "pdf_available"    // PDFリンクはあるがAI未抽出
    } else {
        "not_published"    // 公表システム上に財務諸表なし(uneiスクレイプ済みが前提)
    };

    Ok(json!({
        "financials": financials,
        "cross_metrics": cross_metrics,
        "financial_status": financial_status,
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
            "accounting_type": row_str_opt(row, 26),
            "financial_statement_url_pl": to_kaigokensaku_url(row_str_opt(row, 27)),
            "financial_statement_url_cf": to_kaigokensaku_url(row_str_opt(row, 28)),
            "financial_statement_url_bs": to_kaigokensaku_url(row_str_opt(row, 29)),
            // 要介護度・利用者（FacilityRowExtended準拠キー）
            "care_level_1": row_f64_opt(row, 30),
            "care_level_2": row_f64_opt(row, 31),
            "care_level_3": row_f64_opt(row, 32),
            "care_level_4": row_f64_opt(row, 33),
            "care_level_5": row_f64_opt(row, 34),
            "total_users": row_f64_opt(row, 35),
            "users_pref_avg": row_f64_opt(row, 36),
            "experienced_10yr_ratio": row_f64_opt(row, 37),
            // 職種別人員体制
            "staffing": {
                "kaigo": {"fulltime": row_f64_opt(row, 38), "parttime": row_f64_opt(row, 39), "total": row_f64_opt(row, 40)},
                "nurse": {"fulltime": row_f64_opt(row, 41), "parttime": row_f64_opt(row, 42), "total": row_f64_opt(row, 43)},
                "counselor": {"fulltime": row_f64_opt(row, 44), "parttime": row_f64_opt(row, 45), "total": row_f64_opt(row, 46)},
                "trainer": {"fulltime": row_f64_opt(row, 47), "parttime": row_f64_opt(row, 48), "total": row_f64_opt(row, 49)},
                "dietitian": {"fulltime": row_f64_opt(row, 50), "parttime": row_f64_opt(row, 51), "total": row_f64_opt(row, 52)},
                "clerk": {"fulltime": row_f64_opt(row, 53), "parttime": row_f64_opt(row, 54), "total": row_f64_opt(row, 55)},
            },
            // 資格・夜間体制
            "qualifications": {
                "care_worker": row_f64_opt(row, 56),
                "jitsumusha": row_f64_opt(row, 57),
                "shoninsha": row_f64_opt(row, 58),
                "care_manager": row_f64_opt(row, 59),
            },
            "night_shift_count": row_f64_opt(row, 60),
            "night_watch_count": row_f64_opt(row, 61),
            // 品質（0/1正規化済みフラグ）
            "has_bcp": row_i64(row, 62) == 1,
            "has_ict": row_i64(row, 63) == 1,
            "has_third_party_eval": row_i64(row, 64) == 1,
            "has_liability_insurance": row_i64(row, 65) == 1,
            "quality_score": row_f64_opt(row, 66),
            "quality_rank": row_str_opt(row, 67),
            "addition_count": row_f64_opt(row, 68),
            "occupancy_rate": row_f64_opt(row, 69),
            // 加算フラグ（FacilityRowExtended準拠キー）
            "addition_treatment_i": row_i64(row, 70) == 1,
            "addition_treatment_ii": row_i64(row, 71) == 1,
            "addition_treatment_iii": row_i64(row, 72) == 1,
            "addition_treatment_iv": row_i64(row, 73) == 1,
            "addition_specific_i": row_i64(row, 74) == 1,
            "addition_specific_ii": row_i64(row, 75) == 1,
            "addition_specific_iii": row_i64(row, 76) == 1,
            "addition_specific_iv": row_i64(row, 77) == 1,
            "addition_specific_v": row_i64(row, 78) == 1,
            "addition_dementia_i": row_i64(row, 79) == 1,
            "addition_dementia_ii": row_i64(row, 80) == 1,
            "addition_oral": row_i64(row, 81) == 1,
            "addition_emergency": row_i64(row, 82) == 1,
            // 行政処分・指導（M&A DDリスク、「なし」系表記は除外済み）
            "sanction_date": row_str_opt(row, 83).filter(|_| sanction_detail.is_some()),
            "sanction_detail": sanction_detail,
            "guidance_date": row_str_opt(row, 85).filter(|_| guidance_detail.is_some()),
            "guidance_detail": guidance_detail,
            // データ鮮度
            "scraped_at": row_str_opt(row, 87),
        }
    }))
}

/// 抽出済み財務データ(financialsテーブル)を取得する
/// key_col は "jigyosho_number" または "corp_number"
/// テーブル未作成・エラー時は空配列を返す（財務は付加情報のため本体を止めない）
async fn fetch_financials(conn: &libsql::Connection, key_col: &str, key: &str) -> Vec<Value> {
    let sql = format!(
        "SELECT jigyosho_number, doc_type, fiscal_period, revenue, personnel_cost,
                operating_income, ordinary_income, net_income,
                prior_revenue, prior_operating_income,
                total_assets, net_assets, total_liabilities, confidence, notes
         FROM financials WHERE {} = ?1 ORDER BY jigyosho_number, doc_type",
        key_col
    );
    let rows = match query_rows_params(conn, &sql, vec![libsql::Value::Text(key.to_string())]).await {
        Ok(r) => r,
        Err(_) => return Vec::new(),
    };
    rows.iter()
        .map(|row| {
            json!({
                "jigyosho_number": row_str(row, 0),
                "doc_type": row_str(row, 1),
                "fiscal_period": row_str_opt(row, 2),
                "revenue": row_f64_opt(row, 3),
                "personnel_cost": row_f64_opt(row, 4),
                "operating_income": row_f64_opt(row, 5),
                "ordinary_income": row_f64_opt(row, 6),
                "net_income": row_f64_opt(row, 7),
                "prior_revenue": row_f64_opt(row, 8),
                "prior_operating_income": row_f64_opt(row, 9),
                "total_assets": row_f64_opt(row, 10),
                "net_assets": row_f64_opt(row, 11),
                "total_liabilities": row_f64_opt(row, 12),
                "confidence": row_str_opt(row, 13),
                "notes": row_str_opt(row, 14),
            })
        })
        .collect()
}

/// 行政処分・指導の記載が「実質的な違反記録」かを判定する
/// 公表データには「なし」「無し」「特になし」「該当なし」「ありません」等の
/// 否定表記が大量に含まれるため、それらを違反扱いから除外する
fn is_real_violation(text: &Option<String>) -> bool {
    let Some(t) = text else { return false };
    let cleaned: String = t
        .trim()
        .trim_end_matches(['。', '.', '、', ','])
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();
    if cleaned.is_empty() {
        return false;
    }
    // 完全一致の否定表記
    const NONE_VALUES: [&str; 12] = [
        "なし", "無し", "無", "ない", "無い",
        "特になし", "特に無し", "特にない",
        "該当なし", "該当無し",
        "ありません", "特にありません",
    ];
    if NONE_VALUES.contains(&cleaned.as_str()) {
        return false;
    }
    // 長文の否定表記（例:「処分や指導に関する情報はありません」「該当事項なし」）
    // 否定語で文が終わるものは違反記録ではないとみなす
    for suffix in ["ありません", "なし", "無し", "ない", "無い", "無"] {
        if cleaned.ends_with(suffix) {
            return false;
        }
    }
    true
}

/// 決算PDF由来financials × 公表データのクロス指標を計算する
///
/// レビュー指摘(2026-07-27)を反映した設計:
/// - PL/BSは **同一事業所番号・同一会計期間** のペアだけを結合する。
///   異なる施設・年度のPLとBSを混ぜて自己資本比率等を算出しない。
/// - risk_score(=要確認シグナル)は「未知」と「安全」を混同しないため、
///   必要ファクタが最低限揃わなければ null(算定不能)を返す。
/// - coverage(揃ったファクタ/必要ファクタ)を返し、UIが状態を出し分けられるようにする。
/// - 労働生産性・利用者単価は財務(施設単位)と公表値(施設単位)の粒度が一致する
///   施設単位でのみ意味を持つ。法人合算との混合は呼び出し側で避ける。
fn build_cross_metrics(
    financials: &[Value],
    staff_total: Option<f64>,
    total_users: Option<f64>,
    turnover_rate: Option<f64>,
    occupancy_rate: Option<f64>,
    has_violation: bool,
) -> Value {
    // 同一事業所番号 × 同一会計期間 のPL/BSペアを探す(スコープ混在防止)
    let (pl, bs) = pick_consistent_pl_bs(financials);

    let revenue = pl.and_then(|p| p["revenue"].as_f64());
    let personnel_cost = pl.and_then(|p| p["personnel_cost"].as_f64());
    let operating_income = pl.and_then(|p| p["operating_income"].as_f64());
    // net_assets/total_assets は PL と会計期間・施設が一致する BS からのみ取る
    let net_assets = bs.and_then(|b| b["net_assets"].as_f64());
    let total_assets = bs.and_then(|b| b["total_assets"].as_f64());

    let labor_productivity = match (revenue, staff_total) {
        (Some(r), Some(s)) if s > 0.0 => Some(r / s),
        _ => None,
    };
    let revenue_per_user = match (revenue, total_users) {
        (Some(r), Some(u)) if u > 0.0 => Some(r / u),
        _ => None,
    };
    let personnel_cost_ratio = match (personnel_cost, revenue) {
        (Some(p), Some(r)) if r > 0.0 => Some(p / r),
        _ => None,
    };
    let operating_margin = match (operating_income, revenue) {
        (Some(o), Some(r)) if r > 0.0 => Some(o / r),
        _ => None,
    };
    // 自己資本比率は PL とスコープ一致した BS がある場合のみ
    let equity_ratio = match (net_assets, total_assets) {
        (Some(n), Some(t)) if t > 0.0 => Some(n / t),
        _ => None,
    };

    // 要確認シグナル(旧: 経営危険度スコア)
    // 各ファクタは「判定に必要なデータが有るか(available)」「該当したか(hit)」を区別する。
    // 校正済みの予測モデルではないため0-100スコアや「低/中/高」ラベルは付けない。
    let mut signals: Vec<String> = Vec::new();
    let mut available = 0u32;   // 判定できたファクタ数
    let required = 4u32;        // 最低限ほしいファクタ: 純資産/営業損益/離職率/稼働率

    if net_assets.is_some() { available += 1; }
    if operating_income.is_some() { available += 1; }
    if turnover_rate.is_some() { available += 1; }
    if occupancy_rate.is_some() { available += 1; }

    if net_assets == Some(0.0) {} // no-op(明示: 0は債務超過ではない)
    if let Some(n) = net_assets { if n < 0.0 { signals.push("債務超過".to_string()); } }
    if let Some(o) = operating_income { if o < 0.0 { signals.push("営業赤字".to_string()); } }
    if let Some(t) = turnover_rate { if t > 0.25 { signals.push(format!("高離職率 {:.0}%", t * 100.0)); } }
    if has_violation { signals.push("行政処分・指導歴".to_string()); }
    if let Some(o) = occupancy_rate { if o < 0.5 { signals.push(format!("低稼働率 {:.0}%", o * 100.0)); } }

    let has_financials = pl.is_some() || bs.is_some();
    // 財務データが無い or 必要ファクタが1つも揃わない場合は算定不能(nullで返す)
    let computable = has_financials && available > 0;
    let signal_count: Value = if computable {
        Value::from(signals.len())
    } else {
        Value::Null
    };
    let fiscal_period: Option<String> = pl
        .and_then(|p| p["fiscal_period"].as_str())
        .or_else(|| bs.and_then(|b| b["fiscal_period"].as_str()))
        .map(|s| s.to_string());
    let pl_bs_scope_matched = pl.is_some() && bs.is_some();

    json!({
        "has_financials": has_financials,
        "labor_productivity": labor_productivity,
        "revenue_per_user": revenue_per_user,
        "personnel_cost_ratio": personnel_cost_ratio,
        "operating_margin": operating_margin,
        "equity_ratio": equity_ratio,
        // 要確認シグナル(検証済みスコアではない)
        "signals": signals,
        "signal_count": signal_count,
        // カバレッジ(未知と安全の区別のため)
        "coverage": {
            "available_factors": available,
            "required_factors": required,
            "computable": computable,
            "fiscal_period": fiscal_period,
            "pl_bs_scope_matched": pl_bs_scope_matched,
        },
    })
}

/// financials配列から、同一事業所番号かつ同一会計期間のPL/BSペアを選ぶ。
/// スコープ(施設・年度)が一致する組が無ければ、BSは None(=自己資本比率等を出さない)。
/// PL単独は許容する(収益・営業損益・人件費率はPLだけで完結するため)。
fn pick_consistent_pl_bs(financials: &[Value]) -> (Option<&Value>, Option<&Value>) {
    let pls: Vec<&Value> = financials.iter().filter(|f| f["doc_type"] == "PL").collect();
    let bss: Vec<&Value> = financials.iter().filter(|f| f["doc_type"] == "BS").collect();

    // PLごとに、同一事業所番号・同一会計期間のBSがあるか探す
    for pl in &pls {
        let pj = pl["jigyosho_number"].as_str();
        let pf = pl["fiscal_period"].as_str();
        for bs in &bss {
            if bs["jigyosho_number"].as_str() == pj
                && pf.is_some()
                && bs["fiscal_period"].as_str() == pf
            {
                return (Some(pl), Some(bs));
            }
        }
    }
    // スコープ一致ペアなし: PL単独(あれば)を返す。BSは混ぜない。
    (pls.first().copied(), None)
}

/// 財務DL列の相対パスを介護情報公表システムの絶対URLに変換する
/// （例: /upload/jigyosyofile/... → https://www.kaigokensaku.mhlw.go.jp/upload/...）
fn to_kaigokensaku_url(path: Option<String>) -> Option<String> {
    let path = path?;
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return None;
    }
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        Some(trimmed.to_string())
    } else {
        Some(format!("https://www.kaigokensaku.mhlw.go.jp{}", trimmed))
    }
}

// ================================================================
// M&A系（SQL版）
// ================================================================

/// M&Aスクリーニング
/// 行政処分・指導の内容が実質的な違反記録かを判定するSQL断片
/// （is_real_violation のSQL近似版。否定表記を除外する）
fn sql_real_violation(col: &str) -> String {
    format!(
        "(COALESCE(\"{c}\", '') != '' \
          AND \"{c}\" NOT LIKE '%ありません%' \
          AND \"{c}\" NOT LIKE '%なし%' \
          AND \"{c}\" NOT LIKE '%無し%' \
          AND TRIM(\"{c}\") NOT IN ('無', 'ない', '無い'))",
        c = col
    )
}

/// 財務データのカバレッジ集計(レビュー⑤対応)
/// スクリーニング対象が全国全数ではなく「取得済み」に限られることを数値で示す。
pub async fn financial_coverage(db: &Database) -> Result<Value, AppError> {
    let conn = get_conn(db).await?;
    let sql = "SELECT
        (SELECT COUNT(DISTINCT \"法人番号\") FROM facilities WHERE COALESCE(\"法人番号\",'') != '') AS total_corps,
        (SELECT COUNT(DISTINCT \"法人番号\") FROM facilities WHERE COALESCE(\"法人番号\",'') != '' AND COALESCE(\"財務DL_事業活動計算書\",'') != '') AS pdf_corps,
        (SELECT COUNT(DISTINCT corp_number) FROM financials WHERE corp_number IS NOT NULL) AS extracted_corps";
    let row = query_single_row_params(&conn, sql, vec![]).await?;
    let total = row_i64(&row, 0);
    let pdf = row_i64(&row, 1);
    let extracted = row_i64(&row, 2);
    Ok(json!({
        "total_corps": total,
        "pdf_corps": pdf,          // 財務PDFリンクを保有する法人数
        "extracted_corps": extracted, // 財務数値をAI抽出済みの法人数(=財務フィルタが実効となる母集団)
        "pdf_ratio": if total > 0 { pdf as f64 / total as f64 } else { 0.0 },
        "extracted_ratio": if total > 0 { extracted as f64 / total as f64 } else { 0.0 },
    }))
}

#[allow(clippy::too_many_arguments)]
pub async fn ma_screening(
    db: &Database,
    params: &FilterParams,
    prefectures: &Option<String>,
    corp_types: &Option<String>,
    staff_min: Option<f64>,
    staff_max: Option<f64>,
    turnover_min: Option<f64>,
    turnover_max: Option<f64>,
    only_with_financials: bool,
    only_insolvent: bool,
    only_operating_loss: bool,
    only_with_violations: bool,
    limit: usize,
) -> Result<Value, AppError> {
    // 事前集計テーブル corp_summary から引く。
    // facilities を都度 GROUP BY すると 223,103行 / 190,003法人の集約になり、
    // ORDER BY facility_count DESC のため LIMIT も効かず実測 115 秒かかっていた。
    // corp_summary は施設データ更新時にのみ作り直す（scripts/build_corp_summary.py）。
    let use_summary = corp_summary_available(db).await;

    // corp_summary はカラム名が facilities と異なるため WhereBuilder を使わず自前で組む
    let mut conds: Vec<String> = Vec::new();
    let mut base_params: Vec<libsql::Value> = Vec::new();
    let mut pc = 0usize;

    if use_summary {
        // 都道府県・法人種別は FilterParams 側（画面が送るのはこちら）と
        // 独立クエリパラメータ側の両方を受ける
        let pref_src = params
            .prefecture
            .as_deref()
            .or(prefectures.as_deref());
        if let Some(pref) = pref_src {
            let list: Vec<&str> = pref.split(',').map(str::trim).filter(|s| !s.is_empty()).collect();
            if !list.is_empty() {
                let mut ors = Vec::new();
                for p in list {
                    pc += 1;
                    base_params.push(libsql::Value::Text(format!("%{}%", p)));
                    ors.push(format!("prefectures LIKE ?{}", pc));
                }
                conds.push(format!("({})", ors.join(" OR ")));
            }
        }
        let ct_src = params.corp_type.as_deref().or(corp_types.as_deref());
        if let Some(ct) = ct_src {
            let list: Vec<&str> = ct.split(',').map(str::trim).filter(|s| !s.is_empty()).collect();
            if !list.is_empty() {
                let mut ors = Vec::new();
                for c in list {
                    pc += 1;
                    base_params.push(libsql::Value::Text(c.to_string()));
                    ors.push(format!("corp_type = ?{}", pc));
                }
                conds.push(format!("({})", ors.join(" OR ")));
            }
        }
        if let Some(ref sc) = params.service_code {
            let list: Vec<&str> = sc.split(',').map(str::trim).filter(|s| !s.is_empty()).collect();
            if !list.is_empty() {
                let mut ors = Vec::new();
                for c in list {
                    pc += 1;
                    base_params.push(libsql::Value::Text(format!("%{}%", c)));
                    ors.push(format!("service_names LIKE ?{}", pc));
                }
                conds.push(format!("({})", ors.join(" OR ")));
            }
        }
    }

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

    // 財務フィルタ（financialsテーブン由来のフラグ列に対するHAVING）
    if only_with_financials {
        having_conditions.push("has_financials = 1".to_string());
    }
    if only_insolvent {
        having_conditions.push("is_insolvent = 1".to_string());
    }
    if only_operating_loss {
        having_conditions.push("has_operating_loss = 1".to_string());
    }
    if only_with_violations {
        having_conditions.push("has_violation = 1".to_string());
    }

    extra_counter += 1;
    extra_params.push(libsql::Value::Integer(limit as i64));
    let limit_idx = extra_counter;

    let having = if having_conditions.is_empty() {
        String::new()
    } else {
        format!("HAVING {}", having_conditions.join(" AND "))
    };

    let violation_expr = format!(
        "MAX(CASE WHEN {} OR {} THEN 1 ELSE 0 END)",
        sql_real_violation("行政処分内容"),
        sql_real_violation("行政指導内容"),
    );

    let sql = format!(
        "SELECT
            MAX(COALESCE(\"法人名\", '')) as corp_name,
            \"法人番号\" as corp_number,
            MAX(corp_type) as corp_type,
            COUNT(*) as facility_count,
            SUM(CAST(COALESCE(NULLIF(\"従業者_合計\", ''), '0') AS REAL)) as total_staff,
            AVG(CASE WHEN turnover_rate BETWEEN 0.0 AND 1.0 THEN turnover_rate END) as avg_turnover,
            AVG(CAST(COALESCE(NULLIF(\"定員\", ''), '0') AS REAL)) as avg_capacity,
            GROUP_CONCAT(DISTINCT prefecture) as prefectures,
            GROUP_CONCAT(DISTINCT \"サービス名\") as service_names,
            MAX(CASE WHEN EXISTS (SELECT 1 FROM financials fin WHERE fin.corp_number = facilities.\"法人番号\") THEN 1 ELSE 0 END) as has_financials,
            MAX(CASE WHEN EXISTS (SELECT 1 FROM financials fin WHERE fin.corp_number = facilities.\"法人番号\" AND fin.doc_type = 'BS' AND fin.net_assets < 0) THEN 1 ELSE 0 END) as is_insolvent,
            MAX(CASE WHEN EXISTS (SELECT 1 FROM financials fin WHERE fin.corp_number = facilities.\"法人番号\" AND fin.doc_type = 'PL' AND fin.operating_income < 0) THEN 1 ELSE 0 END) as has_operating_loss,
            {} as has_violation
        FROM facilities {}
        GROUP BY \"法人番号\"
        {}
        ORDER BY facility_count DESC, total_staff DESC
        LIMIT ?{}",
        violation_expr, where_clause, having, limit_idx
    );

    let conn = get_conn(db).await?;

    let rows = if use_summary {
        // corp_summary 版: 列順を facilities 版と揃えて後段の処理を共通化する
        let mut sp = base_params;
        let mut sc_conds = conds;
        let mut n = pc;

        if let Some(min) = staff_min {
            n += 1;
            sp.push(libsql::Value::Real(min));
            sc_conds.push(format!("total_staff >= ?{}", n));
        }
        if let Some(max) = staff_max {
            n += 1;
            sp.push(libsql::Value::Real(max));
            sc_conds.push(format!("total_staff <= ?{}", n));
        }
        if let Some(min) = turnover_min {
            n += 1;
            sp.push(libsql::Value::Real(min));
            sc_conds.push(format!("avg_turnover >= ?{}", n));
        }
        if let Some(max) = turnover_max {
            n += 1;
            sp.push(libsql::Value::Real(max));
            sc_conds.push(format!("avg_turnover <= ?{}", n));
        }
        if only_with_financials {
            sc_conds.push("has_financials = 1".into());
        }
        if only_insolvent {
            sc_conds.push("is_insolvent = 1".into());
        }
        if only_operating_loss {
            sc_conds.push("has_operating_loss = 1".into());
        }
        if only_with_violations {
            sc_conds.push("has_violation = 1".into());
        }
        n += 1;
        sp.push(libsql::Value::Integer(limit as i64));

        let where_sql = if sc_conds.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", sc_conds.join(" AND "))
        };
        let summary_sql = format!(
            "SELECT corp_name, corp_number, corp_type, facility_count, total_staff, \
                    avg_turnover, avg_capacity, prefectures, service_names, \
                    has_financials, is_insolvent, has_operating_loss, has_violation \
             FROM corp_summary {} \
             ORDER BY facility_count DESC, total_staff DESC LIMIT ?{}",
            where_sql, n
        );
        query_rows_params(&conn, &summary_sql, sp).await?
    } else {
        let mut all_params = w.into_params();
        all_params.extend(extra_params);
        query_rows_params(&conn, &sql, all_params).await?
    };

    let items: Vec<Value> = rows.iter().enumerate().map(|(_i, row)| {
        let fac_count = row_i64(row, 3) as f64;
        let total_staff = row_f64(row, 4);
        let avg_turnover = row_f64_opt(row, 5);
        let prefs_str = row_str(row, 7);
        let svcs_str = row_str(row, 8);
        let has_financials = row_i64(row, 9) == 1;
        let is_insolvent = row_i64(row, 10) == 1;
        let has_operating_loss = row_i64(row, 11) == 1;
        let has_violation = row_i64(row, 12) == 1;
        let pref_count = prefs_str.split(',').filter(|s| !s.is_empty()).count() as f64;

        // 規模区分（固定しきい値・母集団非依存）
        let size_tier = if fac_count >= 10.0 || total_staff >= 200.0 {
            "大"
        } else if fac_count >= 3.0 || total_staff >= 50.0 {
            "中"
        } else {
            "小"
        };

        // 用途別スコア（絶対基準: 固定しきい値の加点のみ。母集団で変動しない）
        // M&A観点: 規模の実体 + 財務情報の有無 + リスクの無さ
        let mut ma_score = 0.0_f64;
        if fac_count >= 10.0 { ma_score += 30.0 } else if fac_count >= 3.0 { ma_score += 20.0 } else { ma_score += 10.0 }
        if total_staff >= 200.0 { ma_score += 25.0 } else if total_staff >= 50.0 { ma_score += 15.0 } else { ma_score += 5.0 }
        if has_financials { ma_score += 15.0 }          // 財務情報が取得できる＝精査可能
        if !is_insolvent && !has_operating_loss { ma_score += 20.0 }  // 債務超過・営業赤字なし
        if !has_violation { ma_score += 10.0 }          // 行政指導なし

        // 営業観点: 提案余地(施設数) + 商圏の広さ + 人員規模
        let mut sales_score = 0.0_f64;
        if fac_count >= 10.0 { sales_score += 40.0 } else if fac_count >= 3.0 { sales_score += 25.0 } else { sales_score += 10.0 }
        if pref_count >= 3.0 { sales_score += 25.0 } else if pref_count >= 2.0 { sales_score += 15.0 } else { sales_score += 5.0 }
        if total_staff >= 200.0 { sales_score += 25.0 } else if total_staff >= 50.0 { sales_score += 15.0 } else { sales_score += 5.0 }
        if !has_violation { sales_score += 10.0 }

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
            // 絶対基準スコア（固定しきい値の加点のみ。検索条件を変えても同じ法人の値は変わらない）
            "ma_score": ma_score,
            "sales_score": sales_score,
            "size_tier": size_tier,
            "prefecture_count": pref_count,
            "has_financials": has_financials,
            "is_insolvent": is_insolvent,
            "has_operating_loss": has_operating_loss,
            "has_violation": has_violation,
        })
    }).collect();

    let total = items.len();

    // レビュー対応(2026-07-28): funnelの「全法人」が表示件数(LIMIT後)と同値になる誤りを修正。
    // 全国のユニーク法人総数を別途取得し、「表示中は上位のみ」であることを正直に示す。
    // corp_summary があれば 1 法人 1 行なので COUNT(*) で足りる（COUNT(DISTINCT) より軽い）
    let total_corps_sql = if use_summary {
        "SELECT COUNT(*) FROM corp_summary"
    } else {
        "SELECT COUNT(DISTINCT \"法人番号\") FROM facilities WHERE COALESCE(\"法人番号\",'') != ''"
    };
    let total_corps = query_single_row_params(&conn, total_corps_sql, vec![])
        .await
        .ok()
        .map(|r| row_i64(&r, 0))
        .unwrap_or(total as i64);

    Ok(json!({
        "items": items,
        "total": total,
        "total_corps": total_corps,
        "funnel": [
            {"stage": "全国の法人", "count": total_corps},
            {"stage": "表示（条件該当・上位）", "count": total},
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
        GROUP BY \"法人番号\"
        ORDER BY facility_count DESC
        LIMIT 50",
        where_clause
    );

    let conn = get_conn(db).await?;

    // 法人名の部分一致で facilities を GROUP BY すると全表スキャンになり、実測 138 秒。
    // 1法人1行の corp_summary を引けば同じ結果が桁違いに速く得られる。
    let rows = if params.is_default() && corp_summary_available(db).await {
        let mut p: Vec<libsql::Value> = Vec::new();
        let mut cond = String::new();
        if !query.is_empty() {
            p.push(libsql::Value::Text(format!("%{}%", query)));
            p.push(libsql::Value::Text(query.to_string()));
            cond = "WHERE (corp_name LIKE ?1 OR corp_number = ?2)".to_string();
        }
        query_rows_params(
            &conn,
            &format!(
                "SELECT corp_name, corp_number, facility_count, total_staff \
                 FROM corp_summary {} ORDER BY facility_count DESC LIMIT 50",
                cond
            ),
            p,
        )
        .await?
    } else {
        query_rows_params(&conn, &sql, w.into_params()).await?
    };

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
            CAST(COALESCE(NULLIF(\"品質_BCP策定\", ''), '0') AS INTEGER) as bcp,
            CAST(COALESCE(NULLIF(\"品質_損害賠償保険\", ''), '0') AS INTEGER) as insurance,
            \"事業所番号\", \"会計種類\",
            \"財務DL_事業活動計算書\", \"財務DL_資金収支計算書\", \"財務DL_貸借対照表\",
            \"行政処分日\", \"行政処分内容\", \"行政指導日\", \"行政指導内容\",
            \"介護職員_合計\", \"看護職員_合計\", \"介護福祉士数\", \"介護支援専門員数\"
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

    // 施設ごとの財務諸表リンク（PL/CF/BSのいずれかがある施設のみ）
    let mut financial_links = Vec::new();
    let mut accounting_type: Option<String> = None;

    // 行政処分・指導（コンプライアンスDDの実データ）と法人人員集計
    let mut violations = Vec::new();
    let mut total_kaigo_staff = 0.0f64;
    let mut total_nurse_staff = 0.0f64;
    let mut total_care_workers = 0.0f64;
    let mut total_care_managers = 0.0f64;

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

        // 財務諸表リンク収集
        if accounting_type.is_none() {
            accounting_type = row_str_opt(row, 16).filter(|s| !s.trim().is_empty());
        }
        let pl = to_kaigokensaku_url(row_str_opt(row, 17));
        let cf = to_kaigokensaku_url(row_str_opt(row, 18));
        let bs = to_kaigokensaku_url(row_str_opt(row, 19));
        if pl.is_some() || cf.is_some() || bs.is_some() {
            financial_links.push(json!({
                "facility_name": row_str(row, 0),
                "jigyosho_number": row_str_opt(row, 15),
                "pl_url": pl,
                "cf_url": cf,
                "bs_url": bs,
            }));
        }

        // 行政処分・指導の収集（「なし」等の否定表記は除外）
        let sanction_detail = row_str_opt(row, 21).filter(|_| is_real_violation(&row_str_opt(row, 21)));
        let guidance_detail = row_str_opt(row, 23).filter(|_| is_real_violation(&row_str_opt(row, 23)));
        if sanction_detail.is_some() || guidance_detail.is_some() {
            violations.push(json!({
                "facility_name": row_str(row, 0),
                "jigyosho_number": row_str_opt(row, 15),
                "sanction_date": row_str_opt(row, 20),
                "sanction_detail": sanction_detail,
                "guidance_date": row_str_opt(row, 22),
                "guidance_detail": guidance_detail,
            }));
        }

        // 法人人員集計
        total_kaigo_staff += row_f64(row, 24);
        total_nurse_staff += row_f64(row, 25);
        total_care_workers += row_f64(row, 26);
        total_care_managers += row_f64(row, 27);
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
    if !violations.is_empty() {
        risk_flags.push(json!({
            "level": "red",
            "category": "コンプライアンス",
            "detail": format!("行政処分・指導の記録が{}施設に存在", violations.len()),
        }));
    }

    // 加算取得状況（13項目 × 法人配下の全施設）
    // DBのカラム名は「加算_特定事業所I」「加算_認知症ケアI」、値は TEXT の '0'/'1'。
    let kasan_summary = {
        let cols = [
            "加算_処遇改善I", "加算_処遇改善II", "加算_処遇改善III", "加算_処遇改善IV",
            "加算_特定事業所I", "加算_特定事業所II", "加算_特定事業所III",
            "加算_特定事業所IV", "加算_特定事業所V",
            "加算_認知症ケアI", "加算_認知症ケアII", "加算_口腔連携", "加算_緊急時",
        ];
        // フロントの表示名 = カラム名から「加算_」を除いたもの
        let names: Vec<&str> = cols.iter().map(|c| &c["加算_".len()..]).collect();

        let selects: Vec<String> = cols
            .iter()
            .map(|c| format!("CAST(COALESCE(NULLIF(\"{}\", ''), '0') AS INTEGER)", c))
            .collect();
        let kasan_sql = format!(
            "SELECT \"事業所名\", {} FROM facilities WHERE \"法人番号\" = ?1",
            selects.join(", ")
        );
        let kasan_rows = query_rows_params(
            &conn,
            &kasan_sql,
            vec![libsql::Value::Text(corp_number.to_string())],
        )
        .await
        .unwrap_or_default();

        let mut totals: serde_json::Map<String, Value> = serde_json::Map::new();
        for n in &names {
            totals.insert(n.to_string(), json!(0));
        }
        // totals は全施設で集計する。施設別の明細だけは行数上限を設ける
        // （2,700施設超の法人が実在するため）。切り捨てた件数はレスポンスに含める。
        const FACILITY_DETAIL_LIMIT: usize = 200;
        let mut fac_list = Vec::new();
        for row in &kasan_rows {
            let mut map = serde_json::Map::new();
            for (i, n) in names.iter().enumerate() {
                let has = row_i64(row, (i + 1) as i32) == 1;
                map.insert(n.to_string(), json!(has));
                if has {
                    let cur = totals.get(*n).and_then(|v| v.as_i64()).unwrap_or(0);
                    totals.insert(n.to_string(), json!(cur + 1));
                }
            }
            if fac_list.len() < FACILITY_DETAIL_LIMIT {
                fac_list.push(json!({
                    "facility_name": row_str(row, 0),
                    "kasan": map,
                }));
            }
        }

        json!({
            "facilities": fac_list,
            "totals": totals,
            "facility_count": kasan_rows.len(),
            "facilities_shown": fac_list.len(),
            "has_data": !kasan_rows.is_empty(),
        })
    };

    // 法人レベルのクロス指標:
    // レビュー指摘(2026-07-27)対応 —
    // 旧実装は「売上最大PL」と「総資産最大BS」を独立に選び、別施設・別年度の
    // PL/BSから自己資本比率を合成しうる問題があった。撤廃し、build_cross_metrics内の
    // pick_consistent_pl_bs(同一施設×同一会計期間)に代表施設の選定を委ねる。
    // 労働生産性・利用者単価は分子(代表施設売上)と分母(法人合算従業者)の粒度が
    // 一致しないため、法人レベルでは渡さない(None → 出さない)。
    let extracted_financials = fetch_financials(&conn, "corp_number", corp_number).await;
    let corp_cross_metrics = build_cross_metrics(
        &extracted_financials,
        None, // staff_total: 法人合算と代表施設売上は粒度不一致のため渡さない
        None, // total_users: 同上
        avg_turnover,
        avg_occupancy,
        !violations.is_empty(),
    );
    // 財務データ状態の件数(move前に確定)
    let pdf_facility_count = financial_links.len();
    let extracted_facility_count = extracted_financials
        .iter()
        .filter_map(|f| f["jigyosho_number"].as_str())
        .collect::<std::collections::HashSet<_>>()
        .len();

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
            "total_kaigo_staff": total_kaigo_staff,
            "total_nurse_staff": total_nurse_staff,
            "total_care_workers": total_care_workers,
            "total_care_managers": total_care_managers,
        },
        "compliance_dd": {
            "has_violations": !violations.is_empty(),
            "violations": violations,
            "bcp_rate": bcp_rate,
            "insurance_rate": insurance_rate,
        },
        "financial_dd": {
            "accounting_type": accounting_type,
            "financial_links": financial_links,
            "extracted_financials": extracted_financials,
            // レビュー④対応: 財務データの状態を明示
            "pdf_facility_count": pdf_facility_count,
            "extracted_facility_count": extracted_facility_count,
        },
        "cross_metrics": corp_cross_metrics,
        "risk_flags": risk_flags,
        "benchmark": benchmark,
        "kasan_summary": kasan_summary,
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
            occupancy_rate, quality_score, kasan_count,
            \"サービス名\"
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
    let service_name = row_str_opt(row, 12).unwrap_or_default();

    // 全国平均・都道府県平均。
    // facilities に対する 8 指標の AVG は TEXT の CAST を含み全表スキャンになる。
    // 数値化済みの facility_metrics があればそちらを使う（列が 11 しかなく軽い）。
    let use_metrics_avg = table_exists(db, "facility_metrics").await;

    let avg_sql = if use_metrics_avg {
        "SELECT AVG(staff), AVG(capacity), AVG(turnover), AVG(fulltime), \
                AVG(years), AVG(occupancy), AVG(quality), AVG(kasan) \
         FROM facility_metrics"
    } else {
        "SELECT
            AVG(CAST(COALESCE(NULLIF(\"従業者_合計\", ''), '0') AS REAL)),
            AVG(CAST(COALESCE(NULLIF(\"定員\", ''), '0') AS REAL)),
            AVG(CASE WHEN turnover_rate BETWEEN 0.0 AND 1.0 THEN turnover_rate END),
            AVG(CASE WHEN fulltime_ratio BETWEEN 0.0 AND 1.0 THEN fulltime_ratio END),
            AVG(years_in_business),
            AVG(CASE WHEN occupancy_rate BETWEEN 0.0 AND 3.0 THEN occupancy_rate END),
            AVG(CAST(COALESCE(NULLIF(quality_score, ''), '0') AS REAL)),
            AVG(CAST(COALESCE(NULLIF(kasan_count, ''), '0') AS REAL))
        FROM facilities"
    };
    let nat_row = query_single_row_params(&conn, avg_sql, vec![]).await?;

    let pref_avg_sql = if use_metrics_avg {
        "SELECT AVG(staff), AVG(capacity), AVG(turnover), AVG(fulltime), \
                AVG(years), AVG(occupancy), AVG(quality), AVG(kasan) \
         FROM facility_metrics WHERE prefecture = ?1"
    } else {
        "SELECT
            AVG(CAST(COALESCE(NULLIF(\"従業者_合計\", ''), '0') AS REAL)),
            AVG(CAST(COALESCE(NULLIF(\"定員\", ''), '0') AS REAL)),
            AVG(CASE WHEN turnover_rate BETWEEN 0.0 AND 1.0 THEN turnover_rate END),
            AVG(CASE WHEN fulltime_ratio BETWEEN 0.0 AND 1.0 THEN fulltime_ratio END),
            AVG(years_in_business),
            AVG(CASE WHEN occupancy_rate BETWEEN 0.0 AND 3.0 THEN occupancy_rate END),
            AVG(CAST(COALESCE(NULLIF(quality_score, ''), '0') AS REAL)),
            AVG(CAST(COALESCE(NULLIF(kasan_count, ''), '0') AS REAL))
        FROM facilities WHERE prefecture = ?1"
    };
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

    // パーセンタイル（全国 / 都道府県内 / 同一サービス種別）
    // 旧実装は "percentiles": {} を返しており、画面のパーセンタイル表が常に非表示だった。
    // 各指標について「この施設より低い値の施設が何%あるか」= 高いほど上位。
    // 定着率は 1 - 離職率 として扱うので、離職率が低いほど上位になる。
    // (指標名, 値が有効な行の条件, その施設より下位である条件)
    // 未登録(NULL/空)を 0 とみなして母数に含めるとパーセンタイルが歪むため、
    // 指標ごとに有効な行だけを母数にする。
    let pct_exprs: Vec<(&str, String, String)> = vec![
        (
            "従業者数",
            "NULLIF(\"従業者_合計\", '') IS NOT NULL".into(),
            format!("CAST(NULLIF(\"従業者_合計\", '') AS REAL) < {}", staff),
        ),
        (
            "定員",
            "NULLIF(\"定員\", '') IS NOT NULL".into(),
            format!("CAST(NULLIF(\"定員\", '') AS REAL) < {}", capacity),
        ),
        (
            "定着率",
            "turnover_rate BETWEEN 0.0 AND 1.0".into(),
            format!("turnover_rate BETWEEN 0.0 AND 1.0 AND turnover_rate > {}", turnover),
        ),
        (
            "常勤比率",
            "fulltime_ratio BETWEEN 0.0 AND 1.0".into(),
            format!("fulltime_ratio BETWEEN 0.0 AND 1.0 AND fulltime_ratio < {}", fulltime),
        ),
        (
            "事業年数",
            "years_in_business > 0 AND years_in_business <= 100".into(),
            format!("years_in_business > 0 AND years_in_business <= 100 AND years_in_business < {}", years),
        ),
        (
            "稼働率",
            "occupancy_rate BETWEEN 0.0 AND 3.0".into(),
            format!("occupancy_rate BETWEEN 0.0 AND 3.0 AND occupancy_rate < {}", occupancy),
        ),
        (
            "品質スコア",
            "NULLIF(quality_score, '') IS NOT NULL".into(),
            format!("CAST(NULLIF(quality_score, '') AS REAL) < {}", quality),
        ),
        (
            "加算取得数",
            "NULLIF(kasan_count, '') IS NOT NULL".into(),
            format!("CAST(NULLIF(kasan_count, '') AS REAL) < {}", kasan),
        ),
    ];

    // (スコープ名, 追加WHERE, バインド値)
    let scopes: Vec<(&str, &str, Vec<libsql::Value>)> = vec![
        ("national", "", vec![]),
        ("prefecture", "prefecture = ?1", vec![libsql::Value::Text(pref.clone())]),
        ("service", "\"サービス名\" = ?1", vec![libsql::Value::Text(service_name.clone())]),
    ];

    // 指標ごとに個別のクエリを投げる。
    // 8指標を1クエリで集計すると全行を読む必要がありインデックスが効かず、実測 73 秒だった。
    //
    // さらに facilities の 従業者_合計 / 定員 / quality_score / kasan_count は TEXT 型で、
    // CAST を挟むと索引の範囲スキャンが効かない（指標ごとに分けても 47 秒）。
    // 数値化済みの facility_metrics があればそちらを引く。
    let use_metrics = table_exists(db, "facility_metrics").await;

    // (指標名, facility_metrics の列, 対象施設の値, 大小の向き)
    // 定着率だけは「離職率が高い施設が下位」なので比較を反転する
    let metric_cols: Vec<(&str, &str, f64, bool)> = vec![
        ("従業者数", "staff", staff, true),
        ("定員", "capacity", capacity, true),
        ("定着率", "turnover", turnover, false),
        ("常勤比率", "fulltime", fulltime, true),
        ("事業年数", "years", years, true),
        ("稼働率", "occupancy", occupancy, true),
        ("品質スコア", "quality", quality, true),
        ("加算取得数", "kasan", kasan, true),
    ];

    let mut percentiles = serde_json::Map::new();
    for (scope, scope_cond, bind) in scopes {
        let mut m = serde_json::Map::new();

        if use_metrics {
            // Turso はリモート実行なので往復回数が効く（1往復 実測 約0.066秒）。
            // 指標ごとに 24 クエリへ分けると 47 秒かかった。
            // 主因は当時インデックスが効かず各クエリ自体が重かったことだが、
            // 往復自体も 24 回で約 1.6 秒積み上がるため、
            // スコープ単位の1クエリにまとめる（facility_metrics は 11 列と軽い）。
            let cond = scope_cond.replace("\"サービス名\"", "service_name");
            let selects: Vec<String> = metric_cols
                .iter()
                .flat_map(|(_, col, val, asc)| {
                    let cmp = if *asc { "<" } else { ">" };
                    vec![
                        format!("SUM(CASE WHEN {c} IS NOT NULL THEN 1 ELSE 0 END)", c = col),
                        format!(
                            "SUM(CASE WHEN {c} IS NOT NULL AND {c} {cmp} {v} THEN 1 ELSE 0 END)",
                            c = col, cmp = cmp, v = val
                        ),
                    ]
                })
                .collect();
            let sql = format!(
                "SELECT {} FROM facility_metrics {}",
                selects.join(", "),
                if cond.is_empty() { String::new() } else { format!("WHERE {}", cond) }
            );
            if let Ok(r) = query_single_row_params(&conn, &sql, bind.clone()).await {
                for (i, (name, _, _, _)) in metric_cols.iter().enumerate() {
                    let total = row_i64(&r, (i * 2) as i32);
                    let below = row_i64(&r, (i * 2 + 1) as i32);
                    if total > 0 {
                        let pct = (below as f64 / total as f64 * 1000.0).round() / 10.0;
                        m.insert(name.to_string(), json!(pct));
                    }
                }
            }
        } else {
            for (name, valid, less) in &pct_exprs {
                let where_sql = if scope_cond.is_empty() {
                    format!("WHERE {}", valid)
                } else {
                    format!("WHERE {} AND {}", scope_cond, valid)
                };
                let sql = format!(
                    "SELECT COUNT(*), SUM(CASE WHEN {} THEN 1 ELSE 0 END) FROM facilities {}",
                    less, where_sql
                );
                if let Ok(r) = query_single_row_params(&conn, &sql, bind.clone()).await {
                    let total = row_i64(&r, 0);
                    if total > 0 {
                        let pct = (row_i64(&r, 1) as f64 / total as f64 * 1000.0).round() / 10.0;
                        m.insert(name.to_string(), json!(pct));
                    }
                }
            }
        }
        percentiles.insert(scope.to_string(), Value::Object(m));
    }

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
        "percentiles": percentiles,
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
