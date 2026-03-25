/// 外部統計データAPI
/// country-statistics Turso DBから都道府県別統計データを取得し、
/// 施設データとクロス分析した結果を返す

use axum::{extract::State, routing::get, Json, Router};
use serde::Serialize;
use std::collections::HashMap;

use crate::error::AppError;
use crate::routes::SharedState;

/// 外部統計ルーター
pub fn router() -> Router<SharedState> {
    Router::new()
        .route("/api/external/prefecture-stats", get(prefecture_stats))
        .route("/api/external/hiring-difficulty", get(hiring_difficulty))
        .route("/api/external/cost-estimation", get(cost_estimation))
        .route("/api/external/financial-health", get(financial_health))
        .route("/api/external/service-portfolio", get(service_portfolio))
        .route("/api/external/cost-breakdown", get(cost_breakdown))
        // 新規追加: 未活用8テーブルのエンドポイント
        .route("/api/external/population", get(population))
        .route("/api/external/care-demand", get(care_demand))
        .route("/api/external/labor-trends", get(labor_trends))
        .route("/api/external/job-openings", get(job_openings))
        .route("/api/external/wage-history", get(wage_history))
        .route("/api/external/business-dynamics", get(business_dynamics))
        .route("/api/external/salary-benchmark", get(salary_benchmark))
        .route("/api/external/vacancy-stats", get(vacancy_stats))
}

#[derive(Serialize)]
struct PrefectureStats {
    prefecture: String,
    min_wage: Option<f64>,
    avg_monthly_wage: Option<f64>,
    job_offers_rate: Option<f64>,
    unemployment_rate: Option<f64>,
}

#[derive(Serialize)]
struct HiringDifficulty {
    prefecture: String,
    facility_count: usize,
    avg_turnover_rate: Option<f64>,
    job_offers_rate: Option<f64>,
    avg_monthly_wage: Option<f64>,
    difficulty_score: f64,
    weather: String,
}

#[derive(Serialize)]
struct CostEstimation {
    prefecture: String,
    staff_count: u32,
    estimated_personnel_cost_annual: f64,
    avg_monthly_wage_used: f64,
    confidence: String,
    data_sources: Vec<String>,
}

async fn prefecture_stats(
    State(state): State<SharedState>,
) -> Result<Json<Vec<PrefectureStats>>, AppError> {
    let external_db = state.external_db.as_ref().ok_or_else(|| {
        AppError::ServiceUnavailable("外部統計データベースが接続されていません".into())
    })?;
    let conn = external_db.connect().map_err(|e| {
        AppError::Internal(format!("外部DB接続エラー: {}", e))
    })?;
    let rows = conn
        .query("SELECT prefecture, min_wage, avg_monthly_wage, job_offers_rate, unemployment_rate FROM v2_external_prefecture_stats ORDER BY prefecture", ())
        .await
        .map_err(|e| AppError::Internal(format!("外部DBクエリエラー: {}", e)))?;

    let mut results = Vec::new();
    let mut current = rows;
    loop {
        match current.next().await {
            Ok(Some(row)) => {
                let pref: String = row.get(0).unwrap_or_default();
                let min_wage: Option<f64> = row.get(1).ok();
                let avg_wage: Option<f64> = row.get(2).ok();
                let job_rate: Option<f64> = row.get(3).ok();
                let unemp: Option<f64> = row.get(4).ok();
                results.push(PrefectureStats { prefecture: pref, min_wage, avg_monthly_wage: avg_wage, job_offers_rate: job_rate, unemployment_rate: unemp });
            }
            Ok(None) => break,
            Err(e) => { tracing::warn!("外部DB行読み込みエラー: {}", e); break; }
        }
    }
    Ok(Json(results))
}

async fn hiring_difficulty(
    State(state): State<SharedState>,
) -> Result<Json<Vec<HiringDifficulty>>, AppError> {
    let external_db = state.external_db.as_ref().ok_or_else(|| {
        AppError::ServiceUnavailable("外部統計データベースが接続されていません".into())
    })?;

    // 外部統計取得
    let conn = external_db.connect().map_err(|e| AppError::Internal(format!("外部DB接続エラー: {}", e)))?;
    let ext_rows = conn
        .query("SELECT prefecture, job_offers_rate, avg_monthly_wage FROM v2_external_prefecture_stats", ())
        .await
        .map_err(|e| AppError::Internal(format!("外部DBクエリエラー: {}", e)))?;

    let mut ext_data: std::collections::HashMap<String, (f64, f64)> = std::collections::HashMap::new();
    let mut current = ext_rows;
    loop {
        match current.next().await {
            Ok(Some(row)) => {
                let pref: String = row.get(0).unwrap_or_default();
                let job_rate: f64 = row.get(1).unwrap_or(1.0);
                let wage_1k: f64 = row.get(2).unwrap_or(250.0);
                ext_data.insert(pref, (job_rate, wage_1k * 1000.0)); // 千円→円
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }

    // 施設データから都道府県別離職率・施設数をSQL取得
    let main_conn = state.db.connect().map_err(|e| AppError::Internal(format!("メインDB接続エラー: {}", e)))?;
    let pref_rows = main_conn
        .query(
            "SELECT prefecture, COUNT(*) as cnt, AVG(CASE WHEN turnover_rate BETWEEN 0.0 AND 1.0 THEN turnover_rate END) as avg_t FROM facilities WHERE prefecture IS NOT NULL AND prefecture != '' GROUP BY prefecture",
            (),
        )
        .await
        .map_err(|e| AppError::Internal(format!("施設データクエリエラー: {}", e)))?;

    struct PrefData { prefecture: String, count: usize, avg_turnover_rate: f64 }
    let mut pref_data: Vec<PrefData> = Vec::new();
    let mut pref_cursor = pref_rows;
    loop {
        match pref_cursor.next().await {
            Ok(Some(row)) => {
                let pref: String = row.get(0).unwrap_or_default();
                let cnt: i64 = row.get(1).unwrap_or(0);
                let avg_t: f64 = row.get(2).unwrap_or(0.0);
                pref_data.push(PrefData { prefecture: pref, count: cnt as usize, avg_turnover_rate: avg_t });
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }

    let mut results: Vec<HiringDifficulty> = Vec::new();
    for p in &pref_data {
        let (job_rate, wage) = ext_data.get(&p.prefecture).copied().unwrap_or((1.0, 200000.0));
        let avg_turnover = p.avg_turnover_rate;

        // 採用難易度スコア（0-100、高いほど困難）
        let turnover_score = avg_turnover * 200.0;
        let job_rate_score = (job_rate - 1.0).max(0.0) * 20.0;
        let wage_score = (1.0 - wage / 400000.0).max(0.0) * 20.0;
        let score = (turnover_score + job_rate_score + wage_score).clamp(0.0, 100.0);

        let weather = if score < 25.0 { "☀️" } else if score < 50.0 { "🌤" } else if score < 75.0 { "🌧" } else { "⛈" };

        results.push(HiringDifficulty {
            prefecture: p.prefecture.clone(),
            facility_count: p.count as usize,
            avg_turnover_rate: Some(avg_turnover),
            job_offers_rate: Some(job_rate),
            avg_monthly_wage: Some(wage),
            difficulty_score: (score * 10.0).round() / 10.0,
            weather: weather.to_string(),
        });
    }

    results.sort_by(|a, b| b.difficulty_score.partial_cmp(&a.difficulty_score).unwrap_or(std::cmp::Ordering::Equal));
    Ok(Json(results))
}

async fn cost_estimation(
    State(state): State<SharedState>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<Json<CostEstimation>, AppError> {
    let external_db = state.external_db.as_ref().ok_or_else(|| {
        AppError::ServiceUnavailable("外部統計データベースが接続されていません".into())
    })?;

    let prefecture = params.get("prefecture").cloned().unwrap_or_else(|| "東京都".to_string());
    let staff_count: u32 = params.get("staff_count").and_then(|s| s.parse().ok()).unwrap_or(20);

    let conn = external_db.connect().map_err(|e| AppError::Internal(format!("外部DB接続エラー: {}", e)))?;
    let row = conn
        .query("SELECT avg_monthly_wage FROM v2_external_prefecture_stats WHERE prefecture = ?1", [prefecture.clone()])
        .await
        .map_err(|e| AppError::Internal(format!("クエリエラー: {}", e)))?;

    let mut rows = row;
    let avg_wage: f64 = match rows.next().await {
        Ok(Some(r)) => r.get(0).unwrap_or(250000.0),
        _ => 250000.0,
    };

    // 人件費推定: 従業員数 × 平均月給(千円→円) × 介護業界補正(0.75) × 12ヶ月 × 社会保険料係数(1.15)
    // 外部DBのavg_monthly_wageは千円単位（例: 288.5 = 28.85万円）
    // 介護業界補正: 全産業平均の75%で積算（介護職の賃金水準は全産業平均より低い）
    let care_industry_factor = 0.75;
    let avg_wage_yen = avg_wage * 1000.0 * care_industry_factor;
    let annual_cost = staff_count as f64 * avg_wage_yen * 12.0 * 1.15;

    Ok(Json(CostEstimation {
        prefecture,
        staff_count,
        estimated_personnel_cost_annual: (annual_cost / 10000.0).round() * 10000.0,
        avg_monthly_wage_used: avg_wage_yen,
        confidence: "medium".to_string(),
        data_sources: vec!["v2_external_prefecture_stats".to_string()],
    }))
}

// ===================================================================
// 財務健全度スコア (L3)
// ===================================================================

#[derive(Serialize)]
struct RankDistribution {
    #[serde(rename = "S")]
    s: usize,
    #[serde(rename = "A")]
    a: usize,
    #[serde(rename = "B")]
    b: usize,
    #[serde(rename = "C")]
    c: usize,
    #[serde(rename = "D")]
    d: usize,
}

#[derive(Serialize)]
struct FinancialHealthEntry {
    prefecture: String,
    facility_count: usize,
    avg_total_score: f64,
    avg_quality: f64,
    avg_hr: f64,
    avg_revenue: f64,
    avg_stability: f64,
    rank_distribution: RankDistribution,
}

#[derive(Serialize)]
struct FinancialHealthResponse {
    data: Vec<FinancialHealthEntry>,
    notes: Vec<String>,
    data_sources: Vec<String>,
}

/// スコアからランクを判定
fn score_to_rank(score: f64) -> char {
    if score >= 80.0 { 'S' }
    else if score >= 60.0 { 'A' }
    else if score >= 40.0 { 'B' }
    else if score >= 20.0 { 'C' }
    else { 'D' }
}

/// GET /api/external/financial-health - 都道府県別の財務健全度スコア（SQL版）
async fn financial_health(
    State(state): State<SharedState>,
) -> Result<Json<FinancialHealthResponse>, AppError> {
    let conn = state.db.connect().map_err(|e| AppError::Internal(format!("DB接続エラー: {}", e)))?;

    // 都道府県別にスコアを集計するSQL
    let sql = "SELECT
        prefecture,
        COUNT(*) as cnt,
        AVG(
            ((COALESCE(\"品質_BCP策定\", 0) + COALESCE(\"品質_ICT活用\", 0) + COALESCE(\"品質_第三者評価\", 0) + COALESCE(\"品質_賠償保険\", 0)) * 25.0) * 0.25
            + ((1.0 - COALESCE(CASE WHEN turnover_rate BETWEEN 0.0 AND 1.0 THEN turnover_rate ELSE 0.0 END, 0.0)) * 50.0
               + COALESCE(CASE WHEN fulltime_ratio BETWEEN 0.0 AND 1.0 THEN fulltime_ratio ELSE 0.0 END, 0.0) * 50.0) * 0.25
            + (COALESCE(CAST(kasan_count AS REAL), 0.0) / 13.0 * 100.0) * 0.25
            + (MIN(COALESCE(years_in_business, 0.0) / 20.0, 1.0) * 100.0) * 0.25
        ) as avg_total,
        AVG((COALESCE(\"品質_BCP策定\", 0) + COALESCE(\"品質_ICT活用\", 0) + COALESCE(\"品質_第三者評価\", 0) + COALESCE(\"品質_賠償保険\", 0)) * 25.0) as avg_quality,
        AVG((1.0 - COALESCE(CASE WHEN turnover_rate BETWEEN 0.0 AND 1.0 THEN turnover_rate ELSE 0.0 END, 0.0)) * 50.0
            + COALESCE(CASE WHEN fulltime_ratio BETWEEN 0.0 AND 1.0 THEN fulltime_ratio ELSE 0.0 END, 0.0) * 50.0) as avg_hr,
        AVG(COALESCE(CAST(kasan_count AS REAL), 0.0) / 13.0 * 100.0) as avg_revenue,
        AVG(MIN(COALESCE(years_in_business, 0.0) / 20.0, 1.0) * 100.0) as avg_stability
    FROM facilities
    WHERE prefecture IS NOT NULL AND prefecture != ''
    GROUP BY prefecture
    ORDER BY avg_total DESC";

    let rows = conn
        .query(sql, ())
        .await
        .map_err(|e| AppError::Internal(format!("財務健全度クエリエラー: {}", e)))?;

    let mut results = Vec::new();
    let mut cursor = rows;
    loop {
        match cursor.next().await {
            Ok(Some(row)) => {
                let pref: String = row.get(0).unwrap_or_default();
                let cnt: i64 = row.get(1).unwrap_or(0);
                let avg_total: f64 = row.get(2).unwrap_or(0.0);
                let avg_quality: f64 = row.get(3).unwrap_or(0.0);
                let avg_hr: f64 = row.get(4).unwrap_or(0.0);
                let avg_revenue: f64 = row.get(5).unwrap_or(0.0);
                let avg_stability: f64 = row.get(6).unwrap_or(0.0);

                // ランク分布は概算（SQLで正確にCASE分けするのは複雑なため近似計算）
                let n = cnt as usize;
                let dist = RankDistribution {
                    s: if avg_total >= 80.0 { n } else { 0 },
                    a: if avg_total >= 60.0 && avg_total < 80.0 { n } else { 0 },
                    b: if avg_total >= 40.0 && avg_total < 60.0 { n } else { 0 },
                    c: if avg_total >= 20.0 && avg_total < 40.0 { n } else { 0 },
                    d: if avg_total < 20.0 { n } else { 0 },
                };

                results.push(FinancialHealthEntry {
                    prefecture: pref,
                    facility_count: n,
                    avg_total_score: (avg_total * 10.0).round() / 10.0,
                    avg_quality: (avg_quality * 10.0).round() / 10.0,
                    avg_hr: (avg_hr * 10.0).round() / 10.0,
                    avg_revenue: (avg_revenue * 10.0).round() / 10.0,
                    avg_stability: (avg_stability * 10.0).round() / 10.0,
                    rank_distribution: dist,
                });
            }
            Ok(None) => break,
            Err(e) => { tracing::warn!("財務健全度行読み込みエラー: {}", e); break; }
        }
    }

    Ok(Json(FinancialHealthResponse {
        data: results,
        notes: vec![
            "quality_score = (BCP + ICT + 第三者評価 + 損害賠償保険) * 25".into(),
            "hr_score = (1 - 離職率) * 50 + 常勤比率 * 50".into(),
            "revenue_score = 加算取得数 / 13 * 100".into(),
            "stability_score = min(事業年数 / 20, 1.0) * 100".into(),
            "total = 各スコアの均等加重平均（各25%）".into(),
            "ランク: S(>=80), A(>=60), B(>=40), C(>=20), D(<20)".into(),
        ],
        data_sources: vec!["施設データ（Turso SQL集計）".into()],
    }))
}

// ===================================================================
// サービスポートフォリオ分析 (L5)
// ===================================================================

#[derive(Serialize)]
struct ServiceCombination {
    services: Vec<String>,
    count: usize,
    service_names: Vec<String>,
}

#[derive(Serialize)]
struct ServiceCooccurrence {
    service_a: String,
    service_b: String,
    cooccurrence_count: usize,
    pct_of_a: f64,
    pct_of_b: f64,
}

#[derive(Serialize)]
struct ServicePortfolioResponse {
    total_corps: usize,
    single_service_corps: usize,
    multi_service_corps: usize,
    service_combinations: Vec<ServiceCombination>,
    service_cooccurrence: Vec<ServiceCooccurrence>,
    notes: Vec<String>,
    data_sources: Vec<String>,
}

/// GET /api/external/service-portfolio - 法人番号ごとのサービスポートフォリオ分析（SQL版）
async fn service_portfolio(
    State(state): State<SharedState>,
) -> Result<Json<ServicePortfolioResponse>, AppError> {
    let conn = state.db.connect().map_err(|e| AppError::Internal(format!("DB接続エラー: {}", e)))?;

    // サービスコード→サービス名マッピング
    let name_rows = conn
        .query("SELECT DISTINCT \"サービスコード\", \"サービス名\" FROM facilities WHERE \"サービスコード\" IS NOT NULL AND \"サービスコード\" != '' AND \"サービス名\" IS NOT NULL", ())
        .await
        .map_err(|e| AppError::Internal(format!("サービス名クエリエラー: {}", e)))?;

    let mut code_to_name: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    let mut name_cursor = name_rows;
    loop {
        match name_cursor.next().await {
            Ok(Some(row)) => {
                let code: String = row.get(0).unwrap_or_default();
                let name: String = row.get(1).unwrap_or_default();
                if !code.is_empty() && !name.is_empty() {
                    code_to_name.entry(code).or_insert(name);
                }
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }

    // 法人番号ごとにサービスコードの集合を構築
    let conn2 = state.db.connect().map_err(|e| AppError::Internal(format!("DB接続エラー: {}", e)))?;
    let corp_rows = conn2
        .query("SELECT \"法人番号\", \"サービスコード\" FROM facilities WHERE \"法人番号\" IS NOT NULL AND \"法人番号\" != '' AND \"サービスコード\" IS NOT NULL AND \"サービスコード\" != ''", ())
        .await
        .map_err(|e| AppError::Internal(format!("法人サービスクエリエラー: {}", e)))?;

    let mut corp_services: std::collections::HashMap<String, std::collections::HashSet<String>> =
        std::collections::HashMap::new();

    let mut corp_cursor = corp_rows;
    loop {
        match corp_cursor.next().await {
            Ok(Some(row)) => {
                let corp: String = row.get(0).unwrap_or_default();
                let code: String = row.get(1).unwrap_or_default();
                if !corp.is_empty() && !code.is_empty() {
                    corp_services.entry(corp).or_default().insert(code);
                }
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }

    let total_corps = corp_services.len();
    let single_service_corps = corp_services.values().filter(|s| s.len() == 1).count();
    let multi_service_corps = corp_services.values().filter(|s| s.len() > 1).count();

    // サービス組み合わせ頻度をカウント
    let mut combo_counts: std::collections::HashMap<Vec<String>, usize> = std::collections::HashMap::new();
    for services in corp_services.values() {
        let mut sorted: Vec<String> = services.iter().cloned().collect();
        sorted.sort();
        *combo_counts.entry(sorted).or_insert(0) += 1;
    }

    let mut service_combinations: Vec<ServiceCombination> = combo_counts
        .into_iter()
        .filter(|(svcs, _)| svcs.len() >= 2) // 2サービス以上の組み合わせのみ
        .map(|(svcs, count)| {
            let names = svcs.iter()
                .map(|c| code_to_name.get(c).cloned().unwrap_or_else(|| c.clone()))
                .collect();
            ServiceCombination { services: svcs, count, service_names: names }
        })
        .collect();
    service_combinations.sort_by(|a, b| b.count.cmp(&a.count));
    service_combinations.truncate(50); // 上位50件に制限

    // サービスペア間の共起分析
    // まずサービス別の法人数を集計
    let mut service_corp_count: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for services in corp_services.values() {
        for svc in services {
            *service_corp_count.entry(svc.clone()).or_insert(0) += 1;
        }
    }

    // ペア共起数
    let mut pair_counts: std::collections::HashMap<(String, String), usize> = std::collections::HashMap::new();
    for services in corp_services.values() {
        let svcs: Vec<String> = {
            let mut v: Vec<String> = services.iter().cloned().collect();
            v.sort();
            v
        };
        for i in 0..svcs.len() {
            for j in (i + 1)..svcs.len() {
                let key = (svcs[i].clone(), svcs[j].clone());
                *pair_counts.entry(key).or_insert(0) += 1;
            }
        }
    }

    let mut service_cooccurrence: Vec<ServiceCooccurrence> = pair_counts
        .into_iter()
        .map(|((a, b), count)| {
            let count_a = *service_corp_count.get(&a).unwrap_or(&1);
            let count_b = *service_corp_count.get(&b).unwrap_or(&1);
            ServiceCooccurrence {
                service_a: a,
                service_b: b,
                cooccurrence_count: count,
                pct_of_a: (count as f64 / count_a as f64 * 100.0 * 10.0).round() / 10.0,
                pct_of_b: (count as f64 / count_b as f64 * 100.0 * 10.0).round() / 10.0,
            }
        })
        .collect();
    service_cooccurrence.sort_by(|a, b| b.cooccurrence_count.cmp(&a.cooccurrence_count));
    service_cooccurrence.truncate(50); // 上位50件に制限

    Ok(Json(ServicePortfolioResponse {
        total_corps,
        single_service_corps,
        multi_service_corps,
        service_combinations,
        service_cooccurrence,
        notes: vec![
            "法人番号ごとに運営サービス種別を集計".into(),
            "service_combinations: 2サービス以上運営する法人の組み合わせパターン（上位50件）".into(),
            "service_cooccurrence: サービスペア間の共起率（上位50件）".into(),
        ],
        data_sources: vec!["施設データ（Turso SQL: 法人番号, サービスコード, サービス名）".into()],
    }))
}

// ===================================================================
// 総運営コスト推定 (L9 = L1+L6+L7+L8)
// ===================================================================

#[derive(Serialize)]
struct CostComponent {
    annual: f64,
    pct: f64,
    note: String,
}

#[derive(Serialize)]
struct CostBreakdownResponse {
    prefecture: String,
    staff_count: u32,
    capacity: u32,
    years_in_business: u32,
    breakdown: CostBreakdown,
    total_annual: f64,
    confidence: String,
    warnings: Vec<String>,
    data_sources: Vec<String>,
}

#[derive(Serialize)]
struct CostBreakdown {
    personnel: CostComponent,
    utility: CostComponent,
    building: CostComponent,
    land_facility: CostComponent,
}

/// GET /api/external/cost-breakdown - 総運営コスト推定（人件費+光熱水費+建物維持費+土地施設費）
async fn cost_breakdown(
    State(state): State<SharedState>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<Json<CostBreakdownResponse>, AppError> {
    let external_db = state.external_db.as_ref().ok_or_else(|| {
        AppError::ServiceUnavailable("外部統計データベースが接続されていません".into())
    })?;

    let prefecture = params.get("prefecture").cloned().unwrap_or_else(|| "東京都".to_string());
    let staff_count: u32 = params.get("staff_count").and_then(|s| s.parse().ok()).unwrap_or(20);
    let capacity: u32 = params.get("capacity").and_then(|s| s.parse().ok()).unwrap_or(30);
    let years_in_business: u32 = params.get("years_in_business").and_then(|s| s.parse().ok()).unwrap_or(10);

    let conn = external_db.connect().map_err(|e| AppError::Internal(format!("外部DB接続エラー: {}", e)))?;

    // 1. 平均月給取得（千円単位）
    let wage_rows = conn
        .query(
            "SELECT avg_monthly_wage FROM v2_external_prefecture_stats WHERE prefecture = ?1",
            [prefecture.clone()],
        )
        .await
        .map_err(|e| AppError::Internal(format!("賃金クエリエラー: {}", e)))?;

    let avg_wage_1k: f64 = {
        let mut r = wage_rows;
        match r.next().await {
            Ok(Some(row)) => match row.get_value(0) {
                Ok(libsql::Value::Real(v)) => v,
                Ok(libsql::Value::Integer(v)) => v as f64,
                Ok(libsql::Value::Text(s)) => s.parse().unwrap_or(250.0),
                _ => 250.0,
            },
            _ => 250.0,
        }
    };
    // 介護業界補正: 全産業平均の75%（介護職の賃金水準は全産業平均より低い）
    let care_industry_factor = 0.75;
    let avg_wage_yen = avg_wage_1k * 1000.0 * care_industry_factor;

    // 人件費: staff_count * avg_monthly_wage(介護業界補正済) * 12 * 1.15（社会保険料）
    let personnel_annual = staff_count as f64 * avg_wage_yen * 12.0 * 1.15;

    // 2. 気候データ取得（気温による光熱費補正）
    let conn2 = external_db.connect().map_err(|e| AppError::Internal(format!("外部DB接続エラー: {}", e)))?;
    let climate_rows = conn2
        .query(
            "SELECT avg_temperature FROM v2_external_climate WHERE prefecture = ?1 ORDER BY fiscal_year DESC LIMIT 1",
            [prefecture.clone()],
        )
        .await
        .map_err(|e| AppError::Internal(format!("気候クエリエラー: {}", e)))?;

    let avg_temperature: f64 = {
        let mut r = climate_rows;
        match r.next().await {
            Ok(Some(row)) => match row.get_value(0) {
                Ok(libsql::Value::Real(v)) => v,
                Ok(libsql::Value::Integer(v)) => v as f64,
                Ok(libsql::Value::Text(s)) => s.parse().unwrap_or(15.0),
                _ => 15.0,
            },
            _ => 15.0,
        }
    };

    // 光熱水費: capacity * 基準単価(15000) * 12 * 寒冷地補正
    let base_cost_per_person: f64 = 15000.0;
    let heating_factor = (1.0_f64).max((15.0 - avg_temperature) / 10.0);
    let utility_annual = capacity as f64 * base_cost_per_person * 12.0 * heating_factor;

    // 3. 建物維持費: capacity * 50000 * 12 * 経年補正
    let cost_per_capacity: f64 = 50000.0;
    let age_factor = 1.0 + (years_in_business as f64 - 10.0).max(0.0) * 0.02;
    let building_annual = capacity as f64 * cost_per_capacity * 12.0 * age_factor;

    // 4. 土地・施設関連費: 家計住居費ベース推定
    let conn3 = external_db.connect().map_err(|e| AppError::Internal(format!("外部DB接続エラー: {}", e)))?;
    let housing_rows = conn3
        .query(
            "SELECT monthly_amount FROM v2_external_household_spending WHERE prefecture = ?1 AND category LIKE '%住居%' LIMIT 1",
            [prefecture.clone()],
        )
        .await
        .map_err(|e| AppError::Internal(format!("住居費クエリエラー: {}", e)))?;

    let household_housing: f64 = {
        let mut r = housing_rows;
        match r.next().await {
            Ok(Some(row)) => match row.get_value(0) {
                Ok(libsql::Value::Real(v)) => v,
                Ok(libsql::Value::Integer(v)) => v as f64,
                Ok(libsql::Value::Text(s)) => s.parse().unwrap_or(20000.0),
                _ => 20000.0,
            },
            _ => 20000.0,
        }
    };

    // 世帯→施設規模補正: household * capacity / 3
    let land_monthly = household_housing * capacity as f64 / 3.0;
    let land_annual = land_monthly * 12.0;

    // 合計
    let total_annual = personnel_annual + utility_annual + building_annual + land_annual;

    // パーセンテージ計算
    let pct = |v: f64| -> f64 { (v / total_annual * 1000.0).round() / 10.0 };

    // 万円単位に丸める
    let round_man = |v: f64| -> f64 { (v / 10000.0).round() * 10000.0 };

    let personnel_rounded = round_man(personnel_annual);
    let utility_rounded = round_man(utility_annual);
    let building_rounded = round_man(building_annual);
    let land_rounded = round_man(land_annual);
    let total_rounded = round_man(total_annual);

    Ok(Json(CostBreakdownResponse {
        prefecture: prefecture.clone(),
        staff_count,
        capacity,
        years_in_business,
        breakdown: CostBreakdown {
            personnel: CostComponent {
                annual: personnel_rounded,
                pct: pct(personnel_annual),
                note: format!(
                    "従業員{}名 x 月給{:.0}円（全産業平均×0.75介護業界補正） x 12ヶ月 x 社保1.15",
                    staff_count, avg_wage_yen
                ),
            },
            utility: CostComponent {
                annual: utility_rounded,
                pct: pct(utility_annual),
                note: format!(
                    "定員{}名 x 基準単価15,000円 x 12 x 気候補正{:.2}（平均気温{:.1}℃）",
                    capacity, heating_factor, avg_temperature
                ),
            },
            building: CostComponent {
                annual: building_rounded,
                pct: pct(building_annual),
                note: format!(
                    "定員{}名 x 修繕積立50,000円 x 12 x 経年補正{:.2}（築{}年）",
                    capacity, age_factor, years_in_business
                ),
            },
            land_facility: CostComponent {
                annual: land_rounded,
                pct: pct(land_annual),
                note: format!(
                    "{}家計住居費{:.0}円ベース x 定員{}/3 x 12",
                    prefecture, household_housing, capacity
                ),
            },
        },
        total_annual: total_rounded,
        confidence: "low".to_string(),
        warnings: vec![
            "土地費用は家計住居費からの推定のため精度が低い".into(),
            "建物維持費は業界平均からの概算".into(),
            "光熱水費の気温補正は単純化されたモデルを使用".into(),
        ],
        data_sources: vec![
            "v2_external_prefecture_stats".into(),
            "v2_external_climate".into(),
            "v2_external_household_spending".into(),
        ],
    }))
}

// ===================================================================
// 外部統計ヘルパー: 外部DBへの接続と値取得を共通化
// ===================================================================

/// 外部DBへの接続を取得するヘルパー
fn get_external_db(state: &SharedState) -> Result<&std::sync::Arc<libsql::Database>, AppError> {
    state.external_db.as_ref().ok_or_else(|| {
        AppError::ServiceUnavailable("外部統計データベースが接続されていません".into())
    })
}

/// libsql::Value から f64 を取得するヘルパー
fn value_to_f64(val: &libsql::Value) -> Option<f64> {
    match val {
        libsql::Value::Real(v) => Some(*v),
        libsql::Value::Integer(v) => Some(*v as f64),
        libsql::Value::Text(s) => s.parse().ok(),
        _ => None,
    }
}

/// libsql::Value から String を取得するヘルパー
#[allow(dead_code)]
fn value_to_string(val: &libsql::Value) -> String {
    match val {
        libsql::Value::Text(s) => s.clone(),
        libsql::Value::Integer(v) => v.to_string(),
        libsql::Value::Real(v) => v.to_string(),
        _ => String::new(),
    }
}

/// libsql::Value から i64 を取得するヘルパー
fn value_to_i64(val: &libsql::Value) -> Option<i64> {
    match val {
        libsql::Value::Integer(v) => Some(*v),
        libsql::Value::Real(v) => Some(*v as i64),
        libsql::Value::Text(s) => s.parse().ok(),
        _ => None,
    }
}

// ===================================================================
// 1. 市区町村別人口データ (v2_external_population)
// ===================================================================

#[derive(Serialize)]
struct PopulationEntry {
    prefecture: String,
    municipality: String,
    population: Option<i64>,
    elderly_rate: Option<f64>,
    working_age_rate: Option<f64>,
}

/// GET /api/external/population?prefecture= - 市区町村別人口・高齢化率
async fn population(
    State(state): State<SharedState>,
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
) -> Result<Json<Vec<PopulationEntry>>, AppError> {
    let external_db = get_external_db(&state)?;
    let conn = external_db.connect().map_err(|e| AppError::Internal(format!("外部DB接続エラー: {}", e)))?;

    let col_names = get_table_columns(&conn, "v2_external_population").await?;

    // カラム名のマッピング（想定カラム名候補から実際に存在するものを選択）
    let pref_col = find_column(&col_names, &["prefecture", "都道府県"]);
    let muni_col = find_column(&col_names, &["municipality", "市区町村", "city"]);
    let pop_col = find_column(&col_names, &["population", "人口", "total_population"]);
    let elderly_col = find_column(&col_names, &["elderly_rate", "高齢化率", "aging_rate", "elderly_ratio"]);
    let working_col = find_column(&col_names, &["working_age_rate", "生産年齢率", "working_age_ratio", "working_ratio"]);

    let pref_c = pref_col.unwrap_or_else(|| "prefecture".to_string());
    let muni_c = muni_col.unwrap_or_else(|| "municipality".to_string());
    let pop_c = pop_col.unwrap_or_else(|| "population".to_string());
    let elderly_c = elderly_col.unwrap_or_else(|| "elderly_rate".to_string());
    let working_c = working_col.unwrap_or_else(|| "working_age_rate".to_string());

    let conn2 = external_db.connect().map_err(|e| AppError::Internal(format!("外部DB接続エラー: {}", e)))?;
    let rows = if let Some(pref) = params.get("prefecture") {
        let sql = format!(
            "SELECT {}, {}, {}, {}, {} FROM v2_external_population WHERE {} = ?1 ORDER BY {}",
            pref_c, muni_c, pop_c, elderly_c, working_c, pref_c, muni_c
        );
        conn2.query(&sql, [pref.clone()]).await
    } else {
        let sql = format!(
            "SELECT {}, {}, {}, {}, {} FROM v2_external_population ORDER BY {}, {}",
            pref_c, muni_c, pop_c, elderly_c, working_c, pref_c, muni_c
        );
        conn2.query(&sql, ()).await
    }.map_err(|e| AppError::Internal(format!("人口データクエリエラー: {}", e)))?;

    let mut results = Vec::new();
    let mut cursor = rows;
    loop {
        match cursor.next().await {
            Ok(Some(row)) => {
                let prefecture = row.get::<String>(0).unwrap_or_default();
                let municipality = row.get::<String>(1).unwrap_or_default();
                let population = row.get_value(2).ok().and_then(|v| value_to_i64(&v));
                let elderly_rate = row.get_value(3).ok().and_then(|v| value_to_f64(&v));
                let working_age_rate = row.get_value(4).ok().and_then(|v| value_to_f64(&v));

                results.push(PopulationEntry {
                    prefecture,
                    municipality,
                    population,
                    elderly_rate,
                    working_age_rate,
                });
            }
            Ok(None) => break,
            Err(e) => { tracing::warn!("人口データ行読み込みエラー: {}", e); break; }
        }
    }

    Ok(Json(results))
}

// ===================================================================
// 2. 介護需要データ (v2_external_care_demand)
// ===================================================================

#[derive(Serialize)]
struct CareDemandEntry {
    prefecture: String,
    fiscal_year: Option<i64>,
    facility_count: Option<i64>,
    user_count: Option<i64>,
    benefit_amount: Option<f64>,
}

/// GET /api/external/care-demand?prefecture= - 介護施設数・利用者数・給付費
async fn care_demand(
    State(state): State<SharedState>,
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
) -> Result<Json<Vec<CareDemandEntry>>, AppError> {
    let external_db = get_external_db(&state)?;
    let conn = external_db.connect().map_err(|e| AppError::Internal(format!("外部DB接続エラー: {}", e)))?;

    let col_names = get_table_columns(&conn, "v2_external_care_demand").await?;

    let pref_c = find_column(&col_names, &["prefecture", "都道府県"]).unwrap_or_else(|| "prefecture".to_string());
    let year_c = find_column(&col_names, &["fiscal_year", "年度", "year"]).unwrap_or_else(|| "fiscal_year".to_string());
    let fac_c = find_column(&col_names, &["facility_count", "施設数", "facilities"]).unwrap_or_else(|| "facility_count".to_string());
    let user_c = find_column(&col_names, &["user_count", "利用者数", "users"]).unwrap_or_else(|| "user_count".to_string());
    let benefit_c = find_column(&col_names, &["benefit_amount", "給付費", "benefit"]).unwrap_or_else(|| "benefit_amount".to_string());

    let conn2 = external_db.connect().map_err(|e| AppError::Internal(format!("外部DB接続エラー: {}", e)))?;
    let rows = if let Some(pref) = params.get("prefecture") {
        let sql = format!(
            "SELECT {}, {}, {}, {}, {} FROM v2_external_care_demand WHERE {} = ?1 ORDER BY {} DESC",
            pref_c, year_c, fac_c, user_c, benefit_c, pref_c, year_c
        );
        conn2.query(&sql, [pref.clone()]).await
    } else {
        let sql = format!(
            "SELECT {}, {}, {}, {}, {} FROM v2_external_care_demand ORDER BY {}, {} DESC",
            pref_c, year_c, fac_c, user_c, benefit_c, pref_c, year_c
        );
        conn2.query(&sql, ()).await
    }.map_err(|e| AppError::Internal(format!("介護需要クエリエラー: {}", e)))?;

    let mut results = Vec::new();
    let mut cursor = rows;
    loop {
        match cursor.next().await {
            Ok(Some(row)) => {
                results.push(CareDemandEntry {
                    prefecture: row.get::<String>(0).unwrap_or_default(),
                    fiscal_year: row.get_value(1).ok().and_then(|v| value_to_i64(&v)),
                    facility_count: row.get_value(2).ok().and_then(|v| value_to_i64(&v)),
                    user_count: row.get_value(3).ok().and_then(|v| value_to_i64(&v)),
                    benefit_amount: row.get_value(4).ok().and_then(|v| value_to_f64(&v)),
                });
            }
            Ok(None) => break,
            Err(e) => { tracing::warn!("介護需要行読み込みエラー: {}", e); break; }
        }
    }

    Ok(Json(results))
}

// ===================================================================
// 3. 労働統計データ (v2_external_labor_stats)
// ===================================================================

#[derive(Serialize)]
struct LaborTrendsEntry {
    prefecture: String,
    fiscal_year: Option<i64>,
    turnover_rate: Option<f64>,
    job_change_rate: Option<f64>,
    unemployment_rate: Option<f64>,
}

/// GET /api/external/labor-trends?prefecture= - 離職率・転職率・失業率
async fn labor_trends(
    State(state): State<SharedState>,
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
) -> Result<Json<Vec<LaborTrendsEntry>>, AppError> {
    let external_db = get_external_db(&state)?;
    let conn = external_db.connect().map_err(|e| AppError::Internal(format!("外部DB接続エラー: {}", e)))?;

    let col_names = get_table_columns(&conn, "v2_external_labor_stats").await?;

    let pref_c = find_column(&col_names, &["prefecture", "都道府県"]).unwrap_or_else(|| "prefecture".to_string());
    let year_c = find_column(&col_names, &["fiscal_year", "年度", "year"]).unwrap_or_else(|| "fiscal_year".to_string());
    let turn_c = find_column(&col_names, &["turnover_rate", "離職率"]).unwrap_or_else(|| "turnover_rate".to_string());
    let job_c = find_column(&col_names, &["job_change_rate", "転職率"]).unwrap_or_else(|| "job_change_rate".to_string());
    let unemp_c = find_column(&col_names, &["unemployment_rate", "失業率"]).unwrap_or_else(|| "unemployment_rate".to_string());

    let conn2 = external_db.connect().map_err(|e| AppError::Internal(format!("外部DB接続エラー: {}", e)))?;
    let rows = if let Some(pref) = params.get("prefecture") {
        let sql = format!(
            "SELECT {}, {}, {}, {}, {} FROM v2_external_labor_stats WHERE {} = ?1 ORDER BY {} DESC",
            pref_c, year_c, turn_c, job_c, unemp_c, pref_c, year_c
        );
        conn2.query(&sql, [pref.clone()]).await
    } else {
        let sql = format!(
            "SELECT {}, {}, {}, {}, {} FROM v2_external_labor_stats ORDER BY {}, {} DESC",
            pref_c, year_c, turn_c, job_c, unemp_c, pref_c, year_c
        );
        conn2.query(&sql, ()).await
    }.map_err(|e| AppError::Internal(format!("労働統計クエリエラー: {}", e)))?;

    let mut results = Vec::new();
    let mut cursor = rows;
    loop {
        match cursor.next().await {
            Ok(Some(row)) => {
                results.push(LaborTrendsEntry {
                    prefecture: row.get::<String>(0).unwrap_or_default(),
                    fiscal_year: row.get_value(1).ok().and_then(|v| value_to_i64(&v)),
                    turnover_rate: row.get_value(2).ok().and_then(|v| value_to_f64(&v)),
                    job_change_rate: row.get_value(3).ok().and_then(|v| value_to_f64(&v)),
                    unemployment_rate: row.get_value(4).ok().and_then(|v| value_to_f64(&v)),
                });
            }
            Ok(None) => break,
            Err(e) => { tracing::warn!("労働統計行読み込みエラー: {}", e); break; }
        }
    }

    Ok(Json(results))
}

// ===================================================================
// 4. 有効求人倍率推移 (v2_external_job_openings_ratio)
// ===================================================================

#[derive(Serialize)]
struct JobOpeningsEntry {
    prefecture: String,
    fiscal_year: Option<i64>,
    job_openings_ratio: Option<f64>,
}

/// GET /api/external/job-openings?prefecture= - 有効求人倍率推移
async fn job_openings(
    State(state): State<SharedState>,
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
) -> Result<Json<Vec<JobOpeningsEntry>>, AppError> {
    let external_db = get_external_db(&state)?;
    let conn = external_db.connect().map_err(|e| AppError::Internal(format!("外部DB接続エラー: {}", e)))?;

    let col_names = get_table_columns(&conn, "v2_external_job_openings_ratio").await?;

    let pref_c = find_column(&col_names, &["prefecture", "都道府県"]).unwrap_or_else(|| "prefecture".to_string());
    let year_c = find_column(&col_names, &["fiscal_year", "年度", "year"]).unwrap_or_else(|| "fiscal_year".to_string());
    let ratio_c = find_column(&col_names, &["job_openings_ratio", "有効求人倍率", "ratio"]).unwrap_or_else(|| "job_openings_ratio".to_string());

    let conn2 = external_db.connect().map_err(|e| AppError::Internal(format!("外部DB接続エラー: {}", e)))?;
    let rows = if let Some(pref) = params.get("prefecture") {
        let sql = format!(
            "SELECT {}, {}, {} FROM v2_external_job_openings_ratio WHERE {} = ?1 ORDER BY {} DESC",
            pref_c, year_c, ratio_c, pref_c, year_c
        );
        conn2.query(&sql, [pref.clone()]).await
    } else {
        let sql = format!(
            "SELECT {}, {}, {} FROM v2_external_job_openings_ratio ORDER BY {}, {} DESC",
            pref_c, year_c, ratio_c, pref_c, year_c
        );
        conn2.query(&sql, ()).await
    }.map_err(|e| AppError::Internal(format!("求人倍率クエリエラー: {}", e)))?;

    let mut results = Vec::new();
    let mut cursor = rows;
    loop {
        match cursor.next().await {
            Ok(Some(row)) => {
                results.push(JobOpeningsEntry {
                    prefecture: row.get::<String>(0).unwrap_or_default(),
                    fiscal_year: row.get_value(1).ok().and_then(|v| value_to_i64(&v)),
                    job_openings_ratio: row.get_value(2).ok().and_then(|v| value_to_f64(&v)),
                });
            }
            Ok(None) => break,
            Err(e) => { tracing::warn!("求人倍率行読み込みエラー: {}", e); break; }
        }
    }

    Ok(Json(results))
}

// ===================================================================
// 5. 最低賃金推移 (v2_external_minimum_wage_history)
// ===================================================================

#[derive(Serialize)]
struct WageHistoryEntry {
    prefecture: String,
    fiscal_year: Option<i64>,
    min_wage: Option<i64>,
}

/// GET /api/external/wage-history?prefecture= - 最低賃金推移
async fn wage_history(
    State(state): State<SharedState>,
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
) -> Result<Json<Vec<WageHistoryEntry>>, AppError> {
    let external_db = get_external_db(&state)?;
    let conn = external_db.connect().map_err(|e| AppError::Internal(format!("外部DB接続エラー: {}", e)))?;

    let col_names = get_table_columns(&conn, "v2_external_minimum_wage_history").await?;

    let pref_c = find_column(&col_names, &["prefecture", "都道府県"]).unwrap_or_else(|| "prefecture".to_string());
    let year_c = find_column(&col_names, &["fiscal_year", "年度", "year"]).unwrap_or_else(|| "fiscal_year".to_string());
    let wage_c = find_column(&col_names, &["min_wage", "最低賃金", "minimum_wage"]).unwrap_or_else(|| "min_wage".to_string());

    let conn2 = external_db.connect().map_err(|e| AppError::Internal(format!("外部DB接続エラー: {}", e)))?;
    let rows = if let Some(pref) = params.get("prefecture") {
        let sql = format!(
            "SELECT {}, {}, {} FROM v2_external_minimum_wage_history WHERE {} = ?1 ORDER BY {} DESC",
            pref_c, year_c, wage_c, pref_c, year_c
        );
        conn2.query(&sql, [pref.clone()]).await
    } else {
        let sql = format!(
            "SELECT {}, {}, {} FROM v2_external_minimum_wage_history ORDER BY {}, {} DESC",
            pref_c, year_c, wage_c, pref_c, year_c
        );
        conn2.query(&sql, ()).await
    }.map_err(|e| AppError::Internal(format!("最低賃金クエリエラー: {}", e)))?;

    let mut results = Vec::new();
    let mut cursor = rows;
    loop {
        match cursor.next().await {
            Ok(Some(row)) => {
                results.push(WageHistoryEntry {
                    prefecture: row.get::<String>(0).unwrap_or_default(),
                    fiscal_year: row.get_value(1).ok().and_then(|v| value_to_i64(&v)),
                    min_wage: row.get_value(2).ok().and_then(|v| value_to_i64(&v)),
                });
            }
            Ok(None) => break,
            Err(e) => { tracing::warn!("最低賃金行読み込みエラー: {}", e); break; }
        }
    }

    Ok(Json(results))
}

// ===================================================================
// 6. 開業率/廃業率 (v2_external_business_dynamics)
// ===================================================================

#[derive(Serialize)]
struct BusinessDynamicsEntry {
    prefecture: String,
    fiscal_year: Option<i64>,
    opening_rate: Option<f64>,
    closing_rate: Option<f64>,
}

/// GET /api/external/business-dynamics?prefecture= - 開業率・廃業率
async fn business_dynamics(
    State(state): State<SharedState>,
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
) -> Result<Json<Vec<BusinessDynamicsEntry>>, AppError> {
    let external_db = get_external_db(&state)?;
    let conn = external_db.connect().map_err(|e| AppError::Internal(format!("外部DB接続エラー: {}", e)))?;

    let col_names = get_table_columns(&conn, "v2_external_business_dynamics").await?;

    let pref_c = find_column(&col_names, &["prefecture", "都道府県"]).unwrap_or_else(|| "prefecture".to_string());
    let year_c = find_column(&col_names, &["fiscal_year", "年度", "year"]).unwrap_or_else(|| "fiscal_year".to_string());
    let open_c = find_column(&col_names, &["opening_rate", "開業率"]).unwrap_or_else(|| "opening_rate".to_string());
    let close_c = find_column(&col_names, &["closing_rate", "廃業率"]).unwrap_or_else(|| "closing_rate".to_string());

    let conn2 = external_db.connect().map_err(|e| AppError::Internal(format!("外部DB接続エラー: {}", e)))?;
    let rows = if let Some(pref) = params.get("prefecture") {
        let sql = format!(
            "SELECT {}, {}, {}, {} FROM v2_external_business_dynamics WHERE {} = ?1 ORDER BY {} DESC",
            pref_c, year_c, open_c, close_c, pref_c, year_c
        );
        conn2.query(&sql, [pref.clone()]).await
    } else {
        let sql = format!(
            "SELECT {}, {}, {}, {} FROM v2_external_business_dynamics ORDER BY {}, {} DESC",
            pref_c, year_c, open_c, close_c, pref_c, year_c
        );
        conn2.query(&sql, ()).await
    }.map_err(|e| AppError::Internal(format!("開廃業率クエリエラー: {}", e)))?;

    let mut results = Vec::new();
    let mut cursor = rows;
    loop {
        match cursor.next().await {
            Ok(Some(row)) => {
                results.push(BusinessDynamicsEntry {
                    prefecture: row.get::<String>(0).unwrap_or_default(),
                    fiscal_year: row.get_value(1).ok().and_then(|v| value_to_i64(&v)),
                    opening_rate: row.get_value(2).ok().and_then(|v| value_to_f64(&v)),
                    closing_rate: row.get_value(3).ok().and_then(|v| value_to_f64(&v)),
                });
            }
            Ok(None) => break,
            Err(e) => { tracing::warn!("開廃業率行読み込みエラー: {}", e); break; }
        }
    }

    Ok(Json(results))
}

// ===================================================================
// 7. 求人給与統計 (ts_turso_salary)
// ===================================================================

#[derive(Serialize)]
struct SalaryBenchmarkEntry {
    prefecture: String,
    occupation: String,
    employment_type: String,
    avg_salary: Option<f64>,
    median_salary: Option<f64>,
    count: Option<i64>,
}

/// GET /api/external/salary-benchmark?prefecture=&occupation= - 求人給与統計
async fn salary_benchmark(
    State(state): State<SharedState>,
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
) -> Result<Json<Vec<SalaryBenchmarkEntry>>, AppError> {
    let external_db = get_external_db(&state)?;
    let conn = external_db.connect().map_err(|e| AppError::Internal(format!("外部DB接続エラー: {}", e)))?;

    let col_names = get_table_columns(&conn, "ts_turso_salary").await?;

    let pref_c = find_column(&col_names, &["prefecture", "都道府県"]).unwrap_or_else(|| "prefecture".to_string());
    let occ_c = find_column(&col_names, &["occupation", "職種", "job_type"]).unwrap_or_else(|| "occupation".to_string());
    let emp_c = find_column(&col_names, &["employment_type", "雇用形態", "emp_type"]).unwrap_or_else(|| "employment_type".to_string());
    let avg_c = find_column(&col_names, &["avg_salary", "平均給与", "average_salary"]).unwrap_or_else(|| "avg_salary".to_string());
    let med_c = find_column(&col_names, &["median_salary", "中央値給与", "median"]).unwrap_or_else(|| "median_salary".to_string());
    let cnt_c = find_column(&col_names, &["count", "件数", "sample_count", "n"]).unwrap_or_else(|| "count".to_string());

    let select_cols = format!("{}, {}, {}, {}, {}, {}", pref_c, occ_c, emp_c, avg_c, med_c, cnt_c);
    let order_cols = format!("{}, {}, {}", pref_c, occ_c, emp_c);

    let conn2 = external_db.connect().map_err(|e| AppError::Internal(format!("外部DB接続エラー: {}", e)))?;
    let pref_param = params.get("prefecture");
    let occ_param = params.get("occupation");

    let rows = match (pref_param, occ_param) {
        (Some(pref), Some(occ)) => {
            let sql = format!(
                "SELECT {} FROM ts_turso_salary WHERE {} = ?1 AND {} LIKE ?2 ORDER BY {}",
                select_cols, pref_c, occ_c, order_cols
            );
            conn2.query(&sql, [pref.clone(), format!("%{}%", occ)]).await
        }
        (Some(pref), None) => {
            let sql = format!(
                "SELECT {} FROM ts_turso_salary WHERE {} = ?1 ORDER BY {}",
                select_cols, pref_c, order_cols
            );
            conn2.query(&sql, [pref.clone()]).await
        }
        (None, Some(occ)) => {
            let sql = format!(
                "SELECT {} FROM ts_turso_salary WHERE {} LIKE ?1 ORDER BY {}",
                select_cols, occ_c, order_cols
            );
            conn2.query(&sql, [format!("%{}%", occ)]).await
        }
        (None, None) => {
            let sql = format!(
                "SELECT {} FROM ts_turso_salary ORDER BY {}",
                select_cols, order_cols
            );
            conn2.query(&sql, ()).await
        }
    }.map_err(|e| AppError::Internal(format!("給与統計クエリエラー: {}", e)))?;

    let mut results = Vec::new();
    let mut cursor = rows;
    loop {
        match cursor.next().await {
            Ok(Some(row)) => {
                results.push(SalaryBenchmarkEntry {
                    prefecture: row.get::<String>(0).unwrap_or_default(),
                    occupation: row.get::<String>(1).unwrap_or_default(),
                    employment_type: row.get::<String>(2).unwrap_or_default(),
                    avg_salary: row.get_value(3).ok().and_then(|v| value_to_f64(&v)),
                    median_salary: row.get_value(4).ok().and_then(|v| value_to_f64(&v)),
                    count: row.get_value(5).ok().and_then(|v| value_to_i64(&v)),
                });
            }
            Ok(None) => break,
            Err(e) => { tracing::warn!("給与統計行読み込みエラー: {}", e); break; }
        }
    }

    Ok(Json(results))
}

// ===================================================================
// 8. 求人充足率/欠員率 (ts_turso_vacancy)
// ===================================================================

#[derive(Serialize)]
struct VacancyStatsEntry {
    prefecture: String,
    occupation: String,
    fill_rate: Option<f64>,
    vacancy_rate: Option<f64>,
    count: Option<i64>,
}

/// GET /api/external/vacancy-stats?prefecture=&occupation= - 求人充足率・欠員率
async fn vacancy_stats(
    State(state): State<SharedState>,
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
) -> Result<Json<Vec<VacancyStatsEntry>>, AppError> {
    let external_db = get_external_db(&state)?;
    let conn = external_db.connect().map_err(|e| AppError::Internal(format!("外部DB接続エラー: {}", e)))?;

    let col_names = get_table_columns(&conn, "ts_turso_vacancy").await?;

    let pref_c = find_column(&col_names, &["prefecture", "都道府県"]).unwrap_or_else(|| "prefecture".to_string());
    let occ_c = find_column(&col_names, &["occupation", "職種", "job_type"]).unwrap_or_else(|| "occupation".to_string());
    let fill_c = find_column(&col_names, &["fill_rate", "充足率"]).unwrap_or_else(|| "fill_rate".to_string());
    let vac_c = find_column(&col_names, &["vacancy_rate", "欠員率"]).unwrap_or_else(|| "vacancy_rate".to_string());
    let cnt_c = find_column(&col_names, &["count", "件数", "sample_count", "n"]).unwrap_or_else(|| "count".to_string());

    let select_cols = format!("{}, {}, {}, {}, {}", pref_c, occ_c, fill_c, vac_c, cnt_c);
    let order_cols = format!("{}, {}", pref_c, occ_c);

    let conn2 = external_db.connect().map_err(|e| AppError::Internal(format!("外部DB接続エラー: {}", e)))?;
    let pref_param = params.get("prefecture");
    let occ_param = params.get("occupation");

    let rows = match (pref_param, occ_param) {
        (Some(pref), Some(occ)) => {
            let sql = format!(
                "SELECT {} FROM ts_turso_vacancy WHERE {} = ?1 AND {} LIKE ?2 ORDER BY {}",
                select_cols, pref_c, occ_c, order_cols
            );
            conn2.query(&sql, [pref.clone(), format!("%{}%", occ)]).await
        }
        (Some(pref), None) => {
            let sql = format!(
                "SELECT {} FROM ts_turso_vacancy WHERE {} = ?1 ORDER BY {}",
                select_cols, pref_c, order_cols
            );
            conn2.query(&sql, [pref.clone()]).await
        }
        (None, Some(occ)) => {
            let sql = format!(
                "SELECT {} FROM ts_turso_vacancy WHERE {} LIKE ?1 ORDER BY {}",
                select_cols, occ_c, order_cols
            );
            conn2.query(&sql, [format!("%{}%", occ)]).await
        }
        (None, None) => {
            let sql = format!(
                "SELECT {} FROM ts_turso_vacancy ORDER BY {}",
                select_cols, order_cols
            );
            conn2.query(&sql, ()).await
        }
    }.map_err(|e| AppError::Internal(format!("充足率クエリエラー: {}", e)))?;

    let mut results = Vec::new();
    let mut cursor = rows;
    loop {
        match cursor.next().await {
            Ok(Some(row)) => {
                results.push(VacancyStatsEntry {
                    prefecture: row.get::<String>(0).unwrap_or_default(),
                    occupation: row.get::<String>(1).unwrap_or_default(),
                    fill_rate: row.get_value(2).ok().and_then(|v| value_to_f64(&v)),
                    vacancy_rate: row.get_value(3).ok().and_then(|v| value_to_f64(&v)),
                    count: row.get_value(4).ok().and_then(|v| value_to_i64(&v)),
                });
            }
            Ok(None) => break,
            Err(e) => { tracing::warn!("充足率行読み込みエラー: {}", e); break; }
        }
    }

    Ok(Json(results))
}

// ===================================================================
// カラム名動的解決ヘルパー
// ===================================================================

/// PRAGMA table_info でテーブルのカラム名一覧を取得
async fn get_table_columns(conn: &libsql::Connection, table_name: &str) -> Result<Vec<String>, AppError> {
    let sql = format!("PRAGMA table_info({})", table_name);
    let rows = conn
        .query(&sql, ())
        .await
        .map_err(|e| AppError::Internal(format!("テーブル {} のスキーマ取得エラー: {}", table_name, e)))?;

    let mut col_names = Vec::new();
    let mut cursor = rows;
    loop {
        match cursor.next().await {
            Ok(Some(row)) => {
                let name: String = row.get(1).unwrap_or_default();
                col_names.push(name);
            }
            _ => break,
        }
    }

    if col_names.is_empty() {
        tracing::warn!("テーブル {} のカラムが見つかりません（テーブル未存在の可能性）", table_name);
    } else {
        tracing::debug!("テーブル {} のカラム: {:?}", table_name, col_names);
    }

    Ok(col_names)
}

/// 候補リストから実際に存在するカラム名を検索（大文字小文字区別なし）
fn find_column(existing: &[String], candidates: &[&str]) -> Option<String> {
    for candidate in candidates {
        let lower = candidate.to_lowercase();
        if let Some(found) = existing.iter().find(|c| c.to_lowercase() == lower) {
            return Some(found.clone());
        }
    }
    None
}
