/// 集計ロジック
/// Polarsを使用してDataFrameからKPI・都道府県別・サービス別・法人種別の集計を実行

use chrono::Datelike;
use polars::prelude::*;

use crate::error::AppError;
use crate::models::aggregation::*;

/// DataFrameからダッシュボードKPIを計算
pub fn compute_kpi(df: &DataFrame) -> Result<DashboardKpi, AppError> {
    let total = df.height();

    let avg_staff = mean_of_col(df, "従業者_合計_num");
    // 定員の異常値を除外（1〜500の範囲のみ対象。居宅介護支援等で不適切な値が入るため）
    let avg_capacity = mean_of_col_filtered(df, "定員_num", 1.0, 500.0);
    let avg_turnover = mean_of_col_clamped(df, "離職率", 0.0, 1.0);
    let avg_fulltime = mean_of_col_clamped(df, "常勤比率", 0.0, 1.0);
    let avg_years = mean_of_col(df, "事業年数");

    Ok(DashboardKpi {
        total_facilities: total,
        avg_staff,
        avg_capacity,
        avg_turnover_rate: avg_turnover,
        avg_fulltime_ratio: avg_fulltime,
        avg_years_in_business: avg_years,
    })
}

/// DataFrameから都道府県別サマリーを計算
pub fn compute_by_prefecture(df: &DataFrame) -> Result<Vec<PrefectureSummary>, AppError> {
    // 都道府県カラムがない場合は空を返す
    let pref_col = match df.column("都道府県") {
        Ok(c) => c.clone(),
        Err(_) => return Ok(vec![]),
    };

    let pref_str = pref_col
        .str()
        .map_err(|e| AppError::Internal(format!("都道府県カラムの型エラー: {}", e)))?;

    // ユニークな都道府県を取得
    let unique_prefs: Vec<String> = pref_str
        .into_iter()
        .filter_map(|v| v.map(|s| s.to_string()))
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();

    let mut results = Vec::new();
    for pref in &unique_prefs {
        // この都道府県に該当する行をフィルタ
        let mask = pref_str
            .into_iter()
            .map(|opt_val| opt_val.map_or(false, |v| v == pref))
            .collect::<BooleanChunked>();

        let filtered = df
            .filter(&mask)
            .map_err(|e| AppError::Internal(format!("フィルタエラー: {}", e)))?;

        results.push(PrefectureSummary {
            prefecture: pref.clone(),
            count: filtered.height(),
            avg_staff: mean_of_col(&filtered, "従業者_合計_num"),
            avg_capacity: mean_of_col_filtered(&filtered, "定員_num", 1.0, 500.0),
            avg_turnover_rate: mean_of_col_clamped(&filtered, "離職率", 0.0, 1.0),
        });
    }

    // 施設数降順でソート
    results.sort_by(|a, b| b.count.cmp(&a.count));
    Ok(results)
}

/// DataFrameからサービス別サマリーを計算
/// 19カラム版ではサービスコード/サービス名カラムが存在しないため空配列を返す
pub fn compute_by_service(df: &DataFrame) -> Result<Vec<ServiceSummary>, AppError> {
    // サービスコードカラムがない場合は空を返す
    let _svc_col = match df.column("サービスコード") {
        Ok(c) => c.clone(),
        Err(_) => return Ok(vec![]),
    };

    let svc_str = _svc_col
        .str()
        .map_err(|e| AppError::Internal(format!("サービスコードカラムの型エラー: {}", e)))?;

    let svc_name_col = df.column("サービス名").ok();

    let unique_codes: Vec<String> = svc_str
        .into_iter()
        .filter_map(|v| v.map(|s| s.to_string()))
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();

    let mut results = Vec::new();
    for code in &unique_codes {
        let mask = svc_str
            .into_iter()
            .map(|opt_val| opt_val.map_or(false, |v| v == code))
            .collect::<BooleanChunked>();

        let filtered = df
            .filter(&mask)
            .map_err(|e| AppError::Internal(format!("フィルタエラー: {}", e)))?;

        // サービス名を取得（最初のマッチした行から）
        let service_name = svc_name_col
            .and_then(|c| c.str().ok())
            .and_then(|ca| {
                ca.into_iter()
                    .zip(svc_str.into_iter())
                    .find_map(|(name, svc_code)| {
                        if svc_code == Some(code.as_str()) {
                            name.map(|n| n.to_string())
                        } else {
                            None
                        }
                    })
            })
            .unwrap_or_else(|| code.clone());

        results.push(ServiceSummary {
            service_code: code.clone(),
            service_name,
            count: filtered.height(),
            avg_staff: mean_of_col(&filtered, "従業者_合計_num"),
        });
    }

    results.sort_by(|a, b| b.count.cmp(&a.count));
    Ok(results)
}

/// コロプレスマップ用の都道府県メトリクスを計算
pub fn compute_choropleth(df: &DataFrame) -> Result<Vec<PrefectureMetric>, AppError> {
    let pref_col = match df.column("都道府県") {
        Ok(c) => c.clone(),
        Err(_) => return Ok(vec![]),
    };

    let pref_str = pref_col
        .str()
        .map_err(|e| AppError::Internal(format!("都道府県カラムの型エラー: {}", e)))?;

    let unique_prefs: Vec<String> = pref_str
        .into_iter()
        .filter_map(|v| v.map(|s| s.to_string()))
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();

    let mut results = Vec::new();
    for pref in &unique_prefs {
        let mask = pref_str
            .into_iter()
            .map(|opt_val| opt_val.map_or(false, |v| v == pref))
            .collect::<BooleanChunked>();

        let filtered = df
            .filter(&mask)
            .map_err(|e| AppError::Internal(format!("フィルタエラー: {}", e)))?;

        results.push(PrefectureMetric {
            prefecture: pref.clone(),
            count: filtered.height(),
            avg_staff: mean_of_col(&filtered, "従業者_合計_num"),
            avg_turnover_rate: mean_of_col_clamped(&filtered, "離職率", 0.0, 1.0),
            avg_fulltime_ratio: mean_of_col_clamped(&filtered, "常勤比率", 0.0, 1.0),
        });
    }

    results.sort_by(|a, b| b.count.cmp(&a.count));
    Ok(results)
}

/// サービス別棒グラフ用データを計算
/// 19カラム版では空配列を返す
pub fn compute_service_bar(df: &DataFrame) -> Result<Vec<ServiceBar>, AppError> {
    let svc_col = match df.column("サービス名") {
        Ok(c) => c.clone(),
        Err(_) => return Ok(vec![]),
    };

    let svc_str = svc_col
        .str()
        .map_err(|e| AppError::Internal(format!("サービス名カラムの型エラー: {}", e)))?;

    let unique_names: Vec<String> = svc_str
        .into_iter()
        .filter_map(|v| v.map(|s| s.to_string()))
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();

    let mut results = Vec::new();
    for name in &unique_names {
        let mask = svc_str
            .into_iter()
            .map(|opt_val| opt_val.map_or(false, |v| v == name))
            .collect::<BooleanChunked>();

        let filtered = df
            .filter(&mask)
            .map_err(|e| AppError::Internal(format!("フィルタエラー: {}", e)))?;

        results.push(ServiceBar {
            service_name: name.clone(),
            count: filtered.height(),
            avg_staff: mean_of_col(&filtered, "従業者_合計_num"),
        });
    }

    results.sort_by(|a, b| b.count.cmp(&a.count));
    Ok(results)
}

/// 法人種別ドーナツチャート用データを計算
pub fn compute_corp_type_donut(df: &DataFrame) -> Result<Vec<CorpTypeSlice>, AppError> {
    let ct_col = match df.column("法人種別") {
        Ok(c) => c.clone(),
        Err(_) => return Ok(vec![]),
    };

    let ct_str = ct_col
        .str()
        .map_err(|e| AppError::Internal(format!("法人種別カラムの型エラー: {}", e)))?;

    let total = df.height() as f64;
    let unique_types: Vec<String> = ct_str
        .into_iter()
        .filter_map(|v| v.map(|s| s.to_string()))
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();

    let mut results = Vec::new();
    for ct in &unique_types {
        let count = ct_str
            .into_iter()
            .filter(|opt_val| opt_val.map_or(false, |v| v == ct))
            .count();

        results.push(CorpTypeSlice {
            corp_type: ct.clone(),
            count,
            ratio: if total > 0.0 {
                count as f64 / total
            } else {
                0.0
            },
        });
    }

    results.sort_by(|a, b| b.count.cmp(&a.count));
    Ok(results)
}

/// メタ情報を計算（選択肢リスト、範囲情報）
pub fn compute_meta(df: &DataFrame) -> Result<DataMeta, AppError> {
    // 都道府県一覧
    let prefectures = if let Ok(col) = df.column("都道府県") {
        let ca = col
            .str()
            .map_err(|e| AppError::Internal(format!("都道府県カラムの型エラー: {}", e)))?;
        let mut prefs: Vec<String> = ca
            .into_iter()
            .filter_map(|v| v.map(|s| s.to_string()))
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        prefs.sort();
        prefs
    } else {
        vec![]
    };

    // サービスコード一覧
    let service_codes = if let Ok(col) = df.column("サービスコード") {
        let ca = col
            .str()
            .map_err(|e| AppError::Internal(format!("サービスコードカラムの型エラー: {}", e)))?;
        let mut codes: Vec<String> = ca
            .into_iter()
            .filter_map(|v| v.map(|s| s.to_string()))
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        codes.sort();
        codes
    } else {
        vec![]
    };

    // 法人種別一覧
    let corp_types = if let Ok(col) = df.column("法人種別") {
        let ca = col
            .str()
            .map_err(|e| AppError::Internal(format!("法人種別カラムの型エラー: {}", e)))?;
        let mut types: Vec<String> = ca
            .into_iter()
            .filter_map(|v| v.map(|s| s.to_string()))
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        types.sort();
        types
    } else {
        vec![]
    };

    // 従業者数の範囲
    let staff_range = if let Some(ca) = col_as_f64(df, "従業者_合計_num") {
        let min_val = ca.into_iter().filter_map(|v| v).fold(f64::INFINITY, f64::min);
        let max_val = ca
            .into_iter()
            .filter_map(|v| v)
            .fold(f64::NEG_INFINITY, f64::max);
        if min_val.is_infinite() || max_val.is_infinite() {
            (0.0, 0.0)
        } else {
            (min_val, max_val)
        }
    } else {
        (0.0, 0.0)
    };

    Ok(DataMeta {
        total_count: df.height(),
        prefectures,
        service_codes,
        corp_types,
        staff_range,
    })
}

// ========================================
// Phase 2: 人材分析集計
// ========================================

/// 人材KPIを計算
/// 19カラム版でも離職率・採用率・常勤比率は計算可能
pub fn compute_workforce_kpi(df: &DataFrame) -> Result<WorkforceKpi, AppError> {
    let avg_turnover = mean_of_col_opt(df, "離職率").map(|v| v.clamp(0.0, 1.0));
    let avg_fulltime = mean_of_col_opt(df, "常勤比率").map(|v| v.clamp(0.0, 1.0));

    // 採用率 = 前年度採用数 / 従業者合計
    let avg_hire_rate = compute_ratio_mean(df, "前年度採用数_num", "従業者_合計_num");

    // 経験10年以上割合はString型 "72.7％" 等を含むため、パーセント記号を除去してパース
    // パース結果は72.7等のパーセント値なので、0-1の比率に正規化する（フロントエンドが n*100 で表示するため）
    let avg_experience_10yr_ratio = {
        let values = parse_experience_ratio_values(df);
        let valid: Vec<f64> = values.into_iter().filter_map(|v| v).collect();
        if valid.is_empty() {
            None
        } else {
            let mean = valid.iter().sum::<f64>() / valid.len() as f64;
            // パーセント値（0-100）を比率（0-1）に正規化
            Some(if mean > 1.0 { mean / 100.0 } else { mean })
        }
    };

    Ok(WorkforceKpi {
        avg_turnover_rate: avg_turnover,
        avg_hire_rate,
        avg_fulltime_ratio: avg_fulltime,
        avg_experience_10yr_ratio,
    })
}

/// 離職率の分布を計算（5%刻みヒストグラム）
pub fn compute_turnover_distribution(df: &DataFrame) -> Result<Vec<TurnoverDistribution>, AppError> {
    let ranges = vec![
        ("0-5%", 0.0, 0.05),
        ("5-10%", 0.05, 0.10),
        ("10-15%", 0.10, 0.15),
        ("15-20%", 0.15, 0.20),
        ("20-25%", 0.20, 0.25),
        ("25-30%", 0.25, 0.30),
        ("30%以上", 0.30, f64::INFINITY),
    ];

    let turnover_vals = get_f64_col_values(df, "離職率");

    let results = ranges
        .into_iter()
        .map(|(label, min, max)| {
            let count = turnover_vals
                .iter()
                .filter(|v| **v >= min && **v < max)
                .count();
            TurnoverDistribution {
                range: label.to_string(),
                count,
            }
        })
        .collect();

    Ok(results)
}

/// 都道府県別人材指標を計算
pub fn compute_workforce_by_prefecture(df: &DataFrame) -> Result<Vec<WorkforcePrefecture>, AppError> {
    let pref_col = match df.column("都道府県") {
        Ok(c) => c.clone(),
        Err(_) => return Ok(vec![]),
    };

    let pref_str = pref_col
        .str()
        .map_err(|e| AppError::Internal(format!("都道府県カラムの型エラー: {}", e)))?;

    let unique_prefs: Vec<String> = pref_str
        .into_iter()
        .filter_map(|v| v.map(|s| s.to_string()))
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();

    let mut results = Vec::new();
    for pref in &unique_prefs {
        let mask = pref_str
            .into_iter()
            .map(|opt_val| opt_val.map_or(false, |v| v == pref))
            .collect::<BooleanChunked>();

        let filtered = df
            .filter(&mask)
            .map_err(|e| AppError::Internal(format!("フィルタエラー: {}", e)))?;

        results.push(WorkforcePrefecture {
            prefecture: pref.clone(),
            avg_turnover_rate: mean_of_col_clamped(&filtered, "離職率", 0.0, 1.0),
            avg_fulltime_ratio: mean_of_col_clamped(&filtered, "常勤比率", 0.0, 1.0),
            facility_count: filtered.height(),
        });
    }

    results.sort_by(|a, b| b.facility_count.cmp(&a.facility_count));
    Ok(results)
}

/// 従業者規模別離職率を計算
pub fn compute_workforce_by_size(df: &DataFrame) -> Result<Vec<WorkforceBySize>, AppError> {
    let categories = vec![
        ("小規模(1-10)", 1.0, 10.0),
        ("中規模(11-30)", 11.0, 30.0),
        ("中大規模(31-50)", 31.0, 50.0),
        ("大規模(51-100)", 51.0, 100.0),
        ("超大規模(101以上)", 101.0, f64::INFINITY),
    ];

    let staff_vals = get_f64_col_opt_values(df, "従業者_合計_num");
    let turnover_vals = get_f64_col_opt_values(df, "離職率");
    let fulltime_vals = get_f64_col_opt_values(df, "常勤比率");

    let mut results = Vec::new();
    for (label, min, max) in &categories {
        let mut turnover_sum = 0.0;
        let mut turnover_count = 0usize;
        let mut fulltime_sum = 0.0;
        let mut fulltime_count = 0usize;
        let mut facility_count = 0usize;

        for (i, staff) in staff_vals.iter().enumerate() {
            if let Some(s) = staff {
                if *s >= *min && *s <= *max {
                    facility_count += 1;
                    if let Some(t) = turnover_vals.get(i).and_then(|v| v.as_ref()) {
                        turnover_sum += t;
                        turnover_count += 1;
                    }
                    if let Some(f) = fulltime_vals.get(i).and_then(|v| v.as_ref()) {
                        if *f >= 0.0 && *f <= 1.0 {
                            fulltime_sum += f;
                            fulltime_count += 1;
                        }
                    }
                }
            }
        }

        let avg_turnover = if turnover_count > 0 {
            (turnover_sum / turnover_count as f64).clamp(0.0, 1.0)
        } else {
            0.0
        };

        let avg_fulltime = if fulltime_count > 0 {
            (fulltime_sum / fulltime_count as f64).clamp(0.0, 1.0)
        } else {
            0.0
        };

        results.push(WorkforceBySize {
            size_category: label.to_string(),
            avg_turnover_rate: avg_turnover,
            avg_fulltime_ratio: avg_fulltime,
            count: facility_count,
        });
    }

    Ok(results)
}

// ========================================
// Phase 2: 収益構造集計
// ========================================

/// 収益KPIを計算
/// 19カラム版では定員のみ利用可能、他はnull
pub fn compute_revenue_kpi(df: &DataFrame) -> Result<RevenueKpi, AppError> {
    // 定員の異常値を除外（1〜500の範囲のみ対象）
    let avg_capacity = mean_of_col_filtered_opt(df, "定員_num", 1.0, 500.0);

    // 76カラム拡張: 実データから計算（19カラム版ではnull）
    let avg_kasan_count = mean_of_col_opt(df, "加算取得数");
    // 処遇改善加算率: maru_rateで全施設数を分母にする（mean_of_col_optだとNone除外で100%になるバグ）
    let syogu_kaizen_rate = maru_rate(df, "処遇改善加算フラグ")
        .or_else(|| maru_rate(df, "加算_処遇改善I"));
    // 稼働率の異常値を除外（0〜3.0=300%の範囲のみ対象）
    let avg_occupancy_rate = mean_of_col_filtered_opt(df, "稼働率", 0.0, 3.0);
    let avg_quality_score = mean_of_col_opt(df, "品質スコア");
    let avg_user_count = mean_of_col_opt(df, "利用者総数_num");

    Ok(RevenueKpi {
        avg_kasan_count,
        syogu_kaizen_rate,
        avg_occupancy_rate,
        avg_capacity,
        avg_quality_score,
        avg_user_count,
    })
}

/// 加算取得率を計算（13加算の実取得率）
/// 76カラム版: 各加算の取得率を実データから計算
/// 19カラム版: 加算_*カラムが存在しないため空配列を返す
pub fn compute_kasan_rates(df: &DataFrame) -> Result<Vec<KasanRate>, AppError> {
    let total = df.height();
    if total == 0 {
        return Ok(vec![]);
    }

    // 13加算カラム（DataStoreで計算したf64版）
    let kasan_columns = vec![
        ("処遇改善加算I", "加算_処遇改善I_f"),
        ("処遇改善加算II", "加算_処遇改善II_f"),
        ("処遇改善加算III", "加算_処遇改善III_f"),
        ("処遇改善加算IV", "加算_処遇改善IV_f"),
        ("特定事業所加算I", "加算_特定事業所I_f"),
        ("特定事業所加算II", "加算_特定事業所II_f"),
        ("特定事業所加算III", "加算_特定事業所III_f"),
        ("特定事業所加算IV", "加算_特定事業所IV_f"),
        ("特定事業所加算V", "加算_特定事業所V_f"),
        ("認知症ケア加算I", "加算_認知症ケアI_f"),
        ("認知症ケア加算II", "加算_認知症ケアII_f"),
        ("口腔連携加算", "加算_口腔連携_f"),
        ("緊急時加算", "加算_緊急時_f"),
    ];

    let mut results = Vec::new();
    for (display_name, col_name) in &kasan_columns {
        if let Ok(col) = df.column(*col_name) {
            if let Ok(ca) = col.f64() {
                // 値が存在する行の中でTrue(1.0)のカウント
                let valid_count = ca.into_iter().filter(|v| v.is_some()).count();
                if valid_count == 0 {
                    continue;
                }
                let true_count = ca.into_iter()
                    .filter_map(|v| v)
                    .filter(|v| *v > 0.5)
                    .count();
                // 全施設数を分母にする（valid_countだと○のある施設だけが分母になり100%になるバグ）
                let rate = true_count as f64 / total as f64;
                results.push(KasanRate {
                    kasan_name: display_name.to_string(),
                    rate,
                    count: true_count,
                });
            }
        }
    }

    // f64版カラムがない場合: 元の文字列カラムから「○」を直接カウント
    if results.is_empty() {
        let raw_kasan_columns = vec![
            ("処遇改善加算I", "加算_処遇改善I"),
            ("処遇改善加算II", "加算_処遇改善II"),
            ("処遇改善加算III", "加算_処遇改善III"),
            ("処遇改善加算IV", "加算_処遇改善IV"),
            ("特定事業所加算I", "加算_特定事業所I"),
            ("特定事業所加算II", "加算_特定事業所II"),
            ("特定事業所加算III", "加算_特定事業所III"),
            ("特定事業所加算IV", "加算_特定事業所IV"),
            ("特定事業所加算V", "加算_特定事業所V"),
            ("認知症ケア加算I", "加算_認知症ケアI"),
            ("認知症ケア加算II", "加算_認知症ケアII"),
            ("口腔連携加算", "加算_口腔連携"),
            ("緊急時加算", "加算_緊急時"),
        ];
        for (display_name, col_name) in &raw_kasan_columns {
            let maru_count = count_maru(df, col_name);
            if df.column(*col_name).is_ok() {
                let rate = maru_count as f64 / total as f64;
                results.push(KasanRate {
                    kasan_name: display_name.to_string(),
                    rate,
                    count: maru_count,
                });
            }
        }
    }

    // 旧カラム名との互換性（ETLで旧名が残っている場合）
    if results.is_empty() {
        let legacy_columns = vec![
            "処遇改善加算",
            "特定処遇改善加算",
            "ベースアップ等支援加算",
            "サービス提供体制強化加算",
            "介護職員処遇改善加算",
        ];
        for col_name in &legacy_columns {
            if let Ok(col) = df.column(*col_name) {
                let count = col.len() - col.null_count();
                let rate = count as f64 / total as f64;
                results.push(KasanRate {
                    kasan_name: col_name.to_string(),
                    rate,
                    count,
                });
            }
        }
    }

    Ok(results)
}

/// 稼働率分布を計算（フルデータ用）
/// 19カラム版では空配列を返す
pub fn compute_occupancy_distribution(df: &DataFrame) -> Result<Vec<OccupancyDistribution>, AppError> {
    let occupancy_vals = get_f64_col_values(df, "稼働率");

    if occupancy_vals.is_empty() {
        return Ok(vec![]);
    }

    let ranges = vec![
        ("0-50%", 0.0, 0.50),
        ("50-60%", 0.50, 0.60),
        ("60-70%", 0.60, 0.70),
        ("70-80%", 0.70, 0.80),
        ("80-90%", 0.80, 0.90),
        ("90-100%", 0.90, 1.00),
        ("100%超", 1.00, f64::INFINITY),
    ];

    let results = ranges
        .into_iter()
        .map(|(label, min, max)| {
            let count = occupancy_vals
                .iter()
                .filter(|v| **v >= min && **v < max)
                .count();
            OccupancyDistribution {
                range: label.to_string(),
                count,
            }
        })
        .collect();

    Ok(results)
}

// ========================================
// Phase 2: 賃金分析集計
// ========================================

/// 賃金KPIを計算（フルデータ用）
/// 19カラム版では全てnull
pub fn compute_salary_kpi(df: &DataFrame) -> Result<SalaryKpi, AppError> {
    let avg_salary = mean_of_col_opt(df, "給与水準");
    let median_salary = median_of_col(df, "給与水準");

    let (min_salary, max_salary) = if let Ok(col) = df.column("給与水準") {
        if let Ok(ca) = col.f64() {
            let min_val = ca.into_iter().filter_map(|v| v).fold(f64::INFINITY, f64::min);
            let max_val = ca.into_iter().filter_map(|v| v).fold(f64::NEG_INFINITY, f64::max);
            if min_val.is_infinite() {
                (None, None)
            } else {
                (Some(min_val), Some(max_val))
            }
        } else {
            (None, None)
        }
    } else {
        (None, None)
    };

    Ok(SalaryKpi {
        avg_salary,
        median_salary,
        max_salary,
        min_salary,
    })
}

/// 職種別賃金を計算（フルデータ用）
/// 19カラム版では空配列を返す
pub fn compute_salary_by_job_type(df: &DataFrame) -> Result<Vec<SalaryByJobType>, AppError> {
    // 職種カラムがない場合は空配列を返す
    let job_col = match df.column("職種") {
        Ok(c) => c.clone(),
        Err(_) => return Ok(vec![]),
    };

    let job_str = job_col
        .str()
        .map_err(|e| AppError::Internal(format!("職種カラムの型エラー: {}", e)))?;

    let unique_jobs: Vec<String> = job_str
        .into_iter()
        .filter_map(|v| v.map(|s| s.to_string()))
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();

    let mut results = Vec::new();
    for job in &unique_jobs {
        let mask = job_str
            .into_iter()
            .map(|opt_val| opt_val.map_or(false, |v| v == job))
            .collect::<BooleanChunked>();

        let filtered = df
            .filter(&mask)
            .map_err(|e| AppError::Internal(format!("フィルタエラー: {}", e)))?;

        results.push(SalaryByJobType {
            job_type: job.clone(),
            avg_salary: mean_of_col(&filtered, "給与水準"),
            count: filtered.height(),
        });
    }

    results.sort_by(|a, b| b.count.cmp(&a.count));
    Ok(results)
}

/// 都道府県別賃金を計算（フルデータ用）
/// 19カラム版では空配列を返す（給与水準カラムがないため）
pub fn compute_salary_by_prefecture(df: &DataFrame) -> Result<Vec<SalaryByPrefecture>, AppError> {
    // 給与水準カラムがない場合は空配列を返す
    if df.column("給与水準").is_err() {
        return Ok(vec![]);
    }

    let pref_col = match df.column("都道府県") {
        Ok(c) => c.clone(),
        Err(_) => return Ok(vec![]),
    };

    let pref_str = pref_col
        .str()
        .map_err(|e| AppError::Internal(format!("都道府県カラムの型エラー: {}", e)))?;

    let unique_prefs: Vec<String> = pref_str
        .into_iter()
        .filter_map(|v| v.map(|s| s.to_string()))
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();

    let mut results = Vec::new();
    for pref in &unique_prefs {
        let mask = pref_str
            .into_iter()
            .map(|opt_val| opt_val.map_or(false, |v| v == pref))
            .collect::<BooleanChunked>();

        let filtered = df
            .filter(&mask)
            .map_err(|e| AppError::Internal(format!("フィルタエラー: {}", e)))?;

        results.push(SalaryByPrefecture {
            prefecture: pref.clone(),
            avg_salary: mean_of_col(&filtered, "給与水準"),
            count: filtered.height(),
        });
    }

    results.sort_by(|a, b| b.count.cmp(&a.count));
    Ok(results)
}

// ========================================
// Phase 2: 経営品質集計
// ========================================

/// 「○」マークをカウントするヘルパー関数
/// 品質・加算カラムの「○」値を直接カウントする（フォールバック用）
fn count_maru(df: &DataFrame, col_name: &str) -> usize {
    if let Ok(col) = df.column(col_name) {
        if let Ok(str_col) = col.str() {
            return str_col.into_iter()
                .filter(|v| v.map_or(false, |s| s.contains("○")))
                .count();
        }
        // bool型の場合（ETLで変換済み）
        if let Ok(bool_col) = col.bool() {
            return bool_col.into_iter()
                .filter(|v| v.unwrap_or(false))
                .count();
        }
        // f64型の場合（DataStoreで変換済み: 1.0 = true）
        if let Ok(f64_col) = col.f64() {
            return f64_col.into_iter()
                .filter(|v| v.map_or(false, |f| f > 0.5))
                .count();
        }
    }
    0
}

/// 「○」マークの取得率を計算するヘルパー関数
/// 品質・加算カラムの「○」比率を返す（全施設数ベース）
/// 注意: mean_of_col_optはNone行を除外するため○行だけの平均(=1.0)になるバグがあった
/// count_maruで○の件数を数え、全施設数で割る方式に統一
fn maru_rate(df: &DataFrame, col_name: &str) -> Option<f64> {
    let total = df.height();
    if total == 0 {
        return None;
    }
    let count = count_maru(df, col_name);
    if count > 0 || df.column(col_name).is_ok() {
        Some(count as f64 / total as f64)
    } else {
        None
    }
}

/// 経営品質KPIを計算
/// 76カラム版: 品質スコア、BCP率、ICT率、第三者評価率、保険加入率を実データから計算
/// 派生カラムが存在しない場合は元の「○」カラムから直接計算（フォールバック）
pub fn compute_quality_kpi(df: &DataFrame) -> Result<QualityKpi, AppError> {
    let avg_profit_ratio = mean_of_col_opt(df, "損益差額比率");
    let avg_experienced_ratio = mean_of_col_opt(df, "経験5年以上比率");

    // 黒字施設割合
    let profitable_ratio = if let Ok(col) = df.column("損益差額比率") {
        if let Ok(ca) = col.f64() {
            let total = ca.into_iter().filter_map(|v| v).count();
            if total > 0 {
                let profitable = ca.into_iter().filter_map(|v| v).filter(|v| *v > 0.0).count();
                Some(profitable as f64 / total as f64)
            } else {
                None
            }
        } else {
            None
        }
    } else {
        None
    };

    // 品質指標: まず派生フラグカラムから取得、なければ元カラムから「○」カウント
    let bcp_rate = maru_rate(df, "BCP策定フラグ")
        .or_else(|| maru_rate(df, "品質_BCP策定"));
    let ict_rate = maru_rate(df, "ICT活用フラグ")
        .or_else(|| maru_rate(df, "品質_ICT活用"));
    let third_party_rate = maru_rate(df, "第三者評価フラグ")
        .or_else(|| maru_rate(df, "品質_第三者評価"));
    let insurance_rate = maru_rate(df, "損害賠償保険フラグ")
        .or_else(|| maru_rate(df, "品質_損害賠償保険"));

    // 品質スコア: DataStoreで計算済みの値を使用
    // なければ4指標から簡易計算（各25点、合計100点満点）
    let avg_quality_score = mean_of_col_opt(df, "品質スコア")
        .or_else(|| {
            let total = df.height();
            if total == 0 {
                return None;
            }
            // 元カラムから施設ごとの品質スコアを計算
            let bcp_count = count_maru(df, "品質_BCP策定");
            let ict_count = count_maru(df, "品質_ICT活用");
            let eval_count = count_maru(df, "品質_第三者評価");
            let ins_count = count_maru(df, "品質_損害賠償保険");
            // いずれかのカラムが存在する場合のみ計算
            if bcp_count + ict_count + eval_count + ins_count > 0
                || df.column("品質_BCP策定").is_ok()
            {
                // 各項目25点、施設ごとの合計の平均
                let avg = (bcp_count as f64 * 25.0
                    + ict_count as f64 * 25.0
                    + eval_count as f64 * 25.0
                    + ins_count as f64 * 25.0) / total as f64;
                Some(avg)
            } else {
                None
            }
        });

    Ok(QualityKpi {
        avg_profit_ratio,
        profitable_ratio,
        avg_experienced_ratio,
        facility_count: df.height(),
        avg_quality_score,
        bcp_rate,
        ict_rate,
        third_party_rate,
        insurance_rate,
    })
}

/// 経営品質スコア分布を計算（フルデータ用）
/// 品質スコア（0-100点）の分布を返す
/// 品質スコアカラムがない場合は元の品質カラムから簡易計算
pub fn compute_quality_score_distribution(df: &DataFrame) -> Result<Vec<QualityScoreDistribution>, AppError> {
    // まず品質スコアカラムから取得を試みる
    let mut quality_vals = get_f64_col_values(df, "品質スコア");

    // 品質スコアカラムがない場合は元カラムから施設ごとに計算
    if quality_vals.is_empty() {
        let total = df.height();
        if total == 0 {
            return Ok(vec![]);
        }

        // 各品質カラムを文字列として読み取り、施設ごとのスコアを計算
        let quality_cols = vec![
            "品質_BCP策定",
            "品質_ICT活用",
            "品質_第三者評価",
            "品質_損害賠償保険",
        ];

        // いずれかの品質カラムが存在するか確認
        let has_any_col = quality_cols.iter().any(|c| df.column(c).is_ok());
        if !has_any_col {
            return Ok(vec![]);
        }

        // 各カラムの文字列値を取得
        let col_strs: Vec<Option<&StringChunked>> = quality_cols.iter()
            .map(|c| df.column(c).ok().and_then(|col| col.str().ok()))
            .collect();

        for i in 0..total {
            let mut score = 0.0f64;
            for str_col_opt in &col_strs {
                if let Some(str_col) = str_col_opt {
                    if let Some(val) = str_col.get(i) {
                        if val.contains("○") {
                            score += 25.0;
                        }
                    }
                }
            }
            quality_vals.push(score);
        }
    }

    if quality_vals.is_empty() {
        return Ok(vec![]);
    }

    // 品質スコア（0-100点）のレンジ別分布
    let score_ranges = vec![
        ("0-20点", 0.0, 20.0),
        ("20-40点", 20.0, 40.0),
        ("40-60点", 40.0, 60.0),
        ("60-80点", 60.0, 80.0),
        ("80-100点", 80.0, f64::INFINITY),
    ];

    let results = score_ranges
        .into_iter()
        .map(|(label, min, max)| {
            let count = quality_vals
                .iter()
                .filter(|v| **v >= min && **v < max)
                .count();
            QualityScoreDistribution {
                range: label.to_string(),
                count,
            }
        })
        .collect();

    Ok(results)
}

/// 都道府県別経営品質を計算（フルデータ用）
/// 損益差額比率、品質スコア、品質_*カラムのいずれかがあれば計算可能
pub fn compute_quality_by_prefecture(df: &DataFrame) -> Result<Vec<QualityByPrefecture>, AppError> {
    // 損益差額比率・品質スコア・品質_*カラムのいずれかが存在するか確認
    let has_profit = df.column("損益差額比率").is_ok();
    let has_quality_score = df.column("品質スコア").is_ok();
    let has_quality_raw = df.column("品質_BCP策定").is_ok();

    if !has_profit && !has_quality_score && !has_quality_raw {
        return Ok(vec![]);
    }

    let pref_col = match df.column("都道府県") {
        Ok(c) => c.clone(),
        Err(_) => return Ok(vec![]),
    };

    let pref_str = pref_col
        .str()
        .map_err(|e| AppError::Internal(format!("都道府県カラムの型エラー: {}", e)))?;

    let unique_prefs: Vec<String> = pref_str
        .into_iter()
        .filter_map(|v| v.map(|s| s.to_string()))
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();

    let mut results = Vec::new();
    for pref in &unique_prefs {
        let mask = pref_str
            .into_iter()
            .map(|opt_val| opt_val.map_or(false, |v| v == pref))
            .collect::<BooleanChunked>();

        let filtered = df
            .filter(&mask)
            .map_err(|e| AppError::Internal(format!("フィルタエラー: {}", e)))?;

        // avg_profit_ratioフィールドに品質スコアまたは損益差額比率を入れる
        let quality_value = if has_profit {
            mean_of_col(&filtered, "損益差額比率")
        } else if has_quality_score {
            mean_of_col(&filtered, "品質スコア")
        } else {
            // 元カラムから簡易品質スコアを計算
            let total = filtered.height() as f64;
            if total > 0.0 {
                let bcp = count_maru(&filtered, "品質_BCP策定") as f64;
                let ict = count_maru(&filtered, "品質_ICT活用") as f64;
                let eval = count_maru(&filtered, "品質_第三者評価") as f64;
                let ins = count_maru(&filtered, "品質_損害賠償保険") as f64;
                (bcp * 25.0 + ict * 25.0 + eval * 25.0 + ins * 25.0) / total
            } else {
                0.0
            }
        };

        results.push(QualityByPrefecture {
            prefecture: pref.clone(),
            avg_profit_ratio: quality_value,
            count: filtered.height(),
        });
    }

    results.sort_by(|a, b| b.count.cmp(&a.count));
    Ok(results)
}

// ========================================
// Phase 3: 法人グループ分析集計
// ========================================

/// 法人番号でグループ化した情報を収集するヘルパー
/// 法人番号が空/nullの施設はスキップ
struct CorpGroupData {
    corp_name: String,
    corp_type: Option<String>,
    facility_count: usize,
    total_staff: f64,
    turnover_sum: f64,
    turnover_count: usize,
    capacity_sum: f64,
    capacity_count: usize,
    fulltime_ratio_sum: f64,
    fulltime_ratio_count: usize,
    prefectures: std::collections::HashSet<String>,
    service_names: std::collections::HashSet<String>,
    facility_names: Vec<String>,
    years_in_business: Option<f64>,
    hired_sum: f64,
    left_sum: f64,
    representative: Option<String>,
}

/// DataFrameから法人番号ごとにグループ化
fn build_corp_groups(
    df: &DataFrame,
) -> Result<std::collections::HashMap<String, CorpGroupData>, AppError> {
    let height = df.height();
    let mut groups: std::collections::HashMap<String, CorpGroupData> =
        std::collections::HashMap::new();

    // カラム取得
    let corp_number_col = df.column("法人番号").ok().and_then(|c| c.str().ok().cloned());
    let corp_name_col = df.column("法人名").ok().and_then(|c| c.str().ok().cloned());
    let corp_type_col = df.column("法人種別").ok().and_then(|c| c.str().ok().cloned());
    let staff_col = col_as_f64(df, "従業者_合計_num");
    let turnover_col = col_as_f64(df, "離職率");
    let capacity_col = col_as_f64(df, "定員_num");
    let fulltime_col = col_as_f64(df, "常勤比率");
    let pref_col = df.column("都道府県").ok().and_then(|c| c.str().ok().cloned());
    let svc_col = df.column("サービス名").ok().and_then(|c| c.str().ok().cloned());
    let name_col = df.column("事業所名").ok().and_then(|c| c.str().ok().cloned());
    let years_col = col_as_f64(df, "事業年数");
    let hired_col = col_as_f64(df, "前年度採用数_num");
    let left_col = col_as_f64(df, "前年度退職数_num");
    let rep_col = df.column("代表者名").ok().and_then(|c| c.str().ok().cloned());

    for i in 0..height {
        // 法人番号を取得、空/nullはスキップ
        let corp_num = match &corp_number_col {
            Some(ca) => match ca.get(i) {
                Some(v) if !v.trim().is_empty() => v.trim().to_string(),
                _ => continue,
            },
            None => continue,
        };

        let entry = groups.entry(corp_num).or_insert_with(|| {
            let cname = corp_name_col
                .as_ref()
                .and_then(|ca| ca.get(i))
                .unwrap_or("")
                .to_string();
            let ctype = corp_type_col
                .as_ref()
                .and_then(|ca| ca.get(i))
                .map(|s| s.to_string());
            let rep = rep_col
                .as_ref()
                .and_then(|ca| ca.get(i))
                .map(|s| s.to_string());
            CorpGroupData {
                corp_name: cname,
                corp_type: ctype,
                facility_count: 0,
                total_staff: 0.0,
                turnover_sum: 0.0,
                turnover_count: 0,
                capacity_sum: 0.0,
                capacity_count: 0,
                fulltime_ratio_sum: 0.0,
                fulltime_ratio_count: 0,
                prefectures: std::collections::HashSet::new(),
                service_names: std::collections::HashSet::new(),
                facility_names: Vec::new(),
                years_in_business: None,
                hired_sum: 0.0,
                left_sum: 0.0,
                representative: rep,
            }
        });

        entry.facility_count += 1;

        if let Some(ref ca) = staff_col {
            if let Some(v) = ca.get(i) {
                entry.total_staff += v;
            }
        }
        if let Some(ref ca) = turnover_col {
            if let Some(v) = ca.get(i) {
                entry.turnover_sum += v;
                entry.turnover_count += 1;
            }
        }
        if let Some(ref ca) = capacity_col {
            if let Some(v) = ca.get(i) {
                entry.capacity_sum += v;
                entry.capacity_count += 1;
            }
        }
        if let Some(ref ca) = fulltime_col {
            if let Some(v) = ca.get(i) {
                entry.fulltime_ratio_sum += v;
                entry.fulltime_ratio_count += 1;
            }
        }
        if let Some(ref ca) = pref_col {
            if let Some(v) = ca.get(i) {
                if !v.is_empty() {
                    entry.prefectures.insert(v.to_string());
                }
            }
        }
        if let Some(ref ca) = svc_col {
            if let Some(v) = ca.get(i) {
                if !v.is_empty() {
                    entry.service_names.insert(v.to_string());
                }
            }
        }
        if let Some(ref ca) = name_col {
            if let Some(v) = ca.get(i) {
                entry.facility_names.push(v.to_string());
            }
        }
        if let Some(ref ca) = years_col {
            if let Some(v) = ca.get(i) {
                // 最大事業年数を保持
                entry.years_in_business = Some(
                    entry.years_in_business.map_or(v, |cur| cur.max(v)),
                );
            }
        }
        if let Some(ref ca) = hired_col {
            if let Some(v) = ca.get(i) {
                entry.hired_sum += v;
            }
        }
        if let Some(ref ca) = left_col {
            if let Some(v) = ca.get(i) {
                entry.left_sum += v;
            }
        }
    }

    Ok(groups)
}

/// 法人グループKPIを計算
pub fn compute_corp_group_kpi(df: &DataFrame) -> Result<CorpGroupKpi, AppError> {
    let groups = build_corp_groups(df)?;
    let total_corps = groups.len();
    let multi_facility_corps = groups.values().filter(|g| g.facility_count > 1).count();

    let total_facilities_in_groups: usize = groups.values().map(|g| g.facility_count).sum();
    let avg_facilities = if total_corps > 0 {
        total_facilities_in_groups as f64 / total_corps as f64
    } else {
        0.0
    };

    let max_corp = groups
        .iter()
        .max_by_key(|(_, g)| g.facility_count);

    let (max_name, max_count) = match max_corp {
        Some((_, g)) => (Some(g.corp_name.clone()), g.facility_count),
        None => (None, 0),
    };

    Ok(CorpGroupKpi {
        total_corps,
        multi_facility_corps,
        avg_facilities_per_corp: avg_facilities,
        max_facilities_corp_name: max_name,
        max_facilities_count: max_count,
    })
}

/// 法人規模別分布を計算
pub fn compute_corp_size_distribution(
    df: &DataFrame,
) -> Result<Vec<CorpSizeDistribution>, AppError> {
    let groups = build_corp_groups(df)?;

    let categories = vec![
        ("1施設", 1usize, 1usize),
        ("2-3施設", 2, 3),
        ("4-10施設", 4, 10),
        ("11施設以上", 11, usize::MAX),
    ];

    let results = categories
        .into_iter()
        .map(|(label, min, max)| {
            let count = groups
                .values()
                .filter(|g| g.facility_count >= min && g.facility_count <= max)
                .count();
            CorpSizeDistribution {
                category: label.to_string(),
                count,
            }
        })
        .collect();

    Ok(results)
}

/// 施設数上位法人を取得
pub fn compute_top_corps(df: &DataFrame, limit: usize) -> Result<Vec<TopCorp>, AppError> {
    let groups = build_corp_groups(df)?;

    let mut corps: Vec<_> = groups
        .into_iter()
        .map(|(corp_number, g)| {
            let avg_turnover = if g.turnover_count > 0 {
                Some((g.turnover_sum / g.turnover_count as f64).clamp(0.0, 1.0))
            } else {
                None
            };
            let mut prefs: Vec<String> = g.prefectures.into_iter().collect();
            prefs.sort();
            let mut svcs: Vec<String> = g.service_names.into_iter().collect();
            svcs.sort();

            TopCorp {
                corp_name: g.corp_name,
                corp_number,
                corp_type: g.corp_type,
                facility_count: g.facility_count,
                total_staff: g.total_staff,
                avg_turnover_rate: avg_turnover,
                prefectures: prefs,
                service_names: svcs,
            }
        })
        .collect();

    // 施設数降順でソート
    corps.sort_by(|a, b| b.facility_count.cmp(&a.facility_count));
    corps.truncate(limit);
    Ok(corps)
}

// ========================================
// Phase 3: 成長性分析集計
// ========================================

/// 事業開始日から年を抽出するヘルパー
/// "2004/02/01" または "2004-02-01" 形式の両方に対応
fn extract_start_years(df: &DataFrame) -> Vec<Option<i32>> {
    df.column("事業開始日")
        .ok()
        .and_then(|c| c.str().ok().cloned())
        .map(|ca| {
            ca.into_iter()
                .map(|opt_val| {
                    opt_val.and_then(|v| {
                        // 最初の4文字をYYYYとしてパース（区切り文字に依存しない）
                        v.trim().get(..4).and_then(|y| y.parse::<i32>().ok())
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// 成長性KPIを計算
pub fn compute_growth_kpi(df: &DataFrame) -> Result<GrowthKpi, AppError> {
    let start_years = extract_start_years(df);
    let current_year = chrono::Utc::now().year();

    let total_with_start_date = start_years.iter().filter(|y| y.is_some()).count();
    let recent_3yr_count = start_years
        .iter()
        .filter(|y| matches!(y, Some(yr) if *yr >= current_year - 3))
        .count();

    // 平均事業年数
    let years_vals = get_f64_col_values(df, "事業年数");
    let avg_years = if years_vals.is_empty() {
        0.0
    } else {
        years_vals.iter().sum::<f64>() / years_vals.len() as f64
    };

    let total = df.height();
    let net_growth_rate = if total > 0 {
        recent_3yr_count as f64 / total as f64
    } else {
        0.0
    };

    Ok(GrowthKpi {
        recent_3yr_count,
        avg_years_in_business: avg_years,
        net_growth_rate,
        total_with_start_date,
    })
}

/// 年別設立トレンドを計算
pub fn compute_establishment_trend(df: &DataFrame) -> Result<Vec<EstablishmentTrend>, AppError> {
    let start_years = extract_start_years(df);
    let mut year_counts: std::collections::HashMap<i32, usize> = std::collections::HashMap::new();

    for y in start_years.into_iter().flatten() {
        *year_counts.entry(y).or_insert(0) += 1;
    }

    let mut results: Vec<EstablishmentTrend> = year_counts
        .into_iter()
        .map(|(year, count)| EstablishmentTrend { year, count })
        .collect();

    results.sort_by_key(|e| e.year);
    Ok(results)
}

/// 事業年数分布を計算
pub fn compute_years_distribution(df: &DataFrame) -> Result<Vec<YearsDistribution>, AppError> {
    let years_vals = get_f64_col_values(df, "事業年数");

    let ranges = vec![
        ("0-5年", 0.0, 5.0),
        ("5-10年", 5.0, 10.0),
        ("10-15年", 10.0, 15.0),
        ("15-20年", 15.0, 20.0),
        ("20-25年", 20.0, 25.0),
        ("25-30年", 25.0, 30.0),
        ("30年以上", 30.0, f64::INFINITY),
    ];

    let results = ranges
        .into_iter()
        .map(|(label, min, max)| {
            let count = years_vals
                .iter()
                .filter(|v| **v >= min && **v < max)
                .count();
            YearsDistribution {
                range: label.to_string(),
                count,
            }
        })
        .collect();

    Ok(results)
}

// ========================================
// Phase 3: M&Aスクリーニング集計
// ========================================

/// M&Aスクリーニング用フィルタ
pub struct MaScreeningFilter {
    pub prefectures: Vec<String>,
    pub corp_types: Vec<String>,
    pub staff_min: Option<f64>,
    pub staff_max: Option<f64>,
    pub turnover_min: Option<f64>,
    pub turnover_max: Option<f64>,
}

/// 魅力度スコアを計算（0-100）
/// - 改善余地(30%): 離職率が高い + 常勤比率が低い → 改善でvalueが出る
/// - 事業基盤(30%): 施設数多い + 従業者多い + 事業年数長い
/// - 人材安定性(20%): 離職率低い方が安定
/// - 残り20%: 定員充実度
fn compute_attractiveness_score(g: &CorpGroupData) -> f64 {
    let avg_turnover = if g.turnover_count > 0 {
        (g.turnover_sum / g.turnover_count as f64).clamp(0.0, 1.0)
    } else {
        0.0
    };
    let avg_fulltime = if g.fulltime_ratio_count > 0 {
        (g.fulltime_ratio_sum / g.fulltime_ratio_count as f64).clamp(0.0, 1.0)
    } else {
        0.5
    };

    // 改善余地（離職率高い + 常勤比率低い = 改善余地大 → スコア高）
    // 離職率0.3以上で満点、0で0点
    let improvement_score = (avg_turnover / 0.3).min(1.0) * 50.0
        + (1.0 - avg_fulltime).min(1.0) * 50.0;

    // 事業基盤（施設数、従業者数、事業年数で評価）
    let facility_score = (g.facility_count as f64 / 10.0).min(1.0) * 33.3;
    let staff_score = (g.total_staff / 100.0).min(1.0) * 33.3;
    let years_score = g
        .years_in_business
        .map(|y| (y / 20.0).min(1.0) * 33.4)
        .unwrap_or(0.0);
    let business_score = facility_score + staff_score + years_score;

    // 人材安定性（離職率低い方が高スコア）
    let stability_score = (1.0 - (avg_turnover / 0.3).min(1.0)) * 100.0;

    // 定員充実度
    let avg_cap = if g.capacity_count > 0 {
        g.capacity_sum / g.capacity_count as f64
    } else {
        0.0
    };
    let capacity_score = (avg_cap / 50.0).min(1.0) * 100.0;

    // 加重平均
    let raw = improvement_score * 0.30
        + business_score * 0.30
        + stability_score * 0.20
        + capacity_score * 0.20;

    // 0-100にクランプ
    raw.min(100.0).max(0.0)
}

/// M&Aスクリーニングを実行
pub fn compute_ma_screening(
    df: &DataFrame,
    filter: &MaScreeningFilter,
    limit: usize,
) -> Result<MaScreeningResponse, AppError> {
    let groups = build_corp_groups(df)?;
    let total_all = groups.len();

    // フィルタ適用（ファネル計測）
    let mut after_region = 0usize;
    let mut after_scale = 0usize;

    let mut candidates: Vec<MaCandidate> = Vec::new();

    for (corp_number, g) in &groups {
        // 地域フィルタ
        if !filter.prefectures.is_empty() {
            let has_pref = g
                .prefectures
                .iter()
                .any(|p| filter.prefectures.contains(p));
            if !has_pref {
                continue;
            }
        }
        // 法人種別フィルタ
        if !filter.corp_types.is_empty() {
            let matches = g
                .corp_type
                .as_ref()
                .map(|ct| filter.corp_types.contains(ct))
                .unwrap_or(false);
            if !matches {
                continue;
            }
        }
        after_region += 1;

        // 規模フィルタ（従業者数）
        if let Some(min) = filter.staff_min {
            if g.total_staff < min {
                continue;
            }
        }
        if let Some(max) = filter.staff_max {
            if g.total_staff > max {
                continue;
            }
        }

        // 離職率フィルタ
        let avg_turnover = if g.turnover_count > 0 {
            Some((g.turnover_sum / g.turnover_count as f64).clamp(0.0, 1.0))
        } else {
            None
        };
        if let Some(min) = filter.turnover_min {
            if avg_turnover.map_or(true, |t| t < min) {
                continue;
            }
        }
        if let Some(max) = filter.turnover_max {
            if avg_turnover.map_or(true, |t| t > max) {
                continue;
            }
        }
        after_scale += 1;

        let avg_cap = if g.capacity_count > 0 {
            g.capacity_sum / g.capacity_count as f64
        } else {
            0.0
        };
        let mut prefs: Vec<String> = g.prefectures.iter().cloned().collect();
        prefs.sort();
        let mut svcs: Vec<String> = g.service_names.iter().cloned().collect();
        svcs.sort();

        let score = compute_attractiveness_score(g);

        candidates.push(MaCandidate {
            corp_name: g.corp_name.clone(),
            corp_number: corp_number.clone(),
            corp_type: g.corp_type.clone(),
            facility_count: g.facility_count,
            total_staff: g.total_staff,
            avg_turnover_rate: avg_turnover,
            avg_capacity: avg_cap,
            prefectures: prefs,
            service_names: svcs,
            attractiveness_score: (score * 10.0).round() / 10.0,
        });
    }

    // スコア降順でソート
    candidates.sort_by(|a, b| {
        b.attractiveness_score
            .partial_cmp(&a.attractiveness_score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let total = candidates.len();
    candidates.truncate(limit);

    let funnel = vec![
        MaFunnelStage {
            stage: "全法人".to_string(),
            count: total_all,
        },
        MaFunnelStage {
            stage: "地域フィルタ".to_string(),
            count: after_region,
        },
        MaFunnelStage {
            stage: "規模フィルタ".to_string(),
            count: after_scale,
        },
        MaFunnelStage {
            stage: "最終候補".to_string(),
            count: total,
        },
    ];

    Ok(MaScreeningResponse {
        items: candidates,
        total,
        funnel,
    })
}

// ========================================
// Phase 3: DD支援集計
// ========================================

/// 法人検索（法人名部分一致 or 法人番号完全一致）
pub fn search_corps_for_dd(
    df: &DataFrame,
    query: &str,
) -> Result<Vec<DdCorpSearchResult>, AppError> {
    if query.is_empty() {
        return Ok(vec![]);
    }

    let groups = build_corp_groups(df)?;
    let query_lower = query.to_lowercase();

    let mut results: Vec<DdCorpSearchResult> = groups
        .into_iter()
        .filter(|(corp_number, g)| {
            // 法人番号完全一致 or 法人名部分一致
            corp_number == query || g.corp_name.to_lowercase().contains(&query_lower)
        })
        .map(|(corp_number, g)| DdCorpSearchResult {
            corp_name: g.corp_name,
            corp_number,
            facility_count: g.facility_count,
            total_staff: g.total_staff,
        })
        .collect();

    // 施設数降順
    results.sort_by(|a, b| b.facility_count.cmp(&a.facility_count));
    results.truncate(50);
    Ok(results)
}

/// DDレポートを生成
pub fn compute_dd_report(df: &DataFrame, corp_number: &str) -> Result<DdReport, AppError> {
    let groups = build_corp_groups(df)?;
    let group = groups
        .get(corp_number)
        .ok_or_else(|| AppError::NotFound(format!("法人番号 {} が見つかりません", corp_number)))?;

    // 法人情報
    let mut prefs: Vec<String> = group.prefectures.iter().cloned().collect();
    prefs.sort();
    let corp_info = DdCorpInfo {
        corp_name: group.corp_name.clone(),
        corp_number: corp_number.to_string(),
        representative: group.representative.clone(),
        facility_count: group.facility_count,
        prefectures: prefs.clone(),
    };

    // 事業DD
    let mut svcs: Vec<String> = group.service_names.iter().cloned().collect();
    svcs.sort();
    let avg_capacity = if group.capacity_count > 0 {
        group.capacity_sum / group.capacity_count as f64
    } else {
        0.0
    };
    let business_dd = DdBusinessDd {
        facilities: group.facility_names.clone(),
        service_types: svcs,
        avg_capacity,
        avg_occupancy: None, // データなし
        total_staff: group.total_staff,
    };

    // 人事DD
    let avg_turnover = if group.turnover_count > 0 {
        Some((group.turnover_sum / group.turnover_count as f64).clamp(0.0, 1.0))
    } else {
        None
    };
    let avg_fulltime = if group.fulltime_ratio_count > 0 {
        Some((group.fulltime_ratio_sum / group.fulltime_ratio_count as f64).clamp(0.0, 1.0))
    } else {
        None
    };
    let hr_dd = DdHrDd {
        avg_turnover_rate: avg_turnover,
        avg_fulltime_ratio: avg_fulltime,
        total_hired: group.hired_sum,
        total_left: group.left_sum,
    };

    // コンプライアンスDD（現在のデータにはないためデフォルト）
    let compliance_dd = DdComplianceDd {
        has_violations: false,
        bcp_rate: None,
        insurance_rate: None,
    };

    // 財務DD（現在のデータにはないためデフォルト）
    let financial_dd = DdFinancialDd {
        accounting_type: None,
        financial_links: vec![],
    };

    // リスクフラグ生成
    let mut risk_flags = Vec::new();

    // 地域平均を取得してベンチマーク用に使用
    let region_avg_turnover = mean_of_col(df, "離職率");
    let region_avg_staff = mean_of_col(df, "従業者_合計_num");
    let region_avg_capacity = mean_of_col(df, "定員_num");

    // 離職率リスク判定
    if let Some(t) = avg_turnover {
        if t > 0.25 {
            risk_flags.push(DdRiskFlag {
                level: "red".to_string(),
                category: "人事".to_string(),
                detail: format!("離職率{:.0}%（地域平均{:.0}%を大幅超過）", t * 100.0, region_avg_turnover * 100.0),
            });
        } else if t > region_avg_turnover * 1.2 {
            risk_flags.push(DdRiskFlag {
                level: "yellow".to_string(),
                category: "人事".to_string(),
                detail: format!("離職率{:.0}%（地域平均{:.0}%をやや超過）", t * 100.0, region_avg_turnover * 100.0),
            });
        } else {
            risk_flags.push(DdRiskFlag {
                level: "green".to_string(),
                category: "人事".to_string(),
                detail: format!("離職率{:.0}%（地域平均内）", t * 100.0),
            });
        }
    } else {
        risk_flags.push(DdRiskFlag {
            level: "yellow".to_string(),
            category: "人事".to_string(),
            detail: "離職率データなし".to_string(),
        });
    }

    // 規模リスク
    if group.facility_count == 1 {
        risk_flags.push(DdRiskFlag {
            level: "yellow".to_string(),
            category: "事業".to_string(),
            detail: "単一施設法人（事業集中リスク）".to_string(),
        });
    }

    // 稼働率データなし
    risk_flags.push(DdRiskFlag {
        level: "yellow".to_string(),
        category: "事業".to_string(),
        detail: "稼働率データなし".to_string(),
    });

    let benchmark = DdBenchmark {
        region_avg_turnover,
        region_avg_staff,
        region_avg_capacity,
    };

    // 加算サマリー: 法人番号で行を絞り込み、施設別加算データを収集
    let kasan_summary = compute_dd_kasan_summary(df, corp_number)?;

    Ok(DdReport {
        corp_info,
        business_dd,
        hr_dd,
        compliance_dd,
        financial_dd,
        risk_flags,
        benchmark,
        kasan_summary,
    })
}

/// DD用: 特定法人の施設別加算サマリーを計算
fn compute_dd_kasan_summary(df: &DataFrame, corp_number: &str) -> Result<DdKasanSummary, AppError> {
    let height = df.height();

    let corp_number_col = df.column("法人番号").ok().and_then(|c| c.str().ok().cloned());
    let name_col = df.column("事業所名").ok().and_then(|c| c.str().ok().cloned());

    // 13加算カラム（ヒートマップと同じ定義）
    let kasan_columns = vec![
        ("処遇改善I", "加算_処遇改善I_f"),
        ("処遇改善II", "加算_処遇改善II_f"),
        ("処遇改善III", "加算_処遇改善III_f"),
        ("処遇改善IV", "加算_処遇改善IV_f"),
        ("特定事業所I", "加算_特定事業所I_f"),
        ("特定事業所II", "加算_特定事業所II_f"),
        ("特定事業所III", "加算_特定事業所III_f"),
        ("特定事業所IV", "加算_特定事業所IV_f"),
        ("特定事業所V", "加算_特定事業所V_f"),
        ("認知症ケアI", "加算_認知症ケアI_f"),
        ("認知症ケアII", "加算_認知症ケアII_f"),
        ("口腔連携", "加算_口腔連携_f"),
        ("緊急時", "加算_緊急時_f"),
    ];

    // 加算カラムを事前取得
    let kasan_cols: Vec<(&str, Option<Float64Chunked>)> = kasan_columns
        .iter()
        .map(|(display_name, col_name)| {
            let ca = df.column(col_name).ok().and_then(|c| c.f64().ok().cloned());
            (*display_name, ca)
        })
        .collect();

    // 加算カラムが1つも存在しない場合はデータなし
    let any_kasan_col_exists = kasan_cols.iter().any(|(_, ca)| ca.is_some());

    if !any_kasan_col_exists || corp_number_col.is_none() {
        return Ok(DdKasanSummary {
            facilities: vec![],
            totals: std::collections::HashMap::new(),
            facility_count: 0,
            has_data: false,
        });
    }

    let corp_num_ca = corp_number_col.as_ref().unwrap();

    // 法人番号に一致する行を収集
    let mut facilities = Vec::new();
    let mut totals: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    // 初期化
    for (display_name, _) in &kasan_columns {
        totals.insert(display_name.to_string(), 0);
    }

    for i in 0..height {
        let cn = match corp_num_ca.get(i) {
            Some(v) if v.trim() == corp_number => v.trim().to_string(),
            _ => continue,
        };

        let facility_name = name_col
            .as_ref()
            .and_then(|ca| ca.get(i))
            .unwrap_or("")
            .to_string();

        let mut kasan_map = std::collections::HashMap::new();
        for (display_name, ref ca_opt) in &kasan_cols {
            let has_kasan = ca_opt
                .as_ref()
                .and_then(|ca| ca.get(i))
                .map(|v| v > 0.5)
                .unwrap_or(false);
            kasan_map.insert(display_name.to_string(), has_kasan);
            if has_kasan {
                *totals.entry(display_name.to_string()).or_insert(0) += 1;
            }
        }

        facilities.push(CorpKasanFacility {
            facility_name,
            kasan: kasan_map,
        });
    }

    let facility_count = facilities.len();
    Ok(DdKasanSummary {
        facilities,
        totals,
        facility_count,
        has_data: facility_count > 0,
    })
}

// ========================================
// Phase 3: PMIシナジー集計
// ========================================

/// PMIシミュレーションを実行
pub fn compute_pmi_simulation(
    df: &DataFrame,
    buyer_corp_number: &str,
    target_corp_number: &str,
) -> Result<PmiSimulationResponse, AppError> {
    let groups = build_corp_groups(df)?;

    let buyer = groups.get(buyer_corp_number).ok_or_else(|| {
        AppError::NotFound(format!(
            "買収側法人番号 {} が見つかりません",
            buyer_corp_number
        ))
    })?;
    let target = groups.get(target_corp_number).ok_or_else(|| {
        AppError::NotFound(format!(
            "ターゲット法人番号 {} が見つかりません",
            target_corp_number
        ))
    })?;

    // 買収側サマリー
    let buyer_summary = PmiCorpSummary {
        corp_name: buyer.corp_name.clone(),
        facilities: buyer.facility_names.clone(),
        total_staff: buyer.total_staff,
    };

    // ターゲットサマリー
    let target_summary = PmiCorpSummary {
        corp_name: target.corp_name.clone(),
        facilities: target.facility_names.clone(),
        total_staff: target.total_staff,
    };

    // 統合後のサービス・エリア分析
    let buyer_svcs: std::collections::HashSet<&String> = buyer.service_names.iter().collect();
    let target_svcs: std::collections::HashSet<&String> = target.service_names.iter().collect();
    let buyer_prefs: std::collections::HashSet<&String> = buyer.prefectures.iter().collect();
    let target_prefs: std::collections::HashSet<&String> = target.prefectures.iter().collect();

    let all_svcs: std::collections::HashSet<&String> =
        buyer_svcs.union(&target_svcs).cloned().collect();
    let all_prefs: std::collections::HashSet<&String> =
        buyer_prefs.union(&target_prefs).cloned().collect();

    let overlap_svcs: Vec<String> = buyer_svcs
        .intersection(&target_svcs)
        .map(|s| (*s).clone())
        .collect();
    let new_svcs: Vec<String> = target_svcs
        .difference(&buyer_svcs)
        .map(|s| (*s).clone())
        .collect();
    let new_prefs: Vec<String> = target_prefs
        .difference(&buyer_prefs)
        .map(|s| (*s).clone())
        .collect();

    let mut svc_coverage: Vec<String> = all_svcs.into_iter().cloned().collect();
    svc_coverage.sort();
    let mut pref_coverage: Vec<String> = all_prefs.into_iter().cloned().collect();
    pref_coverage.sort();

    let combined = PmiCombined {
        total_facilities: buyer.facility_count + target.facility_count,
        total_staff: buyer.total_staff + target.total_staff,
        service_coverage: svc_coverage,
        prefecture_coverage: pref_coverage,
        service_overlap: overlap_svcs,
        new_services: new_svcs,
        new_prefectures: new_prefs,
    };

    // シナジー指標
    let buyer_avg_staff = if buyer.facility_count > 0 {
        buyer.total_staff / buyer.facility_count as f64
    } else {
        0.0
    };
    let target_avg_staff = if target.facility_count > 0 {
        target.total_staff / target.facility_count as f64
    } else {
        0.0
    };
    // 給与ギャップの代替として施設あたり平均従業者数の差を使用
    let wage_gap = (buyer_avg_staff - target_avg_staff).abs();

    let buyer_avg_turnover = if buyer.turnover_count > 0 {
        (buyer.turnover_sum / buyer.turnover_count as f64).clamp(0.0, 1.0)
    } else {
        0.0
    };
    let target_avg_turnover = if target.turnover_count > 0 {
        (target.turnover_sum / target.turnover_count as f64).clamp(0.0, 1.0)
    } else {
        0.0
    };
    let turnover_gap = (buyer_avg_turnover - target_avg_turnover).abs();

    // 人材再配置ポテンシャル: 従業者数差の10%
    let staff_diff = (buyer.total_staff - target.total_staff).abs();
    let reallocation = (staff_diff * 0.1).round();

    let synergy = PmiSynergy {
        wage_gap: (wage_gap * 100.0).round() / 100.0,
        turnover_gap: (turnover_gap * 10000.0).round() / 10000.0,
        staff_reallocation_potential: reallocation,
    };

    Ok(PmiSimulationResponse {
        buyer: buyer_summary,
        target: target_summary,
        combined,
        synergy,
    })
}

// ========================================
// 追加エンドポイント: 集計関数
// ========================================

/// 「経験10年以上割合」カラム（String型 "72.7％"等）をパースしてf64ベクタを返す
fn parse_experience_ratio_values(df: &DataFrame) -> Vec<Option<f64>> {
    if let Ok(col) = df.column("経験10年以上割合") {
        if let Ok(ca) = col.str() {
            return ca.into_iter()
                .map(|opt_val| {
                    opt_val.and_then(|v| {
                        v.trim()
                            .replace("％", "")
                            .replace("%", "")
                            .parse::<f64>()
                            .ok()
                    })
                })
                .collect();
        }
        if let Ok(ca) = col.f64() {
            return ca.into_iter().collect();
        }
    }
    vec![]
}

/// 経験10年以上割合の分布を計算（ヒストグラム用）
pub fn compute_experience_distribution(df: &DataFrame) -> Result<Vec<ExperienceDistribution>, AppError> {
    let values = parse_experience_ratio_values(df);
    let valid_values: Vec<f64> = values.into_iter().filter_map(|v| v).collect();

    if valid_values.is_empty() {
        return Ok(vec![]);
    }

    let ranges = vec![
        ("0-20%", 0.0, 20.0),
        ("20-40%", 20.0, 40.0),
        ("40-60%", 40.0, 60.0),
        ("60-80%", 60.0, 80.0),
        ("80-100%", 80.0, 100.01), // 100%ちょうどを含めるため
    ];

    let results = ranges
        .into_iter()
        .map(|(label, min, max)| {
            let count = valid_values
                .iter()
                .filter(|v| **v >= min && **v < max)
                .count();
            ExperienceDistribution {
                range: label.to_string(),
                count,
            }
        })
        .collect();

    Ok(results)
}

/// 経験者割合 vs 離職率（都道府県別散布図データ）
pub fn compute_experience_vs_turnover(df: &DataFrame) -> Result<Vec<ExperienceVsTurnover>, AppError> {
    let pref_col = match df.column("都道府県") {
        Ok(c) => c.clone(),
        Err(_) => return Ok(vec![]),
    };

    let pref_str = pref_col
        .str()
        .map_err(|e| AppError::Internal(format!("都道府県カラムの型エラー: {}", e)))?;

    let exp_values = parse_experience_ratio_values(df);
    let turnover_col = col_as_f64(df, "離職率");

    if exp_values.is_empty() {
        return Ok(vec![]);
    }

    let unique_prefs: Vec<String> = pref_str
        .into_iter()
        .filter_map(|v| v.map(|s| s.to_string()))
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();

    let mut results = Vec::new();
    for pref in &unique_prefs {
        let mut exp_sum = 0.0;
        let mut exp_count = 0usize;
        let mut turnover_sum = 0.0;
        let mut turnover_count = 0usize;
        let mut facility_count = 0usize;

        for i in 0..df.height() {
            let is_pref = pref_str.get(i).map_or(false, |v| v == pref);
            if !is_pref {
                continue;
            }
            facility_count += 1;

            if let Some(Some(exp_val)) = exp_values.get(i) {
                exp_sum += exp_val;
                exp_count += 1;
            }
            if let Some(ref ca) = turnover_col {
                if let Some(t) = ca.get(i) {
                    turnover_sum += t;
                    turnover_count += 1;
                }
            }
        }

        if exp_count > 0 && turnover_count > 0 {
            results.push(ExperienceVsTurnover {
                prefecture: pref.clone(),
                avg_experience_ratio: (exp_sum / exp_count as f64 * 10.0).round() / 10.0,
                avg_turnover_rate: (turnover_sum / turnover_count as f64 * 100.0 * 10.0).round() / 10.0,
                facility_count,
            });
        }
    }

    results.sort_by(|a, b| b.facility_count.cmp(&a.facility_count));
    Ok(results)
}

/// 品質ランク別分布を計算
pub fn compute_quality_rank_distribution(df: &DataFrame) -> Result<Vec<QualityRankDistribution>, AppError> {
    let rank_col = match df.column("品質ランク") {
        Ok(c) => c.clone(),
        Err(_) => return Ok(vec![]),
    };

    let rank_str = rank_col
        .str()
        .map_err(|e| AppError::Internal(format!("品質ランクカラムの型エラー: {}", e)))?;

    // ランクと色のマッピング（順序保持）
    let rank_colors = vec![
        ("S", "#10b981"),
        ("A", "#3b82f6"),
        ("B", "#6366f1"),
        ("C", "#f59e0b"),
        ("D", "#ef4444"),
    ];

    let results = rank_colors
        .into_iter()
        .map(|(rank, color)| {
            let count = rank_str
                .into_iter()
                .filter(|opt_val| opt_val.map_or(false, |v| v == rank))
                .count();
            QualityRankDistribution {
                rank: rank.to_string(),
                count,
                color: color.to_string(),
            }
        })
        .collect();

    Ok(results)
}

/// 品質カテゴリ別平均スコアを計算（レーダーチャート用）
pub fn compute_quality_category_radar(df: &DataFrame) -> Result<Vec<QualityCategoryRadar>, AppError> {
    let height = df.height();
    if height == 0 {
        return Ok(vec![]);
    }

    // 各品質フラグカラムを取得
    let bcp_flags = col_as_f64(df, "BCP策定フラグ");
    let insurance_flags = col_as_f64(df, "損害賠償保険フラグ");
    let third_party_flags = col_as_f64(df, "第三者評価フラグ");
    let ict_flags = col_as_f64(df, "ICT活用フラグ");
    let turnover_col = col_as_f64(df, "離職率");
    let fulltime_col = col_as_f64(df, "常勤比率");
    let exp_values = parse_experience_ratio_values(df);
    let occupancy_col = col_as_f64(df, "稼働率");
    let kasan_col = col_as_f64(df, "加算取得数");

    // いずれかのデータがないと計算不可
    if bcp_flags.is_none() && insurance_flags.is_none() && third_party_flags.is_none() {
        return Ok(vec![]);
    }

    // 安全・リスク管理（30点満点）: BCP(10) + 保険(10) + 処分なし(10)
    let mut safety_sum = 0.0;
    let mut safety_count = 0usize;
    for i in 0..height {
        let mut score = 0.0;
        let mut has_data = false;
        if let Some(ref ca) = bcp_flags {
            if let Some(v) = ca.get(i) {
                if v > 0.5 { score += 10.0; }
                has_data = true;
            }
        }
        if let Some(ref ca) = insurance_flags {
            if let Some(v) = ca.get(i) {
                if v > 0.5 { score += 10.0; }
                has_data = true;
            }
        }
        // 行政処分なし = 10点（データ未実装のため全施設加点）
        if has_data {
            score += 10.0;
            safety_sum += score;
            safety_count += 1;
        }
    }

    // 品質管理（25点満点）: 第三者評価(15) + ICT(10)
    let mut quality_sum = 0.0;
    let mut quality_count = 0usize;
    for i in 0..height {
        let mut score = 0.0;
        let mut has_data = false;
        if let Some(ref ca) = third_party_flags {
            if let Some(v) = ca.get(i) {
                if v > 0.5 { score += 15.0; }
                has_data = true;
            }
        }
        if let Some(ref ca) = ict_flags {
            if let Some(v) = ca.get(i) {
                if v > 0.5 { score += 10.0; }
                has_data = true;
            }
        }
        if has_data {
            quality_sum += score;
            quality_count += 1;
        }
    }

    // 人材安定性（25点満点）: 低離職率(10) + 高常勤比率(8) + 経験10年以上(7)
    let mut hr_sum = 0.0;
    let mut hr_count = 0usize;
    for i in 0..height {
        let mut score = 0.0;
        let mut has_data = false;
        if let Some(ref ca) = turnover_col {
            if let Some(v) = ca.get(i) {
                if v < 0.15 { score += 10.0; }
                has_data = true;
            }
        }
        if let Some(ref ca) = fulltime_col {
            if let Some(v) = ca.get(i) {
                if v > 0.5 { score += 8.0; }
                has_data = true;
            }
        }
        if let Some(Some(ratio)) = exp_values.get(i) {
            if *ratio > 30.0 { score += 7.0; }
            has_data = true;
        }
        if has_data {
            hr_sum += score;
            hr_count += 1;
        }
    }

    // 収益安定性（20点満点）: 高稼働率(10) + 多加算取得(10)
    let mut revenue_sum = 0.0;
    let mut revenue_count = 0usize;
    for i in 0..height {
        let mut score = 0.0;
        let mut has_data = false;
        if let Some(ref ca) = occupancy_col {
            if let Some(v) = ca.get(i) {
                if v > 0.8 { score += 10.0; }
                has_data = true;
            }
        }
        if let Some(ref ca) = kasan_col {
            if let Some(v) = ca.get(i) {
                if v >= 5.0 { score += 10.0; }
                has_data = true;
            }
        }
        if has_data {
            revenue_sum += score;
            revenue_count += 1;
        }
    }

    let results = vec![
        QualityCategoryRadar {
            category: "安全・リスク管理".to_string(),
            score: if safety_count > 0 { (safety_sum / safety_count as f64 * 10.0).round() / 10.0 } else { 0.0 },
            full_mark: 30.0,
        },
        QualityCategoryRadar {
            category: "品質管理".to_string(),
            score: if quality_count > 0 { (quality_sum / quality_count as f64 * 10.0).round() / 10.0 } else { 0.0 },
            full_mark: 25.0,
        },
        QualityCategoryRadar {
            category: "人材安定性".to_string(),
            score: if hr_count > 0 { (hr_sum / hr_count as f64 * 10.0).round() / 10.0 } else { 0.0 },
            full_mark: 25.0,
        },
        QualityCategoryRadar {
            category: "収益安定性".to_string(),
            score: if revenue_count > 0 { (revenue_sum / revenue_count as f64 * 10.0).round() / 10.0 } else { 0.0 },
            full_mark: 20.0,
        },
    ];

    Ok(results)
}

/// 法人内施設の加算取得ヒートマップ（上位法人のみ）
pub fn compute_corp_kasan_heatmap(df: &DataFrame, top_n: usize) -> Result<CorpKasanHeatmapResponse, AppError> {
    let height = df.height();

    // 必要カラムを取得
    let corp_number_col = df.column("法人番号").ok().and_then(|c| c.str().ok().cloned());
    let corp_name_col = df.column("法人名").ok().and_then(|c| c.str().ok().cloned());
    let name_col = df.column("事業所名").ok().and_then(|c| c.str().ok().cloned());

    if corp_number_col.is_none() {
        return Ok(CorpKasanHeatmapResponse { corps: vec![] });
    }

    // 13加算カラム
    let kasan_columns = vec![
        ("処遇改善I", "加算_処遇改善I_f"),
        ("処遇改善II", "加算_処遇改善II_f"),
        ("処遇改善III", "加算_処遇改善III_f"),
        ("処遇改善IV", "加算_処遇改善IV_f"),
        ("特定事業所I", "加算_特定事業所I_f"),
        ("特定事業所II", "加算_特定事業所II_f"),
        ("特定事業所III", "加算_特定事業所III_f"),
        ("特定事業所IV", "加算_特定事業所IV_f"),
        ("特定事業所V", "加算_特定事業所V_f"),
        ("認知症ケアI", "加算_認知症ケアI_f"),
        ("認知症ケアII", "加算_認知症ケアII_f"),
        ("口腔連携", "加算_口腔連携_f"),
        ("緊急時", "加算_緊急時_f"),
    ];

    // 加算カラムの存在チェック
    let kasan_cols: Vec<(&str, Option<Float64Chunked>)> = kasan_columns
        .iter()
        .map(|(display_name, col_name)| {
            let ca = df.column(col_name).ok().and_then(|c| c.f64().ok().cloned());
            (*display_name, ca)
        })
        .collect();

    // 法人番号別に施設をグループ化
    let corp_num_ca = match corp_number_col.as_ref() {
        Some(ca) => ca,
        None => return Ok(CorpKasanHeatmapResponse { corps: vec![] }),
    };
    let mut corp_facility_count: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for i in 0..height {
        if let Some(cn) = corp_num_ca.get(i) {
            let cn = cn.trim();
            if !cn.is_empty() {
                *corp_facility_count.entry(cn.to_string()).or_insert(0) += 1;
            }
        }
    }

    // 施設数上位法人を取得
    let mut sorted_corps: Vec<_> = corp_facility_count.into_iter().collect();
    sorted_corps.sort_by(|a, b| b.1.cmp(&a.1));
    sorted_corps.truncate(top_n);
    let top_corp_numbers: std::collections::HashSet<String> = sorted_corps.iter().map(|(cn, _)| cn.clone()).collect();

    // 各法人の施設データを収集
    let mut corp_data: std::collections::HashMap<String, (String, Vec<CorpKasanFacility>)> = std::collections::HashMap::new();

    for i in 0..height {
        let corp_num = match corp_num_ca.get(i) {
            Some(v) if !v.trim().is_empty() && top_corp_numbers.contains(v.trim()) => v.trim().to_string(),
            _ => continue,
        };

        let corp_name = corp_name_col
            .as_ref()
            .and_then(|ca| ca.get(i))
            .unwrap_or("")
            .to_string();

        let facility_name = name_col
            .as_ref()
            .and_then(|ca| ca.get(i))
            .unwrap_or("")
            .to_string();

        let mut kasan_map = std::collections::HashMap::new();
        for (display_name, ref ca_opt) in &kasan_cols {
            let has_kasan = ca_opt
                .as_ref()
                .and_then(|ca| ca.get(i))
                .map(|v| v > 0.5)
                .unwrap_or(false);
            kasan_map.insert(display_name.to_string(), has_kasan);
        }

        let entry = corp_data.entry(corp_num).or_insert_with(|| (corp_name, Vec::new()));
        entry.1.push(CorpKasanFacility {
            facility_name,
            kasan: kasan_map,
        });
    }

    // 結果を施設数降順で構築（各法人は上位20施設に制限）
    let mut corps: Vec<CorpKasanHeatmapEntry> = corp_data
        .into_iter()
        .map(|(_corp_num, (corp_name, mut facilities))| {
            facilities.truncate(20);
            CorpKasanHeatmapEntry {
                corp_name,
                facilities,
            }
        })
        .collect();

    corps.sort_by(|a, b| b.facilities.len().cmp(&a.facilities.len()));

    Ok(CorpKasanHeatmapResponse { corps })
}

/// 施設ベンチマーク（8軸レーダー + パーセンタイル）
pub fn compute_benchmark(df: &DataFrame, jigyosho_number: &str) -> Result<BenchmarkResponse, AppError> {
    // 事業所番号で施設を検索
    let jig_col = df
        .column("事業所番号")
        .map_err(|e| AppError::Internal(format!("事業所番号カラムが見つかりません: {}", e)))?;
    let jig_str = jig_col
        .str()
        .map_err(|e| AppError::Internal(format!("事業所番号カラムの型エラー: {}", e)))?;

    let target_idx = jig_str
        .into_iter()
        .position(|opt| opt.map_or(false, |v| v.trim() == jigyosho_number))
        .ok_or_else(|| AppError::NotFound(format!("事業所番号 {} が見つかりません", jigyosho_number)))?;

    // 対象施設の各指標値を取得
    let get_f64_val = |col_name: &str| -> Option<f64> {
        col_as_f64(df, col_name).and_then(|ca| ca.get(target_idx))
    };

    let get_str_val = |col_name: &str| -> Option<String> {
        df.column(col_name).ok()
            .and_then(|c| c.str().ok().cloned())
            .and_then(|ca| ca.get(target_idx).map(|s| s.to_string()))
    };

    let target_turnover = get_f64_val("離職率").map(|v| v * 100.0);
    let target_fulltime = get_f64_val("常勤比率").map(|v| v * 100.0);
    let target_occupancy = get_f64_val("稼働率").map(|v| v * 100.0);
    let target_kasan = get_f64_val("加算取得数");
    let target_quality = get_f64_val("品質スコア");
    let target_severe = get_f64_val("重度者割合").map(|v| v * 100.0);

    // 経験者割合をパース
    let exp_values = parse_experience_ratio_values(df);
    let target_experience = exp_values.get(target_idx).and_then(|v| *v);

    // 定員充足率 = 利用者数 / 定員
    let target_user_count = get_f64_val("利用者総数_num");
    let target_capacity = get_f64_val("定員_num");
    let target_capacity_ratio = match (target_user_count, target_capacity) {
        (Some(u), Some(c)) if c > 0.0 => Some((u / c) * 100.0),
        _ => None,
    };

    let target_prefecture = get_str_val("都道府県").unwrap_or_default();
    let target_service = get_str_val("サービス名").unwrap_or_default();
    let target_jigyosho_name = get_str_val("事業所名").unwrap_or_default();

    // 全国平均、都道府県平均を計算するヘルパー
    let compute_avg = |col_name: &str, filter_col: Option<(&str, &str)>| -> f64 {
        if let Some((fcol, fval)) = filter_col {
            if let Ok(fc) = df.column(fcol) {
                if let Ok(fca) = fc.str() {
                    let vals = col_as_f64(df, col_name);
                    if let Some(ca) = vals {
                        let mut sum = 0.0;
                        let mut count = 0usize;
                        for i in 0..df.height() {
                            if fca.get(i).map_or(false, |v| v == fval) {
                                if let Some(v) = ca.get(i) {
                                    sum += v;
                                    count += 1;
                                }
                            }
                        }
                        if count > 0 { return sum / count as f64; }
                    }
                }
            }
            0.0
        } else {
            mean_of_col(df, col_name)
        }
    };

    let compute_exp_avg = |filter_col: Option<(&str, &str)>| -> f64 {
        if let Some((fcol, fval)) = filter_col {
            if let Ok(fc) = df.column(fcol) {
                if let Ok(fca) = fc.str() {
                    let mut sum = 0.0;
                    let mut count = 0usize;
                    for i in 0..df.height() {
                        if fca.get(i).map_or(false, |v| v == fval) {
                            if let Some(Some(v)) = exp_values.get(i) {
                                sum += v;
                                count += 1;
                            }
                        }
                    }
                    if count > 0 { return sum / count as f64; }
                }
            }
            0.0
        } else {
            let valid: Vec<f64> = exp_values.iter().filter_map(|v| *v).collect();
            if valid.is_empty() { 0.0 } else { valid.iter().sum::<f64>() / valid.len() as f64 }
        }
    };

    // 定員充足率の平均を計算
    let compute_capacity_ratio_avg = |filter_col: Option<(&str, &str)>| -> f64 {
        let user_ca = col_as_f64(df, "利用者総数_num");
        let cap_ca = col_as_f64(df, "定員_num");
        match (user_ca, cap_ca) {
            (Some(uca), Some(cca)) => {
                let mut sum = 0.0;
                let mut count = 0usize;
                for i in 0..df.height() {
                    let pass_filter = if let Some((fcol, fval)) = filter_col {
                        df.column(fcol).ok()
                            .and_then(|c| c.str().ok().cloned())
                            .and_then(|fca| fca.get(i).map(|v| v == fval))
                            .unwrap_or(false)
                    } else {
                        true
                    };
                    if pass_filter {
                        if let (Some(u), Some(c)) = (uca.get(i), cca.get(i)) {
                            if c > 0.0 {
                                sum += (u / c) * 100.0;
                                count += 1;
                            }
                        }
                    }
                }
                if count > 0 { sum / count as f64 } else { 0.0 }
            }
            _ => 0.0,
        }
    };

    // 8軸のレーダーデータ構築
    let pref_filter = Some(("都道府県", target_prefecture.as_str()));
    let svc_filter = if target_service.is_empty() { None } else { Some(("サービス名", target_service.as_str())) };

    let radar = vec![
        BenchmarkRadarAxis {
            axis: "離職率".to_string(),
            value: target_turnover.unwrap_or(0.0),
            national_avg: (compute_avg("離職率", None) * 100.0 * 10.0).round() / 10.0,
            pref_avg: (compute_avg("離職率", pref_filter) * 100.0 * 10.0).round() / 10.0,
        },
        BenchmarkRadarAxis {
            axis: "常勤比率".to_string(),
            value: target_fulltime.unwrap_or(0.0),
            national_avg: (compute_avg("常勤比率", None) * 100.0 * 10.0).round() / 10.0,
            pref_avg: (compute_avg("常勤比率", pref_filter) * 100.0 * 10.0).round() / 10.0,
        },
        BenchmarkRadarAxis {
            axis: "稼働率".to_string(),
            value: target_occupancy.unwrap_or(0.0),
            national_avg: (compute_avg("稼働率", None) * 100.0 * 10.0).round() / 10.0,
            pref_avg: (compute_avg("稼働率", pref_filter) * 100.0 * 10.0).round() / 10.0,
        },
        BenchmarkRadarAxis {
            axis: "加算取得数".to_string(),
            value: target_kasan.unwrap_or(0.0),
            national_avg: (mean_of_col(df, "加算取得数") * 10.0).round() / 10.0,
            pref_avg: (compute_avg("加算取得数", pref_filter) * 10.0).round() / 10.0,
        },
        BenchmarkRadarAxis {
            axis: "品質スコア".to_string(),
            value: target_quality.unwrap_or(0.0),
            national_avg: (mean_of_col(df, "品質スコア") * 10.0).round() / 10.0,
            pref_avg: (compute_avg("品質スコア", pref_filter) * 10.0).round() / 10.0,
        },
        BenchmarkRadarAxis {
            axis: "経験者割合".to_string(),
            value: target_experience.unwrap_or(0.0),
            national_avg: (compute_exp_avg(None) * 10.0).round() / 10.0,
            pref_avg: (compute_exp_avg(pref_filter) * 10.0).round() / 10.0,
        },
        BenchmarkRadarAxis {
            axis: "重度率".to_string(),
            value: target_severe.unwrap_or(0.0),
            national_avg: (compute_avg("重度者割合", None) * 100.0 * 10.0).round() / 10.0,
            pref_avg: (compute_avg("重度者割合", pref_filter) * 100.0 * 10.0).round() / 10.0,
        },
        BenchmarkRadarAxis {
            axis: "定員充足率".to_string(),
            value: target_capacity_ratio.unwrap_or(0.0),
            national_avg: (compute_capacity_ratio_avg(None) * 10.0).round() / 10.0,
            pref_avg: (compute_capacity_ratio_avg(pref_filter) * 10.0).round() / 10.0,
        },
    ];

    // パーセンタイル計算
    let compute_percentile = |col_name: &str, target_val: f64, multiply: f64, filter_col: Option<(&str, &str)>| -> f64 {
        let vals: Vec<f64> = if let Some(ca) = col_as_f64(df, col_name) {
            let filter_ca = filter_col.and_then(|(fcol, _)| {
                df.column(fcol).ok().and_then(|c| c.str().ok().cloned())
            });
            (0..df.height())
                .filter_map(|i| {
                    let pass = if let Some((_, fval)) = filter_col {
                        filter_ca.as_ref().and_then(|fca| fca.get(i)).map_or(false, |v| v == fval)
                    } else {
                        true
                    };
                    if pass { ca.get(i).map(|v| v * multiply) } else { None }
                })
                .collect()
        } else {
            vec![]
        };
        if vals.is_empty() { return 0.0; }
        let below_count = vals.iter().filter(|v| **v <= target_val).count();
        (below_count as f64 / vals.len() as f64 * 100.0).round()
    };

    let compute_exp_percentile = |target_val: f64, filter_col: Option<(&str, &str)>| -> f64 {
        let filter_ca = filter_col.and_then(|(fcol, _)| {
            df.column(fcol).ok().and_then(|c| c.str().ok().cloned())
        });
        let vals: Vec<f64> = (0..df.height())
            .filter_map(|i| {
                let pass = if let Some((_, fval)) = filter_col {
                    filter_ca.as_ref().and_then(|fca| fca.get(i)).map_or(false, |v| v == fval)
                } else {
                    true
                };
                if pass { exp_values.get(i).and_then(|v| *v) } else { None }
            })
            .collect();
        if vals.is_empty() { return 0.0; }
        let below_count = vals.iter().filter(|v| **v <= target_val).count();
        (below_count as f64 / vals.len() as f64 * 100.0).round()
    };

    // 離職率は低い方が良いので、反転パーセンタイル（低い=高パーセンタイル）
    let turnover_val = target_turnover.unwrap_or(0.0);
    let fulltime_val = target_fulltime.unwrap_or(0.0);
    let occupancy_val = target_occupancy.unwrap_or(0.0);
    let kasan_val = target_kasan.unwrap_or(0.0);
    let quality_val = target_quality.unwrap_or(0.0);
    let exp_val = target_experience.unwrap_or(0.0);
    let severe_val = target_severe.unwrap_or(0.0);
    let cap_ratio_val = target_capacity_ratio.unwrap_or(0.0);

    let percentiles = serde_json::json!({
        "national": {
            "離職率": 100.0 - compute_percentile("離職率", turnover_val / 100.0, 1.0, None),
            "常勤比率": compute_percentile("常勤比率", fulltime_val / 100.0, 1.0, None),
            "稼働率": compute_percentile("稼働率", occupancy_val / 100.0, 1.0, None),
            "加算取得数": compute_percentile("加算取得数", kasan_val, 1.0, None),
            "品質スコア": compute_percentile("品質スコア", quality_val, 1.0, None),
            "経験者割合": compute_exp_percentile(exp_val, None),
            "重度率": compute_percentile("重度者割合", severe_val / 100.0, 1.0, None),
            "定員充足率": cap_ratio_val, // 簡易版
        },
        "prefecture": {
            "離職率": 100.0 - compute_percentile("離職率", turnover_val / 100.0, 1.0, pref_filter),
            "常勤比率": compute_percentile("常勤比率", fulltime_val / 100.0, 1.0, pref_filter),
            "稼働率": compute_percentile("稼働率", occupancy_val / 100.0, 1.0, pref_filter),
            "加算取得数": compute_percentile("加算取得数", kasan_val, 1.0, pref_filter),
            "品質スコア": compute_percentile("品質スコア", quality_val, 1.0, pref_filter),
            "経験者割合": compute_exp_percentile(exp_val, pref_filter),
            "重度率": compute_percentile("重度者割合", severe_val / 100.0, 1.0, pref_filter),
            "定員充足率": cap_ratio_val,
        },
        "service": {
            "離職率": 100.0 - compute_percentile("離職率", turnover_val / 100.0, 1.0, svc_filter),
            "常勤比率": compute_percentile("常勤比率", fulltime_val / 100.0, 1.0, svc_filter),
            "稼働率": compute_percentile("稼働率", occupancy_val / 100.0, 1.0, svc_filter),
            "加算取得数": compute_percentile("加算取得数", kasan_val, 1.0, svc_filter),
            "品質スコア": compute_percentile("品質スコア", quality_val, 1.0, svc_filter),
            "経験者割合": compute_exp_percentile(exp_val, svc_filter),
            "重度率": compute_percentile("重度者割合", severe_val / 100.0, 1.0, svc_filter),
            "定員充足率": cap_ratio_val,
        }
    });

    // 改善提案生成
    // 各指標の全国平均を計算し、施設値と比較して提案を生成
    let national_avg_kasan = mean_of_col(df, "加算取得数");
    let national_avg_quality = mean_of_col(df, "品質スコア");
    let national_avg_turnover = mean_of_col_clamped(df, "離職率", 0.0, 1.0) * 100.0;
    let national_avg_fulltime = mean_of_col_clamped(df, "常勤比率", 0.0, 1.0) * 100.0;
    let national_avg_occupancy = mean_of_col_filtered(df, "稼働率", 0.0, 3.0) * 100.0;
    let national_avg_exp = {
        let vals = parse_experience_ratio_values(df);
        let valid: Vec<f64> = vals.into_iter().filter_map(|v| v).collect();
        if valid.is_empty() { 0.0 } else { valid.iter().sum::<f64>() / valid.len() as f64 }
    };

    let mut suggestions = Vec::new();

    // 加算取得数が全国平均未満（値が有効な場合のみ）
    if target_kasan.is_some() && kasan_val < national_avg_kasan {
        suggestions.push(ImprovementSuggestion {
            axis: "加算取得数".to_string(),
            current: kasan_val,
            target: (national_avg_kasan.ceil()).min(13.0),
            suggestion: "処遇改善加算や特定事業所加算の追加取得を検討".to_string(),
        });
    }

    // 品質スコアが全国平均未満（値が有効な場合のみ）
    if target_quality.is_some() && quality_val < national_avg_quality {
        suggestions.push(ImprovementSuggestion {
            axis: "品質スコア".to_string(),
            current: quality_val,
            target: national_avg_quality.ceil(),
            suggestion: "BCP策定やICT導入で品質スコア向上を検討".to_string(),
        });
    }

    // 離職率が全国平均超過（値が有効な場合のみ）
    if target_turnover.is_some() && turnover_val > national_avg_turnover && national_avg_turnover > 0.0 {
        suggestions.push(ImprovementSuggestion {
            axis: "離職率".to_string(),
            current: turnover_val,
            target: (national_avg_turnover * 10.0).round() / 10.0,
            suggestion: "職場環境改善や処遇改善による離職率低減を検討".to_string(),
        });
    }

    // 常勤比率が全国平均未満（値が有効な場合のみ）
    if target_fulltime.is_some() && fulltime_val < national_avg_fulltime && national_avg_fulltime > 0.0 {
        suggestions.push(ImprovementSuggestion {
            axis: "常勤比率".to_string(),
            current: fulltime_val,
            target: (national_avg_fulltime * 10.0).round() / 10.0,
            suggestion: "正社員登用や常勤化推進による安定した人材確保を検討".to_string(),
        });
    }

    // 稼働率が全国平均未満（値が有効な場合のみ）
    if target_occupancy.is_some() && occupancy_val < national_avg_occupancy && national_avg_occupancy > 0.0 {
        suggestions.push(ImprovementSuggestion {
            axis: "稼働率".to_string(),
            current: occupancy_val,
            target: (national_avg_occupancy * 10.0).round() / 10.0,
            suggestion: "地域ケアマネとの連携強化や営業活動による利用率向上を検討".to_string(),
        });
    }

    // 経験者割合が全国平均未満（値が有効な場合のみ）
    if target_experience.is_some() && exp_val < national_avg_exp && national_avg_exp > 0.0 {
        suggestions.push(ImprovementSuggestion {
            axis: "経験者割合".to_string(),
            current: exp_val,
            target: (national_avg_exp * 10.0).round() / 10.0,
            suggestion: "経験者採用の強化やベテラン職員の定着支援を検討".to_string(),
        });
    }

    // 施設基本情報
    let facility = serde_json::json!({
        "jigyosho_number": jigyosho_number,
        "jigyosho_name": target_jigyosho_name,
        "prefecture": target_prefecture,
        "service_name": target_service,
    });

    Ok(BenchmarkResponse {
        facility,
        radar,
        percentiles,
        improvement_suggestions: suggestions,
    })
}

// ========================================
// ヘルパー関数
// ========================================

/// カラムからFloat64 ChunkedArrayを取得するヘルパー
/// Float64型ならそのまま、String型ならパースしてFloat64に変換
fn col_as_f64(df: &DataFrame, col_name: &str) -> Option<Float64Chunked> {
    let col = df.column(col_name).ok()?;
    // まずFloat64として直接取得を試みる
    if let Ok(ca) = col.f64() {
        return Some(ca.clone());
    }
    // String型の場合はパースして変換
    if let Ok(ca) = col.str() {
        let parsed: Float64Chunked = ca
            .into_iter()
            .map(|opt_val| opt_val.and_then(|v| v.trim().parse::<f64>().ok()))
            .collect();
        return Some(parsed);
    }
    None
}

/// DataFrameの指定カラムの平均値を計算するヘルパー
/// カラムが存在しない or 空の場合は0.0を返す
fn mean_of_col(df: &DataFrame, col_name: &str) -> f64 {
    col_as_f64(df, col_name)
        .and_then(|ca| ca.mean())
        .unwrap_or(0.0)
}

/// DataFrameの指定カラムの平均値を計算し、指定範囲にクランプするヘルパー
/// 離職率・常勤比率など0.0-1.0の範囲に収めるべき指標に使用
fn mean_of_col_clamped(df: &DataFrame, col_name: &str, min: f64, max: f64) -> f64 {
    mean_of_col(df, col_name).clamp(min, max)
}

/// DataFrameの指定カラムの平均値を計算（値の範囲フィルタ付き）
/// 定員や稼働率など異常値を除外して平均を計算する
fn mean_of_col_filtered(df: &DataFrame, col_name: &str, min: f64, max: f64) -> f64 {
    col_as_f64(df, col_name)
        .map(|ca| {
            let valid: Vec<f64> = ca.into_iter()
                .filter_map(|v| v)
                .filter(|v| *v >= min && *v <= max)
                .collect();
            if valid.is_empty() { 0.0 } else { valid.iter().sum::<f64>() / valid.len() as f64 }
        })
        .unwrap_or(0.0)
}

/// DataFrameの指定カラムの平均値を計算（値の範囲フィルタ付き、Option版）
fn mean_of_col_filtered_opt(df: &DataFrame, col_name: &str, min: f64, max: f64) -> Option<f64> {
    col_as_f64(df, col_name)
        .and_then(|ca| {
            let valid: Vec<f64> = ca.into_iter()
                .filter_map(|v| v)
                .filter(|v| *v >= min && *v <= max)
                .collect();
            if valid.is_empty() { None } else { Some(valid.iter().sum::<f64>() / valid.len() as f64) }
        })
}

/// DataFrameの指定カラムの平均値を計算（Option版）
/// カラムが存在しない場合はNoneを返す
fn mean_of_col_opt(df: &DataFrame, col_name: &str) -> Option<f64> {
    col_as_f64(df, col_name)
        .and_then(|ca| ca.mean())
}

/// DataFrameの指定カラムの中央値を計算
/// カラムが存在しない場合はNoneを返す
fn median_of_col(df: &DataFrame, col_name: &str) -> Option<f64> {
    col_as_f64(df, col_name).and_then(|ca| {
        let mut vals: Vec<f64> = ca.into_iter().filter_map(|v| v).collect();
        if vals.is_empty() {
            return None;
        }
        vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let mid = vals.len() / 2;
        if vals.len() % 2 == 0 {
            Some((vals[mid - 1] + vals[mid]) / 2.0)
        } else {
            Some(vals[mid])
        }
    })
}

/// 2カラムの比率の平均を計算（分子 / 分母）
/// どちらかのカラムが存在しない場合はNoneを返す
fn compute_ratio_mean(df: &DataFrame, numerator_col: &str, denominator_col: &str) -> Option<f64> {
    let num = col_as_f64(df, numerator_col);
    let den = col_as_f64(df, denominator_col);

    match (num, den) {
        (Some(num_ca), Some(den_ca)) => {
            let ratios: Vec<f64> = num_ca
                .into_iter()
                .zip(den_ca.into_iter())
                .filter_map(|(n, d)| match (n, d) {
                    (Some(n_val), Some(d_val)) if d_val > 0.0 => Some(n_val / d_val),
                    _ => None,
                })
                .collect();
            if ratios.is_empty() {
                None
            } else {
                Some(ratios.iter().sum::<f64>() / ratios.len() as f64)
            }
        }
        _ => None,
    }
}

/// 指定カラムのf64値をVecで取得（null除外）
fn get_f64_col_values(df: &DataFrame, col_name: &str) -> Vec<f64> {
    col_as_f64(df, col_name)
        .map(|ca| ca.into_iter().filter_map(|v| v).collect())
        .unwrap_or_default()
}

/// 指定カラムのOption<f64>値をVecで取得
/// Float64型を優先し、String型の場合はパースにフォールバックする
fn get_f64_col_opt_values(df: &DataFrame, col_name: &str) -> Vec<Option<f64>> {
    if let Ok(col) = df.column(col_name) {
        // まずFloat64として直接取得を試みる
        if let Ok(ca) = col.f64() {
            return ca.into_iter().collect();
        }
        // String型の場合はパースして変換
        if let Ok(ca) = col.str() {
            return ca.into_iter().map(|opt| {
                opt.and_then(|s| s.trim().parse::<f64>().ok())
            }).collect();
        }
    }
    vec![]
}
