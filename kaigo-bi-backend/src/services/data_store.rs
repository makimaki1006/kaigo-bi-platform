/// データストア
/// Parquet/CSV/Tursoからデータを読み込み、派生カラムを計算してメモリに保持
/// Arc<DataStore>としてAxum Stateに注入される
///
/// データソース優先度:
/// 1. ローカルファイル（CSV/Parquet）が存在すればそこから読み込み（開発用）
/// 2. ローカルファイルがなければTursoから読み込み（本番/Render用）

use libsql::Database;
use polars::prelude::*;
use std::path::Path;
use tracing::info;

use chrono::Datelike;
use crate::error::AppError;
use crate::models::facility::Facility;
use crate::models::filters::FilterParams;
use crate::utils::corp_type::classify_corp_type;
use crate::utils::prefecture::extract_prefecture_with_fallback;

/// 全角数字・ハイフンを半角に変換する（CSVフォールバック時用）
/// Tursoから読み込む場合はスクレイパーで変換済みなので不要
fn normalize_phone(phone: &str) -> String {
    phone
        .chars()
        .map(|c| match c {
            '\u{FF10}'..='\u{FF19}' => {
                // 全角数字（０-９）→半角数字（0-9）
                (c as u32 - '\u{FF10}' as u32 + '0' as u32) as u8 as char
            }
            '\u{FF0D}' => '-',  // 全角ハイフン（－）
            '\u{FF08}' => '(',  // 全角左括弧（（）
            '\u{FF09}' => ')',  // 全角右括弧（））
            '\u{3000}' => ' ',  // 全角スペース（　）
            _ => c,
        })
        .collect::<String>()
        .trim()
        .to_string()
}

/// インメモリデータストア
/// 起動時に全データを読み込み、フィルタ/集計はPolars LazyFrameで実行
pub struct DataStore {
    /// 元データ + 派生カラムを含むDataFrame
    pub df: DataFrame,
}

impl DataStore {
    /// データを読み込んでDataStoreを初期化
    /// ローカルファイルがあればそこから読み込み（開発用）、
    /// なければTursoから読み込み（本番用）
    pub async fn load(path: &str, db: Option<&Database>) -> Result<Self, AppError> {
        let file_path = Path::new(path);
        let (df, from_local_file) = if file_path.exists() {
            // ローカルファイルがあればそこから読み込み（開発用）
            if file_path.extension().map_or(false, |ext| ext == "parquet") {
                info!("Parquetファイルを読み込み: {:?}", file_path);
                let df = ParquetReader::new(std::fs::File::open(file_path).map_err(|e| {
                    AppError::Internal(format!("ファイルを開けません: {}", e))
                })?)
                .finish()
                .map_err(|e| AppError::Internal(format!("Parquet読み込みエラー: {}", e)))?;
                (df, true)
            } else {
                info!("CSVファイルを読み込み: {:?}", file_path);
                (Self::read_csv_with_bom_skip(file_path)?, true)
            }
        } else if let Some(database) = db {
            // ローカルファイルがなければTursoから読み込み（本番用）
            info!("ローカルファイルが見つかりません: {:?}", file_path);
            info!("Tursoデータベースからデータを読み込みます");
            (Self::read_from_turso(database).await?, false)
        } else {
            return Err(AppError::Internal(
                "データソースがありません。ローカルファイルもTurso接続もありません。".into(),
            ));
        };

        info!("読み込み完了: {} 行 x {} 列", df.height(), df.width());

        // CSVフォールバック時のみ電話番号の全角→半角変換を適用
        // Tursoから読み込む場合はスクレイパーで変換済みなので不要
        let df = if from_local_file {
            Self::normalize_phone_columns(df)?
        } else {
            df
        };

        // Tursoから読み込んだ場合は派生カラムが既に含まれているが、
        // CSVカラム名との互換性のため compute_derived_columns を通す
        let df = Self::compute_derived_columns(df)?;

        info!(
            "派生カラム計算完了: {} 行 x {} 列",
            df.height(),
            df.width()
        );

        Ok(Self { df })
    }

    /// CSVフォールバック時に電話番号カラムの全角→半角変換を適用する
    /// Tursoから読み込む場合はスクレイパーで変換済みなので不要
    fn normalize_phone_columns(mut df: DataFrame) -> Result<DataFrame, AppError> {
        for col_name in &["電話番号", "FAX番号"] {
            if let Ok(col) = df.column(*col_name) {
                if let Ok(ca) = col.str() {
                    // 全角→半角変換を適用してVecに格納
                    let normalized: Vec<Option<String>> = ca
                        .into_iter()
                        .map(|opt_val| opt_val.map(normalize_phone))
                        .collect();
                    let series = Series::new((*col_name).into(), normalized);
                    df.with_column(series)
                        .map_err(|e| AppError::Internal(format!(
                            "電話番号正規化エラー ({}): {}", col_name, e
                        )))?;
                }
            }
        }
        info!("電話番号カラムの全角→半角変換を適用しました");
        Ok(df)
    }

    /// BOM付きUTF-8 CSVを読み込む
    /// polarsのCSVリーダーはBOMを自動処理しないため、
    /// ファイル内容を読み込んでBOMをスキップしてからパースする
    fn read_csv_with_bom_skip(path: &Path) -> Result<DataFrame, AppError> {
        let bytes = std::fs::read(path)
            .map_err(|e| AppError::Internal(format!("ファイル読み込みエラー: {}", e)))?;

        // BOM (0xEF, 0xBB, 0xBF) をスキップ
        let content = if bytes.starts_with(&[0xEF, 0xBB, 0xBF]) {
            info!("BOMを検出、スキップします");
            &bytes[3..]
        } else {
            &bytes[..]
        };

        // UTF-8文字列として処理
        let text = std::str::from_utf8(content)
            .map_err(|e| AppError::Internal(format!("UTF-8デコードエラー: {}", e)))?;
        let cursor = std::io::Cursor::new(text.as_bytes());

        // 全カラムをString型で読み込み
        // Polars 0.44ではCsvReadOptionsで設定する
        let mut df = CsvReader::new(cursor)
            .finish()
            .map_err(|e| AppError::Internal(format!("CSV解析エラー: {}", e)))?;

        // 数値として推論されたカラムをStringに戻す（正確なパースのため）
        let string_cols: Vec<String> = df.get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();
        for col_name in &string_cols {
            if let Ok(col) = df.column(col_name.as_str()) {
                if col.dtype() != &DataType::String {
                    let str_col = col.cast(&DataType::String)
                        .unwrap_or_else(|_| col.clone());
                    df.with_column(str_col)
                        .map_err(|e| AppError::Internal(format!("型変換エラー: {}", e)))?;
                }
            }
        }

        Ok(df)
    }

    /// Tursoデータベースからfacilitiesテーブルの全データを読み込み、
    /// CSVと同じカラム名のPolars DataFrameに変換して返す
    ///
    /// 大量データ対応:
    /// - LIMIT/OFFSETでページネーション取得（HTTP接続切れ防止）
    /// - 各ページごとに新規接続（長時間接続維持によるEOF回避）
    /// - ページ失敗時は最大3回リトライ（一時的なネットワーク障害対応）
    async fn read_from_turso(db: &Database) -> Result<DataFrame, AppError> {
        let conn = db.connect().map_err(|e| {
            AppError::Internal(format!("Turso接続エラー: {}", e))
        })?;

        // カラム名一覧を取得
        let pragma_rows = conn
            .query("PRAGMA table_info(facilities)", ())
            .await
            .map_err(|e| AppError::Internal(format!("Turso PRAGMAエラー: {}", e)))?;

        // メモリ削減: 不要なカラムを除外
        // 512MBメモリ制約のため積極的にスキップ
        let skip_columns: std::collections::HashSet<&str> = [
            "id",
            "サービス提供地域",       // 長文テキスト、未使用
            // 賃金関連（充填率0.1%、外部データで代替）
            "賃金_職種1", "賃金_月額1", "賃金_平均年齢1", "賃金_平均勤続1",
            "賃金_職種2", "賃金_月額2", "賃金_平均年齢2", "賃金_平均勤続2",
            "賃金_職種3", "賃金_月額3", "賃金_平均年齢3", "賃金_平均勤続3",
            "賃金_職種4", "賃金_月額4", "賃金_平均年齢4", "賃金_平均勤続4",
            "賃金_職種5", "賃金_月額5", "賃金_平均年齢5", "賃金_平均勤続5",
            // 不正情報（長文テキスト）
            "不正_処分内容", "不正_処分内容_詳細",
            "不正_指導内容", "不正_指導内容_詳細",
            // その他
            "スクレイピング日",
            "財務諸表DL_事業活動計算書",
            "財務諸表DL_資金収支計算書",
            "財務諸表DL_貸借対照表",
            "会計種類",
            // 要介護度詳細（重度者割合の派生カラムで代替）
            "要介護1", "要介護2", "要介護3", "要介護4", "要介護5",
            // 利用者詳細
            "利用者_都道府県平均",
        ].into_iter().collect();

        let mut col_names: Vec<String> = Vec::new();
        let mut skipped = 0usize;
        let mut pragma_current = pragma_rows;
        loop {
            match pragma_current.next().await {
                Ok(Some(row)) => {
                    if let Ok(name) = row.get::<String>(1) {
                        if skip_columns.contains(name.as_str()) {
                            skipped += 1;
                        } else {
                            col_names.push(name);
                        }
                    }
                }
                Ok(None) => break,
                Err(_) => break,
            }
        }
        info!("Tursoテーブルカラム数: {} (スキップ: {})", col_names.len(), skipped);

        // ページネーションで全データを取得（1回あたり5000行、失敗時は最大3回リトライ）
        const PAGE_SIZE: u64 = 5000;
        const MAX_RETRIES: u32 = 3;
        let select_cols: Vec<String> = col_names.iter().map(|c| format!("\"{}\"", c)).collect();
        let select_cols_str = select_cols.join(", ");
        let num_cols = col_names.len();
        let mut columns_data: Vec<Vec<Option<String>>> = vec![Vec::new(); num_cols];
        let mut total_rows = 0u64;
        let mut offset = 0u64;

        loop {
            let page_sql = format!(
                "SELECT {} FROM facilities ORDER BY id LIMIT {} OFFSET {}",
                select_cols_str, PAGE_SIZE, offset
            );

            let mut page_result: Option<(Vec<Vec<Option<String>>>, u64)> = None;
            let mut last_error = String::new();

            for attempt in 1..=MAX_RETRIES {
                // 各ページ・各リトライで新規接続（接続プール枯渇やEOF回避）
                let page_conn = match db.connect() {
                    Ok(c) => c,
                    Err(e) => {
                        last_error = format!("接続エラー: {}", e);
                        tracing::warn!(
                            "Tursoページ取得リトライ {}/{} (offset={}): {}",
                            attempt, MAX_RETRIES, offset, last_error
                        );
                        if attempt < MAX_RETRIES {
                            tokio::time::sleep(std::time::Duration::from_secs(2 * attempt as u64)).await;
                        }
                        continue;
                    }
                };

                match Self::fetch_page(&page_conn, &page_sql, num_cols).await {
                    Ok((page_data, page_count)) => {
                        page_result = Some((page_data, page_count));
                        break;
                    }
                    Err(e) => {
                        last_error = e.to_string();
                        tracing::warn!(
                            "Tursoページ取得リトライ {}/{} (offset={}): {}",
                            attempt, MAX_RETRIES, offset, last_error
                        );
                        if attempt < MAX_RETRIES {
                            tokio::time::sleep(std::time::Duration::from_secs(2 * attempt as u64)).await;
                        }
                    }
                }
            }

            let (page_data, page_count) = page_result.ok_or_else(|| {
                AppError::Internal(format!(
                    "Tursoページ取得失敗 (offset={}, {}回リトライ後): {}",
                    offset, MAX_RETRIES, last_error
                ))
            })?;

            // ページデータを統合
            for (i, col_data) in page_data.into_iter().enumerate() {
                columns_data[i].extend(col_data);
            }
            total_rows += page_count;

            info!("Tursoページ取得: offset={}, {}件 (累計{}件)", offset, page_count, total_rows);

            if page_count < PAGE_SIZE {
                break; // 最終ページ
            }
            offset += PAGE_SIZE;
        }

        info!("Tursoから合計 {} 件のレコードを取得しました", total_rows);

        // Polars DataFrameを構築
        let series_vec: Vec<Column> = col_names
            .iter()
            .zip(columns_data.into_iter())
            .map(|(name, data)| Column::Series(Series::new(name.as_str().into(), data)))
            .collect();

        let df = DataFrame::new(series_vec)
            .map_err(|e| AppError::Internal(format!("DataFrame構築エラー: {}", e)))?;

        Ok(df)
    }

    /// 1ページ分のデータをTursoから取得するヘルパー
    /// 返り値: (カラムごとのデータ, 取得行数)
    async fn fetch_page(
        conn: &libsql::Connection,
        sql: &str,
        num_cols: usize,
    ) -> Result<(Vec<Vec<Option<String>>>, u64), AppError> {
        let rows = conn
            .query(sql, ())
            .await
            .map_err(|e| AppError::Internal(format!("クエリエラー: {}", e)))?;

        let mut page_data: Vec<Vec<Option<String>>> = vec![Vec::new(); num_cols];
        let mut page_count = 0u64;
        let mut current_row = rows;

        loop {
            match current_row.next().await {
                Ok(Some(row)) => {
                    for i in 0..num_cols {
                        let val: Option<String> = match row.get_value(i as i32) {
                            Ok(libsql::Value::Text(s)) => Some(s),
                            Ok(libsql::Value::Integer(n)) => Some(n.to_string()),
                            Ok(libsql::Value::Real(f)) => Some(f.to_string()),
                            Ok(libsql::Value::Null) => None,
                            Ok(libsql::Value::Blob(_)) => None,
                            Err(_) => None,
                        };
                        page_data[i].push(val);
                    }
                    page_count += 1;
                }
                Ok(None) => break,
                Err(e) => {
                    return Err(AppError::Internal(format!(
                        "行読み込みエラー (page内{}行目): {}",
                        page_count, e
                    )));
                }
            }
        }

        Ok((page_data, page_count))
    }

    /// 派生カラムを計算してDataFrameに追加
    /// - 都道府県（住所から抽出、失敗時は事業所番号先頭2桁からフォールバック）
    /// - 法人種別（法人名から推定）
    /// - 離職率、常勤比率、事業年数
    /// - 76カラム拡張: 加算、品質、利用者、財務、賃金、派生指標
    fn compute_derived_columns(df: DataFrame) -> Result<DataFrame, AppError> {
        let height = df.height();

        // 都道府県を抽出（事業所番号からのフォールバック付き）
        let prefectures: Vec<Option<String>> = if let Ok(addr_col) = df.column("住所") {
            let addr_ca = addr_col.str()
                .map_err(|e| AppError::Internal(format!("住所カラムの型エラー: {}", e)))?;
            // 事業所番号カラムも取得（フォールバック用）
            let jig_ca = df.column("事業所番号")
                .ok()
                .and_then(|c| c.str().ok());
            addr_ca
                .into_iter()
                .enumerate()
                .map(|(i, opt_addr)| {
                    let addr = opt_addr.unwrap_or("");
                    let jig = jig_ca
                        .as_ref()
                        .and_then(|ca| ca.get(i))
                        .unwrap_or("");
                    extract_prefecture_with_fallback(addr, jig)
                })
                .collect()
        } else {
            vec![None; height]
        };

        // 法人種別を推定
        let corp_types: Vec<Option<String>> = if let Ok(col) = df.column("法人名") {
            col.str()
                .map_err(|e| AppError::Internal(format!("法人名カラムの型エラー: {}", e)))?
                .into_iter()
                .map(|opt_val| opt_val.map(|v| classify_corp_type(v).to_string()))
                .collect()
        } else {
            vec![None; height]
        };

        // 数値カラムをf64に変換するヘルパー（借用問題を回避するため関数化）
        fn parse_f64_col(df: &DataFrame, col_name: &str, height: usize) -> Vec<Option<f64>> {
            if let Ok(col) = df.column(col_name) {
                if let Ok(ca) = col.f64() {
                    ca.into_iter().collect()
                } else if let Ok(ca) = col.str() {
                    ca.into_iter()
                        .map(|opt_val| opt_val.and_then(|v| v.trim().parse::<f64>().ok()))
                        .collect()
                } else {
                    vec![None; height]
                }
            } else {
                vec![None; height]
            }
        }

        // ブールカラムを読み取るヘルパー
        // ETLで '○' → True に変換済みのboolカラム、またはString '○'/'true' を読み取る
        fn parse_bool_col(df: &DataFrame, col_name: &str, height: usize) -> Vec<Option<bool>> {
            if let Ok(col) = df.column(col_name) {
                if let Ok(ca) = col.bool() {
                    ca.into_iter().collect()
                } else if let Ok(ca) = col.str() {
                    ca.into_iter()
                        .map(|opt_val| opt_val.map(|v| {
                            let trimmed = v.trim().to_lowercase();
                            trimmed == "true" || trimmed == "○" || trimmed == "1" || trimmed == "あり"
                        }))
                        .collect()
                } else {
                    vec![None; height]
                }
            } else {
                vec![None; height]
            }
        }

        let staff_fulltime = parse_f64_col(&df, "従業者_常勤", height);
        let staff_total = parse_f64_col(&df, "従業者_合計", height);
        let left_last_year = parse_f64_col(&df, "前年度退職数", height);

        // 離職率を計算: 退職数 / (合計 + 退職数)
        let turnover_rates: Vec<Option<f64>> = staff_total
            .iter()
            .zip(left_last_year.iter())
            .map(|(total, left)| match (total, left) {
                (Some(t), Some(l)) => {
                    let denominator = t + l;
                    if denominator > 0.0 {
                        Some(l / denominator)
                    } else {
                        None
                    }
                }
                _ => None,
            })
            .collect();

        // 常勤比率を計算: 常勤 / 合計
        let fulltime_ratios: Vec<Option<f64>> = staff_fulltime
            .iter()
            .zip(staff_total.iter())
            .map(|(ft, total)| match (ft, total) {
                (Some(f), Some(t)) if *t > 0.0 => Some(f / t),
                _ => None,
            })
            .collect();

        // 事業年数を計算: 2026 - 事業開始年（f64で保持し、集計関数と互換性を保つ）
        // 日付形式は "2004/02/01"（CSV）または "2004-02-01"（Turso/Polars Date型のString変換後）の両方に対応
        let years_in_business: Vec<Option<f64>> = if let Ok(col) = df.column("事業開始日") {
            col.str()
                .map_err(|e| AppError::Internal(format!("事業開始日カラムの型エラー: {}", e)))?
                .into_iter()
                .map(|opt_val| {
                    opt_val.and_then(|v| {
                        // "2004/02/01" または "2004-02-01" 形式から年を抽出
                        let trimmed = v.trim();
                        // 最初の4文字が年（YYYY形式）であればパース
                        trimmed.get(..4)
                            .and_then(|y| y.parse::<i32>().ok())
                            .map(|year| (chrono::Utc::now().year() - year).max(0) as f64)
                    })
                })
                .collect()
        } else {
            vec![None; height]
        };

        // === 76カラム拡張: 加算13項目の読み取り ===
        let kasan_syogu_1 = parse_bool_col(&df, "加算_処遇改善I", height);
        let kasan_syogu_2 = parse_bool_col(&df, "加算_処遇改善II", height);
        let kasan_syogu_3 = parse_bool_col(&df, "加算_処遇改善III", height);
        let kasan_syogu_4 = parse_bool_col(&df, "加算_処遇改善IV", height);
        let kasan_tokutei_1 = parse_bool_col(&df, "加算_特定事業所I", height);
        let kasan_tokutei_2 = parse_bool_col(&df, "加算_特定事業所II", height);
        let kasan_tokutei_3 = parse_bool_col(&df, "加算_特定事業所III", height);
        let kasan_tokutei_4 = parse_bool_col(&df, "加算_特定事業所IV", height);
        let kasan_tokutei_5 = parse_bool_col(&df, "加算_特定事業所V", height);
        let kasan_ninchisho_1 = parse_bool_col(&df, "加算_認知症ケアI", height);
        let kasan_ninchisho_2 = parse_bool_col(&df, "加算_認知症ケアII", height);
        let kasan_koku = parse_bool_col(&df, "加算_口腔連携", height);
        let kasan_kinkyuji = parse_bool_col(&df, "加算_緊急時", height);

        // 加算取得数を計算（13加算のTrueカウント）
        let all_kasan_cols: Vec<&Vec<Option<bool>>> = vec![
            &kasan_syogu_1, &kasan_syogu_2, &kasan_syogu_3, &kasan_syogu_4,
            &kasan_tokutei_1, &kasan_tokutei_2, &kasan_tokutei_3, &kasan_tokutei_4, &kasan_tokutei_5,
            &kasan_ninchisho_1, &kasan_ninchisho_2, &kasan_koku, &kasan_kinkyuji,
        ];
        // いずれかの加算カラムが存在する場合のみ計算
        let has_any_kasan = all_kasan_cols.iter().any(|col| col.iter().any(|v| v.is_some()));
        let kasan_counts: Vec<Option<i32>> = if has_any_kasan {
            (0..height).map(|i| {
                let count: i32 = all_kasan_cols.iter()
                    .filter_map(|col| col[i])
                    .filter(|v| *v)
                    .count() as i32;
                // 全てNoneの行はNoneを返す
                let any_value = all_kasan_cols.iter().any(|col| col[i].is_some());
                if any_value { Some(count) } else { None }
            }).collect()
        } else {
            vec![None; height]
        };

        // === 76カラム拡張: 品質4項目の読み取り ===
        let quality_bcp = parse_bool_col(&df, "品質_BCP策定", height);
        let quality_ict = parse_bool_col(&df, "品質_ICT活用", height);
        let quality_third_party = parse_bool_col(&df, "品質_第三者評価", height);
        let quality_insurance = parse_bool_col(&df, "品質_損害賠償保険", height);

        // === 76カラム拡張: 利用者情報 ===
        let user_count = parse_f64_col(&df, "利用者総数", height);
        let user_pref_avg = parse_f64_col(&df, "利用者_都道府県平均", height);
        let care_l1 = parse_f64_col(&df, "要介護1", height);
        let care_l2 = parse_f64_col(&df, "要介護2", height);
        let care_l3 = parse_f64_col(&df, "要介護3", height);
        let care_l4 = parse_f64_col(&df, "要介護4", height);
        let care_l5 = parse_f64_col(&df, "要介護5", height);
        let capacity_vals = parse_f64_col(&df, "定員", height);

        // 稼働率を計算: 利用者総数 / 定員
        // 通所系・入所系のみ計算（訪問系・福祉用具等は定員の概念が異なるためNULL）
        let service_codes: Vec<Option<String>> = if let Ok(col) = df.column("サービスコード") {
            if let Ok(ca) = col.str() {
                ca.into_iter().map(|v| v.map(|s| s.to_string())).collect()
            } else { vec![None; height] }
        } else { vec![None; height] };

        // 定員が意味のあるサービス種別（通所・入所・グループホーム等）
        let capacity_valid_services: std::collections::HashSet<&str> = [
            "150", "155", "160",     // 通所介護、通所リハ
            "210", "220", "230",     // 短期入所
            "320",                   // 認知症GH
            "331", "332", "334",     // 特定施設
            "510", "520", "530", "540", "550", // 入所系
        ].into_iter().collect();

        let occupancy_rates: Vec<Option<f64>> = user_count.iter()
            .zip(capacity_vals.iter())
            .zip(service_codes.iter())
            .map(|((users, cap), svc)| {
                let svc_code = svc.as_deref().unwrap_or("");
                if !capacity_valid_services.contains(svc_code) {
                    return None; // 訪問系等は稼働率を計算しない
                }
                match (users, cap) {
                    (Some(u), Some(c)) if *c >= 5.0 => Some(u / c), // 定員5未満も除外
                    _ => None,
                }
            })
            .collect();

        // ETLで計算済みの平均要介護度カラムを事前に取得
        let etl_avg_care_level = parse_f64_col(&df, "平均要介護度", height);

        // 平均要介護度を計算: (1*CL1 + 2*CL2 + 3*CL3 + 4*CL4 + 5*CL5) / 利用者総数
        let avg_care_levels: Vec<Option<f64>> = (0..height).map(|i| {
            let cl1 = care_l1[i].unwrap_or(0.0);
            let cl2 = care_l2[i].unwrap_or(0.0);
            let cl3 = care_l3[i].unwrap_or(0.0);
            let cl4 = care_l4[i].unwrap_or(0.0);
            let cl5 = care_l5[i].unwrap_or(0.0);
            let total = cl1 + cl2 + cl3 + cl4 + cl5;
            if total > 0.0 {
                Some((1.0 * cl1 + 2.0 * cl2 + 3.0 * cl3 + 4.0 * cl4 + 5.0 * cl5) / total)
            } else {
                // ETLで計算済みの平均要介護度カラムがあればそれを使用
                etl_avg_care_level[i]
            }
        }).collect();

        // 重度者割合: (要介護4+5) / 利用者総数
        let severe_rates: Vec<Option<f64>> = (0..height).map(|i| {
            let cl4 = care_l4[i].unwrap_or(0.0);
            let cl5 = care_l5[i].unwrap_or(0.0);
            match user_count[i] {
                Some(u) if u > 0.0 => Some((cl4 + cl5) / u),
                _ => None,
            }
        }).collect();

        // === 76カラム拡張: 賃金 ===
        // 代表賃金 = 賃金_月額1〜5の中央値
        let wage_cols: Vec<Vec<Option<f64>>> = (1..=5)
            .map(|i| parse_f64_col(&df, &format!("賃金_月額{}", i), height))
            .collect();
        let salary_representatives: Vec<Option<f64>> = (0..height).map(|i| {
            let mut wages: Vec<f64> = wage_cols.iter()
                .filter_map(|col| col[i])
                .filter(|v| *v > 0.0)
                .collect();
            if wages.is_empty() {
                None
            } else {
                wages.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let mid = wages.len() / 2;
                if wages.len() % 2 == 0 {
                    Some((wages[mid - 1] + wages[mid]) / 2.0)
                } else {
                    Some(wages[mid])
                }
            }
        }).collect();

        // === 76カラム拡張: 品質スコア計算 ===
        // 設計書0.3の品質スコアリングモデルに基づく
        let quality_scores: Vec<Option<f64>> = (0..height).map(|i| {
            // 品質関連データが一つでもあるか確認
            let has_data = quality_bcp[i].is_some()
                || quality_ict[i].is_some()
                || quality_third_party[i].is_some()
                || quality_insurance[i].is_some()
                || kasan_counts[i].is_some();
            if !has_data {
                return None;
            }

            let mut score = 0.0f64;

            // 安全・リスク管理（30点）
            if quality_bcp[i].unwrap_or(false) { score += 10.0; }
            if quality_insurance[i].unwrap_or(false) { score += 10.0; }
            // 行政処分なし = 10点（現時点では行政処分データ未実装のため加点）
            score += 10.0;

            // 品質管理（25点）
            if quality_third_party[i].unwrap_or(false) { score += 15.0; }
            if quality_ict[i].unwrap_or(false) { score += 10.0; }

            // 人材安定性（25点）
            if let Some(tr) = turnover_rates[i] {
                if tr < 0.15 { score += 10.0; } // 離職率15%未満
            }
            if let Some(fr) = fulltime_ratios[i] {
                if fr > 0.5 { score += 8.0; }
            }
            // 経験10年以上割合 > 30% → 7点
            if let Ok(col) = df.column("経験10年以上割合") {
                if let Ok(ca) = col.str() {
                    if let Some(v) = ca.get(i) {
                        if let Ok(ratio) = v.trim().replace("％", "").replace("%", "").parse::<f64>() {
                            if ratio > 30.0 { score += 7.0; }
                        }
                    }
                } else if let Ok(ca) = col.f64() {
                    if let Some(ratio) = ca.get(i) {
                        if ratio > 30.0 { score += 7.0; }
                    }
                }
            }

            // 収益安定性（20点）
            if let Some(occ) = occupancy_rates[i] {
                if occ > 0.8 { score += 10.0; }
            }
            if let Some(kc) = kasan_counts[i] {
                if kc >= 5 { score += 10.0; }
            }

            Some(score)
        }).collect();

        // 品質ランク: S(80+) / A(65-79) / B(50-64) / C(35-49) / D(0-34)
        let quality_ranks: Vec<Option<String>> = quality_scores.iter().map(|opt_score| {
            opt_score.map(|s| {
                if s >= 80.0 { "S".to_string() }
                else if s >= 65.0 { "A".to_string() }
                else if s >= 50.0 { "B".to_string() }
                else if s >= 35.0 { "C".to_string() }
                else { "D".to_string() }
            })
        }).collect();

        // === 派生カラムをDataFrameに追加 ===
        let mut df = df;

        // 都道府県
        let pref_series = Series::new("都道府県".into(), prefectures);
        df.with_column(pref_series)
            .map_err(|e| AppError::Internal(format!("都道府県カラム追加エラー: {}", e)))?;

        // 法人種別
        let corp_series = Series::new("法人種別".into(), corp_types);
        df.with_column(corp_series)
            .map_err(|e| AppError::Internal(format!("法人種別カラム追加エラー: {}", e)))?;

        // 離職率
        let turnover_series = Series::new("離職率".into(), turnover_rates);
        df.with_column(turnover_series)
            .map_err(|e| AppError::Internal(format!("離職率カラム追加エラー: {}", e)))?;

        // 常勤比率
        let fulltime_series = Series::new("常勤比率".into(), fulltime_ratios);
        df.with_column(fulltime_series)
            .map_err(|e| AppError::Internal(format!("常勤比率カラム追加エラー: {}", e)))?;

        // 事業年数
        let years_series = Series::new("事業年数".into(), years_in_business);
        df.with_column(years_series)
            .map_err(|e| AppError::Internal(format!("事業年数カラム追加エラー: {}", e)))?;

        // 数値カラムを追加（String→f64変換済み）
        let staff_ft_series = Series::new("従業者_常勤_num".into(), parse_f64_col(&df, "従業者_常勤", height));
        df.with_column(staff_ft_series)
            .map_err(|e| AppError::Internal(format!("従業者_常勤_numカラム追加エラー: {}", e)))?;

        let staff_pt_series = Series::new("従業者_非常勤_num".into(), parse_f64_col(&df, "従業者_非常勤", height));
        df.with_column(staff_pt_series)
            .map_err(|e| AppError::Internal(format!("従業者_非常勤_numカラム追加エラー: {}", e)))?;

        let staff_total_series = Series::new("従業者_合計_num".into(), parse_f64_col(&df, "従業者_合計", height));
        df.with_column(staff_total_series)
            .map_err(|e| AppError::Internal(format!("従業者_合計_numカラム追加エラー: {}", e)))?;

        let capacity_series = Series::new("定員_num".into(), parse_f64_col(&df, "定員", height));
        df.with_column(capacity_series)
            .map_err(|e| AppError::Internal(format!("定員_numカラム追加エラー: {}", e)))?;

        let hired_series = Series::new("前年度採用数_num".into(), parse_f64_col(&df, "前年度採用数", height));
        df.with_column(hired_series)
            .map_err(|e| AppError::Internal(format!("前年度採用数_numカラム追加エラー: {}", e)))?;

        let left_series = Series::new("前年度退職数_num".into(), parse_f64_col(&df, "前年度退職数", height));
        df.with_column(left_series)
            .map_err(|e| AppError::Internal(format!("前年度退職数_numカラム追加エラー: {}", e)))?;

        // === 76カラム拡張: 派生指標をDataFrameに追加 ===

        // 加算取得数
        let kasan_count_f64: Vec<Option<f64>> = kasan_counts.iter()
            .map(|v| v.map(|c| c as f64))
            .collect();
        df.with_column(Series::new("加算取得数".into(), kasan_count_f64))
            .map_err(|e| AppError::Internal(format!("加算取得数カラム追加エラー: {}", e)))?;

        // 稼働率
        df.with_column(Series::new("稼働率".into(), occupancy_rates.clone()))
            .map_err(|e| AppError::Internal(format!("稼働率カラム追加エラー: {}", e)))?;

        // 品質スコア
        df.with_column(Series::new("品質スコア".into(), quality_scores.clone()))
            .map_err(|e| AppError::Internal(format!("品質スコアカラム追加エラー: {}", e)))?;

        // 品質ランク
        df.with_column(Series::new("品質ランク".into(), quality_ranks.clone()))
            .map_err(|e| AppError::Internal(format!("品質ランクカラム追加エラー: {}", e)))?;

        // 平均要介護度
        df.with_column(Series::new("平均要介護度_calc".into(), avg_care_levels.clone()))
            .map_err(|e| AppError::Internal(format!("平均要介護度カラム追加エラー: {}", e)))?;

        // 重度者割合
        df.with_column(Series::new("重度者割合".into(), severe_rates.clone()))
            .map_err(|e| AppError::Internal(format!("重度者割合カラム追加エラー: {}", e)))?;

        // 代表賃金
        df.with_column(Series::new("代表賃金".into(), salary_representatives.clone()))
            .map_err(|e| AppError::Internal(format!("代表賃金カラム追加エラー: {}", e)))?;

        // 利用者総数_num（集計用）
        df.with_column(Series::new("利用者総数_num".into(), user_count.clone()))
            .map_err(|e| AppError::Internal(format!("利用者総数_numカラム追加エラー: {}", e)))?;

        // 処遇改善加算取得率（処遇改善I〜IVのいずれかTrue）
        let syogu_kaizen_flags: Vec<Option<f64>> = (0..height).map(|i| {
            let has_data = kasan_syogu_1[i].is_some() || kasan_syogu_2[i].is_some()
                || kasan_syogu_3[i].is_some() || kasan_syogu_4[i].is_some();
            if !has_data { return None; }
            let any_true = kasan_syogu_1[i].unwrap_or(false)
                || kasan_syogu_2[i].unwrap_or(false)
                || kasan_syogu_3[i].unwrap_or(false)
                || kasan_syogu_4[i].unwrap_or(false);
            Some(if any_true { 1.0 } else { 0.0 })
        }).collect();
        df.with_column(Series::new("処遇改善加算フラグ".into(), syogu_kaizen_flags))
            .map_err(|e| AppError::Internal(format!("処遇改善加算フラグカラム追加エラー: {}", e)))?;

        // 品質関連のboolをf64に変換してDataFrameに追加（集計用）
        let bcp_f64: Vec<Option<f64>> = quality_bcp.iter()
            .map(|v| v.map(|b| if b { 1.0 } else { 0.0 }))
            .collect();
        df.with_column(Series::new("BCP策定フラグ".into(), bcp_f64))
            .map_err(|e| AppError::Internal(format!("BCP策定フラグカラム追加エラー: {}", e)))?;

        let ict_f64: Vec<Option<f64>> = quality_ict.iter()
            .map(|v| v.map(|b| if b { 1.0 } else { 0.0 }))
            .collect();
        df.with_column(Series::new("ICT活用フラグ".into(), ict_f64))
            .map_err(|e| AppError::Internal(format!("ICT活用フラグカラム追加エラー: {}", e)))?;

        let third_party_f64: Vec<Option<f64>> = quality_third_party.iter()
            .map(|v| v.map(|b| if b { 1.0 } else { 0.0 }))
            .collect();
        df.with_column(Series::new("第三者評価フラグ".into(), third_party_f64))
            .map_err(|e| AppError::Internal(format!("第三者評価フラグカラム追加エラー: {}", e)))?;

        let insurance_f64: Vec<Option<f64>> = quality_insurance.iter()
            .map(|v| v.map(|b| if b { 1.0 } else { 0.0 }))
            .collect();
        df.with_column(Series::new("損害賠償保険フラグ".into(), insurance_f64))
            .map_err(|e| AppError::Internal(format!("損害賠償保険フラグカラム追加エラー: {}", e)))?;

        // 加算13項目をf64に変換してDataFrameに追加（集計用）
        let kasan_bool_cols = vec![
            ("加算_処遇改善I_f", &kasan_syogu_1),
            ("加算_処遇改善II_f", &kasan_syogu_2),
            ("加算_処遇改善III_f", &kasan_syogu_3),
            ("加算_処遇改善IV_f", &kasan_syogu_4),
            ("加算_特定事業所I_f", &kasan_tokutei_1),
            ("加算_特定事業所II_f", &kasan_tokutei_2),
            ("加算_特定事業所III_f", &kasan_tokutei_3),
            ("加算_特定事業所IV_f", &kasan_tokutei_4),
            ("加算_特定事業所V_f", &kasan_tokutei_5),
            ("加算_認知症ケアI_f", &kasan_ninchisho_1),
            ("加算_認知症ケアII_f", &kasan_ninchisho_2),
            ("加算_口腔連携_f", &kasan_koku),
            ("加算_緊急時_f", &kasan_kinkyuji),
        ];
        for (col_name, bool_vec) in &kasan_bool_cols {
            let f64_vec: Vec<Option<f64>> = bool_vec.iter()
                .map(|v| v.map(|b| if b { 1.0 } else { 0.0 }))
                .collect();
            df.with_column(Series::new((*col_name).into(), f64_vec))
                .map_err(|e| AppError::Internal(format!("{}カラム追加エラー: {}", col_name, e)))?;
        }

        Ok(df)
    }

    /// フィルタ条件に基づいてDataFrameをフィルタリング
    /// 全条件はAND結合
    pub fn apply_filters(&self, params: &FilterParams) -> Result<DataFrame, AppError> {
        let mut mask = BooleanChunked::full("mask".into(), true, self.df.height());

        // 都道府県フィルタ
        if let Some(prefs) = params.prefecture_list() {
            if !prefs.is_empty() {
                if let Ok(col) = self.df.column("都道府県") {
                    let col_str = col.str().map_err(|e| {
                        AppError::Internal(format!("都道府県カラムの型エラー: {}", e))
                    })?;
                    let pref_mask = col_str
                        .into_iter()
                        .map(|opt_val| {
                            opt_val.map_or(false, |v| prefs.iter().any(|p| p == v))
                        })
                        .collect::<BooleanChunked>();
                    mask = mask & pref_mask;
                }
            }
        }

        // サービスコードフィルタ（19カラム版では常にスキップ）
        if let Some(codes) = params.service_code_list() {
            if !codes.is_empty() {
                if let Ok(col) = self.df.column("サービスコード") {
                    let col_str = col.str().map_err(|e| {
                        AppError::Internal(format!("サービスコードカラムの型エラー: {}", e))
                    })?;
                    let code_mask = col_str
                        .into_iter()
                        .map(|opt_val| {
                            opt_val.map_or(false, |v| codes.iter().any(|c| c == v))
                        })
                        .collect::<BooleanChunked>();
                    mask = mask & code_mask;
                }
            }
        }

        // 法人種別フィルタ
        if let Some(types) = params.corp_type_list() {
            if !types.is_empty() {
                if let Ok(col) = self.df.column("法人種別") {
                    let col_str = col.str().map_err(|e| {
                        AppError::Internal(format!("法人種別カラムの型エラー: {}", e))
                    })?;
                    let type_mask = col_str
                        .into_iter()
                        .map(|opt_val| {
                            opt_val.map_or(false, |v| types.iter().any(|t| t == v))
                        })
                        .collect::<BooleanChunked>();
                    mask = mask & type_mask;
                }
            }
        }

        // 従業者数フィルタ（min/max）
        // Float64型だけでなくString型からのパースにも対応
        if params.staff_min.is_some() || params.staff_max.is_some() {
            if let Ok(col) = self.df.column("従業者_合計_num") {
                // Float64として取得を試み、失敗したらStringからパース
                let vals: Vec<Option<f64>> = if let Ok(ca) = col.f64() {
                    ca.into_iter().collect()
                } else if let Ok(ca) = col.str() {
                    ca.into_iter()
                        .map(|opt_val| opt_val.and_then(|v| v.trim().parse::<f64>().ok()))
                        .collect()
                } else {
                    vec![None; self.df.height()]
                };
                let staff_mask = vals
                    .into_iter()
                    .map(|opt_val| {
                        opt_val.map_or(false, |v| {
                            let min_ok = params.staff_min.map_or(true, |min| v >= min);
                            let max_ok = params.staff_max.map_or(true, |max| v <= max);
                            min_ok && max_ok
                        })
                    })
                    .collect::<BooleanChunked>();
                mask = mask & staff_mask;
            }
        }

        // キーワードフィルタ（住所・事業所名・市区町村の部分一致）
        if let Some(ref kw) = params.keyword {
            let kw = kw.trim();
            if !kw.is_empty() {
                let addr_col = self.df.column("住所").ok().and_then(|c| c.str().ok().map(|ca| ca.clone()));
                let name_col = self.df.column("事業所名").ok().and_then(|c| c.str().ok().map(|ca| ca.clone()));
                let muni_col = self.df.column("municipality").ok().and_then(|c| c.str().ok().map(|ca| ca.clone()));

                let kw_mask = (0..self.df.height())
                    .map(|i| {
                        let in_addr = addr_col.as_ref().and_then(|ca| ca.get(i)).map_or(false, |v| v.contains(kw));
                        let in_name = name_col.as_ref().and_then(|ca| ca.get(i)).map_or(false, |v| v.contains(kw));
                        let in_muni = muni_col.as_ref().and_then(|ca| ca.get(i)).map_or(false, |v| v.contains(kw));
                        in_addr || in_name || in_muni
                    })
                    .collect::<BooleanChunked>();
                mask = mask & kw_mask;
            }
        }

        self.df
            .filter(&mask)
            .map_err(|e| AppError::Internal(format!("フィルタ適用エラー: {}", e)))
    }

    /// DataFrameの1行をFacility構造体に変換
    pub fn row_to_facility(&self, df: &DataFrame, idx: usize) -> Facility {
        let get_str = |col_name: &str| -> Option<String> {
            df.column(col_name)
                .ok()
                .and_then(|c| c.str().ok())
                .and_then(|ca| ca.get(idx).map(|s| s.to_string()))
        };

        let get_f64 = |col_name: &str| -> Option<f64> {
            df.column(col_name)
                .ok()
                .and_then(|c| {
                    // まずFloat64として取得を試みる
                    if let Ok(ca) = c.f64() {
                        return ca.get(idx);
                    }
                    // String型の場合はパースして変換
                    if let Ok(ca) = c.str() {
                        return ca.get(idx).and_then(|v| v.trim().parse::<f64>().ok());
                    }
                    None
                })
        };

        let get_bool = |col_name: &str| -> Option<bool> {
            df.column(col_name)
                .ok()
                .and_then(|c| {
                    if let Ok(ca) = c.bool() {
                        return ca.get(idx);
                    }
                    // f64カラム（_f サフィックス付き）からの変換
                    if let Ok(ca) = c.f64() {
                        return ca.get(idx).map(|v| v > 0.5);
                    }
                    if let Ok(ca) = c.str() {
                        return ca.get(idx).map(|v| {
                            let t = v.trim().to_lowercase();
                            t == "true" || t == "○" || t == "1" || t == "あり"
                        });
                    }
                    None
                })
        };

        let get_i32 = |col_name: &str| -> Option<i32> {
            get_f64(col_name).map(|v| v as i32)
        };

        Facility {
            jigyosho_number: get_str("事業所番号").unwrap_or_default(),
            jigyosho_name: get_str("事業所名").unwrap_or_default(),
            manager_name: get_str("管理者名"),
            manager_title: get_str("管理者職名"),
            representative_name: get_str("代表者名"),
            representative_title: get_str("代表者職名"),
            corp_name: get_str("法人名"),
            corp_number: get_str("法人番号"),
            phone: get_str("電話番号"),
            fax: get_str("FAX番号"),
            address: get_str("住所"),
            homepage: get_str("HP"),
            staff_fulltime: get_f64("従業者_常勤_num"),
            staff_parttime: get_f64("従業者_非常勤_num"),
            staff_total: get_f64("従業者_合計_num"),
            capacity: get_f64("定員_num"),
            start_date: get_str("事業開始日"),
            hired_last_year: get_f64("前年度採用数_num"),
            left_last_year: get_f64("前年度退職数_num"),
            prefecture: get_str("都道府県"),
            corp_type: get_str("法人種別"),
            turnover_rate: get_f64("離職率"),
            fulltime_ratio: get_f64("常勤比率"),
            years_in_business: get_f64("事業年数"),
            // サービス情報
            service_code: get_str("サービスコード"),
            service_name: get_str("サービス名"),
            // 加算13項目
            kasan_syogu_kaizen_1: get_bool("加算_処遇改善I_f"),
            kasan_syogu_kaizen_2: get_bool("加算_処遇改善II_f"),
            kasan_syogu_kaizen_3: get_bool("加算_処遇改善III_f"),
            kasan_syogu_kaizen_4: get_bool("加算_処遇改善IV_f"),
            kasan_tokutei_1: get_bool("加算_特定事業所I_f"),
            kasan_tokutei_2: get_bool("加算_特定事業所II_f"),
            kasan_tokutei_3: get_bool("加算_特定事業所III_f"),
            kasan_tokutei_4: get_bool("加算_特定事業所IV_f"),
            kasan_tokutei_5: get_bool("加算_特定事業所V_f"),
            kasan_ninchisho_1: get_bool("加算_認知症ケアI_f"),
            kasan_ninchisho_2: get_bool("加算_認知症ケアII_f"),
            kasan_koku_renkei: get_bool("加算_口腔連携_f"),
            kasan_kinkyuji: get_bool("加算_緊急時_f"),
            // 品質
            quality_bcp: get_bool("BCP策定フラグ"),
            quality_ict: get_bool("ICT活用フラグ"),
            quality_third_party: get_bool("第三者評価フラグ"),
            quality_insurance: get_bool("損害賠償保険フラグ"),
            // 利用者
            user_count: get_f64("利用者総数_num"),
            // メモリ削減: スキップしたカラム
            user_pref_avg: None,
            care_level_1: None,
            care_level_2: None,
            care_level_3: None,
            care_level_4: None,
            care_level_5: None,
            care_level_support1: get_f64("要支援1"),
            care_level_support2: get_f64("要支援2"),
            experienced_ratio: get_str("経験10年以上割合"),
            // 財務
            // メモリ削減: Tursoから読み込みスキップしたカラム
            accounting_type: None,
            financial_dl_pl: None,
            financial_dl_cf: None,
            financial_dl_bs: None,
            // 賃金
            salary_representative: None, // メモリ削減: 賃金カラムスキップ
            // 派生指標
            occupancy_rate: get_f64("稼働率"),
            kasan_count: get_i32("加算取得数"),
            quality_score: get_f64("品質スコア"),
            quality_rank: get_str("品質ランク"),
            avg_care_level: get_f64("平均要介護度_calc"),
            severe_rate: get_f64("重度者割合"),
            // 位置情報
            latitude: get_f64("latitude"),
            longitude: get_f64("longitude"),
            municipality: get_str("municipality"),
            // 旧互換フィールド
            care_staff_count: None,
            nurse_count: None,
            rehab_staff_count: None,
            profit_ratio: get_f64("損益差額比率"),
            salary_level: get_f64("給与水準"),
        }
    }

    /// DataFrameの全行をFacility Vecに変換
    pub fn df_to_facilities(&self, df: &DataFrame) -> Vec<Facility> {
        (0..df.height())
            .map(|i| self.row_to_facility(df, i))
            .collect()
    }
}
