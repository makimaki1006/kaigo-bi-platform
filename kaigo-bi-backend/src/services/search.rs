/// テキスト検索サービス
/// 事業所名・法人名・電話番号の部分一致検索

use polars::prelude::*;

use crate::error::AppError;

/// テキスト検索を実行
/// 事業所名、法人名、電話番号のいずれかに部分一致する行をフィルタ
/// OR条件で結合（いずれかにマッチすればヒット）
pub fn text_search(df: &DataFrame, query: &str) -> Result<DataFrame, AppError> {
    if query.trim().is_empty() {
        return Ok(df.clone());
    }

    let query_lower = query.to_lowercase();
    let height = df.height();

    // 各検索対象カラムでマッチを判定
    let search_columns = ["事業所名", "法人名", "電話番号"];

    let mut combined_mask = BooleanChunked::full("mask".into(), false, height);

    for col_name in &search_columns {
        if let Ok(col) = df.column(*col_name) {
            if let Ok(ca) = col.str() {
                let col_mask = ca
                    .into_iter()
                    .map(|opt_val| {
                        opt_val.map_or(false, |v| v.to_lowercase().contains(&query_lower))
                    })
                    .collect::<BooleanChunked>();
                combined_mask = combined_mask | col_mask;
            }
        }
    }

    df.filter(&combined_mask)
        .map_err(|e| AppError::Internal(format!("テキスト検索フィルタエラー: {}", e)))
}

/// ソートを適用
/// カラム名が不正な場合はソートなしでそのまま返す
pub fn apply_sort(
    df: &DataFrame,
    sort_by: Option<&str>,
    sort_order: Option<&str>,
) -> Result<DataFrame, AppError> {
    let sort_col = match sort_by {
        Some(col) => col,
        None => return Ok(df.clone()),
    };

    // ソートカラム名のマッピング（API名 → DataFrame内のカラム名）
    let actual_col = match sort_col {
        "jigyosho_number" => "事業所番号",
        "jigyosho_name" => "事業所名",
        "staff_total" => "従業者_合計_num",
        "capacity" => "定員_num",
        "turnover_rate" => "離職率",
        "fulltime_ratio" => "常勤比率",
        "years_in_business" => "事業年数",
        "prefecture" => "都道府県",
        "corp_type" => "法人種別",
        "corp_name" => "法人名",
        // そのまま渡す（日本語カラム名の直接指定も許容）
        other => other,
    };

    // カラムが存在するか確認
    if df.column(actual_col).is_err() {
        return Ok(df.clone());
    }

    let descending = sort_order.map_or(false, |o| o.to_lowercase() == "desc");

    df.sort(
        [actual_col],
        SortMultipleOptions::default().with_order_descending(descending),
    )
    .map_err(|e| AppError::Internal(format!("ソートエラー: {}", e)))
}
