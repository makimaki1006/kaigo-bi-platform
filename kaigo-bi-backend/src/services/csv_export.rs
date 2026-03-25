/// CSVエクスポートサービス
/// BOM付きUTF-8でCSVストリームを生成

use polars::prelude::*;

use crate::error::AppError;

/// DataFrameをBOM付きUTF-8 CSV文字列に変換
/// Excelで文字化けしないようBOM（\xEF\xBB\xBF）を先頭に付与
pub fn dataframe_to_csv_bytes(df: &DataFrame) -> Result<Vec<u8>, AppError> {
    // BOMバイト列
    let bom: Vec<u8> = vec![0xEF, 0xBB, 0xBF];

    // CSV文字列を生成
    let mut csv_buf = Vec::new();
    CsvWriter::new(&mut csv_buf)
        .finish(&mut df.clone())
        .map_err(|e| AppError::Internal(format!("CSVエクスポートエラー: {}", e)))?;

    // BOM + CSV内容を結合
    let mut result = bom;
    result.extend_from_slice(&csv_buf);

    Ok(result)
}

/// エクスポート用にカラムを選択・整形
/// 派生カラム（_num系）を除外し、人間が読みやすいカラム名で出力
pub fn prepare_export_df(df: &DataFrame) -> Result<DataFrame, AppError> {
    // エクスポートに含めるカラム（元のカラム + 派生カラム）
    let export_columns = [
        "事業所番号",
        "事業所名",
        "管理者名",
        "管理者職名",
        "代表者名",
        "代表者職名",
        "法人名",
        "法人番号",
        "電話番号",
        "FAX番号",
        "住所",
        "HP",
        "従業者_常勤",
        "従業者_非常勤",
        "従業者_合計",
        "定員",
        "事業開始日",
        "前年度採用数",
        "前年度退職数",
        "都道府県",
        "法人種別",
        "離職率",
        "常勤比率",
        "事業年数",
    ];

    // 存在するカラムのみ選択
    let available: Vec<&str> = export_columns
        .iter()
        .filter(|&&col| df.column(col).is_ok())
        .copied()
        .collect();

    df.select(available)
        .map_err(|e| AppError::Internal(format!("エクスポートカラム選択エラー: {}", e)))
}
