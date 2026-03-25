/// アプリケーション設定
/// 環境変数からデータパス・ポートを読み込む

/// サーバー設定
pub struct AppConfig {
    /// データファイルのパス（CSV or Parquet）
    pub data_path: String,
    /// サーバーポート番号
    pub port: u16,
}

impl AppConfig {
    /// 環境変数から設定を読み込む
    /// - KAIGO_DATA_PATH: データファイルパス（デフォルト: ../data/output/kaigo_scraping/tokyo_day_care_150_20260319.csv）
    /// - KAIGO_PORT: サーバーポート（デフォルト: 3001）
    pub fn from_env() -> Self {
        let data_path = std::env::var("KAIGO_DATA_PATH").unwrap_or_else(|_| {
            "../data/output/kaigo_scraping/facilities.parquet".to_string()
        });
        let port = std::env::var("KAIGO_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(3001);

        Self { data_path, port }
    }
}
