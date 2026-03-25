/// KPIキャッシュストア
/// kpi_cacheテーブルから事前計算済みのJSON結果を読み込み、メモリに保持する
/// DataFrameベースのリアルタイム集計を置き換えるための読み取り専用キャッシュ層

use std::collections::HashMap;

use libsql::Database;
use serde_json::Value;
use tracing::info;

use crate::error::AppError;

/// キャッシュエントリ（1行分のデータ）
struct CacheEntry {
    /// 事前計算済みのJSON値
    value: Value,
    /// 最終更新日時（ISO 8601形式）
    updated_at: String,
    /// 元データの行数
    row_count: i64,
}

/// KPIキャッシュストア
/// kpi_cacheテーブルの全エントリをメモリに保持し、高速なルックアップを提供する
/// key -> filter_key -> CacheEntry の2階層HashMapで管理
pub struct CacheStore {
    /// key -> (filter_key -> CacheEntry) のネストされたHashMap
    cache: HashMap<String, HashMap<String, CacheEntry>>,
}

impl CacheStore {
    /// kpi_cacheテーブルから全エントリを読み込んでCacheStoreを初期化する
    ///
    /// SQL: SELECT key, filter_key, value, updated_at, row_count FROM kpi_cache
    /// 各行のvalueカラムをserde_json::Valueとしてパースし、2階層HashMapに格納する
    pub async fn load(db: &Database) -> Result<Self, AppError> {
        let conn = db.connect().map_err(|e| {
            AppError::Internal(format!("Turso接続エラー (kpi_cache): {}", e))
        })?;

        let rows = conn
            .query(
                "SELECT key, filter_key, value, updated_at, row_count FROM kpi_cache",
                (),
            )
            .await
            .map_err(|e| {
                AppError::Internal(format!("kpi_cacheクエリエラー: {}", e))
            })?;

        let mut cache: HashMap<String, HashMap<String, CacheEntry>> = HashMap::new();
        let mut total_entries = 0u64;
        let mut parse_errors = 0u64;
        let mut current_rows = rows;

        loop {
            match current_rows.next().await {
                Ok(Some(row)) => {
                    // 各カラムを取得（型エラーは行単位でスキップ）
                    let key = match row.get::<String>(0) {
                        Ok(v) => v,
                        Err(e) => {
                            tracing::warn!("kpi_cache行のkey取得失敗: {}", e);
                            continue;
                        }
                    };
                    let filter_key = row.get::<String>(1).unwrap_or_default();
                    let value_str = match row.get::<String>(2) {
                        Ok(v) => v,
                        Err(e) => {
                            tracing::warn!(
                                "kpi_cache行のvalue取得失敗 (key={}): {}",
                                key,
                                e
                            );
                            continue;
                        }
                    };
                    let updated_at = row.get::<String>(3).unwrap_or_default();
                    let row_count = row.get::<i64>(4).unwrap_or(0);

                    // JSONパース（失敗時はスキップしてログ出力）
                    let value: Value = match serde_json::from_str(&value_str) {
                        Ok(v) => v,
                        Err(e) => {
                            tracing::warn!(
                                "kpi_cache JSONパースエラー (key={}, filter_key={}): {}",
                                key,
                                filter_key,
                                e
                            );
                            parse_errors += 1;
                            continue;
                        }
                    };

                    cache
                        .entry(key)
                        .or_default()
                        .insert(
                            filter_key,
                            CacheEntry {
                                value,
                                updated_at,
                                row_count,
                            },
                        );
                    total_entries += 1;
                }
                Ok(None) => break,
                Err(e) => {
                    tracing::warn!("kpi_cache行読み込みエラー: {}", e);
                    break;
                }
            }
        }

        let unique_keys = cache.len();
        info!(
            "kpi_cacheロード完了: {}エントリ ({}キー), パースエラー: {}件",
            total_entries, unique_keys, parse_errors
        );

        Ok(Self { cache })
    }

    /// 指定されたキーとフィルタキーに対応するキャッシュ値を取得する
    ///
    /// 戻り値: キャッシュにヒットした場合はSome(&Value)、なければNone
    pub fn get(&self, key: &str, filter_key: &str) -> Option<&Value> {
        self.cache.get(key)?.get(filter_key).map(|e| &e.value)
    }

    /// フィルタなし（グローバル集計）のキャッシュ値を取得する
    ///
    /// filter_key="" として検索する。都道府県やサービス種別を絞り込まない
    /// 全体集計結果の取得に使用する
    pub fn get_global(&self, key: &str) -> Option<&Value> {
        self.get(key, "")
    }

    /// キャッシュに格納されている全キーの一覧を返す
    pub fn keys(&self) -> Vec<&str> {
        self.cache.keys().map(|k| k.as_str()).collect()
    }

    /// キャッシュのメタデータをJSON形式で返す（/api/meta エンドポイント用）
    ///
    /// 含まれる情報:
    /// - total_keys: ユニークなキー数
    /// - total_entries: 全エントリ数（キー×フィルタキーの組み合わせ）
    /// - last_updated: 全エントリ中の最新更新日時
    /// - keys: 各キーのフィルタ数と最新更新日時の一覧
    pub fn get_metadata(&self) -> Value {
        let total_keys = self.cache.len();
        let mut total_entries = 0usize;
        let mut last_updated: Option<&str> = None;
        let mut key_details = serde_json::Map::new();

        for (key, filters) in &self.cache {
            let filter_count = filters.len();
            total_entries += filter_count;

            // このキー配下で最も新しいupdated_atを特定
            let key_last_updated = filters
                .values()
                .map(|e| e.updated_at.as_str())
                .max()
                .unwrap_or("");

            // 全体の最新日時を更新
            if let Some(current) = last_updated {
                if key_last_updated > current {
                    last_updated = Some(key_last_updated);
                }
            } else {
                last_updated = Some(key_last_updated);
            }

            key_details.insert(
                key.clone(),
                serde_json::json!({
                    "filter_count": filter_count,
                    "last_updated": key_last_updated,
                }),
            );
        }

        serde_json::json!({
            "total_keys": total_keys,
            "total_entries": total_entries,
            "last_updated": last_updated.unwrap_or(""),
            "keys": key_details,
        })
    }
}
