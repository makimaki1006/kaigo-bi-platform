/// 施設データのシリアライズ構造体
/// 19カラム基本構成 + 76カラム拡張対応（Option型）

use serde::{Deserialize, Serialize};

/// 施設データ（基本19カラム + 76カラム拡張）
/// CSVから読み込んだ1行に対応する構造体
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Facility {
    /// 事業所番号（一意識別子）
    pub jigyosho_number: String,
    /// 事業所名
    pub jigyosho_name: String,
    /// 管理者名
    pub manager_name: Option<String>,
    /// 管理者職名
    pub manager_title: Option<String>,
    /// 代表者名
    pub representative_name: Option<String>,
    /// 代表者職名
    pub representative_title: Option<String>,
    /// 法人名
    pub corp_name: Option<String>,
    /// 法人番号
    pub corp_number: Option<String>,
    /// 電話番号
    pub phone: Option<String>,
    /// FAX番号
    pub fax: Option<String>,
    /// 住所
    pub address: Option<String>,
    /// ホームページURL
    pub homepage: Option<String>,
    /// 従業者数（常勤）
    pub staff_fulltime: Option<f64>,
    /// 従業者数（非常勤）
    pub staff_parttime: Option<f64>,
    /// 従業者数（合計）
    pub staff_total: Option<f64>,
    /// 定員
    pub capacity: Option<f64>,
    /// 事業開始日
    pub start_date: Option<String>,
    /// 前年度採用数
    pub hired_last_year: Option<f64>,
    /// 前年度退職数
    pub left_last_year: Option<f64>,

    // === 派生カラム（DataStoreで計算） ===
    /// 都道府県（住所から抽出）
    #[serde(skip_deserializing)]
    pub prefecture: Option<String>,
    /// 法人種別（法人名から推定）
    #[serde(skip_deserializing)]
    pub corp_type: Option<String>,
    /// 離職率 = 退職数 / (合計 + 退職数)
    #[serde(skip_deserializing)]
    pub turnover_rate: Option<f64>,
    /// 常勤比率 = 常勤 / 合計
    #[serde(skip_deserializing)]
    pub fulltime_ratio: Option<f64>,
    /// 事業年数 = 2026 - 事業開始年
    #[serde(skip_deserializing)]
    pub years_in_business: Option<f64>,

    // === 76カラム拡張: サービス情報 ===
    /// サービスコード
    #[serde(skip_deserializing)]
    pub service_code: Option<String>,
    /// サービス名
    #[serde(skip_deserializing)]
    pub service_name: Option<String>,

    // === 76カラム拡張: 加算13項目 ===
    /// 処遇改善加算I
    #[serde(skip_deserializing)]
    pub kasan_syogu_kaizen_1: Option<bool>,
    /// 処遇改善加算II
    #[serde(skip_deserializing)]
    pub kasan_syogu_kaizen_2: Option<bool>,
    /// 処遇改善加算III
    #[serde(skip_deserializing)]
    pub kasan_syogu_kaizen_3: Option<bool>,
    /// 処遇改善加算IV
    #[serde(skip_deserializing)]
    pub kasan_syogu_kaizen_4: Option<bool>,
    /// 特定事業所加算I
    #[serde(skip_deserializing)]
    pub kasan_tokutei_1: Option<bool>,
    /// 特定事業所加算II
    #[serde(skip_deserializing)]
    pub kasan_tokutei_2: Option<bool>,
    /// 特定事業所加算III
    #[serde(skip_deserializing)]
    pub kasan_tokutei_3: Option<bool>,
    /// 特定事業所加算IV
    #[serde(skip_deserializing)]
    pub kasan_tokutei_4: Option<bool>,
    /// 特定事業所加算V
    #[serde(skip_deserializing)]
    pub kasan_tokutei_5: Option<bool>,
    /// 認知症ケア加算I
    #[serde(skip_deserializing)]
    pub kasan_ninchisho_1: Option<bool>,
    /// 認知症ケア加算II
    #[serde(skip_deserializing)]
    pub kasan_ninchisho_2: Option<bool>,
    /// 口腔連携加算
    #[serde(skip_deserializing)]
    pub kasan_koku_renkei: Option<bool>,
    /// 緊急時加算
    #[serde(skip_deserializing)]
    pub kasan_kinkyuji: Option<bool>,

    // === 76カラム拡張: 品質 ===
    /// BCP策定済み
    #[serde(skip_deserializing)]
    pub quality_bcp: Option<bool>,
    /// ICT活用
    #[serde(skip_deserializing)]
    pub quality_ict: Option<bool>,
    /// 第三者評価実施
    #[serde(skip_deserializing)]
    pub quality_third_party: Option<bool>,
    /// 損害賠償保険加入
    #[serde(skip_deserializing)]
    pub quality_insurance: Option<bool>,

    // === 76カラム拡張: 利用者 ===
    /// 利用者総数
    #[serde(skip_deserializing)]
    pub user_count: Option<f64>,
    /// 利用者_都道府県平均
    #[serde(skip_deserializing)]
    pub user_pref_avg: Option<f64>,
    /// 要介護1人数
    #[serde(skip_deserializing)]
    pub care_level_1: Option<f64>,
    /// 要介護2人数
    #[serde(skip_deserializing)]
    pub care_level_2: Option<f64>,
    /// 要介護3人数
    #[serde(skip_deserializing)]
    pub care_level_3: Option<f64>,
    /// 要介護4人数
    #[serde(skip_deserializing)]
    pub care_level_4: Option<f64>,
    /// 要介護5人数
    #[serde(skip_deserializing)]
    pub care_level_5: Option<f64>,
    /// 要支援1人数
    #[serde(skip_deserializing)]
    pub care_level_support1: Option<f64>,
    /// 要支援2人数
    #[serde(skip_deserializing)]
    pub care_level_support2: Option<f64>,
    /// 経験10年以上割合（例: "72.7"）
    #[serde(skip_deserializing)]
    pub experienced_ratio: Option<String>,

    // === 76カラム拡張: 財務 ===
    /// 会計種類
    #[serde(skip_deserializing)]
    pub accounting_type: Option<String>,
    /// 財務諸表DLリンク_事業活動
    #[serde(skip_deserializing)]
    pub financial_dl_pl: Option<String>,
    /// 財務諸表DLリンク_資金収支
    #[serde(skip_deserializing)]
    pub financial_dl_cf: Option<String>,
    /// 財務諸表DLリンク_貸借対照
    #[serde(skip_deserializing)]
    pub financial_dl_bs: Option<String>,

    // === 76カラム拡張: 賃金 ===
    /// 代表賃金（月額1〜5の中央値）
    #[serde(skip_deserializing)]
    pub salary_representative: Option<f64>,

    // === 76カラム拡張: 派生指標（DataStoreで計算） ===
    /// 稼働率 = 利用者総数 / 定員
    #[serde(skip_deserializing)]
    pub occupancy_rate: Option<f64>,
    /// 加算取得数（13加算のTrueカウント）
    #[serde(skip_deserializing)]
    pub kasan_count: Option<i32>,
    /// 品質スコア（0-100）
    #[serde(skip_deserializing)]
    pub quality_score: Option<f64>,
    /// 品質ランク（S/A/B/C/D）
    #[serde(skip_deserializing)]
    pub quality_rank: Option<String>,
    /// 平均要介護度
    #[serde(skip_deserializing)]
    pub avg_care_level: Option<f64>,
    /// 重度者割合（要介護4+5 / 利用者総数）
    #[serde(skip_deserializing)]
    pub severe_rate: Option<f64>,

    // === 位置情報（オープンデータCSVから突合） ===
    /// 緯度
    #[serde(skip_deserializing)]
    pub latitude: Option<f64>,
    /// 経度
    #[serde(skip_deserializing)]
    pub longitude: Option<f64>,
    /// 市区町村名
    #[serde(skip_deserializing)]
    pub municipality: Option<String>,

    // === 旧76カラム互換フィールド ===
    /// 介護職員数
    #[serde(skip_deserializing)]
    pub care_staff_count: Option<f64>,
    /// 看護職員数
    #[serde(skip_deserializing)]
    pub nurse_count: Option<f64>,
    /// 機能訓練指導員数
    #[serde(skip_deserializing)]
    pub rehab_staff_count: Option<f64>,
    /// 損益差額比率
    #[serde(skip_deserializing)]
    pub profit_ratio: Option<f64>,
    /// 給与水準
    #[serde(skip_deserializing)]
    pub salary_level: Option<f64>,
}

/// 施設詳細レスポンス（単一施設の全情報）
#[derive(Debug, Serialize)]
pub struct FacilityDetail {
    pub facility: Facility,
}

/// 施設検索結果レスポンス（ページネーション付き）
#[derive(Debug, Serialize)]
pub struct FacilitySearchResult {
    /// 検索結果の施設リスト
    pub items: Vec<Facility>,
    /// 総件数（フィルタ後）
    pub total: usize,
    /// 現在のページ番号（1始まり）
    pub page: usize,
    /// 1ページあたりの件数
    pub per_page: usize,
    /// 総ページ数
    pub total_pages: usize,
}
