/// 集計結果構造体
/// ダッシュボード・マーケット分析APIのレスポンス型

use serde::Serialize;

/// ダッシュボードKPI
/// 全体の要約指標
#[derive(Debug, Serialize)]
pub struct DashboardKpi {
    /// 総施設数
    pub total_facilities: usize,
    /// 平均従業者数
    pub avg_staff: f64,
    /// 平均定員
    pub avg_capacity: f64,
    /// 平均離職率
    pub avg_turnover_rate: f64,
    /// 平均常勤比率
    pub avg_fulltime_ratio: f64,
    /// 平均事業年数
    pub avg_years_in_business: f64,
}

/// 都道府県別サマリー
#[derive(Debug, Serialize)]
pub struct PrefectureSummary {
    /// 都道府県名
    pub prefecture: String,
    /// 施設数（JSONでは "facility_count" として出力）
    #[serde(rename = "facility_count")]
    pub count: usize,
    /// 平均従業者数
    pub avg_staff: f64,
    /// 平均定員
    pub avg_capacity: f64,
    /// 平均離職率
    pub avg_turnover_rate: f64,
}

/// サービス別サマリー
#[derive(Debug, Serialize)]
pub struct ServiceSummary {
    /// サービスコード
    pub service_code: String,
    /// サービス名
    pub service_name: String,
    /// 施設数（JSONでは "facility_count" として出力）
    #[serde(rename = "facility_count")]
    pub count: usize,
    /// 平均従業者数
    pub avg_staff: f64,
}

/// 都道府県メトリクス（コロプレスマップ用）
#[derive(Debug, Serialize)]
pub struct PrefectureMetric {
    /// 都道府県名
    pub prefecture: String,
    /// 施設数（JSONでは "facility_count" として出力）
    #[serde(rename = "facility_count")]
    pub count: usize,
    /// 平均従業者数
    pub avg_staff: f64,
    /// 平均離職率
    pub avg_turnover_rate: f64,
    /// 平均常勤比率
    pub avg_fulltime_ratio: f64,
}

/// サービス別棒グラフ用
#[derive(Debug, Serialize)]
pub struct ServiceBar {
    /// サービス名
    pub service_name: String,
    /// 施設数（JSONでは "facility_count" として出力）
    #[serde(rename = "facility_count")]
    pub count: usize,
    /// 平均従業者数
    pub avg_staff: f64,
}

/// 法人種別ドーナツチャート用
#[derive(Debug, Serialize)]
pub struct CorpTypeSlice {
    /// 法人種別名
    pub corp_type: String,
    /// 施設数
    pub count: usize,
    /// 全体に対する割合（0.0 - 1.0）
    pub ratio: f64,
}

// ========================================
// Phase 2: 人材分析レスポンス型
// ========================================

/// 人材KPI
#[derive(Debug, Serialize)]
pub struct WorkforceKpi {
    /// 平均離職率
    pub avg_turnover_rate: Option<f64>,
    /// 平均採用率（採用数 / 合計）
    pub avg_hire_rate: Option<f64>,
    /// 平均常勤比率
    pub avg_fulltime_ratio: Option<f64>,
    /// 平均経験10年以上比率（フルデータ用、19カラム版はnull）
    pub avg_experience_10yr_ratio: Option<f64>,
}

/// 離職率分布（ヒストグラム用）
#[derive(Debug, Serialize)]
pub struct TurnoverDistribution {
    /// 範囲ラベル（例: "0-5%"）
    pub range: String,
    /// 該当施設数
    pub count: usize,
}

/// 都道府県別人材指標
#[derive(Debug, Serialize)]
pub struct WorkforcePrefecture {
    /// 都道府県名
    pub prefecture: String,
    /// 平均離職率
    pub avg_turnover_rate: f64,
    /// 平均常勤比率
    pub avg_fulltime_ratio: f64,
    /// 施設数
    pub facility_count: usize,
}

/// 従業者規模別離職率
#[derive(Debug, Serialize)]
pub struct WorkforceBySize {
    /// 規模カテゴリ（例: "小規模(1-10)"）
    pub size_category: String,
    /// 平均離職率
    pub avg_turnover_rate: f64,
    /// 平均常勤比率
    pub avg_fulltime_ratio: f64,
    /// 施設数
    pub count: usize,
}

// ========================================
// Phase 2: 収益構造レスポンス型
// ========================================

/// 収益KPI
#[derive(Debug, Serialize)]
pub struct RevenueKpi {
    /// 平均加算取得数（76カラム時は実データ、19カラム版はnull）
    pub avg_kasan_count: Option<f64>,
    /// 処遇改善加算取得率（76カラム時は実データ、19カラム版はnull）
    pub syogu_kaizen_rate: Option<f64>,
    /// 平均稼働率（76カラム時は実データ、19カラム版はnull）
    pub avg_occupancy_rate: Option<f64>,
    /// 平均定員
    pub avg_capacity: Option<f64>,
    /// 平均品質スコア（76カラム時は実データ、19カラム版はnull）
    pub avg_quality_score: Option<f64>,
    /// 平均利用者数（76カラム時は実データ、19カラム版はnull）
    pub avg_user_count: Option<f64>,
}

/// 加算取得率（フルデータ用）
#[derive(Debug, Serialize)]
pub struct KasanRate {
    /// 加算名
    pub kasan_name: String,
    /// 取得率（0.0 - 1.0）
    pub rate: f64,
    /// 取得施設数
    pub count: usize,
}

/// 稼働率分布（フルデータ用）
#[derive(Debug, Serialize)]
pub struct OccupancyDistribution {
    /// 範囲ラベル（例: "60-70%"）
    pub range: String,
    /// 該当施設数
    pub count: usize,
}

// ========================================
// Phase 2: 賃金分析レスポンス型
// ========================================

/// 賃金KPI
#[derive(Debug, Serialize)]
pub struct SalaryKpi {
    /// 平均給与水準（フルデータ用、19カラム版はnull）
    pub avg_salary: Option<f64>,
    /// 中央値給与（フルデータ用、19カラム版はnull）
    pub median_salary: Option<f64>,
    /// 最高給与水準（フルデータ用、19カラム版はnull）
    pub max_salary: Option<f64>,
    /// 最低給与水準（フルデータ用、19カラム版はnull）
    pub min_salary: Option<f64>,
}

/// 職種別賃金（フルデータ用）
#[derive(Debug, Serialize)]
pub struct SalaryByJobType {
    /// 職種名
    pub job_type: String,
    /// 平均給与
    pub avg_salary: f64,
    /// 施設数
    pub count: usize,
}

/// 都道府県別賃金（フルデータ用）
#[derive(Debug, Serialize)]
pub struct SalaryByPrefecture {
    /// 都道府県名
    pub prefecture: String,
    /// 平均給与
    pub avg_salary: f64,
    /// 施設数
    pub count: usize,
}

// ========================================
// Phase 2: 経営品質レスポンス型
// ========================================

/// 経営品質KPI
#[derive(Debug, Serialize)]
pub struct QualityKpi {
    /// 平均損益差額比率（フルデータ用、19カラム版はnull）
    pub avg_profit_ratio: Option<f64>,
    /// 黒字施設割合（フルデータ用、19カラム版はnull）
    pub profitable_ratio: Option<f64>,
    /// 平均経験年数5年以上比率（フルデータ用、19カラム版はnull）
    pub avg_experienced_ratio: Option<f64>,
    /// 対象施設数
    pub facility_count: usize,
    /// 平均品質スコア（76カラム時は実データ、19カラム版はnull）
    pub avg_quality_score: Option<f64>,
    /// BCP策定率（76カラム時は実データ、19カラム版はnull）
    pub bcp_rate: Option<f64>,
    /// ICT活用率（76カラム時は実データ、19カラム版はnull）
    pub ict_rate: Option<f64>,
    /// 第三者評価実施率（76カラム時は実データ、19カラム版はnull）
    pub third_party_rate: Option<f64>,
    /// 損害賠償保険加入率（76カラム時は実データ、19カラム版はnull）
    pub insurance_rate: Option<f64>,
}

/// スコア分布（フルデータ用）
#[derive(Debug, Serialize)]
pub struct QualityScoreDistribution {
    /// 範囲ラベル（例: "-20%以下", "-10%〜0%"）
    pub range: String,
    /// 該当施設数
    pub count: usize,
}

/// 都道府県別経営品質（フルデータ用）
#[derive(Debug, Serialize)]
pub struct QualityByPrefecture {
    /// 都道府県名
    pub prefecture: String,
    /// 平均損益差額比率
    pub avg_profit_ratio: f64,
    /// 施設数
    pub count: usize,
}

// ========================================
// Phase 3: 法人グループ分析レスポンス型
// ========================================

/// 法人グループKPI
#[derive(Debug, Serialize)]
pub struct CorpGroupKpi {
    /// 法人総数（法人番号ありのユニーク数）
    pub total_corps: usize,
    /// 複数施設を持つ法人数
    pub multi_facility_corps: usize,
    /// 法人あたり平均施設数
    pub avg_facilities_per_corp: f64,
    /// 最多施設法人名
    pub max_facilities_corp_name: Option<String>,
    /// 最多施設数
    pub max_facilities_count: usize,
}

/// 法人規模別施設数分布
#[derive(Debug, Serialize)]
pub struct CorpSizeDistribution {
    /// カテゴリ名（例: "1施設"）
    pub category: String,
    /// 法人数
    pub count: usize,
}

/// 施設数上位法人
#[derive(Debug, Serialize)]
pub struct TopCorp {
    /// 法人名
    pub corp_name: String,
    /// 法人番号
    pub corp_number: String,
    /// 法人種別
    pub corp_type: Option<String>,
    /// 施設数
    pub facility_count: usize,
    /// 従業者合計
    pub total_staff: f64,
    /// 平均離職率
    pub avg_turnover_rate: Option<f64>,
    /// 展開都道府県
    pub prefectures: Vec<String>,
    /// サービス名一覧
    pub service_names: Vec<String>,
}

// ========================================
// Phase 3: 成長性分析レスポンス型
// ========================================

/// 成長性KPI
#[derive(Debug, Serialize)]
pub struct GrowthKpi {
    /// 直近3年間の設立施設数
    pub recent_3yr_count: usize,
    /// 平均事業年数
    pub avg_years_in_business: f64,
    /// 純成長率（直近3年設立数 / 全体数）
    pub net_growth_rate: f64,
    /// 事業開始日を持つ施設数
    pub total_with_start_date: usize,
}

/// 年別設立トレンド
#[derive(Debug, Serialize)]
pub struct EstablishmentTrend {
    /// 年
    pub year: i32,
    /// 設立施設数
    pub count: usize,
}

/// 事業年数分布
#[derive(Debug, Serialize)]
pub struct YearsDistribution {
    /// 範囲ラベル（例: "0-5年"）
    pub range: String,
    /// 施設数
    pub count: usize,
}

// ========================================
// Phase 3: M&Aスクリーニングレスポンス型
// ========================================

/// M&Aスクリーニング候補法人
#[derive(Debug, Serialize)]
pub struct MaCandidate {
    /// 法人名
    pub corp_name: String,
    /// 法人番号
    pub corp_number: String,
    /// 法人種別
    pub corp_type: Option<String>,
    /// 施設数
    pub facility_count: usize,
    /// 従業者合計
    pub total_staff: f64,
    /// 平均離職率
    pub avg_turnover_rate: Option<f64>,
    /// 平均定員
    pub avg_capacity: f64,
    /// 展開都道府県
    pub prefectures: Vec<String>,
    /// サービス名一覧
    pub service_names: Vec<String>,
    /// 魅力度スコア（0-100）
    pub attractiveness_score: f64,
}

/// M&Aファネル段階
#[derive(Debug, Serialize)]
pub struct MaFunnelStage {
    /// ステージ名
    pub stage: String,
    /// 件数
    pub count: usize,
}

/// M&Aスクリーニングレスポンス
#[derive(Debug, Serialize)]
pub struct MaScreeningResponse {
    /// 候補法人一覧
    pub items: Vec<MaCandidate>,
    /// 合計件数
    pub total: usize,
    /// ファネル情報
    pub funnel: Vec<MaFunnelStage>,
}

// ========================================
// Phase 3: DD支援レスポンス型
// ========================================

/// DD法人検索結果
#[derive(Debug, Serialize)]
pub struct DdCorpSearchResult {
    /// 法人名
    pub corp_name: String,
    /// 法人番号
    pub corp_number: String,
    /// 施設数
    pub facility_count: usize,
    /// 従業者合計
    pub total_staff: f64,
}

/// DD法人情報
#[derive(Debug, Serialize)]
pub struct DdCorpInfo {
    /// 法人名
    pub corp_name: String,
    /// 法人番号
    pub corp_number: String,
    /// 代表者名
    pub representative: Option<String>,
    /// 施設数
    pub facility_count: usize,
    /// 展開都道府県
    pub prefectures: Vec<String>,
}

/// DD事業デューデリジェンス
#[derive(Debug, Serialize)]
pub struct DdBusinessDd {
    /// 施設一覧（名称のみ）
    pub facilities: Vec<String>,
    /// サービス種別一覧
    pub service_types: Vec<String>,
    /// 平均定員
    pub avg_capacity: f64,
    /// 平均稼働率（データなしの場合null）
    pub avg_occupancy: Option<f64>,
    /// 従業者合計
    pub total_staff: f64,
}

/// DD人事デューデリジェンス
#[derive(Debug, Serialize)]
pub struct DdHrDd {
    /// 平均離職率
    pub avg_turnover_rate: Option<f64>,
    /// 平均常勤比率
    pub avg_fulltime_ratio: Option<f64>,
    /// 前年度採用数合計
    pub total_hired: f64,
    /// 前年度退職数合計
    pub total_left: f64,
}

/// DDコンプライアンス
#[derive(Debug, Serialize)]
pub struct DdComplianceDd {
    /// 違反有無（データなしの場合false）
    pub has_violations: bool,
    /// BCP策定率
    pub bcp_rate: Option<f64>,
    /// 賠償責任保険加入率
    pub insurance_rate: Option<f64>,
}

/// DD財務情報
#[derive(Debug, Serialize)]
pub struct DdFinancialDd {
    /// 会計処理方式
    pub accounting_type: Option<String>,
    /// 財務関連リンク
    pub financial_links: Vec<String>,
}

/// DDリスクフラグ
#[derive(Debug, Serialize)]
pub struct DdRiskFlag {
    /// レベル（green / yellow / red）
    pub level: String,
    /// カテゴリ
    pub category: String,
    /// 詳細説明
    pub detail: String,
}

/// DDベンチマーク（地域平均）
#[derive(Debug, Serialize)]
pub struct DdBenchmark {
    /// 地域平均離職率
    pub region_avg_turnover: f64,
    /// 地域平均従業者数
    pub region_avg_staff: f64,
    /// 地域平均定員
    pub region_avg_capacity: f64,
}

/// DD加算サマリー（施設別加算 + 法人集計）
#[derive(Debug, Serialize)]
pub struct DdKasanSummary {
    /// 施設別の加算取得状況
    pub facilities: Vec<CorpKasanFacility>,
    /// 加算項目ごとの取得施設数サマリー（加算名 → 取得施設数）
    pub totals: std::collections::HashMap<String, usize>,
    /// 対象施設数（分母）
    pub facility_count: usize,
    /// 加算データが存在するか
    pub has_data: bool,
}

/// DDレポート（全体）
#[derive(Debug, Serialize)]
pub struct DdReport {
    /// 法人情報
    pub corp_info: DdCorpInfo,
    /// 事業DD
    pub business_dd: DdBusinessDd,
    /// 人事DD
    pub hr_dd: DdHrDd,
    /// コンプライアンスDD
    pub compliance_dd: DdComplianceDd,
    /// 財務DD
    pub financial_dd: DdFinancialDd,
    /// リスクフラグ
    pub risk_flags: Vec<DdRiskFlag>,
    /// ベンチマーク
    pub benchmark: DdBenchmark,
    /// 加算取得サマリー
    pub kasan_summary: DdKasanSummary,
}

// ========================================
// Phase 3: PMIシナジーレスポンス型
// ========================================

/// PMI法人サマリー
#[derive(Debug, Serialize)]
pub struct PmiCorpSummary {
    /// 法人名
    pub corp_name: String,
    /// 施設一覧（名称）
    pub facilities: Vec<String>,
    /// 従業者合計
    pub total_staff: f64,
}

/// PMI統合サマリー
#[derive(Debug, Serialize)]
pub struct PmiCombined {
    /// 合計施設数
    pub total_facilities: usize,
    /// 合計従業者数
    pub total_staff: f64,
    /// 全サービスカバレッジ
    pub service_coverage: Vec<String>,
    /// 全展開エリア
    pub prefecture_coverage: Vec<String>,
    /// 重複サービス
    pub service_overlap: Vec<String>,
    /// 新規獲得サービス（target側にしかないもの）
    pub new_services: Vec<String>,
    /// 新規獲得エリア（target側にしかないもの）
    pub new_prefectures: Vec<String>,
}

/// PMIシナジー指標
#[derive(Debug, Serialize)]
pub struct PmiSynergy {
    /// 平均給与ギャップ（将来拡張用、現在は従業者数差で代替）
    pub wage_gap: f64,
    /// 離職率ギャップ
    pub turnover_gap: f64,
    /// 人材再配置ポテンシャル（従業者数の差分の10%を目安）
    pub staff_reallocation_potential: f64,
}

/// PMIシミュレーションレスポンス
#[derive(Debug, Serialize)]
pub struct PmiSimulationResponse {
    /// 買収側法人
    pub buyer: PmiCorpSummary,
    /// ターゲット法人
    pub target: PmiCorpSummary,
    /// 統合後サマリー
    pub combined: PmiCombined,
    /// シナジー指標
    pub synergy: PmiSynergy,
}

// ========================================
// 追加エンドポイント: レスポンス型
// ========================================

/// 経験10年以上割合の分布（ヒストグラム用）
#[derive(Debug, Serialize)]
pub struct ExperienceDistribution {
    /// 範囲ラベル（例: "0-20%"）
    pub range: String,
    /// 該当施設数
    pub count: usize,
}

/// 経験者割合 vs 離職率（都道府県別散布図用）
#[derive(Debug, Serialize)]
pub struct ExperienceVsTurnover {
    /// 都道府県名
    pub prefecture: String,
    /// 平均経験者割合（%）
    pub avg_experience_ratio: f64,
    /// 平均離職率（%）
    pub avg_turnover_rate: f64,
    /// 施設数
    pub facility_count: usize,
}

/// 品質ランク別分布
#[derive(Debug, Serialize)]
pub struct QualityRankDistribution {
    /// ランク（S/A/B/C/D）
    pub rank: String,
    /// 該当施設数
    pub count: usize,
    /// 表示色
    pub color: String,
}

/// 品質カテゴリ平均（レーダーチャート用）
#[derive(Debug, Serialize)]
pub struct QualityCategoryRadar {
    /// カテゴリ名
    pub category: String,
    /// 平均スコア
    pub score: f64,
    /// 満点値
    #[serde(rename = "fullMark")]
    pub full_mark: f64,
}

/// 法人内施設の加算情報
#[derive(Debug, Serialize)]
pub struct CorpKasanFacility {
    /// 施設名
    pub facility_name: String,
    /// 加算取得マップ（加算名 → 取得有無）
    pub kasan: std::collections::HashMap<String, bool>,
}

/// 法人別加算ヒートマップエントリ
#[derive(Debug, Serialize)]
pub struct CorpKasanHeatmapEntry {
    /// 法人名
    pub corp_name: String,
    /// 施設一覧（加算情報付き）
    pub facilities: Vec<CorpKasanFacility>,
}

/// 法人内加算ヒートマップレスポンス
#[derive(Debug, Serialize)]
pub struct CorpKasanHeatmapResponse {
    /// 法人一覧
    pub corps: Vec<CorpKasanHeatmapEntry>,
}

/// ベンチマークレーダー軸
#[derive(Debug, Serialize)]
pub struct BenchmarkRadarAxis {
    /// 軸名
    pub axis: String,
    /// 施設の値
    pub value: f64,
    /// 全国平均
    pub national_avg: f64,
    /// 都道府県平均
    pub pref_avg: f64,
}

/// ベンチマーク改善提案
#[derive(Debug, Serialize)]
pub struct ImprovementSuggestion {
    /// 対象軸
    pub axis: String,
    /// 現在値
    pub current: f64,
    /// 目標値
    pub target: f64,
    /// 提案内容
    pub suggestion: String,
}

/// 施設ベンチマークレスポンス
#[derive(Debug, Serialize)]
pub struct BenchmarkResponse {
    /// 施設基本情報
    pub facility: serde_json::Value,
    /// レーダーチャートデータ（8軸）
    pub radar: Vec<BenchmarkRadarAxis>,
    /// パーセンタイル情報
    pub percentiles: serde_json::Value,
    /// 改善提案
    pub improvement_suggestions: Vec<ImprovementSuggestion>,
}

/// データメタ情報
#[derive(Debug, Serialize)]
pub struct DataMeta {
    /// 総施設数
    pub total_count: usize,
    /// 都道府県一覧（選択肢）
    pub prefectures: Vec<String>,
    /// サービスコード一覧（選択肢）
    pub service_codes: Vec<String>,
    /// 法人種別一覧（選択肢）
    pub corp_types: Vec<String>,
    /// 従業者数の範囲
    pub staff_range: (f64, f64),
}
