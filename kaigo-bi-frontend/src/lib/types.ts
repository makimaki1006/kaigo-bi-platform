// ===================================================
// API レスポンス型定義
// バックエンド（Rust）のsnake_caseに合わせる
// ===================================================

/** データメタ情報 */
export interface DataMeta {
  total_count: number;
  prefectures: string[];
  service_codes: { code: string; name: string }[];
  corp_types: string[];
  staff_range: [number, number];
}

/** ダッシュボード KPI */
export interface DashboardKpi {
  total_facilities: number;
  avg_staff: number;
  avg_capacity: number;
  avg_turnover_rate: number;
  avg_fulltime_ratio: number;
  avg_years_in_business: number;
}

/** 都道府県別サマリー */
export interface PrefectureSummary {
  prefecture: string;
  facility_count: number;
  total_staff: number;
  avg_staff: number;
  avg_capacity: number;
  avg_turnover_rate: number;
}

/** サービス種別別サマリー */
export interface ServiceSummary {
  service_code: string;
  service_name: string;
  facility_count: number;
  total_staff: number;
}

/** 法人種別別サマリー */
export interface CorpTypeSummary {
  corp_type: string;
  count: number;
  percentage: number;
}

/** 施設行（検索結果） */
export interface FacilityRow {
  jigyosho_number: string;
  jigyosho_name: string;
  manager_name: string | null;
  manager_title: string | null;
  representative_name: string | null;
  representative_title: string | null;
  corp_name: string;
  corp_number: string | null;
  phone: string;
  fax: string | null;
  address: string;
  homepage: string | null;
  staff_fulltime: number | null;
  staff_parttime: number | null;
  staff_total: number | null;
  capacity: number | null;
  start_date: string | null;
  hired_last_year: number | null;
  left_last_year: number | null;
  prefecture: string | null;
  corp_type: string | null;
  turnover_rate: number | null;
  fulltime_ratio: number | null;
  years_in_business: number | null;
  service_code: string | null;
  service_name: string | null;
}

/** 施設検索結果（ページネーション付き） */
export interface FacilitySearchResult {
  items: FacilityRow[];
  total: number;
  page: number;
  per_page: number;
  total_pages: number;
}

/** フィルタ状態 */
export interface FilterState {
  prefectures: string[];
  serviceCodes: string[];
  corpTypes: string[];
  employeeMin: number | null;
  employeeMax: number | null;
  keyword: string;
}

/** テーブルカラム定義 */
export interface ColumnDef<T> {
  key: string;
  label: string;
  sortable?: boolean;
  width?: string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  render?: (value: any, row: T) => React.ReactNode;
}

/** ソート状態 */
export interface SortState {
  key: string;
  direction: "asc" | "desc";
}

/** ページネーション状態 */
export interface PaginationState {
  page: number;
  pageSize: number;
  total: number;
  totalPages: number;
}

// ===================================================
// 認証・ユーザー管理 型定義
// ===================================================

/** ユーザー */
export interface User {
  id: string;
  email: string;
  name: string;
  role: "admin" | "consultant" | "sales" | "viewer";
  is_active: boolean;
  expires_at: string | null;
  created_at: string;
}

/** ログインリクエスト */
export interface LoginRequest {
  email: string;
  password: string;
}

/** ログインレスポンス */
export interface LoginResponse {
  token: string;
  user: User;
}

/** ユーザー作成リクエスト */
export interface UserCreateRequest {
  email: string;
  name: string;
  password: string;
  role: "admin" | "consultant" | "sales" | "viewer";
  expires_at: string | null;
}

/** ユーザー更新リクエスト */
export interface UserUpdateRequest {
  email?: string;
  name?: string;
  password?: string;
  role?: "admin" | "consultant" | "sales" | "viewer";
  expires_at?: string | null;
  is_active?: boolean;
}

/** 操作ログ */
export interface AuditLog {
  id: number;
  user_id: string;
  user_name?: string;
  action: string;
  details: string | null;
  ip_address: string | null;
  created_at: string;
}

/** 操作ログ検索結果 */
export interface AuditLogSearchResult {
  items: AuditLog[];
  total: number;
  page: number;
  pageSize: number;
  totalPages: number;
}

// ===================================================
// Phase 2: 分析ページ型定義
// ===================================================

/** 人材分析 KPI（バックエンド WorkforceKpi に対応） */
export interface WorkforceKpi {
  avg_turnover_rate: number | null;
  avg_hire_rate: number | null;
  avg_fulltime_ratio: number | null;
  avg_experience_10yr_ratio: number | null;
}

/** 人材分析 離職率分布ビン */
export interface TurnoverBin {
  range: string;
  count: number;
}

/** 人材分析 常勤/非常勤構成 */
export interface StaffComposition {
  service_name: string;
  fulltime: number;
  parttime: number;
}

/** 人材分析 離職率 vs 賃金散布図データ */
export interface TurnoverWagePoint {
  name: string;
  turnover_rate: number;
  avg_wage: number;
}

/** 都道府県別離職率 */
export interface PrefectureTurnover {
  prefecture: string;
  turnover_rate: number;
}

/** 従業者規模別離職率（バックエンド WorkforceBySize に対応） */
export interface SizeGroupTurnover {
  size_category: string;
  avg_turnover_rate: number;
  avg_fulltime_ratio: number;
  count: number;
  /** フロント用エイリアス（互換性のため） */
  size_group?: string;
  turnover_rate?: number;
}

/** 都道府県別人材指標（バックエンド WorkforcePrefecture に対応） */
export interface WorkforcePrefectureData {
  prefecture: string;
  avg_turnover_rate: number;
  avg_fulltime_ratio: number;
  facility_count: number;
}

/** 収益構造 KPI（バックエンド RevenueKpi 構造体に対応） */
export interface RevenueKpi {
  avg_kasan_count: number | null;
  syogu_kaizen_rate: number | null;
  avg_occupancy_rate: number | null;
  avg_capacity: number | null;
}

/** 加算項目別取得率（バックエンド KasanRate に対応） */
export interface BonusItemRate {
  kasan_name: string;
  rate: number;
  count: number;
}

/** 稼働率ビン */
export interface OccupancyBin {
  range: string;
  count: number;
}

/** 加算数 vs 従業者規模 散布図データ */
export interface BonusSizePoint {
  name: string;
  bonus_count: number;
  staff_total: number;
}

/** 都道府県別処遇改善加算取得率 */
export interface PrefectureTreatmentRate {
  prefecture: string;
  rate: number;
}

/** 賃金分析 KPI（バックエンド SalaryKpi 構造体に対応） */
export interface SalaryKpi {
  avg_salary: number | null;
  median_salary: number | null;
  max_salary: number | null;
  min_salary: number | null;
  /** 賃金データ件数（kpi_cacheで追加される場合あり） */
  data_count?: number | null;
}

/** 職種別賃金（バックエンド SalaryByJobType に対応） */
export interface JobTypeWage {
  job_type: string;
  avg_salary: number;
  count: number;
  /** 平均年齢（kpi_cacheで追加される場合あり） */
  avg_age?: number | null;
  /** 平均勤続年数（kpi_cacheで追加される場合あり） */
  avg_tenure?: string | null;
}

/** 都道府県別賃金（バックエンド SalaryByPrefecture に対応） */
export interface PrefectureJobWage {
  prefecture: string;
  avg_salary: number;
  count: number;
}

/** 賃金 vs 離職率散布図データ */
export interface WageTurnoverPoint {
  name: string;
  avg_wage: number;
  turnover_rate: number;
}

/** 賃金分布ビン */
export interface WageBin {
  range: string;
  count: number;
}

/** 経営品質 KPI（バックエンド QualityKpi 構造体に対応） */
export interface QualityKpi {
  avg_profit_ratio: number | null;
  profitable_ratio: number | null;
  avg_experienced_ratio: number | null;
  facility_count: number;
}

/** スコア分布ビン */
export interface ScoreRankBin {
  rank: string;
  count: number;
  color: string;
}

/** レーダーチャート用4カテゴリデータ */
export interface QualityRadarItem {
  category: string;
  score: number;
  fullMark: number;
}

/** 都道府県別経営品質（バックエンド QualityByPrefecture に対応） */
export interface PrefectureQualityScore {
  prefecture: string;
  avg_profit_ratio: number;
  count: number;
}

/** BCP x ICT x 第三者評価マトリクス */
export interface QualityMatrixCell {
  bcp: string;
  ict: string;
  third_party: string;
  count: number;
}

// ===================================================
// Phase 3: 法人グループ・M&A分析ページ型定義
// ===================================================

/** 法人グループ KPI（APIレスポンス） */
export interface CorpGroupKpi {
  total_corps: number;
  multi_facility_corps: number;
  avg_facilities_per_corp: number;
  max_facilities_corp_name: string | null;
  max_facilities_count: number;
}

/** 法人規模別分布（APIレスポンス: categoryキー） */
export interface CorpSizeDistribution {
  category: string;
  count: number;
}

/** 法人種別×平均施設数（モック専用、API未提供） */
export interface CorpTypeAvgFacilities {
  corp_type: string;
  avg_facilities: number;
}

/** TOP法人テーブル行（APIレスポンス） */
export interface TopCorpRow {
  corp_name: string;
  corp_number: string;
  corp_type: string | null;
  facility_count: number;
  total_staff: number;
  avg_turnover_rate: number | null;
  prefectures: string[];
  service_names: string[];
}

/** 成長性 KPI（APIレスポンス） */
export interface GrowthKpi {
  recent_3yr_count: number;
  avg_years_in_business: number;
  net_growth_rate: number;
  total_with_start_date: number;
}

/** 施設設立年推移（APIレスポンス: yearはnumber） */
export interface EstablishmentYearCount {
  year: number;
  count: number;
}

/** 事業年数分布（APIレスポンス） */
export interface BusinessYearsBin {
  range: string;
  count: number;
}

/** 安定性マトリクス（散布図データ） */
export interface StabilityPoint {
  name: string;
  occupancy_rate: number;
  turnover_rate: number;
}

/** 成長フェーズ別分布 */
export interface GrowthPhaseDistribution {
  phase: string;
  count: number;
}

/** M&Aスクリーニング フィルタ状態（拡張版） */
export interface MaScreeningFilters extends FilterState {
  occupancyMin: number | null;
  occupancyMax: number | null;
  turnoverMin: number | null;
  turnoverMax: number | null;
  qualityScoreMin: number | null;
  qualityScoreMax: number | null;
  facilityCountMin: number | null;
  facilityCountMax: number | null;
}

/** M&Aスクリーニング ターゲット行 */
export interface MaTargetRow {
  corp_name: string;
  corp_number: string | null;
  facility_count: number;
  prefecture: string;
  total_staff: number;
  turnover_rate: number | null;
  attractiveness_score: number;
  service_types: string[];
}

/** ファネルステップ */
export interface FunnelStep {
  label: string;
  count: number;
}

/** DD法人基本情報 */
export interface DdCorpInfo {
  corp_name: string;
  corp_number: string | null;
  corp_type: string;
  representative: string | null;
  facility_count: number;
  service_types: string[];
  total_staff: number;
  avg_years_in_business: number;
}

/** DD 4軸スコア */
export interface DdAxisScore {
  axis: string;
  score: number;
  fullMark: number;
}

/** DDリスクフラグ */
export interface DdRiskFlag {
  category: string;
  item: string;
  level: "red" | "yellow" | "green";
  detail: string;
}

/** DDベンチマーク比較行 */
export interface DdBenchmarkRow {
  metric: string;
  target_value: string;
  area_avg: string;
  difference: string;
  evaluation: "good" | "average" | "poor";
}

/** PMI統合前後比較 */
export interface PmiBeforeAfter {
  metric: string;
  buyer: number;
  target: number;
  combined: number;
}

/** PMIサービスポートフォリオ補完 */
export interface PmiServiceCoverage {
  service_name: string;
  buyer_has: boolean;
  target_has: boolean;
}

/** PMI地域カバレッジ */
export interface PmiRegionCoverage {
  prefecture: string;
  category: "buyer_only" | "target_only" | "overlap";
}

/** PMI統合リスク */
export interface PmiIntegrationRisk {
  risk_item: string;
  buyer_value: string;
  target_value: string;
  gap: string;
  severity: "high" | "medium" | "low";
}

// ===================================================
// Phase 3: 実API レスポンス型（バックエンドRust構造体に対応）
// ===================================================

/** M&Aスクリーニング候補法人（実API） */
export interface MaCandidate {
  corp_name: string;
  corp_number: string;
  corp_type: string | null;
  facility_count: number;
  total_staff: number;
  avg_turnover_rate: number | null;
  avg_capacity: number;
  prefectures: string[];
  service_names: string[];
  attractiveness_score: number;
}

/** M&Aファネル段階（実API） */
export interface MaFunnelStage {
  stage: string;
  count: number;
}

/** M&Aスクリーニングレスポンス（実API） */
export interface MaScreeningResponse {
  items: MaCandidate[];
  total: number;
  funnel: MaFunnelStage[];
}

/** DD法人検索結果（実API） */
export interface DdSearchResult {
  corp_name: string;
  corp_number: string;
  facility_count: number;
  total_staff: number;
}

/** DD法人基本情報（実API） */
export interface DdCorpInfoApi {
  corp_name: string;
  corp_number: string;
  representative: string | null;
  facility_count: number;
  prefectures: string[];
}

/** DD事業デューデリジェンス（実API） */
export interface DdBusinessDd {
  facilities: string[];
  service_types: string[];
  avg_capacity: number;
  avg_occupancy: number | null;
  total_staff: number;
}

/** DD人事デューデリジェンス（実API） */
export interface DdHrDd {
  avg_turnover_rate: number | null;
  avg_fulltime_ratio: number | null;
  total_hired: number;
  total_left: number;
}

/** DDコンプライアンス（実API） */
export interface DdComplianceDd {
  has_violations: boolean;
  bcp_rate: number | null;
  insurance_rate: number | null;
}

/** DD財務情報（実API） */
export interface DdFinancialDd {
  accounting_type: string | null;
  financial_links: string[];
}

/** DDリスクフラグ（実API） */
export interface DdRiskFlagApi {
  level: string;
  category: string;
  detail: string;
}

/** DDベンチマーク（実API） */
export interface DdBenchmarkApi {
  region_avg_turnover: number;
  region_avg_staff: number;
  region_avg_capacity: number;
}

/** DD加算施設データ（実API） */
export interface DdKasanFacility {
  facility_name: string;
  kasan: Record<string, boolean>;
}

/** DD加算サマリー（実API） */
export interface DdKasanSummary {
  facilities: DdKasanFacility[];
  totals: Record<string, number>;
  facility_count: number;
  has_data: boolean;
}

/** DDレポート全体（実API） */
export interface DdReportResponse {
  corp_info: DdCorpInfoApi;
  business_dd: DdBusinessDd;
  hr_dd: DdHrDd;
  compliance_dd: DdComplianceDd;
  financial_dd: DdFinancialDd;
  risk_flags: DdRiskFlagApi[];
  benchmark: DdBenchmarkApi;
  kasan_summary: DdKasanSummary;
}

/** PMI法人サマリー（実API） */
export interface PmiCorpSummary {
  corp_name: string;
  facilities: string[];
  total_staff: number;
}

/** PMI統合サマリー（実API） */
export interface PmiCombined {
  total_facilities: number;
  total_staff: number;
  service_coverage: string[];
  prefecture_coverage: string[];
  service_overlap: string[];
  new_services: string[];
  new_prefectures: string[];
}

/** PMIシナジー指標（実API） */
export interface PmiSynergyMetrics {
  wage_gap: number;
  turnover_gap: number;
  staff_reallocation_potential: number;
}

/** PMIシミュレーションレスポンス（実API） */
export interface PmiSimulationResponse {
  buyer: PmiCorpSummary;
  target: PmiCorpSummary;
  combined: PmiCombined;
  synergy: PmiSynergyMetrics;
}

// ===================================================
// Phase 4: 新規データ活用 型定義
// 加算・品質・要介護度・経験者割合・BCP・ICT等
// ===================================================

/** 加算取得状況（13項目） */
export interface KasanStatus {
  addition_treatment_i: boolean | null;
  addition_treatment_ii: boolean | null;
  addition_treatment_iii: boolean | null;
  addition_treatment_iv: boolean | null;
  addition_specific_i: boolean | null;
  addition_specific_ii: boolean | null;
  addition_specific_iii: boolean | null;
  addition_specific_iv: boolean | null;
  addition_specific_v: boolean | null;
  addition_dementia_i: boolean | null;
  addition_dementia_ii: boolean | null;
  addition_oral: boolean | null;
  addition_emergency: boolean | null;
}

/** 加算項目ラベルマッピング */
export const KASAN_LABELS: Record<string, string> = {
  addition_treatment_i: "処遇改善加算I",
  addition_treatment_ii: "処遇改善加算II",
  addition_treatment_iii: "処遇改善加算III",
  addition_treatment_iv: "処遇改善加算IV",
  addition_specific_i: "特定事業所加算I",
  addition_specific_ii: "特定事業所加算II",
  addition_specific_iii: "特定事業所加算III",
  addition_specific_iv: "特定事業所加算IV",
  addition_specific_v: "特定事業所加算V",
  addition_dementia_i: "認知症ケア加算I",
  addition_dementia_ii: "認知症ケア加算II",
  addition_oral: "口腔連携加算",
  addition_emergency: "緊急時加算",
};

/** 要介護度別利用者数 */
export interface CareLevelUsers {
  care_level_1: number | null;
  care_level_2: number | null;
  care_level_3: number | null;
  care_level_4: number | null;
  care_level_5: number | null;
  total_users: number | null;
}

/** 品質スコア情報 */
export interface QualityScoreInfo {
  quality_score: number | null;
  quality_rank: string | null;
}

/** 施設行拡張フィールド（新規データ対応） */
export interface FacilityRowExtended extends FacilityRow {
  // 加算関連
  addition_count: number | null;
  addition_treatment_i: boolean | null;
  addition_treatment_ii: boolean | null;
  addition_treatment_iii: boolean | null;
  addition_treatment_iv: boolean | null;
  addition_specific_i: boolean | null;
  addition_specific_ii: boolean | null;
  addition_specific_iii: boolean | null;
  addition_specific_iv: boolean | null;
  addition_specific_v: boolean | null;
  addition_dementia_i: boolean | null;
  addition_dementia_ii: boolean | null;
  addition_oral: boolean | null;
  addition_emergency: boolean | null;
  // 品質スコア
  quality_score: number | null;
  quality_rank: string | null;
  // 要介護度別利用者数
  care_level_1: number | null;
  care_level_2: number | null;
  care_level_3: number | null;
  care_level_4: number | null;
  care_level_5: number | null;
  total_users: number | null;
  // 人材・経営
  experienced_10yr_ratio: number | null;
  occupancy_rate: number | null;
  // 運営情報
  accounting_type: string | null;
  has_bcp: boolean | null;
  has_ict: boolean | null;
  has_care_robot: boolean | null;
  has_third_party_eval: boolean | null;
  has_liability_insurance: boolean | null;
  // 財務諸表
  financial_statement_url_pl: string | null;
  financial_statement_url_cf: string | null;
  financial_statement_url_bs: string | null;
  // 行政処分・指導
  admin_penalty_date: string | null;
  admin_penalty_content: string | null;
  admin_guidance_date: string | null;
  admin_guidance_content: string | null;
}

/** 経験者割合分布ビン */
export interface ExperienceBin {
  range: string;
  count: number;
}

/** 経験者割合 vs 離職率散布図データ */
export interface ExperienceTurnoverPoint {
  prefecture: string;
  avg_experience_ratio: number;
  avg_turnover_rate: number;
  facility_count: number;
}

/** 品質スコア分布（ランク別） */
export interface QualityRankDistribution {
  rank: string;
  count: number;
  color: string;
}

/** 4カテゴリ品質レーダーデータ */
export interface QualityCategoryRadar {
  category: string;
  score: number;
  fullMark: number;
}

/** 品質KPI拡張版 */
export interface QualityKpiExtended extends QualityKpi {
  avg_quality_score: number | null;
  bcp_rate: number | null;
  ict_rate: number | null;
  third_party_rate: number | null;
  insurance_rate: number | null;
}

/** 都道府県別品質スコア */
export interface PrefectureQualityScoreExtended extends PrefectureQualityScore {
  avg_quality_score: number | null;
}

/** ダッシュボードKPI拡張版 */
export interface DashboardKpiExtended extends DashboardKpi {
  avg_quality_score: number | null;
  avg_kasan_count: number | null;
}

/** 人材分析KPI拡張版 */
export interface WorkforceKpiExtended extends WorkforceKpi {
  avg_experience_10yr_ratio: number | null;
}

/** M&A候補拡張版 */
export interface MaCandidateExtended extends MaCandidate {
  quality_score: number | null;
  addition_count: number | null;
  occupancy_rate: number | null;
}

/** 加算取得シミュレーション結果 */
export interface KasanSimulationResult {
  kasan_key: string;
  kasan_name: string;
  estimated_monthly_revenue: number;
  estimated_annual_revenue: number;
  difficulty: "easy" | "medium" | "hard";
}

/** ベンチマークデータ（APIレスポンスに対応） */
export interface BenchmarkData {
  facility: Record<string, unknown>;
  // 8軸レーダー
  radar: BenchmarkRadarItem[];
  // パーセンタイル（全国/都道府県/サービス別のオブジェクト）
  percentiles: {
    national: Record<string, number>;
    prefecture: Record<string, number>;
    service: Record<string, number>;
  };
  // 改善提案
  improvement_suggestions: BenchmarkImprovementSuggestion[];
}

/** ベンチマークレーダー項目（APIレスポンスに対応） */
export interface BenchmarkRadarItem {
  axis: string;
  value: number;
  national_avg: number;
  pref_avg: number;
}

/** ベンチマーク改善提案（APIレスポンスに対応） */
export interface BenchmarkImprovementSuggestion {
  axis: string;
  current: number;
  target: number;
  suggestion: string;
}

/** ベンチマークパーセンタイル（旧型 - 互換用） */
export interface BenchmarkPercentile {
  metric: string;
  value: number | null;
  national_percentile: number;
  regional_percentile: number;
  service_percentile: number;
}

/** ベンチマーク改善提案（旧型 - 互換用） */
export interface BenchmarkRecommendation {
  priority: number;
  metric: string;
  current_value: string;
  target_value: string;
  description: string;
}

/** 法人内加算取得ヒートマップデータ */
export interface CorpKasanHeatmapData {
  facilities: string[];
  kasan_items: string[];
  values: (boolean | null)[][];
}

// ===================================================
// 外部API型定義（/api/external/* エンドポイント）
// ===================================================

/** 市区町村別人口データ（/api/external/population） */
export interface ExternalPopulation {
  municipality: string;
  population: number;
  elderly_rate: number;
  [key: string]: unknown;
}

/** 介護需要トレンド（/api/external/care-demand） */
export interface ExternalCareDemand {
  prefecture: string;
  fiscal_year: string | null;
  day_service_offices: number | null;
  day_service_users: number | null;
  home_care_offices: number | null;
  home_care_users: number | null;
  nursing_home_count: number | null;
  health_facility_count: number | null;
  insurance_benefit_cost: number | null;
  pop_65_over: number | null;
  pop_65_over_rate: number | null;
}

/** 労働市場トレンド（/api/external/labor-trends） */
export interface ExternalLaborTrend {
  prefecture: string;
  fiscal_year: string | null;
  separation_rate: number | null;
  turnover_rate: number | null;
  job_changer_rate: number | null;
  unemployment_rate: number | null;
  employment_rate: number | null;
}

/** 有効求人倍率（/api/external/job-openings） */
export interface ExternalJobOpenings {
  prefecture: string;
  fiscal_year: string | null;
  ratio_total: number | null;
  ratio_excl_part: number | null;
}

/** 最低賃金推移（/api/external/wage-history） */
export interface ExternalWageHistory {
  prefecture: string;
  fiscal_year: number | null;
  hourly_min_wage: number | null;
}

/** 開業率/廃業率（/api/external/business-dynamics） */
export interface ExternalBusinessDynamics {
  prefecture: string;
  fiscal_year: string | null;
  total_establishments: number | null;
  new_establishments: number | null;
  closed_establishments: number | null;
  net_change: number | null;
  opening_rate: number | null;
  closure_rate: number | null;
}

/** 給与ベンチマーク（/api/external/salary-benchmark） */
export interface ExternalSalaryBenchmark {
  prefecture: string;
  industry_major_code: string;
  emp_group: string;
  count: number | null;
  mean_min: number | null;
  mean_max: number | null;
  median_min: number | null;
  min_val: number | null;
  max_val: number | null;
}

/** 欠員率データ（/api/external/vacancy-stats） */
export interface ExternalVacancyStats {
  prefecture: string;
  industry_major_code: string;
  emp_group: string;
  total_count: number | null;
  vacancy_count: number | null;
  growth_count: number | null;
  new_facility_count: number | null;
  vacancy_rate: number | null;
  growth_rate: number | null;
}

// ===================================================
// チャートコンポーネント共通型
// ===================================================

/** チャートデータポイントの汎用型 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type ChartDataPoint = Record<string, any>;

/** Rechartsのカスタムtooltipのpayload型 */
export interface TooltipPayloadEntry {
  name?: string;
  value?: number | string;
  color?: string;
  dataKey?: string;
  payload?: ChartDataPoint;
}

/** 散布図のカスタムtooltipのprops型 */
export interface ScatterTooltipProps {
  active?: boolean;
  payload?: TooltipPayloadEntry[];
}
