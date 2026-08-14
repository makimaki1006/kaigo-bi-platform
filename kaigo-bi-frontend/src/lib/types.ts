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
  /** kpi_cacheの集計世代によっては欠損する */
  total_staff?: number;
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
  /** 緯度経度（国交省 位置参照情報の町丁目レベル。未突合の施設は null） */
  latitude?: number | null;
  longitude?: number | null;
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
  /** 課金プラン */
  plan: "free" | "standard" | "pro" | "ma";
  is_active: boolean;
  expires_at: string | null;
  created_at: string;
}

/** サインアップリクエスト */
export interface SignupRequest {
  email: string;
  password: string;
  name: string;
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
  plan?: "free" | "standard" | "pro" | "ma";
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
  /** 集計に使えた施設数（賃金_月額1が妥当レンジに入るもの） */
  sample_count?: number | null;
  /** 同じフィルタ条件での母集団施設数 */
  population_count?: number | null;
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
  /** kpi_cache の旧キー。キャッシュ再生成までのフォールバック用 */
  max_facilities?: number | null;
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
  recent_3yr_count?: number;
  avg_years_in_business?: number;
  net_growth_rate?: number;
  total_with_start_date?: number;
  /** APIが返す代替キー */
  avg_years?: number;
  new_facilities_5yr?: number;
  oldest_years?: number;
  total_facilities?: number;
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
  /** 絶対基準スコア（固定しきい値の加点のみ。母集団で変動しない） */
  ma_score: number;
  sales_score: number;
  size_tier: "小" | "中" | "大";
  prefecture_count: number;
  has_financials: boolean;
  is_insolvent: boolean;
  has_operating_loss: boolean;
  has_violation: boolean;
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
  /** 規模バッジ（固定しきい値。母集団で変動しない） */
  size_tier: "小" | "中" | "大";
  /** 展開都道府県数 */
  prefecture_count: number;
  /** M&A観点スコア（0-100、固定しきい値の加点合計） */
  ma_score: number;
  /** 営業観点スコア（0-100、固定しきい値の加点合計） */
  sales_score: number;
  /** 財務・リスクフラグ（決算PDF抽出データ由来） */
  has_financials?: boolean;
  is_insolvent?: boolean;
  has_operating_loss?: boolean;
  has_violation?: boolean;
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
  /** 法人合計の職種別人員（施設360°で追加） */
  total_kaigo_staff?: number;
  total_nurse_staff?: number;
  total_care_workers?: number;
  total_care_managers?: number;
}

/** DDコンプライアンス（実API） */
export interface DdComplianceDd {
  has_violations: boolean;
  /** 行政処分・指導の実レコード（「なし」系表記は除外済み） */
  violations?: DdViolation[];
  bcp_rate: number | null;
  insurance_rate: number | null;
}

/** DD財務諸表リンク（施設単位、実API） */
export interface DdFinancialLink {
  facility_name: string;
  jigyosho_number: string | null;
  /** 事業活動計算書（損益計算書相当） */
  pl_url: string | null;
  /** 資金収支計算書 */
  cf_url: string | null;
  /** 貸借対照表 */
  bs_url: string | null;
}

/** 決算PDF由来の抽出済み財務データ（financialsテーブル、実API） */
export interface FinancialRecord {
  jigyosho_number: string;
  doc_type: "PL" | "BS" | "CF";
  fiscal_period: string | null;
  revenue: number | null;
  personnel_cost: number | null;
  operating_income: number | null;
  ordinary_income: number | null;
  net_income: number | null;
  prior_revenue: number | null;
  prior_operating_income: number | null;
  total_assets: number | null;
  net_assets: number | null;
  total_liabilities: number | null;
  confidence: string | null;
  notes: string | null;
  /** 以下は来歴（決算PDFが自由書式のため、値だけでは誤読される） */
  fiscal_year?: number | null;
  /** 円 / 千円 / 百万円 */
  unit?: string | null;
  /** pdf=PDFに明記 / inferred=桁から推定 / assumed=円と仮定 */
  unit_source?: string | null;
  /** 法人単位 / 拠点区分 / サービス区分 / null=記載なし */
  scope?: string | null;
  form?: string | null;
  acct_standard?: string | null;
  /** rule_v1=座標ベースの規則抽出 / ai_vision=AI視覚抽出 */
  extraction_method?: string | null;
  /** false ならスキャン画像で機械抽出不可 */
  text_layer?: boolean | null;
  /** 資産 = 負債 + 純資産 が成立したか */
  identity_ok?: boolean | null;
  uploaded_at?: string | null;
}

// ===================================================
// 決算書の開示状況（全施設で機械的に確定する層）
// ===================================================

/** 施設単位の決算書開示状況 */
export interface FinancialDisclosure {
  has_pl: boolean;
  has_cf: boolean;
  has_bs: boolean;
  doc_count: number;
  is_full_set: boolean;
  file_format: string | null;
  uploaded_at_pl: string | null;
  uploaded_at_cf: string | null;
  uploaded_at_bs: string | null;
  latest_upload: string | null;
  days_since_upload: number | null;
}

/** 全国の開示状況KPI */
export interface FinancialDisclosureKpi {
  facilities: number;
  jigyosho: number;
  corporations: number;
  with_any: number;
  with_any_rate: number;
  with_pl: number;
  with_pl_rate: number;
  with_bs: number;
  with_bs_rate: number;
  with_cf: number;
  with_cf_rate: number;
  full_set: number;
  full_set_rate: number;
  csv_count: number;
  timestamp_available: number;
  oldest_upload: string | null;
  latest_upload: string | null;
  fresh_within_1y: number;
  fresh_within_1y_rate: number;
}

export interface FinancialDisclosureByPrefecture {
  prefecture: string;
  facilities: number;
  with_pl: number;
  disclosure_rate: number;
  full_set: number;
  full_set_rate: number;
  fresh_1y: number;
  fresh_rate: number;
}

export interface FinancialDisclosureByCorpType {
  corp_type: string;
  facilities: number;
  with_pl: number;
  with_bs: number;
  with_cf: number;
  disclosure_rate: number;
  full_set: number;
  full_set_rate: number;
  csv_count: number;
}

export interface FinancialDisclosureByService {
  service: string;
  facilities: number;
  with_pl: number;
  disclosure_rate: number;
  full_set_rate: number;
}

export interface FinancialDisclosureFreshness {
  month: string;
  count: number;
}

/** 未開示・更新停滞のセグメント（営業・DDの対象抽出用） */
export interface FinancialDisclosureGap {
  no_disclosure: number;
  no_disclosure_corporations: number;
  no_disclosure_rate: number;
  stale_over_2y: number;
  stale_over_1y: number;
  note: string;
  by_prefecture: {
    prefecture: string;
    facilities: number;
    no_disclosure: number;
    no_disclosure_rate: number;
    stale_over_2y: number;
  }[];
}

export interface FinancialDisclosureByAcctType {
  acct_type: string;
  facilities: number;
  with_pl: number;
  disclosure_rate: number;
}

/** 金額抽出がどこまでできるかの実測値（financialsテーブルの実データから算出） */
export interface FinancialExtractionStatus {
  analyzed_files: number;
  analyzed_facilities: number;
  text_layer_rate: number;
  pl_files: number;
  pl_revenue_rate: number;
  pl_revenue_and_personnel_rate: number;
  bs_files: number;
  bs_assets_and_equity_rate: number;
  /** 資産 = 負債 + 純資産 が成立した割合（抽出値の信頼性） */
  bs_identity_match_rate: number;
  /** 以下3つはテキスト層のあるファイルを分母とする */
  period_detect_rate: number;
  scope_stated_rate: number;
  unit_stated_rate: number;
  note: string;
  surveyed_at: string | null;
}

/** 抽出できた金額の集計。全国平均ではないので n が必須 */
export interface FinancialMetricStat {
  n: number;
  median: number | null;
  quartiles: { p25: number; p50: number; p75: number } | null;
  published: boolean;
}

export interface FinancialMetricsSummary {
  source: string;
  facilities_with_any_amount: number;
  personnel_ratio: FinancialMetricStat;
  equity_ratio: FinancialMetricStat;
  min_n: number;
}

// ---- 決算データ × 公表データ のクロス集計 ----

/** 職員1人あたり収益（万円/人・年） */
export interface FinancialRevenuePerStaff {
  unit: string;
  n: number;
  median: number | null;
  quartiles: { p25: number; p50: number; p75: number } | null;
  scope_note: string;
  by_service: { service: string; n: number; median: number }[];
  by_corp_type: { corp_type: string; n: number; median: number }[];
  min_n: number;
}

/** 自己資本比率 × 法人の事業所数 */
export interface FinancialEquityByCorpSize {
  band: string;
  n: number;
  median: number;
  quartiles: { p25: number; p50: number; p75: number } | null;
}

/** 決算書の開示有無 × 品質スコア・離職率 */
export interface FinancialDisclosureVsQuality {
  rows: {
    disclosed: boolean;
    facilities: number;
    quality_score_avg: number | null;
    quality_n: number;
    turnover_avg: number | null;
    turnover_n: number;
    staff_avg: number | null;
  }[];
  note: string;
}

/** 都道府県別の人件費率 */
export interface FinancialPersonnelRatioByPrefecture {
  prefecture: string;
  n: number;
  median: number;
}

export interface FinancialMetricsByCorpType {
  corp_type: string;
  personnel_n: number;
  equity_n: number;
  personnel_median?: number;
  equity_median?: number;
}

/** DD財務情報（実API） */
export interface DdFinancialDd {
  accounting_type: string | null;
  financial_links: DdFinancialLink[];
  /** 決算PDFからAI抽出した財務データ */
  extracted_financials: FinancialRecord[];
  /** 財務PDFリンクを持つ施設数 */
  pdf_facility_count?: number;
  /** AI抽出済みの施設数 */
  extracted_facility_count?: number;
}

/** クロス指標カバレッジ（未知と安全を区別するため） */
export interface CrossMetricsCoverage {
  available_factors: number;
  required_factors: number;
  /** 算定に足るファクタが揃っているか */
  computable: boolean;
  /** 採用した決算期（PL/BSがスコープ一致した場合） */
  fiscal_period: string | null;
  /** PLとBSが同一施設・同一決算期で結合できたか */
  pl_bs_scope_matched: boolean;
}

/** クロス指標（財務PDF × 公表データ、実API） */
export interface CrossMetrics {
  has_financials: boolean;
  /** 従業者1人あたり売上（円）。法人レベルでは粒度不一致のためnull */
  labor_productivity: number | null;
  /** 利用者1人あたり収益（円） */
  revenue_per_user: number | null;
  /** 実人件費率（0-1） */
  personnel_cost_ratio: number | null;
  /** 営業利益率（0-1） */
  operating_margin: number | null;
  /** 自己資本比率（0-1、PL/BSスコープ一致時のみ） */
  equity_ratio: number | null;
  /** 要確認シグナル（検証済みスコアではない） */
  signals: string[];
  /** 該当シグナル数。算定不能時はnull（未知≠安全） */
  signal_count: number | null;
  coverage: CrossMetricsCoverage;
}

/** 施設・法人の財務データ状態 */
/** extracted=金額が取れた / parsed_no_amount=解析したが金額が読めない
 *  pdf_available=PDFはあるが未解析 / not_published=決算書の掲載なし */
export type FinancialStatus =
  | "extracted"
  | "parsed_no_amount"
  | "pdf_available"
  | "not_published";

/** 行政処分・指導レコード（法人DD、実API） */
export interface DdViolation {
  facility_name: string;
  jigyosho_number: string | null;
  sanction_date: string | null;
  sanction_detail: string | null;
  guidance_date: string | null;
  guidance_detail: string | null;
}

/** 職種別人員（施設詳細、実API） */
export interface StaffingBreakdown {
  kaigo: { fulltime: number | null; parttime: number | null; total: number | null };
  nurse: { fulltime: number | null; parttime: number | null; total: number | null };
  counselor: { fulltime: number | null; parttime: number | null; total: number | null };
  trainer: { fulltime: number | null; parttime: number | null; total: number | null };
  dietitian: { fulltime: number | null; parttime: number | null; total: number | null };
  clerk: { fulltime: number | null; parttime: number | null; total: number | null };
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
  /** 法人レベルのクロス指標（施設360°で追加） */
  cross_metrics?: CrossMetrics;
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
  /** 決算書の開示状況（URLから機械的に確定する層） */
  financial_disclosure?: FinancialDisclosure | null;
  // 行政処分・指導
  admin_penalty_date: string | null;
  admin_penalty_content: string | null;
  admin_guidance_date: string | null;
  admin_guidance_content: string | null;
  // ==== facility_detail API(施設360°)で追加配信されるフィールド ====
  /** 行政処分（「なし」系表記はAPI側で除外済み） */
  sanction_date?: string | null;
  sanction_detail?: string | null;
  guidance_date?: string | null;
  guidance_detail?: string | null;
  /** 職種別人員体制 */
  staffing?: StaffingBreakdown;
  /** 資格保有者数 */
  qualifications?: {
    care_worker: number | null;
    jitsumusha: number | null;
    shoninsha: number | null;
    care_manager: number | null;
  };
  night_shift_count?: number | null;
  night_watch_count?: number | null;
  /** 利用者数の都道府県平均（自施設比較用） */
  users_pref_avg?: number | null;
  /** データ取得日 */
  scraped_at?: string | null;
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
  total_corps: number | null;
  /** 実APIのキー名（avg_years_in_businessではなくavg_yearsで返る） */
  avg_years?: number | null;
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
// kihon delta: 職種別人数・資格・夜勤・認知症研修・全加算
// ===================================================

/** 職種別従業者構成（/api/workforce/staff-breakdown） */
export interface StaffBreakdown {
  job_type: string;
  avg_count: number | null;
  total_count: number;
  facility_count: number;
  fulltime_ratio: number | null;
}

/** 資格保有者数（/api/workforce/qualifications） */
export interface QualificationStats {
  qualification: string;
  total: number;
  avg_per_facility: number | null;
  facility_count: number;
}

/** 認知症研修修了者（/api/workforce/dementia-training） */
export interface DementiaTraining {
  training: string;
  total: number;
  avg_per_facility: number | null;
  facility_count: number;
}

/** 夜勤体制（/api/workforce/night-shift） */
export interface NightShiftData {
  [key: string]: {
    avg: number;
    facility_count: number;
  };
}

/** 全加算項目（/api/revenue/kasan-all-items） */
export interface KasanAllItem {
  name: string;
  rate: number;
  count: number;
  total: number;
  service_codes: string[];
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
