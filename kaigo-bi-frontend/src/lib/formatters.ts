// ===================================================
// 数値フォーマッター
// ===================================================

/**
 * 千区切り数値フォーマット（例: 1,234）
 * null/undefinedの場合は "-" を返す
 */
export function formatNumber(n: number | null | undefined): string {
  if (n == null || isNaN(n)) return "-";
  return n.toLocaleString("ja-JP");
}

/**
 * パーセント表示（例: 12.3%）
 * 入力値は小数（0.123）を想定
 */
export function formatPercent(n: number | null | undefined): string {
  if (n == null || isNaN(n)) return "-";
  return `${(n * 100).toFixed(1)}%`;
}

/**
 * パーセント表示（入力値がすでにパーセント値の場合）
 * 例: 12.3 → "12.3%"
 */
export function formatPercentRaw(n: number | null | undefined): string {
  if (n == null || isNaN(n)) return "-";
  return `${n.toFixed(1)}%`;
}

/**
 * 円表示（例: ¥200,000）
 */
export function formatYen(n: number | null | undefined): string {
  if (n == null || isNaN(n)) return "-";
  return `¥${n.toLocaleString("ja-JP")}`;
}

/**
 * 万円表示（例: 241559 → "24.2万円"）
 * 円の値を万円に変換して小数1桁で表示
 */
export function formatManYen(n: number | null | undefined): string {
  if (n == null || isNaN(n)) return "--";
  return `${(n / 10000).toFixed(1)}万円`;
}

/**
 * 小数フォーマット（例: 12.34）
 */
export function formatDecimal(
  n: number | null | undefined,
  digits: number = 1
): string {
  if (n == null || isNaN(n)) return "-";
  return n.toFixed(digits);
}

/**
 * KPIカード用フォーマッター
 * format種別に応じて適切なフォーマットを適用
 */
export function formatKpiValue(
  value: number | null | undefined,
  format: "number" | "percent" | "decimal"
): string {
  switch (format) {
    case "number":
      return formatNumber(value);
    case "percent":
      return formatPercent(value);
    case "decimal":
      return formatDecimal(value);
    default:
      return formatNumber(value);
  }
}

/**
 * サービス種別名のフォーマット
 * アンダースコアを括弧表記に変換
 * 例: "特定施設_軽費老人ホーム" → "特定施設（軽費老人ホーム）"
 * 例: "地域密着型特定施設_有料" → "地域密着型特定施設（有料）"
 */
export function formatServiceName(name: string | null | undefined): string {
  if (!name) return "-";
  // アンダースコアを使って分割し、2つ目以降を括弧で囲む
  const parts = name.split("_");
  if (parts.length === 1) return name;
  return `${parts[0]}（${parts.slice(1).join("・")}）`;
}

/**
 * 法人種別の表示名変換
 * 内部的な種別名をより分かりやすい表示名に変換
 */
const CORP_TYPE_DISPLAY_MAP: Record<string, string> = {
  "営利法人": "株式会社・有限会社等",
  "非営利法人": "NPO法人・一般社団法人等",
  "社会福祉法人": "社会福祉法人",
  "医療法人": "医療法人",
  "株式会社": "株式会社",
  "有限会社": "有限会社",
  "合同会社": "合同会社",
  "NPO法人": "NPO法人",
  "一般社団法人": "一般社団法人",
  "一般財団法人": "一般財団法人",
  "公益社団法人": "公益社団法人",
  "公益財団法人": "公益財団法人",
  "地方公共団体": "地方公共団体",
  "社会福祉協議会": "社会福祉協議会",
  "その他": "その他法人",
};

export function formatCorpType(corpType: string | null | undefined): string {
  if (!corpType) return "-";
  return CORP_TYPE_DISPLAY_MAP[corpType] ?? corpType;
}

/**
 * カラム名（snake_case）→ 日本語ラベルの変換
 * DataTableのヘッダーやその他の表示で使用
 */
const COLUMN_LABELS: Record<string, string> = {
  jigyosho_name: "事業所名",
  jigyosho_number: "事業所番号",
  corp_name: "法人名",
  corp_type: "法人種別",
  corp_number: "法人番号",
  phone: "電話番号",
  fax: "FAX",
  address: "住所",
  prefecture: "都道府県",
  service_name: "サービス種別",
  service_code: "サービスコード",
  staff_total: "従業者数",
  staff_fulltime: "常勤",
  staff_parttime: "非常勤",
  capacity: "定員",
  turnover_rate: "離職率",
  fulltime_ratio: "常勤比率",
  occupancy_rate: "稼働率",
  kasan_count: "加算取得数",
  addition_count: "加算取得数",
  quality_score: "品質スコア",
  quality_rank: "品質ランク",
  facility_count: "施設数",
  total_staff: "総従業者数",
  avg_turnover_rate: "平均離職率",
  service_names: "サービス種別",
  prefectures: "展開地域",
  attractiveness_score: "魅力度スコア",
  start_date: "事業開始日",
  manager_name: "管理者名",
  manager_title: "管理者役職",
  representative_name: "代表者名",
  representative_title: "代表者役職",
  homepage: "Webサイト",
  hired_last_year: "前年採用数",
  left_last_year: "前年退職数",
  avg_staff: "平均従業者数",
  avg_capacity: "平均定員",
  avg_quality_score: "平均品質スコア",
  avg_kasan_count: "平均加算取得数",
  avg_hire_rate: "平均採用率",
  avg_fulltime_ratio: "平均常勤比率",
  avg_experience_10yr_ratio: "経験10年以上割合",
};

export function formatColumnLabel(key: string): string {
  return COLUMN_LABELS[key] ?? key;
}
