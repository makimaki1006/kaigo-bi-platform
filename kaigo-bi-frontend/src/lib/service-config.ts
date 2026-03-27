// ===================================================
// サービス種別設定モジュール
// サービス種別に応じたKPI・チャート・機能の表示制御
// ===================================================

import { useMemo } from "react";

// ---------------------------------------------------
// 型定義
// ---------------------------------------------------

/** サービスカテゴリ */
export type ServiceCategory =
  | "visit"
  | "day_care"
  | "residential"
  | "community"
  | "care_plan"
  | "equipment"
  | "other";

/** KPI利用可否の状態（partial = 一部の条件でのみ利用可能） */
type KpiAvailability = boolean | "partial";

/** KPI利用可否マトリクス1行分 */
interface KpiAvailabilityRow {
  visit: KpiAvailability;
  day_care: KpiAvailability;
  residential: KpiAvailability;
  community: KpiAvailability;
  care_plan: KpiAvailability;
  equipment: KpiAvailability;
  other: KpiAvailability;
}

/** 加算適用可否（サービスコード限定条件付き） */
interface KasanApplicability {
  visit: boolean | string[];
  day_care: boolean;
  residential: boolean;
  community: boolean;
}

// ---------------------------------------------------
// サービスコード→カテゴリ マッピング
// ---------------------------------------------------

const SERVICE_CODE_TO_CATEGORY: Record<string, ServiceCategory> = {
  // 訪問系
  "110": "visit",
  "120": "visit",
  "130": "visit",
  "140": "visit",
  "710": "visit",
  "760": "visit",
  // 通所系
  "150": "day_care",
  "155": "day_care",
  "160": "day_care",
  "720": "day_care",
  "780": "day_care",
  // 入所系
  "210": "residential",
  "220": "residential",
  "230": "residential",
  "320": "residential",
  "510": "residential",
  "520": "residential",
  "530": "residential",
  "540": "residential",
  "550": "residential",
  "551": "residential",
  // 地域密着系
  "331": "community",
  "332": "community",
  "334": "community",
  "335": "community",
  "336": "community",
  "337": "community",
  "361": "community",
  "362": "community",
  "364": "community",
  "730": "community",
  "770": "community",
  // 居宅介護支援
  "430": "care_plan",
  // 福祉用具
  "170": "equipment",
  "410": "equipment",
};

// ---------------------------------------------------
// カテゴリ表示名（日本語）
// ---------------------------------------------------

const CATEGORY_LABELS: Record<ServiceCategory, string> = {
  visit: "訪問系サービス",
  day_care: "通所系サービス",
  residential: "入所系サービス",
  community: "地域密着型サービス",
  care_plan: "居宅介護支援",
  equipment: "福祉用具",
  other: "その他サービス",
};

// ---------------------------------------------------
// KPI利用可否マトリクス
// ---------------------------------------------------

const KPI_AVAILABILITY: Record<string, KpiAvailabilityRow> = {
  facility_count: {
    visit: true,
    day_care: true,
    residential: true,
    community: true,
    care_plan: true,
    equipment: true,
    other: true,
  },
  corp_count: {
    visit: true,
    day_care: true,
    residential: true,
    community: true,
    care_plan: true,
    equipment: true,
    other: true,
  },
  avg_staff: {
    visit: true,
    day_care: true,
    residential: true,
    community: true,
    care_plan: true,
    equipment: false,
    other: true,
  },
  avg_turnover: {
    visit: true,
    day_care: true,
    residential: true,
    community: true,
    care_plan: false,
    equipment: false,
    other: true,
  },
  avg_fulltime_ratio: {
    visit: true,
    day_care: true,
    residential: true,
    community: true,
    care_plan: true,
    equipment: false,
    other: true,
  },
  capacity: {
    visit: false,
    day_care: true,
    residential: true,
    community: true,
    care_plan: false,
    equipment: false,
    other: false,
  },
  occupancy: {
    visit: false,
    day_care: true,
    residential: true,
    community: true,
    care_plan: false,
    equipment: false,
    other: false,
  },
  care_level_dist: {
    visit: true,
    day_care: true,
    residential: true,
    community: true,
    care_plan: false,
    equipment: false,
    other: true,
  },
  kasan_rates: {
    visit: true,
    day_care: true,
    residential: true,
    community: true,
    care_plan: true,
    equipment: false,
    other: true,
  },
  quality_score: {
    visit: true,
    day_care: true,
    residential: true,
    community: true,
    care_plan: false,
    equipment: false,
    other: true,
  },
  salary: {
    visit: true,
    day_care: true,
    residential: true,
    community: true,
    care_plan: true,
    equipment: false,
    other: true,
  },
  night_shift: {
    visit: false,
    day_care: false,
    residential: true,
    community: "partial",
    care_plan: false,
    equipment: false,
    other: false,
  },
  experience_ratio: {
    visit: true,
    day_care: true,
    residential: true,
    community: true,
    care_plan: true,
    equipment: false,
    other: true,
  },
  third_party_eval: {
    visit: true,
    day_care: true,
    residential: true,
    community: true,
    care_plan: false,
    equipment: false,
    other: true,
  },
  financial_dl: {
    visit: true,
    day_care: true,
    residential: true,
    community: true,
    care_plan: true,
    equipment: true,
    other: true,
  },
};

// ---------------------------------------------------
// KPI非対応理由（日本語）
// ---------------------------------------------------

const KPI_UNAVAILABLE_REASONS: Record<string, Partial<Record<ServiceCategory, string>>> = {
  avg_staff: {
    equipment: "福祉用具サービスでは従業者数データが公表されていません",
  },
  avg_turnover: {
    care_plan: "居宅介護支援では離職率データが集計対象外です",
    equipment: "福祉用具サービスでは離職率データが公表されていません",
  },
  avg_fulltime_ratio: {
    equipment: "福祉用具サービスでは常勤率データが公表されていません",
  },
  capacity: {
    visit: "訪問系サービスでは定員の概念がありません",
    care_plan: "居宅介護支援では定員の概念がありません",
    equipment: "福祉用具サービスでは定員の概念がありません",
    other: "このサービス種別では定員データがありません",
  },
  occupancy: {
    visit: "訪問系サービスでは稼働率の概念がありません",
    care_plan: "居宅介護支援では稼働率の概念がありません",
    equipment: "福祉用具サービスでは稼働率の概念がありません",
    other: "このサービス種別では稼働率データがありません",
  },
  care_level_dist: {
    care_plan: "居宅介護支援では要介護度別利用者データが集計対象外です",
    equipment: "福祉用具サービスでは要介護度別利用者データがありません",
  },
  kasan_rates: {
    equipment: "福祉用具サービスでは加算制度の対象外です",
  },
  quality_score: {
    care_plan: "居宅介護支援では経営品質スコアが算出対象外です",
    equipment: "福祉用具サービスでは経営品質スコアが算出対象外です",
  },
  salary: {
    equipment: "福祉用具サービスでは賃金データが公表されていません",
  },
  night_shift: {
    visit: "訪問系サービスでは夜勤データがありません（夜間対応型を除く）",
    day_care: "通所系サービスでは夜勤がありません",
    community: "地域密着型サービスでは一部の施設のみ夜勤データがあります",
    care_plan: "居宅介護支援では夜勤がありません",
    equipment: "福祉用具サービスでは夜勤がありません",
    other: "このサービス種別では夜勤データがありません",
  },
  experience_ratio: {
    equipment: "福祉用具サービスでは経験者割合データが公表されていません",
  },
  third_party_eval: {
    care_plan: "居宅介護支援では第三者評価の実施率が集計対象外です",
    equipment: "福祉用具サービスでは第三者評価の実施率が集計対象外です",
  },
};

// ---------------------------------------------------
// 加算適用可否マトリクス
// ---------------------------------------------------

const KASAN_APPLICABILITY: Record<string, KasanApplicability> = {
  // 処遇改善加算I-IV: 全カテゴリ共通
  addition_treatment_i: { visit: true, day_care: true, residential: true, community: true },
  addition_treatment_ii: { visit: true, day_care: true, residential: true, community: true },
  addition_treatment_iii: { visit: true, day_care: true, residential: true, community: true },
  addition_treatment_iv: { visit: true, day_care: true, residential: true, community: true },
  // 特定事業所加算I-V: 訪問介護（110）のみ
  addition_specific_i: { visit: ["110"], day_care: false, residential: false, community: false },
  addition_specific_ii: { visit: ["110"], day_care: false, residential: false, community: false },
  addition_specific_iii: { visit: ["110"], day_care: false, residential: false, community: false },
  addition_specific_iv: { visit: ["110"], day_care: false, residential: false, community: false },
  addition_specific_v: { visit: ["110"], day_care: false, residential: false, community: false },
  // 認知症ケア加算I-II: 通所・入所・地域密着
  addition_dementia_i: { visit: false, day_care: true, residential: true, community: true },
  addition_dementia_ii: { visit: false, day_care: true, residential: true, community: true },
  // 口腔連携加算: 通所・入所
  addition_oral: { visit: false, day_care: true, residential: true, community: false },
  // 緊急時加算: 訪問看護（130）のみ
  addition_emergency: { visit: ["130"], day_care: false, residential: false, community: false },
};

// 加算項目の日本語ラベル（types.tsのKASAN_LABELSと重複を避け、ここでは加算キーのリスト用途）
const KASAN_KEYS = Object.keys(KASAN_APPLICABILITY);

// ---------------------------------------------------
// 公開関数
// ---------------------------------------------------

/**
 * サービスコードからカテゴリを取得する
 * マッピングに存在しないコードは "other" を返す
 */
export function getServiceCategory(code: string): ServiceCategory {
  return SERVICE_CODE_TO_CATEGORY[code] ?? "other";
}

/**
 * カテゴリの日本語表示名を取得する
 */
export function getCategoryLabel(category: ServiceCategory): string {
  return CATEGORY_LABELS[category];
}

/**
 * 指定されたKPIが選択中のサービスコード群で利用可能かどうか判定する
 *
 * - 空配列（未選択）の場合: 全KPIを表示（グローバルビュー）
 * - 複数コード選択時: いずれかのサービスで利用可能なら true（UNION方式）
 */
export function isKpiAvailable(kpiKey: string, selectedServiceCodes: string[]): boolean {
  // マトリクスに存在しないKPIキーは常に表示
  const row = KPI_AVAILABILITY[kpiKey];
  if (!row) return true;

  // 未選択 = グローバルビュー（全KPI表示）
  if (selectedServiceCodes.length === 0) return true;

  // UNION方式: いずれかのサービスで利用可能なら true
  return selectedServiceCodes.some((code) => {
    const category = getServiceCategory(code);
    const availability = row[category];
    // true または "partial" なら利用可能と判定
    return availability === true || availability === "partial";
  });
}

/**
 * KPIが利用不可の理由を日本語で返す
 * 利用可能な場合は null を返す
 */
export function getKpiUnavailableReason(
  kpiKey: string,
  category: ServiceCategory
): string | null {
  const row = KPI_AVAILABILITY[kpiKey];
  if (!row) return null;

  const availability = row[category];
  if (availability === true) return null;

  // partial の場合は注意書きとして理由を返す
  const reasons = KPI_UNAVAILABLE_REASONS[kpiKey];
  if (reasons && reasons[category]) {
    return reasons[category]!;
  }

  // 汎用的な理由
  if (availability === false) {
    return `${CATEGORY_LABELS[category]}ではこの指標は対象外です`;
  }

  return null;
}

/**
 * 選択されたサービスコード群で適用可能な加算キーのリストを返す
 *
 * - 空配列（未選択）の場合: 全加算項目を返す
 * - 複数コード選択時: UNION方式で該当する加算を返す
 */
export function getApplicableKasan(selectedServiceCodes: string[]): string[] {
  // 未選択 = 全加算項目
  if (selectedServiceCodes.length === 0) return [...KASAN_KEYS];

  const applicableSet = new Set<string>();

  for (const code of selectedServiceCodes) {
    const category = getServiceCategory(code);

    // care_plan, equipment, other は加算対象外
    if (category === "care_plan" || category === "equipment" || category === "other") {
      continue;
    }

    for (const kasanKey of KASAN_KEYS) {
      const rule = KASAN_APPLICABILITY[kasanKey];
      const categoryRule = rule[category as keyof KasanApplicability];

      if (categoryRule === true) {
        // カテゴリ全体で適用可能
        applicableSet.add(kasanKey);
      } else if (Array.isArray(categoryRule)) {
        // 特定のサービスコードのみ適用可能
        if (categoryRule.includes(code)) {
          applicableSet.add(kasanKey);
        }
      }
    }
  }

  // 元の定義順序を維持して返す
  return KASAN_KEYS.filter((key) => applicableSet.has(key));
}

/**
 * 選択されたサービスコード群の統合カテゴリを返す
 * 単一カテゴリの場合はそのカテゴリ、複数カテゴリの場合は "mixed" を返す
 */
export function getResolvedCategory(
  selectedServiceCodes: string[]
): ServiceCategory | "mixed" {
  if (selectedServiceCodes.length === 0) return "mixed";

  const categories = new Set(
    selectedServiceCodes.map((code) => getServiceCategory(code))
  );

  if (categories.size === 1) {
    return Array.from(categories)[0];
  }

  return "mixed";
}

// ---------------------------------------------------
// React Hook
// ---------------------------------------------------

/**
 * サービス種別設定を簡単に利用するための React Hook
 *
 * 使用例:
 * ```tsx
 * const { isAvailable, reason, category, applicableKasan } = useServiceConfig(selectedCodes);
 *
 * {isAvailable("capacity") && <CapacityChart />}
 * {!isAvailable("capacity") && <UnavailableNotice message={reason("capacity")} />}
 * ```
 */
export function useServiceConfig(selectedServiceCodes: string[]) {
  return useMemo(() => {
    const category = getResolvedCategory(selectedServiceCodes);
    const applicableKasan = getApplicableKasan(selectedServiceCodes);

    const isAvailable = (kpiKey: string): boolean => {
      return isKpiAvailable(kpiKey, selectedServiceCodes);
    };

    const reason = (kpiKey: string): string | null => {
      // 利用可能な場合は理由なし
      if (isAvailable(kpiKey)) return null;

      // 混合カテゴリの場合、全カテゴリの理由を合成
      if (category === "mixed") {
        // 全サービスコードのカテゴリから理由を集約
        const categories = Array.from(
          new Set(selectedServiceCodes.map((c) => getServiceCategory(c)))
        );
        const reasons = categories
          .map((cat) => getKpiUnavailableReason(kpiKey, cat))
          .filter((r): r is string => r !== null);
        return reasons.length > 0 ? reasons[0] : "選択中のサービスではこの指標は対象外です";
      }

      return (
        getKpiUnavailableReason(kpiKey, category) ??
        "選択中のサービスではこの指標は対象外です"
      );
    };

    return {
      /** KPIの利用可否を判定 */
      isAvailable,
      /** KPIが利用不可の理由（利用可能時は null） */
      reason,
      /** 現在の統合カテゴリ（単一 or "mixed"） */
      category,
      /** 適用可能な加算キーのリスト */
      applicableKasan,
    };
  }, [selectedServiceCodes]);
}
