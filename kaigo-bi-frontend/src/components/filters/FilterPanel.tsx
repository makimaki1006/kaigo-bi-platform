"use client";

// ===================================================
// フィルタパネルコンポーネント
// 都道府県、サービス種別、法人種別、従業者数レンジ
// ===================================================

import MultiSelectDropdown from "@/components/ui/MultiSelectDropdown";
import PrefectureSelect from "./PrefectureSelect";
import ServiceTypeSelect from "./ServiceTypeSelect";
import RangeSlider from "./RangeSlider";
import { CORP_TYPES } from "@/lib/constants";
import type { FilterState } from "@/lib/types";

interface FilterPanelProps {
  /** 現在のフィルタ状態 */
  filters: FilterState;
  /** フィルタ変更コールバック */
  onChange: (filters: Partial<FilterState>) => void;
  /** コンパクトモード（水平レイアウト） */
  compact?: boolean;
  /** 表示するフィルタ項目（未指定の場合は全て表示） */
  visibleFilters?: Array<
    "prefectures" | "serviceCodes" | "corpTypes" | "employeeRange" | "keyword"
  >;
}

export default function FilterPanel({
  filters,
  onChange,
  compact = false,
  visibleFilters,
}: FilterPanelProps) {
  // 表示するフィルタの判定
  const show = (key: string) =>
    !visibleFilters || visibleFilters.includes(key as never);

  // アクティブなフィルタ数を計算
  const activeCount =
    filters.prefectures.length +
    filters.serviceCodes.length +
    filters.corpTypes.length +
    (filters.employeeMin != null ? 1 : 0) +
    (filters.employeeMax != null ? 1 : 0) +
    (filters.keyword ? 1 : 0);

  return (
    <div
      className={`
        bg-white rounded-lg border border-gray-200 p-4
        ${compact ? "flex flex-wrap items-end gap-4" : "space-y-4"}
      `}
    >
      {/* パネルタイトル */}
      {!compact && (
        <div className="flex items-center justify-between mb-2">
          <h3 className="text-sm font-semibold text-gray-700">フィルタ</h3>
          <button
            onClick={() =>
              onChange({
                prefectures: [],
                serviceCodes: [],
                corpTypes: [],
                employeeMin: null,
                employeeMax: null,
                keyword: "",
              })
            }
            className="text-xs text-blue-600 hover:text-blue-800 transition-colors"
          >
            リセット
          </button>
        </div>
      )}

      {/* 市区町村キーワード */}
      {show("keyword") && (
        <div className={compact ? "w-48" : ""}>
          {!compact && (
            <label className="block text-xs font-medium text-gray-600 mb-1">
              市区町村キーワード
            </label>
          )}
          <input
            type="text"
            value={filters.keyword || ""}
            onChange={(e) => onChange({ keyword: e.target.value })}
            placeholder="市区町村名で絞り込み..."
            className="w-full px-3 py-2 text-sm border border-gray-300 rounded-lg bg-white focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500 transition-colors"
          />
        </div>
      )}

      {/* 都道府県 */}
      {show("prefectures") && (
        <div className={compact ? "w-56" : ""}>
          <PrefectureSelect
            value={filters.prefectures}
            onChange={(v) => onChange({ prefectures: v })}
            compact={compact}
          />
        </div>
      )}

      {/* サービス種別 */}
      {show("serviceCodes") && (
        <div className={compact ? "w-56" : ""}>
          <ServiceTypeSelect
            value={filters.serviceCodes}
            onChange={(v) => onChange({ serviceCodes: v })}
            compact={compact}
          />
        </div>
      )}

      {/* 法人種別 */}
      {show("corpTypes") && (
        <div className={compact ? "w-48" : ""}>
          {!compact && (
            <label className="block text-xs font-medium text-gray-600 mb-1">
              法人種別
            </label>
          )}
          <MultiSelectDropdown
            value={filters.corpTypes}
            onValueChange={(v) => onChange({ corpTypes: v })}
            placeholder="法人種別を選択..."
            options={CORP_TYPES.map((type) => ({ value: type, label: type }))}
          />
        </div>
      )}

      {/* 従業者数レンジ */}
      {show("employeeRange") && (
        <div className={compact ? "w-56" : ""}>
          <RangeSlider
            label="従業者数"
            minValue={filters.employeeMin}
            maxValue={filters.employeeMax}
            onMinChange={(v) => onChange({ employeeMin: v })}
            onMaxChange={(v) => onChange({ employeeMax: v })}
            minPlaceholder="下限"
            maxPlaceholder="上限"
            compact={compact}
          />
        </div>
      )}

      {/* アクティブフィルタ数表示 */}
      {activeCount > 0 && (
        <div className="text-xs text-indigo-600 font-medium self-center">
          {activeCount}件のフィルタ適用中
        </div>
      )}

      {/* コンパクトモード時のリセットボタン */}
      {compact && (
        <button
          onClick={() =>
            onChange({
              prefectures: [],
              serviceCodes: [],
              corpTypes: [],
              employeeMin: null,
              employeeMax: null,
              keyword: "",
            })
          }
          className="text-xs text-blue-600 hover:text-blue-800 transition-colors px-2 py-1"
        >
          リセット
        </button>
      )}
    </div>
  );
}
