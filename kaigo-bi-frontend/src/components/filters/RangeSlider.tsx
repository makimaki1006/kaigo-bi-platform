"use client";

// ===================================================
// 数値レンジ入力コンポーネント
// min/max の数値入力フィールド
// ===================================================

import { NumberInput } from "@tremor/react";

interface RangeSliderProps {
  /** ラベル */
  label: string;
  /** 最小値 */
  minValue: number | null;
  /** 最大値 */
  maxValue: number | null;
  /** 最小値変更コールバック */
  onMinChange: (value: number | null) => void;
  /** 最大値変更コールバック */
  onMaxChange: (value: number | null) => void;
  /** プレースホルダー（最小） */
  minPlaceholder?: string;
  /** プレースホルダー（最大） */
  maxPlaceholder?: string;
  /** コンパクトモード */
  compact?: boolean;
}

export default function RangeSlider({
  label,
  minValue,
  maxValue,
  onMinChange,
  onMaxChange,
  minPlaceholder = "下限",
  maxPlaceholder = "上限",
  compact = false,
}: RangeSliderProps) {
  return (
    <div>
      {!compact && (
        <label className="block text-xs font-medium text-gray-600 mb-1">
          {label}
        </label>
      )}
      <div className="flex items-center gap-2">
        <NumberInput
          value={minValue ?? undefined}
          onValueChange={(v) => onMinChange(v !== undefined ? v : null)}
          placeholder={minPlaceholder}
          min={0}
          enableStepper={false}
        />
        <span className="text-gray-400 text-sm flex-shrink-0">~</span>
        <NumberInput
          value={maxValue ?? undefined}
          onValueChange={(v) => onMaxChange(v !== undefined ? v : null)}
          placeholder={maxPlaceholder}
          min={0}
          enableStepper={false}
        />
      </div>
    </div>
  );
}
