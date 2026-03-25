"use client";

// ===================================================
// 数値レンジ入力コンポーネント
// min/max の数値入力フィールド（ネイティブinput使用）
// ===================================================

interface RangeSliderProps {
  label: string;
  minValue: number | null;
  maxValue: number | null;
  onMinChange: (value: number | null) => void;
  onMaxChange: (value: number | null) => void;
  minPlaceholder?: string;
  maxPlaceholder?: string;
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
      <div className="flex items-center gap-1">
        <input
          type="number"
          value={minValue ?? ""}
          onChange={(e) => onMinChange(e.target.value ? Number(e.target.value) : null)}
          placeholder={minPlaceholder}
          min={0}
          className="w-full min-w-0 rounded-md border border-gray-300 px-2 py-1.5 text-sm text-gray-700 placeholder-gray-400 focus:border-brand-500 focus:ring-1 focus:ring-brand-500 outline-none"
        />
        <span className="text-gray-400 text-xs flex-shrink-0">~</span>
        <input
          type="number"
          value={maxValue ?? ""}
          onChange={(e) => onMaxChange(e.target.value ? Number(e.target.value) : null)}
          placeholder={maxPlaceholder}
          min={0}
          className="w-full min-w-0 rounded-md border border-gray-300 px-2 py-1.5 text-sm text-gray-700 placeholder-gray-400 focus:border-brand-500 focus:ring-1 focus:ring-brand-500 outline-none"
        />
      </div>
    </div>
  );
}
