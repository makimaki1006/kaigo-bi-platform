"use client";

// ===================================================
// 都道府県マルチセレクト
// ===================================================

import MultiSelectDropdown from "@/components/ui/MultiSelectDropdown";
import { PREFECTURES } from "@/lib/constants";

interface PrefectureSelectProps {
  /** 選択中の都道府県 */
  value: string[];
  /** 変更コールバック */
  onChange: (values: string[]) => void;
  /** コンパクトモード */
  compact?: boolean;
}

export default function PrefectureSelect({
  value,
  onChange,
  compact = false,
}: PrefectureSelectProps) {
  return (
    <div>
      {!compact && (
        <label className="block text-xs font-medium text-gray-600 mb-1">
          都道府県
        </label>
      )}
      <MultiSelectDropdown
        value={value}
        onValueChange={onChange}
        placeholder="都道府県を選択..."
        options={PREFECTURES.map((p) => ({ value: p, label: p }))}
      />
    </div>
  );
}
