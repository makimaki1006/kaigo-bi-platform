"use client";

// ===================================================
// サービス種別マルチセレクト
// ===================================================

import MultiSelectDropdown from "@/components/ui/MultiSelectDropdown";
import { SERVICE_TYPES } from "@/lib/constants";

interface ServiceTypeSelectProps {
  /** 選択中のサービスコード */
  value: string[];
  /** 変更コールバック */
  onChange: (values: string[]) => void;
  /** コンパクトモード */
  compact?: boolean;
}

export default function ServiceTypeSelect({
  value,
  onChange,
  compact = false,
}: ServiceTypeSelectProps) {
  return (
    <div>
      {!compact && (
        <label className="block text-xs font-medium text-gray-600 mb-1">
          サービス種別
        </label>
      )}
      <MultiSelectDropdown
        value={value}
        onValueChange={onChange}
        placeholder="サービス種別を選択..."
        options={Object.entries(SERVICE_TYPES).map(([code, name]) => ({
          value: code,
          label: name,
        }))}
      />
    </div>
  );
}
