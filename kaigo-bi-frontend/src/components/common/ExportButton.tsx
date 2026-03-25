"use client";

// ===================================================
// CSVエクスポートボタン
// 認証トークン付きダウンロード + ローディング状態
// ===================================================

import { useCsvExport } from "@/hooks/useCsvExport";
import type { FilterState } from "@/lib/types";

interface ExportButtonProps {
  /** 現在のフィルタ状態 */
  filters: FilterState;
  /** ボタンラベル */
  label?: string;
  /** 無効状態 */
  disabled?: boolean;
}

export default function ExportButton({
  filters,
  label = "CSVダウンロード",
  disabled = false,
}: ExportButtonProps) {
  const { downloadCsv, isExporting, exportError } = useCsvExport();

  const isDisabled = disabled || isExporting;

  return (
    <div>
      <button
        onClick={() => downloadCsv(filters)}
        disabled={isDisabled}
        className={`
          inline-flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium
          transition-colors duration-150
          ${
            isDisabled
              ? "bg-gray-100 text-gray-400 cursor-not-allowed"
              : "bg-brand-600 text-white hover:bg-brand-700 active:bg-brand-800"
          }
        `}
      >
        {isExporting ? (
          <>
            {/* ローディングスピナー */}
            <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" />
            エクスポート中...
          </>
        ) : (
          <>
            {/* ダウンロードアイコン（SVG） */}
            <svg
              className="w-4 h-4"
              fill="none"
              stroke="currentColor"
              viewBox="0 0 24 24"
              aria-hidden="true"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M12 10v6m0 0l-3-3m3 3l3-3m2 8H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"
              />
            </svg>
            {label}
          </>
        )}
      </button>
      {/* エラーメッセージ */}
      {exportError && (
        <p className="mt-1 text-xs text-red-500">{exportError}</p>
      )}
    </div>
  );
}
