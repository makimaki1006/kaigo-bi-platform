"use client";

// ===================================================
// データ準備中プレースホルダー
// フルデータ（76カラム）が未取得の場合に表示
// ===================================================

interface DataPendingPlaceholderProps {
  /** メッセージ */
  message?: string;
  /** 補足説明 */
  description?: string;
  /** 高さ */
  height?: number;
}

export default function DataPendingPlaceholder({
  message = "データ準備中",
  description = "フルデータ取得後に表示されます",
  height = 300,
}: DataPendingPlaceholderProps) {
  return (
    <div
      className="flex flex-col items-center justify-center text-center"
      style={{ height }}
    >
      {/* アイコン: データベース風のSVG */}
      <div className="w-16 h-16 mb-4 rounded-full bg-gray-100 flex items-center justify-center">
        <svg
          className="w-8 h-8 text-gray-300"
          fill="none"
          stroke="currentColor"
          viewBox="0 0 24 24"
          aria-hidden="true"
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth={1.5}
            d="M20 7l-8-4-8 4m16 0l-8 4m8-4v10l-8 4m0-10L4 7m8 4v10M4 7v10l8 4"
          />
        </svg>
      </div>
      <p className="text-sm font-medium text-gray-500">{message}</p>
      <p className="text-xs text-gray-400 mt-1 max-w-xs">{description}</p>
    </div>
  );
}
