"use client";

// ===================================================
// ローディングスピナー + スケルトンコンポーネント
// シマーエフェクトベースのスケルトンバリエーション
// ===================================================

interface LoadingSpinnerProps {
  /** スピナーサイズ */
  size?: "sm" | "md" | "lg";
  /** テキスト表示 */
  text?: string;
}

const sizeClasses = {
  sm: "w-4 h-4 border-2",
  md: "w-8 h-8 border-2",
  lg: "w-12 h-12 border-3",
};

/** 従来のスピナー（互換用） */
export default function LoadingSpinner({
  size = "md",
  text,
}: LoadingSpinnerProps) {
  return (
    <div className="flex flex-col items-center justify-center gap-3 py-8">
      <div
        className={`${sizeClasses[size]} border-gray-200 border-t-brand-500 rounded-full animate-spin`}
      />
      {text && <p className="text-sm text-gray-500">{text}</p>}
    </div>
  );
}

// ===================================================
// スケルトンバリエーション
// ===================================================

/** KPIカード用スケルトン */
export function KpiCardSkeleton() {
  return (
    <div className="relative bg-white rounded-xl shadow-card overflow-hidden">
      <div className="absolute left-0 top-0 bottom-0 w-1 bg-gray-200" />
      <div className="pl-5 pr-5 py-5 space-y-3">
        <div className="h-4 w-24 rounded shimmer" />
        <div className="h-9 w-32 rounded shimmer" />
        <div className="h-3 w-20 rounded shimmer" />
      </div>
    </div>
  );
}

/** KPIカードグリッド用スケルトン */
export function KpiGridSkeleton({ count = 4 }: { count?: number }) {
  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
      {Array.from({ length: count }).map((_, i) => (
        <KpiCardSkeleton key={i} />
      ))}
    </div>
  );
}

/** チャートカード用スケルトン */
export function ChartCardSkeleton({ height = 300 }: { height?: number }) {
  return (
    <div className="bg-white rounded-xl shadow-card overflow-hidden">
      {/* ヘッダー */}
      <div className="px-5 pt-5 pb-3 border-b border-gray-100 space-y-2">
        <div className="h-4 w-40 rounded shimmer" />
        <div className="h-3 w-56 rounded shimmer" />
      </div>
      {/* チャートエリア */}
      <div className="px-5 pb-5 pt-3">
        <div className="flex items-end gap-2" style={{ height }}>
          {[40, 65, 50, 80, 55, 70, 45, 60, 75, 50].map((h, i) => (
            <div
              key={i}
              className="flex-1 shimmer rounded-t"
              style={{
                height: `${h}%`,
                animationDelay: `${i * 100}ms`,
              }}
            />
          ))}
        </div>
      </div>
    </div>
  );
}

/** テーブル用スケルトン */
export function TableSkeleton({
  rows = 8,
  cols = 5,
}: {
  rows?: number;
  cols?: number;
}) {
  return (
    <div className="bg-white rounded-xl shadow-card overflow-hidden">
      {/* ヘッダー */}
      <div className="bg-gray-50 border-b border-gray-200 px-4 py-3 flex gap-4">
        {Array.from({ length: cols }).map((_, i) => (
          <div key={i} className="flex-1 h-3 shimmer rounded" />
        ))}
      </div>
      {/* 行 */}
      {Array.from({ length: rows }).map((_, rowIdx) => (
        <div
          key={rowIdx}
          className={`px-4 py-3 flex gap-4 border-b border-gray-100 ${
            rowIdx % 2 === 1 ? "bg-gray-50/50" : ""
          }`}
        >
          {Array.from({ length: cols }).map((_, colIdx) => (
            <div
              key={colIdx}
              className="flex-1 h-4 shimmer rounded"
              style={{
                animationDelay: `${(rowIdx * cols + colIdx) * 30}ms`,
              }}
            />
          ))}
        </div>
      ))}
    </div>
  );
}
