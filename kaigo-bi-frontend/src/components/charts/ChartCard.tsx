"use client";

// ===================================================
// チャートカードラッパー
// Tremor Card脱却、独自div
// ヘッダー/ボーダー/アクションスロット対応
// ===================================================

import { type ReactNode } from "react";

interface ChartCardProps {
  /** チャートタイトル */
  title: string;
  /** サブタイトル */
  subtitle?: string;
  /** チャート本体 */
  children: ReactNode;
  /** ローディング状態 */
  loading?: boolean;
  /** 追加CSSクラス */
  className?: string;
  /** 右上アクションエリア（将来用スロット） */
  actions?: ReactNode;
}

/** チャート用スケルトン */
function ChartSkeleton() {
  return (
    <div className="space-y-3 py-4">
      {/* バーチャート風スケルトン */}
      <div className="flex items-end gap-2 h-48 px-4">
        {[40, 65, 50, 80, 55, 70, 45, 60, 75, 50].map((h, i) => (
          <div
            key={i}
            className="flex-1 shimmer rounded-t"
            style={{ height: `${h}%` }}
          />
        ))}
      </div>
      {/* X軸ラベル風 */}
      <div className="flex gap-2 px-4">
        {Array.from({ length: 5 }).map((_, i) => (
          <div key={i} className="flex-1 h-3 shimmer rounded" />
        ))}
      </div>
    </div>
  );
}

export default function ChartCard({
  title,
  subtitle,
  children,
  loading = false,
  className = "",
  actions,
}: ChartCardProps) {
  return (
    <div
      className={`bg-white rounded-xl shadow-card transition-shadow duration-200 hover:shadow-card-hover overflow-hidden ${className}`}
    >
      {/* ヘッダー */}
      <div className="flex items-start justify-between px-5 pt-5 pb-3 border-b border-gray-100">
        <div className="min-w-0">
          <h3 className="text-heading-sm text-gray-800 truncate">{title}</h3>
          {subtitle && (
            <p className="text-body-sm text-gray-400 mt-0.5 truncate">
              {subtitle}
            </p>
          )}
        </div>
        {/* アクションエリア（将来用） */}
        {actions && (
          <div className="flex-shrink-0 ml-3">{actions}</div>
        )}
      </div>

      {/* コンテンツ */}
      <div className="px-5 pb-5 pt-3">
        {loading ? <ChartSkeleton /> : children}
      </div>
    </div>
  );
}
