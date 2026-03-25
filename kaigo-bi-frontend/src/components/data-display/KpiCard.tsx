"use client";

// ===================================================
// KPIカードコンポーネント
// 左カラーバー + アイコン + メトリクス値 + トレンド
// プロフェッショナルなコンサルティングツール仕様
// ===================================================

import { type ReactNode, useState, useRef } from "react";
import { formatKpiValue } from "@/lib/formatters";

interface KpiCardProps {
  /** ラベル（例: "総施設数"） */
  label: string;
  /** 数値 */
  value: number | null | undefined;
  /** フォーマット種別 */
  format?: "number" | "percent" | "decimal";
  /** アイコン（ReactNode: Lucideアイコン等） */
  icon?: ReactNode;
  /** サブテキスト（補足情報） */
  subtitle?: string;
  /** ローディング状態 */
  loading?: boolean;
  /** カラーバーの色（Tailwind色クラス名） */
  accentColor?: string;
  /** トレンド値（例: +5.2 or -3.1） */
  trend?: number | null;
  /** トレンドのラベル（例: "前月比"） */
  trendLabel?: string;
  /** ツールチップ（ラベルの説明、ホバーで表示） */
  tooltip?: string;
}

/** ツールチップコンポーネント */
function Tooltip({ text }: { text: string }) {
  const [show, setShow] = useState(false);
  const tooltipRef = useRef<HTMLDivElement>(null);

  return (
    <span className="relative inline-flex items-center ml-1">
      <button
        type="button"
        onMouseEnter={() => setShow(true)}
        onMouseLeave={() => setShow(false)}
        onFocus={() => setShow(true)}
        onBlur={() => setShow(false)}
        className="inline-flex items-center justify-center w-4 h-4 rounded-full bg-gray-200 text-gray-500 hover:bg-gray-300 hover:text-gray-700 transition-colors text-[10px] font-bold leading-none cursor-help"
        aria-label="説明を表示"
      >
        ?
      </button>
      {show && (
        <div
          ref={tooltipRef}
          role="tooltip"
          className="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 px-3 py-2 bg-gray-800 text-white text-xs rounded-lg shadow-lg whitespace-nowrap z-50 max-w-xs"
        >
          {text}
          {/* 三角矢印 */}
          <div className="absolute top-full left-1/2 -translate-x-1/2 w-0 h-0 border-l-4 border-r-4 border-t-4 border-l-transparent border-r-transparent border-t-gray-800" />
        </div>
      )}
    </span>
  );
}

export default function KpiCard({
  label,
  value,
  format = "number",
  icon,
  subtitle,
  loading = false,
  accentColor = "bg-brand-500",
  trend,
  trendLabel,
  tooltip,
}: KpiCardProps) {
  // ローディング時: シマースケルトン
  if (loading) {
    return (
      <div className="relative bg-white rounded-xl shadow-card overflow-hidden animate-fade-in-up">
        {/* 左カラーバー（スケルトン） */}
        <div className="absolute left-0 top-0 bottom-0 w-1 bg-gray-200" />
        <div className="pl-5 pr-5 py-5">
          <div className="space-y-3">
            <div className="h-4 w-24 rounded shimmer" />
            <div className="h-9 w-32 rounded shimmer" />
            <div className="h-3 w-20 rounded shimmer" />
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="group relative bg-white rounded-xl shadow-card overflow-hidden animate-fade-in-up transition-all duration-200 hover:shadow-card-hover hover:-translate-y-0.5">
      {/* 左カラーバー（インジケータ） */}
      <div className={`absolute left-0 top-0 bottom-0 w-1 ${accentColor}`} />

      <div className="pl-5 pr-5 py-5">
        {/* ラベル行 + アイコン */}
        <div className="flex items-center gap-2 mb-2">
          {icon && (
            <span className="flex-shrink-0 text-gray-400 [&>svg]:w-4 [&>svg]:h-4">
              {icon}
            </span>
          )}
          <p className="text-body-sm font-medium text-gray-500 truncate inline-flex items-center">
            {label}
            {tooltip && <Tooltip text={tooltip} />}
          </p>
        </div>

        {/* メトリクス値 */}
        <p className="text-3xl font-bold text-gray-900 tabular-nums leading-tight">
          {formatKpiValue(value, format)}
        </p>

        {/* トレンド + サブテキスト */}
        <div className="flex items-center gap-2 mt-2">
          {trend != null && (
            <span
              className={`inline-flex items-center gap-0.5 text-xs font-medium tabular-nums ${
                trend > 0
                  ? "text-emerald-600"
                  : trend < 0
                  ? "text-red-500"
                  : "text-gray-400"
              }`}
            >
              {/* 三角アイコン */}
              {trend > 0 ? (
                <svg className="w-3 h-3" viewBox="0 0 12 12" fill="currentColor" aria-hidden="true">
                  <path d="M6 2L11 10H1L6 2Z" />
                </svg>
              ) : trend < 0 ? (
                <svg className="w-3 h-3" viewBox="0 0 12 12" fill="currentColor" aria-hidden="true">
                  <path d="M6 10L1 2H11L6 10Z" />
                </svg>
              ) : null}
              {trend > 0 ? "+" : ""}
              {trend.toFixed(1)}%
              {trendLabel && (
                <span className="text-gray-400 font-normal ml-0.5">
                  {trendLabel}
                </span>
              )}
            </span>
          )}
          {subtitle && (
            <p className="text-body-sm text-gray-400 truncate">{subtitle}</p>
          )}
        </div>
      </div>
    </div>
  );
}
