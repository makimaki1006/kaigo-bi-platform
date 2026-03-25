"use client";

// ===================================================
// 棒グラフコンポーネント（Rechartsラッパー）
// カスタムTooltip、グラデーション、アニメーション対応
// ===================================================

import {
  BarChart as RechartsBarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from "recharts";
import { CHART_COLORS } from "@/lib/constants";
import type { ChartDataPoint, TooltipPayloadEntry } from "@/lib/types";

/** カスタムTooltip: ダーク背景 */
function CustomTooltip({
  active,
  payload,
  label,
  formatter,
}: {
  active?: boolean;
  payload?: TooltipPayloadEntry[];
  label?: string;
  formatter?: (value: number) => string;
}) {
  if (!active || !payload || payload.length === 0) return null;

  const value = payload[0].value as number;
  const displayValue = formatter
    ? formatter(value)
    : value.toLocaleString("ja-JP");

  return (
    <div className="bg-gray-900 text-white px-3 py-2 rounded-lg shadow-xl text-xs">
      <p className="text-gray-300 mb-0.5">{label}</p>
      <p className="font-semibold tabular-nums">{displayValue}</p>
    </div>
  );
}

interface BarChartProps {
  /** チャートデータ */
  data: ChartDataPoint[];
  /** X軸のキー */
  xKey: string;
  /** Y軸のキー */
  yKey: string;
  /** バーの色 */
  color?: string;
  /** 横棒グラフにする */
  horizontal?: boolean;
  /** ツールチップのフォーマッター */
  tooltipFormatter?: (value: number) => string;
  /** チャートの高さ */
  height?: number;
  /** グラデーション有効化 */
  gradient?: boolean;
}

/** Y軸の千区切りフォーマッター */
function yAxisFormatter(value: number): string {
  if (value >= 10000) return `${(value / 10000).toFixed(0)}万`;
  if (value >= 1000) return `${(value / 1000).toFixed(0)}K`;
  return value.toLocaleString("ja-JP");
}

export default function BarChart({
  data,
  xKey,
  yKey,
  color = CHART_COLORS[0],
  horizontal = false,
  tooltipFormatter,
  height = 300,
  gradient = true,
}: BarChartProps) {
  if (!data || data.length === 0) {
    return (
      <div
        className="flex items-center justify-center text-gray-400 text-sm"
        style={{ height }}
      >
        データがありません
      </div>
    );
  }

  // ユニークID（グラデーション用）
  const gradientId = `bar-gradient-${color.replace("#", "")}`;

  // 横棒グラフ
  if (horizontal) {
    return (
      <div style={{ overflow: "hidden" }}>
      <ResponsiveContainer
        width="100%"
        height={Math.max(height, data.length * 28)}
      >
        <RechartsBarChart
          data={data}
          layout="vertical"
          margin={{ top: 5, right: 30, left: 100, bottom: 5 }}
        >
          <defs>
            <linearGradient id={gradientId} x1="0" y1="0" x2="1" y2="0">
              <stop offset="0%" stopColor={color} stopOpacity={0.7} />
              <stop offset="100%" stopColor={color} stopOpacity={1} />
            </linearGradient>
          </defs>
          <CartesianGrid
            strokeDasharray="4 4"
            horizontal={false}
            stroke="#e5e7eb"
          />
          <XAxis
            type="number"
            tick={{ fontSize: 11, fill: "#6b7280" }}
            tickFormatter={yAxisFormatter}
          />
          <YAxis
            type="category"
            dataKey={xKey}
            tick={{ fontSize: 10, fill: "#374151" }}
            width={90}
            tickFormatter={(v: string) => v.length > 10 ? v.slice(0, 10) + "..." : v}
          />
          <Tooltip
            content={
              <CustomTooltip formatter={tooltipFormatter} />
            }
            cursor={{ fill: "rgba(99, 102, 241, 0.04)" }}
          />
          <Bar
            dataKey={yKey}
            fill={gradient ? `url(#${gradientId})` : color}
            radius={[0, 4, 4, 0]}
            maxBarSize={24}
            animationDuration={800}
            animationEasing="ease-out"
          />
        </RechartsBarChart>
      </ResponsiveContainer>
      </div>
    );
  }

  // 縦棒グラフ
  return (
    <div style={{ overflow: "hidden" }}>
    <ResponsiveContainer width="100%" height={height}>
      <RechartsBarChart
        data={data}
        margin={{ top: 5, right: 20, left: 10, bottom: 20 }}
      >
        <defs>
          <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={color} stopOpacity={1} />
            <stop offset="100%" stopColor={color} stopOpacity={0.6} />
          </linearGradient>
        </defs>
        <CartesianGrid
          strokeDasharray="4 4"
          vertical={false}
          stroke="#e5e7eb"
        />
        <XAxis
          dataKey={xKey}
          tick={{ fontSize: 10, fill: "#374151" }}
          interval={0}
          angle={-45}
          textAnchor="end"
          height={80}
          tickFormatter={(v: string) => v.length > 8 ? v.slice(0, 8) + "..." : v}
        />
        <YAxis
          tick={{ fontSize: 11, fill: "#6b7280" }}
          tickFormatter={yAxisFormatter}
        />
        <Tooltip
          content={
            <CustomTooltip formatter={tooltipFormatter} />
          }
          cursor={{ fill: "rgba(99, 102, 241, 0.04)" }}
        />
        <Bar
          dataKey={yKey}
          fill={gradient ? `url(#${gradientId})` : color}
          radius={[4, 4, 0, 0]}
          maxBarSize={40}
          animationDuration={800}
          animationEasing="ease-out"
        />
      </RechartsBarChart>
    </ResponsiveContainer>
    </div>
  );
}
