"use client";

// ===================================================
// 折れ線グラフコンポーネント（Rechartsラッパー）
// 実績線と予測線（破線）を描き分け可能
// ===================================================

import {
  LineChart as RechartsLineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
  ReferenceLine,
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

  return (
    <div className="bg-gray-900 text-white px-3 py-2 rounded-lg shadow-xl text-xs">
      <p className="text-gray-300 mb-1">{label}</p>
      {payload.map((entry: TooltipPayloadEntry, index: number) => {
        const value = entry.value as number;
        const displayValue = formatter
          ? formatter(value)
          : value.toLocaleString("ja-JP");
        return (
          <p key={index} className="font-semibold tabular-nums" style={{ color: entry.color }}>
            {entry.name}: {displayValue}
          </p>
        );
      })}
    </div>
  );
}

interface LineSeriesConfig {
  /** データキー */
  dataKey: string;
  /** 表示名 */
  name: string;
  /** 線の色 */
  color?: string;
  /** 破線にする（予測線等） */
  dashed?: boolean;
}

interface LineChartProps {
  /** チャートデータ */
  data: ChartDataPoint[];
  /** X軸のキー */
  xKey: string;
  /** 線の定義（複数系列対応） */
  series: LineSeriesConfig[];
  /** ツールチップのフォーマッター */
  tooltipFormatter?: (value: number) => string;
  /** チャートの高さ */
  height?: number;
  /** 実績と予測の境界を示す参照線のX値 */
  referenceLineX?: string;
  /** 参照線のラベル */
  referenceLineLabel?: string;
}

/** Y軸の千区切りフォーマッター */
function yAxisFormatter(value: number): string {
  if (value >= 10000) return `${(value / 10000).toFixed(0)}万`;
  if (value >= 1000) return `${(value / 1000).toFixed(0)}K`;
  return value.toLocaleString("ja-JP");
}

export default function LineChart({
  data,
  xKey,
  series,
  tooltipFormatter,
  height = 300,
  referenceLineX,
  referenceLineLabel,
}: LineChartProps) {
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

  return (
    <div style={{ overflow: "hidden" }}>
    <ResponsiveContainer width="100%" height={height}>
      <RechartsLineChart
        data={data}
        margin={{ top: 5, right: 20, left: 10, bottom: 5 }}
      >
        <CartesianGrid
          strokeDasharray="4 4"
          vertical={false}
          stroke="#e5e7eb"
        />
        <XAxis
          dataKey={xKey}
          tick={{ fontSize: 11, fill: "#374151" }}
          interval={0}
          angle={-45}
          textAnchor="end"
          height={60}
        />
        <YAxis
          tick={{ fontSize: 11, fill: "#6b7280" }}
          tickFormatter={yAxisFormatter}
        />
        <Tooltip
          content={<CustomTooltip formatter={tooltipFormatter} />}
          cursor={{ stroke: "rgba(99, 102, 241, 0.3)" }}
        />
        <Legend
          wrapperStyle={{ fontSize: 12, paddingTop: 8 }}
        />
        {referenceLineX && (
          <ReferenceLine
            x={referenceLineX}
            stroke="#9ca3af"
            strokeDasharray="3 3"
            label={{
              value: referenceLineLabel || "",
              position: "top",
              fill: "#6b7280",
              fontSize: 11,
            }}
          />
        )}
        {series.map((s, i) => (
          <Line
            key={s.dataKey}
            type="monotone"
            dataKey={s.dataKey}
            name={s.name}
            stroke={s.color || CHART_COLORS[i % CHART_COLORS.length]}
            strokeWidth={2}
            strokeDasharray={s.dashed ? "8 4" : undefined}
            dot={!s.dashed}
            activeDot={{ r: 5 }}
            animationDuration={800}
            animationEasing="ease-out"
            connectNulls
          />
        ))}
      </RechartsLineChart>
    </ResponsiveContainer>
    </div>
  );
}
