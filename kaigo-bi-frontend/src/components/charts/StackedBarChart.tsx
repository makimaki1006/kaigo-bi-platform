"use client";

// ===================================================
// 積み上げ棒グラフコンポーネント（Rechartsラッパー）
// 複数のデータ系列を積み上げ表示
// ===================================================

import {
  BarChart as RechartsBarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from "recharts";
import { CHART_COLORS } from "@/lib/constants";
import type { ChartDataPoint } from "@/lib/types";

interface StackedBarSeries {
  /** データキー */
  dataKey: string;
  /** 表示名 */
  name: string;
  /** 色（未指定の場合はパレットから自動割り当て） */
  color?: string;
}

interface StackedBarChartProps {
  /** チャートデータ */
  data: ChartDataPoint[];
  /** X軸（カテゴリ）のキー */
  xKey: string;
  /** データ系列の定義 */
  series: StackedBarSeries[];
  /** チャートの高さ */
  height?: number;
  /** 横棒グラフにする */
  horizontal?: boolean;
}

export default function StackedBarChart({
  data,
  xKey,
  series,
  height = 300,
  horizontal = false,
}: StackedBarChartProps) {
  if (!data || data.length === 0) {
    return (
      <div className="flex items-center justify-center text-gray-400 text-sm" style={{ height }}>
        データがありません
      </div>
    );
  }

  if (horizontal) {
    return (
      <ResponsiveContainer width="100%" height={Math.max(height, data.length * 32)}>
        <RechartsBarChart
          data={data}
          layout="vertical"
          margin={{ top: 5, right: 30, left: 100, bottom: 5 }}
        >
          <CartesianGrid strokeDasharray="3 3" horizontal={false} stroke="#f0f0f0" />
          <XAxis type="number" tick={{ fontSize: 12, fill: "#6b7280" }} />
          <YAxis
            type="category"
            dataKey={xKey}
            tick={{ fontSize: 11, fill: "#374151" }}
            width={90}
          />
          <Tooltip
            contentStyle={{
              fontSize: 12,
              borderRadius: 8,
              border: "1px solid #e5e7eb",
            }}
          />
          <Legend
            verticalAlign="bottom"
            height={36}
            iconSize={10}
            formatter={(value: string) => (
              <span className="text-xs text-gray-600">{value}</span>
            )}
          />
          {series.map((s, i) => (
            <Bar
              key={s.dataKey}
              dataKey={s.dataKey}
              name={s.name}
              stackId="stack"
              fill={s.color || CHART_COLORS[i % CHART_COLORS.length]}
              maxBarSize={24}
            />
          ))}
        </RechartsBarChart>
      </ResponsiveContainer>
    );
  }

  return (
    <ResponsiveContainer width="100%" height={height}>
      <RechartsBarChart
        data={data}
        margin={{ top: 5, right: 20, left: 10, bottom: 5 }}
      >
        <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#f0f0f0" />
        <XAxis
          dataKey={xKey}
          tick={{ fontSize: 11, fill: "#374151" }}
          interval={0}
          angle={-45}
          textAnchor="end"
          height={60}
        />
        <YAxis tick={{ fontSize: 12, fill: "#6b7280" }} />
        <Tooltip
          contentStyle={{
            fontSize: 12,
            borderRadius: 8,
            border: "1px solid #e5e7eb",
          }}
        />
        <Legend
          verticalAlign="bottom"
          height={36}
          iconSize={10}
          formatter={(value: string) => (
            <span className="text-xs text-gray-600">{value}</span>
          )}
        />
        {series.map((s, i) => (
          <Bar
            key={s.dataKey}
            dataKey={s.dataKey}
            name={s.name}
            stackId="stack"
            fill={s.color || CHART_COLORS[i % CHART_COLORS.length]}
            radius={i === series.length - 1 ? [4, 4, 0, 0] : undefined}
            maxBarSize={40}
          />
        ))}
      </RechartsBarChart>
    </ResponsiveContainer>
  );
}
