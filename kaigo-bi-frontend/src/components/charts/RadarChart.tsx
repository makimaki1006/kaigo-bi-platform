"use client";

// ===================================================
// レーダーチャートコンポーネント（Rechartsラッパー）
// 複数カテゴリの比較を多角形で可視化
// ===================================================

import {
  RadarChart as RechartsRadarChart,
  PolarGrid,
  PolarAngleAxis,
  PolarRadiusAxis,
  Radar,
  ResponsiveContainer,
  Tooltip,
  Legend,
} from "recharts";
import { CHART_COLORS } from "@/lib/constants";

interface RadarSeries {
  /** データキー */
  dataKey: string;
  /** 表示名 */
  name: string;
  /** 色 */
  color?: string;
}

interface RadarChartProps {
  /** チャートデータ（各カテゴリの値を含むオブジェクト配列） */
  data: any[];
  /** カテゴリ名のキー */
  categoryKey: string;
  /** 表示するシリーズ定義 */
  series: RadarSeries[];
  /** チャートの高さ */
  height?: number;
}

export default function RadarChart({
  data,
  categoryKey,
  series,
  height = 320,
}: RadarChartProps) {
  if (!data || data.length === 0) {
    return (
      <div className="flex items-center justify-center text-gray-400 text-sm" style={{ height }}>
        データがありません
      </div>
    );
  }

  return (
    <ResponsiveContainer width="100%" height={height}>
      <RechartsRadarChart data={data} cx="50%" cy="50%" outerRadius="70%">
        <PolarGrid stroke="#e5e7eb" />
        <PolarAngleAxis
          dataKey={categoryKey}
          tick={{ fontSize: 11, fill: "#374151" }}
        />
        <PolarRadiusAxis
          tick={{ fontSize: 10, fill: "#9ca3af" }}
          axisLine={false}
        />
        {series.map((s, i) => (
          <Radar
            key={s.dataKey}
            name={s.name}
            dataKey={s.dataKey}
            stroke={s.color || CHART_COLORS[i % CHART_COLORS.length]}
            fill={s.color || CHART_COLORS[i % CHART_COLORS.length]}
            fillOpacity={0.2}
            strokeWidth={2}
          />
        ))}
        <Tooltip
          contentStyle={{
            fontSize: 12,
            borderRadius: 8,
            border: "1px solid #e5e7eb",
          }}
        />
        {series.length > 1 && (
          <Legend
            verticalAlign="bottom"
            height={36}
            iconSize={10}
            formatter={(value: string) => (
              <span className="text-xs text-gray-600">{value}</span>
            )}
          />
        )}
      </RechartsRadarChart>
    </ResponsiveContainer>
  );
}
