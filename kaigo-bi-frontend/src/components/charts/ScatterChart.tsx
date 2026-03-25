"use client";

// ===================================================
// 散布図コンポーネント（Rechartsラッパー）
// X軸 vs Y軸の相関を可視化
// ===================================================

import {
  ScatterChart as RechartsScatterChart,
  Scatter,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  ZAxis,
} from "recharts";
import { CHART_COLORS } from "@/lib/constants";

interface ScatterChartProps {
  /** チャートデータ */
  data: any[];
  /** X軸のキー */
  xKey: string;
  /** Y軸のキー */
  yKey: string;
  /** X軸ラベル */
  xLabel?: string;
  /** Y軸ラベル */
  yLabel?: string;
  /** ツールチップに表示する名前のキー */
  nameKey?: string;
  /** ドットの色 */
  color?: string;
  /** チャートの高さ */
  height?: number;
  /** ツールチップのフォーマッター */
  tooltipFormatter?: (value: number, name: string) => string;
}

export default function ScatterChart({
  data,
  xKey,
  yKey,
  xLabel,
  yLabel,
  nameKey,
  color = CHART_COLORS[0],
  height = 300,
  tooltipFormatter,
}: ScatterChartProps) {
  if (!data || data.length === 0) {
    return (
      <div className="flex items-center justify-center text-gray-400 text-sm" style={{ height }}>
        データがありません
      </div>
    );
  }

  // カスタムツールチップ
  const CustomTooltip = ({ active, payload }: any) => {
    if (!active || !payload || payload.length === 0) return null;
    const item = payload[0].payload;
    return (
      <div className="bg-white border border-gray-200 rounded-lg shadow-sm px-3 py-2 text-xs">
        {nameKey && item[nameKey] && (
          <p className="font-semibold text-gray-700 mb-1">{item[nameKey]}</p>
        )}
        <p className="text-gray-600">
          {xLabel || xKey}: {tooltipFormatter ? tooltipFormatter(item[xKey], xKey) : item[xKey]?.toLocaleString("ja-JP")}
        </p>
        <p className="text-gray-600">
          {yLabel || yKey}: {tooltipFormatter ? tooltipFormatter(item[yKey], yKey) : item[yKey]?.toLocaleString("ja-JP")}
        </p>
      </div>
    );
  };

  return (
    <div style={{ overflow: "hidden" }}>
    <ResponsiveContainer width="100%" height={height}>
      <RechartsScatterChart margin={{ top: 10, right: 20, bottom: 30, left: 10 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
        <XAxis
          type="number"
          dataKey={xKey}
          name={xLabel || xKey}
          tick={{ fontSize: 11, fill: "#6b7280" }}
          label={xLabel ? { value: xLabel, position: "bottom", offset: 10, fontSize: 11, fill: "#6b7280" } : undefined}
        />
        <YAxis
          type="number"
          dataKey={yKey}
          name={yLabel || yKey}
          tick={{ fontSize: 11, fill: "#6b7280" }}
          label={yLabel ? { value: yLabel, angle: -90, position: "insideLeft", offset: 0, fontSize: 11, fill: "#6b7280" } : undefined}
        />
        <ZAxis range={[40, 120]} />
        <Tooltip content={<CustomTooltip />} />
        <Scatter data={data} fill={color} fillOpacity={0.6} />
      </RechartsScatterChart>
    </ResponsiveContainer>
    </div>
  );
}
