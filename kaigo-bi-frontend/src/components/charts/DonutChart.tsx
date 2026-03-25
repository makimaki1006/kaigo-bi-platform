"use client";

// ===================================================
// ドーナツチャートコンポーネント（Rechartsラッパー）
// カスタムTooltip + 中央ラベル + セグメントアニメーション
// ===================================================

import {
  PieChart,
  Pie,
  Cell,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from "recharts";
import { CHART_COLORS } from "@/lib/constants";

/** カスタムTooltip: ダーク背景 */
function CustomTooltip({
  active,
  payload,
  total,
}: {
  active?: boolean;
  payload?: any[];
  total: number;
}) {
  if (!active || !payload || payload.length === 0) return null;

  const item = payload[0];
  const value = item.value as number;
  const pct = total > 0 ? ((value / total) * 100).toFixed(1) : "0.0";

  return (
    <div className="bg-gray-900 text-white px-3 py-2 rounded-lg shadow-xl text-xs">
      <p className="text-gray-300 mb-0.5">{item.name}</p>
      <p className="font-semibold tabular-nums">
        {value.toLocaleString("ja-JP")} ({pct}%)
      </p>
    </div>
  );
}

interface DonutChartProps {
  /** チャートデータ */
  data: any[];
  /** 名前のキー */
  nameKey: string;
  /** 値のキー */
  valueKey: string;
  /** カラーパレット */
  colors?: string[];
  /** 中央ラベル */
  centerLabel?: string;
  /** チャートの高さ */
  height?: number;
}

export default function DonutChart({
  data,
  nameKey,
  valueKey,
  colors = CHART_COLORS,
  centerLabel,
  height = 300,
}: DonutChartProps) {
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

  // 合計値
  const total = data.reduce(
    (sum, item) => sum + (Number(item[valueKey]) || 0),
    0
  );

  return (
    <div className="relative" style={{ height }}>
      <ResponsiveContainer width="100%" height="100%">
        <PieChart>
          <Pie
            data={data}
            cx="50%"
            cy="45%"
            innerRadius="55%"
            outerRadius="80%"
            paddingAngle={2}
            dataKey={valueKey}
            nameKey={nameKey}
            animationDuration={800}
            animationEasing="ease-out"
          >
            {data.map((_, index) => (
              <Cell
                key={`cell-${index}`}
                fill={colors[index % colors.length]}
                stroke="white"
                strokeWidth={2}
              />
            ))}
          </Pie>
          <Tooltip
            content={<CustomTooltip total={total} />}
          />
          <Legend
            verticalAlign="bottom"
            height={36}
            iconSize={8}
            iconType="circle"
            formatter={(value: string) => (
              <span className="text-xs text-gray-600 ml-1">{value}</span>
            )}
          />
        </PieChart>
      </ResponsiveContainer>

      {/* 中央ラベル（合計表示） */}
      {centerLabel && (
        <div
          className="absolute inset-0 flex flex-col items-center justify-center pointer-events-none"
          style={{ paddingBottom: 36 }}
        >
          <span className="text-2xl font-bold text-gray-900 tabular-nums">
            {total.toLocaleString("ja-JP")}
          </span>
          <span className="text-body-sm text-gray-500">{centerLabel}</span>
        </div>
      )}
    </div>
  );
}
