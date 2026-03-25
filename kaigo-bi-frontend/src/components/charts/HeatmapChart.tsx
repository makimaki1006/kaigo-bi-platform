"use client";

// ===================================================
// ヒートマップチャートコンポーネント
// CSSグリッドベースで行×列のセルに色を付与
// Rechartsにはヒートマップがないためカスタム実装
// ===================================================

import { useMemo, useState } from "react";

interface HeatmapChartProps {
  /** 行ラベル配列 */
  rows: string[];
  /** 列ラベル配列 */
  columns: string[];
  /** 値マトリクス（rows.length x columns.length の2次元配列） */
  values: (number | null)[][];
  /** カラースケール（[min色, max色]） */
  colorScale?: [string, string];
  /** ツールチップフォーマッター */
  tooltipFormatter?: (value: number | null, row: string, col: string) => string;
  /** セルの最小高さ */
  cellHeight?: number;
}

/**
 * 2色間の補間を行う
 */
function interpolateColor(color1: string, color2: string, factor: number): string {
  const hex = (c: string) => {
    const h = c.replace("#", "");
    return [
      parseInt(h.substring(0, 2), 16),
      parseInt(h.substring(2, 4), 16),
      parseInt(h.substring(4, 6), 16),
    ];
  };
  const [r1, g1, b1] = hex(color1);
  const [r2, g2, b2] = hex(color2);
  const r = Math.round(r1 + (r2 - r1) * factor);
  const g = Math.round(g1 + (g2 - g1) * factor);
  const b = Math.round(b1 + (b2 - b1) * factor);
  return `rgb(${r}, ${g}, ${b})`;
}

export default function HeatmapChart({
  rows,
  columns,
  values,
  colorScale = ["#eff6ff", "#1d4ed8"],
  tooltipFormatter,
  cellHeight = 32,
}: HeatmapChartProps) {
  const [tooltip, setTooltip] = useState<{
    x: number;
    y: number;
    content: string;
  } | null>(null);

  // 値の範囲を計算
  const { min, max } = useMemo(() => {
    const flat = values.flat().filter((v): v is number => v !== null);
    if (flat.length === 0) return { min: 0, max: 1 };
    return { min: Math.min(...flat), max: Math.max(...flat) };
  }, [values]);

  if (rows.length === 0 || columns.length === 0) {
    return (
      <div className="flex items-center justify-center text-gray-400 text-sm h-64">
        データがありません
      </div>
    );
  }

  // 値から色を取得
  const getCellColor = (value: number | null): string => {
    if (value === null) return "#f3f4f6";
    const range = max - min;
    const factor = range === 0 ? 0.5 : (value - min) / range;
    return interpolateColor(colorScale[0], colorScale[1], factor);
  };

  // ツールチップ表示
  const handleMouseEnter = (
    e: React.MouseEvent,
    rowIdx: number,
    colIdx: number
  ) => {
    const value = values[rowIdx]?.[colIdx];
    const content = tooltipFormatter
      ? tooltipFormatter(value ?? null, rows[rowIdx], columns[colIdx])
      : `${rows[rowIdx]} x ${columns[colIdx]}: ${value != null ? value.toLocaleString("ja-JP") : "-"}`;
    const rect = e.currentTarget.getBoundingClientRect();
    const parentRect = e.currentTarget.closest(".heatmap-container")?.getBoundingClientRect();
    if (parentRect) {
      setTooltip({
        x: rect.left - parentRect.left + rect.width / 2,
        y: rect.top - parentRect.top - 8,
        content,
      });
    }
  };

  return (
    <div className="heatmap-container relative overflow-x-auto">
      {/* ヒートマップ本体 */}
      <div
        className="inline-grid gap-px bg-gray-200 rounded"
        style={{
          gridTemplateColumns: `120px repeat(${columns.length}, minmax(48px, 1fr))`,
        }}
      >
        {/* ヘッダー行: 空セル + 列ラベル */}
        <div className="bg-gray-50 px-2 py-1" />
        {columns.map((col) => (
          <div
            key={col}
            className="bg-gray-50 px-1 py-1 text-[10px] font-medium text-gray-600 text-center truncate"
            title={col}
          >
            {col}
          </div>
        ))}

        {/* データ行 */}
        {rows.map((row, rowIdx) => (
          <>
            {/* 行ラベル */}
            <div
              key={`label-${row}`}
              className="bg-gray-50 px-2 py-1 text-[11px] font-medium text-gray-700 flex items-center truncate"
              title={row}
            >
              {row}
            </div>
            {/* データセル */}
            {columns.map((col, colIdx) => {
              const value = values[rowIdx]?.[colIdx];
              return (
                <div
                  key={`${row}-${col}`}
                  className="flex items-center justify-center text-[10px] font-medium cursor-default transition-opacity hover:opacity-80"
                  style={{
                    backgroundColor: getCellColor(value ?? null),
                    color: value !== null && (value - min) / (max - min || 1) > 0.6 ? "#fff" : "#374151",
                    height: cellHeight,
                  }}
                  onMouseEnter={(e) => handleMouseEnter(e, rowIdx, colIdx)}
                  onMouseLeave={() => setTooltip(null)}
                >
                  {value != null ? value.toLocaleString("ja-JP") : "-"}
                </div>
              );
            })}
          </>
        ))}
      </div>

      {/* ツールチップ */}
      {tooltip && (
        <div
          className="absolute bg-gray-800 text-white text-xs px-2 py-1 rounded shadow pointer-events-none whitespace-nowrap z-10 -translate-x-1/2 -translate-y-full"
          style={{ left: tooltip.x, top: tooltip.y }}
        >
          {tooltip.content}
        </div>
      )}

      {/* カラースケール凡例 */}
      <div className="flex items-center gap-2 mt-3 text-[10px] text-gray-500">
        <span>{min.toLocaleString("ja-JP")}</span>
        <div
          className="h-2 w-24 rounded"
          style={{
            background: `linear-gradient(to right, ${colorScale[0]}, ${colorScale[1]})`,
          }}
        />
        <span>{max.toLocaleString("ja-JP")}</span>
      </div>
    </div>
  );
}
