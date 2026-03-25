"use client";

// ===================================================
// 都道府県別日本地図（地理的配置）
// 47都道府県を地理的な位置関係に基づいて配置
// 外部パッケージ不要のSVGベース実装
// ===================================================

import { useState, useMemo, useCallback } from "react";

/** グリッド上の都道府県配置定義 */
interface PrefGrid {
  name: string;
  /** グリッドX座標（列） */
  gx: number;
  /** グリッドY座標（行） */
  gy: number;
}

/**
 * 日本地図 地理的配置
 * 実際の地理的位置をできるだけ忠実に反映した配置
 * 列数14 x 行数18のグリッド
 */
const PREF_GRID: PrefGrid[] = [
  // 北海道（右上）
  { name: "北海道", gx: 12, gy: 0 },

  // 東北（右寄り上部）
  { name: "青森県", gx: 11, gy: 2 },
  { name: "岩手県", gx: 12, gy: 3 },
  { name: "秋田県", gx: 11, gy: 4 },
  { name: "宮城県", gx: 12, gy: 4 },
  { name: "山形県", gx: 11, gy: 5 },
  { name: "福島県", gx: 11, gy: 6 },

  // 関東（右寄り中部）
  { name: "茨城県", gx: 12, gy: 6 },
  { name: "栃木県", gx: 11, gy: 7 },
  { name: "群馬県", gx: 10, gy: 7 },
  { name: "埼玉県", gx: 11, gy: 8 },
  { name: "千葉県", gx: 12, gy: 8 },
  { name: "東京都", gx: 11, gy: 9 },
  { name: "神奈川県", gx: 12, gy: 9 },

  // 中部（中央）
  { name: "新潟県", gx: 10, gy: 5 },
  { name: "富山県", gx: 8, gy: 6 },
  { name: "石川県", gx: 7, gy: 6 },
  { name: "福井県", gx: 7, gy: 7 },
  { name: "山梨県", gx: 10, gy: 9 },
  { name: "長野県", gx: 9, gy: 7 },
  { name: "岐阜県", gx: 8, gy: 8 },
  { name: "静岡県", gx: 10, gy: 10 },
  { name: "愛知県", gx: 9, gy: 9 },

  // 近畿（中央やや左）
  { name: "三重県", gx: 8, gy: 10 },
  { name: "滋賀県", gx: 8, gy: 9 },
  { name: "京都府", gx: 7, gy: 9 },
  { name: "大阪府", gx: 7, gy: 10 },
  { name: "兵庫県", gx: 6, gy: 10 },
  { name: "奈良県", gx: 8, gy: 11 },
  { name: "和歌山県", gx: 7, gy: 11 },

  // 中国（左寄り中部）
  { name: "鳥取県", gx: 5, gy: 9 },
  { name: "島根県", gx: 4, gy: 9 },
  { name: "岡山県", gx: 5, gy: 10 },
  { name: "広島県", gx: 4, gy: 10 },
  { name: "山口県", gx: 3, gy: 10 },

  // 四国（中央下部）
  { name: "徳島県", gx: 7, gy: 12 },
  { name: "香川県", gx: 6, gy: 11 },
  { name: "愛媛県", gx: 5, gy: 12 },
  { name: "高知県", gx: 6, gy: 13 },

  // 九州（左下）
  { name: "福岡県", gx: 2, gy: 11 },
  { name: "佐賀県", gx: 1, gy: 11 },
  { name: "長崎県", gx: 0, gy: 12 },
  { name: "熊本県", gx: 2, gy: 12 },
  { name: "大分県", gx: 3, gy: 11 },
  { name: "宮崎県", gx: 3, gy: 13 },
  { name: "鹿児島県", gx: 2, gy: 13 },
  { name: "沖縄県", gx: 0, gy: 16 },
];

/** グリッドの最大列・最大行を計算 */
const MAX_GX = Math.max(...PREF_GRID.map((p) => p.gx));
const MAX_GY = Math.max(...PREF_GRID.map((p) => p.gy));

/** 2色間の線形補間 */
function interpolateColor(
  color1: [number, number, number],
  color2: [number, number, number],
  t: number
): string {
  const r = Math.round(color1[0] + (color2[0] - color1[0]) * t);
  const g = Math.round(color1[1] + (color2[1] - color1[1]) * t);
  const b = Math.round(color1[2] + (color2[2] - color1[2]) * t);
  return `rgb(${r},${g},${b})`;
}

/** HEXカラーをRGB配列に変換 */
function hexToRgb(hex: string): [number, number, number] {
  const h = hex.replace("#", "");
  return [
    parseInt(h.substring(0, 2), 16),
    parseInt(h.substring(2, 4), 16),
    parseInt(h.substring(4, 6), 16),
  ];
}

interface ChoroplethMapProps {
  /** 都道府県名 -> 値のマップ */
  data?: Record<string, number>;
  /** 値のラベル（ツールチップ用） */
  metricLabel?: string;
  /** カラースケール [最小色, 最大色] のHEXコード */
  colorScale?: [string, string];
  /** 都道府県クリック時のコールバック */
  onPrefectureClick?: (prefecture: string, value: number | undefined) => void;
}

export default function ChoroplethMap({
  data,
  metricLabel = "",
  colorScale = ["#dbeafe", "#1e40af"],
  onPrefectureClick,
}: ChoroplethMapProps) {
  const [hoveredPref, setHoveredPref] = useState<string | null>(null);
  const [tooltipPos, setTooltipPos] = useState<{ x: number; y: number }>({ x: 0, y: 0 });

  // 値の最小・最大を計算
  const { minVal, maxVal, colorMin, colorMax } = useMemo(() => {
    const values = data ? Object.values(data).filter((v) => v != null && !isNaN(v)) : [];
    const min = values.length > 0 ? Math.min(...values) : 0;
    const max = values.length > 0 ? Math.max(...values) : 1;
    return {
      minVal: min,
      maxVal: max,
      colorMin: hexToRgb(colorScale[0]),
      colorMax: hexToRgb(colorScale[1]),
    };
  }, [data, colorScale]);

  // 都道府県の色を計算
  const getColor = useCallback(
    (prefName: string): string => {
      if (!data || data[prefName] == null) {
        return "#e5e7eb"; // グレー（データなし）
      }
      const value = data[prefName];
      const range = maxVal - minVal;
      const t = range > 0 ? (value - minVal) / range : 0.5;
      return interpolateColor(colorMin, colorMax, t);
    },
    [data, minVal, maxVal, colorMin, colorMax]
  );

  // SVG座標計算
  const cellSize = 38;
  const cellGap = 2;
  const padding = 12;
  const totalWidth = (MAX_GX + 1) * (cellSize + cellGap) - cellGap + padding * 2;
  const legendHeight = 50;
  const totalHeight = (MAX_GY + 1) * (cellSize + cellGap) - cellGap + padding * 2 + legendHeight;

  // ツールチップ表示用のマウス位置追跡
  const handleMouseMove = useCallback(
    (prefName: string, e: React.MouseEvent<SVGRectElement>) => {
      const svg = e.currentTarget.ownerSVGElement;
      if (!svg) return;
      const rect = svg.getBoundingClientRect();
      setTooltipPos({
        x: e.clientX - rect.left,
        y: e.clientY - rect.top,
      });
      setHoveredPref(prefName);
    },
    []
  );

  const handleMouseLeave = useCallback(() => {
    setHoveredPref(null);
  }, []);

  const handleClick = useCallback(
    (prefName: string) => {
      if (onPrefectureClick) {
        onPrefectureClick(prefName, data?.[prefName]);
      }
    },
    [onPrefectureClick, data]
  );

  // ツールチップ表示内容
  const tooltipContent = useMemo(() => {
    if (!hoveredPref) return null;
    const value = data?.[hoveredPref];
    return {
      name: hoveredPref,
      value: value != null ? value.toLocaleString("ja-JP") : "データなし",
      hasData: value != null,
    };
  }, [hoveredPref, data]);

  // 都道府県名を短縮（セル内に表示するため）
  const shortenName = (name: string): string => {
    const base = name.replace(/[県府都道]$/g, "");
    return base.length <= 3 ? base : base.substring(0, 3);
  };

  // テキストの色を背景色に基づいて決定
  const getTextColor = (prefName: string): string => {
    if (!data || data[prefName] == null) return "#9ca3af";
    const value = data[prefName];
    const range = maxVal - minVal;
    const t = range > 0 ? (value - minVal) / range : 0.5;
    // 暗い背景の場合は白、明るい背景の場合は暗い色
    return t > 0.4 ? "#ffffff" : "#374151";
  };

  return (
    <div className="relative w-full" role="img" aria-label="都道府県別日本地図">
      {/* 地図表示の注記 */}
      <div className="flex items-center gap-1.5 mb-2 px-1">
        <svg className="w-3.5 h-3.5 text-gray-400 flex-shrink-0" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
          <circle cx="12" cy="12" r="10" />
          <line x1="12" x2="12" y1="16" y2="12" />
          <line x1="12" x2="12.01" y1="8" y2="8" />
        </svg>
        <span className="text-[11px] text-gray-400">
          地理的配置: 各都道府県を実際の位置関係に基づいて配置しています
        </span>
      </div>
      <svg
        viewBox={`0 0 ${totalWidth} ${totalHeight}`}
        className="w-full h-auto"
        style={{ maxHeight: "700px" }}
      >
        {/* 都道府県セル */}
        {PREF_GRID.map((pref) => {
          const x = padding + pref.gx * (cellSize + cellGap);
          const y = padding + pref.gy * (cellSize + cellGap);
          const fill = getColor(pref.name);
          const isHovered = hoveredPref === pref.name;
          const hasData = data != null && data[pref.name] != null;

          return (
            <g key={pref.name}>
              <rect
                x={x}
                y={y}
                width={cellSize}
                height={cellSize}
                rx={4}
                fill={fill}
                stroke={isHovered ? "#1f2937" : "#ffffff"}
                strokeWidth={isHovered ? 2.5 : 1}
                className="transition-all duration-150"
                style={{
                  cursor: onPrefectureClick ? "pointer" : "default",
                  opacity: hoveredPref && !isHovered ? 0.65 : 1,
                  filter: isHovered ? "brightness(1.1)" : "none",
                }}
                onMouseMove={(e) => handleMouseMove(pref.name, e)}
                onMouseLeave={handleMouseLeave}
                onClick={() => handleClick(pref.name)}
                role="button"
                tabIndex={0}
                aria-label={`${pref.name}: ${hasData ? data![pref.name].toLocaleString("ja-JP") : "データなし"}`}
              />
              <text
                x={x + cellSize / 2}
                y={y + cellSize / 2}
                textAnchor="middle"
                dominantBaseline="central"
                className="pointer-events-none select-none"
                fill={getTextColor(pref.name)}
                fontSize={10}
                fontWeight={500}
              >
                {shortenName(pref.name)}
              </text>
            </g>
          );
        })}

        {/* 沖縄への点線（地理的な飛びを表現） */}
        <line
          x1={padding + 0 * (cellSize + cellGap) + cellSize / 2}
          y1={padding + 13 * (cellSize + cellGap) + cellSize + 4}
          x2={padding + 0 * (cellSize + cellGap) + cellSize / 2}
          y2={padding + 16 * (cellSize + cellGap) - 4}
          stroke="#d1d5db"
          strokeWidth={1}
          strokeDasharray="3 3"
        />

        {/* 凡例（グラデーションバー） */}
        <defs>
          <linearGradient id="choropleth-legend-gradient" x1="0" y1="0" x2="1" y2="0">
            <stop offset="0%" stopColor={colorScale[0]} />
            <stop offset="100%" stopColor={colorScale[1]} />
          </linearGradient>
        </defs>

        {/* 凡例グループ */}
        <g transform={`translate(${padding}, ${totalHeight - legendHeight + 5})`}>
          {/* グラデーションバー */}
          <rect
            x={0}
            y={0}
            width={180}
            height={12}
            rx={3}
            fill="url(#choropleth-legend-gradient)"
          />
          {/* 最小値ラベル */}
          <text
            x={0}
            y={28}
            fontSize={10}
            fill="#6b7280"
          >
            {minVal.toLocaleString("ja-JP")}
          </text>
          {/* 最大値ラベル */}
          <text
            x={180}
            y={28}
            textAnchor="end"
            fontSize={10}
            fill="#6b7280"
          >
            {maxVal.toLocaleString("ja-JP")}
          </text>
          {/* メトリクスラベル */}
          {metricLabel && (
            <text
              x={90}
              y={28}
              textAnchor="middle"
              fontSize={10}
              fill="#9ca3af"
            >
              {metricLabel}
            </text>
          )}
          {/* データなし凡例 */}
          <rect
            x={200}
            y={0}
            width={12}
            height={12}
            rx={2}
            fill="#e5e7eb"
          />
          <text
            x={218}
            y={10}
            fontSize={10}
            fill="#9ca3af"
            dominantBaseline="middle"
          >
            データなし
          </text>
        </g>
      </svg>

      {/* ツールチップ（SVG外のHTML要素） */}
      {tooltipContent && (
        <div
          className="absolute pointer-events-none z-50 bg-gray-900 text-white px-3 py-2 rounded-lg shadow-xl text-xs whitespace-nowrap"
          style={{
            left: tooltipPos.x + 12,
            top: tooltipPos.y - 40,
            transform: "translateX(-50%)",
          }}
        >
          <p className="text-gray-300 mb-0.5">{tooltipContent.name}</p>
          <p className="font-semibold tabular-nums">
            {tooltipContent.value}
            {metricLabel && tooltipContent.hasData ? ` ${metricLabel}` : ""}
          </p>
        </div>
      )}
    </div>
  );
}
