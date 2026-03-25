"use client";

// ===================================================
// 施設分布マップ（ラッパーコンポーネント）
// SSR無効で FacilityMapInner を動的インポート
// ===================================================

import dynamic from "next/dynamic";
import type { MapMarkerData } from "./FacilityMapInner";

const FacilityMapInner = dynamic(() => import("./FacilityMapInner"), {
  ssr: false,
  loading: () => (
    <div
      className="bg-gray-100 rounded-lg animate-pulse flex items-center justify-center"
      style={{ height: 500 }}
    >
      <span className="text-gray-400 text-sm">マップを読み込み中...</span>
    </div>
  ),
});

interface FacilityMapProps {
  /** マーカー配列 */
  markers: MapMarkerData[];
  /** マップの高さ（px） */
  height?: number;
}

export default function FacilityMap({ markers, height = 500 }: FacilityMapProps) {
  if (markers.length === 0) {
    return (
      <div
        className="bg-gray-50 rounded-lg flex items-center justify-center border border-dashed border-gray-200"
        style={{ height }}
      >
        <p className="text-gray-400 text-sm">
          位置情報のある施設データがありません
        </p>
      </div>
    );
  }

  return <FacilityMapInner markers={markers} height={height} />;
}
