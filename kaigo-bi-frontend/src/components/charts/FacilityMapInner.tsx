"use client";

// ===================================================
// 施設分布マップ（内部コンポーネント）
// react-leaflet を使用、SSR無効で動的インポートされる
// ===================================================

import { MapContainer, TileLayer, CircleMarker, Popup } from "react-leaflet";
import "leaflet/dist/leaflet.css";
import L from "leaflet";

// Webpack環境でのLeafletアイコンパス問題を回避
delete (L.Icon.Default.prototype as any)._getIconUrl;

/** マーカーデータの型定義 */
export interface MapMarkerData {
  lat: number;
  lng: number;
  name: string;
  prefecture: string;
  municipality: string;
  corp_name?: string;
  service_name?: string;
  phone?: string;
  address?: string;
  staff_total?: number;
  turnover_rate?: number;
}

interface FacilityMapInnerProps {
  /** マーカー配列 */
  markers: MapMarkerData[];
  /** マップの高さ（px） */
  height: number;
}

export default function FacilityMapInner({ markers, height }: FacilityMapInnerProps) {
  return (
    <MapContainer
      center={[36.5, 137.5]}
      zoom={5}
      style={{ height: `${height}px`, width: "100%" }}
      scrollWheelZoom={true}
    >
      <TileLayer
        attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
        url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
      />
      {markers.map((m, i) => (
        <CircleMarker
          key={`${m.lat}-${m.lng}-${i}`}
          center={[m.lat, m.lng]}
          radius={3}
          pathOptions={{
            color: "#4f46e5",
            fillColor: "#6366f1",
            fillOpacity: 0.6,
            weight: 1,
          }}
        >
          <Popup>
            <div className="text-xs space-y-1 min-w-[200px]">
              <p className="font-bold text-sm text-gray-900">{m.name}</p>
              {m.corp_name && <p className="text-gray-600">{m.corp_name}</p>}
              {m.service_name && (
                <p><span className="inline-block px-1.5 py-0.5 bg-brand-50 text-brand-700 rounded text-[10px] font-medium">{m.service_name}</span></p>
              )}
              <p className="text-gray-500">{m.prefecture} {m.municipality}</p>
              {m.address && <p className="text-gray-400">{m.address}</p>}
              {m.phone && <p className="text-gray-500">TEL: {m.phone}</p>}
              <div className="flex gap-3 pt-1 border-t border-gray-100">
                {m.staff_total != null && <span>従業者: {m.staff_total}名</span>}
                {m.turnover_rate != null && <span>離職率: {(m.turnover_rate * 100).toFixed(1)}%</span>}
              </div>
            </div>
          </Popup>
        </CircleMarker>
      ))}
    </MapContainer>
  );
}
