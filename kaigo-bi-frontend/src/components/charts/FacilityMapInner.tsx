"use client";

// ===================================================
// 施設分布マップ（内部コンポーネント）
// react-leaflet を使用、SSR無効で動的インポートされる
// ===================================================

import { MapContainer, TileLayer, CircleMarker, Circle, Popup, useMap } from "react-leaflet";
import "leaflet/dist/leaflet.css";
import L from "leaflet";
import { useEffect } from "react";

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
  /** 中心施設からの距離（km）。周辺検索時のみ */
  distance_km?: number;
}

interface FacilityMapInnerProps {
  /** マーカー配列 */
  markers: MapMarkerData[];
  /** マップの高さ（px） */
  height: number;
  /** 中心にする施設（周辺検索時）。指定するとピンを強調し、半径円を描く */
  center?: MapMarkerData | null;
  /** 半径円の大きさ（km） */
  radiusKm?: number;
}

/** center/radius が変わったら地図の表示範囲を合わせる */
function FitToCenter({ center, radiusKm }: { center?: MapMarkerData | null; radiusKm?: number }) {
  const map = useMap();
  useEffect(() => {
    if (!center) return;
    // 半径に応じたズーム: 1km≈14, 3km≈13, 10km≈11
    const r = radiusKm ?? 3;
    const zoom = r <= 1 ? 14 : r <= 3 ? 13 : r <= 5 ? 12 : r <= 10 ? 11 : r <= 20 ? 10 : 9;
    map.setView([center.lat, center.lng], zoom);
  }, [map, center, radiusKm]);
  return null;
}

/** マップリサイズ対応: コンテナサイズ変更時にタイル再描画 */
function MapResizeHandler() {
  const map = useMap();
  useEffect(() => {
    // 初回レンダリング後にinvalidateSizeを呼び出しタイルを確実に描画
    const timer = setTimeout(() => {
      map.invalidateSize();
    }, 100);
    return () => clearTimeout(timer);
  }, [map]);
  return null;
}

export default function FacilityMapInner({
  markers,
  height,
  center,
  radiusKm,
}: FacilityMapInnerProps) {
  return (
    <MapContainer
      center={center ? [center.lat, center.lng] : [36.5, 137.5]}
      zoom={center ? 13 : 5}
      style={{ height: `${height}px`, width: "100%" }}
      scrollWheelZoom={true}
    >
      <MapResizeHandler />
      <FitToCenter center={center} radiusKm={radiusKm} />
      <TileLayer
        attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
        url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
      />
      {center && radiusKm != null && (
        <Circle
          center={[center.lat, center.lng]}
          radius={radiusKm * 1000}
          pathOptions={{ color: "#f97316", fillColor: "#fb923c", fillOpacity: 0.06, weight: 1.5 }}
        />
      )}
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
              {m.distance_km != null && (
                <p className="text-orange-600 font-medium">中心から {m.distance_km} km</p>
              )}
            </div>
          </Popup>
        </CircleMarker>
      ))}
      {/* 中心施設は最後に描いて周辺マーカーの上に出す */}
      {center && (
        <CircleMarker
          center={[center.lat, center.lng]}
          radius={9}
          pathOptions={{ color: "#c2410c", fillColor: "#f97316", fillOpacity: 0.95, weight: 3 }}
        >
          <Popup>
            <div className="text-xs space-y-1 min-w-[200px]">
              <p className="inline-block px-1.5 py-0.5 bg-orange-100 text-orange-700 rounded text-[10px] font-bold">
                中心施設
              </p>
              <p className="font-bold text-sm text-gray-900">{center.name}</p>
              {center.corp_name && <p className="text-gray-600">{center.corp_name}</p>}
              {center.address && <p className="text-gray-400">{center.address}</p>}
            </div>
          </Popup>
        </CircleMarker>
      )}
    </MapContainer>
  );
}
