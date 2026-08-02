"use client";

// ===================================================
// 周辺施設マップ
// 選んだ施設を中心に、半径内の施設を地図と一覧で可視化する。
// サービス種別・法人種別・規模でフィルタできる。
// ===================================================

import { Suspense, useCallback, useMemo, useState } from "react";

import ChartCard from "@/components/charts/ChartCard";
import FacilityMap from "@/components/charts/FacilityMap";
import LoadingSpinner from "@/components/common/LoadingSpinner";
import { useApi } from "@/hooks/useApi";
import { CORP_TYPES } from "@/lib/constants";
import { formatServiceName } from "@/lib/formatters";
import type { FacilitySearchResult } from "@/lib/types";

const DEBOUNCE_DELAY = 300;
const RADIUS_OPTIONS = [1, 3, 5, 10, 20, 30];

/** 中心施設 */
interface NearbyCenter {
  jigyosho_number: string;
  jigyosho_name: string;
  corp_name: string | null;
  prefecture: string | null;
  address: string | null;
  service_name: string | null;
  latitude: number;
  longitude: number;
}

/** 周辺施設 */
interface NearbyItem {
  jigyosho_number: string;
  jigyosho_name: string;
  corp_name: string | null;
  corp_type: string | null;
  prefecture: string | null;
  address: string | null;
  service_name: string | null;
  latitude: number;
  longitude: number;
  staff_total: number | null;
  capacity: number | null;
  turnover_rate: number | null;
  phone: string | null;
  distance_km: number;
}

interface NearbyResponse {
  center: NearbyCenter;
  radius_km: number;
  items: NearbyItem[];
  matched: number;
  truncated: boolean;
}

function FacilityMapContent() {
  const [query, setQuery] = useState("");
  const [debouncedQuery, setDebouncedQuery] = useState("");
  const [debounceTimer, setDebounceTimer] = useState<NodeJS.Timeout | null>(null);
  const [centerId, setCenterId] = useState<string | null>(null);
  const [radiusKm, setRadiusKm] = useState(3);
  const [serviceKeyword, setServiceKeyword] = useState("");
  const [corpType, setCorpType] = useState("");
  const [staffMin, setStaffMin] = useState<string>("");

  const handleQueryChange = useCallback(
    (value: string) => {
      setQuery(value);
      if (debounceTimer) clearTimeout(debounceTimer);
      const timer = setTimeout(() => setDebouncedQuery(value), DEBOUNCE_DELAY);
      setDebounceTimer(timer);
    },
    [debounceTimer]
  );

  // 中心施設を選ぶための検索
  const { data: searchResult, isLoading: searchLoading } = useApi<FacilitySearchResult>(
    debouncedQuery.length >= 2 ? "/api/facilities/search" : null,
    { q: debouncedQuery, per_page: 10 }
  );

  // 周辺検索
  const nearbyParams = useMemo(() => {
    const p: Record<string, string | number> = { center: centerId ?? "", radius_km: radiusKm };
    if (corpType) p.corp_type = corpType;
    if (serviceKeyword) p.service_name = serviceKeyword;
    const sm = Number(staffMin);
    if (staffMin !== "" && Number.isFinite(sm)) p.staff_min = sm;
    return p;
  }, [centerId, radiusKm, corpType, serviceKeyword, staffMin]);

  const { data: nearby, isLoading: nearbyLoading, error: nearbyError } =
    useApi<NearbyResponse>(centerId ? "/api/facilities/nearby" : null, nearbyParams);

  const centerMarker = useMemo(() => {
    if (!nearby?.center) return null;
    return {
      lat: nearby.center.latitude,
      lng: nearby.center.longitude,
      name: nearby.center.jigyosho_name,
      prefecture: nearby.center.prefecture ?? "",
      municipality: "",
      corp_name: nearby.center.corp_name ?? undefined,
      address: nearby.center.address ?? undefined,
      service_name: nearby.center.service_name ?? undefined,
    };
  }, [nearby]);

  const markers = useMemo(() => {
    if (!nearby?.items) return [];
    return nearby.items.map((f) => ({
      lat: f.latitude,
      lng: f.longitude,
      name: f.jigyosho_name,
      prefecture: f.prefecture ?? "",
      municipality: "",
      corp_name: f.corp_name ?? undefined,
      service_name: f.service_name ?? undefined,
      phone: f.phone ?? undefined,
      address: f.address ?? undefined,
      staff_total: f.staff_total ?? undefined,
      turnover_rate: f.turnover_rate ?? undefined,
      distance_km: f.distance_km,
    }));
  }, [nearby]);

  // 周辺のサービス種別構成（何が多いエリアか）
  const serviceMix = useMemo(() => {
    if (!nearby?.items) return [];
    const m = new Map<string, number>();
    for (const f of nearby.items) {
      const k = f.service_name ?? "不明";
      m.set(k, (m.get(k) ?? 0) + 1);
    }
    return Array.from(m.entries()).sort((a, b) => b[1] - a[1]).slice(0, 8);
  }, [nearby]);

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-gray-900">周辺施設マップ</h1>
        <p className="text-sm text-gray-500 mt-1">
          施設を選ぶと、その周辺にどんな施設があるかを地図で確認できます
        </p>
      </div>

      {/* 中心施設の検索 */}
      <ChartCard title="中心にする施設を選ぶ" subtitle="施設名・法人名・電話番号で検索（2文字以上）">
        <input
          type="text"
          value={query}
          onChange={(e) => handleQueryChange(e.target.value)}
          placeholder="例: 特別養護老人ホーム さくら"
          className="w-full rounded-lg border border-gray-200 px-3 py-2 text-sm focus:border-brand-500 focus:outline-none focus:ring-1 focus:ring-brand-500"
        />
        {searchLoading && <p className="mt-2 text-xs text-gray-400">検索中...</p>}
        {searchResult?.items && searchResult.items.length > 0 && (
          <ul className="mt-3 divide-y divide-gray-100 rounded-lg border border-gray-100">
            {searchResult.items.map((f) => (
              <li key={f.jigyosho_number}>
                <button
                  type="button"
                  onClick={() => setCenterId(f.jigyosho_number)}
                  className={`w-full px-3 py-2 text-left text-sm hover:bg-gray-50 ${
                    centerId === f.jigyosho_number ? "bg-orange-50" : ""
                  }`}
                >
                  <span className="font-medium text-gray-900">{f.jigyosho_name}</span>
                  <span className="ml-2 text-xs text-gray-500">
                    {f.corp_name} / {f.prefecture}
                  </span>
                  {f.latitude == null && (
                    <span className="ml-2 text-[10px] text-amber-600">位置情報なし</span>
                  )}
                </button>
              </li>
            ))}
          </ul>
        )}
        {debouncedQuery.length >= 2 && !searchLoading && searchResult?.items?.length === 0 && (
          <p className="mt-2 text-xs text-gray-400">該当する施設が見つかりませんでした</p>
        )}
      </ChartCard>

      {/* フィルタ */}
      {centerId && (
        <ChartCard title="絞り込み" subtitle="地図と一覧の両方に反映されます">
          <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
            <div>
              <label className="mb-1 block text-xs font-medium text-gray-500">半径</label>
              <div className="flex flex-wrap gap-1">
                {RADIUS_OPTIONS.map((r) => (
                  <button
                    key={r}
                    type="button"
                    onClick={() => setRadiusKm(r)}
                    className={`rounded-md px-2.5 py-1 text-xs font-medium ${
                      radiusKm === r
                        ? "bg-brand-600 text-white"
                        : "border border-gray-200 text-gray-600 hover:bg-gray-50"
                    }`}
                  >
                    {r}km
                  </button>
                ))}
              </div>
            </div>
            <div>
              <label className="mb-1 block text-xs font-medium text-gray-500">法人種別</label>
              <select
                value={corpType}
                onChange={(e) => setCorpType(e.target.value)}
                className="w-full rounded-lg border border-gray-200 px-3 py-2 text-sm"
              >
                <option value="">すべて</option>
                {CORP_TYPES.map((c) => (
                  <option key={c} value={c}>
                    {c}
                  </option>
                ))}
              </select>
            </div>
            <div>
              <label className="mb-1 block text-xs font-medium text-gray-500">
                サービス種別（部分一致）
              </label>
              <input
                type="text"
                value={serviceKeyword}
                onChange={(e) => setServiceKeyword(e.target.value)}
                placeholder="例: 訪問介護"
                className="w-full rounded-lg border border-gray-200 px-3 py-2 text-sm"
              />
            </div>
            <div>
              <label className="mb-1 block text-xs font-medium text-gray-500">従業者数の下限</label>
              <input
                type="number"
                min={0}
                value={staffMin}
                onChange={(e) => setStaffMin(e.target.value)}
                placeholder="指定なし"
                className="w-full rounded-lg border border-gray-200 px-3 py-2 text-sm"
              />
            </div>
          </div>
        </ChartCard>
      )}

      {/* 地図 */}
      {centerId && (
        <ChartCard
          title="周辺施設"
          subtitle={
            nearbyLoading
              ? "検索中..."
              : nearby
              ? `${nearby.center.jigyosho_name} から半径${nearby.radius_km}km に ${nearby.matched.toLocaleString("ja-JP")}件` +
                (nearby.truncated ? `（地図には近い順に${nearby.items.length}件を表示）` : "")
              : ""
          }
        >
          {nearbyError ? (
            <div className="py-10 text-center text-sm text-gray-500">
              {String(nearbyError).includes("位置情報")
                ? "この施設には位置情報が付与されていないため、周辺検索ができません。別の施設をお試しください。"
                : "周辺施設を取得できませんでした。"}
            </div>
          ) : nearbyLoading ? (
            <LoadingSpinner text="周辺施設を検索中..." />
          ) : (
            <FacilityMap
              markers={markers}
              center={centerMarker}
              radiusKm={nearby?.radius_km}
              height={520}
            />
          )}
          <p className="mt-2 text-[11px] text-gray-400">
            位置は国土交通省「位置参照情報」による町丁目レベルの座標です。同じ町丁目の施設は同じ位置に表示されます。
          </p>
        </ChartCard>
      )}

      {/* サービス構成 */}
      {centerId && !nearbyLoading && serviceMix.length > 0 && (
        <ChartCard title="周辺のサービス構成" subtitle="半径内に多いサービス種別">
          <div className="flex flex-wrap gap-2">
            {serviceMix.map(([name, cnt]) => (
              <span
                key={name}
                className="inline-flex items-center gap-1.5 rounded-full bg-gray-100 px-3 py-1 text-xs text-gray-700"
              >
                {formatServiceName(name)}
                <span className="font-semibold text-gray-900">{cnt}</span>
              </span>
            ))}
          </div>
        </ChartCard>
      )}

      {/* 一覧 */}
      {centerId && !nearbyLoading && nearby && nearby.items.length > 0 && (
        <ChartCard title="周辺施設一覧" subtitle="中心施設からの距離が近い順">
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-200 bg-gray-50/50">
                  <th className="px-3 py-2 text-left text-xs font-semibold text-gray-500">距離</th>
                  <th className="px-3 py-2 text-left text-xs font-semibold text-gray-500">施設名</th>
                  <th className="px-3 py-2 text-left text-xs font-semibold text-gray-500">法人名</th>
                  <th className="px-3 py-2 text-left text-xs font-semibold text-gray-500">サービス</th>
                  <th className="px-3 py-2 text-right text-xs font-semibold text-gray-500">従業者</th>
                  <th className="px-3 py-2 text-right text-xs font-semibold text-gray-500">定員</th>
                </tr>
              </thead>
              <tbody>
                {nearby.items.slice(0, 200).map((f, i) => (
                  <tr
                    key={f.jigyosho_number}
                    className={i % 2 === 0 ? "bg-white" : "bg-gray-50/40"}
                  >
                    <td className="px-3 py-2 tabular-nums font-medium text-orange-600">
                      {f.distance_km} km
                    </td>
                    <td className="px-3 py-2 text-gray-900">{f.jigyosho_name}</td>
                    <td className="px-3 py-2 text-gray-600">{f.corp_name ?? "-"}</td>
                    <td className="px-3 py-2 text-gray-600">
                      {f.service_name ? formatServiceName(f.service_name) : "-"}
                    </td>
                    <td className="px-3 py-2 text-right tabular-nums text-gray-700">
                      {f.staff_total != null ? Math.round(f.staff_total) : "-"}
                    </td>
                    <td className="px-3 py-2 text-right tabular-nums text-gray-700">
                      {f.capacity ? Math.round(f.capacity) : "-"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
            {nearby.items.length > 200 && (
              <p className="mt-2 text-xs text-gray-400">
                表示は近い順に200件までです（該当 {nearby.matched.toLocaleString("ja-JP")}件）。
                半径やフィルタで絞り込んでください。
              </p>
            )}
          </div>
        </ChartCard>
      )}

      {centerId && !nearbyLoading && nearby && nearby.items.length === 0 && !nearbyError && (
        <ChartCard title="周辺施設一覧">
          <p className="py-8 text-center text-sm text-gray-400">
            条件に合う施設が半径{radiusKm}km内に見つかりませんでした。半径を広げるか、絞り込みを緩めてください。
          </p>
        </ChartCard>
      )}
    </div>
  );
}

export default function FacilityMapPage() {
  return (
    <Suspense fallback={<LoadingSpinner text="読み込み中..." />}>
      <FacilityMapContent />
    </Suspense>
  );
}
