"use client";

// ===================================================
// Page 02: 市場構造
// 地図（プレースホルダー）+ サービス種別 + 法人種別構成
// バックエンド個別エンドポイント対応版
// ===================================================

import { Suspense, useMemo } from "react";
import { useApi } from "@/hooks/useApi";
import { useFilters } from "@/hooks/useFilters";
import type { PrefectureSummary, ServiceSummary, CorpTypeSummary, ExternalPopulation, ExternalCareDemand } from "@/lib/types";
import { formatServiceName, formatCorpType } from "@/lib/formatters";
import BarChart from "@/components/charts/BarChart";
import LineChart from "@/components/charts/LineChart";
import DonutChart from "@/components/charts/DonutChart";
import ChartCard from "@/components/charts/ChartCard";
import ChoroplethMap from "@/components/charts/ChoroplethMap";
import FacilityMap from "@/components/charts/FacilityMap";
import FilterPanel from "@/components/filters/FilterPanel";
import ApiErrorBanner from "@/components/common/ApiErrorBanner";

function MarketContent() {
  const { filters, setFilters, toApiParams } = useFilters();
  const apiParams = toApiParams();

  // 都道府県別データ（/api/market/choropleth）
  const { data: byPrefecture, error: prefError, isLoading: prefLoading } = useApi<PrefectureSummary[]>(
    "/api/market/choropleth",
    apiParams
  );

  // サービス種別別データ（/api/market/by-service-bar）
  const { data: byService, error: serviceError, isLoading: serviceLoading } = useApi<ServiceSummary[]>(
    "/api/market/by-service-bar",
    apiParams
  );

  // 法人種別別データ（/api/market/corp-type-donut）
  const { data: byCorpType, error: corpError, isLoading: corpLoading } = useApi<CorpTypeSummary[]>(
    "/api/market/corp-type-donut",
    apiParams
  );

  // 施設マップ用データ（位置情報付き、最大500件）
  const { data: mapFacilities, isLoading: mapLoading } = useApi<{
    items: Array<{
      latitude?: number;
      longitude?: number;
      facility_name?: string;
      jigyosho_name?: string;
      prefecture?: string;
      municipality?: string;
      corp_name?: string;
      service_name?: string;
      phone?: string;
      address?: string;
      staff_total?: number;
      turnover_rate?: number;
      "事業所名"?: string;
      "都道府県名"?: string;
    }>;
  }>("/api/facilities/search", { ...apiParams, per_page: 500, page: 1 });

  // 外部API: 市区町村別人口データ（高齢化率）
  const selectedPrefecture = filters.prefectures.length === 1 ? filters.prefectures[0] : undefined;
  const { data: populationData, isLoading: popLoading } = useApi<ExternalPopulation[]>(
    "/api/external/population",
    { prefecture: selectedPrefecture }
  );

  // 外部API: 介護需要トレンド
  const { data: careDemandData, isLoading: careDemandLoading } = useApi<ExternalCareDemand[]>(
    "/api/external/care-demand",
    { prefecture: selectedPrefecture }
  );

  const apiError = prefError || serviceError || corpError;

  const isLoading = prefLoading || serviceLoading || corpLoading;
  const prefData = byPrefecture ?? [];
  const serviceData = byService ?? [];
  const corpTypeData = byCorpType ?? [];

  // 都道府県→施設数マップ（地図用）
  const prefectureMap = useMemo(
    () =>
      prefData.reduce<Record<string, number>>((acc, item) => {
        acc[item.prefecture] = item.facility_count;
        return acc;
      }, {}),
    [prefData]
  );

  // サービス種別上位12件（名前フォーマット適用、キー名の揺れに対応）
  const serviceTop12 = useMemo(
    () =>
      [...serviceData]
        .map((item) => {
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          const raw = item as any;
          return {
            service_name: formatServiceName(raw.service_name ?? raw.name ?? ""),
            facility_count: Number(raw.facility_count ?? raw.value ?? raw.count ?? 0),
          };
        })
        .filter((item) => item.service_name && item.facility_count > 0)
        .sort((a, b) => b.facility_count - a.facility_count)
        .slice(0, 12),
    [serviceData]
  );

  // 法人種別データ（ドーナツ用、表示名変換適用、キー名の揺れに対応）
  const donutData = useMemo(
    () =>
      corpTypeData.map((item) => {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const raw = item as any;
        return {
          name: formatCorpType(raw.corp_type ?? raw.name ?? ""),
          value: Number(raw.count ?? raw.value ?? 0),
        };
      }).filter((item) => item.name && item.value > 0),
    [corpTypeData]
  );

  // 高齢化率上位20市区町村（棒グラフ用）
  const elderlyRateTop20 = useMemo(() => {
    if (!populationData) return [];
    return [...populationData]
      .filter((p) => p.elderly_rate != null && p.elderly_rate > 0)
      .sort((a, b) => b.elderly_rate - a.elderly_rate)
      .slice(0, 20)
      .map((p) => ({
        municipality: p.municipality,
        elderly_rate: Math.round(p.elderly_rate * 1000) / 10,
      }));
  }, [populationData]);

  // 介護需要トレンド（折れ線グラフ用）
  // バックエンドは day_service_offices + home_care_offices を施設数、
  // day_service_users + home_care_users を利用者数として集計
  const careDemandChartData = useMemo(() => {
    if (!careDemandData) return [];
    return [...careDemandData]
      .filter((d) => d.fiscal_year != null)
      .sort((a, b) => String(a.fiscal_year).localeCompare(String(b.fiscal_year)))
      .map((d) => ({
        year: String(d.fiscal_year),
        facility_count: (d.day_service_offices ?? 0) + (d.home_care_offices ?? 0) + (d.nursing_home_count ?? 0) + (d.health_facility_count ?? 0),
        user_count: (d.day_service_users ?? 0) + (d.home_care_users ?? 0),
      }));
  }, [careDemandData]);

  // 施設マップ用マーカーデータ
  const mapMarkers = useMemo(() => {
    if (!mapFacilities?.items) return [];
    return mapFacilities.items
      .filter(
        (f) =>
          f.latitude != null &&
          f.longitude != null &&
          f.latitude !== 0 &&
          f.longitude !== 0
      )
      .map((f) => ({
        lat: Number(f.latitude),
        lng: Number(f.longitude),
        name: f.jigyosho_name || f.facility_name || f["事業所名"] || "",
        prefecture: f.prefecture || f["都道府県名"] || "",
        municipality: f.municipality || "",
        corp_name: f.corp_name || "",
        service_name: f.service_name || "",
        phone: f.phone || "",
        address: f.address || "",
        staff_total: f.staff_total,
        turnover_rate: f.turnover_rate,
      }));
  }, [mapFacilities]);

  return (
    <div className="space-y-6">
      {/* ページタイトル */}
      <div>
        <h1 className="text-xl font-bold text-gray-900">市場構造</h1>
        <p className="text-sm text-gray-500 mt-1">
          地域・サービス・法人別の市場構造分析
        </p>
      </div>

      {/* APIエラーバナー */}
      <ApiErrorBanner error={apiError} />

      {/* フィルタパネル */}
      <FilterPanel
        filters={filters}
        onChange={setFilters}
        compact
        visibleFilters={["prefectures", "serviceCodes", "corpTypes", "keyword"]}
      />

      {/* 地図セクション */}
      <ChartCard
        title="都道府県別施設分布"
        subtitle="施設数のヒートマップ表示"
        loading={isLoading}
      >
        <ChoroplethMap data={prefectureMap} />
      </ChartCard>

      {/* 施設分布マップ（Leaflet） */}
      <ChartCard
        title="施設分布マップ"
        subtitle="施設の地理的分布（最大500件表示）"
        loading={mapLoading}
      >
        <FacilityMap markers={mapMarkers} height={500} />
      </ChartCard>

      {/* 都道府県別施設数（棒グラフ代替） */}
      <ChartCard
        title="都道府県別施設数"
        subtitle="全47都道府県の施設数分布"
        loading={isLoading}
      >
        <BarChart
          data={prefData}
          xKey="prefecture"
          yKey="facility_count"
          color="#6366f1"
          height={400}
        />
      </ChartCard>

      {/* 下段: サービス種別 + 法人種別 */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* サービス種別別施設数 */}
        <ChartCard
          title="サービス種別別施設数（Top 12）"
          subtitle="主要サービス種別の施設数比較"
          loading={isLoading}
        >
          <BarChart
            data={serviceTop12}
            xKey="service_name"
            yKey="facility_count"
            color="#4f46e5"
            horizontal
            height={380}
          />
        </ChartCard>

        {/* 法人種別構成 */}
        <ChartCard
          title="法人種別構成"
          subtitle="運営法人の種別比率"
          loading={isLoading}
        >
          <DonutChart
            data={donutData}
            nameKey="name"
            valueKey="value"
            centerLabel="施設"
            height={380}
          />
        </ChartCard>
      </div>

      {/* 外部統計データセクション */}
      <div className="border-t border-gray-200 pt-6 mt-2">
        <h2 className="text-lg font-semibold text-gray-800 mb-1">外部統計データ</h2>
        <p className="text-sm text-gray-500 mb-4">
          政府統計に基づく人口動態・介護需要の分析
          {selectedPrefecture ? `（${selectedPrefecture}）` : "（都道府県フィルタで1つ選択すると地域別データが表示されます）"}
        </p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* 高齢化率 市区町村ランキング */}
        <ChartCard
          title="高齢化率ランキング（Top 20）"
          subtitle="市区町村別の高齢者人口比率"
          loading={popLoading}
        >
          {elderlyRateTop20.length > 0 ? (
            <>
              <BarChart
                data={elderlyRateTop20}
                xKey="municipality"
                yKey="elderly_rate"
                color="#dc2626"
                horizontal
                tooltipFormatter={(v) => `${v}%`}
                height={420}
              />
              <p className="text-xs text-gray-400 mt-2 px-1">
                データソース: 総務省統計局
              </p>
            </>
          ) : (
            <div className="flex items-center justify-center h-[300px] text-gray-400 text-sm">
              {popLoading ? "読み込み中..." : "都道府県フィルタを1つ選択してください"}
            </div>
          )}
        </ChartCard>

        {/* 介護需要トレンド */}
        <ChartCard
          title="介護需要トレンド"
          subtitle="施設数・利用者数の年度別推移"
          loading={careDemandLoading}
        >
          {careDemandChartData.length > 0 ? (
            <>
              <LineChart
                data={careDemandChartData}
                xKey="year"
                series={[
                  { dataKey: "facility_count", name: "施設数", color: "#6366f1" },
                  { dataKey: "user_count", name: "利用者数", color: "#f59e0b" },
                ]}
                tooltipFormatter={(v) => v.toLocaleString("ja-JP")}
                height={380}
              />
              <p className="text-xs text-gray-400 mt-2 px-1">
                データソース: 厚生労働省 介護給付費等実態統計
              </p>
            </>
          ) : (
            <div className="flex items-center justify-center h-[300px] text-gray-400 text-sm">
              {careDemandLoading ? "読み込み中..." : "データがありません"}
            </div>
          )}
        </ChartCard>
      </div>
    </div>
  );
}

export default function MarketPage() {
  return (
    <Suspense fallback={<div className="text-gray-400 text-sm p-8">読み込み中...</div>}>
      <MarketContent />
    </Suspense>
  );
}
