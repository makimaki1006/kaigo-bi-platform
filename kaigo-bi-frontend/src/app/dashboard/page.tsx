"use client";

// ===================================================
// Page 01: ダッシュボード
// KPI概要 + 都道府県別 + サービス種別別チャート
// Lucideアイコン対応版
// ===================================================

import { Suspense, useMemo } from "react";
import { useApi } from "@/hooks/useApi";
import { useFilters } from "@/hooks/useFilters";
import type { DashboardKpi, DashboardKpiExtended, PrefectureSummary, ServiceSummary, CorpTypeSummary } from "@/lib/types";
import { formatServiceName } from "@/lib/formatters";
import { CHART_COLORS } from "@/lib/constants";
import KpiCard from "@/components/data-display/KpiCard";
import KpiCardGrid from "@/components/data-display/KpiCardGrid";
import BarChart from "@/components/charts/BarChart";
import DonutChart from "@/components/charts/DonutChart";
import ChartCard from "@/components/charts/ChartCard";
import FilterPanel from "@/components/filters/FilterPanel";
import ApiErrorBanner from "@/components/common/ApiErrorBanner";

/** KPIアイコン: 施設（ビルアイコン） */
const IconFacilities = (
  <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={1.5} aria-hidden="true">
    <path strokeLinecap="round" strokeLinejoin="round" d="M2.25 21h19.5M3.75 3v18m4.5-18v18m4.5-18v18m4.5-18v18M5.25 3h13.5M5.25 21h13.5M9 6.75h1.5m-1.5 3h1.5m-1.5 3h1.5m3-6H15m-1.5 3H15m-1.5 3H15M9 21v-3.375c0-.621.504-1.125 1.125-1.125h3.75c.621 0 1.125.504 1.125 1.125V21" />
  </svg>
);

/** KPIアイコン: 従業者（ユーザーグループ） */
const IconStaff = (
  <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={1.5} aria-hidden="true">
    <path strokeLinecap="round" strokeLinejoin="round" d="M15 19.128a9.38 9.38 0 002.625.372 9.337 9.337 0 004.121-.952 4.125 4.125 0 00-7.533-2.493M15 19.128v-.003c0-1.113-.285-2.16-.786-3.07M15 19.128v.106A12.318 12.318 0 018.624 21c-2.331 0-4.512-.645-6.374-1.766l-.001-.109a6.375 6.375 0 0111.964-3.07M12 6.375a3.375 3.375 0 11-6.75 0 3.375 3.375 0 016.75 0zm8.25 2.25a2.625 2.625 0 11-5.25 0 2.625 2.625 0 015.25 0z" />
  </svg>
);

/** KPIアイコン: 法人数（ビジネスビル） */
const IconCorps = (
  <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={1.5} aria-hidden="true">
    <path strokeLinecap="round" strokeLinejoin="round" d="M3.75 21h16.5M4.5 3h15M5.25 3v18m13.5-18v18M9 6.75h1.5m-1.5 3h1.5m-1.5 3h1.5m3-6H15m-1.5 3H15m-1.5 3H15M9 21v-3.375c0-.621.504-1.125 1.125-1.125h3.75c.621 0 1.125.504 1.125 1.125V21" />
  </svg>
);

/** KPIアイコン: 離職率（トレンドダウン） */
const IconTurnover = (
  <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={1.5} aria-hidden="true">
    <path strokeLinecap="round" strokeLinejoin="round" d="M3 13.125C3 12.504 3.504 12 4.125 12h2.25c.621 0 1.125.504 1.125 1.125v6.75C7.5 20.496 6.996 21 6.375 21h-2.25A1.125 1.125 0 013 19.875v-6.75zM9.75 8.625c0-.621.504-1.125 1.125-1.125h2.25c.621 0 1.125.504 1.125 1.125v11.25c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 01-1.125-1.125V8.625zM16.5 4.125c0-.621.504-1.125 1.125-1.125h2.25C20.496 3 21 3.504 21 4.125v15.75c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 01-1.125-1.125V4.125z" />
  </svg>
);

function DashboardContent() {
  const { filters, setFilters, toApiParams } = useFilters();
  const apiParams = toApiParams();

  // KPIデータ取得（拡張版: 品質スコア・加算取得数含む）
  const { data: kpi, error: kpiError, isLoading: kpiLoading } = useApi<DashboardKpiExtended>(
    "/api/dashboard/kpi",
    apiParams
  );

  // 都道府県別データ取得
  const { data: byPrefecture, error: prefError, isLoading: prefLoading } = useApi<PrefectureSummary[]>(
    "/api/dashboard/by-prefecture",
    apiParams
  );

  // サービス種別別データ取得
  const { data: byService, error: serviceError, isLoading: serviceLoading } = useApi<ServiceSummary[]>(
    "/api/dashboard/by-service",
    apiParams
  );

  // 法人種別別データ取得（ドーナツチャート用）
  const { data: byCorpType, error: corpTypeError, isLoading: corpTypeLoading } = useApi<CorpTypeSummary[]>(
    "/api/market/corp-type-donut",
    apiParams
  );

  const apiError = kpiError || prefError || serviceError || corpTypeError;

  const isLoading = kpiLoading || prefLoading || serviceLoading || corpTypeLoading;
  const prefData = byPrefecture ?? [];
  const serviceData = byService ?? [];

  // 都道府県別データ（上位15件）
  const prefectureTop15 = useMemo(
    () => [...prefData].sort((a, b) => b.facility_count - a.facility_count).slice(0, 15),
    [prefData]
  );

  // サービス種別別データ（上位10件）、サービス名フォーマット適用
  const serviceTop10 = useMemo(
    () =>
      [...serviceData]
        .sort((a, b) => b.facility_count - a.facility_count)
        .slice(0, 10)
        .map((item) => ({
          ...item,
          service_name: formatServiceName(item.service_name),
        })),
    [serviceData]
  );

  return (
    <div className="space-y-6">
      {/* ページタイトル */}
      <div>
        <h1 className="text-heading-lg text-gray-900">ダッシュボード</h1>
        <p className="text-body-md text-gray-500 mt-1">
          介護・福祉施設の全体概要
        </p>
      </div>

      {/* APIエラーバナー */}
      <ApiErrorBanner error={apiError} />

      {/* フィルタパネル */}
      <FilterPanel
        filters={filters}
        onChange={setFilters}
        compact
        visibleFilters={["prefectures", "corpTypes", "keyword"]}
      />

      {/* KPIカード 4枚 */}
      <KpiCardGrid>
        <KpiCard
          label="総施設数"
          value={kpi?.total_facilities}
          format="number"
          icon={IconFacilities}
          subtitle="登録施設数"
          loading={kpiLoading}
          accentColor="bg-brand-500"
        />
        <KpiCard
          label="法人数"
          value={kpi?.total_corps ?? null}
          format="number"
          icon={IconCorps}
          subtitle="ユニーク法人番号数"
          loading={kpiLoading}
          accentColor="bg-emerald-500"
        />
        <KpiCard
          label="平均従業者数"
          value={kpi?.avg_staff}
          format="decimal"
          icon={IconStaff}
          subtitle="1施設あたり"
          loading={kpiLoading}
          accentColor="bg-sky-500"
        />
        <KpiCard
          label="平均離職率"
          value={kpi?.avg_turnover_rate}
          format="percent"
          icon={IconTurnover}
          subtitle="全施設平均"
          loading={kpiLoading}
          accentColor="bg-amber-500"
          tooltip="前年度の退職者数 / (従業者数+退職者数)"
        />
      </KpiCardGrid>

      {/* チャートエリア */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* 都道府県別施設数 */}
        <ChartCard
          title="都道府県別施設数（Top 15）"
          subtitle="施設数の多い上位15都道府県"
          loading={isLoading}
        >
          <BarChart
            data={prefectureTop15}
            xKey="prefecture"
            yKey="facility_count"
            color={CHART_COLORS[0]}
            height={360}
            unit="施設"
            onBarClick={(d) => {
              const pref = d.prefecture;
              if (pref) {
                setFilters({ prefectures: [String(pref)] });
              }
            }}
          />
          <p className="text-xs text-gray-400 mt-1">棒をクリックすると都道府県フィルタが適用されます</p>
        </ChartCard>

        {/* サービス種別別施設数 */}
        <ChartCard
          title="サービス種別別施設数（Top 10）"
          subtitle="施設数の多い上位10サービス種別"
          loading={isLoading}
        >
          <BarChart
            data={serviceTop10}
            xKey="service_name"
            yKey="facility_count"
            color={CHART_COLORS[1]}
            horizontal
            height={360}
            unit="施設"
          />
        </ChartCard>

        {/* 法人種別ドーナツチャート */}
        <ChartCard
          title="法人種別構成"
          subtitle="法人種別ごとの施設数割合"
          loading={corpTypeLoading}
        >
          <DonutChart
            data={byCorpType ?? []}
            nameKey="corp_type"
            valueKey="count"
            centerLabel="法人数"
            height={360}
            unit="法人"
          />
        </ChartCard>

        {/* サービス種別サマリーテーブル */}
        <ChartCard
          title="サービス種別概要"
          subtitle={`上位${serviceTop10.length}種別の施設数・従業者数`}
          loading={serviceLoading}
        >
          {serviceTop10.length > 0 ? (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-gray-200 text-left">
                    <th className="px-3 py-2 text-gray-500 font-medium">サービス種別</th>
                    <th className="px-3 py-2 text-gray-500 font-medium text-right">施設数</th>
                    <th className="px-3 py-2 text-gray-500 font-medium text-right">総従業者数</th>
                  </tr>
                </thead>
                <tbody>
                  {serviceTop10.map((s) => (
                    <tr key={s.service_code} className="border-b border-gray-100 hover:bg-gray-50">
                      <td className="px-3 py-2 text-gray-700">{s.service_name}</td>
                      <td className="px-3 py-2 text-right tabular-nums text-gray-900 font-medium">
                        {s.facility_count.toLocaleString("ja-JP")}
                      </td>
                      <td className="px-3 py-2 text-right tabular-nums text-gray-500">
                        {s.total_staff.toLocaleString("ja-JP")}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="flex items-center justify-center h-[300px] text-gray-400 text-sm">
              データがありません
            </div>
          )}
        </ChartCard>
      </div>
    </div>
  );
}

export default function DashboardPage() {
  return (
    <Suspense fallback={<div className="text-gray-400 text-sm p-8">読み込み中...</div>}>
      <DashboardContent />
    </Suspense>
  );
}
