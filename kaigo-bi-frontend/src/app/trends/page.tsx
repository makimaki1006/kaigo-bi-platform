"use client";

// ===================================================
// Page: トレンド分析（介護版Trends T5）
// 時系列トレンドを複数指標で可視化
// 実API接続版
// ===================================================

import { Suspense, useMemo } from "react";
import { useApi } from "@/hooks/useApi";
import { useFilters } from "@/hooks/useFilters";
import KpiCard from "@/components/data-display/KpiCard";
import KpiCardGrid from "@/components/data-display/KpiCardGrid";
import LineChart from "@/components/charts/LineChart";
import BarChart from "@/components/charts/BarChart";
import ChartCard from "@/components/charts/ChartCard";
import FilterPanel from "@/components/filters/FilterPanel";
import DataPendingPlaceholder from "@/components/common/DataPendingPlaceholder";
import { KpiGridSkeleton } from "@/components/common/LoadingSpinner";
import ApiErrorBanner from "@/components/common/ApiErrorBanner";
import type {
  GrowthKpi,
  EstablishmentYearCount,
  BusinessYearsBin,
} from "@/lib/types";

/** 外部統計（求人倍率）のレスポンス型 */
interface HiringDifficultyData {
  national_ratio: number | null;
  care_ratio: number | null;
  source_year: string | null;
}

/** KPIアイコン: トレンド */
const IconTrend = (
  <svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
    <polyline points="22 7 13.5 15.5 8.5 10.5 2 17" /><polyline points="16 7 22 7 22 13" />
  </svg>
);

/** KPIアイコン: 時計 */
const IconClock = (
  <svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
    <circle cx="12" cy="12" r="10" /><polyline points="12 6 12 12 16 14" />
  </svg>
);

/** KPIアイコン: 求人 */
const IconHiring = (
  <svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
    <path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2" /><circle cx="9" cy="7" r="4" /><path d="M22 21v-2a4 4 0 0 0-3-3.87" /><path d="M16 3.13a4 4 0 0 1 0 7.75" />
  </svg>
);

function TrendsContent() {
  const { filters, setFilters, toApiParams } = useFilters();
  const apiParams = toApiParams();

  // API呼び出し
  const { data: kpi, error: kpiError, isLoading: kpiLoading } = useApi<GrowthKpi>(
    "/api/growth/kpi",
    apiParams
  );

  const { data: establishmentTrend, error: trendError, isLoading: trendLoading } = useApi<EstablishmentYearCount[]>(
    "/api/growth/establishment-trend",
    apiParams
  );

  const { data: yearsDistribution, error: yearsError, isLoading: yearsLoading } = useApi<BusinessYearsBin[]>(
    "/api/growth/years-distribution",
    apiParams
  );

  // 外部統計（求人倍率）- エラーでも他のデータは表示
  const { data: hiringData } = useApi<HiringDifficultyData>(
    "/api/external/hiring-difficulty"
  );

  const apiError = kpiError || trendError || yearsError;

  // 設立年推移を折れ線グラフ用に変換
  const trendLineData = useMemo(() => {
    if (!establishmentTrend) return [];
    return establishmentTrend.map((d) => ({
      year: String(d.year),
      count: d.count,
    }));
  }, [establishmentTrend]);

  // 直近年の新規施設数
  const latestYearCount = useMemo(() => {
    if (!establishmentTrend || establishmentTrend.length === 0) return null;
    return establishmentTrend[establishmentTrend.length - 1].count;
  }, [establishmentTrend]);

  // 直近年の年ラベル
  const latestYear = useMemo(() => {
    if (!establishmentTrend || establishmentTrend.length === 0) return "";
    return String(establishmentTrend[establishmentTrend.length - 1].year);
  }, [establishmentTrend]);

  return (
    <div className="space-y-6">
      {/* ページタイトル */}
      <div>
        <h1 className="text-xl font-bold text-gray-900">トレンド分析</h1>
        <p className="text-sm text-gray-500 mt-1">
          介護市場の時系列変化
        </p>
      </div>

      {/* データソースバナー */}
      <div className="rounded-lg bg-blue-50 border border-blue-200 px-4 py-3">
        <p className="text-xs text-blue-700 leading-relaxed">
          <span className="font-semibold">データソース:</span>{" "}
          厚生労働省「介護サービス情報公表システム」掲載データより算出。
          時系列データは事業開始日から算出。外部統計（求人倍率等）は最新年度のスナップショットです。
        </p>
      </div>

      {/* APIエラーバナー */}
      <ApiErrorBanner error={apiError} />

      {/* フィルタパネル */}
      <FilterPanel
        filters={filters}
        onChange={setFilters}
        compact
        visibleFilters={["prefectures", "serviceCodes", "keyword"]}
      />

      {/* KPIカード */}
      {kpiLoading ? (
        <KpiGridSkeleton count={3} />
      ) : (
        <KpiCardGrid>
          <KpiCard
            label={`${latestYear}年 新規施設数`}
            value={latestYearCount}
            format="number"
            icon={IconTrend}
            accentColor="bg-emerald-500"
            subtitle="直近年の新設数"
          />
          <KpiCard
            label="平均事業年数"
            value={kpi?.avg_years_in_business ?? null}
            format="decimal"
            icon={IconClock}
            accentColor="bg-blue-500"
            subtitle="事業開始日から算出"
          />
          <KpiCard
            label="介護求人倍率"
            value={hiringData?.care_ratio ?? null}
            format="decimal"
            icon={IconHiring}
            accentColor="bg-amber-500"
            subtitle={hiringData?.source_year ? `${hiringData.source_year}年度` : "外部統計"}
          />
        </KpiCardGrid>
      )}

      {/* チャートエリア */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* 1. 年別新規施設数の推移（折れ線グラフ） */}
        <ChartCard
          title="年別新規施設数の推移"
          subtitle="事業開始日ベースの時系列推移"
          loading={trendLoading}
          className="lg:col-span-2"
        >
          {trendLineData.length > 0 ? (
            <LineChart
              data={trendLineData}
              xKey="year"
              series={[
                { dataKey: "count", name: "新規施設数", color: "#6366f1" },
              ]}
              tooltipFormatter={(v) => `${v.toLocaleString("ja-JP")}施設`}
              height={350}
            />
          ) : (
            <DataPendingPlaceholder
              message="データなし"
              description="フィルタ条件に一致する施設がありません"
              height={350}
            />
          )}
        </ChartCard>

        {/* 2. 事業年数分布（棒グラフ） */}
        <ChartCard
          title="事業年数分布"
          subtitle="施設の事業継続年数のヒストグラム"
          loading={yearsLoading}
        >
          {yearsDistribution && yearsDistribution.length > 0 ? (
            <BarChart
              data={yearsDistribution}
              xKey="range"
              yKey="count"
              color="#4f46e5"
              tooltipFormatter={(v) => `${v.toLocaleString("ja-JP")}施設`}
              height={300}
            />
          ) : (
            <DataPendingPlaceholder
              message="データなし"
              description="フィルタ条件に一致する施設がありません"
              height={300}
            />
          )}
        </ChartCard>

        {/* 3. 外部統計サマリー */}
        <ChartCard
          title="外部統計サマリー"
          subtitle="求人倍率等の外部指標（スナップショット）"
        >
          {hiringData ? (
            <div className="space-y-4 py-4">
              <div className="grid grid-cols-2 gap-4">
                <div className="rounded-lg bg-gray-50 p-4 text-center">
                  <p className="text-xs text-gray-500 mb-1">全国求人倍率</p>
                  <p className="text-2xl font-bold text-gray-900 tabular-nums">
                    {hiringData.national_ratio != null
                      ? hiringData.national_ratio.toFixed(2)
                      : "-"}
                  </p>
                  <p className="text-xs text-gray-400 mt-1">倍</p>
                </div>
                <div className="rounded-lg bg-gray-50 p-4 text-center">
                  <p className="text-xs text-gray-500 mb-1">介護職求人倍率</p>
                  <p className="text-2xl font-bold text-amber-600 tabular-nums">
                    {hiringData.care_ratio != null
                      ? hiringData.care_ratio.toFixed(2)
                      : "-"}
                  </p>
                  <p className="text-xs text-gray-400 mt-1">倍</p>
                </div>
              </div>
              <p className="text-xs text-gray-400 text-center">
                {hiringData.source_year
                  ? `出典: ${hiringData.source_year}年度 厚生労働省統計`
                  : "出典: 厚生労働省統計"}
              </p>
            </div>
          ) : (
            <DataPendingPlaceholder
              message="外部統計未取得"
              description="求人倍率データは外部APIから取得されます"
              height={200}
            />
          )}
        </ChartCard>
      </div>

      {/* 注記 */}
      <div className="rounded-lg bg-gray-50 border border-gray-200 px-4 py-3">
        <p className="text-xs text-gray-500 leading-relaxed">
          時系列データは事業開始日から算出しています。外部統計（求人倍率等）は最新年度のスナップショットであり、
          時系列の推移ではありません。フィルタは介護サービス情報公表システムのデータにのみ適用されます。
        </p>
      </div>
    </div>
  );
}

export default function TrendsPage() {
  return (
    <Suspense fallback={<div className="text-gray-400 text-sm p-8">読み込み中...</div>}>
      <TrendsContent />
    </Suspense>
  );
}
