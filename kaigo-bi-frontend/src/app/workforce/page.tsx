"use client";

// ===================================================
// Page 05: 人材分析
// 離職率、常勤比率、採用率など人材に関する分析ページ
// 実API連携版: /api/workforce/kpi, /api/workforce/turnover-distribution,
//              /api/workforce/by-prefecture, /api/workforce/by-size
// ===================================================

import { Suspense, useMemo } from "react";
import { useApi } from "@/hooks/useApi";
import { useFilters } from "@/hooks/useFilters";
import { useServiceConfig } from "@/lib/service-config";
import type {
  WorkforceKpi,
  TurnoverBin,
  SizeGroupTurnover,
  WorkforcePrefectureData,
  ExperienceBin,
  ExperienceTurnoverPoint,
  ExternalJobOpenings,
  ExternalLaborTrend,
} from "@/lib/types";
import KpiCard from "@/components/data-display/KpiCard";
import KpiCardGrid from "@/components/data-display/KpiCardGrid";
import BarChart from "@/components/charts/BarChart";
import LineChart from "@/components/charts/LineChart";
import StackedBarChart from "@/components/charts/StackedBarChart";
import ScatterChart from "@/components/charts/ScatterChart";
import ChartCard from "@/components/charts/ChartCard";
import FilterPanel from "@/components/filters/FilterPanel";
import DataPendingPlaceholder from "@/components/common/DataPendingPlaceholder";
import ApiErrorBanner from "@/components/common/ApiErrorBanner";

/** KPIアイコン: 離職率 */
const IconTurnover = (
  <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={1.5} aria-hidden="true">
    <path strokeLinecap="round" strokeLinejoin="round" d="M2.25 6L9 12.75l4.286-4.286a11.948 11.948 0 014.306 6.43l.776 2.898m0 0l3.182-5.511m-3.182 5.51l-5.511-3.181" />
  </svg>
);

/** KPIアイコン: 採用率 */
const IconHiring = (
  <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={1.5} aria-hidden="true">
    <path strokeLinecap="round" strokeLinejoin="round" d="M2.25 18L9 11.25l4.306 4.307a11.95 11.95 0 015.814-5.519l2.74-1.22m0 0l-5.94-2.28m5.94 2.28l-2.28 5.941" />
  </svg>
);

/** KPIアイコン: 常勤比率 */
const IconFulltime = (
  <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={1.5} aria-hidden="true">
    <path strokeLinecap="round" strokeLinejoin="round" d="M20.25 14.15v4.25c0 1.094-.787 2.036-1.872 2.18-2.087.277-4.216.42-6.378.42s-4.291-.143-6.378-.42c-1.085-.144-1.872-1.086-1.872-2.18v-4.25m16.5 0a2.18 2.18 0 00.75-1.661V8.706c0-1.081-.768-2.015-1.837-2.175a48.114 48.114 0 00-3.413-.387m4.5 8.006c-.194.165-.42.295-.673.38A23.978 23.978 0 0112 15.75c-2.648 0-5.195-.429-7.577-1.22a2.016 2.016 0 01-.673-.38m0 0A2.18 2.18 0 013 12.489V8.706c0-1.081.768-2.015 1.837-2.175a48.111 48.111 0 013.413-.387m7.5 0V5.25A2.25 2.25 0 0013.5 3h-3a2.25 2.25 0 00-2.25 2.25v.894m7.5 0a48.667 48.667 0 00-7.5 0M12 12.75h.008v.008H12v-.008z" />
  </svg>
);

/** KPIアイコン: 経験者割合 */
const IconExperience = (
  <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={1.5} aria-hidden="true">
    <path strokeLinecap="round" strokeLinejoin="round" d="M11.48 3.499a.562.562 0 011.04 0l2.125 5.111a.563.563 0 00.475.345l5.518.442c.499.04.701.663.321.988l-4.204 3.602a.563.563 0 00-.182.557l1.285 5.385a.562.562 0 01-.84.61l-4.725-2.885a.563.563 0 00-.586 0L6.982 20.54a.562.562 0 01-.84-.61l1.285-5.386a.562.562 0 00-.182-.557l-4.204-3.602a.563.563 0 01.321-.988l5.518-.442a.563.563 0 00.475-.345L11.48 3.5z" />
  </svg>
);

/** サービス種別非対応KPIの案内ボックス */
function UnavailableNotice({ message }: { message: string | null }) {
  if (!message) return null;
  return (
    <div className="text-sm text-gray-400 p-4 bg-gray-50 rounded-lg border border-gray-100">
      {message}
    </div>
  );
}

function WorkforceContent() {
  const { filters, setFilters, toApiParams } = useFilters();
  const apiParams = toApiParams();
  const serviceConfig = useServiceConfig(filters.serviceCodes);

  // KPIデータ取得（workforce専用エンドポイント）
  const { data: kpi, error: kpiError, isLoading: kpiLoading } = useApi<WorkforceKpi>(
    "/api/workforce/kpi",
    apiParams
  );

  // 離職率分布
  const { data: turnoverDist, error: turnoverDistError, isLoading: turnoverDistLoading } = useApi<TurnoverBin[]>(
    "/api/workforce/turnover-distribution",
    apiParams
  );

  // 都道府県別データ取得
  const { data: byPrefecture, error: prefError, isLoading: prefLoading } = useApi<WorkforcePrefectureData[]>(
    "/api/workforce/by-prefecture",
    apiParams
  );

  // 規模別離職率
  const { data: bySize, error: sizeError, isLoading: sizeLoading } = useApi<SizeGroupTurnover[]>(
    "/api/workforce/by-size",
    apiParams
  );

  // 経験10年以上割合分布
  const { data: experienceDist, error: expDistError, isLoading: experienceDistLoading } = useApi<ExperienceBin[]>(
    "/api/workforce/experience-distribution",
    apiParams
  );

  // 経験者割合 vs 離職率散布図
  const { data: expTurnoverData, error: expTurnoverError, isLoading: expTurnoverLoading } = useApi<ExperienceTurnoverPoint[]>(
    "/api/workforce/experience-vs-turnover",
    apiParams
  );

  // 外部API: 有効求人倍率推移
  const selectedPrefecture = filters.prefectures.length === 1 ? filters.prefectures[0] : undefined;
  const { data: jobOpeningsData, isLoading: jobOpeningsLoading } = useApi<ExternalJobOpenings[]>(
    "/api/external/job-openings",
    { prefecture: selectedPrefecture }
  );

  // 外部API: 労働市場トレンド
  const { data: laborTrendsData, isLoading: laborTrendsLoading } = useApi<ExternalLaborTrend[]>(
    "/api/external/labor-trends",
    { prefecture: selectedPrefecture }
  );

  // 有効求人倍率推移（折れ線グラフ用）
  const jobOpeningsChartData = useMemo(() => {
    if (!jobOpeningsData) return [];
    return [...jobOpeningsData]
      .sort((a, b) => a.fiscal_year - b.fiscal_year)
      .map((d) => ({
        year: String(d.fiscal_year),
        job_openings_ratio: d.job_openings_ratio,
      }));
  }, [jobOpeningsData]);

  // 労働市場トレンド（折れ線グラフ用）
  const laborTrendsChartData = useMemo(() => {
    if (!laborTrendsData) return [];
    return [...laborTrendsData]
      .sort((a, b) => a.fiscal_year - b.fiscal_year)
      .map((d) => ({
        year: String(d.fiscal_year),
        turnover_rate: Math.round(d.turnover_rate * 1000) / 10,
      }));
  }, [laborTrendsData]);

  const apiError = kpiError || turnoverDistError || prefError || sizeError || expDistError || expTurnoverError;

  const isLoading = kpiLoading || prefLoading || turnoverDistLoading || sizeLoading;

  // 都道府県別離職率ランキング（TOP15）
  const prefTurnoverTop15 = useMemo(() => {
    if (!byPrefecture) return [];
    return [...byPrefecture]
      .filter((p) => p.avg_turnover_rate != null && p.avg_turnover_rate > 0)
      .sort((a, b) => b.avg_turnover_rate - a.avg_turnover_rate)
      .slice(0, 15)
      .map((p) => ({
        prefecture: p.prefecture,
        turnover_rate: Math.round(p.avg_turnover_rate * 1000) / 10,
      }));
  }, [byPrefecture]);

  // 散布図データ（都道府県別: 施設数 vs 離職率）
  const scatterData = useMemo(() => {
    if (!byPrefecture) return [];
    return byPrefecture
      .filter((p) => p.avg_turnover_rate != null && p.facility_count != null)
      .map((p) => ({
        name: p.prefecture,
        turnover_rate: Math.round(p.avg_turnover_rate * 1000) / 10,
        facility_count: p.facility_count,
      }));
  }, [byPrefecture]);

  // 規模別データ整形（バックエンドのsize_categoryをxKeyに使用）
  const sizeData = useMemo(() => {
    if (!bySize) return [];
    return bySize.map((item) => ({
      size_group: item.size_category,
      turnover_rate: Math.round(item.avg_turnover_rate * 1000) / 10,
    }));
  }, [bySize]);

  // 規模別 常勤/非常勤比率データ
  const sizeFulltimeData = useMemo(() => {
    if (!bySize) return [];
    return bySize.map((item) => ({
      size_group: item.size_category,
      fulltime_pct: Math.round((item.avg_fulltime_ratio || 0) * 1000) / 10,
      parttime_pct: Math.round((1 - (item.avg_fulltime_ratio || 0)) * 1000) / 10,
    }));
  }, [bySize]);

  return (
    <div className="space-y-6">
      {/* ページタイトル */}
      <div>
        <h1 className="text-heading-lg text-gray-900">人材分析</h1>
        <p className="text-body-md text-gray-500 mt-1">
          離職率・採用率・常勤比率など、人材に関する分析
        </p>
      </div>

      {/* バイアス補正注意カード */}
      <div className="bg-blue-50 border border-blue-200 rounded-lg p-3 text-sm text-blue-700">
        <span className="font-medium">&#9888;&#65039; データの読み方: </span>
        サービス種別間の単純比較は注意が必要です。施設規模・法人種別・地域分布が異なるため、
        同一条件での比較には「都道府県」「サービス種別」フィルタをご活用ください。
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
      <KpiCardGrid>
        {serviceConfig.isAvailable("avg_turnover") ? (
          <KpiCard
            label="平均離職率"
            value={kpi?.avg_turnover_rate != null ? kpi.avg_turnover_rate : null}
            format="percent"
            icon={IconTurnover}
            subtitle={kpi?.avg_turnover_rate != null ? "全施設平均（実データ）" : "データ準備中"}
            loading={kpiLoading}
            accentColor="bg-red-500"
            tooltip="前年度の退職者数 / (従業者数+退職者数)"
          />
        ) : (
          <UnavailableNotice message={serviceConfig.reason("avg_turnover")} />
        )}
        <KpiCard
          label="平均採用率"
          value={kpi?.avg_hire_rate != null ? kpi.avg_hire_rate : null}
          format="percent"
          icon={IconHiring}
          subtitle={kpi?.avg_hire_rate != null ? "実データ" : "データ準備中"}
          loading={kpiLoading}
          accentColor="bg-emerald-500"
        />
        {serviceConfig.isAvailable("avg_fulltime_ratio") ? (
          <KpiCard
            label="平均常勤比率"
            value={kpi?.avg_fulltime_ratio != null ? kpi.avg_fulltime_ratio : null}
            format="percent"
            icon={IconFulltime}
            subtitle={kpi?.avg_fulltime_ratio != null ? "常勤 / 全従業者（実データ）" : "データ準備中"}
            loading={kpiLoading}
            accentColor="bg-brand-500"
            tooltip="常勤従業者 / 全従業者"
          />
        ) : (
          <UnavailableNotice message={serviceConfig.reason("avg_fulltime_ratio")} />
        )}
        {serviceConfig.isAvailable("experience_ratio") ? (
          <KpiCard
            label="経験10年以上割合"
            value={kpi?.avg_experience_10yr_ratio != null ? kpi.avg_experience_10yr_ratio : null}
            format="percent"
            icon={IconExperience}
            subtitle={kpi?.avg_experience_10yr_ratio != null ? "実データ" : "データ準備中"}
            loading={kpiLoading}
            accentColor="bg-amber-500"
          />
        ) : (
          <UnavailableNotice message={serviceConfig.reason("experience_ratio")} />
        )}
      </KpiCardGrid>

      {/* チャートエリア */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* 1. 離職率の分布ヒストグラム */}
        {serviceConfig.isAvailable("avg_turnover") ? (
          <ChartCard
            title="離職率の分布"
            subtitle="施設数ベースのヒストグラム"
            loading={turnoverDistLoading}
          >
            {turnoverDist && turnoverDist.length > 0 ? (
              <BarChart
                data={turnoverDist}
                xKey="range"
                yKey="count"
                color="#dc2626"
                tooltipFormatter={(v) => `${v.toLocaleString("ja-JP")}施設`}
                height={300}
              />
            ) : (
              <DataPendingPlaceholder
                message="離職率分布データ準備中"
                description="フィルタ条件に一致するデータがありません"
                height={300}
              />
            )}
          </ChartCard>
        ) : (
          <ChartCard title="離職率の分布" subtitle="施設数ベースのヒストグラム">
            <UnavailableNotice message={serviceConfig.reason("avg_turnover")} />
          </ChartCard>
        )}

        {/* 2. 常勤/非常勤構成（従業者規模別） */}
        <ChartCard
          title="常勤/非常勤比率（従業者規模別）"
          subtitle="規模別の常勤・非常勤構成比"
          loading={sizeLoading}
        >
          {sizeFulltimeData.length > 0 ? (
            <StackedBarChart
              data={sizeFulltimeData}
              xKey="size_group"
              series={[
                { dataKey: "fulltime_pct", name: "常勤 (%)", color: "#4f46e5" },
                { dataKey: "parttime_pct", name: "非常勤 (%)", color: "#a5b4fc" },
              ]}
              height={300}
            />
          ) : (
            <DataPendingPlaceholder
              message="常勤/非常勤構成データ準備中"
              description="従業者規模別の常勤比率はデータ取得後に表示されます"
              height={300}
            />
          )}
        </ChartCard>

        {/* 3. 離職率 vs 施設数（散布図） */}
        <ChartCard
          title="離職率 vs 施設数"
          subtitle="都道府県別の相関（APIデータ）"
          loading={prefLoading}
        >
          {scatterData.length > 0 ? (
            <ScatterChart
              data={scatterData}
              xKey="facility_count"
              yKey="turnover_rate"
              xLabel="施設数"
              yLabel="離職率(%)"
              nameKey="name"
              color="#7c3aed"
              height={300}
            />
          ) : (
            <DataPendingPlaceholder
              message="散布図データなし"
              description="都道府県別データのフィルタ結果が空です"
              height={300}
            />
          )}
        </ChartCard>

        {/* 4. 都道府県別離職率ランキング */}
        <ChartCard
          title="都道府県別離職率ランキング（Top 15）"
          subtitle="APIデータから算出"
          loading={prefLoading}
        >
          {prefTurnoverTop15.length > 0 ? (
            <BarChart
              data={prefTurnoverTop15}
              xKey="prefecture"
              yKey="turnover_rate"
              color="#d97706"
              horizontal
              tooltipFormatter={(v) => `${v}%`}
              height={420}
            />
          ) : (
            <DataPendingPlaceholder
              message="ランキングデータなし"
              description="フィルタ条件に一致する都道府県がありません"
              height={300}
            />
          )}
        </ChartCard>

        {/* 5. 従業者規模区分別の離職率比較 */}
        <ChartCard
          title="従業者規模別 離職率"
          subtitle="規模が大きいほど離職率が低い傾向"
          loading={sizeLoading}
          className="lg:col-span-2"
        >
          {sizeData.length > 0 ? (
            <BarChart
              data={sizeData}
              xKey="size_group"
              yKey="turnover_rate"
              color="#4f46e5"
              tooltipFormatter={(v) => `${v}%`}
              height={280}
            />
          ) : (
            <DataPendingPlaceholder
              message="規模別データ準備中"
              description="従業者規模別の離職率データがありません"
              height={280}
            />
          )}
        </ChartCard>

        {/* 6. 経験10年以上割合の分布 */}
        {serviceConfig.isAvailable("experience_ratio") ? (
          <ChartCard
            title="経験10年以上割合の分布"
            subtitle="施設数ベースのヒストグラム"
            loading={experienceDistLoading}
          >
            {experienceDist && experienceDist.length > 0 ? (
              <BarChart
                data={experienceDist}
                xKey="range"
                yKey="count"
                color="#d97706"
                tooltipFormatter={(v) => `${v.toLocaleString("ja-JP")}施設`}
                height={300}
              />
            ) : (
              <DataPendingPlaceholder
                message="経験者割合データ準備中"
                description="経験10年以上割合の分布はフルデータ取得後に表示されます"
                height={300}
              />
            )}
          </ChartCard>
        ) : (
          <ChartCard title="経験10年以上割合の分布" subtitle="施設数ベースのヒストグラム">
            <UnavailableNotice message={serviceConfig.reason("experience_ratio")} />
          </ChartCard>
        )}

        {/* 7. 経験者割合 vs 離職率散布図 */}
        {serviceConfig.isAvailable("experience_ratio") && serviceConfig.isAvailable("avg_turnover") ? (
          <ChartCard
            title="経験者割合 vs 離職率"
            subtitle="経験10年以上割合と離職率の相関"
            loading={expTurnoverLoading}
          >
            {expTurnoverData && expTurnoverData.length > 0 ? (
              <ScatterChart
                data={expTurnoverData}
                xKey="avg_experience_ratio"
                yKey="avg_turnover_rate"
                xLabel="経験10年以上割合(%)"
                yLabel="離職率(%)"
                nameKey="prefecture"
                color="#059669"
                height={300}
              />
            ) : (
              <DataPendingPlaceholder
                message="散布図データ準備中"
                description="経験者割合と離職率の相関はフルデータ取得後に表示されます"
                height={300}
              />
            )}
          </ChartCard>
        ) : (
          <ChartCard title="経験者割合 vs 離職率" subtitle="経験10年以上割合と離職率の相関">
            <UnavailableNotice message={serviceConfig.reason("experience_ratio") || serviceConfig.reason("avg_turnover")} />
          </ChartCard>
        )}
      </div>

      {/* 外部統計: 労働市場データ */}
      <div className="border-t border-gray-200 pt-6 mt-2">
        <h2 className="text-lg font-semibold text-gray-800 mb-1">外部統計: 労働市場データ</h2>
        <p className="text-sm text-gray-500 mb-4">
          政府統計に基づく求人・労働市場の動向
          {selectedPrefecture ? `（${selectedPrefecture}）` : ""}
        </p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* 有効求人倍率推移 */}
        <ChartCard
          title="有効求人倍率推移"
          subtitle="年度別の有効求人倍率の変遷"
          loading={jobOpeningsLoading}
        >
          {jobOpeningsChartData.length > 0 ? (
            <>
              <LineChart
                data={jobOpeningsChartData}
                xKey="year"
                series={[
                  { dataKey: "job_openings_ratio", name: "有効求人倍率", color: "#6366f1" },
                ]}
                tooltipFormatter={(v) => `${v.toFixed(2)}倍`}
                height={320}
              />
              <p className="text-xs text-gray-400 mt-2 px-1">
                データソース: 厚生労働省 職業安定業務統計
              </p>
            </>
          ) : (
            <div className="flex items-center justify-center h-[300px] text-gray-400 text-sm">
              {jobOpeningsLoading ? "読み込み中..." : "データがありません"}
            </div>
          )}
        </ChartCard>

        {/* 労働市場トレンド（離職率推移） */}
        <ChartCard
          title="労働市場トレンド（離職率推移）"
          subtitle="全産業の離職率年度推移"
          loading={laborTrendsLoading}
        >
          {laborTrendsChartData.length > 0 ? (
            <>
              <LineChart
                data={laborTrendsChartData}
                xKey="year"
                series={[
                  { dataKey: "turnover_rate", name: "離職率(%)", color: "#dc2626" },
                ]}
                tooltipFormatter={(v) => `${v}%`}
                height={320}
              />
              <p className="text-xs text-gray-400 mt-2 px-1">
                データソース: 厚生労働省 雇用動向調査
              </p>
            </>
          ) : (
            <div className="flex items-center justify-center h-[300px] text-gray-400 text-sm">
              {laborTrendsLoading ? "読み込み中..." : "データがありません"}
            </div>
          )}
        </ChartCard>
      </div>
    </div>
  );
}

export default function WorkforcePage() {
  return (
    <Suspense fallback={<div className="text-gray-400 text-sm p-8">読み込み中...</div>}>
      <WorkforceContent />
    </Suspense>
  );
}
