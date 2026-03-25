"use client";

// ===================================================
// Page 10: 賃金分析
// 職種別賃金、都道府県別賃金
// 実API連携版: /api/salary/kpi, /api/salary/by-job-type,
//              /api/salary/by-prefecture
// ===================================================

import { Suspense, useMemo, useState } from "react";
import { useApi } from "@/hooks/useApi";
import { useFilters } from "@/hooks/useFilters";
import type { SalaryKpi, JobTypeWage, PrefectureJobWage, ExternalSalaryBenchmark, ExternalWageHistory } from "@/lib/types";
import { formatManYen } from "@/lib/formatters";

/** 外部統計データの都道府県別賃金 */
interface ExternalPrefStats {
  prefecture: string;
  min_wage: number | null;
  avg_monthly_wage: number | null;
  job_offers_rate: number | null;
  unemployment_rate: number | null;
}
import KpiCard from "@/components/data-display/KpiCard";
import KpiCardGrid from "@/components/data-display/KpiCardGrid";
import BarChart from "@/components/charts/BarChart";
import LineChart from "@/components/charts/LineChart";
import ScatterChart from "@/components/charts/ScatterChart";
import ChartCard from "@/components/charts/ChartCard";
import FilterPanel from "@/components/filters/FilterPanel";

/** 介護業界の賃金補正係数（全産業平均からの推定用） */
const CARE_INDUSTRY_WAGE_FACTOR = 0.75;
/** 介護事業所の概算総数（厚生労働省データに基づく推定値） */
const TOTAL_FACILITIES_ESTIMATE = 223107;
import DataPendingPlaceholder from "@/components/common/DataPendingPlaceholder";
import ApiErrorBanner from "@/components/common/ApiErrorBanner";
import { CHART_COLORS } from "@/lib/constants";

/** 万円フォーマットのツールチップ */
function tooltipManYen(v: number): string {
  return `${(v / 10000).toFixed(1)}万円（${v.toLocaleString("ja-JP")}円）`;
}

function SalaryContent() {
  const { filters, setFilters, toApiParams } = useFilters();
  const apiParams = toApiParams();

  // 賃金KPI取得
  const { data: kpi, error: kpiError, isLoading: kpiLoading } = useApi<SalaryKpi>(
    "/api/salary/kpi",
    apiParams
  );

  // 職種別賃金
  const { data: jobTypeWages, error: jobTypeError, isLoading: jobTypeLoading } = useApi<JobTypeWage[]>(
    "/api/salary/by-job-type",
    apiParams
  );

  // 都道府県別賃金
  const { data: prefWages, error: prefError, isLoading: prefLoading } = useApi<PrefectureJobWage[]>(
    "/api/salary/by-prefecture",
    apiParams
  );

  // 外部統計データ（施設固有の賃金データがない場合の代替）
  const { data: externalPrefStats } = useApi<ExternalPrefStats[]>(
    "/api/external/prefecture-stats"
  );

  // 外部API: 求人給与ベンチマーク
  const selectedPrefecture = filters.prefectures.length === 1 ? filters.prefectures[0] : undefined;
  const { data: salaryBenchmark, isLoading: benchmarkLoading } = useApi<ExternalSalaryBenchmark[]>(
    "/api/external/salary-benchmark",
    { prefecture: selectedPrefecture }
  );

  // 外部API: 最低賃金推移
  const { data: wageHistory, isLoading: wageHistoryLoading } = useApi<ExternalWageHistory[]>(
    "/api/external/wage-history",
    { prefecture: selectedPrefecture }
  );

  // 外部データから都道府県別平均月給チャートデータを生成
  const externalWageData = useMemo(() => {
    if (!externalPrefStats) return [];
    return externalPrefStats
      .filter((p) => p.avg_monthly_wage != null)
      .map((p) => ({
        prefecture: p.prefecture,
        avg_wage: Math.round((p.avg_monthly_wage || 0) * 1000 * CARE_INDUSTRY_WAGE_FACTOR),
      }))
      .sort((a, b) => b.avg_wage - a.avg_wage)
      .slice(0, 20);
  }, [externalPrefStats]);

  // 散布図データ: 都道府県別の平均賃金 vs データ件数（施設集中度）
  const scatterData = useMemo(() => {
    if (!prefWages || prefWages.length === 0) return [];
    return prefWages
      .filter((p) => p.avg_salary > 0 && p.count > 0)
      .map((p) => ({
        name: p.prefecture,
        avg_salary: Math.round(p.avg_salary),
        count: p.count,
      }));
  }, [prefWages]);

  // 賃金分布ヒストグラムデータ: 賃金帯別の職種数
  const histogramData = useMemo(() => {
    if (!jobTypeWages || jobTypeWages.length === 0) return [];
    // 賃金帯ごとに集計
    const bins = [
      { label: "~15万円", min: 0, max: 150000 },
      { label: "15~20万円", min: 150000, max: 200000 },
      { label: "20~25万円", min: 200000, max: 250000 },
      { label: "25~30万円", min: 250000, max: 300000 },
      { label: "30~35万円", min: 300000, max: 350000 },
      { label: "35~40万円", min: 350000, max: 400000 },
      { label: "40万円~", min: 400000, max: Infinity },
    ];
    return bins.map((bin) => ({
      range: bin.label,
      count: jobTypeWages.filter(
        (w) => w.avg_salary >= bin.min && w.avg_salary < bin.max
      ).length,
    })).filter((b) => b.count > 0);
  }, [jobTypeWages]);

  // データ充填率の計算
  const dataCountInfo = useMemo(() => {
    if (!kpi) return null;
    const dataCount = kpi.data_count;
    // data_count がキャッシュに含まれている場合
    if (dataCount != null && dataCount > 0) {
      // 総施設数は約223,107件（概算）
      const totalEstimate = TOTAL_FACILITIES_ESTIMATE;
      const rate = ((dataCount / totalEstimate) * 100).toFixed(1);
      return { count: dataCount, total: totalEstimate, rate };
    }
    // data_count がない場合は、jobTypeWagesのcount合計で推定
    if (jobTypeWages && jobTypeWages.length > 0) {
      const totalCount = jobTypeWages.reduce((sum, w) => sum + w.count, 0);
      if (totalCount > 0) {
        const totalEstimate = TOTAL_FACILITIES_ESTIMATE;
        const rate = ((totalCount / totalEstimate) * 100).toFixed(1);
        return { count: totalCount, total: totalEstimate, rate };
      }
    }
    return null;
  }, [kpi, jobTypeWages]);

  // 求人給与ベンチマーク（業種x雇用形態別、棒グラフ用）
  // mean_min と mean_max の中間値を平均給与として使用
  const benchmarkChartData = useMemo(() => {
    if (!salaryBenchmark) return [];
    return [...salaryBenchmark]
      .filter((d) => d.mean_min != null || d.mean_max != null)
      .map((d) => {
        const avgSalary = d.mean_min != null && d.mean_max != null
          ? (d.mean_min + d.mean_max) / 2
          : (d.mean_min ?? d.mean_max ?? 0);
        return {
          occupation: `${d.industry_major_code}(${d.emp_group})`,
          avg_salary: Math.round(avgSalary),
        };
      })
      .sort((a, b) => b.avg_salary - a.avg_salary)
      .slice(0, 15);
  }, [salaryBenchmark]);

  // 最低賃金推移（折れ線グラフ用）
  const wageHistoryChartData = useMemo(() => {
    if (!wageHistory) return [];
    return [...wageHistory]
      .filter((d) => d.fiscal_year != null && d.hourly_min_wage != null)
      .sort((a, b) => (a.fiscal_year ?? 0) - (b.fiscal_year ?? 0))
      .map((d) => ({
        year: String(d.fiscal_year),
        min_wage: d.hourly_min_wage ?? 0,
      }));
  }, [wageHistory]);

  // 職種テーブル展開状態
  const [jobTableExpanded, setJobTableExpanded] = useState(false);

  const apiError = kpiError || jobTypeError || prefError;
  const hasRealData = kpi?.avg_salary != null;

  return (
    <div className="space-y-6">
      {/* ページタイトル */}
      <div>
        <h1 className="text-xl font-bold text-gray-900">賃金分析</h1>
        <p className="text-sm text-gray-500 mt-1">
          職種別賃金水準・地域格差・賃金分布の分析
        </p>
      </div>

      {/* データ充填率バナー */}
      {dataCountInfo ? (
        <div className="text-xs text-gray-500 bg-gray-50 border border-gray-200 rounded-lg px-3 py-2">
          &#x2139;&#xFE0F; 賃金データ対象: {dataCountInfo.count.toLocaleString("ja-JP")}件（充填率{dataCountInfo.rate}%）。外部統計データも参考表示しています。
        </div>
      ) : !kpiLoading && !hasRealData ? (
        <div className="text-xs text-gray-500 bg-gray-50 border border-gray-200 rounded-lg px-3 py-2">
          &#x2139;&#xFE0F; 賃金データが限定的なため、外部統計データを参考値として表示しています。
        </div>
      ) : null}

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
        <KpiCard
          label="平均給与水準"
          value={kpi?.avg_salary ?? null}
          format="number"
          icon={<svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><rect width="20" height="12" x="2" y="6" rx="2" /><circle cx="12" cy="12" r="2" /><path d="M6 12h.01M18 12h.01" /></svg>}
          accentColor="bg-emerald-500"
          subtitle={kpi?.avg_salary != null ? formatManYen(kpi.avg_salary) : "データなし"}
          loading={kpiLoading}
        />
        <KpiCard
          label="中央値給与"
          value={kpi?.median_salary ?? null}
          format="number"
          icon={<svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M19 14c1.49-1.46 3-3.21 3-5.5A5.5 5.5 0 0 0 16.5 3c-1.76 0-3 .5-4.5 2-1.5-1.5-2.74-2-4.5-2A5.5 5.5 0 0 0 2 8.5c0 2.3 1.5 4.05 3 5.5l7 7Z" /></svg>}
          accentColor="bg-rose-500"
          subtitle={kpi?.median_salary != null ? formatManYen(kpi.median_salary) : "データなし"}
          loading={kpiLoading}
        />
        <KpiCard
          label="最高給与水準"
          value={kpi?.max_salary ?? null}
          format="number"
          icon={<svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M21 12V7H5a2 2 0 0 1 0-4h14v4" /><path d="M3 5v14a2 2 0 0 0 2 2h16v-5" /><path d="M18 12a2 2 0 0 0 0 4h4v-4Z" /></svg>}
          accentColor="bg-blue-500"
          subtitle={kpi?.max_salary != null ? formatManYen(kpi.max_salary) : "データなし"}
          loading={kpiLoading}
        />
        <KpiCard
          label="最低給与水準"
          value={kpi?.min_salary ?? null}
          format="number"
          icon={<svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><ellipse cx="12" cy="5" rx="9" ry="3" /><path d="M3 5V19A9 3 0 0 0 21 19V5" /><path d="M3 12A9 3 0 0 0 21 12" /></svg>}
          accentColor="bg-gray-500"
          subtitle={kpi?.min_salary != null ? formatManYen(kpi.min_salary) : "データなし"}
          loading={kpiLoading}
        />
      </KpiCardGrid>

      {/* チャートエリア */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* 1. 職種別賃金比較 */}
        <ChartCard
          title="職種別 平均賃金"
          subtitle={jobTypeWages ? `${jobTypeWages.length}職種のデータ` : "職種の比較棒グラフ"}
        >
          {jobTypeLoading ? (
            <div className="flex items-center justify-center h-[300px] text-gray-400 text-sm">
              読み込み中...
            </div>
          ) : jobTypeWages && jobTypeWages.length > 0 ? (
            <BarChart
              data={[...jobTypeWages].sort((a, b) => b.avg_salary - a.avg_salary).slice(0, 20)}
              xKey="job_type"
              yKey="avg_salary"
              color={CHART_COLORS[0]}
              tooltipFormatter={tooltipManYen}
              height={300}
            />
          ) : (
            <DataPendingPlaceholder
              message="賃金データ準備中"
              description="職種別の平均賃金を比較します"
              height={300}
            />
          )}
        </ChartCard>

        {/* 2. 都道府県別賃金 */}
        <ChartCard
          title="都道府県別 平均賃金（Top 20）"
          subtitle="地域別の賃金水準を比較"
        >
          {prefLoading ? (
            <div className="flex items-center justify-center h-[300px] text-gray-400 text-sm">
              読み込み中...
            </div>
          ) : prefWages && prefWages.length > 0 ? (
            <BarChart
              data={
                [...prefWages]
                  .sort((a, b) => b.avg_salary - a.avg_salary)
                  .slice(0, 20)
              }
              xKey="prefecture"
              yKey="avg_salary"
              color={CHART_COLORS[6]}
              horizontal
              tooltipFormatter={tooltipManYen}
              height={400}
            />
          ) : externalWageData.length > 0 ? (
            <>
              <div className="text-xs text-amber-600 mb-2 px-1">
                ※ 施設固有の賃金データがないため、全産業平均x0.75（介護業界補正）を表示
              </div>
              <BarChart
                data={externalWageData}
                xKey="prefecture"
                yKey="avg_wage"
                color={CHART_COLORS[6]}
                horizontal
                tooltipFormatter={(v) => `${v.toLocaleString("ja-JP")}円/月（推定）`}
                height={400}
              />
            </>
          ) : (
            <DataPendingPlaceholder
              message="賃金データ準備中"
              description="都道府県別の平均賃金を横棒グラフで表示します"
              height={300}
            />
          )}
        </ChartCard>

        {/* 3. 賃金 vs 施設数（散布図） */}
        <ChartCard
          title="都道府県別 平均賃金 vs データ件数"
          subtitle="賃金水準と施設集中度の関係"
        >
          {prefLoading ? (
            <div className="flex items-center justify-center h-[300px] text-gray-400 text-sm">
              読み込み中...
            </div>
          ) : scatterData.length > 0 ? (
            <ScatterChart
              data={scatterData}
              xKey="avg_salary"
              yKey="count"
              xLabel="平均賃金（円）"
              yLabel="データ件数"
              nameKey="name"
              color={CHART_COLORS[5]}
              height={300}
              tooltipFormatter={(value, name) => {
                if (name === "avg_salary") return tooltipManYen(value);
                return `${value}件`;
              }}
            />
          ) : (
            <DataPendingPlaceholder
              message="散布図データ準備中"
              description="都道府県別の平均賃金とデータ件数の相関を表示します"
              height={300}
            />
          )}
        </ChartCard>

        {/* 4. 賃金分布（職種別の賃金帯ヒストグラム） */}
        <ChartCard
          title="職種別 賃金帯分布"
          subtitle="賃金帯ごとの職種数"
        >
          {jobTypeLoading ? (
            <div className="flex items-center justify-center h-[300px] text-gray-400 text-sm">
              読み込み中...
            </div>
          ) : histogramData.length > 0 ? (
            <BarChart
              data={histogramData}
              xKey="range"
              yKey="count"
              color={CHART_COLORS[3]}
              tooltipFormatter={(v) => `${v}職種`}
              height={300}
            />
          ) : (
            <DataPendingPlaceholder
              message="賃金分布データ準備中"
              description="賃金帯別の職種数分布を表示します"
              height={300}
            />
          )}
        </ChartCard>
      </div>

      {/* 職種別賃金詳細テーブル */}
      <ChartCard
        title="職種別 賃金詳細"
        subtitle={jobTypeWages ? `全${jobTypeWages.length}職種の賃金データ` : "賃金分析を行う職種一覧"}
      >
        {jobTypeLoading ? (
          <div className="text-sm text-gray-400 p-4">読み込み中...</div>
        ) : jobTypeWages && jobTypeWages.length > 0 ? (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-200 text-left">
                  <th className="px-4 py-2 text-gray-500 font-medium">職種</th>
                  <th className="px-4 py-2 text-gray-500 font-medium text-right">平均賃金</th>
                  <th className="px-4 py-2 text-gray-500 font-medium text-right">データ件数</th>
                  {jobTypeWages[0]?.avg_age != null && (
                    <th className="px-4 py-2 text-gray-500 font-medium text-right">平均年齢</th>
                  )}
                  {jobTypeWages[0]?.avg_tenure != null && (
                    <th className="px-4 py-2 text-gray-500 font-medium text-right">平均勤続</th>
                  )}
                </tr>
              </thead>
              <tbody>
                {[...jobTypeWages]
                  .sort((a, b) => b.avg_salary - a.avg_salary)
                  .slice(0, jobTableExpanded ? undefined : 10)
                  .map((w) => (
                    <tr key={w.job_type} className="border-b border-gray-100 hover:bg-gray-50">
                      <td className="px-4 py-2.5 text-gray-700 font-medium">{w.job_type}</td>
                      <td className="px-4 py-2.5 text-right tabular-nums">
                        <span className="text-emerald-600 font-medium">
                          {formatManYen(w.avg_salary)}
                        </span>
                        <span className="text-gray-400 text-xs ml-1">
                          ({w.avg_salary.toLocaleString("ja-JP")}円)
                        </span>
                      </td>
                      <td className="px-4 py-2.5 text-right text-gray-500 tabular-nums">
                        {w.count.toLocaleString("ja-JP")}件
                      </td>
                      {jobTypeWages[0]?.avg_age != null && (
                        <td className="px-4 py-2.5 text-right text-gray-500 tabular-nums">
                          {w.avg_age != null ? `${w.avg_age.toFixed(1)}歳` : "--"}
                        </td>
                      )}
                      {jobTypeWages[0]?.avg_tenure != null && (
                        <td className="px-4 py-2.5 text-right text-gray-500 tabular-nums">
                          {w.avg_tenure ?? "--"}
                        </td>
                      )}
                    </tr>
                  ))}
              </tbody>
            </table>
            {jobTypeWages.length > 10 && (
              <div className="flex justify-center pt-3 pb-1">
                <button
                  onClick={() => setJobTableExpanded((prev) => !prev)}
                  className="text-sm text-indigo-600 hover:text-indigo-800 font-medium px-4 py-1.5 rounded-lg hover:bg-indigo-50 transition-colors"
                >
                  {jobTableExpanded ? "折りたたむ" : `もっと見る（全${jobTypeWages.length}件）`}
                </button>
              </div>
            )}
          </div>
        ) : (
          <div className="space-y-2 p-4">
            <p className="text-sm text-gray-400">職種別の賃金データは準備中です</p>
          </div>
        )}
      </ChartCard>

      {/* 外部統計データセクション（補足情報） */}
      {externalWageData.length > 0 && prefWages && prefWages.length > 0 && (
        <ChartCard
          title="参考: 全産業平均賃金（外部統計）"
          subtitle="厚生労働省 賃金構造基本統計調査ベース（介護補正x0.75）"
        >
          <div className="text-xs text-gray-500 mb-2 px-1">
            ※ 上記の施設固有データと比較するための参考値です
          </div>
          <BarChart
            data={externalWageData}
            xKey="prefecture"
            yKey="avg_wage"
            color={CHART_COLORS[6]}
            horizontal
            tooltipFormatter={(v) => `${v.toLocaleString("ja-JP")}円/月（全産業推定）`}
            height={400}
          />
        </ChartCard>
      )}

      {/* 外部API: 求人給与ベンチマーク・最低賃金推移 */}
      <div className="border-t border-gray-200 pt-6 mt-2">
        <h2 className="text-lg font-semibold text-gray-800 mb-1">求人市場の賃金データ</h2>
        <p className="text-sm text-gray-500 mb-4">
          求人媒体・政府統計に基づく賃金ベンチマーク
          {selectedPrefecture ? `（${selectedPrefecture}）` : ""}
        </p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* 求人給与ベンチマーク */}
        <ChartCard
          title="求人給与ベンチマーク（職種別）"
          subtitle="求人市場における職種別平均給与"
          loading={benchmarkLoading}
        >
          {benchmarkChartData.length > 0 ? (
            <>
              <BarChart
                data={benchmarkChartData}
                xKey="occupation"
                yKey="avg_salary"
                color={CHART_COLORS[2]}
                horizontal
                tooltipFormatter={(v) => `${v.toLocaleString("ja-JP")}円`}
                height={380}
              />
              <p className="text-xs text-gray-400 mt-2 px-1">
                データソース: 厚生労働省 賃金構造基本統計調査
              </p>
            </>
          ) : (
            <div className="flex items-center justify-center h-[300px] text-gray-400 text-sm">
              {benchmarkLoading ? "読み込み中..." : "データがありません"}
            </div>
          )}
        </ChartCard>

        {/* 最低賃金推移 */}
        <ChartCard
          title="最低賃金推移"
          subtitle="年度別の地域最低賃金の変遷"
          loading={wageHistoryLoading}
        >
          {wageHistoryChartData.length > 0 ? (
            <>
              <LineChart
                data={wageHistoryChartData}
                xKey="year"
                series={[
                  { dataKey: "min_wage", name: "最低賃金（円/時）", color: "#dc2626" },
                ]}
                tooltipFormatter={(v) => `${v.toLocaleString("ja-JP")}円/時`}
                height={380}
              />
              <p className="text-xs text-gray-400 mt-2 px-1">
                データソース: 厚生労働省 地域別最低賃金
              </p>
            </>
          ) : (
            <div className="flex items-center justify-center h-[300px] text-gray-400 text-sm">
              {wageHistoryLoading ? "読み込み中..." : "データがありません"}
            </div>
          )}
        </ChartCard>
      </div>
    </div>
  );
}

export default function SalaryPage() {
  return (
    <Suspense fallback={<div className="text-gray-400 text-sm p-8">読み込み中...</div>}>
      <SalaryContent />
    </Suspense>
  );
}
