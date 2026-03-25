"use client";

// ===================================================
// Page 09: 成長性・安定性分析
// 施設の設立年推移、事業年数分布、安定性マトリクス
// 実API接続版
// ===================================================

import { Suspense, useMemo } from "react";
import Link from "next/link";
import { useApi } from "@/hooks/useApi";
import { useFilters } from "@/hooks/useFilters";
import KpiCard from "@/components/data-display/KpiCard";
import KpiCardGrid from "@/components/data-display/KpiCardGrid";
import BarChart from "@/components/charts/BarChart";
import LineChart from "@/components/charts/LineChart";
import DonutChart from "@/components/charts/DonutChart";
import ChartCard from "@/components/charts/ChartCard";
import FilterPanel from "@/components/filters/FilterPanel";
import DataPendingPlaceholder from "@/components/common/DataPendingPlaceholder";
import { KpiGridSkeleton } from "@/components/common/LoadingSpinner";
import ApiErrorBanner from "@/components/common/ApiErrorBanner";
import type {
  GrowthKpi,
  EstablishmentYearCount,
  BusinessYearsBin,
  GrowthPhaseDistribution,
  ExternalBusinessDynamics,
} from "@/lib/types";

/** KPIアイコン: 新規施設 */
const IconNewFacility = (
  <svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
    <path d="M7 20h10" /><path d="M10 20c5.5-2.5.8-6.4 3-10" />
    <path d="M9.5 9.4c1.1.8 1.8 2.2 2.3 3.7-2 .4-3.5.4-4.8-.3-1.2-.6-2.3-1.9-3-4.2 2.8-.5 4.4 0 5.5.8z" />
    <path d="M14.1 6a7 7 0 0 0-1.1 4c1.9-.1 3.3-.6 4.3-1.4 1-1 1.6-2.3 1.7-4.6-2.7.1-4 1-4.9 2z" />
  </svg>
);

/** KPIアイコン: 平均事業年数 */
const IconYears = (
  <svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
    <circle cx="12" cy="12" r="10" /><polyline points="12 6 12 12 16 14" />
  </svg>
);

/** KPIアイコン: 純増減率 */
const IconGrowthRate = (
  <svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
    <polyline points="22 7 13.5 15.5 8.5 10.5 2 17" /><polyline points="16 7 22 7 22 13" />
  </svg>
);

/** KPIアイコン: 対象施設数 */
const IconTarget = (
  <svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
    <path d="M3 3v18h18" /><path d="M7 16V8" /><path d="M11 16V11" /><path d="M15 16v-3" /><path d="M19 16V5" />
  </svg>
);

function GrowthContent() {
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

  // 外部API: 開業率/廃業率
  const selectedPrefecture = filters.prefectures.length === 1 ? filters.prefectures[0] : undefined;
  const { data: businessDynamicsData, isLoading: dynamicsLoading } = useApi<ExternalBusinessDynamics[]>(
    "/api/external/business-dynamics",
    { prefecture: selectedPrefecture }
  );

  // 開業率/廃業率（折れ線グラフ用）
  // バックエンドは closure_rate（closing_rateではない）を返す
  const dynamicsChartData = useMemo(() => {
    if (!businessDynamicsData) return [];
    return [...businessDynamicsData]
      .filter((d) => d.fiscal_year != null)
      .sort((a, b) => String(a.fiscal_year).localeCompare(String(b.fiscal_year)))
      .map((d) => ({
        year: String(d.fiscal_year),
        opening_rate: Math.round((d.opening_rate ?? 0) * 1000) / 10,
        closing_rate: Math.round((d.closure_rate ?? 0) * 1000) / 10,
      }));
  }, [businessDynamicsData]);

  const apiError = kpiError || trendError || yearsError;

  // 事業年数分布から成長フェーズ別分布を計算
  // 0-3年=新興, 3-10年=成長, 10-20年=成熟, 20年+=老舗
  const growthPhases: GrowthPhaseDistribution[] = useMemo(() => {
    if (!yearsDistribution || yearsDistribution.length === 0) return [];

    // rangeラベルから数値を解析してフェーズに振り分ける
    let emerging = 0;  // 新興: 0-3年
    let growing = 0;   // 成長: 3-10年
    let mature = 0;    // 成熟: 10-20年
    let veteran = 0;   // 老舗: 20年+

    for (const bin of yearsDistribution) {
      const r = bin.range;
      // rangeパターン: "0-5年", "5-10年", "10-15年", "15-20年", "20-25年", "25年以上" 等
      const match = r.match(/(\d+)/);
      if (!match) continue;
      const startYear = parseInt(match[1], 10);

      if (startYear < 3) {
        emerging += bin.count;
      } else if (startYear < 10) {
        growing += bin.count;
      } else if (startYear < 20) {
        mature += bin.count;
      } else {
        veteran += bin.count;
      }
    }

    return [
      { phase: "新興(3年未満)", count: emerging },
      { phase: "成長(3-10年)", count: growing },
      { phase: "成熟(10-20年)", count: mature },
      { phase: "老舗(20年超)", count: veteran },
    ].filter((p) => p.count > 0);
  }, [yearsDistribution]);

  // 設立年推移を文字列キーに変換（BarChart互換）
  const trendChartData = useMemo(() => {
    if (!establishmentTrend) return [];
    return establishmentTrend.map((d) => ({
      year: String(d.year),
      count: d.count,
    }));
  }, [establishmentTrend]);

  // 年代別設立トレンド: 10年区間で集計（地域トレンドの代替）
  const decadeTrendData = useMemo(() => {
    if (!establishmentTrend || establishmentTrend.length === 0) return [];
    // 10年ごとにグループ化
    const decades: Record<string, number> = {};
    for (const d of establishmentTrend) {
      const decadeStart = Math.floor(d.year / 10) * 10;
      const label = `${decadeStart}年代`;
      decades[label] = (decades[label] || 0) + d.count;
    }
    return Object.entries(decades)
      .map(([decade, count]) => ({ decade, count }))
      .sort((a, b) => a.decade.localeCompare(b.decade));
  }, [establishmentTrend]);

  // 累積成長チャート: 施設数の累積推移（規模別成長の代替）
  const cumulativeGrowthData = useMemo(() => {
    if (!establishmentTrend || establishmentTrend.length === 0) return [];
    let cumulative = 0;
    // 5年ごとに集計して可読性を高める
    const fiveYearBuckets: Record<number, number> = {};
    for (const d of establishmentTrend) {
      cumulative += d.count;
      const bucketYear = Math.floor(d.year / 5) * 5;
      fiveYearBuckets[bucketYear] = cumulative;
    }
    return Object.entries(fiveYearBuckets)
      .map(([year, total]) => ({ year: `${year}年`, total }))
      .sort((a, b) => a.year.localeCompare(b.year));
  }, [establishmentTrend]);

  // 将来予測: 過去5年間の線形トレンドから3年分を外挿
  const projectionChartData = useMemo(() => {
    if (!establishmentTrend || establishmentTrend.length < 5) return [];
    const recent = establishmentTrend.slice(-5);
    const avgGrowth =
      recent.reduce((sum, d, i) => {
        if (i === 0) return 0;
        return sum + (d.count - recent[i - 1].count);
      }, 0) / (recent.length - 1);
    const lastItem = recent[recent.length - 1];

    // 実績データ（過去5年分）
    const actualData = recent.map((d) => ({
      year: String(d.year),
      actual: d.count,
      projected: null as number | null,
    }));

    // 最終実績年は予測線の開始点としても設定
    actualData[actualData.length - 1].projected = lastItem.count;

    // 予測データ（3年分）
    const projectedData = [1, 2, 3].map((offset) => ({
      year: String(lastItem.year + offset),
      actual: null as number | null,
      projected: Math.max(0, Math.round(lastItem.count + avgGrowth * offset)),
    }));

    return [...actualData, ...projectedData];
  }, [establishmentTrend]);

  return (
    <div className="space-y-6">
      {/* ページタイトル */}
      <div>
        <h1 className="text-xl font-bold text-gray-900">成長性・安定性分析</h1>
        <p className="text-sm text-gray-500 mt-1">
          施設の設立年推移、事業年数分布から市場の成熟度と成長トレンドを把握
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
        <KpiGridSkeleton count={4} />
      ) : (
        <KpiCardGrid>
          <KpiCard
            label="直近3年新規施設数"
            value={kpi?.recent_3yr_count ?? null}
            format="number"
            icon={IconNewFacility}
            accentColor="bg-emerald-500"
            subtitle="事業開始日ベース"
          />
          <KpiCard
            label="平均事業年数"
            value={kpi?.avg_years_in_business ?? null}
            format="decimal"
            icon={IconYears}
            accentColor="bg-blue-500"
            subtitle="事業開始日から算出"
          />
          <KpiCard
            label="純増減率"
            value={kpi?.net_growth_rate ?? null}
            format="percent"
            icon={IconGrowthRate}
            accentColor="bg-indigo-500"
            subtitle="直近3年設立数 / 全体数"
          />
          <KpiCard
            label="事業開始日あり施設数"
            value={kpi?.total_with_start_date ?? null}
            format="number"
            icon={IconTarget}
            accentColor="bg-amber-500"
            subtitle="分析対象の母数"
          />
        </KpiCardGrid>
      )}

      {/* チャートエリア */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* 1. 施設設立年推移 */}
        <ChartCard
          title="施設設立年推移"
          subtitle="年別の新規設立施設数"
          loading={trendLoading}
        >
          {trendChartData.length > 0 ? (
            <BarChart
              data={trendChartData}
              xKey="year"
              yKey="count"
              color="#6366f1"
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

        {/* 2. 事業年数分布 */}
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

        {/* 3. 年代別設立トレンド（地域トレンドの代替） */}
        <ChartCard
          title="年代別設立トレンド"
          subtitle="10年区間ごとの施設設立数の推移"
          loading={trendLoading}
        >
          {decadeTrendData.length > 0 ? (
            <BarChart
              data={decadeTrendData}
              xKey="decade"
              yKey="count"
              color="#10b981"
              tooltipFormatter={(v) => `${v.toLocaleString("ja-JP")}施設`}
              height={320}
            />
          ) : (
            <DataPendingPlaceholder
              message="データなし"
              description="フィルタ条件に一致する施設がありません"
              height={320}
            />
          )}
        </ChartCard>

        {/* 4. 成長フェーズ別分布 */}
        <ChartCard
          title="成長フェーズ別分布"
          subtitle="事業年数による成長ステージ分類"
          loading={yearsLoading}
        >
          {growthPhases.length > 0 ? (
            <DonutChart
              data={growthPhases}
              nameKey="phase"
              valueKey="count"
              centerLabel="施設"
              colors={["#22c55e", "#3b82f6", "#6366f1", "#9ca3af"]}
              height={320}
            />
          ) : (
            <DataPendingPlaceholder
              message="データなし"
              description="事業年数データがありません"
              height={320}
            />
          )}
        </ChartCard>
      </div>

      {/* 追加チャートエリア */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* 累積成長チャート */}
        <ChartCard
          title="累積施設数の推移"
          subtitle="5年区間ごとの施設数累積推移"
          loading={trendLoading}
        >
          {cumulativeGrowthData.length > 0 ? (
            <LineChart
              data={cumulativeGrowthData}
              xKey="year"
              series={[
                { dataKey: "total", name: "累積施設数", color: "#6366f1" },
              ]}
              tooltipFormatter={(v) => `${v.toLocaleString("ja-JP")}施設`}
              height={320}
            />
          ) : (
            <DataPendingPlaceholder
              message="データなし"
              description="設立年データがありません"
              height={320}
            />
          )}
        </ChartCard>

        {/* 関連分析へのクロスリファレンス */}
        <ChartCard
          title="関連する詳細分析"
          subtitle="他ページで利用可能な関連データ"
        >
          <div className="flex flex-col gap-4 p-2" style={{ minHeight: 320 }}>
            {/* 安定性マトリクス */}
            <Link
              href="/workforce"
              className="group block rounded-lg border border-gray-200 p-4 hover:border-indigo-300 hover:bg-indigo-50/50 transition-colors"
            >
              <div className="flex items-start gap-3">
                <div className="w-10 h-10 rounded-lg bg-indigo-100 flex items-center justify-center flex-shrink-0">
                  <svg className="w-5 h-5 text-indigo-600" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" aria-hidden="true">
                    <circle cx="12" cy="12" r="10" /><polyline points="12 6 12 12 16 14" />
                  </svg>
                </div>
                <div className="flex-1 min-w-0">
                  <h4 className="text-sm font-semibold text-gray-800 group-hover:text-indigo-700">
                    安定性マトリクス（離職率 x 事業年数）
                  </h4>
                  <p className="text-xs text-gray-500 mt-1 leading-relaxed">
                    離職率と事業年数の相関分析は人材分析ページで確認できます。
                    施設ごとの安定性を4象限で分析します。
                  </p>
                </div>
                <svg className="w-4 h-4 text-gray-400 group-hover:text-indigo-500 flex-shrink-0 mt-1" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" aria-hidden="true">
                  <path d="M9 18l6-6-6-6" />
                </svg>
              </div>
            </Link>

            {/* 競合密度分析 */}
            <Link
              href="/market"
              className="group block rounded-lg border border-gray-200 p-4 hover:border-emerald-300 hover:bg-emerald-50/50 transition-colors"
            >
              <div className="flex items-start gap-3">
                <div className="w-10 h-10 rounded-lg bg-emerald-100 flex items-center justify-center flex-shrink-0">
                  <svg className="w-5 h-5 text-emerald-600" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" aria-hidden="true">
                    <path d="M3 3v18h18" /><path d="M7 16V8" /><path d="M11 16V11" /><path d="M15 16v-3" /><path d="M19 16V5" />
                  </svg>
                </div>
                <div className="flex-1 min-w-0">
                  <h4 className="text-sm font-semibold text-gray-800 group-hover:text-emerald-700">
                    競合密度分析（都道府県別施設数）
                  </h4>
                  <p className="text-xs text-gray-500 mt-1 leading-relaxed">
                    都道府県別の施設密度・競合状況は市場分析ページで確認できます。
                    人口あたりの施設数や地域別の市場飽和度を分析します。
                  </p>
                </div>
                <svg className="w-4 h-4 text-gray-400 group-hover:text-emerald-500 flex-shrink-0 mt-1" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" aria-hidden="true">
                  <path d="M9 18l6-6-6-6" />
                </svg>
              </div>
            </Link>

            {/* 規模別成長 */}
            <Link
              href="/workforce"
              className="group block rounded-lg border border-gray-200 p-4 hover:border-blue-300 hover:bg-blue-50/50 transition-colors"
            >
              <div className="flex items-start gap-3">
                <div className="w-10 h-10 rounded-lg bg-blue-100 flex items-center justify-center flex-shrink-0">
                  <svg className="w-5 h-5 text-blue-600" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" aria-hidden="true">
                    <path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2" /><circle cx="9" cy="7" r="4" /><path d="M23 21v-2a4 4 0 0 0-3-3.87" /><path d="M16 3.13a4 4 0 0 1 0 7.75" />
                  </svg>
                </div>
                <div className="flex-1 min-w-0">
                  <h4 className="text-sm font-semibold text-gray-800 group-hover:text-blue-700">
                    規模別成長分析（従業員規模別）
                  </h4>
                  <p className="text-xs text-gray-500 mt-1 leading-relaxed">
                    従業員規模ごとの施設分布・離職率は人材分析ページで確認できます。
                    規模別の成長パターンを把握します。
                  </p>
                </div>
                <svg className="w-4 h-4 text-gray-400 group-hover:text-blue-500 flex-shrink-0 mt-1" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" aria-hidden="true">
                  <path d="M9 18l6-6-6-6" />
                </svg>
              </div>
            </Link>
          </div>
        </ChartCard>
      </div>

      {/* 成長性指標の説明 */}
      <ChartCard
        title="成長フェーズの定義"
        subtitle="事業年数に基づく4段階分類"
      >
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
          {[
            {
              phase: "新興",
              years: "3年未満",
              description: "設立直後で成長余地が大きい。M&A観点では経営基盤の安定性に要注意。",
              color: "bg-green-50 border-green-200",
              textColor: "text-green-700",
              dotColor: "bg-green-500",
            },
            {
              phase: "成長",
              years: "3-10年",
              description: "事業拡大期。サービス多角化や地域展開のポテンシャルが高い。",
              color: "bg-blue-50 border-blue-200",
              textColor: "text-blue-700",
              dotColor: "bg-blue-500",
            },
            {
              phase: "成熟",
              years: "10-20年",
              description: "安定運営。人材・ノウハウの蓄積があり、M&A対象として魅力的。",
              color: "bg-indigo-50 border-indigo-200",
              textColor: "text-indigo-700",
              dotColor: "bg-indigo-500",
            },
            {
              phase: "老舗",
              years: "20年超",
              description: "長い実績。後継者問題やIT化遅れの可能性。事業承継型M&Aの対象。",
              color: "bg-gray-50 border-gray-200",
              textColor: "text-gray-700",
              dotColor: "bg-gray-500",
            },
          ].map((item) => (
            <div
              key={item.phase}
              className={`rounded-lg border p-4 ${item.color}`}
            >
              <div className="flex items-center gap-2 mb-2">
                <span className={`w-2.5 h-2.5 rounded-full ${item.dotColor}`} />
                <h4 className={`text-sm font-semibold ${item.textColor}`}>
                  {item.phase}
                </h4>
              </div>
              <p className="text-xs text-gray-500 mb-1">{item.years}</p>
              <p className="text-xs text-gray-600 leading-relaxed">
                {item.description}
              </p>
            </div>
          ))}
        </div>
      </ChartCard>

      {/* 将来予測セクション */}
      <ChartCard
        title="将来予測"
        subtitle="過去5年間のトレンドに基づく新規施設数の予測"
        loading={trendLoading}
      >
        {projectionChartData.length > 0 ? (
          <div>
            <LineChart
              data={projectionChartData}
              xKey="year"
              series={[
                { dataKey: "actual", name: "実績", color: "#6366f1" },
                { dataKey: "projected", name: "予測", color: "#f59e0b", dashed: true },
              ]}
              tooltipFormatter={(v) => `${v.toLocaleString("ja-JP")}施設`}
              height={320}
              referenceLineX={
                establishmentTrend && establishmentTrend.length >= 5
                  ? String(establishmentTrend[establishmentTrend.length - 1].year)
                  : undefined
              }
              referenceLineLabel="予測開始"
            />
            <p className="text-xs text-gray-400 mt-3 px-1 leading-relaxed">
              将来予測は過去5年間の線形トレンドに基づく概算であり、実際の市場動向とは異なる場合があります
            </p>
          </div>
        ) : (
          <DataPendingPlaceholder
            message="データ不足"
            description="将来予測には5年以上の設立年データが必要です"
            height={320}
          />
        )}
      </ChartCard>

      {/* 外部統計: 開業率/廃業率 */}
      <div className="border-t border-gray-200 pt-6 mt-2">
        <h2 className="text-lg font-semibold text-gray-800 mb-1">外部統計: 開業率・廃業率</h2>
        <p className="text-sm text-gray-500 mb-4">
          全産業の開業率・廃業率の年度別推移
          {selectedPrefecture ? `（${selectedPrefecture}）` : ""}
        </p>
      </div>

      <ChartCard
        title="開業率/廃業率の推移"
        subtitle="年度別の開業率と廃業率の比較"
        loading={dynamicsLoading}
      >
        {dynamicsChartData.length > 0 ? (
          <>
            <LineChart
              data={dynamicsChartData}
              xKey="year"
              series={[
                { dataKey: "opening_rate", name: "開業率(%)", color: "#059669" },
                { dataKey: "closing_rate", name: "廃業率(%)", color: "#dc2626" },
              ]}
              tooltipFormatter={(v) => `${v}%`}
              height={350}
            />
            <p className="text-xs text-gray-400 mt-2 px-1">
              データソース: 中小企業庁 中小企業白書
            </p>
          </>
        ) : (
          <div className="flex items-center justify-center h-[300px] text-gray-400 text-sm">
            {dynamicsLoading ? "読み込み中..." : "データがありません"}
          </div>
        )}
      </ChartCard>

      {/* データソース表記 */}
      <div className="rounded-lg bg-blue-50 border border-blue-200 px-4 py-3">
        <p className="text-xs text-blue-700 leading-relaxed">
          <span className="font-semibold">データソース:</span>{" "}
          厚生労働省「介護サービス情報公表システム」掲載データより算出。
          事業年数は各施設の事業開始日から本日時点までの年数で計算しています。
        </p>
      </div>
    </div>
  );
}

export default function GrowthPage() {
  return (
    <Suspense fallback={<div className="text-gray-400 text-sm p-8">読み込み中...</div>}>
      <GrowthContent />
    </Suspense>
  );
}
