"use client";

// ===================================================
// Page T2: 採用天気予報
// 都道府県別の介護人材採用難易度を天気アイコンで可視化
// /api/external/hiring-difficulty エンドポイント使用
// ===================================================

import { Suspense, useMemo, useState } from "react";
import { useApi } from "@/hooks/useApi";
import KpiCard from "@/components/data-display/KpiCard";
import KpiCardGrid from "@/components/data-display/KpiCardGrid";
import DataTable from "@/components/data-display/DataTable";
import BarChart from "@/components/charts/BarChart";
import ChartCard from "@/components/charts/ChartCard";
import ApiErrorBanner from "@/components/common/ApiErrorBanner";
import ConfidenceBadge from "@/components/common/ConfidenceBadge";
import type { ColumnDef, SortState, ExternalVacancyStats } from "@/lib/types";

// ===================================================
// 型定義
// ===================================================

/** 採用天気予報APIレスポンス行 */
interface HiringDifficultyRow {
  prefecture: string;
  facility_count: number;
  avg_turnover_rate: number;
  job_offers_rate: number;
  avg_monthly_wage: number;
  difficulty_score: number;
  weather: string;
}

// ===================================================
// KPIアイコン
// ===================================================

/** スコアアイコン */
const IconScore = (
  <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={1.5} aria-hidden="true">
    <path strokeLinecap="round" strokeLinejoin="round" d="M3 13.125C3 12.504 3.504 12 4.125 12h2.25c.621 0 1.125.504 1.125 1.125v6.75C7.5 20.496 6.996 21 6.375 21h-2.25A1.125 1.125 0 013 19.875v-6.75zM9.75 8.625c0-.621.504-1.125 1.125-1.125h2.25c.621 0 1.125.504 1.125 1.125v11.25c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 01-1.125-1.125V8.625zM16.5 4.125c0-.621.504-1.125 1.125-1.125h2.25C20.496 3 21 3.504 21 4.125v15.75c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 01-1.125-1.125V4.125z" />
  </svg>
);

/** 地域アイコン */
const IconRegion = (
  <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={1.5} aria-hidden="true">
    <path strokeLinecap="round" strokeLinejoin="round" d="M15 10.5a3 3 0 11-6 0 3 3 0 016 0z" />
    <path strokeLinecap="round" strokeLinejoin="round" d="M19.5 10.5c0 7.142-7.5 11.25-7.5 11.25S4.5 17.642 4.5 10.5a7.5 7.5 0 1115 0z" />
  </svg>
);

/** 離職率アイコン */
const IconTurnover = (
  <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={1.5} aria-hidden="true">
    <path strokeLinecap="round" strokeLinejoin="round" d="M2.25 6L9 12.75l4.286-4.286a11.948 11.948 0 014.306 6.43l.776 2.898m0 0l3.182-5.511m-3.182 5.51l-5.511-3.181" />
  </svg>
);

/** 採用容易アイコン */
const IconEasy = (
  <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={1.5} aria-hidden="true">
    <path strokeLinecap="round" strokeLinejoin="round" d="M2.25 18L9 11.25l4.306 4.307a11.95 11.95 0 015.814-5.519l2.74-1.22m0 0l-5.94-2.28m5.94 2.28l-2.28 5.941" />
  </svg>
);

// ===================================================
// 天気凡例
// ===================================================

const WEATHER_LEGEND = [
  { emoji: "\u2600\uFE0F", label: "晴れ", description: "採用容易", color: "bg-green-100 text-green-800" },
  { emoji: "\uD83C\uDF24", label: "曇り", description: "やや困難", color: "bg-yellow-100 text-yellow-800" },
  { emoji: "\uD83C\uDF27", label: "雨", description: "困難", color: "bg-orange-100 text-orange-800" },
  { emoji: "\u26C8", label: "嵐", description: "非常に困難", color: "bg-red-100 text-red-800" },
];

// ===================================================
// テーブルカラム定義
// ===================================================

const HIRING_COLUMNS: ColumnDef<HiringDifficultyRow>[] = [
  {
    key: "weather",
    label: "天気",
    width: "60px",
    render: (value: string) => (
      <span className="text-xl" role="img" aria-label="採用天気">
        {value}
      </span>
    ),
  },
  {
    key: "prefecture",
    label: "都道府県",
    sortable: true,
    width: "100px",
    render: (value: string) => (
      <span className="font-medium text-gray-900">{value}</span>
    ),
  },
  {
    key: "difficulty_score",
    label: "スコア",
    sortable: true,
    width: "80px",
    render: (value: number) => {
      const color =
        value >= 75
          ? "text-red-600 font-bold"
          : value >= 50
          ? "text-orange-600 font-semibold"
          : value >= 25
          ? "text-yellow-600"
          : "text-green-600";
      return <span className={color}>{value.toFixed(1)}</span>;
    },
  },
  {
    key: "avg_turnover_rate",
    label: "離職率",
    sortable: true,
    width: "80px",
    render: (value: number) => (
      <span className="tabular-nums">{(value * 100).toFixed(1)}%</span>
    ),
  },
  {
    key: "job_offers_rate",
    label: "求人倍率",
    sortable: true,
    width: "90px",
    render: (value: number) => (
      <span className="tabular-nums">{value.toFixed(2)}倍</span>
    ),
  },
  {
    key: "avg_monthly_wage",
    label: "平均月給",
    sortable: true,
    width: "100px",
    render: (value: number) => (
      <span className="tabular-nums">
        {Math.round(value).toLocaleString("ja-JP")}円
      </span>
    ),
  },
  {
    key: "facility_count",
    label: "施設数",
    sortable: true,
    width: "80px",
    render: (value: number) => (
      <span className="tabular-nums">
        {value.toLocaleString("ja-JP")}
      </span>
    ),
  },
];

// ===================================================
// メインコンテンツ
// ===================================================

function HiringWeatherContent() {
  const [sort, setSort] = useState<SortState>({
    key: "difficulty_score",
    direction: "desc",
  });

  // APIデータ取得
  const { data, error, isLoading } = useApi<HiringDifficultyRow[]>(
    "/api/external/hiring-difficulty"
  );

  // 外部API: 欠員率データ
  const { data: vacancyData, isLoading: vacancyLoading } = useApi<ExternalVacancyStats[]>(
    "/api/external/vacancy-stats"
  );

  const rows = data ?? [];

  // ソート済みデータ
  const sortedData = useMemo(() => {
    if (rows.length === 0) return [];
    const sorted = [...rows].sort((a, b) => {
      const aVal = a[sort.key as keyof HiringDifficultyRow];
      const bVal = b[sort.key as keyof HiringDifficultyRow];
      if (aVal == null || bVal == null) return 0;
      if (typeof aVal === "string" && typeof bVal === "string") {
        return sort.direction === "asc"
          ? aVal.localeCompare(bVal, "ja")
          : bVal.localeCompare(aVal, "ja");
      }
      const numA = Number(aVal);
      const numB = Number(bVal);
      return sort.direction === "asc" ? numA - numB : numB - numA;
    });
    return sorted;
  }, [rows, sort]);

  // KPI算出
  const kpiData = useMemo(() => {
    if (rows.length === 0) return null;
    const avgScore =
      rows.reduce((sum, r) => sum + r.difficulty_score, 0) / rows.length;
    const avgTurnover =
      rows.reduce((sum, r) => sum + r.avg_turnover_rate, 0) / rows.length;

    const hardest = [...rows].sort(
      (a, b) => b.difficulty_score - a.difficulty_score
    )[0];
    const easiest = [...rows].sort(
      (a, b) => a.difficulty_score - b.difficulty_score
    )[0];

    return { avgScore, avgTurnover, hardest, easiest };
  }, [rows]);

  // 欠員率データ（職種別棒グラフ用）
  const vacancyChartData = useMemo(() => {
    if (!vacancyData) return [];
    return [...vacancyData]
      .sort((a, b) => b.vacancy_rate - a.vacancy_rate)
      .slice(0, 15)
      .map((d) => ({
        label: `${d.occupation}（${d.prefecture}）`,
        vacancy_rate: Math.round(d.vacancy_rate * 1000) / 10,
        fill_rate: Math.round(d.fill_rate * 1000) / 10,
      }));
  }, [vacancyData]);

  return (
    <div className="space-y-6">
      {/* ページタイトル */}
      <div>
        <h1 className="text-heading-lg text-gray-900">採用天気予報</h1>
        <p className="text-body-md text-gray-500 mt-1">
          都道府県別の介護人材採用難易度
        </p>
      </div>

      {/* データソースバナー */}
      <div
        className="rounded-lg border border-amber-200 bg-amber-50 px-4 py-3"
        role="note"
        aria-label="データソース情報"
      >
        <div className="flex items-start gap-2">
          <svg className="w-5 h-5 text-amber-600 mt-0.5 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={1.5} aria-hidden="true">
            <path strokeLinecap="round" strokeLinejoin="round" d="M11.25 11.25l.041-.02a.75.75 0 011.063.852l-.708 2.836a.75.75 0 001.063.853l.041-.021M21 12a9 9 0 11-18 0 9 9 0 0118 0zm-9-3.75h.008v.008H12V8.25z" />
          </svg>
          <div className="text-sm text-amber-800">
            <p className="font-medium">
              データソース: 介護情報公表システム（離職率）+ 総務省統計局（求人倍率・平均賃金は全産業平均）
            </p>
            <p className="mt-1 text-amber-700 flex items-center gap-2">
              注意: 求人倍率・平均賃金は全産業平均値です。介護業界固有の値ではありません。
              <ConfidenceBadge level="medium" />
            </p>
          </div>
        </div>
      </div>

      {/* APIエラーバナー */}
      <ApiErrorBanner error={error} />

      {/* KPIカード */}
      <KpiCardGrid>
        <KpiCard
          label="全国平均スコア"
          value={kpiData?.avgScore ?? null}
          format="decimal"
          icon={IconScore}
          subtitle="100点満点（高いほど困難）"
          loading={isLoading}
          accentColor="bg-brand-500"
          tooltip="離職率・求人倍率・賃金水準から算出した採用難易度スコア"
        />
        <KpiCard
          label="最も採用困難な地域"
          value={kpiData?.hardest?.difficulty_score ?? null}
          format="decimal"
          icon={IconRegion}
          subtitle={kpiData?.hardest?.prefecture ?? "---"}
          loading={isLoading}
          accentColor="bg-red-500"
        />
        <KpiCard
          label="最も採用しやすい地域"
          value={kpiData?.easiest?.difficulty_score ?? null}
          format="decimal"
          icon={IconEasy}
          subtitle={kpiData?.easiest?.prefecture ?? "---"}
          loading={isLoading}
          accentColor="bg-emerald-500"
        />
        <KpiCard
          label="平均離職率"
          value={kpiData?.avgTurnover ?? null}
          format="percent"
          icon={IconTurnover}
          subtitle="全国平均"
          loading={isLoading}
          accentColor="bg-amber-500"
        />
      </KpiCardGrid>

      {/* 天気凡例 */}
      <div className="bg-white rounded-xl shadow-card p-4">
        <h2 className="text-sm font-medium text-gray-700 mb-3">天気凡例</h2>
        <div className="flex flex-wrap gap-3" role="list" aria-label="天気凡例">
          {WEATHER_LEGEND.map((item) => (
            <div
              key={item.emoji}
              className={`inline-flex items-center gap-2 px-3 py-1.5 rounded-full text-sm ${item.color}`}
              role="listitem"
            >
              <span className="text-lg" role="img" aria-label={item.label}>
                {item.emoji}
              </span>
              <span className="font-medium">{item.label}</span>
              <span className="opacity-75">({item.description})</span>
            </div>
          ))}
        </div>
      </div>

      {/* データテーブル */}
      <div className="bg-white rounded-xl shadow-card overflow-hidden">
        <div className="px-5 py-4 border-b border-gray-100">
          <h2 className="text-base font-semibold text-gray-900">
            都道府県別 採用難易度一覧
          </h2>
          <p className="text-sm text-gray-500 mt-0.5">
            スコア降順 / カラムヘッダーをクリックでソート切替
          </p>
        </div>
        <DataTable
          columns={HIRING_COLUMNS}
          data={sortedData}
          loading={isLoading}
          currentSort={sort}
          onSort={setSort}
        />
      </div>

      {/* 外部統計: 欠員率データ */}
      <div className="border-t border-gray-200 pt-6 mt-2">
        <h2 className="text-lg font-semibold text-gray-800 mb-1">欠員率データ</h2>
        <p className="text-sm text-gray-500 mb-4">
          職種・地域別の欠員率（充足されていないポジションの割合）
        </p>
      </div>

      <ChartCard
        title="欠員率ランキング（Top 15）"
        subtitle="職種 x 地域別の欠員率比較"
        loading={vacancyLoading}
      >
        {vacancyChartData.length > 0 ? (
          <>
            <BarChart
              data={vacancyChartData}
              xKey="label"
              yKey="vacancy_rate"
              color="#f59e0b"
              horizontal
              tooltipFormatter={(v) => `${v}%`}
              height={420}
            />
            <p className="text-xs text-gray-400 mt-2 px-1">
              データソース: 厚生労働省 介護労働実態調査
            </p>
          </>
        ) : (
          <div className="flex items-center justify-center h-[300px] text-gray-400 text-sm">
            {vacancyLoading ? "読み込み中..." : "データがありません"}
          </div>
        )}
      </ChartCard>
    </div>
  );
}

export default function HiringWeatherPage() {
  return (
    <Suspense fallback={<div className="text-gray-400 text-sm p-8">読み込み中...</div>}>
      <HiringWeatherContent />
    </Suspense>
  );
}
