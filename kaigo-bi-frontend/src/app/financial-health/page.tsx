"use client";

// ===================================================
// Page: 財務健全度分析
// 4指標による施設経営健全度スコアリング
// /api/external/financial-health エンドポイント使用
// ===================================================

import { Suspense, useMemo, useState } from "react";
import { useApi } from "@/hooks/useApi";
import KpiCard from "@/components/data-display/KpiCard";
import KpiCardGrid from "@/components/data-display/KpiCardGrid";
import DataTable from "@/components/data-display/DataTable";
import ApiErrorBanner from "@/components/common/ApiErrorBanner";
import ConfidenceBadge from "@/components/common/ConfidenceBadge";
import type { ColumnDef, SortState } from "@/lib/types";

// ===================================================
// 型定義
// ===================================================

/** ランク分布 */
interface RankDistribution {
  S: number;
  A: number;
  B: number;
  C: number;
  D: number;
}

/** 財務健全度APIレスポンス行 */
interface FinancialHealthRow {
  prefecture: string;
  facility_count: number;
  avg_total_score: number;
  avg_quality: number;
  avg_hr: number;
  avg_revenue: number;
  avg_stability: number;
  rank_distribution: RankDistribution;
}

/** APIレスポンス全体 */
interface FinancialHealthResponse {
  data: FinancialHealthRow[];
  notes: string[];
  data_sources: string[];
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

/** S/Aランクアイコン */
const IconRank = (
  <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={1.5} aria-hidden="true">
    <path strokeLinecap="round" strokeLinejoin="round" d="M11.48 3.499a.562.562 0 011.04 0l2.125 5.111a.563.563 0 00.475.345l5.518.442c.499.04.701.663.321.988l-4.204 3.602a.563.563 0 00-.182.557l1.285 5.385a.562.562 0 01-.84.61l-4.725-2.885a.563.563 0 00-.586 0L6.982 20.54a.562.562 0 01-.84-.61l1.285-5.386a.562.562 0 00-.182-.557l-4.204-3.602a.563.563 0 01.321-.988l5.518-.442a.563.563 0 00.475-.345L11.48 3.5z" />
  </svg>
);

/** 施設数アイコン */
const IconFacilities = (
  <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={1.5} aria-hidden="true">
    <path strokeLinecap="round" strokeLinejoin="round" d="M2.25 21h19.5M3.75 3v18m4.5-18v18m4.5-18v18m4.5-18v18M5.25 3h13.5M5.25 21h13.5M9 6.75h1.5m-1.5 3h1.5m-1.5 3h1.5m3-6H15m-1.5 3H15m-1.5 3H15M9 21v-3.375c0-.621.504-1.125 1.125-1.125h3.75c.621 0 1.125.504 1.125 1.125V21" />
  </svg>
);

// ===================================================
// テーブルカラム定義
// ===================================================

const FINANCIAL_COLUMNS: ColumnDef<FinancialHealthRow>[] = [
  {
    key: "prefecture",
    label: "都道府県",
    sortable: true,
    width: "90px",
    render: (value: string) => (
      <span className="font-medium text-gray-900">{value}</span>
    ),
  },
  {
    key: "avg_total_score",
    label: "平均スコア",
    sortable: true,
    width: "90px",
    render: (value: number) => {
      const color =
        value >= 80
          ? "text-emerald-600 font-bold"
          : value >= 60
          ? "text-blue-600 font-semibold"
          : value >= 40
          ? "text-yellow-600"
          : "text-red-600";
      return <span className={`tabular-nums ${color}`}>{value.toFixed(1)}</span>;
    },
  },
  {
    key: "avg_quality",
    label: "品質",
    sortable: true,
    width: "70px",
    render: (value: number) => (
      <span className="tabular-nums">{value.toFixed(1)}</span>
    ),
  },
  {
    key: "avg_hr",
    label: "人材",
    sortable: true,
    width: "70px",
    render: (value: number) => (
      <span className="tabular-nums">{value.toFixed(1)}</span>
    ),
  },
  {
    key: "avg_revenue",
    label: "収益",
    sortable: true,
    width: "70px",
    render: (value: number) => (
      <span className="tabular-nums">{value.toFixed(1)}</span>
    ),
  },
  {
    key: "avg_stability",
    label: "安定性",
    sortable: true,
    width: "70px",
    render: (value: number) => (
      <span className="tabular-nums">{value.toFixed(1)}</span>
    ),
  },
  {
    key: "rank_distribution",
    label: "S",
    width: "50px",
    render: (_: unknown, row: FinancialHealthRow) => (
      <span className="tabular-nums text-emerald-700 font-medium">
        {row.rank_distribution.S}
      </span>
    ),
  },
  {
    key: "rank_distribution",
    label: "A",
    width: "50px",
    render: (_: unknown, row: FinancialHealthRow) => (
      <span className="tabular-nums text-blue-600">
        {row.rank_distribution.A}
      </span>
    ),
  },
  {
    key: "rank_distribution",
    label: "B",
    width: "50px",
    render: (_: unknown, row: FinancialHealthRow) => (
      <span className="tabular-nums text-gray-600">
        {row.rank_distribution.B}
      </span>
    ),
  },
  {
    key: "rank_distribution",
    label: "C",
    width: "50px",
    render: (_: unknown, row: FinancialHealthRow) => (
      <span className="tabular-nums text-orange-600">
        {row.rank_distribution.C}
      </span>
    ),
  },
  {
    key: "rank_distribution",
    label: "D",
    width: "50px",
    render: (_: unknown, row: FinancialHealthRow) => (
      <span className="tabular-nums text-red-600">
        {row.rank_distribution.D}
      </span>
    ),
  },
];

// ===================================================
// サブスコア説明
// ===================================================

const SUB_SCORES = [
  { name: "品質スコア", weight: "25%", description: "第三者評価、BCP策定、ICT活用、加算取得数等から算出", color: "bg-emerald-100 text-emerald-800" },
  { name: "人材スコア", weight: "25%", description: "離職率、常勤比率、経験者比率、採用実績等から算出", color: "bg-blue-100 text-blue-800" },
  { name: "収益スコア", weight: "25%", description: "加算取得数、稼働率、処遇改善加算取得状況等から算出", color: "bg-violet-100 text-violet-800" },
  { name: "安定性スコア", weight: "25%", description: "事業年数、法人規模、定員充足率等から算出", color: "bg-amber-100 text-amber-800" },
];

// ===================================================
// メインコンテンツ
// ===================================================

function FinancialHealthContent() {
  const [sort, setSort] = useState<SortState>({
    key: "avg_total_score",
    direction: "desc",
  });

  // APIデータ取得
  const { data, error, isLoading } = useApi<FinancialHealthResponse>(
    "/api/external/financial-health"
  );

  const rows = data?.data ?? [];

  // ソート済みデータ
  const sortedData = useMemo(() => {
    if (rows.length === 0) return [];
    const sorted = [...rows].sort((a, b) => {
      const aVal = a[sort.key as keyof FinancialHealthRow];
      const bVal = b[sort.key as keyof FinancialHealthRow];
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
      rows.reduce((sum, r) => sum + r.avg_total_score, 0) / rows.length;

    const best = [...rows].sort(
      (a, b) => b.avg_total_score - a.avg_total_score
    )[0];

    const totalFacilities = rows.reduce((sum, r) => sum + r.facility_count, 0);

    const totalSA = rows.reduce(
      (sum, r) => sum + r.rank_distribution.S + r.rank_distribution.A,
      0
    );
    const saRatio = totalFacilities > 0 ? (totalSA / totalFacilities) * 100 : 0;

    return { avgScore, best, saRatio, totalFacilities };
  }, [rows]);

  return (
    <div className="space-y-6">
      {/* ページタイトル */}
      <div>
        <h1 className="text-heading-lg text-gray-900">財務健全度分析</h1>
        <p className="text-body-md text-gray-500 mt-1">
          4指標による施設経営健全度スコアリング
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
            <p className="font-medium flex items-center gap-2 flex-wrap">
              データソース: {data?.data_sources?.join("、") || "介護情報公表システム + 推計モデル"}
              <ConfidenceBadge level="medium" />
            </p>
            {data?.notes && data.notes.length > 0 && (
              <ul className="mt-1 text-amber-700 space-y-0.5">
                {data.notes.map((note, i) => (
                  <li key={i}>{note}</li>
                ))}
              </ul>
            )}
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
          subtitle="100点満点"
          loading={isLoading}
          accentColor="bg-brand-500"
          tooltip="品質・人材・収益・安定性の4指標を等配分で算出"
        />
        <KpiCard
          label="最高スコア都道府県"
          value={kpiData?.best?.avg_total_score ?? null}
          format="decimal"
          icon={IconRegion}
          subtitle={kpiData?.best?.prefecture ?? "---"}
          loading={isLoading}
          accentColor="bg-emerald-500"
        />
        <KpiCard
          label="S/Aランク施設割合"
          value={kpiData?.saRatio != null ? kpiData.saRatio / 100 : null}
          format="percent"
          icon={IconRank}
          subtitle="上位ランク比率"
          loading={isLoading}
          accentColor="bg-blue-500"
        />
        <KpiCard
          label="分析対象施設数"
          value={kpiData?.totalFacilities ?? null}
          format="number"
          icon={IconFacilities}
          subtitle="全国合計"
          loading={isLoading}
          accentColor="bg-amber-500"
        />
      </KpiCardGrid>

      {/* 4指標の説明 */}
      <div className="bg-white rounded-xl shadow-card p-5">
        <h2 className="text-base font-semibold text-gray-900 mb-3">
          スコア構成（4指標 各25%）
        </h2>
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-3">
          {SUB_SCORES.map((item) => (
            <div
              key={item.name}
              className={`rounded-lg px-4 py-3 ${item.color}`}
            >
              <div className="flex items-center justify-between mb-1">
                <span className="font-semibold text-sm">{item.name}</span>
                <span className="text-xs opacity-75">{item.weight}</span>
              </div>
              <p className="text-xs opacity-85">{item.description}</p>
            </div>
          ))}
        </div>
      </div>

      {/* データテーブル */}
      <div className="bg-white rounded-xl shadow-card overflow-hidden">
        <div className="px-5 py-4 border-b border-gray-100">
          <h2 className="text-base font-semibold text-gray-900">
            都道府県別 財務健全度一覧
          </h2>
          <p className="text-sm text-gray-500 mt-0.5">
            平均スコア降順 / カラムヘッダーをクリックでソート切替
          </p>
        </div>
        <DataTable
          columns={FINANCIAL_COLUMNS}
          data={sortedData}
          loading={isLoading}
          currentSort={sort}
          onSort={setSort}
        />
      </div>
    </div>
  );
}

export default function FinancialHealthPage() {
  return (
    <Suspense fallback={<div className="text-gray-400 text-sm p-8">読み込み中...</div>}>
      <FinancialHealthContent />
    </Suspense>
  );
}
