"use client";

// ===================================================
// Page: サービスポートフォリオ
// 法人のサービス種別組み合わせ分析
// /api/external/service-portfolio エンドポイント使用
// ===================================================

import { Suspense, useMemo, useState } from "react";
import { useApi } from "@/hooks/useApi";
import { SERVICE_TYPES } from "@/lib/constants";
import KpiCard from "@/components/data-display/KpiCard";
import KpiCardGrid from "@/components/data-display/KpiCardGrid";
import DataTable from "@/components/data-display/DataTable";
import ChartCard from "@/components/charts/ChartCard";
import DonutChart from "@/components/charts/DonutChart";
import ApiErrorBanner from "@/components/common/ApiErrorBanner";
import ConfidenceBadge from "@/components/common/ConfidenceBadge";
import type { ColumnDef, SortState } from "@/lib/types";

// ===================================================
// 型定義
// ===================================================

/** サービス組み合わせ */
interface ServiceCombination {
  services: string[];
  count: number;
  service_names: string[];
}

/** サービス間共起 */
interface ServiceCooccurrence {
  service_a: string;
  service_b: string;
  cooccurrence_count: number;
  pct_of_a: number;
  pct_of_b: number;
}

/** APIレスポンス全体 */
interface ServicePortfolioResponse {
  total_corps: number;
  single_service_corps: number;
  multi_service_corps: number;
  service_combinations: ServiceCombination[];
  service_cooccurrence: ServiceCooccurrence[];
  notes: string[];
  data_sources: string[];
}

// ===================================================
// KPIアイコン
// ===================================================

/** 法人数アイコン */
const IconCorps = (
  <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={1.5} aria-hidden="true">
    <path strokeLinecap="round" strokeLinejoin="round" d="M2.25 21h19.5M3.75 3v18m4.5-18v18m4.5-18v18m4.5-18v18M5.25 3h13.5M5.25 21h13.5M9 6.75h1.5m-1.5 3h1.5m-1.5 3h1.5m3-6H15m-1.5 3H15m-1.5 3H15M9 21v-3.375c0-.621.504-1.125 1.125-1.125h3.75c.621 0 1.125.504 1.125 1.125V21" />
  </svg>
);

/** 多サービスアイコン */
const IconMulti = (
  <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={1.5} aria-hidden="true">
    <path strokeLinecap="round" strokeLinejoin="round" d="M3.75 6A2.25 2.25 0 016 3.75h2.25A2.25 2.25 0 0110.5 6v2.25a2.25 2.25 0 01-2.25 2.25H6a2.25 2.25 0 01-2.25-2.25V6zM3.75 15.75A2.25 2.25 0 016 13.5h2.25a2.25 2.25 0 012.25 2.25V18a2.25 2.25 0 01-2.25 2.25H6A2.25 2.25 0 013.75 18v-2.25zM13.5 6a2.25 2.25 0 012.25-2.25H18A2.25 2.25 0 0120.25 6v2.25A2.25 2.25 0 0118 10.5h-2.25a2.25 2.25 0 01-2.25-2.25V6zM13.5 15.75a2.25 2.25 0 012.25-2.25H18a2.25 2.25 0 012.25 2.25V18A2.25 2.25 0 0118 20.25h-2.25A2.25 2.25 0 0113.5 18v-2.25z" />
  </svg>
);

/** 単一サービスアイコン */
const IconSingle = (
  <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={1.5} aria-hidden="true">
    <path strokeLinecap="round" strokeLinejoin="round" d="M5.25 7.5A2.25 2.25 0 017.5 5.25h9a2.25 2.25 0 012.25 2.25v9a2.25 2.25 0 01-2.25 2.25h-9a2.25 2.25 0 01-2.25-2.25v-9z" />
  </svg>
);

// ===================================================
// テーブル用行型
// ===================================================

interface CombinationRow {
  service_names_joined: string;
  count: number;
}

interface CooccurrenceRow {
  service_a: string;
  service_b: string;
  cooccurrence_count: number;
  pct_of_a: number;
  pct_of_b: number;
}

// ===================================================
// テーブルカラム定義
// ===================================================

const COMBINATION_COLUMNS: ColumnDef<CombinationRow>[] = [
  {
    key: "service_names_joined",
    label: "サービス組み合わせ",
    sortable: true,
    width: "400px",
    render: (value: string) => (
      <span className="font-medium text-gray-900 text-sm">{value}</span>
    ),
  },
  {
    key: "count",
    label: "法人数",
    sortable: true,
    width: "100px",
    render: (value: number) => (
      <span className="tabular-nums font-semibold">
        {value.toLocaleString("ja-JP")}
      </span>
    ),
  },
];

const COOCCURRENCE_COLUMNS: ColumnDef<CooccurrenceRow>[] = [
  {
    key: "service_a",
    label: "サービスA",
    sortable: true,
    width: "180px",
    render: (value: string) => (
      <span className="font-medium text-gray-900">{value}</span>
    ),
  },
  {
    key: "service_b",
    label: "サービスB",
    sortable: true,
    width: "180px",
    render: (value: string) => (
      <span className="font-medium text-gray-900">{value}</span>
    ),
  },
  {
    key: "cooccurrence_count",
    label: "共起数",
    sortable: true,
    width: "80px",
    render: (value: number) => (
      <span className="tabular-nums">{value.toLocaleString("ja-JP")}</span>
    ),
  },
  {
    key: "pct_of_a",
    label: "A中の割合",
    sortable: true,
    width: "100px",
    render: (value: number) => (
      <span className="tabular-nums">{value.toFixed(1)}%</span>
    ),
  },
  {
    key: "pct_of_b",
    label: "B中の割合",
    sortable: true,
    width: "100px",
    render: (value: number) => (
      <span className="tabular-nums">{value.toFixed(1)}%</span>
    ),
  },
];

// ===================================================
// メインコンテンツ
// ===================================================

function ServicePortfolioContent() {
  const [comboSort, setComboSort] = useState<SortState>({
    key: "count",
    direction: "desc",
  });
  const [coSort, setCoSort] = useState<SortState>({
    key: "cooccurrence_count",
    direction: "desc",
  });

  // APIデータ取得
  const { data, error, isLoading } = useApi<ServicePortfolioResponse>(
    "/api/external/service-portfolio"
  );

  // ドーナツチャートデータ
  const donutData = useMemo(() => {
    if (!data) return [];
    return [
      { name: "単一サービス法人", value: data.single_service_corps },
      { name: "多サービス法人", value: data.multi_service_corps },
    ];
  }, [data]);

  // 組み合わせテーブルデータ（Top 20）
  const combinationRows = useMemo(() => {
    if (!data?.service_combinations) return [];
    return data.service_combinations
      .slice(0, 20)
      .map((c) => ({
        service_names_joined: c.service_names.map((n: string) => SERVICE_TYPES[n] || n).join(" + "),
        count: c.count,
      }));
  }, [data]);

  // 組み合わせソート
  const sortedCombinations = useMemo(() => {
    if (combinationRows.length === 0) return [];
    return [...combinationRows].sort((a, b) => {
      const aVal = a[comboSort.key as keyof CombinationRow];
      const bVal = b[comboSort.key as keyof CombinationRow];
      if (typeof aVal === "string" && typeof bVal === "string") {
        return comboSort.direction === "asc"
          ? aVal.localeCompare(bVal, "ja")
          : bVal.localeCompare(aVal, "ja");
      }
      return comboSort.direction === "asc"
        ? Number(aVal) - Number(bVal)
        : Number(bVal) - Number(aVal);
    });
  }, [combinationRows, comboSort]);

  // 共起テーブルデータ
  const cooccurrenceRows = useMemo(() => {
    if (!data?.service_cooccurrence) return [];
    return data.service_cooccurrence.map((c) => ({
      service_a: SERVICE_TYPES[c.service_a] || c.service_a,
      service_b: SERVICE_TYPES[c.service_b] || c.service_b,
      cooccurrence_count: c.cooccurrence_count,
      pct_of_a: c.pct_of_a,
      pct_of_b: c.pct_of_b,
    }));
  }, [data]);

  // 共起ソート
  const sortedCooccurrence = useMemo(() => {
    if (cooccurrenceRows.length === 0) return [];
    return [...cooccurrenceRows].sort((a, b) => {
      const aVal = a[coSort.key as keyof CooccurrenceRow];
      const bVal = b[coSort.key as keyof CooccurrenceRow];
      if (typeof aVal === "string" && typeof bVal === "string") {
        return coSort.direction === "asc"
          ? aVal.localeCompare(bVal, "ja")
          : bVal.localeCompare(aVal, "ja");
      }
      return coSort.direction === "asc"
        ? Number(aVal) - Number(bVal)
        : Number(bVal) - Number(aVal);
    });
  }, [cooccurrenceRows, coSort]);

  // 単一サービス割合
  const singleRatio = useMemo(() => {
    if (!data || data.total_corps === 0) return null;
    return data.single_service_corps / data.total_corps;
  }, [data]);

  return (
    <div className="space-y-6">
      {/* ページタイトル */}
      <div>
        <h1 className="text-heading-lg text-gray-900">サービスポートフォリオ</h1>
        <p className="text-body-md text-gray-500 mt-1">
          法人のサービス種別組み合わせ分析
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
          <p className="text-sm text-amber-800 flex items-center gap-2 flex-wrap">
            データソース: {data?.data_sources?.join("、") || "介護情報公表システム（法人番号による名寄せ）"}
            <ConfidenceBadge level="high" />
          </p>
        </div>
      </div>

      {/* APIエラーバナー */}
      <ApiErrorBanner error={error} />

      {/* KPIカード */}
      <KpiCardGrid>
        <KpiCard
          label="総法人数"
          value={data?.total_corps ?? null}
          format="number"
          icon={IconCorps}
          subtitle="分析対象法人"
          loading={isLoading}
          accentColor="bg-brand-500"
        />
        <KpiCard
          label="多サービス法人数"
          value={data?.multi_service_corps ?? null}
          format="number"
          icon={IconMulti}
          subtitle="2種別以上運営"
          loading={isLoading}
          accentColor="bg-sky-500"
        />
        <KpiCard
          label="単一サービス法人割合"
          value={singleRatio}
          format="percent"
          icon={IconSingle}
          subtitle="1種別のみ運営"
          loading={isLoading}
          accentColor="bg-amber-500"
        />
      </KpiCardGrid>

      {/* ドーナツチャート + 組み合わせテーブル */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* ドーナツチャート */}
        <ChartCard
          title="単一 vs 多サービス法人"
          subtitle="法人のサービス展開構成"
          loading={isLoading}
        >
          <DonutChart
            data={donutData}
            nameKey="name"
            valueKey="value"
            centerLabel="法人数"
            height={320}
            colors={["#4f46e5", "#059669"]}
          />
        </ChartCard>

        {/* 組み合わせテーブル */}
        <div className="bg-white rounded-xl shadow-card overflow-hidden">
          <div className="px-5 py-4 border-b border-gray-100">
            <h2 className="text-base font-semibold text-gray-900">
              よくある組み合わせ Top 20
            </h2>
            <p className="text-sm text-gray-500 mt-0.5">
              法人が運営するサービス種別の組み合わせ
            </p>
          </div>
          <DataTable
            columns={COMBINATION_COLUMNS}
            data={sortedCombinations}
            loading={isLoading}
            currentSort={comboSort}
            onSort={setComboSort}
          />
        </div>
      </div>

      {/* 共起率テーブル */}
      <div className="bg-white rounded-xl shadow-card overflow-hidden">
        <div className="px-5 py-4 border-b border-gray-100">
          <h2 className="text-base font-semibold text-gray-900">
            サービス間共起率
          </h2>
          <p className="text-sm text-gray-500 mt-0.5">
            あるサービスを運営する法人が別のサービスも運営している割合
          </p>
        </div>
        <DataTable
          columns={COOCCURRENCE_COLUMNS}
          data={sortedCooccurrence}
          loading={isLoading}
          currentSort={coSort}
          onSort={setCoSort}
        />
      </div>
    </div>
  );
}

export default function ServicePortfolioPage() {
  return (
    <Suspense fallback={<div className="text-gray-400 text-sm p-8">読み込み中...</div>}>
      <ServicePortfolioContent />
    </Suspense>
  );
}
