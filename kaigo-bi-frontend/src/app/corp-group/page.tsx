"use client";

// ===================================================
// Page 08: 法人グループ分析
// 法人番号で施設を名寄せし、法人単位の事業ポートフォリオを分析
// 実API接続版
// ===================================================

import { Suspense, useMemo } from "react";
import { useApi } from "@/hooks/useApi";
import { useFilters } from "@/hooks/useFilters";
import KpiCard from "@/components/data-display/KpiCard";
import KpiCardGrid from "@/components/data-display/KpiCardGrid";
import BarChart from "@/components/charts/BarChart";
import ChartCard from "@/components/charts/ChartCard";
import DataTable from "@/components/data-display/DataTable";
import FilterPanel from "@/components/filters/FilterPanel";
import DataPendingPlaceholder from "@/components/common/DataPendingPlaceholder";
import { KpiGridSkeleton } from "@/components/common/LoadingSpinner";
import ApiErrorBanner from "@/components/common/ApiErrorBanner";
import HeatmapChart from "@/components/charts/HeatmapChart";
import { formatServiceName, formatCorpType } from "@/lib/formatters";
import type { ColumnDef, TopCorpRow, CorpGroupKpi, CorpSizeDistribution, CorpKasanHeatmapData } from "@/lib/types";

/** TOP法人テーブルのカラム定義 */
const TOP_CORP_COLUMNS: ColumnDef<TopCorpRow>[] = [
  {
    key: "corp_name",
    label: "法人名",
    sortable: true,
    width: "220px",
    render: (value: string) => (
      <span className="font-medium text-gray-900">{value}</span>
    ),
  },
  {
    key: "corp_type",
    label: "法人種別",
    sortable: true,
    width: "120px",
    render: (value: string | null) => (
      <span className="text-gray-600 text-xs">{formatCorpType(value)}</span>
    ),
  },
  {
    key: "facility_count",
    label: "施設数",
    sortable: true,
    width: "80px",
    render: (value: number | null) => (
      <span className="font-semibold text-blue-600">
        {value != null ? value.toLocaleString("ja-JP") : "-"}
      </span>
    ),
  },
  {
    key: "service_names",
    label: "サービス種別",
    sortable: false,
    width: "200px",
    render: (value: string[]) => (
      <span className="text-xs text-gray-600 line-clamp-2">
        {Array.isArray(value) ? value.map(formatServiceName).join(", ") : "-"}
      </span>
    ),
  },
  {
    key: "total_staff",
    label: "総従業者数",
    sortable: true,
    width: "100px",
    render: (value: number | null) =>
      value != null ? `${Math.round(value).toLocaleString("ja-JP")}人` : "-",
  },
  {
    key: "avg_turnover_rate",
    label: "平均離職率",
    sortable: true,
    width: "100px",
    render: (value: number | null) => {
      if (value == null) return "-";
      const pct = value * 100;
      const color =
        pct > 20 ? "text-red-600" : pct > 15 ? "text-orange-500" : "text-green-600";
      return <span className={`font-medium ${color}`}>{pct.toFixed(1)}%</span>;
    },
  },
  {
    key: "prefectures",
    label: "展開エリア",
    sortable: false,
    width: "140px",
    render: (value: string[]) => (
      <span className="text-xs text-gray-600 line-clamp-1">
        {Array.isArray(value) ? value.join(", ") : "-"}
      </span>
    ),
  },
];

/** KPIアイコン: 法人数 */
const IconCorp = (
  <svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
    <rect width="16" height="20" x="4" y="2" rx="2" ry="2" />
    <path d="M9 22v-4h6v4" /><path d="M8 6h.01" /><path d="M16 6h.01" /><path d="M12 6h.01" /><path d="M12 10h.01" /><path d="M12 14h.01" /><path d="M16 10h.01" /><path d="M16 14h.01" /><path d="M8 10h.01" /><path d="M8 14h.01" />
  </svg>
);

/** KPIアイコン: 多施設法人 */
const IconMulti = (
  <svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
    <rect x="16" y="16" width="6" height="6" rx="1" /><rect x="2" y="16" width="6" height="6" rx="1" /><rect x="9" y="2" width="6" height="6" rx="1" />
    <path d="M5 16v-3a1 1 0 0 1 1-1h12a1 1 0 0 1 1 1v3" /><path d="M12 12V8" />
  </svg>
);

/** KPIアイコン: 平均施設数 */
const IconAvg = (
  <svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
    <path d="M3 3v18h18" /><path d="M7 16V8" /><path d="M11 16V11" /><path d="M15 16v-3" />
  </svg>
);

/** KPIアイコン: 最大施設数 */
const IconMax = (
  <svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
    <path d="M11.562 3.266a.5.5 0 0 1 .876 0L15.39 8.87a1 1 0 0 0 .415.39l5.296 2.083a.5.5 0 0 1 .27.74l-3.357 4.835a1 1 0 0 0-.168.517l.038 5.666a.5.5 0 0 1-.626.5l-5.479-1.61a1 1 0 0 0-.564 0l-5.479 1.61a.5.5 0 0 1-.626-.5l.038-5.666a1 1 0 0 0-.168-.517L1.629 12.083a.5.5 0 0 1 .27-.74l5.296-2.083a1 1 0 0 0 .415-.39z" />
  </svg>
);

function CorpGroupContent() {
  const { filters, setFilters, toApiParams } = useFilters();
  const apiParams = toApiParams();

  // API呼び出し
  const { data: kpi, error: kpiError, isLoading: kpiLoading } = useApi<CorpGroupKpi>(
    "/api/corp-group/kpi",
    apiParams
  );

  const { data: sizeDistribution, error: sizeError, isLoading: sizeLoading } = useApi<CorpSizeDistribution[]>(
    "/api/corp-group/size-distribution",
    apiParams
  );

  const { data: topCorps, error: topCorpsError, isLoading: topCorpsLoading } = useApi<TopCorpRow[]>(
    "/api/corp-group/top-corps",
    { ...apiParams, limit: 20 }
  );

  // 法人内加算取得ヒートマップ（TOP法人のデータがある場合）
  const { data: kasanHeatmap, error: kasanHeatmapError, isLoading: kasanHeatmapLoading } = useApi<CorpKasanHeatmapData>(
    "/api/corp-group/kasan-heatmap",
    apiParams
  );

  const apiError = kpiError || sizeError || topCorpsError || kasanHeatmapError;

  // topCorpsデータから法人種別ごとの平均施設数を集計
  const corpTypeAvgData = useMemo(() => {
    if (!topCorps || topCorps.length === 0) return [];
    const groups: Record<string, { total: number; count: number }> = {};
    for (const corp of topCorps) {
      const ct = corp.corp_type ?? "不明";
      if (!groups[ct]) groups[ct] = { total: 0, count: 0 };
      groups[ct].total += corp.facility_count;
      groups[ct].count += 1;
    }
    return Object.entries(groups)
      .map(([corp_type, { total, count }]) => ({
        corp_type: formatCorpType(corp_type),
        avg_facilities: Math.round((total / count) * 10) / 10,
      }))
      .sort((a, b) => b.avg_facilities - a.avg_facilities);
  }, [topCorps]);

  return (
    <div className="space-y-6">
      {/* ページタイトル */}
      <div>
        <h1 className="text-xl font-bold text-gray-900">法人グループ分析</h1>
        <p className="text-sm text-gray-500 mt-1">
          法人番号で施設を名寄せし、法人単位の事業ポートフォリオと規模を分析
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

      {/* KPIカード */}
      {kpiLoading ? (
        <KpiGridSkeleton count={4} />
      ) : (
        <KpiCardGrid>
          <KpiCard
            label="総法人数"
            value={kpi?.total_corps ?? null}
            format="number"
            icon={IconCorp}
            accentColor="bg-indigo-500"
            subtitle="ユニーク法人番号ベース"
          />
          <KpiCard
            label="多施設法人数"
            value={kpi?.multi_facility_corps ?? null}
            format="number"
            icon={IconMulti}
            accentColor="bg-blue-500"
            subtitle="2施設以上を運営する法人"
          />
          <KpiCard
            label="平均施設数/法人"
            value={kpi?.avg_facilities_per_corp ?? null}
            format="decimal"
            icon={IconAvg}
            accentColor="bg-emerald-500"
            subtitle="全法人平均"
          />
          <KpiCard
            label="最大施設数法人"
            value={kpi?.max_facilities_count ?? null}
            format="number"
            icon={IconMax}
            accentColor="bg-amber-500"
            subtitle={kpi?.max_facilities_corp_name ?? ""}
          />
        </KpiCardGrid>
      )}

      {/* チャートエリア */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* 1. 法人規模別分布 */}
        <ChartCard
          title="法人規模別分布"
          subtitle="運営施設数による法人の分類"
          loading={sizeLoading}
        >
          {sizeDistribution && sizeDistribution.length > 0 ? (
            <BarChart
              data={sizeDistribution}
              xKey="category"
              yKey="count"
              color="#6366f1"
              tooltipFormatter={(v) => `${v.toLocaleString("ja-JP")}法人`}
              height={300}
            />
          ) : (
            <DataPendingPlaceholder
              message="データなし"
              description="フィルタ条件に一致するデータがありません"
              height={300}
            />
          )}
        </ChartCard>

        {/* 2. 法人種別×平均施設数（APIからtop-corpsデータを集計） */}
        <ChartCard
          title="法人種別 x 平均施設数"
          subtitle="法人種別ごとの平均運営施設数"
          loading={topCorpsLoading}
        >
          {corpTypeAvgData.length > 0 ? (
            <BarChart
              data={corpTypeAvgData}
              xKey="corp_type"
              yKey="avg_facilities"
              color="#4f46e5"
              horizontal
              tooltipFormatter={(v) => `平均 ${Number(v).toFixed(1)} 施設`}
              height={300}
            />
          ) : (
            <DataPendingPlaceholder
              message="データなし"
              description="法人種別データがありません"
              height={300}
            />
          )}
        </ChartCard>
      </div>

      {/* 法人内加算取得ヒートマップ */}
      <ChartCard
        title="法人内施設 加算取得ヒートマップ"
        subtitle="施設 x 13加算項目の取得状況（上位法人）"
        loading={kasanHeatmapLoading}
      >
        {kasanHeatmap && kasanHeatmap.facilities?.length > 0 ? (
          <HeatmapChart
            rows={kasanHeatmap.facilities}
            columns={kasanHeatmap.kasan_items}
            values={kasanHeatmap.values.map((row) =>
              row.map((v) => (v === true ? 1 : v === false ? 0 : null))
            )}
            colorScale={["#f3f4f6", "#059669"]}
            tooltipFormatter={(value, row, col) =>
              `${row} - ${col}: ${value === 1 ? "取得" : value === 0 ? "未取得" : "データなし"}`
            }
            cellHeight={28}
          />
        ) : (
          <DataPendingPlaceholder
            message="データなし"
            description="フィルタ条件に一致する法人の加算データがありません"
            height={300}
          />
        )}
      </ChartCard>

      {/* TOP20法人テーブル */}
      <ChartCard
        title="TOP20 法人ランキング"
        subtitle="施設数が多い順（実データ）"
        loading={topCorpsLoading}
      >
        {topCorps && topCorps.length > 0 ? (
          <DataTable<TopCorpRow>
            columns={TOP_CORP_COLUMNS}
            data={topCorps}
          />
        ) : (
          <DataPendingPlaceholder
            message="データなし"
            description="フィルタ条件に一致する法人がありません"
            height={300}
          />
        )}
      </ChartCard>
    </div>
  );
}

export default function CorpGroupPage() {
  return (
    <Suspense fallback={<div className="text-gray-400 text-sm p-8">読み込み中...</div>}>
      <CorpGroupContent />
    </Suspense>
  );
}
