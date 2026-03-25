"use client";

// ===================================================
// Page C1: データ品質ダッシュボード
// BIデータの鮮度・カバレッジ・信頼性を可視化
// /api/meta, /api/dashboard/by-service, /api/dashboard/by-prefecture 使用
// ===================================================

import { Suspense, useMemo } from "react";
import { useApi } from "@/hooks/useApi";
import type { DataMeta, ServiceSummary, PrefectureSummary, ColumnDef } from "@/lib/types";
import { formatServiceName } from "@/lib/formatters";
import KpiCard from "@/components/data-display/KpiCard";
import KpiCardGrid from "@/components/data-display/KpiCardGrid";
import BarChart from "@/components/charts/BarChart";
import ChartCard from "@/components/charts/ChartCard";
import DataTable from "@/components/data-display/DataTable";
import ApiErrorBanner from "@/components/common/ApiErrorBanner";
import ConfidenceBadge from "@/components/common/ConfidenceBadge";

// ===================================================
// 定数
// ===================================================

/** 全サービス種別数 */
const TOTAL_SERVICE_TYPES = 34;
/** 全都道府県数 */
const TOTAL_PREFECTURES = 47;

// ===================================================
// KPIアイコン
// ===================================================

/** 施設数アイコン */
const IconFacilities = (
  <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={1.5} aria-hidden="true">
    <path strokeLinecap="round" strokeLinejoin="round" d="M2.25 21h19.5M3.75 3v18m4.5-18v18m4.5-18v18m4.5-18v18M5.25 3h13.5M5.25 21h13.5M9 6.75h1.5m-1.5 3h1.5m-1.5 3h1.5m3-6H15m-1.5 3H15m-1.5 3H15M9 21v-3.375c0-.621.504-1.125 1.125-1.125h3.75c.621 0 1.125.504 1.125 1.125V21" />
  </svg>
);

/** サービスカバレッジアイコン */
const IconService = (
  <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={1.5} aria-hidden="true">
    <path strokeLinecap="round" strokeLinejoin="round" d="M3.75 6A2.25 2.25 0 016 3.75h2.25A2.25 2.25 0 0110.5 6v2.25a2.25 2.25 0 01-2.25 2.25H6a2.25 2.25 0 01-2.25-2.25V6zM3.75 15.75A2.25 2.25 0 016 13.5h2.25a2.25 2.25 0 012.25 2.25V18a2.25 2.25 0 01-2.25 2.25H6A2.25 2.25 0 013.75 18v-2.25zM13.5 6a2.25 2.25 0 012.25-2.25H18A2.25 2.25 0 0120.25 6v2.25A2.25 2.25 0 0118 10.5h-2.25a2.25 2.25 0 01-2.25-2.25V6zM13.5 15.75a2.25 2.25 0 012.25-2.25H18a2.25 2.25 0 012.25 2.25V18A2.25 2.25 0 0118 20.25h-2.25A2.25 2.25 0 0113.5 18v-2.25z" />
  </svg>
);

/** 都道府県カバレッジアイコン */
const IconCoverage = (
  <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={1.5} aria-hidden="true">
    <path strokeLinecap="round" strokeLinejoin="round" d="M9 6.75V15m6-6v8.25m.503 3.498l4.875-2.437c.381-.19.622-.58.622-1.006V4.82c0-.836-.88-1.38-1.628-1.006l-3.869 1.934c-.317.159-.69.159-1.006 0L9.503 3.252a1.125 1.125 0 00-1.006 0L3.622 5.689C3.24 5.88 3 6.27 3 6.695V19.18c0 .836.88 1.38 1.628 1.006l3.869-1.934c.317-.159.69-.159 1.006 0l4.994 2.497c.317.158.69.158 1.006 0z" />
  </svg>
);

/** 鮮度アイコン */
const IconFreshness = (
  <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={1.5} aria-hidden="true">
    <path strokeLinecap="round" strokeLinejoin="round" d="M12 6v6h4.5m4.5 0a9 9 0 11-18 0 9 9 0 0118 0z" />
  </svg>
);

// ===================================================
// サービス種別テーブルカラム定義
// ===================================================

interface ServiceCoverageRow {
  service_name: string;
  facility_count: number;
  total_staff: number;
}

const SERVICE_COLUMNS: ColumnDef<ServiceCoverageRow>[] = [
  {
    key: "service_name",
    label: "サービス種別",
    sortable: true,
    width: "250px",
    render: (value: string) => (
      <span className="font-medium text-gray-900">{value}</span>
    ),
  },
  {
    key: "facility_count",
    label: "施設数",
    sortable: true,
    width: "100px",
    render: (value: number) => (
      <span className="tabular-nums">{value.toLocaleString("ja-JP")}</span>
    ),
  },
  {
    key: "total_staff",
    label: "従業員数合計",
    sortable: true,
    width: "120px",
    render: (value: number) => (
      <span className="tabular-nums">{Math.round(value).toLocaleString("ja-JP")}</span>
    ),
  },
];

// ===================================================
// メインコンテンツ
// ===================================================

function DataQualityContent() {
  // メタデータ取得
  const { data: meta, error: metaError, isLoading: metaLoading } = useApi<DataMeta>(
    "/api/meta"
  );

  // サービス種別別データ
  const { data: byService, error: serviceError, isLoading: serviceLoading } = useApi<ServiceSummary[]>(
    "/api/dashboard/by-service"
  );

  // 都道府県別データ
  const { data: byPrefecture, error: prefError, isLoading: prefLoading } = useApi<PrefectureSummary[]>(
    "/api/dashboard/by-prefecture"
  );

  const apiError = metaError || serviceError || prefError;
  const isLoading = metaLoading || serviceLoading || prefLoading;

  // サービスカバレッジ数
  const serviceCoverageCount = meta?.service_codes?.length ?? 0;
  // 都道府県カバレッジ数
  // 都道府県数は最大47（無効なデータが含まれる場合があるためキャップ）
  const prefectureCoverageCount = Math.min(meta?.prefectures?.length ?? 0, 47);

  // サービス種別テーブルデータ
  const serviceTableData = useMemo(() => {
    if (!byService) return [];
    return [...byService]
      .sort((a, b) => b.facility_count - a.facility_count)
      .map((item) => ({
        service_name: formatServiceName(item.service_name),
        facility_count: item.facility_count,
        total_staff: item.total_staff,
      }));
  }, [byService]);

  // 都道府県チャートデータ（上位20件）
  const prefChartData = useMemo(() => {
    if (!byPrefecture) return [];
    return [...byPrefecture]
      .sort((a, b) => b.facility_count - a.facility_count)
      .slice(0, 20);
  }, [byPrefecture]);

  return (
    <div className="space-y-6">
      {/* ページタイトル */}
      <div>
        <h1 className="text-heading-lg text-gray-900">データ品質</h1>
        <p className="text-body-md text-gray-500 mt-1">
          BIデータの鮮度・カバレッジ・信頼性
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
            データソース: 厚労省介護サービス情報公表システム + 総務省統計局（全産業平均）
            <ConfidenceBadge level="high" />
          </p>
        </div>
      </div>

      {/* APIエラーバナー */}
      <ApiErrorBanner error={apiError} />

      {/* KPIカード */}
      <KpiCardGrid>
        <KpiCard
          label="総施設数"
          value={meta?.total_count ?? null}
          format="number"
          icon={IconFacilities}
          subtitle="登録済み施設数"
          loading={metaLoading}
          accentColor="bg-brand-500"
        />
        <KpiCard
          label="取得済みサービス種別"
          value={serviceCoverageCount}
          format="number"
          icon={IconService}
          subtitle={`全${TOTAL_SERVICE_TYPES}種別中`}
          loading={metaLoading}
          accentColor="bg-sky-500"
          tooltip="介護サービス情報公表システムから取得完了したサービス種別数"
        />
        <KpiCard
          label="都道府県カバレッジ"
          value={prefectureCoverageCount}
          format="number"
          icon={IconCoverage}
          subtitle={`全${TOTAL_PREFECTURES}都道府県中`}
          loading={metaLoading}
          accentColor="bg-emerald-500"
        />
        <KpiCard
          label="データ鮮度"
          value={null}
          format="number"
          icon={IconFreshness}
          subtitle="2026-03-22 時点の取得データ"
          loading={metaLoading}
          accentColor="bg-amber-500"
        />
      </KpiCardGrid>

      {/* カバレッジ進捗バー */}
      <div className="bg-white rounded-xl shadow-card p-5">
        <h2 className="text-sm font-medium text-gray-700 mb-4">カバレッジ進捗</h2>
        <div className="space-y-4">
          {/* サービス種別 */}
          <div>
            <div className="flex justify-between text-sm mb-1">
              <span className="text-gray-600">サービス種別</span>
              <span className="text-gray-900 font-medium tabular-nums">
                {serviceCoverageCount} / {TOTAL_SERVICE_TYPES}
                <span className="text-gray-400 ml-1">
                  ({Math.round((serviceCoverageCount / TOTAL_SERVICE_TYPES) * 100)}%)
                </span>
              </span>
            </div>
            <div className="w-full bg-gray-200 rounded-full h-2.5" role="progressbar" aria-valuenow={serviceCoverageCount} aria-valuemin={0} aria-valuemax={TOTAL_SERVICE_TYPES}>
              <div
                className="bg-sky-500 h-2.5 rounded-full transition-all duration-500"
                style={{
                  width: `${Math.round((serviceCoverageCount / TOTAL_SERVICE_TYPES) * 100)}%`,
                }}
              />
            </div>
          </div>
          {/* 都道府県 */}
          <div>
            <div className="flex justify-between text-sm mb-1">
              <span className="text-gray-600">都道府県</span>
              <span className="text-gray-900 font-medium tabular-nums">
                {prefectureCoverageCount} / {TOTAL_PREFECTURES}
                <span className="text-gray-400 ml-1">
                  ({Math.round((prefectureCoverageCount / TOTAL_PREFECTURES) * 100)}%)
                </span>
              </span>
            </div>
            <div className="w-full bg-gray-200 rounded-full h-2.5" role="progressbar" aria-valuenow={prefectureCoverageCount} aria-valuemin={0} aria-valuemax={TOTAL_PREFECTURES}>
              <div
                className="bg-emerald-500 h-2.5 rounded-full transition-all duration-500"
                style={{
                  width: `${Math.round((prefectureCoverageCount / TOTAL_PREFECTURES) * 100)}%`,
                }}
              />
            </div>
          </div>
        </div>
      </div>

      {/* チャート + テーブル */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* 都道府県別施設数チャート */}
        <ChartCard
          title="都道府県別施設数（Top 20）"
          subtitle="データが存在する都道府県の施設数"
          loading={prefLoading}
        >
          {prefChartData.length > 0 ? (
            <BarChart
              data={prefChartData}
              xKey="prefecture"
              yKey="facility_count"
              color="#4f46e5"
              height={400}
            />
          ) : (
            <div className="h-[400px] flex items-center justify-center text-gray-400 text-sm">
              データがありません
            </div>
          )}
        </ChartCard>

        {/* サービス種別別テーブル */}
        <div className="bg-white rounded-xl shadow-card overflow-hidden">
          <div className="px-5 py-4 border-b border-gray-100">
            <h3 className="text-base font-semibold text-gray-900">
              サービス種別別件数
            </h3>
            <p className="text-sm text-gray-500 mt-0.5">
              取得済みサービス種別と施設件数
            </p>
          </div>
          <DataTable
            columns={SERVICE_COLUMNS}
            data={serviceTableData}
            loading={serviceLoading}
          />
        </div>
      </div>

      {/* データソースと制約事項 */}
      <div className="bg-white rounded-xl shadow-card p-5">
        <h2 className="text-base font-semibold text-gray-900 mb-3">
          データソースと制約事項
        </h2>
        <div className="space-y-3">
          <div className="flex items-start gap-3">
            <span className="flex-shrink-0 w-5 h-5 rounded-full bg-blue-100 text-blue-600 flex items-center justify-center text-xs font-bold mt-0.5" aria-hidden="true">
              1
            </span>
            <p className="text-sm text-gray-700">
              介護サービス情報公表システム（厚生労働省）の公開情報から取得したデータです。
            </p>
          </div>
          <div className="flex items-start gap-3">
            <span className="flex-shrink-0 w-5 h-5 rounded-full bg-blue-100 text-blue-600 flex items-center justify-center text-xs font-bold mt-0.5" aria-hidden="true">
              2
            </span>
            <p className="text-sm text-gray-700">
              全{TOTAL_SERVICE_TYPES}サービス種別中、現在
              <span className="font-semibold text-brand-700">{serviceCoverageCount}種別</span>
              が取得完了しています。
            </p>
          </div>
          <div className="flex items-start gap-3">
            <span className="flex-shrink-0 w-5 h-5 rounded-full bg-amber-100 text-amber-600 flex items-center justify-center text-xs font-bold mt-0.5" aria-hidden="true">
              3
            </span>
            <p className="text-sm text-gray-700">
              賃金データの充填率は0.1%のため、都道府県別平均値（全産業）で代替しています。
            </p>
          </div>
          <div className="flex items-start gap-3">
            <span className="flex-shrink-0 w-5 h-5 rounded-full bg-amber-100 text-amber-600 flex items-center justify-center text-xs font-bold mt-0.5" aria-hidden="true">
              4
            </span>
            <p className="text-sm text-gray-700">
              土地・建物コストは概算値であり、個別施設の実態とは異なる場合があります。
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}

export default function DataQualityPage() {
  return (
    <Suspense fallback={<div className="text-gray-400 text-sm p-8">読み込み中...</div>}>
      <DataQualityContent />
    </Suspense>
  );
}
