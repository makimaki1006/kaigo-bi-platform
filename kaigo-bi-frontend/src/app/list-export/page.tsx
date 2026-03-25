"use client";

// ===================================================
// Page 15: リスト生成
// フィルタ → 件数表示 → プレビュー → CSVダウンロード
// バックエンドsnake_caseエンドポイント対応版
// ===================================================

import { Suspense } from "react";
import { useApi } from "@/hooks/useApi";
import { useFilters } from "@/hooks/useFilters";
import type {
  FacilitySearchResult,
  FacilityRow,
  ColumnDef,
} from "@/lib/types";
import { formatNumber, formatServiceName } from "@/lib/formatters";
import KpiCard from "@/components/data-display/KpiCard";
import KpiCardGrid from "@/components/data-display/KpiCardGrid";
import DataTable from "@/components/data-display/DataTable";
import FilterPanel from "@/components/filters/FilterPanel";
import ExportButton from "@/components/common/ExportButton";
import ApiErrorBanner from "@/components/common/ApiErrorBanner";

/** プレビューテーブルのカラム定義（snake_caseキーに合わせる） */
const PREVIEW_COLUMNS: ColumnDef<FacilityRow>[] = [
  { key: "jigyosho_name", label: "事業所名", width: "22%" },
  { key: "corp_name", label: "法人名", width: "18%" },
  { key: "phone", label: "電話番号", width: "12%" },
  { key: "prefecture", label: "都道府県", width: "10%" },
  {
    key: "service_name",
    label: "サービス種別",
    width: "16%",
    render: (value) => formatServiceName(value as string),
  },
  {
    key: "staff_total",
    label: "従業者数",
    width: "10%",
    render: (value) => formatNumber(value as number),
  },
  {
    key: "capacity",
    label: "定員",
    width: "8%",
    render: (value) => formatNumber(value as number),
  },
];

function ListExportContent() {
  const { filters, setFilters, toApiParams } = useFilters();

  // プレビューデータ取得（/api/facilities/search をプレビューとして利用）
  const { data: searchResult, error: searchError, isLoading } = useApi<FacilitySearchResult>(
    "/api/facilities/search",
    {
      ...toApiParams(),
      page: 1,
      per_page: 20,
    }
  );

  const matchCount = searchResult?.total ?? null;
  const preview = searchResult?.items ?? [];

  // フィルタが設定されているかどうか
  const hasFilters =
    filters.prefectures.length > 0 ||
    filters.serviceCodes.length > 0 ||
    filters.corpTypes.length > 0 ||
    filters.employeeMin != null ||
    filters.employeeMax != null;

  return (
    <div className="space-y-6">
      {/* ページタイトル */}
      <div>
        <h1 className="text-xl font-bold text-gray-900">リスト生成</h1>
        <p className="text-sm text-gray-500 mt-1">
          フィルタ条件を指定して施設リストをCSV出力
        </p>
      </div>

      {/* APIエラーバナー */}
      <ApiErrorBanner error={searchError} />

      {/* フィルタパネル（拡張版: 全フィルタ項目） */}
      <FilterPanel
        filters={filters}
        onChange={setFilters}
      />

      {/* 該当件数 + CSVボタン */}
      <div className="flex items-center justify-between">
        <KpiCardGrid>
          <KpiCard
            label="該当件数"
            value={matchCount}
            format="number"
            icon={<svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M16 4h2a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2h2" /><rect width="8" height="4" x="8" y="2" rx="1" ry="1" /><path d="M9 14h6" /><path d="M9 18h6" /><path d="M9 10h6" /></svg>}
            accentColor="bg-brand-500"
            subtitle={
              hasFilters
                ? "フィルタ条件に一致する施設数"
                : "全施設（フィルタ未設定）"
            }
            loading={isLoading}
          />
        </KpiCardGrid>

        <div className="flex-shrink-0 ml-6">
          <ExportButton
            filters={filters}
            disabled={isLoading || matchCount === 0}
          />
        </div>
      </div>

      {/* プレビューテーブル */}
      <div>
        <div className="flex items-center justify-between mb-3">
          <h3 className="text-sm font-semibold text-gray-700">
            プレビュー（上位20件）
          </h3>
          {preview.length > 0 && (
            <p className="text-xs text-gray-400">
              {formatNumber(matchCount)}件中 {preview.length}件を表示
            </p>
          )}
        </div>

        <DataTable<FacilityRow>
          columns={PREVIEW_COLUMNS}
          data={preview}
          loading={isLoading}
        />
      </div>

      {/* 使い方ガイド */}
      {!hasFilters && !isLoading && (
        <div className="bg-blue-50 border border-blue-100 rounded-lg p-4">
          <h4 className="text-sm font-medium text-blue-800 mb-2">
            リスト生成の手順
          </h4>
          <ol className="text-sm text-blue-700 space-y-1 list-decimal list-inside">
            <li>上部のフィルタパネルで条件を指定</li>
            <li>該当件数とプレビューを確認</li>
            <li>「CSVダウンロード」ボタンでリストを取得</li>
          </ol>
        </div>
      )}
    </div>
  );
}

export default function ListExportPage() {
  return (
    <Suspense fallback={<div className="text-gray-400 text-sm p-8">読み込み中...</div>}>
      <ListExportContent />
    </Suspense>
  );
}
