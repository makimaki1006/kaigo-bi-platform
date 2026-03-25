"use client";

// ===================================================
// Page 14: 施設マスタ
// 検索 + フィルタ + テーブル + 詳細パネル
// バックエンドsnake_caseエンドポイント対応版
// ===================================================

import { Suspense, useState, useCallback, useEffect, useRef } from "react";
// TextInput は Tremor の HeadlessUI v2 互換問題があるため HTML input を使用
import { useApi } from "@/hooks/useApi";
import { useFilters } from "@/hooks/useFilters";
import type {
  FacilitySearchResult,
  FacilityRow,
  ColumnDef,
  SortState,
} from "@/lib/types";
import { formatNumber } from "@/lib/formatters";
import { DEBOUNCE_DELAY, DEFAULT_PAGE_SIZE } from "@/lib/constants";
import DataTable from "@/components/data-display/DataTable";
import FacilityDetailPanel from "@/components/data-display/FacilityDetailPanel";
import FilterPanel from "@/components/filters/FilterPanel";
import ApiErrorBanner from "@/components/common/ApiErrorBanner";

/** テーブルカラム定義（snake_caseキーに合わせる） */
const COLUMNS: ColumnDef<FacilityRow>[] = [
  { key: "jigyosho_name", label: "事業所名", sortable: true, width: "20%" },
  { key: "corp_name", label: "法人名", sortable: true, width: "18%" },
  { key: "phone", label: "電話番号", sortable: false, width: "12%" },
  { key: "address", label: "住所", sortable: false, width: "25%" },
  {
    key: "staff_total",
    label: "従業者数",
    sortable: true,
    width: "10%",
    render: (value) => formatNumber(value as number),
  },
  {
    key: "capacity",
    label: "定員",
    sortable: true,
    width: "8%",
    render: (value) => formatNumber(value as number),
  },
];

function FacilitiesContent() {
  const { filters, setFilters, toApiParams } = useFilters();

  // ローカル検索キーワード（デバウンス用）
  const [searchInput, setSearchInput] = useState(filters.keyword);
  const debounceTimerRef = useRef<NodeJS.Timeout | null>(null);

  // ページネーション・ソート状態
  const [page, setPage] = useState(1);
  const [sort, setSort] = useState<SortState>({ key: "jigyosho_name", direction: "asc" });

  // 選択中の施設
  const [selectedFacilityId, setSelectedFacilityId] = useState<string | null>(null);

  // デバウンス付き検索
  useEffect(() => {
    if (debounceTimerRef.current) {
      clearTimeout(debounceTimerRef.current);
    }
    debounceTimerRef.current = setTimeout(() => {
      setFilters({ keyword: searchInput });
      setPage(1);
    }, DEBOUNCE_DELAY);

    return () => {
      if (debounceTimerRef.current) {
        clearTimeout(debounceTimerRef.current);
      }
    };
  }, [searchInput]); // setFilters は安定した参照なので依存配列から除外

  // 施設一覧取得（/api/facilities/search）
  const apiParams = toApiParams();
  const { data: searchResult, error: searchError, isLoading } = useApi<FacilitySearchResult>(
    "/api/facilities/search",
    {
      ...apiParams,
      q: apiParams.keyword || "",
      page,
      per_page: DEFAULT_PAGE_SIZE,
      sort_by: sort.key,
      sort_order: sort.direction,
    }
  );

  const facilities = searchResult?.items ?? [];

  // 施設詳細取得（/api/facilities/{jigyosho_number}）
  // APIレスポンスは {"facility": {...}} でラップされている
  const { data: detailData, error: detailError, isLoading: detailLoading } =
    useApi<{ facility: FacilityRow }>(
      selectedFacilityId ? `/api/facilities/${selectedFacilityId}` : null
    );

  // 行クリックハンドラ
  const handleRowClick = useCallback(
    (row: FacilityRow) => {
      setSelectedFacilityId(
        selectedFacilityId === row.jigyosho_number ? null : row.jigyosho_number
      );
    },
    [selectedFacilityId]
  );

  // ソート変更
  const handleSort = useCallback((newSort: SortState) => {
    setSort(newSort);
    setPage(1);
  }, []);

  // ページ変更
  const handlePageChange = useCallback((newPage: number) => {
    setPage(newPage);
    setSelectedFacilityId(null);
  }, []);

  return (
    <div className="space-y-6">
      {/* ページタイトル */}
      <div>
        <h1 className="text-xl font-bold text-gray-900">施設マスタ</h1>
        <p className="text-sm text-gray-500 mt-1">
          全施設の検索・閲覧
        </p>
      </div>

      {/* APIエラーバナー */}
      <ApiErrorBanner error={searchError || detailError} />

      {/* 検索バー */}
      <div className="max-w-lg">
        <input
          type="text"
          className="w-full px-3 py-2 text-sm border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-brand-500"
          placeholder="事業所名、法人名、住所で検索..."
          value={searchInput}
          onChange={(e) => setSearchInput(e.target.value)}
        />
      </div>

      {/* フィルタパネル（コンパクト） */}
      <FilterPanel
        filters={filters}
        onChange={(newFilters) => {
          setFilters(newFilters);
          setPage(1);
        }}
        compact
        visibleFilters={["prefectures", "serviceCodes", "corpTypes", "employeeRange", "keyword"]}
      />

      {/* 件数表示 */}
      {searchResult && (
        <p className="text-sm text-gray-500">
          検索結果: <span className="font-semibold text-gray-700">{formatNumber(searchResult.total)}</span> 件
        </p>
      )}

      {/* データテーブル */}
      <DataTable<FacilityRow>
        columns={COLUMNS}
        data={facilities}
        onSort={handleSort}
        onRowClick={handleRowClick}
        currentSort={sort}
        loading={isLoading}
        pagination={
          searchResult
            ? {
                page: searchResult.page,
                pageSize: searchResult.per_page,
                total: searchResult.total,
                totalPages: searchResult.total_pages,
              }
            : undefined
        }
        onPageChange={handlePageChange}
      />

      {/* 施設詳細パネル */}
      {selectedFacilityId && (
        <FacilityDetailPanel
          facility={detailData?.facility ?? null}
          loading={detailLoading}
          onClose={() => setSelectedFacilityId(null)}
        />
      )}
    </div>
  );
}

export default function FacilitiesPage() {
  return (
    <Suspense fallback={<div className="text-gray-400 text-sm p-8">読み込み中...</div>}>
      <FacilitiesContent />
    </Suspense>
  );
}
