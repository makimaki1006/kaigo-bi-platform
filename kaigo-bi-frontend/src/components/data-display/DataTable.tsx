"use client";

// ===================================================
// データテーブルコンポーネント
// Tremor Table脱却、独自テーブル
// ゼブラストライプ、ホバー、ページネーション刷新
// ===================================================

import { useCallback } from "react";
import type { ColumnDef, SortState, PaginationState } from "@/lib/types";

interface DataTableProps<T extends Record<string, any>> {
  /** カラム定義 */
  columns: ColumnDef<T>[];
  /** テーブルデータ */
  data: T[];
  /** ソートコールバック */
  onSort?: (sort: SortState) => void;
  /** 行クリックコールバック */
  onRowClick?: (row: T) => void;
  /** ページネーション状態 */
  pagination?: PaginationState;
  /** ページ変更コールバック */
  onPageChange?: (page: number) => void;
  /** ローディング状態 */
  loading?: boolean;
  /** 現在のソート状態 */
  currentSort?: SortState;
}

/** テーブル用スケルトン */
function TableSkeleton({ columns }: { columns: number }) {
  return (
    <div className="overflow-hidden">
      {/* ヘッダースケルトン */}
      <div className="bg-gray-50 border-b border-gray-200 px-4 py-3 flex gap-4">
        {Array.from({ length: columns }).map((_, i) => (
          <div key={i} className="flex-1 h-3 shimmer rounded" />
        ))}
      </div>
      {/* 行スケルトン */}
      {Array.from({ length: 8 }).map((_, rowIdx) => (
        <div
          key={rowIdx}
          className={`px-4 py-3 flex gap-4 border-b border-gray-100 ${
            rowIdx % 2 === 1 ? "bg-gray-50/50" : ""
          }`}
        >
          {Array.from({ length: columns }).map((_, colIdx) => (
            <div
              key={colIdx}
              className="flex-1 h-4 shimmer rounded"
              style={{ animationDelay: `${(rowIdx * columns + colIdx) * 50}ms` }}
            />
          ))}
        </div>
      ))}
    </div>
  );
}

export default function DataTable<T extends Record<string, any>>({
  columns,
  data,
  onSort,
  onRowClick,
  pagination,
  onPageChange,
  loading = false,
  currentSort,
}: DataTableProps<T>) {
  // ソートハンドラ
  const handleSort = useCallback(
    (key: string) => {
      if (!onSort) return;
      const direction =
        currentSort?.key === key && currentSort.direction === "asc"
          ? "desc"
          : "asc";
      onSort({ key, direction });
    },
    [currentSort, onSort]
  );

  // ソートアイコン
  const renderSortIcon = (key: string) => {
    if (!currentSort || currentSort.key !== key) {
      return (
        <svg className="w-3.5 h-3.5 text-gray-300 ml-1" viewBox="0 0 14 14" fill="currentColor" aria-hidden="true">
          <path d="M7 3L10 7H4L7 3ZM7 11L4 7H10L7 11Z" />
        </svg>
      );
    }
    return currentSort.direction === "asc" ? (
      <svg className="w-3.5 h-3.5 text-brand-500 ml-1" viewBox="0 0 14 14" fill="currentColor" aria-hidden="true">
        <path d="M7 3L11 9H3L7 3Z" />
      </svg>
    ) : (
      <svg className="w-3.5 h-3.5 text-brand-500 ml-1" viewBox="0 0 14 14" fill="currentColor" aria-hidden="true">
        <path d="M7 11L3 5H11L7 11Z" />
      </svg>
    );
  };

  if (loading) {
    return <TableSkeleton columns={Math.min(columns.length, 6)} />;
  }

  if (data.length === 0) {
    return (
      <div className="text-center py-16 text-gray-400 text-sm">
        <svg className="w-10 h-10 mx-auto mb-3 text-gray-300" fill="none" stroke="currentColor" viewBox="0 0 24 24" aria-hidden="true">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M20 13V6a2 2 0 00-2-2H6a2 2 0 00-2 2v7m16 0v5a2 2 0 01-2 2H6a2 2 0 01-2-2v-5m16 0h-2.586a1 1 0 00-.707.293l-2.414 2.414a1 1 0 01-.707.293h-3.172a1 1 0 01-.707-.293l-2.414-2.414A1 1 0 006.586 13H4" />
        </svg>
        該当するデータがありません
      </div>
    );
  }

  return (
    <div>
      {/* テーブル本体 */}
      <div className="overflow-x-auto content-scroll">
        <table className="w-full">
          <thead>
            <tr className="bg-gray-50 border-b border-gray-200">
              {columns.map((col) => (
                <th
                  key={col.key}
                  className={`px-4 py-3 text-left text-label-sm uppercase tracking-wider text-gray-500 whitespace-nowrap ${
                    col.sortable
                      ? "cursor-pointer select-none hover:text-brand-600 transition-colors"
                      : ""
                  }`}
                  style={col.width ? { width: col.width } : undefined}
                  onClick={() => col.sortable && handleSort(col.key)}
                  scope="col"
                >
                  <span className="inline-flex items-center">
                    {col.label}
                    {col.sortable && renderSortIcon(col.key)}
                  </span>
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {data.map((row, idx) => (
              <tr
                key={idx}
                className={`transition-colors duration-150 ${
                  idx % 2 === 1 ? "bg-gray-50/50" : "bg-white"
                } ${
                  onRowClick
                    ? "cursor-pointer hover:bg-brand-50/30"
                    : "hover:bg-gray-50"
                }`}
                onClick={() => onRowClick?.(row)}
              >
                {columns.map((col) => (
                  <td
                    key={col.key}
                    className="px-4 py-3 text-sm text-gray-700 whitespace-nowrap"
                  >
                    {col.render
                      ? col.render(row[col.key], row)
                      : String(row[col.key] ?? "-")}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* ページネーション */}
      {pagination && pagination.totalPages > 1 && (
        <div className="flex items-center justify-between mt-4 px-2">
          <p className="text-body-sm text-gray-500 tabular-nums">
            {pagination.total.toLocaleString("ja-JP")}件中{" "}
            {(
              (pagination.page - 1) * pagination.pageSize +
              1
            ).toLocaleString("ja-JP")}
            -
            {Math.min(
              pagination.page * pagination.pageSize,
              pagination.total
            ).toLocaleString("ja-JP")}
            件
          </p>

          <div className="flex items-center gap-1">
            {/* 前へ */}
            <button
              onClick={() => onPageChange?.(pagination.page - 1)}
              disabled={pagination.page <= 1}
              className="px-3 py-1.5 text-sm rounded-lg border border-gray-200 disabled:opacity-30 disabled:cursor-not-allowed hover:bg-gray-50 transition-colors focus-ring"
              aria-label="前のページ"
            >
              前へ
            </button>

            {/* ページ番号 */}
            {generatePageNumbers(pagination.page, pagination.totalPages).map(
              (p, i) =>
                p === "..." ? (
                  <span
                    key={`dots-${i}`}
                    className="px-2 text-gray-400 select-none"
                    aria-hidden="true"
                  >
                    ...
                  </span>
                ) : (
                  <button
                    key={p}
                    onClick={() => onPageChange?.(p as number)}
                    className={`px-3 py-1.5 text-sm rounded-lg border transition-colors focus-ring tabular-nums ${
                      p === pagination.page
                        ? "bg-brand-600 text-white border-brand-600 shadow-sm"
                        : "border-gray-200 hover:bg-gray-50 text-gray-700"
                    }`}
                    aria-label={`ページ ${p}`}
                    aria-current={p === pagination.page ? "page" : undefined}
                  >
                    {p}
                  </button>
                )
            )}

            {/* 次へ */}
            <button
              onClick={() => onPageChange?.(pagination.page + 1)}
              disabled={pagination.page >= pagination.totalPages}
              className="px-3 py-1.5 text-sm rounded-lg border border-gray-200 disabled:opacity-30 disabled:cursor-not-allowed hover:bg-gray-50 transition-colors focus-ring"
              aria-label="次のページ"
            >
              次へ
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

/**
 * ページ番号リストを生成する
 * 現在ページ付近のページ番号と省略記号を返す
 */
function generatePageNumbers(
  current: number,
  total: number
): (number | "...")[] {
  if (total <= 7) {
    return Array.from({ length: total }, (_, i) => i + 1);
  }

  const pages: (number | "...")[] = [1];

  if (current > 3) {
    pages.push("...");
  }

  const start = Math.max(2, current - 1);
  const end = Math.min(total - 1, current + 1);

  for (let i = start; i <= end; i++) {
    pages.push(i);
  }

  if (current < total - 2) {
    pages.push("...");
  }

  pages.push(total);

  return pages;
}
