"use client";

// ===================================================
// フィルタ状態管理フック
// URL SearchParamsと同期する
// ===================================================

import { useCallback, useMemo, useState } from "react";
import { useSearchParams, useRouter, usePathname } from "next/navigation";
import type { FilterState } from "@/lib/types";

/** フィルタの初期状態 */
const DEFAULT_FILTERS: FilterState = {
  prefectures: [],
  serviceCodes: [],
  corpTypes: [],
  employeeMin: null,
  employeeMax: null,
  keyword: "",
};

/**
 * フィルタ状態管理フック
 * URLクエリパラメータとフィルタ状態を双方向同期する
 */
export function useFilters() {
  const searchParams = useSearchParams();
  const router = useRouter();
  const pathname = usePathname();

  // URLパラメータからフィルタ状態を復元
  const filters: FilterState = useMemo(() => {
    const prefectures = searchParams.get("prefectures");
    const serviceCodes = searchParams.get("serviceCodes");
    const corpTypes = searchParams.get("corpTypes");
    const employeeMin = searchParams.get("employeeMin");
    const employeeMax = searchParams.get("employeeMax");
    const keyword = searchParams.get("keyword");

    return {
      prefectures: prefectures ? prefectures.split(",") : [],
      serviceCodes: serviceCodes ? serviceCodes.split(",") : [],
      corpTypes: corpTypes ? corpTypes.split(",") : [],
      employeeMin: employeeMin ? Number(employeeMin) : null,
      employeeMax: employeeMax ? Number(employeeMax) : null,
      keyword: keyword || "",
    };
  }, [searchParams]);

  // フィルタ更新（URLパラメータも同期）
  const setFilters = useCallback(
    (newFilters: Partial<FilterState>) => {
      const merged = { ...filters, ...newFilters };
      const params = new URLSearchParams();

      if (merged.prefectures.length > 0) {
        params.set("prefectures", merged.prefectures.join(","));
      }
      if (merged.serviceCodes.length > 0) {
        params.set("serviceCodes", merged.serviceCodes.join(","));
      }
      if (merged.corpTypes.length > 0) {
        params.set("corpTypes", merged.corpTypes.join(","));
      }
      if (merged.employeeMin != null) {
        params.set("employeeMin", String(merged.employeeMin));
      }
      if (merged.employeeMax != null) {
        params.set("employeeMax", String(merged.employeeMax));
      }
      if (merged.keyword) {
        params.set("keyword", merged.keyword);
      }

      const queryString = params.toString();
      router.push(queryString ? `${pathname}?${queryString}` : pathname);
    },
    [filters, pathname, router]
  );

  // フィルタリセット
  const resetFilters = useCallback(() => {
    router.push(pathname);
  }, [pathname, router]);

  // フィルタをAPIパラメータ形式に変換
  const toApiParams = useCallback((): Record<string, string | string[] | number | null | undefined> => {
    // バックエンドのFilterParamsに合わせたパラメータ名
    // prefecture(カンマ区切り), service_code, corp_type, staff_min, staff_max, keyword
    return {
      prefecture: filters.prefectures.length > 0 ? filters.prefectures.join(",") : undefined,
      service_code: filters.serviceCodes.length > 0 ? filters.serviceCodes.join(",") : undefined,
      corp_type: filters.corpTypes.length > 0 ? filters.corpTypes.join(",") : undefined,
      staff_min: filters.employeeMin,
      staff_max: filters.employeeMax,
      keyword: filters.keyword || undefined,
    };
  }, [filters]);

  return {
    filters,
    setFilters,
    resetFilters,
    toApiParams,
  };
}
