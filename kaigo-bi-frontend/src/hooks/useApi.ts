"use client";

// ===================================================
// SWRベースのAPI呼び出しフック
// エラーハンドリング強化版
// ===================================================

import useSWR, { SWRConfiguration } from "swr";
import { fetcher, buildUrl, ApiError } from "@/lib/api-client";

/** APIエラー情報 */
export interface ApiErrorInfo {
  status: number;
  message: string;
  isNetworkError: boolean;
  isAuthError: boolean;
  isServerError: boolean;
}

/**
 * エラーオブジェクトからApiErrorInfoに変換
 */
function parseApiError(error: unknown): ApiErrorInfo {
  if (error instanceof ApiError) {
    return {
      status: error.status,
      message: error.message,
      isNetworkError: false,
      isAuthError: error.status === 401 || error.status === 403,
      isServerError: error.status >= 500,
    };
  }

  // ネットワークエラー（fetchの失敗）
  if (error instanceof TypeError && error.message.includes("fetch")) {
    return {
      status: 0,
      message: "サーバーに接続できません。ネットワーク接続を確認してください。",
      isNetworkError: true,
      isAuthError: false,
      isServerError: false,
    };
  }

  return {
    status: 0,
    message: error instanceof Error ? error.message : "不明なエラーが発生しました",
    isNetworkError: false,
    isAuthError: false,
    isServerError: false,
  };
}

/**
 * 汎用APIフック
 * エンドポイントとパラメータを受け取り、SWRでデータを取得する
 *
 * @param endpoint - APIエンドポイント（例: "/api/dashboard"）
 * @param params - クエリパラメータ
 * @param config - SWR設定オプション
 */
export function useApi<T>(
  endpoint: string | null,
  params?: Record<string, string | number | boolean | string[] | null | undefined>,
  config?: SWRConfiguration
) {
  // エンドポイントがnullの場合はフェッチしない（条件付きフェッチ）
  const url = endpoint ? buildUrl(endpoint, params) : null;

  const { data, error, isLoading, isValidating, mutate } = useSWR<T>(
    url,
    fetcher<T>,
    {
      revalidateOnFocus: false,
      dedupingInterval: 5000,
      // ネットワークエラー時は自動リトライ（最大3回）
      errorRetryCount: 3,
      errorRetryInterval: 2000,
      // 401/403エラーはリトライしない
      onErrorRetry: (err, _key, _config, revalidate, { retryCount }) => {
        if (err instanceof ApiError && (err.status === 401 || err.status === 403)) return;
        if (retryCount >= 3) return;
        setTimeout(() => revalidate({ retryCount }), 2000 * (retryCount + 1));
      },
      ...config,
    }
  );

  // エラー解析
  const errorInfo = error ? parseApiError(error) : null;

  return {
    data,
    error: errorInfo,
    isLoading,
    isValidating,
    mutate,
    /** データが取得済みであるか（ローディング完了かつエラーなし） */
    hasData: !isLoading && !error && data != null,
  };
}
