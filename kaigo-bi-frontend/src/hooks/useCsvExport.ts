"use client";

// ===================================================
// CSVエクスポートフック
// 認証トークン付きでCSVをダウンロードする
// ===================================================

import { useCallback, useEffect, useRef, useState } from "react";
import { buildCsvExportUrl, getAuthToken } from "@/lib/api-client";
import type { FilterState } from "@/lib/types";

/**
 * CSVダウンロードフック
 * フィルタ条件からCSVダウンロードURLを生成し、
 * 認証トークン付きでfetchしてブラウザダウンロードを実行する
 */
export function useCsvExport() {
  const [isExporting, setIsExporting] = useState(false);
  const [exportError, setExportError] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  // アンマウント時に進行中のリクエストを中断
  useEffect(() => {
    return () => { abortRef.current?.abort(); };
  }, []);

  /**
   * CSVダウンロード実行
   * fetch APIで認証ヘッダーを付与してダウンロードする
   */
  const downloadCsv = useCallback(async (filters: FilterState) => {
    setIsExporting(true);
    setExportError(null);

    const params: Record<string, string | string[] | number | null | undefined> = {
      prefecture: filters.prefectures.length > 0 ? filters.prefectures.join(",") : undefined,
      service_code: filters.serviceCodes.length > 0 ? filters.serviceCodes.join(",") : undefined,
      corp_type: filters.corpTypes.length > 0 ? filters.corpTypes.join(",") : undefined,
      staff_min: filters.employeeMin,
      staff_max: filters.employeeMax,
      keyword: filters.keyword || undefined,
    };

    const url = buildCsvExportUrl(params);

    try {
      // 前回のリクエストを中断し、新しいAbortControllerを作成
      abortRef.current?.abort();
      abortRef.current = new AbortController();

      // 認証ヘッダー付きでCSVをfetch
      const headers: Record<string, string> = {};
      const token = getAuthToken();
      if (token) {
        headers["Authorization"] = `Bearer ${token}`;
      }

      const response = await fetch(url, { headers, signal: abortRef.current.signal });

      if (!response.ok) {
        if (response.status === 401) {
          throw new Error("認証が必要です。再ログインしてください。");
        }
        throw new Error(`CSVエクスポートに失敗しました (${response.status})`);
      }

      // レスポンスをBlobとして取得
      const blob = await response.blob();

      // Content-Dispositionからファイル名を取得（あれば）
      const contentDisposition = response.headers.get("Content-Disposition");
      let filename = `facilities_export_${new Date().toISOString().slice(0, 10)}.csv`;
      if (contentDisposition) {
        const match = contentDisposition.match(/filename[^;=\n]*=((['"]).*?\2|[^;\n]*)/);
        if (match && match[1]) {
          filename = match[1].replace(/['"]/g, "");
        }
      }

      // ダウンロード実行
      const blobUrl = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = blobUrl;
      link.download = filename;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      URL.revokeObjectURL(blobUrl);
    } catch (err) {
      // ユーザー操作による中断は無視
      if (err instanceof DOMException && err.name === "AbortError") return;
      const message = err instanceof Error ? err.message : "CSVダウンロードに失敗しました";
      setExportError(message);
    } finally {
      setIsExporting(false);
    }
  }, []);

  /**
   * エクスポートURLを取得（プレビュー用）
   */
  const getExportUrl = useCallback((filters: FilterState): string => {
    const params: Record<string, string | string[] | number | null | undefined> = {
      prefecture: filters.prefectures.length > 0 ? filters.prefectures.join(",") : undefined,
      service_code: filters.serviceCodes.length > 0 ? filters.serviceCodes.join(",") : undefined,
      corp_type: filters.corpTypes.length > 0 ? filters.corpTypes.join(",") : undefined,
      staff_min: filters.employeeMin,
      staff_max: filters.employeeMax,
      keyword: filters.keyword || undefined,
    };

    return buildCsvExportUrl(params);
  }, []);

  return {
    downloadCsv,
    getExportUrl,
    isExporting,
    exportError,
  };
}
