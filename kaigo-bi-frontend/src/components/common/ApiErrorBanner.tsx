"use client";

// ===================================================
// APIエラーバナーコンポーネント
// 色分けされたエラー表示（ネットワーク/認証/サーバー/その他）
// ===================================================

import { useState } from "react";
import Link from "next/link";
import type { ApiErrorInfo } from "@/hooks/useApi";

interface ApiErrorBannerProps {
  error: ApiErrorInfo | null;
}

export default function ApiErrorBanner({ error }: ApiErrorBannerProps) {
  const [dismissed, setDismissed] = useState(false);

  if (!error || dismissed) return null;

  // 403はプラン制限（アップグレード誘導）として専用表示する
  if (error.status === 403) {
    return (
      <div
        className="p-4 bg-indigo-50 border border-indigo-200 rounded-xl text-sm text-indigo-800 flex items-start gap-3"
        role="alert"
      >
        <svg
          className="w-5 h-5 text-indigo-400 flex-shrink-0 mt-0.5"
          fill="none"
          stroke="currentColor"
          viewBox="0 0 24 24"
          strokeWidth={2}
          aria-hidden="true"
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            d="M16.5 10.5V6.75a4.5 4.5 0 10-9 0v3.75m-.75 11.25h10.5a2.25 2.25 0 002.25-2.25v-6.75a2.25 2.25 0 00-2.25-2.25H6.75a2.25 2.25 0 00-2.25 2.25v6.75a2.25 2.25 0 002.25 2.25z"
          />
        </svg>
        <div className="flex-1">
          <span className="font-semibold">上位プランの機能です: </span>
          <span>{error.message}</span>
          <Link
            href="/pricing"
            className="inline-block ml-2 px-3 py-1 bg-brand-500 text-white text-xs font-semibold rounded-lg hover:bg-brand-600 transition-colors"
          >
            料金プランを見る →
          </Link>
        </div>
        <button
          onClick={() => setDismissed(true)}
          className="text-indigo-400 hover:opacity-70 flex-shrink-0 p-0.5"
          aria-label="閉じる"
        >
          <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24" aria-hidden="true">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
          </svg>
        </button>
      </div>
    );
  }

  // 色分け: ネットワーク=amber, 認証=red, サーバー=orange, その他=red
  const styles = error.isNetworkError
    ? { bg: "bg-amber-50", border: "border-amber-200", text: "text-amber-700", icon: "text-amber-400" }
    : error.isAuthError
    ? { bg: "bg-red-50", border: "border-red-200", text: "text-red-700", icon: "text-red-400" }
    : error.isServerError
    ? { bg: "bg-orange-50", border: "border-orange-200", text: "text-orange-700", icon: "text-orange-400" }
    : { bg: "bg-red-50", border: "border-red-200", text: "text-red-700", icon: "text-red-400" };

  const label = error.isNetworkError
    ? "接続エラー"
    : error.isAuthError
    ? "認証エラー"
    : error.isServerError
    ? "サーバーエラー"
    : "エラー";

  return (
    <div
      className={`p-3 ${styles.bg} border ${styles.border} rounded-xl text-sm ${styles.text} flex items-start gap-2`}
      role="alert"
    >
      <svg
        className={`w-5 h-5 ${styles.icon} flex-shrink-0 mt-0.5`}
        fill="none"
        stroke="currentColor"
        viewBox="0 0 24 24"
        aria-hidden="true"
      >
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeWidth={2}
          d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"
        />
      </svg>
      <div className="flex-1">
        <span className="font-medium">{label}: </span>
        <span>{error.message}</span>
      </div>
      <button
        onClick={() => setDismissed(true)}
        className={`${styles.text} hover:opacity-70 flex-shrink-0 p-0.5`}
        aria-label="エラーを閉じる"
      >
        <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24" aria-hidden="true">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
        </svg>
      </button>
    </div>
  );
}
