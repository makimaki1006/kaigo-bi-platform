"use client";

// ===================================================
// APIエラーバナーコンポーネント
// 色分けされたエラー表示（ネットワーク/認証/サーバー/その他）
// ===================================================

import { useState } from "react";
import type { ApiErrorInfo } from "@/hooks/useApi";

interface ApiErrorBannerProps {
  error: ApiErrorInfo | null;
}

export default function ApiErrorBanner({ error }: ApiErrorBannerProps) {
  const [dismissed, setDismissed] = useState(false);

  if (!error || dismissed) return null;

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
