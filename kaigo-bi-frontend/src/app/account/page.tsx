"use client";

// ===================================================
// アカウント・契約管理ページ
// 現在のプラン表示、エクスポート使用量、
// Stripe Customer Portal（支払い管理）への導線
// ===================================================

import { useState, useCallback, useEffect, Suspense } from "react";
import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { useAuthContext } from "@/components/auth/AuthProvider";
import { apiRequest } from "@/lib/api-client";

/** プラン表示情報 */
const PLAN_LABELS: Record<string, { name: string; color: string }> = {
  free: { name: "フリー", color: "bg-gray-100 text-gray-600" },
  standard: { name: "スタンダード", color: "bg-blue-100 text-blue-700" },
  pro: { name: "プロ", color: "bg-brand-100 text-brand-700" },
  ma: { name: "M&A", color: "bg-purple-100 text-purple-700" },
};

interface ExportUsage {
  used: number;
  limit: number | null;
  plan: string;
}

function AccountContent() {
  const { user } = useAuthContext();
  const searchParams = useSearchParams();
  const checkoutResult = searchParams.get("checkout");

  const [usage, setUsage] = useState<ExportUsage | null>(null);
  const [portalLoading, setPortalLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // エクスポート使用量を取得（pro/ma/adminのみ成功する）
  useEffect(() => {
    apiRequest<ExportUsage>("/api/export/usage", { method: "GET" })
      .then(setUsage)
      .catch(() => {
        // フリー/スタンダードは403になるため無視
      });
  }, []);

  // Stripe Customer Portalを開く
  const handleOpenPortal = useCallback(async () => {
    setError(null);
    setPortalLoading(true);
    try {
      const res = await apiRequest<{ url: string }>("/api/billing/portal", {
        method: "POST",
      });
      if (res.url) {
        window.location.href = res.url;
      }
    } catch (err) {
      setError(
        err instanceof Error ? err.message : "管理ページを開けませんでした。"
      );
    } finally {
      setPortalLoading(false);
    }
  }, []);

  if (!user) return null;

  const planInfo = PLAN_LABELS[user.plan] ?? PLAN_LABELS.free;

  return (
    <div className="max-w-3xl mx-auto py-10 px-4">
      <h1 className="text-2xl font-bold text-gray-900 mb-1">アカウント</h1>
      <p className="text-sm text-gray-500 mb-8">契約プランと利用状況の管理</p>

      {/* 決済完了メッセージ */}
      {checkoutResult === "success" && (
        <div className="mb-6 p-4 bg-green-50 border border-green-200 rounded-xl text-sm text-green-700">
          ご購入ありがとうございます。プランの反映には数秒かかる場合があります。
          反映されない場合は一度ログアウトして再ログインしてください。
        </div>
      )}

      {/* エラーメッセージ */}
      {error && (
        <div className="mb-6 p-4 bg-red-50 border border-red-200 rounded-xl text-sm text-red-700">
          {error}
        </div>
      )}

      {/* プロフィール */}
      <section className="bg-white border border-gray-200 rounded-2xl p-6 mb-6">
        <h2 className="text-sm font-semibold text-gray-900 mb-4">プロフィール</h2>
        <dl className="space-y-3 text-sm">
          <div className="flex justify-between">
            <dt className="text-gray-500">お名前</dt>
            <dd className="text-gray-900 font-medium">{user.name}</dd>
          </div>
          <div className="flex justify-between">
            <dt className="text-gray-500">メールアドレス</dt>
            <dd className="text-gray-900 font-medium">{user.email}</dd>
          </div>
        </dl>
      </section>

      {/* 契約プラン */}
      <section className="bg-white border border-gray-200 rounded-2xl p-6 mb-6">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-sm font-semibold text-gray-900">契約プラン</h2>
          <span
            className={`px-3 py-1 rounded-full text-xs font-semibold ${planInfo.color}`}
          >
            {planInfo.name}
          </span>
        </div>

        {/* エクスポート使用量（pro/ma/adminのみ） */}
        {usage && usage.limit !== null && usage.limit > 0 && (
          <div className="mb-5">
            <div className="flex justify-between text-xs text-gray-500 mb-1.5">
              <span>今月のCSVエクスポート</span>
              <span className="tabular-nums">
                {usage.used.toLocaleString()} / {usage.limit.toLocaleString()} 行
              </span>
            </div>
            <div className="h-2 bg-gray-100 rounded-full overflow-hidden">
              <div
                className="h-full bg-brand-500 rounded-full transition-all"
                style={{
                  width: `${Math.min(100, (usage.used / usage.limit) * 100)}%`,
                }}
              />
            </div>
          </div>
        )}

        <div className="flex flex-wrap gap-3">
          {user.plan === "free" ? (
            <Link
              href="/pricing"
              className="px-4 py-2 bg-brand-500 text-white text-sm font-semibold rounded-xl hover:bg-brand-600 transition-colors"
            >
              プランをアップグレード
            </Link>
          ) : (
            <>
              <button
                type="button"
                onClick={handleOpenPortal}
                disabled={portalLoading}
                className="px-4 py-2 bg-brand-500 text-white text-sm font-semibold rounded-xl hover:bg-brand-600 disabled:opacity-60 transition-colors"
              >
                {portalLoading ? "読み込み中..." : "支払い・プラン管理"}
              </button>
              <Link
                href="/pricing"
                className="px-4 py-2 border border-gray-300 text-gray-600 text-sm font-semibold rounded-xl hover:bg-gray-50 transition-colors"
              >
                プラン比較を見る
              </Link>
            </>
          )}
        </div>

        <p className="text-xs text-gray-400 mt-4">
          プラン変更・解約・請求書の確認はStripeの管理ページで行えます。
        </p>
      </section>
    </div>
  );
}

export default function AccountPage() {
  // 認証ガードはAppShellのProtectedRouteが担うため、ここではSuspenseのみ
  return (
    <Suspense fallback={null}>
      <AccountContent />
    </Suspense>
  );
}
