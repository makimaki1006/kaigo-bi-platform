"use client";

// ===================================================
// 料金プランページ
// 4プラン（Free / Standard / Pro / M&A）の比較と
// Stripe Checkoutへの導線
// ===================================================

import { useState, useCallback } from "react";
import { useRouter } from "next/navigation";
import Link from "next/link";
import { useAuthContext } from "@/components/auth/AuthProvider";
import { apiRequest } from "@/lib/api-client";

/** プラン定義（表示用。金額はStripe側のPriceが正） */
const PLANS = [
  {
    key: "free",
    name: "フリー",
    price: "0",
    unit: "円/月",
    description: "まずは全国の介護市場を俯瞰",
    features: [
      "全国サマリーダッシュボード",
      "基本統計の閲覧",
    ],
    cta: "無料で始める",
    highlight: false,
  },
  {
    key: "standard",
    name: "スタンダード",
    price: "9,800",
    unit: "円/月",
    description: "介護事業者の経営ベンチマークに",
    features: [
      "BIダッシュボード全機能",
      "都道府県・市区町村ドリルダウン",
      "人材・給与・品質・成長分析",
      "自施設ベンチマーク",
      "外部統計（人口動態・求人倍率等）",
    ],
    cta: "スタンダードを始める",
    highlight: false,
  },
  {
    key: "pro",
    name: "プロ",
    price: "29,800",
    unit: "円/月",
    description: "介護業界への営業リスト作成に",
    features: [
      "スタンダードの全機能",
      "条件指定リスト作成",
      "CSVダウンロード（月3,000件）",
      "電話番号・法人情報つき",
    ],
    cta: "プロを始める",
    highlight: true,
  },
  {
    key: "ma",
    name: "M&A",
    price: "49,800",
    unit: "円/月",
    description: "M&Aソーシング・デューデリに",
    features: [
      "プロの全機能",
      "M&Aスクリーニング",
      "簡易デューデリジェンス",
      "PMIシナジー分析",
      "CSVダウンロード（月10,000件）",
    ],
    cta: "M&Aを始める",
    highlight: false,
  },
] as const;

export default function PricingPage() {
  const router = useRouter();
  const { user, isAuthenticated } = useAuthContext();
  const [loadingPlan, setLoadingPlan] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  // プラン選択 → 未ログインならサインアップへ、ログイン済みならStripe Checkoutへ
  const handleSelect = useCallback(
    async (planKey: string) => {
      setError(null);

      if (planKey === "free") {
        router.push(isAuthenticated ? "/dashboard" : "/signup");
        return;
      }

      if (!isAuthenticated) {
        router.push("/signup");
        return;
      }

      setLoadingPlan(planKey);
      try {
        const res = await apiRequest<{ url: string }>("/api/billing/checkout", {
          method: "POST",
          body: { plan: planKey },
        });
        if (res.url) {
          window.location.href = res.url;
        } else {
          setError("決済ページの作成に失敗しました。");
        }
      } catch (err) {
        setError(
          err instanceof Error ? err.message : "決済ページの作成に失敗しました。"
        );
      } finally {
        setLoadingPlan(null);
      }
    },
    [isAuthenticated, router]
  );

  const currentPlan = user?.plan ?? null;

  return (
    <div className="min-h-screen bg-gray-50 py-12 px-4">
      <div className="max-w-6xl mx-auto">
        {/* ヘッダー */}
        <div className="text-center mb-10">
          <h1 className="text-3xl font-bold text-gray-900 mb-3">料金プラン</h1>
          <p className="text-sm text-gray-500">
            全国223,000施設の介護事業所データを、目的に合わせたプランで。
          </p>
          {isAuthenticated ? (
            <p className="text-xs text-gray-400 mt-2">
              <Link href="/account" className="text-brand-500 hover:underline">
                現在の契約を確認する →
              </Link>
            </p>
          ) : (
            <p className="text-xs text-gray-400 mt-2">
              <Link href="/login" className="text-brand-500 hover:underline">
                ログインはこちら →
              </Link>
            </p>
          )}
        </div>

        {/* エラーメッセージ */}
        {error && (
          <div className="max-w-md mx-auto mb-8 p-3 bg-red-50 border border-red-200 rounded-xl text-sm text-red-700 text-center">
            {error}
          </div>
        )}

        {/* プランカード */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          {PLANS.map((plan) => {
            const isCurrent = currentPlan === plan.key;
            return (
              <div
                key={plan.key}
                className={`relative bg-white rounded-2xl p-6 flex flex-col ${
                  plan.highlight
                    ? "ring-2 ring-brand-500 shadow-xl"
                    : "border border-gray-200 shadow-sm"
                }`}
              >
                {plan.highlight && (
                  <span className="absolute -top-3 left-1/2 -translate-x-1/2 px-3 py-0.5 bg-brand-500 text-white text-xs font-semibold rounded-full">
                    人気
                  </span>
                )}

                <h2 className="text-lg font-semibold text-gray-900">{plan.name}</h2>
                <p className="text-xs text-gray-500 mt-1 mb-4">{plan.description}</p>

                <div className="mb-6">
                  <span className="text-3xl font-bold text-gray-900 tabular-nums">
                    {plan.price}
                  </span>
                  <span className="text-sm text-gray-500 ml-1">{plan.unit}</span>
                </div>

                <ul className="space-y-2.5 mb-8 flex-1">
                  {plan.features.map((feature) => (
                    <li key={feature} className="flex items-start gap-2 text-sm text-gray-600">
                      <svg
                        className="w-4 h-4 text-brand-500 flex-shrink-0 mt-0.5"
                        fill="none"
                        stroke="currentColor"
                        viewBox="0 0 24 24"
                        aria-hidden="true"
                      >
                        <path
                          strokeLinecap="round"
                          strokeLinejoin="round"
                          strokeWidth={2}
                          d="M5 13l4 4L19 7"
                        />
                      </svg>
                      <span>{feature}</span>
                    </li>
                  ))}
                </ul>

                <button
                  type="button"
                  onClick={() => handleSelect(plan.key)}
                  disabled={loadingPlan !== null || isCurrent}
                  className={`w-full py-2.5 text-sm font-semibold rounded-xl transition-all duration-200 ${
                    isCurrent
                      ? "bg-gray-100 text-gray-400 cursor-default"
                      : plan.highlight
                        ? "bg-brand-500 text-white hover:bg-brand-600 shadow-lg shadow-brand-500/25"
                        : "border border-brand-500 text-brand-500 hover:bg-brand-50"
                  } disabled:opacity-60`}
                >
                  {isCurrent
                    ? "ご利用中のプラン"
                    : loadingPlan === plan.key
                      ? "処理中..."
                      : plan.cta}
                </button>
              </div>
            );
          })}
        </div>

        {/* 注記 */}
        <p className="text-center text-xs text-gray-400 mt-10">
          価格は税別です。プランの変更・解約はいつでも可能です。
          決済はStripeにより安全に処理されます。
        </p>
      </div>
    </div>
  );
}
