"use client";

// ===================================================
// 経営ホーム（経営支援ワークスペース）
// 介護事業者向け: 全国サマリー + 経営分析への導線
// ===================================================

import { Suspense } from "react";
import Link from "next/link";
import { useApi } from "@/hooks/useApi";
import type { DashboardKpiExtended } from "@/lib/types";
import KpiCard from "@/components/data-display/KpiCard";
import KpiCardGrid from "@/components/data-display/KpiCardGrid";
import ApiErrorBanner from "@/components/common/ApiErrorBanner";

/** 導線カード */
function ActionCard({ href, title, description }: { href: string; title: string; description: string }) {
  return (
    <Link
      href={href}
      className="block p-4 bg-white border border-gray-200 rounded-xl hover:border-brand-300 hover:shadow-md transition-all"
    >
      <p className="text-sm font-semibold text-gray-900">{title}</p>
      <p className="text-xs text-gray-500 mt-1">{description}</p>
      <p className="text-xs text-brand-500 mt-2">開く →</p>
    </Link>
  );
}

function ManagementHomeContent() {
  const { data: kpi, error, isLoading } = useApi<DashboardKpiExtended>("/api/dashboard/kpi");

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-heading-lg text-gray-900">経営ホーム</h1>
        <p className="text-body-md text-gray-500 mt-1">
          自施設の経営を全国223,103施設のデータと比較して改善する
        </p>
      </div>

      <ApiErrorBanner error={error} />

      {/* 全国サマリー（比較の基準線） */}
      <KpiCardGrid>
        <KpiCard label="全国平均離職率" value={kpi?.avg_turnover_rate} format="percent" subtitle="全国の施設平均" loading={isLoading} accentColor="bg-amber-500" />
        <KpiCard label="全国平均従業者数" value={kpi?.avg_staff} format="decimal" subtitle="1施設あたり" loading={isLoading} accentColor="bg-sky-500" />
        <KpiCard label="全国平均常勤比率" value={kpi?.avg_fulltime_ratio} format="percent" subtitle="常勤/全従業者" loading={isLoading} accentColor="bg-emerald-500" />
        <KpiCard label="平均加算取得数" value={kpi?.avg_kasan_count} format="decimal" subtitle="1施設あたり" loading={isLoading} accentColor="bg-brand-500" />
      </KpiCardGrid>

      {/* まず自施設を見つける */}
      <div className="p-4 bg-brand-50 border border-brand-200 rounded-xl">
        <p className="text-sm font-semibold text-gray-900">はじめに: 自施設を探す</p>
        <p className="text-xs text-gray-600 mt-1">
          施設マスタで自施設を検索し、個別ページで地域平均との比較・加算取得状況・品質スコアを確認できます。
        </p>
        <Link
          href="/facilities"
          className="inline-block mt-2 px-4 py-2 bg-brand-500 text-white text-xs font-semibold rounded-lg hover:bg-brand-600 transition-colors"
        >
          施設マスタで自施設を検索 →
        </Link>
      </div>

      {/* 経営分析メニュー */}
      <div>
        <h2 className="text-sm font-semibold text-gray-700 mb-3">経営分析メニュー</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
          <ActionCard href="/benchmark" title="ベンチマーク" description="自施設を全国・都道府県・同サービスの分布と比較" />
          <ActionCard href="/workforce" title="人材分析" description="職種別人員・資格・離職率の全国動向" />
          <ActionCard href="/salary" title="賃金分析" description="職種×地域の賃金相場（処遇の妥当性確認）" />
          <ActionCard href="/quality" title="経営品質" description="BCP・第三者評価・品質スコアの取得状況" />
          <ActionCard href="/revenue" title="収益構造" description="加算取得率と収益構造の分析" />
          <ActionCard href="/cost-estimation" title="コスト推定" description="人員構成からの人件費推定" />
        </div>
      </div>
    </div>
  );
}

export default function ManagementHomePage() {
  return (
    <Suspense fallback={<div className="text-gray-400 text-sm p-8">読み込み中...</div>}>
      <ManagementHomeContent />
    </Suspense>
  );
}
