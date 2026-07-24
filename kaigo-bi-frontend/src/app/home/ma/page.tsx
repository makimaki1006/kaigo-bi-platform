"use client";

// ===================================================
// M&Aホーム（M&Aワークスペース）
// ソーシング→スクリーニング→DD→PMIの起点
// ===================================================

import { Suspense } from "react";
import Link from "next/link";
import { useApi } from "@/hooks/useApi";
import type { DashboardKpiExtended } from "@/lib/types";
import KpiCard from "@/components/data-display/KpiCard";
import KpiCardGrid from "@/components/data-display/KpiCardGrid";
import ApiErrorBanner from "@/components/common/ApiErrorBanner";

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

function MaHomeContent() {
  const { data: kpi, error, isLoading } = useApi<DashboardKpiExtended>("/api/dashboard/kpi");

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-heading-lg text-gray-900">M&Aホーム</h1>
        <p className="text-body-md text-gray-500 mt-1">
          候補発掘からデューデリジェンス・PMIまでを一気通貫で
        </p>
      </div>

      <ApiErrorBanner error={error} />

      <KpiCardGrid>
        <KpiCard label="対象法人数" value={kpi?.total_corps} format="number" subtitle="全国のユニーク法人" loading={isLoading} accentColor="bg-brand-500" />
        <KpiCard label="対象施設数" value={kpi?.total_facilities} format="number" subtitle="全国の介護事業所" loading={isLoading} accentColor="bg-emerald-500" />
        <KpiCard label="全国平均離職率" value={kpi?.avg_turnover_rate} format="percent" subtitle="人材リスクの基準線" loading={isLoading} accentColor="bg-amber-500" />
        <KpiCard label="平均営業年数" value={kpi?.avg_years} format="decimal" subtitle="事業継続の目安" loading={isLoading} accentColor="bg-sky-500" />
      </KpiCardGrid>

      {/* ソーシングの起点 */}
      <div className="p-4 bg-purple-50 border border-purple-200 rounded-xl">
        <p className="text-sm font-semibold text-gray-900">
          売却期待度の高い法人を探す
          <span className="ml-2 text-[10px] font-normal text-purple-600 bg-purple-100 border border-purple-200 rounded px-1.5 py-0.5">
            決算PDF AI抽出
          </span>
        </p>
        <p className="text-xs text-gray-600 mt-1">
          スクリーニングの財務フィルタで「債務超過」「営業赤字」「行政処分歴」の法人を抽出できます。
          法人ページでは経営危険度スコア・労働生産性・実人件費率などのクロス指標を確認できます。
        </p>
        <Link
          href="/ma-screening"
          className="inline-block mt-2 px-4 py-2 bg-brand-500 text-white text-xs font-semibold rounded-lg hover:bg-brand-600 transition-colors"
        >
          M&Aスクリーニングへ →
        </Link>
      </div>

      <div>
        <h2 className="text-sm font-semibold text-gray-700 mb-3">M&Aメニュー</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
          <ActionCard href="/ma-screening" title="M&Aスクリーニング" description="条件×財務フィルタで買収候補を抽出" />
          <ActionCard href="/due-diligence" title="DD支援" description="法人を選んで事業/人事/コンプラ/財務DDを自動生成" />
          <ActionCard href="/pmi-synergy" title="PMIシナジー" description="買い手×売り手の統合効果シミュレーション" />
          <ActionCard href="/corp-compare" title="法人比較" description="複数法人の横並び比較" />
          <ActionCard href="/growth" title="成長性分析" description="開設ペース・拠点展開の分析" />
          <ActionCard href="/financial-health" title="財務健全度" description="地域×サービスの経営体力マップ" />
        </div>
      </div>
    </div>
  );
}

export default function MaHomePage() {
  return (
    <Suspense fallback={<div className="text-gray-400 text-sm p-8">読み込み中...</div>}>
      <MaHomeContent />
    </Suspense>
  );
}
