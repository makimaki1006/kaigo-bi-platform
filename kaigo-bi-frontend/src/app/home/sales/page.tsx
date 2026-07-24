"use client";

// ===================================================
// 営業ホーム（営業支援ワークスペース）
// 介護業界に営業する企業向け: リスト作成の起点
// ===================================================

import { Suspense, useEffect, useState } from "react";
import Link from "next/link";
import { apiRequest } from "@/lib/api-client";
import { useApi } from "@/hooks/useApi";
import type { DashboardKpiExtended } from "@/lib/types";
import KpiCard from "@/components/data-display/KpiCard";
import KpiCardGrid from "@/components/data-display/KpiCardGrid";
import ApiErrorBanner from "@/components/common/ApiErrorBanner";

interface ExportUsage {
  used: number;
  limit: number | null;
  plan: string;
}

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

function SalesHomeContent() {
  const { data: kpi, error, isLoading } = useApi<DashboardKpiExtended>("/api/dashboard/kpi");
  const [usage, setUsage] = useState<ExportUsage | null>(null);

  // エクスポート残量（proプラン以上のみ成功する）
  useEffect(() => {
    apiRequest<ExportUsage>("/api/export/usage", { method: "GET" })
      .then(setUsage)
      .catch(() => {
        // 権限なしプランは表示しない
      });
  }, []);

  const remaining =
    usage?.limit != null ? Math.max(0, usage.limit - usage.used) : null;

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-heading-lg text-gray-900">営業ホーム</h1>
        <p className="text-body-md text-gray-500 mt-1">
          全国223,103施設から条件に合う営業先リストを作成する
        </p>
      </div>

      <ApiErrorBanner error={error} />

      <KpiCardGrid>
        <KpiCard label="総施設数" value={kpi?.total_facilities} format="number" subtitle="全国の介護事業所" loading={isLoading} accentColor="bg-brand-500" />
        <KpiCard label="法人数" value={kpi?.total_corps} format="number" subtitle="ユニーク法人" loading={isLoading} accentColor="bg-emerald-500" />
        <KpiCard
          label="今月のDL残量"
          value={remaining}
          format="number"
          subtitle={usage?.limit != null ? `上限 ${usage.limit.toLocaleString()} 行/月` : "プロプランで利用可"}
          accentColor="bg-sky-500"
        />
        <KpiCard label="電話番号保有率" value={1.0} format="percent" subtitle="全施設に電話番号あり" accentColor="bg-amber-500" />
      </KpiCardGrid>

      {/* リスト作成フロー */}
      <div className="p-4 bg-brand-50 border border-brand-200 rounded-xl">
        <p className="text-sm font-semibold text-gray-900">リスト作成の流れ</p>
        <ol className="text-xs text-gray-600 mt-1 list-decimal list-inside space-y-0.5">
          <li>リスト生成で地域・サービス種別・規模の条件を設定</li>
          <li>プレビューで件数と中身を確認</li>
          <li>CSVダウンロード（Excel対応・電話番号/代表者/法人情報つき）</li>
        </ol>
        <Link
          href="/list-export"
          className="inline-block mt-2 px-4 py-2 bg-brand-500 text-white text-xs font-semibold rounded-lg hover:bg-brand-600 transition-colors"
        >
          リスト生成をはじめる →
        </Link>
      </div>

      <div>
        <h2 className="text-sm font-semibold text-gray-700 mb-3">営業支援メニュー</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
          <ActionCard href="/list-export" title="リスト生成" description="条件指定→CSVダウンロード（月間クレジット制）" />
          <ActionCard href="/facilities" title="施設マスタ" description="個別施設の検索・詳細確認（電話番号・代表者）" />
          <ActionCard href="/market" title="市場構造" description="攻めるべき地域をマップで把握" />
        </div>
      </div>
    </div>
  );
}

export default function SalesHomePage() {
  return (
    <Suspense fallback={<div className="text-gray-400 text-sm p-8">読み込み中...</div>}>
      <SalesHomeContent />
    </Suspense>
  );
}
