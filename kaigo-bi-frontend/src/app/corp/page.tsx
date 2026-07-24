"use client";

// ===================================================
// 法人詳細ページ
// /corp?number=<法人番号>
// 法人単位の統合ビュー: 概要KPI・リスク・財務諸表・施設一覧
// データソースはDDレポートAPI（M&Aプラン）
// ===================================================

import { Suspense, useMemo } from "react";
import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { useApi } from "@/hooks/useApi";
import type { DdReportResponse, FinancialRecord } from "@/lib/types";
import KpiCard from "@/components/data-display/KpiCard";
import KpiCardGrid from "@/components/data-display/KpiCardGrid";
import ChartCard from "@/components/charts/ChartCard";
import FinancialSummaryCard from "@/components/data-display/FinancialSummaryCard";
import ApiErrorBanner from "@/components/common/ApiErrorBanner";
import LoadingSpinner from "@/components/common/LoadingSpinner";

/** リスクフラグの色 */
const RISK_STYLES: Record<string, string> = {
  red: "bg-red-50 border-red-200 text-red-700",
  yellow: "bg-amber-50 border-amber-200 text-amber-700",
};

function CorpDetailContent() {
  const searchParams = useSearchParams();
  const corpNumber = searchParams.get("number") ?? "";

  const { data: report, error, isLoading } = useApi<DdReportResponse>(
    corpNumber ? `/api/dd/report/${corpNumber}` : null
  );

  // 抽出済み財務データを施設ごとにグループ化（施設名はfinancial_linksから引く）
  const financialGroups = useMemo(() => {
    const records = report?.financial_dd?.extracted_financials ?? [];
    if (records.length === 0) return [];
    const nameMap = new Map(
      (report?.financial_dd?.financial_links ?? [])
        .filter((l) => l.jigyosho_number)
        .map((l) => [l.jigyosho_number as string, l.facility_name])
    );
    const groups = new Map<string, FinancialRecord[]>();
    for (const rec of records) {
      const list = groups.get(rec.jigyosho_number) ?? [];
      list.push(rec);
      groups.set(rec.jigyosho_number, list);
    }
    return Array.from(groups.entries()).map(([jigyosho, recs]) => ({
      jigyosho,
      name: nameMap.get(jigyosho) ?? jigyosho,
      records: recs,
    }));
  }, [report]);

  if (!corpNumber) {
    return (
      <div className="p-8 text-center text-gray-500 text-sm">
        法人が指定されていません。
        <Link href="/ma-screening" className="text-brand-500 hover:underline ml-2">
          M&Aスクリーニングから探す →
        </Link>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* ヘッダー */}
      <div>
        <p className="text-xs text-gray-400">法人詳細</p>
        <h1 className="text-heading-lg text-gray-900 mt-1">
          {report?.corp_info?.corp_name ?? (isLoading ? "読み込み中..." : "法人詳細")}
        </h1>
        {report?.corp_info && (
          <p className="text-body-md text-gray-500 mt-1">
            法人番号: {report.corp_info.corp_number}
            {report.corp_info.representative && ` / 代表: ${report.corp_info.representative}`}
            {report.corp_info.prefectures.length > 0 &&
              ` / 展開: ${report.corp_info.prefectures.join("・")}`}
          </p>
        )}
      </div>

      <ApiErrorBanner error={error} />

      {isLoading && (
        <div className="flex justify-center py-16">
          <LoadingSpinner />
        </div>
      )}

      {report && (
        <>
          {/* KPIカード */}
          <KpiCardGrid>
            <KpiCard
              label="施設数"
              value={report.corp_info.facility_count}
              format="number"
              subtitle="運営事業所"
              accentColor="bg-brand-500"
            />
            <KpiCard
              label="総従業者数"
              value={report.business_dd.total_staff}
              format="number"
              subtitle="全施設合計"
              accentColor="bg-emerald-500"
            />
            <KpiCard
              label="平均離職率"
              value={report.hr_dd.avg_turnover_rate}
              format="percent"
              subtitle="施設平均"
              accentColor="bg-amber-500"
            />
            <KpiCard
              label="平均定員"
              value={report.business_dd.avg_capacity}
              format="decimal"
              subtitle="1施設あたり"
              accentColor="bg-sky-500"
            />
          </KpiCardGrid>

          {/* リスクフラグ */}
          {report.risk_flags.length > 0 && (
            <div className="space-y-2">
              {report.risk_flags.map((flag, idx) => (
                <div
                  key={idx}
                  className={`p-3 border rounded-xl text-sm ${RISK_STYLES[flag.level] ?? RISK_STYLES.yellow}`}
                >
                  <span className="font-semibold">[{flag.category}] </span>
                  {flag.detail}
                </div>
              ))}
            </div>
          )}

          {/* 抽出済み財務サマリー（決算PDF AI解析済みの施設） */}
          {financialGroups.length > 0 && (
            <div className="space-y-3">
              {financialGroups.map((group) => (
                <FinancialSummaryCard
                  key={group.jigyosho}
                  records={group.records}
                  subtitle={group.name}
                />
              ))}
            </div>
          )}

          {/* 財務諸表 */}
          <ChartCard
            title="財務諸表（公表PDF）"
            subtitle={
              report.financial_dd.financial_links.length > 0
                ? `${report.financial_dd.financial_links.length}施設が公表 / 会計処理: ${report.financial_dd.accounting_type ?? "不明"}`
                : "介護情報公表システム上の財務諸表"
            }
          >
            {report.financial_dd.financial_links.length > 0 ? (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-gray-200 text-left">
                      <th className="px-3 py-2 text-gray-500 font-medium">施設</th>
                      <th className="px-3 py-2 text-gray-500 font-medium text-center">事業活動計算書</th>
                      <th className="px-3 py-2 text-gray-500 font-medium text-center">資金収支計算書</th>
                      <th className="px-3 py-2 text-gray-500 font-medium text-center">貸借対照表</th>
                    </tr>
                  </thead>
                  <tbody>
                    {report.financial_dd.financial_links.map((link, idx) => (
                      <tr key={idx} className="border-b border-gray-100 hover:bg-gray-50">
                        <td className="px-3 py-2 text-gray-700">
                          {link.jigyosho_number ? (
                            <Link
                              href={`/facility?id=${link.jigyosho_number}`}
                              className="text-brand-600 hover:underline"
                            >
                              {link.facility_name}
                            </Link>
                          ) : (
                            link.facility_name
                          )}
                        </td>
                        {[link.pl_url, link.cf_url, link.bs_url].map((url, i) => (
                          <td key={i} className="px-3 py-2 text-center">
                            {url ? (
                              <a
                                href={url}
                                target="_blank"
                                rel="noopener noreferrer"
                                className="inline-flex items-center gap-1 px-2 py-1 bg-indigo-50 text-indigo-700 text-xs font-medium rounded-md border border-indigo-200 hover:bg-indigo-100 transition-colors"
                              >
                                PDF
                              </a>
                            ) : (
                              <span className="text-gray-300 text-xs">-</span>
                            )}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <p className="text-sm text-gray-400 py-6 text-center">
                この法人の施設は財務諸表をまだ公表していません
              </p>
            )}
          </ChartCard>

          {/* 施設一覧 */}
          <ChartCard
            title="運営施設一覧"
            subtitle={`${report.business_dd.facilities.length}施設 / サービス種別: ${report.business_dd.service_types.join("、") || "-"}`}
          >
            <ul className="grid grid-cols-1 md:grid-cols-2 gap-1.5 text-sm text-gray-700">
              {report.business_dd.facilities.map((name, idx) => (
                <li key={idx} className="px-3 py-1.5 rounded-lg bg-gray-50 truncate" title={name}>
                  {name}
                </li>
              ))}
            </ul>
          </ChartCard>

          {/* 関連リンク */}
          <div className="flex flex-wrap gap-3">
            <Link
              href={`/due-diligence?corp=${report.corp_info.corp_number}`}
              className="px-4 py-2 border border-brand-500 text-brand-500 text-sm font-semibold rounded-xl hover:bg-brand-50 transition-colors"
            >
              DDレポートを見る
            </Link>
            <Link
              href="/pmi-synergy"
              className="px-4 py-2 border border-gray-300 text-gray-600 text-sm font-semibold rounded-xl hover:bg-gray-50 transition-colors"
            >
              PMIシナジー分析へ
            </Link>
          </div>
        </>
      )}
    </div>
  );
}

export default function CorpDetailPage() {
  return (
    <Suspense fallback={<div className="text-gray-400 text-sm p-8">読み込み中...</div>}>
      <CorpDetailContent />
    </Suspense>
  );
}
