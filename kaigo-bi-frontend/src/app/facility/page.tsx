"use client";

// ===================================================
// 施設詳細ページ
// /facility?id=<事業所番号>
// 施設マスタ等からのリンク先。1施設を深掘りする専用ページ
// ===================================================

import { Suspense, useMemo } from "react";
import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import { useApi } from "@/hooks/useApi";
import type { CrossMetrics, FacilityRowExtended, FinancialRecord, StaffingBreakdown } from "@/lib/types";
import FacilityDetailPanel from "@/components/data-display/FacilityDetailPanel";
import FinancialSummaryCard from "@/components/data-display/FinancialSummaryCard";
import CrossMetricsCard from "@/components/data-display/CrossMetricsCard";
import ApiErrorBanner from "@/components/common/ApiErrorBanner";

/** 職種別人員テーブルの表示定義 */
const STAFFING_ROWS: { key: keyof StaffingBreakdown; label: string }[] = [
  { key: "kaigo", label: "介護職員" },
  { key: "nurse", label: "看護職員" },
  { key: "counselor", label: "生活相談員" },
  { key: "trainer", label: "機能訓練指導員" },
  { key: "dietitian", label: "管理栄養士" },
  { key: "clerk", label: "事務員" },
];

/** 人数表示（null→"-"） */
function num(v: number | null | undefined): string {
  return v != null ? v.toLocaleString("ja-JP") : "-";
}

function FacilityDetailContent() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const id = searchParams.get("id") ?? "";

  const { data, error, isLoading } = useApi<{
    facility: FacilityRowExtended;
    financials?: FinancialRecord[];
    cross_metrics?: CrossMetrics;
  }>(id ? `/api/facilities/${id}` : null);

  const facility = useMemo(() => data?.facility ?? null, [data]);
  const financials = data?.financials ?? [];
  const crossMetrics = data?.cross_metrics ?? null;
  const hasViolation = facility?.sanction_detail != null || facility?.guidance_detail != null;
  const hasStaffing =
    facility?.staffing != null &&
    STAFFING_ROWS.some((r) => facility.staffing?.[r.key]?.total != null);

  if (!id) {
    return (
      <div className="p-8 text-center text-gray-500 text-sm">
        施設が指定されていません。
        <Link href="/facilities" className="text-brand-500 hover:underline ml-2">
          施設マスタから探す →
        </Link>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      {/* パンくず + タイトル */}
      <div className="flex items-center justify-between">
        <div>
          <p className="text-xs text-gray-400">
            <Link href="/facilities" className="hover:text-brand-500">施設マスタ</Link>
            {" / "}施設詳細
          </p>
          <h1 className="text-heading-lg text-gray-900 mt-1">
            {facility?.jigyosho_name ?? (isLoading ? "読み込み中..." : "施設詳細")}
          </h1>
          {facility?.corp_name && (
            <p className="text-body-md text-gray-500 mt-1">
              {facility.corp_name}
              {facility.corp_number && (
                <Link
                  href={`/corp?number=${facility.corp_number}`}
                  className="ml-2 text-brand-500 hover:underline text-sm"
                >
                  法人ページを見る →
                </Link>
              )}
            </p>
          )}
        </div>
      </div>

      <ApiErrorBanner error={error} />

      {/* 行政処分・指導の警告（「なし」系表記は除外済み） */}
      {hasViolation && (
        <div className="p-4 bg-red-50 border border-red-200 rounded-xl text-sm text-red-700" role="alert">
          <p className="font-semibold mb-1">⚠️ 行政処分・指導の記録があります</p>
          {facility?.sanction_detail && (
            <p>
              処分{facility.sanction_date ? `（${facility.sanction_date}）` : ""}: {facility.sanction_detail}
            </p>
          )}
          {facility?.guidance_detail && (
            <p>
              指導{facility.guidance_date ? `（${facility.guidance_date}）` : ""}: {facility.guidance_detail}
            </p>
          )}
        </div>
      )}

      {/* 財務サマリー（決算PDF抽出済みの場合のみ表示） */}
      {financials.length > 0 && <FinancialSummaryCard records={financials} />}

      {/* クロス指標（財務 × 公表データ） */}
      {crossMetrics?.has_financials && <CrossMetricsCard metrics={crossMetrics} />}

      {/* 詳細パネル（既存コンポーネントを全画面利用） */}
      <FacilityDetailPanel
        facility={facility}
        loading={isLoading}
        onClose={() => router.back()}
      />

      {/* 職種別人員体制 */}
      {hasStaffing && facility?.staffing && (
        <section className="bg-white border border-gray-200 rounded-xl p-4">
          <h4 className="text-sm font-semibold text-gray-800 mb-3">職種別人員体制</h4>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-200 text-left">
                  <th className="px-3 py-2 text-gray-500 font-medium">職種</th>
                  <th className="px-3 py-2 text-gray-500 font-medium text-right">常勤</th>
                  <th className="px-3 py-2 text-gray-500 font-medium text-right">非常勤</th>
                  <th className="px-3 py-2 text-gray-500 font-medium text-right">合計</th>
                </tr>
              </thead>
              <tbody>
                {STAFFING_ROWS.map(({ key, label }) => {
                  const s = facility.staffing?.[key];
                  if (s?.total == null && s?.fulltime == null) return null;
                  return (
                    <tr key={key} className="border-b border-gray-100 hover:bg-gray-50">
                      <td className="px-3 py-2 text-gray-700">{label}</td>
                      <td className="px-3 py-2 text-right tabular-nums">{num(s?.fulltime)}</td>
                      <td className="px-3 py-2 text-right tabular-nums">{num(s?.parttime)}</td>
                      <td className="px-3 py-2 text-right tabular-nums font-medium">{num(s?.total)}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
          {/* 資格・夜間体制 */}
          <div className="flex flex-wrap gap-x-6 gap-y-1 mt-3 text-xs text-gray-500">
            {facility.qualifications?.care_worker != null && (
              <span>介護福祉士: <strong className="text-gray-800">{num(facility.qualifications.care_worker)}人</strong></span>
            )}
            {facility.qualifications?.care_manager != null && (
              <span>ケアマネ: <strong className="text-gray-800">{num(facility.qualifications.care_manager)}人</strong></span>
            )}
            {facility.night_shift_count != null && (
              <span>夜勤: <strong className="text-gray-800">{num(facility.night_shift_count)}人</strong></span>
            )}
            {facility.night_watch_count != null && (
              <span>宿直: <strong className="text-gray-800">{num(facility.night_watch_count)}人</strong></span>
            )}
          </div>
        </section>
      )}

      {/* データ鮮度 */}
      {facility?.scraped_at && (
        <p className="text-xs text-gray-400 text-right">
          データ取得日: {facility.scraped_at}（介護サービス情報公表システム）
        </p>
      )}
    </div>
  );
}

export default function FacilityDetailPage() {
  return (
    <Suspense fallback={<div className="text-gray-400 text-sm p-8">読み込み中...</div>}>
      <FacilityDetailContent />
    </Suspense>
  );
}
