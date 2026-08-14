"use client";

// ===================================================
// Page: 決算データから見える経営指標
//
// 「決算書の開示状況」は全数で確定する層を扱う。こちらはその一段先で、
// 抽出できた金額（約2.3万施設）を公表データと掛け合わせた分析を扱う。
//
// 出す切り口は scripts/fin_explore.py で信号があるか確かめたものだけ。
// 人件費率 × 離職率は相関係数 r = 0.007（無相関）だったので載せていない。
// 全ての数値に n を添える。母集団は「抽出できた施設」であって全国ではない。
// ===================================================

import { Suspense, useMemo } from "react";
import Link from "next/link";
import { useApi } from "@/hooks/useApi";
import KpiCard from "@/components/data-display/KpiCard";
import KpiCardGrid from "@/components/data-display/KpiCardGrid";
import BarChart from "@/components/charts/BarChart";
import ChartCard from "@/components/charts/ChartCard";
import ApiErrorBanner from "@/components/common/ApiErrorBanner";
import type {
  FinancialRevenuePerStaff,
  FinancialEquityByCorpSize,
  FinancialDisclosureVsQuality,
  FinancialPersonnelRatioByPrefecture,
  FinancialMetricsSummary,
} from "@/lib/types";

const IconYen = (
  <svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
    <path d="M12 3l5 7M12 3L7 10M12 10v11M8 13h8M8 17h8" />
  </svg>
);
const IconPeople = (
  <svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
    <path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2" /><circle cx="9" cy="7" r="4" /><path d="M23 21v-2a4 4 0 0 0-3-3.87" />
  </svg>
);
const IconShield = (
  <svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
    <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z" />
  </svg>
);
const IconTarget = (
  <svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
    <circle cx="12" cy="12" r="10" /><circle cx="12" cy="12" r="6" /><circle cx="12" cy="12" r="2" />
  </svg>
);

function Empty({ loading }: { loading?: boolean }) {
  return (
    <div className="flex items-center justify-center h-[280px] text-gray-400 text-sm">
      {loading ? "読み込み中..." : "件数が足りないため表示していません"}
    </div>
  );
}

function InsightsContent() {
  const { data: rps, error: rpsError, isLoading: rpsLoading } =
    useApi<FinancialRevenuePerStaff>("/api/financial/insights/revenue-per-staff");
  const { data: equity, isLoading: eqLoading } =
    useApi<FinancialEquityByCorpSize[]>("/api/financial/insights/equity-by-corp-size");
  const { data: vsQuality } =
    useApi<FinancialDisclosureVsQuality>("/api/financial/insights/disclosure-vs-quality");
  const { data: prefRatio, isLoading: prefLoading } =
    useApi<FinancialPersonnelRatioByPrefecture[]>(
      "/api/financial/insights/personnel-ratio-by-prefecture");
  const { data: metrics } =
    useApi<FinancialMetricsSummary>("/api/financial/metrics/summary");

  /** サービス種別ごとの職員1人あたり収益（高い順） */
  const svcChart = useMemo(
    () => (rps?.by_service ?? []).map((d) => ({
      name: `${d.service}（n=${d.n}）`,
      value: d.median,
    })),
    [rps]
  );

  /** 法人規模ごとの自己資本比率 */
  const sizeChart = useMemo(
    () => (equity ?? []).map((d) => ({ name: `${d.band}（n=${d.n}）`, value: d.median })),
    [equity]
  );

  /** 都道府県別の人件費率（高い順） */
  const prefChart = useMemo(
    () => (prefRatio ?? []).map((d) => ({ name: `${d.prefecture}（n=${d.n}）`, value: d.median })),
    [prefRatio]
  );

  const disclosed = vsQuality?.rows.find((r) => r.disclosed);
  const notDisclosed = vsQuality?.rows.find((r) => !r.disclosed);

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-gray-900">決算データから見える経営指標</h1>
        <p className="text-sm text-gray-500 mt-1">
          決算書から抽出できた金額を、公表されている職員数・品質評価と掛け合わせた分析です。
          全国平均ではなく「抽出できた範囲の中央値」で、すべての数値に件数(n)を添えています。
          掲載の有無そのものは{" "}
          <Link href="/financial-disclosure" className="text-brand-500 hover:underline">
            決算書の開示状況
          </Link>
          をご覧ください。
        </p>
      </div>

      {rpsError && <ApiErrorBanner error={rpsError} />}

      <KpiCardGrid>
        <KpiCard
          label="1人あたり収益"
          value={rps?.median ?? null}
          format="decimal"
          icon={IconYen}
          subtitle={rps ? `万円/人・年　n=${rps.n.toLocaleString()}` : undefined}
          loading={rpsLoading}
          accentColor="bg-indigo-500"
        />
        <KpiCard
          label="人件費率"
          value={metrics?.personnel_ratio.median ?? null}
          format="percentRaw"
          icon={IconPeople}
          subtitle={metrics ? `n=${metrics.personnel_ratio.n.toLocaleString()}` : undefined}
          accentColor="bg-emerald-500"
        />
        <KpiCard
          label="自己資本比率"
          value={metrics?.equity_ratio.median ?? null}
          format="percentRaw"
          icon={IconShield}
          subtitle={metrics ? `n=${metrics.equity_ratio.n.toLocaleString()}` : undefined}
          accentColor="bg-sky-500"
        />
        <KpiCard
          label="抽出できた施設"
          value={metrics?.facilities_with_any_amount ?? null}
          format="number"
          icon={IconTarget}
          subtitle="全223,103施設のうち"
          accentColor="bg-amber-500"
        />
      </KpiCardGrid>

      {/* 集計単位の制約を先に置く。ここを読まずに数字だけ持って行かれると誤用される */}
      {rps?.scope_note && (
        <div className="rounded-lg bg-amber-50 border border-amber-200 px-4 py-3">
          <p className="text-sm font-semibold text-amber-900 mb-1">この数字の母集団</p>
          <p className="text-xs text-amber-800 leading-relaxed">
            {rps.scope_note}。
            決算書は自由書式のため金額を抽出できたのは全体の約2割で、
            抽出できた事業者に偏りがあります。全国平均としては使えません。
            なお <span className="font-semibold">人件費率と離職率の関係も調べましたが、
            相関係数 r = 0.007 で関連は見られなかった</span>ため掲載していません。
          </p>
        </div>
      )}

      {/* 決算書を出している事業者と出していない事業者の比較。
          開示状況は全数で確定するので、ここだけは母集団の偏りがない */}
      {disclosed && notDisclosed && (
        <section className="rounded-xl border border-gray-200 bg-white p-4">
          <h2 className="text-sm font-semibold text-gray-800">
            決算書を出している事業者と、出していない事業者の違い
          </h2>
          <p className="text-xs text-gray-500 mt-1 mb-3">
            開示の有無は全223,103施設で確定するため、ここは母集団の偏りがありません。
          </p>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-left text-gray-500 border-b border-gray-200">
                  <th className="py-2 pr-4 font-medium">区分</th>
                  <th className="py-2 pr-4 font-medium text-right">施設数</th>
                  <th className="py-2 pr-4 font-medium text-right">品質スコア 平均</th>
                  <th className="py-2 pr-4 font-medium text-right">離職率 平均</th>
                  <th className="py-2 font-medium text-right">従業者数 平均</th>
                </tr>
              </thead>
              <tbody>
                {[disclosed, notDisclosed].map((r) => (
                  <tr key={String(r.disclosed)} className="border-b border-gray-100">
                    <td className="py-2 pr-4 text-gray-800">
                      {r.disclosed ? "決算書あり" : "決算書なし"}
                    </td>
                    <td className="py-2 pr-4 text-right tabular-nums">
                      {r.facilities.toLocaleString()}
                    </td>
                    <td className={`py-2 pr-4 text-right tabular-nums font-semibold ${r.disclosed ? "text-emerald-600" : "text-gray-500"}`}>
                      {r.quality_score_avg ?? "-"}
                    </td>
                    <td className={`py-2 pr-4 text-right tabular-nums ${r.disclosed ? "text-emerald-600" : "text-red-500"}`}>
                      {r.turnover_avg != null ? `${r.turnover_avg}%` : "-"}
                    </td>
                    <td className="py-2 text-right tabular-nums text-gray-600">
                      {r.staff_avg ?? "-"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <p className="text-[11px] text-gray-400 mt-2">{vsQuality?.note}</p>
        </section>
      )}

      <ChartCard
        title="サービス種別ごとの職員1人あたり収益"
        subtitle="法人の事業所が1つ、かつサービスも1つの施設に限定（集計単位が確定するもの）"
        loading={rpsLoading}
      >
        {svcChart.length > 0 ? (
          <>
            <BarChart
              data={svcChart}
              xKey="name"
              yKey="value"
              horizontal
              height={420}
              yAxisWidth={210}
              unit="万円"
              color="#4f46e5"
              tooltipFormatter={(v) => `${v}万円/人`}
            />
            <p className="text-xs text-gray-400 mt-2 px-1">
              訪問看護・居宅介護支援は1人あたりが高く、地域密着型通所介護は低い。
              労働集約の度合いがそのまま出ています。
            </p>
          </>
        ) : (
          <Empty loading={rpsLoading} />
        )}
      </ChartCard>

      <ChartCard
        title="法人の規模別 自己資本比率"
        subtitle="法人が持つ事業所数で区切った中央値"
        loading={eqLoading}
      >
        {sizeChart.length > 0 ? (
          <>
            <BarChart
              data={sizeChart}
              xKey="name"
              yKey="value"
              height={300}
              unit="%"
              color="#0ea5e9"
              tooltipFormatter={(v) => `${v}%`}
            />
            <p className="text-xs text-gray-400 mt-2 px-1">
              事業所数が増えるほど自己資本比率が上がります（1事業所39.6% → 10事業所以上65.2%）。
              規模の小さい法人ほど借入への依存が大きい傾向です。
            </p>
          </>
        ) : (
          <Empty loading={eqLoading} />
        )}
      </ChartCard>

      <ChartCard
        title="都道府県別の人件費率（高い順）"
        subtitle={`n=${rps?.min_n ?? 30} 件以上の都道府県のみ`}
        loading={prefLoading}
      >
        {prefChart.length > 0 ? (
          <BarChart
            data={prefChart}
            xKey="name"
            yKey="value"
            horizontal
            height={Math.max(320, prefChart.length * 22)}
            yAxisWidth={150}
            unit="%"
            color="#10b981"
            tooltipFormatter={(v) => `${v}%`}
          />
        ) : (
          <Empty loading={prefLoading} />
        )}
      </ChartCard>

      {rps?.by_corp_type && rps.by_corp_type.length > 0 && (
        <ChartCard title="法人種別ごとの職員1人あたり収益" subtitle="同上の限定条件">
          <BarChart
            data={rps.by_corp_type.map((d) => ({
              name: `${d.corp_type}（n=${d.n}）`,
              value: d.median,
            }))}
            xKey="name"
            yKey="value"
            horizontal
            height={300}
            yAxisWidth={210}
            unit="万円"
            color="#f59e0b"
            tooltipFormatter={(v) => `${v}万円/人`}
          />
        </ChartCard>
      )}

      <div className="rounded-lg bg-blue-50 border border-blue-200 px-4 py-3">
        <p className="text-xs text-blue-700 leading-relaxed">
          <span className="font-semibold">算出方法:</span>{" "}
          決算PDFから規則ベースで抽出した金額を使用しています。
          抽出値は「資産 = 負債 + 純資産」の検算を通しており、実測の一致率は89.7%です。
          単位がPDFに明記されていない場合は金額の桁から推定しています。
          比率は分子・分母が同じ書類から来るため単位の影響を受けません。
        </p>
      </div>
    </div>
  );
}

export default function FinancialInsightsPage() {
  return (
    <Suspense fallback={<div className="text-gray-400 text-sm p-8">読み込み中...</div>}>
      <InsightsContent />
    </Suspense>
  );
}
