"use client";

// ===================================================
// Page: データインサイト（反直感的発見 C4）
// データから自動検出した意外なパターンを表示
// 実API接続版
// ===================================================

import { Suspense, useMemo } from "react";
import { useApi } from "@/hooks/useApi";
import { useFilters } from "@/hooks/useFilters";
import ChartCard from "@/components/charts/ChartCard";
import FilterPanel from "@/components/filters/FilterPanel";
import { KpiGridSkeleton } from "@/components/common/LoadingSpinner";
import ApiErrorBanner from "@/components/common/ApiErrorBanner";
import type {
  DashboardKpi,
  SizeGroupTurnover,
  WorkforcePrefectureData,
  CorpGroupKpi,
  QualityKpiExtended,
} from "@/lib/types";

/** インサイト1件の型 */
interface Insight {
  icon: string;
  title: string;
  expected: string;
  actual: string;
  interpretation: string;
}

function InsightsContent() {
  const { filters, setFilters, toApiParams } = useFilters();
  const apiParams = toApiParams();

  // 複数APIからデータ取得
  const { data: dashboardKpi, error: dashErr, isLoading: dashLoading } = useApi<DashboardKpi>(
    "/api/dashboard/kpi",
    apiParams
  );

  const { data: sizeData, error: sizeErr, isLoading: sizeLoading } = useApi<SizeGroupTurnover[]>(
    "/api/workforce/by-size",
    apiParams
  );

  const { data: prefData, error: prefErr, isLoading: prefLoading } = useApi<WorkforcePrefectureData[]>(
    "/api/workforce/by-prefecture",
    apiParams
  );

  const { data: corpKpi, error: corpErr, isLoading: corpLoading } = useApi<CorpGroupKpi>(
    "/api/corp-group/kpi",
    apiParams
  );

  const { data: qualityKpi, error: qualErr, isLoading: qualLoading } = useApi<QualityKpiExtended>(
    "/api/quality/kpi",
    apiParams
  );

  const isLoading = dashLoading || sizeLoading || prefLoading || corpLoading || qualLoading;
  const apiError = dashErr || sizeErr || prefErr || corpErr || qualErr;

  // インサイト自動検出
  const insights = useMemo<Insight[]>(() => {
    const results: Insight[] = [];

    // インサイト1: 規模別離職率（大規模 > 小規模は直感に反する）
    if (sizeData && sizeData.length >= 2) {
      const small = sizeData.find((d) =>
        d.size_category?.includes("小") || d.size_category?.includes("1-")
      );
      const large = sizeData.find((d) =>
        d.size_category?.includes("大") || d.size_category?.includes("100")
      );
      if (small && large && large.avg_turnover_rate > small.avg_turnover_rate) {
        results.push({
          icon: "warning",
          title: "大規模施設ほど離職率が高い",
          expected: "規模が大きいほど安定性が高く離職率が低いと予想",
          actual: `小規模: ${(small.avg_turnover_rate * 100).toFixed(1)}% / 大規模: ${(large.avg_turnover_rate * 100).toFixed(1)}%`,
          interpretation:
            "大規模施設では人間関係の複雑化や業務の細分化による不満が影響している可能性があります。一方で、小規模施設はアットホームな環境が離職抑制に寄与していると考えられます。",
        });
      }
    }

    // インサイト2: BCP策定率 vs ICT活用率のギャップ
    if (qualityKpi) {
      const bcpRate = qualityKpi.bcp_rate;
      const ictRate = qualityKpi.ict_rate;
      if (bcpRate != null && ictRate != null && bcpRate > 0.5 && ictRate < 0.2) {
        results.push({
          icon: "insight",
          title: "BCP策定率は高いがICT活用は低い",
          expected:
            "BCP策定（防災計画）とICT活用は組織の成熟度として連動すると予想",
          actual: `BCP策定率: ${(bcpRate * 100).toFixed(1)}%, ICT活用率: ${(ictRate * 100).toFixed(1)}%`,
          interpretation:
            "BCP策定は法的要件（努力義務化）の影響で進んでいますが、ICT導入は投資コストの壁があり、特に中小規模施設では導入が遅れている可能性があります。",
        });
      }
    }

    // インサイト3: 常勤比率と離職率の関係
    if (sizeData && sizeData.length >= 2) {
      const highFulltime = sizeData.filter(
        (d) => d.avg_fulltime_ratio > 0.7
      );
      const lowFulltime = sizeData.filter(
        (d) => d.avg_fulltime_ratio <= 0.7 && d.avg_fulltime_ratio > 0
      );
      if (highFulltime.length > 0 && lowFulltime.length > 0) {
        const avgHighTurnover =
          highFulltime.reduce((s, d) => s + d.avg_turnover_rate, 0) /
          highFulltime.length;
        const avgLowTurnover =
          lowFulltime.reduce((s, d) => s + d.avg_turnover_rate, 0) /
          lowFulltime.length;
        if (avgHighTurnover > avgLowTurnover) {
          results.push({
            icon: "warning",
            title: "常勤比率が高い施設ほど離職率が高い傾向",
            expected:
              "常勤比率が高いほど雇用安定性が高く離職率は低いと予想",
            actual: `常勤比率70%超: 離職率${(avgHighTurnover * 100).toFixed(1)}% / 常勤比率70%以下: ${(avgLowTurnover * 100).toFixed(1)}%`,
            interpretation:
              "常勤職員は業務負荷が高い傾向があり、パートタイム混合の施設の方がワークライフバランスが取れている可能性があります。",
          });
        }
      }
    }

    // インサイト4: 都道府県間の離職率格差
    if (prefData && prefData.length >= 5) {
      const sorted = [...prefData]
        .filter((d) => d.avg_turnover_rate > 0)
        .sort((a, b) => a.avg_turnover_rate - b.avg_turnover_rate);
      if (sorted.length >= 2) {
        const lowest = sorted[0];
        const highest = sorted[sorted.length - 1];
        const gap = highest.avg_turnover_rate - lowest.avg_turnover_rate;
        if (gap > 0.05) {
          results.push({
            icon: "insight",
            title: "都道府県間で離職率に大きな格差がある",
            expected: "地域差はあるが大きな差は出にくいと予想",
            actual: `最低: ${lowest.prefecture} ${(lowest.avg_turnover_rate * 100).toFixed(1)}% / 最高: ${highest.prefecture} ${(highest.avg_turnover_rate * 100).toFixed(1)}%（差: ${(gap * 100).toFixed(1)}pt）`,
            interpretation:
              "地域の介護需給バランスや賃金水準の違いが離職率格差に影響しています。都市部は求人競争が激しく、地方は施設数が限られることが要因の一つです。",
          });
        }
      }
    }

    // インサイト5: 法人グループの多施設展開
    if (corpKpi && dashboardKpi) {
      const multiFacilityRatio =
        corpKpi.multi_facility_corps / Math.max(corpKpi.total_corps, 1);
      if (multiFacilityRatio < 0.3) {
        results.push({
          icon: "insight",
          title: "多施設展開している法人は全体の少数派",
          expected:
            "介護業界はチェーン展開が進んでいると予想",
          actual: `複数施設法人の割合: ${(multiFacilityRatio * 100).toFixed(1)}%（${corpKpi.multi_facility_corps.toLocaleString("ja-JP")}法人 / ${corpKpi.total_corps.toLocaleString("ja-JP")}法人）`,
          interpretation:
            "介護業界は依然として単独施設運営が主流です。M&Aによる業界再編の余地が大きいことを示しており、投資観点では統合余地のある市場と言えます。",
        });
      }
    }

    // インサイト6: 第三者評価の普及率
    if (qualityKpi && qualityKpi.third_party_rate != null) {
      if (qualityKpi.third_party_rate < 0.1) {
        results.push({
          icon: "warning",
          title: "第三者評価を受けている施設は極めて少ない",
          expected: "品質透明性の観点から一定の普及率があると予想",
          actual: `第三者評価受審率: ${(qualityKpi.third_party_rate * 100).toFixed(1)}%`,
          interpretation:
            "第三者評価は任意のため普及率が低く、利用者側からの品質比較が困難な状況です。評価を受けている施設は差別化要因として活用できる可能性があります。",
        });
      }
    }

    return results;
  }, [sizeData, qualityKpi, prefData, corpKpi, dashboardKpi]);

  return (
    <div className="space-y-6">
      {/* ページタイトル */}
      <div>
        <h1 className="text-xl font-bold text-gray-900">データインサイト</h1>
        <p className="text-sm text-gray-500 mt-1">
          データが示す意外な発見
        </p>
      </div>

      {/* データソースバナー */}
      <div className="rounded-lg bg-blue-50 border border-blue-200 px-4 py-3">
        <p className="text-xs text-blue-700 leading-relaxed">
          <span className="font-semibold">データソース:</span>{" "}
          厚生労働省「介護サービス情報公表システム」掲載データおよび公開統計データに基づく分析結果です。
          インサイトはデータの統計的パターンから自動検出しており、因果関係を保証するものではありません。
        </p>
      </div>

      {/* APIエラーバナー */}
      <ApiErrorBanner error={apiError} />

      {/* フィルタパネル */}
      <FilterPanel
        filters={filters}
        onChange={setFilters}
        compact
        visibleFilters={["prefectures", "serviceCodes", "keyword"]}
      />

      {/* インサイトカード一覧 */}
      {isLoading ? (
        <KpiGridSkeleton count={4} />
      ) : insights.length > 0 ? (
        <div className="space-y-4">
          {insights.map((insight, index) => (
            <div
              key={index}
              className="bg-white rounded-xl shadow-card overflow-hidden transition-shadow duration-200 hover:shadow-card-hover"
            >
              <div className="p-5">
                {/* ヘッダー */}
                <div className="flex items-start gap-3 mb-4">
                  <span
                    className={`flex-shrink-0 w-10 h-10 rounded-lg flex items-center justify-center text-lg ${
                      insight.icon === "warning"
                        ? "bg-amber-50 text-amber-600"
                        : "bg-indigo-50 text-indigo-600"
                    }`}
                    role="img"
                    aria-label={
                      insight.icon === "warning" ? "注目" : "発見"
                    }
                  >
                    {insight.icon === "warning" ? (
                      <svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
                        <path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z" />
                        <line x1="12" y1="9" x2="12" y2="13" />
                        <line x1="12" y1="17" x2="12.01" y2="17" />
                      </svg>
                    ) : (
                      <svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
                        <path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z" />
                        <circle cx="12" cy="12" r="3" />
                      </svg>
                    )}
                  </span>
                  <div className="min-w-0">
                    <h3 className="text-base font-semibold text-gray-900">
                      {insight.title}
                    </h3>
                  </div>
                </div>

                {/* 期待 vs 実態 */}
                <div className="grid grid-cols-1 md:grid-cols-2 gap-3 mb-4">
                  <div className="rounded-lg bg-gray-50 border border-gray-200 p-3">
                    <p className="text-xs font-medium text-gray-500 mb-1">
                      期待（一般的な予想）
                    </p>
                    <p className="text-sm text-gray-700 leading-relaxed">
                      {insight.expected}
                    </p>
                  </div>
                  <div className="rounded-lg bg-amber-50 border border-amber-200 p-3">
                    <p className="text-xs font-medium text-amber-700 mb-1">
                      実態（データが示す事実）
                    </p>
                    <p className="text-sm text-gray-700 leading-relaxed tabular-nums">
                      {insight.actual}
                    </p>
                  </div>
                </div>

                {/* 解釈 */}
                <div className="rounded-lg bg-indigo-50 border border-indigo-100 p-3">
                  <p className="text-xs font-medium text-indigo-700 mb-1">
                    考察
                  </p>
                  <p className="text-sm text-gray-600 leading-relaxed">
                    {insight.interpretation}
                  </p>
                </div>
              </div>
            </div>
          ))}
        </div>
      ) : (
        <ChartCard
          title="インサイト"
          subtitle="データパターンの検出結果"
        >
          <div className="flex flex-col items-center justify-center py-12 text-center">
            <div className="w-16 h-16 mb-4 rounded-full bg-gray-100 flex items-center justify-center">
              <svg className="w-8 h-8 text-gray-300" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" aria-hidden="true">
                <path strokeLinecap="round" strokeLinejoin="round" d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" />
              </svg>
            </div>
            <p className="text-sm font-medium text-gray-500">
              現在のフィルタ条件では反直感的なパターンが検出されませんでした
            </p>
            <p className="text-xs text-gray-400 mt-1">
              フィルタを変更して、異なるデータセットを分析してみてください
            </p>
          </div>
        </ChartCard>
      )}

      {/* 注記 */}
      <div className="rounded-lg bg-gray-50 border border-gray-200 px-4 py-3">
        <p className="text-xs text-gray-500 leading-relaxed">
          インサイトはデータの統計的パターンから自動検出しています。
          相関関係は因果関係を意味しません。ビジネス上の意思決定には、追加の定性調査や専門家の知見を併せてご活用ください。
        </p>
      </div>
    </div>
  );
}

export default function InsightsPage() {
  return (
    <Suspense fallback={<div className="text-gray-400 text-sm p-8">読み込み中...</div>}>
      <InsightsContent />
    </Suspense>
  );
}
