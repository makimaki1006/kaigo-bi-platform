"use client";

// ===================================================
// Page 07: 経営品質分析
// 品質スコア分布・4カテゴリレーダー・BCP/ICT/第三者評価KPI
// 実API連携版
// ===================================================

import { Suspense, useMemo } from "react";
import { useApi } from "@/hooks/useApi";
import { useFilters } from "@/hooks/useFilters";
import { useServiceConfig } from "@/lib/service-config";
import type {
  QualityKpi,
  PrefectureQualityScore,
  QualityKpiExtended,
  QualityRankDistribution,
  QualityCategoryRadar,
} from "@/lib/types";
import KpiCard from "@/components/data-display/KpiCard";
import KpiCardGrid from "@/components/data-display/KpiCardGrid";
import BarChart from "@/components/charts/BarChart";
import RadarChart from "@/components/charts/RadarChart";
import ChartCard from "@/components/charts/ChartCard";
import FilterPanel from "@/components/filters/FilterPanel";
import DataPendingPlaceholder from "@/components/common/DataPendingPlaceholder";
import ApiErrorBanner from "@/components/common/ApiErrorBanner";
import { CHART_COLORS } from "@/lib/constants";

/** スコア分布の型（バックエンド QualityScoreDistribution に対応） */
interface QualityScoreDist {
  range: string;
  count: number;
}

/** ランクの色設定 */
const RANK_COLORS: Record<string, string> = {
  S: "#f59e0b", // 金
  A: "#9ca3af", // 銀
  B: "#3b82f6", // 青
  C: "#eab308", // 黄
  D: "#ef4444", // 赤
};

/** サービス種別非対応KPIの案内ボックス */
function UnavailableNotice({ message }: { message: string | null }) {
  if (!message) return null;
  return (
    <div className="text-sm text-gray-400 p-4 bg-gray-50 rounded-lg border border-gray-100">
      {message}
    </div>
  );
}

function QualityContent() {
  const { filters, setFilters, toApiParams } = useFilters();
  const apiParams = toApiParams();
  const serviceConfig = useServiceConfig(filters.serviceCodes);

  // 品質KPI取得（拡張版: BCP率、ICT率、第三者評価率含む）
  const { data: kpi, error: kpiError, isLoading: kpiLoading } = useApi<QualityKpiExtended>(
    "/api/quality/kpi",
    apiParams
  );

  // スコア分布
  const { data: scoreDist, error: scoreDistError, isLoading: scoreDistLoading } = useApi<QualityScoreDist[]>(
    "/api/quality/score-distribution",
    apiParams
  );

  // 品質ランク別分布
  const { data: rankDist, error: rankDistError, isLoading: rankDistLoading } = useApi<QualityRankDistribution[]>(
    "/api/quality/rank-distribution",
    apiParams
  );

  // 4カテゴリレーダー（APIがflat objectまたは配列を返す可能性があるため両方対応）
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const { data: radarDataRaw, error: radarError, isLoading: radarLoading } = useApi<any>(
    "/api/quality/category-radar",
    apiParams
  );

  // 都道府県別平均スコア
  const { data: prefScores, error: prefError, isLoading: prefLoading } = useApi<PrefectureQualityScore[]>(
    "/api/quality/by-prefecture",
    apiParams
  );

  const apiError = kpiError || scoreDistError || rankDistError || radarError || prefError;

  // APIレスポンスがflat object({hr, operations, quality, safety})の場合、配列に変換
  const RADAR_CATEGORY_MAP: Record<string, string> = {
    safety: "安全管理",
    quality: "品質管理",
    hr: "人材安定性",
    operations: "収益効率",
  };

  const radarData: QualityCategoryRadar[] = useMemo(() => {
    if (!radarDataRaw) return [];
    // 既に配列形式の場合はそのまま使用
    if (Array.isArray(radarDataRaw)) return radarDataRaw;
    // flat object形式の場合は配列に変換
    if (typeof radarDataRaw === "object") {
      return Object.entries(radarDataRaw)
        .filter(([key]) => key in RADAR_CATEGORY_MAP)
        .map(([key, value]) => ({
          category: RADAR_CATEGORY_MAP[key] || key,
          score: Number(value) || 0,
          fullMark: 30,
        }));
    }
    return [];
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [radarDataRaw]);

  // KPIがAPIから取得できたか
  const kpiFromApi = kpi != null;
  const hasScoreDist = scoreDist != null && scoreDist.length > 0;
  const hasRankDist = rankDist != null && rankDist.length > 0;
  const hasRadarData = radarData.length > 0;

  return (
    <div className="space-y-6">
      {/* ページタイトル */}
      <div>
        <h1 className="text-xl font-bold text-gray-900">経営品質分析</h1>
        <p className="text-sm text-gray-500 mt-1">
          品質スコア・安全管理・サービス品質・人材管理・収益効率の多面的分析
        </p>
      </div>

      {/* APIエラーバナー */}
      <ApiErrorBanner error={apiError} />

      {/* データ取得状況バナー */}
      {!kpiFromApi && !kpiLoading && (
        <div className="bg-amber-50 border border-amber-200 rounded-lg px-4 py-3">
          <div className="flex items-start gap-2">
            <span className="text-amber-500 text-sm mt-0.5">&#9888;</span>
            <div>
              <p className="text-sm font-medium text-amber-800">
                経営品質データは準備中です
              </p>
              <p className="text-xs text-amber-600 mt-0.5">
                フルデータ取得後に実数値で表示されます。
              </p>
            </div>
          </div>
        </div>
      )}

      {/* フィルタパネル */}
      <FilterPanel
        filters={filters}
        onChange={setFilters}
        compact
        visibleFilters={["prefectures", "serviceCodes", "keyword"]}
      />

      {/* KPIカード（拡張版: BCP率、ICT率、第三者評価率追加） */}
      <KpiCardGrid>
        {serviceConfig.isAvailable("quality_score") ? (
          <KpiCard
            label="平均品質スコア"
            value={kpi?.avg_quality_score ?? kpi?.avg_profit_ratio ?? null}
            format={kpi?.avg_quality_score != null ? "decimal" : "percent"}
            icon={<svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10" /></svg>}
            accentColor="bg-indigo-500"
            subtitle={kpi?.avg_quality_score != null ? "100点満点評価" : kpi?.avg_profit_ratio != null ? "損益差額比率" : "データなし"}
            loading={kpiLoading}
            tooltip="安全管理・品質管理・人材安定性・収益安定性の100点満点評価"
          />
        ) : (
          <UnavailableNotice message={serviceConfig.reason("quality_score")} />
        )}
        <KpiCard
          label="BCP策定率"
          value={kpi?.bcp_rate ?? null}
          format="percent"
          icon={<svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10" /><path d="m9 12 2 2 4-4" /></svg>}
          accentColor="bg-emerald-500"
          subtitle={kpi?.bcp_rate != null ? "介護情報公表システム" : "データなし"}
          loading={kpiLoading}
          tooltip="BCP(事業継続計画): 災害・感染症等で事業が中断した際の復旧計画の策定状況"
        />
        <KpiCard
          label="ICT活用率"
          value={kpi?.ict_rate ?? null}
          format="percent"
          icon={<svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><rect width="20" height="14" x="2" y="3" rx="2" /><line x1="8" x2="16" y1="21" y2="21" /><line x1="12" x2="12" y1="17" y2="21" /></svg>}
          accentColor="bg-blue-500"
          subtitle={kpi?.ict_rate != null ? "介護情報公表システム" : "データなし"}
          loading={kpiLoading}
          tooltip="ICT(情報通信技術): 介護記録ソフト・見守りセンサー等のテクノロジー活用状況"
        />
        {serviceConfig.isAvailable("third_party_eval") ? (
          <KpiCard
            label="第三者評価受審率"
            value={kpi?.third_party_rate ?? null}
            format="percent"
            icon={<svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2" /></svg>}
            accentColor="bg-amber-500"
            subtitle={kpi?.third_party_rate != null ? "介護情報公表システム" : "データなし"}
            loading={kpiLoading}
            tooltip="外部の第三者機関による福祉サービスの評価を受けた施設の割合"
          />
        ) : (
          <UnavailableNotice message={serviceConfig.reason("third_party_eval")} />
        )}
        <KpiCard
          label="損害賠償保険加入率"
          value={kpi?.insurance_rate ?? null}
          format="percent"
          icon={<svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10" /></svg>}
          accentColor="bg-teal-500"
          subtitle={kpi?.insurance_rate != null ? "介護情報公表システム" : "データなし"}
          loading={kpiLoading}
          tooltip="損害賠償責任保険への加入状況"
        />
      </KpiCardGrid>

      {/* チャートエリア */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* 1. 品質スコア分布（ランク別ヒストグラム） */}
        {!serviceConfig.isAvailable("quality_score") ? (
          <ChartCard title="品質スコア分布（ランク別）" subtitle="S/A/B/C/Dランク別施設数">
            <UnavailableNotice message={serviceConfig.reason("quality_score")} />
          </ChartCard>
        ) : (
        <ChartCard
          title="品質スコア分布（ランク別）"
          subtitle="S/A/B/C/Dランク別施設数"
        >
          {rankDistLoading ? (
            <div className="flex items-center justify-center h-[300px] text-gray-400 text-sm">
              読み込み中...
            </div>
          ) : hasRankDist ? (
            <div className="space-y-3">
              {rankDist!.map((item) => {
                const maxCount = Math.max(...rankDist!.map((r) => r.count));
                const widthPct = maxCount > 0 ? (item.count / maxCount) * 100 : 0;
                return (
                  <div key={item.rank} className="flex items-center gap-3">
                    <div
                      className="w-8 h-8 rounded-full flex items-center justify-center text-xs font-bold text-white flex-shrink-0"
                      style={{ backgroundColor: RANK_COLORS[item.rank] || "#6b7280" }}
                    >
                      {item.rank}
                    </div>
                    <div className="flex-1">
                      <div className="h-6 bg-gray-100 rounded-full overflow-hidden">
                        <div
                          className="h-full rounded-full transition-all duration-500"
                          style={{
                            width: `${Math.max(widthPct, 2)}%`,
                            backgroundColor: RANK_COLORS[item.rank] || "#6b7280",
                            opacity: 0.7,
                          }}
                        />
                      </div>
                    </div>
                    <span className="text-sm font-mono text-gray-700 w-16 text-right">
                      {item.count.toLocaleString("ja-JP")}
                    </span>
                  </div>
                );
              })}
            </div>
          ) : hasScoreDist ? (
            <BarChart
              data={scoreDist!}
              xKey="range"
              yKey="count"
              color={CHART_COLORS[0]}
              tooltipFormatter={(v) => `${v.toLocaleString("ja-JP")}施設`}
              height={300}
            />
          ) : (
            <DataPendingPlaceholder
              message="データなし"
              description="フィルタ条件に一致する品質スコアデータがありません"
              height={300}
            />
          )}
        </ChartCard>
        )}

        {/* 2. 4カテゴリレーダー */}
        <ChartCard
          title="経営品質レーダー"
          subtitle="安全/品質/人材/収益の4軸平均評価"
        >
          {radarLoading ? (
            <div className="flex items-center justify-center h-[320px] text-gray-400 text-sm">
              読み込み中...
            </div>
          ) : hasRadarData ? (
            <RadarChart
              data={radarData}
              categoryKey="category"
              series={[
                { dataKey: "score", name: "平均スコア", color: CHART_COLORS[0] },
              ]}
              height={320}
            />
          ) : (
            <DataPendingPlaceholder
              message="データなし"
              description="フィルタ条件に一致するレーダーチャートデータがありません"
              height={320}
            />
          )}
        </ChartCard>

        {/* 3. 都道府県別品質スコア */}
        <ChartCard
          title="都道府県別 平均品質スコア（Top 15）"
          subtitle="スコア上位15都道府県"
        >
          {prefLoading ? (
            <div className="flex items-center justify-center h-[300px] text-gray-400 text-sm">
              読み込み中...
            </div>
          ) : prefScores && prefScores.length > 0 ? (
            <BarChart
              data={
                [...prefScores]
                  .sort((a, b) => b.avg_profit_ratio - a.avg_profit_ratio)
                  .slice(0, 15)
              }
              xKey="prefecture"
              yKey="avg_profit_ratio"
              color={CHART_COLORS[2]}
              horizontal
              tooltipFormatter={(v) => `${(v * 100).toFixed(1)}%`}
              height={420}
            />
          ) : (
            <DataPendingPlaceholder
              message="データなし"
              description="フィルタ条件に一致する都道府県別データがありません"
              height={300}
            />
          )}
        </ChartCard>

        {/* 4. BCP x ICT x 第三者評価 概要 */}
        <ChartCard
          title="BCP / ICT / 第三者評価 概要"
          subtitle="3項目の取得状況サマリー"
        >
          {kpiLoading ? (
            <div className="flex items-center justify-center h-[300px] text-gray-400 text-sm">
              読み込み中...
            </div>
          ) : kpi?.bcp_rate != null || kpi?.ict_rate != null || kpi?.third_party_rate != null ? (
            <div className="space-y-4 py-4">
              {[
                { label: "BCP策定", rate: kpi?.bcp_rate, color: "bg-emerald-500", bgColor: "bg-emerald-100" },
                { label: "ICT活用", rate: kpi?.ict_rate, color: "bg-blue-500", bgColor: "bg-blue-100" },
                { label: "第三者評価", rate: kpi?.third_party_rate, color: "bg-amber-500", bgColor: "bg-amber-100" },
                { label: "保険加入", rate: kpi?.insurance_rate, color: "bg-teal-500", bgColor: "bg-teal-100" },
              ].map((item) => (
                <div key={item.label} className="space-y-1">
                  <div className="flex items-center justify-between text-sm">
                    <span className="font-medium text-gray-700">{item.label}</span>
                    <span className="text-gray-900 font-semibold tabular-nums">
                      {item.rate != null ? `${(item.rate * 100).toFixed(1)}%` : "-"}
                    </span>
                  </div>
                  <div className={`h-3 rounded-full ${item.bgColor} overflow-hidden`}>
                    <div
                      className={`h-full rounded-full ${item.color} transition-all duration-500`}
                      style={{ width: `${item.rate != null ? Math.max(item.rate * 100, 1) : 0}%` }}
                    />
                  </div>
                </div>
              ))}
              <p className="text-xs text-gray-400 mt-2">
                対象施設数: {kpi?.facility_count?.toLocaleString("ja-JP") ?? "-"}
              </p>
            </div>
          ) : (
            <DataPendingPlaceholder
              message="データなし"
              description="BCP・ICT・第三者評価のKPIデータがありません"
              height={300}
            />
          )}
        </ChartCard>
      </div>

      {/* スコアリング基準の説明 */}
      <ChartCard
        title="品質スコアリング基準（100点満点）"
        subtitle="S(80+) / A(65-79) / B(50-64) / C(35-49) / D(0-34)"
      >
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
          {[
            {
              category: "安全・リスク管理（30点）",
              items: ["BCP策定済み: 10点", "損害賠償保険加入: 10点", "行政処分なし: 10点"],
              color: "bg-orange-50 border-orange-200",
              textColor: "text-orange-700",
            },
            {
              category: "品質管理（25点）",
              items: ["第三者評価受審: 15点", "ICT又は介護ロボット活用: 10点"],
              color: "bg-blue-50 border-blue-200",
              textColor: "text-blue-700",
            },
            {
              category: "人材安定性（25点）",
              items: ["離職率<都道府県平均: 10点", "常勤比率>50%: 8点", "経験10年以上割合>30%: 7点"],
              color: "bg-purple-50 border-purple-200",
              textColor: "text-purple-700",
            },
            {
              category: "収益安定性（20点）",
              items: ["稼働率>80%: 10点", "加算取得数>=5: 10点"],
              color: "bg-green-50 border-green-200",
              textColor: "text-green-700",
            },
          ].map((cat) => (
            <div
              key={cat.category}
              className={`rounded-lg border p-4 ${cat.color}`}
            >
              <h4 className={`text-sm font-semibold ${cat.textColor} mb-2`}>
                {cat.category}
              </h4>
              <ul className="space-y-1">
                {cat.items.map((item) => (
                  <li key={item} className="text-xs text-gray-600 flex items-center gap-1.5">
                    <span className="w-1 h-1 rounded-full bg-gray-400 flex-shrink-0" />
                    {item}
                  </li>
                ))}
              </ul>
            </div>
          ))}
        </div>
      </ChartCard>
    </div>
  );
}

export default function QualityPage() {
  return (
    <Suspense fallback={<div className="text-gray-400 text-sm p-8">読み込み中...</div>}>
      <QualityContent />
    </Suspense>
  );
}
