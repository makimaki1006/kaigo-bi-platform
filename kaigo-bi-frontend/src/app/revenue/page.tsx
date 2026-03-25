"use client";

// ===================================================
// Page 06: 収益構造分析
// 加算取得率、稼働率、定員など収益に関する分析ページ
// 実API連携版 + 加算取得シミュレーター
// ===================================================

import { Suspense, useState, useMemo, useCallback } from "react";
import { useApi } from "@/hooks/useApi";
import { useFilters } from "@/hooks/useFilters";
import { useServiceConfig } from "@/lib/service-config";
import type { RevenueKpi, BonusItemRate, OccupancyBin } from "@/lib/types";
import { KASAN_LABELS } from "@/lib/types";
import KpiCard from "@/components/data-display/KpiCard";
import KpiCardGrid from "@/components/data-display/KpiCardGrid";
import BarChart from "@/components/charts/BarChart";
import ChartCard from "@/components/charts/ChartCard";
import FilterPanel from "@/components/filters/FilterPanel";
import DataPendingPlaceholder from "@/components/common/DataPendingPlaceholder";
import ApiErrorBanner from "@/components/common/ApiErrorBanner";

/** 加算項目リスト（参考表示用） */
const BONUS_ITEMS = [
  "処遇改善加算",
  "特定処遇改善加算",
  "介護職員等ベースアップ等支援加算",
  "サービス提供体制強化加算",
  "中重度者ケア体制加算",
  "認知症加算",
  "若年性認知症利用者受入加算",
  "栄養アセスメント加算",
  "口腔・栄養スクリーニング加算",
  "科学的介護推進体制加算",
  "ADL維持等加算",
  "生活機能向上連携加算",
  "個別機能訓練加算",
];

/** 加算シミュレーター用: 加算項目と推定月額増収単価（円/利用者） */
const KASAN_SIMULATION_ITEMS: {
  key: string;
  name: string;
  monthlyPerUser: number;
  difficulty: "easy" | "medium" | "hard";
}[] = [
  { key: "addition_treatment_i", name: "処遇改善加算I", monthlyPerUser: 5400, difficulty: "medium" },
  { key: "addition_treatment_ii", name: "処遇改善加算II", monthlyPerUser: 3600, difficulty: "easy" },
  { key: "addition_treatment_iii", name: "処遇改善加算III", monthlyPerUser: 2400, difficulty: "easy" },
  { key: "addition_treatment_iv", name: "処遇改善加算IV", monthlyPerUser: 1200, difficulty: "easy" },
  { key: "addition_specific_i", name: "特定事業所加算I", monthlyPerUser: 6000, difficulty: "hard" },
  { key: "addition_specific_ii", name: "特定事業所加算II", monthlyPerUser: 4500, difficulty: "hard" },
  { key: "addition_specific_iii", name: "特定事業所加算III", monthlyPerUser: 3000, difficulty: "medium" },
  { key: "addition_specific_iv", name: "特定事業所加算IV", monthlyPerUser: 2000, difficulty: "medium" },
  { key: "addition_specific_v", name: "特定事業所加算V", monthlyPerUser: 1500, difficulty: "easy" },
  { key: "addition_dementia_i", name: "認知症ケア加算I", monthlyPerUser: 3000, difficulty: "medium" },
  { key: "addition_dementia_ii", name: "認知症ケア加算II", monthlyPerUser: 1500, difficulty: "easy" },
  { key: "addition_oral", name: "口腔連携加算", monthlyPerUser: 1000, difficulty: "easy" },
  { key: "addition_emergency", name: "緊急時加算", monthlyPerUser: 2000, difficulty: "medium" },
];

const DIFFICULTY_LABELS: Record<string, { label: string; color: string }> = {
  easy: { label: "容易", color: "bg-emerald-100 text-emerald-700" },
  medium: { label: "中程度", color: "bg-amber-100 text-amber-700" },
  hard: { label: "困難", color: "bg-red-100 text-red-700" },
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

function RevenueContent() {
  const { filters, setFilters, toApiParams } = useFilters();
  const apiParams = toApiParams();
  const serviceConfig = useServiceConfig(filters.serviceCodes);

  // 収益KPI取得
  const { data: kpi, error: kpiError, isLoading: kpiLoading } = useApi<RevenueKpi>(
    "/api/revenue/kpi",
    apiParams
  );

  // 加算項目別取得率
  const { data: kasanRates, error: kasanError, isLoading: kasanLoading } = useApi<BonusItemRate[]>(
    "/api/revenue/kasan-rates",
    apiParams
  );

  // 稼働率分布
  const { data: occupancyDist, error: occupancyError, isLoading: occupancyLoading } = useApi<OccupancyBin[]>(
    "/api/revenue/occupancy-distribution",
    apiParams
  );

  const apiError = kpiError || kasanError || occupancyError;

  // 加算取得シミュレーター状態
  const [selectedKasan, setSelectedKasan] = useState<Set<string>>(new Set());
  const [simulatorUserCount, setSimulatorUserCount] = useState<number>(30);

  // シミュレーション結果計算（フロント計算）
  const simulationResults = useMemo(() => {
    if (selectedKasan.size === 0) return { items: [], totalMonthly: 0, totalAnnual: 0 };

    const items = KASAN_SIMULATION_ITEMS
      .filter((item) => selectedKasan.has(item.key))
      .map((item) => ({
        ...item,
        estimatedMonthly: item.monthlyPerUser * simulatorUserCount,
        estimatedAnnual: item.monthlyPerUser * simulatorUserCount * 12,
      }));

    const totalMonthly = items.reduce((sum, item) => sum + item.estimatedMonthly, 0);
    const totalAnnual = items.reduce((sum, item) => sum + item.estimatedAnnual, 0);

    return { items, totalMonthly, totalAnnual };
  }, [selectedKasan, simulatorUserCount]);

  const toggleKasan = useCallback((key: string) => {
    setSelectedKasan((prev) => {
      const next = new Set(prev);
      if (next.has(key)) {
        next.delete(key);
      } else {
        next.add(key);
      }
      return next;
    });
  }, []);

  // KPIの表示判定ヘルパー
  const hasKpi = kpi != null;

  return (
    <div className="space-y-6">
      {/* ページタイトル */}
      <div>
        <h1 className="text-xl font-bold text-gray-900">収益構造分析</h1>
        <p className="text-sm text-gray-500 mt-1">
          加算取得率・稼働率・定員など収益に関する指標を分析
        </p>
      </div>

      {/* APIエラーバナー */}
      <ApiErrorBanner error={apiError} />

      {/* データ取得状況バナー（KPIがない場合のみ表示） */}
      {!hasKpi && !kpiLoading && (
        <div className="bg-amber-50 border border-amber-200 rounded-lg px-4 py-3">
          <div className="flex items-start gap-2">
            <span className="text-amber-500 text-sm mt-0.5">&#9888;</span>
            <div>
              <p className="text-sm font-medium text-amber-800">
                加算データは準備中です
              </p>
              <p className="text-xs text-amber-600 mt-0.5">
                現在のデータソースには加算関連項目が含まれていない可能性があります。フルデータ取得後にチャートが有効化されます。
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

      {/* KPIカード */}
      <KpiCardGrid>
        <KpiCard
          label="平均加算取得数"
          value={kpi?.avg_kasan_count ?? null}
          format="decimal"
          icon={<svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><circle cx="12" cy="12" r="10" /><path d="M8 12h8" /><path d="M12 8v8" /></svg>}
          accentColor="bg-emerald-500"
          subtitle={kpi?.avg_kasan_count != null ? "実データ" : "データ準備中"}
          loading={kpiLoading}
          tooltip="13種類の介護報酬加算のうち取得している数"
        />
        <KpiCard
          label="処遇改善加算取得率"
          value={kpi?.syogu_kaizen_rate ?? null}
          format="percent"
          icon={<svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><path d="M3.85 8.62a4 4 0 0 1 4.78-2.65 4 4 0 0 1 2.9 2.4 4 4 0 0 1 2.9-2.4 4 4 0 0 1 4.78 2.65" /><path d="M12 2v2" /><path d="M3.85 8.62 12 22l8.15-13.38" /><path d="M12 14.5 7.5 8h9z" /></svg>}
          accentColor="bg-blue-500"
          subtitle={kpi?.syogu_kaizen_rate != null ? "実データ" : "データ準備中"}
          loading={kpiLoading}
        />
        {serviceConfig.isAvailable("occupancy") ? (
          <KpiCard
            label="平均稼働率"
            value={kpi?.avg_occupancy_rate ?? null}
            format="percent"
            icon={<svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12" /></svg>}
            accentColor="bg-amber-500"
            subtitle={kpi?.avg_occupancy_rate != null ? "実データ" : "データ準備中"}
            loading={kpiLoading}
            tooltip="利用者数 / 定員"
          />
        ) : (
          <UnavailableNotice message={serviceConfig.reason("occupancy")} />
        )}
        {serviceConfig.isAvailable("capacity") ? (
          <KpiCard
            label="平均定員"
            value={kpi?.avg_capacity ?? null}
            format="decimal"
            icon={<svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2" /><circle cx="9" cy="7" r="4" /><path d="M22 21v-2a4 4 0 0 0-3-3.87" /><path d="M16 3.13a4 4 0 0 1 0 7.75" /></svg>}
            accentColor="bg-indigo-500"
            subtitle={kpi?.avg_capacity != null ? "1施設あたり（実データ）" : "データ準備中"}
            loading={kpiLoading}
          />
        ) : (
          <UnavailableNotice message={serviceConfig.reason("capacity")} />
        )}
      </KpiCardGrid>

      {/* チャートエリア */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* 1. 加算項目別取得率 */}
        <ChartCard
          title="加算項目別取得率"
          subtitle={kasanRates && kasanRates.length > 0 ? `${kasanRates.length}項目` : `${BONUS_ITEMS.length}項目の取得率`}
        >
          {kasanLoading ? (
            <div className="flex items-center justify-center h-[400px] text-gray-400 text-sm">
              読み込み中...
            </div>
          ) : kasanRates && kasanRates.length > 0 ? (
            <BarChart
              data={kasanRates}
              xKey="kasan_name"
              yKey="rate"
              color="#059669"
              horizontal
              tooltipFormatter={(v) => `${(v * 100).toFixed(1)}%`}
              height={400}
            />
          ) : (
            <DataPendingPlaceholder
              message="加算データ準備中"
              description={`${BONUS_ITEMS.length}項目の加算取得率を横棒グラフで表示します`}
              height={400}
            />
          )}
        </ChartCard>

        {/* 2. 稼働率分布 */}
        {serviceConfig.isAvailable("occupancy") ? (
          <ChartCard
            title="稼働率分布"
            subtitle="施設数ベースのヒストグラム"
          >
            {occupancyLoading ? (
              <div className="flex items-center justify-center h-[400px] text-gray-400 text-sm">
                読み込み中...
              </div>
            ) : occupancyDist && occupancyDist.length > 0 ? (
              <BarChart
                data={occupancyDist}
                xKey="range"
                yKey="count"
                color="#d97706"
                tooltipFormatter={(v) => `${v.toLocaleString("ja-JP")}施設`}
                height={400}
              />
            ) : (
              <DataPendingPlaceholder
                message="稼働率データ準備中"
                description="稼働率の分布をヒストグラムで表示します"
                height={400}
              />
            )}
          </ChartCard>
        ) : (
          <ChartCard title="稼働率分布" subtitle="施設数ベースのヒストグラム">
            <UnavailableNotice message={serviceConfig.reason("occupancy")} />
          </ChartCard>
        )}
      </div>

      {/* 加算取得シミュレーター */}
      <ChartCard
        title="加算取得シミュレーター"
        subtitle="未取得加算を選択して推定増収額を計算（フロント計算）"
      >
        <div className="space-y-4">
          {/* 利用者数入力 */}
          <div className="flex items-center gap-3">
            <label className="text-sm font-medium text-gray-700">
              利用者数（月平均）:
            </label>
            <input
              type="number"
              min={1}
              max={500}
              value={simulatorUserCount}
              onChange={(e) => setSimulatorUserCount(Math.max(1, parseInt(e.target.value) || 1))}
              className="w-24 px-3 py-1.5 text-sm border border-gray-300 rounded-lg focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500"
            />
            <span className="text-xs text-gray-500">名</span>
          </div>

          {/* 適用可能な加算の案内（サービス種別選択時） */}
          {filters.serviceCodes.length > 0 && serviceConfig.applicableKasan.length < KASAN_SIMULATION_ITEMS.length && (
            <p className="text-xs text-gray-500 bg-gray-50 rounded-lg px-3 py-2 border border-gray-100">
              選択中のサービス種別に適用可能な加算項目のみ表示しています（{serviceConfig.applicableKasan.length}/{KASAN_SIMULATION_ITEMS.length}項目）
            </p>
          )}

          {/* 加算選択グリッド */}
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-2">
            {KASAN_SIMULATION_ITEMS.filter(
              (item) => filters.serviceCodes.length === 0 || serviceConfig.applicableKasan.includes(item.key)
            ).map((item) => {
              const isSelected = selectedKasan.has(item.key);
              const diffStyle = DIFFICULTY_LABELS[item.difficulty];
              return (
                <button
                  key={item.key}
                  onClick={() => toggleKasan(item.key)}
                  role="checkbox"
                  aria-checked={isSelected}
                  className={`flex items-center justify-between gap-2 px-3 py-2.5 rounded-lg border text-left text-sm transition-all ${
                    isSelected
                      ? "border-indigo-400 bg-indigo-50 text-indigo-900 ring-1 ring-indigo-200"
                      : "border-gray-200 bg-white text-gray-700 hover:border-gray-300"
                  }`}
                >
                  <div className="flex items-center gap-2 min-w-0">
                    <div
                      className={`w-4 h-4 rounded border flex-shrink-0 flex items-center justify-center ${
                        isSelected
                          ? "bg-indigo-500 border-indigo-500"
                          : "border-gray-300 bg-white"
                      }`}
                    >
                      {isSelected && (
                        <svg className="w-3 h-3 text-white" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="3" aria-hidden="true">
                          <polyline points="20 6 9 17 4 12" />
                        </svg>
                      )}
                    </div>
                    <span className="truncate">{item.name}</span>
                  </div>
                  <span className={`flex-shrink-0 text-[10px] px-1.5 py-0.5 rounded-full font-medium ${diffStyle.color}`}>
                    {diffStyle.label}
                  </span>
                </button>
              );
            })}
          </div>

          {/* シミュレーション結果 */}
          {simulationResults?.items?.length > 0 && (
            <div className="mt-4 rounded-lg border border-indigo-200 bg-indigo-50/50 p-4">
              <h4 className="text-sm font-semibold text-indigo-900 mb-3">
                推定増収額（{simulatorUserCount}名ベース）
              </h4>
              <div className="space-y-1.5 mb-3">
                {simulationResults?.items?.map((item) => (
                  <div key={item.key} className="flex items-center justify-between text-xs">
                    <span className="text-gray-700">{item.name}</span>
                    <span className="font-mono text-gray-900">
                      +{item.estimatedMonthly.toLocaleString("ja-JP")}円/月
                    </span>
                  </div>
                ))}
              </div>
              <div className="border-t border-indigo-200 pt-3 flex items-center justify-between">
                <div>
                  <span className="text-xs text-gray-500">月額合計: </span>
                  <span className="text-sm font-bold text-indigo-700">
                    +{simulationResults.totalMonthly.toLocaleString("ja-JP")}円
                  </span>
                </div>
                <div>
                  <span className="text-xs text-gray-500">年間合計: </span>
                  <span className="text-lg font-bold text-indigo-700">
                    +{simulationResults.totalAnnual.toLocaleString("ja-JP")}円
                  </span>
                </div>
              </div>
            </div>
          )}

          {selectedKasan.size === 0 && (
            <p className="text-xs text-gray-400 text-center py-2">
              上記の加算項目を選択すると、推定増収額が表示されます
            </p>
          )}
        </div>
      </ChartCard>

      {/* 加算項目一覧テーブル（参考表示） */}
      <ChartCard
        title="対象加算項目一覧"
        subtitle="フルデータ取得後に分析対象となる項目"
      >
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-2">
          {BONUS_ITEMS.map((item, i) => (
            <div
              key={item}
              className="flex items-center gap-2 px-3 py-2 bg-gray-50 rounded text-sm text-gray-600"
            >
              <span className="text-xs text-gray-400 w-5 text-right">{i + 1}.</span>
              {item}
            </div>
          ))}
        </div>
      </ChartCard>
    </div>
  );
}

export default function RevenuePage() {
  return (
    <Suspense fallback={<div className="text-gray-400 text-sm p-8">読み込み中...</div>}>
      <RevenueContent />
    </Suspense>
  );
}
