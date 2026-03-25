"use client";

// ===================================================
// Page: ベンチマーク（施設比較）
// 施設選択 -> 8軸レーダー + パーセンタイル + 改善提案
// ===================================================

import { Suspense, useState, useCallback, useMemo } from "react";
import { Card } from "@tremor/react";
import { formatServiceName } from "@/lib/formatters";
import { useApi } from "@/hooks/useApi";
import type { BenchmarkData, FacilitySearchResult } from "@/lib/types";
import RadarChart from "@/components/charts/RadarChart";
import ChartCard from "@/components/charts/ChartCard";
import DataPendingPlaceholder from "@/components/common/DataPendingPlaceholder";
import LoadingSpinner from "@/components/common/LoadingSpinner";
import ApiErrorBanner from "@/components/common/ApiErrorBanner";

/** パーセンタイルのバー色 */
function getPercentileColor(percentile: number): string {
  if (percentile >= 75) return "bg-emerald-500";
  if (percentile >= 50) return "bg-blue-500";
  if (percentile >= 25) return "bg-amber-500";
  return "bg-red-500";
}

/** パーセンタイルラベル */
function getPercentileLabel(percentile: number): string {
  if (percentile >= 90) return "上位10%";
  if (percentile >= 75) return "上位25%";
  if (percentile >= 50) return "上位50%";
  if (percentile >= 25) return "下位25%";
  return "下位10%";
}

function BenchmarkContent() {
  const [searchQuery, setSearchQuery] = useState("");
  const [selectedFacilityId, setSelectedFacilityId] = useState<string | null>(null);
  const [selectedFacilityName, setSelectedFacilityName] = useState<string>("");
  const [showDropdown, setShowDropdown] = useState(false);

  // 施設検索
  const { data: searchResult, isLoading: searchLoading } = useApi<FacilitySearchResult>(
    searchQuery.length >= 2 ? "/api/facilities/search" : null,
    { q: searchQuery, per_page: 10, page: 1 }
  );

  // ベンチマークデータ取得
  const { data: benchmarkData, isLoading: benchmarkLoading, error: benchmarkError } =
    useApi<BenchmarkData>(
      selectedFacilityId ? `/api/benchmark/${selectedFacilityId}` : null
    );

  // 施設選択
  const handleSelect = useCallback((id: string, name: string) => {
    setSelectedFacilityId(id);
    setSelectedFacilityName(name);
    setSearchQuery(name);
    setShowDropdown(false);
  }, []);

  // 検索クリア
  const handleClear = useCallback(() => {
    setSelectedFacilityId(null);
    setSelectedFacilityName("");
    setSearchQuery("");
    setShowDropdown(false);
  }, []);

  // レーダーチャート用データ整形
  const radarData = useMemo(() => {
    if (!benchmarkData?.radar) return [];
    return benchmarkData.radar.map((item) => ({
      axis: item.axis,
      value: item.value,
      national_avg: item.national_avg,
      pref_avg: item.pref_avg,
    }));
  }, [benchmarkData]);

  return (
    <div className="space-y-6">
      {/* ページタイトル */}
      <div>
        <h1 className="text-xl font-bold text-gray-900">ベンチマーク（施設比較）</h1>
        <p className="text-sm text-gray-500 mt-1">
          施設を選択して、全国・地域・同サービスでの位置を8軸で比較分析
        </p>
      </div>

      {/* 施設検索バー */}
      <Card>
        <div className="relative">
          <label className="block text-sm font-medium text-gray-700 mb-2">
            施設名 / 事業所番号で検索
          </label>
          <div className="flex gap-2">
            <div className="flex-1 relative">
              <input
                type="text"
                className="w-full px-3 py-2 text-sm border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-brand-500"
                placeholder="例: 特別養護老人ホーム和光苑 / 1370500123"
                value={searchQuery}
                onChange={(e) => {
                  setSearchQuery(e.target.value);
                  setShowDropdown(true);
                  if (e.target.value.length < 2) {
                    setShowDropdown(false);
                  }
                }}
                onFocus={() => {
                  if (searchQuery.length >= 2) setShowDropdown(true);
                }}
              />

              {/* 検索候補ドロップダウン */}
              {showDropdown && searchQuery.length >= 2 && (
                <div className="absolute top-full left-0 right-0 mt-1 bg-white border border-gray-200 rounded-lg shadow-lg z-20 max-h-64 overflow-y-auto">
                  {searchLoading ? (
                    <div className="px-4 py-3 text-sm text-gray-400">検索中...</div>
                  ) : searchResult && searchResult.items?.length > 0 ? (
                    searchResult.items.map((facility) => (
                      <button
                        key={facility.jigyosho_number}
                        onClick={() => handleSelect(facility.jigyosho_number, facility.jigyosho_name)}
                        className="w-full text-left px-4 py-2.5 hover:bg-indigo-50 transition-colors border-b border-gray-50 last:border-b-0"
                      >
                        <div className="text-sm font-medium text-gray-900">{facility.jigyosho_name}</div>
                        <div className="text-xs text-gray-500 mt-0.5">
                          {facility.corp_name} | {facility.address} | {formatServiceName(facility.service_name)}
                        </div>
                      </button>
                    ))
                  ) : (
                    <div className="px-4 py-3 text-sm text-gray-400">該当する施設が見つかりません</div>
                  )}
                </div>
              )}
            </div>
            {selectedFacilityId && (
              <button
                onClick={handleClear}
                className="px-3 py-2 text-sm text-gray-500 hover:text-gray-700 border border-gray-300 rounded-lg hover:bg-gray-50 transition-colors"
              >
                クリア
              </button>
            )}
          </div>
          {selectedFacilityName && (
            <p className="mt-2 text-sm text-indigo-600 font-medium">
              選択中: {selectedFacilityName}
            </p>
          )}
        </div>
      </Card>

      {/* APIエラーバナー */}
      <ApiErrorBanner error={benchmarkError} />

      {/* ローディング */}
      {benchmarkLoading && selectedFacilityId && (
        <Card>
          <LoadingSpinner text="ベンチマークデータを計算中..." />
        </Card>
      )}

      {/* ベンチマーク結果 */}
      {benchmarkData && !benchmarkLoading && (
        <div className="space-y-6 animate-fade-in-up">
          {/* 8軸レーダーチャート */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <ChartCard
              title="8軸ベンチマークレーダー"
              subtitle="対象施設 vs 全国平均"
            >
              {radarData.length > 0 ? (
                <RadarChart
                  data={radarData}
                  categoryKey="axis"
                  series={[
                    { dataKey: "value", name: "対象施設", color: "#4f46e5" },
                    { dataKey: "national_avg", name: "全国平均", color: "#d1d5db" },
                    { dataKey: "pref_avg", name: "都道府県平均", color: "#f59e0b" },
                  ]}
                  height={360}
                />
              ) : (
                <DataPendingPlaceholder
                  message="レーダーデータなし"
                  description="ベンチマーク指標が不足しています"
                  height={360}
                />
              )}
            </ChartCard>

            {/* パーセンタイル一覧 */}
            <ChartCard
              title="パーセンタイル（全国/地域/同サービス）"
              subtitle="各指標の順位（高いほど上位）"
            >
              {benchmarkData?.percentiles && Object.keys(benchmarkData.percentiles.national || {}).length > 0 ? (
                <div className="space-y-3">
                  {Object.keys(benchmarkData.percentiles.national).map((metric) => {
                    const nationalVal = benchmarkData.percentiles.national[metric] ?? 0;
                    const prefVal = benchmarkData.percentiles.prefecture?.[metric] ?? 0;
                    const serviceVal = benchmarkData.percentiles.service?.[metric] ?? 0;
                    return (
                      <div key={metric}>
                        <div className="flex items-center justify-between mb-1">
                          <span className="text-xs font-medium text-gray-700">{metric}</span>
                        </div>
                        <div className="grid grid-cols-3 gap-1.5">
                          {/* 全国 */}
                          <div>
                            <div className="flex items-center justify-between mb-0.5">
                              <span className="text-[9px] text-gray-400">全国</span>
                              <span className="text-[9px] font-medium text-gray-600">
                                {getPercentileLabel(nationalVal)}
                              </span>
                            </div>
                            <div className="h-1.5 bg-gray-100 rounded-full overflow-hidden">
                              <div
                                className={`h-full rounded-full transition-all duration-500 ${getPercentileColor(nationalVal)}`}
                                style={{ width: `${nationalVal}%` }}
                              />
                            </div>
                          </div>
                          {/* 都道府県 */}
                          <div>
                            <div className="flex items-center justify-between mb-0.5">
                              <span className="text-[9px] text-gray-400">都道府県</span>
                              <span className="text-[9px] font-medium text-gray-600">
                                {getPercentileLabel(prefVal)}
                              </span>
                            </div>
                            <div className="h-1.5 bg-gray-100 rounded-full overflow-hidden">
                              <div
                                className={`h-full rounded-full transition-all duration-500 ${getPercentileColor(prefVal)}`}
                                style={{ width: `${prefVal}%` }}
                              />
                            </div>
                          </div>
                          {/* 同サービス */}
                          <div>
                            <div className="flex items-center justify-between mb-0.5">
                              <span className="text-[9px] text-gray-400">同種別</span>
                              <span className="text-[9px] font-medium text-gray-600">
                                {getPercentileLabel(serviceVal)}
                              </span>
                            </div>
                            <div className="h-1.5 bg-gray-100 rounded-full overflow-hidden">
                              <div
                                className={`h-full rounded-full transition-all duration-500 ${getPercentileColor(serviceVal)}`}
                                style={{ width: `${serviceVal}%` }}
                              />
                            </div>
                          </div>
                        </div>
                      </div>
                    );
                  })}
                </div>
              ) : (
                <DataPendingPlaceholder
                  message="パーセンタイルデータなし"
                  description="各指標のパーセンタイルはフルデータ取得後に表示されます"
                  height={360}
                />
              )}
            </ChartCard>
          </div>

          {/* 改善提案 */}
          <ChartCard
            title="改善提案（自動生成）"
            subtitle="最も弱い指標に対する優先度付き改善案"
          >
            {benchmarkData?.improvement_suggestions && benchmarkData.improvement_suggestions.length > 0 ? (
              <div className="space-y-3">
                {benchmarkData.improvement_suggestions.map((sug, idx) => (
                  <div
                    key={idx}
                    className="flex gap-3 p-3 rounded-lg border border-gray-200 bg-white"
                  >
                    <div className="flex-shrink-0">
                      <div className={`w-8 h-8 rounded-full flex items-center justify-center text-xs font-bold text-white ${
                        idx === 0 ? "bg-red-500" : idx === 1 ? "bg-amber-500" : "bg-blue-500"
                      }`}>
                        {idx + 1}
                      </div>
                    </div>
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-2 mb-1">
                        <span className="text-sm font-semibold text-gray-900">{sug.axis}</span>
                        <span className="text-xs text-gray-500">
                          現在: {sug.current} → 目標: {sug.target}
                        </span>
                      </div>
                      <p className="text-xs text-gray-600">{sug.suggestion}</p>
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <DataPendingPlaceholder
                message="改善提案データなし"
                description="フルデータ取得後に自動生成される改善提案を表示します"
                height={200}
              />
            )}
          </ChartCard>
        </div>
      )}

      {/* 未選択状態 */}
      {!selectedFacilityId && (
        <Card>
          <div className="flex flex-col items-center justify-center py-16 text-center">
            <div className="w-20 h-20 mb-4 rounded-full bg-gray-100 flex items-center justify-center">
              <svg
                className="w-10 h-10 text-gray-300"
                fill="none"
                stroke="currentColor"
                viewBox="0 0 24 24"
                aria-hidden="true"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={1.5}
                  d="M3 13.125C3 12.504 3.504 12 4.125 12h2.25c.621 0 1.125.504 1.125 1.125v6.75C7.5 20.496 6.996 21 6.375 21h-2.25A1.125 1.125 0 013 19.875v-6.75zM9.75 8.625c0-.621.504-1.125 1.125-1.125h2.25c.621 0 1.125.504 1.125 1.125v11.25c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 01-1.125-1.125V8.625zM16.5 4.125c0-.621.504-1.125 1.125-1.125h2.25C20.496 3 21 3.504 21 4.125v15.75c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 01-1.125-1.125V4.125z"
                />
              </svg>
            </div>
            <p className="text-sm font-medium text-gray-500">
              施設を検索して選択してください
            </p>
            <p className="text-xs text-gray-400 mt-1">
              8軸レーダー・パーセンタイル・改善提案が自動生成されます
            </p>
            <div className="mt-6 grid grid-cols-2 md:grid-cols-4 gap-3 text-center">
              {["離職率", "常勤比率", "稼働率", "加算取得数", "品質スコア", "経験者割合", "重度率", "定員充足率"].map((metric) => (
                <div key={metric} className="px-3 py-2 bg-gray-50 rounded-lg text-xs text-gray-500">
                  {metric}
                </div>
              ))}
            </div>
          </div>
        </Card>
      )}
    </div>
  );
}

export default function BenchmarkPage() {
  return (
    <Suspense fallback={<div className="text-gray-400 text-sm p-8">読み込み中...</div>}>
      <BenchmarkContent />
    </Suspense>
  );
}
