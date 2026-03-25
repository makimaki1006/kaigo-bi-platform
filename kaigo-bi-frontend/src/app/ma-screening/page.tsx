"use client";

// ===================================================
// Page 11: M&Aスクリーニング
// 買収候補を体系的にスクリーニングする最重要ページ
// 拡張フィルタ + ターゲットリスト + ファネルチャート
// 実API: /api/ma/screening (サーバーサイドフィルタリング)
// ===================================================

import { Suspense, useState, useMemo, useCallback } from "react";
import { Card } from "@tremor/react";
import KpiCard from "@/components/data-display/KpiCard";
import ChartCard from "@/components/charts/ChartCard";
import DataTable from "@/components/data-display/DataTable";
import RangeSlider from "@/components/filters/RangeSlider";
import PrefectureSelect from "@/components/filters/PrefectureSelect";
import ServiceTypeSelect from "@/components/filters/ServiceTypeSelect";
import LoadingSpinner from "@/components/common/LoadingSpinner";
import MultiSelectDropdown from "@/components/ui/MultiSelectDropdown";
import { CORP_TYPES } from "@/lib/constants";
import { formatServiceName } from "@/lib/formatters";
import { useApi } from "@/hooks/useApi";
import ApiErrorBanner from "@/components/common/ApiErrorBanner";
import type { ColumnDef, MaCandidate, MaCandidateExtended, MaScreeningResponse } from "@/lib/types";

/** スクリーニングフィルタの状態 */
interface ScreeningFilters {
  prefectures: string[];
  serviceCodes: string[];
  corpTypes: string[];
  employeeMin: number | null;
  employeeMax: number | null;
  turnoverMin: number | null;
  turnoverMax: number | null;
  facilityCountMin: number | null;
  facilityCountMax: number | null;
}

const DEFAULT_SCREENING_FILTERS: ScreeningFilters = {
  prefectures: [],
  serviceCodes: [],
  corpTypes: [],
  employeeMin: null,
  employeeMax: null,
  turnoverMin: null,
  turnoverMax: null,
  facilityCountMin: null,
  facilityCountMax: null,
};

/** ターゲットリストのカラム定義（実APIのMaCandidateに対応） */
const TARGET_COLUMNS: ColumnDef<MaCandidate>[] = [
  {
    key: "corp_name",
    label: "法人名",
    sortable: true,
    width: "200px",
    render: (value: string) => (
      <span className="font-medium text-gray-900">{value}</span>
    ),
  },
  {
    key: "facility_count",
    label: "施設数",
    sortable: true,
    width: "70px",
    render: (value: number) => (
      <span className="font-semibold text-blue-600">{value}</span>
    ),
  },
  {
    key: "prefectures",
    label: "地域",
    sortable: false,
    width: "120px",
    render: (value: string[]) => (
      <span className="text-gray-700">
        {value.slice(0, 2).join(", ")}
        {value.length > 2 && ` +${value.length - 2}`}
      </span>
    ),
  },
  {
    key: "total_staff",
    label: "従業者数",
    sortable: true,
    width: "80px",
    render: (value: number) => `${Math.round(value)}人`,
  },
  {
    key: "avg_turnover_rate",
    label: "離職率",
    sortable: true,
    width: "80px",
    render: (value: number | null) => {
      if (value == null) return "-";
      const color =
        value > 25 ? "text-red-600" : value > 15 ? "text-orange-500" : "text-green-600";
      return <span className={`font-medium ${color}`}>{value.toFixed(1)}%</span>;
    },
  },
  {
    key: "attractiveness_score",
    label: "魅力度",
    sortable: true,
    width: "90px",
    render: (value: number) => {
      const score = Math.round(value);
      const barColor =
        score >= 80
          ? "bg-emerald-500"
          : score >= 60
          ? "bg-amber-500"
          : "bg-red-400";
      const textColor =
        score >= 80
          ? "text-emerald-700"
          : score >= 60
          ? "text-amber-700"
          : "text-red-600";
      return (
        <div className="flex items-center gap-2">
          <div className="w-16 h-2 bg-gray-100 rounded-full overflow-hidden">
            <div
              className={`h-full rounded-full ${barColor}`}
              style={{ width: `${score}%` }}
            />
          </div>
          <span className={`text-xs font-semibold tabular-nums ${textColor}`}>
            {score}
          </span>
        </div>
      );
    },
  },
  {
    key: "quality_score",
    label: "品質",
    sortable: true,
    width: "60px",
    render: (value: number | null) => {
      if (value == null) return <span className="text-gray-300 text-xs">-</span>;
      const score = Math.round(value);
      const color = score >= 65 ? "text-emerald-600" : score >= 50 ? "text-amber-600" : "text-red-500";
      return <span className={`text-xs font-semibold ${color}`}>{score}</span>;
    },
  },
  {
    key: "addition_count",
    label: "加算",
    sortable: true,
    width: "60px",
    render: (value: number | null) => {
      if (value == null) return <span className="text-gray-300 text-xs">-</span>;
      return <span className="text-xs font-medium text-gray-700">{value}/13</span>;
    },
  },
  {
    key: "occupancy_rate",
    label: "稼働率",
    sortable: true,
    width: "70px",
    render: (value: number | null) => {
      if (value == null) return <span className="text-gray-300 text-xs">-</span>;
      const pct = (value * 100).toFixed(0);
      const color = value >= 0.8 ? "text-emerald-600" : value >= 0.6 ? "text-amber-500" : "text-red-500";
      return <span className={`text-xs font-medium ${color}`}>{pct}%</span>;
    },
  },
  {
    key: "service_names",
    label: "サービス種別",
    width: "200px",
    render: (value: string[]) => (
      <div className="flex flex-wrap gap-1">
        {value.slice(0, 3).map((s) => (
          <span
            key={s}
            className="inline-block px-1.5 py-0.5 bg-gray-100 text-gray-600 rounded text-[10px]"
          >
            {formatServiceName(s)}
          </span>
        ))}
        {value.length > 3 && (
          <span className="text-[10px] text-gray-400">+{value.length - 3}</span>
        )}
      </div>
    ),
  },
];

function MaScreeningContent() {
  const [screeningFilters, setScreeningFilters] = useState<ScreeningFilters>(
    DEFAULT_SCREENING_FILTERS
  );

  // フィルタ更新ヘルパー
  const updateFilter = useCallback(
    (partial: Partial<ScreeningFilters>) => {
      setScreeningFilters((prev) => ({ ...prev, ...partial }));
    },
    []
  );

  // フィルタリセット
  const resetFilters = useCallback(() => {
    setScreeningFilters(DEFAULT_SCREENING_FILTERS);
  }, []);

  // フィルタ値をAPIパラメータに変換
  const apiParams = useMemo(() => {
    const params: Record<string, string | number | string[] | null | undefined> = {
      limit: 100,
    };
    if (screeningFilters.prefectures.length > 0) {
      params.prefecture = screeningFilters.prefectures.join(",");
    }
    if (screeningFilters.serviceCodes.length > 0) {
      params.service_code = screeningFilters.serviceCodes.join(",");
    }
    if (screeningFilters.corpTypes.length > 0) {
      params.corp_type = screeningFilters.corpTypes.join(",");
    }
    if (screeningFilters.employeeMin != null) {
      params.staff_min = screeningFilters.employeeMin;
    }
    if (screeningFilters.employeeMax != null) {
      params.staff_max = screeningFilters.employeeMax;
    }
    if (screeningFilters.turnoverMin != null) {
      params.turnover_min = screeningFilters.turnoverMin;
    }
    if (screeningFilters.turnoverMax != null) {
      params.turnover_max = screeningFilters.turnoverMax;
    }
    return params;
  }, [screeningFilters]);

  // 実API呼び出し
  const { data, isLoading, error } = useApi<MaScreeningResponse>(
    "/api/ma/screening",
    apiParams
  );

  // 候補リスト
  const items = data?.items ?? [];
  const total = data?.total ?? 0;
  const funnel = data?.funnel ?? [];

  // 平均魅力度スコア
  const avgAttractiveness = useMemo(() => {
    if (items.length === 0) return 0;
    return Math.round(
      items.reduce((sum, t) => sum + t.attractiveness_score, 0) / items.length
    );
  }, [items]);

  return (
    <div className="space-y-6">
      {/* ページタイトル */}
      <div>
        <h1 className="text-xl font-bold text-gray-900">M&Aスクリーニング</h1>
        <p className="text-sm text-gray-500 mt-1">
          多段階フィルタで買収候補法人を体系的にスクリーニング。魅力度スコアによるランキング。
        </p>
      </div>

      {/* メインレイアウト: 左フィルタ + 右コンテンツ */}
      <div className="flex flex-col lg:flex-row gap-6 overflow-hidden">
        {/* 左: 拡張フィルタパネル（モバイルでは全幅、lgで固定幅） */}
        <div className="w-full lg:w-72 lg:flex-shrink-0">
          <Card className="lg:sticky lg:top-20 !border-brand-200 !ring-1 !ring-brand-100">
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-sm font-semibold text-brand-800">
                スクリーニング条件
              </h3>
              <button
                onClick={resetFilters}
                className="text-xs text-brand-600 hover:text-brand-800 transition-colors"
              >
                リセット
              </button>
            </div>

            <div className="space-y-4">
              {/* 地域 */}
              <PrefectureSelect
                value={screeningFilters.prefectures}
                onChange={(v) => updateFilter({ prefectures: v })}
              />

              {/* サービス種別 */}
              <ServiceTypeSelect
                value={screeningFilters.serviceCodes}
                onChange={(v) => updateFilter({ serviceCodes: v })}
              />

              {/* 法人種別 */}
              <div>
                <label className="block text-xs font-medium text-gray-600 mb-1">
                  法人種別
                </label>
                <MultiSelectDropdown
                  value={screeningFilters.corpTypes}
                  onValueChange={(v) => updateFilter({ corpTypes: v })}
                  placeholder="法人種別..."
                  options={CORP_TYPES.map((type) => ({
                    value: type,
                    label: type,
                  }))}
                />
              </div>

              {/* 従業者数レンジ */}
              <RangeSlider
                label="従業者数"
                minValue={screeningFilters.employeeMin}
                maxValue={screeningFilters.employeeMax}
                onMinChange={(v) => updateFilter({ employeeMin: v })}
                onMaxChange={(v) => updateFilter({ employeeMax: v })}
                minPlaceholder="下限"
                maxPlaceholder="上限"
              />

              {/* 離職率レンジ */}
              <RangeSlider
                label="離職率(%)"
                minValue={screeningFilters.turnoverMin}
                maxValue={screeningFilters.turnoverMax}
                onMinChange={(v) => updateFilter({ turnoverMin: v })}
                onMaxChange={(v) => updateFilter({ turnoverMax: v })}
                minPlaceholder="下限"
                maxPlaceholder="上限"
              />

              {/* 法人施設数レンジ */}
              <RangeSlider
                label="施設数"
                minValue={screeningFilters.facilityCountMin}
                maxValue={screeningFilters.facilityCountMax}
                onMinChange={(v) => updateFilter({ facilityCountMin: v })}
                onMaxChange={(v) => updateFilter({ facilityCountMax: v })}
                minPlaceholder="下限"
                maxPlaceholder="上限"
              />

              {/* 稼働率・品質スコア（プレースホルダー） */}
              <div className="border-t border-gray-200 pt-3">
                <p className="text-[10px] text-gray-400 mb-2">
                  以下はフルデータ取得後に有効化
                </p>
                <div className="hidden space-y-3">
                  <RangeSlider
                    label="稼働率(%)"
                    minValue={null}
                    maxValue={null}
                    onMinChange={() => {}}
                    onMaxChange={() => {}}
                    minPlaceholder="下限"
                    maxPlaceholder="上限"
                  />
                  <RangeSlider
                    label="品質スコア"
                    minValue={null}
                    maxValue={null}
                    onMinChange={() => {}}
                    onMaxChange={() => {}}
                    minPlaceholder="下限"
                    maxPlaceholder="上限"
                  />
                </div>
              </div>
            </div>
          </Card>
        </div>

        {/* 右: メインコンテンツ（min-w-0でflex子要素のオーバーフロー防止） */}
        <div className="flex-1 min-w-0 space-y-6">
          {/* APIエラーバナー */}
          <ApiErrorBanner error={error} />

          {/* KPIカード */}
          <div className="grid grid-cols-2 gap-4">
            <KpiCard
              label="候補法人数"
              value={isLoading ? null : total}
              format="number"
              icon={<svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="12" r="10" /><circle cx="12" cy="12" r="6" /><circle cx="12" cy="12" r="2" /></svg>}
              accentColor="bg-brand-500"
              subtitle="現在のフィルタ条件に合致"
            />
            <KpiCard
              label="平均魅力度スコア"
              value={isLoading ? null : avgAttractiveness}
              format="number"
              icon={<svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2" /></svg>}
              accentColor="bg-amber-500"
              subtitle="100点満点"
            />
          </div>

          {/* ターゲットリスト */}
          <ChartCard
            title="ターゲットリスト"
            subtitle={
              isLoading
                ? "データを読み込み中..."
                : `${items.length}件の買収候補法人（全${total}件中）`
            }
          >
            {isLoading ? (
              <LoadingSpinner text="スクリーニング中..." />
            ) : items.length === 0 ? (
              <div className="py-12 text-center text-sm text-gray-400">
                条件に合致する法人が見つかりませんでした。フィルタ条件を調整してください。
              </div>
            ) : (
              <DataTable<MaCandidate>
                columns={TARGET_COLUMNS}
                data={items}
              />
            )}
          </ChartCard>

          {/* ファネルチャート */}
          <ChartCard
            title="スクリーニングファネル"
            subtitle="フィルタ段階ごとの候補数の推移"
          >
            {isLoading ? (
              <LoadingSpinner text="ファネル計算中..." />
            ) : funnel.length === 0 ? (
              <div className="py-8 text-center text-sm text-gray-400">
                ファネルデータなし
              </div>
            ) : (
              <div className="space-y-2">
                {funnel.map((step, idx) => {
                  const maxCount = funnel[0].count;
                  const widthPercent = maxCount > 0 ? (step.count / maxCount) * 100 : 0;
                  const brandGradient = ["#a5b4fc", "#818cf8", "#6366f1", "#4f46e5", "#4338ca", "#3730a3", "#312e81"];
                  const color = brandGradient[idx % brandGradient.length];

                  return (
                    <div key={step.stage} className="flex items-center gap-3">
                      <div className="w-28 text-right text-xs text-gray-600 flex-shrink-0">
                        {step.stage}
                      </div>
                      <div className="flex-1 relative h-8">
                        <div
                          className="absolute inset-y-0 left-0 rounded-r-md flex items-center transition-all duration-500"
                          style={{
                            width: `${Math.max(widthPercent, 3)}%`,
                            backgroundColor: color,
                          }}
                        >
                          <span className="text-white text-xs font-semibold pl-3 whitespace-nowrap">
                            {step.count.toLocaleString("ja-JP")}
                          </span>
                        </div>
                      </div>
                      {idx > 0 && funnel[idx - 1].count > 0 && (
                        <div className="w-16 text-right text-[10px] text-gray-400 flex-shrink-0">
                          -{((1 - step.count / funnel[idx - 1].count) * 100).toFixed(0)}%
                        </div>
                      )}
                    </div>
                  );
                })}
              </div>
            )}
          </ChartCard>

          {/* 魅力度スコア算出基準 */}
          <ChartCard
            title="魅力度スコア算出基準"
            subtitle="4カテゴリの加重平均で0-100点をスコアリング"
          >
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
              {[
                {
                  category: "改善余地",
                  weight: "30%",
                  items: ["(1-稼働率)の大きさ", "未取得加算の数", "離職率の高さ"],
                  color: "bg-orange-50 border-orange-200",
                  textColor: "text-orange-700",
                },
                {
                  category: "市場ポテンシャル",
                  weight: "30%",
                  items: ["需給ギャップ", "高齢化率", "人口動態"],
                  color: "bg-blue-50 border-blue-200",
                  textColor: "text-blue-700",
                },
                {
                  category: "事業基盤",
                  weight: "20%",
                  items: ["事業年数", "従業者規模", "サービス多角化"],
                  color: "bg-green-50 border-green-200",
                  textColor: "text-green-700",
                },
                {
                  category: "リスク調整",
                  weight: "20%",
                  items: ["行政処分なし", "BCP策定", "保険加入"],
                  color: "bg-purple-50 border-purple-200",
                  textColor: "text-purple-700",
                },
              ].map((cat) => (
                <div
                  key={cat.category}
                  className={`rounded-lg border p-4 ${cat.color}`}
                >
                  <div className="flex items-center justify-between mb-2">
                    <h4 className={`text-sm font-semibold ${cat.textColor}`}>
                      {cat.category}
                    </h4>
                    <span className="text-xs font-medium text-gray-500">
                      ウェイト: {cat.weight}
                    </span>
                  </div>
                  <ul className="space-y-1">
                    {cat.items.map((item) => (
                      <li
                        key={item}
                        className="text-xs text-gray-600 flex items-center gap-1.5"
                      >
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
      </div>
    </div>
  );
}

export default function MaScreeningPage() {
  return (
    <Suspense fallback={<div className="text-gray-400 text-sm p-8">読み込み中...</div>}>
      <MaScreeningContent />
    </Suspense>
  );
}
