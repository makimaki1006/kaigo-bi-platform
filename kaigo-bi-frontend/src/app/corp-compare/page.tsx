"use client";

// ===================================================
// Page T4: 法人比較カード
// 2法人を選択して経営指標をサイドバイサイドで比較
// /api/corp-group/top-corps エンドポイント使用
// ===================================================

import { Suspense, useState, useMemo, useCallback } from "react";
import { useApi } from "@/hooks/useApi";
import ApiErrorBanner from "@/components/common/ApiErrorBanner";
import type { TopCorpRow } from "@/lib/types";

// ===================================================
// 比較指標定義
// ===================================================

interface CompareMetric {
  label: string;
  key: string;
  getValue: (corp: TopCorpRow) => number | null;
  format: (value: number | null) => string;
  /** trueの場合は値が高いほうが良い、falseの場合は値が低いほうが良い */
  higherIsBetter: boolean;
}

const COMPARE_METRICS: CompareMetric[] = [
  {
    label: "施設数",
    key: "facility_count",
    getValue: (c) => c.facility_count,
    format: (v) => (v != null ? `${v.toLocaleString("ja-JP")}施設` : "---"),
    higherIsBetter: true,
  },
  {
    label: "従業員数合計",
    key: "total_staff",
    getValue: (c) => c.total_staff,
    format: (v) => (v != null ? `${Math.round(v).toLocaleString("ja-JP")}人` : "---"),
    higherIsBetter: true,
  },
  {
    label: "平均離職率",
    key: "avg_turnover_rate",
    getValue: (c) => c.avg_turnover_rate,
    format: (v) => (v != null ? `${(v * 100).toFixed(1)}%` : "---"),
    higherIsBetter: false,
  },
  {
    label: "展開都道府県数",
    key: "prefectures_count",
    getValue: (c) => c.prefectures?.length ?? 0,
    format: (v) => (v != null ? `${v}都道府県` : "---"),
    higherIsBetter: true,
  },
  {
    label: "展開サービス種別数",
    key: "service_count",
    getValue: (c) => c.service_names?.length ?? 0,
    format: (v) => (v != null ? `${v}種別` : "---"),
    higherIsBetter: true,
  },
];

// ===================================================
// 比較バッジの色
// ===================================================

function getComparisonColor(
  valueA: number | null,
  valueB: number | null,
  higherIsBetter: boolean,
  side: "A" | "B"
): string {
  if (valueA == null || valueB == null || valueA === valueB) {
    return "text-gray-700";
  }
  const isSideWinning = side === "A"
    ? (higherIsBetter ? valueA > valueB : valueA < valueB)
    : (higherIsBetter ? valueB > valueA : valueB < valueA);
  return isSideWinning ? "text-emerald-700 font-bold" : "text-red-600";
}

function getComparisonBg(
  valueA: number | null,
  valueB: number | null,
  higherIsBetter: boolean,
  side: "A" | "B"
): string {
  if (valueA == null || valueB == null || valueA === valueB) {
    return "bg-gray-50";
  }
  const isSideWinning = side === "A"
    ? (higherIsBetter ? valueA > valueB : valueA < valueB)
    : (higherIsBetter ? valueB > valueA : valueB < valueA);
  return isSideWinning ? "bg-emerald-50" : "bg-red-50";
}

// ===================================================
// 法人選択ドロップダウンコンポーネント
// ===================================================

interface CorpSelectProps {
  label: string;
  corps: TopCorpRow[];
  selectedCorp: TopCorpRow | null;
  onSelect: (corp: TopCorpRow | null) => void;
  disabledCorpNumber?: string | null;
}

function CorpSelect({ label, corps, selectedCorp, onSelect, disabledCorpNumber }: CorpSelectProps) {
  const [query, setQuery] = useState("");

  const filteredCorps = useMemo(() => {
    if (!query) return corps.slice(0, 50);
    const q = query.toLowerCase();
    return corps
      .filter((c) => c.corp_name.toLowerCase().includes(q) || c.corp_number?.includes(q))
      .slice(0, 50);
  }, [corps, query]);

  // 選択済み表示
  if (selectedCorp) {
    return (
      <div className="flex-1">
        <label className="block text-xs font-medium text-gray-600 mb-1">{label}</label>
        <div className="flex items-center gap-2 px-3 py-2 bg-brand-50 border border-brand-200 rounded-lg">
          <span className="flex-1 text-sm font-medium text-brand-800 truncate">
            {selectedCorp.corp_name}
          </span>
          <span className="text-[10px] font-mono text-brand-500">
            {selectedCorp.facility_count}施設
          </span>
          <button
            onClick={() => {
              onSelect(null);
              setQuery("");
            }}
            className="ml-1 w-5 h-5 flex items-center justify-center rounded-full bg-brand-200 text-brand-600 hover:bg-brand-300 transition-colors"
            aria-label={`${label}の選択を解除`}
          >
            <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24" aria-hidden="true">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="flex-1">
      <label className="block text-xs font-medium text-gray-600 mb-1">{label}</label>
      <input
        type="text"
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        placeholder="法人名を検索..."
        className="w-full px-3 py-2 text-sm border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-brand-500 focus:border-brand-500 mb-2"
      />
      <div className="border border-gray-200 rounded-lg max-h-48 overflow-y-auto bg-white">
        {filteredCorps.length === 0 ? (
          <div className="px-3 py-4 text-center text-sm text-gray-400">
            該当する法人がありません
          </div>
        ) : (
          filteredCorps.map((corp) => {
            const isDisabled = corp.corp_number === disabledCorpNumber;
            return (
              <button
                key={corp.corp_number}
                onClick={() => !isDisabled && onSelect(corp)}
                disabled={isDisabled}
                className={`w-full text-left px-3 py-2 border-b border-gray-100 last:border-b-0 transition-colors ${
                  isDisabled
                    ? "opacity-40 cursor-not-allowed bg-gray-50"
                    : "hover:bg-brand-50 cursor-pointer"
                }`}
              >
                <div className="text-sm font-medium text-gray-900 truncate">
                  {corp.corp_name}
                </div>
                <div className="flex items-center gap-3 mt-0.5 text-[10px] text-gray-400">
                  <span>{corp.facility_count}施設</span>
                  <span>{Math.round(corp.total_staff)}人</span>
                  <span>{corp.corp_type ?? "---"}</span>
                </div>
              </button>
            );
          })
        )}
      </div>
    </div>
  );
}

// ===================================================
// メインコンテンツ
// ===================================================

function CorpCompareContent() {
  const [corpA, setCorpA] = useState<TopCorpRow | null>(null);
  const [corpB, setCorpB] = useState<TopCorpRow | null>(null);

  // TOP法人リストを取得
  const { data: topCorps, error, isLoading } = useApi<TopCorpRow[]>(
    "/api/corp-group/top-corps",
    { limit: 200 }
  );

  const corps = topCorps ?? [];

  const handleSelectA = useCallback((corp: TopCorpRow | null) => setCorpA(corp), []);
  const handleSelectB = useCallback((corp: TopCorpRow | null) => setCorpB(corp), []);

  const bothSelected = corpA != null && corpB != null;

  return (
    <div className="space-y-6">
      {/* ページタイトル */}
      <div>
        <h1 className="text-heading-lg text-gray-900">法人比較</h1>
        <p className="text-body-md text-gray-500 mt-1">
          2法人を選択して経営指標を比較
        </p>
      </div>

      {/* データソースバナー */}
      <div
        className="rounded-lg border border-amber-200 bg-amber-50 px-4 py-3"
        role="note"
        aria-label="データソース情報"
      >
        <div className="flex items-start gap-2">
          <svg className="w-5 h-5 text-amber-600 mt-0.5 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={1.5} aria-hidden="true">
            <path strokeLinecap="round" strokeLinejoin="round" d="M11.25 11.25l.041-.02a.75.75 0 011.063.852l-.708 2.836a.75.75 0 001.063.853l.041-.021M21 12a9 9 0 11-18 0 9 9 0 0118 0zm-9-3.75h.008v.008H12V8.25z" />
          </svg>
          <p className="text-sm text-amber-800">
            データソース: 厚労省介護サービス情報公表システム（施設数・従業員数・離職率等は公表データに基づく）
          </p>
        </div>
      </div>

      {/* APIエラーバナー */}
      <ApiErrorBanner error={error} />

      {/* 法人選択エリア */}
      <div className="bg-white rounded-xl shadow-card p-5">
        <h2 className="text-sm font-medium text-gray-700 mb-4">
          比較する法人を選択してください
        </h2>
        {isLoading ? (
          <div className="flex gap-6">
            <div className="flex-1 h-32 shimmer rounded-lg" />
            <div className="flex-1 h-32 shimmer rounded-lg" />
          </div>
        ) : (
          <div className="flex gap-6 flex-col md:flex-row">
            <CorpSelect
              label="法人A"
              corps={corps}
              selectedCorp={corpA}
              onSelect={handleSelectA}
              disabledCorpNumber={corpB?.corp_number}
            />
            <div className="hidden md:flex items-center justify-center">
              <span className="text-2xl text-gray-300 font-bold" aria-hidden="true">
                VS
              </span>
            </div>
            <CorpSelect
              label="法人B"
              corps={corps}
              selectedCorp={corpB}
              onSelect={handleSelectB}
              disabledCorpNumber={corpA?.corp_number}
            />
          </div>
        )}
      </div>

      {/* 比較結果 */}
      {bothSelected && (
        <div className="bg-white rounded-xl shadow-card overflow-hidden animate-fade-in-up">
          {/* ヘッダー */}
          <div className="grid grid-cols-3 border-b border-gray-200">
            <div className="px-5 py-4 bg-gray-50">
              <span className="text-xs font-medium text-gray-500 uppercase tracking-wider">
                指標
              </span>
            </div>
            <div className="px-5 py-4 bg-blue-50 border-l border-gray-200 text-center">
              <span className="text-sm font-semibold text-blue-900 truncate block">
                {corpA.corp_name}
              </span>
              <span className="text-[10px] text-blue-500">
                {corpA.corp_type ?? "---"}
              </span>
            </div>
            <div className="px-5 py-4 bg-purple-50 border-l border-gray-200 text-center">
              <span className="text-sm font-semibold text-purple-900 truncate block">
                {corpB.corp_name}
              </span>
              <span className="text-[10px] text-purple-500">
                {corpB.corp_type ?? "---"}
              </span>
            </div>
          </div>

          {/* 指標行 */}
          {COMPARE_METRICS.map((metric) => {
            const valA = metric.getValue(corpA);
            const valB = metric.getValue(corpB);

            return (
              <div
                key={metric.key}
                className="grid grid-cols-3 border-b border-gray-100 last:border-b-0"
              >
                {/* ラベル */}
                <div className="px-5 py-3 bg-gray-50 flex items-center">
                  <span className="text-sm font-medium text-gray-700">
                    {metric.label}
                  </span>
                </div>
                {/* 法人A値 */}
                <div
                  className={`px-5 py-3 border-l border-gray-100 text-center ${getComparisonBg(
                    valA,
                    valB,
                    metric.higherIsBetter,
                    "A"
                  )}`}
                >
                  <span
                    className={`text-sm tabular-nums ${getComparisonColor(
                      valA,
                      valB,
                      metric.higherIsBetter,
                      "A"
                    )}`}
                  >
                    {metric.format(valA)}
                  </span>
                </div>
                {/* 法人B値 */}
                <div
                  className={`px-5 py-3 border-l border-gray-100 text-center ${getComparisonBg(
                    valA,
                    valB,
                    metric.higherIsBetter,
                    "B"
                  )}`}
                >
                  <span
                    className={`text-sm tabular-nums ${getComparisonColor(
                      valA,
                      valB,
                      metric.higherIsBetter,
                      "B"
                    )}`}
                  >
                    {metric.format(valB)}
                  </span>
                </div>
              </div>
            );
          })}

          {/* 展開地域・サービスの詳細 */}
          <div className="grid grid-cols-3 border-t border-gray-200">
            <div className="px-5 py-3 bg-gray-50">
              <span className="text-sm font-medium text-gray-700">展開地域</span>
            </div>
            <div className="px-5 py-3 border-l border-gray-100">
              <div className="flex flex-wrap gap-1">
                {corpA.prefectures?.slice(0, 10).map((p) => (
                  <span
                    key={p}
                    className="inline-block px-1.5 py-0.5 text-[10px] bg-blue-100 text-blue-700 rounded"
                  >
                    {p}
                  </span>
                ))}
                {(corpA.prefectures?.length ?? 0) > 10 && (
                  <span className="text-[10px] text-gray-400">
                    +{(corpA.prefectures?.length ?? 0) - 10}
                  </span>
                )}
              </div>
            </div>
            <div className="px-5 py-3 border-l border-gray-100">
              <div className="flex flex-wrap gap-1">
                {corpB.prefectures?.slice(0, 10).map((p) => (
                  <span
                    key={p}
                    className="inline-block px-1.5 py-0.5 text-[10px] bg-purple-100 text-purple-700 rounded"
                  >
                    {p}
                  </span>
                ))}
                {(corpB.prefectures?.length ?? 0) > 10 && (
                  <span className="text-[10px] text-gray-400">
                    +{(corpB.prefectures?.length ?? 0) - 10}
                  </span>
                )}
              </div>
            </div>
          </div>

          {/* サービス種別 */}
          <div className="grid grid-cols-3 border-t border-gray-100">
            <div className="px-5 py-3 bg-gray-50">
              <span className="text-sm font-medium text-gray-700">サービス種別</span>
            </div>
            <div className="px-5 py-3 border-l border-gray-100">
              <div className="flex flex-wrap gap-1">
                {corpA.service_names?.slice(0, 8).map((s) => (
                  <span
                    key={s}
                    className="inline-block px-1.5 py-0.5 text-[10px] bg-gray-100 text-gray-600 rounded"
                  >
                    {s}
                  </span>
                ))}
                {(corpA.service_names?.length ?? 0) > 8 && (
                  <span className="text-[10px] text-gray-400">
                    +{(corpA.service_names?.length ?? 0) - 8}
                  </span>
                )}
              </div>
            </div>
            <div className="px-5 py-3 border-l border-gray-100">
              <div className="flex flex-wrap gap-1">
                {corpB.service_names?.slice(0, 8).map((s) => (
                  <span
                    key={s}
                    className="inline-block px-1.5 py-0.5 text-[10px] bg-gray-100 text-gray-600 rounded"
                  >
                    {s}
                  </span>
                ))}
                {(corpB.service_names?.length ?? 0) > 8 && (
                  <span className="text-[10px] text-gray-400">
                    +{(corpB.service_names?.length ?? 0) - 8}
                  </span>
                )}
              </div>
            </div>
          </div>
        </div>
      )}

      {/* 未選択時のガイド */}
      {!bothSelected && !isLoading && (
        <div className="bg-white rounded-xl shadow-card p-12 text-center">
          <svg className="w-12 h-12 mx-auto mb-4 text-gray-300" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={1} aria-hidden="true">
            <path strokeLinecap="round" strokeLinejoin="round" d="M7.5 21L3 16.5m0 0L7.5 12M3 16.5h13.5m0-13.5L21 7.5m0 0L16.5 12M21 7.5H7.5" />
          </svg>
          <p className="text-gray-400 text-sm">
            上から2つの法人を選択すると、経営指標の比較が表示されます
          </p>
        </div>
      )}
    </div>
  );
}

export default function CorpComparePage() {
  return (
    <Suspense fallback={<div className="text-gray-400 text-sm p-8">読み込み中...</div>}>
      <CorpCompareContent />
    </Suspense>
  );
}
