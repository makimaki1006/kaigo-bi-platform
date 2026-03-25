"use client";

// ===================================================
// Page: 運営コスト推定
// 施設パラメータから推定年間コストを算出
// /api/external/cost-breakdown エンドポイント使用
// ===================================================

import { Suspense, useState, useCallback } from "react";
import { PREFECTURES } from "@/lib/constants";
import { fetcher } from "@/lib/api-client";
import KpiCard from "@/components/data-display/KpiCard";
import KpiCardGrid from "@/components/data-display/KpiCardGrid";
import DataTable from "@/components/data-display/DataTable";
import ChartCard from "@/components/charts/ChartCard";
import DonutChart from "@/components/charts/DonutChart";
import ApiErrorBanner from "@/components/common/ApiErrorBanner";
import ConfidenceBadge from "@/components/common/ConfidenceBadge";
import type { ColumnDef } from "@/lib/types";
import type { ApiErrorInfo } from "@/hooks/useApi";

// ===================================================
// 型定義
// ===================================================

/** コスト内訳項目 */
interface CostItem {
  annual: number;
  pct: number;
  note: string;
}

/** コスト内訳 */
interface CostBreakdown {
  personnel: CostItem;
  utility: CostItem;
  building: CostItem;
  land_facility: CostItem;
}

/** APIレスポンス全体 */
interface CostEstimationResponse {
  prefecture: string;
  staff_count: number;
  capacity: number;
  years_in_business: number;
  breakdown: CostBreakdown;
  total_annual: number;
  confidence: string;
  warnings: string[];
  data_sources: string[];
}

// ===================================================
// KPIアイコン
// ===================================================

/** コスト合計アイコン */
const IconCost = (
  <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={1.5} aria-hidden="true">
    <path strokeLinecap="round" strokeLinejoin="round" d="M12 6v12m-3-2.818l.879.659c1.171.879 3.07.879 4.242 0 1.172-.879 1.172-2.303 0-3.182C13.536 12.219 12.768 12 12 12c-.725 0-1.45-.22-2.003-.659-1.106-.879-1.106-2.303 0-3.182s2.9-.879 4.006 0l.415.33M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
  </svg>
);

// ===================================================
// テーブル行型とカラム定義
// ===================================================

interface BreakdownRow {
  category: string;
  annual: number;
  pct: number;
  note: string;
}

const BREAKDOWN_COLUMNS: ColumnDef<BreakdownRow>[] = [
  {
    key: "category",
    label: "項目",
    width: "140px",
    render: (value: string) => (
      <span className="font-medium text-gray-900">{value}</span>
    ),
  },
  {
    key: "annual",
    label: "年間コスト",
    width: "140px",
    render: (value: number) => (
      <span className="tabular-nums font-semibold">
        {Math.round(value / 10000).toLocaleString("ja-JP")} 万円
      </span>
    ),
  },
  {
    key: "pct",
    label: "割合",
    width: "80px",
    render: (value: number) => (
      <span className="tabular-nums">{value.toFixed(1)}%</span>
    ),
  },
  {
    key: "note",
    label: "算出根拠",
    width: "300px",
    render: (value: string) => (
      <span className="text-sm text-gray-600">{value}</span>
    ),
  },
];

// ===================================================
// カテゴリ名マッピング
// ===================================================

const CATEGORY_LABELS: Record<string, string> = {
  personnel: "人件費",
  utility: "光熱水費",
  building: "建物維持費",
  land_facility: "土地施設費",
};

const DONUT_COLORS = ["#4f46e5", "#0284c7", "#059669", "#d97706"];

// ===================================================
// メインコンテンツ
// ===================================================

function CostEstimationContent() {
  // フォーム状態
  const [prefecture, setPrefecture] = useState("東京都");
  const [staffCount, setStaffCount] = useState(20);
  const [capacity, setCapacity] = useState(30);
  const [yearsInBusiness, setYearsInBusiness] = useState(10);

  // API結果状態
  const [result, setResult] = useState<CostEstimationResponse | null>(null);
  const [error, setError] = useState<ApiErrorInfo | null>(null);
  const [isLoading, setIsLoading] = useState(false);

  // API呼び出し
  const handleEstimate = useCallback(async () => {
    setIsLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams({
        prefecture,
        staff_count: String(staffCount),
        capacity: String(capacity),
        years_in_business: String(yearsInBusiness),
      });
      const data = await fetcher<CostEstimationResponse>(
        `/api/external/cost-breakdown?${params.toString()}`
      );
      setResult(data);
    } catch (err) {
      if (err instanceof Error) {
        setError({
          status: 0,
          message: err.message,
          isNetworkError: false,
          isAuthError: false,
          isServerError: false,
        });
      }
      setResult(null);
    } finally {
      setIsLoading(false);
    }
  }, [prefecture, staffCount, capacity, yearsInBusiness]);

  // テーブルデータ
  const breakdownRows: BreakdownRow[] = result
    ? (Object.entries(result.breakdown) as [string, CostItem][]).map(
        ([key, item]) => ({
          category: CATEGORY_LABELS[key] || key,
          annual: item.annual,
          pct: item.pct,
          note: item.note,
        })
      )
    : [];

  // ドーナツチャートデータ
  const donutData = result
    ? (Object.entries(result.breakdown) as [string, CostItem][]).map(
        ([key, item]) => ({
          name: CATEGORY_LABELS[key] || key,
          value: Math.round(item.annual / 10000),
        })
      )
    : [];

  return (
    <div className="space-y-6">
      {/* ページタイトル */}
      <div>
        <h1 className="text-heading-lg text-gray-900">運営コスト推定</h1>
        <p className="text-body-md text-gray-500 mt-1">
          施設パラメータから推定年間コストを算出
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
          <div className="text-sm text-amber-800">
            <p className="font-medium flex items-center gap-2 flex-wrap">
              データソース: {result?.data_sources?.join("、") || "統計局賃金データ + 介護経営概況調査（推計モデル）"}
              <ConfidenceBadge level="low" />
            </p>
            <p className="mt-1 text-amber-700">
              注意: 人件費は全産業平均x0.75（介護業界補正）で積算。推計値のため実際のコストとは異なります。
            </p>
          </div>
        </div>
      </div>

      {/* 警告表示（API結果にwarningsがある場合） */}
      {result?.warnings && result.warnings.length > 0 && (
        <div className="rounded-lg border border-amber-300 bg-amber-50 px-4 py-3">
          <div className="flex items-start gap-2">
            <svg className="w-5 h-5 text-amber-600 mt-0.5 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={1.5} aria-hidden="true">
              <path strokeLinecap="round" strokeLinejoin="round" d="M12 9v3.75m-9.303 3.376c-.866 1.5.217 3.374 1.948 3.374h14.71c1.73 0 2.813-1.874 1.948-3.374L13.949 3.378c-.866-1.5-3.032-1.5-3.898 0L2.697 16.126zM12 15.75h.007v.008H12v-.008z" />
            </svg>
            <div className="text-sm text-amber-800">
              <p className="font-medium mb-1">注意事項</p>
              <ul className="space-y-0.5 list-disc list-inside">
                {result.warnings.map((w, i) => (
                  <li key={i}>{w}</li>
                ))}
              </ul>
            </div>
          </div>
        </div>
      )}

      {/* APIエラーバナー */}
      <ApiErrorBanner error={error} />

      {/* 入力フォーム */}
      <div className="bg-white rounded-xl shadow-card p-5">
        <h2 className="text-base font-semibold text-gray-900 mb-4">
          施設パラメータ入力
        </h2>
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
          {/* 都道府県 */}
          <div>
            <label
              htmlFor="prefecture"
              className="block text-sm font-medium text-gray-700 mb-1"
            >
              都道府県
            </label>
            <select
              id="prefecture"
              value={prefecture}
              onChange={(e) => setPrefecture(e.target.value)}
              className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-brand-500 focus:border-brand-500"
            >
              {PREFECTURES.map((p) => (
                <option key={p} value={p}>
                  {p}
                </option>
              ))}
            </select>
          </div>

          {/* 従業員数 */}
          <div>
            <label
              htmlFor="staffCount"
              className="block text-sm font-medium text-gray-700 mb-1"
            >
              従業員数
            </label>
            <input
              id="staffCount"
              type="number"
              min={1}
              max={500}
              value={staffCount}
              onChange={(e) => setStaffCount(Number(e.target.value) || 1)}
              className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm tabular-nums focus:outline-none focus:ring-2 focus:ring-brand-500 focus:border-brand-500"
            />
          </div>

          {/* 定員 */}
          <div>
            <label
              htmlFor="capacity"
              className="block text-sm font-medium text-gray-700 mb-1"
            >
              定員
            </label>
            <input
              id="capacity"
              type="number"
              min={1}
              max={500}
              value={capacity}
              onChange={(e) => setCapacity(Number(e.target.value) || 1)}
              className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm tabular-nums focus:outline-none focus:ring-2 focus:ring-brand-500 focus:border-brand-500"
            />
          </div>

          {/* 事業年数 */}
          <div>
            <label
              htmlFor="yearsInBusiness"
              className="block text-sm font-medium text-gray-700 mb-1"
            >
              事業年数
            </label>
            <input
              id="yearsInBusiness"
              type="number"
              min={0}
              max={100}
              value={yearsInBusiness}
              onChange={(e) => setYearsInBusiness(Number(e.target.value) || 0)}
              className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm tabular-nums focus:outline-none focus:ring-2 focus:ring-brand-500 focus:border-brand-500"
            />
          </div>
        </div>

        <div className="mt-4">
          <button
            type="button"
            onClick={handleEstimate}
            disabled={isLoading}
            className="inline-flex items-center px-5 py-2.5 rounded-lg bg-brand-600 text-white font-medium text-sm hover:bg-brand-700 focus:outline-none focus:ring-2 focus:ring-brand-500 focus:ring-offset-2 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
          >
            {isLoading ? (
              <>
                <svg className="animate-spin -ml-1 mr-2 h-4 w-4 text-white" fill="none" viewBox="0 0 24 24" aria-hidden="true">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
                </svg>
                推定中...
              </>
            ) : (
              "推定する"
            )}
          </button>
        </div>
      </div>

      {/* 結果表示（APIレスポンスがある場合のみ） */}
      {result && (
        <>
          {/* 合計コスト KPI */}
          <KpiCardGrid>
            <KpiCard
              label="推定年間運営コスト"
              value={Math.round(result.total_annual / 10000)}
              format="number"
              icon={IconCost}
              subtitle="万円"
              loading={false}
              accentColor="bg-brand-500"
              tooltip={`${result.prefecture} / 従業員${result.staff_count}名 / 定員${result.capacity}名 / 事業年数${result.years_in_business}年`}
            />
          </KpiCardGrid>

          {/* チャート + テーブル */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {/* ドーナツチャート */}
            <ChartCard
              title="コスト構成比"
              subtitle="項目別の年間コスト割合"
              loading={false}
            >
              <DonutChart
                data={donutData}
                nameKey="name"
                valueKey="value"
                centerLabel="万円"
                height={320}
                colors={DONUT_COLORS}
              />
            </ChartCard>

            {/* 内訳テーブル */}
            <div className="bg-white rounded-xl shadow-card overflow-hidden">
              <div className="px-5 py-4 border-b border-gray-100">
                <h2 className="text-base font-semibold text-gray-900">
                  コスト内訳
                </h2>
                <p className="text-sm text-gray-500 mt-0.5">
                  項目別の年間コストと算出根拠
                </p>
              </div>
              <DataTable
                columns={BREAKDOWN_COLUMNS}
                data={breakdownRows}
                loading={false}
              />
            </div>
          </div>

          {/* 補足情報 */}
          <div className="bg-white rounded-xl shadow-card p-5">
            <h2 className="text-base font-semibold text-gray-900 mb-3">
              推定モデルについて
            </h2>
            <div className="space-y-2 text-sm text-gray-700">
              <p>
                人件費は全産業平均賃金（都道府県別）x 0.75（介護業界補正係数）で積算しています。
                介護業界の平均賃金は全産業平均の約75%程度であることが各種調査で示されています。
              </p>
              <p>
                光熱水費・建物維持費・土地施設費は介護経営概況調査の費用構造比率をもとに推計しています。
                個別施設の立地・規模・設備状況により実際のコストは大きく異なる場合があります。
              </p>
            </div>
          </div>
        </>
      )}

      {/* 初期状態（まだ推定していない場合） */}
      {!result && !isLoading && !error && (
        <div className="bg-white rounded-xl shadow-card p-10 text-center">
          <svg className="w-12 h-12 text-gray-300 mx-auto mb-3" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={1} aria-hidden="true">
            <path strokeLinecap="round" strokeLinejoin="round" d="M15.75 15.75V18m-7.5-6.75h.008v.008H8.25v-.008zm0 2.25h.008v.008H8.25V13.5zm0 2.25h.008v.008H8.25v-.008zm0 2.25h.008v.008H8.25V18zm2.498-6.75h.007v.008h-.007v-.008zm0 2.25h.007v.008h-.007V13.5zm0 2.25h.007v.008h-.007v-.008zm0 2.25h.007v.008h-.007V18zm2.504-6.75h.008v.008h-.008v-.008zm0 2.25h.008v.008h-.008V13.5zm0 2.25h.008v.008h-.008v-.008zm0 2.25h.008v.008h-.008V18zm2.498-6.75h.008v.008h-.008v-.008zm0 2.25h.008v.008h-.008V13.5zM8.25 6h7.5v2.25h-7.5V6zM12 2.25c-1.892 0-3.758.11-5.593.322C5.307 2.7 4.5 3.65 4.5 4.757V19.5a2.25 2.25 0 002.25 2.25h10.5a2.25 2.25 0 002.25-2.25V4.757c0-1.108-.806-2.057-1.907-2.185A48.507 48.507 0 0012 2.25z" />
          </svg>
          <p className="text-gray-500 text-sm">
            上のフォームにパラメータを入力して「推定する」ボタンをクリックしてください
          </p>
        </div>
      )}
    </div>
  );
}

export default function CostEstimationPage() {
  return (
    <Suspense fallback={<div className="text-gray-400 text-sm p-8">読み込み中...</div>}>
      <CostEstimationContent />
    </Suspense>
  );
}
