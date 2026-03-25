"use client";

// ===================================================
// Page T3: 施設ヘルスチェック
// 個別施設の経営診断レポートカード
// /api/facilities/search, /api/dashboard/kpi を使用
// ===================================================

import { Suspense, useState, useMemo, useCallback } from "react";
import { useApi } from "@/hooks/useApi";
import type { FacilitySearchResult, FacilityRow, DashboardKpi } from "@/lib/types";
import ApiErrorBanner from "@/components/common/ApiErrorBanner";
import ConfidenceBadge from "@/components/common/ConfidenceBadge";
import { DEBOUNCE_DELAY } from "@/lib/constants";

// ===================================================
// 型定義
// ===================================================

/** バイタル判定結果 */
type VitalStatus = "normal" | "caution" | "danger";

interface VitalItem {
  label: string;
  value: string;
  status: VitalStatus;
  detail: string;
}

// ===================================================
// ユーティリティ
// ===================================================

/** ステータスに応じた配色 */
function statusConfig(status: VitalStatus) {
  switch (status) {
    case "normal":
      return { bg: "bg-green-50", border: "border-green-200", text: "text-green-700", badge: "bg-green-100 text-green-800", label: "正常" };
    case "caution":
      return { bg: "bg-yellow-50", border: "border-yellow-200", text: "text-yellow-700", badge: "bg-yellow-100 text-yellow-800", label: "要注意" };
    case "danger":
      return { bg: "bg-red-50", border: "border-red-200", text: "text-red-700", badge: "bg-red-100 text-red-800", label: "危険" };
  }
}

/** 離職率の判定（業界平均との比較） */
function judgeTurnover(rate: number | null, avg: number | null): VitalStatus {
  if (rate == null || avg == null) return "caution";
  if (rate <= avg * 0.8) return "normal";
  if (rate <= avg * 1.2) return "caution";
  return "danger";
}

/** 常勤比率の判定 */
function judgeFulltime(ratio: number | null, avg: number | null): VitalStatus {
  if (ratio == null || avg == null) return "caution";
  if (ratio >= avg) return "normal";
  if (ratio >= avg * 0.8) return "caution";
  return "danger";
}

/** ○/×カウント */
function countMaruBatsu(facility: FacilityRow): { maru: number; batsu: number; items: { label: string; value: boolean | null }[] } {
  const ext = facility as unknown as Record<string, unknown>;
  const items = [
    { label: "BCP策定", value: ext.has_bcp as boolean | null ?? null },
    { label: "ICT導入", value: ext.has_ict as boolean | null ?? null },
    { label: "第三者評価", value: ext.has_third_party_eval as boolean | null ?? null },
    { label: "賠償責任保険", value: ext.has_liability_insurance as boolean | null ?? null },
  ];
  const maru = items.filter((i) => i.value === true).length;
  const batsu = items.filter((i) => i.value === false).length;
  return { maru, batsu, items };
}

// ===================================================
// メインコンテンツ
// ===================================================

function HealthCheckContent() {
  const [searchQuery, setSearchQuery] = useState("");
  const [debouncedQuery, setDebouncedQuery] = useState("");
  const [selectedFacility, setSelectedFacility] = useState<FacilityRow | null>(null);
  const [debounceTimer, setDebounceTimer] = useState<ReturnType<typeof setTimeout> | null>(null);

  // 検索クエリのデバウンス
  const handleSearchChange = useCallback(
    (value: string) => {
      setSearchQuery(value);
      if (debounceTimer) clearTimeout(debounceTimer);
      const timer = setTimeout(() => {
        setDebouncedQuery(value);
      }, DEBOUNCE_DELAY);
      setDebounceTimer(timer);
    },
    [debounceTimer]
  );

  // 施設検索API
  const { data: searchResult, error: searchError, isLoading: searchLoading } = useApi<FacilitySearchResult>(
    debouncedQuery.length >= 2 ? "/api/facilities/search" : null,
    { keyword: debouncedQuery, per_page: 10 }
  );

  // 業界平均KPI（ベンチマーク用）
  const { data: industryKpi } = useApi<DashboardKpi>("/api/dashboard/kpi");

  // 加算数カウント（拡張フィールドから）
  const additionCount = useMemo(() => {
    if (!selectedFacility) return 0;
    const ext = selectedFacility as unknown as Record<string, unknown>;
    const fields = [
      "addition_treatment_i", "addition_treatment_ii", "addition_treatment_iii", "addition_treatment_iv",
      "addition_specific_i", "addition_specific_ii", "addition_specific_iii", "addition_specific_iv", "addition_specific_v",
      "addition_dementia_i", "addition_dementia_ii", "addition_oral", "addition_emergency",
    ];
    return fields.filter((f) => ext[f] === true).length;
  }, [selectedFacility]);

  // 人材バイタル
  const staffVitals: VitalItem[] = useMemo(() => {
    if (!selectedFacility) return [];
    const f = selectedFacility;
    const avgTurnover = industryKpi?.avg_turnover_rate ?? null;
    const avgFulltime = industryKpi?.avg_fulltime_ratio ?? null;

    const turnoverStatus = judgeTurnover(f.turnover_rate, avgTurnover);
    const fulltimeStatus = judgeFulltime(f.fulltime_ratio, avgFulltime);

    return [
      {
        label: "離職率",
        value: f.turnover_rate != null ? `${(f.turnover_rate * 100).toFixed(1)}%` : "---",
        status: turnoverStatus,
        detail: avgTurnover != null ? `業界平均: ${(avgTurnover * 100).toFixed(1)}%` : "業界平均: データなし",
      },
      {
        label: "常勤比率",
        value: f.fulltime_ratio != null ? `${(f.fulltime_ratio * 100).toFixed(1)}%` : "---",
        status: fulltimeStatus,
        detail: avgFulltime != null ? `業界平均: ${(avgFulltime * 100).toFixed(1)}%` : "業界平均: データなし",
      },
    ];
  }, [selectedFacility, industryKpi]);

  // 品質バイタル
  const qualityVitals = useMemo(() => {
    if (!selectedFacility) return { maru: 0, batsu: 0, items: [] };
    return countMaruBatsu(selectedFacility);
  }, [selectedFacility]);

  // 施設選択ハンドラ
  const handleSelect = (facility: FacilityRow) => {
    setSelectedFacility(facility);
    setSearchQuery("");
    setDebouncedQuery("");
  };

  return (
    <div className="space-y-6">
      {/* ページタイトル */}
      <div>
        <h1 className="text-heading-lg text-gray-900">施設ヘルスチェック</h1>
        <p className="text-body-md text-gray-500 mt-1">
          個別施設の経営診断レポート
        </p>
      </div>

      {/* APIエラーバナー */}
      <ApiErrorBanner error={searchError} />

      {/* 施設検索 */}
      <div className="bg-white rounded-xl shadow-card p-5">
        <label htmlFor="facility-search" className="block text-sm font-medium text-gray-700 mb-2">
          施設名で検索
        </label>
        <input
          id="facility-search"
          type="text"
          value={searchQuery}
          onChange={(e) => handleSearchChange(e.target.value)}
          placeholder="施設名を入力してください（2文字以上）"
          className="w-full px-4 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-brand-500 focus:border-brand-500"
          aria-describedby="search-hint"
        />
        <p id="search-hint" className="text-xs text-gray-400 mt-1">
          施設名の一部を入力すると候補が表示されます
        </p>

        {/* 検索結果リスト */}
        {searchLoading && (
          <div className="mt-3 text-sm text-gray-400">検索中...</div>
        )}
        {searchResult && searchResult.items.length > 0 && debouncedQuery.length >= 2 && (
          <ul className="mt-3 border border-gray-200 rounded-lg divide-y divide-gray-100 max-h-64 overflow-y-auto" role="listbox" aria-label="検索結果">
            {searchResult.items.map((facility) => (
              <li key={facility.jigyosho_number}>
                <button
                  type="button"
                  className="w-full text-left px-4 py-3 hover:bg-gray-50 focus:bg-brand-50 focus:outline-none transition-colors"
                  onClick={() => handleSelect(facility)}
                  role="option"
                  aria-selected={false}
                >
                  <div className="text-sm font-medium text-gray-900">{facility.jigyosho_name}</div>
                  <div className="text-xs text-gray-500 mt-0.5">
                    {facility.corp_name} / {facility.address} / {facility.service_name ?? "---"}
                  </div>
                </button>
              </li>
            ))}
          </ul>
        )}
        {searchResult && searchResult.items.length === 0 && debouncedQuery.length >= 2 && (
          <div className="mt-3 text-sm text-gray-400">該当する施設が見つかりません</div>
        )}
      </div>

      {/* ヘルスチェックレポートカード */}
      {selectedFacility && (
        <div className="space-y-6">
          {/* 施設基本情報ヘッダー */}
          <div className="bg-white rounded-xl shadow-card p-5">
            <div className="flex items-start justify-between">
              <div>
                <h2 className="text-lg font-bold text-gray-900">{selectedFacility.jigyosho_name}</h2>
                <p className="text-sm text-gray-500 mt-1">{selectedFacility.corp_name}</p>
                <div className="flex flex-wrap gap-2 mt-2 text-xs text-gray-500">
                  <span>{selectedFacility.address}</span>
                  {selectedFacility.service_name && (
                    <>
                      <span aria-hidden="true">|</span>
                      <span>{selectedFacility.service_name}</span>
                    </>
                  )}
                  {selectedFacility.staff_total != null && (
                    <>
                      <span aria-hidden="true">|</span>
                      <span>従業者数: {selectedFacility.staff_total}名</span>
                    </>
                  )}
                </div>
              </div>
              <button
                type="button"
                className="text-sm text-gray-400 hover:text-gray-600 px-2 py-1"
                onClick={() => setSelectedFacility(null)}
                aria-label="レポートを閉じる"
              >
                閉じる
              </button>
            </div>
          </div>

          {/* 診断セクション: 人材バイタル */}
          <div className="bg-white rounded-xl shadow-card p-5">
            <h3 className="text-base font-semibold text-gray-900 mb-4 flex items-center gap-2">
              <span className="w-2 h-2 bg-brand-500 rounded-full" aria-hidden="true" />
              人材バイタル
              <ConfidenceBadge level={selectedFacility.turnover_rate != null ? "high" : "low"} />
            </h3>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
              {staffVitals.map((vital) => {
                const sc = statusConfig(vital.status);
                return (
                  <div
                    key={vital.label}
                    className={`${sc.bg} ${sc.border} border rounded-lg p-4`}
                  >
                    <div className="flex items-center justify-between mb-1">
                      <span className="text-sm font-medium text-gray-700">{vital.label}</span>
                      <span className={`text-xs px-2 py-0.5 rounded-full font-medium ${sc.badge}`}>
                        {sc.label}
                      </span>
                    </div>
                    <div className={`text-2xl font-bold ${sc.text} tabular-nums`}>{vital.value}</div>
                    <div className="text-xs text-gray-500 mt-1">{vital.detail}</div>
                  </div>
                );
              })}
            </div>
          </div>

          {/* 診断セクション: 収益バイタル */}
          <div className="bg-white rounded-xl shadow-card p-5">
            <h3 className="text-base font-semibold text-gray-900 mb-4 flex items-center gap-2">
              <span className="w-2 h-2 bg-emerald-500 rounded-full" aria-hidden="true" />
              収益バイタル
              <ConfidenceBadge level={additionCount > 0 ? "medium" : "low"} />
            </h3>
            <div className={`${additionCount >= 5 ? "bg-green-50 border-green-200" : additionCount >= 2 ? "bg-yellow-50 border-yellow-200" : "bg-red-50 border-red-200"} border rounded-lg p-4`}>
              <div className="flex items-center justify-between mb-1">
                <span className="text-sm font-medium text-gray-700">加算取得数</span>
                <span className={`text-xs px-2 py-0.5 rounded-full font-medium ${additionCount >= 5 ? "bg-green-100 text-green-800" : additionCount >= 2 ? "bg-yellow-100 text-yellow-800" : "bg-red-100 text-red-800"}`}>
                  {additionCount >= 5 ? "正常" : additionCount >= 2 ? "要注意" : "危険"}
                </span>
              </div>
              <div className={`text-2xl font-bold tabular-nums ${additionCount >= 5 ? "text-green-700" : additionCount >= 2 ? "text-yellow-700" : "text-red-700"}`}>
                {additionCount} / 13
              </div>
              <div className="text-xs text-gray-500 mt-1">
                処遇改善・特定事業所・認知症ケア等の加算取得状況
              </div>
            </div>
          </div>

          {/* 診断セクション: 品質バイタル */}
          <div className="bg-white rounded-xl shadow-card p-5">
            <h3 className="text-base font-semibold text-gray-900 mb-4 flex items-center gap-2">
              <span className="w-2 h-2 bg-sky-500 rounded-full" aria-hidden="true" />
              品質バイタル
              <ConfidenceBadge level="medium" />
            </h3>
            <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
              {qualityVitals.items.map((item) => (
                <div
                  key={item.label}
                  className={`border rounded-lg p-3 text-center ${item.value === true ? "bg-green-50 border-green-200" : item.value === false ? "bg-red-50 border-red-200" : "bg-gray-50 border-gray-200"}`}
                >
                  <div className={`text-2xl font-bold ${item.value === true ? "text-green-600" : item.value === false ? "text-red-500" : "text-gray-400"}`}>
                    {item.value === true ? "\u25CB" : item.value === false ? "\u00D7" : "---"}
                  </div>
                  <div className="text-xs text-gray-600 mt-1">{item.label}</div>
                </div>
              ))}
            </div>
          </div>

          {/* 診断セクション: 地域ポジション */}
          <div className="bg-white rounded-xl shadow-card p-5">
            <h3 className="text-base font-semibold text-gray-900 mb-4 flex items-center gap-2">
              <span className="w-2 h-2 bg-amber-500 rounded-full" aria-hidden="true" />
              地域ポジション
              <ConfidenceBadge level="low" />
            </h3>
            <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
              <div className="bg-gray-50 border border-gray-200 rounded-lg p-4 text-center">
                <div className="text-xs text-gray-500 mb-1">都道府県</div>
                <div className="text-lg font-bold text-gray-900">{selectedFacility.prefecture ?? "---"}</div>
              </div>
              <div className="bg-gray-50 border border-gray-200 rounded-lg p-4 text-center">
                <div className="text-xs text-gray-500 mb-1">事業開始</div>
                <div className="text-lg font-bold text-gray-900">{selectedFacility.start_date ?? "---"}</div>
              </div>
              <div className="bg-gray-50 border border-gray-200 rounded-lg p-4 text-center">
                <div className="text-xs text-gray-500 mb-1">事業年数</div>
                <div className="text-lg font-bold text-gray-900">
                  {selectedFacility.years_in_business != null ? `${selectedFacility.years_in_business.toFixed(0)}年` : "---"}
                </div>
              </div>
            </div>
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
                <p className="font-medium">
                  データソース: 厚生労働省 介護サービス情報公表システム
                </p>
                <p className="mt-1 text-amber-700">
                  加算・品質・BCP等の情報は公表データに基づきます。賃金データの充填率が低いため、収益バイタルは加算取得数で代替しています。
                </p>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* 未選択時のガイド */}
      {!selectedFacility && (
        <div className="bg-white rounded-xl shadow-card p-12 text-center">
          <svg className="w-16 h-16 text-gray-300 mx-auto mb-4" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={1} aria-hidden="true">
            <path strokeLinecap="round" strokeLinejoin="round" d="M21 21l-5.197-5.197m0 0A7.5 7.5 0 105.196 5.196a7.5 7.5 0 0010.607 10.607z" />
          </svg>
          <h2 className="text-lg font-medium text-gray-500">施設を検索してください</h2>
          <p className="text-sm text-gray-400 mt-2">
            上の検索バーに施設名を入力すると、ヘルスチェックレポートが表示されます
          </p>
        </div>
      )}
    </div>
  );
}

export default function HealthCheckPage() {
  return (
    <Suspense fallback={<div className="text-gray-400 text-sm p-8">読み込み中...</div>}>
      <HealthCheckContent />
    </Suspense>
  );
}
