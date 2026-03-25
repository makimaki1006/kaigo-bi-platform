"use client";

// ===================================================
// Page 13: PMIシナジー分析
// 買い手 x 対象の統合シミュレーションダッシュボード
// 実API: /api/pmi/simulate?buyer_corp=X&target_corp=Y
// ===================================================

import { Suspense, useState, useCallback, useMemo } from "react";
import { Card } from "@tremor/react";
import StackedBarChart from "@/components/charts/StackedBarChart";
import ChartCard from "@/components/charts/ChartCard";
import DataPendingPlaceholder from "@/components/common/DataPendingPlaceholder";
import CorpSearchInput from "@/components/common/CorpSearchInput";
import LoadingSpinner from "@/components/common/LoadingSpinner";
import { formatServiceName } from "@/lib/formatters";
import { useApi } from "@/hooks/useApi";
import ApiErrorBanner from "@/components/common/ApiErrorBanner";
import type { PmiSimulationResponse } from "@/lib/types";

/** 地域カバレッジのカテゴリスタイル */
const REGION_CATEGORY_STYLES: Record<string, { bg: string; text: string; label: string }> = {
  buyer_only: { bg: "bg-blue-100", text: "text-blue-800", label: "買い手のみ" },
  target_only: { bg: "bg-orange-100", text: "text-orange-800", label: "対象のみ" },
  overlap: { bg: "bg-purple-100", text: "text-purple-800", label: "重複" },
};

/** リスク重大度スタイル */
const SEVERITY_STYLES: Record<string, { bg: string; text: string; label: string }> = {
  high: { bg: "bg-red-50 border border-red-200", text: "text-red-600", label: "高" },
  medium: { bg: "bg-amber-50 border border-amber-200", text: "text-amber-600", label: "中" },
  low: { bg: "bg-emerald-50 border border-emerald-200", text: "text-emerald-600", label: "低" },
};

/** 地域カバレッジのカテゴリ分類ヘルパー */
function classifyPrefectures(data: PmiSimulationResponse) {
  const result: { prefecture: string; category: "buyer_only" | "target_only" | "overlap" }[] = [];

  // 重複エリア: 両方に含まれる
  const buyerPrefs = new Set<string>();
  const targetPrefs = new Set<string>();

  // buyer施設の都道府県を特定（facilities名から直接は取れないが、combined.prefecture_coverageとnew_prefecturesから推論）
  const allPrefs = data.combined.prefecture_coverage;
  const newPrefs = new Set(data.combined.new_prefectures);

  for (const pref of allPrefs) {
    if (newPrefs.has(pref)) {
      // target側にしかない地域
      targetPrefs.add(pref);
    } else {
      buyerPrefs.add(pref);
    }
  }

  // overlapping: 両方に存在する（service_overlapの地域版はAPIにないため、new_prefecturesに含まれない=buyerが持っている）
  // new_prefecturesに含まれる = target_only
  // それ以外 = buyer_only (overlapの判定はAPIデータからは厳密にできないが近似)
  for (const pref of allPrefs) {
    if (newPrefs.has(pref)) {
      result.push({ prefecture: pref, category: "target_only" });
    } else {
      // buyerが持っていて、targetも持っている可能性があるが、APIからは区別できない
      result.push({ prefecture: pref, category: "buyer_only" });
    }
  }

  return result;
}

/** 統合リスク行の生成 */
function buildIntegrationRisks(data: PmiSimulationResponse) {
  const risks: {
    risk_item: string;
    buyer_value: string;
    target_value: string;
    gap: string;
    severity: "high" | "medium" | "low";
  }[] = [];

  // 離職率ギャップ
  const turnoverGap = data.synergy.turnover_gap;
  risks.push({
    risk_item: "離職率差",
    buyer_value: "-",
    target_value: "-",
    gap: `${Math.abs(turnoverGap).toFixed(1)}pt`,
    severity: Math.abs(turnoverGap) > 10 ? "high" : Math.abs(turnoverGap) > 5 ? "medium" : "low",
  });

  // 賃金ギャップ
  const wageGap = data.synergy.wage_gap;
  risks.push({
    risk_item: "従業者数格差",
    buyer_value: `${Math.round(data.buyer.total_staff)}人`,
    target_value: `${Math.round(data.target.total_staff)}人`,
    gap: `${Math.abs(wageGap).toFixed(0)}人差`,
    severity: Math.abs(wageGap) > 200 ? "high" : Math.abs(wageGap) > 50 ? "medium" : "low",
  });

  // 人材再配置
  const realloc = data.synergy.staff_reallocation_potential;
  risks.push({
    risk_item: "人材再配置ポテンシャル",
    buyer_value: "-",
    target_value: "-",
    gap: `${realloc.toFixed(0)}人`,
    severity: realloc > 30 ? "medium" : "low",
  });

  // サービス統合
  const overlapCount = data.combined.service_overlap.length;
  const newCount = data.combined.new_services.length;
  risks.push({
    risk_item: "サービス統合の複雑さ",
    buyer_value: `${data.combined.service_coverage.length - newCount}種別`,
    target_value: `${newCount}種別追加`,
    gap: `重複${overlapCount}種別`,
    severity: overlapCount > 3 ? "medium" : "low",
  });

  return risks;
}

function PmiSynergyContent() {
  const [buyerCorpNumber, setBuyerCorpNumber] = useState<string | null>(null);
  const [buyerCorpName, setBuyerCorpName] = useState("");
  const [targetCorpNumber, setTargetCorpNumber] = useState<string | null>(null);
  const [targetCorpName, setTargetCorpName] = useState("");

  // 法人選択ハンドラ
  const handleBuyerSelect = useCallback((corpNumber: string, corpName: string) => {
    setBuyerCorpNumber(corpNumber);
    setBuyerCorpName(corpName);
  }, []);

  const handleTargetSelect = useCallback((corpNumber: string, corpName: string) => {
    setTargetCorpNumber(corpNumber);
    setTargetCorpName(corpName);
  }, []);

  const handleBuyerClear = useCallback(() => {
    setBuyerCorpNumber(null);
    setBuyerCorpName("");
  }, []);

  const handleTargetClear = useCallback(() => {
    setTargetCorpNumber(null);
    setTargetCorpName("");
  }, []);

  // PMIシミュレーションAPI（両方選択済みの場合のみ）
  const pmiEndpoint =
    buyerCorpNumber && targetCorpNumber ? "/api/pmi/simulate" : null;
  const pmiParams = useMemo(
    () =>
      buyerCorpNumber && targetCorpNumber
        ? { buyer_corp: buyerCorpNumber, target_corp: targetCorpNumber }
        : undefined,
    [buyerCorpNumber, targetCorpNumber]
  );

  const { data, isLoading, error } = useApi<PmiSimulationResponse>(
    pmiEndpoint,
    pmiParams
  );

  // 統合前後比較チャートデータ
  const beforeAfterChartData = useMemo(() => {
    if (!data) return [];
    return [
      {
        metric: "施設数",
        buyer: data.buyer.facilities.length,
        target: data.target.facilities.length,
      },
      {
        metric: "従業者数",
        buyer: Math.round(data.buyer.total_staff),
        target: Math.round(data.target.total_staff),
      },
    ];
  }, [data]);

  // サービス補完度テーブル
  const serviceCoverageTable = useMemo(() => {
    if (!data) return [];
    const overlapSet = new Set(data.combined.service_overlap);
    const newSet = new Set(data.combined.new_services);
    return data.combined.service_coverage.map((svc) => ({
      service_name: svc,
      buyer_has: !newSet.has(svc),
      target_has: overlapSet.has(svc) || newSet.has(svc),
    }));
  }, [data]);

  // 地域カバレッジ
  const regionCoverage = useMemo(() => {
    if (!data) return [];
    return classifyPrefectures(data);
  }, [data]);

  // 統合リスク
  const integrationRisks = useMemo(() => {
    if (!data) return [];
    return buildIntegrationRisks(data);
  }, [data]);

  const bothSelected = buyerCorpNumber && targetCorpNumber;

  return (
    <div className="space-y-6">
      {/* ページタイトル */}
      <div>
        <h1 className="text-xl font-bold text-gray-900">PMIシナジー分析</h1>
        <p className="text-sm text-gray-500 mt-1">
          買い手法人と対象法人を選択し、統合シミュレーションとシナジー効果を分析
        </p>
      </div>

      {/* 法人選択パネル */}
      <Card>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          {/* 買い手法人 */}
          <div>
            <div className="flex items-center gap-2 mb-2">
              <span className="inline-flex items-center justify-center w-6 h-6 rounded-full bg-blue-100 text-blue-700 text-xs font-bold">
                A
              </span>
              <span className="text-sm font-semibold text-gray-700">
                買い手法人
              </span>
            </div>
            <CorpSearchInput
              label="買い手法人を検索"
              onSelect={handleBuyerSelect}
              onClear={handleBuyerClear}
              placeholder="法人名 / 法人番号を入力..."
            />
          </div>

          {/* 対象法人 */}
          <div>
            <div className="flex items-center gap-2 mb-2">
              <span className="inline-flex items-center justify-center w-6 h-6 rounded-full bg-orange-100 text-orange-700 text-xs font-bold">
                B
              </span>
              <span className="text-sm font-semibold text-gray-700">
                対象法人（買収ターゲット）
              </span>
            </div>
            <CorpSearchInput
              label="対象法人を検索"
              onSelect={handleTargetSelect}
              onClear={handleTargetClear}
              placeholder="法人名 / 法人番号を入力..."
            />
          </div>
        </div>
      </Card>

      {/* APIエラーバナー */}
      <ApiErrorBanner error={error} />

      {/* ローディング */}
      {isLoading && bothSelected && (
        <Card>
          <LoadingSpinner text="統合シミュレーションを実行中..." />
        </Card>
      )}

      {/* シナジー分析ダッシュボード（データ取得後に表示） */}
      {data && !isLoading && (
        <div className="space-y-6 animate-fade-in-up">
          {/* 統合サマリーカード */}
          <Card>
            <div className="flex items-center gap-4 text-center">
              {/* 買い手 */}
              <div className="flex-1 py-3 rounded-lg bg-blue-50">
                <p className="text-xs text-blue-600 mb-1">買い手法人 (A)</p>
                <p className="text-sm font-bold text-gray-900">
                  {data.buyer.corp_name}
                </p>
                <p className="text-xs text-gray-500 mt-1">
                  {data.buyer.facilities.length}施設 / {Math.round(data.buyer.total_staff)}人
                </p>
              </div>

              {/* 結合アイコン */}
              <div className="flex-shrink-0">
                <div className="w-10 h-10 rounded-full bg-gray-100 flex items-center justify-center">
                  <svg className="w-5 h-5 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24" aria-hidden="true">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4" />
                  </svg>
                </div>
              </div>

              {/* 対象 */}
              <div className="flex-1 py-3 rounded-lg bg-orange-50">
                <p className="text-xs text-orange-600 mb-1">対象法人 (B)</p>
                <p className="text-sm font-bold text-gray-900">
                  {data.target.corp_name}
                </p>
                <p className="text-xs text-gray-500 mt-1">
                  {data.target.facilities.length}施設 / {Math.round(data.target.total_staff)}人
                </p>
              </div>

              {/* 矢印 */}
              <div className="flex-shrink-0">
                <svg className="w-6 h-6 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24" aria-hidden="true">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 7l5 5m0 0l-5 5m5-5H6" />
                </svg>
              </div>

              {/* 統合後 */}
              <div className="flex-1 py-3 rounded-lg bg-green-50 border-2 border-green-200">
                <p className="text-xs text-green-600 mb-1">統合後</p>
                <p className="text-sm font-bold text-gray-900">
                  統合法人
                </p>
                <p className="text-xs text-gray-500 mt-1">
                  {data.combined.total_facilities}施設 / {Math.round(data.combined.total_staff)}人 / {data.combined.service_coverage.length}種別
                </p>
              </div>
            </div>
          </Card>

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {/* 1. 統合前後の施設数・従業者数 */}
            <ChartCard
              title="統合前後比較"
              subtitle="買い手 (A) + 対象 (B) の主要指標"
            >
              <StackedBarChart
                data={beforeAfterChartData}
                xKey="metric"
                series={[
                  { dataKey: "buyer", name: `買い手 (${data.buyer.corp_name.slice(0, 8)})`, color: "#6366f1" },
                  { dataKey: "target", name: `対象 (${data.target.corp_name.slice(0, 8)})`, color: "#10b981" },
                ]}
                height={300}
              />
            </ChartCard>

            {/* 2. サービスポートフォリオ補完度 */}
            <ChartCard
              title="サービスポートフォリオ補完度"
              subtitle={`新規獲得: ${data.combined.new_services.length}種別 / 重複: ${data.combined.service_overlap.length}種別`}
            >
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-gray-200">
                      <th className="text-left py-2 px-3 text-xs font-semibold text-gray-500">
                        サービス種別
                      </th>
                      <th className="text-center py-2 px-3 text-xs font-semibold text-blue-600">
                        買い手 (A)
                      </th>
                      <th className="text-center py-2 px-3 text-xs font-semibold text-orange-600">
                        対象 (B)
                      </th>
                      <th className="text-center py-2 px-3 text-xs font-semibold text-gray-500">
                        統合効果
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {serviceCoverageTable.map((svc, idx) => {
                      const bothHave = svc.buyer_has && svc.target_has;
                      const synergy = !svc.buyer_has && svc.target_has;
                      return (
                        <tr
                          key={svc.service_name}
                          className={idx % 2 === 0 ? "bg-white" : "bg-gray-50"}
                        >
                          <td className="py-2 px-3 font-medium text-gray-800">
                            {formatServiceName(svc.service_name)}
                          </td>
                          <td className="py-2 px-3 text-center">
                            {svc.buyer_has ? (
                              <span className="text-blue-600 font-bold">●</span>
                            ) : (
                              <span className="text-gray-300">-</span>
                            )}
                          </td>
                          <td className="py-2 px-3 text-center">
                            {svc.target_has ? (
                              <span className="text-orange-600 font-bold">●</span>
                            ) : (
                              <span className="text-gray-300">-</span>
                            )}
                          </td>
                          <td className="py-2 px-3 text-center">
                            {bothHave && (
                              <span className="inline-flex items-center px-2 py-0.5 rounded-full text-[10px] font-medium bg-purple-100 text-purple-700">
                                規模拡大
                              </span>
                            )}
                            {synergy && (
                              <span className="inline-flex items-center px-2 py-0.5 rounded-full text-[10px] font-medium bg-green-100 text-green-700">
                                新規獲得
                              </span>
                            )}
                            {!svc.target_has && svc.buyer_has && (
                              <span className="inline-flex items-center px-2 py-0.5 rounded-full text-[10px] font-medium bg-blue-100 text-blue-700">
                                既存維持
                              </span>
                            )}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
              <div className="mt-3 flex items-center gap-4 text-[10px] text-gray-500">
                <span className="flex items-center gap-1">
                  <span className="w-3 h-3 rounded bg-green-100 border border-green-300" />
                  新規獲得: 統合で初めて提供可能になるサービス
                </span>
                <span className="flex items-center gap-1">
                  <span className="w-3 h-3 rounded bg-purple-100 border border-purple-300" />
                  規模拡大: 両社が提供中でスケールメリット
                </span>
              </div>
            </ChartCard>

            {/* 3. 地域カバレッジ統合 */}
            <ChartCard
              title="地域カバレッジ統合"
              subtitle={`統合後: ${data.combined.prefecture_coverage.length}都道府県 (新規: ${data.combined.new_prefectures.length})`}
            >
              {regionCoverage.length === 0 ? (
                <div className="py-8 text-center text-sm text-gray-400">
                  地域カバレッジデータなし
                </div>
              ) : (
                <>
                  <div className="space-y-2">
                    {regionCoverage.map((region) => {
                      const style = REGION_CATEGORY_STYLES[region.category];
                      return (
                        <div
                          key={region.prefecture}
                          className="flex items-center justify-between py-2.5 px-3 rounded-lg bg-gray-50"
                        >
                          <span className="text-sm font-medium text-gray-800">
                            {region.prefecture}
                          </span>
                          <span
                            className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-semibold ${style.bg} ${style.text}`}
                          >
                            {style.label}
                          </span>
                        </div>
                      );
                    })}
                  </div>

                  {/* サマリー */}
                  <div className="mt-4 grid grid-cols-3 gap-3">
                    <div className="text-center p-2 rounded bg-blue-50">
                      <p className="text-lg font-bold text-blue-700">
                        {regionCoverage.filter((r) => r.category === "buyer_only").length}
                      </p>
                      <p className="text-[10px] text-blue-600">買い手のみ</p>
                    </div>
                    <div className="text-center p-2 rounded bg-orange-50">
                      <p className="text-lg font-bold text-orange-700">
                        {regionCoverage.filter((r) => r.category === "target_only").length}
                      </p>
                      <p className="text-[10px] text-orange-600">対象のみ</p>
                    </div>
                    <div className="text-center p-2 rounded bg-purple-50">
                      <p className="text-lg font-bold text-purple-700">
                        {regionCoverage.filter((r) => r.category === "overlap").length}
                      </p>
                      <p className="text-[10px] text-purple-600">重複エリア</p>
                    </div>
                  </div>
                </>
              )}
            </ChartCard>

            {/* 4. シナジー金額推定（プレースホルダー） */}
            <ChartCard
              title="シナジー金額推定"
              subtitle="統合による収益改善効果の試算"
            >
              <DataPendingPlaceholder
                message="シナジー金額推定（将来実装）"
                description="財務データ統合後に、コストシナジー・レベニューシナジーの金額推定を表示します"
                height={260}
              />
            </ChartCard>
          </div>

          {/* 統合リスク分析 */}
          <ChartCard
            title="統合リスク分析"
            subtitle="買い手と対象の格差によるPMIリスク要因"
          >
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-gray-200">
                    <th className="text-left py-2 px-3 text-xs font-semibold text-gray-500">
                      リスク項目
                    </th>
                    <th className="text-left py-2 px-3 text-xs font-semibold text-blue-600">
                      買い手 (A)
                    </th>
                    <th className="text-left py-2 px-3 text-xs font-semibold text-orange-600">
                      対象 (B)
                    </th>
                    <th className="text-left py-2 px-3 text-xs font-semibold text-gray-500">
                      格差
                    </th>
                    <th className="text-left py-2 px-3 text-xs font-semibold text-gray-500">
                      重大度
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {integrationRisks.map((risk, idx) => {
                    const style = SEVERITY_STYLES[risk.severity];
                    return (
                      <tr
                        key={risk.risk_item}
                        className={idx % 2 === 0 ? "bg-white" : "bg-gray-50"}
                      >
                        <td className="py-2.5 px-3 font-medium text-gray-800">
                          {risk.risk_item}
                        </td>
                        <td className="py-2.5 px-3 text-gray-700 font-mono text-xs">
                          {risk.buyer_value}
                        </td>
                        <td className="py-2.5 px-3 text-gray-700 font-mono text-xs">
                          {risk.target_value}
                        </td>
                        <td className="py-2.5 px-3 text-gray-600 text-xs">
                          {risk.gap}
                        </td>
                        <td className="py-2.5 px-3">
                          <span
                            className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-semibold ${style.bg} ${style.text}`}
                          >
                            {style.label}
                          </span>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </ChartCard>

          {/* PMI実行ロードマップ（プレースホルダー） */}
          <Card>
            <div className="flex items-center justify-between">
              <div>
                <h3 className="text-sm font-semibold text-gray-700">
                  PMI実行ロードマップ
                </h3>
                <p className="text-xs text-gray-400 mt-0.5">
                  統合後100日計画の自動生成は今後実装予定
                </p>
              </div>
              <button
                disabled
                className="px-4 py-2 bg-gray-200 text-gray-400 text-sm font-medium rounded-lg cursor-not-allowed"
              >
                ロードマップ生成（準備中）
              </button>
            </div>
          </Card>
        </div>
      )}

      {/* 未選択状態 */}
      {!bothSelected && !isLoading && (
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
                  d="M8 7h12m0 0l-4-4m4 4l-4 4m0 6H4m0 0l4 4m-4-4l4-4"
                />
              </svg>
            </div>
            <p className="text-sm font-medium text-gray-500">
              買い手法人と対象法人を選択してシナジー分析を実行してください
            </p>
            <p className="text-xs text-gray-400 mt-1 max-w-md">
              両法人のデータを比較し、サービスポートフォリオの補完度、地域カバレッジの拡大、統合リスクを自動分析します
            </p>
          </div>
        </Card>
      )}
    </div>
  );
}

export default function PmiSynergyPage() {
  return (
    <Suspense fallback={<div className="text-gray-400 text-sm p-8">読み込み中...</div>}>
      <PmiSynergyContent />
    </Suspense>
  );
}
