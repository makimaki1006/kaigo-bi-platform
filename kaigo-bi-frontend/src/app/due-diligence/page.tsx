"use client";

// ===================================================
// Page 12: DD支援（デューデリジェンス）
// 法人検索 → DDレポート自動生成ダッシュボード
// 実API: /api/dd/search, /api/dd/report/:corpNumber
// ===================================================

import { Suspense, useState, useCallback } from "react";
import { Card } from "@tremor/react";
import RadarChart from "@/components/charts/RadarChart";
import ChartCard from "@/components/charts/ChartCard";
import DataPendingPlaceholder from "@/components/common/DataPendingPlaceholder";
import CorpSearchInput from "@/components/common/CorpSearchInput";
import LoadingSpinner from "@/components/common/LoadingSpinner";
import { formatServiceName } from "@/lib/formatters";
import { useApi } from "@/hooks/useApi";
import ApiErrorBanner from "@/components/common/ApiErrorBanner";
import type { DdReportResponse, DdRiskFlagApi } from "@/lib/types";

/** 加算項目の表示順序（バックエンドのキー名に一致） */
const DD_KASAN_DISPLAY_ORDER = [
  "処遇改善I", "処遇改善II", "処遇改善III", "処遇改善IV",
  "特定事業所I", "特定事業所II", "特定事業所III", "特定事業所IV", "特定事業所V",
  "認知症ケアI", "認知症ケアII",
  "口腔連携", "緊急時",
];

/** 施設別テーブル用の短縮ラベル */
const DD_KASAN_SHORT_LABELS = [
  "処I", "処II", "処III", "処IV",
  "特I", "特II", "特III", "特IV", "特V",
  "認I", "認II",
  "口腔", "緊急",
];

/** リスクレベルの表示スタイル */
const RISK_LEVEL_STYLES: Record<string, { bg: string; text: string; label: string }> = {
  red: { bg: "bg-red-50 border border-red-200", text: "text-red-600", label: "高リスク" },
  yellow: { bg: "bg-amber-50 border border-amber-200", text: "text-amber-600", label: "注意" },
  green: { bg: "bg-emerald-50 border border-emerald-200", text: "text-emerald-600", label: "良好" },
};

/** ベンチマーク比較行の生成（法人実値とリージョン平均から算出） */
function buildBenchmarkRows(report: DdReportResponse) {
  const rows: {
    metric: string;
    targetValue: string;
    areaAvg: string;
    difference: string;
    evaluation: "good" | "average" | "poor";
  }[] = [];

  // 離職率比較
  const corpTurnover = report.hr_dd.avg_turnover_rate;
  const regionTurnover = report.benchmark.region_avg_turnover;
  if (corpTurnover != null) {
    const diff = corpTurnover - regionTurnover;
    rows.push({
      metric: "平均離職率",
      targetValue: `${corpTurnover.toFixed(1)}%`,
      areaAvg: `${regionTurnover.toFixed(1)}%`,
      difference: `${diff > 0 ? "+" : ""}${diff.toFixed(1)}pt`,
      evaluation: diff > 5 ? "poor" : diff > 0 ? "average" : "good",
    });
  }

  // 従業者数比較
  const corpStaff = report.business_dd.total_staff;
  const regionStaff = report.benchmark.region_avg_staff;
  const staffDiff = corpStaff - regionStaff;
  rows.push({
    metric: "平均従業者数/施設",
    targetValue: `${Math.round(corpStaff / Math.max(report.corp_info.facility_count, 1))}人`,
    areaAvg: `${Math.round(regionStaff)}人`,
    difference: `${staffDiff > 0 ? "+" : ""}${Math.round(staffDiff)}人`,
    evaluation: staffDiff > 0 ? "good" : staffDiff < -5 ? "poor" : "average",
  });

  // 定員比較
  const corpCapacity = report.business_dd.avg_capacity;
  const regionCapacity = report.benchmark.region_avg_capacity;
  const capDiff = corpCapacity - regionCapacity;
  rows.push({
    metric: "平均定員",
    targetValue: `${corpCapacity.toFixed(1)}人`,
    areaAvg: `${regionCapacity.toFixed(1)}人`,
    difference: `${capDiff > 0 ? "+" : ""}${capDiff.toFixed(1)}人`,
    evaluation: Math.abs(capDiff) < 5 ? "average" : capDiff > 0 ? "good" : "poor",
  });

  // 常勤比率
  const fulltimeRatio = report.hr_dd.avg_fulltime_ratio;
  if (fulltimeRatio != null) {
    rows.push({
      metric: "常勤比率",
      targetValue: `${fulltimeRatio.toFixed(1)}%`,
      areaAvg: "65.0%",
      difference: `${(fulltimeRatio - 65).toFixed(1)}pt`,
      evaluation: fulltimeRatio >= 65 ? "good" : fulltimeRatio >= 55 ? "average" : "poor",
    });
  }

  // サービス種別数
  const svcCount = report.business_dd?.service_types?.length ?? 0;
  rows.push({
    metric: "サービス種別数",
    targetValue: `${svcCount}種類`,
    areaAvg: "2.3種別",
    difference: `${(svcCount - 2.3).toFixed(1)}`,
    evaluation: svcCount >= 4 ? "good" : svcCount >= 2 ? "average" : "poor",
  });

  return rows;
}

/** DDスコア計算（レーダーチャート用） */
function computeDdScores(report: DdReportResponse) {
  // 事業DD: サービス多角化 + 施設数 + 定員
  const bizScore = Math.min(
    100,
    (report.business_dd?.service_types?.length ?? 0) * 10 +
      report.corp_info.facility_count * 5 +
      (report.business_dd.avg_occupancy ?? 70) * 0.3
  );

  // 人事DD: 離職率が低いほど高スコア + 常勤比率
  const turnover = report.hr_dd.avg_turnover_rate ?? 20;
  const fulltime = report.hr_dd.avg_fulltime_ratio ?? 50;
  const hrScore = Math.max(0, Math.min(100, 100 - turnover * 2 + fulltime * 0.3));

  // コンプラDD: 違反なし + BCP + 保険
  const compScore =
    (report.compliance_dd.has_violations ? 0 : 40) +
    ((report.compliance_dd.bcp_rate ?? 0) * 30) +
    ((report.compliance_dd.insurance_rate ?? 0) * 30);

  // 財務DD: データ有無で判定
  const finScore = report.financial_dd.accounting_type ? 50 : 30;

  return [
    { axis: "事業DD", score: Math.round(bizScore), fullMark: 100 },
    { axis: "人事DD", score: Math.round(hrScore), fullMark: 100 },
    { axis: "コンプラDD", score: Math.round(Math.min(100, compScore)), fullMark: 100 },
    { axis: "財務DD", score: Math.round(finScore), fullMark: 100 },
  ];
}

/** 評価の表示スタイル */
const EVAL_STYLES: Record<string, { bg: string; text: string; label: string }> = {
  good: { bg: "bg-emerald-50 border border-emerald-200", text: "text-emerald-600", label: "良好" },
  average: { bg: "bg-gray-100 border border-gray-200", text: "text-gray-600", label: "平均" },
  poor: { bg: "bg-red-50 border border-red-200", text: "text-red-600", label: "要改善" },
};

function DueDiligenceContent() {
  const [selectedCorpNumber, setSelectedCorpNumber] = useState<string | null>(null);
  const [selectedCorpName, setSelectedCorpName] = useState<string>("");

  // 法人選択ハンドラ
  const handleCorpSelect = useCallback((corpNumber: string, corpName: string) => {
    setSelectedCorpNumber(corpNumber);
    setSelectedCorpName(corpName);
  }, []);

  const handleCorpClear = useCallback(() => {
    setSelectedCorpNumber(null);
    setSelectedCorpName("");
  }, []);

  // DDレポート取得（法人選択後）
  const ddEndpoint = selectedCorpNumber
    ? `/api/dd/report/${selectedCorpNumber}`
    : null;
  const { data: report, isLoading: isReportLoading, error: reportError } =
    useApi<DdReportResponse>(ddEndpoint);

  // レーダーチャート用データ
  const ddScores = report ? computeDdScores(report) : [];
  const benchmarkRows = report ? buildBenchmarkRows(report) : [];

  return (
    <div className="space-y-6">
      {/* ページタイトル */}
      <div>
        <h1 className="text-xl font-bold text-gray-900">DD支援</h1>
        <p className="text-sm text-gray-500 mt-1">
          法人名または法人番号で検索し、デューデリジェンスレポートを自動生成
        </p>
      </div>

      {/* 検索バー */}
      <Card>
        <CorpSearchInput
          label="法人名 / 法人番号で検索"
          onSelect={handleCorpSelect}
          onClear={handleCorpClear}
          placeholder="例: 社会福祉法人 和光会 / 5012405000081"
        />
      </Card>

      {/* APIエラーバナー */}
      <ApiErrorBanner error={reportError} />

      {/* ローディング */}
      {isReportLoading && selectedCorpNumber && (
        <Card>
          <LoadingSpinner text="DDレポートを生成中..." />
        </Card>
      )}

      {/* DDダッシュボード（レポート取得後に表示） */}
      {report && !isReportLoading && (
        <div className="space-y-6 animate-fade-in-up">
          {/* 法人基本情報カード */}
          <Card>
            <div className="flex items-start justify-between">
              <div>
                <div className="flex items-center gap-3 mb-2">
                  <h2 className="text-lg font-bold text-gray-900">
                    {report.corp_info.corp_name}
                  </h2>
                </div>
                <div className="grid grid-cols-2 md:grid-cols-4 gap-x-8 gap-y-2 text-sm">
                  <div>
                    <span className="text-gray-500">法人番号:</span>{" "}
                    <span className="font-mono text-gray-700">{report.corp_info.corp_number}</span>
                  </div>
                  <div>
                    <span className="text-gray-500">代表者:</span>{" "}
                    <span className="text-gray-700">{report.corp_info.representative || "-"}</span>
                  </div>
                  <div>
                    <span className="text-gray-500">施設数:</span>{" "}
                    <span className="font-semibold text-blue-600">{report.corp_info.facility_count}</span>
                  </div>
                  <div>
                    <span className="text-gray-500">総従業者数:</span>{" "}
                    <span className="font-semibold text-gray-700">{Math.round(report.business_dd.total_staff)}人</span>
                  </div>
                  <div>
                    <span className="text-gray-500">展開地域:</span>{" "}
                    <span className="text-gray-700">{report.corp_info.prefectures.join("、") || "-"}</span>
                  </div>
                  <div className="col-span-2 md:col-span-3">
                    <span className="text-gray-500">サービス種別:</span>{" "}
                    <span className="text-gray-700">
                      {report.business_dd?.service_types?.map(formatServiceName).join("、") || "-"}
                    </span>
                  </div>
                </div>
              </div>
            </div>
          </Card>

          {/* 4軸レーダー + DD詳細 */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {/* 4軸レーダーチャート */}
            <ChartCard
              title="DD 4軸評価"
              subtitle="事業DD / 人事DD / コンプラDD / 財務DD"
            >
              <RadarChart
                data={ddScores}
                categoryKey="axis"
                series={[
                  { dataKey: "score", name: "評価スコア", color: "#6366f1" },
                ]}
                height={320}
              />
              <div className="mt-3 text-center p-3 bg-brand-50 rounded-lg border border-brand-100">
                <span className="text-3xl font-bold text-brand-700">
                  {ddScores.length > 0
                    ? Math.round(ddScores.reduce((sum, d) => sum + d.score, 0) / ddScores.length)
                    : "-"}
                </span>
                <span className="text-sm text-brand-400 ml-1">/ 100 (総合)</span>
              </div>
            </ChartCard>

            {/* DD詳細カテゴリ */}
            <ChartCard
              title="DD カテゴリ別詳細"
              subtitle="各DDカテゴリのチェック項目"
            >
              <div className="space-y-4">
                {/* 事業DD */}
                <div className="rounded-lg border border-blue-200 bg-blue-50 p-3">
                  <h4 className="text-sm font-semibold text-blue-700 mb-2">
                    事業DD ({ddScores[0]?.score ?? "-"}点)
                  </h4>
                  <div className="grid grid-cols-2 gap-2 text-xs text-gray-600">
                    <div>サービスポートフォリオ: {report.business_dd?.service_types?.length ?? 0}種別</div>
                    <div>施設一覧: {report.business_dd?.facilities?.length ?? 0}施設</div>
                    <div>平均定員: {report.business_dd?.avg_capacity?.toFixed(1) ?? "-"}人</div>
                    <div>稼働率: {report.business_dd?.avg_occupancy != null ? `${report.business_dd.avg_occupancy.toFixed(1)}%` : "データなし"}</div>
                  </div>
                </div>

                {/* 人事DD */}
                <div className="rounded-lg border border-orange-200 bg-orange-50 p-3">
                  <h4 className="text-sm font-semibold text-orange-700 mb-2">
                    人事DD ({ddScores[1]?.score ?? "-"}点)
                  </h4>
                  <div className="grid grid-cols-2 gap-2 text-xs text-gray-600">
                    <div>離職率: {report.hr_dd.avg_turnover_rate != null ? `${report.hr_dd.avg_turnover_rate.toFixed(1)}%` : "-"}</div>
                    <div>常勤比率: {report.hr_dd.avg_fulltime_ratio != null ? `${report.hr_dd.avg_fulltime_ratio.toFixed(1)}%` : "-"}</div>
                    <div>前年度採用: {Math.round(report.hr_dd.total_hired)}人</div>
                    <div>前年度退職: {Math.round(report.hr_dd.total_left)}人</div>
                  </div>
                </div>

                {/* コンプラDD */}
                <div className="rounded-lg border border-green-200 bg-green-50 p-3">
                  <h4 className="text-sm font-semibold text-green-700 mb-2">
                    コンプラDD ({ddScores[2]?.score ?? "-"}点)
                  </h4>
                  <div className="grid grid-cols-2 gap-2 text-xs text-gray-600">
                    <div>行政処分: {report.compliance_dd.has_violations ? "あり" : "なし"}</div>
                    <div>BCP策定率: {report.compliance_dd.bcp_rate != null ? `${(report.compliance_dd.bcp_rate * 100).toFixed(0)}%` : "データなし"}</div>
                    <div>賠償保険: {report.compliance_dd.insurance_rate != null ? `${(report.compliance_dd.insurance_rate * 100).toFixed(0)}%` : "データなし"}</div>
                  </div>
                </div>

                {/* 財務DD */}
                <div className="rounded-lg border border-gray-200 bg-gray-50 p-3">
                  <h4 className="text-sm font-semibold text-gray-600 mb-2">
                    財務DD ({ddScores[3]?.score ?? "-"}点)
                  </h4>
                  {report.financial_dd?.accounting_type ? (
                    <div className="text-xs text-gray-600">
                      <div>会計処理: {report.financial_dd.accounting_type}</div>
                      {report.financial_dd?.financial_links?.length > 0 && (
                        <div>関連リンク: {report.financial_dd.financial_links.length}件</div>
                      )}
                    </div>
                  ) : (
                    <DataPendingPlaceholder
                      message="財務データ未取得"
                      description="WAM NETの財務諸表データと連携後に表示"
                      height={60}
                    />
                  )}
                </div>
              </div>
            </ChartCard>
          </div>

          {/* リスクフラグ一覧 */}
          <ChartCard
            title="リスクフラグ一覧"
            subtitle="DDチェック項目ごとのリスク評価"
          >
            {(!report.risk_flags || report.risk_flags.length === 0) ? (
              <div className="py-8 text-center text-sm text-gray-400">
                リスクフラグはありません
              </div>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-gray-200">
                      <th className="text-left py-2 px-3 text-xs font-semibold text-gray-500">
                        カテゴリ
                      </th>
                      <th className="text-left py-2 px-3 text-xs font-semibold text-gray-500">
                        レベル
                      </th>
                      <th className="text-left py-2 px-3 text-xs font-semibold text-gray-500">
                        詳細
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {report.risk_flags?.map((risk: DdRiskFlagApi, idx: number) => {
                      const style = RISK_LEVEL_STYLES[risk.level] ?? RISK_LEVEL_STYLES.yellow;
                      return (
                        <tr
                          key={`${risk.category}-${idx}`}
                          className={idx % 2 === 0 ? "bg-white" : "bg-gray-50"}
                        >
                          <td className="py-2.5 px-3 text-gray-600">
                            {risk.category}
                          </td>
                          <td className="py-2.5 px-3">
                            <span
                              className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-semibold ${style.bg} ${style.text}`}
                            >
                              {style.label}
                            </span>
                          </td>
                          <td className="py-2.5 px-3 text-gray-600 text-xs">
                            {risk.detail}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            )}
          </ChartCard>

          {/* ベンチマーク比較 */}
          <ChartCard
            title="同地域ベンチマーク比較"
            subtitle="対象法人 vs エリア平均の比較"
          >
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-gray-200">
                    <th className="text-left py-2 px-3 text-xs font-semibold text-gray-500">
                      指標
                    </th>
                    <th className="text-left py-2 px-3 text-xs font-semibold text-gray-500">
                      対象法人
                    </th>
                    <th className="text-left py-2 px-3 text-xs font-semibold text-gray-500">
                      エリア平均
                    </th>
                    <th className="text-left py-2 px-3 text-xs font-semibold text-gray-500">
                      差分
                    </th>
                    <th className="text-left py-2 px-3 text-xs font-semibold text-gray-500">
                      評価
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {benchmarkRows.map((row, idx) => {
                    const evalStyle = EVAL_STYLES[row.evaluation];
                    return (
                      <tr
                        key={row.metric}
                        className={idx % 2 === 0 ? "bg-white" : "bg-gray-50"}
                      >
                        <td className="py-2.5 px-3 font-medium text-gray-800">
                          {row.metric}
                        </td>
                        <td className="py-2.5 px-3 text-gray-700 font-mono">
                          {row.targetValue}
                        </td>
                        <td className="py-2.5 px-3 text-gray-500 font-mono">
                          {row.areaAvg}
                        </td>
                        <td className="py-2.5 px-3 text-gray-600 font-mono text-xs">
                          {row.difference}
                        </td>
                        <td className="py-2.5 px-3">
                          <span
                            className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-semibold ${evalStyle.bg} ${evalStyle.text}`}
                          >
                            {evalStyle.label}
                          </span>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </ChartCard>

          {/* 加算取得状況テーブル */}
          <ChartCard
            title="加算取得状況"
            subtitle={report.kasan_summary?.has_data
              ? `${report.kasan_summary.facility_count}施設の加算取得一覧`
              : "13項目の取得/未取得一覧"
            }
          >
            {report.kasan_summary?.has_data ? (
              <div className="overflow-x-auto">
                {/* 法人サマリー: 加算項目ごとの取得施設数 */}
                <table className="w-full text-sm mb-4">
                  <thead>
                    <tr className="border-b border-gray-200">
                      <th className="text-left py-2 px-3 text-xs font-semibold text-gray-500">加算項目</th>
                      <th className="text-center py-2 px-3 text-xs font-semibold text-gray-500 w-32">取得状況</th>
                      <th className="text-center py-2 px-3 text-xs font-semibold text-gray-500 w-20">取得率</th>
                    </tr>
                  </thead>
                  <tbody>
                    {DD_KASAN_DISPLAY_ORDER.map((kasanName, idx) => {
                      const count = report.kasan_summary.totals[kasanName] ?? 0;
                      const total = report.kasan_summary.facility_count;
                      const ratio = total > 0 ? Math.round((count / total) * 100) : 0;
                      return (
                        <tr key={kasanName} className={idx % 2 === 0 ? "bg-white" : "bg-gray-50"}>
                          <td className="py-2 px-3 text-gray-700">{kasanName}</td>
                          <td className="py-2 px-3 text-center">
                            {count > 0 ? (
                              <span className="text-emerald-600 font-semibold text-xs">
                                {count}/{total}施設
                              </span>
                            ) : (
                              <span className="text-gray-400 text-xs">0/{total}施設</span>
                            )}
                          </td>
                          <td className="py-2 px-3 text-center">
                            <span className={`text-xs font-medium ${ratio > 0 ? "text-emerald-600" : "text-gray-400"}`}>
                              {ratio}%
                            </span>
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>

                {/* 施設別詳細（施設が2つ以上の場合に表示） */}
                {report.kasan_summary.facilities.length > 1 && (
                  <>
                    <h4 className="text-xs font-semibold text-gray-500 mb-2 mt-4 px-3">施設別詳細</h4>
                    <table className="w-full text-xs">
                      <thead>
                        <tr className="border-b border-gray-200">
                          <th className="text-left py-1.5 px-2 text-[10px] font-semibold text-gray-500 sticky left-0 bg-white min-w-[120px]">施設名</th>
                          {DD_KASAN_SHORT_LABELS.map((label) => (
                            <th key={label} className="text-center py-1.5 px-1 text-[10px] font-semibold text-gray-500 min-w-[32px]">
                              {label}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {report.kasan_summary.facilities.map((fac, idx) => (
                          <tr key={`${fac.facility_name}-${idx}`} className={idx % 2 === 0 ? "bg-white" : "bg-gray-50"}>
                            <td className="py-1.5 px-2 text-gray-700 truncate max-w-[180px] sticky left-0 bg-inherit" title={fac.facility_name}>
                              {fac.facility_name}
                            </td>
                            {DD_KASAN_DISPLAY_ORDER.map((kasanName) => {
                              const has = fac.kasan[kasanName] ?? false;
                              return (
                                <td key={kasanName} className="py-1.5 px-1 text-center">
                                  {has ? (
                                    <span className="text-emerald-500 font-bold">&#9675;</span>
                                  ) : (
                                    <span className="text-gray-300">&times;</span>
                                  )}
                                </td>
                              );
                            })}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </>
                )}
              </div>
            ) : (
              <DataPendingPlaceholder
                message="加算データ未取得"
                description="フルデータ取得後に13項目の加算取得状況を表示します"
                height={200}
              />
            )}
          </ChartCard>

          {/* コンプライアンスDD詳細 */}
          <ChartCard
            title="コンプライアンスDD"
            subtitle="行政処分・指導歴・BCP・保険"
          >
            <div className="space-y-3">
              {/* 行政処分 */}
              <div className={`rounded-lg border p-3 ${
                report.compliance_dd.has_violations
                  ? "bg-red-50 border-red-200"
                  : "bg-emerald-50 border-emerald-200"
              }`}>
                <div className="flex items-center gap-2 mb-1">
                  {report.compliance_dd.has_violations ? (
                    <svg className="w-4 h-4 text-red-500" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z" /><line x1="12" y1="9" x2="12" y2="13" /><line x1="12" y1="17" x2="12.01" y2="17" /></svg>
                  ) : (
                    <svg className="w-4 h-4 text-emerald-500" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14" /><polyline points="22 4 12 14.01 9 11.01" /></svg>
                  )}
                  <span className={`text-sm font-semibold ${
                    report.compliance_dd.has_violations ? "text-red-700" : "text-emerald-700"
                  }`}>
                    行政処分: {report.compliance_dd.has_violations ? "あり" : "なし"}
                  </span>
                </div>
                <p className="text-xs text-gray-500 ml-6">
                  行政処分・指導歴の詳細はフルデータ取得後に表示されます
                </p>
              </div>

              {/* BCP/保険 */}
              <div className="grid grid-cols-2 gap-3">
                <div className="rounded-lg border border-gray-200 p-3">
                  <p className="text-xs text-gray-500 mb-1">BCP策定率</p>
                  <p className="text-lg font-bold text-gray-900">
                    {report.compliance_dd.bcp_rate != null
                      ? `${(report.compliance_dd.bcp_rate * 100).toFixed(0)}%`
                      : "データなし"
                    }
                  </p>
                </div>
                <div className="rounded-lg border border-gray-200 p-3">
                  <p className="text-xs text-gray-500 mb-1">賠償保険加入率</p>
                  <p className="text-lg font-bold text-gray-900">
                    {report.compliance_dd.insurance_rate != null
                      ? `${(report.compliance_dd.insurance_rate * 100).toFixed(0)}%`
                      : "データなし"
                    }
                  </p>
                </div>
              </div>
            </div>
          </ChartCard>

          {/* 財務DDリンク */}
          <ChartCard
            title="財務DD"
            subtitle="財務諸表ダウンロード・会計情報"
          >
            <div className="space-y-3">
              <div className="text-sm text-gray-700">
                <span className="text-gray-500">会計処理: </span>
                {report.financial_dd?.accounting_type || "データなし"}
              </div>
              {report.financial_dd?.financial_links?.length > 0 ? (
                <div className="flex flex-wrap gap-2">
                  {report.financial_dd?.financial_links?.map((link, idx) => (
                    <a
                      key={idx}
                      href={link}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="inline-flex items-center gap-1.5 px-3 py-2 bg-indigo-50 text-indigo-700 text-sm font-medium rounded-lg border border-indigo-200 hover:bg-indigo-100 transition-colors"
                    >
                      <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" /><polyline points="7 10 12 15 17 10" /><line x1="12" x2="12" y1="15" y2="3" /></svg>
                      財務諸表 {idx + 1}
                    </a>
                  ))}
                </div>
              ) : (
                <DataPendingPlaceholder
                  message="財務諸表データなし"
                  description="WAM NET連携後に財務諸表ダウンロードリンクを表示します"
                  height={80}
                />
              )}
            </div>
          </ChartCard>

          {/* DDレポートエクスポートボタン */}
          <Card>
            <div className="flex items-center justify-between">
              <div>
                <h3 className="text-sm font-semibold text-gray-700">
                  DDレポートエクスポート
                </h3>
                <p className="text-xs text-gray-400 mt-0.5">
                  PDF/Excelフォーマットでのレポート出力は今後実装予定
                </p>
              </div>
              <button
                disabled
                className="px-4 py-2 bg-gray-200 text-gray-400 text-sm font-medium rounded-lg cursor-not-allowed"
              >
                PDF出力（準備中）
              </button>
            </div>
          </Card>
        </div>
      )}

      {/* 未選択状態 */}
      {!selectedCorpNumber && (
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
                  d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"
                />
              </svg>
            </div>
            <p className="text-sm font-medium text-gray-500">
              法人を検索して選択してください
            </p>
            <p className="text-xs text-gray-400 mt-1">
              検索結果からDDレポートが自動生成されます
            </p>
          </div>
        </Card>
      )}
    </div>
  );
}

export default function DueDiligencePage() {
  return (
    <Suspense fallback={<div className="text-gray-400 text-sm p-8">読み込み中...</div>}>
      <DueDiligenceContent />
    </Suspense>
  );
}
