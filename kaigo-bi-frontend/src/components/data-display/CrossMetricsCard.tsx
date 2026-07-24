"use client";

// ===================================================
// クロス指標カード
// 決算PDF由来の財務 × 公表データの複合指標を表示
// 労働生産性 / 利用者単価 / 実人件費率 / 営業利益率 /
// 自己資本比率 + 経営危険度スコア
// ===================================================

import type { CrossMetrics } from "@/lib/types";

interface CrossMetricsCardProps {
  metrics: CrossMetrics;
  /** M&A文脈では「売却期待度」として見せる等のラベル調整 */
  riskLabel?: string;
}

/** 金額を読みやすく整形 */
function formatYen(value: number | null): string {
  if (value == null) return "-";
  const abs = Math.abs(value);
  const sign = value < 0 ? "△" : "";
  if (abs >= 100_000_000) return `${sign}${(abs / 100_000_000).toFixed(1)}億円`;
  if (abs >= 10_000) return `${sign}${(abs / 10_000).toFixed(0)}万円`;
  return `${sign}${abs.toLocaleString("ja-JP")}円`;
}

function formatPct(value: number | null): string {
  if (value == null) return "-";
  return `${(value * 100).toFixed(1)}%`;
}

/** 危険度スコアの色 */
function riskColor(score: number): { bar: string; text: string; label: string } {
  if (score >= 60) return { bar: "bg-red-500", text: "text-red-600", label: "高" };
  if (score >= 30) return { bar: "bg-amber-500", text: "text-amber-600", label: "中" };
  return { bar: "bg-emerald-500", text: "text-emerald-600", label: "低" };
}

function MetricCell({ label, value, negative }: { label: string; value: string; negative?: boolean }) {
  return (
    <div className="p-3 bg-white rounded-lg border border-gray-100">
      <p className="text-[11px] text-gray-500">{label}</p>
      <p className={`text-lg font-bold tabular-nums mt-0.5 ${negative || value.startsWith("△") ? "text-red-600" : "text-gray-900"}`}>
        {value}
      </p>
    </div>
  );
}

export default function CrossMetricsCard({ metrics, riskLabel = "経営危険度" }: CrossMetricsCardProps) {
  if (!metrics.has_financials) return null;

  const risk = riskColor(metrics.risk_score);

  return (
    <section className="border border-purple-100 bg-purple-50/30 rounded-xl p-4">
      <div className="flex items-center justify-between mb-3 flex-wrap gap-2">
        <h4 className="text-sm font-semibold text-gray-800 flex items-center gap-2">
          クロス指標
          <span className="text-[10px] font-normal text-purple-600 bg-purple-50 border border-purple-200 rounded px-1.5 py-0.5">
            財務 × 公表データ
          </span>
        </h4>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-5 gap-2 mb-4">
        <MetricCell label="労働生産性（1人あたり売上）" value={formatYen(metrics.labor_productivity)} />
        <MetricCell label="利用者1人あたり収益" value={formatYen(metrics.revenue_per_user)} />
        <MetricCell
          label="実人件費率"
          value={formatPct(metrics.personnel_cost_ratio)}
          negative={(metrics.personnel_cost_ratio ?? 0) > 0.8}
        />
        <MetricCell
          label="営業利益率"
          value={formatPct(metrics.operating_margin)}
          negative={(metrics.operating_margin ?? 0) < 0}
        />
        <MetricCell
          label="自己資本比率"
          value={formatPct(metrics.equity_ratio)}
          negative={(metrics.equity_ratio ?? 0) < 0}
        />
      </div>

      {/* 経営危険度スコア */}
      <div className="flex items-center gap-3">
        <span className="text-xs text-gray-500 flex-shrink-0">{riskLabel}</span>
        <div className="flex-1 h-2.5 bg-gray-100 rounded-full overflow-hidden">
          <div
            className={`h-full rounded-full ${risk.bar} transition-all`}
            style={{ width: `${Math.min(100, metrics.risk_score)}%` }}
          />
        </div>
        <span className={`text-sm font-bold tabular-nums ${risk.text}`}>
          {metrics.risk_score}
          <span className="text-[10px] font-normal ml-0.5">/100（{risk.label}）</span>
        </span>
      </div>
      {metrics.risk_factors.length > 0 && (
        <div className="flex flex-wrap gap-1.5 mt-2">
          {metrics.risk_factors.map((factor) => (
            <span
              key={factor}
              className="inline-flex items-center px-2 py-0.5 rounded-md text-[11px] font-medium bg-red-50 text-red-600 border border-red-200"
            >
              {factor}
            </span>
          ))}
        </div>
      )}
    </section>
  );
}
