"use client";

// ===================================================
// クロス指標カード
// 決算PDF由来の財務 × 公表データの複合指標
// レビュー指摘(2026-07-27)反映:
//  - 経営危険度スコア/低中高 → 「要確認シグナル」(検証済みモデルでないため点数化しない)
//  - 未知と安全を区別: 算定不能時は「算定不能」と明示
//  - PL/BSのスコープ(施設・決算期)一致状況を表示
// ===================================================

import type { CrossMetrics } from "@/lib/types";

interface CrossMetricsCardProps {
  metrics: CrossMetrics;
}

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

export default function CrossMetricsCard({ metrics }: CrossMetricsCardProps) {
  if (!metrics.has_financials) return null;

  const cov = metrics.coverage;
  const signals = metrics.signals ?? [];

  return (
    <section className="border border-purple-100 bg-purple-50/30 rounded-xl p-4">
      <div className="flex items-center justify-between mb-1 flex-wrap gap-2">
        <h4 className="text-sm font-semibold text-gray-800 flex items-center gap-2">
          クロス指標
          <span className="text-[10px] font-normal text-purple-600 bg-purple-50 border border-purple-200 rounded px-1.5 py-0.5">
            財務 × 公表データ
          </span>
        </h4>
        {cov?.fiscal_period && (
          <span className="text-[11px] text-gray-400">決算期: {cov.fiscal_period}</span>
        )}
      </div>

      {/* スコープ注記(自己資本比率はPL/BS一致時のみ意味を持つ) */}
      {!cov?.pl_bs_scope_matched && metrics.equity_ratio == null && (
        <p className="text-[11px] text-gray-400 mb-2">
          ※ 同一施設・同一決算期のPL/BSが揃わないため、自己資本比率は非表示です。
        </p>
      )}

      <div className="grid grid-cols-2 md:grid-cols-5 gap-2 mb-4">
        {metrics.labor_productivity != null && (
          <MetricCell label="労働生産性（1人あたり売上）" value={formatYen(metrics.labor_productivity)} />
        )}
        {metrics.revenue_per_user != null && (
          <MetricCell label="利用者1人あたり収益" value={formatYen(metrics.revenue_per_user)} />
        )}
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
        {metrics.equity_ratio != null && (
          <MetricCell
            label="自己資本比率"
            value={formatPct(metrics.equity_ratio)}
            negative={metrics.equity_ratio < 0}
          />
        )}
      </div>

      {/* 要確認シグナル(点数化しない) */}
      <div className="border-t border-purple-100 pt-3">
        <div className="flex items-center gap-2 mb-1.5">
          <span className="text-xs font-medium text-gray-700">要確認シグナル</span>
          {metrics.signal_count == null ? (
            <span className="text-[11px] text-gray-400">算定に必要なデータが不足（判定不能）</span>
          ) : (
            <span className="text-[11px] text-gray-500">
              該当 {metrics.signal_count} 件 / 判定できたファクタ {cov?.available_factors ?? 0} of {cov?.required_factors ?? 4}
            </span>
          )}
        </div>
        {signals.length > 0 ? (
          <div className="flex flex-wrap gap-1.5">
            {signals.map((s) => (
              <span
                key={s}
                className="inline-flex items-center px-2 py-0.5 rounded-md text-[11px] font-medium bg-red-50 text-red-600 border border-red-200"
              >
                {s}
              </span>
            ))}
          </div>
        ) : (
          metrics.signal_count != null && (
            <p className="text-[11px] text-gray-400">
              取得できた範囲では該当シグナルなし（未取得のファクタは評価対象外）
            </p>
          )
        )}
        <p className="text-[10px] text-gray-400 mt-2">
          ※ 要確認シグナルは公表・決算データからの機械的抽出であり、経営状態や売却意向を断定するものではありません。実査でのご確認を推奨します。
        </p>
      </div>
    </section>
  );
}
