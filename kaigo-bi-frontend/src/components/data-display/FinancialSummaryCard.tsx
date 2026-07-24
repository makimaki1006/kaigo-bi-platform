"use client";

// ===================================================
// 財務サマリーカード
// 決算PDFからAI抽出した財務データ(financialsテーブル)を表示
// PL: 収益・営業損益・人件費率・前年比 / BS: 総資産・純資産・自己資本比率
// ===================================================

import type { FinancialRecord } from "@/lib/types";

interface FinancialSummaryCardProps {
  records: FinancialRecord[];
  /** タイトル横に表示する補足（例: 法人全体） */
  subtitle?: string;
}

/** 金額を読みやすく整形（億・万円区切り） */
function formatYen(value: number | null): string {
  if (value == null) return "-";
  const abs = Math.abs(value);
  const sign = value < 0 ? "△" : "";
  if (abs >= 100_000_000) return `${sign}${(abs / 100_000_000).toFixed(1)}億円`;
  if (abs >= 10_000) return `${sign}${(abs / 10_000).toFixed(0)}万円`;
  return `${sign}${abs.toLocaleString("ja-JP")}円`;
}

/** 増減率（%） */
function growthRate(current: number | null, prior: number | null): string | null {
  if (current == null || prior == null || prior === 0) return null;
  const rate = ((current - prior) / Math.abs(prior)) * 100;
  return `${rate > 0 ? "+" : ""}${rate.toFixed(1)}%`;
}

/** 確度バッジ */
function ConfidenceBadge({ confidence }: { confidence: string | null }) {
  if (!confidence) return null;
  const styles: Record<string, string> = {
    high: "bg-emerald-50 text-emerald-600 border-emerald-200",
    medium: "bg-amber-50 text-amber-600 border-amber-200",
    low: "bg-red-50 text-red-500 border-red-200",
  };
  const labels: Record<string, string> = {
    high: "確度: 高",
    medium: "確度: 中",
    low: "確度: 低",
  };
  return (
    <span
      className={`inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-medium border ${styles[confidence] ?? styles.medium}`}
    >
      {labels[confidence] ?? confidence}
    </span>
  );
}

/** 数値セル */
function MetricCell({ label, value, sub }: { label: string; value: string; sub?: string | null }) {
  return (
    <div className="p-3 bg-gray-50 rounded-lg">
      <p className="text-[11px] text-gray-500">{label}</p>
      <p className={`text-lg font-bold tabular-nums mt-0.5 ${value.startsWith("△") ? "text-red-600" : "text-gray-900"}`}>
        {value}
      </p>
      {sub && <p className="text-[11px] text-gray-400 mt-0.5">前年比 {sub}</p>}
    </div>
  );
}

export default function FinancialSummaryCard({ records, subtitle }: FinancialSummaryCardProps) {
  const pl = records.find((r) => r.doc_type === "PL");
  const bs = records.find((r) => r.doc_type === "BS");

  if (!pl && !bs) return null;

  // 人件費率
  const personnelRatio =
    pl?.personnel_cost != null && pl.revenue != null && pl.revenue > 0
      ? `${((pl.personnel_cost / pl.revenue) * 100).toFixed(1)}%`
      : null;
  // 自己資本比率
  const equityRatio =
    bs?.net_assets != null && bs.total_assets != null && bs.total_assets > 0
      ? `${((bs.net_assets / bs.total_assets) * 100).toFixed(1)}%`
      : null;

  return (
    <section className="border border-indigo-100 bg-indigo-50/30 rounded-xl p-4">
      <div className="flex items-center justify-between mb-3 flex-wrap gap-2">
        <h4 className="text-sm font-semibold text-gray-800 flex items-center gap-2">
          財務サマリー
          <span className="text-[10px] font-normal text-indigo-500 bg-indigo-50 border border-indigo-200 rounded px-1.5 py-0.5">
            決算PDF AI抽出
          </span>
          {subtitle && <span className="text-xs font-normal text-gray-500">{subtitle}</span>}
        </h4>
        <div className="flex items-center gap-1.5">
          <ConfidenceBadge confidence={pl?.confidence ?? bs?.confidence ?? null} />
          {(pl?.fiscal_period || bs?.fiscal_period) && (
            <span className="text-[11px] text-gray-400">{pl?.fiscal_period ?? bs?.fiscal_period}</span>
          )}
        </div>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-2">
        {pl && (
          <>
            <MetricCell
              label="収益（売上）"
              value={formatYen(pl.revenue)}
              sub={growthRate(pl.revenue, pl.prior_revenue)}
            />
            <MetricCell
              label="営業損益"
              value={formatYen(pl.operating_income)}
              sub={growthRate(pl.operating_income, pl.prior_operating_income)}
            />
            {personnelRatio ? (
              <MetricCell label="人件費率" value={personnelRatio} />
            ) : (
              <MetricCell label="当期損益" value={formatYen(pl.net_income)} />
            )}
          </>
        )}
        {bs && (
          <>
            <MetricCell label="純資産" value={formatYen(bs.net_assets)} />
            {equityRatio && <MetricCell label="自己資本比率" value={equityRatio} />}
          </>
        )}
      </div>

      {(pl?.notes || bs?.notes) && (
        <p className="text-[11px] text-gray-400 mt-2">{pl?.notes ?? bs?.notes}</p>
      )}
    </section>
  );
}
