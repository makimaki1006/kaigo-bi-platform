"use client";

// ===================================================
// Page: 決算書の開示状況
//
// 決算PDFの「中身の金額」は網羅できない（実測: PL収益が取れたのは24.9%、
// PDFの47.5%はスキャン画像）。そこを無理に指標化すると母集団が歪む。
//
// このページが扱うのは、全223,103施設で機械的に確定する層だけ:
//   決算書を出しているか / 3点セットが揃っているか / いつ出したか / 形式は何か
// 金額の話は「取れた分だけ」として施設詳細側で扱い、
// ここでは実測値を明示して過信を防ぐ。
// ===================================================

import { Suspense, useMemo } from "react";
import Link from "next/link";
import { useApi } from "@/hooks/useApi";
import KpiCard from "@/components/data-display/KpiCard";
import KpiCardGrid from "@/components/data-display/KpiCardGrid";
import BarChart from "@/components/charts/BarChart";
import LineChart from "@/components/charts/LineChart";
import DonutChart from "@/components/charts/DonutChart";
import ChartCard from "@/components/charts/ChartCard";
import ApiErrorBanner from "@/components/common/ApiErrorBanner";
import type {
  FinancialDisclosureKpi,
  FinancialDisclosureByPrefecture,
  FinancialDisclosureByCorpType,
  FinancialDisclosureByService,
  FinancialDisclosureFreshness,
  FinancialDisclosureGap,
  FinancialDisclosureByAcctType,
  FinancialExtractionStatus,
  FinancialMetricsSummary,
  FinancialMetricsByCorpType,
} from "@/lib/types";

const IconDoc = (
  <svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
    <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" /><polyline points="14 2 14 8 20 8" />
  </svg>
);
const IconFullSet = (
  <svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
    <path d="M9 11l3 3L22 4" /><path d="M21 12v7a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11" />
  </svg>
);
const IconClock = (
  <svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
    <circle cx="12" cy="12" r="10" /><polyline points="12 6 12 12 16 14" />
  </svg>
);
const IconCorp = (
  <svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
    <rect x="3" y="7" width="18" height="14" rx="2" /><path d="M8 7V5a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2" />
  </svg>
);

function FinancialDisclosureContent() {
  const { data: kpi, error: kpiError, isLoading: kpiLoading } =
    useApi<FinancialDisclosureKpi>("/api/financial/disclosure/kpi");
  const { data: byPref, isLoading: prefLoading } =
    useApi<FinancialDisclosureByPrefecture[]>("/api/financial/disclosure/by-prefecture");
  const { data: byCorp, isLoading: corpLoading } =
    useApi<FinancialDisclosureByCorpType[]>("/api/financial/disclosure/by-corp-type");
  const { data: bySvc, isLoading: svcLoading } =
    useApi<FinancialDisclosureByService[]>("/api/financial/disclosure/by-service");
  const { data: fresh, isLoading: freshLoading } =
    useApi<FinancialDisclosureFreshness[]>("/api/financial/disclosure/freshness");
  const { data: byAcct, isLoading: acctLoading } =
    useApi<FinancialDisclosureByAcctType[]>("/api/financial/disclosure/by-acct-type");
  const { data: extraction } =
    useApi<FinancialExtractionStatus>("/api/financial/extraction-status");
  const { data: metrics } =
    useApi<FinancialMetricsSummary>("/api/financial/metrics/summary");
  const { data: metricsByCorp } =
    useApi<FinancialMetricsByCorpType[]>("/api/financial/metrics/by-corp-type");
  const { data: gap } =
    useApi<FinancialDisclosureGap>("/api/financial/disclosure/gap");

  /** 法人種別別の開示率（母数の大きい順） */
  const corpChart = useMemo(
    () =>
      (byCorp ?? [])
        .filter((d) => d.facilities >= 500)
        .map((d) => ({ name: d.corp_type, rate: d.disclosure_rate })),
    [byCorp]
  );

  /** 都道府県別の開示率。低い順に出す（＝取りこぼしが多い地域が上に来る） */
  const prefChart = useMemo(
    () =>
      [...(byPref ?? [])]
        .sort((a, b) => a.disclosure_rate - b.disclosure_rate)
        .map((d) => ({ name: d.prefecture, rate: d.disclosure_rate })),
    [byPref]
  );

  /** サービス種別別（開示率の高い順・上位15） */
  const svcChart = useMemo(
    () =>
      [...(bySvc ?? [])]
        .sort((a, b) => b.disclosure_rate - a.disclosure_rate)
        .slice(0, 15)
        .map((d) => ({ name: d.service, rate: d.disclosure_rate })),
    [bySvc]
  );

  /** アップロード月次推移 */
  const freshChart = useMemo(
    () => (fresh ?? []).map((d) => ({ month: d.month, count: d.count })),
    [fresh]
  );

  /** 会計種類の内訳（上位6件＋その他） */
  const acctChart = useMemo(() => {
    const rows = [...(byAcct ?? [])].sort((a, b) => b.facilities - a.facilities);
    const top = rows.slice(0, 6).map((d) => ({
      name: d.acct_type.length > 18 ? `${d.acct_type.slice(0, 18)}…` : d.acct_type,
      value: d.facilities,
    }));
    const rest = rows.slice(6).reduce((s, d) => s + d.facilities, 0);
    return rest > 0 ? [...top, { name: "その他", value: rest }] : top;
  }, [byAcct]);

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-gray-900">決算書の開示状況</h1>
        <p className="text-sm text-gray-500 mt-1">
          介護サービス情報公表システムに決算書（事業活動計算書・貸借対照表・資金収支計算書）を
          掲載しているかどうかを全施設について集計しています。
        </p>
      </div>

      {kpiError && <ApiErrorBanner error={kpiError} />}

      <KpiCardGrid>
        <KpiCard
          label="決算書を掲載している施設"
          value={kpi?.with_any_rate ?? null}
          format="percent"
          icon={IconDoc}
          subtitle={kpi ? `${kpi.with_any.toLocaleString()} / ${kpi.facilities.toLocaleString()} 施設` : undefined}
          loading={kpiLoading}
          accentColor="bg-indigo-500"
        />
        <KpiCard
          label="3点セットが揃っている"
          value={kpi?.full_set_rate ?? null}
          format="percent"
          icon={IconFullSet}
          subtitle={kpi ? `${kpi.full_set.toLocaleString()} 施設（PL+BS+CF）` : undefined}
          loading={kpiLoading}
          accentColor="bg-emerald-500"
        />
        <KpiCard
          label="直近1年以内に更新"
          value={kpi?.fresh_within_1y_rate ?? null}
          format="percent"
          icon={IconClock}
          subtitle={kpi ? `最新 ${kpi.latest_upload ?? "-"} / 最古 ${kpi.oldest_upload ?? "-"}` : undefined}
          loading={kpiLoading}
          accentColor="bg-sky-500"
        />
        <KpiCard
          label="対象法人数"
          value={kpi?.corporations ?? null}
          format="number"
          icon={IconCorp}
          subtitle={kpi ? `事業所 ${kpi.jigyosho.toLocaleString()} 件` : undefined}
          loading={kpiLoading}
          accentColor="bg-amber-500"
        />
      </KpiCardGrid>

      {/* 何が分かって何が分からないかを最初に置く。
          「決算データがある」と「金額が使える」は別物なので、ここで線を引く */}
      {extraction && !("unavailable" in (extraction as object)) && (
        <div className="rounded-lg bg-amber-50 border border-amber-200 px-4 py-3">
          <p className="text-sm font-semibold text-amber-900 mb-1">
            金額（売上・利益など）はこのページの集計に入っていません
          </p>
          <p className="text-xs text-amber-800 leading-relaxed">
            決算書は各事業者が任意の書式でアップロードしたファイルです。
            {extraction.analyzed_files.toLocaleString()}ファイル（
            {extraction.analyzed_facilities.toLocaleString()}施設）を実際に解析したところ、
            <span className="font-semibold">テキスト層があるのは {extraction.text_layer_rate}%</span>
            （残りはスキャン画像）、
            <span className="font-semibold">売上高を機械抽出できたのは {extraction.pl_revenue_rate}%</span>
            でした。会計期間の記載を特定できたのはテキスト層があるファイルの
            {extraction.period_detect_rate}%、法人全体か拠点単位かの記載は{extraction.scope_stated_rate}%です。
            取得率が法人種別に大きく偏るため、金額の全国平均・順位付けは行っていません。
            個別施設で抽出できた金額は施設詳細に、抽出できなかったものは理由と原本リンクを表示します。
            {extraction.surveyed_at && (
              <span className="text-amber-600">（最終更新 {extraction.surveyed_at}）</span>
            )}
          </p>
        </div>
      )}

      {/* 開示ギャップ: そのまま営業リスト・DD対象になるセグメント。
          数字を見せて終わりにせず、同じ条件の一覧へ直接飛ばす */}
      {gap && gap.no_disclosure > 0 && (
        <section className="rounded-xl border border-gray-200 bg-white p-4">
          <h2 className="text-sm font-semibold text-gray-800 mb-1">
            決算書が手に入らない事業者
          </h2>
          <p className="text-xs text-gray-500 mb-3">
            リンクを押すと施設マスタが同じ条件で絞り込まれ、そのままCSVに出せます。
          </p>
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
            <GapCard
              href="/facilities?financial_status=none"
              label="決算書を1つも出していない"
              value={gap.no_disclosure}
              sub={`${gap.no_disclosure_rate}% / ${gap.no_disclosure_corporations.toLocaleString()} 法人`}
              tone="red"
            />
            <GapCard
              href="/facilities?financial_status=stale"
              label="1年以上更新されていない"
              value={gap.stale_over_1y}
              sub={
                gap.stale_over_2y > 0
                  ? `2年以上は ${gap.stale_over_2y.toLocaleString()} 施設`
                  : "2年以上は0件（公表開始が2024年秋のため）"
              }
              tone="amber"
            />
            <GapCard
              href="/facilities?financial_status=full"
              label="3点セットが揃っている"
              value={kpi?.full_set ?? 0}
              sub="DD対象として財務が読める候補"
              tone="emerald"
            />
          </div>

          {gap.by_prefecture.length > 0 && (
            <p className="text-[11px] text-gray-400 mt-3">
              未開示が多い都道府県:{" "}
              {gap.by_prefecture.slice(0, 5).map((p) =>
                `${p.prefecture} ${p.no_disclosure.toLocaleString()}件(${p.no_disclosure_rate}%)`
              ).join(" / ")}
            </p>
          )}
        </section>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <ChartCard
          title="法人種別ごとの開示率"
          subtitle="決算書（事業活動計算書）を掲載している施設の割合"
          loading={corpLoading}
        >
          {corpChart.length > 0 ? (
            <BarChart
              data={corpChart}
              xKey="name"
              yKey="rate"
              horizontal
              unit="%"
              height={320}
              tooltipFormatter={(v) => `${v}%`}
            />
          ) : (
            <EmptyBox loading={corpLoading} />
          )}
        </ChartCard>

        <ChartCard
          title="会計種類の内訳"
          subtitle="公表システムに登録されている会計基準（未記載を含む）"
          loading={acctLoading}
        >
          {acctChart.length > 0 ? (
            <DonutChart data={acctChart} nameKey="name" valueKey="value" height={320} unit="施設" />
          ) : (
            <EmptyBox loading={acctLoading} />
          )}
        </ChartCard>
      </div>

      <ChartCard
        title="決算書がアップロードされた月"
        subtitle="URLに埋め込まれた更新時刻から集計。全リンクの100%で取得できる"
        loading={freshLoading}
      >
        {freshChart.length > 0 ? (
          <LineChart
            data={freshChart}
            xKey="month"
            series={[{ dataKey: "count", name: "件数", color: "#4f46e5" }]}
            height={300}
            tooltipFormatter={(v) => `${v.toLocaleString()}件`}
          />
        ) : (
          <EmptyBox loading={freshLoading} />
        )}
      </ChartCard>

      <ChartCard
        title="都道府県別の開示率（低い順）"
        subtitle="上に出ている地域ほど決算書が手に入らない。営業リストの精度に効く"
        loading={prefLoading}
      >
        {prefChart.length > 0 ? (
          <BarChart
            data={prefChart}
            xKey="name"
            yKey="rate"
            horizontal
            unit="%"
            height={900}
            color="#0ea5e9"
            tooltipFormatter={(v) => `${v}%`}
          />
        ) : (
          <EmptyBox loading={prefLoading} />
        )}
      </ChartCard>

      <ChartCard
        title="サービス種別ごとの開示率（上位15）"
        subtitle="施設数100件以上のサービスのみ"
        loading={svcLoading}
      >
        {svcChart.length > 0 ? (
          <BarChart
            data={svcChart}
            xKey="name"
            yKey="rate"
            horizontal
            unit="%"
            height={520}
            color="#10b981"
            tooltipFormatter={(v) => `${v}%`}
          />
        ) : (
          <EmptyBox loading={svcLoading} />
        )}
      </ChartCard>

      {/* 抽出できた金額。参考値であることを構造で示す:
          カード群とは別枠・グレー基調・件数を必ず併記する */}
      {metrics && metrics.facilities_with_any_amount > 0 && (
        <section className="rounded-xl border border-gray-200 bg-gray-50 p-4">
          <h2 className="text-sm font-semibold text-gray-800">
            参考: 決算書から抽出できた金額の中央値
          </h2>
          <p className="text-xs text-gray-500 mt-1 mb-3">
            {metrics.source}。抽出できた施設は{" "}
            {metrics.facilities_with_any_amount.toLocaleString()} 件。
            n が {metrics.min_n} 件未満の区分は表示していません。
          </p>

          <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
            <MetricStat
              label="人件費率（人件費 ÷ 収益）"
              stat={metrics.personnel_ratio}
              note="5〜100%の範囲外は除外"
            />
            <MetricStat
              label="自己資本比率（純資産 ÷ 総資産）"
              stat={metrics.equity_ratio}
            />
          </div>

          {metricsByCorp && metricsByCorp.some((d) => d.personnel_median != null) && (
            <div className="mt-3 overflow-x-auto">
              <table className="w-full text-xs">
                <thead>
                  <tr className="text-left text-gray-500 border-b border-gray-200">
                    <th className="py-1.5 pr-3 font-medium">法人種別</th>
                    <th className="py-1.5 pr-3 font-medium text-right">人件費率 中央値</th>
                    <th className="py-1.5 pr-3 font-medium text-right">n</th>
                    <th className="py-1.5 pr-3 font-medium text-right">自己資本比率 中央値</th>
                    <th className="py-1.5 font-medium text-right">n</th>
                  </tr>
                </thead>
                <tbody>
                  {metricsByCorp.map((d) => (
                    <tr key={d.corp_type} className="border-b border-gray-100">
                      <td className="py-1.5 pr-3 text-gray-700">{d.corp_type}</td>
                      <td className="py-1.5 pr-3 text-right tabular-nums text-gray-900">
                        {d.personnel_median != null ? `${d.personnel_median}%` : "-"}
                      </td>
                      <td className="py-1.5 pr-3 text-right tabular-nums text-gray-400">{d.personnel_n}</td>
                      <td className="py-1.5 pr-3 text-right tabular-nums text-gray-900">
                        {d.equity_median != null ? `${d.equity_median}%` : "-"}
                      </td>
                      <td className="py-1.5 text-right tabular-nums text-gray-400">{d.equity_n}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </section>
      )}

      <div className="rounded-lg bg-blue-50 border border-blue-200 px-4 py-3">
        <p className="text-xs text-blue-700 leading-relaxed">
          <span className="font-semibold">データソース:</span>{" "}
          厚生労働省「介護サービス情報公表システム」の運営情報ページに掲載された財務諸表リンクより集計。
          掲載の有無・掲載日はリンクから機械的に判定しています。
          {kpi?.csv_count ? `うち ${kpi.csv_count.toLocaleString()} 件は会計ソフトから出力したCSVです。` : ""}
          個別の決算書は{" "}
          <Link href="/facilities" className="underline">施設マスタ</Link>
          {" "}の各施設詳細から原本を開けます。
        </p>
      </div>
    </div>
  );
}

/** セグメント1枚。クリックで同条件の施設一覧へ */
function GapCard({
  href, label, value, sub, tone,
}: { href: string; label: string; value: number; sub: string; tone: "red" | "amber" | "emerald" }) {
  const tones = {
    red: "border-red-200 bg-red-50 hover:bg-red-100 text-red-700",
    amber: "border-amber-200 bg-amber-50 hover:bg-amber-100 text-amber-700",
    emerald: "border-emerald-200 bg-emerald-50 hover:bg-emerald-100 text-emerald-700",
  };
  return (
    <Link href={href} className={`block rounded-lg border p-3 transition-colors ${tones[tone]}`}>
      <p className="text-[11px] font-medium">{label}</p>
      <p className="text-xl font-bold tabular-nums mt-0.5">{value.toLocaleString()} 施設</p>
      <p className="text-[11px] opacity-70 mt-0.5">{sub}</p>
      <p className="text-[11px] underline mt-1">一覧を見る →</p>
    </Link>
  );
}

/** 参考値の1枚。n が閾値未満なら数値を出さず件数だけ示す */
function MetricStat({
  label, stat, note,
}: { label: string; stat: FinancialMetricsSummary["personnel_ratio"]; note?: string }) {
  return (
    <div className="bg-white rounded-lg border border-gray-200 p-3">
      <p className="text-[11px] text-gray-500">{label}</p>
      {stat.published && stat.median != null ? (
        <>
          <p className="text-xl font-bold text-gray-900 tabular-nums mt-0.5">{stat.median}%</p>
          {stat.quartiles && (
            <p className="text-[11px] text-gray-400 mt-0.5">
              四分位 {stat.quartiles.p25}% / {stat.quartiles.p50}% / {stat.quartiles.p75}%
            </p>
          )}
        </>
      ) : (
        <p className="text-sm text-gray-400 mt-1">件数不足のため非表示</p>
      )}
      <p className="text-[11px] text-gray-400 mt-0.5">
        n = {stat.n.toLocaleString()}
        {note ? ` / ${note}` : ""}
      </p>
    </div>
  );
}

function EmptyBox({ loading }: { loading?: boolean }) {
  return (
    <div className="flex items-center justify-center h-[280px] text-gray-400 text-sm">
      {loading ? "読み込み中..." : "データがありません"}
    </div>
  );
}

export default function FinancialDisclosurePage() {
  return (
    <Suspense fallback={<div className="text-gray-400 text-sm p-8">読み込み中...</div>}>
      <FinancialDisclosureContent />
    </Suspense>
  );
}
