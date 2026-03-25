"use client";

// ===================================================
// 施設詳細パネル
// 行クリック時に展開表示するパネル
// 新規データ（加算・品質・要介護度・経験者割合等）対応版
// ===================================================

import { Card } from "@tremor/react";
import type { FacilityRow, FacilityRowExtended } from "@/lib/types";
import { KASAN_LABELS } from "@/lib/types";
import { formatNumber, formatServiceName, formatCorpType } from "@/lib/formatters";
import LoadingSpinner from "@/components/common/LoadingSpinner";

interface FacilityDetailPanelProps {
  /** 施設詳細データ */
  facility: FacilityRow | FacilityRowExtended | null;
  /** ローディング状態 */
  loading?: boolean;
  /** 閉じるコールバック */
  onClose: () => void;
}

/** 情報行コンポーネント */
function InfoRow({ label, value }: { label: string; value: string | number | null | undefined }) {
  return (
    <div className="flex">
      <dt className="w-36 flex-shrink-0 text-xs text-gray-500 py-1">{label}</dt>
      <dd className="text-sm text-gray-900 py-1">{value ?? "-"}</dd>
    </div>
  );
}

/** 拡張フィールドの有無チェック */
function isExtended(facility: FacilityRow | FacilityRowExtended): facility is FacilityRowExtended {
  return "quality_score" in facility || "addition_count" in facility || "care_level_1" in facility;
}

/** 品質ランクのバッジスタイル */
const RANK_BADGE_STYLES: Record<string, string> = {
  S: "bg-amber-100 text-amber-800 border-amber-300",    // 金
  A: "bg-gray-100 text-gray-700 border-gray-300",        // 銀
  B: "bg-blue-100 text-blue-700 border-blue-300",        // 青
  C: "bg-yellow-100 text-yellow-700 border-yellow-300",  // 黄
  D: "bg-red-100 text-red-700 border-red-300",           // 赤
};

/** 加算バッジコンポーネント */
function KasanBadge({ kasanKey, acquired }: { kasanKey: string; acquired: boolean | null }) {
  const label = KASAN_LABELS[kasanKey] || kasanKey;
  if (acquired === null) return null;
  return (
    <span
      className={`inline-flex items-center gap-1 px-2 py-1 rounded-md text-[10px] font-medium border ${
        acquired
          ? "bg-emerald-50 text-emerald-700 border-emerald-200"
          : "bg-gray-50 text-gray-400 border-gray-200"
      }`}
    >
      {acquired ? (
        <svg className="w-3 h-3" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5">
          <polyline points="20 6 9 17 4 12" />
        </svg>
      ) : (
        <svg className="w-3 h-3" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
          <line x1="18" y1="6" x2="6" y2="18" />
          <line x1="6" y1="6" x2="18" y2="18" />
        </svg>
      )}
      {label}
    </span>
  );
}

export default function FacilityDetailPanel({
  facility,
  loading = false,
  onClose,
}: FacilityDetailPanelProps) {
  if (loading) {
    return (
      <Card className="mt-2 border-l-4 border-l-blue-500">
        <LoadingSpinner text="施設情報を読み込んでいます..." />
      </Card>
    );
  }

  if (!facility) return null;

  const ext = isExtended(facility) ? facility : null;

  // 加算項目のキー一覧
  const kasanKeys = Object.keys(KASAN_LABELS);
  // 加算データの有無判定
  const hasKasanData = ext != null && kasanKeys.some((k) => (ext as any)[k] !== null && (ext as any)[k] !== undefined);
  // 品質データの有無判定
  const hasQualityData = ext?.quality_score != null;
  // 要介護度データの有無判定（care_level_1-5が全てnullの場合は非表示）
  const hasAnyCareLevel = ext?.care_level_1 != null || ext?.care_level_2 != null || ext?.care_level_3 != null || ext?.care_level_4 != null || ext?.care_level_5 != null;
  const hasCareLevelData = hasAnyCareLevel || ext?.total_users != null;
  // 財務諸表データの有無判定
  const hasFinancialData = ext?.financial_statement_url_pl != null || ext?.financial_statement_url_cf != null || ext?.financial_statement_url_bs != null;

  // 要介護度別小バーチャートデータ
  const careLevelData = hasCareLevelData && ext ? [
    { level: "要介護1", count: ext.care_level_1 ?? 0 },
    { level: "要介護2", count: ext.care_level_2 ?? 0 },
    { level: "要介護3", count: ext.care_level_3 ?? 0 },
    { level: "要介護4", count: ext.care_level_4 ?? 0 },
    { level: "要介護5", count: ext.care_level_5 ?? 0 },
  ] : [];
  const maxCareLevel = Math.max(...careLevelData.map((d) => d.count), 1);

  return (
    <Card className="mt-2 border-l-4 border-l-blue-500 animate-fade-in-up">
      {/* ヘッダー */}
      <div className="flex items-start justify-between mb-4">
        <div className="flex items-center gap-3">
          <div>
            <h3 className="text-lg font-bold text-gray-900">
              {facility.jigyosho_name}
            </h3>
            <p className="text-sm text-gray-500">{facility.corp_name}</p>
          </div>
          {/* 品質ランクバッジ */}
          {hasQualityData && ext?.quality_rank && (
            <span
              className={`inline-flex items-center px-2.5 py-1 rounded-full text-sm font-bold border ${
                RANK_BADGE_STYLES[ext.quality_rank] || "bg-gray-100 text-gray-600 border-gray-300"
              }`}
            >
              {ext.quality_rank}ランク
              <span className="ml-1 text-xs font-normal">
                ({ext.quality_score?.toFixed(1)}点)
              </span>
            </span>
          )}
        </div>
        <button
          onClick={onClose}
          className="text-gray-400 hover:text-gray-600 transition-colors p-1"
          aria-label="閉じる"
        >
          <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
          </svg>
        </button>
      </div>

      {/* メイングリッド */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        {/* 基本情報 */}
        <section>
          <h4 className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3 border-b border-gray-100 pb-2 flex items-center gap-1.5">
            <svg className="w-3.5 h-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><rect width="16" height="20" x="4" y="2" rx="2" ry="2" /><path d="M9 22v-4h6v4" /><path d="M8 6h.01" /><path d="M16 6h.01" /><path d="M12 6h.01" /><path d="M12 10h.01" /><path d="M12 14h.01" /><path d="M16 10h.01" /><path d="M16 14h.01" /><path d="M8 10h.01" /><path d="M8 14h.01" /></svg>
            基本情報
          </h4>
          <dl className="space-y-1">
            <InfoRow label="事業所番号" value={facility.jigyosho_number} />
            <InfoRow label="サービス種別" value={formatServiceName(facility.service_name)} />
            <InfoRow label="法人種別" value={formatCorpType(facility.corp_type)} />
            <InfoRow label="開始日" value={facility.start_date} />
            <InfoRow label="住所" value={facility.address} />
            <InfoRow label="電話番号" value={facility.phone} />
            <InfoRow label="FAX" value={facility.fax} />
            <InfoRow label="定員" value={facility.capacity ? `${formatNumber(facility.capacity)}名` : null} />
            {ext?.accounting_type && (
              <InfoRow label="会計種類" value={ext.accounting_type} />
            )}
          </dl>
        </section>

        {/* 管理者・代表者 */}
        <section>
          <h4 className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3 border-b border-gray-100 pb-2 flex items-center gap-1.5">
            <svg className="w-3.5 h-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2" /><circle cx="9" cy="7" r="4" /><path d="M22 21v-2a4 4 0 0 0-3-3.87" /><path d="M16 3.13a4 4 0 0 1 0 7.75" /></svg>
            管理者・代表者
          </h4>
          <dl className="space-y-1">
            <InfoRow label="管理者名" value={facility.manager_name} />
            <InfoRow label="管理者役職" value={facility.manager_title} />
            <InfoRow label="代表者名" value={facility.representative_name} />
            <InfoRow label="代表者役職" value={facility.representative_title} />
          </dl>
        </section>

        {/* 従業者情報 */}
        <section>
          <h4 className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3 border-b border-gray-100 pb-2 flex items-center gap-1.5">
            <svg className="w-3.5 h-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M16 20V4a2 2 0 0 0-2-2h-4a2 2 0 0 0-2 2v16" /><rect width="20" height="14" x="2" y="6" rx="2" /></svg>
            従業者情報
          </h4>
          <dl className="space-y-1">
            <InfoRow label="総従業者数" value={facility.staff_total ? `${formatNumber(facility.staff_total)}名` : null} />
            <InfoRow label="常勤" value={facility.staff_fulltime ? `${formatNumber(facility.staff_fulltime)}名` : null} />
            <InfoRow label="非常勤" value={facility.staff_parttime ? `${formatNumber(facility.staff_parttime)}名` : null} />
            <InfoRow label="離職率" value={facility.turnover_rate != null ? `${(facility.turnover_rate * 100).toFixed(1)}%` : null} />
            <InfoRow label="常勤比率" value={facility.fulltime_ratio != null ? `${(facility.fulltime_ratio * 100).toFixed(1)}%` : null} />
            <InfoRow label="前年採用数" value={facility.hired_last_year} />
            <InfoRow label="前年退職数" value={facility.left_last_year} />
            {ext?.experienced_10yr_ratio != null && (
              <InfoRow label="経験10年以上割合" value={`${ext.experienced_10yr_ratio.toFixed(1)}%`} />
            )}
          </dl>
        </section>

        {/* 外部リンク + 財務諸表 */}
        <section>
          <h4 className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3 border-b border-gray-100 pb-2 flex items-center gap-1.5">
            <svg className="w-3.5 h-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M15 3h6v6" /><path d="M10 14 21 3" /><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6" /></svg>
            外部リンク
          </h4>
          <dl className="space-y-1">
            {facility.homepage ? (
              <div className="flex">
                <dt className="w-36 flex-shrink-0 text-xs text-gray-500 py-1">Webサイト</dt>
                <dd className="text-sm py-1">
                  <a
                    href={facility.homepage}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="text-blue-600 hover:underline"
                  >
                    {facility.homepage}
                  </a>
                </dd>
              </div>
            ) : (
              <InfoRow label="Webサイト" value="-" />
            )}
            <InfoRow label="法人番号" value={facility.corp_number} />
          </dl>

          {/* 財務諸表ダウンロードリンク */}
          {hasFinancialData && (
            <div className="mt-3 pt-3 border-t border-gray-100">
              <p className="text-xs font-medium text-gray-500 mb-2">財務諸表</p>
              <div className="flex flex-wrap gap-2">
                {ext?.financial_statement_url_pl && (
                  <a
                    href={ext.financial_statement_url_pl}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="inline-flex items-center gap-1 px-2.5 py-1.5 bg-indigo-50 text-indigo-700 text-xs font-medium rounded-lg border border-indigo-200 hover:bg-indigo-100 transition-colors"
                  >
                    <svg className="w-3.5 h-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" /><polyline points="7 10 12 15 17 10" /><line x1="12" x2="12" y1="15" y2="3" /></svg>
                    事業活動計算書
                  </a>
                )}
                {ext?.financial_statement_url_cf && (
                  <a
                    href={ext.financial_statement_url_cf}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="inline-flex items-center gap-1 px-2.5 py-1.5 bg-indigo-50 text-indigo-700 text-xs font-medium rounded-lg border border-indigo-200 hover:bg-indigo-100 transition-colors"
                  >
                    <svg className="w-3.5 h-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" /><polyline points="7 10 12 15 17 10" /><line x1="12" x2="12" y1="15" y2="3" /></svg>
                    資金収支計算書
                  </a>
                )}
                {ext?.financial_statement_url_bs && (
                  <a
                    href={ext.financial_statement_url_bs}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="inline-flex items-center gap-1 px-2.5 py-1.5 bg-indigo-50 text-indigo-700 text-xs font-medium rounded-lg border border-indigo-200 hover:bg-indigo-100 transition-colors"
                  >
                    <svg className="w-3.5 h-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" /><polyline points="7 10 12 15 17 10" /><line x1="12" x2="12" y1="15" y2="3" /></svg>
                    貸借対照表
                  </a>
                )}
              </div>
            </div>
          )}
        </section>
      </div>

      {/* 加算取得状況 */}
      {hasKasanData && (
        <div className="mt-6 pt-4 border-t border-gray-100">
          <h4 className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3 flex items-center gap-1.5">
            <svg className="w-3.5 h-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="12" r="10" /><path d="M8 12h8" /><path d="M12 8v8" /></svg>
            加算取得状況
            {ext?.addition_count != null && (
              <span className="text-gray-500 normal-case tracking-normal">
                （{ext.addition_count}/13項目取得）
              </span>
            )}
          </h4>
          <div className="flex flex-wrap gap-1.5">
            {kasanKeys.map((key) => {
              const value = (ext as any)?.[key];
              if (value === null || value === undefined) return null;
              return <KasanBadge key={key} kasanKey={key} acquired={value} />;
            })}
          </div>
        </div>
      )}

      {/* 要介護度別利用者数（care_level_1-5のいずれかに実データがある場合のみバーを表示） */}
      {hasCareLevelData && (
        <div className="mt-6 pt-4 border-t border-gray-100">
          <h4 className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3 flex items-center gap-1.5">
            <svg className="w-3.5 h-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2" /><circle cx="9" cy="7" r="4" /></svg>
            要介護度別利用者数
            {ext?.total_users != null && (
              <span className="text-gray-500 normal-case tracking-normal">
                （合計 {ext.total_users}名）
              </span>
            )}
          </h4>
          {hasAnyCareLevel && careLevelData.length > 0 ? (
            <div className="space-y-1.5">
              {careLevelData.map((d) => (
                <div key={d.level} className="flex items-center gap-2">
                  <span className="text-xs text-gray-600 w-16 flex-shrink-0">{d.level}</span>
                  <div className="flex-1 h-4 bg-gray-100 rounded-full overflow-hidden">
                    <div
                      className="h-full bg-indigo-400 rounded-full transition-all duration-300"
                      style={{ width: `${(d.count / maxCareLevel) * 100}%` }}
                    />
                  </div>
                  <span className="text-xs font-mono text-gray-700 w-8 text-right">{d.count}</span>
                </div>
              ))}
            </div>
          ) : (
            <p className="text-xs text-gray-400">要介護度別の内訳データはありません</p>
          )}
        </div>
      )}
    </Card>
  );
}
