"use client";

// ===================================================
// 決算書の開示状況カード
//
// 金額そのものは半分以上のPDFから機械抽出できない（実測: スキャン画像が52.5%）。
// 「数字が出ない = データがない」ではなく「原本はあるが機械では読めない」なので、
//   ① 何が出ているか（PL/BS/CF・形式・掲載日）
//   ② なぜ数字が出ていないか（備考）
//   ③ 原本へのリンク
// をセットで見せる。ユーザーが自分で原本を開けば済む話を、
// 「データなし」の空欄で終わらせない。
// ===================================================

import type { FinancialDisclosure, FinancialRecord } from "@/lib/types";

interface Props {
  disclosure?: FinancialDisclosure | null;
  urlPl?: string | null;
  urlCf?: string | null;
  urlBs?: string | null;
  /** 抽出済み財務レコード（0件なら未抽出） */
  records?: FinancialRecord[];
  accountingType?: string | null;
}

/** 掲載日からの経過を人が読める形に */
function freshnessLabel(days: number | null | undefined): { text: string; cls: string } | null {
  if (days == null) return null;
  if (days <= 180) return { text: "直近6か月以内に更新", cls: "text-emerald-600 bg-emerald-50 border-emerald-200" };
  if (days <= 365) return { text: "1年以内に更新", cls: "text-sky-600 bg-sky-50 border-sky-200" };
  if (days <= 730) return { text: "1〜2年前の掲載", cls: "text-amber-600 bg-amber-50 border-amber-200" };
  return { text: "2年以上更新なし", cls: "text-red-500 bg-red-50 border-red-200" };
}

function DocChip({ label, url, uploadedAt }: { label: string; url?: string | null; uploadedAt?: string | null }) {
  if (!url) {
    return (
      <span className="inline-flex items-center gap-1 px-2.5 py-1.5 bg-gray-50 text-gray-400 text-xs rounded-lg border border-gray-200">
        <svg className="w-3.5 h-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="12" r="10" /><line x1="15" y1="9" x2="9" y2="15" /><line x1="9" y1="9" x2="15" y2="15" /></svg>
        {label}（未掲載）
      </span>
    );
  }
  return (
    <a
      href={url}
      target="_blank"
      rel="noopener noreferrer"
      className="inline-flex items-center gap-1 px-2.5 py-1.5 bg-indigo-50 text-indigo-700 text-xs font-medium rounded-lg border border-indigo-200 hover:bg-indigo-100 transition-colors"
      title={uploadedAt ? `掲載日 ${uploadedAt}` : undefined}
    >
      <svg className="w-3.5 h-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" /><polyline points="7 10 12 15 17 10" /><line x1="12" x2="12" y1="15" y2="3" /></svg>
      {label}
      {uploadedAt && <span className="text-indigo-400 font-normal">{uploadedAt}</span>}
    </a>
  );
}

export default function FinancialDisclosureCard({
  disclosure, urlPl, urlCf, urlBs, records = [], accountingType,
}: Props) {
  const hasAny = Boolean(urlPl || urlCf || urlBs);
  if (!hasAny) {
    return (
      <div className="mt-3 pt-3 border-t border-gray-100">
        <p className="text-xs font-medium text-gray-500 mb-1">決算書</p>
        <p className="text-xs text-gray-400">
          公表システムに決算書の掲載がありません（全国の約33%が未掲載）。
        </p>
      </div>
    );
  }

  const fresh = freshnessLabel(disclosure?.days_since_upload);
  // 抽出できたレコード / できなかった理由
  const extracted = records.filter((r) => r.revenue != null || r.total_assets != null || r.net_assets != null);
  const failed = records.filter((r) => !extracted.includes(r));
  const notes = Array.from(new Set(failed.map((r) => r.notes).filter(Boolean))) as string[];

  return (
    <div className="mt-3 pt-3 border-t border-gray-100">
      <div className="flex items-center justify-between flex-wrap gap-2 mb-2">
        <p className="text-xs font-medium text-gray-500">
          決算書
          {disclosure && (
            <span className="ml-1.5 text-gray-400 font-normal">
              {disclosure.doc_count}/3 種類
              {disclosure.is_full_set && "（3点セット）"}
            </span>
          )}
        </p>
        <div className="flex items-center gap-1.5">
          {disclosure?.file_format === "csv" && (
            <span className="text-[10px] px-1.5 py-0.5 rounded border border-violet-200 bg-violet-50 text-violet-600">
              CSV形式（会計ソフト出力）
            </span>
          )}
          {fresh && (
            <span className={`text-[10px] px-1.5 py-0.5 rounded border ${fresh.cls}`}>{fresh.text}</span>
          )}
        </div>
      </div>

      <div className="flex flex-wrap gap-2">
        <DocChip label="事業活動計算書" url={urlPl} uploadedAt={disclosure?.uploaded_at_pl} />
        <DocChip label="貸借対照表" url={urlBs} uploadedAt={disclosure?.uploaded_at_bs} />
        <DocChip label="資金収支計算書" url={urlCf} uploadedAt={disclosure?.uploaded_at_cf} />
      </div>

      {accountingType && (
        <p className="text-[11px] text-gray-400 mt-2">会計種類: {accountingType}</p>
      )}

      {/* 備考: 構造化できなかったものはここで理由を明示する */}
      {records.length > 0 && extracted.length === 0 && (
        <div className="mt-2 rounded-md bg-gray-50 border border-gray-200 px-2.5 py-2">
          <p className="text-[11px] text-gray-600 font-medium">金額は未抽出</p>
          <ul className="mt-0.5 space-y-0.5">
            {notes.length > 0 ? (
              notes.map((n) => (
                <li key={n} className="text-[11px] text-gray-500 leading-relaxed">・{n}</li>
              ))
            ) : (
              <li className="text-[11px] text-gray-500">・標準的な勘定科目名に一致しませんでした</li>
            )}
          </ul>
          <p className="text-[11px] text-gray-400 mt-1">上のリンクから原本を確認できます。</p>
        </div>
      )}
      {records.length === 0 && (
        <p className="text-[11px] text-gray-400 mt-2">
          金額の抽出は未実施です（原本リンクから確認できます）。
        </p>
      )}
    </div>
  );
}
