// ===================================================
// データについてページ（公開SEOページ）
//
// 参照: claudedocs/SEO_IMPLEMENTATION_BRIEF_20260727.md §5.3
//       claudedocs/SEO_BRIEF_SUPPLEMENT_20260728.md（数値・4層区分の正）
//
// 表記ルール（誇張防止）:
//  - 「223,103施設」と断定せず「223,103の施設・サービスレコード（約19万事業所）」と書く
//  - 財務PDFのURL取得（全国規模）とAI抽出（24施設のみ）を明確に区別する
//  - 実測できない数値・将来予定の数値化は現在値として書かない
// ===================================================

import type { Metadata } from "next";
import Link from "next/link";
import PublicLayout from "@/components/public/PublicLayout";
import { buildPublicMetadata } from "@/lib/seo";

export const metadata: Metadata = buildPublicMetadata({
  path: "/data",
  title: "データについて",
  description:
    "kaigo-biが扱う公開データの出典、件数、粒度、更新方法、欠損の扱い、財務データの範囲を説明します。",
});

export default function DataPage() {
  return (
    <PublicLayout>
      <section className="mx-auto max-w-3xl px-4 py-16">
        <h1 className="text-3xl font-bold tracking-tight text-gray-900 sm:text-4xl">
          データについて
        </h1>
        <p className="mt-4 text-base leading-relaxed text-gray-600">
          kaigo-biは、国が公開している介護事業所の情報公表制度のデータをもとに分析を提供しています。
          このページでは、データの出典、件数、粒度、更新方法、欠損の意味、財務データの現状の範囲について、
          誇張せずに説明します。指標の計算方法については
          <Link href="/methodology" className="text-brand-700 underline hover:text-brand-800">
            指標・データ取扱方針
          </Link>
          をご覧ください。
        </p>

        {/* 主要データ源 */}
        <h2 className="mt-12 text-xl font-semibold text-gray-900">主要データ源</h2>
        <p className="mt-3 text-sm leading-relaxed text-gray-600">
          都道府県が運営する介護サービス情報公表システム（厚生労働省所管の情報公表制度）を主なデータ源としています。
          各事業所が制度に基づき届け出た内容を、公表ページから収集しています。
        </p>
        <p className="mt-3 text-sm leading-relaxed text-gray-600">
          出典:{" "}
          <a
            href="https://www.kaigokensaku.mhlw.go.jp/"
            target="_blank"
            rel="noopener noreferrer"
            className="text-brand-700 underline hover:text-brand-800"
          >
            介護サービス情報公表システム（kaigokensaku.mhlw.go.jp）
          </a>
        </p>

        {/* データの粒度と件数 */}
        <h2 className="mt-12 text-xl font-semibold text-gray-900">データの粒度と件数</h2>
        <p className="mt-3 text-sm leading-relaxed text-gray-600">
          kaigo-biの基本単位は「施設×提供サービス種別」です。1つの事業所が複数のサービス
          （例: 訪問介護と通所介護を同一拠点で提供）を届け出ている場合、それぞれが別レコードとして扱われます。
          そのため、レコード数はユニークな事業所数より多くなります。
        </p>
        <dl className="mt-6 divide-y divide-gray-200 rounded-xl border border-gray-200">
          <div className="grid grid-cols-1 gap-1 px-5 py-4 sm:grid-cols-3 sm:gap-4">
            <dt className="text-sm font-medium text-gray-500">施設・サービスレコード数</dt>
            <dd className="text-sm text-gray-900 sm:col-span-2">
              <span className="font-semibold tabular-nums">223,103</span> 件（施設×サービス種別の組み合わせ）
            </dd>
          </div>
          <div className="grid grid-cols-1 gap-1 px-5 py-4 sm:grid-cols-3 sm:gap-4">
            <dt className="text-sm font-medium text-gray-500">ユニーク事業所数</dt>
            <dd className="text-sm text-gray-900 sm:col-span-2">
              約<span className="font-semibold tabular-nums">190,003</span> 事業所
            </dd>
          </div>
          <div className="grid grid-cols-1 gap-1 px-5 py-4 sm:grid-cols-3 sm:gap-4">
            <dt className="text-sm font-medium text-gray-500">ユニーク法人数</dt>
            <dd className="text-sm text-gray-900 sm:col-span-2">
              <span className="font-semibold tabular-nums">68,563</span> 法人
            </dd>
          </div>
        </dl>
        <p className="mt-3 text-xs text-gray-500">
          「223,103」はユニークな施設の数ではありません。同一施設が複数サービスを提供している場合、
          サービスの数だけレコードが存在するため、施設数（約19万）より大きい数値になります。
        </p>

        {/* 更新方法・頻度 */}
        <h2 className="mt-12 text-xl font-semibold text-gray-900">更新方法・更新頻度</h2>
        <p className="mt-3 text-sm leading-relaxed text-gray-600">
          データは介護サービス情報公表システムの公開ページを収集（スクレイピング）して取得しています。
          全国データの一次取得に加え、変更があった事業所を対象とした差分取得を行っていますが、
          全事業所を固定の周期（毎週・毎月など）で更新する仕組みは現時点では確立していません。
        </p>
        <p className="mt-3 text-sm leading-relaxed text-gray-600">
          そのため、事業所ごとの「最終更新日」を個別のタイムスタンプとして画面上に表示する機能は、
          現時点では未実装です。データ全体としての取得・反映のタイミングは不定期であることを前提にご利用ください。
        </p>

        {/* 欠損の意味 */}
        <h2 className="mt-12 text-xl font-semibold text-gray-900">欠損データの意味</h2>
        <p className="mt-3 text-sm leading-relaxed text-gray-600">
          項目が空欄・未回答となっている場合、それは「実際の値がゼロである」ことを意味しません。
          事業所が当該項目を届け出ていない、または情報公表システム上に記載がないことを示しています。
          kaigo-biでは、欠損を0として扱わず、算定に必要な値が揃わない指標は「算定不能（null）」として扱う方針です。
          詳細は
          <Link href="/methodology" className="text-brand-700 underline hover:text-brand-800">
            指標・データ取扱方針
          </Link>
          をご覧ください。
        </p>

        {/* 財務データの範囲 */}
        <h2 className="mt-12 text-xl font-semibold text-gray-900">
          財務データの範囲（PDF取得とAI抽出の違い）
        </h2>
        <p className="mt-3 text-sm leading-relaxed text-gray-600">
          介護サービス情報公表システムでは、法人によって決算情報のPDFファイルが公開されている場合があります。
          kaigo-biでは、この「PDFファイルの所在（URL）を把握している範囲」と、
          「PDFの中身を実際に数値化（AIによる抽出）できている範囲」を明確に分けて管理しています。
        </p>
        <dl className="mt-6 divide-y divide-gray-200 rounded-xl border border-gray-200">
          <div className="grid grid-cols-1 gap-1 px-5 py-4 sm:grid-cols-3 sm:gap-4">
            <dt className="text-sm font-medium text-gray-500">財務PDFの所在を把握している法人数</dt>
            <dd className="text-sm text-gray-900 sm:col-span-2">
              約<span className="font-semibold tabular-nums">41,362</span> 法人（全国調査は完了。日々増加）
            </dd>
          </div>
          <div className="grid grid-cols-1 gap-1 px-5 py-4 sm:grid-cols-3 sm:gap-4">
            <dt className="text-sm font-medium text-gray-500">
              財務PDFの中身をAIで数値化できている施設数
            </dt>
            <dd className="text-sm text-gray-900 sm:col-span-2">
              <span className="font-semibold tabular-nums">24</span> 施設 /{" "}
              <span className="font-semibold tabular-nums">52</span> レコード（パイロット段階）
            </dd>
          </div>
        </dl>
        <p className="mt-3 text-xs text-gray-500">
          財務PDFのリンク自体は全国規模で整備を進めていますが、PDFに記載された貸借対照表・損益計算書の数値を
          実際にkaigo-bi上で分析可能な形に変換できているのは、現時点では24施設・52レコードのみです。
          「全国規模の財務分析」ではなく、財務数値化のパイロット段階であることをご理解のうえご利用ください。
        </p>

        {/* CTA */}
        <div className="mt-14 rounded-2xl border border-gray-200 bg-gray-50 p-6 text-center">
          <p className="text-sm text-gray-700">
            これらのデータをどのように分析・比較に活用できるか、機能ページでご覧いただけます。
          </p>
          <div className="mt-4 flex flex-col items-center justify-center gap-3 sm:flex-row">
            <Link
              href="/features/management"
              className="rounded-lg bg-brand-600 px-5 py-2.5 text-sm font-semibold text-white transition-colors hover:bg-brand-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500 focus-visible:ring-offset-2"
            >
              経営支援の機能を見る
            </Link>
            <Link
              href="/features/sales"
              className="rounded-lg border border-brand-600 px-5 py-2.5 text-sm font-semibold text-brand-700 transition-colors hover:bg-brand-50 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500 focus-visible:ring-offset-2"
            >
              営業支援の機能を見る
            </Link>
            <Link
              href="/features/ma"
              className="rounded-lg border border-brand-600 px-5 py-2.5 text-sm font-semibold text-brand-700 transition-colors hover:bg-brand-50 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500 focus-visible:ring-offset-2"
            >
              M&amp;A支援の機能を見る
            </Link>
          </div>
        </div>
      </section>
    </PublicLayout>
  );
}
