// ===================================================
// 公開トップページ（ランディング本文）
//
// Server Component のまま（"use client" 禁止）。初期HTMLに
// H1・本文・CTAリンクを含める。必須セクション（指示書§5.1）:
//   ヒーロー / データ概要 / 3つの利用目的 / 主要機能 /
//   データの誠実さ / 料金要約 / 最終CTA / FAQ
//
// 表記ルール（誇張防止・SEO_BRIEF_SUPPLEMENT 準拠）:
//   - 「223,103の施設・サービスレコード（約19万事業所）」と書く。
//     「223,103施設」と断定しない
//   - 財務の数値化はパイロット段階（24施設 / 52レコード）である旨を隠さない
//   - 「経営危険度」ではなく「要確認シグナル」。予測・確定表現を使わない
//   - 未実装機能（マイ施設登録）を利用可能と書かない
// ===================================================

import type { Metadata } from "next";
import Link from "next/link";
import PublicLayout from "@/components/public/PublicLayout";
import { SoftwareApplicationJsonLd } from "@/components/public/StructuredData";
import { buildPublicMetadata } from "@/lib/seo";

export const metadata: Metadata = buildPublicMetadata({
  path: "/",
  title: "公開情報でわかる介護事業所のBI・データ分析",
  description:
    "全国223,103の施設・サービスレコード（約19万事業所）の公開情報をもとに、市場・人材・品質・法人情報を分析。経営支援・営業支援・M&A支援に対応したBIサービスです。",
});

// 3つの利用目的（ペルソナ別ページへの導線）
const PURPOSES = [
  {
    href: "/features/management",
    title: "経営支援",
    audience: "介護事業者・経営企画・施設責任者",
    body: "同一サービス・同一地域の事業所と、人員配置や品質・稼働の指標を比較。自事業所の立ち位置を把握します。",
  },
  {
    href: "/features/sales",
    title: "営業支援",
    audience: "介護業界へ営業する企業",
    body: "サービス種別・地域・規模などの条件で対象市場を把握し、アプローチ先リストを作成します（リスト作成・CSV出力はProプラン以上）。",
  },
  {
    href: "/features/ma",
    title: "M&A支援",
    audience: "M&A仲介・買い手・事業開発",
    body: "候補となる法人・施設を探索し、公開情報を整理。簡易的なデューデリジェンスの下調べを支援します。",
  },
] as const;

// 主要機能
const FEATURES = [
  {
    title: "市場分析",
    body: "全国・都道府県・市区町村の単位で、サービス種別ごとの事業所数や供給の状況を可視化します。",
  },
  {
    title: "法人・施設分析",
    body: "法人と施設・サービスのつながりを整理し、規模や展開エリア、サービス構成を確認できます。",
  },
  {
    title: "人材・品質分析",
    body: "公表情報をもとにした人員配置や、品質に関する派生指標を確認できます。欠損は0とみなさず区別します。",
  },
] as const;

// 料金要約（pricing/page.tsx と一致：税別・月額）
const PLANS = [
  { name: "Free", price: "0", summary: "全国サマリーを中心とした業界データの閲覧" },
  { name: "Standard", price: "9,800", summary: "経営支援・BIの全機能" },
  { name: "Pro", price: "29,800", summary: "＋営業リスト作成（CSV出力 月3,000行）" },
  { name: "M&A", price: "49,800", summary: "＋M&Aスクリーニング・DD・PMI（CSV 月10,000行）" },
] as const;

// FAQ（画面表示とJSON-LDで同一データを使い、内容を完全一致させる）
const FAQ = [
  {
    q: "「223,103」は施設の数ですか？",
    a: "いいえ。施設とサービス種別の組み合わせで数えたレコード数です。ユニークな事業所数は約19万で、法人数は約6.8万です。1つの事業所が複数のサービスを提供している場合、複数レコードになります。",
  },
  {
    q: "どのようなデータをもとにしていますか？",
    a: "介護サービス情報の公表制度など、公開されている情報を中心にしています。公表値・正規化値・計算による派生値・決算PDF由来の抽出値を区別して扱っています。",
  },
  {
    q: "財務データは全国の事業所で見られますか？",
    a: "いいえ。決算PDFの数値化はパイロット段階で、現在は24施設・52レコードにとどまります。決算PDFへのリンク整備は全国規模で進めていますが、中身の数値化はこれからです。",
  },
  {
    q: "無料で使えますか？",
    a: "Freeプラン（0円）で、全国サマリーを中心とした業界データを閲覧できます。経営支援の全機能はStandard、営業リスト作成はPro以上でご利用いただけます（いずれも税別・月額）。",
  },
  {
    q: "M&Aの判断はどこまでできますか？",
    a: "候補の探索や法人情報の整理、簡易的なデューデリジェンスの下調べを支援します。確認すべき点を「要確認シグナル」として示すもので、売却可能性の予測や確定的な判定は行いません。専門的なデューデリジェンスの代替にはなりません。",
  },
] as const;

// 画面表示のFAQと完全一致するFAQPage構造化データ
const faqJsonLd = {
  "@context": "https://schema.org",
  "@type": "FAQPage",
  mainEntity: FAQ.map((item) => ({
    "@type": "Question",
    name: item.q,
    acceptedAnswer: {
      "@type": "Answer",
      text: item.a,
    },
  })),
};

export default function HomePage() {
  return (
    <PublicLayout>
      {/* 公開トップ専用の構造化データ */}
      <SoftwareApplicationJsonLd />
      <script
        type="application/ld+json"
        // JSON.stringify 出力のみ（画面表示のFAQと同一データ）
        dangerouslySetInnerHTML={{ __html: JSON.stringify(faqJsonLd) }}
      />

      {/* 1. ヒーロー */}
      <section className="border-b border-gray-100 bg-gradient-to-b from-brand-50/60 to-white">
        <div className="mx-auto max-w-3xl px-4 py-20 text-center">
          <h1 className="text-3xl font-bold tracking-tight text-gray-900 sm:text-4xl">
            公開情報でわかる、介護事業所のBI・データ分析
          </h1>
          <p className="mx-auto mt-6 max-w-2xl text-base leading-relaxed text-gray-600">
            全国223,103の施設・サービスレコード（約19万事業所）の公開情報をもとに、
            市場・人材・品質・法人情報を分析します。
            介護事業者の経営支援、介護業界への営業支援、M&A支援の3つの目的に対応したBIサービスです。
          </p>
          <div className="mt-8 flex flex-col items-center justify-center gap-3 sm:flex-row">
            <Link
              href="/signup"
              className="rounded-lg bg-brand-600 px-6 py-3 text-sm font-semibold text-white transition-colors hover:bg-brand-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500 focus-visible:ring-offset-2"
            >
              無料で始める
            </Link>
            <Link
              href="/pricing"
              className="rounded-lg border border-brand-600 px-6 py-3 text-sm font-semibold text-brand-700 transition-colors hover:bg-brand-50 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500 focus-visible:ring-offset-2"
            >
              料金を見る
            </Link>
          </div>
        </div>
      </section>

      {/* 2. データ概要 */}
      <section className="mx-auto max-w-4xl px-4 py-16">
        <h2 className="text-2xl font-bold tracking-tight text-gray-900">
          全国規模の公開情報を、比較できる形に
        </h2>
        <p className="mt-4 max-w-2xl text-base leading-relaxed text-gray-600">
          介護サービスの公表制度など、公開されている情報をもとにデータベースを構築しています。
          数値は誇張せず、集計の単位を正確にお伝えします。
        </p>
        <dl className="mt-8 grid grid-cols-1 gap-6 sm:grid-cols-3">
          <div className="rounded-xl border border-gray-200 p-6">
            <dt className="text-sm font-medium text-gray-500">施設・サービスレコード</dt>
            <dd className="mt-2 text-3xl font-bold tabular-nums text-gray-900">223,103</dd>
            <dd className="mt-2 text-sm text-gray-500">
              施設×サービス種別の組み合わせ。ユニークな事業所の数ではありません。
            </dd>
          </div>
          <div className="rounded-xl border border-gray-200 p-6">
            <dt className="text-sm font-medium text-gray-500">ユニーク事業所数</dt>
            <dd className="mt-2 text-3xl font-bold tabular-nums text-gray-900">約19万</dd>
            <dd className="mt-2 text-sm text-gray-500">
              1事業所が複数サービスを提供する場合を名寄せした概数です。
            </dd>
          </div>
          <div className="rounded-xl border border-gray-200 p-6">
            <dt className="text-sm font-medium text-gray-500">ユニーク法人数</dt>
            <dd className="mt-2 text-3xl font-bold tabular-nums text-gray-900">約6.8万</dd>
            <dd className="mt-2 text-sm text-gray-500">
              事業所を運営する法人の数です。
            </dd>
          </div>
        </dl>
        <p className="mt-6 text-sm text-gray-500">
          データの出典・粒度・更新の考え方は
          <Link href="/data" className="mx-1 font-medium text-brand-700 underline hover:text-brand-800">
            データについて
          </Link>
          で公開しています。
        </p>
      </section>

      {/* 3. 3つの利用目的 */}
      <section className="border-y border-gray-100 bg-gray-50">
        <div className="mx-auto max-w-5xl px-4 py-16">
          <h2 className="text-2xl font-bold tracking-tight text-gray-900">
            3つの目的に合わせて使えます
          </h2>
          <p className="mt-4 max-w-2xl text-base leading-relaxed text-gray-600">
            経営支援・営業支援・M&A支援のそれぞれで、必要な分析と機能を用意しています。
          </p>
          <div className="mt-8 grid grid-cols-1 gap-6 md:grid-cols-3">
            {PURPOSES.map((p) => (
              <Link
                key={p.href}
                href={p.href}
                className="group flex flex-col rounded-xl border border-gray-200 bg-white p-6 transition-colors hover:border-brand-400 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500 focus-visible:ring-offset-2"
              >
                <h3 className="text-lg font-semibold text-gray-900">{p.title}</h3>
                <p className="mt-1 text-xs font-medium text-gray-500">{p.audience}</p>
                <p className="mt-3 flex-1 text-sm leading-relaxed text-gray-600">{p.body}</p>
                <span className="mt-4 text-sm font-semibold text-brand-700 group-hover:underline">
                  {p.title}の詳細を見る
                </span>
              </Link>
            ))}
          </div>
        </div>
      </section>

      {/* 4. 主要機能 */}
      <section className="mx-auto max-w-5xl px-4 py-16">
        <h2 className="text-2xl font-bold tracking-tight text-gray-900">主要な分析機能</h2>
        <p className="mt-4 max-w-2xl text-base leading-relaxed text-gray-600">
          市場・法人施設・人材品質を、公開情報の範囲で分析します。
        </p>
        <div className="mt-8 grid grid-cols-1 gap-6 md:grid-cols-3">
          {FEATURES.map((f) => (
            <div key={f.title} className="rounded-xl border border-gray-200 p-6">
              <h3 className="text-lg font-semibold text-gray-900">{f.title}</h3>
              <p className="mt-3 text-sm leading-relaxed text-gray-600">{f.body}</p>
            </div>
          ))}
        </div>
        <div className="mt-6 rounded-xl border border-amber-200 bg-amber-50 p-5">
          <h3 className="text-sm font-semibold text-amber-900">財務分析はパイロット段階です</h3>
          <p className="mt-2 text-sm leading-relaxed text-amber-800">
            決算PDFのリンク整備は全国規模で進めていますが、PDFの中身を数値化できているのは
            現在24施設・52レコードにとどまります。財務は「全国対応」ではなく、限定的なパイロットとしてご案内しています。
          </p>
        </div>
      </section>

      {/* 5. データの誠実さ */}
      <section className="border-y border-gray-100 bg-gray-50">
        <div className="mx-auto max-w-4xl px-4 py-16">
          <h2 className="text-2xl font-bold tracking-tight text-gray-900">
            データの扱いを正直にお伝えします
          </h2>
          <dl className="mt-8 space-y-6">
            <div>
              <dt className="text-base font-semibold text-gray-900">出典</dt>
              <dd className="mt-1 text-sm leading-relaxed text-gray-600">
                介護サービス情報の公表制度など、公開されている情報を中心に構築しています。
              </dd>
            </div>
            <div>
              <dt className="text-base font-semibold text-gray-900">更新の考え方</dt>
              <dd className="mt-1 text-sm leading-relaxed text-gray-600">
                公開元の更新に合わせてデータを取り込みます。ビルドのたびに更新日を偽らず、
                根拠を管理できる範囲で基準日を表示します。
              </dd>
            </div>
            <div>
              <dt className="text-base font-semibold text-gray-900">欠損と推定の区別</dt>
              <dd className="mt-1 text-sm leading-relaxed text-gray-600">
                欠損値を0とはみなしません。公表値・正規化値・計算による派生値・AIによる抽出値を区別し、
                算定できない指標は空欄として扱います。
              </dd>
            </div>
          </dl>
          <p className="mt-6 text-sm text-gray-500">
            件数や粒度の詳細は
            <Link href="/data" className="mx-1 font-medium text-brand-700 underline hover:text-brand-800">
              データについて
            </Link>
            、指標の定義や免責は
            <Link href="/methodology" className="mx-1 font-medium text-brand-700 underline hover:text-brand-800">
              指標とデータの考え方
            </Link>
            をご覧ください。
          </p>
        </div>
      </section>

      {/* 6. 料金要約 */}
      <section className="mx-auto max-w-5xl px-4 py-16">
        <h2 className="text-2xl font-bold tracking-tight text-gray-900">料金プラン</h2>
        <p className="mt-4 max-w-2xl text-base leading-relaxed text-gray-600">
          目的に合わせて4つのプランを用意しています（金額は税別・月額）。
        </p>
        <div className="mt-8 grid grid-cols-1 gap-6 sm:grid-cols-2 lg:grid-cols-4">
          {PLANS.map((plan) => (
            <div key={plan.name} className="flex flex-col rounded-xl border border-gray-200 p-6">
              <h3 className="text-lg font-semibold text-gray-900">{plan.name}</h3>
              <p className="mt-2">
                <span className="text-2xl font-bold tabular-nums text-gray-900">{plan.price}</span>
                <span className="ml-1 text-sm text-gray-500">円/月（税別）</span>
              </p>
              <p className="mt-3 flex-1 text-sm leading-relaxed text-gray-600">{plan.summary}</p>
            </div>
          ))}
        </div>
        <p className="mt-6 text-sm text-gray-500">
          各プランの機能比較は
          <Link href="/pricing" className="mx-1 font-medium text-brand-700 underline hover:text-brand-800">
            料金プランの詳細
          </Link>
          をご確認ください。
        </p>
      </section>

      {/* 7. 最終CTA */}
      <section className="border-y border-gray-100 bg-brand-50/60">
        <div className="mx-auto max-w-3xl px-4 py-16 text-center">
          <h2 className="text-2xl font-bold tracking-tight text-gray-900">
            まずは無料で、全国の介護市場を俯瞰する
          </h2>
          <p className="mx-auto mt-4 max-w-xl text-base leading-relaxed text-gray-600">
            Freeプランは0円ではじめられます。目的に合わせて、いつでもプランを変更できます。
          </p>
          <div className="mt-8 flex flex-col items-center justify-center gap-3 sm:flex-row">
            <Link
              href="/signup"
              className="rounded-lg bg-brand-600 px-6 py-3 text-sm font-semibold text-white transition-colors hover:bg-brand-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500 focus-visible:ring-offset-2"
            >
              無料で始める
            </Link>
            <Link
              href="/pricing"
              className="rounded-lg border border-brand-600 px-6 py-3 text-sm font-semibold text-brand-700 transition-colors hover:bg-brand-50 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500 focus-visible:ring-offset-2"
            >
              料金を見る
            </Link>
          </div>
        </div>
      </section>

      {/* 8. FAQ（faqJsonLd と内容を完全一致させている） */}
      <section className="mx-auto max-w-3xl px-4 py-16">
        <h2 className="text-2xl font-bold tracking-tight text-gray-900">よくある質問</h2>
        <dl className="mt-8 divide-y divide-gray-100">
          {FAQ.map((item) => (
            <div key={item.q} className="py-6">
              <dt className="text-base font-semibold text-gray-900">{item.q}</dt>
              <dd className="mt-2 text-sm leading-relaxed text-gray-600">{item.a}</dd>
            </div>
          ))}
        </dl>
      </section>
    </PublicLayout>
  );
}
