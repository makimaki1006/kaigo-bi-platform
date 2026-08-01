import type { Metadata } from "next";
import Link from "next/link";
import {
  ArrowRight,
  BarChart3,
  Building2,
  Check,
  Map,
  Search,
  Sparkles,
  Target,
  Users,
} from "lucide-react";
import PublicLayout from "@/components/public/PublicLayout";
import { buildPublicMetadata } from "@/lib/seo";

export const metadata: Metadata = buildPublicMetadata({
  path: "/",
  title: "介護業界の営業・経営・M&Aを速くするデータBI",
  description:
    "全国223,103件の介護施設・サービスデータを横断検索。市場分析、営業リスト作成、法人調査、M&Aの初期調査をひとつの画面で進められる介護業界特化BIです。",
});

const USE_CASES = [
  {
    href: "/features/sales",
    icon: Target,
    eyebrow: "営業",
    title: "狙うべき営業先を、すぐに見つける",
    body: "地域、サービス種別、運営法人などの条件から候補を絞り込み、営業リストを作成。リスト作りにかかる時間を、提案活動へ振り向けられます。",
    link: "営業支援を見る",
  },
  {
    href: "/features/management",
    icon: BarChart3,
    eyebrow: "経営",
    title: "自社と地域の現在地を、数字でつかむ",
    body: "エリアごとの施設分布やサービス構成、人員・品質の指標を比較。出店、採用、事業計画の判断材料をひとつにまとめます。",
    link: "経営支援を見る",
  },
  {
    href: "/features/ma",
    icon: Building2,
    eyebrow: "M&A",
    title: "候補法人の初期調査を、もっと速く",
    body: "法人が運営する施設、地域展開、サービス構成を横断して確認。詳しく調査する候補を効率よく絞り込めます。",
    link: "M&A支援を見る",
  },
] as const;

const STEPS = [
  {
    number: "01",
    title: "条件を選ぶ",
    body: "都道府県、市区町村、サービス種別、法人種別などから対象を指定します。",
  },
  {
    number: "02",
    title: "比較して見つける",
    body: "地図、ランキング、法人グループなど複数の視点で候補を比較します。",
  },
  {
    number: "03",
    title: "次の行動につなげる",
    body: "営業先の選定、経営会議、M&A候補の初期調査へ、そのまま活用できます。",
  },
] as const;

const PLANS = [
  {
    name: "Free",
    price: "0",
    description: "まず全国の介護市場を見てみたい方に",
  },
  {
    name: "Standard",
    price: "9,800",
    description: "市場・法人・施設分析を経営に活かしたい方に",
  },
  {
    name: "Pro",
    price: "29,800",
    description: "営業候補の抽出とCSV出力まで進めたい方に",
    featured: true,
  },
  {
    name: "M&A",
    price: "49,800",
    description: "候補探索と初期調査を効率化したい方に",
  },
] as const;

const FAQ = [
  {
    q: "どのような人が利用できますか？",
    a: "介護事業者の経営・事業開発担当者、介護業界へ商品やサービスを提供する企業、M&A仲介会社や買い手企業などを想定しています。",
  },
  {
    q: "どのようなデータを収録していますか？",
    a: "介護サービス情報の公表制度をはじめとする公開情報を整理し、施設・サービス、運営法人、地域、人員、品質などを横断して確認できる形にしています。",
  },
  {
    q: "223,103件とは何の件数ですか？",
    a: "施設とサービス種別の組み合わせで数えた収録レコード数です。同じ事業所が複数の介護サービスを提供する場合は、サービスごとに1件として収録されます。",
  },
  {
    q: "無料で試せますか？",
    a: "Freeプランは月額0円です。全国サマリーなどからkaigo-biのデータをお試しいただき、必要に応じて有料プランへ変更できます。",
  },
] as const;

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

const primaryButton =
  "inline-flex items-center justify-center gap-2 rounded-xl bg-brand-600 px-6 py-3.5 text-sm font-bold text-white shadow-lg shadow-brand-600/20 transition hover:-translate-y-0.5 hover:bg-brand-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500 focus-visible:ring-offset-2";
const secondaryButton =
  "inline-flex items-center justify-center gap-2 rounded-xl border border-gray-300 bg-white px-6 py-3.5 text-sm font-bold text-gray-800 transition hover:border-brand-300 hover:bg-brand-50 hover:text-brand-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500 focus-visible:ring-offset-2";

export default function HomePage() {
  return (
    <PublicLayout>
      <script
        type="application/ld+json"
        dangerouslySetInnerHTML={{ __html: JSON.stringify(faqJsonLd) }}
      />

      <section className="relative overflow-hidden border-b border-gray-100 bg-white">
        <div
          className="absolute inset-x-0 top-0 -z-0 h-[520px] bg-[radial-gradient(circle_at_50%_0%,rgba(99,102,241,0.16),transparent_65%)]"
          aria-hidden="true"
        />
        <div className="relative mx-auto grid max-w-6xl gap-12 px-4 py-20 lg:grid-cols-[1.08fr_0.92fr] lg:items-center lg:py-28">
          <div>
            <p className="inline-flex items-center gap-2 rounded-full border border-brand-200 bg-brand-50 px-3 py-1.5 text-xs font-bold text-brand-700">
              <Sparkles className="h-3.5 w-3.5" aria-hidden="true" />
              介護業界に特化したデータBI
            </p>
            <h1 className="mt-6 text-4xl font-bold leading-tight tracking-tight text-gray-950 sm:text-5xl lg:text-[3.5rem] lg:leading-[1.15]">
              介護業界の営業・経営・M&Aを、
              <span className="text-brand-600">データでもっと速く。</span>
            </h1>
            <p className="mt-6 max-w-2xl text-lg leading-8 text-gray-600">
              全国の介護事業所と運営法人を横断検索。
              市場分析、営業リスト作成、法人調査を、ひとつの画面で進められます。
            </p>
            <div className="mt-8 flex flex-col gap-3 sm:flex-row">
              <Link href="/signup" className={primaryButton}>
                無料で始める
                <ArrowRight className="h-4 w-4" aria-hidden="true" />
              </Link>
              <Link href="#use-cases" className={secondaryButton}>
                活用方法を見る
              </Link>
            </div>
            <p className="mt-4 flex items-center gap-2 text-sm text-gray-500">
              <Check className="h-4 w-4 text-emerald-600" aria-hidden="true" />
              Freeプランは月額0円。クレジットカード不要
            </p>
          </div>

          <div className="relative">
            <div className="absolute -inset-6 rounded-[2rem] bg-gradient-to-br from-brand-100 to-sky-100 opacity-70 blur-2xl" />
            <div className="relative overflow-hidden rounded-2xl border border-gray-200 bg-white shadow-2xl shadow-gray-900/10">
              <div className="flex items-center gap-2 border-b border-gray-100 px-5 py-4">
                <span className="h-2.5 w-2.5 rounded-full bg-red-400" />
                <span className="h-2.5 w-2.5 rounded-full bg-amber-400" />
                <span className="h-2.5 w-2.5 rounded-full bg-emerald-400" />
                <span className="ml-3 text-xs font-semibold text-gray-400">市場分析ダッシュボード</span>
              </div>
              <div className="grid gap-4 p-5 sm:grid-cols-2">
                <div className="rounded-xl bg-gray-950 p-5 text-white sm:col-span-2">
                  <p className="text-xs font-medium text-gray-400">収録データ</p>
                  <p className="mt-2 text-3xl font-bold tabular-nums">223,103<span className="ml-1 text-sm font-medium text-gray-400">件</span></p>
                  <div className="mt-5 grid grid-cols-4 gap-1">
                    {[52, 76, 64, 90, 70, 84, 58, 94, 81, 100, 74, 88].map((height, index) => (
                      <span
                        key={index}
                        className="self-end rounded-sm bg-brand-400"
                        style={{ height: `${height * 0.44}px`, opacity: 0.45 + index * 0.04 }}
                      />
                    ))}
                  </div>
                </div>
                <div className="rounded-xl border border-gray-100 bg-gray-50 p-4">
                  <Map className="h-5 w-5 text-brand-600" aria-hidden="true" />
                  <p className="mt-4 text-xs text-gray-500">分析エリア</p>
                  <p className="mt-1 font-bold text-gray-900">全国47都道府県</p>
                </div>
                <div className="rounded-xl border border-gray-100 bg-gray-50 p-4">
                  <Building2 className="h-5 w-5 text-brand-600" aria-hidden="true" />
                  <p className="mt-4 text-xs text-gray-500">検索単位</p>
                  <p className="mt-1 font-bold text-gray-900">施設・法人</p>
                </div>
                <div className="rounded-xl border border-gray-100 bg-gray-50 p-4 sm:col-span-2">
                  <div className="flex items-center justify-between">
                    <span className="flex items-center gap-2 text-sm font-bold text-gray-900">
                      <Search className="h-4 w-4 text-brand-600" aria-hidden="true" />
                      条件を組み合わせて候補を検索
                    </span>
                    <ArrowRight className="h-4 w-4 text-gray-400" aria-hidden="true" />
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </section>

      <section className="border-b border-gray-100 bg-gray-50">
        <div className="mx-auto grid max-w-6xl grid-cols-2 divide-x divide-gray-200 px-4 py-8 sm:grid-cols-4">
          {[
            ["223,103件", "施設・サービスデータ"],
            ["全国対応", "47都道府県を収録"],
            ["約19万", "ユニーク事業所"],
            ["4領域", "営業・経営・M&A・地域"],
          ].map(([value, label]) => (
            <div key={label} className="px-3 py-3 text-center sm:px-6">
              <p className="text-xl font-bold tabular-nums text-gray-950 sm:text-2xl">{value}</p>
              <p className="mt-1 text-xs text-gray-500 sm:text-sm">{label}</p>
            </div>
          ))}
        </div>
      </section>

      <section id="use-cases" className="scroll-mt-20 bg-white py-20 sm:py-24">
        <div className="mx-auto max-w-6xl px-4">
          <div className="max-w-2xl">
            <p className="text-sm font-bold text-brand-600">活用方法</p>
            <h2 className="mt-3 text-3xl font-bold tracking-tight text-gray-950 sm:text-4xl">
              探す時間を減らし、判断する時間を増やす。
            </h2>
            <p className="mt-4 text-base leading-7 text-gray-600">
              バラバラに公開されている介護情報を、目的に合わせて検索・比較できる形に整理しました。
            </p>
          </div>
          <div className="mt-12 grid gap-6 lg:grid-cols-3">
            {USE_CASES.map((item) => {
              const Icon = item.icon;
              return (
                <Link
                  key={item.href}
                  href={item.href}
                  className="group flex flex-col rounded-2xl border border-gray-200 bg-white p-7 shadow-sm transition hover:-translate-y-1 hover:border-brand-300 hover:shadow-xl hover:shadow-gray-900/5 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500"
                >
                  <div className="flex h-11 w-11 items-center justify-center rounded-xl bg-brand-50 text-brand-600">
                    <Icon className="h-5 w-5" aria-hidden="true" />
                  </div>
                  <p className="mt-6 text-xs font-bold text-brand-600">{item.eyebrow}</p>
                  <h3 className="mt-2 text-xl font-bold leading-8 text-gray-950">{item.title}</h3>
                  <p className="mt-3 flex-1 text-sm leading-7 text-gray-600">{item.body}</p>
                  <span className="mt-6 inline-flex items-center gap-2 text-sm font-bold text-brand-700">
                    {item.link}
                    <ArrowRight className="h-4 w-4 transition group-hover:translate-x-1" aria-hidden="true" />
                  </span>
                </Link>
              );
            })}
          </div>
        </div>
      </section>

      <section className="bg-gray-950 py-20 text-white sm:py-24">
        <div className="mx-auto max-w-6xl px-4">
          <div className="grid gap-12 lg:grid-cols-[0.8fr_1.2fr] lg:items-start">
            <div>
              <p className="text-sm font-bold text-brand-300">使い方はシンプル</p>
              <h2 className="mt-3 text-3xl font-bold tracking-tight sm:text-4xl">
                データ探しから、次の一手まで。
              </h2>
              <p className="mt-5 leading-7 text-gray-400">
                複数の公開サイトや資料を行き来せず、条件設定から比較までkaigo-biで進められます。
              </p>
            </div>
            <ol className="grid gap-4 sm:grid-cols-3">
              {STEPS.map((step) => (
                <li key={step.number} className="rounded-2xl border border-white/10 bg-white/5 p-6">
                  <p className="font-mono text-sm font-bold text-brand-300">{step.number}</p>
                  <h3 className="mt-6 text-lg font-bold">{step.title}</h3>
                  <p className="mt-3 text-sm leading-6 text-gray-400">{step.body}</p>
                </li>
              ))}
            </ol>
          </div>
        </div>
      </section>

      <section className="bg-white py-20 sm:py-24">
        <div className="mx-auto max-w-6xl px-4">
          <div className="grid gap-12 lg:grid-cols-2 lg:items-center">
            <div className="rounded-3xl bg-brand-50 p-8 sm:p-10">
              <div className="grid grid-cols-2 gap-4">
                {[
                  [Search, "横断検索"],
                  [Map, "地域分析"],
                  [Users, "法人名寄せ"],
                  [BarChart3, "比較・可視化"],
                ].map(([Icon, label]) => {
                  const FeatureIcon = Icon as typeof Search;
                  return (
                    <div key={label as string} className="rounded-2xl bg-white p-5 shadow-sm">
                      <FeatureIcon className="h-5 w-5 text-brand-600" aria-hidden="true" />
                      <p className="mt-4 text-sm font-bold text-gray-900">{label as string}</p>
                    </div>
                  );
                })}
              </div>
            </div>
            <div>
              <p className="text-sm font-bold text-brand-600">介護業界特化</p>
              <h2 className="mt-3 text-3xl font-bold tracking-tight text-gray-950 sm:text-4xl">
                表計算では見えにくい、施設と法人のつながりまで。
              </h2>
              <p className="mt-5 text-base leading-8 text-gray-600">
                施設単位の検索だけでなく、同じ法人が運営する施設をまとめて確認。
                規模、展開エリア、サービス構成を横断的に捉えられます。
              </p>
              <ul className="mt-7 space-y-4">
                {[
                  "都道府県・市区町村・サービス種別で絞り込み",
                  "運営法人ごとに施設とサービスを集約",
                  "市場・営業・M&Aで使える複数の分析画面",
                ].map((item) => (
                  <li key={item} className="flex items-start gap-3 text-sm font-medium text-gray-700">
                    <span className="mt-0.5 flex h-5 w-5 shrink-0 items-center justify-center rounded-full bg-emerald-100 text-emerald-700">
                      <Check className="h-3 w-3" aria-hidden="true" />
                    </span>
                    {item}
                  </li>
                ))}
              </ul>
              <Link href="/data" className="mt-8 inline-flex items-center gap-2 text-sm font-bold text-brand-700 hover:text-brand-800">
                収録データについて
                <ArrowRight className="h-4 w-4" aria-hidden="true" />
              </Link>
            </div>
          </div>
        </div>
      </section>

      <section className="border-y border-gray-100 bg-gray-50 py-20 sm:py-24">
        <div className="mx-auto max-w-6xl px-4">
          <div className="text-center">
            <p className="text-sm font-bold text-brand-600">料金プラン</p>
            <h2 className="mt-3 text-3xl font-bold tracking-tight text-gray-950 sm:text-4xl">
              無料から、目的に合わせて。
            </h2>
            <p className="mx-auto mt-4 max-w-2xl text-gray-600">
              まずはFreeプランでデータを確認。必要な機能に合わせてプランを選べます。
            </p>
          </div>
          <div className="mt-12 grid gap-5 sm:grid-cols-2 lg:grid-cols-4">
            {PLANS.map((plan) => (
              <div
                key={plan.name}
                className={`relative flex flex-col rounded-2xl border bg-white p-6 ${
                  "featured" in plan && plan.featured
                    ? "border-brand-500 shadow-xl shadow-brand-600/10"
                    : "border-gray-200"
                }`}
              >
                {"featured" in plan && plan.featured && (
                  <span className="absolute -top-3 left-5 rounded-full bg-brand-600 px-3 py-1 text-[11px] font-bold text-white">
                    営業活用におすすめ
                  </span>
                )}
                <h3 className="text-lg font-bold text-gray-950">{plan.name}</h3>
                <p className="mt-4">
                  <span className="text-3xl font-bold tabular-nums text-gray-950">¥{plan.price}</span>
                  <span className="text-xs text-gray-500"> / 月・税別</span>
                </p>
                <p className="mt-4 flex-1 text-sm leading-6 text-gray-600">{plan.description}</p>
              </div>
            ))}
          </div>
          <div className="mt-8 text-center">
            <Link href="/pricing" className={secondaryButton}>
              プランを詳しく比較する
              <ArrowRight className="h-4 w-4" aria-hidden="true" />
            </Link>
          </div>
        </div>
      </section>

      <section className="bg-white py-20 sm:py-24">
        <div className="mx-auto max-w-3xl px-4">
          <div className="text-center">
            <p className="text-sm font-bold text-brand-600">FAQ</p>
            <h2 className="mt-3 text-3xl font-bold tracking-tight text-gray-950">よくある質問</h2>
          </div>
          <dl className="mt-10 divide-y divide-gray-200 border-y border-gray-200">
            {FAQ.map((item) => (
              <div key={item.q} className="py-6">
                <dt className="text-base font-bold text-gray-950">{item.q}</dt>
                <dd className="mt-3 text-sm leading-7 text-gray-600">{item.a}</dd>
              </div>
            ))}
          </dl>
          <p className="mt-6 text-center text-xs leading-6 text-gray-500">
            掲載範囲・更新日・指標の定義は
            <Link href="/data" className="mx-1 font-semibold text-brand-700 underline">
              データについて
            </Link>
            および
            <Link href="/methodology" className="mx-1 font-semibold text-brand-700 underline">
              指標とデータの考え方
            </Link>
            でご確認いただけます。
          </p>
        </div>
      </section>

      <section className="px-4 pb-20 sm:pb-24">
        <div className="mx-auto max-w-6xl overflow-hidden rounded-3xl bg-brand-600 px-6 py-14 text-center text-white shadow-2xl shadow-brand-600/20 sm:px-12">
          <h2 className="text-3xl font-bold tracking-tight sm:text-4xl">
            介護市場を、探すところから始めよう。
          </h2>
          <p className="mx-auto mt-4 max-w-2xl leading-7 text-brand-100">
            全国のデータを俯瞰し、気になる地域・施設・法人を見つける。
            kaigo-biは無料から始められます。
          </p>
          <div className="mt-8 flex flex-col justify-center gap-3 sm:flex-row">
            <Link
              href="/signup"
              className="inline-flex items-center justify-center gap-2 rounded-xl bg-white px-6 py-3.5 text-sm font-bold text-brand-700 transition hover:-translate-y-0.5 hover:bg-brand-50 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-white"
            >
              無料で始める
              <ArrowRight className="h-4 w-4" aria-hidden="true" />
            </Link>
            <Link
              href="/login"
              className="inline-flex items-center justify-center rounded-xl border border-white/30 px-6 py-3.5 text-sm font-bold text-white transition hover:bg-white/10 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-white"
            >
              ログイン
            </Link>
          </div>
        </div>
      </section>
    </PublicLayout>
  );
}
