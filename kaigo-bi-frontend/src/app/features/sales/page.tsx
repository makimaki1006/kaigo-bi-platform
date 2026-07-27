// ===================================================
// 公開SEOページ: 介護業界向け 営業支援
//
// 対象: 介護業界へ営業する企業
// 表記ルール（SEO_BRIEF_SUPPLEMENT準拠）:
//   - 「223,103施設・サービスレコード（約19万事業所）」と書く
//   - CSV件数はプラン条件（pricing/page.tsx・SEO_BRIEF_SUPPLEMENT）と一致させる
//     Pro: 月3,000行 / M&A: 月10,000行
// ===================================================

import type { Metadata } from "next";
import Link from "next/link";
import PublicLayout from "@/components/public/PublicLayout";
import { buildPublicMetadata } from "@/lib/seo";

export const metadata: Metadata = buildPublicMetadata({
  path: "/features/sales",
  title: "介護業界向け営業支援｜市場把握・条件検索・リスト作成",
  description:
    "全国223,103の施設・サービスレコード（約19万事業所）の公開情報から、条件検索で対象施設・法人を絞り込み、営業リストをCSVで作成できます。",
});

export default function SalesPage() {
  return (
    <PublicLayout>
      {/* ヒーロー */}
      <section className="mx-auto max-w-3xl px-4 pb-4 pt-16 text-center sm:pt-20">
        <h1 className="text-3xl font-bold tracking-tight text-gray-900 sm:text-4xl">
          介護業界向け営業支援
          <br className="hidden sm:block" />
          市場把握と条件検索・リスト作成
        </h1>
        <p className="mx-auto mt-6 max-w-2xl text-base leading-relaxed text-gray-600">
          全国223,103の施設・サービスレコード（約19万事業所）の公開情報から、
          地域やサービス種別などの条件で対象を絞り込み、営業リストを作成できます。
        </p>
      </section>

      {/* 1. 対象顧客の課題 */}
      <section className="mx-auto max-w-3xl px-4 py-10">
        <h2 className="text-xl font-bold text-gray-900 sm:text-2xl">
          こんな課題はありませんか
        </h2>
        <ul className="mt-5 space-y-3 text-sm leading-relaxed text-gray-700 sm:text-base">
          <li className="flex gap-2">
            <span aria-hidden="true">・</span>
            <span>介護業界向けにシステム・人材・金融等のサービスを提案したいが、対象施設・法人を効率的に絞り込めない</span>
          </li>
          <li className="flex gap-2">
            <span aria-hidden="true">・</span>
            <span>営業リストの整備を自社で行うと、収集・名寄せ・更新のコストが大きい</span>
          </li>
          <li className="flex gap-2">
            <span aria-hidden="true">・</span>
            <span>地域やサービス種別ごとの市場規模を把握したうえで営業活動の優先順位をつけたい</span>
          </li>
        </ul>
      </section>

      {/* 2. kaigo-biで可能になること */}
      <section className="mx-auto max-w-3xl px-4 py-10">
        <h2 className="text-xl font-bold text-gray-900 sm:text-2xl">
          kaigo-biで可能になること
        </h2>
        <p className="mt-4 text-sm leading-relaxed text-gray-700 sm:text-base">
          全国の施設・サービスの公開情報を地域・サービス種別・法人単位で整理し、
          条件を指定して対象を絞り込んだうえで、CSV形式の営業リストとして出力できます。
        </p>
        <div className="mt-6 grid gap-4 sm:grid-cols-2">
          <div className="rounded-xl border border-gray-200 p-5">
            <h3 className="text-sm font-semibold text-gray-900">市場把握</h3>
            <p className="mt-2 text-sm leading-relaxed text-gray-600">
              地域・サービス種別ごとの施設数や法人数を確認し、営業対象市場の規模感をつかめます。
            </p>
          </div>
          <div className="rounded-xl border border-gray-200 p-5">
            <h3 className="text-sm font-semibold text-gray-900">条件検索とリスト作成</h3>
            <p className="mt-2 text-sm leading-relaxed text-gray-600">
              地域・サービス種別・規模等の条件で施設・法人を絞り込み、CSVでダウンロードできます。
            </p>
          </div>
        </div>
      </section>

      {/* 3. 使用するデータと機能 */}
      <section className="mx-auto max-w-3xl px-4 py-10">
        <h2 className="text-xl font-bold text-gray-900 sm:text-2xl">
          使用するデータと機能
        </h2>
        <ul className="mt-5 space-y-3 text-sm leading-relaxed text-gray-700 sm:text-base">
          <li className="flex gap-2">
            <span aria-hidden="true">・</span>
            <span>全国223,103の施設・サービスレコード（約19万事業所、法人ユニーク数68,563）の公開情報</span>
          </li>
          <li className="flex gap-2">
            <span aria-hidden="true">・</span>
            <span>都道府県・市区町村単位のドリルダウン分析（スタンダードプラン以上）</span>
          </li>
          <li className="flex gap-2">
            <span aria-hidden="true">・</span>
            <span>条件指定によるリスト作成（プロプラン以上）</span>
          </li>
          <li className="flex gap-2">
            <span aria-hidden="true">・</span>
            <span>電話番号・法人情報つきCSVダウンロード</span>
          </li>
        </ul>
        <div className="mt-5 rounded-xl bg-brand-50 p-4 text-sm leading-relaxed text-brand-800">
          <p>
            CSVダウンロードの上限は月間行数で管理されています。プロプランは月3,000行、M&amp;Aプランは月10,000行までダウンロードできます。
          </p>
        </div>
      </section>

      {/* 4. 利用フロー */}
      <section className="mx-auto max-w-3xl px-4 py-10">
        <h2 className="text-xl font-bold text-gray-900 sm:text-2xl">利用フロー</h2>
        <ol className="mt-5 space-y-4 text-sm leading-relaxed text-gray-700 sm:text-base">
          <li className="flex gap-3">
            <span className="flex h-6 w-6 flex-shrink-0 items-center justify-center rounded-full bg-brand-600 text-xs font-semibold text-white">
              1
            </span>
            <span>無料登録し、ダッシュボードで全国の市場感を把握する</span>
          </li>
          <li className="flex gap-3">
            <span className="flex h-6 w-6 flex-shrink-0 items-center justify-center rounded-full bg-brand-600 text-xs font-semibold text-white">
              2
            </span>
            <span>プロプランへアップグレードし、地域・サービス種別等の条件で絞り込む</span>
          </li>
          <li className="flex gap-3">
            <span className="flex h-6 w-6 flex-shrink-0 items-center justify-center rounded-full bg-brand-600 text-xs font-semibold text-white">
              3
            </span>
            <span>条件に合致する施設・法人リストをCSVでダウンロードする（月間上限あり）</span>
          </li>
          <li className="flex gap-3">
            <span className="flex h-6 w-6 flex-shrink-0 items-center justify-center rounded-full bg-brand-600 text-xs font-semibold text-white">
              4
            </span>
            <span>営業活動やターゲティングに活用する</span>
          </li>
        </ol>
      </section>

      {/* 5. 制約・注意点 */}
      <section className="mx-auto max-w-3xl px-4 py-10">
        <h2 className="text-xl font-bold text-gray-900 sm:text-2xl">制約・注意点</h2>
        <ul className="mt-5 space-y-3 text-sm leading-relaxed text-gray-700 sm:text-base">
          <li className="flex gap-2">
            <span aria-hidden="true">・</span>
            <span>
              データは公開情報に基づきます。電話番号・法人情報等は公表情報に由来するため、
              最新の連絡可否や現況を保証するものではありません。営業活動時は各自で最新性をご確認ください。
            </span>
          </li>
          <li className="flex gap-2">
            <span aria-hidden="true">・</span>
            <span>CSVダウンロードはプランごとに月間の行数上限があります（プロ：月3,000行、M&amp;A：月10,000行）。</span>
          </li>
          <li className="flex gap-2">
            <span aria-hidden="true">・</span>
            <span>223,103件は施設×サービス単位のレコード数であり、ユニーク事業所数（約19万事業所）とは異なります。</span>
          </li>
        </ul>
      </section>

      {/* 6. 料金/登録CTA */}
      <section className="mx-auto max-w-3xl px-4 py-14">
        <div className="rounded-2xl border border-gray-200 bg-gray-50 p-8 text-center">
          <h2 className="text-xl font-bold text-gray-900 sm:text-2xl">
            条件検索・リスト作成はプロプランから
          </h2>
          <p className="mt-3 text-sm leading-relaxed text-gray-600">
            条件検索とCSVリスト作成はプロプラン（月額29,800円・税別、CSV月3,000行）からご利用いただけます。
            まずは料金プランをご確認ください。
          </p>
          <div className="mt-6 flex flex-col items-center justify-center gap-3 sm:flex-row">
            <Link
              href="/pricing"
              className="rounded-lg bg-brand-600 px-6 py-3 text-sm font-semibold text-white transition-colors hover:bg-brand-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500 focus-visible:ring-offset-2"
            >
              料金を見る
            </Link>
            <Link
              href="/signup"
              className="rounded-lg border border-brand-600 px-6 py-3 text-sm font-semibold text-brand-700 transition-colors hover:bg-brand-50 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500 focus-visible:ring-offset-2"
            >
              無料で始める
            </Link>
          </div>
        </div>
      </section>
    </PublicLayout>
  );
}
