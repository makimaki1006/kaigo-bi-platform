// ===================================================
// 公開SEOページ: 介護事業者向け 経営支援
//
// 対象: 介護事業者、経営企画、施設責任者
// 表記ルール（SEO_BRIEF_SUPPLEMENT準拠）:
//   - 「223,103施設・サービスレコード（約19万事業所）」と書く
//   - 「マイ施設登録」は未実装のため「予定」としてのみ言及する
// ===================================================

import type { Metadata } from "next";
import Link from "next/link";
import PublicLayout from "@/components/public/PublicLayout";
import { buildPublicMetadata } from "@/lib/seo";

export const metadata: Metadata = buildPublicMetadata({
  path: "/features/management",
  title: "介護事業者の経営支援｜地域比較・人員/品質/稼働の把握",
  description:
    "全国223,103の施設・サービスレコード（約19万事業所）の公開情報をもとに、地域比較や人員配置・品質評価・稼働状況を確認できる経営支援機能です。",
});

export default function ManagementPage() {
  return (
    <PublicLayout>
      {/* ヒーロー */}
      <section className="mx-auto max-w-3xl px-4 pb-4 pt-16 text-center sm:pt-20">
        <h1 className="text-3xl font-bold tracking-tight text-gray-900 sm:text-4xl">
          介護事業所の経営支援
          <br className="hidden sm:block" />
          地域比較と人員・品質・稼働の把握
        </h1>
        <p className="mx-auto mt-6 max-w-2xl text-base leading-relaxed text-gray-600">
          全国223,103の施設・サービスレコード（約19万事業所）の公開情報をもとに、
          自施設の立ち位置を地域単位・サービス種別単位で客観的に確認できます。
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
            <span>近隣エリアの競合施設が何件あり、どのようなサービス構成なのか把握しづらい</span>
          </li>
          <li className="flex gap-2">
            <span aria-hidden="true">・</span>
            <span>自施設の人員配置や稼働率が、地域や同種サービスと比べて高いのか低いのか判断しづらい</span>
          </li>
          <li className="flex gap-2">
            <span aria-hidden="true">・</span>
            <span>品質評価や第三者評価の公表情報が分散していて、横並びで比較する手段が限られる</span>
          </li>
          <li className="flex gap-2">
            <span aria-hidden="true">・</span>
            <span>経営会議や事業計画のために、根拠となる地域データをその都度自分で集計している</span>
          </li>
        </ul>
      </section>

      {/* 2. kaigo-biで可能になること */}
      <section className="mx-auto max-w-3xl px-4 py-10">
        <h2 className="text-xl font-bold text-gray-900 sm:text-2xl">
          kaigo-biで可能になること
        </h2>
        <p className="mt-4 text-sm leading-relaxed text-gray-700 sm:text-base">
          公開情報を都道府県・市区町村・サービス種別ごとに整理し、BIダッシュボード上で地域比較ができます。
          自施設を直接登録して自動比較する機能ではなく、条件を選んで地域・サービス単位の集計値を確認する形で、
          経営会議や事業計画の裏付けとして活用できます。
        </p>
        <div className="mt-6 grid gap-4 sm:grid-cols-2">
          <div className="rounded-xl border border-gray-200 p-5">
            <h3 className="text-sm font-semibold text-gray-900">地域比較</h3>
            <p className="mt-2 text-sm leading-relaxed text-gray-600">
              都道府県・市区町村単位で施設数やサービス種別の分布を確認し、自地域の競合状況を把握できます。
            </p>
          </div>
          <div className="rounded-xl border border-gray-200 p-5">
            <h3 className="text-sm font-semibold text-gray-900">人員・稼働・品質の確認</h3>
            <p className="mt-2 text-sm leading-relaxed text-gray-600">
              公表情報に基づく人員配置状況、稼働率、品質評価の指標を、地域・サービス種別で比較できます。
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
            <span>全国223,103の施設・サービスレコード（約19万事業所）の公開情報</span>
          </li>
          <li className="flex gap-2">
            <span aria-hidden="true">・</span>
            <span>都道府県・市区町村単位のドリルダウン分析</span>
          </li>
          <li className="flex gap-2">
            <span aria-hidden="true">・</span>
            <span>人材・給与・品質・成長の各種分析</span>
          </li>
          <li className="flex gap-2">
            <span aria-hidden="true">・</span>
            <span>外部統計（人口動態・有効求人倍率等）との突き合わせ</span>
          </li>
        </ul>
        <div className="mt-5 rounded-xl bg-brand-50 p-4 text-sm leading-relaxed text-brand-800">
          <p>
            <strong>予定機能:</strong>{" "}
            「マイ施設登録」（自施設を登録し、周辺施設と自動的に比較する機能）は現時点では未実装です。
            現状は条件検索と地域別集計を通じた比較でご利用いただけます。
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
            <span>無料登録し、ダッシュボードで全国サマリーを確認する</span>
          </li>
          <li className="flex gap-3">
            <span className="flex h-6 w-6 flex-shrink-0 items-center justify-center rounded-full bg-brand-600 text-xs font-semibold text-white">
              2
            </span>
            <span>比較したい地域・サービス種別を選択する</span>
          </li>
          <li className="flex gap-3">
            <span className="flex h-6 w-6 flex-shrink-0 items-center justify-center rounded-full bg-brand-600 text-xs font-semibold text-white">
              3
            </span>
            <span>人員配置・稼働率・品質評価の指標を地域単位で比較する</span>
          </li>
          <li className="flex gap-3">
            <span className="flex h-6 w-6 flex-shrink-0 items-center justify-center rounded-full bg-brand-600 text-xs font-semibold text-white">
              4
            </span>
            <span>より詳細な分析が必要な場合は、スタンダードプランへアップグレードする</span>
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
              データは介護情報公表システム等の公開情報に基づきます。223,103件は施設×サービス単位のレコード数であり、
              ユニーク事業所数（約19万事業所）とは異なります。
            </span>
          </li>
          <li className="flex gap-2">
            <span aria-hidden="true">・</span>
            <span>「マイ施設登録」機能は未実装です（予定機能）。</span>
          </li>
          <li className="flex gap-2">
            <span aria-hidden="true">・</span>
            <span>
              人員・稼働・品質の各指標は公表情報に基づく参考値です。経営判断の唯一の根拠とせず、
              自施設の実態と照らし合わせてご利用ください。
            </span>
          </li>
        </ul>
      </section>

      {/* 6. 料金/登録CTA */}
      <section className="mx-auto max-w-3xl px-4 py-14">
        <div className="rounded-2xl border border-gray-200 bg-gray-50 p-8 text-center">
          <h2 className="text-xl font-bold text-gray-900 sm:text-2xl">
            経営支援機能を無料で試す
          </h2>
          <p className="mt-3 text-sm leading-relaxed text-gray-600">
            まずは無料登録で全国サマリーをご覧ください。BIダッシュボード全機能はスタンダードプラン（月額9,800円・税別）でご利用いただけます。
          </p>
          <div className="mt-6 flex flex-col items-center justify-center gap-3 sm:flex-row">
            <Link
              href="/signup"
              className="rounded-lg bg-brand-600 px-6 py-3 text-sm font-semibold text-white transition-colors hover:bg-brand-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500 focus-visible:ring-offset-2"
            >
              無料で試す
            </Link>
            <Link
              href="/pricing"
              className="rounded-lg border border-brand-600 px-6 py-3 text-sm font-semibold text-brand-700 transition-colors hover:bg-brand-50 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500 focus-visible:ring-offset-2"
            >
              料金プランを見る
            </Link>
          </div>
        </div>
      </section>
    </PublicLayout>
  );
}
