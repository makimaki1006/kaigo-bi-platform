// ===================================================
// 指標・データ取扱方針ページ（公開SEOページ）
//
// 参照: claudedocs/SEO_IMPLEMENTATION_BRIEF_20260727.md §5.4
//       claudedocs/SEO_BRIEF_SUPPLEMENT_20260728.md（数値・4層区分・表記の正）
//
// 表記ルール（誇張防止）:
//  - 「経営危険度スコア」等の点数化された表現は使わない → 「要確認シグナル」
//  - DDレーダーは3軸（事業DD/人事DD/コンプラDD）。財務DDは点数化しない
//  - ルールベースの検出であり、検証済みの予測モデルではない旨を明記する
//  - 投資判断・診断・法務・会計助言の代替でない旨を明記する
// ===================================================

import type { Metadata } from "next";
import Link from "next/link";
import PublicLayout from "@/components/public/PublicLayout";
import { buildPublicMetadata } from "@/lib/seo";

export const metadata: Metadata = buildPublicMetadata({
  path: "/methodology",
  title: "指標・データ取扱方針",
  description:
    "kaigo-biが提供する指標の定義、データの層構造、比率の分母、欠損の扱い、財務指標の範囲、要確認シグナルの位置づけを説明します。",
});

export default function MethodologyPage() {
  return (
    <PublicLayout>
      <section className="mx-auto max-w-3xl px-4 py-16">
        <h1 className="text-3xl font-bold tracking-tight text-gray-900 sm:text-4xl">
          指標・データ取扱方針
        </h1>
        <p className="mt-4 text-base leading-relaxed text-gray-600">
          kaigo-biが表示する数値は、単純な集計値だけでなく、正規化・計算・AIによる抽出を経たものが混在しています。
          このページでは、指標がどのように作られているか、どこまでが確からしく、どこからが参考情報かを説明します。
          データそのものの出典や件数については
          <Link href="/data" className="text-brand-700 underline hover:text-brand-800">
            データについて
          </Link>
          をご覧ください。
        </p>

        {/* データの4層区分 */}
        <h2 className="mt-12 text-xl font-semibold text-gray-900">データの4層区分</h2>
        <p className="mt-3 text-sm leading-relaxed text-gray-600">
          kaigo-bi内部のデータは、確からしさの異なる4つの層に分かれています。表示している数値がどの層に属するかによって、
          解釈の仕方が変わります。
        </p>
        <ol className="mt-4 space-y-3 text-sm text-gray-700">
          <li className="rounded-lg border border-gray-200 px-4 py-3">
            <span className="font-semibold text-gray-900">①公表値</span> ―
            介護サービス情報公表システムに事業所・法人が届け出た内容をそのまま収集した原データです。
          </li>
          <li className="rounded-lg border border-gray-200 px-4 py-3">
            <span className="font-semibold text-gray-900">②正規化値</span> ―
            「○」「有」等の表記を1/0のフラグへ変換する、単位や表記のゆれを揃えるなど、
            比較・集計しやすい形へ機械的に変換した値です。
          </li>
          <li className="rounded-lg border border-gray-200 px-4 py-3">
            <span className="font-semibold text-gray-900">③派生値</span> ―
            稼働率（occupancy_rate）や品質スコア（quality_score）など、複数項目から計算して導き出す値です。
            分母となる項目が欠損している場合は算定不能（null）として扱います。
          </li>
          <li className="rounded-lg border border-gray-200 px-4 py-3">
            <span className="font-semibold text-gray-900">④AI抽出値</span> ―
            法人が公開する決算書PDFから、AIが貸借対照表・損益計算書の項目を読み取った値です。
            現時点で数値化が完了しているのは<span className="font-semibold tabular-nums">24施設・52レコード</span>
            のみのパイロット段階で、読み取り誤りが含まれる可能性があります。
          </li>
        </ol>

        {/* 単位の違い */}
        <h2 className="mt-12 text-xl font-semibold text-gray-900">
          施設単位・施設サービス単位・法人単位の違い
        </h2>
        <p className="mt-3 text-sm leading-relaxed text-gray-600">
          kaigo-biの指標は、集計される単位によって意味が異なります。
        </p>
        <ul className="mt-3 list-disc space-y-2 pl-5 text-sm text-gray-600">
          <li>
            <span className="font-medium text-gray-800">施設サービス単位</span>
            ― 1つの拠点が提供する1つのサービス種別ごとの値（定員数、稼働率など）
          </li>
          <li>
            <span className="font-medium text-gray-800">施設単位</span>
            ― 1つの拠点内の複数サービスを合算・集約した値
          </li>
          <li>
            <span className="font-medium text-gray-800">法人単位</span>
            ― 同一法人が運営する複数施設を束ねた値。財務指標（PL/BS）は原則として法人単位で公表されるため、
            法人配下の施設単位の値へ機械的に按分することはできません。
          </li>
        </ul>

        {/* 比率の分母・金額単位 */}
        <h2 className="mt-12 text-xl font-semibold text-gray-900">比率の分母と金額単位</h2>
        <p className="mt-3 text-sm leading-relaxed text-gray-600">
          稼働率などの比率指標は、定員数・在籍者数等の分母となる項目が公表されている場合に限り算定します。
          分母となる項目が欠損している場合、その指標は「算定不能（null）」として扱い、0や平均値で補完しません。
        </p>
        <p className="mt-3 text-sm leading-relaxed text-gray-600">
          金額項目は、決算書PDFの記載単位が法人によって円・千円と混在しています。AI抽出時に単位を判定し、
          円単位に正規化したうえでkaigo-bi内に保持しています。
        </p>

        {/* 財務の決算期・対象スコープ */}
        <h2 className="mt-12 text-xl font-semibold text-gray-900">財務指標の決算期・対象スコープ</h2>
        <p className="mt-3 text-sm leading-relaxed text-gray-600">
          法人単位の財務指標（自己資本比率、売上高等）は、同一施設・同一決算期の損益計算書（PL）と
          貸借対照表（BS）が両方そろって初めて結合します。決算期が異なるPLとBSを組み合わせて指標を算出することはありません。
          そのため、財務PDFの所在を把握していても、PLまたはBSの片方しか数値化できていない、あるいは決算期が一致しない場合は、
          該当する財務指標を表示できません。
        </p>

        {/* 欠損の扱い */}
        <h2 className="mt-12 text-xl font-semibold text-gray-900">欠損時の扱い</h2>
        <p className="mt-3 text-sm leading-relaxed text-gray-600">
          複数の項目を組み合わせて算出するクロス指標（比率・スコア等）は、必要な元データの一部が欠損している場合、
          0として計算するのではなく、算定不能（null）として表示します。「0」と「データがない」を区別することで、
          実態より良く見せる、あるいは悪く見せることを避けています。
        </p>

        {/* ベンチマークの母集団 */}
        <h2 className="mt-12 text-xl font-semibold text-gray-900">ベンチマークの母集団</h2>
        <p className="mt-3 text-sm leading-relaxed text-gray-600">
          地域比較やベンチマーク表示は、同一都道府県・同一サービス種別の事業所群を母集団として算出しています。
          母集団の事業所数が少ない地域・サービス種別では、比較値の統計的な安定性が低くなる点にご留意ください。
        </p>

        {/* 要確認シグナル */}
        <h2 className="mt-12 text-xl font-semibold text-gray-900">
          「要確認シグナル」について
        </h2>
        <p className="mt-3 text-sm leading-relaxed text-gray-600">
          kaigo-biのM&amp;A関連機能では、財務・人事・コンプライアンス等の公開情報からあらかじめ定義した条件に
          該当する項目を「要確認シグナル」として提示します。これは0〜100点のような危険度スコアや、
          統計的に検証された予測モデルによる判定ではありません。あらかじめ設定したルールに該当したかどうかを示す
          「該当シグナル数」「判定できたファクタ N of 4」という形式の情報です。
        </p>
        <p className="mt-3 text-sm leading-relaxed text-gray-600">
          簡易デューデリジェンス（DD）機能のレーダーチャートは、事業DD・人事DD・コンプラDDの3軸で構成しており、
          財務DDは軸に含めず点数化していません。財務は現時点で24施設のみ数値化済みのパイロット段階であるため、
          点数化して信頼性が高いように見せることを避けています。
        </p>

        {/* 免責 */}
        <h2 className="mt-12 text-xl font-semibold text-gray-900">ご利用にあたっての注意</h2>
        <p className="mt-3 text-sm leading-relaxed text-gray-600">
          kaigo-biが提供する分析・シグナル・スコアは、公開情報をもとにした参考情報であり、投資判断、経営診断、
          法務助言、会計・税務助言のいずれの代替にもなりません。M&amp;Aや投資、経営上の重要な意思決定にあたっては、
          必ず専門的なデューデリジェンスおよび専門家への相談を行ってください。
        </p>

        {/* CTA */}
        <div className="mt-14 rounded-2xl border border-gray-200 bg-gray-50 p-6 text-center">
          <p className="text-sm text-gray-700">
            実際の指標画面を無料アカウントでご確認いただけます。
          </p>
          <div className="mt-4 flex flex-col items-center justify-center gap-3 sm:flex-row">
            <Link
              href="/signup"
              className="rounded-lg bg-brand-600 px-5 py-2.5 text-sm font-semibold text-white transition-colors hover:bg-brand-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500 focus-visible:ring-offset-2"
            >
              無料で確認する
            </Link>
            <Link
              href="/data"
              className="rounded-lg border border-brand-600 px-5 py-2.5 text-sm font-semibold text-brand-700 transition-colors hover:bg-brand-50 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500 focus-visible:ring-offset-2"
            >
              データについて見る
            </Link>
          </div>
        </div>
      </section>
    </PublicLayout>
  );
}
