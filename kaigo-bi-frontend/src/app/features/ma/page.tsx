// ===================================================
// 公開SEOページ: M&A支援
//
// 対象: M&A仲介、買い手、事業開発
// 表記ルール（SEO_BRIEF_SUPPLEMENT準拠、必須の明記事項）:
//   - 財務数値化は現在24施設・52レコードのパイロット段階のみ（全国対応と誤認させない）
//   - 「経営危険度」「売却期待度」等の点数化・断定表現は使わない → 「要確認シグナル」
//   - 「要確認シグナル」は経営状態・売却意向を断定するものではない旨を明記
//   - 専門的デューデリジェンスの代替ではない旨を明記
// ===================================================

import type { Metadata } from "next";
import Link from "next/link";
import PublicLayout from "@/components/public/PublicLayout";
import { buildPublicMetadata } from "@/lib/seo";

export const metadata: Metadata = buildPublicMetadata({
  path: "/features/ma",
  title: "M&A支援｜介護業界の候補探索・法人整理・簡易DD",
  description:
    "全国223,103の施設・サービスレコードから介護業界のM&A候補を探索し、法人情報整理と簡易デューデリジェンスを支援します。財務数値化は24施設のパイロット段階です。",
});

export default function MaPage() {
  return (
    <PublicLayout>
      {/* ヒーロー */}
      <section className="mx-auto max-w-3xl px-4 pb-4 pt-16 text-center sm:pt-20">
        <h1 className="text-3xl font-bold tracking-tight text-gray-900 sm:text-4xl">
          M&amp;A支援
          <br className="hidden sm:block" />
          候補探索・法人情報整理・簡易デューデリジェンス
        </h1>
        <p className="mx-auto mt-6 max-w-2xl text-base leading-relaxed text-gray-600">
          全国223,103の施設・サービスレコード（約19万事業所、法人ユニーク数68,563）の公開情報から、
          介護業界のM&amp;A候補探索と初期スクリーニングを支援します。
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
            <span>介護業界のM&amp;A候補となる法人・施設を、地域やサービス種別を横断して幅広く探索する手段が限られる</span>
          </li>
          <li className="flex gap-2">
            <span aria-hidden="true">・</span>
            <span>候補ごとの法人情報（施設数、サービス構成、地域展開等）の整理に時間がかかる</span>
          </li>
          <li className="flex gap-2">
            <span aria-hidden="true">・</span>
            <span>本格的なデューデリジェンスに進む前の一次スクリーニングの精度・効率を上げたい</span>
          </li>
        </ul>
      </section>

      {/* 2. kaigo-biで可能になること */}
      <section className="mx-auto max-w-3xl px-4 py-10">
        <h2 className="text-xl font-bold text-gray-900 sm:text-2xl">
          kaigo-biで可能になること
        </h2>
        <p className="mt-4 text-sm leading-relaxed text-gray-700 sm:text-base">
          全国の施設・サービスの公開情報を法人単位で整理し、候補探索と法人情報の整理、
          簡易デューデリジェンスによる一次スクリーニングを支援します。
        </p>
        <div className="mt-6 grid gap-4 sm:grid-cols-2">
          <div className="rounded-xl border border-gray-200 p-5">
            <h3 className="text-sm font-semibold text-gray-900">候補探索・法人整理</h3>
            <p className="mt-2 text-sm leading-relaxed text-gray-600">
              地域・サービス種別・施設数等の条件で候補法人を探索し、法人単位の情報を整理して確認できます。
            </p>
          </div>
          <div className="rounded-xl border border-gray-200 p-5">
            <h3 className="text-sm font-semibold text-gray-900">簡易デューデリジェンス</h3>
            <p className="mt-2 text-sm leading-relaxed text-gray-600">
              事業DD・人事DD・コンプライアンスDDの3軸で、公表情報に基づく一次スクリーニングを行えます。
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
            <span>全国223,103の施設・サービスレコード（約19万事業所、法人ユニーク数68,563）</span>
          </li>
          <li className="flex gap-2">
            <span aria-hidden="true">・</span>
            <span>M&amp;Aスクリーニング、簡易デューデリジェンス（事業DD／人事DD／コンプラDDの3軸）</span>
          </li>
          <li className="flex gap-2">
            <span aria-hidden="true">・</span>
            <span>PMI（統合後）シナジー分析</span>
          </li>
          <li className="flex gap-2">
            <span aria-hidden="true">・</span>
            <span>「要確認シグナル」による初期スクリーニング補助（該当ファクタ数を提示。点数化はしていません）</span>
          </li>
        </ul>
        <div className="mt-5 rounded-xl bg-amber-50 p-4 text-sm leading-relaxed text-amber-900">
          <p>
            <strong>財務データについて:</strong>{" "}
            決算PDFのURL取得は全国規模で進行中ですが、PDF内容を数値として構造化するAI抽出は、
            現時点で24施設・52レコードのパイロット段階です。全ての候補で財務数値をご覧いただけるわけではありません。
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
            <span>無料登録し、ダッシュボードで全国の市場・法人分布を把握する</span>
          </li>
          <li className="flex gap-3">
            <span className="flex h-6 w-6 flex-shrink-0 items-center justify-center rounded-full bg-brand-600 text-xs font-semibold text-white">
              2
            </span>
            <span>M&amp;Aプランへアップグレードし、地域・サービス種別等の条件で候補を探索する</span>
          </li>
          <li className="flex gap-3">
            <span className="flex h-6 w-6 flex-shrink-0 items-center justify-center rounded-full bg-brand-600 text-xs font-semibold text-white">
              3
            </span>
            <span>候補法人について、法人情報・簡易DD・要確認シグナルを確認する</span>
          </li>
          <li className="flex gap-3">
            <span className="flex h-6 w-6 flex-shrink-0 items-center justify-center rounded-full bg-brand-600 text-xs font-semibold text-white">
              4
            </span>
            <span>絞り込んだ候補について、専門家による本格的なデューデリジェンスへ進む</span>
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
              財務諸表の数値化（AI抽出）は現在24施設・52レコードのみで、パイロット段階です。
              決算PDFのリンク自体は全国規模で整備が進んでいますが、リンクがあることと数値が見られることは異なります。
            </span>
          </li>
          <li className="flex gap-2">
            <span aria-hidden="true">・</span>
            <span>
              「要確認シグナル」は、公表情報から確認を推奨するファクタの該当有無を示す参考情報です。
              経営状態が悪化している、または売却意向があることを断定・予測するものではありません。
            </span>
          </li>
          <li className="flex gap-2">
            <span aria-hidden="true">・</span>
            <span>
              簡易デューデリジェンス（事業DD／人事DD／コンプラDD）は公表情報に基づく一次スクリーニングであり、
              専門家（公認会計士、弁護士等）による本格的なデューデリジェンスの代替ではありません。
              最終的な投資判断・法務・会計上の意思決定には専門家の助言を別途受けてください。
            </span>
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
            M&amp;A候補探索はM&amp;Aプランから
          </h2>
          <p className="mt-3 text-sm leading-relaxed text-gray-600">
            M&amp;Aスクリーニング・簡易DD・PMI分析はM&amp;Aプラン（月額49,800円・税別、CSV月10,000行）でご利用いただけます。
            まずは料金プランをご確認ください。
          </p>
          <div className="mt-6 flex flex-col items-center justify-center gap-3 sm:flex-row">
            <Link
              href="/pricing"
              className="rounded-lg bg-brand-600 px-6 py-3 text-sm font-semibold text-white transition-colors hover:bg-brand-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500 focus-visible:ring-offset-2"
            >
              M&amp;Aプランを見る
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
