// ===================================================
// 特定商取引法に基づく表記（公開ページ）
//
// 注意:
//  - 本ページは実装（Stripe Checkout / カスタマーポータル、料金ページの
//    プラン定義）に基づいて作成した「たたき台」。施行前に専門家のレビューが必要。
//  - 事業者名・所在地・連絡先・電話番号は src/lib/legal.ts の OPERATOR にある。
//    特商法は「請求があれば遅滞なく開示」する運用が認められる項目があるが、
//    通信販売の広告としては原則すべて表示が必要。公開前に必ず埋めること。
//  - 未設定のうちは noindex かつ sitemap 対象外（buildLegalMetadata / LEGAL_PATHS）。
// ===================================================

import type { Metadata } from "next";
import Link from "next/link";
import PublicLayout from "@/components/public/PublicLayout";
import { buildLegalMetadata } from "@/lib/seo";
import { OPERATOR, LAST_UPDATED, showsAddressAndPhone } from "@/lib/legal";

export const metadata: Metadata = buildLegalMetadata({
  path: "/legal",
  title: "特定商取引法に基づく表記",
  description:
    "kaigo-bi の販売事業者、料金、支払方法、提供時期、解約・返金の条件を特定商取引法に基づき表示します。",
});

/**
 * 住所・電話番号の行。
 *
 * 消費者庁「通信販売広告Q&A」Q15・Q17 により、請求があれば遅滞なく提供する旨を
 * 表示し実際にその体制を講じていれば、住所・電話番号の表示は省略できる。
 * src/lib/legal.ts の DISCLOSURE_MODE で切り替える。
 */
const CONTACT_ROWS: { label: string; value: React.ReactNode }[] = showsAddressAndPhone()
  ? [
      { label: "所在地", value: OPERATOR.address },
      { label: "電話番号", value: OPERATOR.phone },
    ]
  : [
      {
        label: "所在地・電話番号",
        value: (
          <>
            <strong className="text-gray-900">
              ご請求をいただいた場合、遅滞なく電子メールにて開示します。
            </strong>
            <span className="mt-1 block">
              下記のメールアドレス宛に、その旨をご連絡ください。
              お申し込みのご判断に先立ち、十分な時間的余裕をもってご提供します。
            </span>
          </>
        ),
      },
    ];

/** 表示項目 */
const ROWS: { label: string; value: React.ReactNode }[] = [
  { label: "販売事業者", value: OPERATOR.name },
  { label: "運営責任者", value: OPERATOR.representative },
  ...CONTACT_ROWS,
  {
    label: "メールアドレス",
    value: (
      <>
        {OPERATOR.contactEmail}
        {!showsAddressAndPhone() && (
          <span className="mt-1 block text-xs text-gray-500">
            所在地・電話番号のご請求も、こちらで承ります。
          </span>
        )}
      </>
    ),
  },
  { label: "ホームページ", value: "https://kaigo-bi.onrender.com" },
  {
    label: "販売価格",
    value: (
      <>
        <table className="w-full text-sm">
          <tbody>
            {[
              ["フリー", "0円/月"],
              ["スタンダード", "9,800円/月"],
              ["プロ", "29,800円/月"],
              ["M&A", "49,800円/月"],
            ].map(([plan, price]) => (
              <tr key={plan}>
                <td className="py-1 pr-6 text-gray-700">{plan}</td>
                <td className="py-1 tabular-nums text-gray-900">{price}</td>
              </tr>
            ))}
          </tbody>
        </table>
        <span className="mt-2 block text-xs text-gray-500">
          いずれも税別表示です。別途消費税を申し受けます。
          最新の価格は
          <Link href="/pricing" className="text-brand-700 underline hover:text-brand-800">
            料金ページ
          </Link>
          をご確認ください。
        </span>
      </>
    ),
  },
  {
    label: "商品代金以外の必要料金",
    value: (
      <>
        消費税、および本サービスの利用に必要な通信料金（インターネット接続料等）は
        利用者のご負担となります。当社が別途手数料を申し受けることはありません。
      </>
    ),
  },
  {
    label: "支払方法",
    value: (
      <>
        クレジットカード決済（Stripe, Inc. の決済システムを利用）
        <span className="mt-1 block text-xs text-gray-500">
          カード情報は Stripe が直接取得し、当社は保持しません。
        </span>
      </>
    ),
  },
  {
    label: "支払時期",
    value: (
      <>
        初回はお申し込み手続きの完了時に決済されます。
        以後は毎月、初回決済日に応当する日に自動で決済されます。
      </>
    ),
  },
  {
    label: "サービスの提供時期",
    value: (
      <>
        決済の完了後、ただちにご利用いただけます。
        機能の解放に時間を要する場合でも、遅くとも決済完了から24時間以内に提供します。
      </>
    ),
  },
  {
    label: "解約",
    value: (
      <>
        ログイン後の「現在の契約」画面（Stripe カスタマーポータル）からいつでも手続きできます。
        解約後も、当該課金期間の末日までご利用いただけます。
        次回更新日の前日までに手続きされない場合、自動更新されます。
      </>
    ),
  },
  {
    label: "返品・返金について",
    value: (
      <>
        本サービスはデジタルコンテンツの提供であり、
        <strong className="text-gray-900">
          お客様都合による中途解約に伴う日割り返金は行いません。
        </strong>
        <span className="mt-1 block">
          当社の責めに帰すべき事由によりサービスを提供できなかった場合、
          および当社がサービスの提供を終了する場合は、
          未経過期間に相当する料金を日割りで返金します。
        </span>
        <span className="mt-1 block text-xs text-gray-500">
          なお、通信販売にはクーリング・オフ制度の適用はありません。
        </span>
      </>
    ),
  },
  {
    label: "動作環境",
    value: (
      <>
        最新版の Google Chrome、Microsoft Edge、Safari、Firefox のいずれか。
        JavaScript および localStorage が有効である必要があります。
      </>
    ),
  },
];

export default function LegalPage() {
  return (
    <PublicLayout>
      <section className="mx-auto max-w-3xl px-4 py-16">
        <h1 className="text-3xl font-bold tracking-tight text-gray-900 sm:text-4xl">
          特定商取引法に基づく表記
        </h1>
        <p className="mt-3 text-sm text-gray-500">最終改定日: {LAST_UPDATED}</p>

        <dl className="mt-10 divide-y divide-gray-200 border-t border-gray-200">
          {ROWS.map((r) => (
            <div key={r.label} className="grid grid-cols-1 gap-1 py-5 sm:grid-cols-3 sm:gap-6">
              <dt className="text-sm font-semibold text-gray-900">{r.label}</dt>
              <dd className="text-sm leading-relaxed text-gray-700 sm:col-span-2">
                {r.value}
              </dd>
            </div>
          ))}
        </dl>

        <div className="mt-14 rounded-lg border border-gray-200 bg-gray-50 px-5 py-4">
          <p className="text-sm text-gray-600">
            関連ページ:{" "}
            <Link href="/terms" className="text-brand-700 underline hover:text-brand-800">
              利用規約
            </Link>
            {" / "}
            <Link href="/privacy" className="text-brand-700 underline hover:text-brand-800">
              プライバシーポリシー
            </Link>
            {" / "}
            <Link href="/pricing" className="text-brand-700 underline hover:text-brand-800">
              料金
            </Link>
          </p>
        </div>
      </section>
    </PublicLayout>
  );
}
