// ===================================================
// プライバシーポリシー（公開ページ）
//
// 注意:
//  - 記載内容は実装（users / sessions / audit_logs / export_logs /
//    password_reset_tokens テーブル、Stripe、Resend、Turso、Render、
//    OpenStreetMap、Google Fonts）の実態に基づく。
//  - 施行前に弁護士等の専門家によるレビューが必要。
//  - 事業者名・連絡先は src/lib/legal.ts の OPERATOR を差し替えること。
//  - 実装を変更して収集項目や外部送信先が変わったら、本ページも更新すること。
// ===================================================

import type { Metadata } from "next";
import Link from "next/link";
import PublicLayout from "@/components/public/PublicLayout";
import { buildLegalMetadata } from "@/lib/seo";
import { OPERATOR, LAST_UPDATED, SERVICE_NAME } from "@/lib/legal";

export const metadata: Metadata = buildLegalMetadata({
  path: "/privacy",
  title: "プライバシーポリシー",
  description:
    "kaigo-bi における個人情報の取得項目、利用目的、第三者提供、外部サービスへの提供、保存期間、開示等の請求方法を定めます。",
});

/** 取得する情報（実装のテーブル定義に対応） */
const COLLECTED: { category: string; items: string; purpose: string }[] = [
  {
    category: "アカウント情報",
    items: "メールアドレス、氏名、パスワード（ハッシュ化して保存）、権限、契約プラン、有効期限",
    purpose: "本人確認、認証、機能の提供範囲の判定",
  },
  {
    category: "セッション情報",
    items: "セッショントークン（ハッシュ化して保存）、有効期限",
    purpose: "ログイン状態の維持",
  },
  {
    category: "操作ログ",
    items: "操作の種別、操作内容、IPアドレス、日時",
    purpose: "不正利用の検知、障害調査、セキュリティの確保",
  },
  {
    category: "出力ログ",
    items: "出力した行数、日時",
    purpose: "プランごとの利用上限の管理",
  },
  {
    category: "パスワード再設定情報",
    items: "再設定用トークン（ハッシュ化して保存）、有効期限、使用日時",
    purpose: "パスワード再設定手続きの実施",
  },
  {
    category: "決済関連の識別子",
    items: "Stripe の顧客ID、サブスクリプションID",
    purpose: "有料プランの課金状態の管理",
  },
];

/** 外部サービス */
const THIRD_PARTIES: { name: string; role: string; data: string }[] = [
  {
    name: "Stripe, Inc.（米国）",
    role: "決済処理",
    data: "メールアドレス、決済に必要な情報（クレジットカード情報は Stripe が直接取得し、当社は保持しません）",
  },
  {
    name: "Turso（ChiselStrike, Inc.）",
    role: "データベース基盤",
    data: "上記「取得する情報」に記載のすべて",
  },
  {
    name: "Render Services, Inc.（米国）",
    role: "アプリケーションの実行基盤",
    data: "アクセスログ、IPアドレス",
  },
  {
    name: "Resend, Inc.（米国）",
    role: "メール送信",
    data: "メールアドレス、送信するメールの本文",
  },
  {
    name: "OpenStreetMap Foundation",
    role: "地図タイルの配信",
    data: "地図表示時のIPアドレス、表示範囲（施設の個別情報は送信しません）",
  },
  {
    name: "Google LLC（Google Fonts）",
    role: "Webフォントの配信",
    data: "フォント読み込み時のIPアドレス、ブラウザ情報",
  },
];

export default function PrivacyPage() {
  return (
    <PublicLayout>
      <section className="mx-auto max-w-3xl px-4 py-16">
        <h1 className="text-3xl font-bold tracking-tight text-gray-900 sm:text-4xl">
          プライバシーポリシー
        </h1>
        <p className="mt-3 text-sm text-gray-500">最終改定日: {LAST_UPDATED}</p>

        <p className="mt-6 text-sm leading-relaxed text-gray-700">
          {OPERATOR.name}（以下「当社」）は、{SERVICE_NAME}
          （以下「本サービス」）の提供にあたり取得する個人情報を、
          個人情報の保護に関する法律その他の法令を遵守して取り扱います。
        </p>

        {/* 1 */}
        <h2 className="mt-12 text-lg font-semibold text-gray-900">1. 取得する情報と利用目的</h2>
        <p className="mt-3 text-sm leading-relaxed text-gray-700">
          本サービスは、次の情報を取得します。記載のない情報は取得しません。
        </p>
        <div className="mt-4 overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-200 bg-gray-50/60">
                <th className="px-3 py-2 text-left text-xs font-semibold text-gray-500">区分</th>
                <th className="px-3 py-2 text-left text-xs font-semibold text-gray-500">項目</th>
                <th className="px-3 py-2 text-left text-xs font-semibold text-gray-500">目的</th>
              </tr>
            </thead>
            <tbody>
              {COLLECTED.map((c, i) => (
                <tr key={c.category} className={i % 2 === 0 ? "bg-white" : "bg-gray-50/40"}>
                  <td className="px-3 py-2.5 font-medium text-gray-800">{c.category}</td>
                  <td className="px-3 py-2.5 text-gray-700">{c.items}</td>
                  <td className="px-3 py-2.5 text-gray-600">{c.purpose}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <p className="mt-4 text-sm leading-relaxed text-gray-700">
          本サービスは、行動ターゲティング広告のための情報収集は行いません。
          アクセス解析ツールも導入していません。
        </p>

        {/* 2 */}
        <h2 className="mt-12 text-lg font-semibold text-gray-900">2. ブラウザに保存する情報</h2>
        <p className="mt-3 text-sm leading-relaxed text-gray-700">
          本サービスは、ログイン状態を保持するため、認証トークンを利用者のブラウザの
          localStorage に保存します。広告目的の Cookie は使用しません。
          ブラウザの設定で当該データを削除した場合、再度ログインが必要になります。
        </p>

        {/* 3 */}
        <h2 className="mt-12 text-lg font-semibold text-gray-900">3. 第三者提供</h2>
        <p className="mt-3 text-sm leading-relaxed text-gray-700">
          当社は、次の場合を除き、本人の同意なく個人情報を第三者に提供しません。
        </p>
        <ul className="mt-3 list-disc space-y-1.5 pl-5 text-sm text-gray-700">
          <li>法令に基づく場合</li>
          <li>人の生命、身体または財産の保護のために必要があり、本人の同意を得ることが困難な場合</li>
          <li>次項の業務委託先に対し、利用目的の達成に必要な範囲で提供する場合</li>
        </ul>

        {/* 4 */}
        <h2 className="mt-12 text-lg font-semibold text-gray-900">
          4. 業務委託先および外部送信先
        </h2>
        <p className="mt-3 text-sm leading-relaxed text-gray-700">
          本サービスは、次の外部サービスを利用しています。
          <strong className="text-gray-900">
            米国等の外国にある事業者へ個人情報が移転されます。
          </strong>
        </p>
        <div className="mt-4 overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-200 bg-gray-50/60">
                <th className="px-3 py-2 text-left text-xs font-semibold text-gray-500">提供先</th>
                <th className="px-3 py-2 text-left text-xs font-semibold text-gray-500">役割</th>
                <th className="px-3 py-2 text-left text-xs font-semibold text-gray-500">
                  提供される情報
                </th>
              </tr>
            </thead>
            <tbody>
              {THIRD_PARTIES.map((t, i) => (
                <tr key={t.name} className={i % 2 === 0 ? "bg-white" : "bg-gray-50/40"}>
                  <td className="px-3 py-2.5 font-medium text-gray-800">{t.name}</td>
                  <td className="px-3 py-2.5 text-gray-700">{t.role}</td>
                  <td className="px-3 py-2.5 text-gray-600">{t.data}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {/* 5 */}
        <h2 className="mt-12 text-lg font-semibold text-gray-900">5. 保存期間</h2>
        <ul className="mt-3 list-disc space-y-1.5 pl-5 text-sm text-gray-700">
          <li>アカウント情報: 退会の申出を受けてから合理的な期間内に削除します</li>
          <li>セッション情報・パスワード再設定情報: 有効期限の経過後に無効化します</li>
          <li>操作ログ・出力ログ: 不正利用の調査および利用上限の管理に必要な期間保存します</li>
          <li>法令により保存が義務づけられる情報: 当該法令に定める期間</li>
        </ul>

        {/* 6 */}
        <h2 className="mt-12 text-lg font-semibold text-gray-900">6. 安全管理措置</h2>
        <ul className="mt-3 list-disc space-y-1.5 pl-5 text-sm text-gray-700">
          <li>パスワードおよび各種トークンはハッシュ化して保存し、平文では保持しません</li>
          <li>通信は TLS により暗号化します</li>
          <li>権限に応じて閲覧・操作できる機能を制限します</li>
          <li>操作ログを記録し、不正利用の検知に用います</li>
        </ul>

        {/* 7 */}
        <h2 className="mt-12 text-lg font-semibold text-gray-900">
          7. 開示・訂正・利用停止等の請求
        </h2>
        <p className="mt-3 text-sm leading-relaxed text-gray-700">
          利用者は、当社が保有する自己の個人情報について、開示、訂正、追加、削除、
          利用停止、第三者提供の停止を請求できます。
          {OPERATOR.privacyContact} 宛にご連絡ください。
          ご本人であることを確認したうえで、法令に従い対応します。
        </p>

        {/* 8 */}
        <h2 className="mt-12 text-lg font-semibold text-gray-900">8. 事業所データの取り扱い</h2>
        <p className="mt-3 text-sm leading-relaxed text-gray-700">
          本サービスが分析対象とする事業所・法人のデータは、
          介護サービス情報公表システム等で公表されている情報を収集・加工したものであり、
          利用者の個人情報とは区別して取り扱います。
          データの範囲と確からしさについては
          <Link href="/methodology" className="text-brand-700 underline hover:text-brand-800">
            指標・データ取扱方針
          </Link>
          をご覧ください。
        </p>
        <p className="mt-3 text-sm leading-relaxed text-gray-700">
          公表情報に含まれる代表者名・管理者名等について、
          掲載の停止をご希望の場合は {OPERATOR.privacyContact} までご連絡ください。
          公表元の状況を確認のうえ対応します。
        </p>

        {/* 9 */}
        <h2 className="mt-12 text-lg font-semibold text-gray-900">9. 本ポリシーの変更</h2>
        <p className="mt-3 text-sm leading-relaxed text-gray-700">
          当社は、必要に応じて本ポリシーを変更することがあります。
          変更後の内容は本ページに掲示した時点から適用されます。
          利用者に重要な影響を与える変更を行う場合は、
          本サービス上または登録メールアドレス宛に通知します。
        </p>

        {/* 10 */}
        <h2 className="mt-12 text-lg font-semibold text-gray-900">10. お問い合わせ窓口</h2>
        <div className="mt-3 rounded-lg border border-gray-200 bg-gray-50 px-5 py-4 text-sm text-gray-700">
          <p>{OPERATOR.name}</p>
          <p className="mt-1">{OPERATOR.address}</p>
          <p className="mt-1">個人情報に関するお問い合わせ: {OPERATOR.privacyContact}</p>
        </div>

        <div className="mt-14 rounded-lg border border-gray-200 bg-gray-50 px-5 py-4">
          <p className="text-sm text-gray-600">
            関連ページ:{" "}
            <Link href="/terms" className="text-brand-700 underline hover:text-brand-800">
              利用規約
            </Link>
            {" / "}
            <Link href="/methodology" className="text-brand-700 underline hover:text-brand-800">
              指標・データ取扱方針
            </Link>
          </p>
        </div>
      </section>
    </PublicLayout>
  );
}
