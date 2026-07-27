// ===================================================
// 公開サイト共通フッター（Server Component）
//
// 認証に依存しない。実ページが存在しない項目（利用規約・
// プライバシーポリシー・運営者情報）はリンク切れを作らず
// 「準備中」の非リンクテキストとして表示する。
// TODO: これらの実ページ作成は別タスク。作成後にLinkへ差し替える。
// ===================================================

import Link from "next/link";

const FOOTER_SECTIONS = [
  {
    heading: "機能",
    links: [
      { href: "/features/management", label: "経営支援" },
      { href: "/features/sales", label: "営業支援" },
      { href: "/features/ma", label: "M&A支援" },
    ],
  },
  {
    heading: "データと指標",
    links: [
      { href: "/data", label: "データについて" },
      { href: "/methodology", label: "指標の定義と注意事項" },
    ],
  },
  {
    heading: "サービス",
    links: [
      { href: "/pricing", label: "料金プラン" },
      { href: "/login", label: "ログイン" },
      { href: "/signup", label: "無料で始める" },
    ],
  },
] as const;

/** 実ページ未整備の項目（リンク化しない） */
const PENDING_LEGAL = ["利用規約", "プライバシーポリシー", "運営者情報"] as const;

export default function PublicFooter() {
  const year = new Date().getFullYear();

  return (
    <footer className="border-t border-gray-200 bg-gray-50">
      <div className="mx-auto max-w-6xl px-4 py-12">
        <div className="grid grid-cols-2 gap-8 md:grid-cols-4">
          {/* ブランド */}
          <div className="col-span-2 md:col-span-1">
            <Link
              href="/"
              className="rounded text-base font-bold text-brand-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500"
            >
              kaigo-bi
            </Link>
            <p className="mt-2 text-xs leading-relaxed text-gray-500">
              公開情報をもとにした介護事業所のBI・分析サービス
            </p>
          </div>

          {FOOTER_SECTIONS.map((section) => (
            <nav key={section.heading} aria-label={section.heading}>
              <h2 className="text-xs font-semibold uppercase tracking-wide text-gray-500">
                {section.heading}
              </h2>
              <ul className="mt-3 space-y-2">
                {section.links.map((link) => (
                  <li key={link.href}>
                    <Link
                      href={link.href}
                      className="rounded text-sm text-gray-600 hover:text-brand-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500"
                    >
                      {link.label}
                    </Link>
                  </li>
                ))}
              </ul>
            </nav>
          ))}
        </div>

        {/* 法務・運営者（実ページ未整備のため非リンク） */}
        <div className="mt-10 flex flex-col gap-3 border-t border-gray-200 pt-6 text-xs text-gray-400 md:flex-row md:items-center md:justify-between">
          <p>&copy; {year} kaigo-bi</p>
          <ul className="flex flex-wrap gap-x-4 gap-y-1">
            {PENDING_LEGAL.map((label) => (
              <li key={label}>
                <span>{label}（準備中）</span>
              </li>
            ))}
          </ul>
        </div>
      </div>
    </footer>
  );
}
