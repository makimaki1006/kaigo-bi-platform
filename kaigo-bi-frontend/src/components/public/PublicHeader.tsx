// ===================================================
// 公開サイト共通ヘッダー（Server Component）
//
// 認証・Turso・SWR に依存しない。JS実行前でも導線が機能するよう、
// モバイルメニューはネイティブの <details>/<summary> で実装している
// （キーボード操作可能・ハイドレーション不要）。
// ===================================================

import Link from "next/link";

/** ヘッダー/フッターで共有する主ナビゲーション項目 */
const NAV_LINKS = [
  { href: "/features/management", label: "経営支援" },
  { href: "/features/sales", label: "営業支援" },
  { href: "/features/ma", label: "M&A" },
  { href: "/data", label: "データについて" },
  { href: "/pricing", label: "料金" },
] as const;

export default function PublicHeader() {
  return (
    <header className="sticky top-0 z-40 border-b border-gray-200 bg-white/95 backdrop-blur">
      <div className="mx-auto flex h-16 max-w-6xl items-center justify-between px-4">
        {/* ロゴ / サービス名 */}
        <Link
          href="/"
          className="rounded text-lg font-bold tracking-tight text-brand-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500 focus-visible:ring-offset-2"
        >
          kaigo-bi
        </Link>

        {/* デスクトップナビ */}
        <nav aria-label="メインナビゲーション" className="hidden md:block">
          <ul className="flex items-center gap-6">
            {NAV_LINKS.map((link) => (
              <li key={link.href}>
                <Link
                  href={link.href}
                  className="rounded text-sm text-gray-700 transition-colors hover:text-brand-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500 focus-visible:ring-offset-2"
                >
                  {link.label}
                </Link>
              </li>
            ))}
          </ul>
        </nav>

        {/* デスクトップ右側CTA */}
        <div className="hidden items-center gap-3 md:flex">
          <Link
            href="/login"
            className="rounded text-sm text-gray-700 transition-colors hover:text-brand-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500 focus-visible:ring-offset-2"
          >
            ログイン
          </Link>
          <Link
            href="/signup"
            className="rounded-lg bg-brand-600 px-4 py-2 text-sm font-semibold text-white transition-colors hover:bg-brand-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500 focus-visible:ring-offset-2"
          >
            無料で始める
          </Link>
        </div>

        {/* モバイルメニュー（JS不要のネイティブ開閉） */}
        <details className="group relative md:hidden">
          <summary
            className="flex h-10 w-10 cursor-pointer list-none items-center justify-center rounded-lg text-gray-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500 [&::-webkit-details-marker]:hidden"
            aria-label="メニューを開く"
          >
            <svg
              className="h-6 w-6"
              fill="none"
              stroke="currentColor"
              strokeWidth={2}
              viewBox="0 0 24 24"
              aria-hidden="true"
            >
              <path strokeLinecap="round" strokeLinejoin="round" d="M4 6h16M4 12h16M4 18h16" />
            </svg>
          </summary>
          <nav
            aria-label="モバイルナビゲーション"
            className="absolute right-0 top-12 w-56 rounded-xl border border-gray-200 bg-white p-2 shadow-lg"
          >
            <ul className="flex flex-col">
              {NAV_LINKS.map((link) => (
                <li key={link.href}>
                  <Link
                    href={link.href}
                    className="block rounded-lg px-3 py-2 text-sm text-gray-700 hover:bg-gray-50 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500"
                  >
                    {link.label}
                  </Link>
                </li>
              ))}
              <li className="my-1 border-t border-gray-100" aria-hidden="true" />
              <li>
                <Link
                  href="/login"
                  className="block rounded-lg px-3 py-2 text-sm text-gray-700 hover:bg-gray-50 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500"
                >
                  ログイン
                </Link>
              </li>
              <li>
                <Link
                  href="/signup"
                  className="mt-1 block rounded-lg bg-brand-600 px-3 py-2 text-center text-sm font-semibold text-white hover:bg-brand-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500"
                >
                  無料で始める
                </Link>
              </li>
            </ul>
          </nav>
        </details>
      </div>
    </header>
  );
}
