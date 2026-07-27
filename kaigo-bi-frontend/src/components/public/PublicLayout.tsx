// ===================================================
// 公開サイト共通レイアウト（Server Component）
//
// 公開SEOページ（/, /features/*, /data, /methodology）はこの
// コンポーネントで本文をラップする。ヘッダー・フッターと、
// サイト共通の JSON-LD（Organization / WebSite）を全公開ページに付与する。
//
// 各ページの使い方（担当エージェントはこのインターフェースに従うこと）:
//
//   // src/app/features/management/page.tsx
//   import type { Metadata } from "next";
//   import PublicLayout from "@/components/public/PublicLayout";
//   import { buildPublicMetadata } from "@/lib/seo";
//
//   export const metadata: Metadata = buildPublicMetadata({
//     path: "/features/management",
//     title: "介護事業者の経営支援",
//     description: "…",
//   });
//
//   export default function Page() {
//     return (
//       <PublicLayout>
//         <h1>…</h1>   {/* H1は各ページに1個。PublicLayoutはH1を出さない */}
//         {/* 本文セクション … */}
//       </PublicLayout>
//     );
//   }
//
// 注意:
//  - "use client" を付けない（Server Component維持）。計測ボタン等の
//    インタラクティブ要素だけを個別のClient Componentへ分離すること。
//  - metadata は各ページで buildPublicMetadata() を使って個別に定義する。
//  - <main> と skip-link はこのレイアウトが提供するため、各ページは
//    見出し（H1から開始）と本文だけを書く。
// ===================================================

import type { ReactNode } from "react";
import PublicHeader from "@/components/public/PublicHeader";
import PublicFooter from "@/components/public/PublicFooter";
import { OrganizationJsonLd, WebSiteJsonLd } from "@/components/public/StructuredData";

interface PublicLayoutProps {
  children: ReactNode;
}

export default function PublicLayout({ children }: PublicLayoutProps) {
  return (
    <div className="flex min-h-screen flex-col bg-white text-gray-900">
      {/* キーボード利用者向けスキップリンク */}
      <a
        href="#main-content"
        className="sr-only focus:not-sr-only focus:absolute focus:left-4 focus:top-4 focus:z-50 focus:rounded-lg focus:bg-brand-600 focus:px-4 focus:py-2 focus:text-sm focus:font-semibold focus:text-white"
      >
        本文へスキップ
      </a>

      {/* サイト共通の構造化データ（全公開ページに付与） */}
      <OrganizationJsonLd />
      <WebSiteJsonLd />

      <PublicHeader />

      <main id="main-content" className="flex-1">
        {children}
      </main>

      <PublicFooter />
    </div>
  );
}
