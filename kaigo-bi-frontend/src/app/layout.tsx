import type { Metadata } from "next";
import { Suspense } from "react";
import "./globals.css";
import { AuthProvider } from "@/components/auth/AuthProvider";
import AppShell from "@/components/layout/AppShell";
import ErrorBoundary from "@/components/common/ErrorBoundary";
import { SITE_URL } from "@/lib/site";

// ブランド表記は "kaigo-bi" に統一（旧「介護BI - 戦略コンサルティング」は使わない）。
const SITE_NAME = "kaigo-bi";
const GOOGLE_SITE_VERIFICATION =
  process.env.NEXT_PUBLIC_GOOGLE_SITE_VERIFICATION?.trim();

export const metadata: Metadata = {
  metadataBase: new URL(SITE_URL),
  title: {
    // 公開ページは buildPublicMetadata() で個別titleを設定し、この template で
    // "…｜kaigo-bi" になる。トップ等 title 未指定時は default を使う。
    default: "kaigo-bi｜公開情報でわかる介護事業所のBI・データ分析",
    template: "%s｜kaigo-bi",
  },
  description:
    "全国の介護事業所の公開情報をもとに、市場・人材・品質・法人情報を分析できるBIサービス。経営支援・営業支援・M&A支援に対応。",
  applicationName: SITE_NAME,
  // Search ConsoleのHTMLタグ方式を使う場合のみ出力する。
  // NEXT_PUBLIC_GOOGLE_SITE_VERIFICATIONにはcontent属性の値だけを設定する。
  ...(GOOGLE_SITE_VERIFICATION
    ? { verification: { google: GOOGLE_SITE_VERIFICATION } }
    : {}),
  // 既定は noindex。アプリ画面・認証系ページを検索対象外にする。
  // 公開SEOページは buildPublicMetadata() で robots.index を true に上書きする。
  robots: {
    index: false,
    follow: false,
  },
  openGraph: {
    type: "website",
    siteName: SITE_NAME,
    locale: "ja_JP",
    url: SITE_URL,
    title: "kaigo-bi｜公開情報でわかる介護事業所のBI・データ分析",
    description:
      "全国の介護事業所の公開情報をもとに、市場・人材・品質・法人情報を分析できるBIサービス。",
  },
  twitter: {
    card: "summary",
    title: "kaigo-bi｜公開情報でわかる介護事業所のBI・データ分析",
    description:
      "全国の介護事業所の公開情報をもとに、市場・人材・品質・法人情報を分析できるBIサービス。",
  },
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="ja">
      <body className="bg-surface min-h-screen">
        <AuthProvider>
          <Suspense
            fallback={
              <div className="flex items-center justify-center h-screen">
                <div className="text-gray-400 text-sm">読み込み中...</div>
              </div>
            }
          >
            <ErrorBoundary>
              <AppShell>{children}</AppShell>
            </ErrorBoundary>
          </Suspense>
        </AuthProvider>
      </body>
    </html>
  );
}

