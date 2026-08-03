// ===================================================
// 公開SEOページ用メタデータヘルパー
//
// ルートlayoutは既定で noindex（＝アプリ/認証ページを検索対象外にする）。
// 公開SEOページはこのヘルパーで robots.index を明示的に true に上書きし、
// canonical / OpenGraph を絶対URLで付与する。
//
// 各公開ページの page.tsx で次のように使う:
//
//   import type { Metadata } from "next";
//   import { buildPublicMetadata } from "@/lib/seo";
//
//   export const metadata: Metadata = buildPublicMetadata({
//     path: "/pricing",
//     title: "料金プラン",
//     description: "kaigo-bi の4プラン（Free / Standard / Pro / M&A）を比較。",
//   });
//
// title はルートの template により自動的に "… | kaigo-bi" になる。
// ===================================================

import type { Metadata } from "next";
import { absoluteUrl } from "@/lib/site";
import { SEO_CONTENT_APPROVED } from "@/lib/site";
import { hasUnsetOperatorFields } from "@/lib/legal";

interface PublicMetadataInput {
  /** サイト内の絶対パス（例: "/pricing"）。canonical/OG URLの基準になる */
  path: string;
  /** ページ固有タイトル（ブランド接尾辞はtemplateが付与するため含めない） */
  title: string;
  /** ページ固有ディスクリプション（120〜160字目安、キーワード詰め込み禁止） */
  description: string;
}

/**
 * 公開SEOページ用の Metadata を生成する。
 * - robots を index:true, follow:true に明示（ルート既定の noindex を上書き）
 * - canonical を絶対URLで付与
 * - openGraph に title/description/url/type を付与
 */
export function buildPublicMetadata({
  path,
  title,
  description,
}: PublicMetadataInput): Metadata {
  const url = absoluteUrl(path);
  return {
    title,
    description,
    alternates: {
      canonical: url,
    },
    robots: {
      index: SEO_CONTENT_APPROVED,
      follow: SEO_CONTENT_APPROVED,
    },
    openGraph: {
      title,
      description,
      url,
      type: "website",
    },
  };
}

/**
 * 法務ページ（利用規約・プライバシーポリシー）用の Metadata。
 *
 * 事業者名や連絡先が「（未設定）」のまま検索結果に載ると、
 * 実在しない事業者情報を掲示しているのと同じ状態になる。
 * OPERATOR が埋まるまでは noindex にする。
 */
export function buildLegalMetadata(input: PublicMetadataInput): Metadata {
  const base = buildPublicMetadata(input);
  if (!hasUnsetOperatorFields()) return base;
  return {
    ...base,
    robots: { index: false, follow: true },
  };
}
