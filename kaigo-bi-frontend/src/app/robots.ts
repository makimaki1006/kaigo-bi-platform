// ===================================================
// robots.txt（Next.js Metadata Route）
//
// 方針:
//  - 公開SEOページはクロール許可
//  - /api/ は拒否
//  - HTMLページはクロールを許可し、各ページの noindex を読ませる
//  - sitemap.xml の絶対URLを指定
//  - CSS/JS など描画に必要な /_next/ を全面拒否しない
//
// 注意: robots.txt はアクセス制御ではない。認証必須APIの保護手段ではない。
// ===================================================

import type { MetadataRoute } from "next";
import { absoluteUrl } from "@/lib/site";

/**
 * クロール拒否する非HTMLエンドポイント。
 *
 * HTMLページをここで拒否すると、Googlebotがページ上のnoindexを読めず、
 * URLだけが検索結果に残る可能性がある。非公開データは認証で保護し、
 * 検索結果からの除外は各ページのnoindexで行う。
 */
const DISALLOWED = [
  "/api/",
];

export default function robots(): MetadataRoute.Robots {
  return {
    rules: [
      {
        userAgent: "*",
        allow: "/",
        disallow: DISALLOWED,
      },
    ],
    sitemap: absoluteUrl("/sitemap.xml"),
  };
}
