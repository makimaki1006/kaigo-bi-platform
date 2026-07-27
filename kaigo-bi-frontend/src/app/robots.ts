// ===================================================
// robots.txt（Next.js Metadata Route）
//
// 方針:
//  - 公開SEOページはクロール許可
//  - /api/ は拒否
//  - アプリ画面・アカウント・管理画面・認証系は拒否
//  - sitemap.xml の絶対URLを指定
//  - CSS/JS など描画に必要な /_next/ を全面拒否しない
//
// 注意: robots.txt はアクセス制御ではない。認証必須APIの保護手段ではない。
// ===================================================

import type { MetadataRoute } from "next";
import { absoluteUrl } from "@/lib/site";

/** クロール拒否するパス接頭辞（アプリ画面・認証系・API） */
const DISALLOWED = [
  "/api/",
  "/login",
  "/signup",
  "/forgot-password",
  "/reset-password",
  "/account",
  "/admin",
  "/dashboard",
  "/facility",
  "/facilities",
  "/corp",
  "/corp-compare",
  "/corp-group",
  "/ma-screening",
  "/due-diligence",
  "/pmi-synergy",
  "/benchmark",
  "/cost-estimation",
  "/data-quality",
  "/financial-health",
  "/growth",
  "/health-check",
  "/hiring-weather",
  "/home",
  "/insights",
  "/list-export",
  "/market",
  "/quality",
  "/revenue",
  "/salary",
  "/service-portfolio",
  "/trends",
  "/workforce",
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
