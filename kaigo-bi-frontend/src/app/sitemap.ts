// ===================================================
// sitemap.xml（Next.js Metadata Route）
//
// index対象の公開SEOページ7本のみを含める。
// 認証・登録・アプリ画面・クエリ付きURLは含めない。
//
// lastModified はビルド時刻で毎回偽更新しない。信頼できる更新日を
// 管理していないため、現状は省略する（更新管理が入ったら付与する）。
// ===================================================

import type { MetadataRoute } from "next";
import { absoluteUrl, SEO_CONTENT_APPROVED } from "@/lib/site";
import { PUBLIC_SEO_PATHS, LEGAL_PATHS } from "@/lib/public-paths";
import { hasUnsetOperatorFields } from "@/lib/legal";

export default function sitemap(): MetadataRoute.Sitemap {
  if (!SEO_CONTENT_APPROVED) return [];

  // 事業者情報が「（未設定）」のままの法務ページは載せない
  const paths: readonly string[] = hasUnsetOperatorFields()
    ? PUBLIC_SEO_PATHS
    : [...PUBLIC_SEO_PATHS, ...LEGAL_PATHS];

  return paths.map((path) => ({
    url: absoluteUrl(path),
  }));
}
