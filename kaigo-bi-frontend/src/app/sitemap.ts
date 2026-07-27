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
import { absoluteUrl } from "@/lib/site";
import { PUBLIC_SEO_PATHS } from "@/lib/public-paths";

export default function sitemap(): MetadataRoute.Sitemap {
  return PUBLIC_SEO_PATHS.map((path) => ({
    url: absoluteUrl(path),
  }));
}
