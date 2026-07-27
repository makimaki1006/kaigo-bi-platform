// ===================================================
// /pricing のSEOメタデータ（Server Component layout）
//
// pricing/page.tsx は "use client" のため metadata を export できない。
// ここでページ固有 metadata を付与し、ルート既定の noindex を
// index に上書きする（/pricing は公開index対象の7ページの1つ）。
//
// 注: 本文コピーの表記修正（223,000→223,103・税別明記など）は
//     pricing ページ担当の範囲。ここは metadata 層のみ。
// ===================================================

import type { Metadata } from "next";
import type { ReactNode } from "react";
import { buildPublicMetadata } from "@/lib/seo";

export const metadata: Metadata = buildPublicMetadata({
  path: "/pricing",
  title: "料金プラン",
  description:
    "kaigo-bi の料金プラン。Free / Standard / Pro / M&A の4プランを比較。営業リスト作成やM&A支援まで、目的に合わせて選べます（価格は税別）。",
});

export default function PricingLayout({ children }: { children: ReactNode }) {
  return <>{children}</>;
}
