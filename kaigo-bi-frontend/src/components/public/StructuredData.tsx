// ===================================================
// 構造化データ（JSON-LD）
//
// JSON.stringify で生成し、ユーザー入力を混ぜない。
// - Organization / WebSite: PublicLayout が全公開ページに付与
//
// SoftwareApplicationは、Googleのリッチリザルト要件上、実在するratingまたは
// reviewが必須のため現時点では出力しない。評価を捏造してはならない。
// ===================================================

import { SITE_URL } from "@/lib/site";

const ORG_NAME = "kaigo-bi";

/** <script type="application/ld+json"> を安全に描画する */
function JsonLdScript({ data }: { data: Record<string, unknown> }) {
  return (
    <script
      type="application/ld+json"
      // JSON.stringify 出力のみを埋め込む（ユーザー入力なし）
      dangerouslySetInnerHTML={{ __html: JSON.stringify(data) }}
    />
  );
}

export function OrganizationJsonLd() {
  return (
    <JsonLdScript
      data={{
        "@context": "https://schema.org",
        "@type": "Organization",
        name: ORG_NAME,
        url: SITE_URL,
      }}
    />
  );
}

export function WebSiteJsonLd() {
  return (
    <JsonLdScript
      data={{
        "@context": "https://schema.org",
        "@type": "WebSite",
        name: ORG_NAME,
        url: SITE_URL,
        inLanguage: "ja",
      }}
    />
  );
}
