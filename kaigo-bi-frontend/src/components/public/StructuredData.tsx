// ===================================================
// 構造化データ（JSON-LD）
//
// JSON.stringify で生成し、ユーザー入力を混ぜない。
// - Organization / WebSite: PublicLayout が全公開ページに付与
// - SoftwareApplication: 公開トップ（src/app/page.tsx）でのみ使用
//
// 価格は pricing/page.tsx と一致させること（税別 / JPY）。
// レビュー点数・導入社数などページ本文に無い情報は入れない。
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

/**
 * SoftwareApplication + 料金オファー。
 * 価格は pricing/page.tsx の4プラン（税別）と一致させている。
 */
export function SoftwareApplicationJsonLd() {
  const offers = [
    { name: "Free", price: "0" },
    { name: "Standard", price: "9800" },
    { name: "Pro", price: "29800" },
    { name: "M&A", price: "49800" },
  ].map((plan) => ({
    "@type": "Offer",
    name: plan.name,
    price: plan.price,
    priceCurrency: "JPY",
    // 税別・月額であることを注記
    description: `${plan.name}プラン（月額・税別）`,
  }));

  return (
    <JsonLdScript
      data={{
        "@context": "https://schema.org",
        "@type": "SoftwareApplication",
        name: ORG_NAME,
        applicationCategory: "BusinessApplication",
        operatingSystem: "Web",
        url: SITE_URL,
        inLanguage: "ja",
        offers,
      }}
    />
  );
}
