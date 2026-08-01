// ===================================================
// サイトURLの一元管理
// canonical / OG URL / sitemap / robots の基準URLを
// ここだけで管理する。個別にハードコードしないこと。
// ===================================================

/**
 * 環境変数未設定時のfallback。
 * 本番ビルド(NODE_ENV=production)では現行の本番URLを既定にする。
 * これにより NEXT_PUBLIC_SITE_URL 未設定でも canonical/OG/sitemap が
 * localhost を指してSEOが壊れる事故を防ぐ。
 * 独自ドメインへ移行する際は NEXT_PUBLIC_SITE_URL を設定すれば上書きされる。
 */
const FALLBACK_SITE_URL =
  process.env.NODE_ENV === "production"
    ? "https://kaigo-bi.onrender.com"
    : "http://localhost:3000";

/**
 * 公開サイトの基準URL（末尾スラッシュなしの絶対URL）。
 *
 * 本番で独自ドメインに切り替える場合のみ環境変数 `NEXT_PUBLIC_SITE_URL` を設定する。
 * 未設定でも本番ビルドでは上記 FALLBACK_SITE_URL（現行の本番URL）が使われる。
 */
export const SITE_URL: string = normalizeBaseUrl(
  process.env.NEXT_PUBLIC_SITE_URL ?? FALLBACK_SITE_URL
);

/**
 * 本番ビルドで `NEXT_PUBLIC_SITE_URL` 未設定を検知するためのフラグ。
 * true の場合、環境に応じたfallback URLを使用している。
 * 独自ドメインへ移行した本番ではfalseになるよう環境変数を設定する。
 */
export const IS_SITE_URL_FALLBACK: boolean = !process.env.NEXT_PUBLIC_SITE_URL;

/**
 * 公開SEOページのインデックス制御。
 *
 * 公開コピーは2026-08-01に承認済みのため、通常はindex対象とする。
 * 緊急時や検証環境で明示的に"false"を設定した場合だけnoindexへ戻す。
 */
export const SEO_CONTENT_APPROVED: boolean =
  process.env.NEXT_PUBLIC_SEO_CONTENT_APPROVED !== "false";

/** 末尾スラッシュを除去して基準URLを正規化する */
function normalizeBaseUrl(url: string): string {
  return url.trim().replace(/\/+$/, "");
}

/**
 * サイト内パスから絶対URLを生成する。
 * canonical / openGraph.url / sitemap で使用する。
 *
 * @example absoluteUrl("/pricing") // => "https://.../pricing"
 * @example absoluteUrl("/")        // => "https://..."（ルートは末尾スラッシュなし）
 */
export function absoluteUrl(path: string = "/"): string {
  if (!path || path === "/") return SITE_URL;
  const normalized = path.startsWith("/") ? path : `/${path}`;
  return `${SITE_URL}${normalized}`;
}
