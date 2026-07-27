// ===================================================
// サイトURLの一元管理
// canonical / OG URL / sitemap / robots の基準URLを
// ここだけで管理する。個別にハードコードしないこと。
// ===================================================

/**
 * 公開サイトの基準URL（末尾スラッシュなしの絶対URL）。
 *
 * 本番は環境変数 `NEXT_PUBLIC_SITE_URL` で設定する（例: https://kaigo-bi.onrender.com）。
 * 独自ドメインへ移行する際も、この環境変数の変更だけで全URLが切り替わる。
 * ローカル開発時の fallback は http://localhost:3000。
 */
export const SITE_URL: string = normalizeBaseUrl(
  process.env.NEXT_PUBLIC_SITE_URL ?? "http://localhost:3000"
);

/**
 * 本番ビルドで `NEXT_PUBLIC_SITE_URL` 未設定を検知するためのフラグ。
 * true の場合、canonical/OG が localhost を指すため本番前に設定が必要。
 */
export const IS_SITE_URL_FALLBACK: boolean = !process.env.NEXT_PUBLIC_SITE_URL;

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
