// ===================================================
// 公開SEOパスの一元管理
// ここで定義したパスは:
//  - AppShell が Sidebar/Header/ProtectedRoute を通さない（未ログインで閲覧可）
//  - robots.ts / sitemap.ts が index 対象として参照する
// ===================================================

/**
 * 検索エンジンに index させる公開SEOページ。
 * この配列とサイトマップ・robots・ヘッダー導線は一致させること。
 */
export const PUBLIC_SEO_PATHS = [
  "/",
  "/features/management",
  "/features/sales",
  "/features/ma",
  "/data",
  "/methodology",
  "/pricing",
] as const;

/**
 * 公開はするが、事業者情報が未設定のうちは index させない法務ページ。
 * 「（未設定）」と書かれた規約が検索結果に載るのを防ぐ。
 * src/lib/legal.ts の OPERATOR を埋めると自動的に index 対象になる。
 */
export const LEGAL_PATHS = ["/terms", "/privacy", "/legal"] as const;

/**
 * 公開アクセスは許可するが index はさせない認証系ページ。
 * （AppShell では公開扱い＝ProtectedRoute を通さないが、
 *  robots のデフォルト noindex 側に含める）
 */
export const AUTH_PATHS = [
  "/login",
  "/signup",
  "/forgot-password",
  "/reset-password",
] as const;

/**
 * AppShell が「サイドバー/認証ガードなし」で素通しにする全パス。
 * = 公開SEOページ + 認証系ページ。
 */
export const PUBLIC_PATHS: readonly string[] = [
  ...PUBLIC_SEO_PATHS,
  ...LEGAL_PATHS,
  ...AUTH_PATHS,
];

/**
 * 指定パスが公開扱い（サイドバー/認証ガードなし）かを判定する。
 * 完全一致で判定する（現状の公開パスに動的セグメントは無い）。
 */
export function isPublicPath(pathname: string): boolean {
  return PUBLIC_PATHS.includes(pathname);
}
