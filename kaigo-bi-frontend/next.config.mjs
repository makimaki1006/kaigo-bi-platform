/** @type {import('next').NextConfig} */
const nextConfig = {
  // Dockerデプロイ用: standaloneモードで最小限のNode.jsサーバーを出力
  // node_modules不要でイメージサイズを大幅に削減
  output: "standalone",

  // 画像最適化設定
  images: {
    // standalone環境では外部画像サービスを使わないため無効化
    unoptimized: true,
  },

  // 本番ビルド時のソースマップ（デバッグ用に有効化）
  productionBrowserSourceMaps: false,

  // 実験的機能
  experimental: {
    // スクロール復元の改善
    scrollRestoration: true,
  },

  // 注: 以前あった "/" → "/dashboard" リダイレクトは削除した。
  // "/" は公開SEOトップページ（src/app/page.tsx）として提供する。

  // 環境変数のバリデーション（ビルド時警告）
  env: {
    // NEXT_PUBLIC_API_URL はビルド時に埋め込まれる
    // Dockerfile の ARG で注入可能
  },
};

export default nextConfig;
