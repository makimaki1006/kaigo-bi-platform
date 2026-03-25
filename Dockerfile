# ============================================
# kaigo-bi: 統合 Dockerfile
# Rust/Axum バックエンド + Next.js フロントエンド + nginx リバースプロキシ
# 単一コンテナで全サービスを実行
# ============================================

# --- ステージ1: Rust バックエンドビルド ---
FROM rust:latest AS rust-builder

WORKDIR /app

# 依存関係キャッシュ: Cargo.toml/Cargo.lock を先にコピーしてビルド
COPY kaigo-bi-backend/Cargo.toml kaigo-bi-backend/Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs
RUN cargo build --release
RUN rm -rf src

# 実際のソースコードをコピーしてビルド
COPY kaigo-bi-backend/src/ src/
RUN touch src/main.rs
RUN cargo build --release

# --- ステージ2: Next.js フロントエンドビルド ---
FROM node:20-slim AS node-builder

WORKDIR /app

# 依存関係キャッシュ
COPY kaigo-bi-frontend/package.json kaigo-bi-frontend/package-lock.json* ./
RUN if [ -f package-lock.json ]; then npm ci --legacy-peer-deps; else npm install --legacy-peer-deps; fi

# ソースコードコピー
COPY kaigo-bi-frontend/ .

# Next.js テレメトリ無効化
ENV NEXT_TELEMETRY_DISABLED=1

# API URL を空文字に設定（同一オリジンの nginx 経由でアクセスするため）
ENV NEXT_PUBLIC_API_URL=""

RUN npm run build

# --- ステージ3: 実行環境 ---
FROM node:20-slim

# nginx と必要パッケージをインストール
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      nginx \
      ca-certificates \
      curl \
      tini \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Rust バイナリをコピー
COPY --from=rust-builder /app/target/release/kaigo-bi-backend /usr/local/bin/kaigo-bi-backend

# Next.js standalone 出力をコピー
COPY --from=node-builder /app/.next/standalone ./
COPY --from=node-builder /app/.next/static ./.next/static
COPY --from=node-builder /app/public ./public

# nginx 設定をコピー
COPY nginx.conf /etc/nginx/nginx.conf

# 起動スクリプトをコピー
COPY start.sh /start.sh
RUN chmod +x /start.sh

# デフォルト環境変数
ENV KAIGO_DATA_PATH=/data/facilities.parquet
ENV KAIGO_PORT=3001
ENV PORT=3000
ENV HOSTNAME=0.0.0.0
ENV NODE_ENV=production
ENV NEXT_TELEMETRY_DISABLED=1

# Render はデフォルトでポート 10000 を公開
EXPOSE 10000

# ヘルスチェック: nginx 経由で /api/health を確認
HEALTHCHECK --interval=30s --timeout=5s --start-period=120s --retries=3 \
    CMD curl -f http://localhost:10000/api/health || exit 1

# tini をエントリポイントに使用（ゾンビプロセス防止）
ENTRYPOINT ["tini", "--"]
CMD ["/start.sh"]
