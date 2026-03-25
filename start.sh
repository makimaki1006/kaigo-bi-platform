#!/bin/bash
set -e

echo "=== kaigo-bi 統合コンテナ起動 ==="

# Rust バックエンドをバックグラウンドで起動
echo "Rust バックエンド起動中 (ポート ${KAIGO_PORT:-3001})..."
kaigo-bi-backend &
BACKEND_PID=$!

# バックエンドの準備待ち（最大120秒）
echo "バックエンドの準備を待機中..."
for i in $(seq 1 60); do
    if curl -sf http://localhost:${KAIGO_PORT:-3001}/api/health > /dev/null 2>&1; then
        echo "バックエンド準備完了 (${i}x2秒)"
        break
    fi
    if ! kill -0 $BACKEND_PID 2>/dev/null; then
        echo "エラー: バックエンドプロセスが異常終了しました"
        exit 1
    fi
    sleep 2
done

# バックエンドが起動したか最終確認
if ! curl -sf http://localhost:${KAIGO_PORT:-3001}/api/health > /dev/null 2>&1; then
    echo "警告: バックエンドのヘルスチェックがタイムアウトしましたが、起動を続行します"
fi

# Next.js フロントエンドをバックグラウンドで起動
# RenderはPORT=10000を設定するが、Next.jsは3000で起動し、nginxが10000でプロキシする
export PORT=3000
echo "Next.js フロントエンド起動中 (ポート 3000)..."
node /app/server.js &
FRONTEND_PID=$!

# フロントエンドの準備待ち（最大30秒）
echo "フロントエンドの準備を待機中..."
for i in $(seq 1 15); do
    if curl -sf http://localhost:${PORT:-3000}/ > /dev/null 2>&1; then
        echo "フロントエンド準備完了 (${i}x2秒)"
        break
    fi
    if ! kill -0 $FRONTEND_PID 2>/dev/null; then
        echo "エラー: フロントエンドプロセスが異常終了しました"
        exit 1
    fi
    sleep 2
done

# nginx をフォアグラウンドで起動（メインプロセス）
echo "nginx リバースプロキシ起動中 (ポート 10000)..."
echo "=== 全サービス起動完了 ==="

# シグナルハンドラ: SIGTERM受信時に全プロセスを停止
cleanup() {
    echo "シャットダウン中..."
    kill $BACKEND_PID $FRONTEND_PID 2>/dev/null || true
    nginx -s quit 2>/dev/null || true
    wait
    echo "シャットダウン完了"
    exit 0
}
trap cleanup SIGTERM SIGINT

# nginx をデーモンモード無効で起動（フォアグラウンド）
nginx -g "daemon off;" &
NGINX_PID=$!

# いずれかのプロセスが終了したら全体を停止
wait -n $BACKEND_PID $FRONTEND_PID $NGINX_PID
EXIT_CODE=$?
echo "プロセスが終了しました (exit code: $EXIT_CODE)"
cleanup
