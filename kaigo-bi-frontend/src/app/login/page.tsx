"use client";

// ===================================================
// ログインページ
// brand-900グラデーション背景 + ドットパターン装飾
// ===================================================

import { useState, useCallback, useEffect, type FormEvent } from "react";
import { useRouter } from "next/navigation";
import { useAuthContext } from "@/components/auth/AuthProvider";

export default function LoginPage() {
  const router = useRouter();
  const { login, isAuthenticated, isLoading: authLoading } = useAuthContext();

  // フォーム状態
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [isSubmitting, setIsSubmitting] = useState(false);

  // フォーム送信ハンドラ
  const handleSubmit = useCallback(
    async (e: FormEvent) => {
      e.preventDefault();
      setError(null);

      if (!email.trim()) {
        setError("メールアドレスを入力してください。");
        return;
      }
      if (!password) {
        setError("パスワードを入力してください。");
        return;
      }

      setIsSubmitting(true);

      try {
        await login(email, password);
        router.push("/dashboard");
      } catch (err) {

        if (err instanceof Error) {
          const msg = err.message;
          if (msg.includes("Network") || msg.includes("fetch") || msg.includes("Failed")) {
            setError("サーバーに接続できません。しばらくしてからお試しください。");
          } else if (msg.includes("403") || msg.includes("無効") || msg.includes("期限")) {
            setError("アカウントが無効化されているか、有効期限が切れています。");
          } else {
            setError(msg);
          }
        } else {
          setError("ログインに失敗しました。");
        }
      } finally {
        setIsSubmitting(false);
      }
    },
    [email, password, login, router]
  );

  // 既にログイン済みの場合はダッシュボードにリダイレクト
  useEffect(() => {
    if (isAuthenticated && !authLoading) {
      router.push("/dashboard");
    }
  }, [isAuthenticated, authLoading, router]);

  if (isAuthenticated && !authLoading) {
    return null;
  }

  return (
    <div className="min-h-screen relative flex items-center justify-center px-4 overflow-hidden">
      {/* 背景グラデーション */}
      <div
        className="absolute inset-0"
        style={{
          background: "linear-gradient(135deg, #312e81 0%, #3730a3 40%, #1e1b4b 100%)",
        }}
      />

      {/* ドットパターン装飾 */}
      <div
        className="absolute inset-0 opacity-[0.07]"
        style={{
          backgroundImage: "radial-gradient(circle, #fff 1px, transparent 1px)",
          backgroundSize: "24px 24px",
        }}
      />

      {/* 装飾: 右上の光彩 */}
      <div
        className="absolute -top-32 -right-32 w-96 h-96 rounded-full opacity-10"
        style={{
          background: "radial-gradient(circle, #818cf8 0%, transparent 70%)",
        }}
      />

      {/* 装飾: 左下の光彩 */}
      <div
        className="absolute -bottom-32 -left-32 w-96 h-96 rounded-full opacity-10"
        style={{
          background: "radial-gradient(circle, #a5b4fc 0%, transparent 70%)",
        }}
      />

      {/* コンテンツ */}
      <div className="relative z-10 w-full max-w-md">
        {/* ロゴ・タイトルエリア */}
        <div className="text-center mb-8">
          <h1 className="text-4xl font-bold text-white mb-2">
            <span className="text-brand-300">介護</span>BI
          </h1>
          <p className="text-sm text-indigo-300">
            Strategic Consulting Platform
          </p>
        </div>

        {/* ログインカード */}
        <div className="bg-white rounded-2xl shadow-2xl p-8">
          <h2 className="text-lg font-semibold text-gray-900 mb-6 text-center">
            ログイン
          </h2>

          {/* エラーメッセージ */}
          {error && (
            <div
              className="mb-6 p-3 bg-red-50 border border-red-200 rounded-xl text-sm text-red-700 flex items-start gap-2"
              role="alert"
            >
              <svg
                className="w-5 h-5 text-red-400 flex-shrink-0 mt-0.5"
                fill="none"
                stroke="currentColor"
                viewBox="0 0 24 24"
                aria-hidden="true"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"
                />
              </svg>
              <span>{error}</span>
            </div>
          )}

          {/* ログインフォーム */}
          <form onSubmit={handleSubmit} noValidate>
            {/* メールアドレス */}
            <div className="mb-4">
              <label
                htmlFor="email"
                className="block text-sm font-medium text-gray-700 mb-1.5"
              >
                メールアドレス
              </label>
              <input
                id="email"
                type="email"
                autoComplete="email"
                required
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                disabled={isSubmitting}
                className="w-full px-4 py-2.5 border border-gray-300 rounded-xl text-sm
                  focus:outline-none focus:ring-2 focus:ring-brand-500 focus:border-brand-500
                  disabled:bg-gray-50 disabled:text-gray-400
                  placeholder:text-gray-400 transition-all"
                placeholder="example@company.com"
              />
            </div>

            {/* パスワード */}
            <div className="mb-6">
              <label
                htmlFor="password"
                className="block text-sm font-medium text-gray-700 mb-1.5"
              >
                パスワード
              </label>
              <input
                id="password"
                type="password"
                autoComplete="current-password"
                required
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                disabled={isSubmitting}
                className="w-full px-4 py-2.5 border border-gray-300 rounded-xl text-sm
                  focus:outline-none focus:ring-2 focus:ring-brand-500 focus:border-brand-500
                  disabled:bg-gray-50 disabled:text-gray-400
                  placeholder:text-gray-400 transition-all"
                placeholder="8文字以上"
              />
            </div>

            {/* ログインボタン */}
            <button
              type="submit"
              disabled={isSubmitting}
              className="w-full py-3 bg-brand-500 text-white text-sm font-semibold rounded-xl
                hover:bg-brand-600 focus:outline-none focus:ring-2 focus:ring-brand-500 focus:ring-offset-2
                disabled:bg-brand-300 disabled:cursor-not-allowed
                transition-all duration-200 flex items-center justify-center gap-2
                shadow-lg shadow-brand-500/25 hover:shadow-xl hover:shadow-brand-500/30"
            >
              {isSubmitting ? (
                <>
                  <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" />
                  <span>ログイン中...</span>
                </>
              ) : (
                "ログイン"
              )}
            </button>
          </form>
        </div>

        {/* フッター */}
        <p className="text-center text-xs text-indigo-400/60 mt-6">
          v0.4.0 | Phase 1+2+3
        </p>
      </div>
    </div>
  );
}
