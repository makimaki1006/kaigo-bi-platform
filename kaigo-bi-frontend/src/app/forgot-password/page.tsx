"use client";

// ===================================================
// パスワードリセット要求ページ
// メールアドレスを入力 → リセットメール送信
// ===================================================

import { useState, useCallback, type FormEvent } from "react";
import Link from "next/link";
import { apiRequest } from "@/lib/api-client";

export default function ForgotPasswordPage() {
  const [email, setEmail] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [sent, setSent] = useState(false);
  const [isSubmitting, setIsSubmitting] = useState(false);

  const handleSubmit = useCallback(
    async (e: FormEvent) => {
      e.preventDefault();
      setError(null);

      if (!email.trim()) {
        setError("メールアドレスを入力してください。");
        return;
      }

      setIsSubmitting(true);
      try {
        await apiRequest("/api/auth/forgot-password", {
          method: "POST",
          body: { email },
          skipAuth: true,
        });
        setSent(true);
      } catch (err) {
        setError(
          err instanceof Error ? err.message : "送信に失敗しました。"
        );
      } finally {
        setIsSubmitting(false);
      }
    },
    [email]
  );

  return (
    <div className="min-h-screen relative flex items-center justify-center px-4 overflow-hidden">
      {/* 背景グラデーション */}
      <div
        className="absolute inset-0"
        style={{
          background: "linear-gradient(135deg, #312e81 0%, #3730a3 40%, #1e1b4b 100%)",
        }}
      />
      <div
        className="absolute inset-0 opacity-[0.07]"
        style={{
          backgroundImage: "radial-gradient(circle, #fff 1px, transparent 1px)",
          backgroundSize: "24px 24px",
        }}
      />

      <div className="relative z-10 w-full max-w-md">
        <div className="text-center mb-8">
          <h1 className="text-4xl font-bold text-white mb-2">
            <span className="text-brand-300">介護</span>BI
          </h1>
          <p className="text-sm text-indigo-300">Strategic Consulting Platform</p>
        </div>

        <div className="bg-white rounded-2xl shadow-2xl p-8">
          <h2 className="text-lg font-semibold text-gray-900 mb-2 text-center">
            パスワードをお忘れですか？
          </h2>

          {sent ? (
            <div className="text-center">
              <div className="my-6 p-4 bg-green-50 border border-green-200 rounded-xl text-sm text-green-700">
                入力されたメールアドレスが登録されている場合、パスワード再設定のご案内を送信しました。メールをご確認ください（有効期限30分）。
              </div>
              <Link href="/login" className="text-sm text-brand-500 hover:text-brand-600 font-medium">
                ログインに戻る
              </Link>
            </div>
          ) : (
            <>
              <p className="text-xs text-gray-500 mb-6 text-center">
                登録済みのメールアドレスを入力してください。再設定用のリンクをお送りします。
              </p>

              {error && (
                <div className="mb-6 p-3 bg-red-50 border border-red-200 rounded-xl text-sm text-red-700" role="alert">
                  {error}
                </div>
              )}

              <form onSubmit={handleSubmit} noValidate>
                <div className="mb-6">
                  <label htmlFor="email" className="block text-sm font-medium text-gray-700 mb-1.5">
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

                <button
                  type="submit"
                  disabled={isSubmitting}
                  className="w-full py-3 bg-brand-500 text-white text-sm font-semibold rounded-xl
                    hover:bg-brand-600 focus:outline-none focus:ring-2 focus:ring-brand-500 focus:ring-offset-2
                    disabled:bg-brand-300 disabled:cursor-not-allowed
                    transition-all duration-200 flex items-center justify-center gap-2
                    shadow-lg shadow-brand-500/25"
                >
                  {isSubmitting ? (
                    <>
                      <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" />
                      <span>送信中...</span>
                    </>
                  ) : (
                    "再設定メールを送信"
                  )}
                </button>
              </form>

              <p className="text-center text-sm text-gray-500 mt-6">
                <Link href="/login" className="text-brand-500 hover:text-brand-600 font-medium">
                  ログインに戻る
                </Link>
              </p>
            </>
          )}
        </div>
      </div>
    </div>
  );
}
