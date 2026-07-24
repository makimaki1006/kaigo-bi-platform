"use client";

// ===================================================
// パスワード再設定ページ
// メールのリンク（?token=...）から遷移し、新パスワードを設定
// ===================================================

import { useState, useCallback, Suspense, type FormEvent } from "react";
import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import { apiRequest } from "@/lib/api-client";

function ResetPasswordContent() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const token = searchParams.get("token") ?? "";

  const [password, setPassword] = useState("");
  const [passwordConfirm, setPasswordConfirm] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [done, setDone] = useState(false);
  const [isSubmitting, setIsSubmitting] = useState(false);

  const handleSubmit = useCallback(
    async (e: FormEvent) => {
      e.preventDefault();
      setError(null);

      if (password.length < 8) {
        setError("パスワードは8文字以上で設定してください。");
        return;
      }
      if (password !== passwordConfirm) {
        setError("パスワードが一致しません。");
        return;
      }

      setIsSubmitting(true);
      try {
        await apiRequest("/api/auth/reset-password", {
          method: "POST",
          body: { token, password },
          skipAuth: true,
        });
        setDone(true);
        // 3秒後にログインへ
        setTimeout(() => router.push("/login"), 3000);
      } catch (err) {
        setError(
          err instanceof Error ? err.message : "再設定に失敗しました。"
        );
      } finally {
        setIsSubmitting(false);
      }
    },
    [token, password, passwordConfirm, router]
  );

  const inputClassName = `w-full px-4 py-2.5 border border-gray-300 rounded-xl text-sm
    focus:outline-none focus:ring-2 focus:ring-brand-500 focus:border-brand-500
    disabled:bg-gray-50 disabled:text-gray-400
    placeholder:text-gray-400 transition-all`;

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
          <h2 className="text-lg font-semibold text-gray-900 mb-6 text-center">
            新しいパスワードを設定
          </h2>

          {!token ? (
            <div className="text-center">
              <div className="mb-6 p-3 bg-red-50 border border-red-200 rounded-xl text-sm text-red-700">
                リセットリンクが無効です。メールに記載されたURLからアクセスしてください。
              </div>
              <Link href="/forgot-password" className="text-sm text-brand-500 hover:text-brand-600 font-medium">
                リセットを再要求する
              </Link>
            </div>
          ) : done ? (
            <div className="text-center">
              <div className="mb-6 p-4 bg-green-50 border border-green-200 rounded-xl text-sm text-green-700">
                パスワードを再設定しました。まもなくログインページへ移動します。
              </div>
              <Link href="/login" className="text-sm text-brand-500 hover:text-brand-600 font-medium">
                今すぐログインする
              </Link>
            </div>
          ) : (
            <>
              {error && (
                <div className="mb-6 p-3 bg-red-50 border border-red-200 rounded-xl text-sm text-red-700" role="alert">
                  {error}
                </div>
              )}

              <form onSubmit={handleSubmit} noValidate>
                <div className="mb-4">
                  <label htmlFor="password" className="block text-sm font-medium text-gray-700 mb-1.5">
                    新しいパスワード
                  </label>
                  <input
                    id="password"
                    type="password"
                    autoComplete="new-password"
                    required
                    value={password}
                    onChange={(e) => setPassword(e.target.value)}
                    disabled={isSubmitting}
                    className={inputClassName}
                    placeholder="8文字以上"
                  />
                </div>

                <div className="mb-6">
                  <label htmlFor="passwordConfirm" className="block text-sm font-medium text-gray-700 mb-1.5">
                    新しいパスワード（確認）
                  </label>
                  <input
                    id="passwordConfirm"
                    type="password"
                    autoComplete="new-password"
                    required
                    value={passwordConfirm}
                    onChange={(e) => setPasswordConfirm(e.target.value)}
                    disabled={isSubmitting}
                    className={inputClassName}
                    placeholder="もう一度入力"
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
                      <span>設定中...</span>
                    </>
                  ) : (
                    "パスワードを再設定"
                  )}
                </button>
              </form>
            </>
          )}
        </div>
      </div>
    </div>
  );
}

export default function ResetPasswordPage() {
  // useSearchParams利用のためSuspenseが必要
  return (
    <Suspense fallback={null}>
      <ResetPasswordContent />
    </Suspense>
  );
}
