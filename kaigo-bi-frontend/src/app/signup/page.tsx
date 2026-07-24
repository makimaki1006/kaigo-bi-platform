"use client";

// ===================================================
// サインアップページ
// ログインページと同じbrand-900グラデーション背景
// 登録成功で即ログイン状態 → ダッシュボードへ
// ===================================================

import { useState, useCallback, useEffect, type FormEvent } from "react";
import { useRouter } from "next/navigation";
import Link from "next/link";
import { useAuthContext } from "@/components/auth/AuthProvider";
import { persistWorkspace } from "@/hooks/useWorkspace";
import { WORKSPACES, type WorkspaceId } from "@/lib/constants";

/** 利用目的の選択肢（ワークスペースにマッピング） */
const PURPOSE_OPTIONS: { value: WorkspaceId; label: string }[] = [
  { value: "bi", label: "介護業界のデータを見たい" },
  { value: "management", label: "自施設の経営を改善したい（介護事業者）" },
  { value: "sales", label: "営業先リストを作りたい" },
  { value: "ma", label: "M&A・事業承継の検討" },
  { value: "all", label: "その他・全部見たい" },
];

export default function SignupPage() {
  const router = useRouter();
  const { signup, isAuthenticated, isLoading: authLoading } = useAuthContext();

  // フォーム状態
  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [passwordConfirm, setPasswordConfirm] = useState("");
  const [purpose, setPurpose] = useState<WorkspaceId>("all");
  const [error, setError] = useState<string | null>(null);
  const [isSubmitting, setIsSubmitting] = useState(false);

  // フォーム送信ハンドラ
  const handleSubmit = useCallback(
    async (e: FormEvent) => {
      e.preventDefault();
      setError(null);

      if (!name.trim()) {
        setError("お名前を入力してください。");
        return;
      }
      if (!email.trim()) {
        setError("メールアドレスを入力してください。");
        return;
      }
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
        await signup(email, password, name);
        // 利用目的に応じたワークスペースを保存し、そのホームへ誘導
        persistWorkspace(purpose);
        const home = WORKSPACES.find((w) => w.id === purpose)?.home ?? "/dashboard";
        router.push(home);
      } catch (err) {
        if (err instanceof Error) {
          const msg = err.message;
          if (msg.includes("Network") || msg.includes("fetch") || msg.includes("Failed")) {
            setError("サーバーに接続できません。しばらくしてからお試しください。");
          } else {
            setError(msg);
          }
        } else {
          setError("登録に失敗しました。");
        }
      } finally {
        setIsSubmitting(false);
      }
    },
    [name, email, password, passwordConfirm, purpose, signup, router]
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

      {/* ドットパターン装飾 */}
      <div
        className="absolute inset-0 opacity-[0.07]"
        style={{
          backgroundImage: "radial-gradient(circle, #fff 1px, transparent 1px)",
          backgroundSize: "24px 24px",
        }}
      />

      {/* コンテンツ */}
      <div className="relative z-10 w-full max-w-md py-10">
        {/* ロゴ・タイトルエリア */}
        <div className="text-center mb-8">
          <h1 className="text-4xl font-bold text-white mb-2">
            <span className="text-brand-300">介護</span>BI
          </h1>
          <p className="text-sm text-indigo-300">
            Strategic Consulting Platform
          </p>
        </div>

        {/* サインアップカード */}
        <div className="bg-white rounded-2xl shadow-2xl p-8">
          <h2 className="text-lg font-semibold text-gray-900 mb-2 text-center">
            無料アカウント登録
          </h2>
          <p className="text-xs text-gray-500 mb-6 text-center">
            クレジットカード不要。全国サマリーダッシュボードを今すぐ利用できます。
          </p>

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

          {/* サインアップフォーム */}
          <form onSubmit={handleSubmit} noValidate>
            {/* お名前 */}
            <div className="mb-4">
              <label
                htmlFor="name"
                className="block text-sm font-medium text-gray-700 mb-1.5"
              >
                お名前
              </label>
              <input
                id="name"
                type="text"
                autoComplete="name"
                required
                value={name}
                onChange={(e) => setName(e.target.value)}
                disabled={isSubmitting}
                className={inputClassName}
                placeholder="山田 太郎"
              />
            </div>

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
                className={inputClassName}
                placeholder="example@company.com"
              />
            </div>

            {/* パスワード */}
            <div className="mb-4">
              <label
                htmlFor="password"
                className="block text-sm font-medium text-gray-700 mb-1.5"
              >
                パスワード
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

            {/* パスワード確認 */}
            <div className="mb-4">
              <label
                htmlFor="passwordConfirm"
                className="block text-sm font-medium text-gray-700 mb-1.5"
              >
                パスワード（確認）
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

            {/* 利用目的（初期ワークスペースの選択） */}
            <div className="mb-6">
              <label
                htmlFor="purpose"
                className="block text-sm font-medium text-gray-700 mb-1.5"
              >
                利用目的
              </label>
              <select
                id="purpose"
                value={purpose}
                onChange={(e) => setPurpose(e.target.value as WorkspaceId)}
                disabled={isSubmitting}
                className={inputClassName}
              >
                {PURPOSE_OPTIONS.map((opt) => (
                  <option key={opt.value} value={opt.value}>
                    {opt.label}
                  </option>
                ))}
              </select>
              <p className="text-[11px] text-gray-400 mt-1">
                目的に合わせた画面構成で始まります（あとから切替可能）
              </p>
            </div>

            {/* 登録ボタン */}
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
                  <span>登録中...</span>
                </>
              ) : (
                "無料で登録"
              )}
            </button>
          </form>

          {/* ログインリンク */}
          <p className="text-center text-sm text-gray-500 mt-6">
            アカウントをお持ちの方は{" "}
            <Link href="/login" className="text-brand-500 hover:text-brand-600 font-medium">
              ログイン
            </Link>
          </p>
        </div>
      </div>
    </div>
  );
}
