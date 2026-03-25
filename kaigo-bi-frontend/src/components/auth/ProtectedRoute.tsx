"use client";

// ===================================================
// 認証ガードコンポーネント
// 未認証時はログインにリダイレクト
// ロール不足時はアクセス権限エラーを表示
// ローディング中はスケルトンUIを表示
// ===================================================

import { useEffect, type ReactNode } from "react";
import { useRouter, usePathname } from "next/navigation";
import { useAuthContext } from "@/components/auth/AuthProvider";
import type { User } from "@/lib/types";

interface ProtectedRouteProps {
  children: ReactNode;
  /** 許可するロール一覧（指定しない場合は認証済みであればOK） */
  allowedRoles?: User["role"][];
}

/** 認証確認中のスケルトンUI */
function AuthSkeleton() {
  return (
    <div className="min-h-screen bg-surface animate-pulse">
      {/* サイドバースケルトン */}
      <div className="fixed left-0 top-0 bottom-0 w-60 bg-gray-800">
        <div className="px-5 py-5 border-b border-gray-700">
          <div className="h-6 w-20 bg-gray-600 rounded" />
          <div className="h-3 w-40 bg-gray-700 rounded mt-2" />
        </div>
        <div className="py-4 px-3 space-y-2">
          {Array.from({ length: 8 }).map((_, i) => (
            <div
              key={i}
              className="h-8 bg-gray-700/50 rounded-lg"
              style={{ animationDelay: `${i * 80}ms` }}
            />
          ))}
        </div>
      </div>

      {/* メインコンテンツスケルトン */}
      <div className="ml-60">
        {/* ヘッダースケルトン */}
        <div className="h-16 bg-white/80 border-b border-gray-200/60 flex items-center justify-between px-6">
          <div className="h-5 w-64 bg-gray-200 rounded" />
          <div className="h-7 w-32 bg-gray-100 rounded-full" />
        </div>
        {/* コンテンツスケルトン */}
        <div className="p-6 space-y-4">
          <div className="grid grid-cols-4 gap-4">
            {Array.from({ length: 4 }).map((_, i) => (
              <div key={i} className="h-28 bg-white rounded-xl shadow-sm" />
            ))}
          </div>
          <div className="grid grid-cols-2 gap-4">
            <div className="h-80 bg-white rounded-xl shadow-sm" />
            <div className="h-80 bg-white rounded-xl shadow-sm" />
          </div>
        </div>
      </div>
    </div>
  );
}

/**
 * 認証ガードコンポーネント
 * - 未ログイン: /login にリダイレクト
 * - ロール不足: アクセス権限エラー表示
 * - ログインページでは動作しない
 */
export default function ProtectedRoute({
  children,
  allowedRoles,
}: ProtectedRouteProps) {
  const { user, isLoading, isAuthenticated } = useAuthContext();
  const router = useRouter();
  const pathname = usePathname();

  useEffect(() => {
    // ローディング中は何もしない
    if (isLoading) return;

    // ログインページでは認証チェックをスキップ
    if (pathname === "/login") return;

    // 未認証の場合はログインページにリダイレクト
    if (!isAuthenticated) {
      router.push("/login");
    }
  }, [isLoading, isAuthenticated, pathname, router]);

  // ローディング中: スケルトンUI表示
  if (isLoading) {
    return <AuthSkeleton />;
  }

  // ログインページではそのまま表示
  if (pathname === "/login") {
    return <>{children}</>;
  }

  // 未認証（リダイレクト待ち）
  if (!isAuthenticated || !user) {
    return null;
  }

  // ロール制限チェック
  if (allowedRoles && !allowedRoles.includes(user.role)) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-8 text-center max-w-md">
          <div className="w-12 h-12 bg-red-100 rounded-full flex items-center justify-center mx-auto mb-4">
            <svg
              className="w-6 h-6 text-red-500"
              fill="none"
              stroke="currentColor"
              viewBox="0 0 24 24"
              aria-hidden="true"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-2.5L13.732 4c-.77-.833-1.964-.833-2.732 0L4.082 16.5c-.77.833.192 2.5 1.732 2.5z"
              />
            </svg>
          </div>
          <h3 className="text-lg font-semibold text-gray-900 mb-2">
            アクセス権限がありません
          </h3>
          <p className="text-sm text-gray-500">
            このページにアクセスするには管理者権限が必要です。
            <br />
            管理者にお問い合わせください。
          </p>
        </div>
      </div>
    );
  }

  return <>{children}</>;
}
