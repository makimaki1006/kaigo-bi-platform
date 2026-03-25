"use client";

// ===================================================
// アプリケーションシェル
// ログインページではサイドバー/ヘッダー非表示
// それ以外はサイドバー + ヘッダー + メインコンテンツ
// ===================================================

import { Suspense, type ReactNode } from "react";
import { usePathname } from "next/navigation";
import Sidebar from "@/components/layout/Sidebar";
import Header from "@/components/layout/Header";
import ProtectedRoute from "@/components/auth/ProtectedRoute";

interface AppShellProps {
  children: ReactNode;
}

export default function AppShell({ children }: AppShellProps) {
  const pathname = usePathname();

  // ログインページではサイドバー/ヘッダーなしの全画面レイアウト
  if (pathname === "/login") {
    return <>{children}</>;
  }

  // 通常ページ: サイドバー + ヘッダー + 認証ガード
  return (
    <ProtectedRoute>
      {/* サイドバー（固定幅240px） */}
      <Sidebar />

      {/* メインコンテンツエリア（サイドバー分の左マージン） */}
      <div className="ml-60 min-h-screen flex flex-col">
        {/* ヘッダー */}
        <Suspense fallback={null}>
          <Header />
        </Suspense>

        {/* ページコンテンツ */}
        <main className="flex-1 p-6">
          <Suspense
            fallback={
              <div className="flex items-center justify-center h-64">
                <div className="text-gray-400 text-sm">読み込み中...</div>
              </div>
            }
          >
            {children}
          </Suspense>
        </main>
      </div>
    </ProtectedRoute>
  );
}
