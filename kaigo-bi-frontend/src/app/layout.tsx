import type { Metadata } from "next";
import { Suspense } from "react";
import "./globals.css";
import { AuthProvider } from "@/components/auth/AuthProvider";
import AppShell from "@/components/layout/AppShell";
import ErrorBoundary from "@/components/common/ErrorBoundary";

export const metadata: Metadata = {
  title: "介護BI - 戦略コンサルティング",
  description: "介護・福祉業界の戦略コンサルティング向けBIダッシュボード",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="ja">
      <body className="bg-surface min-h-screen">
        <AuthProvider>
          <Suspense
            fallback={
              <div className="flex items-center justify-center h-screen">
                <div className="text-gray-400 text-sm">読み込み中...</div>
              </div>
            }
          >
            <ErrorBoundary>
              <AppShell>{children}</AppShell>
            </ErrorBoundary>
          </Suspense>
        </AuthProvider>
      </body>
    </html>
  );
}

