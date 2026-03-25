"use client";

// ===================================================
// 認証コンテキストプロバイダー
// アプリ全体に認証状態を提供する
// ===================================================

import {
  createContext,
  useContext,
  type ReactNode,
} from "react";
import { useAuth, type AuthState } from "@/hooks/useAuth";

/** 認証コンテキスト */
const AuthContext = createContext<AuthState | null>(null);

/** 認証コンテキストプロバイダーのProps */
interface AuthProviderProps {
  children: ReactNode;
}

/**
 * 認証コンテキストプロバイダー
 * layout.tsx でアプリ全体をラップして使用する
 */
export function AuthProvider({ children }: AuthProviderProps) {
  const auth = useAuth();

  return (
    <AuthContext.Provider value={auth}>
      {children}
    </AuthContext.Provider>
  );
}

/**
 * 認証コンテキストを使用するフック
 * AuthProvider 内でのみ使用可能
 */
export function useAuthContext(): AuthState {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error("useAuthContext は AuthProvider 内で使用してください");
  }
  return context;
}
