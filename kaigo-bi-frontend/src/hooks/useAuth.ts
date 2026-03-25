"use client";

// ===================================================
// 認証状態管理フック
// JWTトークンベースの認証を提供
// ===================================================

import { useState, useEffect, useCallback, useRef } from "react";
import type { User, LoginResponse } from "@/lib/types";
import {
  getAuthToken,
  setAuthToken,
  removeAuthToken,
  apiRequest,
  ApiError,
} from "@/lib/api-client";

/** 認証状態 */
export interface AuthState {
  user: User | null;
  isLoading: boolean;
  isAuthenticated: boolean;
  login: (email: string, password: string) => Promise<void>;
  logout: () => Promise<void>;
}

/**
 * 認証状態管理フック
 * - JWTをlocalStorageに保存
 * - /api/auth/me で初期化（ページリロード・直接アクセス時にユーザー情報を復元）
 * - /api/auth/me 失敗時は /api/auth/refresh でトークン再取得を試みる
 * - ログアウト時にトークン削除
 */
export function useAuth(): AuthState {
  const [user, setUser] = useState<User | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  // 初期化が複数回走るのを防止（StrictMode対策）
  const initRef = useRef(false);

  // 初期化: トークンが存在する場合にユーザー情報を取得
  useEffect(() => {
    // StrictMode等で二重実行されるのを防止
    if (initRef.current) return;
    initRef.current = true;

    const token = getAuthToken();
    if (!token) {
      setIsLoading(false);
      return;
    }

    // /api/auth/me でユーザー情報を取得
    // バックエンドは { user: {...} } 形式で返す
    apiRequest<{ user: User }>("/api/auth/me", { method: "GET" })
      .then((data) => {
        setUser(data.user);
      })
      .catch(async (err) => {
        // 401の場合はトークンリフレッシュを試行
        if (err instanceof ApiError && err.status === 401) {
          try {
            const refreshResult = await apiRequest<{ token: string }>(
              "/api/auth/refresh",
              { method: "POST" }
            );
            // 新しいトークンを保存
            setAuthToken(refreshResult.token);
            // リフレッシュ後に再度ユーザー情報を取得
            const meResult = await apiRequest<{ user: User }>(
              "/api/auth/me",
              { method: "GET" }
            );
            setUser(meResult.user);
            return;
          } catch {
            // リフレッシュも失敗した場合はトークンを削除
          }
        }
        // トークンが無効な場合は削除
        removeAuthToken();
        setUser(null);
      })
      .finally(() => {
        setIsLoading(false);
      });
  }, []);

  // ログイン処理
  const login = useCallback(async (email: string, password: string) => {
    const response = await apiRequest<LoginResponse>("/api/auth/login", {
      method: "POST",
      body: { email, password },
      skipAuth: true,
    });

    // トークンを保存
    setAuthToken(response.token);
    setUser(response.user);
  }, []);

  // ログアウト処理
  const logout = useCallback(async () => {
    try {
      // サーバー側のログアウト処理（失敗しても続行）
      await apiRequest("/api/auth/logout", { method: "POST" });
    } catch {
      // ログアウトAPIが失敗してもクライアント側はクリーンアップ
    }
    removeAuthToken();
    setUser(null);
  }, []);

  return {
    user,
    isLoading,
    isAuthenticated: !!user,
    login,
    logout,
  };
}
