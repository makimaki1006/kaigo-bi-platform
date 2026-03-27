// ===================================================
// API クライアント（fetchラッパー）
// ===================================================

/** APIベースURL（環境変数から取得、未設定時は空文字=同一オリジン） */
const API_BASE_URL =
  process.env.NEXT_PUBLIC_API_URL ?? "";

/** 認証トークンのlocalStorageキー */
const TOKEN_KEY = "kaigo_bi_token";

/** APIエラークラス */
export class ApiError extends Error {
  constructor(
    public status: number,
    message: string
  ) {
    super(message);
    this.name = "ApiError";
  }
}

/**
 * 認証トークンを取得する
 */
export function getAuthToken(): string | null {
  if (typeof window === "undefined") return null;
  return localStorage.getItem(TOKEN_KEY);
}

/**
 * 認証トークンを保存する
 */
export function setAuthToken(token: string): void {
  localStorage.setItem(TOKEN_KEY, token);
}

/**
 * 認証トークンを削除する
 */
export function removeAuthToken(): void {
  localStorage.removeItem(TOKEN_KEY);
}

/**
 * 認証ヘッダー付きのヘッダーオブジェクトを生成する
 */
function getHeaders(): Record<string, string> {
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
  };
  const token = getAuthToken();
  if (token) {
    headers["Authorization"] = `Bearer ${token}`;
  }
  return headers;
}

/**
 * SWR用のfetcher関数
 * エンドポイントを受け取り、JSON を返す
 * 401レスポンス時は /login にリダイレクト
 */
export async function fetcher<T>(endpoint: string): Promise<T> {
  const url = endpoint.startsWith("http")
    ? endpoint
    : `${API_BASE_URL}${endpoint}`;

  const response = await fetch(url, {
    headers: getHeaders(),
  });

  // 401の場合はログインページにリダイレクト
  if (response.status === 401) {
    removeAuthToken();
    if (typeof window !== "undefined" && !window.location.pathname.startsWith("/login")) {
      window.location.href = "/login";
    }
    throw new ApiError(401, "認証が必要です。ログインしてください。");
  }

  if (!response.ok) {
    throw new ApiError(
      response.status,
      `API Error: ${response.status} ${response.statusText}`
    );
  }

  return response.json();
}

/**
 * POST/PUT/DELETE用の汎用リクエスト関数
 * 認証ヘッダーを自動付与し、401時にリダイレクトする
 */
export async function apiRequest<T>(
  endpoint: string,
  options: {
    method?: "GET" | "POST" | "PUT" | "PATCH" | "DELETE";
    body?: unknown;
    skipAuth?: boolean;
  } = {}
): Promise<T> {
  const { method = "POST", body, skipAuth = false } = options;

  const url = endpoint.startsWith("http")
    ? endpoint
    : `${API_BASE_URL}${endpoint}`;

  const headers: Record<string, string> = {
    "Content-Type": "application/json",
  };

  // skipAuth=true（ログインAPI等）の場合は認証ヘッダーを付けない
  if (!skipAuth) {
    const token = getAuthToken();
    if (token) {
      headers["Authorization"] = `Bearer ${token}`;
    }
  }

  const response = await fetch(url, {
    method,
    headers,
    body: body ? JSON.stringify(body) : undefined,
  });

  // 401の場合はログインページにリダイレクト
  if (response.status === 401 && !skipAuth) {
    removeAuthToken();
    if (typeof window !== "undefined" && !window.location.pathname.startsWith("/login")) {
      window.location.href = "/login";
    }
    throw new ApiError(401, "認証が必要です。ログインしてください。");
  }

  if (!response.ok) {
    // エラーレスポンスのJSONを試行
    let message = `API Error: ${response.status} ${response.statusText}`;
    try {
      const errorData = await response.json();
      // バックエンドは {"error": "..."} 形式で返す
      if (errorData.error) message = errorData.error;
      else if (errorData.detail) message = errorData.detail;
      else if (errorData.message) message = errorData.message;
    } catch {
      // JSONパース失敗時はデフォルトメッセージ
    }
    throw new ApiError(response.status, message);
  }

  // 204 No Content の場合
  if (response.status === 204) {
    return undefined as T;
  }

  return response.json();
}

/**
 * クエリパラメータ付きURLを生成
 * API_BASE_URLが空の場合（同一オリジンプロキシ構成）は相対URLを返す
 */
export function buildUrl(
  endpoint: string,
  params?: Record<string, string | number | boolean | string[] | null | undefined>
): string {
  // API_BASE_URL が空（同一オリジン構成）の場合は相対URLを構築
  if (!API_BASE_URL) {
    const searchParams = new URLSearchParams();
    if (params) {
      Object.entries(params).forEach(([key, value]) => {
        if (value == null) return;
        if (Array.isArray(value)) {
          if (value.length > 0) {
            searchParams.set(key, value.join(","));
          }
        } else {
          searchParams.set(key, String(value));
        }
      });
    }
    const qs = searchParams.toString();
    return qs ? `${endpoint}?${qs}` : endpoint;
  }

  // API_BASE_URL が設定されている場合は絶対URLを構築
  const url = new URL(`${API_BASE_URL}${endpoint}`);

  if (params) {
    Object.entries(params).forEach(([key, value]) => {
      if (value == null) return;
      if (Array.isArray(value)) {
        // 配列はカンマ区切りで送信
        if (value.length > 0) {
          url.searchParams.set(key, value.join(","));
        }
      } else {
        url.searchParams.set(key, String(value));
      }
    });
  }

  return url.toString();
}

/**
 * CSVダウンロード用URL生成
 */
export function buildCsvExportUrl(
  params?: Record<string, string | number | boolean | string[] | null | undefined>
): string {
  return buildUrl("/api/export/csv", params);
}
