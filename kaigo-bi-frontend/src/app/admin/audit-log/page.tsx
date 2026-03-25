"use client";

// ===================================================
// 操作ログ画面（admin専用）
// 日時、ユーザー、操作、詳細、IP を一覧表示
// フィルタ: ユーザーID絞り込み + アクション種別
// ===================================================

import { useState, useMemo, useCallback } from "react";
import { Card } from "@tremor/react";
import ProtectedRoute from "@/components/auth/ProtectedRoute";
import DataTable from "@/components/data-display/DataTable";
import { useApi } from "@/hooks/useApi";
import ApiErrorBanner from "@/components/common/ApiErrorBanner";
import type { AuditLog, ColumnDef } from "@/lib/types";

/** バックエンドの監査ログレスポンス形式 */
interface AuditLogApiResponse {
  logs: AuditLog[];
  limit: number;
  offset: number;
}

/** 日時フォーマット */
function formatDateTime(dateStr: string): string {
  const d = new Date(dateStr);
  return d.toLocaleString("ja-JP", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

/** 操作ログ画面の内部コンテンツ */
function AuditLogContent() {
  // フィルタ状態
  const [actionFilter, setActionFilter] = useState("");
  const [page, setPage] = useState(1);
  const pageSize = 30;

  // APIパラメータ構築
  // バックエンドは /api/users/audit-log で { limit, offset, user_id?, action? } を受け取る
  const params = useMemo(() => {
    const p: Record<string, string | number> = {
      limit: pageSize,
      offset: (page - 1) * pageSize,
    };
    if (actionFilter.trim()) p.action = actionFilter.trim();
    return p;
  }, [actionFilter, page]);

  // 操作ログ取得
  const { data, isLoading, error } = useApi<AuditLogApiResponse>(
    "/api/users/audit-log",
    params
  );

  // フィルタリセット
  const handleReset = useCallback(() => {
    setActionFilter("");
    setPage(1);
  }, []);

  // テーブルカラム定義
  const columns: ColumnDef<AuditLog>[] = useMemo(
    () => [
      {
        key: "created_at" as keyof AuditLog & string,
        label: "日時",
        sortable: true,
        width: "180px",
        render: (value: AuditLog[keyof AuditLog]) => (
          <span className="text-gray-600 text-xs font-mono">
            {formatDateTime(value as string)}
          </span>
        ),
      },
      {
        key: "user_id" as keyof AuditLog & string,
        label: "ユーザーID",
        width: "220px",
        render: (value: AuditLog[keyof AuditLog]) => (
          <span className="font-medium text-gray-700 text-xs font-mono truncate max-w-[200px] block">
            {(value as string) || "-"}
          </span>
        ),
      },
      {
        key: "action" as keyof AuditLog & string,
        label: "操作",
        width: "160px",
        render: (value: AuditLog[keyof AuditLog]) => {
          const action = value as string;
          // 操作タイプに応じたバッジカラー
          let badgeStyle = "bg-gray-100 text-gray-600";
          if (action.includes("login")) badgeStyle = "bg-brand-50 text-brand-700";
          if (action.includes("create")) badgeStyle = "bg-emerald-50 text-emerald-700";
          if (action.includes("update")) badgeStyle = "bg-amber-50 text-amber-700";
          if (action.includes("delete")) badgeStyle = "bg-red-50 text-red-700";
          if (action.includes("logout")) badgeStyle = "bg-gray-100 text-gray-600";

          const ACTION_LABELS: Record<string, string> = {
            login: "ログイン",
            logout: "ログアウト",
            login_failed: "ログイン失敗",
            create_user: "ユーザー作成",
            update_user: "ユーザー更新",
            delete_user: "ユーザー削除",
          };
          return (
            <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${badgeStyle}`}>
              {ACTION_LABELS[action] ?? action}
            </span>
          );
        },
      },
      {
        key: "details" as keyof AuditLog & string,
        label: "詳細",
        render: (value: AuditLog[keyof AuditLog]) => (
          <span className="text-gray-500 text-xs truncate max-w-xs block">
            {(value as string) || "-"}
          </span>
        ),
      },
      {
        key: "ip_address" as keyof AuditLog & string,
        label: "IPアドレス",
        width: "140px",
        render: (value: AuditLog[keyof AuditLog]) => (
          <span className="text-gray-400 text-xs font-mono">
            {(value as string) || "-"}
          </span>
        ),
      },
    ],
    []
  );

  // ログデータ
  const logs = data?.logs ?? [];

  // ページネーション情報（バックエンドはtotalを返さないので、取得数からおおよそ推測）
  // 取得件数がpageSizeと同数なら次ページがある可能性
  const hasMore = logs.length === pageSize;
  const pagination = {
    page,
    pageSize,
    total: hasMore ? page * pageSize + 1 : (page - 1) * pageSize + logs.length,
    totalPages: hasMore ? page + 1 : page,
  };

  return (
    <div className="space-y-6">
      {/* ページヘッダー */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">操作ログ</h1>
          <p className="text-sm text-gray-500 mt-1">
            システム操作の履歴を確認できます
          </p>
        </div>
        <a
          href="/admin"
          className="px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-lg
            hover:bg-gray-50 transition-colors"
        >
          ユーザー管理に戻る
        </a>
      </div>

      {/* フィルタ */}
      <Card>
        <div className="flex flex-wrap items-end gap-4">
          {/* アクション種別 */}
          <div>
            <label htmlFor="al-action" className="block text-xs font-medium text-gray-500 mb-1">
              操作種別
            </label>
            <select
              id="al-action"
              value={actionFilter}
              onChange={(e) => {
                setActionFilter(e.target.value);
                setPage(1);
              }}
              className="px-3 py-2 border border-gray-300 rounded-lg text-sm w-48
                focus:outline-none focus:ring-2 focus:ring-brand-500 transition-colors"
            >
              <option value="">すべて</option>
              <option value="login">ログイン</option>
              <option value="login_failed">ログイン失敗</option>
              <option value="logout">ログアウト</option>
              <option value="create_user">ユーザー作成</option>
              <option value="update_user">ユーザー更新</option>
              <option value="delete_user">ユーザー削除</option>
            </select>
          </div>

          {/* リセットボタン */}
          <button
            onClick={handleReset}
            className="px-3 py-2 text-sm text-gray-600 bg-gray-100 rounded-lg
              hover:bg-gray-200 transition-colors"
          >
            リセット
          </button>
        </div>
      </Card>

      {/* APIエラーバナー */}
      <ApiErrorBanner error={error} />

      {/* ログテーブル */}
      {!error && (
        <Card>
          {/* eslint-disable-next-line @typescript-eslint/no-explicit-any */}
          <DataTable<any>
            columns={columns as any}
            data={logs as any[]}
            loading={isLoading}
            pagination={logs.length > 0 ? pagination : undefined}
            onPageChange={setPage}
          />
        </Card>
      )}
    </div>
  );
}

/** 操作ログページ（ProtectedRoute でadmin限定） */
export default function AuditLogPage() {
  return (
    <ProtectedRoute allowedRoles={["admin"]}>
      <AuditLogContent />
    </ProtectedRoute>
  );
}
