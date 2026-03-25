"use client";

// ===================================================
// ユーザー管理画面（admin専用）
// KPIカード + ユーザー一覧テーブル + 追加/編集/削除
// ===================================================

import { useState, useCallback, useMemo } from "react";
import { Card } from "@tremor/react";
import ProtectedRoute from "@/components/auth/ProtectedRoute";
import KpiCard from "@/components/data-display/KpiCard";
import DataTable from "@/components/data-display/DataTable";
import UserFormModal from "@/components/admin/UserFormModal";
import DeleteConfirmModal from "@/components/admin/DeleteConfirmModal";
import { useApi } from "@/hooks/useApi";
import { apiRequest } from "@/lib/api-client";
import ApiErrorBanner from "@/components/common/ApiErrorBanner";
import type { User, ColumnDef } from "@/lib/types";

/** ロールバッジの色定義 */
const ROLE_BADGE_STYLES: Record<User["role"], string> = {
  admin: "bg-brand-100 text-brand-700",
  consultant: "bg-brand-50 text-brand-600",
  sales: "bg-emerald-50 text-emerald-700",
  viewer: "bg-gray-100 text-gray-600",
};

/** ロール表示名 */
const ROLE_LABELS: Record<User["role"], string> = {
  admin: "管理者",
  consultant: "コンサルタント",
  sales: "営業",
  viewer: "閲覧者",
};

/** 有効期限が切れているか判定 */
function isExpired(expiresAt: string | null): boolean {
  if (!expiresAt) return false;
  return new Date(expiresAt) < new Date();
}

/** 日付フォーマット */
function formatDate(dateStr: string | null): string {
  if (!dateStr) return "-";
  const d = new Date(dateStr);
  return d.toLocaleDateString("ja-JP", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });
}

/** ユーザー管理画面の内部コンテンツ */
function AdminContent() {
  // ユーザー一覧取得
  // バックエンドは /api/users で { users: [...], total: N } を返す
  const {
    data: usersResponse,
    error: usersError,
    isLoading,
    mutate,
  } = useApi<{ users: User[]; total: number }>("/api/users");
  const users = usersResponse?.users ?? null;

  // モーダル状態
  const [isFormOpen, setIsFormOpen] = useState(false);
  const [editingUser, setEditingUser] = useState<User | null>(null);
  const [isDeleteOpen, setIsDeleteOpen] = useState(false);
  const [deletingUser, setDeletingUser] = useState<User | null>(null);

  // KPI計算
  const kpi = useMemo(() => {
    if (!users) return { total: 0, active: 0, expired: 0 };
    return {
      total: users.length,
      active: users.filter((u) => u.is_active && !isExpired(u.expires_at)).length,
      expired: users.filter((u) => isExpired(u.expires_at)).length,
    };
  }, [users]);

  // ユーザー追加ボタン
  const handleAdd = useCallback(() => {
    setEditingUser(null);
    setIsFormOpen(true);
  }, []);

  // 編集ボタン
  const handleEdit = useCallback((user: User) => {
    setEditingUser(user);
    setIsFormOpen(true);
  }, []);

  // 削除ボタン
  const handleDeleteClick = useCallback((user: User) => {
    setDeletingUser(user);
    setIsDeleteOpen(true);
  }, []);

  // ユーザー保存（作成/更新）
  const handleSave = useCallback(
    async (
      data: {
        email: string;
        name: string;
        password: string;
        role: User["role"];
        expires_at: string;
        is_active: boolean;
      },
      isEdit: boolean
    ) => {
      const body = {
        email: data.email,
        name: data.name,
        role: data.role,
        is_active: data.is_active,
        expires_at: data.expires_at || null,
        ...(data.password ? { password: data.password } : {}),
      };

      if (isEdit && editingUser) {
        await apiRequest(`/api/users/${editingUser.id}`, {
          method: "PUT",
          body,
        });
      } else {
        await apiRequest("/api/users", {
          method: "POST",
          body: { ...body, password: data.password },
        });
      }

      // 一覧を再取得
      mutate();
    },
    [editingUser, mutate]
  );

  // ユーザー削除
  const handleDelete = useCallback(async () => {
    if (!deletingUser) return;
    await apiRequest(`/api/users/${deletingUser.id}`, {
      method: "DELETE",
    });
    mutate();
  }, [deletingUser, mutate]);

  // テーブルカラム定義
  const columns: ColumnDef<User>[] = useMemo(
    () => [
      {
        key: "name" as keyof User & string,
        label: "名前",
        sortable: true,
        width: "160px",
      },
      {
        key: "email" as keyof User & string,
        label: "メール",
        sortable: true,
        width: "220px",
      },
      {
        key: "role" as keyof User & string,
        label: "ロール",
        width: "120px",
        render: (value: User[keyof User]) => {
          const role = value as User["role"];
          return (
            <span
              className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${ROLE_BADGE_STYLES[role]}`}
            >
              {ROLE_LABELS[role]}
            </span>
          );
        },
      },
      {
        key: "is_active" as keyof User & string,
        label: "状態",
        width: "80px",
        render: (value: User[keyof User]) => {
          const active = value as boolean;
          return (
            <span
              className={`inline-flex items-center gap-1.5 text-xs ${
                active ? "text-green-600" : "text-gray-400"
              }`}
            >
              <span
                className={`w-2 h-2 rounded-full ${
                  active ? "bg-green-400" : "bg-gray-300"
                }`}
              />
              {active ? "有効" : "無効"}
            </span>
          );
        },
      },
      {
        key: "expires_at" as keyof User & string,
        label: "有効期限",
        sortable: true,
        width: "120px",
        render: (value: User[keyof User]) => {
          const expiresAt = value as string | null;
          const expired = isExpired(expiresAt);
          return (
            <span className={expired ? "text-red-600 font-medium" : "text-gray-600"}>
              {expiresAt ? formatDate(expiresAt) : "無期限"}
              {expired && (
                <span className="ml-1 text-xs text-red-500">(期限切れ)</span>
              )}
            </span>
          );
        },
      },
      {
        key: "created_at" as keyof User & string,
        label: "作成日",
        sortable: true,
        width: "120px",
        render: (value: User[keyof User]) => (
          <span className="text-gray-500">{formatDate(value as string)}</span>
        ),
      },
      {
        key: "id" as keyof User & string,
        label: "操作",
        width: "120px",
        render: (_value: User[keyof User], row: User) => (
          <div className="flex items-center gap-2">
            {/* 編集ボタン */}
            <button
              onClick={(e) => {
                e.stopPropagation();
                handleEdit(row);
              }}
              className="px-2.5 py-1 text-xs font-medium text-blue-600 bg-blue-50 rounded hover:bg-blue-100 transition-colors"
              aria-label={`${row.name} を編集`}
            >
              編集
            </button>
            {/* 削除ボタン */}
            <button
              onClick={(e) => {
                e.stopPropagation();
                handleDeleteClick(row);
              }}
              className="px-2.5 py-1 text-xs font-medium text-red-600 bg-red-50 rounded hover:bg-red-100 transition-colors"
              aria-label={`${row.name} を削除`}
            >
              削除
            </button>
          </div>
        ),
      },
    ],
    [handleEdit, handleDeleteClick]
  );

  return (
    <div className="space-y-6">
      {/* ページヘッダー */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">ユーザー管理</h1>
          <p className="text-sm text-gray-500 mt-1">
            システムユーザーの管理と操作ログの確認
          </p>
        </div>
        <div className="flex items-center gap-3">
          <a
            href="/admin/audit-log"
            className="px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-lg
              hover:bg-gray-50 transition-colors"
          >
            操作ログ
          </a>
          <button
            onClick={handleAdd}
            className="px-4 py-2 text-sm font-medium text-white bg-brand-600 rounded-lg
              hover:bg-brand-700 focus:outline-none focus:ring-2 focus:ring-brand-500 focus:ring-offset-2
              transition-colors flex items-center gap-2"
          >
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24" aria-hidden="true">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4" />
            </svg>
            ユーザー追加
          </button>
        </div>
      </div>

      {/* APIエラーバナー */}
      <ApiErrorBanner error={usersError} />

      {/* KPIカード */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <KpiCard
          label="総ユーザー数"
          value={kpi.total}
          icon={<svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2" /><circle cx="9" cy="7" r="4" /><path d="M22 21v-2a4 4 0 0 0-3-3.87" /><path d="M16 3.13a4 4 0 0 1 0 7.75" /></svg>}
          accentColor="bg-brand-500"
          loading={isLoading}
        />
        <KpiCard
          label="アクティブユーザー"
          value={kpi.active}
          icon={<svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14" /><polyline points="22 4 12 14.01 9 11.01" /></svg>}
          accentColor="bg-emerald-500"
          loading={isLoading}
          subtitle="有効 + 期限内"
        />
        <KpiCard
          label="期限切れユーザー"
          value={kpi.expired}
          icon={<svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="12" r="10" /><polyline points="12 6 12 12 16 14" /></svg>}
          accentColor="bg-amber-500"
          loading={isLoading}
          subtitle="要対応"
        />
      </div>

      {/* ユーザー一覧テーブル */}
      <Card>
        <div className="mb-4">
          <h2 className="text-base font-semibold text-gray-900">ユーザー一覧</h2>
        </div>
        {/* eslint-disable-next-line @typescript-eslint/no-explicit-any */}
        <DataTable<any>
          columns={columns as any}
          data={(users || []) as any[]}
          loading={isLoading}
        />
      </Card>

      {/* ユーザー追加/編集モーダル */}
      <UserFormModal
        user={editingUser}
        isOpen={isFormOpen}
        onClose={() => {
          setIsFormOpen(false);
          setEditingUser(null);
        }}
        onSave={handleSave}
      />

      {/* 削除確認モーダル */}
      <DeleteConfirmModal
        userName={deletingUser?.name || ""}
        isOpen={isDeleteOpen}
        onClose={() => {
          setIsDeleteOpen(false);
          setDeletingUser(null);
        }}
        onConfirm={handleDelete}
      />
    </div>
  );
}

/** ユーザー管理ページ（ProtectedRoute でadmin限定） */
export default function AdminPage() {
  return (
    <ProtectedRoute allowedRoles={["admin"]}>
      <AdminContent />
    </ProtectedRoute>
  );
}
