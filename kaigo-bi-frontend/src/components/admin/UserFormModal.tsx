"use client";

// ===================================================
// ユーザー追加/編集モーダル
// メール、名前、パスワード、ロール、有効期限、有効/無効
// ===================================================

import {
  useState,
  useCallback,
  useEffect,
  type FormEvent,
} from "react";
import type { User } from "@/lib/types";

/** ロール選択肢 */
const ROLE_OPTIONS: { value: User["role"]; label: string }[] = [
  { value: "admin", label: "管理者 (Admin)" },
  { value: "consultant", label: "コンサルタント (Consultant)" },
  { value: "sales", label: "営業 (Sales)" },
  { value: "viewer", label: "閲覧者 (Viewer)" },
];

/** フォームデータ */
interface UserFormData {
  email: string;
  name: string;
  password: string;
  role: User["role"];
  expires_at: string;
  is_active: boolean;
}

interface UserFormModalProps {
  /** 編集対象ユーザー（nullの場合は新規作成） */
  user: User | null;
  /** モーダルが開いているか */
  isOpen: boolean;
  /** 閉じるコールバック */
  onClose: () => void;
  /** 保存コールバック */
  onSave: (data: UserFormData, isEdit: boolean) => Promise<void>;
}

export default function UserFormModal({
  user,
  isOpen,
  onClose,
  onSave,
}: UserFormModalProps) {
  const isEdit = !!user;

  // フォーム状態
  const [formData, setFormData] = useState<UserFormData>({
    email: "",
    name: "",
    password: "",
    role: "viewer",
    expires_at: "",
    is_active: true,
  });
  const [errors, setErrors] = useState<Partial<Record<keyof UserFormData, string>>>({});
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState<string | null>(null);

  // ユーザーデータが変わったらフォームを初期化
  useEffect(() => {
    if (isOpen) {
      if (user) {
        setFormData({
          email: user.email,
          name: user.name,
          password: "", // 編集時はパスワード空
          role: user.role,
          expires_at: user.expires_at
            ? user.expires_at.split("T")[0]
            : "",
          is_active: user.is_active,
        });
      } else {
        setFormData({
          email: "",
          name: "",
          password: "",
          role: "viewer",
          expires_at: "",
          is_active: true,
        });
      }
      setErrors({});
      setSubmitError(null);
    }
  }, [isOpen, user]);

  // バリデーション
  const validate = useCallback((): boolean => {
    const newErrors: Partial<Record<keyof UserFormData, string>> = {};

    // メールアドレス
    if (!formData.email.trim()) {
      newErrors.email = "メールアドレスは必須です。";
    } else if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(formData.email)) {
      newErrors.email = "正しいメールアドレス形式で入力してください。";
    }

    // 名前
    if (!formData.name.trim()) {
      newErrors.name = "名前は必須です。";
    }

    // パスワード（新規作成時は必須、編集時は空なら変更なし）
    if (!isEdit && !formData.password) {
      newErrors.password = "パスワードは必須です。";
    } else if (formData.password && formData.password.length < 8) {
      newErrors.password = "パスワードは8文字以上で入力してください。";
    }

    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  }, [formData, isEdit]);

  // フォーム送信
  const handleSubmit = useCallback(
    async (e: FormEvent) => {
      e.preventDefault();
      setSubmitError(null);

      if (!validate()) return;

      setIsSubmitting(true);
      try {
        await onSave(formData, isEdit);
        onClose();
      } catch (err) {
        setSubmitError(
          err instanceof Error ? err.message : "保存に失敗しました。"
        );
      } finally {
        setIsSubmitting(false);
      }
    },
    [formData, isEdit, validate, onSave, onClose]
  );

  // フィールド更新ヘルパー
  const updateField = useCallback(
    <K extends keyof UserFormData>(key: K, value: UserFormData[K]) => {
      setFormData((prev) => ({ ...prev, [key]: value }));
      // エラーをクリア
      if (errors[key]) {
        setErrors((prev) => {
          const next = { ...prev };
          delete next[key];
          return next;
        });
      }
    },
    [errors]
  );

  if (!isOpen) return null;

  return (
    // モーダルオーバーレイ
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/40"
      onClick={onClose}
      role="dialog"
      aria-modal="true"
      aria-labelledby="user-form-title"
    >
      {/* モーダルコンテンツ（クリックイベントの伝播を停止） */}
      <div
        className="bg-white rounded-xl shadow-xl w-full max-w-lg mx-4 max-h-[90vh] overflow-y-auto"
        onClick={(e) => e.stopPropagation()}
      >
        {/* ヘッダー */}
        <div className="flex items-center justify-between px-6 py-4 border-b border-gray-200">
          <h2 id="user-form-title" className="text-lg font-semibold text-gray-900">
            {isEdit ? "ユーザー編集" : "ユーザー追加"}
          </h2>
          <button
            onClick={onClose}
            className="text-gray-400 hover:text-gray-600 transition-colors"
            aria-label="閉じる"
          >
            <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24" aria-hidden="true">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        {/* フォーム */}
        <form onSubmit={handleSubmit} noValidate>
          <div className="px-6 py-4 space-y-4">
            {/* 送信エラー */}
            {submitError && (
              <div className="p-3 bg-red-50 border border-red-200 rounded-lg text-sm text-red-700" role="alert">
                {submitError}
              </div>
            )}

            {/* メールアドレス */}
            <div>
              <label htmlFor="uf-email" className="block text-sm font-medium text-gray-700 mb-1">
                メールアドレス <span className="text-red-500">*</span>
              </label>
              <input
                id="uf-email"
                type="email"
                value={formData.email}
                onChange={(e) => updateField("email", e.target.value)}
                disabled={isSubmitting}
                className={`w-full px-3 py-2 border rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500
                  ${errors.email ? "border-red-300 bg-red-50" : "border-gray-300"}
                  disabled:bg-gray-50 disabled:text-gray-400 transition-colors`}
                placeholder="user@example.com"
              />
              {errors.email && (
                <p className="mt-1 text-xs text-red-600">{errors.email}</p>
              )}
            </div>

            {/* 名前 */}
            <div>
              <label htmlFor="uf-name" className="block text-sm font-medium text-gray-700 mb-1">
                名前 <span className="text-red-500">*</span>
              </label>
              <input
                id="uf-name"
                type="text"
                value={formData.name}
                onChange={(e) => updateField("name", e.target.value)}
                disabled={isSubmitting}
                className={`w-full px-3 py-2 border rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500
                  ${errors.name ? "border-red-300 bg-red-50" : "border-gray-300"}
                  disabled:bg-gray-50 disabled:text-gray-400 transition-colors`}
                placeholder="山田 太郎"
              />
              {errors.name && (
                <p className="mt-1 text-xs text-red-600">{errors.name}</p>
              )}
            </div>

            {/* パスワード */}
            <div>
              <label htmlFor="uf-password" className="block text-sm font-medium text-gray-700 mb-1">
                パスワード {!isEdit && <span className="text-red-500">*</span>}
              </label>
              <input
                id="uf-password"
                type="password"
                value={formData.password}
                onChange={(e) => updateField("password", e.target.value)}
                disabled={isSubmitting}
                className={`w-full px-3 py-2 border rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500
                  ${errors.password ? "border-red-300 bg-red-50" : "border-gray-300"}
                  disabled:bg-gray-50 disabled:text-gray-400 transition-colors`}
                placeholder={isEdit ? "変更する場合のみ入力（8文字以上）" : "8文字以上"}
              />
              {errors.password && (
                <p className="mt-1 text-xs text-red-600">{errors.password}</p>
              )}
              {isEdit && !errors.password && (
                <p className="mt-1 text-xs text-gray-400">
                  空のままにすると現在のパスワードが維持されます。
                </p>
              )}
            </div>

            {/* ロール */}
            <div>
              <label htmlFor="uf-role" className="block text-sm font-medium text-gray-700 mb-1">
                ロール
              </label>
              <select
                id="uf-role"
                value={formData.role}
                onChange={(e) => updateField("role", e.target.value as User["role"])}
                disabled={isSubmitting}
                className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm
                  focus:outline-none focus:ring-2 focus:ring-blue-500
                  disabled:bg-gray-50 disabled:text-gray-400 transition-colors"
              >
                {ROLE_OPTIONS.map((opt) => (
                  <option key={opt.value} value={opt.value}>
                    {opt.label}
                  </option>
                ))}
              </select>
            </div>

            {/* 有効期限 */}
            <div>
              <label htmlFor="uf-expires" className="block text-sm font-medium text-gray-700 mb-1">
                有効期限
              </label>
              <input
                id="uf-expires"
                type="date"
                value={formData.expires_at}
                onChange={(e) => updateField("expires_at", e.target.value)}
                disabled={isSubmitting}
                className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm
                  focus:outline-none focus:ring-2 focus:ring-blue-500
                  disabled:bg-gray-50 disabled:text-gray-400 transition-colors"
              />
              <p className="mt-1 text-xs text-gray-400">
                空にすると無期限になります。
              </p>
            </div>

            {/* 有効/無効トグル */}
            <div className="flex items-center justify-between">
              <label htmlFor="uf-active" className="text-sm font-medium text-gray-700">
                アカウント有効
              </label>
              <button
                id="uf-active"
                type="button"
                role="switch"
                aria-checked={formData.is_active}
                onClick={() => updateField("is_active", !formData.is_active)}
                disabled={isSubmitting}
                className={`relative inline-flex h-6 w-11 items-center rounded-full transition-colors
                  ${formData.is_active ? "bg-blue-600" : "bg-gray-300"}
                  disabled:opacity-50`}
              >
                <span
                  className={`inline-block h-4 w-4 rounded-full bg-white shadow transition-transform
                    ${formData.is_active ? "translate-x-6" : "translate-x-1"}`}
                />
              </button>
            </div>
          </div>

          {/* フッター（ボタン） */}
          <div className="flex items-center justify-end gap-3 px-6 py-4 border-t border-gray-200 bg-gray-50 rounded-b-xl">
            <button
              type="button"
              onClick={onClose}
              disabled={isSubmitting}
              className="px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-lg
                hover:bg-gray-50 disabled:opacity-50 transition-colors"
            >
              キャンセル
            </button>
            <button
              type="submit"
              disabled={isSubmitting}
              className="px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-lg
                hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2
                disabled:bg-blue-300 disabled:cursor-not-allowed
                transition-colors flex items-center gap-2"
            >
              {isSubmitting ? (
                <>
                  <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" />
                  <span>保存中...</span>
                </>
              ) : isEdit ? (
                "更新"
              ) : (
                "作成"
              )}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
