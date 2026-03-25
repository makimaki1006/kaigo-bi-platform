"use client";

// ===================================================
// 削除確認モーダル
// ユーザー削除前の確認ダイアログ
// ===================================================

import { useState, useCallback } from "react";

interface DeleteConfirmModalProps {
  /** 削除対象ユーザー名 */
  userName: string;
  /** モーダルが開いているか */
  isOpen: boolean;
  /** 閉じるコールバック */
  onClose: () => void;
  /** 削除実行コールバック */
  onConfirm: () => Promise<void>;
}

export default function DeleteConfirmModal({
  userName,
  isOpen,
  onClose,
  onConfirm,
}: DeleteConfirmModalProps) {
  const [isDeleting, setIsDeleting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleDelete = useCallback(async () => {
    setError(null);
    setIsDeleting(true);
    try {
      await onConfirm();
      onClose();
    } catch (err) {
      setError(
        err instanceof Error ? err.message : "削除に失敗しました。"
      );
    } finally {
      setIsDeleting(false);
    }
  }, [onConfirm, onClose]);

  if (!isOpen) return null;

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/40"
      onClick={onClose}
      role="dialog"
      aria-modal="true"
      aria-labelledby="delete-confirm-title"
    >
      <div
        className="bg-white rounded-xl shadow-xl w-full max-w-sm mx-4"
        onClick={(e) => e.stopPropagation()}
      >
        {/* アイコンとメッセージ */}
        <div className="px-6 pt-6 pb-4 text-center">
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
                d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"
              />
            </svg>
          </div>
          <h3 id="delete-confirm-title" className="text-lg font-semibold text-gray-900 mb-2">
            ユーザーを削除
          </h3>
          <p className="text-sm text-gray-500">
            <span className="font-medium text-gray-700">{userName}</span> を削除しますか？
            <br />
            この操作は取り消せません。
          </p>

          {/* エラーメッセージ */}
          {error && (
            <div className="mt-4 p-3 bg-red-50 border border-red-200 rounded-lg text-sm text-red-700" role="alert">
              {error}
            </div>
          )}
        </div>

        {/* ボタン */}
        <div className="flex items-center gap-3 px-6 pb-6">
          <button
            type="button"
            onClick={onClose}
            disabled={isDeleting}
            className="flex-1 px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-lg
              hover:bg-gray-50 disabled:opacity-50 transition-colors"
          >
            キャンセル
          </button>
          <button
            type="button"
            onClick={handleDelete}
            disabled={isDeleting}
            className="flex-1 px-4 py-2 text-sm font-medium text-white bg-red-600 rounded-lg
              hover:bg-red-700 focus:outline-none focus:ring-2 focus:ring-red-500 focus:ring-offset-2
              disabled:bg-red-300 disabled:cursor-not-allowed
              transition-colors flex items-center justify-center gap-2"
          >
            {isDeleting ? (
              <>
                <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" />
                <span>削除中...</span>
              </>
            ) : (
              "削除"
            )}
          </button>
        </div>
      </div>
    </div>
  );
}
