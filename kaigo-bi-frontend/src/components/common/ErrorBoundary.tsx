"use client";

// ===================================================
// ErrorBoundary コンポーネント
// チャートやページセクションのレンダリングエラーをキャッチし、
// アプリ全体のクラッシュを防止する
// ===================================================

import React from "react";

interface Props {
  children: React.ReactNode;
  /** エラー時に表示するカスタムフォールバック */
  fallback?: React.ReactNode;
}

interface State {
  hasError: boolean;
  error?: Error;
}

export default class ErrorBoundary extends React.Component<Props, State> {
  constructor(props: Props) {
    super(props);
    this.state = { hasError: false };
  }

  static getDerivedStateFromError(error: Error): State {
    return { hasError: true, error };
  }

  componentDidCatch(error: Error, errorInfo: React.ErrorInfo) {
    console.error("ErrorBoundary caught:", error, errorInfo);
  }

  render() {
    if (this.state.hasError) {
      return (
        this.props.fallback || (
          <div className="p-6 bg-red-50 border border-red-200 rounded-lg text-center">
            <p className="text-red-600 font-medium">
              表示エラーが発生しました
            </p>
            <p className="text-sm text-red-400 mt-1">
              ページを再読み込みしてください
            </p>
            <button
              onClick={() => this.setState({ hasError: false })}
              className="mt-3 px-4 py-2 bg-red-100 text-red-700 rounded hover:bg-red-200 text-sm"
            >
              再試行
            </button>
          </div>
        )
      );
    }
    return this.props.children;
  }
}
