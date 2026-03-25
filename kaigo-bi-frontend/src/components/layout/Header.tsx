"use client";

// ===================================================
// ヘッダーコンポーネント
// ガラスモーフィズム + データステータスバッジ
// ===================================================

import { useApi } from "@/hooks/useApi";
import { PREFECTURES } from "@/lib/constants";
import type { DataMeta } from "@/lib/types";

export default function Header() {
  // メタ情報をAPIから取得（/api/meta）
  const { data: meta } = useApi<DataMeta>("/api/meta");

  return (
    <header className="h-16 backdrop-blur-xl bg-white/80 border-b border-gray-200/60 flex items-center justify-between px-6 sticky top-0 z-40">
      {/* タイトル */}
      <h2 className="text-heading-sm text-gray-700">
        介護・福祉 戦略コンサルティング BI
      </h2>

      {/* データステータスバッジ */}
      <div className="flex items-center gap-4">
        {meta ? (
          <div className="flex items-center gap-3">
            {/* ステータスバッジ */}
            <div className="flex items-center gap-2 px-3 py-1.5 bg-emerald-50 border border-emerald-200/60 rounded-full">
              <span className="inline-block w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse" />
              <span className="text-body-sm font-medium text-emerald-700 tabular-nums">
                {meta.total_count?.toLocaleString("ja-JP")}施設
              </span>
            </div>
            <span className="text-body-sm text-gray-400 tabular-nums">
              {/* 正規の47都道府県リストとの交差で正確な数を表示 */}
              {meta.prefectures
                ? meta.prefectures.filter((p: string) => PREFECTURES.includes(p)).length
                : 0}都道府県
            </span>
          </div>
        ) : (
          <div className="flex items-center gap-2 px-3 py-1.5 bg-gray-50 border border-gray-200/60 rounded-full">
            <span className="inline-block w-1.5 h-1.5 rounded-full shimmer-dark bg-gray-300" />
            <span className="text-body-sm text-gray-400">
              データ読み込み中...
            </span>
          </div>
        )}
      </div>
    </header>
  );
}
