"use client";

// ===================================================
// 法人検索入力コンポーネント
// デバウンス付きテキスト検索 + ドロップダウン選択
// DD支援・PMIシナジーページ共通で使用
// ===================================================

import { useState, useEffect, useRef, useCallback } from "react";
import { useApi } from "@/hooks/useApi";
import type { DdSearchResult } from "@/lib/types";

interface CorpSearchInputProps {
  /** ラベル表示 (例: "買い手法人") */
  label: string;
  /** 法人選択時コールバック */
  onSelect: (corpNumber: string, corpName: string) => void;
  /** プレースホルダーテキスト */
  placeholder?: string;
  /** 選択済み解除時コールバック */
  onClear?: () => void;
}

export default function CorpSearchInput({
  label,
  onSelect,
  placeholder = "法人名 / 法人番号を入力...",
  onClear,
}: CorpSearchInputProps) {
  const [query, setQuery] = useState("");
  const [debouncedQuery, setDebouncedQuery] = useState("");
  const [selectedCorp, setSelectedCorp] = useState<{
    name: string;
    number: string;
  } | null>(null);
  const [isOpen, setIsOpen] = useState(false);
  const wrapperRef = useRef<HTMLDivElement>(null);

  // デバウンス: 300ms後にクエリをセット
  useEffect(() => {
    const timer = setTimeout(() => {
      setDebouncedQuery(query);
    }, 300);
    return () => clearTimeout(timer);
  }, [query]);

  // クリック外でドロップダウンを閉じる
  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (
        wrapperRef.current &&
        !wrapperRef.current.contains(event.target as Node)
      ) {
        setIsOpen(false);
      }
    }
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  // 検索API呼び出し（2文字以上で発火）
  const searchEndpoint =
    debouncedQuery.length >= 2 && !selectedCorp ? "/api/dd/search" : null;
  const { data: searchResults, isLoading: isSearching } = useApi<
    DdSearchResult[]
  >(searchEndpoint, { q: debouncedQuery });

  // 法人選択ハンドラ
  const handleSelect = useCallback(
    (result: DdSearchResult) => {
      setSelectedCorp({
        name: result.corp_name,
        number: result.corp_number,
      });
      setQuery("");
      setIsOpen(false);
      onSelect(result.corp_number, result.corp_name);
    },
    [onSelect]
  );

  // 選択クリア
  const handleClear = useCallback(() => {
    setSelectedCorp(null);
    setQuery("");
    setDebouncedQuery("");
    onClear?.();
  }, [onClear]);

  // 選択済み状態の表示
  if (selectedCorp) {
    return (
      <div>
        <label className="block text-xs font-medium text-gray-600 mb-1">
          {label}
        </label>
        <div className="flex items-center gap-2 px-3 py-2 bg-brand-50 border border-brand-200 rounded-lg">
          <span className="flex-1 text-sm font-medium text-brand-800">
            {selectedCorp.name}
          </span>
          <span className="text-[10px] font-mono text-brand-500">
            {selectedCorp.number}
          </span>
          <button
            onClick={handleClear}
            className="ml-1 w-5 h-5 flex items-center justify-center rounded-full bg-brand-200 text-brand-600 hover:bg-brand-300 transition-colors"
            aria-label={`${label}の選択を解除`}
          >
            <svg
              className="w-3 h-3"
              fill="none"
              stroke="currentColor"
              viewBox="0 0 24 24"
              aria-hidden="true"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M6 18L18 6M6 6l12 12"
              />
            </svg>
          </button>
        </div>
      </div>
    );
  }

  return (
    <div ref={wrapperRef} className="relative">
      <label className="block text-xs font-medium text-gray-600 mb-1">
        {label}
      </label>
      <input
        type="text"
        value={query}
        onChange={(e) => {
          setQuery(e.target.value);
          setIsOpen(true);
        }}
        onFocus={() => {
          if (searchResults && searchResults.length > 0) setIsOpen(true);
        }}
        placeholder={placeholder}
        className="w-full px-3 py-2 text-sm border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-brand-500 focus:border-brand-500"
      />

      {/* ドロップダウン */}
      {isOpen && debouncedQuery.length >= 2 && (
        <div className="absolute z-50 top-full left-0 right-0 mt-1 bg-white border border-gray-200 rounded-lg shadow-lg max-h-64 overflow-y-auto">
          {isSearching && (
            <div className="px-3 py-4 text-center text-sm text-gray-400">
              検索中...
            </div>
          )}
          {!isSearching && searchResults && searchResults.length === 0 && (
            <div className="px-3 py-4 text-center text-sm text-gray-400">
              該当する法人が見つかりません
            </div>
          )}
          {!isSearching &&
            searchResults &&
            searchResults.map((result) => (
              <button
                key={result.corp_number}
                onClick={() => handleSelect(result)}
                className="w-full text-left px-3 py-2.5 hover:bg-gray-50 transition-colors border-b border-gray-100 last:border-b-0"
              >
                <div className="text-sm font-medium text-gray-900">
                  {result.corp_name}
                </div>
                <div className="flex items-center gap-3 mt-0.5 text-[10px] text-gray-400">
                  <span className="font-mono">{result.corp_number}</span>
                  <span>{result.facility_count}施設</span>
                  <span>{Math.round(result.total_staff)}人</span>
                </div>
              </button>
            ))}
        </div>
      )}
    </div>
  );
}
