"use client";

// ===================================================
// カスタムマルチセレクトドロップダウン
// Tremor MultiSelect の代替（headlessui v2互換問題の回避）
// ===================================================

import { useState, useRef, useEffect, useCallback } from "react";

interface MultiSelectDropdownProps {
  /** 選択中の値 */
  value: string[];
  /** 変更コールバック */
  onValueChange: (values: string[]) => void;
  /** プレースホルダーテキスト */
  placeholder?: string;
  /** 選択肢 */
  options: { value: string; label: string }[];
}

export default function MultiSelectDropdown({
  value,
  onValueChange,
  placeholder = "選択...",
  options,
}: MultiSelectDropdownProps) {
  const [isOpen, setIsOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  // 外側クリックで閉じる
  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        setIsOpen(false);
      }
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, []);

  const toggle = useCallback(
    (optionValue: string) => {
      if (value.includes(optionValue)) {
        onValueChange(value.filter((v) => v !== optionValue));
      } else {
        onValueChange([...value, optionValue]);
      }
    },
    [value, onValueChange]
  );

  const displayText =
    value.length > 0 ? `${value.length}件選択中` : placeholder;

  return (
    <div ref={ref} className="relative">
      <button
        type="button"
        onClick={() => setIsOpen(!isOpen)}
        className="w-full flex items-center justify-between px-3 py-2 text-sm border border-gray-300 rounded-lg bg-white hover:border-gray-400 focus:outline-none focus:ring-2 focus:ring-brand-500 focus:border-brand-500 transition-colors"
      >
        <span
          className={value.length > 0 ? "text-gray-900" : "text-gray-400"}
        >
          {displayText}
        </span>
        <svg
          className={`w-4 h-4 text-gray-400 transition-transform ${isOpen ? "rotate-180" : ""}`}
          fill="none"
          stroke="currentColor"
          viewBox="0 0 24 24"
          aria-hidden="true"
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth={2}
            d="M19 9l-7 7-7-7"
          />
        </svg>
      </button>

      {isOpen && (
        <div
          className="absolute z-50 mt-1 w-full max-h-60 overflow-auto bg-white border border-gray-200 rounded-lg shadow-lg"
          role="listbox"
          aria-multiselectable="true"
        >
          {options.map((opt) => {
            const selected = value.includes(opt.value);
            return (
              <button
                key={opt.value}
                type="button"
                role="option"
                aria-selected={selected}
                onClick={() => toggle(opt.value)}
                className={`w-full flex items-center gap-2 px-3 py-2 text-sm text-left hover:bg-brand-50 transition-colors ${
                  selected ? "bg-brand-50 text-brand-700" : "text-gray-700"
                }`}
              >
                <span
                  className={`flex-shrink-0 w-4 h-4 rounded border ${
                    selected
                      ? "bg-brand-500 border-brand-500 text-white"
                      : "border-gray-300"
                  } flex items-center justify-center`}
                >
                  {selected && (
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
                        strokeWidth={3}
                        d="M5 13l4 4L19 7"
                      />
                    </svg>
                  )}
                </span>
                <span>{opt.label}</span>
              </button>
            );
          })}
        </div>
      )}
    </div>
  );
}
