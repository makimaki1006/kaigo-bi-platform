import type { Config } from "tailwindcss";

const config: Config = {
  content: [
    "./src/**/*.{js,ts,jsx,tsx,mdx}",
    // Tremor コンポーネントのパス（段階的脱却中）
    "./node_modules/@tremor/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      fontFamily: {
        sans: [
          "Inter",
          "Noto Sans JP",
          "system-ui",
          "-apple-system",
          "sans-serif",
        ],
      },
      colors: {
        // ブランドカラー（indigo系）
        brand: {
          50: "#eef2ff",
          100: "#e0e7ff",
          200: "#c7d2fe",
          300: "#a5b4fc",
          400: "#818cf8",
          500: "#6366f1",
          600: "#4f46e5",
          700: "#4338ca",
          800: "#3730a3",
          900: "#312e81",
          950: "#1e1b4b",
        },
        // サーフェスカラー（背景レイヤー）
        surface: {
          DEFAULT: "#f8fafc",
          raised: "#ffffff",
          sunken: "#f1f5f9",
          overlay: "rgba(15, 23, 42, 0.5)",
        },
        // アクセントカラー
        accent: {
          success: "#059669",
          "success-light": "#d1fae5",
          warning: "#d97706",
          "warning-light": "#fef3c7",
          danger: "#dc2626",
          "danger-light": "#fee2e2",
          info: "#0284c7",
          "info-light": "#e0f2fe",
        },
        // サイドバー用（brand-900ベース）
        sidebar: {
          DEFAULT: "#312e81",
          hover: "#3730a3",
          active: "#1e1b4b",
        },
      },
      boxShadow: {
        card: "0 1px 3px 0 rgba(0, 0, 0, 0.04), 0 1px 2px -1px rgba(0, 0, 0, 0.03)",
        "card-hover":
          "0 10px 15px -3px rgba(0, 0, 0, 0.06), 0 4px 6px -4px rgba(0, 0, 0, 0.04)",
        panel:
          "0 4px 6px -1px rgba(0, 0, 0, 0.05), 0 2px 4px -2px rgba(0, 0, 0, 0.03)",
      },
      // タイポグラフィユーティリティ
      fontSize: {
        "display-lg": [
          "2.25rem",
          { lineHeight: "2.5rem", fontWeight: "700" },
        ],
        "display-md": [
          "1.875rem",
          { lineHeight: "2.25rem", fontWeight: "700" },
        ],
        "display-sm": [
          "1.5rem",
          { lineHeight: "2rem", fontWeight: "600" },
        ],
        "heading-lg": [
          "1.25rem",
          { lineHeight: "1.75rem", fontWeight: "600" },
        ],
        "heading-md": [
          "1rem",
          { lineHeight: "1.5rem", fontWeight: "600" },
        ],
        "heading-sm": [
          "0.875rem",
          { lineHeight: "1.25rem", fontWeight: "600" },
        ],
        "body-lg": [
          "1rem",
          { lineHeight: "1.5rem", fontWeight: "400" },
        ],
        "body-md": [
          "0.875rem",
          { lineHeight: "1.25rem", fontWeight: "400" },
        ],
        "body-sm": [
          "0.75rem",
          { lineHeight: "1rem", fontWeight: "400" },
        ],
        "label-md": [
          "0.75rem",
          { lineHeight: "1rem", fontWeight: "500", letterSpacing: "0.025em" },
        ],
        "label-sm": [
          "0.625rem",
          { lineHeight: "0.875rem", fontWeight: "600", letterSpacing: "0.05em" },
        ],
      },
    },
  },
  plugins: [],
};

export default config;
