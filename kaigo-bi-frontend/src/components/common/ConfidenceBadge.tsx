// ===================================================
// 信頼区間・推定精度バッジ
// 推定値の信頼度を視覚的に表示するコンポーネント
// ===================================================

interface ConfidenceBadgeProps {
  level: "high" | "medium" | "low";
}

const config = {
  high: { label: "高信頼度", color: "bg-green-100 text-green-700", stars: "\u2605\u2605\u2605" },
  medium: { label: "中信頼度", color: "bg-yellow-100 text-yellow-700", stars: "\u2605\u2605\u2606" },
  low: { label: "低信頼度", color: "bg-red-100 text-red-700", stars: "\u2605\u2606\u2606" },
};

export default function ConfidenceBadge({ level }: ConfidenceBadgeProps) {
  const c = config[level];
  return (
    <span
      className={`inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium ${c.color}`}
      title={`データ信頼度: ${c.label}`}
    >
      {c.stars} {c.label}
    </span>
  );
}
