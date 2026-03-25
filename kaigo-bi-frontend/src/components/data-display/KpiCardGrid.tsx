"use client";

// ===================================================
// KPIカードグリッド
// 4枚横並び（レスポンシブ）
// ===================================================

interface KpiCardGridProps {
  children: React.ReactNode;
}

export default function KpiCardGrid({ children }: KpiCardGridProps) {
  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
      {children}
    </div>
  );
}
