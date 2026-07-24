"use client";

// ===================================================
// 施設詳細ページ
// /facility?id=<事業所番号>
// 施設マスタ等からのリンク先。1施設を深掘りする専用ページ
// ===================================================

import { Suspense, useMemo } from "react";
import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import { useApi } from "@/hooks/useApi";
import type { FacilityRow } from "@/lib/types";
import FacilityDetailPanel from "@/components/data-display/FacilityDetailPanel";
import ApiErrorBanner from "@/components/common/ApiErrorBanner";

function FacilityDetailContent() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const id = searchParams.get("id") ?? "";

  const { data, error, isLoading } = useApi<{ facility: FacilityRow }>(
    id ? `/api/facilities/${id}` : null
  );

  const facility = useMemo(() => data?.facility ?? null, [data]);

  if (!id) {
    return (
      <div className="p-8 text-center text-gray-500 text-sm">
        施設が指定されていません。
        <Link href="/facilities" className="text-brand-500 hover:underline ml-2">
          施設マスタから探す →
        </Link>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      {/* パンくず + タイトル */}
      <div className="flex items-center justify-between">
        <div>
          <p className="text-xs text-gray-400">
            <Link href="/facilities" className="hover:text-brand-500">施設マスタ</Link>
            {" / "}施設詳細
          </p>
          <h1 className="text-heading-lg text-gray-900 mt-1">
            {facility?.jigyosho_name ?? (isLoading ? "読み込み中..." : "施設詳細")}
          </h1>
          {facility?.corp_name && (
            <p className="text-body-md text-gray-500 mt-1">
              {facility.corp_name}
              {facility.corp_number && (
                <Link
                  href={`/corp?number=${facility.corp_number}`}
                  className="ml-2 text-brand-500 hover:underline text-sm"
                >
                  法人ページを見る →
                </Link>
              )}
            </p>
          )}
        </div>
      </div>

      <ApiErrorBanner error={error} />

      {/* 詳細パネル（既存コンポーネントを全画面利用） */}
      <FacilityDetailPanel
        facility={facility}
        loading={isLoading}
        onClose={() => router.back()}
      />
    </div>
  );
}

export default function FacilityDetailPage() {
  return (
    <Suspense fallback={<div className="text-gray-400 text-sm p-8">読み込み中...</div>}>
      <FacilityDetailContent />
    </Suspense>
  );
}
