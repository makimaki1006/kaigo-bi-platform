// ===================================================
// OGP画像（動的生成）
//
// Next.js の ImageResponse でビルド時に生成する。
// 静的PNGを置く代わりにコードで持つことで、
// キャッチコピーや件数を変えたいときにコードだけ直せば済む。
//
// 件数は表示のたびにDBを引くわけにはいかないので定数で持つ。
// データ更新で大きく変わったらここも直すこと（現在 223,103施設）。
// ===================================================

import { ImageResponse } from "next/og";

export const runtime = "edge";
export const alt = "kaigo-bi — 公開情報を、介護業界の次の判断につなげる";
export const size = { width: 1200, height: 630 };
export const contentType = "image/png";

/** 収録施設数。scripts の集計と一致させる */
const FACILITY_COUNT = "223,103";

export default function OpengraphImage() {
  return new ImageResponse(
    (
      <div
        style={{
          width: "100%",
          height: "100%",
          display: "flex",
          flexDirection: "column",
          justifyContent: "space-between",
          background: "linear-gradient(135deg, #1e1b4b 0%, #312e81 55%, #4338ca 100%)",
          padding: "72px 80px",
          fontFamily: "sans-serif",
        }}
      >
        {/* 上段: サービス名 */}
        <div style={{ display: "flex", alignItems: "center", gap: 16 }}>
          <div
            style={{
              width: 14,
              height: 44,
              borderRadius: 999,
              background: "#a5b4fc",
              display: "flex",
            }}
          />
          <div style={{ display: "flex", fontSize: 44, fontWeight: 700, color: "#ffffff" }}>
            kaigo-bi
          </div>
        </div>

        {/* 中段: キャッチコピー */}
        <div style={{ display: "flex", flexDirection: "column", gap: 18 }}>
          <div
            style={{
              display: "flex",
              fontSize: 62,
              fontWeight: 700,
              color: "#ffffff",
              lineHeight: 1.25,
              letterSpacing: -1,
            }}
          >
            公開情報を、
          </div>
          <div
            style={{
              display: "flex",
              fontSize: 62,
              fontWeight: 700,
              color: "#ffffff",
              lineHeight: 1.25,
              letterSpacing: -1,
            }}
          >
            介護業界の次の判断につなげる。
          </div>
        </div>

        {/* 下段: 収録規模 */}
        <div style={{ display: "flex", alignItems: "flex-end", gap: 40 }}>
          <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
            <div style={{ display: "flex", fontSize: 22, color: "#c7d2fe" }}>収録施設</div>
            <div
              style={{
                display: "flex",
                fontSize: 46,
                fontWeight: 700,
                color: "#ffffff",
              }}
            >
              {FACILITY_COUNT}
            </div>
          </div>
          <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
            <div style={{ display: "flex", fontSize: 22, color: "#c7d2fe" }}>対象</div>
            <div
              style={{
                display: "flex",
                fontSize: 46,
                fontWeight: 700,
                color: "#ffffff",
              }}
            >
              全国47都道府県
            </div>
          </div>
        </div>
      </div>
    ),
    size
  );
}
