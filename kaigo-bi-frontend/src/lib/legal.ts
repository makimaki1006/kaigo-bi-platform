// ===================================================
// 法務ページ共通の事業者情報
//
// 🔴 未確定の項目は "（未設定）" のままにしてある。
//    公開前に必ず実際の値へ差し替えること。
//    差し替え漏れがあると法務ページに「（未設定）」と表示されたまま公開される。
//    hasUnsetOperatorFields() が true の間は noindex + sitemap 対象外。
//
// 住所・電話番号の扱いは DISCLOSURE_MODE で切り替える（下記参照）。
// ===================================================

/**
 * 住所・電話番号の表示方式。
 *
 * 特定商取引法では、通信販売の広告に住所・電話番号の表示が必要だが、
 * 消費者庁「通信販売広告Q&A」Q15・Q17 により、
 * **請求があれば遅滞なく書面・電子メール等で提供する旨を広告に表示し、
 * 実際にその措置を講じていれば表示を省略できる**。
 *   https://www.no-trouble.caa.go.jp/qa/advertising.html
 *
 * "on-request" を選ぶ場合、次の運用が前提になる。
 *   - 請求先（メールアドレス）を明示すること
 *   - 請求があったら「遅滞なく」提供できる体制を実際に用意すること
 *     （「遅滞なく」= 申込みの意思決定に先立ち十分な時間的余裕をもって）
 *
 * バーチャルオフィスの住所を表示する場合は "display" にしたうえで、
 * 同Q&A Q18 の3要件（連絡先機能の合意 / 運営事業者が現住所と本人名義の
 * 電話番号を把握 / 確実に連絡が取れる状態）を満たす契約であることを確認する。
 * 郵便私書箱は住所の要件を満たさない。
 */
export const DISCLOSURE_MODE: "display" | "on-request" = "on-request";

/** 事業者情報 */
export const OPERATOR = {
  /** 事業者名（法人名または屋号）。氏名・名称は省略できない */
  name: "（未設定）",
  /** 運営責任者名 */
  representative: "（未設定）",
  /** 所在地。DISCLOSURE_MODE が "on-request" のときは表示しない */
  address: "（未設定）",
  /** 電話番号。DISCLOSURE_MODE が "on-request" のときは表示しない */
  phone: "（未設定）",
  /** 問い合わせ先メールアドレス。請求時開示の請求先も兼ねる */
  contactEmail: "（未設定）",
  /** 個人情報の相談窓口（未設定なら contactEmail と同じで可） */
  privacyContact: "（未設定）",
  /** 専属的合意管轄裁判所 */
  court: "（未設定）地方裁判所",
} as const;

/** 法務ページの最終改定日 */
export const LAST_UPDATED = "2026年8月7日";

/** サービス名 */
export const SERVICE_NAME = "kaigo-bi";

/**
 * 公開に必要な項目が埋まっているか。
 *
 * 判定対象は DISCLOSURE_MODE によって変わる。
 * "on-request" のときは住所・電話番号を表示しないため、
 * それらが未設定でも公開できる（請求があったときに開示できればよい）。
 */
export function hasUnsetOperatorFields(): boolean {
  const required: (keyof typeof OPERATOR)[] =
    DISCLOSURE_MODE === "on-request"
      ? ["name", "representative", "contactEmail", "privacyContact", "court"]
      : (Object.keys(OPERATOR) as (keyof typeof OPERATOR)[]);

  return required.some((k) => OPERATOR[k].includes("（未設定）"));
}

/** 住所・電話番号を画面に表示するか */
export function showsAddressAndPhone(): boolean {
  return DISCLOSURE_MODE === "display";
}
