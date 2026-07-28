// ===================================================
// 定数定義
// ===================================================

/** 47都道府県リスト */
export const PREFECTURES: string[] = [
  "北海道",
  "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
  "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
  "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県",
  "岐阜県", "静岡県", "愛知県",
  "三重県", "滋賀県", "京都府", "大阪府", "兵庫県", "奈良県", "和歌山県",
  "鳥取県", "島根県", "岡山県", "広島県", "山口県",
  "徳島県", "香川県", "愛媛県", "高知県",
  "福岡県", "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県",
  "沖縄県",
];

/** サービスコード→名前マッピング（3桁コード、バックエンドのサービスコードと一致） */
export const SERVICE_TYPES: Record<string, string> = {
  "110": "訪問介護",
  "120": "訪問入浴介護",
  "130": "訪問看護",
  "140": "訪問リハビリテーション",
  "150": "通所介護",
  "155": "通所介護（療養通所型）",
  "160": "通所リハビリテーション",
  "170": "福祉用具貸与",
  "210": "短期入所生活介護",
  "220": "短期入所療養介護（老健）",
  "230": "短期入所療養介護（病院）",
  "320": "認知症対応型共同生活介護",
  "331": "特定施設（有料老人ホーム）",
  "332": "特定施設（軽費老人ホーム）",
  "334": "特定施設（サービス付き高齢者向け住宅）",
  "335": "特定施設（有料・外部サービス利用型）",
  "336": "特定施設（軽費・外部サービス利用型）",
  "337": "特定施設（サ高住・外部サービス利用型）",
  "361": "地域密着型特定施設（有料）",
  "362": "地域密着型特定施設（軽費）",
  "364": "地域密着型特定施設（サ高住）",
  "410": "特定福祉用具販売",
  "430": "居宅介護支援",
  "510": "介護老人福祉施設",
  "520": "介護老人保健施設",
  "530": "介護療養型医療施設",
  "540": "地域密着型介護老人福祉施設",
  "550": "介護医療院",
  "551": "短期入所（介護医療院）",
  "710": "夜間対応型訪問介護",
  "720": "認知症対応型通所介護",
  "730": "小規模多機能型居宅介護",
  "760": "定期巡回・随時対応型訪問介護看護",
  "770": "看護小規模多機能型居宅介護",
  "780": "地域密着型通所介護",
  // 介護予防サービス
  "111": "介護予防訪問入浴介護",
  "131": "介護予防訪問看護",
  "141": "介護予防訪問リハビリテーション",
  "151": "介護予防通所介護",
  "161": "介護予防通所リハビリテーション",
  "171": "介護予防福祉用具貸与",
  "211": "介護予防短期入所生活介護",
  "221": "介護予防短期入所療養介護（老健）",
  "231": "介護予防短期入所療養介護（病院）",
  "321": "介護予防認知症対応型共同生活介護",
  "341": "介護予防特定施設入居者生活介護",
  "411": "特定介護予防福祉用具販売",
  "431": "介護予防支援",
  "731": "介護予防小規模多機能型居宅介護",
};

/** 法人種別リスト */
// DBの corp_type 列に実際に格納されている値と一致させること。
// 値は scripts/turso_helpers.py の classify_corp_type / _CORP_TYPE_MAPPING が生成する。
// ここがズレると WHERE corp_type IN (...) が0件になる（例: 「株式会社」単体はDBに無く
// 「株式会社・有限会社等」で格納されている）。
export const CORP_TYPES: string[] = [
  "社会福祉法人",
  "医療法人",
  "社会医療法人",
  "株式会社・有限会社等",
  "NPO法人",
  "社団法人",
  "財団法人",
  "地方公共団体",
  "その他法人",
  "不明",
];

/** テーブルのデフォルトページサイズ */
export const DEFAULT_PAGE_SIZE = 20;

/** デバウンス遅延（ms） */
export const DEBOUNCE_DELAY = 300;

/** チャートカラーパレット（コンサルティング品質の調和配色） */
export const CHART_COLORS = [
  "#4f46e5", // brand-600 (indigo)
  "#0284c7", // sky-600
  "#059669", // emerald-600
  "#d97706", // amber-600
  "#dc2626", // red-600
  "#7c3aed", // violet-600
  "#0891b2", // cyan-600
  "#65a30d", // lime-600
  "#ea580c", // orange-600
  "#db2777", // pink-600
  "#2563eb", // blue-600
  "#9333ea", // purple-600
];

/** 課金プランの階層（バックエンド auth/plan.rs と対応） */
export const PLAN_LEVELS: Record<string, number> = {
  free: 0,
  standard: 1,
  pro: 2,
  ma: 3,
};

/** プラン表示名 */
export const PLAN_NAMES: Record<string, string> = {
  free: "フリー",
  standard: "スタンダード",
  pro: "プロ",
  ma: "M&A",
};

/** サイドバーナビゲーション定義 */
export interface NavItem {
  label: string;
  href: string;
  disabled?: boolean;
  group?: string;
  /** このページの利用に必要な最低プラン（未指定はfree=全員） */
  minPlan?: "free" | "standard" | "pro" | "ma";
  /** adminロールのみ表示 */
  adminOnly?: boolean;
}

/** ワークスペース定義（ペルソナ切替） */
export type WorkspaceId = "all" | "bi" | "management" | "sales" | "ma";

export interface Workspace {
  id: WorkspaceId;
  label: string;
  /** 表示するナビグループ（「市場を知る」は全ワークスペース共通） */
  groups: string[];
  /** ワークスペースのホームパス */
  home: string;
}

export const WORKSPACES: Workspace[] = [
  { id: "all", label: "すべての機能", groups: ["市場を知る", "経営分析", "リスト作成", "M&A"], home: "/dashboard" },
  { id: "bi", label: "業界データを見る", groups: ["市場を知る"], home: "/dashboard" },
  { id: "management", label: "経営支援", groups: ["市場を知る", "経営分析"], home: "/home/management" },
  { id: "sales", label: "営業支援", groups: ["市場を知る", "リスト作成"], home: "/home/sales" },
  { id: "ma", label: "M&A", groups: ["市場を知る", "M&A"], home: "/home/ma" },
];

/** ワークスペース保存用localStorageキー */
export const WORKSPACE_STORAGE_KEY = "kaigo_bi_workspace";

/**
 * ナビゲーション（ターゲット別グルーピング）
 * minPlanはバックエンドのルートゲート（routes/mod.rs）と一致させること
 */
export const NAV_ITEMS: NavItem[] = [
  // 市場を知る（全ターゲット共通）
  { label: "ダッシュボード", href: "/dashboard", group: "市場を知る" },
  { label: "市場構造", href: "/market", group: "市場を知る", minPlan: "standard" },
  { label: "トレンド分析", href: "/trends", group: "市場を知る", minPlan: "standard" },
  { label: "データインサイト", href: "/insights", group: "市場を知る", minPlan: "standard" },
  // 経営分析（介護事業者向け）
  { label: "経営ホーム", href: "/home/management", group: "経営分析", minPlan: "standard" },
  { label: "人材分析", href: "/workforce", group: "経営分析", minPlan: "standard" },
  { label: "採用天気予報", href: "/hiring-weather", group: "経営分析", minPlan: "standard" },
  { label: "収益構造", href: "/revenue", group: "経営分析", minPlan: "standard" },
  { label: "賃金分析", href: "/salary", group: "経営分析", minPlan: "standard" },
  { label: "経営品質", href: "/quality", group: "経営分析", minPlan: "standard" },
  { label: "ベンチマーク", href: "/benchmark", group: "経営分析", minPlan: "standard" },
  { label: "コスト推定", href: "/cost-estimation", group: "経営分析", minPlan: "standard" },
  // リスト作成（営業企業向け）
  { label: "営業ホーム", href: "/home/sales", group: "リスト作成", minPlan: "standard" },
  { label: "施設マスタ", href: "/facilities", group: "リスト作成", minPlan: "standard" },
  { label: "リスト生成", href: "/list-export", group: "リスト作成", minPlan: "pro" },
  // M&A
  { label: "M&Aホーム", href: "/home/ma", group: "M&A", minPlan: "ma" },
  { label: "法人グループ", href: "/corp-group", group: "M&A", minPlan: "standard" },
  { label: "法人比較", href: "/corp-compare", group: "M&A", minPlan: "standard" },
  { label: "成長性分析", href: "/growth", group: "M&A", minPlan: "standard" },
  { label: "財務健全度", href: "/financial-health", group: "M&A", minPlan: "standard" },
  { label: "サービスポートフォリオ", href: "/service-portfolio", group: "M&A", minPlan: "standard" },
  { label: "M&Aスクリーニング", href: "/ma-screening", group: "M&A", minPlan: "ma" },
  { label: "DD支援", href: "/due-diligence", group: "M&A", minPlan: "ma" },
  { label: "PMIシナジー", href: "/pmi-synergy", group: "M&A", minPlan: "ma" },
  // 運用（admin専用）
  { label: "施設ヘルスチェック", href: "/health-check", group: "運用", adminOnly: true },
  { label: "データ品質", href: "/data-quality", group: "運用", adminOnly: true },
];
