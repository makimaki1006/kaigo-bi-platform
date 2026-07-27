# kaigo-bi SEO公開サイト 実装指示書

作成日: 2026-07-27  
対象: `kaigo-bi-frontend`（Next.js 14 App Router）  
本番URL（現状）: `https://kaigo-bi.onrender.com`  
目的: 検索流入から無料登録・有料プラン検討につながる公開サイトを構築する

---

## 0. この作業の最重要方針

今回の目的は、検索順位だけを上げることではなく、**検索した見込み顧客にkaigo-biの価値と信頼性を伝え、無料登録または料金ページへ送客すること**である。

以下を厳守すること。

1. SEO用の未ログイン公開サイトと、ログイン後のSaaS画面を明確に分離する
2. 公開ページは、JavaScript実行前のHTMLだけでも主要な本文・見出し・リンクを読めるようにする
3. 公開データの件数・対象範囲・更新頻度を誇張しない
4. 「223,103施設」と表現せず、原則として「223,103施設・サービスレコード」と表現する
5. 財務AI抽出については、現時点の対象が24施設、52レコードのパイロットであることを隠さない
6. ログイン後の詳細データやAPIをSEO目的で公開しない
7. 今回はログイン後のスクレイピング対策を実装しない。別プロジェクトとして扱う
8. 既存の認証、課金、ダッシュボード、API契約を壊さない
9. 実装前に現在の作業ツリーを確認し、既存の未コミット変更を上書きしない
10. 数値や機能をコード・既存ドキュメントで確認できない場合は、推測で広告文を作らずTODOとして残す

---

## 1. 現状と問題

### 現在の構成

- Next.js 14 App Router
- ルートレイアウト: `kaigo-bi-frontend/src/app/layout.tsx`
- `/` は `next.config.mjs` で `/dashboard` に一時リダイレクト
- ルートレイアウト全体が `AuthProvider`、`AppShell`、`ProtectedRoute`の影響下にある
- 公開扱いのパスは現在、`AppShell.tsx`の以下のみ
  - `/login`
  - `/signup`
  - `/pricing`
  - `/forgot-password`
  - `/reset-password`
- ルートmetadataは全ページ共通の簡易設定
- `sitemap.xml`、`robots.txt`、公開トップページが存在しない
- `pricing/page.tsx`はClient Componentで、公開ページとしての個別metadataがない

### 解決する問題

- サービス名や介護BI関連語で検索しても、価値を説明する公開ページがない
- 未ログイン訪問者が最初に見るランディングページがない
- 検索エンジンへ公開すべきページと、インデックスさせないアプリ画面が未整理
- 料金、データ根拠、指標の注意事項が検索可能なHTMLとして十分に公開されていない
- トップページから登録までのコンバージョン経路がない

---

## 2. スコープ

### 今回実装する

- 公開トップページ
- ペルソナ別の機能ページ3本
- データと信頼性の説明ページ
- 指標・データ取扱方針ページ
- 料金ページのSEO対応
- 公開サイト共通ヘッダー・フッター
- ページ単位metadata
- canonical URL
- `sitemap.xml`
- `robots.txt`
- 構造化データ
- ログイン系・アプリ系ページの`noindex`
- CTA計測のためのイベント設計
- SEO・HTML・リンク・ビルドの検証

### 今回実装しない

- 施設ごとの公開ページ
- 223,103レコードを使った大量の自動生成ページ
- 都道府県×サービスの大量ページ
- ブログCMS
- ログイン後のスクレイピング対策
- WAF、Bot Management、端末識別
- APIレート制限の変更
- 独自ドメイン購入・DNS変更
- Google Search Consoleの所有権確認そのもの
- GA4等の本番アカウント作成
- バックエンドやDBの変更

---

## 3. URL設計

以下を公開・index対象にする。

| URL | 役割 | 主CTA |
|---|---|---|
| `/` | サービス総合トップ | 無料で始める |
| `/features/management` | 介護事業者向け経営支援 | 無料で試す |
| `/features/sales` | 介護業界向け営業支援 | 料金を見る |
| `/features/ma` | M&A支援 | M&Aプランを見る |
| `/data` | データ源・件数・更新・制約 | 分析機能を見る |
| `/methodology` | 指標定義・欠損・推定・免責 | 無料で確認する |
| `/pricing` | プラン比較 | 無料登録／プラン選択 |

以下は公開アクセス可能でも`noindex, nofollow`とする。

- `/login`
- `/signup`
- `/forgot-password`
- `/reset-password`

以下は認証必須かつ`noindex, nofollow`とする。

- 上記index対象・認証系以外の既存アプリページ
- `/dashboard`
- `/facility`
- `/corp`
- `/facilities`
- `/ma-screening`
- `/due-diligence`
- `/account`
- `/admin/*`
- その他ログイン後の全画面

APIはすべて検索対象外とする。

---

## 4. 推奨アーキテクチャ

### 4.1 公開サイトとアプリシェルを分離する

ルートレイアウトから認証依存の表示構造を外し、以下の責務へ整理する。

```text
src/app/layout.tsx
  HTML、body、全体metadataの最低限のみ

src/components/public/PublicHeader.tsx
src/components/public/PublicFooter.tsx
src/components/public/PublicLayout.tsx
  公開サイト専用。認証を前提にしない

src/components/layout/AppShell.tsx
  既存のログイン後SaaS専用
```

Next.jsのRoute Groupsを使って既存ページを大量移動すると、差分が大きく回帰リスクが高い。今回は以下のどちらかを選択する。

#### 推奨A: パス判定による段階的分離

- `AppShell`で公開SEOパスを認識し、公開パスではSidebar/Header/ProtectedRouteを通さない
- 公開各ページ自身が`PublicLayout`を使用する
- 認証初期化が公開HTMLの生成を妨げないことを確認する
- 既存URL移動を避け、最小差分で公開層を追加する

#### 将来B: Route Groupsへの完全分離

```text
src/app/(public)/...
src/app/(auth)/...
src/app/(app)/...
```

今回は必須ではない。実施する場合は、既存全URLを維持し、全ページの回帰確認まで行うこと。

### 4.2 Server Componentを基本とする

公開SEOページは原則Server Componentとする。

- ページ全体へ`"use client"`を付けない
- ボタン計測や料金決済など、必要な小部分だけClient Componentへ分離する
- 本文、見出し、FAQ、CTAリンクはサーバー生成HTMLへ含める
- 公開ページの表示に認証APIやTursoを必須にしない

### 4.3 サイトURLを一元管理する

`NEXT_PUBLIC_SITE_URL`を追加し、末尾スラッシュを除いた絶対URLを返すヘルパーを作る。

```text
本番想定: https://kaigo-bi.onrender.com
将来: 独自ドメインへ環境変数だけで切替可能
```

ローカル開発時のfallbackは`http://localhost:3000`でよいが、本番ビルドでは未設定を検知できるようにする。canonical、OG URL、sitemap、robotsの基準URLを別々にハードコードしない。

---

## 5. 公開ページの内容要件

### 5.1 `/` 公開トップページ

既存の`/`→`/dashboard`リダイレクトを削除し、`src/app/page.tsx`を新規作成する。

#### 必須セクション

1. ヒーロー
   - H1は1個
   - 誰向けの何のサービスかを具体的に説明
   - CTA「無料で始める」→`/signup`
   - サブCTA「料金を見る」→`/pricing`
2. 信頼性を伴うデータ概要
   - 全国223,103施設・サービスレコード
   - 施設×サービス単位であり、ユニーク事業所数ではない旨
   - 公開情報を基にしていること
3. 3つの利用目的
   - 経営支援
   - 営業支援
   - M&A支援
4. 主要機能
   - 市場分析
   - 法人・施設分析
   - 人材・品質分析
   - 財務はパイロットである旨を併記
5. データの誠実さ
   - 出典
   - 基準日または更新日の考え方
   - 欠損と推定の区別
   - `/data`と`/methodology`へのリンク
6. 料金の要約
   - Free / Standard / Pro / M&A
   - 詳細は`/pricing`
7. 最終CTA
8. FAQ

#### コピー上の禁止事項

- 「全国223,103施設」と断定しない
- 24施設しかない財務抽出を全国対応のように見せない
- 「AIが正確に判定」「売却可能性を予測」等の未検証表現を使わない
- 未実装機能を現在利用可能として書かない

### 5.2 ペルソナ別ページ

各ページは、単なる機能一覧ではなく次の順に構成する。

1. 対象顧客の課題
2. kaigo-biで可能になること
3. 使用するデータと機能
4. 利用フロー
5. 制約・注意点
6. 料金または登録CTA

#### `/features/management`

- 対象: 介護事業者、経営企画、施設責任者
- 訴求: 地域比較、人員・品質・稼働の確認
- 「マイ施設登録」は未実装なので、利用可能と書かない

#### `/features/sales`

- 対象: 介護業界へ営業する企業
- 訴求: 対象市場の把握、条件検索、リスト作成
- CSV件数やプラン条件は`pricing/page.tsx`およびバックエンド設定と照合する

#### `/features/ma`

- 対象: M&A仲介、買い手、事業開発
- 訴求: 候補探索、法人情報整理、簡易DD支援
- 財務抽出済みが24施設のみであることをページ内で明記
- 経営危険度や売却期待度を確定的・予測的に表現しない
- 専門的DDの代替ではない旨を明記

### 5.3 `/data`

最低限、以下を表形式または定義リストで掲載する。

- 主要データ源
- データの粒度
- レコード数
- ユニーク施設数との違い
- 更新方法
- 更新頻度
- 最終更新日の表示方法
- 欠損の意味
- 財務PDF URL取得とAI抽出の違い
- 現状の財務抽出範囲
- 出典元へのリンク

実測できない数値は掲載しない。既存ドキュメントの将来予定を現在値として掲載しない。

### 5.4 `/methodology`

以下を説明する。

- 公開値、正規化値、派生値、AI抽出値の区分
- 施設単位・施設サービス単位・法人単位の違い
- 比率の分母
- 金額単位
- 欠損時は0とみなさない方針
- 財務の決算期・対象スコープ
- ベンチマークの母集団
- 経営危険度スコアが現状ルールベースであること
- 投資判断・診断・法務・会計助言の代替ではないこと

既存実装がこの方針を満たしていない箇所は、事実を隠さず「改善中」とするか、その指標を公開ページで宣伝しない。

### 5.5 `/pricing`

現在のページはClient Componentなので、次のいずれかでSEO metadataを追加する。

- `src/app/pricing/layout.tsx`をServer Componentとして追加
- またはページ本体をServer Component化し、決済操作だけClient Componentへ分離

次を修正・確認する。

- 「全国223,000施設」を「全国223,103施設・サービスレコード」に変更
- 税別表示
- Stripeがテストモードなら、本番公開時に実課金と誤認させない
- 各プラン機能が実際のプランゲートと一致する
- CTAは未ログイン時に`/signup`へ遷移
- FAQまたは解約・変更条件を明示

---

## 6. 公開サイト共通UI

### PublicHeader

- ロゴまたはサービス名 → `/`
- 機能
  - 経営支援
  - 営業支援
  - M&A
- データについて → `/data`
- 料金 → `/pricing`
- ログイン → `/login`
- 強調CTA「無料で始める」→`/signup`
- モバイルメニュー対応

### PublicFooter

- サービス
- 機能
- データと指標
- 料金
- ログイン・登録
- 利用規約・プライバシーポリシー
- 運営者情報

利用規約、プライバシーポリシー、運営者情報の実ページが存在しない場合は、リンク切れを作らない。実装可否を報告し、別タスクとしてTODO化する。

### アクセシビリティ

- 見出し階層を飛ばさない
- キーボード操作可能
- フォーカス表示を消さない
- アイコンだけのリンクへaria-labelを付ける
- 色だけで意味を表さない
- CTA文言を「こちら」だけにしない

---

## 7. Metadata要件

### 7.1 ルートmetadata

`src/app/layout.tsx`で以下を設定する。

- `metadataBase`
- default title
- title template
- description
- applicationName
- Open Graphの基本値
- Twitter Card
- icons（実ファイルがある場合のみ）
- 日本語locale

仮のブランド表記は「kaigo-bi」とし、既存の「介護BI - 戦略コンサルティング」を全ページ共通タイトルにしない。

### 7.2 ページ固有metadata

各index対象ページに固有の以下を設定する。

- title
- description
- alternates.canonical
- openGraph title/description/url

タイトルとdescriptionへ不自然にキーワードを詰め込まない。

### 7.3 noindex

認証系には、ネストしたlayout等で以下を設定する。

```ts
robots: {
  index: false,
  follow: false,
  nocache: true,
}
```

ログイン後アプリにも同等の設定を適用する。ただし公開SEOページに誤って継承させない。

---

## 8. robots.txtとsitemap.xml

### 8.1 `src/app/robots.ts`

Next.js Metadata Routeとして実装する。

基本方針:

- 公開SEOページはクロール許可
- `/api/`は拒否
- アプリ画面は拒否
- 認証・アカウント・管理画面は拒否
- `sitemap.xml`の絶対URLを指定
- Googlebotだけに依存せず、一般user-agent向けに定義

注意:

- `robots.txt`はアクセス制御ではない
- 認証必須APIの保護手段として扱わない
- CSSやJSなど、公開ページの描画に必要な`/_next/`アセットを全面拒否しない

### 8.2 `src/app/sitemap.ts`

今回のindex対象7ページだけを含める。

- `/`
- `/features/management`
- `/features/sales`
- `/features/ma`
- `/data`
- `/methodology`
- `/pricing`

ログイン、登録、アプリ画面、クエリ付きURLは含めない。`lastModified`をビルド時刻で毎回偽更新しない。信頼できる更新日を管理できない場合は省略してよい。

---

## 9. 構造化データ

JSON-LDを公開トップへ追加する。

候補:

- `Organization`
- `WebSite`
- `SoftwareApplication`

ページ本文にないレビュー点数、導入社数、受賞歴などを構造化データだけに入れない。価格情報を入れる場合は、画面の価格と一致させる。

FAQ構造化データは、検索結果での特別表示を保証しない。実際に画面へ表示するFAQと完全一致する場合だけ追加する。

JSON-LDは`JSON.stringify`で生成し、ユーザー入力を混ぜない。

---

## 10. コンバージョン計測

検索流入から売上へつながるかを判断できるよう、以下のイベント名と発火場所をコード上で定義する。

| イベント | 発火 |
|---|---|
| `public_cta_click` | 公開ページの主要CTA |
| `pricing_view` | 料金ページ表示 |
| `signup_start` | 公開ページから登録へ遷移 |
| `login_start` | 公開ページからログインへ遷移 |
| `persona_page_view` | ペルソナ別ページ表示 |

イベントプロパティ:

- `source_page`
- `cta_location`
- `cta_label`
- `target_path`
- `persona`（該当時）

今回、GA4等の送信先が未設定なら外部SDKを追加しない。型付きの薄い計測関数を作り、開発環境ではno-op、将来providerを差し替えられる設計にする。Cookieや個人情報を無断で外部送信しない。

主要な事業指標は以下とする。

- 検索表示回数
- 検索クリック数
- 公開トップ→料金ページ遷移率
- 公開トップ→登録開始率
- ペルソナページ→登録開始率
- 登録完了率
- Free→有料転換率

SEO順位だけを完了指標にしない。

---

## 11. コンテンツ上の根拠

実装前に最低限、以下を確認する。

- `claudedocs/REVIEW_BRIEF_20260727.md`
- `claudedocs/PERSONA_DESIGN_20260724.md`
- `claudedocs/FACILITIES_DATA_AUDIT_20260724.md`
- `claudedocs/PLAN_FACILITY_360_20260724.md`
- `claudedocs/research/competitor_analysis.md`
- `kaigo-bi-backend/src/routes/mod.rs`
- `kaigo-bi-frontend/src/app/pricing/page.tsx`
- `kaigo-bi-frontend/src/components/layout/AppShell.tsx`

ドキュメントと実装が食い違う場合は、現在の実装を優先して「現在利用可能」と「予定」を区別する。判断できない場合は勝手に決めず、実装報告へ質問事項として残す。

---

## 12. 実装手順

### Phase 1: 公開・認証境界

1. `git status --short`で既存変更を確認
2. `/`のリダイレクトを削除
3. 公開パス定義を一元化
4. `PublicHeader`、`PublicFooter`、`PublicLayout`を追加
5. 公開ページが`ProtectedRoute`やアプリSidebarを通らないようにする
6. 既存ログイン後ページの認証挙動が変わっていないことを確認

### Phase 2: 売上直結ページ

1. `/`
2. `/pricing`のSEO対応・表現修正
3. `/features/management`
4. `/features/sales`
5. `/features/ma`
6. `/data`
7. `/methodology`

まずトップ・料金・3ペルソナを完成させ、データとmethodologyで信頼性を補完する。

### Phase 3: 技術SEO

1. `metadataBase`
2. 各ページmetadata
3. noindex
4. canonical
5. `robots.ts`
6. `sitemap.ts`
7. JSON-LD
8. 内部リンク

### Phase 4: 計測とQA

1. CTAイベントの共通関数
2. レスポンシブ確認
3. キーボード操作確認
4. ビルド確認
5. 生成HTML確認
6. robots・sitemap確認
7. リンク切れ確認

---

## 13. 受け入れ条件

### 公開・認証

- [ ] 未ログインで`/`が200となり、公開トップが表示される
- [ ] `/`が`/dashboard`へリダイレクトされない
- [ ] 未ログインで全index対象ページを閲覧できる
- [ ] 未ログインで`/dashboard`へ行くと既存どおりログインへ誘導される
- [ ] ログイン後の既存主要画面が従来どおり表示される
- [ ] 公開ページにアプリSidebarが表示されない
- [ ] 公開ページの初期HTMLにH1、本文、CTAリンクが含まれる

### SEO

- [ ] index対象7ページに固有のtitleとdescriptionがある
- [ ] canonicalが絶対URLで正しい
- [ ] `/robots.txt`が200
- [ ] `/sitemap.xml`が200
- [ ] sitemapに認証・アプリページが含まれない
- [ ] ログイン・登録・アプリページがnoindex
- [ ] 公開ページをrobotsで誤って拒否していない
- [ ] JSON-LDが有効なJSON
- [ ] ページ内にリンク切れがない

### 表現・データ

- [ ] 「223,103施設・サービスレコード」と正確に記載
- [ ] ユニーク施設数ではない旨を明記
- [ ] 財務24施設・52レコードの現状を明記
- [ ] 未実装機能を利用可能と表現していない
- [ ] 経営危険度を検証済み予測モデルとして表現していない
- [ ] 公開ページの価格と実装上のプラン価格が一致

### 品質

- [ ] `npm run build`が成功
- [ ] TypeScriptエラーがない
- [ ] 主要公開ページを375px、768px、1280pxで確認
- [ ] キーボードだけでヘッダーとCTAを操作可能
- [ ] 既存の認証・課金・APIコードに不要な変更がない

---

## 14. 検証コマンド例

PowerShell:

```powershell
cd C:\Users\fuji1\kaigo-bi-platform\kaigo-bi-frontend
npm run build
npm run dev
```

起動後:

```powershell
curl.exe -I http://localhost:3000/
curl.exe http://localhost:3000/robots.txt
curl.exe http://localhost:3000/sitemap.xml
curl.exe http://localhost:3000/ | Select-String -Pattern "<h1|無料で始める|施設・サービスレコード"
```

可能であればLighthouseをモバイル条件で実行し、公開トップについて以下を確認する。

- SEO
- Accessibility
- Best Practices
- Performance

Lighthouseスコア100を目的化せず、重大なエラーと検索不能要因を優先して修正する。

---

## 15. 作業完了時の報告形式

別AIは、完了時に以下を報告すること。

1. 変更したファイル一覧
2. 公開したURL一覧
3. 公開／認証境界をどう実装したか
4. metadata、robots、sitemap、JSON-LDの実装内容
5. コピー上の重要な制約
6. 実行した検証と結果
7. 未解決事項
8. Search Console、独自ドメイン、アクセス解析など人間側で必要な作業

勝手にコミット、push、デプロイしない。依頼者から明示的に指示された場合のみ行う。

---

## 16. 実装後に人間が行う作業

コード実装とは別に、以下が必要である。

1. 独自ドメインの決定
2. `NEXT_PUBLIC_SITE_URL`の本番設定
3. Google Search Consoleへの登録
4. sitemapの送信
5. 本番URLのインデックス確認
6. 検索クエリとCTA転換率の月次確認
7. 利用規約・プライバシーポリシー・運営者情報の公開
8. GA4等を使う場合の同意・プライバシー対応

公開後4〜8週間は、ページ数を急増させず、検索クエリと登録への貢献を確認する。その結果を見てから、都道府県別ページ、サービス別ページ、独自調査記事の追加を判断する。
