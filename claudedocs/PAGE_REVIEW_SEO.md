# kaigo-bi 公開SEOサイト検証レポート

- 検証日時: 2026-07-28
- 対象環境: https://kaigo-bi.onrender.com （本番）
- 検証方法: `curl`によるSSR HTML取得（未ログイン・認証クッキー無し）+ Playwright（chromium）による375/768/1280pxスクリーンショット
- スクリーンショット格納先: `C:\Users\fuji1\kaigo-bi-platform\claudedocs\review_shots\`（{ページ名}_{375|768|1280}.png、計21枚）
- 前提: デプロイ確認済み（`/features/management` が200を返すことをポーリング不要で即確認）

## サマリー

| 重大度 | 件数 |
|---|---|
| 重大（Critical） | 1 |
| 中（Medium） | 0 |
| 軽微（Minor） | 3 |

技術的なSEO実装（canonical・robots meta・robots.txt・sitemap.xml・JSON-LD・文言の一貫性・レスポンシブ）は総じて良好。唯一かつ最重要の問題は、**フッターの利用規約・プライバシーポリシー・運営者情報がリンクではなく「（準備中）」の単なるテキストのままStripe決済を伴う有料課金ページが公開されている**点。

---

## ページ別 詳細

### 全ページ共通

| 重大度 | 問題 | 根拠 | 推奨対応 |
|---|---|---|---|
| 🔴 重大 | フッターの「利用規約（準備中）」「プライバシーポリシー（準備中）」「運営者情報（準備中）」が`<a>`タグではなく`<span>`要素。実体ページが存在せずクリックもできない | HTML実査: `<li><span>利用規約<!-- -->（準備中）</span></li><li><span>プライバシーポリシー<!-- -->（準備中）</span></li><li><span>運営者情報<!-- -->（準備中）</span></li>`（全7公開ページのフッターで同一） | 有料プラン（Standard 9,800円/月〜M&A 49,800円/月、Stripe決済）を提供する以上、特定商取引法に基づく表記に相当する「運営者情報」と、個人情報を扱う以上必須の「プライバシーポリシー」は法務観点で公開前必須。少なくとも「準備中」である旨とサービス提供者の実在性が分かる最低限の情報（運営者名・所在地・連絡先）を先に用意すべき |
| 🟢 良好 | canonicalが全ページ`https://kaigo-bi.onrender.com/...`で本番URL。localhost混入なし | 7ページ全てcurlで確認済み | 対応不要 |
| 🟢 良好 | 公開ページにアプリのSidebar UIが出ていない（未ログイン公開レイアウトが機能） | 全ページで`class="...sidebar..."`一致0件 | 対応不要 |
| 🟢 良好 | 数字表記（223,103件／約19万事業所／約6.8万法人／24施設・52レコードのパイロット段階）が全ページで一貫 | 各ページHTML内grep結果一致 | 対応不要 |
| 🟢 良好 | 「経営危険度」等の誇張表現、「保証」「完璧」「100%」等のマーケティング誇張は検出されず | 全ページgrep該当0件 | 対応不要 |
| 🟡 軽微 | `/pricing`ページ自体にJSON-LD（Offer/Product）が無い。SoftwareApplication＋4プランのOfferスキーマはトップページ（`/`）にのみ存在し、`/pricing`のHTMLにはJSON-LDが1つも無い | `body_pricing.html`に`application/ld+json`が0件。価格数値（9,800/29,800/49,800）自体はSSR HTML本文には正しく存在（トップページJSON-LDの価格とも一致） | 必須ではないが、Google Merchant的なリッチリザルト対象を狙うなら`/pricing`側にOffer/Productスキーマを移すか複製するとより適切 |
| 🟡 軽微 | 全7公開ページで`og:image`が未設定（`og:title`/`og:description`/`og:url`/`og:type`は設定済み） | 7ページ全て`grep -oc 'og:image'` = 0 | SNSシェア時にサムネイル無しになる。OGP画像1枚（できればページ種別ごと）を用意し`og:image`を追加すべき |

### / （トップ）

- title: `公開情報でわかる介護事業所のBI・データ分析`
- description: 223,103件／約19万事業所／経営支援・営業支援・M&A支援を明記
- canonical: `https://kaigo-bi.onrender.com`（末尾スラッシュ無し、他ページと表記統一されているか要注意だが実害なし）
- robots meta: `index, follow`
- H1: 「公開情報でわかる、介護事業所のBI・データ分析」がSSR HTML内に存在
- JSON-LD: Organization / WebSite / SoftwareApplication（4プラン価格入り）/ FAQPage の4種、全てvalid JSON
- レイアウト: 375/768/1280pxとも崩れなし

### /features/management

- title/description/canonical/robots meta: 固有かつ正常（`index, follow`）
- H1: 「介護事業所の経営支援 地域比較と人員・品質・稼働の把握」
- 768px表示: 「予定機能：『マイ施設登録』（自施設を登録し、周辺施設と自動的に比較する機能）は現時点では未実装です」と未実装機能を正直に明記（良好）
- レイアウト崩れなし

### /features/sales

- title/description/canonical/robots meta: 固有かつ正常
- レイアウト崩れなし（詳細スクショ未添付だがHTML構造は他featuresページと同一テンプレート）

### /features/ma

- title/description/canonical/robots meta: 固有かつ正常
- description内に「財務数値化は24施設のパイロット段階です」を明記（誠実）
- 本文内に「『要確認シグナル』は…売却可能性の予測や確定的な判定は行いません」「専門家による本格的なデューデリジェンスの代替ではありません」と免責が明記されている（良好）
- レイアウト崩れなし（1280px確認済み）

### /data

- title: `データについて｜kaigo-bi`
- 「223,103」はユニークな施設数ではない旨、財務PDF数値化が24施設・52レコードのパイロット段階である旨を明記（誠実）
- レイアウト崩れなし

### /methodology

- title: `指標・データ取扱方針｜kaigo-bi`
- データの4層区分（公表値・正規化値・派生値・AI抽出値）や欠損の扱いを明記
- レイアウト崩れなし

### /pricing

- title: `料金プラン｜kaigo-bi`
- 価格（0円／9,800円／29,800円／49,800円、いずれも税別・月額）がSSR HTML本文に正しく存在し、トップページJSON-LDの価格と一致
- robots meta: `index, follow`
- JSON-LDは無し（上記軽微指摘）
- レイアウト: 375/768/1280pxとも崩れなし。1280pxで4プランが横並びカードになり視認性良好

### /login, /signup, /dashboard（非公開ページ・参考確認）

- 3ページとも `<meta name="robots" content="noindex, nofollow"/>` が正しく設定
- 🟡 軽微: titleが3ページとも同一の`kaigo-bi｜公開情報でわかる介護事業所のBI・データ分析`（noindexのためSEO実害はないが、ユーザー向けタブ表示としては`ログイン｜kaigo-bi`等の固有titleが望ましい）

### /robots.txt

- 200 OK
- `/api/`、`/login`、`/signup`、`/dashboard`、`/facility`等アプリ内部URLを網羅的にDisallow
- `Sitemap: https://kaigo-bi.onrender.com/sitemap.xml` の絶対URL記載あり
- 問題なし

### /sitemap.xml

- 200 OK、valid XML
- 公開7ページ（`/`, `/features/management`, `/features/sales`, `/features/ma`, `/data`, `/methodology`, `/pricing`）のみを含み、認証・アプリURLの混入なし
- 問題なし

---

## 内部リンク

全公開ページ内の`href`は `/`, `/data`, `/features/ma`, `/features/management`, `/features/sales`, `/login`, `/methodology`, `/pricing`, `/signup` の9種のみで、いずれも200を確認済み。リンク切れなし。

---

## 未検証・スコープ外

- 実際のGoogle Search Consoleへのインデックス登録状況（本レビューはHTML/メタデータの静的検証のみ）
- コード修正は指示通り未実施（調査・報告のみ）
