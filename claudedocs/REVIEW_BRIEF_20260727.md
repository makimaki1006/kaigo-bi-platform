# kaigo-bi レビュー依頼ブリーフ(外部AI/レビュアー向け)

作成: 2026-07-27 / 対象コミット: `d2d2647`(main) / 本番: https://kaigo-bi.onrender.com

このドキュメントは、前提知識ゼロのレビュアー(別のAI含む)がこのプロジェクトを
評価できるよう、全体像・現状・レビューしてほしい観点をまとめたものです。

---

## 1. プロダクト概要

**介護業界の公開データを使った BI / M&A / 営業支援 / 経営支援 SaaS。**

- データ源: 介護サービス情報公表システム(厚労省)をスクレイピングした全国223,103施設のデータ + 決算PDF(AI抽出) + 国のオープン統計(人口・求人倍率・賃金等)
- 5ペルソナ: ①情報可視化BI ②経営支援(介護事業者) ③採用コンサル ④営業支援(介護業界に営業する企業) ⑤M&A(仲介・買い手)
- 課金: Free / Standard 9,800円 / Pro 29,800円 / M&A 49,800円(Stripeテストモードで稼働中、本番キー未切替)

## 2. 技術スタックとアーキテクチャ

```
[スクレイピング(Python)] → CSV → [Turso(LibSQL) facilities 223k行 + financials + external統計81テーブル]
   → [Python事前集計 aggregate_to_cache.py → kpi_cache] → [Rust/Axum API] → [Next.js 14 フロント]
   全部を1 Dockerコンテナ(nginx+Rust+静的Next)に同梱 → Render Starter($7) にデプロイ
```

- **バックエンド**: Rust/Axum、`kaigo-bi-backend/src/`。ルートは `routes/*.rs`(20ファイル)。データアクセスは CacheStore(事前計算JSON)+ Turso SQL 直クエリ
- **フロント**: Next.js 14 App Router、`kaigo-bi-frontend/src/`。32ページ。SWR + 独自fetchラッパー(`lib/api-client.ts`)
- **認証**: 自前JWT(argon2)。マルチロール(admin/consultant/sales/viewer)+プラン(free/standard/pro/ma)。プラン別ルートゲート(`routes/mod.rs` の require_plan)
- **DB**: Turso 2つ。メイン(cw-makimaki1006)= facilities/financials/users等、外部統計(country-statistics)= v2_external_* 81テーブル。**どちらも接続情報は kaigo-bi-backend/.env(gitignore)**
- **デプロイ**: main push → GitHub Actions `Build and Push Docker Image`(手動 workflow_dispatch)→ ghcr.io → Render Deploy Hook

## 3. 主要ディレクトリ

| パス | 役割 |
|---|---|
| `kaigo-bi-backend/src/services/sql_aggregator.rs` | 全SQL集計の本体(最大ファイル、要レビュー) |
| `kaigo-bi-backend/src/routes/` | APIエンドポイント(auth/billing/ma_screening/due_diligence/facilities等) |
| `kaigo-bi-frontend/src/app/` | ページ(dashboard, facility, corp, ma-screening, home/*, account 等) |
| `kaigo-bi-frontend/src/components/data-display/` | FinancialSummaryCard, CrossMetricsCard, FacilityDetailPanel 等 |
| `scripts/` | スクレイパー・ETL・マイグレーション(254ファイル、うち現行主要は下記) |
| `claudedocs/` | 設計・調査ドキュメント |

現行の主要スクリプト: `scrape_unei_financial.py`(財務スクレイプ・最新), `aggregate_to_cache.py`(集計), `merge_kihon_and_reload.py`(Turso投入), `migrate_*.py`(スキーマ変更), `turso_helpers.py`(共通)

## 4. 直近で実装した機能(レビュー対象)

1. **SaaS基盤**: セルフサインアップ、Stripe課金(checkout/portal/webhook、REST直叩き・署名検証)、プラン別ゲート、パスワードリセット(Resend)、レート制限(メモリ内)
2. **施設360°ビュー**: `/facility?id=` と `/corp?number=` に、公表データ88カラム + 決算PDF由来financials + **クロス指標**(労働生産性/実人件費率/営業利益率/自己資本比率/経営危険度スコア0-100)
3. **M&Aスクリーニング財務フィルタ**: 債務超過/営業赤字/処分歴/財務あり
4. **ワークスペース**: ペルソナ別(業界データ/経営支援/営業支援/M&A)にナビを絞りホームを出し分け
5. **データ層修正**: 3月のテーブル再構築で全カラムがTEXT化+`○`表記になり、①libsqlの型パニック(502) ②品質/加算フラグの `=1` 比較が全空振り ③派生カラム消失、を修正

## 5. データの現状(重要な前提)

- facilities 223,103行(施設×サービスの行。ユニーク事業所ではない点に注意)
- **financials テーブルは現在52レコード(24施設分)のみ** — 決算PDFのAI抽出はパイロット段階。財務サマリー/クロス指標/スクリーニング財務フィルタは今この24施設でしか値が出ない
- 財務PDFのURLは現在**全国スクレイピング実行中**(uneiページHTML方式、鳥取実測でPL 81.2%)。完了後10万件超に拡大予定だが、URLがあっても中身のAI抽出は未実施
- 加算・品質フラグは `○`/NULL を 1/0 に正規化済み。派生カラム(occupancy_rate/kasan_count/quality_score/quality_rank)は復元済み

## 6. レビューしてほしい観点

### A. コード品質・保守性
- `sql_aggregator.rs` の巨大化(集計ロジックが1ファイルに集中)。分割・重複の是非
- row_f64/row_str 等のTEXT型変換ヘルパー(libsqlパニック対策)の妥当性
- フロントの型定義(`lib/types.ts`)とAPI実レスポンスの乖離リスク(過去に avg_years / total_staff 欠損でビルド破損の実績あり)

### B. セキュリティ
- 自前JWT認証(argon2)+プランゲートの設計。ロール/プランのバイパス経路が無いか
- Stripe Webフック署名検証(`routes/billing.rs` の verify_stripe_signature)
- レート制限がメモリ内(単一インスタンス前提)。スケール時の穴
- SQLインジェクション(動的WHERE構築箇所、`WhereBuilder`)

### C. データの誠実さ(最重要)
- 「財務サマリー」「クロス指標」「経営危険度スコア」が24施設でしか出ないのに、UIが「データ準備中」と適切に見せているか、それとも実データより多く見えてしまうか
- 経営危険度スコアの重み(債務超過40/営業赤字20/離職15/処分15/低稼働10)の妥当性
- 行政処分の `is_real_violation`(「なし」系除外)の取りこぼし/誤検出
- クロス指標の分母ゼロ・単位混在(円/千円)・スコープ混在(法人全体/事業所単位)の扱い

### D. プロダクト/UX
- ペルソナ別ワークスペースの分け方は妥当か。ナビ設計
- Freeで何が見えるべきか(現状: ダッシュボード+市場の一部)。コンバージョン設計
- M&A/営業/経営 各ペルソナで「閲覧で終わらず次のアクション(リスト化・DD・バリュエーション)に繋がるか」

### E. 事業性(競合比較は claudedocs/research/competitor_analysis.md 参照)
- 差別化の核は「公開データ横断 × 多目的 × 高頻度更新 × 実財務ベンチマーク」。これが競合(福祉施設DB BI 年8-26万・年2回更新等)に対して成立するか
- 最大リスク: 厚労省「経営情報DB」(2025義務化)が公式無料分析UIを出す可能性

## 7. 関連ドキュメント(claudedocs/)

- `PERSONA_DESIGN_20260724.md` — ペルソナ設計
- `DATA_EXPANSION_ROADMAP_20260724.md` — データ拡充計画(4調査の統合)
- `FACILITIES_DATA_AUDIT_20260724.md` — 112カラムの充足率×使用状況監査
- `PLAN_FACILITY_360_20260724.md` — 施設360°ビュー計画
- `research/SCRAPER_REDESIGN_20260725.md` — スクレイパー改修設計(財務URL問題の根本原因)
- `research/competitor_analysis.md` / `external_kaigo_sources.md` / `external_db_inventory.md` / `gov_opendata_kaigo.md` — 4本の調査

## 8. 既知の未完了・宿題

- 決算PDFの本格AI抽出パイプライン(10万件規模、Claude APIキー必要)= 財務機能が本当に価値を出すための最重要残件
- マイ施設登録(経営支援ペルソナの核、未実装)
- Resendドメイン認証 + Stripe本番キー切替(課金開始の前提)
- 全ページ1枚ずつの中身レビュー(数値の正しさ・空データ表示・モバイル)は未実施
- 賃金表・行政処分・サービス提供地域(充足0%)の追加スクレイプ(設計書§3.2)
