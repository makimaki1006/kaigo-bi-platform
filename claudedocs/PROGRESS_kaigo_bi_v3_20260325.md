# 介護BI v3 最適化 進捗レポート（2026-03-25）

## 完了タスク

### Phase 0: 方式D基盤構築
- [x] **0-1**: kpi_cacheテーブル作成 + facilitiesインデックス4件追加（Turso）
- [x] **0-2**: `scripts/aggregate_to_cache.py` 作成・実行（31キー全て投入成功）
  - 128,792件分のデータで集計完了
  - 賃金KPI修復済み（avg_salary: 241,559円、median: 246,666円）
  - 品質スコア修復済み（avg: 44.7点/100点）

### Phase 1: KPI計算ロジック修正
- [x] 賃金KPI: 賃金_月額1-5をunpivotして正しく計算
- [x] 品質スコア: 4軸モデル（Safety/Quality/HR/Operations各25点）
- [x] 要介護度: 要介護1-5から平均・重度者割合を計算
- 上記はaggregate_to_cache.py内に統合

### Phase 2: Rustバックエンド方式D対応
- [x] **2-1**: `cache_store.rs` 新規作成（kpi_cache読み込みモジュール）
- [x] **2-2**: 全31APIルートにキャッシュファーストロジック適用
  - FilterParams::is_default()追加
  - フィルタなし→キャッシュ、フィルタあり→DataFrame（フォールバック）
- [ ] **未反映**: ローカルのRustリンカーエラーでビルドできず。Docker/CIでビルド必要

### Phase 3: フロントエンド修復
- [x] **3-1**: 賃金ページ修復（散布図、ヒストグラム、職種テーブル実装、formatManYen追加）
- [x] **3-2**: 成長分析ページ修復（年代別トレンド、累積成長、クロスリファレンスカード）
- [x] **3-3**: DD支援ページ修復（加算テーブル実データ表示、法人サマリー+施設別詳細）

### Phase 4: サービス種別制御
- [x] **4-2**: `service-config.ts` 新規作成（36サービスコード×15KPI×6カテゴリの制御マトリクス）

### データアップロード
- [ ] 未投入94,315件のTursoアップロード（進行中）
- [x] テストユーザー作成（test@test.com / test1234）

## E2Eテスト結果（2026-03-25 01:26）

### 22/23ページ PASS

| ページ | チャート数 | 状態 |
|---|---|---|
| ダッシュボード | 4 | PASS - 128,792施設、全KPI正常 |
| 市場構造 | 15 | PASS - マップ+全チャート |
| 成長分析 | 19 | PASS - 年代別トレンド等 |
| 経営品質 | 4 | PASS |
| 収益構造 | 4 | PASS |
| 法人グループ | 4 | PASS |
| 施設マスタ | 0 | PASS（テーブル系） |
| 他16ページ | - | 全PASS |

### 1/23ページ FAIL

| ページ | 問題 | 原因 |
|---|---|---|
| 人材分析 | 経験10年以上割合が「-」 | バックエンドnull返却 |

### 既知の制約

| 項目 | 状態 | 解決方法 |
|---|---|---|
| 賃金KPI「-」表示 | バックエンドが賃金カラムskip中 | 方式D切替後に解決 |
| Rustビルドエラー | ローカルのリンカーキャッシュ破損 | Docker/CIビルドで対応 |
| Turso未投入データ | 94,315件のアップロード進行中 | 完了後に再集計 |

## 作成ファイル一覧

| ファイル | 種別 | 説明 |
|---|---|---|
| `scripts/setup_kpi_cache.py` | 新規 | kpi_cacheテーブル作成スクリプト |
| `scripts/aggregate_to_cache.py` | 新規 | 事前集計パイプライン |
| `scripts/upload_missing_services.py` | 新規 | 未投入データアップロード |
| `scripts/e2e_test_v2.py` | 新規 | E2Eテストスクリプト |
| `kaigo-bi-backend/src/services/cache_store.rs` | 新規 | キャッシュ読み込みモジュール |
| `kaigo-bi-frontend/src/lib/service-config.ts` | 新規 | サービス種別制御 |
| `kaigo-bi-frontend/src/lib/formatters.ts` | 修正 | formatManYen追加 |
| `kaigo-bi-frontend/src/lib/types.ts` | 修正 | SalaryKpi, DdKasanSummary型追加 |
| `kaigo-bi-frontend/src/app/salary/page.tsx` | 修正 | 散布図・ヒストグラム・職種テーブル |
| `kaigo-bi-frontend/src/app/growth/page.tsx` | 修正 | DataPendingPlaceholder除去 |
| `kaigo-bi-frontend/src/app/due-diligence/page.tsx` | 修正 | 加算テーブル実データ |
| `kaigo-bi-backend/src/services/mod.rs` | 修正 | cache_storeモジュール追加 |
| `kaigo-bi-backend/src/main.rs` | 修正 | CacheStore初期化追加 |
| `kaigo-bi-backend/src/models/filters.rs` | 修正 | is_default()追加 |
| `kaigo-bi-backend/src/models/aggregation.rs` | 修正 | DdKasanSummary追加 |
| `kaigo-bi-backend/src/services/aggregator.rs` | 修正 | compute_dd_kasan_summary追加 |
| `kaigo-bi-backend/src/routes/*.rs` | 修正 | 9ファイルにキャッシュファースト適用 |

## 次のステップ

1. データアップロード完了 → 再集計（aggregate_to_cache.py再実行）
2. Docker/CIでRustバックエンドビルド → デプロイ
3. 方式D切替後のE2Eテスト再実行（賃金ページ等の検証）
4. service-config.tsの各ページへの適用
