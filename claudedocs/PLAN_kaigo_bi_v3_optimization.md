# 介護BI v3 最適化実装プラン（改訂版）

## 現状分析

### データ資産

| データソース | 内容 | 状態 |
|---|---|---|
| Turso (facilities) | 78カラム × 223,107行 | アップロード済み |
| Turso (country-statistics) | 11テーブル | 3/11のみ活用中 |
| スクレイピングCSV | kaigo_fast_20260324.csv (97MB) | 全34種別完了 |

### メモリ検証結果

| シナリオ | 推定メモリ |
|---|---|
| 現在（45カラムロード） | ~155MB |
| 全78カラムロード | ~215MB |
| 全78+派生5=83カラム | ~235MB |
| +compute_derived_columns（20+派生） | ~300-400MB（推定） |
| Rustバイナリ+バッファ込み | ~500-700MB（推定） |
| **Renderプラン上限** | **2GB** |

**結論**: 全カラムをDataFrameに載せれば現時点では収まる可能性はあるが、安全マージンが薄く、データ増加で破綻する。**方式Dへの移行が本質的な解決策**。

### 壊れている箇所

| 問題 | 原因 | 影響ページ |
|---|---|---|
| 賃金系が全て死亡 | 賃金20カラムをskip + 存在しない`給与水準`参照 | salary |
| 要介護度が死亡 | 要介護1-5をskip | dashboard, benchmark, DD |
| 品質スコアが不正 | `損益差額比率`(存在しない)、`経験5年以上比率`(存在しない)を参照 | quality |
| 外部DB 8テーブル未活用 | APIエンドポイント未実装 | market, workforce, salary等 |
| DataPendingPlaceholder多数 | API未接続/データ不在 | salary, growth, DD, PMI等 |
| サービス種別コントロールなし | 全種別で同じKPIを表示 | 全ページ |

---

## 改訂フェーズ構成

```
Phase 0: 方式D基盤（Python事前集計パイプライン）
  ↓
Phase 1: 壊れているKPIの修正（Python集計ロジックで正しく計算）
  ↓
Phase 2: Rustバックエンド方式D対応（DataFrame → kpi_cache読み込み）
  ↓
Phase 3: フロントエンド修復（DataPendingPlaceholder除去、実データ接続）
  ↓
Phase 4: 外部DB統合 + サービス種別制御
```

---

## Phase 0: 方式D基盤構築

**目的**: Python側で全データを事前集計し、Turso kpi_cacheテーブルに格納する仕組みを作る
**依存**: なし（独立して着手可能）

### Task 0-1: kpi_cacheテーブル作成

Tursoに新テーブル作成:

```sql
CREATE TABLE IF NOT EXISTS kpi_cache (
    key TEXT PRIMARY KEY,
    filter_key TEXT DEFAULT '',    -- フィルタ条件のハッシュ（''=全体）
    value TEXT NOT NULL,           -- JSON文字列
    updated_at TEXT NOT NULL,
    row_count INTEGER DEFAULT 0
);
CREATE INDEX idx_kpi_cache_filter ON kpi_cache(key, filter_key);
```

### Task 0-2: aggregate_to_cache.py 作成

**新規ファイル**: `scripts/aggregate_to_cache.py`

処理フロー:
1. Turso facilitiesテーブルから全データSELECT（Python側なのでメモリ制約なし）
2. pandas/polarsで全APIに対応する集計を実行
3. 集計結果をJSON化してkpi_cacheにINSERT OR REPLACE

集計対象（現在の42 APIエンドポイントすべてに対応）:

| キー | 対応API | 集計内容 |
|---|---|---|
| dashboard_kpi | /api/dashboard/kpi | 総施設数、平均従業者数、離職率等 |
| dashboard_by_prefecture | /api/dashboard/by-prefecture | 都道府県別サマリー |
| dashboard_by_service | /api/dashboard/by-service | サービス別サマリー |
| workforce_kpi | /api/workforce/kpi | 離職率、常勤比率、経験年数等 |
| workforce_turnover_dist | /api/workforce/turnover-distribution | 離職率ヒストグラム |
| workforce_by_prefecture | /api/workforce/by-prefecture | 都道府県別人材KPI |
| workforce_by_size | /api/workforce/by-size | 規模別人材KPI |
| workforce_exp_dist | /api/workforce/experience-distribution | 経験年数分布 |
| workforce_exp_turnover | /api/workforce/experience-vs-turnover | 経験vs離職率 |
| revenue_kpi | /api/revenue/kpi | 加算取得数、稼働率等 |
| revenue_kasan_rates | /api/revenue/kasan-rates | 加算13項目取得率 |
| revenue_occupancy_dist | /api/revenue/occupancy-distribution | 稼働率分布 |
| salary_kpi | /api/salary/kpi | **賃金KPI（賃金_月額1-5をunpivotして正しく計算）** |
| salary_by_job_type | /api/salary/by-job-type | **職種別賃金（賃金_職種1-5をunpivot）** |
| salary_by_prefecture | /api/salary/by-prefecture | **都道府県別賃金** |
| quality_kpi | /api/quality/kpi | **品質KPI（損益差額比率→削除、経験5年→10年に修正）** |
| quality_score_dist | /api/quality/score-distribution | 品質スコア分布 |
| quality_rank_dist | /api/quality/rank-distribution | ランク分布 |
| quality_radar | /api/quality/category-radar | 4軸レーダー |
| quality_by_prefecture | /api/quality/by-prefecture | 都道府県別品質 |
| growth_kpi | /api/growth/kpi | 新規施設数、事業年数 |
| growth_trend | /api/growth/establishment-trend | 設立年推移 |
| growth_years_dist | /api/growth/years-distribution | 事業年数分布 |
| corp_group_kpi | /api/corp-group/kpi | 法人数、多施設法人数 |
| corp_group_size_dist | /api/corp-group/size-distribution | 法人規模分布 |
| corp_group_top_corps | /api/corp-group/top-corps | Top20法人 |
| corp_group_kasan_heatmap | /api/corp-group/kasan-heatmap | 加算ヒートマップ |
| market_choropleth | /api/market/choropleth | 都道府県ヒートマップ |
| market_by_service | /api/market/by-service-bar | サービス別棒グラフ |
| market_corp_donut | /api/market/corp-type-donut | 法人種別ドーナツ |
| meta | /api/meta | メタ情報 |
| ma_screening_base | /api/ma/screening | M&Aスクリーニング基礎データ |
| benchmark_base | /api/benchmark | ベンチマーク基礎データ |

**重要**: この段階で壊れているKPIを正しく修正する（Phase 1と統合）

### Task 0-3: フィルタ対応

フィルタ付きリクエスト（都道府県、サービス種別）は2つの方式で対応:

**方式A**: 主要フィルタ組み合わせを事前集計
- 47都道府県 × 全体 = 47キー
- 34サービス種別 × 全体 = 34キー
- 合計: ~100キー（許容範囲）

**方式B**: フィルタ付きは Turso SQLを直接実行（動的クエリ）
- フィルタが複雑な場合（複数都道府県×サービス×法人種別）はSQLを発行
- facilitiesテーブルにインデックスを追加

→ **方式A+Bのハイブリッド推奨**

---

## Phase 1: KPI計算ロジック修正（aggregate_to_cache.py内）

**目的**: Python集計時に正しいロジックでKPIを計算する
**統合先**: Phase 0のaggregate_to_cache.pyに組み込み

### Task 1-1: 賃金KPI修正

**問題**: 現行Rustは`給与水準`カラムを参照→存在しない→常にNull
**修正**:
- `賃金_月額1`〜`賃金_月額5`をunpivot（1行→最大5行に展開）
- 充填率: 賃金_月額1=6,084件(2.7%)、月額2=3,437件(1.5%)、月額3=1,791件(0.8%)...
- 合計で約12,000件程度のデータポイント（少ないがゼロではない）
- 「データ件数: N件」をレスポンスに含める

### Task 1-2: 品質スコア修正

**問題**: `損益差額比率`(存在しない)、`経験5年以上比率`(存在しない)を参照
**修正**:
- 品質スコア4軸を実在データのみで再設計:
  - 安全性: BCP策定、損害賠償保険
  - 品質: 第三者評価、ICT活用
  - 人材: 常勤比率、経験10年以上割合、離職率（逆数）
  - 運営: 加算取得数、事業年数

### Task 1-3: 要介護度・重度者割合

**問題**: 要介護1-5をskipしていたため常にNull
**修正**: Python側で直接計算（skipの概念がない）
- 平均要介護度 = Σ(i × 要介護i) / Σ(要介護i)
- 重度者割合 = (要介護4+5) / 利用者総数

### Task 1-4: 稼働率計算の適正化

**問題**: 利用者総数/定員が100%超え、訪問系にも適用
**修正**:
- 通所・入所系（定員が意味のある種別）のみ計算
- 上限クランプなし（登録充足率として表示）

---

## Phase 2: Rustバックエンド方式D対応

**目的**: DataFrameベースの集計をkpi_cacheベースに切り替える
**依存**: Phase 0完了

### Task 2-1: kpi_cache読み込みモジュール

**新規ファイル**: `kaigo-bi-backend/src/services/cache_store.rs`

```rust
pub struct CacheStore {
    cache: HashMap<String, serde_json::Value>,
}

impl CacheStore {
    pub async fn load(db: &Database) -> Result<Self, AppError> {
        // SELECT key, value FROM kpi_cache
        // → HashMap<String, Value> に格納
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        self.cache.get(key)
    }
}
```

### Task 2-2: 既存APIルートの切り替え

各ルートハンドラを変更:

```rust
// Before: DataFrame集計
async fn dashboard_kpi(store: &DataStore, params: FilterParams) -> Json<DashboardKpi> {
    let df = store.get_filtered_df(&params);
    let result = aggregator::compute_dashboard_kpi(&df);
    Json(result)
}

// After: キャッシュ読み込み or Turso直接SQL
async fn dashboard_kpi(cache: &CacheStore, db: &Database, params: FilterParams) -> Json<Value> {
    if params.is_empty() {
        // フィルタなし → キャッシュから返す
        Json(cache.get("dashboard_kpi").clone())
    } else {
        // フィルタあり → Turso SQLクエリ
        let result = query_filtered_kpi(db, &params).await;
        Json(result)
    }
}
```

### Task 2-3: 施設検索の直接SQL化

`/api/facilities/search`と`/api/facilities/:id`はDataFrameからTurso SQLに切り替え:
- WHERE句でフィルタ
- LIMIT/OFFSETでページネーション
- ORDER BYでソート

### Task 2-4: DataStore/aggregator.rsの段階的廃止

- data_store.rs: read_from_turso()のDataFrame読み込みを削除
- aggregator.rs: Python側に移行した集計ロジックを削除
- 移行完了まで両方式を並行稼働可能にする（feature flagまたは環境変数）

---

## Phase 3: フロントエンド修復

**目的**: DataPendingPlaceholderを実データに置換
**依存**: Phase 2完了（APIレスポンス形式が変わる場合は調整が必要）

### Task 3-1: 賃金ページ修復

**ファイル**: `kaigo-bi-frontend/src/app/salary/page.tsx`

- scatter/histogram → Phase 1-1で計算された賃金データに接続
- 充填率0.1%の注意バナーを条件付き表示（データ件数をAPIから取得）
- 外部求人給与統計（ts_turso_salary）の並列表示

### Task 3-2: 成長分析ページ修復

**ファイル**: `kaigo-bi-frontend/src/app/growth/page.tsx`

- 安定性マトリクス → 離職率×事業年数散布図（データ存在確認済み）
- 競合密度分析 → 同一地域内施設数で計算可能

### Task 3-3: DD支援ページ修復

**ファイル**: `kaigo-bi-frontend/src/app/due-diligence/page.tsx`

- 加算テーブル → 13項目の○/×表示
- 財務DLリンク → kpi_cacheに含める

### Task 3-4: その他Placeholder除去

- PMIシナジー: シナジー推定ロジック実装
- M&Aスクリーニング: hiddenフィルタ復活
- 収益構造: 加算データ準備中バナー条件化

---

## Phase 4: 拡張機能

**依存**: Phase 3完了推奨

### Task 4-1: 外部DB未使用テーブル活用

**ファイル**: `kaigo-bi-backend/src/routes/external.rs` または `scripts/aggregate_to_cache.py`

未使用8テーブルのAPI追加:
- ts_turso_salary (27,166行) → 求人給与統計
- ts_turso_vacancy (27,166行) → 充足率・欠員率
- v2_external_population (1,742行) → 高齢化率×施設密度
- v2_external_care_demand (576行) → 市場規模・給付費
- v2_external_labor_stats (432行) → 離職率・転職率推移
- v2_external_job_openings_ratio (432行) → 求人倍率推移
- v2_external_minimum_wage_history (151行) → 最賃推移
- v2_external_business_dynamics (235行) → 開業率/廃業率

**外部DBは小規模データなのでRust側で直接SQLクエリで問題なし**（DataFrameに載せる必要なし）

### Task 4-2: サービス種別コントロール

**新規ファイル**:
- `kaigo-bi-frontend/src/lib/service-config.ts`
- `scripts/generate_service_config.py`（HTMLサンプルから自動生成）

34種別のメタデータ:
- 該当KPI一覧
- 該当加算一覧（現行13項目のうちどれが適用されるか）
- カテゴリ分類（訪問系/通所系/入所系/地域密着/居宅計画/その他）

### Task 4-3: 加算分析ページ（新規）

現行13項目 → サービス種別ごとの表示最適化
- 全種別共通: 処遇改善加算I-IV
- 通所系: 入浴介助、個別機能訓練、ADL維持等
- 入所系: 看取り、夜勤配置、栄養マネジメント等

**注意**: 追加の加算（310種中の残り297種）のスクレイピングはPhase 4の範囲外。
現行13項目で分析し、将来的にscrape_kaigo_full.pyのBI_COLUMNSを拡張する形で対応。

---

## 依存関係図

```
Phase 0 (方式D基盤) ← 最優先、他の全てが依存
  ├── 0-1 kpi_cacheテーブル
  ├── 0-2 aggregate_to_cache.py（Phase 1のKPI修正を含む）
  └── 0-3 フィルタ対応

Phase 1 (KPI修正) ← Phase 0に統合
  └── aggregate_to_cache.py内で正しいロジック実装

Phase 2 (Rustバックエンド切替) ← Phase 0完了必須
  ├── 2-1 cache_store.rs
  ├── 2-2 APIルート切替
  ├── 2-3 施設検索SQL化
  └── 2-4 DataFrame廃止

Phase 3 (フロントエンド修復) ← Phase 2完了必須
  ├── 3-1 salary
  ├── 3-2 growth
  ├── 3-3 DD
  └── 3-4 その他

Phase 4 (拡張) ← Phase 3完了推奨
  ├── 4-1 外部DB → Phase 2と並行可能（Rust側で直接SQL）
  ├── 4-2 サービス種別
  └── 4-3 加算分析
```

---

## メモリ比較（方式D移行後）

| 方式 | 20万件時 | 50万件時 | 100万件時 |
|---|---|---|---|
| 現行（全件DataFrame） | ~500-700MB | ~1.5-2GB | OOM |
| **方式D（kpi_cache + SQL）** | **~50MB** | **~50MB** | **~50MB** |

---

## ファイル変更一覧

| Phase | ファイル | 変更種別 | 説明 |
|---|---|---|---|
| 0-1 | Turso DB | DDL | kpi_cacheテーブル作成 |
| 0-2 | `scripts/aggregate_to_cache.py` | **新規** | 事前集計スクリプト |
| 0-3 | Turso DB | DDL | facilitiesテーブルにインデックス追加 |
| 2-1 | `kaigo-bi-backend/src/services/cache_store.rs` | **新規** | キャッシュ読み込みモジュール |
| 2-2 | `kaigo-bi-backend/src/routes/*.rs` | 修正 | 各APIハンドラをキャッシュ読み込みに変更 |
| 2-3 | `kaigo-bi-backend/src/routes/facilities.rs` | 修正 | 施設検索をSQL化 |
| 2-4 | `kaigo-bi-backend/src/services/data_store.rs` | 修正/削除 | DataFrame読み込み廃止 |
| 2-4 | `kaigo-bi-backend/src/services/aggregator.rs` | 修正/削除 | Rust集計ロジック廃止 |
| 3-* | `kaigo-bi-frontend/src/app/*/page.tsx` | 修正 | Placeholder除去、実データ接続 |
| 4-1 | `kaigo-bi-backend/src/routes/external.rs` | 修正 | 新エンドポイント追加 |
| 4-2 | `kaigo-bi-frontend/src/lib/service-config.ts` | **新規** | サービス種別コントロール |
| 4-3 | `kaigo-bi-frontend/src/app/revenue/page.tsx` | 修正 | 加算分析MECE化 |

---

*作成: 2026-03-25*
*前版: 2026-03-24（方式D未考慮で作成→破棄）*
