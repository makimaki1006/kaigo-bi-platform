# kaigo-bi データパイプライン設計

> スクレイピング → ETL → 集計 → キャッシュ → API配信

---

## 課題

Rustバックエンドが起動時に全施設データ（現在128K件、最終20万件）をメモリに載せるため、Render 512MBでOOMが発生する。データ増加に伴い、プランを上げても同じ問題が再発する。

## 解決策: 事前集計パイプライン

```
[スクレイピング] → [ETL/集計(Python)] → [Turso集計テーブル] → [Rust API(軽量読み込み)]
```

---

## パイプライン構成

### Step 1: スクレイピング（既存）

```
python scripts/scrape_kaigo_fast.py --mode full --workers 8
```
- 出力: `data/output/kaigo_scraping/by_service/{code}_{name}.csv`
- 実行タイミング: 手動、または定期（月1回程度）

### Step 2: Turso生データアップロード（既存）

```
python scripts/upload_full_services_to_turso.py
```
- CSVをTursoの`facilities`テーブルにINSERT
- 派生カラム（prefecture, corp_type, turnover_rate等）も計算して格納

### Step 3: 集計実行（新規）

```
python scripts/aggregate_to_cache.py
```

このスクリプトが以下を実行:

#### 3-1. Tursoから生データ読み込み
```python
# facilitiesテーブルから必要カラムだけSELECT
SELECT prefecture, service_code, service_name, corp_type, corp_number,
       staff_total, staff_fulltime, capacity, turnover_rate, fulltime_ratio,
       years_in_business, kasan_count, quality_score, ...
FROM facilities
```

#### 3-2. Pandas/Polarsで集計

```python
# Dashboard KPI
dashboard_kpi = {
    "total_facilities": len(df),
    "avg_staff": df["staff_total"].mean(),
    "avg_capacity": df["capacity"].mean(),
    "avg_turnover_rate": df["turnover_rate"].mean(),
    ...
}

# 都道府県別集計
by_prefecture = df.groupby("prefecture").agg({
    "facility_count": "count",
    "avg_staff": ("staff_total", "mean"),
    "avg_turnover_rate": ("turnover_rate", "mean"),
    ...
})

# サービス別集計
by_service = df.groupby("service_code").agg(...)

# 法人グループ集計
corp_group = df.groupby("corp_number").agg(...)

# 品質KPI
quality_kpi = {
    "bcp_rate": df["bcp"].sum() / len(df),
    "ict_rate": df["ict"].sum() / len(df),
    ...
}

# 加算取得率
kasan_rates = [
    {"name": "処遇改善I", "rate": df["kasan_1"].sum() / len(df)},
    ...
]

# 離職率分布
turnover_dist = [
    {"range": "0-5%", "count": len(df[df.turnover < 0.05])},
    ...
]

# etc. 全APIエンドポイントに対応する集計を実施
```

#### 3-3. 集計結果をTursoのキャッシュテーブルに保存

```python
# kpi_cache テーブル構造
# | key (TEXT PRIMARY KEY) | value (TEXT/JSON) | updated_at (TEXT) |

cache_data = {
    "dashboard_kpi": json.dumps(dashboard_kpi),
    "by_prefecture": json.dumps(by_prefecture_list),
    "by_service": json.dumps(by_service_list),
    "workforce_kpi": json.dumps(workforce_kpi),
    "turnover_distribution": json.dumps(turnover_dist),
    "kasan_rates": json.dumps(kasan_rates),
    "quality_kpi": json.dumps(quality_kpi),
    "quality_score_distribution": json.dumps(score_dist),
    "corp_group_kpi": json.dumps(corp_group_kpi),
    "growth_kpi": json.dumps(growth_kpi),
    "establishment_trend": json.dumps(trend),
    ...
}

for key, value in cache_data.items():
    INSERT OR REPLACE INTO kpi_cache (key, value, updated_at)
    VALUES (?, ?, datetime('now'))
```

### Step 4: Rustバックエンド（変更）

現在: 起動時に全件DataFrame → 各APIでPolars集計
変更後: 起動時にkpi_cacheテーブルを読み込み → 各APIはJSONを返すだけ

```rust
// 起動時
let cache: HashMap<String, serde_json::Value> = read_kpi_cache(db).await;

// API: GET /api/dashboard/kpi
async fn dashboard_kpi(cache: &Cache) -> Json<Value> {
    Json(cache.get("dashboard_kpi").clone())
}
```

**施設検索だけは生データが必要** → 検索時にTursoにSQLクエリ（LIMIT付き）

---

## テーブル設計

### kpi_cache テーブル

```sql
CREATE TABLE IF NOT EXISTS kpi_cache (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,          -- JSON文字列
    updated_at TEXT NOT NULL,     -- ISO 8601
    row_count INTEGER DEFAULT 0  -- 元データ件数（参考）
);
```

### キャッシュキー一覧

| キー | 対応API | 内容 |
|------|--------|------|
| `dashboard_kpi` | `/api/dashboard/kpi` | 総施設数、平均従業者数等 |
| `dashboard_by_prefecture` | `/api/dashboard/by-prefecture` | 都道府県別施設数・KPI |
| `dashboard_by_service` | `/api/dashboard/by-service` | サービス別施設数 |
| `workforce_kpi` | `/api/workforce/kpi` | 離職率、常勤比率等 |
| `workforce_turnover_dist` | `/api/workforce/turnover-distribution` | 離職率ヒストグラム |
| `workforce_by_prefecture` | `/api/workforce/by-prefecture` | 都道府県別離職率 |
| `workforce_by_size` | `/api/workforce/by-size` | 規模別離職率 |
| `workforce_exp_dist` | `/api/workforce/experience-distribution` | 経験者分布 |
| `workforce_exp_turnover` | `/api/workforce/experience-vs-turnover` | 経験vs離職率 |
| `revenue_kpi` | `/api/revenue/kpi` | 加算取得数、稼働率等 |
| `revenue_kasan_rates` | `/api/revenue/kasan-rates` | 加算13項目取得率 |
| `revenue_occupancy_dist` | `/api/revenue/occupancy-distribution` | 稼働率分布 |
| `quality_kpi` | `/api/quality/kpi` | 品質スコア、BCP率等 |
| `quality_score_dist` | `/api/quality/score-distribution` | 品質スコア分布 |
| `quality_rank_dist` | `/api/quality/rank-distribution` | ランク分布 |
| `quality_radar` | `/api/quality/category-radar` | 4軸レーダー |
| `quality_by_prefecture` | `/api/quality/by-prefecture` | 都道府県別品質 |
| `growth_kpi` | `/api/growth/kpi` | 新規施設数、平均事業年数 |
| `growth_trend` | `/api/growth/establishment-trend` | 年別設立推移 |
| `growth_years_dist` | `/api/growth/years-distribution` | 事業年数分布 |
| `corp_group_kpi` | `/api/corp-group/kpi` | 法人数、多施設法人数 |
| `corp_group_size_dist` | `/api/corp-group/size-distribution` | 法人規模分布 |
| `corp_group_top_corps` | `/api/corp-group/top-corps` | Top20法人 |
| `market_choropleth` | `/api/market/choropleth` | 都道府県別ヒートマップ |
| `market_by_service` | `/api/market/by-service-bar` | サービス別棒グラフ |
| `market_corp_donut` | `/api/market/corp-type-donut` | 法人種別ドーナツ |
| `meta` | `/api/meta` | メタ情報（件数、都道府県一覧等） |
| `financial_health` | `/api/external/financial-health` | 財務健全度 |
| `service_portfolio` | `/api/external/service-portfolio` | サービスポートフォリオ |
| `hiring_difficulty` | `/api/external/hiring-difficulty` | 採用難易度 |

---

## フィルタ対応

### 問題
事前集計ではフィルタ（都道府県、サービス種別）の組み合わせごとに集計が必要。

### 解決策
1. **フィルタなしの全体集計**をキャッシュ（上記テーブル）
2. **フィルタ付きリクエスト**はTursoにSQLクエリを直接発行

```rust
// フィルタなし → キャッシュから返す（高速）
if params.is_empty() {
    return cache.get("dashboard_kpi");
}

// フィルタあり → Tursoに直接SQL
let sql = "SELECT COUNT(*) as total, AVG(staff_total) as avg_staff ...
           FROM facilities WHERE prefecture IN (?, ?)";
let result = db.query(sql, params).await;
```

---

## 実行フロー

### 初回セットアップ
```bash
# 1. スクレイピング
python scripts/scrape_kaigo_fast.py --mode full --workers 8

# 2. Tursoアップロード
python scripts/upload_full_services_to_turso.py

# 3. 集計実行
python scripts/aggregate_to_cache.py

# 4. バックエンド起動
cargo run  # kpi_cacheから読み込み、メモリ数十MB
```

### データ更新時（ルーチン）
```bash
# 1. 新サービスのスクレイピング完了
# 2. Tursoにアップロード
python scripts/upload_full_services_to_turso.py

# 3. 集計再実行（全キャッシュ更新）
python scripts/aggregate_to_cache.py

# 4. バックエンド再起動（またはキャッシュリロードAPI呼び出し）
```

### 注意事項
- 集計スクリプトは**冪等**（何度実行しても同じ結果）
- `INSERT OR REPLACE`でキャッシュを上書き
- `updated_at`で集計日時を記録（データ鮮度の確認用）
- フィルタ付きクエリは生テーブルに直接SQL → インデックスが重要

---

## メモリ比較

| 方式 | データ20万件時 | データ50万件時 |
|------|-------------|-------------|
| 現在（全件DataFrame） | ~800MB | ~2GB |
| 方式D（キャッシュ+遅延SQL） | ~50MB | ~50MB |

---

## 実装優先順位

1. `scripts/aggregate_to_cache.py` の作成
2. Tursoに`kpi_cache`テーブル作成
3. Rustバックエンドのキャッシュ読み込み対応
4. フィルタ付きクエリのSQL直接発行対応
5. 施設検索のSQL直接発行対応（DataFrameなし）
