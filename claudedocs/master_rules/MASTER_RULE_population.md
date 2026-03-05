# 市区町村人口データ付与プロジェクト

> **作成日**: 2026-01-09
> **ステータス**: データ準備完了、Salesforce更新待ち

---

## 概要

Lead/Accountレコードに市区町村単位の人口データを付与し、人口規模でのセグメント分析を可能にする。

### 目的
- 人口規模による営業優先度の判断
- 地域セグメント分析
- ターゲティング精度の向上

---

## データソース

### 内閣府 市区町村人口データ
- **出典**: 総務省『国勢調査（2010年）』
- **件数**: 1,741 市区町村
- **年次**: 2010年（最新の市区町村単位データ）

### 作成したファイル

| ファイル | 説明 |
|---------|------|
| `data/population/cao_population.csv` | 元データ（CP932エンコード） |
| `data/population/municipality_population.csv` | 整形済みマスタCSV |
| `data/population/population_mapping.json` | 住所マッチング用JSON（5,183エントリ） |

---

## スクリプト

### 1. データ準備スクリプト
**ファイル**: `scripts/prepare_population_data.py`

```bash
python scripts/prepare_population_data.py
```

- 内閣府CSVを読み込み（CP932エンコード）
- 市区町村マスタCSVを出力
- 住所マッチング用JSONを出力

### 2. 人口データ付与スクリプト
**ファイル**: `scripts/add_population_to_records.py`

```bash
# Lead（テスト：1000件）
python scripts/add_population_to_records.py --object Lead --limit 1000 --dry-run

# Account（テスト：1000件）
python scripts/add_population_to_records.py --object Account --limit 1000 --dry-run

# 全件処理（本番）
python scripts/add_population_to_records.py --object Lead
python scripts/add_population_to_records.py --object Account
```

---

## 住所フィールドマッピング

### Lead
| 優先度 | フィールド | 説明 |
|-------|-----------|------|
| 1 | `Address__c` | 詳細住所（最優先） |
| 2 | `Street` | 町名・番地 |
| 3 | `City` | 市区郡 |
| 4 | `Prefecture__c` | 都道府県 |

### Account
| 優先度 | フィールド | 説明 |
|-------|-----------|------|
| 1 | `Address__c` | 詳細住所（最優先） |
| 2 | `HJBG_Address__c` | 法人番号による住所 |
| 3 | `BillingStreet` | 請求先町名・番地 |
| 4 | `BillingCity` | 請求先市区郡 |

---

## マッチ率

### テスト結果（2026-01-09）

| オブジェクト | サンプル | マッチ成功 | マッチ失敗 | 成功率 |
|-------------|---------|-----------|-----------|--------|
| Lead | 1,000件 | 933件 | 67件 | **93.3%** |
| Account | 1,000件 | 709件 | 291件 | **70.9%** |

### マッチ失敗の原因
1. 住所データが空（`Address__c` が未入力）
2. 都道府県名のみ（市区町村がない）
3. 人口データに存在しない市区町村（合併等）

---

## 人口分布（Lead 1,000件サンプル）

| 人口帯 | 件数 | 割合 |
|-------|------|------|
| 人口不明 | 67 | 6.7% |
| 〜5万 | 57 | 5.7% |
| 5〜10万 | 140 | 14.0% |
| 10〜30万 | 265 | **26.5%** |
| 30〜50万 | 147 | 14.7% |
| 50〜100万 | 142 | 14.2% |
| 100万〜 | 182 | 18.2% |

---

## 市区町村抽出ロジック

### 住所パターン

```python
# パターン1: 政令指定都市（〇〇市〇〇区）
# 例: "横浜市西区" → 検索キー: "横浜市"（市全体の人口を使用）

# パターン2: 東京23区
# 例: "新宿区" → 検索キー: "新宿区"

# パターン3: 通常の市町村
# 例: "前橋市" → 検索キー: "前橋市"

# パターン4: 郡部
# 例: "中頭郡嘉手納町" → 検索キー: "嘉手納町"
```

### 注意点
- 政令指定都市の区は、市全体の人口を返す（区単位データなし）
- 2010年以降の市町村合併は反映されていない可能性あり

---

## 次のステップ

### Salesforce更新を行う場合

1. **カスタムフィールド作成**
   - Lead: `Population__c`（数値、18桁、小数0）
   - Account: `Population__c`（数値、18桁、小数0）

2. **更新スクリプト実行**
   ```bash
   python scripts/add_population_to_records.py --object Lead
   python scripts/add_population_to_records.py --object Account
   ```

3. **レポート作成**
   - 人口帯別リードレポート
   - 人口帯別取引先レポート

### セグメント例
- 大都市圏（100万以上）: 都市部の大規模施設
- 中規模都市（10〜50万）: 地方中核都市
- 小規模市町村（〜10万）: 地方の施設

---

## 改善余地

1. **2020年国勢調査データの取得**
   - e-Statから取得可能だが、動的コンテンツのため手動ダウンロード推奨
   - 市区町村合併が反映された最新データ

2. **区単位データの追加**
   - 政令指定都市の区別人口データを追加すれば精度向上

3. **郵便番号マッチング**
   - 住所が不完全な場合、郵便番号から市区町村を特定

---

## 関連ファイル

```
Salesforce_List/
├── data/
│   └── population/
│       ├── cao_population.csv          # 元データ
│       ├── municipality_population.csv # 市区町村マスタ
│       └── population_mapping.json     # マッチング用JSON
├── scripts/
│   ├── prepare_population_data.py      # データ準備
│   └── add_population_to_records.py    # 人口付与
└── data/output/population/
    ├── lead_population_YYYYMMDD.csv    # Lead処理結果
    └── account_population_YYYYMMDD.csv # Account処理結果
```
