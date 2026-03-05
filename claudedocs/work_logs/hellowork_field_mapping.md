# ハローワーク → Salesforce フィールドマッピング

**作成日**: 2026-01-07
**最終更新**: 2026-01-07 15:30
**ステータス**: ✅ 確定・実装完了

## マッピング方針

- **上書き**: 許可（既存データを上書き）
- **メモフィールド**: ★区切りで追記（既存テキストの後ろに追加）
- **空値**: 空の場合は更新しない（既存値を保持）

---

## Account（取引先）マッピング

### 基本フィールド

| Salesforce フィールド | UI名 | ハローワーク カラム | 変換処理 |
|---------------------|------|-------------------|---------|
| `CorporateIdentificationNumber__c` | 法人番号（入力用） | `法人番号` | そのまま（13桁） |
| `NumberOfEmployees` | 従業員数 | `従業員数企業全体（コード）` | 整数変換 |
| `BillingPostalCode` | 郵便番号(請求先) | `事業所郵便番号` | XXX-XXXX形式 |
| `BillingStreet` | 町名・番地(請求先) | `事業所所在地` | そのまま |
| `Website` | Web サイト | `事業所ホームページ` | URL検証 |
| `ContactName__c` | 担当者名 | `選考担当者氏名漢字` | そのまま |
| `ContactTitle__c` | 担当者役職 | `選考担当者課係名／役職名` | そのまま |
| `PresidentName__c` | 代表者氏名 | `代表者名` | そのまま |
| `PresidentTitle__c` | 代表者役職 | `代表者役職` | そのまま |

### ハローワーク専用フィールド

| Salesforce フィールド | UI名 | ハローワーク カラム |
|---------------------|------|-------------------|
| `Hellowork_JobPublicationDate__c` | 【ハローワーク】求人公開日 | `受付年月日（西暦）` |
| `Hellowork_JobClosedDate__c` | 【ハローワーク】求人掲載終了日 | `求人有効年月日（西暦）` |
| `Hellowork_Industry__c` | 【ハローワーク】産業区分 | `産業分類（名称）` |
| `Hellowork_RecuritmentType__c` | 【ハローワーク】募集職種 | `職種` |
| `Hellowork_EmploymentType__c` | 【ハローワーク】雇用形態 | `雇用形態` |
| `Hellowork_RecruitmentReasonCategory__c` | 【ハローワーク】募集理由区分 | `募集理由区分` |
| `Hellowork_NumberOfRecruitment__c` | 【ハローワーク】採用人数 | `採用人数（コード）` |
| `Hellowork_NumberOfEmployee_Office__c` | 【ハローワーク】従業員数（就業場所） | `従業員数就業場所（コード）` |
| `Hellowork_DataImportDate__c` | 【ハローワーク】データ入稿日 | 処理実行日 |
| `Hellowork_URL__c` | 【ハローワーク】求人URL | `事業所ホームページ` |

### メモフィールド

| Salesforce フィールド | UI名 | 形式 |
|---------------------|------|------|
| `Publish_ImportText__c` | 【掲載情報】入稿データメモ | 更新ログ（★区切り追記） |

---

## Contact（責任者）マッピング

| Salesforce フィールド | UI名 | ハローワーク カラム | 備考 |
|---------------------|------|-------------------|------|
| `Email` | メール | `選考担当者Ｅメール` | メール検証済みのみ |
| `Description` | 説明 | （生成） | 更新ログ（★区切り追記） |

### Contact 更新範囲について

Contactは既存レコードとの突合結果のみ更新。突合に使用した電話番号以外のフィールドは基本的にSalesforce側の既存データを尊重。

**更新対象**:
- `Email`: ハローワークにメールアドレスがある場合のみ
- `Description`: 更新ログ追記

---

## Lead（リード）マッピング

### 基本フィールド

| Salesforce フィールド | UI名 | ハローワーク カラム | 変換処理 |
|---------------------|------|-------------------|---------|
| `Company` | 事業所名 / 法人名 | `事業所名漢字` | そのまま |
| `NumberOfEmployees` | 従業員数 | `従業員数企業全体（コード）` | 整数変換 |
| `PostalCode` | 郵便番号 | `事業所郵便番号` | XXX-XXXX形式 |
| `Street` | 町名・番地 | `事業所所在地` | そのまま |
| `Website` | Web サイト | `事業所ホームページ` | URL検証 |
| `Email` | メール | `選考担当者Ｅメール` | メール検証 |
| `PresidentName__c` | 代表者氏名 | `代表者名` | そのまま |
| `PresidentTitle__c` | 代表者役職 | `代表者役職` | そのまま |

### ハローワーク専用フィールド

| Salesforce フィールド | UI名 | ハローワーク カラム |
|---------------------|------|-------------------|
| `Hellowork_JobPublicationDate__c` | 【ハローワーク】求人公開日 | `受付年月日（西暦）` |
| `Hellowork_JobClosedDate__c` | 【ハローワーク】求人掲載終了日 | `求人有効年月日（西暦）` |
| `Hellowork_Industry__c` | 【ハローワーク】産業区分 | `産業分類（名称）` |
| `Hellowork_RecuritmentType__c` | 【ハローワーク】募集職種 | `職種` |
| `Hellowork_EmploymentType__c` | 【ハローワーク】雇用形態 | `雇用形態` |
| `Hellowork_RecruitmentReasonCategory__c` | 【ハローワーク】募集理由区分 | `募集理由区分` |
| `Hellowork_NumberOfRecruitment__c` | 【ハローワーク】採用人数 | `採用人数（コード）` |
| `Hellowork_NumberOfEmployee_Office__c` | 【ハローワーク】従業員数（就業場所） | `従業員数就業場所（コード）` |
| `Hellowork_DataImportDate__c` | 【ハローワーク】データ入稿日 | 処理実行日 |
| `Hellowork_URL__c` | 【ハローワーク】求人URL | `事業所ホームページ` |

### メモフィールド

| Salesforce フィールド | UI名 | 形式 | 備考 |
|---------------------|------|------|------|
| `LeadSourceMemo__c` | リードソースメモ | `YYYYMMDD_ハロワ_カテゴリ` | 255文字制限 |
| `Publish_ImportText__c` | 【掲載情報】入稿データメモ | 更新ログ | ★区切り追記 |

---

## メモフィールド詳細

### 設計思想
メモ系フィールドの役割は**レポート作成時の検索機能**（SEO的なキーワード置き場）

### LeadSourceMemo__c（255文字制限）

```
20260107_ハロワ_介護
```

- **形式**: `YYYYMMDD_ハロワ_カテゴリ`
- **カテゴリ抽出**: 職種から主要キーワード（介護、看護、清掃、調理 等）
- **複数回インポート時**: 既存値に★区切りで追記

### Publish_ImportText__c / Description（長文可）

```
[2026-01-07 ハロワ突合]
【ハローワーク】求人公開日: 空→2026-01-05
【ハローワーク】産業区分: 空→サービス業
【ハローワーク】募集職種: 空→清掃員
従業員数: 空→100
```

- **フィールド名**: UI名を使用
- **変更表示**: `空→新値` または `旧値→新値`
- **追記区切り**: 既存データがある場合は `★` で区切って追加

---

## 変換ロジック

### 従業員数
```python
# コード列を直接使用（整数値）
従業員数企業全体（コード） → int
```

### 郵便番号
```python
# 数字のみ抽出 → ハイフン挿入
re.sub(r'[^\d]', '', val) → XXX-XXXX
```

### 日付
```python
# YYYY/MM/DD → YYYY-MM-DD
受付年月日（西暦） → Salesforce Date型
```

### URL
```python
# http:// または https:// で始まるもののみ許可
if url.startswith('http://') or url.startswith('https://'):
    return url
```

### メール
```python
# @ と . を含むもののみ許可
if '@' in email and '.' in email:
    return email
```

---

## 注意事項

### 空値の扱い
- ハローワークCSVで空の項目は**更新しない**（Salesforce既存値を保持）
- 特に `選考担当者Ｅメール` はほぼ空のため、Emailは既存値優先

### フィールド名の注意
- API名: `Hellowork_*__c`（大文字H、小文字ellowork）
- 旧API名（使用しない）: `HelloWork_*__c`

### 文字数制限
- `LeadSourceMemo__c`: 255文字
- `Hellowork_RecuritmentType__c`: 255文字（職種を切り詰め）

---

## 実装ファイル

| ファイル | 役割 |
|---------|------|
| `scripts/generate_hellowork_updates.py` | フィールドマッピング、メモ生成 |
| `scripts/bulk_update.py` | Bulk API更新、★区切り追記 |

---

最終更新: 2026-01-07
