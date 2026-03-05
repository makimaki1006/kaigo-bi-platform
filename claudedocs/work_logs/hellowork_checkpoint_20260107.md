# ハローワークデータ処理チェックポイント

**作成日**: 2026-01-07
**最終更新**: 2026-01-07 15:30
**ステータス**: ✅ 全フィールドマッピング確定、メモ更新ルール確定、実装完了

---

## 処理済み内容

### 1. ハローワークCSV クレンジング
- **入力**: `RCMEB002002_M100.csv` (340,622件)
- **処理**: 電話番号正規化（`選考担当者ＴＥＬ` → `選考担当者ＴＥＬ_加工`）
- **出力**: `data/output/hellowork/processed_hellowork.csv`

### 2. Salesforce データ取得（Bulk API 2.0）
| オブジェクト | 件数 | ファイル |
|-------------|------|---------|
| Account | 253,859 | `data/output/Account_20260107_003958.csv` |
| Contact | 249,930 | `data/output/Contact_20260107_004329.csv` |
| Lead | 218,647 | `data/output/Lead_20260107_003026.csv` |

### 3. 突合処理（電話番号 melt方式）

| オブジェクト | マッチ件数 | マッチ率 |
|-------------|-----------|---------|
| Account | 19,193 | 7.6% |
| Contact | 22,437 | 9.0% |
| Lead | 67,816 | 31.0% |

### 4. 職業分類コードフィルタ
医療系 + 飲食系コードセットで絞り込み適用済み

### 5. 新規リード候補
- Account/Contact/Lead いずれにもマッチしなかったデータ
- **4,381件** (`data/output/hellowork/final_new_leads.csv`)

---

## フィールドマッピング（確定）

### Account/Lead 共通 基本フィールド

| Salesforce フィールド | UI名 | ハローワーク カラム | 備考 |
|---------------------|------|-------------------|------|
| `CorporateIdentificationNumber__c` | 法人番号（入力用） | `法人番号` | 13桁 |
| `NumberOfEmployees` | 従業員数 | `従業員数企業全体（コード）` | 整数値 |
| `BillingPostalCode` / `PostalCode` | 郵便番号 | `事業所郵便番号` | XXX-XXXX形式 |
| `BillingStreet` / `Street` | 町名・番地 | `事業所所在地` | |
| `Website` | Web サイト | `事業所ホームページ` | URL検証済みのみ |
| `ContactName__c` | 担当者名 | `選考担当者氏名漢字` | Account のみ |
| `ContactTitle__c` | 担当者役職 | `選考担当者課係名／役職名` | Account のみ |
| `PresidentName__c` | 代表者氏名 | `代表者名` | |
| `PresidentTitle__c` | 代表者役職 | `代表者役職` | |

### ハローワーク専用フィールド（Account/Lead 共通）

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
| `Hellowork_DataImportDate__c` | 【ハローワーク】データ入稿日 | 処理実行日（自動） |
| `Hellowork_URL__c` | 【ハローワーク】求人URL | `事業所ホームページ` |

### Contact 更新フィールド

| Salesforce フィールド | UI名 | ハローワーク カラム | 備考 |
|---------------------|------|-------------------|------|
| `Email` | メール | `選考担当者Ｅメール` | メール検証済みのみ |
| `Description` | 説明 | （生成） | 更新ログ追記 |

※ Contactは既存データを尊重し、メールと更新ログ以外は基本的に更新しない

### Lead 追加フィールド

| Salesforce フィールド | UI名 | ハローワーク カラム |
|---------------------|------|-------------------|
| `Company` | 事業所名 / 法人名 | `事業所名漢字` |
| `Email` | メール | `選考担当者Ｅメール` |
| `LeadSourceMemo__c` | リードソースメモ | （生成） |

---

## メモ更新ルール（確定）

### 設計思想
- **メモ系フィールドの役割**: レポート作成時の検索機能（SEO的なキーワード置き場）
- **追記方式**: 既存データの後に `★` 区切りで追加

### フィールド別ルール

| オブジェクト | フィールド | 形式 | 内容 |
|------------|----------|------|------|
| Account | `Publish_ImportText__c` | 更新ログ | 変更内容を記録 |
| Contact | `Description` | 更新ログ | 変更内容を記録 |
| Lead | `LeadSourceMemo__c` | キーワード | `YYYYMMDD_ハロワ_カテゴリ` |
| Lead | `Publish_ImportText__c` | 更新ログ | 変更内容を記録 |

### 更新ログ形式

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

### LeadSourceMemo形式

```
20260107_ハロワ_介護
```

- **形式**: `YYYYMMDD_ハロワ_カテゴリ`
- **カテゴリ**: 職種から主要キーワードを抽出（介護、看護、清掃、調理 等）
- **用途**: レポート検索用キーワード

---

## 生成済み更新データ

| ファイル | 件数 | 対象 |
|---------|------|-----|
| `account_full_updates.csv` | 19,193 | Account全フィールド + 更新ログ |
| `contact_full_updates.csv` | 22,437 | Contact（Email + 更新ログ） |
| `lead_full_updates.csv` | 67,816 | Lead全フィールド + メモ |

---

## 出力ファイル一覧

```
data/output/hellowork/
├── processed_hellowork.csv              # クレンジング済みハロワデータ
├── merged_取引先.csv                     # Account突合結果
├── merged_責任者.csv                     # Contact突合結果
├── merged_リード.csv                     # Lead突合結果
├── diff_取引先_not_matched.csv          # Account未マッチ
├── diff_責任者_not_matched.csv          # Contact未マッチ
├── diff_リード_not_matched.csv          # Lead未マッチ
├── final_new_leads.csv                  # 新規リード候補 (4,381件)
├── account_full_updates.csv             # Account更新用 (19,193件)
├── contact_full_updates.csv             # Contact更新用 (22,437件)
└── lead_full_updates.csv                # Lead更新用 (67,816件)
```

---

## 実装ファイル

| スクリプト | 機能 |
|-----------|------|
| `scripts/generate_hellowork_updates.py` | 更新データ生成（フィールドマッピング + メモ生成） |
| `scripts/bulk_update.py` | Bulk API 2.0 更新（★区切り追記対応） |
| `src/services/hellowork_service.py` | 突合処理、フィルタ処理 |

---

## インポート実行コマンド

```bash
# Bulk API 2.0 を使用

# Account 更新
python scripts/bulk_update.py --object Account \
  --csv data/output/hellowork/account_full_updates.csv \
  --field Publish_ImportText__c \
  --execute

# Contact 更新
python scripts/bulk_update.py --object Contact \
  --csv data/output/hellowork/contact_full_updates.csv \
  --field Description \
  --execute

# Lead 更新
python scripts/bulk_update.py --object Lead \
  --csv data/output/hellowork/lead_full_updates.csv \
  --field Publish_ImportText__c \
  --execute
```

---

## 次のステップ

1. **インポート実行**（ユーザー許可後）
   - dry-run で確認 → --execute で実行

2. **他データソースの処理**

---

## ultrathinkレビュー結果

**実施日**: 2026-01-07
**結果**: 20観点中18項目問題なし、2項目軽微注意（意図的設計）

### 確認済み項目
- フィールドマッピング正確性 ✅
- API名の正確性（`Hellowork_*__c`形式） ✅
- 日付フォーマット（YYYY-MM-DD） ✅
- 数値変換ロジック ✅
- 郵便番号正規化（XXX-XXXX） ✅
- URL/メール検証 ✅
- 変更ログ形式 ✅
- LeadSourceMemo形式 ✅
- 空値ハンドリング ✅
- エンコーディング（utf-8-sig） ✅

### 逆証明結果
全項目で反証失敗 → 設計の妥当性を確認

---

**注意**: インポート実行前に必ずユーザー許可を取得すること（CLAUDE.md ルール）
