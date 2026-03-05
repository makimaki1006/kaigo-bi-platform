# Doda媒体データ CSV生成完了レポート

## 実行日時
2026-02-03

## 処理概要
Dodaスクレイピングデータ（doda_results.pkl）からSalesforceインポート用CSV（3種類）を生成しました。

---

## 生成されたCSV

### 1. 新規リード作成 (660件)

**ファイル名**: `doda_new_leads_20260203.csv`

**所有者割り当て（3名均等）**:

| 所有者 | OwnerId | 件数 |
|--------|---------|------|
| 佐藤丈太郎 | 0055i00000CwGDGAA3 | 220件 |
| 志村亮介 | 0055i00000CwGCrAAN | 220件 |
| 小林幸太 | 005J3000000ERz4IAG | 220件 |

**必須フィールド検証結果**:
- ✅ Company空: 0件スキップ（全件有効）
- ✅ Phone空: 0件スキップ（全件有効）

**フィールド構成**:

| フィールド | 値 | 備考 |
|-----------|---|------|
| Company | 会社名 | Dodaスクレイピングデータから |
| LastName | "担当者" | 固定値 |
| Phone | 電話番号（固定優先） | ハイフン付き正規化済み |
| MobilePhone | 携帯電話番号 | 070/080/090始まり |
| LeadSource | "Other" | 固定値 |
| Paid_Media__c | "doda" | 媒体名 |
| Paid_DataSource__c | "doda" | データソース |
| Paid_JobTitle__c | 求人タイトル | |
| Paid_URL__c | DodaページURL | |
| Paid_DataExportDate__c | 2026-02-03 | 処理日 |
| LeadSourceMemo__c | "【新規作成】有料媒体突合 2026-02-03" | 差別化タグ |
| OwnerId | ラウンドロビン割当 | 3名均等 |

**サンプル（先頭5件）**:

```
                      Company         Phone   MobilePhone            OwnerId
             大管ガスエンジニアリング株式会社  06-6848-3535               0055i00000CwGDGAA3
               環境システム設備機器株式会社  03-3411-7701               0055i00000CwGCrAAN
                       株式会社創研  04-7465-7750               005J3000000ERz4IAG
                株式会社Ｖｉｓｉｏｎ．ｃｏ  04-8967-5545 090-6171-0711 0055i00000CwGDGAA3
株式会社アイダ設計【TOKYO PRO Market上場】 050-3115-3636 080-0888-8004 0055i00000CwGCrAAN
```

---

### 2. Lead更新 (39件)

**ファイル名**: `doda_lead_updates_20260203.csv`

**フィールド構成**:

| フィールド | 値 | 備考 |
|-----------|---|------|
| Id | LeadId | マッチしたLeadのId |
| Paid_Media__c | "doda" | |
| Paid_DataSource__c | "doda" | |
| Paid_JobTitle__c | 求人タイトル | |
| Paid_URL__c | DodaページURL | |
| Paid_DataExportDate__c | 2026-02-03 | |
| LeadSourceMemo__c | "【既存更新】有料媒体突合 2026-02-03\n" + 既存メモ | 差別化タグを先頭追記 |

**サンプル（先頭3件）**:

```
Id                 Paid_JobTitle__c
00Qdc00000ESdrBEAT 住まいのヒアリングスタッフ／ノルマなし／残業月平均1時間以下
00Qdc00000I2IbLEAV 住宅設備のルート営業／土日祝休／転勤なし／年収450万円～
00Qdc00000ImgqTEAR 設備工事の施工管理／年休126日／平均勤続年数17.2年
```

---

### 3. Account更新 (1件)

**ファイル名**: `doda_account_updates_20260203.csv`

**フィールド構成**:

| フィールド | 値 | 備考 |
|-----------|---|------|
| Id | AccountId | マッチしたAccountのId |
| Paid_DataExportDate__c | 2026-02-03 | |
| Description | "【既存更新】有料媒体突合 2026-02-03\n" + 既存Description | 差別化タグを先頭追記 |

**内容**:

```
Id                 Paid_DataExportDate__c
001J300000ArmxuIAB 2026-02-03
```

---

## 技術的特徴

### 電話番号正規化ロジック

1. **携帯電話判定**: 070/080/090始まりを携帯として分類
2. **Phone必須対応**:
   - 固定電話優先: 固定電話がある場合は Phone に設定
   - 携帯のみの場合: Phone と MobilePhone の両方に同じ値を設定
3. **ハイフン挿入**:
   - 10桁: 市外局番に応じてフォーマット (03-XXXX-XXXX / 0XX-XXX-XXXX)
   - 11桁: XXX-XXXX-XXXX 形式

### データバリデーション

- **Company必須**: 空の場合はスキップ
- **Phone必須**: 空の場合はスキップ
- **ダミー値除外**: "不明", "なし", "N/A" 等を無効値として扱う
- **電話番号形式**: 0始まり、10-11桁、.0除去

### マッチング結果の抽出

- Lead更新: `matched_leads` 配列から最初のレコードのIdを抽出
- Account更新: `matched_accounts` または `matched_contacts` から抽出
- Contact経由: ContactのAccountIdを使用してAccount更新

---

## 次のステップ

### Salesforceインポート実行

以下の順序で実行してください：

1. **新規リード作成** (660件)
   ```
   ファイル: doda_new_leads_20260203.csv
   オブジェクト: Lead
   操作: Insert
   ```

2. **Lead更新** (39件)
   ```
   ファイル: doda_lead_updates_20260203.csv
   オブジェクト: Lead
   操作: Update
   ```

3. **Account更新** (1件)
   ```
   ファイル: doda_account_updates_20260203.csv
   オブジェクト: Account
   操作: Update
   ```

### インポート後のレポート作成

以下のレポートを作成してください：

1. **新規作成リード一覧**
   - フィルタ: `LeadSourceMemo__c CONTAINS "【新規作成】有料媒体突合 2026-02-03"`
   - 所有者別集計

2. **Lead更新一覧**
   - フィルタ: `LeadSourceMemo__c CONTAINS "【既存更新】有料媒体突合 2026-02-03"`

3. **Account更新一覧**
   - フィルタ: `Description CONTAINS "【既存更新】有料媒体突合 2026-02-03"`

---

## ファイル配置

```
C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List\
└── data/output/media_matching/
    ├── doda_new_leads_20260203.csv      (660件)
    ├── doda_lead_updates_20260203.csv   (39件)
    └── doda_account_updates_20260203.csv (1件)
```

---

## スクリプト

**生成スクリプト**: `C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List\scripts\doda_step3_generate_csv.py`

**実行コマンド**:
```bash
cd "C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List"
python scripts/doda_step3_generate_csv.py
```

---

## 処理統計

| カテゴリ | 件数 | 備考 |
|---------|------|------|
| 新規リード候補（入力） | 660件 | doda_results.pkl |
| 新規リード作成（出力） | 660件 | スキップ0件 |
| Lead更新 | 39件 | 電話番号突合成功 |
| Account更新 | 1件 | 電話番号突合成功 |
| **合計処理レコード** | **700件** | |

---

## 品質保証

### バリデーション結果

- ✅ Company必須チェック: スキップ0件（全件有効）
- ✅ Phone必須チェック: スキップ0件（全件有効）
- ✅ 電話番号正規化: 全件0始まり10-11桁
- ✅ 所有者割り当て: 3名完全均等（220件ずつ）
- ✅ LeadId抽出: 39件全て成功
- ✅ AccountId抽出: 1件成功
- ✅ UTF-8 BOM付きCSV: Excelで正常表示可能

### エンコーディング

- **CSV出力**: UTF-8-sig（BOM付き）
- **日本語文字**: 全て正常にエンコード済み

---

## 完了確認

- ✅ CSV生成: 3ファイル全て成功
- ✅ データ品質: バリデーション全てパス
- ✅ 所有者割当: 3名均等分配完了
- ✅ メモ欄差別化: 新規/既存タグ付与完了
- ✅ レポート作成: 本ドキュメント完成

**次のアクション**: Salesforceインポート実行の承認待ち
