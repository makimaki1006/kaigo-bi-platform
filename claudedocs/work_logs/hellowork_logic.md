# ハローワーク突合ロジック（既存）

## 全体フロー

```
ハローワークCSV
      ↓
[Cell 0] 電話番号正規化
      ↓
[Cell 1] 取引先(Account)突合 → merged_取引先_対象.csv
      ↓                    → diff_取引先_not_matched.csv
[Cell 2] 責任者(Contact)突合 → merged_責任者_対象.csv
      ↓                    → diff_責任者_not_matched.csv
[Cell 4] 差分結合
      ↓
combined_diff_processed_not_matched.csv（新規候補）
```

## 詳細ロジック

### Cell 0: 電話番号正規化
- **入力カラム**: `選考担当者ＴＥＬ`
- **出力カラム**: `選考担当者ＴＥＬ_加工`
- **処理内容**:
  - 数字以外をすべて除去 `re.sub(r"[^\d]", "", str(val))`
  - 先頭に `'` を付与（Excel数値化防止）

### Cell 1: 取引先との突合
- **突合キー（Salesforce側）**: `Phone`, `Phone2__c`（優先順）
- **突合方式**: melt（縦持ち変換）+ merge
- **出力**:
  - `merged_取引先_対象.csv` - マッチした取引先にハロワデータを結合
  - `diff_取引先_not_matched.csv` - マッチしなかったハロワデータ

### Cell 2: 責任者との突合
- **突合キー（Salesforce側）**: `Phone`, `Phone2__c`, `MobilePhone`, `MobilePhone2__c`（優先順）
- **突合方式**: 取引先と同じ
- **出力**:
  - `merged_責任者_対象.csv` - マッチした責任者にハロワデータを結合
  - `diff_責任者_not_matched.csv` - マッチしなかったハロワデータ

### Cell 3: リードとの突合
- **突合キー（Salesforce側）**: `Phone`, `MobilePhone`, `Phone2__c`, `MobilePhone2__c`（優先順）
- **突合方式**: 取引先と同じ
- **出力**:
  - `merged_リード.csv` - マッチしたリードにハロワデータを結合
  - `diff_リード_not_matched.csv` - マッチしなかったハロワデータ

### Cell 4: 差分結合
- 取引先でも責任者でもマッチしなかったデータを結合
- 重複キーは先頭行のみ採用
- → **新規リード候補**

### Cell 5: 職業分類コードフィルタ ⚠️ 重要
- **対象カラム**: `職業分類１（コード）`, `職業分類２（コード）`, `職業分類３（コード）`
- **フィルタ条件**: いずれかのカラムが指定コードセットに含まれる行のみ抽出
- **コード正規化**: 全角→半角、各種ハイフン統一、5桁→3桁-2桁形式

#### 医療系コードセット（メイン）
```
021-01〜021-04, 022-01〜022-02, 023-01〜023-99
024-01〜024-09, 026-01〜026-02, 027-99
028-01〜028-99, 029-01〜029-03, 030-01〜030-03
031-01〜031-99, 032-01〜032-03
049-01〜049-99, 050-01〜050-99, 051-01〜051-02
```

#### 飲食系コードセット（サブ）
```
025-01〜025-02, 055-06〜055-99
072-04〜072-05, 040-99, 096-01, 099-04
```

#### 追加コードセット
```
361-01, 361-03, 144-01〜144-02, 145-01, 037-01, 037-05
```

### Cell 10: 事業所番号フィルタ
- 形式: `4桁-6桁-1桁`（例: 1301-123456-7）
- 全角→半角変換、各種ハイフン統一

## 重要なポイント

### 電話番号正規化ルール
```python
def normalize_phone(val):
    if pd.isna(val):
        return val
    digits = re.sub(r"[^\d]", "", str(val))
    return f"'{digits}" if digits else pd.NA
```

### 複数電話番号での突合（melt方式）
```python
# 縦持ち変換
lookup_long = (
    df_lookup.reset_index()
    .melt(id_vars="index", value_vars=LOOKUP_KEYS_ORDER,
          var_name="phone_field", value_name="raw_phone")
    .assign(phone_norm=lambda d: d["raw_phone"].apply(normalize_phone))
    .dropna(subset=["phone_norm"])
)

# 優先順位マップ
priority_map = {name: idx for idx, name in enumerate(LOOKUP_KEYS_ORDER)}
lookup_long["priority"] = lookup_long["phone_field"].map(priority_map)

# マージ後、同一indexで優先順位の高いものを採用
matched_long = (
    df_merge_long[df_merge_long["_merge"] == "both"]
    .sort_values(["index", "priority"])
    .drop_duplicates(subset="index", keep="first")
)
```

## Salesforceオブジェクトの電話番号フィールド

| オブジェクト | フィールド | 優先順位 |
|-------------|-----------|---------|
| Account（取引先） | Phone | 1 |
| Account（取引先） | Phone2__c | 2 |
| Contact（責任者） | Phone | 1 |
| Contact（責任者） | Phone2__c | 2 |
| Contact（責任者） | MobilePhone | 3 |
| Contact（責任者） | MobilePhone2__c | 4 |

## ハローワークCSVの主要カラム

- `選考担当者ＴＥＬ` - 突合キー（電話番号）
- `事業所番号` - 4桁-6桁-1桁形式

## 実装状況

| 項目 | 状況 | 備考 |
|------|------|------|
| 電話番号正規化 | ✅ 完了 | `HelloWorkService.normalize_phone()` |
| 取引先突合 | ✅ 完了 | Phone, Phone2__c |
| 責任者突合 | ✅ 完了 | Phone, Phone2__c, MobilePhone, MobilePhone2__c |
| **リード突合** | ✅ **完了** | Phone, MobilePhone, Phone2__c, MobilePhone2__c |
| 差分結合 | ✅ 完了 | 新規リード候補抽出（3オブジェクト共通差分） |
| **職業分類フィルタ** | ✅ **完了** | 医療系 + 飲食系コードセット |
| **契約先フィルタ** | ✅ **完了** | `HelloWorkService.filter_by_contract_accounts()` |
| **Bulk API 2.0 Update** | ✅ **完了** | `scripts/bulk_update.py` |

## 契約先フィルタ連携

### 概要
- **レポート名**: 成約先と重複検知する用
- **レポートID**: `00Odc000005FHs5EAG`
- **取得方法**: Bulk API 2.0（Report API の 2000件制限回避）

### フィルタ条件（レポート相当）
```sql
Status__c LIKE '%商談中%'
OR Status__c LIKE '%プロジェクト進行中%'
OR Status__c LIKE '%深耕対象%'
OR Status__c LIKE '%過去客%'
OR RelatedAccountFlg__c = 'グループ案件進行中'
OR RelatedAccountFlg__c = 'グループ過去案件実績あり'
```

### 取得フィールド
- Id, Name, Phone, Email__c, CompanyName__c
- CorporateNumber__c, CorporateIdentificationNumber__c
- Address__c, PresidentName__c, PresidentTitle__c
- Status__c, RelatedAccountFlg__c

### 使用方法

```bash
# 契約先CSVを指定してパイプライン実行
python scripts/hellowork_pipeline.py input.csv \
  --accounts-csv accounts.csv \
  --contacts-csv contacts.csv \
  --contract-csv contract_accounts.csv

# Salesforceから契約先を自動取得
python scripts/hellowork_pipeline.py input.csv \
  --fetch-from-sf \
  --fetch-contract

# 契約先データのみ取得
python scripts/export_contract_report.py
```

### 出力ファイル（契約先フィルタ使用時）
- `final_new_leads.csv` - 契約先除外後の最終新規リード候補
- `excluded_contract_matches.csv` - 契約先に該当して除外されたデータ

---
最終更新: 2026-01-07
元ファイル: ハロワデータをmedicaように加工するやつ_月例インポート用 (1).ipynb
チェックポイント: claudedocs/hellowork_checkpoint_20260107.md
