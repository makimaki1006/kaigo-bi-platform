# ハローワークデータ処理ルール・ロジック

## 概要

ハローワーク求人データをSalesforceと突合し、既存レコードの更新および新規リード候補の抽出を行う。
**成約先への誤架電を防ぐため、電話番号と法人番号の両方で成約先を除外する。**

---

## 全体フロー

```
ハローワークCSV（約340,000件）
        │
        ▼
[STEP 1] 電話番号正規化
        │
        ▼
[STEP 2] Salesforce突合（Account/Contact/Lead）
        │
        ├─→ マッチ → 既存レコード更新データ
        │              ├─ account_full_updates.csv
        │              ├─ contact_full_updates.csv
        │              └─ lead_full_updates.csv
        │
        └─→ 未マッチ → combined_diff_new_leads.csv（差分）
                │
                ▼
        [STEP 3] 職種フィルタ（医療系・飲食系等）
                │
                ▼
        filtered_new_leads.csv
                │
                ▼
        [STEP 4] 成約先フィルタ（電話番号 + 法人番号）
                │
                ▼
        ★ true_new_leads.csv（真の新規リード候補）
```

---

## STEP 1: 電話番号正規化

### 対象カラム
- ハローワーク: `選考担当者ＴＥＬ` → `選考担当者ＴＥＬ_加工`

### 正規化ルール
```python
def normalize_phone(val):
    if pd.isna(val):
        return val
    # 数字以外をすべて除去
    digits = re.sub(r"[^\d]", "", str(val))
    return digits if digits else ''
```

---

## STEP 2: Salesforce突合

### 突合方式
- **melt方式**: Salesforceの複数電話フィールドを縦持ちに変換
- **マッチキー**: 正規化済み電話番号

### オブジェクト別電話フィールド（優先順）

| オブジェクト | フィールド | 優先順位 |
|-------------|-----------|---------|
| Account | Phone | 1 |
| Account | Phone2__c | 2 |
| Contact | Phone | 1 |
| Contact | Phone2__c | 2 |
| Contact | MobilePhone | 3 |
| Contact | MobilePhone2__c | 4 |
| Lead | Phone | 1 |
| Lead | MobilePhone | 2 |
| Lead | Phone2__c | 3 |
| Lead | MobilePhone2__c | 4 |

### 出力ファイル
- `merged_取引先.csv` - Accountとマッチしたデータ
- `merged_責任者.csv` - Contactとマッチしたデータ
- `merged_リード.csv` - Leadとマッチしたデータ
- `diff_*_not_matched.csv` - 各オブジェクトで未マッチのデータ
- `combined_diff_new_leads.csv` - 3オブジェクトすべて未マッチ（新規候補）

---

## STEP 3: 職種フィルタ

### 対象カラム
- `職業分類１（コード）`
- `職業分類２（コード）`
- `職業分類３（コード）`

### フィルタ条件
いずれかのカラムが指定コードセットに含まれる行のみ抽出

### 医療系コードセット（メイン）
```
021-01〜021-04, 022-01〜022-02, 023-01〜023-99
024-01〜024-09, 026-01〜026-02, 027-99
028-01〜028-99, 029-01〜029-03, 030-01〜030-03
031-01〜031-99, 032-01〜032-03
049-01〜049-99, 050-01〜050-99, 051-01〜051-02
```

### 飲食系コードセット（サブ）
```
025-01〜025-02, 055-06〜055-99
072-04〜072-05, 040-99, 096-01, 099-04
```

### 追加コードセット
```
361-01, 361-03, 144-01〜144-02, 145-01, 037-01, 037-05
```

### 出力ファイル
- `filtered_new_leads.csv` - 職種フィルタ後の新規候補

---

## STEP 4: 成約先フィルタ（重要）

### 目的
**成約先への営業架電事故を防止する**

### 成約先の定義（Salesforceレポート条件）
```sql
Status__c LIKE '%商談中%'
OR Status__c LIKE '%プロジェクト進行中%'
OR Status__c LIKE '%深耕対象%'
OR Status__c LIKE '%過去客%'
OR RelatedAccountFlg__c = 'グループ案件進行中'
OR RelatedAccountFlg__c = 'グループ過去案件実績あり'
```

### 突合キー（両方で突合）

| キー | ハローワーク側カラム | 成約先側カラム |
|-----|-------------------|--------------|
| 電話番号 | 選考担当者ＴＥＬ_加工 | Phone |
| 法人番号 | 法人番号 | CorporateNumber__c, CorporateIdentificationNumber__c |

### 除外ロジック
```python
# 電話番号 OR 法人番号でマッチしたものを除外
phone_match = df['phone_norm'].isin(contract_phones)
corp_match = df['corp_norm'].isin(contract_corps)
is_contract = phone_match | corp_match

# 成約先を除外
true_new_leads = df[~is_contract]
```

### 法人番号正規化
```python
def normalize_corp_num(num):
    if pd.isna(num) or not num:
        return ''
    num = str(num).strip()
    # 数字のみ抽出
    num = re.sub(r'[^0-9]', '', num)
    return num
```

### 出力ファイル
- `true_new_leads.csv` - 真の新規リード候補
- `excluded_by_corp_number.csv` - 法人番号で除外されたデータ
- `excluded_contract_matches.csv` - 電話番号で除外されたデータ

---

## 既存レコード更新時の成約先除外

### 対象
- Account更新データ (`account_full_updates.csv`)
- Contact更新データ (`contact_full_updates.csv`)
- Lead更新データ (`lead_full_updates.csv`)

### 突合キー（merged_*.csvから取得）

| オブジェクト | 電話番号カラム | 法人番号カラム |
|-------------|--------------|--------------|
| Account | Phone | CorporateIdentificationNumber__c, CorporateNumber__c, 法人番号 |
| Contact | Phone | Account_CorporateNumber__c, 法人番号 |
| Lead | Phone | CorporateNumber__c, HJBG_CorporateNumber__c, 法人番号 |

### 出力ファイル
- `*_full_updates_filtered.csv` - 成約先除外後の更新データ
- `*_full_updates_excluded_contract.csv` - 除外されたデータ（確認用）

---

## 更新フィールドマッピング

### Lead/Account共通
| ハローワーク | Salesforce API名 | 備考 |
|-------------|-----------------|------|
| 従業員数企業全体 | NumberOfEmployees | 整数（.0除去） |
| 事業所郵便番号 | PostalCode / BillingPostalCode | |
| 事業所所在地 | Street / BillingStreet | |
| 代表者名 | PresidentName__c | |
| 代表者役職 | PresidentTitle__c | |
| 受付年月日（西暦） | Hellowork_JobPublicationDate__c | 日付形式 |
| 求人有効年月日（西暦） | Hellowork_JobClosedDate__c | 日付形式 |
| 産業分類（名称） | Hellowork_Industry__c | |
| 職種 | Hellowork_RecuritmentType__c | |
| 雇用形態 | Hellowork_EmploymentType__c | |
| 募集理由区分 | Hellowork_RecruitmentReasonCategory__c | |
| 採用人数 | Hellowork_NumberOfRecruitment__c | 整数（.0除去） |
| 従業員数就業場所 | Hellowork_NumberOfEmployee_Office__c | 整数（.0除去） |
| (処理日) | Hellowork_DataImportDate__c | YYYY-MM-DD |
| 事業所ホームページ | Website | |

### メモフィールド（★区切りで追記）
| Addition列 | Salesforce API名 | オブジェクト |
|-----------|-----------------|------------|
| Publish_ImportText_Addition | Publish_ImportText__c | Account, Lead |
| LeadSourceMemo_Addition | LeadSourceMemo__c | Lead |
| Description_Addition | Description | Contact |

### メモ追記ルール
```
既存メモ
★
[YYYY-MM-DD ハロワ突合]
従業員数: 旧値→新値
郵便番号: 空→新値
...
```

### 重複チェック
同じ日付のログが既にあれば追記しない（再実行時の重複防止）

---

## 整数フィールド処理

以下のフィールドは `.0` を除去して整数として送信：
- NumberOfEmployees
- Hellowork_NumberOfRecruitment__c
- Hellowork_NumberOfEmployee_Office__c

```python
INTEGER_FIELDS = [
    'NumberOfEmployees',
    'Hellowork_NumberOfRecruitment__c',
    'Hellowork_NumberOfEmployee_Office__c',
]

# .0 を除去
for field in INTEGER_FIELDS:
    if field in df.columns:
        df[field] = df[field].apply(
            lambda x: str(int(float(x))) if pd.notna(x) and str(x).strip() else ''
        )
```

---

## Lead除外条件

### 取引開始済Lead
`Status = '取引開始済'` のLeadは更新対象から除外（ConvertされたLeadは更新不可）

```python
EXCLUDED_LEAD_STATUSES = ['取引開始済']
matched = matched[~matched['Status'].isin(EXCLUDED_LEAD_STATUSES)]
```

---

## 実行コマンド

### 1. 成約先データ取得
```bash
python scripts/export_contract_report.py
```

### 2. ハローワーク突合パイプライン実行
```bash
python scripts/hellowork_pipeline.py data/input/hellowork.csv \
  --fetch-from-sf \
  --fetch-contract
```

### 3. 更新データ生成
```bash
python scripts/generate_hellowork_updates.py
```

### 4. 成約先除外（更新データ）
```python
# 電話番号 + 法人番号で除外
python -c "
# ... 成約先除外スクリプト（本ドキュメントのSTEP4参照）
"
```

### 5. Salesforce更新実行
```bash
# Dry-run（確認のみ）
python scripts/bulk_full_update.py --object Lead --csv data/output/hellowork/lead_full_updates_filtered.csv

# 実行
python scripts/bulk_full_update.py --object Lead --csv data/output/hellowork/lead_full_updates_filtered.csv --execute
```

---

## API使用量

### Bulk API 2.0 Query使用
バックアップ・既存値取得にBulk API 2.0 Queryを使用し、REST API使用量を99%削減

| 処理 | 従来（REST API） | 現在（Bulk API 2.0） |
|-----|-----------------|---------------------|
| バックアップ取得 | ~337回 | 3-4回 |
| メモ既存値取得 | ~337回 | 3-4回 |
| 一括更新 | 3回 | 3回 |
| **合計** | **~677回** | **~10回** |

---

## 出力ファイル一覧

| ファイル | 内容 |
|---------|------|
| `processed_hellowork.csv` | 電話番号正規化済みハローワークデータ |
| `merged_取引先.csv` | Account突合結果 |
| `merged_責任者.csv` | Contact突合結果 |
| `merged_リード.csv` | Lead突合結果 |
| `combined_diff_new_leads.csv` | 3オブジェクト未マッチ（差分） |
| `filtered_new_leads.csv` | 職種フィルタ後 |
| `true_new_leads.csv` | **真の新規リード候補** |
| `account_full_updates_filtered.csv` | Account更新データ（成約先除外済） |
| `contact_full_updates_filtered.csv` | Contact更新データ（成約先除外済） |
| `lead_full_updates_filtered.csv` | Lead更新データ（成約先除外済） |
| `backup_*.csv` | 更新前バックアップ |

---

## 処理実績（2026-01-07）

| 項目 | 件数 |
|-----|------|
| ハローワーク入力 | 340,622件 |
| Account更新（成約先除外後） | 17,496件 |
| Contact更新（成約先除外後） | 20,314件 |
| Lead更新（成約先除外後） | 65,794件 |
| 真の新規リード候補 | 4,245件 |
| 成約先除外合計 | 5,221件 + 136件 |

---

最終更新: 2026-01-07
