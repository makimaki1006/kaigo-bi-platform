# 看護媒体インポート作業レポート（2026-01-13）

## 概要

看護rooおよびナース専科の媒体データをSalesforceにインポートした。

## 処理対象データ

| 媒体 | ソースファイル |
|------|---------------|
| 看護roo | `final_kango_with_google_v2.xlsx` |
| ナース専科 | `final_fallback_nursejinzaibank_final_structured_v3.xlsx` |

## 突合ロジック

### 1. 電話番号マッチング（優先）
- 媒体の電話番号とSalesforce Account/Lead/Contactの電話番号を突合
- 正規化処理: 数字のみ抽出、先頭0補完、10-11桁検証

### 2. 住所+名前マッチング（補助）
- 電話番号マッチしない場合に適用
- 都道府県+市区町村が一致 AND 会社名が類似（Jaccard類似度≥0.85）
- 成約先除外にも使用

### 3. 成約先除外
- RecordType = '成約先（様付け）' のAccountを除外
- 電話番号マッチ OR 住所+名前マッチで判定

## 処理結果

| 処理種別 | 件数 | 状態 |
|---------|------|------|
| 新規リード作成 | 1,048件 | 完了 |
| Lead更新 | 6,472件成功 / 198件失敗* | 完了 |
| Account更新 | 1,029件 | 完了 |

*失敗198件は `CANNOT_UPDATE_CONVERTED_LEAD`（既にコンバート済みのLead）

## Owner割当て

| 担当者 | 件数 |
|-------|------|
| 篠木 | 350件 |
| 小林 | 233件 |
| 清飛羅 | 233件 |
| 灰野 | 232件 |

## 作成したレポート

| レポート名 | URL | 用途 |
|-----------|-----|------|
| 看護媒体インポート_20260113 | [リンク](https://fora-career6.my.salesforce.com/lightning/r/Report/00Odc00000HlqroEAB/view) | 新規リード |
| 看護媒体_Lead更新_20260113 | [リンク](https://fora-career6.my.salesforce.com/lightning/r/Report/00Odc00000HlqsfEAB/view) | Lead更新 |
| 看護媒体_Account更新_20260113 | [リンク](https://fora-career6.my.salesforce.com/lightning/r/Report/00Odc00000HlqtJEAR/view) | Account更新 |

## バッチID

レポート検索用: `【BATCH_20260113_KANGO】`

Paid_Memo__cフィールドに付与済み

## 発生した問題と対処

### 1. Descriptionフィールドエラー
- **問題**: LeadオブジェクトにDescription標準フィールドが存在しない
- **対処**: `Paid_Memo__c`（カスタムフィールド）に変更

### 2. Phone空エラー
- **問題**: Salesforceバリデーションルールにより`Phone`が空だとエラー
- **対処**: 携帯電話のみの場合でも`Phone`フィールドに値を設定
- **追加対処**: 既存6件のPhone空レコードをSalesforce上で修正

### 3. CANNOT_UPDATE_CONVERTED_LEAD
- **問題**: 既にコンバート済みのLeadは更新不可
- **対処**: 想定内エラーとして許容（198件）

## スクリプト修正

### generate_kango_nurse_csv.py

1. **住所+名前マッチングによるAccount検出追加**
   - `load_account_location_index()` 関数追加
   - 電話番号マッチしないがAccount住所+名前マッチする場合をAccount更新に分類

2. **必須フィールドバリデーション追加**
   - Company空チェック → スキップ
   - Phone空チェック → スキップ
   - 携帯のみの場合でもPhoneに値を設定

3. **フィールド名修正**
   - `Description` → `Paid_Memo__c`

## ルール追加

CLAUDE.mdに「新規リード作成時の必須フィールド」ルールを追加:

| フィールド | API名 | 必須種別 | 対処 |
|-----------|------|---------|------|
| 会社名 | `Company` | 標準必須 | 空→スキップ |
| 姓 | `LastName` | 標準必須 | 固定値「担当者」 |
| 電話番号 | `Phone` | カスタム必須 | 空→スキップ |

## 成果物ファイル

| ファイル | 用途 |
|---------|------|
| `data/output/media_matching/kango_nurse_new_leads_final_20260113_105609.csv` | 新規リード作成用CSV |
| `data/output/media_matching/kango_nurse_lead_updates_final_20260113_105609.csv` | Lead更新用CSV |
| `data/output/media_matching/kango_nurse_account_updates_final_20260113_105609.csv` | Account更新用CSV |
| `data/output/media_matching/created_lead_ids_20260113.csv` | 作成済みリードID一覧 |

## 今後の改善候補

1. 住所+名前マッチングの精度向上（町名レベルまで検証）
2. バッチIDの自動採番
3. コンバート済みLead除外の事前チェック
