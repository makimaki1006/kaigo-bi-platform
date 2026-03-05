# PT・OT・STネット マスタールール

> **最終更新日**: 2026-01-09
> **親ルール**: [MASTER_RULE_paid_media.md](./MASTER_RULE_paid_media.md)（必ず参照）

PT・OT・STネットからのスクレイピングデータをSalesforceに突合・更新するためのルール定義。

---

## 重要: 総合ルール参照

**本ドキュメントはPT・OT・STネット固有のルールのみを記載。以下の共通ルールは総合版を参照すること。**

| 項目 | 参照先 |
|-----|-------|
| 絶対ルール（成約先除外等） | MASTER_RULE_paid_media.md §絶対ルール |
| 所有者割り当てルール | MASTER_RULE_paid_media.md §所有者割り当てルール |
| 担当者名更新ルール | MASTER_RULE_paid_media.md §担当者名更新ルール |
| メモ欄差別化 | MASTER_RULE_paid_media.md §メモ欄差別化 |
| レポート作成 | MASTER_RULE_paid_media.md §レポート作成 |
| チェックリスト | MASTER_RULE_paid_media.md §チェックリスト |

---

## PT・OT・STネット固有の特徴

### データソースの特殊性

| 項目 | 特徴 | 他媒体との違い |
|-----|------|--------------|
| ファイル形式 | Excel (.xlsx) | ミイダスはCSV |
| 電話番号の位置 | 専用列（電話番号_1〜10）複数列 | ミイダスは連絡先フィールド内抽出 |
| 職種 | 募集職種列（専用） | ミイダスは全本文ダンプから抽出 |
| 業界分類 | リハビリ分類（急性期/回復期等） | ミイダスは企業規模（人数） |
| 閲覧数情報 | あり | 他媒体はなし |
| 掲載日情報 | あり | 他媒体はなし |
| 代表者情報 | **なし** | ミイダスのみあり |

### 対象職種

PT・OT・STネットはリハビリテーション専門媒体のため、以下の職種が中心：

| 職種 | 説明 |
|-----|------|
| 理学療法士（PT） | Physical Therapist |
| 作業療法士（OT） | Occupational Therapist |
| 言語聴覚士（ST） | Speech Therapist |

---

## データソース仕様

### ファイル形式
- **ファイル名**: `PT・OT・STネット_スクレイピングデータ.xlsx`
- **形式**: Excel (.xlsx)
- **エンコーディング**: UTF-8

### 列構成（2026年1月時点）

| 列名 | 説明 | 必須 | 備考 |
|-----|------|------|------|
| 事業所名 | 会社名 | Yes | |
| 募集職種 | 職種（PT/OT/ST等） | Yes | 専用列 |
| 所在地・勤務地 | 住所（都道府県から） | Yes | |
| 雇用形態 | 常勤, 非常勤（パート）等 | No | Paid_EmploymentType__cにマッピング |
| リハビリ分類 | 急性期, 回復期, 維持期・生活期等 | No | Paid_Industry__cにマッピング |
| 採用人数 | 募集人数 | No | Paid_NumberOfRecruitment__cにマッピング |
| 担当者 | 担当者名（改行でカナが続く場合あり） | No | 1行目のみ使用 |
| 連絡先 | 電話番号や住所の詳細 | No | |
| 電話番号_1〜10 | 複数の電話番号列 | Yes（1つ以上） | 最大10列 |
| URL | 求人ページURL | Yes | |
| 掲載日 | 掲載開始日 | No | メモ欄に記載 |
| 求人情報閲覧数 | 閲覧数 | No | メモ欄に記載 |

### 電話番号の特徴

- **複数列**: 電話番号_1 から 電話番号_10 まで最大10列
- 固定電話と携帯電話が混在
- 正規化後10〜11桁で判定
- **最初に見つかった有効な番号を使用**

### 担当者名の特徴

```
杉浦
スギウラ
```

- 改行で名前とカナが分離されている場合あり
- **最初の行のみを使用すること**

---

## 電話番号処理ロジック

### 複数列からの電話番号収集

```python
def collect_phone_numbers(row: pd.Series) -> list:
    """電話番号_1〜電話番号_10から電話番号を収集"""
    phones = []
    for i in range(1, 11):
        col_name = f'電話番号_{i}'
        if col_name in row and pd.notna(row[col_name]):
            phone = normalize_phone(str(row[col_name]))
            if len(phone) in (10, 11):
                phones.append(phone)
    return phones
```

### 固定電話・携帯電話の分類

```python
def classify_phone(phone: str) -> str:
    """電話番号を固定電話/携帯電話に分類"""
    normalized = re.sub(r'[^0-9]', '', phone)
    if normalized.startswith(('070', '080', '090')):
        return 'mobile'
    return 'landline'
```

---

## リハビリ分類（Paid_Industry__c）

PT・OT・STネット固有のフィールド。リハビリテーションの段階分類：

| 分類 | 説明 |
|-----|------|
| 急性期 | 発症直後〜2週間程度、集中治療 |
| 回復期 | 急性期後〜6ヶ月程度、機能回復訓練 |
| 維持期・生活期 | 慢性期、在宅復帰後のリハビリ |
| 終末期 | ターミナルケア |

---

## Salesforceフィールドマッピング

### 新規リード作成時

| スクレイピング項目 | Salesforce API名 | 変換ロジック |
|------------------|------------------|-------------|
| 事業所名 | Company | そのまま |
| 担当者（1行目） | LastName | 改行前のみ使用、空なら「担当者」 |
| 電話番号（固定） | Phone | 070/080/090以外で最初の1件 |
| 電話番号（携帯） | MobilePhone | 070/080/090始まりで最初の1件 |
| 所在地・勤務地（都道府県） | Prefecture__c | 都道府県抽出 |
| 所在地・勤務地（市区町村以下） | Street | 都道府県除去後 |
| - | LeadSource | "Other" 固定 |
| - | Paid_Media__c | "PT・OT・STネット" 固定 |
| - | Paid_DataSource__c | "PT・OT・STネット" 固定 |
| 募集職種 | Paid_JobTitle__c | そのまま |
| 募集職種 | Paid_RecruitmentType__c | そのまま |
| **雇用形態** | Paid_EmploymentType__c | そのまま（**PT・OT・STネット固有**） |
| **リハビリ分類** | Paid_Industry__c | そのまま（**PT・OT・STネット固有**） |
| **採用人数** | Paid_NumberOfRecruitment__c | 数値抽出（**PT・OT・STネット固有**） |
| 掲載日 + 閲覧数 + 連絡先 | Paid_Memo__c | 結合してメモ |
| URL | Paid_URL__c | そのまま |
| - | Paid_DataExportDate__c | 処理日 |

### 既存レコード更新時

| フィールド | 更新条件 |
|-----------|---------|
| LastName | 担当者名更新ルール参照（MASTER_RULE_paid_media.md） |
| Paid_* フィールド | 既存値が空欄の場合のみ補完 |
| Paid_DataExportDate__c | 常に更新（処理日） |
| LeadSourceMemo__c | メモ欄差別化（MASTER_RULE_paid_media.md参照） |

---

## セグメント分析

### PT・OT・STネットの制限

**PT・OT・STネットには「代表者」フィールドがないため、ミイダスのような「代表者直通」セグメント分析は不可。**

代替として以下の品質指標を使用：

| 指標 | 判定方法 | 優先度 |
|-----|---------|--------|
| 携帯電話あり | MobilePhoneに値あり | ★★★ |
| バイネームあり | LastNameが一般名称でない | ★★ |
| 閲覧数多い | 求人情報閲覧数が上位 | ★ |

### 品質スコア計算

```python
def calculate_quality_score(row: pd.Series) -> int:
    score = 0
    if row.get('MobilePhone'):
        score += 3  # 携帯電話あり
    if row.get('LastName') and not is_generic_name(row['LastName']):
        score += 2  # バイネームあり
    if row.get('求人情報閲覧数', 0) >= 100:
        score += 1  # 閲覧数多い
    return score
```

---

## 処理フロー

```
1. データ読み込み
   ├─ スクレイピングデータ（Excel）
   ├─ 成約先電話番号（CSV）
   ├─ 電話済み電話番号（Excel全シート）
   └─ Salesforce既存データ（Lead/Account/Contact）

2. 電話番号収集
   └─ 電話番号_1〜10から有効な電話番号を収集

3. 担当者名処理
   └─ 改行がある場合は1行目のみを抽出

4. 除外処理
   ├─ 成約先電話番号一致 → 除外（理由: 成約先電話番号）
   └─ 電話済み電話番号一致 → 除外（理由: 電話済み）

5. 突合処理
   ├─ 電話番号で既存レコード検索
   ├─ マッチあり → 更新対象
   └─ マッチなし → 新規リード候補

6. 品質スコア計算
   ├─ 携帯電話有無
   ├─ バイネーム有無
   └─ 閲覧数

7. CSV生成
   ├─ new_leads_final_*.csv（新規リード作成用）
   ├─ lead_updates_final_*.csv（Lead更新用）
   ├─ account_updates_final_*.csv（Account更新用）
   └─ excluded_final_*.csv（除外リスト）

8. 所有者割り当て
   └─ MASTER_RULE_paid_media.md §所有者割り当てルール参照

9. Salesforceインポート（ユーザー確認後）

10. メモ欄差別化
    └─ MASTER_RULE_paid_media.md §メモ欄差別化参照
```

---

## 実行手順

### 前提条件
1. スクレイピングデータ（Excel）を作業ディレクトリに配置
   - ファイル名: `PT・OT・STネット_スクレイピングデータ.xlsx`
2. 最新の成約先データをエクスポート済み
3. 電話済みリストを `C:\Users\fuji1\Downloads\媒体掲載中のリスト.xlsx` に配置

### コマンド

```bash
# 作業ディレクトリに移動
cd "C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List"

# CSVジェネレータ実行（PT・OT・STネット + ジョブポスター共通）
python scripts/generate_media_csv_final.py
```

### 出力確認
```
data/output/media_matching/
├── new_leads_final_YYYYMMDD_HHMMSS.csv      # 新規リード
├── lead_updates_final_YYYYMMDD_HHMMSS.csv   # Lead更新
├── account_updates_final_YYYYMMDD_HHMMSS.csv # Account更新
└── excluded_final_YYYYMMDD_HHMMSS.csv       # 除外リスト
```

---

## PT・OT・STネット固有のチェックリスト

### 処理前（PT・OT・STネット固有）
- [ ] ファイル名が `PT・OT・STネット_スクレイピングデータ.xlsx` か
- [ ] Excel形式か（CSV形式ではない）
- [ ] 「電話番号_1」〜「電話番号_10」列が存在するか
- [ ] 「募集職種」列が存在するか
- [ ] 「リハビリ分類」列が存在するか

### 処理後（PT・OT・STネット固有）
- [ ] 電話番号_1〜10から有効な電話番号が収集できているか
- [ ] 募集職種がPaid_JobTitle__cに正しくマッピングされているか
- [ ] リハビリ分類がPaid_Industry__cに正しくマッピングされているか
- [ ] 雇用形態がPaid_EmploymentType__cに正しくマッピングされているか
- [ ] 採用人数がPaid_NumberOfRecruitment__cに正しくマッピングされているか

### インポート前（共通チェック）
→ MASTER_RULE_paid_media.md §チェックリスト参照

---

## 他媒体との違い（まとめ）

| 項目 | PT・OT・STネット | ミイダス | ジョブポスター |
|-----|-----------------|---------|--------------|
| ファイル形式 | **Excel** | CSV | Excel |
| 電話番号の位置 | **専用列（複数）** | 連絡先フィールド内 | 専用列（単一） |
| 職種の位置 | **募集職種列** | 全本文ダンプ1行目 | 職 種列 |
| 代表者情報 | **なし** | あり | なし |
| 業界分類 | **リハビリ分類** | 企業規模 | なし |
| 雇用形態 | **あり** | なし | なし |
| 採用人数 | **あり** | 抽出 | なし |
| 閲覧数 | **あり** | なし | なし |
| ウェブサイト | なし | 企業サイトURL | ホームページ |
| セグメント分析 | **不可** | 可能 | 不可 |

---

## 関連ファイル

| ファイル | 説明 |
|---------|------|
| `claudedocs/MASTER_RULE_paid_media.md` | **総合ルール（必ず参照）** |
| `scripts/generate_media_csv_final.py` | メイン処理スクリプト（ジョブポスターと共通） |
| `data/output/contract_accounts_*.csv` | 成約先電話番号リスト |
| `data/output/media_matching/*.csv` | 出力ファイル |

---

## 更新履歴

| 日付 | 内容 |
|-----|------|
| 2026-01-08 | 初版作成 |
| 2026-01-09 | MASTER_RULE_paid_media.md へのクロスリファレンス追加 |
| 2026-01-09 | セグメント分析（品質スコア）セクション追加 |
| 2026-01-09 | リハビリ分類の詳細説明追加 |
| 2026-01-09 | 電話番号処理ロジックの詳細追加 |

---

**このルールに加えて、必ずMASTER_RULE_paid_media.mdの共通ルールを参照すること。**
