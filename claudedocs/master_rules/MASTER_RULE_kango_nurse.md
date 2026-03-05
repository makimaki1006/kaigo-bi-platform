# 看護媒体リスト更新 マスタールール

> **最終確定日**: 2026-01-13
> **目的**: 看護rooおよびナース専科の媒体データをSalesforceと突合し、既存レコードの更新および新規リード作成を行う

---

## 🔴 絶対ルール（違反厳禁）

### 1. 成約先への架電事故防止
**成約先に営業架電してしまうと事故になる。絶対にやってはダメ。**

- 成約先は **電話番号 OR 住所+名前** で突合して除外
- 新規リード候補から必ず除外すること

### 2. 新規リード必須フィールド
**以下のフィールドが空の場合はレコードをスキップする**

| フィールド | API名 | 必須種別 | 対処 |
|-----------|------|---------|------|
| 会社名 | `Company` | 標準必須 | 空→スキップ |
| 姓 | `LastName` | 標準必須 | 固定値「担当者」を設定 |
| 電話番号 | `Phone` | カスタム必須 | 空→スキップ |

### 3. Phoneフィールド必須
**Salesforceのバリデーションルールにより`Phone`が空だとエラーになる**

携帯電話のみの場合でも`Phone`フィールドに値を入れること。

```python
if is_mobile_phone(phone):
    phone_field = phone      # 携帯番号を Phone にも設定
    mobile_field = phone     # MobilePhone にも設定
else:
    phone_field = phone      # 固定電話を Phone に設定
    mobile_field = ''        # MobilePhone は空
```

---

## ソースデータ

### 看護roo
| 項目 | 値 |
|-----|-----|
| ファイル | `final_kango_with_google_v2.xlsx` |
| 保存場所 | `C:\Users\fuji1\OneDrive\デスクトップ\pythonスクリプト置き場\` |
| 電話番号列 | `phone_cleaned` (優先), `google_phone` (フォールバック) |
| 会社名列 | `shisetsu_name` |
| 住所列 | `font-xs` |

### ナース専科
| 項目 | 値 |
|-----|-----|
| ファイル | `final_fallback_nursejinzaibank_final_structured_v3.xlsx` |
| 保存場所 | `C:\Users\fuji1\OneDrive\デスクトップ\pythonスクリプト置き場\` |
| 電話番号列 | `phone_cleaned` |
| 会社名列 | `名称` |
| 住所列 | `所在地` |

---

## 全体処理フロー

```
看護roo / ナース専科 データ
        │
        ▼
[STEP 1] データ読み込み・電話番号正規化
        │  - 電話番号なし → スキップ
        │  - 会社名なし → スキップ
        │
        ▼
[STEP 2] Salesforce突合
        │
        ├─→ Account電話マッチ ──────────→ Account更新候補
        │
        ├─→ Lead電話マッチ ────────────→ Lead更新候補
        │
        ├─→ Account住所+名前マッチ ───→ Account更新候補（電話未マッチ分）
        │
        └─→ 未マッチ
                │
                ▼
        [STEP 3] 成約先除外（電話 OR 住所+名前）
                │
                ▼
        ★ 新規リード候補
                │
                ▼
        [STEP 4] 必須フィールドバリデーション
                │  - Company空 → スキップ
                │  - Phone空 → スキップ
                │
                ▼
        [STEP 5] Owner割当て
                │
                ▼
        ★ 最終CSV出力
```

---

## STEP 1: 電話番号正規化

### ルール
```python
def normalize_phone(phone):
    """電話番号を正規化（10-11桁、先頭0付き）"""
    if pd.isna(phone) or phone == '' or str(phone) == 'nan':
        return None
    phone_str = str(phone).strip()
    if phone_str.endswith('.0'):
        phone_str = phone_str[:-2]
    digits = re.sub(r'\D', '', phone_str)
    # 10桁で0始まりでない場合は先頭に0を補完
    if len(digits) == 10 and not digits.startswith('0'):
        digits = '0' + digits
    if len(digits) >= 10 and len(digits) <= 11:
        return digits
    return None
```

### 携帯電話判定
```python
def is_mobile_phone(phone):
    """携帯電話番号かどうかを判定"""
    if not phone:
        return False
    return phone.startswith(('090', '080', '070'))
```

---

## STEP 2: Salesforce突合

### 突合方式

#### 2-1. 電話番号マッチング（優先）

| オブジェクト | 電話フィールド |
|-------------|--------------|
| Account | Phone, PersonMobilePhone, Phone2__c |
| Lead | Phone, MobilePhone, Phone2__c |

```python
# 電話番号インデックス作成
phone_to_acc = {}
for _, row in df_acc.iterrows():
    for col in ['Phone', 'PersonMobilePhone', 'Phone2__c']:
        normalized = normalize_phone(row[col])
        if normalized:
            phone_to_acc[normalized] = row
            break  # 最初にマッチした電話番号のみ使用
```

#### 2-2. 住所+名前マッチング（補助）

電話番号マッチしない場合に適用。

**住所抽出ロジック**
```python
PREFECTURES = [
    '北海道', '青森県', '岩手県', '宮城県', '秋田県', '山形県', '福島県',
    '茨城県', '栃木県', '群馬県', '埼玉県', '千葉県', '東京都', '神奈川県',
    '新潟県', '富山県', '石川県', '福井県', '山梨県', '長野県',
    '岐阜県', '静岡県', '愛知県', '三重県',
    '滋賀県', '京都府', '大阪府', '兵庫県', '奈良県', '和歌山県',
    '鳥取県', '島根県', '岡山県', '広島県', '山口県',
    '徳島県', '香川県', '愛媛県', '高知県',
    '福岡県', '佐賀県', '長崎県', '熊本県', '大分県', '宮崎県', '鹿児島県', '沖縄県'
]

def extract_city(address):
    """住所から都道府県+市区町村を抽出"""
    # 都道府県を特定
    pref = None
    for p in PREFECTURES:
        if address.startswith(p):
            pref = p
            address = address[len(p):]
            break
    if not pref:
        return None, None

    # 市区町村を抽出（〇〇市、〇〇区、〇〇郡〇〇町/村）
    city_pattern = re.compile(r'^(.+?[市区])|(.*?郡.+?[町村])')
    match = city_pattern.match(address)
    if match:
        return pref, match.group(0)
    return pref, None
```

**名前類似度判定**
```python
def is_similar_name(name1, name2, threshold=0.85):
    """会社名の類似度を判定（Jaccard係数）"""
    # 法人格を除去して正規化
    n1 = normalize_company_name(name1)
    n2 = normalize_company_name(name2)

    # 完全一致
    if n1 == n2:
        return True

    # 部分文字列一致（5文字以上）
    shorter = n1 if len(n1) <= len(n2) else n2
    longer = n2 if len(n1) <= len(n2) else n1
    if len(shorter) >= 5 and shorter in longer:
        return True

    # 短い名前は完全一致のみ
    if len(n1) <= 5 or len(n2) <= 5:
        return n1 == n2

    # Jaccard類似度
    set1, set2 = set(n1), set(n2)
    intersection = len(set1 & set2)
    union = len(set1 | set2)
    similarity = intersection / union if union > 0 else 0
    return similarity >= threshold
```

**法人格除去リスト**
```python
LEGAL_PREFIXES = [
    '医療法人社団', '医療法人財団', '医療法人', '社会福祉法人',
    '株式会社', '有限会社', '合同会社', '一般社団法人', '公益社団法人'
]
```

---

## STEP 3: 成約先除外

### 成約先の定義
RecordType = '成約先（様付け）' のAccount

```python
# 成約先インデックス（電話番号）
contract_phones = set()
for _, row in df_contract.iterrows():
    phone = normalize_phone(row.get('Phone', ''))
    if phone:
        contract_phones.add(phone)

# 成約先インデックス（住所+名前）
contract_location_index = {}
for _, row in df_contract.iterrows():
    pref, city = extract_city(row.get('Address__c', ''))
    if pref and city:
        key = (pref, city)
        normalized_name = normalize_company_name(row.get('Name', ''))
        if key not in contract_location_index:
            contract_location_index[key] = []
        contract_location_index[key].append(normalized_name)
```

### 除外判定
```python
# 電話番号マッチ OR 住所+名前マッチで除外
is_contract_phone = media_phone in contract_phones

is_contract_location = False
if (pref, city) in contract_location_index:
    for contract_name in contract_location_index[(pref, city)]:
        if is_similar_name(media_company, contract_name):
            is_contract_location = True
            break

if is_contract_phone or is_contract_location:
    # 除外
    excluded_records.append(row)
    continue
```

---

## STEP 4: 必須フィールドバリデーション

### CSV生成時のチェック
```python
# Company必須チェック
company_name = row.get('company_name', '')
if not company_name or not is_valid_value(company_name):
    skipped_no_company += 1
    continue

# Phone必須チェック
phone = row.get('phone_normalized', '')
if not phone:
    skipped_no_phone += 1
    print(f"  警告: Phone空のためスキップ: {company_name}")
    continue
```

### スキップサマリー出力
```python
if skipped_no_company > 0:
    print(f"  ※Company空でスキップ: {skipped_no_company}件")
if skipped_no_phone > 0:
    print(f"  ※Phone空でスキップ: {skipped_no_phone}件")
```

---

## STEP 5: Owner割当て

### 割当てロジック
ユーザー指定の件数配分に従ってOwnerIdを設定。

```python
# 例: 2026-01-13の割当て
owner_distribution = {
    '篠木': 350,
    '小林': 233,
    '清飛羅': 233,
    '灰野': 232
}

# OwnerId取得（User.Nameから検索）
def get_owner_ids(client, names):
    soql = f"SELECT Id, Name FROM User WHERE Name IN {tuple(names)}"
    result = client.query(soql)
    return {r['Name']: r['Id'] for r in result['records']}
```

---

## 更新フィールドマッピング

### 新規リード作成

| 媒体データ | Salesforce API名 | 備考 |
|-----------|-----------------|------|
| company_name | Company | 必須 |
| (固定値) | LastName | '担当者' |
| phone | Phone | 必須（携帯でも設定） |
| phone (携帯の場合) | MobilePhone | 携帯の場合のみ |
| prefecture | Prefecture__c | 都道府県 |
| street | Street | 住所（都道府県除く） |
| memo | Paid_Memo__c | メモ（バッチID含む）※Descriptionは使用不可 |
| source | LeadSource | '看護roo' or 'ナース専科' |
| source | Paid_Media__c | 有料媒体名 |
| (処理日) | Paid_DataExportDate__c | 処理日 |
| source | Paid_DataSource__c | データソース |

### Lead/Account更新

| フィールド | API名 | 備考 |
|-----------|------|------|
| メモ追記 | Paid_Memo__c | バッチID付きで追記 |

### 🔴 注意: LeadにはDescriptionフィールドがない
Lead作成時は `Paid_Memo__c`（有料求人メモ）を使用すること。

### メモフィールド形式

**バッチID形式**
```
【BATCH_YYYYMMDD_KANGO】
```

**新規リード**
```
【BATCH_20260113_KANGO】
【看護roo】
住所: 〇〇県〇〇市...
施設形態: 病院
設立: 2010年
診療科目: 内科、外科
更新日: 2026-01-10
募集: 正看護師
取得日: 2026-01-13
```

**既存更新**
```
(既存メモ)
★
【BATCH_20260113_KANGO】[看護roo]
取得日: 2026-01-13
```

---

## 実行手順

### 1. スクリプト実行
```bash
python scripts/generate_kango_nurse_csv.py
```

### 2. 出力ファイル確認
- 新規リード: `kango_nurse_new_leads_*.csv`
- Lead更新: `kango_nurse_lead_updates_*.csv`
- Account更新: `kango_nurse_account_updates_*.csv`

### 3. Salesforceインポート

#### 新規リード作成
```python
from src.api.salesforce_client import SalesforceClient
client = SalesforceClient()
client.authenticate()
client.bulk_insert('Lead', 'data/output/media_matching/kango_nurse_new_leads_*.csv')
```

#### Lead更新
```python
client.bulk_update('Lead', 'data/output/media_matching/kango_nurse_lead_updates_*.csv')
```

#### Account更新
```python
client.bulk_update('Account', 'data/output/media_matching/kango_nurse_account_updates_*.csv')
```

### 4. レポート作成
インポート後、Salesforceでレポートを作成:
- フィルタ条件: `Paid_Memo__c CONTAINS '【BATCH_YYYYMMDD_KANGO】'`

---

## 出力ファイル一覧

### 最終成果物

| ファイル | 内容 | 用途 |
|---------|------|------|
| kango_nurse_new_leads_*.csv | 新規リード作成データ | Bulk Insert |
| kango_nurse_lead_updates_*.csv | Lead更新データ | Bulk Update |
| kango_nurse_account_updates_*.csv | Account更新データ | Bulk Update |
| created_lead_ids_*.csv | 作成済みリードID一覧 | レポート用 |

### 確認用

| ファイル | 内容 |
|---------|------|
| kango_nurse_excluded_*.csv | 成約先除外データ |
| location_match_sample.csv | 住所+名前マッチサンプル |

---

## エラー対処

| エラー | 原因 | 対処 |
|-------|------|------|
| CANNOT_UPDATE_CONVERTED_LEAD | コンバート済みLeadは更新不可 | 想定内、許容 |
| STRING_TOO_LONG | フィールド長超過 | データ切り詰め |
| Field name not found: Description | LeadにDescriptionがない | `Paid_Memo__c`を使用 |
| Phone空エラー | バリデーションルール | 携帯でもPhoneに設定 |

---

## チェックリスト

### 実行前確認
- [ ] 媒体ソースファイルが最新か
- [ ] Salesforce認証が有効か
- [ ] 成約先データが最新か

### バリデーション確認
- [ ] Company空のレコードがないか
- [ ] Phone空のレコードがないか
- [ ] 成約先が新規リードに含まれていないか

### 実行後確認
- [ ] 作成件数が想定通りか
- [ ] エラー件数が許容範囲か
- [ ] レポートが作成できるか（バッチIDで検索）

---

## 処理実績

### 2026-01-13

| 項目 | 件数 |
|-----|------|
| 看護roo入力 | 約3,900件 |
| ナース専科入力 | 約2,700件 |
| 新規リード作成 | 1,048件 |
| Lead更新 | 6,472件成功 / 198件失敗 |
| Account更新 | 1,029件 |
| 成約先除外 | 約160件（住所+名前マッチ含む） |

**Owner別内訳（新規リード）**

| Owner | 件数 |
|-------|------|
| 篠木 | 350件 |
| 小林 | 233件 |
| 清飛羅 | 233件 |
| 灰野 | 232件 |

**レポート**
- 新規リード: https://fora-career6.my.salesforce.com/lightning/r/Report/00Odc00000HlqroEAB/view
- Lead更新: https://fora-career6.my.salesforce.com/lightning/r/Report/00Odc00000HlqsfEAB/view
- Account更新: https://fora-career6.my.salesforce.com/lightning/r/Report/00Odc00000HlqtJEAR/view

---

## 関連スクリプト

| スクリプト | 用途 |
|-----------|------|
| `scripts/generate_kango_nurse_csv.py` | メイン処理スクリプト |
| `scripts/extract_location_matches.py` | 住所+名前マッチサンプル抽出 |

---

**このマスタールールに従って処理を行うこと。**

最終更新: 2026-01-13
