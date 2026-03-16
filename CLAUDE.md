# Salesforce List Manager - プロジェクト設定

このファイルはClaude Codeがこのプロジェクトを理解するための設定ファイルです。

## プロジェクト概要

- **名前**: Salesforce List Manager
- **目的**: SalesforceのAPIデータとスクレイピングデータを突合し、Salesforce側にAPIで返す
- **言語**: Python 3.10+
- **主要機能**: API連携、リスト管理、自動化

## ディレクトリ構造

```
Salesforce_List/
├── .claude/                # Claude Code設定
│   ├── commands/          # カスタムスラッシュコマンド
│   ├── hooks/             # フックスクリプト
│   └── settings.local.json # ローカル設定
├── src/                    # メインソースコード
│   ├── api/               # Salesforce APIクライアント
│   ├── scrapers/          # Webスクレイピング
│   ├── services/          # ビジネスロジック
│   ├── models/            # データモデル
│   └── utils/             # ユーティリティ
├── data/                   # データファイル
├── scripts/               # 実行スクリプト
├── tests/                 # テスト
├── config/                # 設定ファイル
├── logs/                  # ログ
├── bin/                   # ユーティリティスクリプト
└── claudedocs/            # Claude生成ドキュメント（分析結果、レポート等）
```

## コーディング規約

### Python
- PEP 8に準拠
- 型ヒントを使用（Python 3.10+ スタイル）
- docstringはGoogle形式
- インポート順序: 標準ライブラリ → サードパーティ → ローカル

### 命名規則
- ファイル名: snake_case（例: salesforce_client.py）
- クラス名: PascalCase（例: SalesforceClient）
- 関数/変数: snake_case（例: get_access_token）
- 定数: UPPER_SNAKE_CASE（例: API_VERSION）

## 重要なファイル

| ファイル | 説明 |
|---------|------|
| `config/.env` | Salesforce認証情報（機密） |
| `config/settings.yaml` | アプリケーション設定 |
| `src/api/salesforce_client.py` | Salesforce APIクライアント |
| `scripts/export_salesforce.py` | データ抽出スクリプト |

## よく使うコマンド

```bash
# 依存関係インストール
pip install -r requirements.txt

# Salesforceデータ抽出
python scripts/export_salesforce.py

# テスト実行
pytest tests/

# コードフォーマット
black src/ tests/
```

## 環境変数

| 変数名 | 説明 |
|-------|------|
| SF_CLIENT_ID | Salesforce OAuth クライアントID |
| SF_CLIENT_SECRET | Salesforce OAuth クライアントシークレット |
| SF_REFRESH_TOKEN | Salesforce リフレッシュトークン |
| SF_INSTANCE_URL | Salesforce インスタンスURL |

## セキュリティ注意事項

- `.env`ファイルはGitにコミットしない
- 認証情報はログに出力しない
- APIレスポンスの機密データはマスキングする

---

## 🔴 必須参照ドキュメント

### Salesforce × Zoom 商談分析統合プロジェクト（長期）

**商談分析・成約率向上を目的とした統合プロジェクト。**

📋 **`claudedocs/PROJECT_SalesforceZoom_Integration.md`**

このファイルには以下が定義されている：
- プロジェクト全体像とフェーズ構成
- データ資産（SF Opportunity/Account/Lead、Zoom分析626件）
- Phase 1: データ基盤強化
- Phase 2: 分析・可視化
- Phase 3: 自動化・運用
- 優先度マトリクス

関連スクリプト：
| スクリプト | 用途 |
|-----------|------|
| `src/services/opportunity_service.py` | Opportunity CRUD (Bulk API 2.0) |
| `scripts/match_zoom_opportunity.py` | Zoom-SFマッチングロジック |
| `scripts/zoom_sf_sync_pipeline.py` | 統合パイプライン |

---

### FS引継ぎメモ品質改善プロジェクト

**FS（フィールドセールス）から役務（カスタマーサクセス）への引継ぎメモ品質改善。**

📋 **`claudedocs/fs_handover/README.md`**

このフォルダには以下が含まれる：
- プロジェクト経緯・全体像
- 6項目の定義・意図・記入基準
- Gemini Gem用AIプロンプト（商談文字起こし→引継ぎメモ自動生成）
- 品質改善計画

**主要成果物**:
| ファイル | 用途 |
|---------|------|
| `03_gem_prompt/GEM_PROMPT_FINAL.md` | Gem作成時にInstructions欄に貼り付け |
| `01_analysis/FIELD_DEFINITIONS.md` | FS教育・品質基準の参照 |

**対象6項目**: 募集職種、採用課題、導入ポイント、刺さったポイント、先方関係者情報、担当者注意点

---

### アタックリスト セグメント管理

**営業架電用アタックリストのセグメント定義・レポート・KPI追跡。**

📋 **`claudedocs/attack_list/ATTACK_LIST_SEGMENTS.md`**

このファイルには以下が定義されている：
- セグメント定義（優先度S/A/B/C × 従業員規模）
- 架電可能条件（MECE）
- 作成済みレポートURL一覧（16件）
- セグメント別推定KPI
- 月次KPI追跡フレームワーク

**架電可能条件（完全版）:**
- Status__c に「商談中」「プロジェクト進行中」「深耕対象」「過去客」を含まない
- RelatedAccountFlg__c が「グループ案件進行中」「グループ過去案件実績あり」でない
- ApproachNG__c = false（アプローチ禁止でない）
- CallNotApplicable__c = false（架電対象外でない）
- Phone != null（電話番号あり）

関連スクリプト：
| スクリプト | 用途 |
|-----------|------|
| `scripts/create_attack_list_report_callable.py` | 優先度別レポート作成（架電可能のみ） |
| `scripts/create_attack_list_report_with_employees.py` | 優先度×従業員規模レポート作成 |
| `scripts/export_attack_list_accounts.py` | セグメント別Account抽出（CSV） |
| `scripts/track_attack_list_kpi.py` | 月次KPI追跡レポート |

---

### ハローワークリスト定期更新

**ハローワークデータに関するリスト更新作業を行う場合は、必ず以下のマスタールールを参照すること。**

📋 **`claudedocs/MASTER_RULE_hellowork.md`**

このファイルには以下が定義されている：
- 絶対ルール（成約先除外、取引開始済Lead除外）
- 全体処理フロー
- 突合ロジック・職種フィルタ・成約先除外の詳細
- 更新フィールドマッピング
- 実行手順・コマンド例
- チェックリスト

**⚠️ 成約先に営業架電してしまうと事故になる。絶対にやってはダメ。**

関連スクリプト：
| スクリプト | 用途 |
|-----------|------|
| `scripts/hellowork_pipeline.py` | ハローワーク突合パイプライン |
| `scripts/generate_hellowork_updates.py` | 更新データ生成 |
| `scripts/exclude_contract_accounts.py` | 成約先除外 |
| `scripts/bulk_full_update.py` | Salesforce一括更新 |

---

### 市区町村人口データ付与

**Lead/Accountに人口データを付与してセグメント分析を行う場合は、以下を参照すること。**

📋 **`claudedocs/MASTER_RULE_population.md`**

このファイルには以下が定義されている：
- データソース（内閣府 2010年国勢調査）
- 住所フィールドマッピング（Address__c優先）
- マッチ率（Lead 93.3%、Account 70.9%）
- 人口分布・セグメント例

関連スクリプト：
| スクリプト | 用途 |
|-----------|------|
| `scripts/prepare_population_data.py` | 人口データ準備 |
| `scripts/add_population_to_records.py` | Lead/Account人口付与 |

---

### 有料媒体（スクレイピングデータ）更新

**有料媒体からのスクレイピングデータを処理する場合は、必ず総合ルールを参照すること。**

📋 **`claudedocs/MASTER_RULE_paid_media.md`**（総合版 - 最初に参照）

このファイルには以下が定義されている：
- 絶対ルール（成約先除外、電話済み除外、バックアップ取得）
- 所有者割り当てルール（人材開発7名均等、市来に最強セグメント優先）
- メモ欄差別化（【新規作成】【既存更新】）
- レポート作成手順
- 共通チェックリスト

#### 媒体別個別ルール

| 媒体 | マスタールール | 固有の特徴 |
|-----|---------------|-----------|
| ミイダス | `MASTER_RULE_miidas.md` | 代表者情報、役職抽出、セグメント分析可能 |
| PT・OT・STネット | `MASTER_RULE_pt_ot_st_net.md` | リハビリ分類、雇用形態、採用人数、複数電話番号列 |
| ジョブポスター | `MASTER_RULE_jobposter.md` | ホームページ、掲載期間、シンプル構造 |
| 看護roo・ナース専科 | `MASTER_RULE_kango_nurse.md` | 施設形態、診療科目、住所+名前マッチング |

#### 所有者割り当て（新規リード）

| チーム | メンバー | 割り当て方式 |
|-------|---------|-------------|
| 人材開発 | 市来、嶋谷、小林、熊谷、松風、篠木、澤田 | 7名均等分配 |
| 深堀チーム | 深堀 | チーム単位 |
| 服部チーム | 服部 | チーム単位 |

**最強セグメント（代表者直通×携帯あり）→ 市来に優先割当**

#### 共通ルール

- **成約先除外必須**: 成約先電話番号に一致するレコードは絶対に含めない
- **電話済み除外**: 媒体掲載中リスト内の電話番号は除外
- **突合キー**: 電話番号（正規化後10〜11桁）
- **担当者名更新**: 一般名称→バイネームは常に更新、バイネーム→バイネームは未接触時のみ
- **メモ欄差別化**: 新規は【新規作成】、既存は【既存更新】を追記

関連スクリプト：
| スクリプト | 用途 |
|-----------|------|
| `scripts/generate_media_csv_final.py` | 有料媒体CSV生成（PT・OT・STネット + ジョブポスター） |
| `scripts/generate_miidas_csv.py` | ミイダスCSV生成 |
| `scripts/generate_kango_nurse_csv.py` | 看護媒体CSV生成（看護roo + ナース専科） |

**⚠️ 成約先に営業架電してしまうと事故になる。絶対にやってはダメ。**

---

## Salesforceインポート（書き込み）ルール

### 必須: ユーザー許可の確認

**Salesforceへのデータインポート（作成・更新・削除）を行う前に、必ずユーザーの明示的な許可を取得すること。**

インポート実行前に以下を確認・提示する：
1. **対象オブジェクト**: Lead, Account, Contact 等
2. **操作種別**: 新規作成 / 更新 / 削除
3. **件数**: 処理対象レコード数
4. **影響範囲**: 更新されるフィールド一覧
5. **プレビュー**: 実際の変更内容サンプル（数件）

```
例: 確認メッセージ
==================================================
Salesforceインポート確認
==================================================
対象: Lead（リード）
操作: 更新
件数: 150 件
更新フィールド: Description（メモ欄）

サンプル:
  - 00Q... : メモ欄に「ハローワーク求人情報...」追記
  - 00Q... : メモ欄に「ハローワーク求人情報...」追記

実行してよろしいですか？ [y/N]
==================================================
```

### 禁止事項
- ユーザー確認なしでの自動インポート実行
- dry_run=false でのテストなし本番実行
- 大量データ（1000件以上）の一括削除

### 🔴 新規リード作成時の必須フィールド

**新規リードをSalesforceにインポートする際は、以下の必須フィールドが確実に値を持つことを検証すること。**

| フィールド | API名 | 必須種別 | 対処 |
|-----------|------|---------|------|
| 会社名 | `Company` | 標準必須 | 空の場合はレコードをスキップ |
| 姓 | `LastName` | 標準必須 | 固定値「担当者」を設定 |
| 電話番号 | `Phone` | カスタム必須 | 空の場合はレコードをスキップ |

#### Phone フィールドの特別ルール

Salesforceのバリデーションルールにより `Phone` が空だとエラーになる。以下のロジックを適用：

```python
# 携帯電話のみの場合でも Phone に値を入れる
if is_mobile_phone(phone):
    phone_field = phone      # 携帯番号を Phone にも設定
    mobile_field = phone     # MobilePhone にも設定
else:
    phone_field = phone      # 固定電話を Phone に設定
    mobile_field = ''        # MobilePhone は空
```

#### CSV生成時のバリデーション

新規リードCSV生成時に以下のチェックを行い、不正レコードをスキップ：

```python
# Company必須チェック
if not company_name or not is_valid_value(company_name):
    skipped_no_company += 1
    continue

# Phone必須チェック
if not phone:
    skipped_no_phone += 1
    print(f"  警告: Phone空のためスキップ: {company_name}")
    continue
```

スキップ件数は処理終了時にサマリー出力すること。

## Salesforce API パターン

### 認証フロー
OAuth 2.0 リフレッシュトークンフローを使用：
1. `SalesforceClient.authenticate()` でアクセストークン取得
2. トークンは自動的に `_get_headers()` で管理

### データ抽出パターン
| API | 用途 | 使用例 |
|-----|------|--------|
| Describe API | オブジェクトメタデータ取得 | `get_all_fields('Account')` |
| Bulk API 2.0 | 大量データ抽出 | `export_object('Account')` |
| Report Export | レポートCSV出力 | `export_report(report_id, 'name')` |

### 抽出対象オブジェクト（デフォルト）
- Account（取引先）
- Contact（取引先責任者）
- Lead（リード）

### 🔴 レポート作成（Analytics API）

**カラム名・フィルタ列のフォーマット:**

レポート作成時のカラム名は **オブジェクト名のプレフィックスが必須**。

| ❌ 間違い | ✅ 正しい |
|----------|----------|
| `LegalPersonality__c` | `Account.LegalPersonality__c` |
| `ServiceType__c` | `Account.ServiceType__c` |
| `NAME` | `ACCOUNT.NAME` |

**レポート作成例:**
```python
report_metadata = {
    'reportMetadata': {
        'name': 'レポート名',
        'reportFormat': 'TABULAR',
        'reportType': {'type': 'AccountList'},
        'detailColumns': [
            'ACCOUNT.NAME',
            'Account.LegalPersonality__c',  # カスタムフィールドも Account. が必要
            'Account.ServiceType__c',
            'PHONE1'
        ],
        'reportFilters': [
            {'column': 'Account.LegalPersonality__c', 'operator': 'equals', 'value': '株式会社'},
            {'column': 'Account.ServiceType__c', 'operator': 'contains', 'value': '訪問'}
        ]
    }
}

url = f'{instance_url}/services/data/{api_version}/analytics/reports'
response = requests.post(url, headers=headers, json=report_metadata)
```

**主要なレポートタイプ:**
| レポートタイプ | 用途 |
|--------------|------|
| `AccountList` | 取引先レポート |
| `ContactList` | 取引先責任者レポート |
| `LeadList` | リードレポート |
| `OpportunityList` | 商談レポート |

**フィルタ演算子:**
| 演算子 | 意味 |
|-------|------|
| `equals` | 完全一致 |
| `notEqual` | 不一致 |
| `contains` | 部分一致（含む） |
| `notContain` | 部分不一致（含まない） |
| `startsWith` | 前方一致 |
| `greaterThan` | より大きい |
| `lessThan` | より小さい |

## Claude Code フック設定

### 保護対象ファイル（編集ブロック）
以下のファイルは `protect_files.py` フックにより編集がブロックされます：
- `.env`, `config/.env` - 環境変数
- `credentials.json` - 認証情報
- `.pem`, `.key` - 証明書・秘密鍵
- `secrets/` - シークレットディレクトリ

### 警告対象ファイル（確認付き編集可）
- `requirements.txt` - 依存関係
- `package.json`, `package-lock.json` - Node依存関係
- `.gitignore` - Git除外設定

## 設定パラメータ詳細

### settings.yaml 主要設定

| セクション | パラメータ | デフォルト | 説明 |
|-----------|-----------|----------|------|
| salesforce | api_version | 59.0 | Salesforce APIバージョン |
| salesforce | batch_size | 200 | Bulk API バッチサイズ |
| matching | similarity_threshold | 0.85 | 突合一致判定閾値 |
| sync | mode | incremental | 同期モード（full/incremental） |
| sync | dry_run | false | テスト実行フラグ |

### 追加の環境変数

| 変数名 | 説明 |
|-------|------|
| SF_API_VERSION | APIバージョン上書き（例: v59.0） |
| OUTPUT_DIR | 出力ディレクトリ（デフォルト: data/output） |

## 主要ライブラリ

| カテゴリ | ライブラリ | 用途 |
|---------|-----------|------|
| API | simple-salesforce | Salesforce接続 |
| スクレイピング | beautifulsoup4, selenium | Web抽出 |
| データ処理 | pandas, numpy | データ操作・分析 |
| 設定 | python-dotenv, pyyaml | 環境変数・YAML管理 |
| 検証 | pydantic | データバリデーション |
| ログ | loguru | 構造化ログ出力 |

## よく使うコマンド（追加）

```bash
# 型チェック
mypy src/

# Lint
flake8 src/ tests/

# カバレッジ付きテスト
pytest --cov=src tests/

# 単一オブジェクト抽出（Pythonインタプリタ）
python -c "from src.api.salesforce_client import SalesforceClient; c = SalesforceClient(); c.authenticate(); c.export_object('Account')"
```

## Windows環境での注意事項

### パス区切り文字
- Windows: `\`（バックスラッシュ）
- コード内では `pathlib.Path` を使用して OS 非依存にする
- 文字列でパスを書く場合は `r"path\to\file"` または `"path/to/file"` を使用

### 文字エンコーディング
- CSV読み書き時は `encoding='utf-8-sig'` を推奨（BOM付きUTF-8）
- Salesforceからのエクスポートは UTF-8
- Excel で開く場合は `utf-8-sig` が文字化けしにくい

### 改行コード
- Git設定: `core.autocrlf=true`（推奨）
- Python: `newline=''` オプションでCSV処理

### PowerShell/CMD の違い
```powershell
# PowerShell: 環境変数設定
$env:SF_CLIENT_ID = "your_id"

# CMD: 環境変数設定
set SF_CLIENT_ID=your_id
```

### よくあるトラブル
| 問題 | 原因 | 解決策 |
|------|------|--------|
| `UnicodeDecodeError` | エンコーディング不一致 | `encoding='utf-8-sig'` を指定 |
| パスが見つからない | バックスラッシュのエスケープ | raw文字列 `r"..."` を使用 |
| 権限エラー | ファイルが開かれている | Excel等を閉じてから実行 |

## 開発ワークフロー

### 1. 新機能開発フロー
```
1. 要件確認 → 2. 設計 → 3. 実装 → 4. テスト → 5. レビュー
```

### 2. 日常の開発サイクル
```bash
# 1. 最新コードを取得（Git使用時）
git pull origin main

# 2. 仮想環境をアクティベート
# Windows
.\venv\Scripts\activate

# 3. 依存関係を更新
pip install -r requirements.txt

# 4. コード品質チェック
black src/ tests/
flake8 src/ tests/
mypy src/

# 5. テスト実行
pytest tests/ -v

# 6. Salesforceデータ抽出（必要時）
python scripts/export_salesforce.py
```

### 3. コード品質チェックリスト
- [ ] `black` でフォーマット済み
- [ ] `flake8` でLintエラーなし
- [ ] `mypy` で型エラーなし
- [ ] `pytest` でテスト通過
- [ ] docstring記載済み（公開関数）

### 4. トラブルシューティング手順
1. **エラーメッセージを確認** → `logs/` ディレクトリをチェック
2. **認証エラー** → `config/.env` の設定値を確認
3. **API制限** → Salesforce APIの制限を確認（Bulk APIは1日10,000件制限あり）
4. **依存関係エラー** → `pip install -r requirements.txt` を再実行



---

## ハローワーク定例更新ワークフロー

### 全体フロー

### 重要なフィールドマッピング

#### 新規リード作成時の必須フィールド

| ハローワーク項目 | Salesforce API名 | 注意事項 |
|-----------------|------------------|---------|
| 事業所名称 | Company | - |
| 選考担当者ＴＥＬ | Phone | 正規化必要 |
| 選考担当者ＴＥＬ(携帯) | MobilePhone | 090/080/070始まりを判定 |
| 事業所郵便番号 | PostalCode | - |
| 事業所所在地 | Street | - |
| 事業所所在地(先頭) | Prefecture__c | 都道府県抽出ロジック必要 |
| 従業員数企業全体 | NumberOfEmployees | 整数変換必要 |
| 法人番号 | CorporateNumber__c | **13文字制限、.0除去必須** |
| 設立年 | Establish__c | - |
| 事業所ホームページ | Website | - |
| 選考担当者 | LastName | - |
| 選考担当者役職 | Title | - |
| 事業所名称カナ | Name_Kana__c | - |
| 代表者名 | PresidentName__c | - |
| 代表者役職 | PresidentTitle__c | - |

#### 既存レコード更新時の追加フィールド（漏れやすい）

| フィールド | オブジェクト | 備考 |
|-----------|-------------|------|
| Name_Kana__c | Lead | 氏名カナ |
| Title | Lead, Contact | 役職 |

### よくあるエラーと対処

| エラー | 原因 | 対処 |
|-------|------|------|
| STRING_TOO_LONG (CorporateNumber__c) | float型で読み込み→.0が付加 | dtype=strで読み込み |
| INVALID_EMAIL_ADDRESS | 不正なメールアドレス | 元データ品質問題として許容 |
| 列名無効（レポート作成時） | 標準フィールド命名規則違反 | PHONE等は大文字、カスタムはLead.Field__c形式 |

### Bulk API 2.0 注意事項

- バッチサイズ: 10,000件/ジョブ推奨
- 削除ジョブ失敗時: Composite API を200件ずつ使用
- CSVエンコーディング: utf-8-sig（BOM付き）

---

## 処理実績

### 2026-01-07 ハローワーク定例更新

| 処理 | 件数 | 状態 |
|-----|------|------|
| Account更新 | 17,496件 | 完了 |
| Contact更新 | 20,314件 | 完了 |
| Lead更新 | 65,794件 | 完了 |
| Lead追加更新（Name_Kana, Title） | 30,617件 | 完了 |
| Contact追加更新（Title） | 13,218件 | 完了 |
| 新規リード作成 | 3,627件 | 完了 |
| レポート作成 | 1件 | 完了 |

**成果物**:
- 作成済みリードID: data/output/hellowork/created_lead_ids_20260107.csv
- レポート: https://fora-career6.my.salesforce.com/lightning/r/Report/00Odc00000Hliv3EAB/view

### 2026-01-08 有料媒体更新（ミイダス、PT・OT・STネット、ジョブポスター）

| 処理 | 件数 | 状態 |
|-----|------|------|
| 新規リード作成 | 734件 | 完了 |
| 既存Lead更新 | 624件 | 完了 |
| 既存Account更新 | 430件 | 完了 |
| メモ欄差別化 | 全件 | 完了 |
| レポート作成 | 3件 | 完了 |

**所有者別内訳（新規リード）:**

| 所有者 | 件数 | 備考 |
|-------|------|------|
| 市来 | 38件 | 最強セグメント32件含む |
| 嶋谷 | 38件 | |
| 小林 | 38件 | |
| 熊谷 | 38件 | |
| 松風 | 38件 | |
| 篠木 | 38件 | |
| 澤田 | 38件 | |
| 深堀 | 235件 | |
| 服部 | 233件 | |

**成果物**:
- 作成済みリードID: data/output/media_matching/created_lead_ids_20260108.csv
- レポート（新規作成）: https://fora-career6.my.salesforce.com/lightning/r/Report/00Odc00000HqDnNEAV/view
- レポート（既存更新Lead）: https://fora-career6.my.salesforce.com/lightning/r/Report/00Odc00000HqE0HEAV/view
- レポート（既存更新Account）: https://fora-career6.my.salesforce.com/lightning/r/Report/00Odc00000HqEI1EAN/view

### 2026-01-13 看護媒体インポート（看護roo、ナース専科）

| 処理 | 件数 | 状態 |
|-----|------|------|
| 新規リード作成 | 1,048件 | 完了 |
| Lead更新 | 6,472件成功 / 198件失敗* | 完了 |
| Account更新 | 1,029件 | 完了 |
| レポート作成 | 3件 | 完了 |

*失敗198件は `CANNOT_UPDATE_CONVERTED_LEAD`（コンバート済みLead）

**所有者別内訳（新規リード）:**

| 所有者 | 件数 |
|-------|------|
| 篠木 | 350件 |
| 小林 | 233件 |
| 清飛羅 | 233件 |
| 灰野 | 232件 |

**バッチID**: `【BATCH_20260113_KANGO】`

**成果物**:
- 作成済みリードID: data/output/media_matching/created_lead_ids_20260113.csv
- レポート（新規作成）: https://fora-career6.my.salesforce.com/lightning/r/Report/00Odc00000HlqroEAB/view
- レポート（Lead更新）: https://fora-career6.my.salesforce.com/lightning/r/Report/00Odc00000HlqsfEAB/view
- レポート（Account更新）: https://fora-career6.my.salesforce.com/lightning/r/Report/00Odc00000HlqtJEAR/view
- 詳細レポート: claudedocs/media_matching_import_20260113.md

**スクリプト改善**:
- 住所+名前マッチングによるAccount検出追加
- 新規リード必須フィールドバリデーション追加（Company, Phone空チェック）
- Phoneフィールド必須対応（携帯のみでもPhoneに値を設定）

### 2026-03-02 ハローワーク セグメントA/B 初回インポート

| 処理段階 | 件数 |
|---------|------|
| ハローワーク入力（2ファイル結合） | 473,682件 |
| A/B抽出 | 147,140件 |
| 品質フィルタ後 | 25,918件 |
| 重複排除後 | 10,382件 |
| SF既存マッチ | 10,051件 |
| 新規リード候補 | 331件 |
| 成約先除外 | 0件 |
| SF既存重複削除 | -204件 |
| **最終Salesforce登録** | **127件** |

| 項目 | 値 |
|------|-----|
| 所有者 | 小林 幸太（全件） |
| レポート（全件） | `00Odc00000LErQ9EAL` |

### 2026-03-05 ハローワーク セグメントA/B 第2回インポート

| 処理段階 | 件数 |
|---------|------|
| ハローワーク入力（3ファイル結合） | 536,223件 |
| A/B抽出（2軸MECE） | 167,794件 |
| 多角的職種フィルタ（step2b） | 164,757件（-3,037） |
| 品質フィルタ後 | 32,991件 |
| 重複排除後 | 10,981件 |
| SF既存マッチ | 10,839件 |
| 新規リード候補 | 142件 |
| 成約先除外 | 10件 |
| **最終Salesforce登録** | **132件** |

**所有者別内訳:**

| 所有者 | 件数 | 備考 |
|-------|------|------|
| 深堀 勇侍 | 77件 | 保育園22件 + その他55件 |
| 服部 翔太郎 | 55件 | |

**成果物**:
- 作成済みリードID: data/output/hellowork_segments/import_ready/created_lead_ids_20260305.csv
- レポート（全体）: https://fora-career6.my.salesforce.com/lightning/r/Report/00Odc00000LV37BEAT/view
- レポート（深堀）: https://fora-career6.my.salesforce.com/lightning/r/Report/00Odc00000LV3APEA1/view
- レポート（服部）: https://fora-career6.my.salesforce.com/lightning/r/Report/00Odc00000LV3C1EAL/view

**改善点**:
- 3ファイル対応（RCMEB002002_M100 (3).csv 追加）
- step2b 多角的職種フィルタ実装（44コード許可、募集難易度高を除外）
- 同事業所の他募集情報をメモ欄に付与
- CorporateNumber__c の .0 除去バグ修正

### 2026-03-05 ミイダス保育園リスト インポート

| 処理 | 件数 | 状態 |
|-----|------|------|
| 新規リード作成 | 66件 + 1件リトライ = **67件** | 完了 |
| 既存Lead更新 | 76件成功 + 6件リトライ = **82件** | 完了 |
| コンバート済みリードスキップ | 3件 | スキップ |
| 既存Account更新 | **39件** | 完了 |
| 成約先除外 | 7件 | 除外 |

**所有者**: 新規リード全件 → 深堀、既存更新 → 所有者変更なし

**メモ**:
- 新規: `【新規作成】有料媒体突合 2026-03-05 ミイダス保育園リスト`
- 更新: `【既存更新】有料媒体突合 2026-03-05 ミイダス保育園リスト`

**成果物**:
- レポート（新規リード）: https://fora-career6.my.salesforce.com/lightning/r/Report/00Odc00000LVWPKEA5/view
- レポート（既存更新リード）: https://fora-career6.my.salesforce.com/lightning/r/Report/00Odc00000LVfyrEAD/view
- レポート（Account更新）: https://fora-career6.my.salesforce.com/lightning/r/Report/00Odc00000LVdc6EAD/view

**データソース**: `miidas_structured_data.csv` (339件、197列、2026-03-02スクレイピング)

**既知の問題**:
- Lead.Descriptionフィールドがこのorg上に存在しない → Description追記は未実行
- LastNameにスペース含む6件 → LastName除外で他フィールドのみ更新
- Paid_NumberOfRecruitment__c はdouble型 → 「3名」→「3」に変換

### 2026-03-09 ウェルミージョブ（kaigojob）新規リードインポート

| 処理段階 | 件数 |
|---------|------|
| kaigojob求人データ入力 | 9,661件 |
| Google検索スクレイピング（2ファイル結合） | 4,132件 + 6,946件 |
| URL突合で電話番号取得 | 5,743件（ユニーク電話番号） |
| SF既存マッチ（Lead/Account） | 3,434件 |
| 成約先除外（電話番号+法人番号） | 380件 |
| 新規リード候補 | 1,929件 |
| ハローワーク従業員数補完 | 346件マッチ（67件電話+279件名前住所） |
| 追加成約先除外（HW法人番号） | 49件 |
| 従業員数11-100フィルタ後 | **101件** |
| **Salesforceインポート** | **101件成功 / 0件失敗** |

| 項目 | 値 |
|------|-----|
| 所有者 | 藤巻 真弥（全件） |
| LeadSource | 有料媒体 |
| メモフィールド | `Paid_Memo__c`（【有料】求人メモ） |
| バッチジョブID | `750dc00000eFOSnAAO` |

**メモ欄内容**:
- `【新規作成】ウェルミージョブ 求人入稿日: 2026-03-09`
- `ハローワーク突合済み`
- 求人情報（法人名、事業所名、サービス区分/種別、募集職種、給与、雇用形態、勤務地、応募資格、仕事内容、勤務時間、福利厚生、求人URL）
- ハローワーク突合データ（突合方法、従業員数、法人番号、代表者名/役職、資本金、設立年、産業分類、事業内容）

**データパイプライン**:
1. kaigojob求人データ（法人名、事業所名、サービス種別等）
2. Google検索スクレイピング（電話番号、住所取得）
3. URL突合で情報統合
4. SF Lead/Account電話番号マッチング（既存除外）
5. 成約先除外（電話番号 + HW法人番号）
6. ハローワーク補完（名前+住所マッチング、住所検証付き）
7. 従業員数11-100名フィルタ
8. Salesforce Bulk API 2.0 インポート

**成果物**:
- 作成済みリードID: data/output/google_scraping/created_lead_ids_20260309.csv
- インポートCSV: data/output/google_scraping/kaigojob_sf_import_20260309.csv
- レポート: https://fora-career6.my.salesforce.com/lightning/r/Report/00Odc00000LjoUPEAZ/view

**既知の問題・対応**:
- `Description` 標準フィールドがこのorgに存在しない → `Paid_Memo__c` を使用
- 従業員数未補完の残り1,584件は保留中（HWマッチなし）

**関連スクリプト**:
| スクリプト | 用途 |
|-----------|------|
| `scripts/merge_all_kaigojob_v2.py` | 全データソース統合（kaigojob+Google+SF突合） |
| `scripts/enrich_kaigojob_v2_from_hellowork.py` | ハローワーク従業員数・法人情報補完 |
| `scripts/prepare_kaigojob_import_v2.py` | SFインポート用CSV生成（藤巻全件割当） |
| `scripts/import_kaigojob_20260309.py` | Bulk API 2.0インポート＋レポート作成 |

### 2026-03-10 有料媒体インポート（PT・OT・STネット + ジョブポスター）

| 処理 | 成功 | 失敗 | 備考 |
|-----|------|------|------|
| 新規リード作成 | 210件 | 0件 | 全件成功（リトライ含む） |
| 既存Lead更新 | 402件 | 4件 | コンバート済み4件スキップ |
| 既存Account更新 | 307件 | 0件 | 全件成功 |

**所有者別内訳（新規リード）:**

| 所有者 | 件数 |
|-------|------|
| 深堀 勇侍 | 105件 |
| 服部 翔太郎 | 105件 |

**メモ**:
- 新規: `【新規作成】有料媒体突合 2026-03-10 PT・OT・STネット / ジョブポスター`
- 更新: `【既存更新】有料媒体突合 2026-03-10 PT・OT・STネット / ジョブポスター`

**成果物**:
- 作成済みリードID: data/output/media_matching/created_lead_ids_20260310.csv（222件）
- レポート（新規作成・全体）: https://fora-career6.my.salesforce.com/lightning/r/Report/00Odc00000LpHAnEAN/view
- レポート（新規作成・深堀）: https://fora-career6.my.salesforce.com/lightning/r/Report/00Odc00000LqczZEAR/view
- レポート（新規作成・服部）: https://fora-career6.my.salesforce.com/lightning/r/Report/00Odc00000Lqd1BEAR/view
- レポート（Lead更新）: https://fora-career6.my.salesforce.com/lightning/r/Report/00Odc00000LpHE1EAN/view
- レポート（Account更新）: https://fora-career6.my.salesforce.com/lightning/r/Report/00Odc00000LpQQzEAN/view

**データソース**:
- PT・OT・STネット: `extracted_phone_and_name.csv`（スクレイピング）
- ジョブポスター: `job-poster.com-から詳細をスクレイピングします--2--2026-03-10.csv`

**処理フロー**:
1. 2ファイル読み込み・電話番号正規化
2. 成約先除外（電話+会社名）: 65件除外
3. 媒体掲載中リスト除外（電話済み）
4. SF既存Lead/Account/Contact突合
5. 新規/更新/Account更新のCSV生成
6. Bulk API 2.0インポート + レポート作成

**リトライで解決した問題**:
- LeadSourceMemo__c 255文字超過 → 252文字+`...`に切り詰め
- Prefecture__c 制限付き選択リスト無効値 → Describe APIで有効値取得、無効値クリア
- Paid_EmploymentType__c 255文字超過 → 切り詰め
- 全角数字（Paid_NumberOfRecruitment__c） → 半角変換
- LastNameバリデーション（スペース含む） → LastName列除外で更新
- AccountレポートACCOUNT_NAME無効 → `ACCOUNT.NAME`に修正

**関連スクリプト**:
| スクリプト | 用途 |
|-----------|------|
| `scripts/generate_media_csv_20260310.py` | マスタールール準拠CSV生成 |
| `scripts/import_media_20260310.py` | Bulk API 2.0インポート＋レポート作成 |
| `scripts/retry_media_20260310.py` | リトライ v1（LastName除外等） |
| `scripts/retry_media_20260310_v2.py` | リトライ v2（Prefecture修正、全角数字修正） |

## 今後の改善候補

（このセクションはsuggest-claude-mdコマンドで自動更新されます）


<claude-mem-context>
# Recent Activity

<!-- This section is auto-generated by claude-mem. Edit content outside the tags. -->

*No recent activity*
</claude-mem-context>