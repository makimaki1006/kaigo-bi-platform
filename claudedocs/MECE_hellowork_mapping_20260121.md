# 市区町村×ハローワーク MECEマッピング ドキュメント

**作成日**: 2026-01-21
**最終更新**: 2026-01-21

---

## 1. 概要

全国1,918市区町村に対して、管轄ハローワークと有効求人倍率を紐付けたMECE（漏れなく・ダブりなく）マッピングデータを構築した。

### 目的
- Salesforceの Lead/Account に対して、所在地から有効求人倍率を付与できるようにする
- 地域別の採用難易度を可視化する

### 最終成果物
```
data/job_openings_ratio/complete_mece_municipality_hellowork_mapping.csv
```

---

## 2. データソース

### 2.1 ハローワーク求人倍率データ（オリジナル）

| ソース | URL | 取得データ |
|-------|-----|-----------|
| hwiroha.com | https://www.hwiroha.com/ | 475ハローワークの有効求人倍率 |

**取得日**: 2026-01-20
**データ時点**: 令和7年11月分

### 2.2 補完データ（実データのみ）

| ソース | ハローワーク数 | 内容 |
|-------|--------------|------|
| 新潟転職.com | 13 | 新潟県全ハローワーク |
| 岐阜労働局 | 1 | 岐阜 1.47倍（令和7年7月） |
| 佐賀労働局 | 1 | 武雄 1.10倍（令和7年5月） |

**補完データファイル**: `data/job_openings_ratio/supplementary_ratio_data.csv`

### 2.3 市区町村マスタ

| ソース | 内容 |
|-------|------|
| 総務省 | 全国地方公共団体コード（1,918市区町村） |

---

## 3. マッピング手法

### 3.1 マッチタイプ

| タイプ | 件数 | 説明 |
|-------|------|------|
| direct | 1,587 | 市区町村名から直接マッチ |
| additional_manual | 196 | 追加手動マッピング（町村等） |
| fallback | 79 | 都道府県フォールバック |
| seirei_borrowed | 26 | 政令指定都市区の借用 |
| manual | 17 | 完全手動マッピング |
| seirei_ward | 9 | 政令指定都市区 |
| no_hellowork | 4 | ハローワークなし（北方領土） |

### 3.2 処理フロー

```
1. 市区町村マスタ読み込み（1,918件）
2. ハローワーク管轄データ読み込み（538箇所）
3. hwiroha.comから求人倍率取得（475箇所）
4. 補完データ適用（15箇所追加 → 490箇所）
5. 市区町村 × ハローワーク マッチング
6. MECE検証（漏れ・ダブりチェック）
7. CSV出力
```

---

## 4. 最終結果

### 4.1 カバレッジ

| 項目 | 件数 | 割合 |
|------|------|------|
| 総市区町村数 | 1,918 | 100% |
| 求人倍率あり | 1,755 | 91.5% |
| 求人倍率なし | 163 | 8.5% |

### 4.2 求人倍率なしの内訳（163件）

**方針**: 推計値は使用せず、実データが取得できない市区町村は空欄とする

| 都道府県 | 件数 | 該当ハローワーク | 理由 |
|---------|------|-----------------|------|
| 山梨県 | 27 | 甲府, 富士吉田, 都留, 塩山, 大月, 韮崎, 鰍沢 | hwiroha.comで「非公表」 |
| 北海道 | 15 | 苫小牧, 根室, 北方領土4村 | 一部データなし |
| 千葉県 | 11 | 千葉 | データなし |
| 岡山県 | 10 | 岡山, 笠岡 | データなし |
| 和歌山県 | 10 | 橋本, 御坊 | データなし |
| 東京都 | 9 | 青梅, 町田 | データなし |
| 鹿児島県 | 7 | 鹿児島, 熊毛 | データなし |
| 大分県 | 7 | 日田, 豊後大野, 宇佐 | データなし |
| その他 | 67 | 各県1〜6件 | データなし |

### 4.3 求人倍率の統計（空欄除く）

| 指標 | 値 |
|------|-----|
| 最小 | 0.45倍 |
| 最大 | 5.44倍 |
| 平均 | 1.16倍 |
| 中央値 | 1.11倍 |

---

## 5. 出力ファイル

### 5.1 メインファイル

**パス**: `data/job_openings_ratio/complete_mece_municipality_hellowork_mapping.csv`

| カラム | 型 | 説明 |
|-------|-----|------|
| municipality_code | int | 市区町村コード（総務省） |
| prefecture | str | 都道府県名 |
| municipality | str | 市区町村名 |
| hellowork_name | str | 管轄ハローワーク名 |
| ratio | float | 有効求人倍率（空欄あり） |
| year | int | データ年 |
| month | int | データ月 |
| match_type | str | マッチタイプ |

### 5.2 関連ファイル

| ファイル | 内容 |
|---------|------|
| `supplementary_ratio_data.csv` | 補完用求人倍率データ（実データのみ15件） |
| `job_ratio_all_20260120_165953.csv` | hwiroha.comからの元データ |
| `additional_manual_mapping.csv` | 追加手動マッピング定義 |
| `mece_summary.txt` | サマリーレポート |

---

## 6. スクリプト

### 6.1 メインスクリプト

**パス**: `scripts/build_complete_mece_mapping.py`

```bash
# 実行方法
cd C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List
python scripts/build_complete_mece_mapping.py
```

### 6.2 主要関数

| 関数 | 説明 |
|------|------|
| `load_supplementary_ratio_data()` | 補完データ読み込み |
| `apply_supplementary_ratios()` | 補完データ適用 |
| `match_municipality_to_hellowork()` | 市区町村マッチング |
| `verify_mece()` | MECE検証 |

---

## 7. 更新手順

### 7.1 求人倍率データの更新

1. hwiroha.comから最新データをスクレイピング
2. `job_ratio_*.csv` として保存
3. `build_complete_mece_mapping.py` の `load_hellowork_ratio_data()` で参照先を更新
4. スクリプト実行

### 7.2 補完データの追加

1. 労働局サイト等から実データを取得
2. `supplementary_ratio_data.csv` に追記
   ```csv
   prefecture,hellowork_name,ratio,year,month,source,notes
   〇〇県,〇〇,1.23,2025,11,〇〇労働局,令和7年11月分
   ```
3. スクリプト実行

### 7.3 注意事項

- **推計値は使用しない**: 県平均等での推計は行わない方針
- **実データのみ**: Web検索やPDFから確認できた実データのみを使用
- **北方領土**: 4村（色丹村、留夜別村、留別村、紗那村）はハローワークなし

---

## 8. Salesforce連携

### 8.1 使用方法

Lead/Accountの住所から市区町村を特定し、本マッピングデータと突合することで有効求人倍率を付与できる。

```python
# 例: Leadに求人倍率を付与
import pandas as pd

# マッピングデータ読み込み
mapping = pd.read_csv('complete_mece_municipality_hellowork_mapping.csv')

# Lead住所から市区町村を抽出して突合
# → ratio カラムの値を Lead.JobOpeningsRatio__c 等に設定
```

### 8.2 カスタムフィールド案

| フィールド | API名 | 型 |
|-----------|------|-----|
| 有効求人倍率 | JobOpeningsRatio__c | Number(3,2) |
| 管轄ハローワーク | HelloWorkName__c | Text(50) |
| 求人倍率更新日 | JobOpeningsRatioDate__c | Date |

---

## 9. 参考リンク

- [hwiroha.com](https://www.hwiroha.com/) - ハローワーク求人倍率データ
- [新潟転職.com](https://niigata-tenshoku.com/) - 新潟県ハローワーク別データ
- [厚生労働省 一般職業紹介状況](https://www.mhlw.go.jp/stf/newpage_64026.html) - 公式統計
- [総務省 地方公共団体コード](https://www.soumu.go.jp/denshijiti/code.html) - 市区町村コード

---

## 10. 変更履歴

| 日付 | 内容 |
|------|------|
| 2026-01-20 | 初版作成、hwiroha.comからデータ取得 |
| 2026-01-21 | 新潟転職.comから補完データ追加（13件） |
| 2026-01-21 | 岐阜・武雄の実データ追加 |
| 2026-01-21 | 推計値削除、実データのみに変更 |
| 2026-01-21 | 最終版完成（1,755件に求人倍率あり） |

