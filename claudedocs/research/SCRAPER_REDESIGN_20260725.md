# 介護スクレイパー改修 分析・設計書

作成日: 2026-07-25
対象: kaigo-bi-platform の介護サービス情報公表システム スクレイピング一式
目的: facilities 223,103 行のうち財務DL URL が 8,806 行しか無い原因の特定と、全施設で財務諸表 URL を取得するための改修設計。**本書は分析・設計のみ。コード変更は含まない。**

---

## 0. 結論サマリー

- **8,806 件の直接原因**: 財務DL URL を取得できるのは `scrape_kaigo_full.py` の `parse_unei()`(= 運営状況 unei ページ）**だけ**。全国 223k 行の骨格を作った `scrape_kaigo_nationwide.py` と差分の `scrape_kihon_delta.py` は **kihon(詳細)ページしか見ておらず、unei ページを一度も取得していない**。財務DL 列を持つのは古い `kaigo_fast_20260324.csv` の残存値だけで、これは unei ページを流した一部(≈8,806 件)の部分実行の産物。つまり **カバレッジ不足が主因**。
- **副次要因(真の上限)**: 財務諸表公表(経営情報の見える化)は FY2024 開始の新制度で、公表は段階的・一部任意。全 223k 施設が PDF を持つわけではない。ただし実地確認の限り URL は決定論的に判定でき、8,806 は明らかに過少。
- **朗報(実地検証済み)**: 財務PDF の URL は **タイムスタンプ不要で完全に決定論的**。`https://www.kaigokensaku.mhlw.go.jp/upload/jigyosyofile/{PP}/{JigyosyoCd}_00_{ServiceCd}/{IncomeStatementFile|CashFlowFile|BalanceSheetsFile}/kouhyou/1.pdf` を GET し、200+`%PDF` なら有り・404 なら無し、で全施設を機械的に判定できる。HTML パース不要のルートが存在する。

---

## 1. 現状分析

### 1.1 スクレイパーの構成と財務DLの取得ロジック

| スクリプト | 取得ページ | 財務DL列 | 並列 | 役割 |
|---|---|---|---|---|
| `scrape_kaigo_nationwide.py` | **kihon のみ** | **無し**(OUTPUT_COLUMNS に列自体が無い) | 逐次(1.5秒間隔) | 全国の基本情報スクレイプ。223k の骨格 |
| `scrape_kaigo_full.py` (mode=full) | kihon+kani+**unei**+original | **有り**(`parse_unei`) | 逐次(1.5秒間隔) | 4ページ全取得。財務DLはここだけ |
| `scrape_kaigo_full.py` (mode=list) | kihon のみ | 無し | 逐次 | 架電リスト用 |
| `scrape_kihon_delta.py` | **kihon のみ**(15並列) | **無し**(DELTA_COLUMNS に無い) | 15並列 | 職種別人数・資格・加算の差分取得 |
| `merge_kihon_and_reload.py` | — | 既存CSV由来を保持するだけ | — | `kaigo_fast_20260324.csv` + `kihon_delta_*` を left join → Turso |

財務DL URL を実際に書き込むコードは **`scrape_kaigo_full.py:610-636`(`parse_unei` 内)の1箇所のみ**。抽出条件は以下:

```python
for table in tables:
    text = table.get_text()
    if '会計の種類' in text or '財務状況' in text:   # ← ゲート条件
        links = table.find_all('a')
        for link in links:
            href = link.get('href', '')
            text = link.get_text(strip=True)
            full_text = (link.parent.get_text(strip=True) or '') + text
            if href and ('download' in href.lower() or 'ダウンロード' in text):
                if   '事業活動' in full_text or '損益'   in full_text: result['財務DL_事業活動計算書'] = href
                elif '資金収支' in full_text or 'キャッシュ' in full_text: result['財務DL_資金収支計算書'] = href
                elif '貸借'     in full_text or 'バランス' in full_text: result['財務DL_貸借対照表']   = href
```

### 1.2 「8,806 件しか無い」原因の分解

**主因 = unei ページの未取得(カバレッジ不足)**
- `merge_kihon_and_reload.py` が現行 facilities テーブルを組み立てるが、そのベース `kaigo_fast_20260324.csv` は kihon 中心のスクレイプ。差分 `kihon_delta` も kihon のみ。**再構築のどの経路でも unei ページを新規取得していない**ため、財務DL 列は「昔 unei を流した約 8,806 件」から増えようがない。

**parse_unei 側の抽出漏れ(unei を流した施設内でも取りこぼす脆さ)** — 改修で必ず直す点:
1. **ゲート条件が弱い**: 財務DLリンクは実ページでは「**11．経営情報の見える化のために講じている措置**」セクション配下にある(§2.2 で実HTML確認済み)。しかしコードは `'会計の種類'`/`'財務状況'` を含むテーブルに限定。会計種類が空欄の施設(パイロットmanifestに実在: 事業所 0270200355 の accounting_type 空)ではゲートが外れ、リンクが取れない可能性。
2. **href 判定が本文依存**: 実 href は `/upload/.../IncomeStatementFile/kouhyou/1.pdf` で `download` 文字列を含まない。`'ダウンロード' in text`(リンク文言)頼み。文言に「[ダウンロード]」が無い様式では取りこぼす。
3. **フォルダ名という最強の手掛かりを使っていない**: PL/CF/BS は href のフォルダ名 `IncomeStatementFile`/`CashFlowFile`/`BalanceSheetsFile` で確実に分類できるのに、本文の「損益/キャッシュ/貸借」文字列でマッチしている。

**真の上限(制度要因)**: 財務諸表公表は介護サービス事業者経営情報報告制度(FY2024〜)に基づく新しい開示で、掲載は段階導入・一部任意。全施設が PDF を持つわけではない。したがって全 unei を流しても 223k 全件に URL が付くことはない。**8,806 が過少なのは確実だが、真の到達可能件数は未知**(数万件規模と推定、要実測)。

**確定させるための最小クエリ(推奨)**: facilities で `会計種類` の充足件数と `財務DL_事業活動計算書` の充足件数を比較する。
- もし `会計種類 ≈ 財務DL ≈ 8,806` → unei ページ自体が 8,806 件しか流れていない(=カバレッジ不足が主因、で確定)。
- もし `会計種類 >> 財務DL` → unei は広く流れているが parse ロジックが取りこぼしている(=parse バグが主因)。
現状のパイプライン構造からは前者(カバレッジ不足)がほぼ確実。

### 1.3 URL 構造・ページ種別(実確認済み)

詳細ページの URL パターン(`build_url`):
```
https://www.kaigokensaku.mhlw.go.jp/{PP}/index.php?action_kouhyou_detail_{ACTION}_{kani|kihon|unei}=true&JigyosyoCd={JigyosyoCd}-00&ServiceCd={ServiceCd}
（original のみ action_kouhyou_detail_original_index=true）
```
- `PP` = 都道府県コード2桁(事業所番号 先頭2桁)
- `ACTION` = サービスコード→action_code マッピング(3桁, 例 110→"001")
- 4ページ種別: kani(概要) / kihon(詳細) / unei(運営状況) / original(その他)
- **財務DL は unei ページの「11．経営情報の見える化のために講じている措置」にある**

### 1.4 行政処分・賃金表・サービス提供地域の取得状況

| 項目 | 取得ページ | 実装場所 | 現状 |
|---|---|---|---|
| 賃金表(5職種) | **original** | `parse_original:705-735` | full モードでしか取得しない。nationwide/delta では未取得 |
| 行政処分・行政指導 | **original** | `parse_original:737-755` | 同上。original を流した施設のみ |
| サービス提供地域 | **kani** | `parse_kani:521-532` | 同上。kani を流した施設のみ。**充足0%** は kani 未取得が原因 |

→ 賃金・行政処分・サービス提供地域も財務DLと**同じ構造的問題**(kani/original ページ未取得)。財務DL 改修と同一の「不足ページを全施設で流す」設計で一括解決できる。

### 1.5 実行時間・並列・エラー処理・resume の現状

- **並列**: nationwide/full は逐次(REQUEST_DELAY 1.5秒)。delta は `ThreadPoolExecutor` 15並列 + スレッドごと 1秒レート制限。
- **エラー処理**: リトライ 3回(指数バックオフ)。nationwide/full は連続10エラーで Bot 検知とみなし中断(`CONSECUTIVE_ERROR_LIMIT`)。delta は失敗行も空レコードで記録し継続。
- **resume**: 有り。nationwide/full はサービス別 `progress_{svc}.csv` に事業所番号ベースで済みを記録し再開。delta は `delta_progress*.csv` に `{事業所番号}_{サービスコード}` 複合キーで記録。`--fresh` でリセット。
- **所要時間目安**: delta(kihon 1ページ/施設・15並列)で全国 ≈ 6〜12時間の実績(CLAUDE.md)。

---

## 2. 実ページHTML構造(検証結果)

### 2.1 財務PDF の URL は決定論的(curl 実測 2026-07-25)

サンプル施設 `0170105449`(訪問介護, ServiceCd 110, 北海道 PP=01):

| 検証 | URL | 結果 |
|---|---|---|
| timestamp あり(基準) | `.../IncomeStatementFile/kouhyou/1.pdf?1738224245` | **200** / 307,402 B |
| **timestamp なし** | `.../IncomeStatementFile/kouhyou/1.pdf` | **200** / 307,402 B(同一) |
| 2.pdf(複数年度?) | `.../IncomeStatementFile/kouhyou/2.pdf` | 404 |
| BS フォルダ | `.../BalanceSheetsFile/kouhyou/1.pdf` | 200 / 286,563 B |
| 存在しない施設 | `.../13/9999999999_00_110/.../1.pdf` | 404(4,074 B のエラーページ) |

**結論**: `?timestamp` はキャッシュバスターで**不要**。URL は `(PP, JigyosyoCd, ServiceCd, docフォルダ)` から完全構築でき、200+`%PDF` 判定で有無を機械的に確定できる。年度は基本 `1.pdf` のみ(複数年度施設は少数、要 unei 確認)。

### 2.2 unei ページの財務セクション(WebFetch 実確認)

- セクション見出し: **「11．経営情報の見える化のために講じている措置」**
- 財務テーブル内のリンク3種(リンク文言 / href):
  - 事業活動計算書: 「(名称)損益計算書 [ダウンロード]」 / `.../IncomeStatementFile/kouhyou/1.pdf?...`
  - 資金収支計算書: 「(名称)キャッシュフロー計算書 [ダウンロード]」 / `.../CashFlowFile/kouhyou/1.pdf?...`
  - 貸借対照表: 「(名称)貸借対照表 [ダウンロード]」 / `.../BalanceSheetsFile/kouhyou/1.pdf?...`
- 会計基準による様式差(社福会計基準/医療法人会計/病院会計準則/株式会社等)あり。DL リンクのフォルダ名は会計基準に依らず共通(`IncomeStatementFile`/`CashFlowFile`/`BalanceSheetsFile`)。
- 既存パイロット(`data/financial_pilot/`)が5法人種別×5件でダウンロード成功、構造化抽出(revenue 等)まで実施済み。並行チーム(extract-* / pdf-research)が PDF→数値抽出を進行中。

---

## 3. 改修設計

### 3.1 財務DL URL を全施設で取る改修方針

**推奨: 方式B(unei ページ差分スクレイプ)を主軸**、方式A(URL 直接プローブ)を高速代替として併走可能。

**方式B: `scrape_unei_delta.py`(新規, delta と同構造)**
- 対象: facilities 全施設(事業所番号・サービスコード・都道府県コード)。既存 `kaigo_fast` の施設リストを入力にする(delta と同じ入力方式)。
- 各施設 unei ページを 1 リクエスト取得し、以下を抽出:
  - 財務DL 3種(PL/CF/BS)— **href のフォルダ名で分類**(`IncomeStatementFile`→事業活動, `CashFlowFile`→資金収支, `BalanceSheetsFile`→貸借。§2.2 で確定)。本文文字列マッチはやめる。
  - 会計種類、品質項目(BCP/ICT/第三者評価/損害賠償)、全加算 — 既存 `parse_unei` の資産を流用。
- parse ロジックの修正点(§1.2 の脆弱性を除去):
  1. ゲートを `'会計の種類'` 依存から外し、**ページ内の全 `<a href>` を走査**して `/upload/jigyosyofile/` かつ `(IncomeStatement|CashFlow|BalanceSheets)File/` を含む href を直接拾う。
  2. 分類は href のフォルダ名で決定(取りこぼしゼロ、本文様式差に非依存)。
  3. 取得した相対 href はそのまま保存(タイムスタンプ付きでも可。ダウンロード時は §2.1 より無視できる)。
- 出力列: `財務DL_事業活動計算書 / 財務DL_資金収支計算書 / 財務DL_貸借対照表 / 会計種類`(+ 任意で品質・加算)。事業所番号+サービスコードで既存にマージ。

**方式A: URL 直接プローブ(HTML パース不要, 最速の presence 判定)**
- 各施設で PL の URL を 1 回 GET(Range 0-3 で `%PDF` 先頭確認、または HEAD)。200 なら CF/BS も同型で構築して確定。
- 長所: HTML 取得・解析コスト無し、1施設1リクエストで presence 確定。
- 短所: 会計種類・複数年度・稀な様式差を取れない。**PDF の存在確認と URL 生成だけが目的なら方式Aが最速**。
- 推奨運用: まず方式A で全施設の presence + URL を高速確定 → 200 の施設だけ方式B相当で会計種類等の付随情報を補完、という2段構えが最も効率的。

**いずれの方式でも parse_unei の財務抽出は href フォルダ名ベースに書き換えること(最重要)。**

### 3.2 オープンデータCSV取込との役割分担

別調査(gov_opendata_kaigo)の通り、事業所マスタは介護サービス情報公表システムのオープンデータ CSV(年2回, CC BY)で更新可能。スクレイピングは CSV に無い差分項目に集中する。

| データ | 取得元 | 更新頻度 | 内容 |
|---|---|---|---|
| **事業所マスタ** | **オープンデータ CSV**(`jigyosho_{svc}.csv`) | 年2回 | 事業所番号/名称/住所/電話/法人番号/定員/URL 等。※現行 `download_open_data_csv()` で既に取得している基盤 |
| 財務DL URL + 財務諸表 | **スクレイピング(unei / URL プローブ)** | 随時 | CSV に無い。本改修の主対象 |
| 詳細運営情報(会計種類・品質・全加算) | スクレイピング(unei) | 随時 | CSV に無い |
| 職種別人数・資格・認知症研修 | スクレイピング(kihon delta, 既存) | 随時 | CSV に無い |
| 賃金表・行政処分 | スクレイピング(original) | 随時 | CSV に無い。§1.4、財務と同時に流すと効率的 |
| サービス提供地域 | スクレイピング(kani) | 随時 | CSV に無い。充足0%、要取得 |

役割: **マスタ=CSV で洗い替え、スクレイピング=CSV に無い付加項目のみ差分取得**。unei を流すなら kani/original も同時取得して賃金・行政処分・サービス提供地域の 0% 問題も一括解消するのが合理的。

### 3.3 実行計画(対象件数・リクエスト数・所要時間・レート制御・resume)

前提: 対象 ≈ 223,000 施設×サービス行。

| 方式 | リクエスト/施設 | 総リクエスト | 15並列・~1秒/req 時の目安 |
|---|---|---|---|
| 方式A(PL プローブのみ) | 1 | ≈ 223k | 約 4〜5 時間 |
| 方式B(unei 1ページ) | 1 | ≈ 223k | 約 4〜10 時間(HTML 取得/解析込み、実測 delta で 6〜12h) |
| unei+kani+original 同時 | 3 | ≈ 669k | 約 12〜24 時間 |

- **レート制御**: 既存 delta 準拠。15並列 + スレッドごと 1 秒間隔(実効 ~15 req/s)。gov サイトなので過負荷回避を優先し、必要なら並列を 10 に落とす。連続エラー時のバックオフ・中断を踏襲。
- **resume**: delta と同じ複合キー(`{事業所番号}_{サービスコード}`)で `progress` CSV に済みを記録、再開時スキップ。`--fresh` でリセット。
- **エラー/Bot 対策**: リトライ3回、404 は「財務なし」として正常記録(エラー計上しない)。連続タイムアウトで一時停止し時間を空けて再開。
- **マージ**: `merge_kihon_and_reload.py` に unei-delta の財務列マージを追加(delta と同じ left join)。財務列は毎回スクレイプ結果で上書き。

### 3.4 改修工数見積り

| 作業 | 内容 | 目安 |
|---|---|---|
| parse_unei 財務抽出の書き換え | href フォルダ名ベースに変更、ゲート撤廃 | 0.5 日 |
| `scrape_unei_delta.py` 新規 | delta 構造を複製し unei 用に。15並列/resume/404正常扱い | 1.0 日 |
| (任意)方式A プローブ実装 | PL プローブ→CF/BS 構築の高速 presence 判定 | 0.5 日 |
| merge パイプライン統合 | 財務列を merge に追加、Turso 再投入 | 0.5 日 |
| 1〜2県で検証 | 充足率・URL 有効性(200/404)・件数を実測 | 0.5 日 |
| 全国本実行 + 再集計 | 無人実行(4〜12h)+ aggregate 再実行 | 実装 0.5 日 + 計算数時間 |
| **合計** | | **約 2.5〜3 人日 + 計算時間** |

### 3.5 実行順序(推奨)

1. **確認クエリ**(§1.2): facilities の `会計種類` vs `財務DL` 充足件数を比較し、カバレッジ不足 or parse バグを確定(5分)。
2. parse_unei 財務抽出を href フォルダ名ベースに修正。
3. `scrape_unei_delta.py` を 1〜2 県(例: 13 東京)で試走 → 充足率と 200/404 分布を実測(真の到達可能件数の見積り)。
4. 問題なければ全国本実行。並行して kani/original も同時取得するか判断(賃金・行政処分・サービス提供地域の 0% 解消)。
5. merge → Turso 再投入 → aggregate 再集計 → E2E テスト。

---

## 付録: 参照ファイル

- `scripts/scrape_kaigo_full.py`(`parse_unei` 610-636 が財務DL、mode=full のみ 4ページ)
- `scripts/scrape_kaigo_nationwide.py`(kihon のみ、財務列なし=223k の骨格)
- `scripts/scrape_kihon_delta.py`(kihon 15並列、財務列なし)
- `scripts/merge_kihon_and_reload.py`(kaigo_fast + delta を Turso へ)
- `scripts/download_financial_pdfs_pilot.py`(決定論 URL で PDF 取得、既存パイロット)
- `data/financial_pilot/pilot_manifest.csv`(URL 形式の実証、5法人種別×PL/CF/BS)
- `claudedocs/research/gov_opendata_kaigo.md`(オープンデータ CSV 調査)
