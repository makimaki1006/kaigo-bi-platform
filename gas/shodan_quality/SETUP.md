# 商談品質管理システム - セットアップ手順

## 1. スプレッドシート作成

Google スプレッドシートで新規ファイルを作成し、以下のシートを用意してください。

### 1.1 「設定」シート

| A列（キー） | B列（値） |
|------------|----------|
| ZOOM_ACCOUNT_ID | [Zoom Account ID] |
| ZOOM_CLIENT_ID | [Zoom Client ID] |
| ZOOM_CLIENT_SECRET | [Zoom Client Secret] |
| GEMINI_API_KEY | [Gemini API Key] |
| SLACK_WEBHOOK_URL | [Slack Webhook URL] |
| MODEL_TRANSCRIPT | [お手本商談の文字起こし全文] |
| TEST_EMAIL | [テスト用Zoomメール] |

### 1.2 「対象メンバー」シート

| A列 | B列 | C列 | D列 |
|-----|-----|-----|-----|
| メンバー名 | Zoomメールアドレス | Slackメンション | 有効 |
| 服部 | s_hattori@f-a-c.co.jp | <@U12345678> | TRUE |
| 深堀 | y_fukabori@f-a-c.co.jp | <@U23456789> | TRUE |
| 澤田 | k_sawada@f-a-c.co.jp | <@U34567890> | TRUE |
| 篠木 | s_shinoki@f-a-c.co.jp | <@U45678901> | TRUE |
| 市来 | yo_ichiki@f-a-c.co.jp | <@U56789012> | TRUE |

### 1.3 「処理済み」シート

| A列 | B列 | C列 |
|-----|-----|-----|
| ミーティングID | 会議名 | 処理日時 |
| （自動追加される） | | |

### 1.4 「分析結果」シート

| 列 | 項目 |
|----|------|
| A | 分析日時 |
| B | メンバー名 |
| C | 商談日時 |
| D | 顧客名 |
| E | 総合スコア |
| F | 判定ランク |
| G | データ殴打 |
| H | 構造暴露 |
| I | 定義転換 |
| J | 戦略提案 |
| K | クロージング |
| L | RC抽象的逃げ |
| M | RCスルー |
| N | 処方箋 |
| O | 詳細JSON |
| P | ミーティングID |

### 1.5 「除外キーワード」シート

| A列 | B列 |
|-----|-----|
| キーワード | 備考 |
| ロープレ | 社内練習 |
| VS | ロープレ対戦 |
| MTG | 社内ミーティング |
| 定例 | 定例会議 |
| 研修 | 研修会議 |
| パーソナルミーティング | 個人ルーム |
| 1on1 | 1対1面談 |
| YMCX | 社内イベント |
| 組み手 | 新人研修 |
| 振り返り | 社内振り返り |

---

## 2. Zoom Server-to-Server OAuth App 作成

### 2.1 Zoom Marketplace にアクセス
https://marketplace.zoom.us/

### 2.2 App 作成
1. 「Develop」→「Build App」
2. 「Server-to-Server OAuth」を選択
3. App Name: `Medica商談分析`

### 2.3 必要なスコープ
```
recording:read:admin
user:read:admin
```

### 2.4 認証情報を取得
- Account ID
- Client ID
- Client Secret

→ スプレッドシートの「設定」シートに入力

---

## 3. Gemini API Key 取得

### 3.1 Google AI Studio にアクセス
https://aistudio.google.com/

### 3.2 API Key 作成
1. 「Get API Key」をクリック
2. 新しいプロジェクトを作成 or 既存を選択
3. API Key をコピー

→ スプレッドシートの「設定」シートに入力

---

## 4. Slack Webhook URL 取得

### 4.1 Slack App 作成
https://api.slack.com/apps

### 4.2 Incoming Webhooks 有効化
1. 「Incoming Webhooks」→「On」
2. 「Add New Webhook to Workspace」
3. 通知チャンネル（例: #medica-商談品質）を選択
4. Webhook URL をコピー

→ スプレッドシートの「設定」シートに入力

---

## 5. GAS プロジェクト設定

### 5.1 スクリプトエディタを開く
スプレッドシート → 拡張機能 → Apps Script

### 5.2 コードを貼り付け
`Code.gs` の内容をコピー＆ペースト

### 5.3 お手本商談の設定
1. お手本商談（服部さん）の文字起こしファイルを開く
2. VTTファイルの場合は、以下のPythonでプレーンテキスト化:
   ```python
   # parse_vtt.py
   import re
   with open('transcript.vtt', 'r', encoding='utf-8') as f:
       lines = []
       for line in f:
           line = line.strip()
           if not line or line == 'WEBVTT': continue
           if re.match(r'^\d+$', line): continue
           if re.match(r'\d{2}:\d{2}:\d{2}', line): continue
           if ':' in line:
               lines.append(line)
       print('\n'.join(lines))
   ```
3. 「設定」シートの `MODEL_TRANSCRIPT` に全文を貼り付け

### 5.4 テスト実行
1. `testSingleMeeting` 関数を選択
2. 「実行」をクリック
3. 初回は認証を許可

### 5.5 トリガー設定
1. `createHourlyTrigger` 関数を実行
2. 1時間毎に自動実行されるようになる

---

## 6. 動作確認チェックリスト

- [ ] Zoom API 認証が通る（testSingleMeetingで録画一覧が取得できる）
- [ ] Gemini API が動作する
- [ ] Slack 通知が届く
- [ ] 分析結果がスプレッドシートに書き込まれる
- [ ] 処理済みIDが記録される
- [ ] 除外キーワードが機能する

---

## 7. トラブルシューティング

### Zoom API エラー
- Account ID / Client ID / Secret を再確認
- スコープが正しく設定されているか確認
- Zoom Pro以上のライセンスが必要

### Gemini API エラー
- API Key が有効か確認
- 無料枠を超えていないか確認

### 文字起こしが取得できない
- Zoom設定で「クラウド録画の自動文字起こし」がONになっているか確認
- 録画後、文字起こし生成に数分〜数時間かかる場合あり

### GAS 実行時間エラー（6分超過）
- `maxPerRun` の値を下げる（5件など）
- トリガー間隔を短くする（30分毎など）

---

## 8. 運用

### 日次確認
- Slack通知を確認
- 低スコア（C判定）の商談をフォローアップ

### 週次確認
- スプレッドシートでメンバー別集計
- 共通の改善ポイントを抽出

### 月次確認
- 平均スコアの推移を確認
- ベストプラクティス商談を共有
