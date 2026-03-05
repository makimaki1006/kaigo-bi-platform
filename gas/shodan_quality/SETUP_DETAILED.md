# 商談品質管理システム - 詳細セットアップ手順

## Step 1: Googleスプレッドシート作成

### 1.1 新規スプレッドシート作成
1. https://sheets.google.com にアクセス
2. 「空白」で新規作成
3. ファイル名を「商談品質管理システム」に変更

### 1.2 シートをインポート
以下のCSVファイルを各シートにインポート：

```
gas/shodan_quality/templates/
├── 設定.csv
├── 対象メンバー.csv
├── 処理済み.csv
├── 分析結果.csv
└── 除外キーワード.csv
```

**インポート方法:**
1. シート名をダブルクリック → 「設定」に変更
2. ファイル → インポート → アップロード → 「設定.csv」
3. 「現在のシートに挿入」を選択
4. 残りのシートも同様に作成

---

## Step 2: Zoom Server-to-Server OAuth App 作成

### 2.1 Zoom Marketplace にアクセス
URL: https://marketplace.zoom.us/

### 2.2 App 作成手順
1. 右上の「Develop」→「Build App」をクリック
2. 「Server-to-Server OAuth」を選択して「Create」

### 2.3 App 情報入力
- **App Name**: `Medica商談分析`
- **Company Name**: `株式会社フォーラ`
- **Developer Name**: あなたの名前
- **Developer Email**: あなたのメール

### 2.4 スコープ設定
「Scopes」タブで以下を追加：
```
recording:read:admin    ← 録画読み取り
user:read:admin         ← ユーザー情報読み取り
```

**追加方法:**
1. 「+ Add Scopes」をクリック
2. 検索ボックスに「recording」と入力
3. 「recording:read:admin」にチェック
4. 同様に「user:read:admin」を追加
5. 「Done」をクリック

### 2.5 認証情報をコピー
「App Credentials」タブから：
- **Account ID**: コピー → スプレッドシート「設定」シートのB2に貼り付け
- **Client ID**: コピー → B3に貼り付け
- **Client Secret**: コピー → B4に貼り付け

### 2.6 App を有効化
1. 「Activation」タブに移動
2. 「Activate your app」をクリック

---

## Step 3: Gemini API Key 取得

### 3.1 Google AI Studio にアクセス
URL: https://aistudio.google.com/

### 3.2 API Key 作成
1. 左メニュー「Get API Key」をクリック
2. 「Create API key in new project」をクリック
3. API Key が表示される → コピー

### 3.3 スプレッドシートに設定
- コピーしたAPI Key → 「設定」シートのB5に貼り付け

---

## Step 4: Slack Webhook URL 取得

### 4.1 Slack App 作成
URL: https://api.slack.com/apps

1. 「Create New App」をクリック
2. 「From scratch」を選択
3. App Name: `商談品質Bot`
4. ワークスペースを選択 → 「Create App」

### 4.2 Incoming Webhooks 有効化
1. 左メニュー「Incoming Webhooks」をクリック
2. 「Activate Incoming Webhooks」を **On** に
3. 「Add New Webhook to Workspace」をクリック
4. 通知先チャンネルを選択（例: #medica-商談品質）
5. 「許可する」をクリック

### 4.3 Webhook URL をコピー
- 「Webhook URL」欄のURLをコピー
- 形式: `https://hooks.slack.com/services/T.../B.../xxx...`

### 4.4 スプレッドシートに設定
- コピーしたURL → 「設定」シートのB6に貼り付け

---

## Step 5: お手本商談テキスト設定

### 5.1 お手本テキストファイルを開く
```
data/output/model_transcript.txt
```

### 5.2 全文をコピー
- ファイルを開く
- Ctrl+A で全選択
- Ctrl+C でコピー

### 5.3 スプレッドシートに貼り付け
1. 「設定」シートのB7セル（MODEL_TRANSCRIPT）をクリック
2. Ctrl+V で貼り付け

**注意**: テキストが長いため、セルが縦に伸びます。これで正常です。

---

## Step 6: GAS プロジェクト設定

### 6.1 スクリプトエディタを開く
1. スプレッドシートを開いた状態で
2. メニュー「拡張機能」→「Apps Script」

### 6.2 コードを貼り付け
1. デフォルトの `コード.gs` の内容を全て削除
2. `gas/shodan_quality/Code.gs` の内容を全てコピー
3. エディタに貼り付け
4. Ctrl+S で保存

### 6.3 初回認証
1. 関数選択ドロップダウンで `testSingleMeeting` を選択
2. 「実行」ボタンをクリック
3. 「権限を確認」ダイアログが表示される
4. Googleアカウントでログイン
5. 「詳細」→「〇〇（安全ではないページ）に移動」
6. 「許可」をクリック

### 6.4 テスト実行
1. `testSingleMeeting` を実行
2. 「実行ログ」を確認
3. 「録画件数: X」と表示されればZoom API接続成功

---

## Step 7: トリガー設定

### 7.1 時間トリガー作成
1. 関数選択で `createHourlyTrigger` を選択
2. 「実行」をクリック
3. ログに「1時間毎トリガーを設定しました」と表示される

### 7.2 トリガー確認
1. 左メニュー「トリガー」（時計アイコン）をクリック
2. `processRecordings` が1時間毎で設定されていることを確認

---

## Step 8: 動作確認

### 8.1 手動実行テスト
1. 関数選択で `processRecordings` を選択
2. 「実行」をクリック
3. 実行ログを確認

### 8.2 確認ポイント
- [ ] 「処理中: 〇〇」とメンバー名が表示される
- [ ] 「分析中: 〇〇」と商談名が表示される
- [ ] 「完了: 〇〇 - XX/100 (Y)」と結果が表示される

### 8.3 結果確認
1. 「分析結果」シートを確認 → 新しい行が追加されている
2. 「処理済み」シートを確認 → ミーティングIDが記録されている
3. Slackチャンネルを確認 → 通知が届いている

---

## トラブルシューティング

### エラー: "Zoom認証失敗"
- Account ID / Client ID / Secret を再確認
- Zoom App が「Activated」状態か確認
- スコープが正しく設定されているか確認

### エラー: "お手本商談が設定されていません"
- 「設定」シートのMODEL_TRANSCRIPTに値が入っているか確認
- セルB7が空になっていないか確認

### エラー: "文字起こしなし"
- Zoom設定で「自動文字起こし」がONになっているか確認
- 録画後、文字起こし生成に時間がかかる場合あり（数分〜数時間）

### Slack通知が届かない
- Webhook URLが正しいか確認
- Slack Appがチャンネルに招待されているか確認

---

## 完了チェックリスト

- [ ] Googleスプレッドシート作成完了
- [ ] 5つのシート（設定、対象メンバー、処理済み、分析結果、除外キーワード）作成完了
- [ ] Zoom OAuth App作成・有効化完了
- [ ] Zoom認証情報をスプレッドシートに設定完了
- [ ] Gemini API Key取得・設定完了
- [ ] Slack Webhook URL取得・設定完了
- [ ] お手本テキスト設定完了
- [ ] GASコード貼り付け完了
- [ ] 初回認証完了
- [ ] テスト実行成功
- [ ] トリガー設定完了
- [ ] Slack通知確認完了

全てにチェックが入れば、セットアップ完了です！
