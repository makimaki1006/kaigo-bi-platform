/**
 * 商談品質管理システム - メインスクリプト
 * Zoom録画 × Gemini分析 × Slack通知
 */

// ===========================================
// 設定シートから読み込む定数
// ===========================================
function getConfig() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const configSheet = ss.getSheetByName('設定');
  const data = configSheet.getDataRange().getValues();

  const config = {};
  for (let i = 1; i < data.length; i++) {
    if (data[i][0]) {
      config[data[i][0]] = data[i][1];
    }
  }
  return config;
}

// ===========================================
// Zoom API認証
// ===========================================
function getZoomAccessToken() {
  const config = getConfig();
  const accountId = config['ZOOM_ACCOUNT_ID'];
  const clientId = config['ZOOM_CLIENT_ID'];
  const clientSecret = config['ZOOM_CLIENT_SECRET'];

  // キャッシュチェック
  const cache = CacheService.getScriptCache();
  const cachedToken = cache.get('zoom_access_token');
  if (cachedToken) {
    return cachedToken;
  }

  // Server-to-Server OAuth
  const url = 'https://zoom.us/oauth/token';
  const credentials = Utilities.base64Encode(clientId + ':' + clientSecret);

  const response = UrlFetchApp.fetch(url, {
    method: 'post',
    headers: {
      'Authorization': 'Basic ' + credentials,
      'Content-Type': 'application/x-www-form-urlencoded'
    },
    payload: {
      'grant_type': 'account_credentials',
      'account_id': accountId
    },
    muteHttpExceptions: true
  });

  const result = JSON.parse(response.getContentText());
  if (result.access_token) {
    // 50分間キャッシュ（有効期限1時間の余裕を持たせる）
    cache.put('zoom_access_token', result.access_token, 3000);
    return result.access_token;
  } else {
    throw new Error('Zoom認証失敗: ' + JSON.stringify(result));
  }
}

// ===========================================
// Zoom録画一覧取得
// ===========================================
function getZoomRecordings(userEmail, fromDate, toDate) {
  const token = getZoomAccessToken();

  const url = `https://api.zoom.us/v2/users/${encodeURIComponent(userEmail)}/recordings?from=${fromDate}&to=${toDate}&page_size=30`;

  const response = UrlFetchApp.fetch(url, {
    method: 'get',
    headers: {
      'Authorization': 'Bearer ' + token
    },
    muteHttpExceptions: true
  });

  const result = JSON.parse(response.getContentText());
  return result.meetings || [];
}

// ===========================================
// 文字起こしVTT取得
// ===========================================
function getTranscriptVtt(downloadUrl) {
  const token = getZoomAccessToken();

  // download_urlにはトークンを付与
  const url = downloadUrl + '?access_token=' + token;

  const response = UrlFetchApp.fetch(url, {
    method: 'get',
    muteHttpExceptions: true
  });

  if (response.getResponseCode() === 200) {
    return response.getContentText();
  }
  return null;
}

// ===========================================
// VTTパース（プレーンテキスト化）
// ===========================================
function parseVtt(vttText) {
  const lines = vttText.split('\n');
  const result = [];

  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed === 'WEBVTT') continue;
    if (/^\d+$/.test(trimmed)) continue;
    if (/^\d{2}:\d{2}:\d{2}/.test(trimmed)) continue;
    if (trimmed.includes(':')) {
      result.push(trimmed);
    }
  }

  return result.join('\n');
}

// ===========================================
// Gemini API呼び出し
// ===========================================
function callGemini(prompt) {
  const config = getConfig();
  const apiKey = config['GEMINI_API_KEY'];

  const url = `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key=${apiKey}`;

  const payload = {
    contents: [{
      parts: [{ text: prompt }]
    }],
    generationConfig: {
      maxOutputTokens: 4096,
      temperature: 0.3
    }
  };

  // リトライロジック（最大3回）
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const response = UrlFetchApp.fetch(url, {
        method: 'post',
        contentType: 'application/json',
        payload: JSON.stringify(payload),
        muteHttpExceptions: true
      });

      const result = JSON.parse(response.getContentText());
      if (result.candidates && result.candidates[0]) {
        return result.candidates[0].content.parts[0].text;
      }

      // エラーの場合は待機してリトライ
      Utilities.sleep(Math.pow(2, attempt) * 1000);
    } catch (e) {
      if (attempt === 3) throw e;
      Utilities.sleep(Math.pow(2, attempt) * 1000);
    }
  }

  return null;
}

// ===========================================
// 分析プロンプト
// ===========================================
function getAnalysisPrompt(modelTranscript, targetTranscript) {
  return `# 指示
あなたは採用支援サービス「Medica（メディカ）」の教育責任者兼、セールスイネイブルメント担当です。

【お手本商談】と【評価対象商談】を比較し、評価対象がお手本のレベルに達しているか厳しく採点してください。

---

# 【お手本商談】（服部のデータ殴打・商品訴求の見本）

この商談では以下のデータ殴打が実践されています：
- 市場データ：求職者減少×採用サービス1000超の現状説明
- 媒体比較：Indeed vs カルケルの訪問数・滞在時間比較
- 競合数：「生野区×介護職で5000件の求人が出てる」と具体的数字を提示
- 競合調査レポート：エリアの給与分布・求人数ランキングを可視化
- ターゲットデータ：年齢層別の転職者分布（50代が最多という事実）
- 流入流出データ：エリアへの人口移動データ

【お手本の文字起こし】
${modelTranscript}

---

# 【評価対象商談】
${targetTranscript}

---

# 評価基準

お手本と比較して、以下5項目を各20点満点で採点してください。

## 1. データ殴打（Data Shock）
以下の3つの訴求ポイントを評価する：
  ①市場求人数（序盤推奨）：エリア×職種の求人数を提示し「競争が激しい」と認識させる
  ②競合調査の必要性（中盤〜）：他社求人との比較・差別化が必要であることを合意する
  ③人口調査（タイミング問わず）：具体的なターゲット層（年齢・属性）への言及

- 判定基準：
  - 20点：①②③すべて実施
  - 15点：3つのうち2つ実施、または3つあるが深掘りが弱い
  - 10点：3つのうち1つのみ実施
  - 5点：データへの言及はあるが数字なし・抽象的
  - 0点：データ訴求なし

- 評価時の注意：
  - ①市場求人数は序盤での提示が望ましい
  - ②競合調査は「導入後の具体策」ではなく「差別化の必要性を認識させる」ことがゴール
  - ③人口調査はタイミング問わず評価対象

## 2. 構造暴露（Structure Reveal）
- 人材紹介会社や大手媒体の「不都合な真実」（優先順位ロジック、中抜き構造）を説明できているか
- 判定基準：
  - 20点：紹介会社の仕組みを具体的に暴露
  - 10点：「高い」「ミスマッチ」など表面的言及のみ
  - 0点：言及なし

## 3. 定義転換（Reframing）
- Medicaの費用を「広告費」ではなく「労働力」「資産」「プロの人事チーム」として再定義できているか
- 再定義の例：「プロの人事担当を一人雇ったような」「媒体屋ではなく採用パートナー」「広告費ではなく人件費」

- 判定基準（顧客の反応・理解度で判定）：
  - 20点：ニーズと紐づけて再定義し、顧客から理解・合意を引き出した
    （例：「人手不足で困っている」→「だからこそプロの人事チームが必要」→顧客「なるほど」）
  - 15点：顧客から理解を引き出したが、顧客の具体的ニーズとの紐づけが弱い
    （例：再定義を説明→顧客「そうなんですね」と反応あり）
  - 10点：再定義の発言はあるが、顧客からの理解確認なし（ただ言っただけ）
    （例：「プロの人事チームです」と言ったが顧客の反応を確認していない）
  - 5点：費用対効果の説明はあるが、価値の再定義になっていない
  - 0点：単なる料金説明のみ、または言及なし

## 4. 戦略提案（Data Logic）
- エリアの人口動態、流入経路、競合施設の実名など具体的根拠で提案できているか
- 判定基準：
  - 20点：固有名詞・データに基づく具体的提案
  - 10点：一般論的な提案
  - 0点：「頑張ります」等の精神論のみ

## 5. クロージング（Closing Authority）
- 「今決める理由」を作れているか（機会損失、特典、期限）
- 判定基準：
  - 20点：デッドライン設定あり、決断を促している
  - 10点：検討促進はあるが弱い
  - 0点：「検討お願いします」で終了

---

# 出力形式

以下のJSON形式で出力してください。JSONのみを出力し、他のテキストは不要です。

\`\`\`json
{
  "total_score": [合計点],
  "rank": "[S/A/B/C]",
  "data_shock": {
    "score": [0-20],
    "symbol": "[○/△/×]",
    "comment": "[お手本と比較した寸評]",
    "evidence": "[該当する発言の引用または「なし」]"
  },
  "structure_reveal": {
    "score": [0-20],
    "symbol": "[○/△/×]",
    "comment": "[寸評]",
    "evidence": "[該当発言引用または「なし」]"
  },
  "reframing": {
    "score": [0-20],
    "symbol": "[○/△/×]",
    "comment": "[寸評]",
    "evidence": "[該当発言引用または「なし」]"
  },
  "data_logic": {
    "score": [0-20],
    "symbol": "[○/△/×]",
    "comment": "[寸評]",
    "evidence": "[該当発言引用または「なし」]"
  },
  "closing": {
    "score": [0-20],
    "symbol": "[○/△/×]",
    "comment": "[寸評]",
    "evidence": "[該当発言引用または「なし」]"
  },
  "good_points": {
    "summary": "[この商談で特に良かった点の総括（1-2文）]",
    "details": [
      {
        "category": "[データ殴打/構造暴露/定義転換/戦略提案/クロージング/顧客対応/その他]",
        "point": "[良かったポイントの説明]",
        "quote": "[該当する発言の引用]"
      }
    ]
  },
  "red_card_abstract": {
    "detected": [true/false],
    "quote": "[抽象的逃げの発言引用または「なし」]",
    "feedback": "[改善指導]"
  },
  "red_card_ignore": {
    "detected": [true/false],
    "customer_quote": "[顧客の懸念発言]",
    "sales_reaction": "[営業の反応]",
    "feedback": "[改善指導]"
  },
  "prescription": {
    "action": "[具体的改善アクション]",
    "killer_phrase": "[明日から使えるフレーズ]"
  }
}
\`\`\`

判定ランク基準：
- S（80-100点）：即戦力
- A（60-79点）：合格
- B（40-59点）：要指導
- C（0-39点）：再教育`;
}

// ===========================================
// JSON解析
// ===========================================
function parseAnalysisResult(text) {
  // ```json ... ``` を除去
  let jsonStr = text;
  const match = text.match(/```json\s*([\s\S]*?)\s*```/);
  if (match) {
    jsonStr = match[1];
  }

  try {
    return JSON.parse(jsonStr);
  } catch (e) {
    Logger.log('JSONパース失敗: ' + e.message);
    return null;
  }
}

// ===========================================
// 除外キーワードチェック
// ===========================================
function shouldExclude(topic) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const excludeSheet = ss.getSheetByName('除外キーワード');
  if (!excludeSheet) return false;

  const keywords = excludeSheet.getDataRange().getValues()
    .slice(1)
    .map(row => row[0])
    .filter(k => k);

  for (const keyword of keywords) {
    if (topic.includes(keyword)) {
      return true;
    }
  }
  return false;
}

// ===========================================
// 処理済みチェック
// ===========================================
function isProcessed(meetingId) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const processedSheet = ss.getSheetByName('処理済み');
  const data = processedSheet.getDataRange().getValues();

  for (let i = 1; i < data.length; i++) {
    if (data[i][0] === meetingId) {
      return true;
    }
  }
  return false;
}

function markAsProcessed(meetingId, topic) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const processedSheet = ss.getSheetByName('処理済み');
  processedSheet.appendRow([meetingId, topic, new Date()]);
}

// ===========================================
// 結果をスプレッドシートに書き込み
// ===========================================
function writeResult(memberName, meeting, result) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const resultSheet = ss.getSheetByName('分析結果');

  // 顧客名を会議名から抽出（簡易的に）
  const customerName = meeting.topic || '';

  // レッドカード（抽象的逃げ）全文
  let redCardAbstractFull = '';
  if (result.red_card_abstract?.detected) {
    redCardAbstractFull = `【発言】${result.red_card_abstract.quote || ''}\n【改善指導】${result.red_card_abstract.feedback || ''}`;
  }

  // レッドカード（スルー）全文
  let redCardIgnoreFull = '';
  if (result.red_card_ignore?.detected) {
    redCardIgnoreFull = `【顧客発言】${result.red_card_ignore.customer_quote || ''}\n【営業反応】${result.red_card_ignore.sales_reaction || ''}\n【改善指導】${result.red_card_ignore.feedback || ''}`;
  }

  // 良かったポイント
  let goodPointsSummary = result.good_points?.summary || '';
  let goodPointsDetails = '';
  if (result.good_points?.details && result.good_points.details.length > 0) {
    goodPointsDetails = result.good_points.details.map(p =>
      `【${p.category}】${p.point}\n引用: ${p.quote}`
    ).join('\n\n');
  }

  const row = [
    new Date(),                                          // A: 分析日時
    memberName,                                          // B: メンバー名
    meeting.start_time,                                  // C: 商談日時
    customerName,                                        // D: 顧客名
    result.total_score || 0,                            // E: 総合スコア
    result.rank || 'C',                                 // F: 判定ランク
    result.data_shock?.score || 0,                      // G: データ殴打
    result.structure_reveal?.score || 0,                // H: 構造暴露
    result.reframing?.score || 0,                       // I: 定義転換
    result.data_logic?.score || 0,                      // J: 戦略提案
    result.closing?.score || 0,                         // K: クロージング
    redCardAbstractFull,                                // L: RC抽象的逃げ（全文）
    redCardIgnoreFull,                                  // M: RCスルー（全文）
    goodPointsSummary,                                  // N: 良かったポイント（総括）
    goodPointsDetails,                                  // O: 良かったポイント（詳細）
    result.prescription?.action || '',                  // P: 処方箋
    result.prescription?.killer_phrase || '',           // Q: キラーフレーズ
    JSON.stringify(result),                             // R: 詳細JSON
    meeting.id                                          // S: ミーティングID
  ];

  resultSheet.appendRow(row);
}

// ===========================================
// Slack通知
// ===========================================
function sendSlackNotification(memberName, meeting, result, slackMention) {
  const config = getConfig();
  const webhookUrl = config['SLACK_WEBHOOK_URL'];
  if (!webhookUrl) return;

  const rankEmoji = {
    'S': '🏆',
    'A': '✅',
    'B': '⚠️',
    'C': '🚨'
  };

  const blocks = [
    {
      type: 'header',
      text: {
        type: 'plain_text',
        text: '📊 商談品質レポート'
      }
    },
    {
      type: 'section',
      fields: [
        { type: 'mrkdwn', text: `*担当者:* ${memberName} ${slackMention || ''}` },
        { type: 'mrkdwn', text: `*顧客:* ${meeting.topic || '不明'}` },
        { type: 'mrkdwn', text: `*判定:* ${rankEmoji[result.rank] || ''} ${result.rank}` }
      ]
    },
    {
      type: 'section',
      text: {
        type: 'mrkdwn',
        text: `*5軸評価:*\n• データ殴打: ${result.data_shock?.symbol || '−'}\n• 構造暴露: ${result.structure_reveal?.symbol || '−'}\n• 定義転換: ${result.reframing?.symbol || '−'}\n• 戦略提案: ${result.data_logic?.symbol || '−'}\n• クロージング: ${result.closing?.symbol || '−'}`
      }
    }
  ];

  // 良かったポイント
  if (result.good_points?.summary) {
    let goodPointsText = `*✨ 良かったポイント:*\n${result.good_points.summary}`;
    if (result.good_points.details && result.good_points.details.length > 0) {
      const detailsText = result.good_points.details.slice(0, 3).map(p =>
        `• 【${p.category}】${p.point}`
      ).join('\n');
      goodPointsText += `\n${detailsText}`;
    }
    blocks.push({
      type: 'section',
      text: {
        type: 'mrkdwn',
        text: goodPointsText.substring(0, 2000)
      }
    });
  }

  // レッドカード（全文）
  const redCards = [];
  if (result.red_card_abstract?.detected) {
    redCards.push(`*【抽象的逃げ】*\n発言: "${result.red_card_abstract.quote || ''}"\n改善: ${result.red_card_abstract.feedback || ''}`);
  }
  if (result.red_card_ignore?.detected) {
    redCards.push(`*【スルー】*\n顧客: "${result.red_card_ignore.customer_quote || ''}"\n営業: "${result.red_card_ignore.sales_reaction || ''}"\n改善: ${result.red_card_ignore.feedback || ''}`);
  }

  if (redCards.length > 0) {
    blocks.push({
      type: 'section',
      text: {
        type: 'mrkdwn',
        text: `*🚨 レッドカード:*\n${redCards.join('\n\n').substring(0, 2000)}`
      }
    });
  }

  // 処方箋
  if (result.prescription?.action) {
    blocks.push({
      type: 'section',
      text: {
        type: 'mrkdwn',
        text: `*💊 処方箋:*\n${result.prescription.action.substring(0, 200)}`
      }
    });
  }

  const payload = { blocks: blocks };

  UrlFetchApp.fetch(webhookUrl, {
    method: 'post',
    contentType: 'application/json',
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  });
}

// ===========================================
// メイン処理
// ===========================================
function processRecordings() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const config = getConfig();

  // お手本商談を取得（設定シートから）
  const modelTranscript = config['MODEL_TRANSCRIPT'] || '';
  if (!modelTranscript) {
    Logger.log('お手本商談が設定されていません');
    return;
  }

  // 対象メンバー取得
  const memberSheet = ss.getSheetByName('対象メンバー');
  const members = memberSheet.getDataRange().getValues().slice(1);

  // 日付範囲（過去24時間）
  const now = new Date();
  const yesterday = new Date(now.getTime() - 24 * 60 * 60 * 1000);
  const fromDate = Utilities.formatDate(yesterday, 'Asia/Tokyo', 'yyyy-MM-dd');
  const toDate = Utilities.formatDate(now, 'Asia/Tokyo', 'yyyy-MM-dd');

  let processedCount = 0;
  const maxPerRun = 10; // 実行時間制限対策

  for (const member of members) {
    const [memberName, email, slackMention, isActive] = member;
    if (!isActive || !email) continue;

    Logger.log(`処理中: ${memberName} (${email})`);

    try {
      const recordings = getZoomRecordings(email, fromDate, toDate);

      for (const meeting of recordings) {
        if (processedCount >= maxPerRun) {
          Logger.log('処理上限に達しました');
          return;
        }

        // 処理済みチェック
        if (isProcessed(meeting.id)) {
          continue;
        }

        // 除外チェック
        if (shouldExclude(meeting.topic || '')) {
          Logger.log(`除外: ${meeting.topic}`);
          markAsProcessed(meeting.id, meeting.topic);
          continue;
        }

        // 文字起こしファイルを探す
        const transcriptFile = (meeting.recording_files || []).find(
          f => f.file_type === 'TRANSCRIPT'
        );

        if (!transcriptFile) {
          Logger.log(`文字起こしなし: ${meeting.topic}`);
          continue;
        }

        // VTT取得
        const vttText = getTranscriptVtt(transcriptFile.download_url);
        if (!vttText) {
          Logger.log(`VTT取得失敗: ${meeting.topic}`);
          continue;
        }

        // パース
        const transcript = parseVtt(vttText);
        if (transcript.length < 500) {
          Logger.log(`文字起こしが短すぎます: ${meeting.topic}`);
          markAsProcessed(meeting.id, meeting.topic);
          continue;
        }

        // トークン制限対策（30000文字に制限）
        const trimmedModel = modelTranscript.substring(0, 30000);
        const trimmedTarget = transcript.substring(0, 30000);

        // Gemini分析
        Logger.log(`分析中: ${meeting.topic}`);
        const prompt = getAnalysisPrompt(trimmedModel, trimmedTarget);
        const response = callGemini(prompt);

        if (!response) {
          Logger.log(`Gemini応答なし: ${meeting.topic}`);
          continue;
        }

        // 結果パース
        const result = parseAnalysisResult(response);
        if (!result) {
          Logger.log(`結果パース失敗: ${meeting.topic}`);
          continue;
        }

        // 書き込み
        writeResult(memberName, meeting, result);

        // 処理済みマーク
        markAsProcessed(meeting.id, meeting.topic);

        // Slack通知
        sendSlackNotification(memberName, meeting, result, slackMention);

        processedCount++;
        Logger.log(`完了: ${meeting.topic} - ${result.total_score}/100 (${result.rank})`);

        // API制限対策
        Utilities.sleep(2000);
      }
    } catch (e) {
      Logger.log(`エラー (${memberName}): ${e.message}`);
    }
  }

  Logger.log(`処理完了: ${processedCount}件`);
}

// ===========================================
// トリガー設定
// ===========================================
function createHourlyTrigger() {
  // 既存トリガー削除
  const triggers = ScriptApp.getProjectTriggers();
  for (const trigger of triggers) {
    if (trigger.getHandlerFunction() === 'processRecordings') {
      ScriptApp.deleteTrigger(trigger);
    }
  }

  // 1時間毎トリガー作成
  ScriptApp.newTrigger('processRecordings')
    .timeBased()
    .everyHours(1)
    .create();

  Logger.log('1時間毎トリガーを設定しました');
}

// ===========================================
// 手動テスト用
// ===========================================
function testSingleMeeting() {
  // テスト用：特定のメンバーの最新録画を分析
  const config = getConfig();
  const testEmail = config['TEST_EMAIL'] || '';

  if (!testEmail) {
    Logger.log('TEST_EMAILが設定されていません');
    return;
  }

  const now = new Date();
  const weekAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
  const fromDate = Utilities.formatDate(weekAgo, 'Asia/Tokyo', 'yyyy-MM-dd');
  const toDate = Utilities.formatDate(now, 'Asia/Tokyo', 'yyyy-MM-dd');

  const recordings = getZoomRecordings(testEmail, fromDate, toDate);
  Logger.log(`録画件数: ${recordings.length}`);

  if (recordings.length > 0) {
    Logger.log('最新録画: ' + JSON.stringify(recordings[0], null, 2));
  }
}

// ===========================================
// デバッグ用：録画取得の詳細確認（拡張版）
// ===========================================
function debugZoomRecordings() {
  const config = getConfig();
  const testEmail = config['TEST_EMAIL'] || '';

  Logger.log('╔════════════════════════════════════════════════════════════╗');
  Logger.log('║            詳細デバッグ - Zoom録画・文字起こし確認            ║');
  Logger.log('╚════════════════════════════════════════════════════════════╝');
  Logger.log('');
  Logger.log('【設定情報】');
  Logger.log('  TEST_EMAIL: ' + testEmail);

  if (!testEmail) {
    Logger.log('❌ エラー: TEST_EMAILが設定されていません');
    return;
  }

  // 過去30日間で検索
  const now = new Date();
  const monthAgo = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);
  const fromDate = Utilities.formatDate(monthAgo, 'Asia/Tokyo', 'yyyy-MM-dd');
  const toDate = Utilities.formatDate(now, 'Asia/Tokyo', 'yyyy-MM-dd');

  Logger.log('  検索期間: ' + fromDate + ' 〜 ' + toDate);
  Logger.log('');

  try {
    // Step 1: トークン取得
    Logger.log('【Step 1: Zoom認証】');
    const token = getZoomAccessToken();
    Logger.log('  ✅ トークン取得成功');
    Logger.log('');

    // Step 2: 録画一覧取得
    Logger.log('【Step 2: 録画一覧取得】');
    const url = `https://api.zoom.us/v2/users/${encodeURIComponent(testEmail)}/recordings?from=${fromDate}&to=${toDate}&page_size=30`;
    Logger.log('  API URL: ' + url);

    const response = UrlFetchApp.fetch(url, {
      method: 'get',
      headers: {
        'Authorization': 'Bearer ' + token
      },
      muteHttpExceptions: true
    });

    const statusCode = response.getResponseCode();
    Logger.log('  HTTPステータス: ' + statusCode);

    if (statusCode !== 200) {
      Logger.log('  ❌ APIエラー: ' + response.getContentText().substring(0, 500));
      return;
    }

    const data = JSON.parse(response.getContentText());
    const meetingCount = data.meetings ? data.meetings.length : 0;
    Logger.log('  ✅ 録画件数: ' + meetingCount + '件');
    Logger.log('');

    if (meetingCount === 0) {
      Logger.log('⚠️ 録画が見つかりません。以下を確認してください：');
      Logger.log('  - TEST_EMAILが正しいか');
      Logger.log('  - 指定期間内に録画があるか');
      Logger.log('  - Zoomの録画設定がONになっているか');
      return;
    }

    // Step 3: 各録画の詳細
    Logger.log('【Step 3: 録画詳細分析】');
    Logger.log('');

    data.meetings.forEach((meeting, i) => {
      Logger.log(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);
      Logger.log(`録画 #${i + 1}`);
      Logger.log(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);
      Logger.log(`  会議名: ${meeting.topic || '(なし)'}`);
      Logger.log(`  会議ID: ${meeting.id}`);
      Logger.log(`  UUID: ${meeting.uuid}`);
      Logger.log(`  開始日時: ${meeting.start_time}`);
      Logger.log(`  所要時間: ${meeting.duration}分`);
      Logger.log(`  ホスト: ${meeting.host_email}`);
      Logger.log(`  合計サイズ: ${meeting.total_size ? (meeting.total_size / 1024 / 1024).toFixed(2) + ' MB' : '不明'}`);
      Logger.log('');

      // recording_files の詳細
      const files = meeting.recording_files || [];
      Logger.log(`  📁 添付ファイル数: ${files.length}`);

      if (files.length === 0) {
        Logger.log('  ⚠️ recording_filesが空です！');
      } else {
        Logger.log('  ファイル一覧:');
        files.forEach((file, j) => {
          Logger.log(`    [${j + 1}] file_type: "${file.file_type}"`);
          Logger.log(`        recording_type: "${file.recording_type || '(なし)'}"`);
          Logger.log(`        status: "${file.status || '(なし)'}"`);
          Logger.log(`        file_size: ${file.file_size ? (file.file_size / 1024).toFixed(1) + ' KB' : '不明'}`);
          Logger.log(`        file_extension: "${file.file_extension || '(なし)'}"`);
          Logger.log(`        download_url: ${file.download_url ? '✅ あり' : '❌ なし'}`);
          Logger.log('');
        });
      }

      // 文字起こしファイルの有無を判定
      const transcriptFile = files.find(f => f.file_type === 'TRANSCRIPT');
      const audioTranscript = files.find(f => f.file_type === 'AUDIO_TRANSCRIPT');
      const closedCaption = files.find(f => f.file_type === 'CC' || f.file_type === 'CLOSED_CAPTION');
      const vttFile = files.find(f => f.file_extension === 'VTT');

      Logger.log('  📝 文字起こし判定:');
      Logger.log(`    TRANSCRIPT: ${transcriptFile ? '✅ 発見' : '❌ なし'}`);
      Logger.log(`    AUDIO_TRANSCRIPT: ${audioTranscript ? '✅ 発見' : '❌ なし'}`);
      Logger.log(`    CC/CLOSED_CAPTION: ${closedCaption ? '✅ 発見' : '❌ なし'}`);
      Logger.log(`    VTTファイル: ${vttFile ? '✅ 発見' : '❌ なし'}`);

      // 全てのfile_typeを一覧表示
      const allFileTypes = files.map(f => f.file_type).filter(Boolean);
      Logger.log(`    全file_type一覧: [${allFileTypes.join(', ')}]`);

      if (!transcriptFile && !audioTranscript && !closedCaption && !vttFile) {
        Logger.log('');
        Logger.log('  ⚠️ 文字起こしが見つかりません！考えられる原因：');
        Logger.log('    1. Zoom設定で「クラウド録画の文字起こし」がOFFになっている');
        Logger.log('    2. 録画後、文字起こし生成中（数時間かかる場合あり）');
        Logger.log('    3. 会議の言語設定が日本語になっていない');
      }

      Logger.log('');
    });

    // Step 4: サマリー
    Logger.log('【Step 4: サマリー】');
    const withTranscript = data.meetings.filter(m =>
      (m.recording_files || []).some(f =>
        f.file_type === 'TRANSCRIPT' ||
        f.file_type === 'AUDIO_TRANSCRIPT' ||
        f.file_type === 'CC' ||
        f.file_extension === 'VTT'
      )
    ).length;

    Logger.log(`  総録画数: ${meetingCount}`);
    Logger.log(`  文字起こしあり: ${withTranscript}`);
    Logger.log(`  文字起こしなし: ${meetingCount - withTranscript}`);
    Logger.log('');

    if (withTranscript === 0) {
      Logger.log('🚨 警告: すべての録画に文字起こしがありません！');
      Logger.log('');
      Logger.log('【対処法】');
      Logger.log('1. Zoomの設定を確認:');
      Logger.log('   - Zoom Webポータル → 設定 → 録画');
      Logger.log('   - 「クラウド録画の自動文字起こし」をONにする');
      Logger.log('');
      Logger.log('2. 録画の言語設定を確認:');
      Logger.log('   - 日本語の文字起こしが有効か確認');
      Logger.log('');
      Logger.log('3. 時間を置いて再確認:');
      Logger.log('   - 文字起こし生成には数時間かかる場合があります');
    }

  } catch (e) {
    Logger.log('❌ エラー発生: ' + e.message);
    Logger.log('スタックトレース: ' + e.stack);
  }

  Logger.log('');
  Logger.log('╔════════════════════════════════════════════════════════════╗');
  Logger.log('║                      デバッグ終了                           ║');
  Logger.log('╚════════════════════════════════════════════════════════════╝');
}

// ===========================================
// デバッグ用：特定の録画の文字起こし取得テスト
// ===========================================
function debugTranscriptDownload() {
  const config = getConfig();
  const testEmail = config['TEST_EMAIL'] || '';

  Logger.log('=== 文字起こしダウンロードテスト ===');

  if (!testEmail) {
    Logger.log('❌ TEST_EMAILが設定されていません');
    return;
  }

  const now = new Date();
  const monthAgo = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);
  const fromDate = Utilities.formatDate(monthAgo, 'Asia/Tokyo', 'yyyy-MM-dd');
  const toDate = Utilities.formatDate(now, 'Asia/Tokyo', 'yyyy-MM-dd');

  try {
    const recordings = getZoomRecordings(testEmail, fromDate, toDate);
    Logger.log('録画件数: ' + recordings.length);

    // 文字起こしのある録画を探す
    for (const meeting of recordings) {
      const files = meeting.recording_files || [];

      // 様々なファイルタイプを試す
      const transcriptTypes = ['TRANSCRIPT', 'AUDIO_TRANSCRIPT', 'CC', 'CLOSED_CAPTION'];

      for (const fileType of transcriptTypes) {
        const file = files.find(f => f.file_type === fileType);
        if (file && file.download_url) {
          Logger.log('');
          Logger.log(`会議: ${meeting.topic}`);
          Logger.log(`ファイルタイプ: ${fileType}`);
          Logger.log(`ダウンロードURL: ${file.download_url.substring(0, 100)}...`);

          // ダウンロード試行
          Logger.log('ダウンロード試行中...');
          const content = getTranscriptVtt(file.download_url);

          if (content) {
            Logger.log('✅ ダウンロード成功！');
            Logger.log('コンテンツ長: ' + content.length + '文字');
            Logger.log('先頭100文字: ' + content.substring(0, 100));

            // パース試行
            const parsed = parseVtt(content);
            Logger.log('パース後の長さ: ' + parsed.length + '文字');
            if (parsed.length > 0) {
              Logger.log('パース後の先頭100文字: ' + parsed.substring(0, 100));
            }
            return; // 1件成功したら終了
          } else {
            Logger.log('❌ ダウンロード失敗');
          }
        }
      }

      // VTT拡張子のファイルも試す
      const vttFile = files.find(f => f.file_extension === 'VTT');
      if (vttFile && vttFile.download_url) {
        Logger.log('');
        Logger.log(`会議: ${meeting.topic}`);
        Logger.log(`ファイル: VTT拡張子`);

        const content = getTranscriptVtt(vttFile.download_url);
        if (content) {
          Logger.log('✅ VTTダウンロード成功！');
          Logger.log('コンテンツ長: ' + content.length + '文字');
          return;
        }
      }
    }

    Logger.log('');
    Logger.log('⚠️ ダウンロード可能な文字起こしが見つかりませんでした');

  } catch (e) {
    Logger.log('❌ エラー: ' + e.message);
  }
}

// ===========================================
// デバッグ用：Zoomユーザー一覧取得（全件・ページネーション対応）
// ===========================================
function debugListZoomUsers() {
  Logger.log('=== Zoomユーザー一覧（全件取得） ===');

  try {
    const token = getZoomAccessToken();
    let allUsers = [];
    let nextPageToken = '';
    let pageCount = 0;

    do {
      pageCount++;
      let url = 'https://api.zoom.us/v2/users?page_size=300&status=active';
      if (nextPageToken) {
        url += '&next_page_token=' + nextPageToken;
      }

      Logger.log(`ページ ${pageCount} を取得中...`);

      const response = UrlFetchApp.fetch(url, {
        method: 'get',
        headers: {
          'Authorization': 'Bearer ' + token
        },
        muteHttpExceptions: true
      });

      const statusCode = response.getResponseCode();
      if (statusCode !== 200) {
        Logger.log('エラー: ' + response.getContentText().substring(0, 500));
        break;
      }

      const data = JSON.parse(response.getContentText());
      if (data.users) {
        allUsers = allUsers.concat(data.users);
      }

      nextPageToken = data.next_page_token || '';
      Logger.log(`  取得: ${data.users ? data.users.length : 0}件 (累計: ${allUsers.length}件)`);

    } while (nextPageToken);

    Logger.log('');
    Logger.log(`✅ 全ユーザー数: ${allUsers.length}件`);
    Logger.log('');
    Logger.log('--- ユーザー一覧 ---');

    allUsers.forEach((u, i) => {
      Logger.log(`${i+1}. ${u.email} (${u.first_name} ${u.last_name})`);
    });

    // 検索ヘルパー: 特定のキーワードでフィルタ
    Logger.log('');
    Logger.log('--- "hattori" を含むユーザー ---');
    const hattoriUsers = allUsers.filter(u =>
      u.email.toLowerCase().includes('hattori') ||
      (u.last_name && u.last_name.toLowerCase().includes('hattori')) ||
      (u.first_name && u.first_name.toLowerCase().includes('hattori'))
    );

    if (hattoriUsers.length > 0) {
      hattoriUsers.forEach(u => {
        Logger.log(`  → ${u.email} (${u.first_name} ${u.last_name})`);
      });
    } else {
      Logger.log('  見つかりませんでした');
    }

    // 服部で検索
    Logger.log('');
    Logger.log('--- "服部" を含むユーザー ---');
    const hattoriJpUsers = allUsers.filter(u =>
      (u.last_name && u.last_name.includes('服部')) ||
      (u.first_name && u.first_name.includes('服部'))
    );

    if (hattoriJpUsers.length > 0) {
      hattoriJpUsers.forEach(u => {
        Logger.log(`  → ${u.email} (${u.first_name} ${u.last_name})`);
      });
    } else {
      Logger.log('  見つかりませんでした');
    }

  } catch (e) {
    Logger.log('エラー: ' + e.message);
  }
}

// ===========================================
// デバッグ用：特定ユーザーを検索
// ===========================================
function debugSearchUser() {
  // 設定シートのTEST_EMAILまたはここで指定
  const searchKeyword = 'hattori'; // ← 検索したいキーワードに変更

  Logger.log(`=== ユーザー検索: "${searchKeyword}" ===`);

  try {
    const token = getZoomAccessToken();
    let allUsers = [];
    let nextPageToken = '';

    do {
      let url = 'https://api.zoom.us/v2/users?page_size=300&status=active';
      if (nextPageToken) {
        url += '&next_page_token=' + nextPageToken;
      }

      const response = UrlFetchApp.fetch(url, {
        method: 'get',
        headers: {
          'Authorization': 'Bearer ' + token
        },
        muteHttpExceptions: true
      });

      if (response.getResponseCode() !== 200) break;

      const data = JSON.parse(response.getContentText());
      if (data.users) {
        allUsers = allUsers.concat(data.users);
      }
      nextPageToken = data.next_page_token || '';
    } while (nextPageToken);

    Logger.log(`全ユーザー: ${allUsers.length}件`);

    const keyword = searchKeyword.toLowerCase();
    const matches = allUsers.filter(u =>
      u.email.toLowerCase().includes(keyword) ||
      (u.last_name && u.last_name.toLowerCase().includes(keyword)) ||
      (u.first_name && u.first_name.toLowerCase().includes(keyword))
    );

    Logger.log(`マッチ: ${matches.length}件`);
    matches.forEach(u => {
      Logger.log(`  ${u.email} (${u.first_name} ${u.last_name})`);
    });

  } catch (e) {
    Logger.log('エラー: ' + e.message);
  }
}
