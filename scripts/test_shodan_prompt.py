# -*- coding: utf-8 -*-
"""商談品質分析プロンプト テスト（お手本比較版）"""

import os
import re
import json
import requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv("config/.env")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# お手本商談（服部さん - データ殴打の見本）
MODEL_TRANSCRIPT_PATH = r"C:\Users\fuji1\Downloads\GMT20260202-094820_Recording.transcript.vtt"

# テスト対象（適当な商談を1件選ぶ）
TEST_TRANSCRIPT_DIR = Path(r"C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List\data\output\zoom_transcripts")

def parse_vtt(vtt_text):
    """VTTをプレーンテキストに変換"""
    lines = []
    for line in vtt_text.split('\n'):
        line = line.strip()
        if not line or line == 'WEBVTT':
            continue
        if re.match(r'^\d+$', line):
            continue
        if re.match(r'\d{2}:\d{2}:\d{2}', line):
            continue
        # 話者名: テキスト の形式
        if ':' in line:
            lines.append(line)
    return '\n'.join(lines)


def call_gemini(prompt, max_tokens=4096):
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={GEMINI_API_KEY}"
    resp = requests.post(url, json={
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"maxOutputTokens": max_tokens, "temperature": 0.3}
    }, timeout=120)
    data = resp.json()
    return data["candidates"][0]["content"]["parts"][0]["text"]


ANALYSIS_PROMPT = """# 指示
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
{model_transcript}

---

# 【評価対象商談】
{target_transcript}

---

# 評価基準

お手本と比較して、以下5項目を各20点満点で採点してください。

## 1. データ殴打（Data Shock）
- 基準：冒頭（序盤）で、そのエリア×職種の「競合求人数」「検索順位（圏外である事実）」などの客観的データを提示し、顧客の主観を壊せているか
- 「厳しいですね」等の感想ではなく、ファクトで顧客の主観を壊せているか
- **重要**: 序盤で必須なのは「競合求人数」「検索順位」など市場競争の厳しさを示すデータ。人口動態・年齢分布などの補足データはタイミング問わない（終盤でも可）
- 判定基準：
  - 20点：お手本同等以上。序盤で競合求人数・検索順位を提示
  - 15点：データ提示あり、但しお手本より弱い（競合数は出したが検索順位なし等）
  - 10点：一部データあり、但し序盤に競合求人数・検索順位がない
  - 5点：抽象的な市場説明のみ（数字なし）
  - 0点：データ提示なし

## 2. 構造暴露（Structure Reveal）
- 人材紹介会社や大手媒体の「不都合な真実」（優先順位ロジック、中抜き構造）を説明できているか
- 判定基準：
  - 20点：紹介会社の仕組みを具体的に暴露
  - 10点：「高い」「ミスマッチ」など表面的言及のみ
  - 0点：言及なし

## 3. 定義転換（Reframing）
- Medicaの費用を「広告費」ではなく「労働力」「資産」として再定義できているか
- 判定基準：
  - 20点：明確に価値の再定義ができている
  - 10点：費用対効果の説明はあるが転換は弱い
  - 0点：単なる料金説明のみ

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

```json
{{
  "total_score": [合計点],
  "rank": "[S/A/B/C]",
  "data_shock": {{
    "score": [0-20],
    "symbol": "[○/△/×]",
    "comment": "[お手本と比較した寸評]",
    "evidence": "[該当する発言の引用または「なし」]"
  }},
  "structure_reveal": {{
    "score": [0-20],
    "symbol": "[○/△/×]",
    "comment": "[寸評]",
    "evidence": "[該当発言引用または「なし」]"
  }},
  "reframing": {{
    "score": [0-20],
    "symbol": "[○/△/×]",
    "comment": "[寸評]",
    "evidence": "[該当発言引用または「なし」]"
  }},
  "data_logic": {{
    "score": [0-20],
    "symbol": "[○/△/×]",
    "comment": "[寸評]",
    "evidence": "[該当発言引用または「なし」]"
  }},
  "closing": {{
    "score": [0-20],
    "symbol": "[○/△/×]",
    "comment": "[寸評]",
    "evidence": "[該当発言引用または「なし」]"
  }},
  "red_card_abstract": {{
    "detected": [true/false],
    "quote": "[抽象的逃げの発言引用または「なし」]",
    "feedback": "[改善指導]"
  }},
  "red_card_ignore": {{
    "detected": [true/false],
    "customer_quote": "[顧客の懸念発言]",
    "sales_reaction": "[営業の反応]",
    "feedback": "[改善指導]"
  }},
  "prescription": {{
    "action": "[具体的改善アクション]",
    "killer_phrase": "[明日から使えるフレーズ]"
  }}
}}
```

判定ランク基準：
- S（80-100点）：即戦力
- A（60-79点）：合格
- B（40-59点）：要指導
- C（0-39点）：再教育
"""


def main():
    import sys
    # UTF-8出力を強制
    sys.stdout.reconfigure(encoding='utf-8')

    # お手本読み込み
    print("【お手本商談読み込み】")
    with open(MODEL_TRANSCRIPT_PATH, 'r', encoding='utf-8') as f:
        model_vtt = f.read()
    model_text = parse_vtt(model_vtt)
    print(f"  文字数: {len(model_text)}")

    # テスト対象を複数メンバーから取得
    test_candidates = []
    for member_dir in ["k_sawada", "s_shinoki", "yo_ichiki"]:
        member_path = TEST_TRANSCRIPT_DIR / member_dir
        if member_path.exists():
            files = list(member_path.glob("2026-01-2*.txt"))
            # ロープレ等を除外
            files = [f for f in files if not any(kw in f.name for kw in ["ロープレ", "組み手", "パーソナル", "VS"])]
            if files:
                test_candidates.append(files[0])

    if not test_candidates:
        print("テスト対象ファイルが見つかりません")
        return

    # 全候補をテスト
    for test_file in test_candidates:
        print(f"\n{'='*60}")
        print(f"【評価対象商談】")
        print(f"  ファイル: {test_file.name}")
        run_analysis(model_text, test_file)
        print()


def run_analysis(model_text, test_file):
    """単一商談の分析を実行"""
    test_file = test_files[0]
    print(f"\n【評価対象商談】")
    print(f"  ファイル: {test_file.name}")

    with open(test_file, 'r', encoding='utf-8') as f:
        target_text = f.read()
    print(f"  文字数: {len(target_text)}")

    # トークン制限対策
    if len(model_text) > 30000:
        model_text = model_text[:30000] + "\n[...省略...]"
    if len(target_text) > 30000:
        target_text = target_text[:30000] + "\n[...省略...]"

    # プロンプト作成
    prompt = ANALYSIS_PROMPT.format(
        model_transcript=model_text,
        target_transcript=target_text
    )
    print(f"\n【プロンプト】")
    print(f"  総文字数: {len(prompt)}")

    # Gemini呼び出し
    print("\n【Gemini API呼び出し中...】")
    result = call_gemini(prompt, max_tokens=4096)

    print("\n" + "="*60)
    print("【分析結果】")
    print("="*60)
    print(result)

    # JSONパース試行
    try:
        # ```json ... ``` を除去
        json_match = re.search(r'```json\s*(.*?)\s*```', result, re.DOTALL)
        if json_match:
            json_str = json_match.group(1)
        else:
            json_str = result

        data = json.loads(json_str)
        print("\n" + "="*60)
        print("【パース成功 - 構造化データ】")
        print("="*60)
        print(f"総合スコア: {data.get('total_score')}/100")
        print(f"判定ランク: {data.get('rank')}")
        print()
        for key in ['data_shock', 'structure_reveal', 'reframing', 'data_logic', 'closing']:
            if key in data:
                item = data[key]
                print(f"{key}: {item.get('score')}/20 {item.get('symbol')} - {item.get('comment', '')[:50]}")
        print()
        if data.get('red_card_abstract', {}).get('detected'):
            print(f"🚨 抽象的逃げ検出: {data['red_card_abstract'].get('quote', '')[:50]}")
        if data.get('red_card_ignore', {}).get('detected'):
            print(f"🚨 スルー検出: {data['red_card_ignore'].get('customer_quote', '')[:50]}")
        print()
        print(f"処方箋: {data.get('prescription', {}).get('action', '')[:80]}")

    except json.JSONDecodeError as e:
        print(f"\nJSONパース失敗: {e}")


if __name__ == '__main__':
    main()
