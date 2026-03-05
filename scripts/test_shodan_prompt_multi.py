# -*- coding: utf-8 -*-
"""商談品質分析プロンプト 複数商談テスト"""

import os
import re
import json
import sys
import requests
from pathlib import Path
from dotenv import load_dotenv

# UTF-8出力を強制
sys.stdout.reconfigure(encoding='utf-8')

load_dotenv("config/.env")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# お手本商談（服部さん - データ殴打の見本）
MODEL_TRANSCRIPT_PATH = r"C:\Users\fuji1\Downloads\GMT20260202-094820_Recording.transcript.vtt"

# テスト対象ディレクトリ
TEST_TRANSCRIPT_DIR = Path(r"C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List\data\output\zoom_transcripts")

# 除外キーワード
EXCLUDE_KEYWORDS = ["ロープレ", "組み手", "パーソナル", "VS", "vs", "MTG", "定例", "研修"]


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


def analyze_meeting(model_text, test_file):
    """単一商談を分析"""
    with open(test_file, 'r', encoding='utf-8') as f:
        target_text = f.read()

    # トークン制限対策
    model_trimmed = model_text[:30000] if len(model_text) > 30000 else model_text
    target_trimmed = target_text[:30000] if len(target_text) > 30000 else target_text

    prompt = ANALYSIS_PROMPT.format(
        model_transcript=model_trimmed,
        target_transcript=target_trimmed
    )

    result = call_gemini(prompt, max_tokens=4096)

    # JSONパース
    json_match = re.search(r'```json\s*(.*?)\s*```', result, re.DOTALL)
    if json_match:
        json_str = json_match.group(1)
    else:
        json_str = result

    try:
        data = json.loads(json_str)
        return data
    except json.JSONDecodeError:
        return None


def main():
    # お手本読み込み
    print("=" * 70)
    print("【最終テスト: 30商談フル分析】")
    print("=" * 70)
    print()
    print("【お手本商談読み込み】")
    with open(MODEL_TRANSCRIPT_PATH, 'r', encoding='utf-8') as f:
        model_vtt = f.read()
    model_text = parse_vtt(model_vtt)
    print(f"  文字数: {len(model_text)}")

    # テスト対象を複数メンバーから取得（各メンバー最大6件、合計30件目標）
    test_files = []
    member_dirs = ["s_hattori", "y_fukabori", "k_sawada", "s_shinoki", "yo_ichiki"]

    for member_dir in member_dirs:
        member_path = TEST_TRANSCRIPT_DIR / member_dir
        if member_path.exists():
            # 2026-01の商談を取得
            files = list(member_path.glob("2026-01-*.txt"))
            # 除外キーワードでフィルタ
            files = [f for f in files if not any(kw in f.name for kw in EXCLUDE_KEYWORDS)]
            # 日付でソート（新しい順）
            files = sorted(files, key=lambda x: x.name, reverse=True)
            # 各メンバーから最大6件取得
            for f in files[:6]:
                test_files.append((member_dir, f))

    # 30件に制限
    test_files = test_files[:30]
    print(f"  分析対象: {len(test_files)}件")

    if not test_files:
        print("テスト対象ファイルが見つかりません")
        return

    # 結果格納
    results = []

    # 各商談を分析
    for member, test_file in test_files:
        print()
        print("=" * 70)
        print(f"【分析中】{member}")
        print(f"  ファイル: {test_file.name}")
        print("  Gemini API呼び出し中...")

        data = analyze_meeting(model_text, test_file)

        if data:
            results.append({
                "member": member,
                "file": test_file.name,
                "data": data
            })
            print(f"  → 完了: {data.get('total_score')}/100 ({data.get('rank')})")
        else:
            print(f"  → 分析失敗")

    # サマリー出力
    print()
    print("=" * 70)
    print("【テスト結果サマリー】")
    print("=" * 70)
    print()
    print(f"{'メンバー':<12} {'スコア':>8} {'ランク':>6} {'データ殴打':>10} {'構造暴露':>10} {'定義転換':>10} {'戦略提案':>10} {'クロージング':>12}")
    print("-" * 90)

    for r in results:
        d = r["data"]
        print(f"{r['member']:<12} {d.get('total_score', 0):>6}/100 {d.get('rank', '-'):>6} "
              f"{d.get('data_shock', {}).get('score', 0):>8}/20 "
              f"{d.get('structure_reveal', {}).get('score', 0):>8}/20 "
              f"{d.get('reframing', {}).get('score', 0):>8}/20 "
              f"{d.get('data_logic', {}).get('score', 0):>8}/20 "
              f"{d.get('closing', {}).get('score', 0):>10}/20")

    # 詳細出力
    print()
    print("=" * 70)
    print("【詳細結果】")
    print("=" * 70)

    for r in results:
        d = r["data"]
        print()
        print(f"■ {r['member']} - {r['file']}")
        print(f"  総合: {d.get('total_score')}/100 ({d.get('rank')})")
        print()

        for key, label in [
            ('data_shock', 'データ殴打'),
            ('structure_reveal', '構造暴露'),
            ('reframing', '定義転換'),
            ('data_logic', '戦略提案'),
            ('closing', 'クロージング')
        ]:
            item = d.get(key, {})
            print(f"  [{label}] {item.get('score', 0)}/20 {item.get('symbol', '-')}")
            comment = item.get('comment', '')
            if comment:
                print(f"    → {comment[:80]}")

        # レッドカード詳細
        print()
        rc_abstract = d.get('red_card_abstract', {})
        if rc_abstract.get('detected'):
            print(f"  🚨【レッドカード：抽象的逃げ】")
            quote = rc_abstract.get('quote', '')
            if quote and quote != 'なし':
                print(f"    発言: 「{quote[:100]}」")
            feedback = rc_abstract.get('feedback', '')
            if feedback:
                print(f"    指導: {feedback[:100]}")
        else:
            print(f"  ✅ 抽象的逃げ: 検出なし")

        rc_ignore = d.get('red_card_ignore', {})
        if rc_ignore.get('detected'):
            print(f"  🚨【レッドカード：スルー（無視）】")
            cust_quote = rc_ignore.get('customer_quote', '')
            if cust_quote and cust_quote != 'なし':
                print(f"    顧客発言: 「{cust_quote[:80]}」")
            sales_reaction = rc_ignore.get('sales_reaction', '')
            if sales_reaction and sales_reaction != 'なし':
                print(f"    営業反応: 「{sales_reaction[:80]}」")
            feedback = rc_ignore.get('feedback', '')
            if feedback:
                print(f"    指導: {feedback[:100]}")
        else:
            print(f"  ✅ スルー: 検出なし")

        # 処方箋
        print()
        prescription = d.get('prescription', {})
        print(f"  💊【処方箋】")
        action = prescription.get('action', '')
        if action:
            print(f"    改善アクション: {action[:120]}")
        killer = prescription.get('killer_phrase', '')
        if killer:
            print(f"    キラーフレーズ: 「{killer[:100]}」")

    # 統計サマリー
    print()
    print("=" * 70)
    print("【統計サマリー】")
    print("=" * 70)

    # メンバー別集計
    member_stats = {}
    for r in results:
        member = r["member"]
        if member not in member_stats:
            member_stats[member] = {"count": 0, "total": 0, "scores": {
                "data_shock": 0, "structure_reveal": 0, "reframing": 0,
                "data_logic": 0, "closing": 0
            }, "red_abstract": 0, "red_ignore": 0}
        d = r["data"]
        member_stats[member]["count"] += 1
        member_stats[member]["total"] += d.get("total_score", 0)
        for key in ["data_shock", "structure_reveal", "reframing", "data_logic", "closing"]:
            member_stats[member]["scores"][key] += d.get(key, {}).get("score", 0)
        if d.get("red_card_abstract", {}).get("detected"):
            member_stats[member]["red_abstract"] += 1
        if d.get("red_card_ignore", {}).get("detected"):
            member_stats[member]["red_ignore"] += 1

    print()
    print("【メンバー別平均スコア】")
    print(f"{'メンバー':<12} {'件数':>4} {'平均':>8} {'データ殴打':>10} {'構造暴露':>10} {'定義転換':>10} {'戦略提案':>10} {'クロージング':>12} {'RC抽象':>8} {'RCスルー':>8}")
    print("-" * 110)

    for member, stats in member_stats.items():
        cnt = stats["count"]
        avg = stats["total"] / cnt if cnt > 0 else 0
        print(f"{member:<12} {cnt:>4} {avg:>6.1f}/100 "
              f"{stats['scores']['data_shock']/cnt:>8.1f}/20 "
              f"{stats['scores']['structure_reveal']/cnt:>8.1f}/20 "
              f"{stats['scores']['reframing']/cnt:>8.1f}/20 "
              f"{stats['scores']['data_logic']/cnt:>8.1f}/20 "
              f"{stats['scores']['closing']/cnt:>10.1f}/20 "
              f"{stats['red_abstract']:>6}件 "
              f"{stats['red_ignore']:>6}件")

    # 全体統計
    print()
    total_count = len(results)
    if total_count > 0:
        avg_score = sum(r["data"]["total_score"] for r in results) / total_count
        rank_counts = {"S": 0, "A": 0, "B": 0, "C": 0}
        for r in results:
            rank = r["data"].get("rank", "C")
            if rank in rank_counts:
                rank_counts[rank] += 1

        total_red_abstract = sum(1 for r in results if r["data"].get("red_card_abstract", {}).get("detected"))
        total_red_ignore = sum(1 for r in results if r["data"].get("red_card_ignore", {}).get("detected"))

        print("【全体統計】")
        print(f"  分析件数: {total_count}件")
        print(f"  平均スコア: {avg_score:.1f}/100")
        print(f"  ランク分布: S={rank_counts['S']}件, A={rank_counts['A']}件, B={rank_counts['B']}件, C={rank_counts['C']}件")
        print(f"  レッドカード: 抽象的逃げ={total_red_abstract}件, スルー={total_red_ignore}件")

        # 5軸別平均
        print()
        print("【5軸別平均スコア】")
        for key, label in [
            ('data_shock', 'データ殴打'),
            ('structure_reveal', '構造暴露'),
            ('reframing', '定義転換'),
            ('data_logic', '戦略提案'),
            ('closing', 'クロージング')
        ]:
            avg = sum(r["data"].get(key, {}).get("score", 0) for r in results) / total_count
            print(f"  {label}: {avg:.1f}/20")


if __name__ == '__main__':
    main()
