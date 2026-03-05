# -*- coding: utf-8 -*-
"""商談品質分析プロンプト 最終テスト（15商談）"""

import os
import re
import json
import sys
import random
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
EXCLUDE_KEYWORDS = ["ロープレ", "組み手", "パーソナル", "VS", "vs", "MTG", "定例", "研修", "YMCX", "1on1"]


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
    }, timeout=180)
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


def get_member_files(member_dir, count=5):
    """指定メンバーの商談ファイルを取得"""
    member_path = TEST_TRANSCRIPT_DIR / member_dir
    if not member_path.exists():
        return []

    files = list(member_path.glob("2026-01-*.txt"))
    # 除外キーワードでフィルタ
    files = [f for f in files if not any(kw in f.name for kw in EXCLUDE_KEYWORDS)]
    # 日付でソート（新しい順）
    files.sort(key=lambda x: x.name, reverse=True)
    return files[:count]


def get_random_files(exclude_members, count=5):
    """ランダムなメンバーの商談ファイルを取得"""
    all_members = [d.name for d in TEST_TRANSCRIPT_DIR.iterdir() if d.is_dir()]
    # 除外メンバー以外
    other_members = [m for m in all_members if m not in exclude_members]

    all_files = []
    for member in other_members:
        member_path = TEST_TRANSCRIPT_DIR / member
        files = list(member_path.glob("2026-01-*.txt"))
        files = [f for f in files if not any(kw in f.name for kw in EXCLUDE_KEYWORDS)]
        for f in files:
            all_files.append((member, f))

    # ランダムに選択
    random.shuffle(all_files)
    return all_files[:count]


def main():
    # お手本読み込み
    print("=" * 80)
    print("【商談品質分析 最終テスト】")
    print("=" * 80)
    print()
    print("【お手本商談読み込み】")
    with open(MODEL_TRANSCRIPT_PATH, 'r', encoding='utf-8') as f:
        model_vtt = f.read()
    model_text = parse_vtt(model_vtt)
    print(f"  文字数: {len(model_text)}")

    # テスト対象を収集
    test_files = []

    # 服部さん 5件
    hattori_files = get_member_files("s_hattori", 5)
    for f in hattori_files:
        test_files.append(("s_hattori", f))
    print(f"\n服部さん: {len(hattori_files)}件")

    # 深堀さん 5件
    fukabori_files = get_member_files("y_fukabori", 5)
    for f in fukabori_files:
        test_files.append(("y_fukabori", f))
    print(f"深堀さん: {len(fukabori_files)}件")

    # ランダム 5件
    random_files = get_random_files(["s_hattori", "y_fukabori"], 5)
    test_files.extend(random_files)
    print(f"ランダム: {len(random_files)}件")

    print(f"\n合計: {len(test_files)}件")

    # 結果格納
    results = []

    # 各商談を分析
    for i, (member, test_file) in enumerate(test_files, 1):
        print()
        print(f"[{i}/{len(test_files)}] {member} - {test_file.name[:40]}...")

        try:
            data = analyze_meeting(model_text, test_file)
            if data:
                results.append({
                    "member": member,
                    "file": test_file.name,
                    "data": data
                })
                print(f"  → {data.get('total_score')}/100 ({data.get('rank')})")
            else:
                print(f"  → 分析失敗（JSONパースエラー）")
        except Exception as e:
            print(f"  → エラー: {e}")

    # サマリー出力
    print()
    print("=" * 80)
    print("【テスト結果サマリー】")
    print("=" * 80)

    # メンバー別に整理
    print("\n■ 服部さん（s_hattori）")
    print("-" * 80)
    print(f"{'商談名':<45} {'スコア':>8} {'DS':>5} {'SR':>5} {'RF':>5} {'DL':>5} {'CL':>5}")
    print("-" * 80)
    for r in results:
        if r["member"] == "s_hattori":
            d = r["data"]
            name = r["file"][:42] + "..." if len(r["file"]) > 45 else r["file"]
            print(f"{name:<45} {d.get('total_score', 0):>6}/100 "
                  f"{d.get('data_shock', {}).get('score', 0):>4} "
                  f"{d.get('structure_reveal', {}).get('score', 0):>4} "
                  f"{d.get('reframing', {}).get('score', 0):>4} "
                  f"{d.get('data_logic', {}).get('score', 0):>4} "
                  f"{d.get('closing', {}).get('score', 0):>4}")

    print("\n■ 深堀さん（y_fukabori）")
    print("-" * 80)
    print(f"{'商談名':<45} {'スコア':>8} {'DS':>5} {'SR':>5} {'RF':>5} {'DL':>5} {'CL':>5}")
    print("-" * 80)
    for r in results:
        if r["member"] == "y_fukabori":
            d = r["data"]
            name = r["file"][:42] + "..." if len(r["file"]) > 45 else r["file"]
            print(f"{name:<45} {d.get('total_score', 0):>6}/100 "
                  f"{d.get('data_shock', {}).get('score', 0):>4} "
                  f"{d.get('structure_reveal', {}).get('score', 0):>4} "
                  f"{d.get('reframing', {}).get('score', 0):>4} "
                  f"{d.get('data_logic', {}).get('score', 0):>4} "
                  f"{d.get('closing', {}).get('score', 0):>4}")

    print("\n■ その他メンバー（ランダム）")
    print("-" * 80)
    print(f"{'メンバー':<12} {'商談名':<35} {'スコア':>8} {'DS':>5} {'SR':>5} {'RF':>5} {'DL':>5} {'CL':>5}")
    print("-" * 80)
    for r in results:
        if r["member"] not in ["s_hattori", "y_fukabori"]:
            d = r["data"]
            name = r["file"][:32] + "..." if len(r["file"]) > 35 else r["file"]
            print(f"{r['member']:<12} {name:<35} {d.get('total_score', 0):>6}/100 "
                  f"{d.get('data_shock', {}).get('score', 0):>4} "
                  f"{d.get('structure_reveal', {}).get('score', 0):>4} "
                  f"{d.get('reframing', {}).get('score', 0):>4} "
                  f"{d.get('data_logic', {}).get('score', 0):>4} "
                  f"{d.get('closing', {}).get('score', 0):>4}")

    # 統計
    print()
    print("=" * 80)
    print("【統計サマリー】")
    print("=" * 80)

    # メンバー別平均
    for member_name, display_name in [("s_hattori", "服部"), ("y_fukabori", "深堀")]:
        member_results = [r for r in results if r["member"] == member_name]
        if member_results:
            avg_score = sum(r["data"]["total_score"] for r in member_results) / len(member_results)
            avg_ds = sum(r["data"].get("data_shock", {}).get("score", 0) for r in member_results) / len(member_results)
            avg_sr = sum(r["data"].get("structure_reveal", {}).get("score", 0) for r in member_results) / len(member_results)
            avg_rf = sum(r["data"].get("reframing", {}).get("score", 0) for r in member_results) / len(member_results)
            avg_dl = sum(r["data"].get("data_logic", {}).get("score", 0) for r in member_results) / len(member_results)
            avg_cl = sum(r["data"].get("closing", {}).get("score", 0) for r in member_results) / len(member_results)

            print(f"\n{display_name}さん平均（{len(member_results)}件）:")
            print(f"  総合: {avg_score:.1f}/100")
            print(f"  データ殴打: {avg_ds:.1f}/20 | 構造暴露: {avg_sr:.1f}/20 | 定義転換: {avg_rf:.1f}/20")
            print(f"  戦略提案: {avg_dl:.1f}/20 | クロージング: {avg_cl:.1f}/20")

    # その他平均
    other_results = [r for r in results if r["member"] not in ["s_hattori", "y_fukabori"]]
    if other_results:
        avg_score = sum(r["data"]["total_score"] for r in other_results) / len(other_results)
        print(f"\nその他メンバー平均（{len(other_results)}件）: {avg_score:.1f}/100")

    # 全体平均
    if results:
        total_avg = sum(r["data"]["total_score"] for r in results) / len(results)
        print(f"\n全体平均（{len(results)}件）: {total_avg:.1f}/100")

    # ランク分布
    print("\n【ランク分布】")
    ranks = {"S": 0, "A": 0, "B": 0, "C": 0}
    for r in results:
        rank = r["data"].get("rank", "")
        if rank in ranks:
            ranks[rank] += 1
    for rank, count in ranks.items():
        bar = "█" * count
        print(f"  {rank}: {count}件 {bar}")

    # レッドカード検出状況
    print("\n【レッドカード検出状況】")
    abstract_count = sum(1 for r in results if r["data"].get("red_card_abstract", {}).get("detected"))
    ignore_count = sum(1 for r in results if r["data"].get("red_card_ignore", {}).get("detected"))
    print(f"  抽象的逃げ: {abstract_count}/{len(results)}件")
    print(f"  スルー: {ignore_count}/{len(results)}件")


if __name__ == '__main__':
    main()
