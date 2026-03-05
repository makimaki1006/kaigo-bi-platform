"""
個人別傾向分析 + 組織全体サマリー生成

各メンバーの全商談分析結果をGeminiに投入し、
1) 個人ごとの傾向（強み・弱み・改善ポイント）
2) 組織全体の傾向と改善提言
を生成する。
"""

import io
import json
import os
import sys
import time
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / "config" / ".env")

GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
GEMINI_URL = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={GEMINI_API_KEY}"

OUTPUT_DIR = PROJECT_ROOT / "data" / "output" / "zoom_transcripts"

MEMBER_NAMES = {
    "k_sawada": "澤田", "s_shinoki": "篠木", "yo_ichiki": "市来",
    "y_mitsuishi": "三石", "k_kitami": "北見", "h_bamba": "番場",
    "t_nakaya": "中谷", "r_komatsu": "小松", "m_tanaka": "田中",
    "s_shimatani": "嶋谷", "s_hattori": "服部", "m_akasaka": "赤坂",
    "j_kubo": "久保", "k_kobayashi": "小林", "y_fukabori": "深堀",
    "y_haino": "灰野", "i_kumagai": "熊谷", "n_kiyohira": "清飛羅",
    "y_kamibayashi": "上林", "h_matsukaze": "松風", "j_sato": "佐藤",
    "r_shimura": "志村", "y_abe": "阿部",
}

INDIVIDUAL_PROMPT = """【指示】
あなたは採用支援サービス「Medica」の教育責任者です。
以下は、営業メンバー「{name}」（{email}）の2026年1月の全商談（{count}件）の分析結果です。

【重要な前提】
- これらの分析結果は「失注分析」のフォーマットで書かれていますが、商談自体の質は様々です
- 分析結果の中にも「良い対応」「適切なヒアリング」「効果的な提案」が含まれている場合があります
- 失注分析フォーマットだからといって全てを「失敗」と捉えないでください
- 特に「## 3. 【実際のトーク】」のセクションで、営業担当者が良い質問をしている場面や、顧客の反応が良い場面にも着目してください
{performance_context}

これらの分析結果を横断的に読み、このメンバー個人の**営業傾向レポート**を作成してください。

【出力フォーマット（厳守）】
- **必ず** 最初に「## 0. スコアカード」セクションを出力すること（これが最重要）
- その後に## 1〜## 6のセクションを出力すること
- 各セクションは「## N. セクション名」の形式で始めること
- レポート全体を3500〜5000文字程度に収めること
- 箇条書きを活用し、読みやすくすること

## 0. スコアカード
以下の5軸を**1〜10の整数**で採点してください（10が最高）。
**必ず以下の形式で1行ずつ出力してください（形式厳守、他のテキストを混ぜないこと）：**

課題発見力: [1-10の数値]
提案力: [1-10の数値]
危機感醸成力: [1-10の数値]
差別化力: [1-10の数値]
クロージング力: [1-10の数値]

【採点基準】
- 課題発見力: 顧客の表面的な回答を鵜呑みにせず、「真の痛み」「実務のボトルネック」まで到達できているか
  - 1-3: 顧客の言葉をオウム返しするだけ。深掘り質問がほぼない
  - 4-5: 質問はするが表面的。顧客の回答に対する追加深掘りが弱い
  - 6-7: 一定の深掘りができている。顧客の潜在ニーズに時々到達する
  - 8-10: 顧客自身が気づいていない課題まで引き出せている
- 提案力: Medicaのサービスを顧客の具体的な課題に紐づけて提案できているか
  - 1-3: 一般的なサービス説明のみ。顧客の状況に合わせたカスタマイズなし
  - 4-5: サービス説明はできるが、顧客課題との接続が弱い
  - 6-7: 顧客の課題に対して具体的なMedicaの解決策を提示できている
  - 8-10: 顧客の業界・規模・状況に完全にカスタマイズされた提案ができている
- 危機感醸成力: 「今のままだとどうなるか」を顧客に具体的にイメージさせられているか
  - 1-3: 危機感の醸成をしていない。現状維持でOKと思わせてしまっている
  - 4-5: 一般論での危機感提示のみ。顧客の状況に即していない
  - 6-7: 顧客の状況に基づいた具体的なリスクを提示できている
  - 8-10: 顧客が「このままではまずい」と自ら行動を起こすレベル
- 差別化力: 「なぜMedicaなのか」を他社と明確に区別して伝えられているか
  - 1-3: 他社との違いに言及なし。どの会社でも言えることしか言っていない
  - 4-5: 差別化ポイントに触れるが、抽象的で印象に残らない
  - 6-7: Medicaならではの強みを具体例で説明できている
  - 8-10: 顧客が「Medicaしかない」と感じるレベルの差別化
- クロージング力: 商談の最後に具体的な次のアクション・契約への道筋を作れているか
  - 1-3: 商談が自然消滅。次のステップが曖昧なまま終了
  - 4-5: 「検討してください」で終了。具体的なアクションなし
  - 6-7: 次回日程や検討期限を設定できている
  - 8-10: 顧客が自ら動き出す状態を作れている。決裁プロセスまで握れている

## 1. 総合所見
上記スコアカードの結果をもとに、このメンバーの全体的な営業力を2〜3文で簡潔に総括してください。

## 2. このメンバーの「型」（営業スタイルの特徴）
- どのような商談パターンを繰り返しているか（2〜3パターン）
- 得意な場面と苦手な場面

## 3. 繰り返し見られる課題パターン TOP3
各パターンについて：
- パターン名
- 該当商談名（複数あれば列挙）
- 具体的な発言例（分析結果から引用）
- なぜこのパターンに陥るのかの仮説

## 4. 強み（活かすべきポイント）
- 実際にうまくいっている場面があれば具体的に
- 分析結果の中で顧客が前向きな反応を示した場面

## 5. 改善アクション TOP3
- 優先度順に、具体的に何をすればいいか
- 「意識しましょう」ではなく、「この場面でこう言え」レベルの具体性

## 6. 服部・深堀との比較
- 組織トップの服部・深堀と比較して、最も大きな違いは何か
- ※服部・深堀本人の場合は、他メンバーとの差別化要因を記述すること

【全商談の分析結果】
"""

ORG_PROMPT = """【指示】
あなたは採用支援サービス「Medica」の教育責任者です。
以下は、medica事業部の営業メンバー{member_count}名の2026年1月の個人別傾向レポートです。
各メンバーには5軸のスコア（1〜10）が付与されています。

【重要な前提】
- 深堀・服部は組織内で最も成約率が高いトップパフォーマーです
- 各メンバーのスコアカード（5軸×1〜10点）を参照してランキングを作成してください

{score_summary}

これらを横断的に分析し、**組織全体のサマリーレポート**を作成してください。

【出力フォーマット（厳守）】
- 必ず以下の6セクションを全て含めること
- レポート全体を5000〜8000文字程度に収めること

## 1. 組織全体の営業力スコアカード
上記のスコア集計から、5軸の組織平均スコア（10点満点）を算出し記載してください。

## 2. 組織全体で最も多い失敗パターン TOP5
- パターン名
- 該当メンバー数と割合
- 代表的な発言例

## 3. メンバー別ランキング
- 合計スコア順に一覧（名前、合計点、5軸スコア、一言コメント）
- テーブル形式で出力すること

## 4. 組織として取り組むべき改善施策 TOP3
- 施策名
- 対象者（全員 or 特定メンバー）
- 具体的なアクション（ロープレテーマ、トークスクリプト改善等）
- 期待効果

## 5. 「勝ちパターン」の言語化
- 深堀・服部の分析結果から見える、成約に繋がるトークの共通パターン
- 組織に展開すべき具体的なトークフレーム

## 6. 来月の教育優先テーマ
- 最もインパクトの大きい1テーマに絞り、具体的な教育プランを提示

【個人別傾向レポート】
"""


def call_gemini(prompt: str, max_tokens: int = 8192) -> str:
    """Gemini APIを呼び出す"""
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.3, "maxOutputTokens": max_tokens},
    }
    for attempt in range(5):
        resp = requests.post(GEMINI_URL, json=payload, timeout=180)
        if resp.status_code == 429:
            wait = 20 * (attempt + 1)
            print(f"  429 - {wait}秒待機...", flush=True)
            time.sleep(wait)
            continue
        if resp.status_code != 200:
            print(f"  ERR {resp.status_code}: {resp.text[:200]}", flush=True)
            return f"エラー: status={resp.status_code}"
        data = resp.json()
        candidates = data.get("candidates", [])
        if candidates:
            parts = candidates[0].get("content", {}).get("parts", [])
            return "".join(p.get("text", "") for p in parts)
        return "エラー: 応答なし"
    return "エラー: リトライ上限"


def detect_actual_salesperson(txt_path: Path) -> str | None:
    """文字起こしファイルから実際の営業担当者を特定する。
    先頭20行の発言者名から社内メンバーを検出し返す。
    テレアポ担当者のZoomアカウントで記録されていても、
    実際に商談を行っているのは別のメンバーである場合を特定する。
    """
    import re as _re
    if not txt_path.exists():
        return None
    text = txt_path.read_text(encoding="utf-8", errors="replace")
    lines = text.split("\n")[:20]
    speakers = set()
    for line in lines:
        m = _re.match(r"\[.*?\]\s*(.+?):", line)
        if m:
            speakers.add(m.group(1).strip())

    # 社内メンバー名の検出（姓で判定）
    DISPLAY_TO_KEY = {
        "澤田": "k_sawada", "篠木": "s_shinoki", "市来": "yo_ichiki",
        "三石": "y_mitsuishi", "北見": "k_kitami", "番場": "h_bamba",
        "中谷": "t_nakaya", "小松": "r_komatsu", "田中": "m_tanaka",
        "嶋谷": "s_shimatani", "服部": "s_hattori", "赤坂": "m_akasaka",
        "久保": "j_kubo", "小林": "k_kobayashi", "深堀": "y_fukabori",
        "灰野": "y_haino", "熊谷": "i_kumagai", "清飛羅": "n_kiyohira",
        "上林": "y_kamibayashi", "松風": "h_matsukaze", "佐藤": "j_sato",
        "志村": "r_shimura", "阿部": "y_abe",
    }
    found = []
    for sp in speakers:
        for name, key in DISPLAY_TO_KEY.items():
            if name in sp:
                found.append(key)
                break
    # 1人だけ見つかればその人が営業担当
    if len(found) == 1:
        return found[0]
    # 複数見つかった場合（ロープレ等）はNone
    return None


def main():
    print("=" * 60, flush=True)
    print("個人別傾向 + 組織サマリー生成", flush=True)
    print("=" * 60, flush=True)

    xls = pd.ExcelFile(str(OUTPUT_DIR / "zoom_analysis_v2_202601.xlsx"))
    sheets = [s for s in xls.sheet_names if s != "サマリー"]

    # テレアポ担当者（自分で商談しない）
    TELEAPO_MEMBERS = {"y_mitsuishi", "h_bamba", "k_kitami", "t_nakaya"}

    # トップパフォーマーのコンテキスト
    PERFORMANCE_CONTEXT = {
        "y_fukabori": "\n- ※深堀は組織内で最も成約率が高いトップパフォーマーです。失注分析データだけでなく、商談の質の高さ（ヒアリング力、提案力、クロージング力）を正当に評価してください。",
        "s_hattori": "\n- ※服部は組織内でトップクラスの成約実績を持つメンバーです。失注分析データだけでなく、商談の質の高さを正当に評価してください。",
    }

    # --- Phase 0: テレアポ担当の商談を実際の営業担当者に再分類 ---
    print("\n【Phase 0】テレアポ商談の再分類\n", flush=True)

    # テレアポ担当のtxtファイルから、ミーティング名→実際の営業担当のマッピングを構築
    teleapo_mapping: dict[str, dict[str, str]] = {}  # {teleapo_member: {meeting_name_fragment: actual_owner}}
    for tm in TELEAPO_MEMBERS:
        folder = OUTPUT_DIR / tm
        if not folder.exists():
            continue
        teleapo_mapping[tm] = {}
        for txt_path in folder.glob("*.txt"):
            detected = detect_actual_salesperson(txt_path)
            if detected and detected != tm:
                # ファイル名からミーティング名部分を抽出（日時を除く）
                # 形式: 2026-01-06_2358_商談名.txt
                parts = txt_path.stem.split("_", 2)
                if len(parts) >= 3:
                    meeting_fragment = parts[2].strip()
                else:
                    meeting_fragment = txt_path.stem
                teleapo_mapping[tm][meeting_fragment] = detected

    # 全シートのデータを読み込み、再分類
    all_data: dict[str, list] = {}  # key=実際の営業担当, value=[(topic, result), ...]
    reassign_count = 0

    for sheet_name in sheets:
        df = pd.read_excel(xls, sheet_name=sheet_name)
        if len(df) == 0:
            continue

        for _, row in df.iterrows():
            topic = str(row["ミーティング名"]).strip()
            result = str(row["分析結果"])

            actual_owner = sheet_name

            if sheet_name in TELEAPO_MEMBERS and sheet_name in teleapo_mapping:
                # ミーティング名でマッチング
                mapping = teleapo_mapping[sheet_name]
                matched = False
                for fragment, owner in mapping.items():
                    # 部分一致（ミーティング名がファイル名の商談名部分を含む or 逆）
                    if fragment in topic or topic in fragment:
                        actual_owner = owner
                        matched = True
                        break
                if matched:
                    jp_from = MEMBER_NAMES.get(sheet_name, sheet_name)
                    jp_to = MEMBER_NAMES.get(actual_owner, actual_owner)
                    print(f"  再分類: {topic[:35]}... {jp_from} → {jp_to}", flush=True)
                    reassign_count += 1

            if actual_owner not in all_data:
                all_data[actual_owner] = []
            all_data[actual_owner].append((topic, result))

    print(f"\n  再分類合計: {reassign_count}件", flush=True)

    # テレアポ担当者で残存があるか確認
    for tm in TELEAPO_MEMBERS:
        if tm in all_data:
            remaining = len(all_data[tm])
            if remaining > 0:
                print(f"  注意: {MEMBER_NAMES.get(tm, tm)}に{remaining}件残存（ロープレ等、分析から除外）", flush=True)

    individual_reports = {}

    # --- Phase 1: 個人別傾向分析 ---
    active_members = [k for k in all_data if k not in TELEAPO_MEMBERS and len(all_data[k]) > 0]
    print(f"\n【Phase 1】個人別傾向分析（{len(active_members)}名）\n", flush=True)

    for member_key in active_members:
        items = all_data[member_key]
        if not items:
            continue

        jp_name = MEMBER_NAMES.get(member_key, member_key)
        email = f"{member_key}@cyxen.co.jp"
        print(f"  分析中: {jp_name}（{len(items)}件）...", flush=True)

        analyses = []
        for topic, result in items:
            if len(result) > 2000:
                result = result[:2000] + "...(省略)"
            analyses.append(f"### {topic}\n{result}")

        all_analyses = "\n\n---\n\n".join(analyses)

        if len(all_analyses) > 180000:
            all_analyses = all_analyses[:180000] + "\n\n...(以降省略)"

        performance_context = PERFORMANCE_CONTEXT.get(member_key, "")
        prompt = INDIVIDUAL_PROMPT.format(
            name=jp_name, email=email, count=len(items),
            performance_context=performance_context,
        ) + all_analyses

        result = call_gemini(prompt, max_tokens=8192)
        individual_reports[member_key] = {
            "name": jp_name,
            "count": len(items),
            "report": result,
        }
        print(f"  ✓ {jp_name} 完了（{len(result)}文字）", flush=True)
        time.sleep(2)

    # --- 相対スコアリング ---
    print(f"\n【Phase 1.5】相対スコアリング（全員比較）\n", flush=True)

    import re as _re
    AXES = ["課題発見力", "提案力", "危機感醸成力", "差別化力", "クロージング力"]

    # まずPhase 1のスコアカードから個別スコアを抽出（初期値）
    for key, data in individual_reports.items():
        scores = {}
        for axis in AXES:
            m = _re.search(rf"{axis}:\s*(\d+)", data["report"])
            scores[axis] = int(m.group(1)) if m else 5
        data["scores"] = scores
        data["total_score"] = sum(scores.values())

    # 全員の個人レポートを比較して相対スコアをつけ直す
    RELATIVE_SCORE_PROMPT = """【指示】
あなたは採用支援サービス「Medica」の教育責任者です。
以下に{member_count}名の営業メンバーの傾向レポートを提示します。

【重要な前提】
- 深堀・服部は組織内で最も成約率が高いトップパフォーマーです。スコアは最も高くなるべきです。
- 全メンバーを**相対的に比較**して、差がつくようにスコアをつけてください。
- 全員同じスコアにしないでください。必ず差をつけてください。
- スコアの分布イメージ: トップ（深堀・服部）は7〜9、中堅は4〜6、要改善は2〜4

【タスク】
以下の5軸で各メンバーを1〜10の整数でスコアリングしてください。
- 課題発見力
- 提案力
- 危機感醸成力
- 差別化力
- クロージング力

【出力形式（厳守）】
各メンバーについて、以下の形式で1行ずつ出力してください。他のテキストは一切不要です。

メンバー名|課題発見力|提案力|危機感醸成力|差別化力|クロージング力

例: 山田|7|6|5|6|7

【メンバーの傾向レポート】
"""

    # レポートを要約して投入（トークン節約）
    summary_for_scoring = []
    for key, data in individual_reports.items():
        report = data["report"]
        # スコアカード部分 + 総合所見 + 型 + 強みを抽出（最大800文字）
        summary_for_scoring.append(f"### {data['name']}（{data['count']}件）\n{report[:800]}")

    scoring_input = "\n\n---\n\n".join(summary_for_scoring)
    scoring_prompt = RELATIVE_SCORE_PROMPT.format(member_count=len(individual_reports)) + scoring_input

    scoring_result = call_gemini(scoring_prompt, max_tokens=2048)
    print(f"  スコアリング結果（{len(scoring_result)}文字）:", flush=True)

    # パース
    for line in scoring_result.strip().split("\n"):
        line = line.strip()
        if not line or "|" not in line:
            continue
        parts = [p.strip() for p in line.split("|")]
        if len(parts) < 6:
            continue
        name = parts[0]
        try:
            scores_vals = [int(p) for p in parts[1:6]]
        except ValueError:
            continue
        # メンバー名でマッチング
        matched_key = None
        for key, data in individual_reports.items():
            if data["name"] == name or name in data["name"]:
                matched_key = key
                break
        if matched_key:
            new_scores = dict(zip(AXES, scores_vals))
            individual_reports[matched_key]["scores"] = new_scores
            individual_reports[matched_key]["total_score"] = sum(scores_vals)

    # 結果表示
    for key, data in sorted(individual_reports.items(), key=lambda x: -x[1]["total_score"]):
        s = data["scores"]
        print(f"  {data['name']:6s}: {' / '.join(f'{a}={s[a]}' for a in AXES)} = {data['total_score']}点", flush=True)

    # --- 組織サマリー ---
    print(f"\n【Phase 2】組織全体サマリー生成\n", flush=True)

    # スコアサマリーテキスト生成
    score_lines = ["【メンバー別スコア一覧（各軸1〜10点、合計50点満点）】"]
    sorted_members = sorted(individual_reports.items(), key=lambda x: -x[1]["total_score"])
    for rank, (key, data) in enumerate(sorted_members, 1):
        s = data["scores"]
        score_lines.append(
            f"{rank}. {data['name']}（{data['count']}件）: "
            f"課題発見={s['課題発見力']}, 提案={s['提案力']}, 危機感={s['危機感醸成力']}, "
            f"差別化={s['差別化力']}, CL={s['クロージング力']} → 合計{data['total_score']}点"
        )
    score_summary = "\n".join(score_lines)

    # 個人レポートを結合
    all_individual = []
    for sheet_name, data in individual_reports.items():
        all_individual.append(
            f"## {data['name']}（{data['count']}件）\n\n{data['report']}"
        )
    combined = "\n\n===\n\n".join(all_individual)

    if len(combined) > 180000:
        combined = combined[:180000] + "\n\n...(以降省略)"

    org_prompt = ORG_PROMPT.format(
        member_count=len(individual_reports),
        score_summary=score_summary,
    ) + combined
    org_report = call_gemini(org_prompt, max_tokens=8192)
    print(f"  ✓ 組織サマリー完了（{len(org_report)}文字）", flush=True)

    # --- HTML出力 ---
    print("\n【Phase 3】HTML出力\n", flush=True)

    tab_buttons = ['<button class="tab-btn active" onclick="openTab(event, \'tab-org\')">組織サマリー</button>']
    tab_contents = []

    # 組織サマリータブ
    org_html = markdown_to_html(org_report)
    tab_contents.append(
        f'<div id="tab-org" class="tab-content" style="display:block;">'
        f'<h2>組織全体サマリー</h2>{org_html}</div>'
    )

    # 個人タブ
    for i, (sheet_name, data) in enumerate(individual_reports.items()):
        tab_buttons.append(
            f'<button class="tab-btn" onclick="openTab(event, \'tab-{i}\')">'
            f'{data["name"]}({data["count"]}件)</button>'
        )
        report_html = markdown_to_html(data["report"])
        tab_contents.append(
            f'<div id="tab-{i}" class="tab-content" style="display:none;">'
            f'<h2>{data["name"]}の傾向レポート（{data["count"]}件分析）</h2>'
            f'{report_html}</div>'
        )

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<title>Medica 営業分析レポート - 2026年1月</title>
<style>
body {{ font-family: 'Meiryo', 'Hiragino Sans', sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }}
h1 {{ color: #1a237e; border-bottom: 3px solid #303f9f; padding-bottom: 10px; }}
h2 {{ color: #1565c0; }}
h3 {{ color: #1976d2; border-left: 4px solid #42a5f5; padding-left: 12px; margin-top: 24px; }}
h4 {{ color: #455a64; margin-top: 16px; }}
.tab-bar {{ display: flex; flex-wrap: wrap; gap: 4px; margin: 20px 0; background: #fff; padding: 10px; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
.tab-btn {{ padding: 8px 16px; border: 1px solid #ddd; background: #f9f9f9; cursor: pointer; border-radius: 4px; font-size: 13px; }}
.tab-btn.active {{ background: #1565c0; color: #fff; border-color: #1565c0; }}
.tab-btn:hover {{ background: #e3f2fd; }}
.tab-btn.active:hover {{ background: #0d47a1; }}
.tab-content {{ background: #fff; padding: 30px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); line-height: 1.8; }}
ul {{ margin: 8px 0; padding-left: 24px; }}
li {{ margin: 6px 0; }}
p {{ margin: 8px 0; }}
strong {{ color: #d32f2f; }}
</style>
<script>
function openTab(evt, tabId) {{
    document.querySelectorAll('.tab-content').forEach(el => el.style.display = 'none');
    document.querySelectorAll('.tab-btn').forEach(el => el.classList.remove('active'));
    document.getElementById(tabId).style.display = 'block';
    evt.currentTarget.classList.add('active');
}}
</script>
</head>
<body>
<h1>Medica 営業分析レポート - 2026年1月</h1>
<p>対象: {len(individual_reports)}名 / 463商談</p>
<div class="tab-bar">{"".join(tab_buttons)}</div>
{"".join(tab_contents)}
</body>
</html>"""

    html_path = OUTPUT_DIR / "zoom_analysis_summary_202601.html"
    html_path.write_text(html, encoding="utf-8")
    print(f"HTML出力: {html_path}", flush=True)

    # Excel出力
    excel_path = OUTPUT_DIR / "zoom_analysis_summary_202601.xlsx"
    with pd.ExcelWriter(str(excel_path), engine="openpyxl") as writer:
        # 組織サマリー
        pd.DataFrame([{"レポート": org_report}]).to_excel(
            writer, sheet_name="組織サマリー", index=False
        )
        # 個人別
        for sheet_name, data in individual_reports.items():
            report_text = data["report"]
            if len(report_text) > 32767:
                report_text = report_text[:32700] + "\n\n...(セル上限省略)"
            pd.DataFrame([{
                "メンバー": data["name"],
                "商談数": data["count"],
                "傾向レポート": report_text,
            }]).to_excel(writer, sheet_name=data["name"], index=False)

    print(f"Excel出力: {excel_path}", flush=True)
    print("\n完了", flush=True)


def markdown_to_html(text: str) -> str:
    """簡易Markdown→HTML変換"""
    import html as h
    lines = text.split("\n")
    result = []
    in_list = False

    for line in lines:
        stripped = line.strip()
        if not stripped:
            if in_list:
                result.append("</ul>")
                in_list = False
            continue

        if stripped.startswith("### "):
            if in_list:
                result.append("</ul>")
                in_list = False
            result.append(f"<h4>{h.escape(stripped[4:])}</h4>")
        elif stripped.startswith("## "):
            if in_list:
                result.append("</ul>")
                in_list = False
            result.append(f"<h3>{h.escape(stripped[3:])}</h3>")
        elif stripped.startswith("# "):
            if in_list:
                result.append("</ul>")
                in_list = False
            result.append(f"<h2>{h.escape(stripped[2:])}</h2>")
        elif stripped.startswith("- ") or stripped.startswith("* "):
            if not in_list:
                result.append("<ul>")
                in_list = True
            content = stripped[2:]
            # bold変換
            import re
            content = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', h.escape(content))
            result.append(f"<li>{content}</li>")
        else:
            if in_list:
                result.append("</ul>")
                in_list = False
            import re
            content = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', h.escape(stripped))
            result.append(f"<p>{content}</p>")

    if in_list:
        result.append("</ul>")

    return "\n".join(result)


if __name__ == "__main__":
    main()
