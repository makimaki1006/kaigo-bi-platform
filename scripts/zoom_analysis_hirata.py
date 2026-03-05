"""
k_hirata（リクロジ事業部）の商談分析 + 個人傾向レポート + HTML出力
"""

import asyncio
import io
import json
import os
import re
import sys
import time
from pathlib import Path

import aiohttp
import pandas as pd
from dotenv import load_dotenv

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / "config" / ".env")

GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
GEMINI_URL = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={GEMINI_API_KEY}"

OUTPUT_DIR = PROJECT_ROOT / "data" / "output" / "zoom_transcripts"
MEMBER_DIR = OUTPUT_DIR / "k_hirata"
PROGRESS_FILE = OUTPUT_DIR / "_analysis_hirata_progress.json"
MAX_CONCURRENT = 5
MIN_LINES = 30  # 30行未満は除外

ANALYSIS_PROMPT = """【指示】
あなたは、採用支援サービス「リクロジ」の教育責任者です。
リクロジは製造・物流業界に特化した採用コンサルティングサービスです。
提示する商談ログを分析し、営業担当者が**「どのパスを見逃し、なぜ失注（見送り・競合負け）に至ったのか」**を特定するため、以下の7つの項目に整理して出力してください。特に「3」と「5」は、創作をせずログから営業担当者の実際の発言（またはスルーした事実）をそのまま抽出してください。

【分析項目と出力形式】
以下の番号付きリスト形式で出力してください。各項目は改行で区切り、見出しを明確にしてください。

## 1. 顧客の事実（パス）
顧客が語った現状（募集背景、欠員状況、既存手法への不満、期限など）を箇条書きで抽出してください。

## 2. 見逃した「真の痛み」と「実務のボトルネック」
### 見逃した痛み
顧客の発言の裏にあったはずの経営・現場リスク（離職連鎖、コスト損失等）を箇条書きで。
### 放置したボトルネック
顧客が「できない・面倒だ」と漏らした実務上の課題で、営業が拾いきれなかったものを箇条書きで。

## 3. 【実際のトーク】課題に対して「スルー」または「引いた」発言
ログの中から、顧客が課題や懸念を口にした際、担当者がどう返したか。深く踏み込めなかった、あるいは話題を変えてしまった箇所を以下の形式で抽出：
- 顧客発言:「（実際の発言）」
- 担当者の返し:「（実際の発言）」
- 問題点:（なぜこれがスルーなのか）

## 4. 本来提示すべきだったリクロジによる解決
顧客のボトルネックに対し、リクロジが「プロの採用コンサルタント」としてどう介入すべきだったかを箇条書きで。

## 5. 【実際のトーク】響かなかった（あるいは弱かった）提案内容
ログの中から、リクロジの価値を提案した際の実際の発言を以下の形式で抽出：
- 担当者発言:「（実際の発言）」
- 問題点:（なぜ響かなかったか）

## 6. 「Why Change（なぜ変えるか）」の構築失敗
なぜ顧客は「今のやり方のままでいい」と判断したか。危機感の醸成に失敗した原因を箇条書きで。

## 7. 「Why リクロジ（なぜリクロジか）」の差別化失敗
なぜリクロジが「他と変わらない選択肢」になったか。要因を箇条書きで。

## 【リテイク】トップセールスならどう言ったか
上記の分岐点で、主導権を握り直すための具体的な発言例を記載してください。
各分岐点について：
- 場面:（どの発言の後か）
- リテイクトーク:「（具体的な発言）」
- 狙い:（このトークで何を引き出すか）

【商談ログ】
"""

SUMMARY_PROMPT = """【指示】
あなたは採用支援サービス「リクロジ」の教育責任者です。
リクロジは製造・物流業界に特化した採用コンサルティングサービスです。
以下は、営業メンバー「平田 孝太」（k_hirata@f-a-c.co.jp）の2026年1月の全商談（{count}件）の分析結果です。

【重要な前提】
- これらの分析結果は「失注分析」のフォーマットで書かれていますが、商談自体の質は様々です
- 分析結果の中にも「良い対応」「適切なヒアリング」「効果的な提案」が含まれている場合があります
- 失注分析フォーマットだからといって全てを「失敗」と捉えないでください
- 商談の中で顧客が前向きな反応を示している箇所、メンバーが効果的な質問や提案をしている箇所にも注目してください

これらの分析結果を横断的に読み、**営業傾向レポート**を作成してください。

【出力フォーマット（厳守）】
- 必ず以下の6セクションを全て含めること
- 各セクションは「## N. セクション名」の形式で始めること
- レポート全体を4000〜6000文字程度に収めること

## 1. 総合評価（S/A/B/C/Dの5段階）
以下の基準で評価してください：
- S: エースレベル。顧客の本質的課題を引き出し、具体的提案でクロージングまで導ける
- A: 高水準。基本スキルが安定しており、改善点が少ない
- B: 標準レベル。基本はできているが、深掘り・提案・クロージングのいずれかに課題
- C: 改善必要。複数の営業プロセスに課題が見られる
- D: 基礎から見直し必要。営業プロセス全般に大きな課題

## 2. このメンバーの「型」（営業スタイルの特徴）
- どのような商談パターンを繰り返しているか
- 得意な場面と苦手な場面

## 3. 繰り返し見られる課題パターン TOP3
各パターンについて：
- パターン名
- 該当商談名（複数あれば列挙）
- 具体的な発言例（分析結果から引用）
- なぜこのパターンに陥るのかの仮説

## 4. 強み（活かすべきポイント）
- 実際にうまくいっている場面があれば具体的に
- 他メンバーと比較して優れている点

## 5. 改善アクション TOP3
- 優先度順に、具体的に何をすればいいか
- 「意識しましょう」ではなく、「この場面でこう言え」レベルの具体性

## 6. 総括
- 今後の成長に向けた具体的なアドバイス

【全商談の分析結果】
"""


def load_progress() -> dict:
    if PROGRESS_FILE.exists():
        return json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
    return {}


def save_progress(progress: dict):
    PROGRESS_FILE.write_text(json.dumps(progress, ensure_ascii=False), encoding="utf-8")


async def analyze_one(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    idx: int, total: int,
    key: str, topic: str, transcript: str,
    progress: dict, done_count: list,
) -> tuple[str, str]:
    if key in progress:
        done_count[0] += 1
        print(f"  [{done_count[0]}/{total}] SKIP {topic[:50]}", flush=True)
        return key, progress[key]

    async with semaphore:
        if len(transcript) > 200000:
            transcript = transcript[:200000] + "\n\n...(以降省略)"

        payload = {
            "contents": [{"parts": [{"text": ANALYSIS_PROMPT + transcript}]}],
            "generationConfig": {"temperature": 0.3, "maxOutputTokens": 8192},
        }

        result = "エラー: 未処理"
        for attempt in range(5):
            try:
                async with session.post(GEMINI_URL, json=payload, timeout=aiohttp.ClientTimeout(total=180)) as resp:
                    if resp.status == 429:
                        wait = 20 * (attempt + 1)
                        print(f"  [{idx+1}/{total}] 429 - {wait}秒待機...", flush=True)
                        await asyncio.sleep(wait)
                        continue
                    if resp.status != 200:
                        text = await resp.text()
                        result = f"エラー: status={resp.status}"
                        print(f"  [{idx+1}/{total}] ERR {resp.status}: {text[:100]}", flush=True)
                        break
                    data = await resp.json()
                    candidates = data.get("candidates", [])
                    if candidates:
                        parts = candidates[0].get("content", {}).get("parts", [])
                        result = "".join(p.get("text", "") for p in parts)
                    else:
                        result = "エラー: 応答なし"
                    break
            except asyncio.TimeoutError:
                result = "エラー: タイムアウト"
                if attempt < 4:
                    await asyncio.sleep(10)
            except Exception as e:
                result = f"エラー: {str(e)[:100]}"
                break

        done_count[0] += 1
        is_err = result.startswith("エラー")
        mark = "✗" if is_err else "✓"
        print(f"  [{done_count[0]}/{total}] {mark} {topic[:50]}", flush=True)

        if not is_err:
            progress[key] = result
            if done_count[0] % 5 == 0:
                save_progress(progress)

        return key, result


def call_gemini_sync(prompt: str, max_tokens: int = 8192) -> str:
    """同期版Gemini API呼び出し"""
    import requests
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
            return f"エラー: status={resp.status_code}"
        data = resp.json()
        candidates = data.get("candidates", [])
        if candidates:
            parts = candidates[0].get("content", {}).get("parts", [])
            return "".join(p.get("text", "") for p in parts)
        return "エラー: 応答なし"
    return "エラー: リトライ上限"


def markdown_to_html(text: str) -> str:
    """Markdown→HTML変換"""
    import html as h
    lines = text.split("\n")
    result = []
    in_list = False
    in_table = False

    for line in lines:
        stripped = line.strip()
        if not stripped:
            if in_list:
                result.append("</ul>")
                in_list = False
            if in_table:
                result.append("</tbody></table></div>")
                in_table = False
            continue

        if "|" in stripped and stripped.startswith("|"):
            cells = [c.strip() for c in stripped.split("|")[1:-1]]
            if all(set(c) <= set("- :") for c in cells):
                continue
            if not in_table:
                result.append('<div class="table-wrap"><table><thead><tr>')
                for c in cells:
                    c_html = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", h.escape(c))
                    result.append(f"<th>{c_html}</th>")
                result.append("</tr></thead><tbody>")
                in_table = True
            else:
                result.append("<tr>")
                for c in cells:
                    c_html = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", h.escape(c))
                    result.append(f"<td>{c_html}</td>")
                result.append("</tr>")
            continue

        if in_table:
            result.append("</tbody></table></div>")
            in_table = False

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
            content = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", h.escape(content))
            result.append(f"<li>{content}</li>")
        else:
            if in_list:
                result.append("</ul>")
                in_list = False
            content = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", h.escape(stripped))
            result.append(f"<p>{content}</p>")

    if in_list:
        result.append("</ul>")
    if in_table:
        result.append("</tbody></table></div>")
    return "\n".join(result)


async def main():
    print("=" * 60, flush=True)
    print("k_hirata（リクロジ）商談分析", flush=True)
    print("=" * 60, flush=True)

    # 文字起こしファイル一覧
    txt_files = sorted(MEMBER_DIR.glob("*.txt"))
    print(f"\n全ファイル数: {len(txt_files)}", flush=True)

    # フィルタリング
    targets = []
    for f in txt_files:
        text = f.read_text(encoding="utf-8", errors="replace")
        lines = [l for l in text.split("\n") if l.strip()]
        if len(lines) < MIN_LINES:
            print(f"  除外（{len(lines)}行）: {f.name[:50]}", flush=True)
            continue
        # 顧客名をテキストから抽出
        speakers = set()
        for line in text.split("\n")[:20]:
            m = re.match(r"\[.*?\]\s*(.+?):", line)
            if m:
                sp = m.group(1).strip()
                if "平田" not in sp:
                    speakers.add(sp)
        customer = list(speakers)[0] if speakers else "不明"
        # ファイル名から日時取得
        date_part = f.stem[:15] if len(f.stem) >= 15 else f.stem
        topic = f"{date_part} ({customer})"
        targets.append({"file": f, "topic": topic, "text": text, "customer": customer})

    print(f"分析対象: {len(targets)}件\n", flush=True)

    # --- Phase 1: 個別商談分析 ---
    print("【Phase 1】商談別分析\n", flush=True)

    progress = load_progress()
    done_count = [0]

    async with aiohttp.ClientSession() as session:
        semaphore = asyncio.Semaphore(MAX_CONCURRENT)
        tasks = []
        for i, t in enumerate(targets):
            key = t["file"].name
            tasks.append(
                analyze_one(session, semaphore, i, len(targets),
                            key, t["topic"], t["text"], progress, done_count)
            )
        results = await asyncio.gather(*tasks)

    save_progress(progress)

    # 結果整理
    analysis_results = []
    errors = 0
    for t, (key, result) in zip(targets, results):
        is_err = result.startswith("エラー")
        if is_err:
            errors += 1
        analysis_results.append({
            "ファイル": t["file"].name,
            "日時": t["file"].stem[:15],
            "顧客": t["customer"],
            "分析結果": result,
        })

    print(f"\n完了: {len(targets) - errors}/{len(targets)}件, エラー: {errors}件", flush=True)

    # --- Phase 2: 個人傾向レポート ---
    print("\n【Phase 2】個人傾向レポート生成\n", flush=True)

    analyses_text = []
    for r in analysis_results:
        if not r["分析結果"].startswith("エラー"):
            text = r["分析結果"]
            if len(text) > 2000:
                text = text[:2000] + "...(省略)"
            analyses_text.append(f"### {r['日時']} ({r['顧客']})\n{text}")

    all_analyses = "\n\n---\n\n".join(analyses_text)
    if len(all_analyses) > 180000:
        all_analyses = all_analyses[:180000] + "\n\n...(以降省略)"

    summary_prompt = SUMMARY_PROMPT.format(count=len(analyses_text)) + all_analyses
    summary_report = call_gemini_sync(summary_prompt, max_tokens=8192)
    print(f"  ✓ 傾向レポート完了（{len(summary_report)}文字）", flush=True)

    # --- Phase 3: Excel出力 ---
    print("\n【Phase 3】Excel出力\n", flush=True)

    excel_path = OUTPUT_DIR / "zoom_analysis_hirata_202601.xlsx"
    df = pd.DataFrame(analysis_results)
    with pd.ExcelWriter(str(excel_path), engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="商談分析", index=False)
        pd.DataFrame([{"傾向レポート": summary_report}]).to_excel(
            writer, sheet_name="傾向レポート", index=False
        )
    print(f"  Excel: {excel_path}", flush=True)

    # --- Phase 4: HTML出力 ---
    print("\n【Phase 4】HTML出力\n", flush=True)

    # 評価抽出
    m = re.search(r"\*\*([SABCD])[:\s]", summary_report)
    if not m:
        m = re.search(r"\*\*([SABCD])\*\*", summary_report)
    rating = m.group(1) if m else "?"

    rating_colors = {"S": "#7c4dff", "A": "#2979ff", "B": "#00bfa5", "C": "#ff9100", "D": "#ff1744"}
    r_color = rating_colors.get(rating, "#757575")

    # タブ生成
    tab_buttons = [
        '<button class="nav-btn active" onclick="showSection(\'summary\', this)">傾向レポート</button>'
    ]
    tab_contents = []

    # サマリータブ
    summary_html = markdown_to_html(summary_report)
    tab_contents.append(
        f'<div id="sec-summary" class="section active"><div class="content-card">'
        f'<div style="display:flex;align-items:center;gap:16px;margin-bottom:24px;">'
        f'<span class="badge-lg" style="background:{r_color}">{rating}</span>'
        f'<div><h2 style="margin:0;color:#f8fafc;">平田 孝太 - 傾向レポート</h2>'
        f'<span style="color:#94a3b8;">{len(targets)}件分析</span></div></div>'
        f'{summary_html}</div></div>'
    )

    # 商談別タブ
    for i, r in enumerate(analysis_results):
        tab_buttons.append(
            f'<button class="nav-btn" onclick="showSection(\'deal-{i}\', this)">'
            f'{r["日時"][:10]}<br><small>{r["顧客"][:8]}</small></button>'
        )
        deal_html = markdown_to_html(r["分析結果"])
        tab_contents.append(
            f'<div id="sec-deal-{i}" class="section"><div class="content-card">'
            f'<h2 style="color:#f8fafc;margin-bottom:4px;">{r["顧客"]}</h2>'
            f'<p style="color:#94a3b8;margin-bottom:20px;">{r["日時"]} | {r["ファイル"]}</p>'
            f'{deal_html}</div></div>'
        )

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>リクロジ 営業分析 - 平田 孝太 - 2026年1月</title>
<style>
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{ font-family: 'Meiryo','Hiragino Sans',sans-serif; background: #0f172a; color: #e2e8f0; }}
.header {{ background: linear-gradient(135deg,#1e293b,#0f172a); padding: 32px 40px; border-bottom: 1px solid #334155; }}
.header h1 {{ font-size: 26px; color: #f8fafc; }}
.header .sub {{ color: #94a3b8; font-size: 14px; margin-top: 6px; }}
.nav-wrap {{ display: flex; flex-wrap: wrap; gap: 4px; background: #1e293b; padding: 10px 40px; border-bottom: 1px solid #334155; position: sticky; top: 0; z-index: 100; overflow-x: auto; }}
.nav-btn {{ padding: 10px 16px; background: none; border: 1px solid #334155; color: #94a3b8; font-size: 12px; cursor: pointer; border-radius: 6px; white-space: nowrap; line-height: 1.3; }}
.nav-btn:hover {{ background: #334155; color: #e2e8f0; }}
.nav-btn.active {{ background: #3b82f6; color: #fff; border-color: #3b82f6; }}
.container {{ max-width: 1200px; margin: 0 auto; padding: 32px 40px; }}
.section {{ display: none; }}
.section.active {{ display: block; }}
.content-card {{ background: #1e293b; border: 1px solid #334155; border-radius: 12px; padding: 32px; line-height: 1.9; }}
.content-card h2 {{ color: #60a5fa; font-size: 20px; margin: 24px 0 10px; border-bottom: 1px solid #334155; padding-bottom: 8px; }}
.content-card h3 {{ color: #38bdf8; font-size: 17px; margin: 20px 0 8px; padding-left: 14px; border-left: 4px solid #3b82f6; }}
.content-card h4 {{ color: #93c5fd; font-size: 15px; margin: 16px 0 6px; }}
.content-card p {{ color: #cbd5e1; margin: 8px 0; }}
.content-card ul {{ margin: 8px 0; padding-left: 24px; }}
.content-card li {{ color: #cbd5e1; margin: 5px 0; }}
.content-card strong {{ color: #f87171; }}
.badge-lg {{ display: inline-flex; align-items: center; justify-content: center; width: 48px; height: 48px; border-radius: 10px; color: #fff; font-weight: 800; font-size: 24px; flex-shrink: 0; }}
.table-wrap {{ overflow-x: auto; margin: 12px 0; }}
.content-card table {{ width: 100%; border-collapse: collapse; }}
.content-card th {{ background: #334155; color: #e2e8f0; padding: 10px 14px; text-align: left; font-size: 13px; border: 1px solid #475569; }}
.content-card td {{ padding: 10px 14px; border: 1px solid #334155; color: #cbd5e1; font-size: 13px; }}
.kpi-row {{ display: grid; grid-template-columns: repeat(3,1fr); gap: 16px; margin-bottom: 24px; }}
.kpi-card {{ background: #0f172a; border: 1px solid #334155; border-radius: 10px; padding: 20px; text-align: center; }}
.kpi-label {{ color: #94a3b8; font-size: 12px; margin-bottom: 6px; }}
.kpi-value {{ font-size: 32px; font-weight: 700; color: #f8fafc; }}
</style>
</head>
<body>
<div class="header">
    <h1>リクロジ 営業分析レポート</h1>
    <div class="sub">平田 孝太（k_hirata@f-a-c.co.jp）| 2026年1月 | {len(targets)}商談</div>
</div>
<div class="nav-wrap">{"".join(tab_buttons)}</div>
<div class="container">
{"".join(tab_contents)}
</div>
<script>
function showSection(id, btn) {{
    document.querySelectorAll('.section').forEach(el => el.classList.remove('active'));
    document.querySelectorAll('.nav-btn').forEach(el => el.classList.remove('active'));
    document.getElementById('sec-' + id).classList.add('active');
    btn.classList.add('active');
}}
</script>
</body>
</html>"""

    html_path = OUTPUT_DIR / "zoom_analysis_hirata_202601.html"
    html_path.write_text(html, encoding="utf-8")
    print(f"  HTML: {html_path}", flush=True)

    # プログレスファイル削除
    if PROGRESS_FILE.exists():
        PROGRESS_FILE.unlink()

    print("\n完了", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
