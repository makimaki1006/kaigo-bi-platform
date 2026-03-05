"""
Zoom商談文字起こし失注分析スクリプト v2

Gemini 2.0 Flashを使って商談ログを並列分析。
出力形式: Excel（番号付きリスト） + HTML（ユーザー別タブ）
"""

import asyncio
import html as html_lib
import io
import json
import os
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
PROGRESS_FILE = OUTPUT_DIR / "_analysis_v2_progress.json"
MAX_CONCURRENT = 5

EXCLUDE_KEYWORDS = [
    "ロープレ", "練習", "パーソナルミーティング", "打ち合わせ",
    "お打ち合わせ", "MTG", "YMCX", "Zoom Meeting", "Zoom ミーティング",
    "[New Zoom Meeting]",
]

ANALYSIS_PROMPT = """【指示】
あなたは、採用支援サービス「Medica（メディカ）」の教育責任者です。
提示する商談ログを分析し、新人メンバーが**「どのパスを見逃し、なぜ失注（見送り・競合負け）に至ったのか」**を特定するため、以下の7つの項目に整理して出力してください。特に「3」と「5」は、創作をせずログから営業担当者の実際の発言（またはスルーした事実）をそのまま抽出してください。

【分析項目と出力形式】
以下の番号付きリスト形式で出力してください。各項目は改行で区切り、見出しを明確にしてください。

## 1. 顧客の事実（パス）
顧客が語った現状（募集背景、欠員状況、既存手法への不満、期限など）を箇条書きで抽出してください。

## 2. 見逃した「真の痛み」と「実務のボトルネック」
### 見逃した痛み
顧客の発言の裏にあったはずの経営・現場リスク（離職連鎖、コスト損失等）を箇条書きで。
### 放置したボトルネック
顧客が「できない・面倒だ」と漏らした実務上の課題（相場調査、求人更新、決済資料等）で、営業が拾いきれなかったものを箇条書きで。

## 3. 【実際のトーク】課題に対して「スルー」または「引いた」発言
ログの中から、顧客が課題や懸念を口にした際、担当者がどう返したか。深く踏み込めなかった、あるいは話題を変えてしまった箇所を以下の形式で抽出：
- 顧客発言:「（実際の発言）」
- 担当者の返し:「（実際の発言）」
- 問題点:（なぜこれがスルーなのか）

## 4. 本来提示すべきだったMedicaによる解決
顧客のボトルネックに対し、Medicaが「プロの労働力」としてどう介入すべきだったかを箇条書きで。

## 5. 【実際のトーク】響かなかった（あるいは弱かった）提案内容
ログの中から、Medicaの価値を提案した際の実際の発言を以下の形式で抽出：
- 担当者発言:「（実際の発言）」
- 問題点:（なぜ響かなかったか）

## 6. 「Why Change（なぜ変えるか）」の構築失敗
なぜ顧客は「今のやり方のままでいい」と判断したか。危機感の醸成に失敗した原因を箇条書きで。

## 7. 「Why Medica（なぜMedicaか）」の差別化失敗
なぜMedicaが「他と変わらない選択肢」になったか。要因を箇条書きで。

## 【リテイク】売れているメンバー（服部・深堀）ならどう言ったか
上記の分岐点で、服部・深堀が主導権を握り直すための具体的な発言例を記載してください。
各分岐点について：
- 場面:（どの発言の後か）
- リテイクトーク:「（具体的な発言）」
- 狙い:（このトークで何を引き出すか）

【商談ログ】
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
        max_chars = 200000
        if len(transcript) > max_chars:
            transcript = transcript[:max_chars] + "\n\n...（以降省略）"

        payload = {
            "contents": [{"parts": [{"text": ANALYSIS_PROMPT + transcript}]}],
            "generationConfig": {"temperature": 0.3, "maxOutputTokens": 8192},
        }

        result = "エラー: 未処理"
        for attempt in range(10):
            try:
                async with session.post(
                    GEMINI_URL, json=payload,
                    timeout=aiohttp.ClientTimeout(total=180),
                ) as resp:
                    if resp.status == 429:
                        wait = 20 * (attempt + 1)
                        print(f"  [{idx}/{total}] 429 - {wait}秒待機...", flush=True)
                        await asyncio.sleep(wait)
                        continue
                    if resp.status != 200:
                        text = await resp.text()
                        print(f"  [{idx}/{total}] ERR {resp.status}: {text[:100]}", flush=True)
                        result = f"エラー: status={resp.status}"
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
                if attempt == 9:
                    result = "エラー: タイムアウト"
                    break
                await asyncio.sleep(5)
            except Exception as e:
                if attempt == 9:
                    result = f"エラー: {e}"
                    break
                await asyncio.sleep(5)

        done_count[0] += 1
        ok = "OK" if not result.startswith("エラー") else "NG"
        print(f"  [{done_count[0]}/{total}] {ok} {topic[:50]}", flush=True)

        progress[key] = result
        if done_count[0] % 10 == 0:
            save_progress(progress)

        return key, result


def generate_html(target_df: pd.DataFrame, email_to_name: dict, output_path: Path):
    """ユーザー別タブ付きHTMLを生成"""
    emails = target_df["メール"].unique()

    # タブボタン
    tab_buttons = []
    tab_contents = []

    for i, email in enumerate(emails):
        name = email_to_name.get(email, email.split("@")[0])
        active = "active" if i == 0 else ""
        tab_buttons.append(f'<button class="tab-btn {active}" onclick="openTab(event, \'tab-{i}\')">{name}</button>')

        user_df = target_df[target_df["メール"] == email]
        display = "block" if i == 0 else "none"

        cards = []
        for _, row in user_df.iterrows():
            topic = html_lib.escape(str(row["ミーティング名"]))
            date = str(row["開始日時"])[:10]
            duration = row["時間(分)"]
            analysis = str(row["分析結果"])
            # Markdown見出しをHTMLに変換
            analysis_html = analysis
            analysis_html = analysis_html.replace("## 【リテイク】", "<h3 class='retake'>【リテイク】")
            analysis_html = analysis_html.replace("## ", "<h3>")
            # 見出し閉じタグ（次の見出しまたは終端）
            import re
            analysis_html = re.sub(r'<h3([^>]*)>([^<]+)', r'<h3\1>\2</h3>', analysis_html)
            # 箇条書きをHTMLリストに
            lines = analysis_html.split("\n")
            html_lines = []
            in_list = False
            for line in lines:
                stripped = line.strip()
                if stripped.startswith("- "):
                    if not in_list:
                        html_lines.append("<ul>")
                        in_list = True
                    html_lines.append(f"<li>{html_lib.escape(stripped[2:])}</li>")
                else:
                    if in_list:
                        html_lines.append("</ul>")
                        in_list = False
                    if stripped.startswith("<h3"):
                        html_lines.append(stripped)
                    elif stripped.startswith("###"):
                        html_lines.append(f"<h4>{html_lib.escape(stripped.replace('### ', ''))}</h4>")
                    elif stripped:
                        html_lines.append(f"<p>{html_lib.escape(stripped)}</p>")
            if in_list:
                html_lines.append("</ul>")
            analysis_rendered = "\n".join(html_lines)

            cards.append(f"""
            <div class="card">
                <div class="card-header">
                    <span class="date">{date}</span>
                    <span class="topic">{topic}</span>
                    <span class="duration">{duration}分</span>
                </div>
                <div class="card-body">{analysis_rendered}</div>
            </div>""")

        tab_contents.append(
            f'<div id="tab-{i}" class="tab-content" style="display:{display};">'
            f'<h2>{name} ({len(user_df)}件)</h2>'
            f'{"".join(cards)}</div>'
        )

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<title>Zoom商談 失注分析 - 2026年1月</title>
<style>
body {{ font-family: 'Meiryo', 'Hiragino Sans', sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }}
h1 {{ color: #333; border-bottom: 3px solid #2196F3; padding-bottom: 10px; }}
.tab-bar {{ display: flex; flex-wrap: wrap; gap: 4px; margin: 20px 0; background: #fff; padding: 10px; border-radius: 8px; }}
.tab-btn {{ padding: 8px 16px; border: 1px solid #ddd; background: #f9f9f9; cursor: pointer; border-radius: 4px; font-size: 13px; }}
.tab-btn.active {{ background: #2196F3; color: #fff; border-color: #2196F3; }}
.tab-btn:hover {{ background: #e3f2fd; }}
.tab-btn.active:hover {{ background: #1976D2; }}
.card {{ background: #fff; border-radius: 8px; margin: 16px 0; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
.card-header {{ background: #2196F3; color: #fff; padding: 12px 20px; border-radius: 8px 8px 0 0; display: flex; gap: 16px; align-items: center; }}
.card-header .date {{ font-weight: bold; }}
.card-header .topic {{ flex: 1; }}
.card-header .duration {{ background: rgba(255,255,255,0.2); padding: 2px 8px; border-radius: 4px; font-size: 13px; }}
.card-body {{ padding: 20px; line-height: 1.8; }}
.card-body h3 {{ color: #1565C0; border-left: 4px solid #2196F3; padding-left: 12px; margin-top: 24px; }}
.card-body h3.retake {{ color: #E65100; border-left-color: #FF9800; }}
.card-body h4 {{ color: #455A64; margin-top: 16px; }}
.card-body ul {{ margin: 8px 0; padding-left: 24px; }}
.card-body li {{ margin: 4px 0; }}
.card-body p {{ margin: 6px 0; }}
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
<h1>Zoom商談 失注分析 - 2026年1月</h1>
<p>対象: {len(target_df)}件 / {len(emails)}名</p>
<div class="tab-bar">{"".join(tab_buttons)}</div>
{"".join(tab_contents)}
</body>
</html>"""

    output_path.write_text(html, encoding="utf-8")
    print(f"HTML出力: {output_path}", flush=True)


async def main():
    print("=" * 60, flush=True)
    print("Zoom商談 失注分析 v2（並列 Gemini 2.0 Flash）", flush=True)
    print("=" * 60, flush=True)

    csv_df = pd.read_csv(
        OUTPUT_DIR / "zoom_transcripts_2026-01-01_2026-01-31.csv",
        encoding="utf-8-sig",
    )

    mask = csv_df["ミーティング名"].apply(
        lambda x: not any(kw in str(x) for kw in EXCLUDE_KEYWORDS)
    )
    target_df = csv_df[mask].copy().reset_index(drop=True)
    excluded = len(csv_df) - len(target_df)

    progress = load_progress()
    keys = [f"{row['メール']}_{row['ミーティングID']}" for _, row in target_df.iterrows()]
    already_done = sum(1 for k in keys if k in progress)
    remaining = len(target_df) - already_done
    print(f"全件: {len(csv_df)} / 除外: {excluded} / 分析対象: {len(target_df)}", flush=True)
    print(f"完了済み: {already_done} / 残り: {remaining}\n", flush=True)

    transcripts = []
    for _, row in target_df.iterrows():
        txt_path = OUTPUT_DIR / row["文字起こしファイル"]
        if txt_path.exists():
            transcripts.append(txt_path.read_text(encoding="utf-8"))
        else:
            transcripts.append("")

    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    done_count = [0]
    start_time = time.time()

    async with aiohttp.ClientSession() as session:
        tasks = [
            analyze_one(
                session, semaphore, i + 1, len(target_df),
                keys[i], target_df.iloc[i]["ミーティング名"],
                transcripts[i], progress, done_count,
            )
            for i in range(len(target_df))
        ]
        results_pairs = await asyncio.gather(*tasks)

    for key, result in results_pairs:
        progress[key] = result
    save_progress(progress)

    elapsed = time.time() - start_time
    print(f"\n分析完了: {elapsed:.0f}秒 ({elapsed/60:.1f}分)", flush=True)

    results = [progress.get(k, "エラー: 未処理") for k in keys]
    target_df["分析結果"] = results
    error_count = sum(1 for r in results if str(r).startswith("エラー"))
    print(f"成功: {len(results) - error_count} / エラー: {error_count}", flush=True)

    # Excel出力
    members_df = pd.read_excel(
        r"C:\Users\fuji1\Downloads\音声分析.xlsx", sheet_name="ユーザー一覧"
    )
    email_to_name = {m["メール"]: m["メール"].split("@")[0] for _, m in members_df.iterrows()}

    excel_path = OUTPUT_DIR / "zoom_analysis_v2_202601.xlsx"
    with pd.ExcelWriter(str(excel_path), engine="openpyxl") as writer:
        summary_rows = []
        existing_emails = set(target_df["メール"].unique())
        for email in target_df["メール"].unique():
            user_df = target_df[target_df["メール"] == email]
            summary_rows.append({
                "メール": email,
                "シート名": email_to_name.get(email, email.split("@")[0]),
                "分析対象数": len(user_df),
                "エラー数": sum(1 for r in user_df["分析結果"] if str(r).startswith("エラー")),
            })
        for _, m in members_df.iterrows():
            if m["メール"] not in existing_emails:
                summary_rows.append({
                    "メール": m["メール"],
                    "シート名": email_to_name.get(m["メール"], m["メール"].split("@")[0]),
                    "分析対象数": 0,
                    "エラー数": 0,
                })
        pd.DataFrame(summary_rows).to_excel(writer, sheet_name="サマリー", index=False)

        for email in target_df["メール"].unique():
            user_df = target_df[target_df["メール"] == email][
                ["ミーティング名", "開始日時", "時間(分)", "分析結果"]
            ].copy()
            user_df["分析結果"] = user_df["分析結果"].apply(
                lambda x: str(x) if len(str(x)) <= 32767
                else str(x)[:32700] + "\n\n...（セル文字数上限のため省略）"
            )
            sheet_name = email_to_name.get(email, email.split("@")[0])[:31]
            user_df.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"Excel出力: {excel_path}", flush=True)

    # HTML出力
    html_path = OUTPUT_DIR / "zoom_analysis_v2_202601.html"
    generate_html(target_df, email_to_name, html_path)

    if error_count == 0 and PROGRESS_FILE.exists():
        PROGRESS_FILE.unlink()
        print("プログレスファイル削除済み", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
