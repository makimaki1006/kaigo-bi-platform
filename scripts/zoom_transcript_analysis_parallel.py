"""
Zoom商談文字起こし失注分析スクリプト（並列版）

Gemini 2.0 Flashを使って商談ログを並列分析。
途中結果を逐次保存し、中断しても再開可能。
"""

import asyncio
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
PROGRESS_FILE = OUTPUT_DIR / "_analysis_progress.json"
MAX_CONCURRENT = 5

EXCLUDE_KEYWORDS = [
    "ロープレ", "練習", "パーソナルミーティング", "打ち合わせ",
    "お打ち合わせ", "MTG", "YMCX", "Zoom Meeting", "Zoom ミーティング",
    "[New Zoom Meeting]",
]

ANALYSIS_PROMPT = """【指示】
あなたは、採用支援サービス「Medica（メディカ）」の教育責任者です。
提示する商談ログを分析し、新人メンバーが**「どのパスを見逃し、なぜ失注（見送り・競合負け）に至ったのか」**を特定するため、以下の7つの項目に整理して出力してください。特に「3」と「5」は、創作をせずログから営業担当者の実際の発言（またはスルーした事実）をそのまま抽出してください。

【分析項目】
1. 顧客の事実（パス）
顧客が語った現状（募集背景、欠員状況、既存手法への不満、期限など）を抽出してください。
2. 見逃した「真の痛み」と「実務のボトルネック」
・見逃した痛み： 顧客の発言の裏にあったはずの経営・現場リスク（離職連鎖、コスト損失等）。
・放置したボトルネック： 顧客が「できない・面倒だ」と漏らした実務上の課題（相場調査、求人更新、決済資料等）で、営業が拾いきれなかったものを特定してください。
3. 【実際のトーク】課題に対して「スルー」または「引いた」発言
ログの中から、顧客が課題や懸念を口にした際、担当者がどう返したか。深く踏み込めなかった、あるいは話題を変えてしまった箇所をそのまま抽出してください。

4. 本来提示すべきだったMedicaによる解決
顧客のボトルネックに対し、Medicaが「プロの労働力」としてどう介入すべきだったかを定義してください。
5. 【実際のトーク】響かなかった（あるいは弱かった）提案内容
ログの中から、Medicaの価値を提案した際の実際の発言を抽出してください。
6. 「Why Change（なぜ変えるか）」の構築失敗
なぜ顧客は「今のやり方のままでいい（現状維持）」、あるいは「変えるほどではない」と判断してしまったのか。危機感の醸成に失敗した原因を分析してください。

7. 「Why Medica（なぜMedicaか）」の差別化失敗
なぜ「紹介会社」「他社媒体」「自社運用」と比較して、Medicaが選ばれなかったのか。顧客にとってMedicaが「他と変わらない選択肢」になってしまった要因を特定してください。

【出力形式】
テーブル（表形式）で出力してください。
表の後に、**「売れているメンバー（服部・深堀）なら、この分岐点で何と言って、どう主導権を握り直したか」**の具体的リテイク（録り直し）トークを添えてください。

【商談ログ】
"""


def load_progress() -> dict:
    """途中結果を読み込み"""
    if PROGRESS_FILE.exists():
        return json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
    return {}


def save_progress(progress: dict):
    """途中結果を保存"""
    PROGRESS_FILE.write_text(json.dumps(progress, ensure_ascii=False), encoding="utf-8")


async def analyze_one(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    idx: int,
    total: int,
    key: str,
    topic: str,
    transcript: str,
    progress: dict,
    done_count: list,
) -> tuple[str, str]:
    """1件の商談を分析"""
    # 既に完了済みならスキップ
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
            "generationConfig": {"temperature": 0.3, "maxOutputTokens": 4096},
        }

        for attempt in range(10):
            try:
                async with session.post(
                    GEMINI_URL, json=payload,
                    timeout=aiohttp.ClientTimeout(total=120),
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
        else:
            result = "エラー: リトライ上限"

        done_count[0] += 1
        ok = "OK" if not result.startswith("エラー") else "NG"
        print(f"  [{done_count[0]}/{total}] {ok} {topic[:50]}", flush=True)

        # 逐次保存
        progress[key] = result
        if done_count[0] % 10 == 0:
            save_progress(progress)

        return key, result


async def main():
    print("=" * 60, flush=True)
    print("Zoom商談 失注分析（並列版 Gemini 2.0 Flash）", flush=True)
    print(f"並列数: {MAX_CONCURRENT}", flush=True)
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

    # 途中結果読み込み
    progress = load_progress()
    already_done = sum(1 for _, row in target_df.iterrows()
                       if f"{row['メール']}_{row['ミーティングID']}" in progress)
    remaining = len(target_df) - already_done
    print(f"全件: {len(csv_df)} / 除外: {excluded} / 分析対象: {len(target_df)}", flush=True)
    print(f"完了済み: {already_done} / 残り: {remaining}\n", flush=True)

    # 文字起こし読み込み
    transcripts = []
    keys = []
    for _, row in target_df.iterrows():
        txt_path = OUTPUT_DIR / row["文字起こしファイル"]
        if txt_path.exists():
            transcripts.append(txt_path.read_text(encoding="utf-8"))
        else:
            transcripts.append("")
        keys.append(f"{row['メール']}_{row['ミーティングID']}")

    # 並列分析
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    done_count = [0]
    start_time = time.time()

    async with aiohttp.ClientSession() as session:
        tasks = [
            analyze_one(
                session, semaphore,
                i + 1, len(target_df),
                keys[i],
                target_df.iloc[i]["ミーティング名"],
                transcripts[i],
                progress,
                done_count,
            )
            for i in range(len(target_df))
        ]
        results_pairs = await asyncio.gather(*tasks)

    # 最終保存
    for key, result in results_pairs:
        progress[key] = result
    save_progress(progress)

    elapsed = time.time() - start_time
    print(f"\n分析完了: {elapsed:.0f}秒 ({elapsed/60:.1f}分)", flush=True)

    # 結果をDataFrameに格納
    results = [progress.get(k, "エラー: 未処理") for k in keys]
    target_df["分析結果"] = results
    error_count = sum(1 for r in results if str(r).startswith("エラー"))
    print(f"成功: {len(results) - error_count} / エラー: {error_count}", flush=True)

    # Excel出力
    members_df = pd.read_excel(
        r"C:\Users\fuji1\Downloads\音声分析.xlsx", sheet_name="ユーザー一覧"
    )
    email_to_name = {m["メール"]: m["メール"].split("@")[0] for _, m in members_df.iterrows()}

    excel_path = OUTPUT_DIR / "zoom_analysis_202601_by_user.xlsx"
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

    print(f"\nExcel出力: {excel_path}", flush=True)

    # プログレスファイル削除
    if error_count == 0 and PROGRESS_FILE.exists():
        PROGRESS_FILE.unlink()
        print("プログレスファイル削除済み", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
