"""
Zoom商談文字起こし失注分析スクリプト（同期版）

Gemini 2.0 Flashを使って商談ログを分析し、
敗因特定プロンプトに基づく分析結果をExcelに出力する。
"""

import io
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


def analyze_one(idx: int, total: int, topic: str, transcript: str) -> str:
    """1件の商談を分析"""
    max_chars = 200000
    if len(transcript) > max_chars:
        transcript = transcript[:max_chars] + "\n\n...（以降省略）"

    payload = {
        "contents": [{"parts": [{"text": ANALYSIS_PROMPT + transcript}]}],
        "generationConfig": {"temperature": 0.3, "maxOutputTokens": 4096},
    }

    for attempt in range(5):
        try:
            resp = requests.post(GEMINI_URL, json=payload, timeout=120)
            if resp.status_code == 429:
                wait = 15 * (attempt + 1)
                print(f"  [{idx}/{total}] レート制限 - {wait}秒待機...", flush=True)
                time.sleep(wait)
                continue
            if resp.status_code != 200:
                print(f"  [{idx}/{total}] エラー status={resp.status_code}: {resp.text[:200]}", flush=True)
                return f"エラー: status={resp.status_code}"

            data = resp.json()
            candidates = data.get("candidates", [])
            if candidates:
                parts = candidates[0].get("content", {}).get("parts", [])
                result = "".join(p.get("text", "") for p in parts)
                print(f"  [{idx}/{total}] OK {topic[:50]}", flush=True)
                return result
            return "エラー: 応答なし"
        except requests.exceptions.Timeout:
            print(f"  [{idx}/{total}] タイムアウト (試行{attempt+1})", flush=True)
            if attempt == 4:
                return "エラー: タイムアウト"
            time.sleep(5)
        except Exception as e:
            print(f"  [{idx}/{total}] 例外: {e}", flush=True)
            if attempt == 4:
                return f"エラー: {e}"
            time.sleep(5)

    return "エラー: リトライ上限"


def main():
    print("=" * 60, flush=True)
    print("Zoom商談 失注分析（Gemini 2.0 Flash）", flush=True)
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
    print(f"全件: {len(csv_df)} / 除外: {excluded} / 分析対象: {len(target_df)}\n", flush=True)

    # 文字起こし読み込み
    transcripts = []
    for _, row in target_df.iterrows():
        txt_path = OUTPUT_DIR / row["文字起こしファイル"]
        if txt_path.exists():
            transcripts.append(txt_path.read_text(encoding="utf-8"))
        else:
            transcripts.append("")

    # 逐次分析
    start_time = time.time()
    results = []
    for i in range(len(target_df)):
        result = analyze_one(
            i + 1, len(target_df),
            target_df.iloc[i]["ミーティング名"],
            transcripts[i],
        )
        results.append(result)
        time.sleep(1)  # レート制限対策

    elapsed = time.time() - start_time
    print(f"\n分析完了: {elapsed:.0f}秒 ({elapsed/60:.1f}分)", flush=True)

    target_df["分析結果"] = results
    error_count = sum(1 for r in results if r.startswith("エラー"))
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
                "エラー数": sum(1 for r in user_df["分析結果"] if r.startswith("エラー")),
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
                lambda x: x if len(str(x)) <= 32767
                else x[:32700] + "\n\n...（セル文字数上限のため省略）"
            )
            sheet_name = email_to_name.get(email, email.split("@")[0])[:31]
            user_df.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"\nExcel出力: {excel_path}", flush=True)


if __name__ == "__main__":
    main()
