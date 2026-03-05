"""
Zoom録画文字起こしダウンロードスクリプト

2026年1月のミーティング録画からTRANSCRIPT（VTT）をダウンロードし、
CSV一覧 + メンバー別テキストファイルとして出力する。
"""

import csv
import io
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

# Windows cp932対策
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import pandas as pd
import requests
from dotenv import load_dotenv

# プロジェクトルート
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# .env読み込み
load_dotenv(PROJECT_ROOT / "config" / ".env")

ZOOM_ACCOUNT_ID = os.environ["ZOOM_ACCOUNT_ID"]
ZOOM_CLIENT_ID = os.environ["ZOOM_CLIENT_ID"]
ZOOM_CLIENT_SECRET = os.environ["ZOOM_CLIENT_SECRET"]
ZOOM_API_BASE = "https://api.zoom.us/v2"

# 出力先
OUTPUT_DIR = PROJECT_ROOT / "data" / "output" / "zoom_transcripts"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# 対象期間
FROM_DATE = "2026-01-01"
TO_DATE = "2026-01-31"


def get_access_token() -> str:
    """Server-to-Server OAuthでアクセストークンを取得"""
    resp = requests.post(
        "https://zoom.us/oauth/token",
        params={
            "grant_type": "account_credentials",
            "account_id": ZOOM_ACCOUNT_ID,
        },
        auth=(ZOOM_CLIENT_ID, ZOOM_CLIENT_SECRET),
        timeout=30,
    )
    resp.raise_for_status()
    token = resp.json()["access_token"]
    print(f"アクセストークン取得成功")
    return token


def get_user_recordings(token: str, user_email: str) -> list[dict]:
    """ユーザーの録画一覧を取得（ページネーション対応）"""
    headers = {"Authorization": f"Bearer {token}"}
    all_meetings = []
    next_page_token = ""

    while True:
        params = {
            "from": FROM_DATE,
            "to": TO_DATE,
            "page_size": 300,
        }
        if next_page_token:
            params["next_page_token"] = next_page_token

        resp = requests.get(
            f"{ZOOM_API_BASE}/users/{user_email}/recordings",
            headers=headers,
            params=params,
            timeout=60,
        )

        if resp.status_code == 404:
            print(f"  ユーザー未検出: {user_email}")
            return []
        if resp.status_code == 429:
            print(f"  レート制限 - 10秒待機...")
            time.sleep(10)
            continue

        resp.raise_for_status()
        data = resp.json()
        meetings = data.get("meetings", [])
        all_meetings.extend(meetings)

        next_page_token = data.get("next_page_token", "")
        if not next_page_token:
            break

    return all_meetings


def download_transcript(token: str, download_url: str) -> str | None:
    """文字起こしVTTファイルをダウンロード"""
    resp = requests.get(
        download_url,
        headers={"Authorization": f"Bearer {token}"},
        timeout=120,
    )
    if resp.status_code == 200:
        return resp.text
    print(f"  ダウンロード失敗: status={resp.status_code}")
    return None


def vtt_to_plain_text(vtt_content: str) -> str:
    """VTT形式からタイムスタンプ付きプレーンテキストに変換"""
    lines = vtt_content.strip().split("\n")
    result = []
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        # タイムスタンプ行を検出
        if "-->" in line:
            timestamp = line.split("-->")[0].strip()
            # 次の行がテキスト
            text_lines = []
            i += 1
            while i < len(lines) and lines[i].strip() and "-->" not in lines[i]:
                text_lines.append(lines[i].strip())
                i += 1
            if text_lines:
                text = " ".join(text_lines)
                result.append(f"[{timestamp}] {text}")
            continue
        i += 1
    return "\n".join(result)


def sanitize_filename(name: str) -> str:
    """ファイル名に使えない文字を除去"""
    return re.sub(r'[<>:"/\\|?*]', '_', name)[:100]


def main():
    print("=" * 60)
    print("Zoom文字起こしダウンロード")
    print(f"対象期間: {FROM_DATE} ～ {TO_DATE}")
    print("=" * 60)

    # メンバー一覧読み込み
    members_file = Path(r"C:\Users\fuji1\Downloads\音声分析.xlsx")
    df = pd.read_excel(members_file, sheet_name="ユーザー一覧")
    print(f"対象メンバー: {len(df)}名\n")

    # トークン取得
    token = get_access_token()

    # CSV出力用
    csv_rows = []
    total_transcripts = 0
    total_meetings = 0

    for _, row in df.iterrows():
        email = row["メール"]
        user_id = row["ZoomユーザーID"]
        print(f"\n--- {email} ---")

        meetings = get_user_recordings(token, email)
        print(f"  録画ミーティング数: {len(meetings)}")
        total_meetings += len(meetings)

        # メンバー別フォルダ
        member_dir = OUTPUT_DIR / sanitize_filename(email.split("@")[0])
        member_dir.mkdir(parents=True, exist_ok=True)

        for meeting in meetings:
            topic = meeting.get("topic", "不明")
            start_time = meeting.get("start_time", "")
            duration = meeting.get("duration", 0)
            meeting_id = meeting.get("id", "")

            recording_files = meeting.get("recording_files", [])
            for rf in recording_files:
                if rf.get("file_type") == "TRANSCRIPT" and rf.get("status") == "completed":
                    download_url = rf.get("download_url", "")
                    if not download_url:
                        continue

                    vtt_content = download_transcript(token, download_url)
                    if not vtt_content:
                        continue

                    total_transcripts += 1
                    plain_text = vtt_to_plain_text(vtt_content)

                    # ファイル名: 商談日_商談名
                    date_str = start_time[:10] if start_time else "unknown"
                    time_str = start_time[11:16].replace(":", "") if len(start_time) > 15 else ""
                    safe_topic = sanitize_filename(topic)
                    base_name = f"{date_str}_{time_str}_{safe_topic}" if time_str else f"{date_str}_{safe_topic}"

                    # VTTファイル保存
                    vtt_path = member_dir / f"{base_name}.vtt"
                    vtt_path.write_text(vtt_content, encoding="utf-8")

                    # プレーンテキスト保存
                    txt_path = member_dir / f"{base_name}.txt"
                    txt_path.write_text(plain_text, encoding="utf-8")

                    # CSV行追加
                    csv_rows.append({
                        "メール": email,
                        "ミーティングID": meeting_id,
                        "ミーティング名": topic,
                        "開始日時": start_time,
                        "時間(分)": duration,
                        "文字起こしファイル": str(txt_path.relative_to(OUTPUT_DIR)),
                        "テキスト(先頭500文字)": plain_text[:500],
                    })

                    print(f"  ✓ {date_str} {topic}")

        # レート制限対策
        time.sleep(1)

    # CSV出力
    csv_path = OUTPUT_DIR / f"zoom_transcripts_{FROM_DATE}_{TO_DATE}.csv"
    with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "メール", "ミーティングID", "ミーティング名",
            "開始日時", "時間(分)", "文字起こしファイル", "テキスト(先頭500文字)",
        ])
        writer.writeheader()
        writer.writerows(csv_rows)

    print("\n" + "=" * 60)
    print("完了サマリー")
    print("=" * 60)
    print(f"対象メンバー: {len(df)}名")
    print(f"ミーティング総数: {total_meetings}")
    print(f"文字起こし取得数: {total_transcripts}")
    print(f"CSV出力: {csv_path}")
    print(f"テキスト出力先: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
