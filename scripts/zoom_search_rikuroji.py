"""
リクロジ アポ先 × Zoom録画 マッチングスクリプト

1. 全Zoomユーザーの録画一覧を取得（2025/04〜現在）
2. ミーティングトピックとリクロジ会社名を部分一致マッチング
3. マッチした録画のリンク一覧化＋文字起こしダウンロード

途中再開対応:
- ユーザーごとの録画データをJSONキャッシュに保存
- 再実行時は取得済みユーザーをスキップ
- --reset オプションでキャッシュをクリアして最初からやり直し
"""

import csv
import io
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import pandas as pd
import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / "config" / ".env")

ZOOM_ACCOUNT_ID = os.environ["ZOOM_ACCOUNT_ID"]
ZOOM_CLIENT_ID = os.environ["ZOOM_CLIENT_ID"]
ZOOM_CLIENT_SECRET = os.environ["ZOOM_CLIENT_SECRET"]
ZOOM_API_BASE = "https://api.zoom.us/v2"

OUTPUT_DIR = PROJECT_ROOT / "data" / "output" / "zoom_rikuroji"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
TRANSCRIPT_DIR = OUTPUT_DIR / "transcripts"
TRANSCRIPT_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR = OUTPUT_DIR / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

FROM_DATE = "2025-04-01"
TO_DATE = datetime.now().strftime("%Y-%m-%d")


def get_access_token() -> str:
    resp = requests.post(
        "https://zoom.us/oauth/token",
        params={"grant_type": "account_credentials", "account_id": ZOOM_ACCOUNT_ID},
        auth=(ZOOM_CLIENT_ID, ZOOM_CLIENT_SECRET),
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def get_all_users(token: str) -> list[dict]:
    headers = {"Authorization": f"Bearer {token}"}
    all_users = []
    next_page = ""
    while True:
        params = {"page_size": 300}
        if next_page:
            params["next_page_token"] = next_page
        resp = requests.get(f"{ZOOM_API_BASE}/users", headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        all_users.extend(data["users"])
        next_page = data.get("next_page_token", "")
        if not next_page:
            break
    return all_users


def get_user_recordings(token: str, user_id: str, from_date: str, to_date: str) -> list[dict]:
    """ユーザーの録画一覧取得（30日制限のためループ）"""
    headers = {"Authorization": f"Bearer {token}"}
    all_meetings = []

    start = datetime.strptime(from_date, "%Y-%m-%d")
    end = datetime.strptime(to_date, "%Y-%m-%d")

    current = start
    while current < end:
        chunk_end = min(current + timedelta(days=29), end)
        next_page = ""
        while True:
            params = {
                "from": current.strftime("%Y-%m-%d"),
                "to": chunk_end.strftime("%Y-%m-%d"),
                "page_size": 300,
            }
            if next_page:
                params["next_page_token"] = next_page

            try:
                resp = requests.get(
                    f"{ZOOM_API_BASE}/users/{user_id}/recordings",
                    headers=headers, params=params, timeout=60,
                )
                if resp.status_code == 404:
                    break
                if resp.status_code == 429:
                    print("    レート制限 - 10秒待機...")
                    time.sleep(10)
                    continue
                resp.raise_for_status()
                data = resp.json()
                all_meetings.extend(data.get("meetings", []))
                next_page = data.get("next_page_token", "")
                if not next_page:
                    break
            except requests.exceptions.RequestException as e:
                print(f"    API エラー: {e}")
                break

        current = chunk_end + timedelta(days=1)

    return all_meetings


def save_user_cache(email: str, meetings: list[dict]):
    """ユーザーの録画データをキャッシュに保存"""
    safe_email = email.replace("@", "_at_").replace(".", "_")
    cache_file = CACHE_DIR / f"{safe_email}.json"
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(meetings, f, ensure_ascii=False, default=str)


def load_user_cache(email: str) -> list[dict] | None:
    """キャッシュからユーザーの録画データを読み込み"""
    safe_email = email.replace("@", "_at_").replace(".", "_")
    cache_file = CACHE_DIR / f"{safe_email}.json"
    if cache_file.exists():
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def get_cached_emails() -> set[str]:
    """キャッシュ済みのメールアドレス一覧を取得"""
    cached = set()
    for f in CACHE_DIR.glob("*.json"):
        email = f.stem.replace("_at_", "@").replace("_", ".")
        cached.add(email)
    return cached


def download_transcript(token: str, download_url: str) -> str | None:
    try:
        resp = requests.get(
            download_url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=120,
        )
        if resp.status_code == 200:
            return resp.text
    except Exception as e:
        print(f"    文字起こしDLエラー: {e}")
    return None


def vtt_to_plain_text(vtt_content: str) -> str:
    lines = vtt_content.strip().split("\n")
    result = []
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if "-->" in line:
            timestamp = line.split("-->")[0].strip()
            text_lines = []
            i += 1
            while i < len(lines) and lines[i].strip() and "-->" not in lines[i]:
                text_lines.append(lines[i].strip())
                i += 1
            if text_lines:
                result.append(f"[{timestamp}] {' '.join(text_lines)}")
            continue
        i += 1
    return "\n".join(result)


def sanitize_filename(name: str) -> str:
    name = re.sub(r'[<>:"/\\|?*\t\n\r\x00-\x1f]', '_', name)
    name = re.sub(r'_+', '_', name)
    return name.strip('_ ')[:100]


def normalize_for_match(text: str) -> str:
    import unicodedata
    text = unicodedata.normalize('NFKC', text)
    text = re.sub(r'[\s\u3000]+', '', text)
    text = re.sub(r'[‐−–—ー―]+', '-', text)
    return text.lower()


CORP_TYPES = ['株式会社', '有限会社', '合同会社', '一般社団法人', '社会福祉法人',
              '医療法人', '合資会社', '特定非営利活動法人', '公益社団法人',
              '学校法人', '一般財団法人', '公益財団法人', '協同組合', '農業協同組合']


def extract_core(name: str) -> str:
    for corp in CORP_TYPES:
        name = name.replace(corp, '')
    name = re.sub(r'[（(][^）)]*[）)]', '', name)
    return name.strip()


def load_company_names() -> list[dict]:
    """リクロジ統合結果からユニーク会社名を読み込み"""
    path = Path(r"C:\Users\fuji1\Downloads\リクロジ_アポ先_マッチ結果_統合.xlsx")
    df = pd.read_excel(path, sheet_name="統合結果")
    companies = []
    seen = set()
    for _, row in df.iterrows():
        name = row.get("会社名")
        if pd.isna(name):
            continue
        name = str(name).strip()
        if name in seen:
            continue
        seen.add(name)

        clean = re.sub(r'^\d{4}/\d{1,2}/\d{1,2}_', '', name)
        clean = re.sub(r'_\d{4}/\d{1,2}/\d{1,2}$', '', clean)
        clean = re.sub(r'[\s\u3000]+', '', clean)
        core = extract_core(clean)
        norm = normalize_for_match(core)

        branch_patterns = ['工場', '事業所', '営業所', '支店', '支社', '出張所',
                           'センター', '事業部', '本社', '本店', '物流', 'dc']
        core_no_branch = norm
        for bp in branch_patterns:
            core_no_branch = core_no_branch.replace(bp, '')
        core_no_branch = core_no_branch.strip()

        companies.append({
            "original": name,
            "clean": clean,
            "core": core,
            "norm": norm,
            "norm_no_branch": core_no_branch if core_no_branch != norm else "",
        })
    return companies


def match_topic_to_company(topic: str, companies: list[dict]) -> list[dict]:
    """ミーティングトピックとリクロジ会社名をマッチング"""
    topic_norm = normalize_for_match(topic)
    topic_core = normalize_for_match(extract_core(topic))
    matches = []

    for comp in companies:
        if len(comp["norm"]) < 3:
            continue

        if comp["norm"] in topic_norm or comp["norm"] in topic_core:
            matches.append(comp)
            continue
        if topic_core in comp["norm"] and len(topic_core) >= 4:
            matches.append(comp)
            continue

        if comp["norm_no_branch"] and len(comp["norm_no_branch"]) >= 3:
            if comp["norm_no_branch"] in topic_norm:
                matches.append(comp)
                continue

    return matches


def main():
    reset_mode = "--reset" in sys.argv

    print("=" * 70)
    print("リクロジ アポ先 × Zoom録画 マッチング")
    print(f"対象期間: {FROM_DATE} ～ {TO_DATE}")
    if reset_mode:
        print("*** リセットモード: キャッシュをクリアして最初から ***")
        import shutil
        if CACHE_DIR.exists():
            shutil.rmtree(CACHE_DIR)
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
    print("=" * 70)

    # 会社名読み込み
    companies = load_company_names()
    print(f"リクロジ会社名: {len(companies)}社")

    # Zoom認証
    token = get_access_token()
    print("Zoomトークン取得OK")

    # 全ユーザー取得
    users = get_all_users(token)
    print(f"Zoomユーザー: {len(users)}名")

    # キャッシュ状況確認
    cached_emails = get_cached_emails()
    remaining = [u for u in users if u["email"] not in cached_emails]
    print(f"キャッシュ済み: {len(users) - len(remaining)}名, 残り: {len(remaining)}名\n")

    # Phase 1: 録画データ収集（キャッシュ未取得ユーザーのみ）
    if remaining:
        print(f"--- Phase 1: 録画データ収集 ({len(remaining)}名) ---")
        for idx, user in enumerate(remaining):
            email = user["email"]
            user_id = user["id"]
            global_idx = users.index(user) + 1
            print(f"[{global_idx}/{len(users)}] {email}...", end="", flush=True)

            meetings = get_user_recordings(token, user_id, FROM_DATE, TO_DATE)
            for m in meetings:
                m["_user_email"] = email

            # キャッシュ保存
            save_user_cache(email, meetings)

            if meetings:
                print(f" {len(meetings)}件 (保存済)")
            else:
                print(" 0件 (保存済)")

            # レート制限対策
            if (idx + 1) % 10 == 0:
                time.sleep(1)

        print(f"\nPhase 1 完了: 全ユーザーの録画データ取得完了\n")
    else:
        print("全ユーザーのキャッシュが存在します。Phase 1 スキップ。\n")

    # Phase 2: キャッシュから全ミーティングを読み込み
    print("--- Phase 2: マッチング ---")
    all_meetings = []
    for user in users:
        email = user["email"]
        cached = load_user_cache(email)
        if cached:
            all_meetings.extend(cached)

    print(f"録画ミーティング総数: {len(all_meetings)}")

    # マッチング
    matched_results = []
    matched_meeting_ids = set()

    for meeting in all_meetings:
        topic = meeting.get("topic", "")
        if not topic:
            continue

        matches = match_topic_to_company(topic, companies)
        if matches:
            meeting_id = meeting.get("uuid", meeting.get("id", ""))
            if meeting_id in matched_meeting_ids:
                continue
            matched_meeting_ids.add(meeting_id)

            recording_files = meeting.get("recording_files", [])
            video_urls = []
            transcript_url = None
            for rf in recording_files:
                if rf.get("file_type") in ("MP4", "M4A") and rf.get("status") == "completed":
                    play_url = rf.get("play_url", "")
                    download_url = rf.get("download_url", "")
                    video_urls.append(play_url or download_url)
                if rf.get("file_type") == "TRANSCRIPT" and rf.get("status") == "completed":
                    transcript_url = rf.get("download_url", "")

            share_url = meeting.get("share_url", "")

            for comp in matches:
                matched_results.append({
                    "リクロジ会社名": comp["original"],
                    "Zoomトピック": topic,
                    "開始日時": meeting.get("start_time", ""),
                    "時間(分)": meeting.get("duration", 0),
                    "ホスト": meeting.get("_user_email", ""),
                    "共有リンク": share_url,
                    "動画URL": "; ".join(video_urls) if video_urls else "",
                    "文字起こしあり": "○" if transcript_url else "",
                    "_transcript_url": transcript_url,
                    "_meeting_uuid": meeting.get("uuid", ""),
                })

    print(f"マッチした録画: {len(matched_results)}件")
    print(f"マッチしたユニーク会社数: {len(set(r['リクロジ会社名'] for r in matched_results))}")

    # Phase 3: 文字起こしダウンロード（未DL分のみ）
    print(f"\n--- Phase 3: 文字起こしダウンロード ---")
    transcript_count = 0
    skipped_count = 0
    for result in matched_results:
        t_url = result["_transcript_url"]
        if not t_url:
            continue

        topic = result["Zoomトピック"]
        date_str = result["開始日時"][:10] if result["開始日時"] else "unknown"
        safe_name = sanitize_filename(f"{date_str}_{topic}")

        txt_path = TRANSCRIPT_DIR / f"{safe_name}.txt"
        if txt_path.exists():
            result["文字起こしファイル"] = str(txt_path.name)
            skipped_count += 1
            continue

        vtt_content = download_transcript(token, t_url)
        if vtt_content:
            vtt_path = TRANSCRIPT_DIR / f"{safe_name}.vtt"
            vtt_path.write_text(vtt_content, encoding="utf-8")

            plain = vtt_to_plain_text(vtt_content)
            txt_path.write_text(plain, encoding="utf-8")

            result["文字起こしファイル"] = str(txt_path.name)
            transcript_count += 1
            print(f"  ✓ {safe_name}")

            time.sleep(0.5)

    print(f"新規DL: {transcript_count}件, スキップ(DL済): {skipped_count}件")

    # Phase 4: 結果出力
    print(f"\n--- Phase 4: 結果出力 ---")
    output_cols = ["リクロジ会社名", "Zoomトピック", "開始日時", "時間(分)",
                   "ホスト", "共有リンク", "動画URL", "文字起こしあり", "文字起こしファイル"]
    for r in matched_results:
        r.setdefault("文字起こしファイル", "")

    csv_path = OUTPUT_DIR / "zoom_rikuroji_match.csv"
    with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=output_cols, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(matched_results)

    xlsx_path = OUTPUT_DIR / "zoom_rikuroji_match.xlsx"
    df_out = pd.DataFrame(matched_results)[output_cols]
    df_out.to_excel(xlsx_path, index=False)

    print("\n" + "=" * 70)
    print("完了サマリー")
    print("=" * 70)
    print(f"Zoomユーザー: {len(users)}名")
    print(f"録画ミーティング総数: {len(all_meetings)}")
    print(f"マッチした録画: {len(matched_results)}件")
    print(f"マッチしたユニーク会社: {len(set(r['リクロジ会社名'] for r in matched_results))}社")
    print(f"文字起こしDL: {transcript_count}件 (スキップ: {skipped_count}件)")
    print(f"結果CSV: {csv_path}")
    print(f"結果Excel: {xlsx_path}")
    print(f"文字起こし: {TRANSCRIPT_DIR}")
    print(f"\n※ 再実行時はキャッシュ済みユーザーをスキップします")
    print(f"※ 最初からやり直す場合: python {Path(__file__).name} --reset")


if __name__ == "__main__":
    main()
