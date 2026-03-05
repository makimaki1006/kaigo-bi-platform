# -*- coding: utf-8 -*-
"""全営業メンバー比較分析
2024年前半 vs 2026年1月 の行動指標比較
+ 各期間内のトップ vs ボトムパフォーマー比較
"""
import io
import json
import math
import os
import re
import sys
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
load_dotenv(PROJECT_ROOT / "config" / ".env")

ZOOM_ACCOUNT_ID = os.environ["ZOOM_ACCOUNT_ID"]
ZOOM_CLIENT_ID = os.environ["ZOOM_CLIENT_ID"]
ZOOM_CLIENT_SECRET = os.environ["ZOOM_CLIENT_SECRET"]
ZOOM_API_BASE = "https://api.zoom.us/v2"

OUTPUT_DIR = PROJECT_ROOT / "data" / "output" / "team_comparison"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# 期間定義
PERIODS = {
    "2024h1": {
        "label": "2024年前半（1-6月）",
        "ranges": [
            ("2024-01-01", "2024-01-31"),
            ("2024-02-01", "2024-02-29"),
            ("2024-03-01", "2024-03-31"),
            ("2024-04-01", "2024-04-30"),
            ("2024-05-01", "2024-05-31"),
            ("2024-06-01", "2024-06-30"),
        ],
    },
    "2026jan": {
        "label": "2026年1月",
        "ranges": [("2026-01-01", "2026-01-31")],
    },
}

EXCLUDE_KEYWORDS = [
    "ロープレ", "roleplay", "RP", "role play",
    "研修", "MTG", "定例", "朝会", "勉強会",
    "VS", "vs", "パーソナルミーティング",
    "YMCX", "Young Man", "新人", "組み手",
    "1on1", "振り返り", "共有会", "報告会",
    "Zoom ミーティング", "New Zoom Meeting",
    "Personal Meeting Room", "テスト", "test",
]

LINE_RE = re.compile(r"\[(\d{2}):(\d{2}):(\d{2})\.\d+\]\s+(.+?):\s+(.*)")


# === Zoom API ===
def get_zoom_token():
    resp = requests.post(
        "https://zoom.us/oauth/token",
        params={"grant_type": "account_credentials", "account_id": ZOOM_ACCOUNT_ID},
        auth=(ZOOM_CLIENT_ID, ZOOM_CLIENT_SECRET),
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def list_zoom_users(token):
    """全Zoomユーザーを取得"""
    headers = {"Authorization": f"Bearer {token}"}
    users = []
    next_page = ""
    while True:
        params = {"page_size": 300, "status": "active"}
        if next_page:
            params["next_page_token"] = next_page
        resp = requests.get(f"{ZOOM_API_BASE}/users", headers=headers, params=params, timeout=60)
        if resp.status_code == 429:
            time.sleep(10)
            continue
        resp.raise_for_status()
        data = resp.json()
        users.extend(data.get("users", []))
        next_page = data.get("next_page_token", "")
        if not next_page:
            break
    return users


def get_recordings(token, email, from_date, to_date):
    headers = {"Authorization": f"Bearer {token}"}
    meetings = []
    next_page = ""
    while True:
        params = {"from": from_date, "to": to_date, "page_size": 300}
        if next_page:
            params["next_page_token"] = next_page
        resp = requests.get(
            f"{ZOOM_API_BASE}/users/{email}/recordings",
            headers=headers, params=params, timeout=60,
        )
        if resp.status_code == 429:
            time.sleep(10)
            continue
        if resp.status_code == 404:
            return []
        resp.raise_for_status()
        data = resp.json()
        meetings.extend(data.get("meetings", []))
        next_page = data.get("next_page_token", "")
        if not next_page:
            break
    return meetings


def download_transcript(token, download_url):
    try:
        resp = requests.get(download_url, headers={"Authorization": f"Bearer {token}"}, timeout=120)
        return resp.text if resp.status_code == 200 else None
    except Exception:
        return None


def vtt_to_text(vtt):
    lines = vtt.strip().split("\n")
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


def is_excluded(topic):
    if not topic:
        return True
    return any(kw.lower() in topic.lower() for kw in EXCLUDE_KEYWORDS)


def sanitize(name):
    return re.sub(r'[<>:"/\\|?*]', '_', name)[:80]


# === 行動指標分析 ===
def parse_seconds(h, m, s):
    return int(h) * 3600 + int(m) * 60 + int(s)


def analyze_transcript_text(text, host_name=""):
    """テキストから行動指標を計算。host_nameで話者を識別"""
    entries = []
    for line in text.strip().split("\n"):
        m = LINE_RE.match(line.strip())
        if m:
            sec = parse_seconds(m.group(1), m.group(2), m.group(3))
            speaker = m.group(4)
            entries.append({
                "sec": sec, "speaker": speaker,
                "text": m.group(5),
            })

    if len(entries) < 15:
        return None

    # ホスト（営業）の識別: host_nameで部分一致、またはmost frequent speaker
    speaker_counts = Counter(e["speaker"] for e in entries)

    host_speaker = None
    if host_name:
        # host_nameの各部分で一致を試みる
        name_parts = host_name.replace("　", " ").split()
        for speaker in speaker_counts:
            for part in name_parts:
                if len(part) >= 2 and part in speaker:
                    host_speaker = speaker
                    break
            if host_speaker:
                break

    if not host_speaker:
        # 最も発言の多い人をホスト扱い
        host_speaker = speaker_counts.most_common(1)[0][0]

    for e in entries:
        e["is_host"] = e["speaker"] == host_speaker

    host_lines = [e for e in entries if e["is_host"]]
    cust_lines = [e for e in entries if not e["is_host"]]
    total_chars = sum(len(e["text"]) for e in entries)
    host_chars = sum(len(e["text"]) for e in host_lines)
    cust_chars = sum(len(e["text"]) for e in cust_lines)
    duration = (entries[-1]["sec"] - entries[0]["sec"]) / 60 if len(entries) > 1 else 1
    if duration < 1:
        duration = 1
    start = entries[0]["sec"]

    # 質問
    host_q = sum(1 for e in host_lines if "？" in e["text"] or "?" in e["text"])
    cust_q = sum(1 for e in cust_lines if "？" in e["text"] or "?" in e["text"])

    # モノローグ
    monos = []
    streak_chars = 0
    for e in entries:
        if e["is_host"]:
            streak_chars += len(e["text"])
        else:
            if streak_chars > 0:
                monos.append(streak_chars)
            streak_chars = 0
    if streak_chars > 0:
        monos.append(streak_chars)

    # ターン
    turns = 0
    prev = None
    for e in entries:
        if prev is not None and e["is_host"] != prev:
            turns += 1
        prev = e["is_host"]

    # 冒頭3分
    f3 = [e for e in entries if e["sec"] - start <= 180]
    f3_host_c = sum(len(e["text"]) for e in f3 if e["is_host"])
    f3_total_c = sum(len(e["text"]) for e in f3)
    f3_host_q = sum(1 for e in f3 if e["is_host"] and ("？" in e["text"] or "?" in e["text"]))

    # 数字使用
    num_pat = re.compile(r'\d+')
    host_nums = sum(len(num_pat.findall(e["text"])) for e in host_lines)

    # 顧客エンゲージメント
    cust_long = sum(1 for e in cust_lines if len(e["text"]) >= 30)
    cust_short = sum(1 for e in cust_lines if len(e["text"]) <= 10)

    # データ系キーワード
    data_keywords = ["調査", "市場", "人口", "競合", "施設", "検索", "データ", "エリア", "求人", "件数"]
    host_data_kw = sum(
        sum(e["text"].count(kw) for kw in data_keywords)
        for e in host_lines
    )

    # クロージング系
    close_keywords = ["キャンペーン", "割引", "限定", "今月", "期限", "損失", "機会"]
    host_close_kw = sum(
        sum(e["text"].count(kw) for kw in close_keywords)
        for e in host_lines
    )

    # SPIN: Problem質問
    p_keywords = ["課題", "困って", "問題", "悩み", "大変", "足りない", "できない", "難しい"]
    host_p_q = sum(
        1 for e in host_lines
        if ("？" in e["text"] or "?" in e["text"]) and any(kw in e["text"] for kw in p_keywords)
    )

    return {
        "duration": round(duration, 1),
        "total_lines": len(entries),
        "host_speaker": host_speaker,
        "host_char_ratio": round(host_chars / total_chars, 3) if total_chars else 0,
        "host_avg_len": round(host_chars / len(host_lines), 1) if host_lines else 0,
        "cust_avg_len": round(cust_chars / len(cust_lines), 1) if cust_lines else 0,
        "host_q": host_q,
        "host_q_per_min": round(host_q / duration, 2),
        "cust_q": cust_q,
        "max_mono": max(monos, default=0),
        "avg_mono": round(sum(monos) / len(monos), 1) if monos else 0,
        "long_mono": sum(1 for m in monos if m >= 200),
        "turns_per_min": round(turns / duration, 2),
        "f3_host_ratio": round(f3_host_c / f3_total_c, 3) if f3_total_c else 0,
        "f3_host_q": f3_host_q,
        "nums_per_min": round(host_nums / duration, 2),
        "cust_long_ratio": round(cust_long / len(cust_lines), 3) if cust_lines else 0,
        "cust_short_ratio": round(cust_short / len(cust_lines), 3) if cust_lines else 0,
        "data_kw_per_min": round(host_data_kw / duration, 2),
        "close_kw_total": host_close_kw,
        "problem_q": host_p_q,
    }


# === Phase 1: Transcript ダウンロード ===
def phase1_download():
    print("=" * 70)
    print("  Phase 1: 全メンバーのTranscriptダウンロード")
    print("=" * 70)

    token = get_zoom_token()
    users = list_zoom_users(token)
    print(f"\n  Zoomユーザー数: {len(users)}")

    # ユーザーリスト表示
    for u in sorted(users, key=lambda x: x.get("last_name", "")):
        name = f"{u.get('last_name', '')} {u.get('first_name', '')}".strip()
        print(f"    {u['email']:<40} {name}")

    # 各ユーザー×各期間でrecording取得
    download_plan = []

    for period_key, period_info in PERIODS.items():
        print(f"\n  --- {period_info['label']} ---")
        period_dir = OUTPUT_DIR / period_key
        period_dir.mkdir(exist_ok=True)

        for u in users:
            email = u["email"]
            name = f"{u.get('last_name', '')} {u.get('first_name', '')}".strip()
            user_dir = period_dir / sanitize(email.split("@")[0])
            user_dir.mkdir(exist_ok=True)

            total_recordings = 0
            total_transcripts = 0

            for from_d, to_d in period_info["ranges"]:
                try:
                    meetings = get_recordings(token, email, from_d, to_d)
                except Exception as e:
                    print(f"    [{email}] {from_d}~{to_d} エラー: {e}")
                    continue

                for mtg in meetings:
                    topic = mtg.get("topic", "")
                    if is_excluded(topic):
                        continue

                    total_recordings += 1
                    # Transcriptファイルを探す
                    for rf in mtg.get("recording_files", []):
                        if rf.get("file_type") == "TRANSCRIPT" and rf.get("status") == "completed":
                            dt = mtg.get("start_time", "")[:10]
                            tm = mtg.get("start_time", "")[11:16].replace(":", "")
                            fname = f"{dt}_{tm}_{sanitize(topic)}.txt"
                            fpath = user_dir / fname

                            if fpath.exists() and fpath.stat().st_size > 100:
                                total_transcripts += 1
                                continue

                            download_plan.append({
                                "email": email, "name": name,
                                "period": period_key, "topic": topic,
                                "date": dt, "time": tm,
                                "url": rf.get("download_url", ""),
                                "fpath": fpath,
                                "duration": mtg.get("duration", 0),
                            })
                            total_transcripts += 1

            if total_recordings > 0:
                print(f"    {name:<20} ({email}): 録画{total_recordings}件, Transcript{total_transcripts}件")

    # ダウンロード実行
    print(f"\n  新規ダウンロード: {len(download_plan)}件")
    if download_plan:
        token = get_zoom_token()  # リフレッシュ
        downloaded = 0
        for i, item in enumerate(download_plan):
            if i > 0 and i % 50 == 0:
                token = get_zoom_token()
                print(f"    ...{i}/{len(download_plan)} トークンリフレッシュ")

            vtt = download_transcript(token, item["url"])
            if vtt:
                text = vtt_to_text(vtt)
                lines = text.strip().split("\n")
                if len(lines) >= 15:
                    with open(item["fpath"], "w", encoding="utf-8") as f:
                        f.write(text)
                    downloaded += 1

            if i % 20 == 0 and i > 0:
                print(f"    ダウンロード進捗: {i}/{len(download_plan)} ({downloaded}件保存)")
                time.sleep(1)

        print(f"  ダウンロード完了: {downloaded}件")


# === Phase 2: 行動指標分析 ===
def phase2_analyze():
    print(f"\n\n{'=' * 70}")
    print("  Phase 2: 行動指標分析")
    print("=" * 70)

    results = {}  # {period: {email: [metrics...]}}

    for period_key in PERIODS:
        period_dir = OUTPUT_DIR / period_key
        if not period_dir.exists():
            continue

        results[period_key] = {}

        for user_dir in sorted(period_dir.iterdir()):
            if not user_dir.is_dir():
                continue

            email_prefix = user_dir.name
            txts = sorted(user_dir.glob("*.txt"))
            if not txts:
                continue

            metrics_list = []
            for fp in txts:
                text = fp.read_text(encoding="utf-8")
                m = analyze_transcript_text(text, host_name=email_prefix)
                if m:
                    m["file"] = fp.name
                    metrics_list.append(m)

            if metrics_list:
                results[period_key][email_prefix] = metrics_list
                print(f"  [{period_key}] {email_prefix}: {len(metrics_list)}件分析完了")

    # 結果保存
    with open(OUTPUT_DIR / "team_analysis_raw.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2, default=str)

    return results


# === Phase 3: 比較レポート ===
def phase3_report(results):
    print(f"\n\n{'=' * 70}")
    print("  Phase 3: 比較レポート生成")
    print("=" * 70)

    def avg(lst, key):
        vals = [x[key] for x in lst if x.get(key) is not None]
        return sum(vals) / len(vals) if vals else 0

    def stdev(vals):
        if len(vals) < 2:
            return 0
        m = sum(vals) / len(vals)
        return math.sqrt(sum((x - m) ** 2 for x in vals) / (len(vals) - 1))

    # メンバー別サマリー
    member_summary = {}  # {email: {period: {metric: avg}}}

    metric_keys = [
        "host_char_ratio", "host_avg_len", "cust_avg_len",
        "host_q_per_min", "cust_q", "nums_per_min",
        "avg_mono", "long_mono", "turns_per_min",
        "f3_host_ratio", "f3_host_q",
        "cust_long_ratio", "cust_short_ratio",
        "data_kw_per_min", "close_kw_total", "problem_q",
        "duration",
    ]
    metric_labels = {
        "host_char_ratio": "文字比率",
        "host_avg_len": "平均発話長",
        "cust_avg_len": "顧客平均発話長",
        "host_q_per_min": "質問/分",
        "cust_q": "顧客質問数",
        "nums_per_min": "数字使用/分",
        "avg_mono": "平均独白長",
        "long_mono": "200字超独白",
        "turns_per_min": "ターン/分",
        "f3_host_ratio": "冒頭3分比率",
        "f3_host_q": "冒頭3分質問",
        "cust_long_ratio": "顧客長い発話比率",
        "cust_short_ratio": "顧客相槌比率",
        "data_kw_per_min": "データKW/分",
        "close_kw_total": "クロージングKW",
        "problem_q": "P質問数",
        "duration": "商談時間",
    }

    for period_key, period_data in results.items():
        for email, metrics_list in period_data.items():
            if email not in member_summary:
                member_summary[email] = {}
            member_summary[email][period_key] = {
                "count": len(metrics_list),
                "metrics": {k: round(avg(metrics_list, k), 3) for k in metric_keys},
            }

    # === メンバー別一覧 ===
    print(f"\n\n  メンバー別商談数:")
    print(f"  {'メンバー':<25} {'2024H1件数':>12} {'2026Jan件数':>12}")
    print(f"  {'-'*52}")

    all_members = sorted(member_summary.keys())
    for email in all_members:
        h1 = member_summary[email].get("2024h1", {}).get("count", 0)
        jan = member_summary[email].get("2026jan", {}).get("count", 0)
        if h1 > 0 or jan > 0:
            print(f"  {email:<25} {h1:>12} {jan:>12}")

    # === 期間比較（全メンバー合算） ===
    print(f"\n\n  === 全メンバー合算: 期間別行動指標比較 ===\n")
    all_h1 = []
    all_jan = []
    for email, periods in member_summary.items():
        if "2024h1" in periods:
            for _ in range(periods["2024h1"]["count"]):
                all_h1.append(periods["2024h1"]["metrics"])
        if "2026jan" in periods:
            for _ in range(periods["2026jan"]["count"]):
                all_jan.append(periods["2026jan"]["metrics"])

    # 元の個別メトリクスを使う
    flat_h1 = []
    flat_jan = []
    for period_key, period_data in results.items():
        for email, metrics_list in period_data.items():
            if period_key == "2024h1":
                flat_h1.extend(metrics_list)
            else:
                flat_jan.extend(metrics_list)

    print(f"  商談数: 2024H1 = {len(flat_h1)}, 2026Jan = {len(flat_jan)}\n")
    print(f"  {'指標':<20} {'2024H1':>10} {'2026Jan':>10} {'差分':>10} {'変化率':>10}")
    print(f"  {'-'*65}")

    for key in metric_keys:
        h1_val = avg(flat_h1, key)
        jan_val = avg(flat_jan, key)
        diff = jan_val - h1_val
        pct = ((jan_val - h1_val) / h1_val * 100) if h1_val != 0 else 0
        sign = "+" if diff > 0 else ""
        label = metric_labels.get(key, key)
        print(f"  {label:<20} {h1_val:>10.3f} {jan_val:>10.3f} {sign}{diff:>9.3f} {sign}{pct:>8.1f}%")

    # === メンバー別比較（両期間にデータがある人のみ） ===
    print(f"\n\n  === メンバー別: 主要指標比較 ===\n")
    compare_keys = ["host_char_ratio", "nums_per_min", "f3_host_q", "data_kw_per_min", "problem_q", "cust_short_ratio"]
    compare_labels = ["文字比率", "数字/分", "冒頭3分Q", "データKW/分", "P質問", "顧客相槌率"]

    # ヘッダー
    header = f"  {'メンバー':<20} {'件数':>5}"
    for l in compare_labels:
        header += f" {l:>10}"
    print(header)
    print(f"  {'-'*90}")

    for period_key, period_label in [("2024h1", "--- 2024年前半 ---"), ("2026jan", "--- 2026年1月 ---")]:
        print(f"\n  {period_label}")
        period_members = []
        for email in all_members:
            if period_key in member_summary[email]:
                data = member_summary[email][period_key]
                period_members.append((email, data))

        # データKW/分でソート（降順）
        period_members.sort(key=lambda x: x[1]["metrics"].get("data_kw_per_min", 0), reverse=True)

        for email, data in period_members:
            row = f"  {email:<20} {data['count']:>5}"
            for k in compare_keys:
                row += f" {data['metrics'].get(k, 0):>10.3f}"
            print(row)

    # === トップ vs ボトムパフォーマー分析 ===
    print(f"\n\n  === トップ vs ボトム パフォーマー分析 ===\n")

    for period_key, period_label in [("2024h1", "2024年前半"), ("2026jan", "2026年1月")]:
        period_data = results.get(period_key, {})
        if not period_data:
            continue

        # データKW/分 × 文字比率でスコアリング
        member_scores = []
        for email, metrics_list in period_data.items():
            if len(metrics_list) < 3:  # 3件未満は除外
                continue
            data_kw = avg(metrics_list, "data_kw_per_min")
            char_ratio = avg(metrics_list, "host_char_ratio")
            composite = data_kw * 10 + char_ratio * 5  # 重み付けスコア
            member_scores.append((email, composite, metrics_list))

        if len(member_scores) < 4:
            print(f"  [{period_label}] メンバー数不足（{len(member_scores)}人）、スキップ")
            continue

        member_scores.sort(key=lambda x: x[1], reverse=True)
        n = len(member_scores)
        top_half = member_scores[:max(n // 2, 1)]
        bot_half = member_scores[max(n // 2, 1):]

        top_all = [m for _, _, mlist in top_half for m in mlist]
        bot_all = [m for _, _, mlist in bot_half for m in mlist]

        print(f"\n  【{period_label}】")
        print(f"  上位グループ: {', '.join(e for e, _, _ in top_half)} ({len(top_all)}件)")
        print(f"  下位グループ: {', '.join(e for e, _, _ in bot_half)} ({len(bot_all)}件)")
        print(f"\n  {'指標':<20} {'上位':>10} {'下位':>10} {'差分':>10}")
        print(f"  {'-'*55}")

        for key in metric_keys:
            t_val = avg(top_all, key)
            b_val = avg(bot_all, key)
            diff = t_val - b_val
            sign = "+" if diff > 0 else ""
            label = metric_labels.get(key, key)
            marker = " ★" if abs(diff) > 0.1 * max(abs(t_val), abs(b_val), 0.001) else ""
            print(f"  {label:<20} {t_val:>10.3f} {b_val:>10.3f} {sign}{diff:>9.3f}{marker}")

    # JSON保存
    summary_output = {
        "member_summary": member_summary,
        "flat_h1_count": len(flat_h1),
        "flat_jan_count": len(flat_jan),
    }
    with open(OUTPUT_DIR / "team_comparison_summary.json", "w", encoding="utf-8") as f:
        json.dump(summary_output, f, ensure_ascii=False, indent=2, default=str)

    print(f"\n\n  結果保存: {OUTPUT_DIR / 'team_comparison_summary.json'}")


def main():
    phase1_download()
    results = phase2_analyze()
    if results:
        phase3_report(results)


if __name__ == "__main__":
    main()
