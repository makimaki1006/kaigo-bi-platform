# -*- coding: utf-8 -*-
"""営業メンバー絞込み比較分析
2024年前半 vs 2026年1月 — 主要メンバーのみ
"""
import io
import json
import math
import os
import re
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / "config" / ".env")

ZOOM_ACCOUNT_ID = os.environ["ZOOM_ACCOUNT_ID"]
ZOOM_CLIENT_ID = os.environ["ZOOM_CLIENT_ID"]
ZOOM_CLIENT_SECRET = os.environ["ZOOM_CLIENT_SECRET"]
ZOOM_API_BASE = "https://api.zoom.us/v2"

OUTPUT_DIR = PROJECT_ROOT / "data" / "output" / "team_comparison"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# 分析対象メンバー（営業チーム + 商談数多いメンバー）
TARGET_MEMBERS = {
    "j_sato@cyxen.co.jp": "佐藤丈太郎",
    "yo_ichiki@cyxen.co.jp": "市来洋平",
    "s_shimatani@cyxen.co.jp": "嶋谷",
    "k_kobayashi@cyxen.co.jp": "小林幸太",
    "i_kumagai@cyxen.co.jp": "熊谷",
    "h_matsukaze@cyxen.co.jp": "松風穂香",
    "s_shinoki@cyxen.co.jp": "篠木柊志",
    "k_sawada@cyxen.co.jp": "澤田和歩",
    "y_fukabori@cyxen.co.jp": "深堀勇侍",
    "s_hattori@cyxen.co.jp": "服部翔太郎",
    "n_kiyohira@cyxen.co.jp": "清飛羅直樹",
    "y_haino@cyxen.co.jp": "灰野大和",
    "r_shimura@cyxen.co.jp": "志村亮介",
    "h_obata@cyxen.co.jp": "小幡英稔",
    "y_tejima@cyxen.co.jp": "手島唯那",
    "a_tanji@cyxen.co.jp": "丹司杏奈",
    "s_endo@cyxen.co.jp": "遠藤紗月",
    "ko_suzuki@f-a-c.co.jp": "鈴木孝太郎",
    "i_kitao@f-a-c.co.jp": "北尾一朗",
    "t_kitamoto@f-a-c.co.jp": "北本天祐",
    "r_uehata@f-a-c.co.jp": "上畑綾太郎",
    "r_yao@f-a-c.co.jp": "八尾龍斗",
    "d_watanabe@f-a-c.co.jp": "渡邉大貴",
    "a_yasutomo@f-a-c.co.jp": "安友愛理沙",
    "h_tsuji@f-a-c.co.jp": "辻花佳",
}

PERIODS = {
    "2024h1": {
        "label": "2024年前半（1-6月）",
        "ranges": [
            ("2024-01-01", "2024-01-31"), ("2024-02-01", "2024-02-29"),
            ("2024-03-01", "2024-03-31"), ("2024-04-01", "2024-04-30"),
            ("2024-05-01", "2024-05-31"), ("2024-06-01", "2024-06-30"),
        ],
    },
    "2026jan": {
        "label": "2026年1月",
        "ranges": [("2026-01-01", "2026-01-31")],
    },
}

EXCLUDE_KEYWORDS = [
    "ロープレ", "roleplay", "RP", "role play", "研修", "MTG", "定例",
    "朝会", "勉強会", "VS", "vs", "パーソナルミーティング",
    "YMCX", "Young Man", "新人", "組み手", "1on1", "振り返り",
    "共有会", "報告会", "Zoom ミーティング", "New Zoom Meeting",
    "Personal Meeting Room", "テスト", "test",
]
LINE_RE = re.compile(r"\[(\d{2}):(\d{2}):(\d{2})\.\d+\]\s+(.+?):\s+(.*)")


def get_zoom_token():
    resp = requests.post(
        "https://zoom.us/oauth/token",
        params={"grant_type": "account_credentials", "account_id": ZOOM_ACCOUNT_ID},
        auth=(ZOOM_CLIENT_ID, ZOOM_CLIENT_SECRET), timeout=30)
    resp.raise_for_status()
    return resp.json()["access_token"]


def get_recordings(token, email, from_d, to_d):
    headers = {"Authorization": f"Bearer {token}"}
    meetings = []
    npt = ""
    while True:
        params = {"from": from_d, "to": to_d, "page_size": 300}
        if npt:
            params["next_page_token"] = npt
        r = requests.get(f"{ZOOM_API_BASE}/users/{email}/recordings",
                         headers=headers, params=params, timeout=60)
        if r.status_code == 429:
            time.sleep(10); continue
        if r.status_code == 404:
            return []
        r.raise_for_status()
        d = r.json()
        meetings.extend(d.get("meetings", []))
        npt = d.get("next_page_token", "")
        if not npt:
            break
    return meetings


def download_transcript(token, url):
    try:
        r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=120)
        return r.text if r.status_code == 200 else None
    except:
        return None


def vtt_to_text(vtt):
    lines = vtt.strip().split("\n")
    result = []
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if "-->" in line:
            ts = line.split("-->")[0].strip()
            tl = []
            i += 1
            while i < len(lines) and lines[i].strip() and "-->" not in lines[i]:
                tl.append(lines[i].strip())
                i += 1
            if tl:
                result.append(f"[{ts}] {' '.join(tl)}")
            continue
        i += 1
    return "\n".join(result)


def is_excluded(topic):
    if not topic:
        return True
    return any(kw.lower() in topic.lower() for kw in EXCLUDE_KEYWORDS)


def sanitize(name):
    return re.sub(r'[<>:"/\\|?*]', '_', name)[:80]


def parse_seconds(h, m, s):
    return int(h) * 3600 + int(m) * 60 + int(s)


def analyze_transcript(text, host_name=""):
    entries = []
    for line in text.strip().split("\n"):
        m = LINE_RE.match(line.strip())
        if m:
            sec = parse_seconds(m.group(1), m.group(2), m.group(3))
            entries.append({"sec": sec, "speaker": m.group(4), "text": m.group(5)})
    if len(entries) < 15:
        return None

    sc = Counter(e["speaker"] for e in entries)
    host_speaker = None
    if host_name:
        for sp in sc:
            for part in host_name.replace("　", " ").split():
                if len(part) >= 2 and part in sp:
                    host_speaker = sp; break
            if host_speaker:
                break
    if not host_speaker:
        host_speaker = sc.most_common(1)[0][0]

    for e in entries:
        e["is_host"] = e["speaker"] == host_speaker

    host = [e for e in entries if e["is_host"]]
    cust = [e for e in entries if not e["is_host"]]
    tc = sum(len(e["text"]) for e in entries)
    hc = sum(len(e["text"]) for e in host)
    cc = sum(len(e["text"]) for e in cust)
    dur = max((entries[-1]["sec"] - entries[0]["sec"]) / 60, 1)
    start = entries[0]["sec"]

    hq = sum(1 for e in host if "？" in e["text"] or "?" in e["text"])
    cq = sum(1 for e in cust if "？" in e["text"] or "?" in e["text"])

    monos = []
    sc2 = 0
    for e in entries:
        if e["is_host"]:
            sc2 += len(e["text"])
        else:
            if sc2 > 0: monos.append(sc2)
            sc2 = 0
    if sc2 > 0: monos.append(sc2)

    turns = sum(1 for i in range(1, len(entries)) if entries[i]["is_host"] != entries[i-1]["is_host"])

    f3 = [e for e in entries if e["sec"] - start <= 180]
    f3hc = sum(len(e["text"]) for e in f3 if e["is_host"])
    f3tc = sum(len(e["text"]) for e in f3)
    f3hq = sum(1 for e in f3 if e["is_host"] and ("？" in e["text"] or "?" in e["text"]))

    np = re.compile(r'\d+')
    hn = sum(len(np.findall(e["text"])) for e in host)

    dk = ["調査", "市場", "人口", "競合", "施設", "検索", "データ", "エリア", "求人", "件数"]
    hdk = sum(sum(e["text"].count(k) for k in dk) for e in host)
    ck = ["キャンペーン", "割引", "限定", "今月", "期限", "損失", "機会"]
    hck = sum(sum(e["text"].count(k) for k in ck) for e in host)
    pk = ["課題", "困って", "問題", "悩み", "大変", "足りない", "できない", "難しい"]
    hpq = sum(1 for e in host if ("？" in e["text"] or "?" in e["text"]) and any(k in e["text"] for k in pk))

    cl = sum(1 for e in cust if len(e["text"]) >= 30)
    cs = sum(1 for e in cust if len(e["text"]) <= 10)

    return {
        "duration": round(dur, 1),
        "host_char_ratio": round(hc/tc, 3) if tc else 0,
        "host_avg_len": round(hc/len(host), 1) if host else 0,
        "cust_avg_len": round(cc/len(cust), 1) if cust else 0,
        "host_q_per_min": round(hq/dur, 2),
        "cust_q": cq,
        "nums_per_min": round(hn/dur, 2),
        "avg_mono": round(sum(monos)/len(monos), 1) if monos else 0,
        "long_mono": sum(1 for m in monos if m >= 200),
        "turns_per_min": round(turns/dur, 2),
        "f3_host_ratio": round(f3hc/f3tc, 3) if f3tc else 0,
        "f3_host_q": f3hq,
        "cust_long_ratio": round(cl/len(cust), 3) if cust else 0,
        "cust_short_ratio": round(cs/len(cust), 3) if cust else 0,
        "data_kw_per_min": round(hdk/dur, 2),
        "close_kw_total": hck,
        "problem_q": hpq,
    }


def main():
    print("=" * 70)
    print("  営業メンバー 行動指標比較分析")
    print(f"  対象: {len(TARGET_MEMBERS)}名")
    print("=" * 70)

    token = get_zoom_token()
    all_results = {}  # {email: {period: [metrics]}}

    for email, name in TARGET_MEMBERS.items():
        all_results[email] = {"name": name, "2024h1": [], "2026jan": []}

        for period_key, pinfo in PERIODS.items():
            pdir = OUTPUT_DIR / period_key / sanitize(email.split("@")[0])
            pdir.mkdir(parents=True, exist_ok=True)

            # 既存ファイル読み込み
            existing = list(pdir.glob("*.txt"))
            new_downloads = 0

            if not existing:
                # ダウンロード
                for fd, td in pinfo["ranges"]:
                    try:
                        mtgs = get_recordings(token, email, fd, td)
                    except Exception as e:
                        continue
                    for mtg in mtgs:
                        topic = mtg.get("topic", "")
                        if is_excluded(topic):
                            continue
                        for rf in mtg.get("recording_files", []):
                            if rf.get("file_type") == "TRANSCRIPT" and rf.get("status") == "completed":
                                dt = mtg.get("start_time", "")[:10]
                                tm = mtg.get("start_time", "")[11:16].replace(":", "")
                                fp = pdir / f"{dt}_{tm}_{sanitize(topic)}.txt"
                                if fp.exists():
                                    continue
                                vtt = download_transcript(token, rf.get("download_url", ""))
                                if vtt:
                                    txt = vtt_to_text(vtt)
                                    if len(txt.strip().split("\n")) >= 15:
                                        fp.write_text(txt, encoding="utf-8")
                                        new_downloads += 1
                existing = list(pdir.glob("*.txt"))

            # 分析
            for fp in existing:
                txt = fp.read_text(encoding="utf-8")
                m = analyze_transcript(txt, host_name=name)
                if m:
                    all_results[email][period_key].append(m)

            cnt = len(all_results[email][period_key])
            dl_note = f" (新規DL:{new_downloads})" if new_downloads else ""
            if cnt > 0:
                print(f"  {name:<15} [{period_key}] {cnt}件{dl_note}")

        # トークンリフレッシュ（5人ごと）
        if list(TARGET_MEMBERS.keys()).index(email) % 5 == 4:
            token = get_zoom_token()

    # === 結果集計 ===
    def avg(lst, key):
        vals = [x[key] for x in lst if x.get(key) is not None]
        return sum(vals)/len(vals) if vals else 0

    metric_keys = [
        "host_char_ratio", "host_avg_len", "cust_avg_len",
        "host_q_per_min", "cust_q", "nums_per_min",
        "avg_mono", "long_mono", "turns_per_min",
        "f3_host_ratio", "f3_host_q",
        "cust_long_ratio", "cust_short_ratio",
        "data_kw_per_min", "close_kw_total", "problem_q", "duration",
    ]
    labels = {
        "host_char_ratio": "文字比率", "host_avg_len": "平均発話長",
        "cust_avg_len": "顧客発話長", "host_q_per_min": "質問/分",
        "cust_q": "顧客質問数", "nums_per_min": "数字/分",
        "avg_mono": "平均独白長", "long_mono": "200字超独白",
        "turns_per_min": "ターン/分", "f3_host_ratio": "冒頭3分比率",
        "f3_host_q": "冒頭3分Q", "cust_long_ratio": "顧客長発話率",
        "cust_short_ratio": "顧客相槌率", "data_kw_per_min": "データKW/分",
        "close_kw_total": "クロージングKW", "problem_q": "P質問",
        "duration": "商談時間(分)",
    }

    # --- メンバー別一覧 ---
    print(f"\n\n{'='*75}")
    print("  メンバー別商談件数")
    print(f"{'='*75}")
    print(f"  {'名前':<15} {'2024H1':>8} {'2026Jan':>8}")
    print(f"  {'-'*35}")
    both_members = []
    for email, data in all_results.items():
        h1 = len(data["2024h1"])
        jan = len(data["2026jan"])
        if h1 > 0 or jan > 0:
            print(f"  {data['name']:<15} {h1:>8} {jan:>8}")
            if h1 >= 3 and jan >= 3:
                both_members.append(email)

    # --- 全体合算比較 ---
    flat_h1 = [m for e in all_results.values() for m in e["2024h1"]]
    flat_jan = [m for e in all_results.values() for m in e["2026jan"]]

    print(f"\n\n{'='*75}")
    print(f"  全メンバー合算: 期間比較 (2024H1: {len(flat_h1)}件 vs 2026Jan: {len(flat_jan)}件)")
    print(f"{'='*75}")
    print(f"  {'指標':<15} {'2024H1':>10} {'2026Jan':>10} {'差分':>10} {'変化率':>10}")
    print(f"  {'-'*60}")
    for k in metric_keys:
        v1 = avg(flat_h1, k)
        v2 = avg(flat_jan, k)
        d = v2-v1
        p = ((v2-v1)/v1*100) if v1 != 0 else 0
        s = "+" if d > 0 else ""
        print(f"  {labels[k]:<15} {v1:>10.2f} {v2:>10.2f} {s}{d:>9.2f} {s}{p:>8.1f}%")

    # --- メンバー別主要指標（両期間データあり） ---
    compare_keys = ["host_char_ratio", "nums_per_min", "data_kw_per_min", "f3_host_q", "problem_q", "cust_short_ratio", "close_kw_total"]
    compare_labels = ["文字比率", "数字/分", "データKW/分", "冒頭3分Q", "P質問", "相槌率", "CloseKW"]

    for pk, pl in [("2024h1", "2024年前半"), ("2026jan", "2026年1月")]:
        print(f"\n\n{'='*75}")
        print(f"  メンバー別主要指標: {pl}")
        print(f"{'='*75}")
        h = f"  {'名前':<12} {'件数':>4}"
        for l in compare_labels:
            h += f" {l:>9}"
        print(h)
        print(f"  {'-'*90}")

        rows = []
        for email in sorted(all_results.keys(), key=lambda e: avg(all_results[e][pk], "data_kw_per_min"), reverse=True):
            data = all_results[email]
            mlist = data[pk]
            if len(mlist) < 2:
                continue
            row = f"  {data['name']:<12} {len(mlist):>4}"
            for ck in compare_keys:
                row += f" {avg(mlist, ck):>9.3f}"
            rows.append(row)
        for r in rows:
            print(r)

    # --- トップ vs ボトム ---
    for pk, pl in [("2024h1", "2024年前半"), ("2026jan", "2026年1月")]:
        scored = []
        for email, data in all_results.items():
            mlist = data[pk]
            if len(mlist) < 3:
                continue
            score = avg(mlist, "data_kw_per_min") * 10 + avg(mlist, "host_char_ratio") * 5
            scored.append((email, data["name"], score, mlist))

        if len(scored) < 4:
            continue

        scored.sort(key=lambda x: x[2], reverse=True)
        n = len(scored)
        top = scored[:max(n//3, 1)]
        bot = scored[-max(n//3, 1):]

        top_all = [m for _, _, _, ml in top for m in ml]
        bot_all = [m for _, _, _, ml in bot for m in ml]

        print(f"\n\n{'='*75}")
        print(f"  トップ vs ボトム: {pl}")
        print(f"{'='*75}")
        print(f"  上位: {', '.join(n for _, n, _, _ in top)} ({len(top_all)}件)")
        print(f"  下位: {', '.join(n for _, n, _, _ in bot)} ({len(bot_all)}件)")
        print(f"\n  {'指標':<15} {'上位':>10} {'下位':>10} {'差分':>10} {'差%':>10}")
        print(f"  {'-'*60}")
        for k in metric_keys:
            tv = avg(top_all, k)
            bv = avg(bot_all, k)
            d = tv - bv
            p = (d/bv*100) if bv != 0 else 0
            s = "+" if d > 0 else ""
            marker = " ★" if abs(d) > 0.15 * max(abs(tv), abs(bv), 0.001) else ""
            print(f"  {labels[k]:<15} {tv:>10.2f} {bv:>10.2f} {s}{d:>9.2f} {s}{p:>8.1f}%{marker}")

    # --- 個人別変化ランキング ---
    if both_members:
        print(f"\n\n{'='*75}")
        print(f"  個人別変化ランキング（両期間データあるメンバー: {len(both_members)}名）")
        print(f"{'='*75}")

        for k, lbl in [("nums_per_min", "数字使用/分"), ("data_kw_per_min", "データKW/分"),
                        ("host_char_ratio", "文字比率"), ("f3_host_q", "冒頭3分Q"),
                        ("cust_short_ratio", "顧客相槌率"), ("problem_q", "P質問")]:
            print(f"\n  【{lbl}】")
            changes = []
            for email in both_members:
                d = all_results[email]
                v1 = avg(d["2024h1"], k)
                v2 = avg(d["2026jan"], k)
                diff = v2 - v1
                pct = ((v2-v1)/v1*100) if v1 != 0 else 0
                changes.append((d["name"], v1, v2, diff, pct))
            changes.sort(key=lambda x: x[3])
            for name, v1, v2, diff, pct in changes:
                s = "+" if diff > 0 else ""
                arrow = "↑" if diff > 0 else "↓" if diff < 0 else "→"
                print(f"    {name:<12} {v1:>7.2f} → {v2:>7.2f} ({s}{diff:.2f}, {s}{pct:.0f}%) {arrow}")

    # JSON保存
    summary = {}
    for email, data in all_results.items():
        if len(data["2024h1"]) > 0 or len(data["2026jan"]) > 0:
            summary[email] = {
                "name": data["name"],
                "2024h1": {"count": len(data["2024h1"]), "metrics": {k: round(avg(data["2024h1"], k), 3) for k in metric_keys}},
                "2026jan": {"count": len(data["2026jan"]), "metrics": {k: round(avg(data["2026jan"], k), 3) for k in metric_keys}},
            }
    with open(OUTPUT_DIR / "team_targeted_summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    print(f"\n\n結果保存: {OUTPUT_DIR / 'team_targeted_summary.json'}")


if __name__ == "__main__":
    main()
