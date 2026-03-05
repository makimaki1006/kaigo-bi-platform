# -*- coding: utf-8 -*-
"""佐藤丈太郎 商談Transcript行動指標分析
生のTranscriptから客観的行動指標を抽出し、イケイケ期 vs だめだめ期を比較"""
import io
import json
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

BASE = Path("data/output/sato_comparison")
SATO_NAMES = ["佐藤ファビオ丈太郎", "佐藤", "sato"]
LINE_RE = re.compile(r"\[(\d{2}):(\d{2}):(\d{2})\.\d+\]\s+(.+?):\s+(.*)")


def parse_seconds(h, m, s):
    return int(h) * 3600 + int(m) * 60 + int(s)


def is_sato(speaker):
    sp = speaker.lower().strip()
    for name in SATO_NAMES:
        if name.lower() in sp:
            return True
    return False


def analyze_transcript(filepath):
    """1つのTranscriptから行動指標を抽出"""
    with open(filepath, "r", encoding="utf-8") as f:
        lines = f.readlines()

    entries = []
    for line in lines:
        m = LINE_RE.match(line.strip())
        if m:
            sec = parse_seconds(m.group(1), m.group(2), m.group(3))
            speaker = m.group(4)
            text = m.group(5)
            entries.append({
                "sec": sec,
                "speaker": speaker,
                "is_sato": is_sato(speaker),
                "text": text,
                "chars": len(text),
                "has_question": "？" in text or "?" in text,
            })

    if len(entries) < 10:
        return None

    # --- 基本指標 ---
    sato_lines = [e for e in entries if e["is_sato"]]
    cust_lines = [e for e in entries if not e["is_sato"]]
    total_lines = len(entries)

    sato_chars = sum(e["chars"] for e in sato_lines)
    cust_chars = sum(e["chars"] for e in cust_lines)
    total_chars = sato_chars + cust_chars

    # --- 発話比率 ---
    sato_line_ratio = len(sato_lines) / total_lines if total_lines else 0
    sato_char_ratio = sato_chars / total_chars if total_chars else 0

    # --- 質問頻度 ---
    sato_questions = sum(1 for e in sato_lines if e["has_question"])
    cust_questions = sum(1 for e in cust_lines if e["has_question"])

    # --- 独白（モノローグ）分析 ---
    monologues = []
    current_streak = 0
    current_streak_chars = 0
    for e in entries:
        if e["is_sato"]:
            current_streak += 1
            current_streak_chars += e["chars"]
        else:
            if current_streak > 0:
                monologues.append({"lines": current_streak, "chars": current_streak_chars})
            current_streak = 0
            current_streak_chars = 0
    if current_streak > 0:
        monologues.append({"lines": current_streak, "chars": current_streak_chars})

    max_mono_lines = max((m["lines"] for m in monologues), default=0)
    max_mono_chars = max((m["chars"] for m in monologues), default=0)
    avg_mono_lines = sum(m["lines"] for m in monologues) / len(monologues) if monologues else 0
    avg_mono_chars = sum(m["chars"] for m in monologues) / len(monologues) if monologues else 0
    long_monologues = sum(1 for m in monologues if m["chars"] >= 200)

    # --- ターンテイキング（会話の切り替え回数） ---
    turns = 0
    prev_is_sato = None
    for e in entries:
        if prev_is_sato is not None and e["is_sato"] != prev_is_sato:
            turns += 1
        prev_is_sato = e["is_sato"]

    # --- 商談時間 ---
    duration_sec = entries[-1]["sec"] - entries[0]["sec"] if len(entries) > 1 else 0
    duration_min = duration_sec / 60

    # --- 冒頭3分パターン ---
    start_sec = entries[0]["sec"]
    first_3min = [e for e in entries if e["sec"] - start_sec <= 180]
    first_3_sato = [e for e in first_3min if e["is_sato"]]
    first_3_cust = [e for e in first_3min if not e["is_sato"]]
    first_3_sato_chars = sum(e["chars"] for e in first_3_sato)
    first_3_total_chars = sum(e["chars"] for e in first_3min)
    first_3_sato_ratio = first_3_sato_chars / first_3_total_chars if first_3_total_chars else 0
    first_3_sato_questions = sum(1 for e in first_3_sato if e["has_question"])

    # --- 冒頭5分パターン ---
    first_5min = [e for e in entries if e["sec"] - start_sec <= 300]
    first_5_sato = [e for e in first_5min if e["is_sato"]]
    first_5_sato_chars = sum(e["chars"] for e in first_5_sato)
    first_5_total_chars = sum(e["chars"] for e in first_5min)
    first_5_sato_ratio = first_5_sato_chars / first_5_total_chars if first_5_total_chars else 0
    first_5_sato_questions = sum(1 for e in first_5_sato if e["has_question"])

    # --- 終盤5分パターン ---
    end_sec = entries[-1]["sec"]
    last_5min = [e for e in entries if end_sec - e["sec"] <= 300]
    last_5_sato = [e for e in last_5min if e["is_sato"]]
    last_5_cust = [e for e in last_5min if not e["is_sato"]]
    last_5_sato_chars = sum(e["chars"] for e in last_5_sato)
    last_5_total_chars = sum(e["chars"] for e in last_5min)
    last_5_sato_ratio = last_5_sato_chars / last_5_total_chars if last_5_total_chars else 0

    # --- 顧客エンゲージメント指標 ---
    cust_avg_chars = sum(e["chars"] for e in cust_lines) / len(cust_lines) if cust_lines else 0
    # 顧客の長い発話（30文字以上）= 深い回答
    cust_long_responses = sum(1 for e in cust_lines if e["chars"] >= 30)
    cust_long_ratio = cust_long_responses / len(cust_lines) if cust_lines else 0
    # 顧客の短い相槌（10文字以下）= 受け身
    cust_short = sum(1 for e in cust_lines if e["chars"] <= 10)
    cust_short_ratio = cust_short / len(cust_lines) if cust_lines else 0

    # --- 佐藤の平均発話長 ---
    sato_avg_chars = sato_chars / len(sato_lines) if sato_lines else 0

    # --- 1分あたりのターン数（会話のテンポ） ---
    turns_per_min = turns / duration_min if duration_min > 0 else 0

    # --- 具体性指標：数字の使用頻度 ---
    number_pattern = re.compile(r'\d+')
    sato_numbers = sum(len(number_pattern.findall(e["text"])) for e in sato_lines)
    sato_numbers_per_min = sato_numbers / duration_min if duration_min > 0 else 0

    # --- 「ありがとう」「なるほど」等のリアクション ---
    sato_thanks = sum(1 for e in sato_lines if "ありがと" in e["text"])
    sato_naruhodo = sum(1 for e in sato_lines if "なるほど" in e["text"])
    sato_shouchi = sum(1 for e in sato_lines if "承知" in e["text"])

    return {
        "file": str(filepath),
        "total_lines": total_lines,
        "duration_min": round(duration_min, 1),

        # 発話比率
        "sato_line_ratio": round(sato_line_ratio, 3),
        "sato_char_ratio": round(sato_char_ratio, 3),

        # 発話量
        "sato_lines": len(sato_lines),
        "cust_lines": len(cust_lines),
        "sato_chars": sato_chars,
        "cust_chars": cust_chars,
        "sato_avg_chars": round(sato_avg_chars, 1),
        "cust_avg_chars": round(cust_avg_chars, 1),

        # 質問
        "sato_questions": sato_questions,
        "cust_questions": cust_questions,
        "sato_questions_per_min": round(sato_questions / duration_min, 2) if duration_min > 0 else 0,

        # モノローグ
        "max_mono_lines": max_mono_lines,
        "max_mono_chars": max_mono_chars,
        "avg_mono_lines": round(avg_mono_lines, 2),
        "avg_mono_chars": round(avg_mono_chars, 1),
        "long_monologues": long_monologues,

        # ターンテイキング
        "turns": turns,
        "turns_per_min": round(turns_per_min, 2),

        # 冒頭パターン
        "first_3min_sato_ratio": round(first_3_sato_ratio, 3),
        "first_3min_sato_questions": first_3_sato_questions,
        "first_5min_sato_ratio": round(first_5_sato_ratio, 3),
        "first_5min_sato_questions": first_5_sato_questions,

        # 終盤パターン
        "last_5min_sato_ratio": round(last_5_sato_ratio, 3),

        # 顧客エンゲージメント
        "cust_long_responses": cust_long_responses,
        "cust_long_ratio": round(cust_long_ratio, 3),
        "cust_short_ratio": round(cust_short_ratio, 3),

        # 具体性
        "sato_numbers_per_min": round(sato_numbers_per_min, 2),

        # リアクション
        "sato_thanks": sato_thanks,
        "sato_naruhodo": sato_naruhodo,
        "sato_shouchi": sato_shouchi,
    }


def main():
    results = {"ikeike": [], "damedame": []}

    for period, folder in [("ikeike", BASE / "ikeike"), ("damedame", BASE / "damedame")]:
        if not folder.exists():
            print(f"フォルダが見つかりません: {folder}")
            continue
        files = sorted(folder.glob("*.txt"))
        print(f"\n{'='*60}")
        print(f" {period}: {len(files)}件のTranscript分析中...")
        print(f"{'='*60}")
        for fp in files:
            r = analyze_transcript(fp)
            if r:
                r["period"] = period
                results[period].append(r)
        print(f"  → {len(results[period])}件 分析完了")

    # --- 比較サマリー ---
    print(f"\n{'='*70}")
    print("  行動指標比較サマリー: イケイケ期 vs だめだめ期")
    print(f"{'='*70}\n")

    def avg(lst, key):
        vals = [x[key] for x in lst if x[key] is not None]
        return sum(vals) / len(vals) if vals else 0

    def median(lst, key):
        vals = sorted([x[key] for x in lst if x[key] is not None])
        n = len(vals)
        if n == 0:
            return 0
        if n % 2 == 0:
            return (vals[n // 2 - 1] + vals[n // 2]) / 2
        return vals[n // 2]

    ik = results["ikeike"]
    dm = results["damedame"]

    metrics = [
        ("商談時間(分)", "duration_min", "avg"),
        ("", "", ""),
        ("--- 発話比率 ---", "", ""),
        ("佐藤 発話行比率", "sato_line_ratio", "avg"),
        ("佐藤 文字比率", "sato_char_ratio", "avg"),
        ("佐藤 平均発話長(文字)", "sato_avg_chars", "avg"),
        ("顧客 平均発話長(文字)", "cust_avg_chars", "avg"),
        ("", "", ""),
        ("--- 質問頻度 ---", "", ""),
        ("佐藤 質問数/商談", "sato_questions", "avg"),
        ("佐藤 質問数/分", "sato_questions_per_min", "avg"),
        ("顧客 質問数/商談", "cust_questions", "avg"),
        ("", "", ""),
        ("--- 独白(モノローグ) ---", "", ""),
        ("最長独白(行)", "max_mono_lines", "avg"),
        ("最長独白(文字)", "max_mono_chars", "avg"),
        ("平均独白(文字)", "avg_mono_chars", "avg"),
        ("200文字超独白 回数/商談", "long_monologues", "avg"),
        ("", "", ""),
        ("--- 会話テンポ ---", "", ""),
        ("ターン数/商談", "turns", "avg"),
        ("ターン数/分", "turns_per_min", "avg"),
        ("", "", ""),
        ("--- 冒頭パターン ---", "", ""),
        ("冒頭3分 佐藤文字比率", "first_3min_sato_ratio", "avg"),
        ("冒頭3分 佐藤質問数", "first_3min_sato_questions", "avg"),
        ("冒頭5分 佐藤文字比率", "first_5min_sato_ratio", "avg"),
        ("冒頭5分 佐藤質問数", "first_5min_sato_questions", "avg"),
        ("", "", ""),
        ("--- 終盤パターン ---", "", ""),
        ("終盤5分 佐藤文字比率", "last_5min_sato_ratio", "avg"),
        ("", "", ""),
        ("--- 顧客エンゲージメント ---", "", ""),
        ("顧客 長い発話(30文字+)件数", "cust_long_responses", "avg"),
        ("顧客 長い発話比率", "cust_long_ratio", "avg"),
        ("顧客 短い相槌比率(10文字以下)", "cust_short_ratio", "avg"),
        ("", "", ""),
        ("--- 具体性 ---", "", ""),
        ("数字使用頻度/分", "sato_numbers_per_min", "avg"),
        ("", "", ""),
        ("--- リアクション ---", "", ""),
        ("「ありがとう」回数/商談", "sato_thanks", "avg"),
        ("「なるほど」回数/商談", "sato_naruhodo", "avg"),
        ("「承知」回数/商談", "sato_shouchi", "avg"),
    ]

    print(f"{'指標':<35} {'イケイケ':>10} {'だめだめ':>10} {'差分':>10} {'変化率':>10}")
    print("-" * 80)

    significant_diffs = []

    for label, key, agg in metrics:
        if not key:
            if label:
                print(f"\n{label}")
            continue
        ik_val = avg(ik, key) if agg == "avg" else median(ik, key)
        dm_val = avg(dm, key) if agg == "avg" else median(dm, key)
        diff = dm_val - ik_val
        pct = ((dm_val - ik_val) / ik_val * 100) if ik_val != 0 else 0

        sign = "+" if diff > 0 else ""
        print(f"{label:<35} {ik_val:>10.2f} {dm_val:>10.2f} {sign}{diff:>9.2f} {sign}{pct:>8.1f}%")

        if abs(pct) >= 15 and key not in ("", None):
            significant_diffs.append((label, ik_val, dm_val, diff, pct))

    # --- 有意な差 TOP ---
    print(f"\n\n{'='*70}")
    print("  ★ 有意な差がある指標 TOP (変化率15%以上)")
    print(f"{'='*70}\n")

    significant_diffs.sort(key=lambda x: abs(x[4]), reverse=True)
    for i, (label, ik_v, dm_v, diff, pct) in enumerate(significant_diffs, 1):
        direction = "↑悪化" if diff > 0 and "佐藤" in label and "比率" in label else ""
        if "顧客" in label and "長い" in label and diff < 0:
            direction = "↓悪化"
        if "質問" in label and diff < 0:
            direction = "↓悪化"
        if "ターン" in label and diff < 0:
            direction = "↓悪化"
        sign = "+" if pct > 0 else ""
        print(f"  {i:2d}. {label}")
        print(f"      イケイケ: {ik_v:.2f} → だめだめ: {dm_v:.2f} ({sign}{pct:.1f}%) {direction}")

    # JSON保存
    output = {
        "ikeike": ik,
        "damedame": dm,
        "summary": {
            "ikeike_count": len(ik),
            "damedame_count": len(dm),
            "significant_diffs": [
                {"label": s[0], "ikeike": s[1], "damedame": s[2], "diff": s[3], "pct": s[4]}
                for s in significant_diffs
            ],
        },
    }
    out_path = BASE / "behavioral_analysis.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"\n結果保存: {out_path}")


if __name__ == "__main__":
    main()
