# -*- coding: utf-8 -*-
"""佐藤丈太郎 3つの専門家視点分析
1. データサイエンティスト: 分布・相関・クラスタ・統計検定
2. プロマーケター: バリュープロポジション・CTA・顧客ジャーニー
3. プロセールス: SPIN分析・オブジェクション・クロージング・ラポール
"""
import io
import json
import math
import os
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

BASE = Path("data/output/sato_comparison")
SATO_NAMES = ["佐藤ファビオ丈太郎", "佐藤", "sato"]
LINE_RE = re.compile(r"\[(\d{2}):(\d{2}):(\d{2})\.\d+\]\s+(.+?):\s+(.*)")


def parse_seconds(h, m, s):
    return int(h) * 3600 + int(m) * 60 + int(s)


def is_sato(speaker):
    sp = speaker.lower().strip()
    return any(n.lower() in sp for n in SATO_NAMES)


def parse_transcript(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        lines = f.readlines()
    entries = []
    for line in lines:
        m = LINE_RE.match(line.strip())
        if m:
            sec = parse_seconds(m.group(1), m.group(2), m.group(3))
            entries.append({
                "sec": sec, "speaker": m.group(4),
                "is_sato": is_sato(m.group(4)), "text": m.group(5),
            })
    return entries


def load_all():
    data = {"ikeike": [], "damedame": []}
    filenames = {"ikeike": [], "damedame": []}
    for period, folder in [("ikeike", BASE / "ikeike"), ("damedame", BASE / "damedame")]:
        for fp in sorted(folder.glob("*.txt")):
            entries = parse_transcript(fp)
            if len(entries) >= 10:
                data[period].append(entries)
                filenames[period].append(fp.name)
    return data, filenames


# =====================================================================
# ユーティリティ
# =====================================================================
def calc_metrics(entries):
    """1商談から全指標を計算"""
    if len(entries) < 10:
        return None
    sato = [e for e in entries if e["is_sato"]]
    cust = [e for e in entries if not e["is_sato"]]
    total_chars = sum(len(e["text"]) for e in entries)
    sato_chars = sum(len(e["text"]) for e in sato)
    cust_chars = sum(len(e["text"]) for e in cust)
    duration = (entries[-1]["sec"] - entries[0]["sec"]) / 60 if len(entries) > 1 else 1
    start = entries[0]["sec"]

    # 質問
    sato_q = sum(1 for e in sato if "？" in e["text"] or "?" in e["text"])
    cust_q = sum(1 for e in cust if "？" in e["text"] or "?" in e["text"])

    # モノローグ
    monos = []
    streak = 0
    streak_chars = 0
    for e in entries:
        if e["is_sato"]:
            streak += 1
            streak_chars += len(e["text"])
        else:
            if streak > 0:
                monos.append(streak_chars)
            streak = 0
            streak_chars = 0
    if streak > 0:
        monos.append(streak_chars)

    # ターン
    turns = 0
    prev = None
    for e in entries:
        if prev is not None and e["is_sato"] != prev:
            turns += 1
        prev = e["is_sato"]

    # 冒頭3分
    f3 = [e for e in entries if e["sec"] - start <= 180]
    f3_sato_c = sum(len(e["text"]) for e in f3 if e["is_sato"])
    f3_total_c = sum(len(e["text"]) for e in f3)
    f3_sato_q = sum(1 for e in f3 if e["is_sato"] and ("？" in e["text"] or "?" in e["text"]))

    # 数字
    num_pat = re.compile(r'\d+')
    sato_nums = sum(len(num_pat.findall(e["text"])) for e in sato)

    # 顧客エンゲージメント
    cust_long = sum(1 for e in cust if len(e["text"]) >= 30)
    cust_short = sum(1 for e in cust if len(e["text"]) <= 10)

    return {
        "duration": round(duration, 1),
        "sato_char_ratio": round(sato_chars / total_chars, 3) if total_chars else 0,
        "sato_avg_len": round(sato_chars / len(sato), 1) if sato else 0,
        "cust_avg_len": round(cust_chars / len(cust), 1) if cust else 0,
        "sato_q": sato_q,
        "sato_q_per_min": round(sato_q / duration, 2) if duration else 0,
        "cust_q": cust_q,
        "max_mono": max(monos, default=0),
        "avg_mono": round(sum(monos) / len(monos), 1) if monos else 0,
        "long_mono": sum(1 for m in monos if m >= 200),
        "turns_per_min": round(turns / duration, 2) if duration else 0,
        "f3_sato_ratio": round(f3_sato_c / f3_total_c, 3) if f3_total_c else 0,
        "f3_sato_q": f3_sato_q,
        "nums_per_min": round(sato_nums / duration, 2) if duration else 0,
        "cust_long_ratio": round(cust_long / len(cust), 3) if cust else 0,
        "cust_short_ratio": round(cust_short / len(cust), 3) if cust else 0,
        "total_lines": len(entries),
        "sato_lines": len(sato),
        "cust_lines": len(cust),
    }


def percentile(vals, p):
    s = sorted(vals)
    k = (len(s) - 1) * p / 100
    f = int(k)
    c = min(f + 1, len(s) - 1)
    d = k - f
    return s[f] + d * (s[c] - s[f]) if s else 0


def stdev(vals):
    if len(vals) < 2:
        return 0
    m = sum(vals) / len(vals)
    return math.sqrt(sum((x - m) ** 2 for x in vals) / (len(vals) - 1))


def welch_t(vals1, vals2):
    """Welch's t-test（等分散を仮定しない）"""
    n1, n2 = len(vals1), len(vals2)
    if n1 < 2 or n2 < 2:
        return 0, 1.0
    m1, m2 = sum(vals1) / n1, sum(vals2) / n2
    s1, s2 = stdev(vals1), stdev(vals2)
    se = math.sqrt(s1 ** 2 / n1 + s2 ** 2 / n2) if (s1 > 0 or s2 > 0) else 1
    t = (m1 - m2) / se if se > 0 else 0
    # 簡易p値（自由度が大きい前提でz近似）
    p = 2 * (1 - 0.5 * (1 + math.erf(abs(t) / math.sqrt(2))))
    return round(t, 3), round(p, 4)


# =====================================================================
# 分析1: データサイエンティスト視点
# =====================================================================
def ds_analysis(data, filenames):
    print("=" * 75)
    print("  【データサイエンティスト視点】統計的分析")
    print("=" * 75)

    metrics_all = {"ikeike": [], "damedame": []}
    for period in ["ikeike", "damedame"]:
        for entries in data[period]:
            m = calc_metrics(entries)
            if m:
                metrics_all[period].append(m)

    ik = metrics_all["ikeike"]
    dm = metrics_all["damedame"]

    # --- 1. 分布分析 ---
    print(f"\n  1. 分布分析（平均だけでなく散らばりを見る）\n")
    dist_keys = [
        ("sato_char_ratio", "佐藤文字比率"),
        ("sato_q_per_min", "質問数/分"),
        ("nums_per_min", "数字使用/分"),
        ("avg_mono", "平均独白長"),
        ("f3_sato_q", "冒頭3分質問数"),
        ("cust_long_ratio", "顧客長い発話比率"),
        ("turns_per_min", "ターン/分"),
    ]
    print(f"  {'指標':<20} {'期間':<8} {'平均':>8} {'中央値':>8} {'SD':>8} {'25%':>8} {'75%':>8} {'最小':>8} {'最大':>8}")
    print(f"  {'-'*96}")
    for key, label in dist_keys:
        for period, lst, pname in [("ikeike", ik, "イケイケ"), ("damedame", dm, "だめだめ")]:
            vals = [x[key] for x in lst]
            if not vals:
                continue
            avg = sum(vals) / len(vals)
            med = percentile(vals, 50)
            sd = stdev(vals)
            q25 = percentile(vals, 25)
            q75 = percentile(vals, 75)
            mn = min(vals)
            mx = max(vals)
            print(f"  {label:<20} {pname:<8} {avg:>8.3f} {med:>8.3f} {sd:>8.3f} {q25:>8.3f} {q75:>8.3f} {mn:>8.3f} {mx:>8.3f}")
        print()

    # --- 2. 統計的有意差検定 ---
    print(f"\n  2. Welch's t-test（統計的有意差検定）\n")
    print(f"  {'指標':<25} {'t値':>8} {'p値':>8} {'有意':>6} {'効果量d':>8}")
    print(f"  {'-'*60}")
    test_keys = [
        ("sato_char_ratio", "佐藤文字比率"),
        ("sato_q_per_min", "質問/分"),
        ("nums_per_min", "数字使用/分"),
        ("avg_mono", "平均独白長"),
        ("max_mono", "最長独白"),
        ("f3_sato_q", "冒頭3分質問数"),
        ("f3_sato_ratio", "冒頭3分佐藤比率"),
        ("cust_long_ratio", "顧客長い発話比率"),
        ("cust_short_ratio", "顧客相槌比率"),
        ("turns_per_min", "ターン/分"),
        ("cust_q", "顧客質問数"),
        ("long_mono", "200字超独白数"),
        ("duration", "商談時間"),
    ]
    sig_results = []
    for key, label in test_keys:
        v1 = [x[key] for x in ik]
        v2 = [x[key] for x in dm]
        t, p = welch_t(v1, v2)
        sig = "***" if p < 0.001 else "**" if p < 0.01 else "*" if p < 0.05 else ""
        # Cohen's d
        pooled_sd = math.sqrt((stdev(v1) ** 2 + stdev(v2) ** 2) / 2) if (stdev(v1) > 0 or stdev(v2) > 0) else 1
        d = abs(sum(v1) / len(v1) - sum(v2) / len(v2)) / pooled_sd if pooled_sd > 0 else 0
        d_label = "大" if d >= 0.8 else "中" if d >= 0.5 else "小" if d >= 0.2 else "-"
        print(f"  {label:<25} {t:>8.3f} {p:>8.4f} {sig:>6} {d:>7.3f}({d_label})")
        sig_results.append((label, t, p, d, sig))

    # --- 3. 相関行列（イケイケ期、スコア付き） ---
    print(f"\n  3. 行動指標間の相関分析（イケイケ期）\n")

    # analysis_results.jsonからスコアをマッチ
    with open(BASE / "analysis_results.json", "r", encoding="utf-8") as f:
        ar = json.load(f)
    scores = {}
    for item in ar:
        if item.get("parsed"):
            scores[item["filename"]] = item["parsed"]["total_score"]

    # メトリクスにスコアを付与
    ik_scored = []
    for i, m in enumerate(ik):
        fname = filenames["ikeike"][i] if i < len(filenames["ikeike"]) else ""
        if fname in scores:
            m["score"] = scores[fname]
            ik_scored.append(m)

    dm_scored = []
    for i, m in enumerate(dm):
        fname = filenames["damedame"][i] if i < len(filenames["damedame"]) else ""
        if fname in scores:
            m["score"] = scores[fname]
            dm_scored.append(m)

    corr_keys = ["score", "sato_char_ratio", "nums_per_min", "f3_sato_q",
                 "avg_mono", "turns_per_min", "cust_long_ratio", "cust_short_ratio", "sato_q_per_min"]
    corr_labels = ["スコア", "佐藤比率", "数字/分", "冒頭3分Q", "平均独白", "ターン/分", "顧客長発話", "顧客相槌", "質問/分"]

    def pearson_r(x_vals, y_vals):
        n = len(x_vals)
        if n < 3:
            return 0
        mx = sum(x_vals) / n
        my = sum(y_vals) / n
        num = sum((x - mx) * (y - my) for x, y in zip(x_vals, y_vals))
        dx = math.sqrt(sum((x - mx) ** 2 for x in x_vals))
        dy = math.sqrt(sum((y - my) ** 2 for y in y_vals))
        return num / (dx * dy) if dx > 0 and dy > 0 else 0

    for period_label, scored_data in [("イケイケ期", ik_scored), ("だめだめ期", dm_scored)]:
        if len(scored_data) < 5:
            continue
        print(f"  【{period_label}】スコアとの相関 (n={len(scored_data)})")
        print(f"  {'指標':<15} {'r':>8} {'強さ':>8}")
        print(f"  {'-'*35}")
        corr_with_score = []
        for key, label in zip(corr_keys[1:], corr_labels[1:]):
            x = [d["score"] for d in scored_data]
            y = [d[key] for d in scored_data]
            r = pearson_r(x, y)
            strength = "強" if abs(r) >= 0.5 else "中" if abs(r) >= 0.3 else "弱"
            print(f"  {label:<15} {r:>8.3f} {strength:>8}")
            corr_with_score.append((label, r))
        print()

    # --- 4. クラスタ分析（簡易：佐藤比率×質問頻度で4象限） ---
    print(f"\n  4. 商談タイプ分類（佐藤文字比率 × 質問頻度 の4象限）\n")
    for period_label, lst in [("イケイケ期", ik), ("だめだめ期", dm)]:
        med_ratio = percentile([x["sato_char_ratio"] for x in lst], 50)
        med_q = percentile([x["sato_q_per_min"] for x in lst], 50)

        types = {
            "A:支配×質問多(理想型)": [],
            "B:支配×質問少(独演会)": [],
            "C:非支配×質問多(振回され)": [],
            "D:非支配×質問少(沈黙型)": [],
        }
        for m in lst:
            high_ratio = m["sato_char_ratio"] >= med_ratio
            high_q = m["sato_q_per_min"] >= med_q
            if high_ratio and high_q:
                types["A:支配×質問多(理想型)"].append(m)
            elif high_ratio and not high_q:
                types["B:支配×質問少(独演会)"].append(m)
            elif not high_ratio and high_q:
                types["C:非支配×質問多(振回され)"].append(m)
            else:
                types["D:非支配×質問少(沈黙型)"].append(m)

        print(f"  【{period_label}】(中央値: 佐藤比率={med_ratio:.3f}, 質問/分={med_q:.2f})")
        for tname, tlist in types.items():
            pct = len(tlist) / len(lst) * 100 if lst else 0
            avg_score = sum(x.get("score", 0) for x in tlist) / len(tlist) if tlist else 0
            bar = "█" * int(pct / 3)
            print(f"    {tname:<30} {len(tlist):>3}件 ({pct:>5.1f}%) {bar} 平均スコア:{avg_score:.0f}")
        print()

    # --- 5. 異常値分析: だめだめ期でイケイケ的な商談 ---
    print(f"\n  5. 異常値: だめだめ期でイケイケ的パターンの商談\n")
    ik_avg_ratio = sum(x["sato_char_ratio"] for x in ik) / len(ik)
    ik_avg_nums = sum(x["nums_per_min"] for x in ik) / len(ik)

    outliers = []
    for i, m in enumerate(dm):
        if m["sato_char_ratio"] >= ik_avg_ratio * 0.9 and m["nums_per_min"] >= ik_avg_nums * 0.7:
            fname = filenames["damedame"][i] if i < len(filenames["damedame"]) else f"deal_{i}"
            outliers.append((fname, m))

    if outliers:
        print(f"  イケイケ的パターン（佐藤比率≧{ik_avg_ratio * 0.9:.2f} & 数字使用≧{ik_avg_nums * 0.7:.2f}）:")
        for fname, m in outliers:
            score = m.get("score", "?")
            print(f"    {fname[:50]}")
            print(f"      佐藤比率:{m['sato_char_ratio']:.3f} 数字:{m['nums_per_min']:.2f} スコア:{score}")
    else:
        print(f"  → だめだめ期にイケイケ的パターンの商談は存在しない")

    return metrics_all, sig_results, ik_scored, dm_scored


# =====================================================================
# 分析2: プロマーケター視点
# =====================================================================
def marketer_analysis(data):
    print(f"\n\n{'=' * 75}")
    print("  【プロマーケター視点】メッセージング・ポジショニング分析")
    print("=" * 75)

    # --- 1. バリュープロポジション構成要素 ---
    print(f"\n  1. バリュープロポジション（価値訴求）の構成分析\n")

    vp_categories = {
        "機能的価値（What）": {
            "keywords": ["求人", "掲載", "表示", "SEO", "検索", "順位", "上位", "アクセス", "PV", "閲覧"],
            "description": "サービスが何をするか"
        },
        "経済的価値（How much）": {
            "keywords": ["コスト", "費用", "円", "万", "投資", "ROI", "回収", "節約", "無駄", "削減", "広告費", "人件費"],
            "description": "費用対効果"
        },
        "感情的価値（Feel）": {
            "keywords": ["安心", "任せ", "寄り添", "一緒に", "伴走", "サポート", "頼", "専任", "専属", "プロ", "人事部"],
            "description": "安心感・信頼感"
        },
        "社会的価値（Status）": {
            "keywords": ["実績", "導入", "法人", "他社", "事例", "成功", "選ば"],
            "description": "他者の行動・社会的証明"
        },
        "緊急性（Why now）": {
            "keywords": ["今", "機会", "損失", "タイミング", "早い", "先に", "競合", "取られ", "キャンペーン", "割引", "限定", "特別"],
            "description": "今行動すべき理由"
        },
        "差別化（Why us）": {
            "keywords": ["人材紹介", "紹介会社", "手数料", "中抜き", "マージン", "優先", "特化", "専門", "メディカ", "弊社だけ"],
            "description": "なぜ他社でなく我々か"
        },
    }

    for period_label, period_key in [("イケイケ期", "ikeike"), ("だめだめ期", "damedame")]:
        print(f"\n  【{period_label}】")
        total_sato_chars = 0
        vp_counts = {cat: 0 for cat in vp_categories}

        for entries in data[period_key]:
            for e in entries:
                if e["is_sato"]:
                    text = e["text"]
                    total_sato_chars += len(text)
                    for cat, info in vp_categories.items():
                        for kw in info["keywords"]:
                            vp_counts[cat] += text.count(kw)

        total_vp = sum(vp_counts.values())
        print(f"  {'カテゴリ':<30} {'回数':>8} {'構成比':>8} {'バー'}")
        print(f"  {'-'*60}")
        for cat, count in sorted(vp_counts.items(), key=lambda x: -x[1]):
            pct = count / total_vp * 100 if total_vp else 0
            bar = "█" * int(pct / 2)
            print(f"  {cat:<30} {count:>8} {pct:>7.1f}% {bar}")

    # --- 2. ペインポイント対応分析 ---
    print(f"\n\n  2. 顧客ペインポイント検出 → 対応分析\n")

    pain_patterns = {
        "採用できない": ["採用できない", "人が来ない", "応募がない", "集まらない", "見つからない"],
        "コスト高い": ["高い", "コスト", "費用", "予算がない", "お金", "金額"],
        "時間がない": ["時間がない", "忙しい", "手が回らない", "余裕がない"],
        "効果不明": ["効果", "わからない", "不安", "うまくいく", "本当に"],
        "他社使用中": ["インディード", "ジョブメドレー", "他社", "今使って", "紹介会社"],
        "決裁問題": ["上に", "相談", "決裁", "確認", "理事", "上司"],
    }

    for period_label, period_key in [("イケイケ期", "ikeike"), ("だめだめ期", "damedame")]:
        pain_found = {k: 0 for k in pain_patterns}
        pain_addressed = {k: 0 for k in pain_patterns}
        total_deals = len(data[period_key])

        for entries in data[period_key]:
            for i, e in enumerate(entries):
                if not e["is_sato"]:
                    for pain_name, keywords in pain_patterns.items():
                        for kw in keywords:
                            if kw in e["text"]:
                                pain_found[pain_name] += 1
                                # 次の3発話以内で佐藤が対応しているか
                                next_sato = [
                                    entries[j]["text"] for j in range(i + 1, min(i + 4, len(entries)))
                                    if entries[j]["is_sato"]
                                ]
                                response_text = " ".join(next_sato)
                                # 対応判定：具体的データや共感語があるか
                                if any(w in response_text for w in ["数字", "件", "円", "調査", "データ", "実績", "事例", "おっしゃる", "そうですよね", "わかります"]):
                                    pain_addressed[pain_name] += 1
                                break

        print(f"\n  【{period_label}】ペインポイント検出→対応率")
        print(f"  {'ペイン':<20} {'検出回数':>10} {'対応回数':>10} {'対応率':>10}")
        print(f"  {'-'*55}")
        for pain in pain_patterns:
            found = pain_found[pain]
            addr = pain_addressed[pain]
            rate = addr / found * 100 if found else 0
            bar = "█" * int(rate / 5)
            print(f"  {pain:<20} {found:>10} {addr:>10} {rate:>9.1f}% {bar}")

    # --- 3. CTA（Call to Action）分析 ---
    print(f"\n\n  3. CTA（行動喚起）パターン分析\n")

    cta_patterns = {
        "直接クロージング": ["やりましょう", "始めましょう", "お申し込み", "契約", "決めて"],
        "テストクロージング": ["いかがですか", "どうですか", "お考え", "ご興味", "前向き"],
        "次回アクション設定": ["次回", "来週", "また", "もう一度", "資料送り", "メール"],
        "期限設定": ["今月", "今週", "本日中", "キャンペーン", "期限", "締め切り"],
        "機会損失訴求": ["損失", "機会", "遅れ", "競合に", "先に", "取られ"],
        "社内稟議サポート": ["資料", "見積", "お出し", "社内", "ご説明", "上の方"],
    }

    for period_label, period_key in [("イケイケ期", "ikeike"), ("だめだめ期", "damedame")]:
        cta_counts = {k: 0 for k in cta_patterns}
        n_deals = len(data[period_key])

        for entries in data[period_key]:
            # 後半（商談の60%以降）の発話に絞る
            start = entries[0]["sec"]
            end = entries[-1]["sec"]
            cutoff = start + (end - start) * 0.6

            for e in entries:
                if e["is_sato"] and e["sec"] >= cutoff:
                    for cta_name, keywords in cta_patterns.items():
                        for kw in keywords:
                            if kw in e["text"]:
                                cta_counts[cta_name] += 1
                                break

        print(f"\n  【{period_label}】商談後半のCTAパターン")
        print(f"  {'CTA種類':<25} {'回数':>8} {'回/商談':>10}")
        print(f"  {'-'*48}")
        for cta, cnt in sorted(cta_counts.items(), key=lambda x: -x[1]):
            per = cnt / n_deals if n_deals else 0
            bar = "█" * int(per * 2)
            print(f"  {cta:<25} {cnt:>8} {per:>10.2f} {bar}")

    # --- 4. ストーリーテリング分析 ---
    print(f"\n\n  4. 説得構造パターン分析\n")

    story_elements = {
        "問題提起": ["課題", "問題", "悩み", "困って", "大変", "厳しい"],
        "データ提示": ["データ", "調査", "統計", "数字", "件数", "人口"],
        "他社事例": ["他の法人", "他の施設", "事例", "成功", "導入先", "他社さん"],
        "比較優位": ["人材紹介", "手数料", "比べ", "違い", "メリット", "優位"],
        "ビジョン提示": ["未来", "将来", "これから", "変わ", "実現", "理想"],
        "具体的プラン": ["プラン", "スタンダード", "ライト", "プレミアム", "ヶ月", "内容"],
    }

    for period_label, period_key in [("イケイケ期", "ikeike"), ("だめだめ期", "damedame")]:
        element_counts = {k: 0 for k in story_elements}
        n = len(data[period_key])

        for entries in data[period_key]:
            for e in entries:
                if e["is_sato"]:
                    for elem, keywords in story_elements.items():
                        for kw in keywords:
                            if kw in e["text"]:
                                element_counts[elem] += 1
                                break

        print(f"\n  【{period_label}】説得構造要素 (/商談)")
        total_e = sum(element_counts.values())
        for elem, cnt in element_counts.items():
            per = cnt / n if n else 0
            pct = cnt / total_e * 100 if total_e else 0
            bar = "█" * int(per)
            print(f"    {elem:<15} {per:>6.1f}回 ({pct:>5.1f}%) {bar}")


# =====================================================================
# 分析3: プロセールス視点
# =====================================================================
def sales_analysis(data):
    print(f"\n\n{'=' * 75}")
    print("  【プロセールス視点】商談技術分析")
    print("=" * 75)

    # --- 1. SPIN分析 ---
    print(f"\n  1. SPIN分析（Situation/Problem/Implication/Need-payoff）\n")

    spin_patterns = {
        "S:状況質問": {
            "keywords": ["今", "現在", "今の", "使って", "やって", "どのような", "何名", "何人", "どれくらい", "いつから"],
            "desc": "現状把握の質問"
        },
        "P:問題質問": {
            "keywords": ["課題", "困って", "問題", "悩み", "大変", "足りない", "できない", "不満", "難しい"],
            "desc": "課題を引き出す質問"
        },
        "I:示唆質問": {
            "keywords": ["もし", "このまま", "放置", "続けた", "将来", "影響", "リスク", "損", "取られ"],
            "desc": "問題の深刻さを認識させる質問"
        },
        "N:解決質問": {
            "keywords": ["解決", "改善", "もしも", "理想", "いかが", "ご興味", "お役に立て", "メリット"],
            "desc": "解決策への欲求を引き出す質問"
        },
    }

    for period_label, period_key in [("イケイケ期", "ikeike"), ("だめだめ期", "damedame")]:
        spin_counts = {k: 0 for k in spin_patterns}
        n = len(data[period_key])

        for entries in data[period_key]:
            for e in entries:
                if e["is_sato"] and ("？" in e["text"] or "?" in e["text"]):
                    for spin, info in spin_patterns.items():
                        for kw in info["keywords"]:
                            if kw in e["text"]:
                                spin_counts[spin] += 1
                                break

        total = sum(spin_counts.values())
        print(f"\n  【{period_label}】SPIN質問構成")
        print(f"  {'タイプ':<25} {'回数':>8} {'回/商談':>10} {'構成比':>8}")
        print(f"  {'-'*55}")
        for spin, cnt in spin_counts.items():
            per = cnt / n if n else 0
            pct = cnt / total * 100 if total else 0
            bar = "█" * int(pct / 3)
            print(f"  {spin:<25} {cnt:>8} {per:>10.1f} {pct:>7.1f}% {bar}")

    # --- 2. オブジェクション ハンドリング ---
    print(f"\n\n  2. オブジェクション（反論）ハンドリング分析\n")

    objection_patterns = {
        "価格反論": {
            "trigger": ["高い", "費用", "予算", "お金", "コスト"],
            "good_response": ["投資", "回収", "比べ", "人件費", "紹介手数料", "年間", "月あたり", "一人あたり"],
            "bad_response": ["安心", "大丈夫", "頑張", "値引き"],
        },
        "必要性反論": {
            "trigger": ["必要ない", "いらない", "今は", "間に合って", "困ってない"],
            "good_response": ["データ", "調査", "実は", "他の施設", "将来", "人口"],
            "bad_response": ["そうですか", "わかりました", "また"],
        },
        "タイミング反論": {
            "trigger": ["今じゃない", "検討", "考え", "まだ", "先に"],
            "good_response": ["今だから", "機会", "損失", "競合", "先に動い", "今の時期"],
            "bad_response": ["わかりました", "また", "いつ頃"],
        },
        "競合反論": {
            "trigger": ["他社", "インディード", "ジョブメドレー", "紹介", "今使って"],
            "good_response": ["違い", "手数料", "比べ", "優先", "中抜", "特化", "メディカだけ"],
            "bad_response": ["そうですね", "なるほど", "確かに"],
        },
    }

    for period_label, period_key in [("イケイケ期", "ikeike"), ("だめだめ期", "damedame")]:
        n = len(data[period_key])
        obj_stats = {}

        for obj_name, patterns in objection_patterns.items():
            found = 0
            good = 0
            bad = 0

            for entries in data[period_key]:
                for i, e in enumerate(entries):
                    if not e["is_sato"]:
                        triggered = any(kw in e["text"] for kw in patterns["trigger"])
                        if triggered:
                            found += 1
                            # 佐藤の次の3発話を確認
                            next_texts = " ".join(
                                entries[j]["text"]
                                for j in range(i + 1, min(i + 4, len(entries)))
                                if entries[j]["is_sato"]
                            )
                            if any(kw in next_texts for kw in patterns["good_response"]):
                                good += 1
                            elif any(kw in next_texts for kw in patterns["bad_response"]):
                                bad += 1

            obj_stats[obj_name] = {"found": found, "good": good, "bad": bad}

        print(f"\n  【{period_label}】オブジェクション対応品質")
        print(f"  {'反論タイプ':<20} {'検出':>6} {'良い対応':>8} {'悪い対応':>8} {'良い率':>8} {'悪い率':>8}")
        print(f"  {'-'*63}")
        for obj_name, stats in obj_stats.items():
            f = stats["found"]
            g = stats["good"]
            b = stats["bad"]
            g_rate = g / f * 100 if f else 0
            b_rate = b / f * 100 if f else 0
            print(f"  {obj_name:<20} {f:>6} {g:>8} {b:>8} {g_rate:>7.1f}% {b_rate:>7.1f}%")

    # --- 3. ラポール（信頼関係構築） ---
    print(f"\n\n  3. ラポール構築パターン分析\n")

    rapport_patterns = {
        "名前呼び": ["さん", "様"],
        "共感フレーズ": ["おっしゃる通り", "そうですよね", "わかります", "大変ですよね", "ですよね"],
        "自己開示": ["実は私も", "正直", "ぶっちゃけ", "本音"],
        "褒め/承認": ["すごい", "素晴らしい", "さすが", "いい", "ご立派"],
        "ミラーリング": ["なるほど", "ですね", "そうなんですね"],
        "バックトラック": ["つまり", "ということは", "おっしゃるのは"],
    }

    for period_label, period_key in [("イケイケ期", "ikeike"), ("だめだめ期", "damedame")]:
        n = len(data[period_key])
        rapport_counts = {k: 0 for k in rapport_patterns}

        for entries in data[period_key]:
            for e in entries:
                if e["is_sato"]:
                    for rp, keywords in rapport_patterns.items():
                        for kw in keywords:
                            rapport_counts[rp] += e["text"].count(kw)

        print(f"\n  【{period_label}】ラポール構築 (/商談)")
        for rp, cnt in sorted(rapport_counts.items(), key=lambda x: -x[1]):
            per = cnt / n if n else 0
            bar = "█" * min(int(per), 40)
            print(f"    {rp:<20} {per:>8.1f} {bar}")

    # --- 4. 商談フェーズ構造分析 ---
    print(f"\n\n  4. 商談フェーズ構造（時間配分）分析\n")

    phase_keywords = {
        "アイスブレイク": ["お世話", "よろしく", "ありがとうございます。あの", "お忙しい"],
        "ヒアリング": ["ですかね", "教えて", "どのような", "どうですか", "何名", "現在"],
        "プレゼン": ["弊社", "サービス", "メディカ", "ご紹介", "ご提案", "させていただ"],
        "データ提示": ["データ", "調査", "求人数", "検索", "人口", "市場", "競合"],
        "クロージング": ["いかが", "どうでしょう", "前向き", "キャンペーン", "お見積", "契約"],
        "ネクスト設定": ["次回", "来週", "資料", "メール", "お送り", "ご連絡"],
    }

    for period_label, period_key in [("イケイケ期", "ikeike"), ("だめだめ期", "damedame")]:
        # 各商談を4分割して各フェーズのキーワード出現を分析
        quarter_phases = {q: {p: 0 for p in phase_keywords} for q in ["Q1(0-25%)", "Q2(25-50%)", "Q3(50-75%)", "Q4(75-100%)"]}
        n = len(data[period_key])

        for entries in data[period_key]:
            total = len(entries)
            for i, e in enumerate(entries):
                if not e["is_sato"]:
                    continue
                q_idx = min(int(i / total * 4), 3)
                q_name = ["Q1(0-25%)", "Q2(25-50%)", "Q3(50-75%)", "Q4(75-100%)"][q_idx]
                for phase, keywords in phase_keywords.items():
                    for kw in keywords:
                        if kw in e["text"]:
                            quarter_phases[q_name][phase] += 1
                            break

        print(f"\n  【{period_label}】商談4分割のフェーズキーワード密度 (/商談)")
        print(f"  {'時間帯':<15}", end="")
        for phase in phase_keywords:
            print(f" {phase[:6]:>8}", end="")
        print()
        print(f"  {'-'*75}")
        for q_name in ["Q1(0-25%)", "Q2(25-50%)", "Q3(50-75%)", "Q4(75-100%)"]:
            print(f"  {q_name:<15}", end="")
            for phase in phase_keywords:
                val = quarter_phases[q_name][phase] / n if n else 0
                print(f" {val:>8.1f}", end="")
            print()

    # --- 5. 沈黙・間の分析 ---
    print(f"\n\n  5. 沈黙パターン分析（発話間隔）\n")

    for period_label, period_key in [("イケイケ期", "ikeike"), ("だめだめ期", "damedame")]:
        gaps = []
        long_gaps = 0
        n = len(data[period_key])
        total_gaps = 0

        for entries in data[period_key]:
            for i in range(1, len(entries)):
                gap = entries[i]["sec"] - entries[i - 1]["sec"]
                if 0 < gap < 300:  # 5分以内
                    gaps.append(gap)
                    total_gaps += 1
                    if gap >= 10:
                        long_gaps += 1

        avg_gap = sum(gaps) / len(gaps) if gaps else 0
        med_gap = percentile(gaps, 50) if gaps else 0
        long_per = long_gaps / n if n else 0

        print(f"  【{period_label}】")
        print(f"    平均発話間隔: {avg_gap:.1f}秒 / 中央値: {med_gap:.1f}秒")
        print(f"    10秒以上の沈黙: {long_gaps}回 ({long_per:.1f}回/商談)")


def main():
    data, filenames = load_all()
    print(f"読み込み完了: イケイケ {len(data['ikeike'])}件, だめだめ {len(data['damedame'])}件\n")

    # 3つの専門家分析を順次実行
    metrics_all, sig_results, ik_scored, dm_scored = ds_analysis(data, filenames)
    marketer_analysis(data)
    sales_analysis(data)

    # JSON保存
    output = {
        "ds_significance": [
            {"label": s[0], "t": s[1], "p": s[2], "d": s[3], "sig": s[4]}
            for s in sig_results
        ],
    }
    out_path = BASE / "expert_analysis.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"\n\n結果保存: {out_path}")


if __name__ == "__main__":
    main()
