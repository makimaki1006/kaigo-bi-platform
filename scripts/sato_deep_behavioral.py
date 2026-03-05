# -*- coding: utf-8 -*-
"""佐藤丈太郎 深層行動分析
- 消えたフレーズ / 新出フレーズ比較
- 顧客の質問内容分類（興味 vs 懐疑）
- 時間帯別の支配率推移
- スコア上位 vs 下位の行動差
- 冒頭スクリプト比較
"""
import io
import json
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
    for name in SATO_NAMES:
        if name.lower() in sp:
            return True
    return False


def parse_transcript(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        lines = f.readlines()
    entries = []
    for line in lines:
        m = LINE_RE.match(line.strip())
        if m:
            sec = parse_seconds(m.group(1), m.group(2), m.group(3))
            entries.append({
                "sec": sec,
                "speaker": m.group(4),
                "is_sato": is_sato(m.group(4)),
                "text": m.group(5),
            })
    return entries


# =========================================================
# 分析1: フレーズ頻度比較（佐藤の発話から2-4gramを抽出）
# =========================================================
def extract_ngrams(text, ns=(2, 3, 4)):
    """テキストからn-gramを抽出"""
    grams = []
    for n in ns:
        for i in range(len(text) - n + 1):
            gram = text[i:i + n]
            if not re.search(r'[\s\d\[\]：]', gram):
                grams.append(gram)
    return grams


def analyze_sato_phrases(period_entries):
    """佐藤の発話から特徴的フレーズを抽出"""
    # 定型フレーズのカウント
    phrase_patterns = {
        # ピッチ系
        "させていただ": 0, "ご紹介": 0, "ご提案": 0, "お伝え": 0,
        "ご案内": 0, "ご説明": 0,
        # データ系
        "求人": 0, "検索": 0, "競合": 0, "エリア": 0,
        "人口": 0, "施設": 0, "件数": 0, "順位": 0,
        "ランキング": 0, "データ": 0, "数字": 0, "分析": 0,
        "市場": 0, "統計": 0, "調査": 0,
        # クロージング系
        "今決め": 0, "今日": 0, "特典": 0, "限定": 0,
        "損失": 0, "機会": 0, "デッドライン": 0, "期限": 0,
        "キャンペーン": 0, "割引": 0,
        # 承認・共感系
        "承知": 0, "なるほど": 0, "ありがと": 0, "おっしゃる": 0,
        "そうですよね": 0, "わかります": 0,
        # 抽象逃げ系
        "実績": 0, "しっかり": 0, "プロ": 0, "安心": 0,
        "頑張": 0, "効果": 0, "サポート": 0, "寄り添": 0,
        # 再定義系
        "広告費": 0, "人事部": 0, "投資": 0, "資産": 0,
        "コスト": 0,
        # 構造暴露系
        "人材紹介": 0, "手数料": 0, "中抜き": 0, "紹介会社": 0,
        "優先順位": 0, "マージン": 0,
        # ヒアリング系
        "ですかね": 0, "いかがですか": 0, "でしょうか": 0,
        "どうですか": 0, "どのような": 0, "教えて": 0,
        "ちなみに": 0, "ぶっちゃけ": 0,
    }

    total_sato_chars = 0
    total_deals = 0

    for entries in period_entries:
        total_deals += 1
        for e in entries:
            if e["is_sato"]:
                text = e["text"]
                total_sato_chars += len(text)
                for phrase in phrase_patterns:
                    phrase_patterns[phrase] += text.count(phrase)

    # 商談あたりに正規化
    per_deal = {k: round(v / total_deals, 2) if total_deals else 0 for k, v in phrase_patterns.items()}
    return phrase_patterns, per_deal, total_deals


# =========================================================
# 分析2: 顧客質問の内容分類
# =========================================================
def classify_customer_questions(period_entries):
    """顧客の質問を分類"""
    categories = {
        "費用・価格": [],  # いくら, 費用, 料金, 金額, 高い
        "効果・実績": [],  # 効果, 実績, 結果, 成果
        "競合比較": [],  # 他社, 違い, 比較, インディード
        "時期・期間": [],  # いつ, 期間, 契約, 解約
        "仕組み・詳細": [],  # どうやって, 仕組み, 具体的, 内容
        "懐疑・否定": [],  # 本当に, でも, ただ, 難しい, 必要ない
        "その他": [],
    }

    keywords = {
        "費用・価格": ["いくら", "費用", "料金", "金額", "高い", "安い", "予算", "コスト", "値段", "円"],
        "効果・実績": ["効果", "実績", "結果", "成果", "どれくらい", "何人", "何件"],
        "競合比較": ["他社", "違い", "比較", "インディード", "ジョブ", "ハローワーク", "媒体", "他に"],
        "時期・期間": ["いつ", "期間", "契約", "解約", "何ヶ月", "何年", "最低"],
        "仕組み・詳細": ["どうやって", "仕組み", "具体的", "内容", "どういう", "なにを", "何を"],
        "懐疑・否定": ["本当に", "でも", "ただ", "難しい", "必要ない", "いらない", "大丈夫", "うーん", "ちょっと"],
    }

    for entries in period_entries:
        for e in entries:
            if not e["is_sato"] and ("？" in e["text"] or "?" in e["text"]):
                text = e["text"]
                classified = False
                for cat, kws in keywords.items():
                    for kw in kws:
                        if kw in text:
                            categories[cat].append(text[:100])
                            classified = True
                            break
                    if classified:
                        break
                if not classified:
                    categories["その他"].append(text[:100])

    return {k: len(v) for k, v in categories.items()}, categories


# =========================================================
# 分析3: 時間帯別支配率推移（5分刻み）
# =========================================================
def time_segment_analysis(period_entries):
    """5分刻みの支配率推移"""
    segments = defaultdict(lambda: {"sato_chars": 0, "cust_chars": 0, "sato_q": 0, "count": 0})

    for entries in period_entries:
        if len(entries) < 10:
            continue
        start = entries[0]["sec"]
        for e in entries:
            elapsed = e["sec"] - start
            seg = min(elapsed // 300, 11)  # 5分刻み、最大60分
            seg_key = f"{seg * 5:02d}-{(seg + 1) * 5:02d}分"
            if e["is_sato"]:
                segments[seg_key]["sato_chars"] += len(e["text"])
                if "？" in e["text"] or "?" in e["text"]:
                    segments[seg_key]["sato_q"] += 1
            else:
                segments[seg_key]["cust_chars"] += len(e["text"])
            segments[seg_key]["count"] += 1

    result = {}
    for seg_key in sorted(segments.keys()):
        d = segments[seg_key]
        total = d["sato_chars"] + d["cust_chars"]
        result[seg_key] = {
            "sato_ratio": round(d["sato_chars"] / total, 3) if total else 0,
            "sato_questions": d["sato_q"],
            "total_chars": total,
        }
    return result


# =========================================================
# 分析4: 冒頭スクリプト比較（最初の10発話）
# =========================================================
def opening_patterns(period_entries):
    """冒頭10発話のパターン分析"""
    openings = []
    for entries in period_entries:
        sato_first_10 = []
        count = 0
        for e in entries:
            if e["is_sato"]:
                sato_first_10.append(e["text"])
                count += 1
                if count >= 10:
                    break
        openings.append(sato_first_10)

    # 冒頭でよく使うフレーズ
    first_phrases = Counter()
    for opening in openings:
        for i, text in enumerate(opening[:5]):
            # 5文字以上の部分文字列をカウント
            for phrase_len in range(5, min(20, len(text) + 1)):
                for start in range(len(text) - phrase_len + 1):
                    substr = text[start:start + phrase_len]
                    if not re.search(r'[\d\[\]]', substr):
                        first_phrases[substr] += 1

    # 最頻出の冒頭パターン（5回以上出現）
    common = [(p, c) for p, c in first_phrases.most_common(200) if c >= 3 and len(p) >= 8]
    return common[:30], openings


# =========================================================
# 分析5: スコア上位 vs 下位の行動差
# =========================================================
def score_behavioral_correlation(period, behavioral_json, analysis_json):
    """Geminiスコアと行動指標の相関"""
    # analysis_results.jsonからスコアを取得
    scores = {}
    for item in analysis_json:
        if item.get("period") == period and item.get("parsed"):
            fname = item["filename"]
            scores[fname] = item["parsed"]["total_score"]

    # behavioral_analysis.jsonから行動指標を取得
    behavioral = behavioral_json.get(period, [])

    # ファイル名でマッチング
    matched = []
    for b in behavioral:
        fname = Path(b["file"]).name
        if fname in scores:
            b["gemini_score"] = scores[fname]
            matched.append(b)

    if not matched:
        return None

    # 上位25% vs 下位25%
    matched.sort(key=lambda x: x["gemini_score"], reverse=True)
    n = len(matched)
    top_q = matched[:max(n // 4, 1)]
    bot_q = matched[-max(n // 4, 1):]

    def avg_metric(lst, key):
        vals = [x[key] for x in lst if key in x]
        return round(sum(vals) / len(vals), 3) if vals else 0

    compare_keys = [
        "sato_char_ratio", "sato_avg_chars", "cust_avg_chars",
        "sato_questions", "sato_questions_per_min",
        "max_mono_chars", "avg_mono_chars", "long_monologues",
        "turns_per_min",
        "first_3min_sato_ratio", "first_3min_sato_questions",
        "cust_long_ratio", "cust_short_ratio",
        "sato_numbers_per_min",
    ]

    result = {}
    for key in compare_keys:
        top_val = avg_metric(top_q, key)
        bot_val = avg_metric(bot_q, key)
        diff = round(top_val - bot_val, 3)
        result[key] = {"top25": top_val, "bottom25": bot_val, "diff": diff}

    return {
        "top25_avg_score": avg_metric(top_q, "gemini_score"),
        "bottom25_avg_score": avg_metric(bot_q, "gemini_score"),
        "top25_count": len(top_q),
        "bottom25_count": len(bot_q),
        "metrics": result,
    }


def main():
    # Transcript読み込み
    period_data = {"ikeike": [], "damedame": []}
    for period, folder in [("ikeike", BASE / "ikeike"), ("damedame", BASE / "damedame")]:
        files = sorted(folder.glob("*.txt"))
        for fp in files:
            entries = parse_transcript(fp)
            if len(entries) >= 10:
                period_data[period].append(entries)

    print(f"読み込み: イケイケ {len(period_data['ikeike'])}件, だめだめ {len(period_data['damedame'])}件")

    # ===== 分析1: フレーズ頻度比較 =====
    print(f"\n{'='*70}")
    print("  分析1: フレーズ頻度比較（佐藤の発話 / 商談あたり）")
    print(f"{'='*70}\n")

    ik_total, ik_per, ik_n = analyze_sato_phrases(period_data["ikeike"])
    dm_total, dm_per, dm_n = analyze_sato_phrases(period_data["damedame"])

    # カテゴリ別に表示
    categories = {
        "データ系（武器）": ["求人", "検索", "競合", "エリア", "人口", "施設", "件数", "順位", "ランキング", "データ", "数字", "分析", "市場", "統計", "調査"],
        "構造暴露系": ["人材紹介", "手数料", "中抜き", "紹介会社", "優先順位", "マージン"],
        "再定義系": ["広告費", "人事部", "投資", "資産", "コスト"],
        "クロージング系": ["今決め", "今日", "特典", "限定", "損失", "機会", "デッドライン", "期限", "キャンペーン", "割引"],
        "ヒアリング系": ["ですかね", "いかがですか", "でしょうか", "どうですか", "どのような", "教えて", "ちなみに", "ぶっちゃけ"],
        "抽象逃げ系": ["実績", "しっかり", "プロ", "安心", "頑張", "効果", "サポート", "寄り添"],
        "ピッチ系": ["させていただ", "ご紹介", "ご提案", "お伝え", "ご案内", "ご説明"],
        "承認・共感系": ["承知", "なるほど", "ありがと", "おっしゃる", "そうですよね", "わかります"],
    }

    significant_changes = []

    for cat_name, phrases in categories.items():
        print(f"\n  【{cat_name}】")
        print(f"  {'フレーズ':<12} {'イケイケ/商談':>14} {'だめだめ/商談':>14} {'差分':>10} {'変化率':>10}")
        print(f"  {'-'*65}")
        for phrase in phrases:
            ik_v = ik_per.get(phrase, 0)
            dm_v = dm_per.get(phrase, 0)
            diff = dm_v - ik_v
            pct = ((dm_v - ik_v) / ik_v * 100) if ik_v != 0 else (999 if dm_v > 0 else 0)
            sign = "+" if diff > 0 else ""
            pct_str = f"{sign}{pct:.0f}%" if abs(pct) < 500 else ("NEW" if dm_v > ik_v else "消滅")
            print(f"  {phrase:<12} {ik_v:>14.2f} {dm_v:>14.2f} {sign}{diff:>9.2f} {pct_str:>10}")
            if abs(pct) >= 30 and (ik_v >= 0.5 or dm_v >= 0.5):
                significant_changes.append((phrase, cat_name, ik_v, dm_v, pct))

    print(f"\n\n{'='*70}")
    print("  ★ 大きく変化したフレーズ TOP (変化率30%以上 & 0.5回/商談以上)")
    print(f"{'='*70}\n")
    significant_changes.sort(key=lambda x: abs(x[4]), reverse=True)
    for i, (phrase, cat, ik_v, dm_v, pct) in enumerate(significant_changes[:20], 1):
        sign = "+" if pct > 0 else ""
        print(f"  {i:2d}. 「{phrase}」({cat})")
        print(f"      {ik_v:.2f} → {dm_v:.2f} 回/商談 ({sign}{pct:.0f}%)")

    # ===== 分析2: 顧客質問分類 =====
    print(f"\n\n{'='*70}")
    print("  分析2: 顧客の質問内容分類")
    print(f"{'='*70}\n")

    ik_q_counts, ik_q_samples = classify_customer_questions(period_data["ikeike"])
    dm_q_counts, dm_q_samples = classify_customer_questions(period_data["damedame"])

    ik_q_total = sum(ik_q_counts.values())
    dm_q_total = sum(dm_q_counts.values())

    print(f"  {'カテゴリ':<20} {'イケイケ件数':>10} {'比率':>8} {'だめだめ件数':>10} {'比率':>8} {'変化':>8}")
    print(f"  {'-'*70}")
    for cat in ik_q_counts:
        ik_c = ik_q_counts[cat]
        dm_c = dm_q_counts.get(cat, 0)
        ik_pct = ik_c / ik_q_total * 100 if ik_q_total else 0
        dm_pct = dm_c / dm_q_total * 100 if dm_q_total else 0
        diff = dm_pct - ik_pct
        sign = "+" if diff > 0 else ""
        print(f"  {cat:<20} {ik_c:>10} {ik_pct:>7.1f}% {dm_c:>10} {dm_pct:>7.1f}% {sign}{diff:>6.1f}pt")

    # サンプル表示
    for cat in ["費用・価格", "懐疑・否定", "効果・実績"]:
        print(f"\n  [{cat}] だめだめ期サンプル:")
        for s in dm_q_samples.get(cat, [])[:5]:
            print(f"    「{s}」")

    # ===== 分析3: 時間帯別支配率推移 =====
    print(f"\n\n{'='*70}")
    print("  分析3: 時間帯別 佐藤の文字支配率推移（5分刻み）")
    print(f"{'='*70}\n")

    ik_seg = time_segment_analysis(period_data["ikeike"])
    dm_seg = time_segment_analysis(period_data["damedame"])

    all_segs = sorted(set(list(ik_seg.keys()) + list(dm_seg.keys())))
    print(f"  {'時間帯':<12} {'イケイケ支配率':>14} {'だめだめ支配率':>14} {'差分':>10} {'イケ質問':>8} {'ダメ質問':>8}")
    print(f"  {'-'*70}")
    for seg in all_segs:
        ik_r = ik_seg.get(seg, {}).get("sato_ratio", 0)
        dm_r = dm_seg.get(seg, {}).get("sato_ratio", 0)
        ik_q = ik_seg.get(seg, {}).get("sato_questions", 0)
        dm_q = dm_seg.get(seg, {}).get("sato_questions", 0)
        diff = dm_r - ik_r
        sign = "+" if diff > 0 else ""
        # 支配率のバー表示
        ik_bar = "█" * int(ik_r * 20) + "░" * (20 - int(ik_r * 20))
        dm_bar = "█" * int(dm_r * 20) + "░" * (20 - int(dm_r * 20))
        print(f"  {seg:<12} {ik_r:>7.1%} {ik_bar}  {dm_r:>7.1%} {dm_bar}  {sign}{diff:>6.1%}  {ik_q:>6}  {dm_q:>6}")

    # ===== 分析4: スコアと行動指標の相関 =====
    print(f"\n\n{'='*70}")
    print("  分析4: Geminiスコア上位25% vs 下位25% の行動指標差")
    print(f"{'='*70}\n")

    with open(BASE / "behavioral_analysis.json", "r", encoding="utf-8") as f:
        beh_json = json.load(f)
    with open(BASE / "analysis_results.json", "r", encoding="utf-8") as f:
        analysis_json = json.load(f)

    for period, label in [("ikeike", "イケイケ期"), ("damedame", "だめだめ期")]:
        corr = score_behavioral_correlation(period, beh_json, analysis_json)
        if not corr:
            print(f"  {label}: マッチングデータなし")
            continue
        print(f"\n  【{label}】 上位25%平均: {corr['top25_avg_score']:.0f}点 ({corr['top25_count']}件) / 下位25%平均: {corr['bottom25_avg_score']:.0f}点 ({corr['bottom25_count']}件)")
        print(f"  {'指標':<30} {'上位25%':>10} {'下位25%':>10} {'差分':>10}")
        print(f"  {'-'*65}")

        label_map = {
            "sato_char_ratio": "佐藤文字比率",
            "sato_avg_chars": "佐藤平均発話長",
            "cust_avg_chars": "顧客平均発話長",
            "sato_questions": "佐藤質問数/商談",
            "sato_questions_per_min": "佐藤質問数/分",
            "max_mono_chars": "最長独白(文字)",
            "avg_mono_chars": "平均独白(文字)",
            "long_monologues": "200文字超独白数",
            "turns_per_min": "ターン数/分",
            "first_3min_sato_ratio": "冒頭3分佐藤比率",
            "first_3min_sato_questions": "冒頭3分質問数",
            "cust_long_ratio": "顧客長い発話比率",
            "cust_short_ratio": "顧客相槌比率",
            "sato_numbers_per_min": "数字使用/分",
        }

        for key, vals in corr["metrics"].items():
            lbl = label_map.get(key, key)
            diff = vals["diff"]
            sign = "+" if diff > 0 else ""
            print(f"  {lbl:<30} {vals['top25']:>10.3f} {vals['bottom25']:>10.3f} {sign}{diff:>9.3f}")

    # ===== 分析5: だめだめ期 顧客の長い発話の中身 =====
    print(f"\n\n{'='*70}")
    print("  分析5: だめだめ期 顧客の長い発話（50文字以上）のサンプル")
    print(f"{'='*70}\n")

    cust_long_texts = []
    for entries in period_data["damedame"]:
        for e in entries:
            if not e["is_sato"] and len(e["text"]) >= 50:
                cust_long_texts.append(e["text"][:150])

    # キーワードカウント
    concern_keywords = Counter()
    concern_words = ["費用", "高い", "コスト", "金額", "予算", "必要", "難しい",
                     "考え", "検討", "相談", "上", "決裁", "うち",
                     "今", "タイミング", "時期", "まだ", "ちょっと"]
    for text in cust_long_texts:
        for kw in concern_words:
            if kw in text:
                concern_keywords[kw] += 1

    print(f"  長い発話総数: {len(cust_long_texts)}件\n")
    print(f"  頻出キーワード:")
    for kw, cnt in concern_keywords.most_common(15):
        bar = "█" * min(cnt // 2, 30)
        print(f"    「{kw}」{cnt:>5}件 {bar}")

    print(f"\n  サンプル（ランダム10件）:")
    import random
    random.seed(42)
    for text in random.sample(cust_long_texts, min(10, len(cust_long_texts))):
        print(f"    「{text}」")

    # ===== 結果JSON保存 =====
    output = {
        "phrase_comparison": {
            "ikeike_per_deal": ik_per,
            "damedame_per_deal": dm_per,
            "significant_changes": [
                {"phrase": s[0], "category": s[1], "ikeike": s[2], "damedame": s[3], "pct": s[4]}
                for s in significant_changes
            ],
        },
        "customer_questions": {
            "ikeike": ik_q_counts,
            "damedame": dm_q_counts,
        },
        "time_segments": {
            "ikeike": ik_seg,
            "damedame": dm_seg,
        },
    }
    out_path = BASE / "deep_behavioral_analysis.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"\n結果保存: {out_path}")


if __name__ == "__main__":
    main()
