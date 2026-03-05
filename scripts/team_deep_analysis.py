# -*- coding: utf-8 -*-
"""チーム全員深掘り分析: SPIN分析 + 受注パターン教師あり学習
1. SPIN話法分析（全メンバー × 全商談）
2. フレーズパターン分析（データ武装・CTA・ラポール等）
3. Salesforce Opportunity連携（受注/失注別行動パターン）
4. 特徴量重要度ランキング（受注と相関する行動指標）
5. 成功テンプレート生成
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

# === 設定 ===
TEAM_DIR = Path("data/output/team_comparison")
OUTPUT_DIR = Path("data/output/team_comparison")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

LINE_RE = re.compile(r"\[(\d{2}):(\d{2}):(\d{2})\.\d+\]\s+(.+?):\s+(.*)")

# コンサル除外
CONSUL_EMAILS = {"h_obata", "a_tanji", "s_endo"}

# セールスメンバー
SALES_MEMBERS = {
    "j_sato": "佐藤丈太郎",
    "yo_ichiki": "市来洋平",
    "s_shimatani": "嶋谷",
    "k_kobayashi": "小林幸太",
    "i_kumagai": "熊谷",
    "h_matsukaze": "松風穂香",
    "s_shinoki": "篠木柊志",
    "k_sawada": "澤田和歩",
    "y_fukabori": "深堀勇侍",
    "s_hattori": "服部翔太郎",
    "n_kiyohira": "清飛羅直樹",
    "y_haino": "灰野大和",
    "r_shimura": "志村亮介",
    "y_tejima": "手島唯那",
    "ko_suzuki": "鈴木孝太郎",
    "i_kitao": "北尾一朗",
    "t_kitamoto": "北本天祐",
    "r_uehata": "上畑綾太郎",
    "r_yao": "八尾龍斗",
    "d_watanabe": "渡邉大貴",
    "a_yasutomo": "安友愛理沙",
    "h_tsuji": "辻花佳",
}

# ===== SPIN話法キーワード =====
SPIN_PATTERNS = {
    "S": {
        "label": "状況質問",
        "keywords": ["今", "現在", "今の", "使って", "やって", "どのような",
                     "何名", "何人", "どれくらい", "いつから", "普段", "現状",
                     "何を", "どこ", "誰が", "体制", "運用"],
    },
    "P": {
        "label": "問題質問",
        "keywords": ["課題", "困って", "問題", "悩み", "大変", "足りない",
                     "できない", "不満", "難しい", "苦労", "辛い", "なかなか",
                     "うまくいか", "厳しい", "ネック"],
    },
    "I": {
        "label": "示唆質問",
        "keywords": ["もし", "このまま", "放置", "続けた", "将来", "影響",
                     "リスク", "損", "取られ", "他社", "競合", "先に",
                     "遅れ", "機会損失", "このペース"],
    },
    "N": {
        "label": "解決質問",
        "keywords": ["解決", "改善", "もしも", "理想", "いかが", "ご興味",
                     "お役に立て", "メリット", "効果", "できたら", "実現",
                     "可能", "ご提案", "ご検討"],
    },
}

# ===== フレーズカテゴリ =====
PHRASE_CATEGORIES = {
    "data_evidence": {
        "label": "データ根拠",
        "keywords": ["調査", "市場", "人口", "競合", "統計", "実績",
                     "データ", "割合", "パーセント", "%", "件数", "増加",
                     "減少", "推移", "平均", "業界", "数字"],
    },
    "close_urgency": {
        "label": "クロージング・緊急性",
        "keywords": ["キャンペーン", "割引", "限定", "今月", "期限", "特別",
                     "お申し込み", "契約", "決めて", "早め", "今なら",
                     "残り", "月末", "損失", "機会"],
    },
    "rapport_empathy": {
        "label": "ラポール・共感",
        "keywords": ["おっしゃる通り", "なるほど", "確かに", "そうですよね",
                     "わかります", "素晴らしい", "さすが", "ありがとう",
                     "嬉しい", "感謝", "承知"],
    },
    "value_proposition": {
        "label": "価値提案",
        "keywords": ["メリット", "効果", "成果", "コスト削減", "改善",
                     "向上", "成功事例", "事例", "お客様", "導入",
                     "ROI", "投資対効果"],
    },
    "objection_handle": {
        "label": "反論処理",
        "keywords": ["ご不安", "ご心配", "他社", "比較", "費用", "高い",
                     "予算", "コスト", "時間", "手間", "リスク",
                     "検討", "上司", "相談"],
    },
    "name_calling": {
        "label": "名前呼び",
        "keywords": ["様", "さん"],
    },
    "number_specific": {
        "label": "具体的数字",
        "pattern": re.compile(r'\d+(?:\.\d+)?(?:円|万|億|%|パーセント|名|人|件|社|倍|年|月|回|時間)'),
    },
}

# ===== 時間セグメント分析 =====
TIME_SEGMENTS = [
    (0, 180, "0-3分"),
    (180, 300, "3-5分"),
    (300, 600, "5-10分"),
    (600, 1200, "10-20分"),
    (1200, 1800, "20-30分"),
    (1800, 3600, "30分以降"),
]


def parse_seconds(h, m, s):
    return int(h) * 3600 + int(m) * 60 + int(s)


def parse_transcript(filepath):
    """トランスクリプトをパース"""
    with open(filepath, "r", encoding="utf-8") as f:
        lines = f.readlines()
    entries = []
    for line in lines:
        m = LINE_RE.match(line.strip())
        if m:
            sec = parse_seconds(m.group(1), m.group(2), m.group(3))
            entries.append({
                "sec": sec, "speaker": m.group(4), "text": m.group(5),
            })
    return entries


def identify_host(entries, member_name):
    """ホスト（営業側）を特定"""
    sc = Counter(e["speaker"] for e in entries)
    # メンバー名でマッチ
    for sp in sc:
        for part in member_name:
            if len(part) >= 2 and part in sp:
                return sp
    # 最頻出 = ホスト
    return sc.most_common(1)[0][0] if sc else ""


def deep_analyze_transcript(entries, host_speaker):
    """1商談の深掘り分析"""
    if len(entries) < 10:
        return None

    host = [e for e in entries if e["speaker"] == host_speaker]
    cust = [e for e in entries if e["speaker"] != host_speaker]
    if not host or not cust:
        return None

    total_chars = sum(len(e["text"]) for e in entries)
    host_chars = sum(len(e["text"]) for e in host)
    duration = (entries[-1]["sec"] - entries[0]["sec"]) / 60 if len(entries) > 1 else 1
    start = entries[0]["sec"]
    if duration < 3:
        return None

    # === 基本指標 ===
    host_q = [e for e in host if "？" in e["text"] or "?" in e["text"]]
    cust_q = [e for e in cust if "？" in e["text"] or "?" in e["text"]]

    # モノローグ
    monos = []
    streak_chars = 0
    for e in entries:
        if e["speaker"] == host_speaker:
            streak_chars += len(e["text"])
        else:
            if streak_chars > 0:
                monos.append(streak_chars)
            streak_chars = 0
    if streak_chars > 0:
        monos.append(streak_chars)

    # ターン
    turns = 0
    prev_is_host = None
    for e in entries:
        is_h = e["speaker"] == host_speaker
        if prev_is_host is not None and is_h != prev_is_host:
            turns += 1
        prev_is_host = is_h

    # 冒頭3分
    f3 = [e for e in entries if e["sec"] - start <= 180]
    f3_host_c = sum(len(e["text"]) for e in f3 if e["speaker"] == host_speaker)
    f3_total_c = sum(len(e["text"]) for e in f3)
    f3_host_q = sum(1 for e in f3 if e["speaker"] == host_speaker and ("？" in e["text"] or "?" in e["text"]))

    # 数字
    num_pat = re.compile(r'\d+')
    host_nums = sum(len(num_pat.findall(e["text"])) for e in host)

    # 顧客エンゲージメント
    cust_long = sum(1 for e in cust if len(e["text"]) >= 30)
    cust_short = sum(1 for e in cust if len(e["text"]) <= 10)

    metrics = {
        "duration": round(duration, 1),
        "host_char_ratio": round(host_chars / total_chars, 3) if total_chars else 0,
        "host_avg_len": round(host_chars / len(host), 1) if host else 0,
        "cust_avg_len": round(sum(len(e["text"]) for e in cust) / len(cust), 1) if cust else 0,
        "host_q_per_min": round(len(host_q) / duration, 3) if duration else 0,
        "cust_q_total": len(cust_q),
        "nums_per_min": round(host_nums / duration, 3) if duration else 0,
        "avg_mono": round(sum(monos) / len(monos), 1) if monos else 0,
        "max_mono": max(monos, default=0),
        "long_mono": sum(1 for m in monos if m >= 200),
        "turns_per_min": round(turns / duration, 2) if duration else 0,
        "f3_host_ratio": round(f3_host_c / f3_total_c, 3) if f3_total_c else 0,
        "f3_host_q": f3_host_q,
        "cust_long_ratio": round(cust_long / len(cust), 3) if cust else 0,
        "cust_short_ratio": round(cust_short / len(cust), 3) if cust else 0,
    }

    # === SPIN分析 ===
    spin_counts = {"S": 0, "P": 0, "I": 0, "N": 0}
    spin_examples = {"S": [], "P": [], "I": [], "N": []}
    for e in host_q:
        classified = False
        for spin_type, info in SPIN_PATTERNS.items():
            for kw in info["keywords"]:
                if kw in e["text"]:
                    spin_counts[spin_type] += 1
                    if len(spin_examples[spin_type]) < 3:
                        spin_examples[spin_type].append(e["text"][:80])
                    classified = True
                    break
            if classified:
                break

    total_spin = sum(spin_counts.values())
    metrics["spin_S"] = spin_counts["S"]
    metrics["spin_P"] = spin_counts["P"]
    metrics["spin_I"] = spin_counts["I"]
    metrics["spin_N"] = spin_counts["N"]
    metrics["spin_total"] = total_spin
    metrics["spin_S_ratio"] = round(spin_counts["S"] / total_spin, 3) if total_spin else 0
    metrics["spin_P_ratio"] = round(spin_counts["P"] / total_spin, 3) if total_spin else 0
    metrics["spin_I_ratio"] = round(spin_counts["I"] / total_spin, 3) if total_spin else 0
    metrics["spin_N_ratio"] = round(spin_counts["N"] / total_spin, 3) if total_spin else 0
    metrics["spin_PI_ratio"] = round((spin_counts["P"] + spin_counts["I"]) / total_spin, 3) if total_spin else 0

    # === フレーズカテゴリ分析 ===
    host_text_all = " ".join(e["text"] for e in host)
    for cat_key, cat_info in PHRASE_CATEGORIES.items():
        if "pattern" in cat_info:
            count = len(cat_info["pattern"].findall(host_text_all))
        else:
            count = sum(host_text_all.count(kw) for kw in cat_info["keywords"])
        metrics[f"phrase_{cat_key}"] = count
        metrics[f"phrase_{cat_key}_per_min"] = round(count / duration, 3) if duration else 0

    # === 時間セグメント分析 ===
    for seg_start, seg_end, seg_label in TIME_SEGMENTS:
        seg_entries = [e for e in entries if seg_start <= (e["sec"] - start) < seg_end]
        if not seg_entries:
            metrics[f"seg_{seg_label}_host_ratio"] = 0
            metrics[f"seg_{seg_label}_host_q"] = 0
            continue
        seg_host_c = sum(len(e["text"]) for e in seg_entries if e["speaker"] == host_speaker)
        seg_total_c = sum(len(e["text"]) for e in seg_entries)
        seg_host_q = sum(1 for e in seg_entries
                        if e["speaker"] == host_speaker and ("？" in e["text"] or "?" in e["text"]))
        metrics[f"seg_{seg_label}_host_ratio"] = round(seg_host_c / seg_total_c, 3) if seg_total_c else 0
        metrics[f"seg_{seg_label}_host_q"] = seg_host_q

    # === クロージングフェーズ分析（終盤20%） ===
    total_entries = len(entries)
    close_start_idx = int(total_entries * 0.8)
    close_entries = entries[close_start_idx:]
    close_host = [e for e in close_entries if e["speaker"] == host_speaker]
    close_text = " ".join(e["text"] for e in close_host)
    close_kw_count = sum(close_text.count(kw) for kw in PHRASE_CATEGORIES["close_urgency"]["keywords"])
    close_q = sum(1 for e in close_host if "？" in e["text"] or "?" in e["text"])
    metrics["close_phase_kw"] = close_kw_count
    metrics["close_phase_q"] = close_q
    metrics["close_phase_host_ratio"] = round(
        sum(len(e["text"]) for e in close_host) / sum(len(e["text"]) for e in close_entries), 3
    ) if close_entries and sum(len(e["text"]) for e in close_entries) > 0 else 0

    # === 沈黙分析（10秒以上の間） ===
    silences = 0
    for i in range(1, len(entries)):
        gap = entries[i]["sec"] - entries[i - 1]["sec"]
        if gap >= 10:
            silences += 1
    metrics["silence_count"] = silences
    metrics["silence_per_min"] = round(silences / duration, 3) if duration else 0

    return metrics


def load_all_transcripts():
    """全メンバーのトランスクリプトを読み込み"""
    all_data = {}  # {email_prefix: {"2024h1": [...], "2026jan": [...]}}

    for period in ["2024h1", "2026jan"]:
        period_dir = TEAM_DIR / period
        if not period_dir.exists():
            continue
        for member_dir in period_dir.iterdir():
            if not member_dir.is_dir():
                continue
            prefix = member_dir.name
            if prefix in CONSUL_EMAILS:
                continue
            if prefix not in SALES_MEMBERS:
                continue

            if prefix not in all_data:
                all_data[prefix] = {"2024h1": [], "2026jan": [], "name": SALES_MEMBERS[prefix]}

            for fp in sorted(member_dir.glob("*.txt")):
                entries = parse_transcript(fp)
                if len(entries) >= 10:
                    host = identify_host(entries, SALES_MEMBERS[prefix])
                    m = deep_analyze_transcript(entries, host)
                    if m:
                        m["filename"] = fp.name
                        all_data[prefix][period].append(m)

    return all_data


def avg_metrics(metrics_list, keys):
    """指標の平均を計算"""
    if not metrics_list:
        return {k: 0 for k in keys}
    result = {}
    for k in keys:
        vals = [m[k] for m in metrics_list if k in m]
        result[k] = round(sum(vals) / len(vals), 3) if vals else 0
    return result


def stdev(vals):
    if len(vals) < 2:
        return 0
    m = sum(vals) / len(vals)
    return math.sqrt(sum((x - m) ** 2 for x in vals) / (len(vals) - 1))


def pearson_r(x_vals, y_vals):
    n = len(x_vals)
    if n < 5:
        return 0
    mx = sum(x_vals) / n
    my = sum(y_vals) / n
    num = sum((x - mx) * (y - my) for x, y in zip(x_vals, y_vals))
    dx = math.sqrt(sum((x - mx) ** 2 for x in x_vals))
    dy = math.sqrt(sum((y - my) ** 2 for y in y_vals))
    return num / (dx * dy) if dx > 0 and dy > 0 else 0


def welch_t(vals1, vals2):
    n1, n2 = len(vals1), len(vals2)
    if n1 < 3 or n2 < 3:
        return 0, 1.0
    m1, m2 = sum(vals1) / n1, sum(vals2) / n2
    s1, s2 = stdev(vals1), stdev(vals2)
    se = math.sqrt(s1 ** 2 / n1 + s2 ** 2 / n2) if (s1 > 0 or s2 > 0) else 1
    t = (m1 - m2) / se if se > 0 else 0
    p = 2 * (1 - 0.5 * (1 + math.erf(abs(t) / math.sqrt(2))))
    return round(t, 3), round(p, 4)


# =====================================================================
# Salesforce Opportunity データ取得
# =====================================================================
def get_opportunity_data():
    """Salesforce Opportunityデータを取得して受注/失注を分類"""
    try:
        project_root = str(Path(__file__).parent.parent)
        sys.path.insert(0, project_root)
        sys.path.insert(0, str(Path(project_root) / "src"))
        from src.api.salesforce_client import SalesforceClient
        client = SalesforceClient()
        client.authenticate()

        import requests as req
        # 対象メンバーのOwnerIdを取得
        print("  Salesforce: Owner情報を取得中...")
        query = """
        SELECT Id, Name, Email
        FROM User
        WHERE IsActive = true AND Email != null
        """
        url = f"{client.instance_url}/services/data/{client.api_version}/query"
        resp = req.get(url, headers=client._get_headers(),
                       params={"q": query})
        users = resp.json().get("records", [])

        # メールでマッチング
        owner_map = {}  # email_prefix -> OwnerId
        for u in users:
            email = (u.get("Email") or "").lower()
            for prefix, name in SALES_MEMBERS.items():
                if prefix.replace("_", "") in email.replace("_", "").replace("-", "").replace(".", ""):
                    owner_map[prefix] = u["Id"]
                    break

        print(f"  マッチしたOwner: {len(owner_map)}名")

        # 各OwnerのOpportunityを取得
        opp_data = {}
        for prefix, owner_id in owner_map.items():
            # 2024H1
            q1 = f"""
            SELECT Id, Name, StageName, CloseDate, Amount, AccountId, Account.Name,
                   CreatedDate, Probability
            FROM Opportunity
            WHERE OwnerId = '{owner_id}'
            AND CreatedDate >= 2024-01-01T00:00:00Z
            AND CreatedDate < 2024-07-01T00:00:00Z
            """
            resp1 = req.get(url, headers=client._get_headers(), params={"q": q1})
            opps_2024 = resp1.json().get("records", [])

            # 2026Jan
            q2 = f"""
            SELECT Id, Name, StageName, CloseDate, Amount, AccountId, Account.Name,
                   CreatedDate, Probability
            FROM Opportunity
            WHERE OwnerId = '{owner_id}'
            AND CreatedDate >= 2026-01-01T00:00:00Z
            AND CreatedDate < 2026-02-01T00:00:00Z
            """
            resp2 = req.get(url, headers=client._get_headers(), params={"q": q2})
            opps_2026 = resp2.json().get("records", [])

            def classify(opps):
                won = [o for o in opps if "Won" in (o.get("StageName") or "")
                       or "受注" in (o.get("StageName") or "")
                       or "成約" in (o.get("StageName") or "")]
                lost = [o for o in opps if "Lost" in (o.get("StageName") or "")
                        or "失注" in (o.get("StageName") or "")
                        or "不成立" in (o.get("StageName") or "")]
                active = [o for o in opps if o not in won and o not in lost]
                return {"won": len(won), "lost": len(lost), "active": len(active),
                        "total": len(opps),
                        "win_rate": round(len(won) / (len(won) + len(lost)), 3) if (len(won) + len(lost)) > 0 else 0}

            opp_data[prefix] = {
                "2024h1": classify(opps_2024),
                "2026jan": classify(opps_2026),
                "name": SALES_MEMBERS[prefix],
            }

        return opp_data

    except Exception as e:
        print(f"  Salesforce接続エラー: {e}")
        print("  → Opportunity無しで分析を続行します")
        return None


# =====================================================================
# メイン分析
# =====================================================================
def main():
    print("=" * 75)
    print("  営業チーム深掘り分析: SPIN + 受注パターン教師あり学習")
    print("=" * 75)

    # 1. トランスクリプト読み込み
    print("\n[1/5] トランスクリプト読み込み中...")
    all_data = load_all_transcripts()

    for prefix, info in sorted(all_data.items()):
        n24 = len(info["2024h1"])
        n26 = len(info["2026jan"])
        if n24 + n26 > 0:
            print(f"  {info['name']:<12} 2024H1:{n24:>3}件  2026Jan:{n26:>3}件")

    # 2. Salesforce Opportunity取得
    print("\n[2/5] Salesforce Opportunityデータ取得中...")
    opp_data = get_opportunity_data()

    if opp_data:
        print(f"\n  メンバー別受注データ:")
        print(f"  {'名前':<12} {'期間':<8} {'受注':>4} {'失注':>4} {'進行中':>4} {'受注率':>8}")
        print(f"  {'-'*50}")
        for prefix in sorted(opp_data.keys()):
            info = opp_data[prefix]
            for period in ["2024h1", "2026jan"]:
                d = info[period]
                if d["total"] > 0:
                    print(f"  {info['name']:<12} {period:<8} {d['won']:>4} {d['lost']:>4} {d['active']:>4} {d['win_rate']:>7.1%}")

    # 3. SPIN分析
    print(f"\n{'=' * 75}")
    print(f"  [3/5] SPIN話法分析")
    print(f"{'=' * 75}")

    spin_keys = ["spin_S", "spin_P", "spin_I", "spin_N", "spin_total",
                 "spin_S_ratio", "spin_P_ratio", "spin_I_ratio", "spin_N_ratio", "spin_PI_ratio"]
    main_keys = ["host_char_ratio", "host_q_per_min", "nums_per_min",
                 "f3_host_ratio", "f3_host_q", "cust_short_ratio",
                 "phrase_data_evidence_per_min", "phrase_close_urgency_per_min",
                 "phrase_rapport_empathy_per_min", "phrase_value_proposition_per_min",
                 "phrase_objection_handle_per_min", "phrase_name_calling_per_min",
                 "phrase_number_specific_per_min",
                 "close_phase_kw", "close_phase_q", "silence_per_min"]

    all_keys = spin_keys + main_keys

    for period in ["2024h1", "2026jan"]:
        period_label = "2024年前半" if period == "2024h1" else "2026年1月"
        print(f"\n  === {period_label} ===")
        print(f"  {'名前':<12} {'件数':>4} {'S':>5} {'P':>5} {'I':>5} {'N':>5} {'計':>5} {'S%':>6} {'P%':>6} {'I%':>6} {'N%':>6} {'PI%':>6}")
        print(f"  {'-'*85}")

        members_spin = []
        for prefix in sorted(all_data.keys()):
            info = all_data[prefix]
            mlist = info[period]
            if not mlist:
                continue
            avg = avg_metrics(mlist, spin_keys)
            members_spin.append((info["name"], len(mlist), avg))

        members_spin.sort(key=lambda x: x[2]["spin_PI_ratio"], reverse=True)
        for name, cnt, avg in members_spin:
            print(f"  {name:<12} {cnt:>4}"
                  f" {avg['spin_S']:>5.1f} {avg['spin_P']:>5.1f}"
                  f" {avg['spin_I']:>5.1f} {avg['spin_N']:>5.1f}"
                  f" {avg['spin_total']:>5.1f}"
                  f" {avg['spin_S_ratio']:>5.1%} {avg['spin_P_ratio']:>5.1%}"
                  f" {avg['spin_I_ratio']:>5.1%} {avg['spin_N_ratio']:>5.1%}"
                  f" {avg['spin_PI_ratio']:>5.1%}")

    # 4. フレーズパターン分析
    print(f"\n{'=' * 75}")
    print(f"  [3b] フレーズパターン分析（/分）")
    print(f"{'=' * 75}")

    phrase_keys = [k for k in main_keys if k.startswith("phrase_")]
    phrase_labels = {
        "phrase_data_evidence_per_min": "データ根拠",
        "phrase_close_urgency_per_min": "Close緊急",
        "phrase_rapport_empathy_per_min": "ラポール",
        "phrase_value_proposition_per_min": "価値提案",
        "phrase_objection_handle_per_min": "反論処理",
        "phrase_name_calling_per_min": "名前呼び",
        "phrase_number_specific_per_min": "具体数字",
    }

    for period in ["2024h1", "2026jan"]:
        period_label = "2024年前半" if period == "2024h1" else "2026年1月"
        print(f"\n  === {period_label} ===")
        header = f"  {'名前':<12} {'件':>3}"
        for pk in phrase_keys:
            header += f" {phrase_labels.get(pk, pk)[-6:]:>7}"
        header += f" {'Close_Q':>7} {'沈黙/分':>7}"
        print(header)
        print(f"  {'-'*100}")

        members_phrase = []
        for prefix in sorted(all_data.keys()):
            info = all_data[prefix]
            mlist = info[period]
            if not mlist:
                continue
            avg = avg_metrics(mlist, phrase_keys + ["close_phase_q", "silence_per_min"])
            members_phrase.append((info["name"], len(mlist), avg))

        members_phrase.sort(key=lambda x: x[2].get("phrase_data_evidence_per_min", 0), reverse=True)
        for name, cnt, avg in members_phrase:
            row = f"  {name:<12} {cnt:>3}"
            for pk in phrase_keys:
                row += f" {avg.get(pk, 0):>7.3f}"
            row += f" {avg.get('close_phase_q', 0):>7.1f}"
            row += f" {avg.get('silence_per_min', 0):>7.3f}"
            print(row)

    # 5. 教師あり学習的アプローチ
    print(f"\n{'=' * 75}")
    print(f"  [4/5] 教師あり学習: 行動指標 × 受注率の相関分析")
    print(f"{'=' * 75}")

    # メンバー単位のアプローチ: 受注率 vs 行動指標平均
    if opp_data:
        for period in ["2024h1", "2026jan"]:
            period_label = "2024年前半" if period == "2024h1" else "2026年1月"
            print(f"\n  === {period_label}: メンバー別受注率 × 行動指標 ===")

            points = []  # (win_rate, metrics_dict)
            for prefix in all_data:
                if prefix not in opp_data:
                    continue
                opp_info = opp_data[prefix][period]
                if opp_info["won"] + opp_info["lost"] < 2:
                    continue
                mlist = all_data[prefix][period]
                if len(mlist) < 3:
                    continue
                avg = avg_metrics(mlist, all_keys)
                avg["win_rate"] = opp_info["win_rate"]
                avg["name"] = all_data[prefix]["name"]
                avg["won"] = opp_info["won"]
                avg["lost"] = opp_info["lost"]
                points.append(avg)

            if len(points) < 5:
                print(f"  データ不足（{len(points)}名）- スキップ")
                continue

            # 受注率との相関
            win_rates = [p["win_rate"] for p in points]
            feature_keys = [k for k in all_keys if not k.startswith("spin_") or k.endswith("_ratio")]

            correlations = []
            for k in feature_keys:
                vals = [p.get(k, 0) for p in points]
                r = pearson_r(win_rates, vals)
                correlations.append((k, r))

            correlations.sort(key=lambda x: abs(x[1]), reverse=True)

            print(f"\n  受注率との相関ランキング（n={len(points)}名）:")
            print(f"  {'指標':<35} {'相関r':>8} {'方向':>6} {'強さ':>6}")
            print(f"  {'-'*60}")
            for k, r in correlations[:20]:
                direction = "正↑" if r > 0 else "負↓"
                strength = "強" if abs(r) >= 0.5 else "中" if abs(r) >= 0.3 else "弱"
                print(f"  {k:<35} {r:>+8.3f} {direction:>6} {strength:>6}")

            # 受注率でメンバーをランキング
            print(f"\n  メンバー受注率ランキング:")
            points.sort(key=lambda x: x["win_rate"], reverse=True)
            for p in points:
                print(f"    {p['name']:<12} 受注率:{p['win_rate']:.1%}"
                      f" ({p['won']}勝/{p['lost']}敗)"
                      f" SPIN_PI:{p.get('spin_PI_ratio', 0):.1%}"
                      f" データ根拠:{p.get('phrase_data_evidence_per_min', 0):.2f}/分")

    # トップ vs ボトム（データKW/分ベース）を受注代替指標として使用
    print(f"\n  === 代替アプローチ: トッパフォーマー商談 vs ボトム商談 ===")
    print(f"  （データKW/分上位5名の全商談 vs 下位5名の全商談を比較）")

    for period in ["2024h1", "2026jan"]:
        period_label = "2024年前半" if period == "2024h1" else "2026年1月"

        # メンバー別データKW/分でランキング
        member_scores = []
        for prefix in all_data:
            mlist = all_data[prefix][period]
            if len(mlist) < 3:
                continue
            avg_dkw = sum(m.get("phrase_data_evidence_per_min", 0) for m in mlist) / len(mlist)
            member_scores.append((prefix, all_data[prefix]["name"], avg_dkw, mlist))

        member_scores.sort(key=lambda x: x[2], reverse=True)
        if len(member_scores) < 6:
            continue

        top_n = min(5, len(member_scores) // 2)
        top_members = member_scores[:top_n]
        bottom_members = member_scores[-top_n:]

        top_deals = [m for _, _, _, mlist in top_members for m in mlist]
        bottom_deals = [m for _, _, _, mlist in bottom_members for m in mlist]

        print(f"\n  --- {period_label} ---")
        print(f"  上位: {', '.join(n for _, n, _, _ in top_members)} ({len(top_deals)}件)")
        print(f"  下位: {', '.join(n for _, n, _, _ in bottom_members)} ({len(bottom_deals)}件)")

        # 統計検定
        test_features = [
            ("spin_S", "SPIN S(状況)"),
            ("spin_P", "SPIN P(問題)"),
            ("spin_I", "SPIN I(示唆)"),
            ("spin_N", "SPIN N(解決)"),
            ("spin_PI_ratio", "SPIN PI比率"),
            ("host_char_ratio", "文字比率"),
            ("host_q_per_min", "質問/分"),
            ("nums_per_min", "数字/分"),
            ("f3_host_q", "冒頭3分Q"),
            ("cust_short_ratio", "顧客相槌率"),
            ("cust_long_ratio", "顧客長発話率"),
            ("phrase_data_evidence_per_min", "データ根拠/分"),
            ("phrase_close_urgency_per_min", "Close緊急/分"),
            ("phrase_rapport_empathy_per_min", "ラポール/分"),
            ("phrase_value_proposition_per_min", "価値提案/分"),
            ("phrase_objection_handle_per_min", "反論処理/分"),
            ("phrase_name_calling_per_min", "名前呼び/分"),
            ("phrase_number_specific_per_min", "具体数字/分"),
            ("close_phase_kw", "終盤CloseKW"),
            ("close_phase_q", "終盤質問数"),
            ("silence_per_min", "沈黙/分"),
            ("turns_per_min", "ターン/分"),
            ("avg_mono", "平均独白長"),
            ("long_mono", "200字超独白"),
        ]

        print(f"\n  {'指標':<25} {'上位平均':>8} {'下位平均':>8} {'t値':>8} {'p値':>8} {'効果量d':>8} {'有意':>4}")
        print(f"  {'-'*80}")

        sig_features = []
        for feat_key, feat_label in test_features:
            v_top = [m.get(feat_key, 0) for m in top_deals]
            v_bot = [m.get(feat_key, 0) for m in bottom_deals]
            avg_top = sum(v_top) / len(v_top) if v_top else 0
            avg_bot = sum(v_bot) / len(v_bot) if v_bot else 0
            t, p = welch_t(v_top, v_bot)
            pooled_sd = math.sqrt((stdev(v_top) ** 2 + stdev(v_bot) ** 2) / 2) if (stdev(v_top) > 0 or stdev(v_bot) > 0) else 1
            d = abs(avg_top - avg_bot) / pooled_sd if pooled_sd > 0 else 0
            sig = "***" if p < 0.001 else "**" if p < 0.01 else "*" if p < 0.05 else ""
            print(f"  {feat_label:<25} {avg_top:>8.3f} {avg_bot:>8.3f} {t:>8.2f} {p:>8.4f} {d:>8.3f} {sig:>4}")
            if p < 0.05:
                sig_features.append((feat_label, avg_top, avg_bot, t, p, d, sig))

        # 有意な特徴量まとめ
        if sig_features:
            print(f"\n  有意な差がある指標（p<0.05）: {len(sig_features)}個")
            sig_features.sort(key=lambda x: x[5], reverse=True)
            for i, (label, at, ab, t, p, d, sig) in enumerate(sig_features, 1):
                direction = "上位>" if at > ab else "下位>"
                print(f"    {i}. {label}: 効果量d={d:.2f} ({direction}) p={p:.4f}{sig}")

    # 6. 成功テンプレート生成
    print(f"\n{'=' * 75}")
    print(f"  [5/5] 成功テンプレート: トップ営業の行動プロファイル")
    print(f"{'=' * 75}")

    for period in ["2024h1", "2026jan"]:
        period_label = "2024年前半" if period == "2024h1" else "2026年1月"

        member_scores = []
        for prefix in all_data:
            mlist = all_data[prefix][period]
            if len(mlist) < 5:
                continue
            avg = avg_metrics(mlist, all_keys)
            avg_dkw = avg.get("phrase_data_evidence_per_min", 0)
            member_scores.append((prefix, all_data[prefix]["name"], avg_dkw, avg, len(mlist)))

        member_scores.sort(key=lambda x: x[2], reverse=True)
        if len(member_scores) < 3:
            continue

        top3 = member_scores[:3]
        print(f"\n  === {period_label}: トップ3プロファイル ===")
        print(f"  ベース: {', '.join(n for _, n, _, _, _ in top3)}")

        # トップ3の平均を「成功テンプレート」とする
        all_top_keys = list(top3[0][3].keys())
        template = {}
        for k in all_top_keys:
            vals = [t[3].get(k, 0) for t in top3]
            template[k] = round(sum(vals) / len(vals), 3)

        print(f"\n  --- SPIN配分（理想形） ---")
        print(f"    S(状況)  : {template.get('spin_S_ratio', 0):.1%}")
        print(f"    P(問題)  : {template.get('spin_P_ratio', 0):.1%}")
        print(f"    I(示唆)  : {template.get('spin_I_ratio', 0):.1%}")
        print(f"    N(解決)  : {template.get('spin_N_ratio', 0):.1%}")
        print(f"    PI比率   : {template.get('spin_PI_ratio', 0):.1%}")

        print(f"\n  --- 行動指標（理想値） ---")
        ideal_items = [
            ("host_char_ratio", "文字比率"),
            ("host_q_per_min", "質問/分"),
            ("nums_per_min", "数字/分"),
            ("f3_host_q", "冒頭3分Q"),
            ("cust_short_ratio", "顧客相槌率"),
            ("phrase_data_evidence_per_min", "データ根拠/分"),
            ("phrase_close_urgency_per_min", "Close緊急/分"),
            ("phrase_rapport_empathy_per_min", "ラポール/分"),
            ("phrase_value_proposition_per_min", "価値提案/分"),
            ("phrase_number_specific_per_min", "具体数字/分"),
            ("close_phase_q", "終盤質問数"),
            ("silence_per_min", "沈黙/分"),
            ("turns_per_min", "ターン/分"),
        ]
        for k, label in ideal_items:
            print(f"    {label:<18}: {template.get(k, 0):.3f}")

        # 各メンバーの「成功テンプレートとの距離」
        print(f"\n  --- メンバー別「成功テンプレート」適合度 ---")
        distance_keys = ["spin_PI_ratio", "host_char_ratio", "host_q_per_min",
                        "nums_per_min", "f3_host_q", "phrase_data_evidence_per_min",
                        "phrase_close_urgency_per_min", "phrase_rapport_empathy_per_min",
                        "close_phase_q", "silence_per_min"]

        member_distances = []
        for prefix in all_data:
            mlist = all_data[prefix][period]
            if len(mlist) < 3:
                continue
            avg = avg_metrics(mlist, all_keys)
            # ユークリッド距離（正規化）
            dist = 0
            for k in distance_keys:
                t_val = template.get(k, 0)
                m_val = avg.get(k, 0)
                if t_val != 0:
                    dist += ((m_val - t_val) / t_val) ** 2
            dist = math.sqrt(dist / len(distance_keys))
            member_distances.append((all_data[prefix]["name"], dist, avg))

        member_distances.sort(key=lambda x: x[1])
        print(f"  {'名前':<12} {'距離':>8} {'適合度':>8} {'SPIN_PI':>8} {'データ根拠':>10}")
        print(f"  {'-'*55}")
        for name, dist, avg in member_distances:
            fit = max(0, 1 - dist)
            print(f"  {name:<12} {dist:>8.3f} {fit:>7.1%}"
                  f" {avg.get('spin_PI_ratio', 0):>7.1%}"
                  f" {avg.get('phrase_data_evidence_per_min', 0):>10.3f}")

    # 結果をJSON保存
    result = {
        "members": {},
        "opportunity": opp_data,
    }
    for prefix, info in all_data.items():
        result["members"][prefix] = {
            "name": info["name"],
            "2024h1": {"count": len(info["2024h1"]),
                       "avg": avg_metrics(info["2024h1"], all_keys) if info["2024h1"] else {}},
            "2026jan": {"count": len(info["2026jan"]),
                        "avg": avg_metrics(info["2026jan"], all_keys) if info["2026jan"] else {}},
        }

    out_path = OUTPUT_DIR / "team_deep_analysis.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"\n結果保存: {out_path}")


if __name__ == "__main__":
    main()
