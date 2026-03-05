"""チーム比較分析 再集計（コンサル除外版）"""
import json
from pathlib import Path

DATA_DIR = Path(r"C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List\data\output\team_comparison")
data = json.loads((DATA_DIR / "team_targeted_summary.json").read_text(encoding="utf-8"))

# コンサル（除外対象）
CONSUL_EMAILS = {
    "h_obata@cyxen.co.jp",   # 小幡英稔
    "a_tanji@cyxen.co.jp",   # 丹司杏奈
    "s_endo@cyxen.co.jp",    # 遠藤紗月
}

METRICS_KEYS = [
    "host_char_ratio", "host_avg_len", "cust_avg_len", "host_q_per_min",
    "cust_q", "nums_per_min", "avg_mono", "long_mono", "turns_per_min",
    "f3_host_ratio", "f3_host_q", "cust_long_ratio", "cust_short_ratio",
    "data_kw_per_min", "close_kw_total", "problem_q", "duration"
]

METRIC_LABELS = {
    "host_char_ratio": "文字比率", "host_avg_len": "平均発話長",
    "cust_avg_len": "顧客発話長", "host_q_per_min": "質問/分",
    "cust_q": "顧客質問数", "nums_per_min": "数字/分",
    "avg_mono": "平均独白長", "long_mono": "200字超独白",
    "turns_per_min": "ターン/分", "f3_host_ratio": "冒頭3分比率",
    "f3_host_q": "冒頭3分Q", "cust_long_ratio": "顧客長発話率",
    "cust_short_ratio": "顧客相槌率", "data_kw_per_min": "データKW/分",
    "close_kw_total": "クロージングKW", "problem_q": "P質問",
    "duration": "商談時間(分)"
}

# セールスのみフィルタ
sales_data = {k: v for k, v in data.items() if k not in CONSUL_EMAILS}

def weighted_avg(members, period):
    """件数加重平均"""
    totals = {k: 0.0 for k in METRICS_KEYS}
    total_count = 0
    for email, info in members.items():
        c = info[period]["count"]
        if c == 0:
            continue
        total_count += c
        for k in METRICS_KEYS:
            totals[k] += info[period]["metrics"][k] * c
    if total_count == 0:
        return {k: 0 for k in METRICS_KEYS}, 0
    return {k: totals[k] / total_count for k in METRICS_KEYS}, total_count

# === 全体合算 ===
print("=" * 75)
print("  セールスメンバーのみ: 期間比較（コンサル3名除外）")
print("=" * 75)

avg_2024, cnt_2024 = weighted_avg(sales_data, "2024h1")
avg_2026, cnt_2026 = weighted_avg(sales_data, "2026jan")
print(f"  2024H1: {cnt_2024}件 / 2026Jan: {cnt_2026}件\n")

print(f"  {'指標':<18} {'2024H1':>8} {'2026Jan':>8} {'差分':>12} {'変化率':>10}")
print("  " + "-" * 60)
for k in METRICS_KEYS:
    v1, v2 = avg_2024[k], avg_2026[k]
    diff = v2 - v1
    pct = (diff / v1 * 100) if v1 != 0 else 0
    sign = "+" if diff > 0 else ""
    print(f"  {METRIC_LABELS[k]:<16} {v1:>8.2f} {v2:>8.2f} {sign:>1}{diff:>8.2f} {pct:>+8.1f}%")

# === メンバー別（2024H1）===
print("\n" + "=" * 75)
print("  セールスメンバー別: 2024年前半（データKW/分ランキング）")
print("=" * 75)

members_2024 = []
for email, info in sales_data.items():
    c = info["2024h1"]["count"]
    if c == 0:
        continue
    m = info["2024h1"]["metrics"]
    members_2024.append((info["name"], c, m))

members_2024.sort(key=lambda x: x[2]["data_kw_per_min"], reverse=True)
print(f"  {'名前':<14} {'件数':>4} {'文字比率':>8} {'数字/分':>8} {'データKW/分':>10} {'冒頭3分Q':>8} {'P質問':>8} {'相槌率':>8} {'CloseKW':>8}")
print("  " + "-" * 90)
for name, cnt, m in members_2024:
    print(f"  {name:<12} {cnt:>4} {m['host_char_ratio']:>8.3f} {m['nums_per_min']:>8.3f} {m['data_kw_per_min']:>10.3f} {m['f3_host_q']:>8.3f} {m['problem_q']:>8.3f} {m['cust_short_ratio']:>8.3f} {m['close_kw_total']:>8.3f}")

# === メンバー別（2026Jan）===
print("\n" + "=" * 75)
print("  セールスメンバー別: 2026年1月（データKW/分ランキング）")
print("=" * 75)

members_2026 = []
for email, info in sales_data.items():
    c = info["2026jan"]["count"]
    if c == 0:
        continue
    m = info["2026jan"]["metrics"]
    members_2026.append((info["name"], c, m))

members_2026.sort(key=lambda x: x[2]["data_kw_per_min"], reverse=True)
print(f"  {'名前':<14} {'件数':>4} {'文字比率':>8} {'数字/分':>8} {'データKW/分':>10} {'冒頭3分Q':>8} {'P質問':>8} {'相槌率':>8} {'CloseKW':>8}")
print("  " + "-" * 90)
for name, cnt, m in members_2026:
    print(f"  {name:<12} {cnt:>4} {m['host_char_ratio']:>8.3f} {m['nums_per_min']:>8.3f} {m['data_kw_per_min']:>10.3f} {m['f3_host_q']:>8.3f} {m['problem_q']:>8.3f} {m['cust_short_ratio']:>8.3f} {m['close_kw_total']:>8.3f}")

# === トップ vs ボトム ===
def top_bottom(members_list, top_n, bottom_n, period_label):
    """データKW/分でランキングし、上位・下位を比較"""
    print(f"\n{'=' * 75}")
    print(f"  トップ vs ボトム: {period_label}（セールスのみ）")
    print(f"{'=' * 75}")

    top = members_list[:top_n]
    bottom = members_list[-bottom_n:]

    top_names = ", ".join([m[0] for m in top])
    bottom_names = ", ".join([m[0] for m in bottom])
    top_cnt = sum(m[1] for m in top)
    bottom_cnt = sum(m[1] for m in bottom)

    print(f"  上位: {top_names} ({top_cnt}件)")
    print(f"  下位: {bottom_names} ({bottom_cnt}件)\n")

    # 加重平均
    top_avg = {k: 0.0 for k in METRICS_KEYS}
    bottom_avg = {k: 0.0 for k in METRICS_KEYS}

    for _, cnt, m in top:
        for k in METRICS_KEYS:
            top_avg[k] += m[k] * cnt
    for k in METRICS_KEYS:
        top_avg[k] /= top_cnt

    for _, cnt, m in bottom:
        for k in METRICS_KEYS:
            bottom_avg[k] += m[k] * cnt
    for k in METRICS_KEYS:
        bottom_avg[k] /= bottom_cnt

    print(f"  {'指標':<20} {'上位':>10} {'下位':>10} {'差分':>10} {'差%':>10}")
    print("  " + "-" * 60)
    for k in METRICS_KEYS:
        v_top, v_bot = top_avg[k], bottom_avg[k]
        diff = v_top - v_bot
        pct = (diff / v_bot * 100) if v_bot != 0 else 0
        star = " ★" if abs(pct) > 15 else ""
        print(f"  {METRIC_LABELS[k]:<18} {v_top:>10.2f} {v_bot:>10.2f} {diff:>+10.2f} {pct:>+8.1f}%{star}")

# 2024H1: 上位5, 下位5（16名中）
if len(members_2024) >= 10:
    top_bottom(members_2024, 5, 5, "2024年前半")

# 2026Jan: 上位5, 下位5
if len(members_2026) >= 10:
    top_bottom(members_2026, 5, 5, "2026年1月")

# === 個人変化ランキング（両期間データあり、セールスのみ）===
print(f"\n{'=' * 75}")
print(f"  個人変化ランキング（両期間データあり、セールスのみ）")
print(f"{'=' * 75}")

both = []
for email, info in sales_data.items():
    if info["2024h1"]["count"] > 0 and info["2026jan"]["count"] > 0:
        both.append((info["name"], info["2024h1"]["metrics"], info["2026jan"]["metrics"]))

print(f"  対象: {len(both)}名\n")

change_metrics = ["nums_per_min", "data_kw_per_min", "host_char_ratio",
                  "f3_host_q", "cust_short_ratio", "problem_q", "close_kw_total"]

for mk in change_metrics:
    print(f"\n  【{METRIC_LABELS[mk]}】")
    changes = []
    for name, m1, m2 in both:
        v1, v2 = m1[mk], m2[mk]
        diff = v2 - v1
        pct = (diff / v1 * 100) if v1 != 0 else 0
        changes.append((name, v1, v2, diff, pct))
    changes.sort(key=lambda x: x[3])
    for name, v1, v2, diff, pct in changes:
        arrow = "↑" if diff > 0 else "↓"
        print(f"    {name:<14} {v1:.2f} → {v2:>7.2f} ({diff:+.2f}, {pct:+.0f}%) {arrow}")

print(f"\n結果保存完了")
