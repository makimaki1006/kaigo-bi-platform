"""
1月受注低調（27件）の根幹原因分析
営業日数の減少率（~10%）に対して受注の減少率（~40-47%）が大きすぎる理由を解明する

分析軸:
1. 月別の商談パイプライン（作成数・進行数・クローズ数）
2. 月別の商談ステージ推移（失注理由含む）
3. 月別の初回/再商談・代表者/担当者の構造変化
4. リードソース別の月別受注
5. 商談サイクル（作成→クローズの日数）
6. 施設タイプ・金額帯の変化
7. 営業担当者別の月別受注
8. 週別の受注推移（年始休暇の影響の精密測定）
"""

import pandas as pd
import numpy as np
import re
import sys
import io
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data" / "output"

opps = pd.read_csv(DATA_DIR / "analysis" / "opportunities_detailed.csv", encoding="utf-8-sig", low_memory=False)
leads = pd.read_csv(DATA_DIR / "Lead_20260305_115825.csv", encoding="utf-8-sig", low_memory=False)
opps["Amount"] = pd.to_numeric(opps["Amount"], errors="coerce")
opps["CloseDate_dt"] = pd.to_datetime(opps["CloseDate"], errors="coerce")
opps["CreatedDate_dt"] = pd.to_datetime(opps["CreatedDate"], errors="coerce")
opps["close_month"] = opps["CloseDate_dt"].dt.to_period("M")
opps["created_month"] = opps["CreatedDate_dt"].dt.to_period("M")
opps["close_week"] = opps["CloseDate_dt"].dt.isocalendar().week.astype(int)
opps["close_year"] = opps["CloseDate_dt"].dt.year

print("=" * 80)
print("1月受注低調（27件）の根幹原因分析")
print("=" * 80)

# === 0. 基本確認: 各月の営業日と受注 ===
print("\n" + "=" * 80)
print("0. 月別基本比較")
print("=" * 80)

won = opps[opps["IsWon"] == True].copy()
lost = opps[(opps["IsClosed"] == True) & (opps["IsWon"] != True)].copy()

# 営業日数（土日祝除く概算）
biz_days = {"2025-10": 22, "2025-11": 19, "2025-12": 21, "2026-01": 19, "2026-02": 19}
# 1月は年始休暇(1/1-1/3)で実質17日程度

for m in ["2025-10", "2025-11", "2025-12", "2026-01", "2026-02"]:
    m_won = won[won["close_month"] == m]
    m_lost = lost[lost["close_month"] == m]
    m_all = opps[(opps["IsClosed"] == True) & (opps["close_month"] == m)]
    bd = biz_days.get(m, 20)
    win_rate = len(m_won) / len(m_all) * 100 if len(m_all) > 0 else 0
    per_day = len(m_won) / bd
    print(f"  {m}: 受注 {len(m_won):>3}件 / 失注 {len(m_lost):>3}件 / 勝率 {win_rate:.1f}% / 営業日 {bd}日 / 受注/日 {per_day:.2f}")

# === 1. 商談パイプライン: 作成 vs クローズ ===
print("\n" + "=" * 80)
print("1. 商談パイプライン: 月別 作成数 vs クローズ数")
print("=" * 80)

print(f"\n  {'月':>8} {'新規作成':>8} {'受注':>6} {'失注':>6} {'クローズ計':>10} {'受注金額':>14} {'平均単価':>10}")
print(f"  {'-'*70}")

for m in ["2025-10", "2025-11", "2025-12", "2026-01", "2026-02"]:
    created = len(opps[opps["created_month"] == m])
    m_won = won[won["close_month"] == m]
    m_lost = lost[lost["close_month"] == m]
    closed = len(m_won) + len(m_lost)
    amt = m_won["Amount"].sum()
    avg = m_won["Amount"].mean() if len(m_won) > 0 else 0
    print(f"  {m:>8} {created:>7}件 {len(m_won):>5}件 {len(m_lost):>5}件 {closed:>9}件 {amt:>13,.0f}円 {avg:>9,.0f}円")

# === 2. 初回商談 vs 再商談 の月別構造 ===
print("\n" + "=" * 80)
print("2. 初回商談 vs 再商談 の月別構造変化")
print("=" * 80)

won["is_re"] = won["OpportunityCategory__c"].fillna("").str.contains("再")

print(f"\n  {'月':>8} {'初回受注':>8} {'再商談受注':>10} {'再商談率':>8} {'初回金額':>12} {'再商談金額':>12}")
print(f"  {'-'*65}")

for m in ["2025-10", "2025-11", "2025-12", "2026-01", "2026-02"]:
    md = won[won["close_month"] == m]
    initial = md[~md["is_re"]]
    re_opp = md[md["is_re"]]
    re_rate = len(re_opp) / len(md) * 100 if len(md) > 0 else 0
    print(f"  {m:>8} {len(initial):>7}件 {len(re_opp):>9}件 {re_rate:>7.1f}% {initial['Amount'].sum():>11,.0f}円 {re_opp['Amount'].sum():>11,.0f}円")

# === 3. 代表者商談 vs 担当者商談 の月別構造 ===
print("\n" + "=" * 80)
print("3. 商談タイプ（代表者/担当者）の月別構造変化")
print("=" * 80)

print(f"\n  {'月':>8}", end="")
for otype in ["代表者商談", "代表者商談(決裁者)", "担当者商談", "担当者商談(決裁者)", "その他/空"]:
    print(f" {otype:>12}", end="")
print()
print(f"  {'-'*80}")

for m in ["2025-10", "2025-11", "2025-12", "2026-01", "2026-02"]:
    md = won[won["close_month"] == m]
    print(f"  {m:>8}", end="")
    for otype_key in ["代表者商談", "代表者商談（決裁者）", "担当者商談", "担当者商談（決裁者）"]:
        cnt = len(md[md["OpportunityType__c"].fillna("") == otype_key])
        print(f" {cnt:>12}件", end="")
    # その他/空
    other = len(md[~md["OpportunityType__c"].fillna("").isin(["代表者商談", "代表者商談（決裁者）", "担当者商談", "担当者商談（決裁者）"])])
    print(f" {other:>12}件")

# === 4. 失注理由の月別変化 ===
print("\n" + "=" * 80)
print("4. 失注理由の月別変化")
print("=" * 80)

for m in ["2025-11", "2025-12", "2026-01", "2026-02"]:
    m_lost = lost[lost["close_month"] == m]
    if len(m_lost) == 0:
        continue
    print(f"\n  --- {m} (失注 {len(m_lost)}件) ---")
    for reason, cnt in m_lost["LostReason_Large__c"].fillna("(空)").value_counts().head(8).items():
        print(f"    {reason}: {cnt}件 ({cnt/len(m_lost)*100:.1f}%)")

# === 5. 商談サイクル日数 ===
print("\n" + "=" * 80)
print("5. 商談サイクル日数（作成→クローズ）の月別変化")
print("=" * 80)

won["cycle_days"] = (won["CloseDate_dt"] - won["CreatedDate_dt"].dt.tz_localize(None)).dt.days

for m in ["2025-10", "2025-11", "2025-12", "2026-01", "2026-02"]:
    md = won[won["close_month"] == m]
    if len(md) == 0:
        continue
    cycle = md["cycle_days"].dropna()
    same_day = len(cycle[cycle == 0])
    within_7 = len(cycle[cycle <= 7])
    within_30 = len(cycle[cycle <= 30])
    print(f"  {m}: 中央値 {cycle.median():.0f}日 / 平均 {cycle.mean():.1f}日 / 即日 {same_day}件 / 7日以内 {within_7}件 / 30日以内 {within_30}件 / 全 {len(md)}件")

# === 6. 週別の受注推移（年始休暇の影響を精密測定） ===
print("\n" + "=" * 80)
print("6. 週別受注推移（12月〜2月）")
print("=" * 80)

won_detail = won[(won["CloseDate_dt"] >= "2025-12-01") & (won["CloseDate_dt"] <= "2026-02-28")].copy()
won_detail["week_start"] = won_detail["CloseDate_dt"].dt.to_period("W").apply(lambda x: x.start_time)

print(f"\n  {'週開始':>12} {'受注件数':>8} {'受注金額':>14}")
print(f"  {'-'*40}")

for ws in sorted(won_detail["week_start"].unique()):
    wd = won_detail[won_detail["week_start"] == ws]
    print(f"  {ws.strftime('%Y-%m-%d'):>12} {len(wd):>7}件 {wd['Amount'].sum():>13,.0f}円")

# === 7. 営業担当者別の月別受注 ===
print("\n" + "=" * 80)
print("7. 営業担当者別 月別受注件数")
print("=" * 80)

# 主要担当者（2件以上受注）
top_owners = won["Owner.Name"].value_counts().head(15).index.tolist()

print(f"\n  {'担当者':<14}", end="")
for m in ["2025-11", "2025-12", "2026-01", "2026-02"]:
    print(f" {m:>8}", end="")
print(f" {'合計':>6}")
print(f"  {'-'*55}")

for owner in top_owners:
    print(f"  {owner:<14}", end="")
    total = 0
    for m in ["2025-11", "2025-12", "2026-01", "2026-02"]:
        cnt = len(won[(won["Owner.Name"] == owner) & (won["close_month"] == m)])
        total += cnt
        print(f" {cnt:>7}件", end="")
    print(f" {total:>5}件")

# === 8. 施設タイプの月別変化 ===
print("\n" + "=" * 80)
print("8. 施設タイプ（大分類）の月別受注変化")
print("=" * 80)

top_types = won["FacilityType_Large__c"].fillna("(空)").value_counts().head(10).index.tolist()

print(f"\n  {'施設タイプ':<18}", end="")
for m in ["2025-11", "2025-12", "2026-01", "2026-02"]:
    print(f" {m:>8}", end="")
print()
print(f"  {'-'*55}")

for ft in top_types:
    print(f"  {ft:<18}", end="")
    for m in ["2025-11", "2025-12", "2026-01", "2026-02"]:
        cnt = len(won[(won["FacilityType_Large__c"].fillna("(空)") == ft) & (won["close_month"] == m)])
        print(f" {cnt:>7}件", end="")
    print()

# === 9. 金額帯の月別分布 ===
print("\n" + "=" * 80)
print("9. 金額帯別 月別受注件数")
print("=" * 80)

def amount_band(amt):
    if pd.isna(amt) or amt <= 0:
        return "0以下"
    elif amt <= 200000:
        return "〜20万"
    elif amt <= 500000:
        return "20〜50万"
    elif amt <= 1000000:
        return "50〜100万"
    else:
        return "100万超"

won["amount_band"] = won["Amount"].apply(amount_band)

print(f"\n  {'金額帯':<12}", end="")
for m in ["2025-11", "2025-12", "2026-01", "2026-02"]:
    print(f" {m:>8}", end="")
print()
print(f"  {'-'*50}")

for band in ["〜20万", "20〜50万", "50〜100万", "100万超", "0以下"]:
    print(f"  {band:<12}", end="")
    for m in ["2025-11", "2025-12", "2026-01", "2026-02"]:
        cnt = len(won[(won["amount_band"] == band) & (won["close_month"] == m)])
        print(f" {cnt:>7}件", end="")
    print()

# === 10. 1月だけ特異的に下がった指標の特定 ===
print("\n" + "=" * 80)
print("10. 1月特異指標サマリー")
print("=" * 80)

# 各月のパイプライン転換率
print("\n  月別パイプライン転換率（新規作成 → 受注）:")
for m in ["2025-10", "2025-11", "2025-12", "2026-01", "2026-02"]:
    created_before = len(opps[(opps["created_month"] <= m)])
    m_won_cnt = len(won[won["close_month"] == m])
    m_created = len(opps[opps["created_month"] == m])
    print(f"  {m}: 新規作成 {m_created}件 → 当月受注 {m_won_cnt}件")

# 再商談の受注が落ちたのか、初回が落ちたのか
print("\n  初回/再商談の月別変化:")
for m in ["2025-11", "2025-12", "2026-01", "2026-02"]:
    md = won[won["close_month"] == m]
    initial = md[~md["is_re"]]
    re_opp = md[md["is_re"]]
    print(f"  {m}: 初回 {len(initial)}件({initial['Amount'].sum():,.0f}円) / 再商談 {len(re_opp)}件({re_opp['Amount'].sum():,.0f}円)")

# 代表者商談の変化
print("\n  代表者商談の月別変化:")
for m in ["2025-11", "2025-12", "2026-01", "2026-02"]:
    md = won[won["close_month"] == m]
    daihyo = md[md["OpportunityType__c"].fillna("").str.contains("代表者")]
    tanto = md[md["OpportunityType__c"].fillna("").str.contains("担当者")]
    print(f"  {m}: 代表者 {len(daihyo)}件 / 担当者 {len(tanto)}件 / その他 {len(md)-len(daihyo)-len(tanto)}件")

# === 11. Lead作成時期 → 受注との関係 ===
print("\n" + "=" * 80)
print("11. 受注案件のLead作成タイミング分析")
print("=" * 80)

# Lead媒体分類
hw_cols = ["Hellowork_Occupation__c", "Hellowork_DataImportDate__c",
           "Hellowork_URL__c", "Hellowork_JobPublicationDate__c"]
paid_cols = ["Paid_Media__c", "Paid_DataSource__c", "Paid_URL__c", "Paid_Memo__c"]
has_hw = leads[hw_cols].notna().any(axis=1) & (leads[hw_cols].astype(str) != "").any(axis=1)
has_paid = leads[paid_cols].notna().any(axis=1) & (leads[paid_cols].astype(str) != "").any(axis=1)
leads["src"] = "その他"
leads.loc[has_hw & has_paid, "src"] = "両方"
leads.loc[has_hw & ~has_paid, "src"] = "HW"
leads.loc[~has_hw & has_paid, "src"] = "有料媒体"

# ConvertedAccountId経由で1月受注に紐づくLeadを特定
converted_leads = leads[leads["ConvertedAccountId"].notna()].copy()
converted_leads["ConvertedDate_dt"] = pd.to_datetime(converted_leads["ConvertedDate"], errors="coerce")
converted_leads["CreatedDate_dt"] = pd.to_datetime(converted_leads["CreatedDate"], errors="coerce")

jan_won = won[won["close_month"] == "2026-01"]
feb_won = won[won["close_month"] == "2026-02"]

for label, month_won in [("1月受注", jan_won), ("2月受注", feb_won)]:
    month_acct_ids = set(month_won["AccountId"].dropna())
    matched_leads = converted_leads[converted_leads["ConvertedAccountId"].isin(month_acct_ids)]

    print(f"\n  {label} ({len(month_won)}件) に紐づくLead:")
    print(f"    AccountId経由マッチ: {len(matched_leads)}件")

    if len(matched_leads) > 0:
        # Lead作成月分布
        matched_leads_copy = matched_leads.copy()
        matched_leads_copy["lead_created_month"] = matched_leads_copy["CreatedDate_dt"].dt.to_period("M")
        print(f"    Lead作成月分布:")
        for cm, cnt in matched_leads_copy["lead_created_month"].value_counts().sort_index().tail(8).items():
            print(f"      {cm}: {cnt}件")

        # Leadソース分布
        print(f"    Leadソース分布:")
        for src, cnt in matched_leads["src"].value_counts().items():
            print(f"      {src}: {cnt}件")

        # コンバート月分布
        matched_leads_copy["cv_month"] = matched_leads_copy["ConvertedDate_dt"].dt.to_period("M")
        print(f"    コンバート月分布:")
        for cm, cnt in matched_leads_copy["cv_month"].value_counts().sort_index().tail(6).items():
            print(f"      {cm}: {cnt}件")

# === 12. 全Opportunity（受注+失注）の月別推移 ===
print("\n" + "=" * 80)
print("12. 全Opportunity（受注+失注+進行中）のステータス月別推移")
print("=" * 80)

for m in ["2025-11", "2025-12", "2026-01", "2026-02"]:
    m_all = opps[opps["close_month"] == m]
    m_open = opps[(opps["IsClosed"] != True) & (opps["created_month"] <= m)]
    print(f"\n  {m}: クローズ計 {len(m_all)}件")
    for stage, cnt in m_all["StageName"].value_counts().items():
        print(f"    {stage}: {cnt}件")

# === 13. アポイント経路の月別変化 ===
print("\n" + "=" * 80)
print("13. アポイント取得元（AppointUnit__c）の月別変化")
print("=" * 80)

print(f"\n  {'アポ取得元':<20}", end="")
for m in ["2025-11", "2025-12", "2026-01", "2026-02"]:
    print(f" {m:>8}", end="")
print()
print(f"  {'-'*55}")

top_units = won["AppointUnit__c"].fillna("(空)").value_counts().head(10).index.tolist()
for unit in top_units:
    print(f"  {unit:<20}", end="")
    for m in ["2025-11", "2025-12", "2026-01", "2026-02"]:
        cnt = len(won[(won["AppointUnit__c"].fillna("(空)") == unit) & (won["close_month"] == m)])
        print(f" {cnt:>7}件", end="")
    print()

# === 14. アポインター（個人）の月別実績 ===
print("\n" + "=" * 80)
print("14. アポインター別 月別受注件数")
print("=" * 80)

top_appointers = won["Appointer__c"].fillna("(空)").value_counts().head(15).index.tolist()

print(f"\n  {'アポインター':<14}", end="")
for m in ["2025-11", "2025-12", "2026-01", "2026-02"]:
    print(f" {m:>8}", end="")
print(f" {'合計':>6}")
print(f"  {'-'*55}")

for ap in top_appointers:
    print(f"  {ap:<14}", end="")
    total = 0
    for m in ["2025-11", "2025-12", "2026-01", "2026-02"]:
        cnt = len(won[(won["Appointer__c"].fillna("(空)") == ap) & (won["close_month"] == m)])
        total += cnt
        print(f" {cnt:>7}件", end="")
    print(f" {total:>5}件")

# === 15. WonReason（受注理由）の月別変化 ===
print("\n" + "=" * 80)
print("15. 受注理由の月別変化")
print("=" * 80)

for m in ["2025-11", "2025-12", "2026-01", "2026-02"]:
    md = won[won["close_month"] == m]
    if len(md) == 0:
        continue
    print(f"\n  --- {m} (受注 {len(md)}件) ---")
    for reason, cnt in md["WonReason__c"].fillna("(空)").value_counts().head(5).items():
        print(f"    {reason}: {cnt}件")

print("\n" + "=" * 80)
print("分析完了")
print("=" * 80)
