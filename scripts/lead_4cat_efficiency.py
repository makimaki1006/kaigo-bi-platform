"""
4カテゴリ別リード効率比較
HWのみ / 有料媒体のみ / 両方 / その他（HWでも有料でもない）
各カテゴリのアポ率・成約率・Lead→受注の通過率を算出
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

leads = pd.read_csv(DATA_DIR / "Lead_20260305_115825.csv", encoding="utf-8-sig", low_memory=False)
opps = pd.read_csv(DATA_DIR / "analysis" / "opportunities_detailed.csv", encoding="utf-8-sig", low_memory=False)
accounts = pd.read_csv(DATA_DIR / "Account_20260305_115035.csv", encoding="utf-8-sig", low_memory=False)
opps["Amount"] = pd.to_numeric(opps["Amount"], errors="coerce")

# --- Lead媒体分類 ---
hw_cols = ["Hellowork_Occupation__c", "Hellowork_DataImportDate__c",
           "Hellowork_URL__c", "Hellowork_JobPublicationDate__c"]
paid_cols = ["Paid_Media__c", "Paid_DataSource__c", "Paid_URL__c", "Paid_Memo__c"]
has_hw = leads[hw_cols].notna().any(axis=1) & (leads[hw_cols].astype(str) != "").any(axis=1)
has_paid = leads[paid_cols].notna().any(axis=1) & (leads[paid_cols].astype(str) != "").any(axis=1)

leads["cat"] = "その他"
leads.loc[has_hw & has_paid, "cat"] = "両方"
leads.loc[has_hw & ~has_paid, "cat"] = "HWのみ"
leads.loc[~has_hw & has_paid, "cat"] = "有料のみ"

# --- コンバート済みLead → Opportunity紐づけ ---
# ConvertedAccountId経由でOpportunityを逆引き
converted = leads[leads["IsConverted"] == True].copy()

# AccountId → Opportunity のマップ
won_opps = opps[opps["IsWon"] == True].copy()
all_opps = opps.copy()

# ConvertedAccountId → そのAccountの全Opportunity
account_opps = {}  # AccountId -> list of {IsWon, Amount, StageName}
for _, r in all_opps.iterrows():
    aid = r["AccountId"]
    if pd.isna(aid):
        continue
    if aid not in account_opps:
        account_opps[aid] = []
    account_opps[aid].append({
        "IsWon": r["IsWon"],
        "Amount": r["Amount"],
        "StageName": r["StageName"],
    })

# 各コンバートLeadに対して、そのAccountの商談結果を紐づけ
converted["has_opp"] = converted["ConvertedAccountId"].apply(
    lambda x: x in account_opps if pd.notna(x) else False
)
converted["has_won"] = converted["ConvertedAccountId"].apply(
    lambda x: any(o["IsWon"] == True for o in account_opps.get(x, []))
    if pd.notna(x) else False
)
converted["won_amount"] = converted["ConvertedAccountId"].apply(
    lambda x: sum(o["Amount"] for o in account_opps.get(x, []) if o["IsWon"] == True and pd.notna(o["Amount"]))
    if pd.notna(x) else 0
)

# --- Statusベースの架電済み判定 ---
untouched = ["未架電"]
leads["contacted"] = ~leads["Status"].isin(untouched)

print("=" * 70)
print("4カテゴリ別リード効率比較")
print("=" * 70)

# === 全体サマリー ===
print(f"\n{'カテゴリ':<12} {'Lead総数':>10} {'架電済み':>10} {'架電率':>8} {'CV数':>8} {'アポ率(全体)':>12} {'アポ率(架電済)':>14}")
print("-" * 80)

for cat in ["HWのみ", "有料のみ", "両方", "その他"]:
    cat_leads = leads[leads["cat"] == cat]
    total = len(cat_leads)
    contacted = len(cat_leads[cat_leads["contacted"] == True])
    cv = len(cat_leads[cat_leads["IsConverted"] == True])
    call_rate = contacted / total * 100 if total > 0 else 0
    apo_all = cv / total * 100 if total > 0 else 0
    apo_contacted = cv / contacted * 100 if contacted > 0 else 0
    print(f"  {cat:<10} {total:>10,} {contacted:>10,} {call_rate:>7.1f}% {cv:>7,} {apo_all:>11.2f}% {apo_contacted:>13.2f}%")

# === コンバート後の成約率 ===
print(f"\n\n{'カテゴリ':<12} {'CV数':>8} {'商談あり':>8} {'受注あり':>8} {'成約率(CV後)':>12} {'受注金額':>14}")
print("-" * 70)

for cat in ["HWのみ", "有料のみ", "両方", "その他"]:
    cat_cv = converted[converted["cat"] == cat]
    cv_count = len(cat_cv)
    has_opp = len(cat_cv[cat_cv["has_opp"] == True])
    has_won = len(cat_cv[cat_cv["has_won"] == True])
    won_amount = cat_cv["won_amount"].sum()
    win_rate = has_won / cv_count * 100 if cv_count > 0 else 0
    print(f"  {cat:<10} {cv_count:>7,} {has_opp:>7,} {has_won:>7,} {win_rate:>11.1f}% {won_amount:>13,.0f}円")

# === 全ファネル通過率 ===
print(f"\n\n{'':=<70}")
print("全ファネル: Lead → 架電 → アポ(CV) → 受注")
print(f"{'':=<70}")

print(f"\n{'カテゴリ':<12} {'Lead数':>10} {'架電率':>8} {'アポ率':>10} {'成約率':>10} {'Lead→受注':>10} {'1件売上':>12} {'受注金額':>14}")
print("-" * 90)

for cat in ["HWのみ", "有料のみ", "両方", "その他"]:
    cat_leads = leads[leads["cat"] == cat]
    cat_cv = converted[converted["cat"] == cat]

    total = len(cat_leads)
    contacted = len(cat_leads[cat_leads["contacted"] == True])
    cv = len(cat_cv)
    won = len(cat_cv[cat_cv["has_won"] == True])
    won_amount = cat_cv["won_amount"].sum()

    call_rate = contacted / total * 100 if total > 0 else 0
    apo_rate = cv / contacted * 100 if contacted > 0 else 0
    win_rate = won / cv * 100 if cv > 0 else 0
    lead_to_won = won / total * 100 if total > 0 else 0
    per_lead = won_amount / total if total > 0 else 0

    print(f"  {cat:<10} {total:>10,} {call_rate:>7.1f}% {apo_rate:>9.2f}% {win_rate:>9.1f}% {lead_to_won:>9.3f}% {per_lead:>11,.0f}円 {won_amount:>13,.0f}円")

# === 「その他」の内訳: 何から来たのか ===
print(f"\n\n{'':=<70}")
print("「その他」リードの内訳分析")
print(f"{'':=<70}")

other_leads = leads[leads["cat"] == "その他"]

# LeadSource分布
print(f"\n  LeadSource分布:")
ls_counts = other_leads["LeadSource"].fillna("(空)").value_counts().head(15)
for ls, cnt in ls_counts.items():
    pct = cnt / len(other_leads) * 100
    print(f"    {ls}: {cnt:,}件 ({pct:.1f}%)")

# Status分布
print(f"\n  Status分布:")
st_counts = other_leads["Status"].fillna("(空)").value_counts().head(10)
for st, cnt in st_counts.items():
    pct = cnt / len(other_leads) * 100
    print(f"    {st}: {cnt:,}件 ({pct:.1f}%)")

# CreatedDate分布（いつ作られたか）
other_leads_copy = other_leads.copy()
other_leads_copy["CreatedDate"] = pd.to_datetime(other_leads_copy["CreatedDate"], errors="coerce")
other_leads_copy["created_month"] = other_leads_copy["CreatedDate"].dt.to_period("M")

print(f"\n  作成月分布（上位10件）:")
cm_counts = other_leads_copy["created_month"].value_counts().sort_index().tail(12)
for cm, cnt in cm_counts.items():
    print(f"    {cm}: {cnt:,}件")

# === 月別推移: 4カテゴリ別受注 ===
print(f"\n\n{'':=<70}")
print("月別: 4カテゴリ別コンバート→受注")
print(f"{'':=<70}")

converted["ConvertedDate_dt"] = pd.to_datetime(converted["ConvertedDate"], errors="coerce")
converted["cv_month"] = converted["ConvertedDate_dt"].dt.to_period("M")

print(f"\n  {'月':>8} ", end="")
for cat in ["HWのみ", "有料のみ", "両方", "その他"]:
    print(f" {cat+'CV':>8} {cat+'受注':>8}", end="")
print()
print(f"  {'-'*75}")

for m in ["2025-09", "2025-10", "2025-11", "2025-12", "2026-01", "2026-02"]:
    print(f"  {m:>8} ", end="")
    for cat in ["HWのみ", "有料のみ", "両方", "その他"]:
        m_cv = converted[(converted["cv_month"] == m) & (converted["cat"] == cat)]
        cv_cnt = len(m_cv)
        won_cnt = len(m_cv[m_cv["has_won"] == True])
        print(f" {cv_cnt:>8} {won_cnt:>8}", end="")
    print()

# === 最終比較表 ===
print(f"\n\n{'':=<70}")
print("最終比較: 「その他」vs 3カテゴリ")
print(f"{'':=<70}")

for cat in ["HWのみ", "有料のみ", "両方", "その他"]:
    cat_leads_df = leads[leads["cat"] == cat]
    cat_cv = converted[converted["cat"] == cat]
    total = len(cat_leads_df)
    contacted = len(cat_leads_df[cat_leads_df["contacted"] == True])
    cv = len(cat_cv)
    won = len(cat_cv[cat_cv["has_won"] == True])
    won_amount = cat_cv["won_amount"].sum()

    apo_rate = cv / contacted * 100 if contacted > 0 else 0
    win_rate = won / cv * 100 if cv > 0 else 0
    per_lead = won_amount / total if total > 0 else 0

    print(f"""
  [{cat}]
    Lead数: {total:,}  架電済み: {contacted:,} ({contacted/total*100:.1f}%)
    アポ(CV): {cv:,}  アポ率(架電済みベース): {apo_rate:.2f}%
    受注: {won:,}  成約率(CV後): {win_rate:.1f}%
    受注金額: {won_amount:,.0f}円
    Lead1件あたり売上: {per_lead:,.0f}円
    Lead→受注 通過率: {won/total*100:.4f}%""")

print(f"\n{'':=<70}")
print("分析完了")
print(f"{'':=<70}")
