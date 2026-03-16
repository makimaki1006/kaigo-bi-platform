"""
「その他」カテゴリ成約率0.8%の検証
- CV974件のうち受注8件は本当か
- ConvertedAccountId → Opportunity追跡が漏れていないか
- 時期別・LeadSource別に分解
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
opps["Amount"] = pd.to_numeric(opps["Amount"], errors="coerce")

# Lead媒体分類
hw_cols = ["Hellowork_Occupation__c", "Hellowork_DataImportDate__c",
           "Hellowork_URL__c", "Hellowork_JobPublicationDate__c"]
paid_cols = ["Paid_Media__c", "Paid_DataSource__c", "Paid_URL__c", "Paid_Memo__c"]
has_hw = leads[hw_cols].notna().any(axis=1) & (leads[hw_cols].astype(str) != "").any(axis=1)
has_paid = leads[paid_cols].notna().any(axis=1) & (leads[paid_cols].astype(str) != "").any(axis=1)
leads["cat"] = "その他"
leads.loc[has_hw & has_paid, "cat"] = "両方"
leads.loc[has_hw & ~has_paid, "cat"] = "HWのみ"
leads.loc[~has_hw & has_paid, "cat"] = "有料のみ"

print("=" * 70)
print("「その他」成約率0.8%の検証")
print("=" * 70)

# === 1. 「その他」コンバート974件の内訳 ===
other_cv = leads[(leads["cat"] == "その他") & (leads["IsConverted"] == True)].copy()
print(f"\n「その他」コンバート: {len(other_cv)}件")

# ConvertedAccountIdの有無
has_account = other_cv["ConvertedAccountId"].notna().sum()
no_account = other_cv["ConvertedAccountId"].isna().sum()
print(f"  ConvertedAccountIdあり: {has_account}件")
print(f"  ConvertedAccountIdなし: {no_account}件")

# ConvertedOpportunityIdの有無
has_opp_id = other_cv["ConvertedOpportunityId"].notna().sum()
print(f"  ConvertedOpportunityIdあり: {has_opp_id}件")

# === 2. AccountId経由で商談を探す ===
account_ids = set(other_cv["ConvertedAccountId"].dropna().unique())
print(f"\n  ユニークAccountId: {len(account_ids)}件")

related_opps = opps[opps["AccountId"].isin(account_ids)].copy()
print(f"  これらAccountに紐づくOpportunity: {len(related_opps)}件")

# ステージ別
print(f"\n  商談ステージ分布:")
for stage, cnt in related_opps["StageName"].value_counts().items():
    print(f"    {stage}: {cnt}件")

won = related_opps[related_opps["IsWon"] == True]
print(f"\n  受注: {len(won)}件 / {won['Amount'].sum():,.0f}円")

# === 3. Opportunityデータの期間を確認 ===
print(f"\n\n{'':=<70}")
print("Opportunityデータの期間確認")
print(f"{'':=<70}")

opps["CreatedDate_dt"] = pd.to_datetime(opps["CreatedDate"], errors="coerce")
opps["CloseDate_dt"] = pd.to_datetime(opps["CloseDate"], errors="coerce")

print(f"\n  Opportunity全体:")
print(f"    CreatedDate: {opps['CreatedDate_dt'].min()} ～ {opps['CreatedDate_dt'].max()}")
print(f"    CloseDate: {opps['CloseDate_dt'].min()} ～ {opps['CloseDate_dt'].max()}")
print(f"    総件数: {len(opps):,}")

# 「その他」CVのコンバート時期
other_cv["ConvertedDate_dt"] = pd.to_datetime(other_cv["ConvertedDate"], errors="coerce")
print(f"\n  「その他」コンバート時期:")
print(f"    {other_cv['ConvertedDate_dt'].min()} ～ {other_cv['ConvertedDate_dt'].max()}")

# コンバート月別
other_cv["cv_month"] = other_cv["ConvertedDate_dt"].dt.to_period("M")
print(f"\n  月別コンバート数:")
for m, cnt in other_cv["cv_month"].value_counts().sort_index().items():
    # この月にCVしたLeadのうち受注あり
    month_cv = other_cv[other_cv["cv_month"] == m]
    month_account_ids = set(month_cv["ConvertedAccountId"].dropna().unique())
    month_won = opps[(opps["AccountId"].isin(month_account_ids)) & (opps["IsWon"] == True)]
    print(f"    {m}: CV {cnt}件 → 受注 {len(month_won)}件")

# === 4. 全カテゴリでの商談追跡率を比較 ===
print(f"\n\n{'':=<70}")
print("全カテゴリ: コンバート→商談追跡率の比較")
print(f"{'':=<70}")

for cat in ["HWのみ", "有料のみ", "両方", "その他"]:
    cat_cv = leads[(leads["cat"] == cat) & (leads["IsConverted"] == True)]
    cv_count = len(cat_cv)
    acct_ids = set(cat_cv["ConvertedAccountId"].dropna().unique())
    cat_opps = opps[opps["AccountId"].isin(acct_ids)]
    cat_won = cat_opps[cat_opps["IsWon"] == True]

    opp_rate = len(cat_opps) / cv_count * 100 if cv_count > 0 else 0
    print(f"\n  [{cat}]")
    print(f"    CV: {cv_count}件 → AccountId: {len(acct_ids)}件")
    print(f"    紐づくOpp: {len(cat_opps)}件 (CV1件あたり {opp_rate/100:.2f}件)")
    print(f"    受注: {len(cat_won)}件 ({len(cat_won)/cv_count*100:.1f}%)")
    print(f"    受注金額: {cat_won['Amount'].sum():,.0f}円")

# === 5. 「その他」のLeadSourceサブグループ別 ===
print(f"\n\n{'':=<70}")
print("「その他」内サブグループ別効率")
print(f"{'':=<70}")

other_all = leads[leads["cat"] == "その他"].copy()
other_all["ls_group"] = other_all["LeadSource"].fillna("(空)")

# ハロワ系をまとめる
other_all.loc[other_all["ls_group"].str.contains("ハロワ|ハローワーク", na=False), "ls_group"] = "HW系(フィールド未入力)"

print(f"\n  {'サブグループ':<25} {'Lead数':>8} {'架電済':>8} {'CV数':>6} {'アポ率':>8} {'受注':>4} {'成約率':>8}")
print(f"  {'-'*75}")

untouched = ["未架電"]
for grp in other_all["ls_group"].value_counts().index:
    grp_leads = other_all[other_all["ls_group"] == grp]
    total = len(grp_leads)
    contacted = len(grp_leads[~grp_leads["Status"].isin(untouched)])
    cv = len(grp_leads[grp_leads["IsConverted"] == True])
    apo_rate = cv / contacted * 100 if contacted > 0 else 0

    # 受注追跡
    cv_accts = set(grp_leads[grp_leads["IsConverted"] == True]["ConvertedAccountId"].dropna().unique())
    grp_won = opps[(opps["AccountId"].isin(cv_accts)) & (opps["IsWon"] == True)]
    win_rate = len(grp_won) / cv * 100 if cv > 0 else 0

    print(f"  {grp:<25} {total:>8,} {contacted:>8,} {cv:>5} {apo_rate:>7.2f}% {len(grp_won):>3} {win_rate:>7.1f}%")

# === 6. 重複チェック: 同じAccountIdが複数カテゴリのCVに存在するか ===
print(f"\n\n{'':=<70}")
print("AccountId重複チェック（複数カテゴリからのCV）")
print(f"{'':=<70}")

all_cv = leads[leads["IsConverted"] == True].copy()
cv_by_account = all_cv.groupby("ConvertedAccountId")["cat"].apply(set).reset_index()
cv_by_account.columns = ["AccountId", "categories"]
cv_by_account["n_cats"] = cv_by_account["categories"].apply(len)

multi_cat = cv_by_account[cv_by_account["n_cats"] > 1]
print(f"\n  複数カテゴリからCVされたAccount: {len(multi_cat)}件 / {len(cv_by_account)}件")

if len(multi_cat) > 0:
    print(f"\n  組み合わせ:")
    combo_counts = multi_cat["categories"].apply(lambda x: " + ".join(sorted(x))).value_counts()
    for combo, cnt in combo_counts.items():
        print(f"    {combo}: {cnt}件")

    # この重複Accountの受注
    multi_acct_ids = set(multi_cat["AccountId"].dropna())
    multi_won = opps[(opps["AccountId"].isin(multi_acct_ids)) & (opps["IsWon"] == True)]
    print(f"\n  重複Accountの受注: {len(multi_won)}件 / {multi_won['Amount'].sum():,.0f}円")

print(f"\n{'':=<70}")
print("検証完了")
print(f"{'':=<70}")
