"""
マッチなし受注の徹底調査
- Account電話番号(Phone + PersonMobilePhone)でLead全体を検索
- 会社名正規化マッチ
- 真のマッチなし案件の特徴分析
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

print("=" * 70)
print("マッチなし受注 徹底調査")
print("=" * 70)

leads = pd.read_csv(DATA_DIR / "Lead_20260305_115825.csv", encoding="utf-8-sig", low_memory=False)
opps = pd.read_csv(DATA_DIR / "analysis" / "opportunities_detailed.csv", encoding="utf-8-sig", low_memory=False)
accounts = pd.read_csv(DATA_DIR / "Account_20260305_115035.csv", encoding="utf-8-sig", low_memory=False)
opps["Amount"] = pd.to_numeric(opps["Amount"], errors="coerce")

print(f"Lead: {len(leads):,} / Opp: {len(opps):,} / Account: {len(accounts):,}")

# --- ユーティリティ ---
def norm_phone(val):
    if pd.isna(val) or str(val).strip() == "":
        return ""
    d = re.sub(r"[^\d]", "", str(val))
    return d if 10 <= len(d) <= 11 else ""

CORP_WORDS = ["株式会社", "有限会社", "合同会社", "一般社団法人", "一般財団法人",
              "社会福祉法人", "医療法人社団", "医療法人財団", "医療法人",
              "特定非営利活動法人", "NPO法人", "学校法人", "宗教法人",
              "社会医療法人", "公益社団法人", "公益財団法人"]

def norm_company(val):
    if pd.isna(val) or str(val).strip() == "":
        return ""
    s = str(val).strip()
    for c in CORP_WORDS:
        s = s.replace(c, "")
    return re.sub(r"\s+", "", s)

# --- Lead媒体分類 ---
hw_cols = ["Hellowork_Occupation__c", "Hellowork_DataImportDate__c",
           "Hellowork_URL__c", "Hellowork_JobPublicationDate__c"]
paid_cols = ["Paid_Media__c", "Paid_DataSource__c", "Paid_URL__c", "Paid_Memo__c"]

has_hw = leads[hw_cols].notna().any(axis=1) & (leads[hw_cols].astype(str) != "").any(axis=1)
has_paid = leads[paid_cols].notna().any(axis=1) & (leads[paid_cols].astype(str) != "").any(axis=1)

leads["src"] = "その他"
leads.loc[has_hw & has_paid, "src"] = "両方"
leads.loc[has_hw & ~has_paid, "src"] = "HW"
leads.loc[~has_hw & has_paid, "src"] = "有料媒体"

# --- Lead電話番号→媒体マップ（4列）---
print("\nLead電話番号マップ構築中...")
phone_map = {}  # norm_phone -> {"hw": bool, "paid": bool}
for col in ["Phone", "MobilePhone", "Phone2__c", "MobilePhone2__c"]:
    if col not in leads.columns:
        continue
    valid = leads[leads[col].notna()][[col, "src"]].copy()
    valid["_p"] = valid[col].apply(norm_phone)
    valid = valid[valid["_p"] != ""]
    for _, r in valid.iterrows():
        p = r["_p"]
        if p not in phone_map:
            phone_map[p] = {"hw": False, "paid": False, "other": False}
        if r["src"] in ["HW", "両方"]:
            phone_map[p]["hw"] = True
        if r["src"] in ["有料媒体", "両方"]:
            phone_map[p]["paid"] = True
        if r["src"] == "その他":
            phone_map[p]["other"] = True

print(f"  ユニーク電話番号: {len(phone_map):,}")
print(f"  HW情報あり: {sum(1 for v in phone_map.values() if v['hw']):,}")
print(f"  有料媒体あり: {sum(1 for v in phone_map.values() if v['paid']):,}")

# --- Lead会社名→媒体マップ ---
print("\nLead会社名マップ構築中...")
company_map = {}
leads["_cn"] = leads["Company"].apply(norm_company)
for _, r in leads[leads["_cn"] != ""][["_cn", "src"]].iterrows():
    cn = r["_cn"]
    if cn not in company_map:
        company_map[cn] = {"hw": False, "paid": False, "other": False}
    if r["src"] in ["HW", "両方"]:
        company_map[cn]["hw"] = True
    if r["src"] in ["有料媒体", "両方"]:
        company_map[cn]["paid"] = True
    if r["src"] == "その他":
        company_map[cn]["other"] = True

print(f"  ユニーク会社名: {len(company_map):,}")

# --- 全受注225件を4段階マッチ ---
print("\n" + "=" * 70)
print("全受注225件の4段階マッチ")
print("=" * 70)

won = opps[opps["IsWon"] == True].copy()
converted_account_ids = set(leads[leads["ConvertedAccountId"].notna()]["ConvertedAccountId"].unique())

# Account情報を結合
won = won.merge(
    accounts[["Id", "Name", "Phone", "PersonMobilePhone"]],
    left_on="AccountId", right_on="Id", how="left", suffixes=("", "_acc")
)

results = []
for _, row in won.iterrows():
    acc_id = row["AccountId"]
    acc_name = row.get("Name_acc", "") or row.get("Account.Name", "")
    if pd.isna(acc_name):
        acc_name = ""
    amount = row.get("Amount", 0)

    method = "マッチなし"
    hw = False
    paid = False
    other_lead = False

    # Step1: ConvertedAccountId
    if acc_id in converted_account_ids:
        method = "1.ConvertedAccountId"
        matched = leads[leads["ConvertedAccountId"] == acc_id]
        srcs = set(matched["src"])
        hw = any(s in srcs for s in ["HW", "両方"])
        paid = any(s in srcs for s in ["有料媒体", "両方"])
        other_lead = "その他" in srcs

    # Step2: Account.Phone → Lead電話番号
    if method == "マッチなし":
        for pcol in ["Phone", "PersonMobilePhone"]:
            p = norm_phone(row.get(pcol, ""))
            if p and p in phone_map:
                method = "2.電話番号"
                hw = phone_map[p]["hw"]
                paid = phone_map[p]["paid"]
                other_lead = phone_map[p]["other"]
                break

    # Step3: Account.Name → Lead.Company
    if method == "マッチなし":
        cn = norm_company(acc_name)
        if cn and cn in company_map:
            method = "3.会社名"
            hw = company_map[cn]["hw"]
            paid = company_map[cn]["paid"]
            other_lead = company_map[cn]["other"]

    # 媒体判定
    if hw and paid:
        source = "両方(HW+有料)"
    elif hw:
        source = "HW"
    elif paid:
        source = "有料媒体"
    elif other_lead:
        source = "その他Lead"
    else:
        source = "Lead不在"

    results.append({
        "account_name": acc_name,
        "amount": amount,
        "method": method,
        "source": source,
        "hw": hw,
        "paid": paid,
        "opp_category": row.get("OpportunityCategory__c", ""),
        "opp_type": row.get("OpportunityType__c", ""),
        "owner": row.get("Owner.Name", ""),
        "close_date": row.get("CloseDate", ""),
        "account_phone": norm_phone(row.get("Phone", "")),
    })

df = pd.DataFrame(results)

# === 集計 ===
print(f"\n■ マッチ方法別:")
for m in ["1.ConvertedAccountId", "2.電話番号", "3.会社名", "マッチなし"]:
    s = df[df["method"] == m]
    print(f"  {m}: {len(s):,}件 ({len(s)/len(df)*100:.1f}%) / {s['amount'].sum():,.0f}円")

print(f"\n■ 媒体帰属:")
total_amount = df["amount"].sum()
for src in ["HW", "有料媒体", "両方(HW+有料)", "その他Lead", "Lead不在"]:
    s = df[df["source"] == src]
    if len(s) > 0:
        print(f"  {src}: {len(s):,}件 ({len(s)/len(df)*100:.1f}%) / {s['amount'].sum():,.0f}円 ({s['amount'].sum()/total_amount*100:.1f}%)")

# HW関与合計
hw_all = df[df["hw"] == True]
print(f"\n  >>> HW関与合計: {len(hw_all):,}件 ({len(hw_all)/len(df)*100:.1f}%) / {hw_all['amount'].sum():,.0f}円 ({hw_all['amount'].sum()/total_amount*100:.1f}%)")

# === 月別 ===
print("\n" + "=" * 70)
print("月別HW貢献率")
print("=" * 70)

df["close_date"] = pd.to_datetime(df["close_date"], errors="coerce")
df["month"] = df["close_date"].dt.to_period("M")

for m in ["2025-11", "2025-12", "2026-01", "2026-02"]:
    md = df[df["month"] == m]
    if len(md) == 0:
        continue
    mhw = md[md["hw"] == True]
    m_nomatch = md[md["source"] == "Lead不在"]
    m_amount = md["amount"].sum()
    print(f"\n  {m}: 受注{len(md)}件 / {m_amount:,.0f}円")
    print(f"    HW関与: {len(mhw)}件 ({len(mhw)/len(md)*100:.1f}%) / {mhw['amount'].sum():,.0f}円 ({mhw['amount'].sum()/m_amount*100:.1f}%)")
    print(f"    Lead不在: {len(m_nomatch)}件 ({len(m_nomatch)/len(md)*100:.1f}%)")

    # マッチ方法内訳
    for method in ["1.ConvertedAccountId", "2.電話番号", "3.会社名", "マッチなし"]:
        cnt = len(md[md["method"] == method])
        if cnt > 0:
            print(f"      [{method}]: {cnt}件")

# === 再商談 ===
print("\n" + "=" * 70)
print("再商談 × 媒体帰属")
print("=" * 70)

re_cat = df[df["opp_category"].fillna("").str.contains("再")]
new_cat = df[~df["opp_category"].fillna("").str.contains("再")]

for label, subset in [("初回商談", new_cat), ("再商談", re_cat)]:
    hw_sub = subset[subset["hw"] == True]
    no_lead = subset[subset["source"] == "Lead不在"]
    print(f"\n  {label}: {len(subset)}件 / {subset['amount'].sum():,.0f}円")
    print(f"    HW関与: {len(hw_sub)}件 ({len(hw_sub)/len(subset)*100:.1f}%) / {hw_sub['amount'].sum():,.0f}円")
    print(f"    Lead不在: {len(no_lead)}件 ({len(no_lead)/len(subset)*100:.1f}%)")

# === Lead不在の詳細 ===
print("\n" + "=" * 70)
print("Lead不在案件の詳細分析")
print("=" * 70)

no_lead_df = df[df["source"] == "Lead不在"]
print(f"\n  Lead不在: {len(no_lead_df)}件 / {no_lead_df['amount'].sum():,.0f}円")

# 電話番号有無
has_phone = (no_lead_df["account_phone"] != "").sum()
print(f"  Account電話あり（だがLeadに電話番号なし）: {has_phone}件")
print(f"  Account電話なし: {len(no_lead_df) - has_phone}件")

# 商談タイプ
print(f"\n  商談カテゴリ:")
for cat, cnt in no_lead_df["opp_category"].fillna("(空)").value_counts().items():
    print(f"    {cat}: {cnt}件")

print(f"\n  商談タイプ:")
for t, cnt in no_lead_df["opp_type"].fillna("(空)").value_counts().items():
    print(f"    {t}: {cnt}件")

# オーナー
print(f"\n  オーナー:")
for o, cnt in no_lead_df["owner"].value_counts().head(10).items():
    amt = no_lead_df[no_lead_df["owner"] == o]["amount"].sum()
    print(f"    {o}: {cnt}件 / {amt:,.0f}円")

# 案件名サンプル（金額順上位20件）
print(f"\n  金額上位20件:")
for _, r in no_lead_df.sort_values("amount", ascending=False).head(20).iterrows():
    print(f"    {r['account_name']}: {r['amount']:,.0f}円 [{r['opp_category']}] {r['owner']}")

# === 最終サマリー ===
print("\n" + "=" * 70)
print("=" * 70)
print("最終サマリー")
print("=" * 70)

media_all = df[df["source"].isin(["HW", "有料媒体", "両方(HW+有料)"])]
other_lead = df[df["source"] == "その他Lead"]
no_lead_total = df[df["source"] == "Lead不在"]

print(f"""
全受注: {len(df)}件 / {total_amount:,.0f}円

=== マッチ結果 ===
  媒体経由（HW/有料/両方）:  {len(media_all):,}件 ({len(media_all)/len(df)*100:.1f}%) / {media_all['amount'].sum():,.0f}円 ({media_all['amount'].sum()/total_amount*100:.1f}%)
  その他Lead経由:            {len(other_lead):,}件 ({len(other_lead)/len(df)*100:.1f}%) / {other_lead['amount'].sum():,.0f}円
  Lead不在:                  {len(no_lead_total):,}件 ({len(no_lead_total)/len(df)*100:.1f}%) / {no_lead_total['amount'].sum():,.0f}円

=== HW関与の受注 ===
  {len(hw_all):,}件 ({len(hw_all)/len(df)*100:.1f}%) / {hw_all['amount'].sum():,.0f}円 ({hw_all['amount'].sum()/total_amount*100:.1f}%)

=== 分析の変遷 ===
  第1回（ConvertedOpportunityId）: 0.0% → ConvertedOpportunityIdが全件空
  第2回（ConvertedAccountId）:     25.8% → 58件
  第3回（+電話+会社名マッチ）:       {len(media_all)/len(df)*100:.1f}% → {len(media_all)}件
  → HW関与（有料含む）:             {len(hw_all)/len(df)*100:.1f}%

=== Lead不在{len(no_lead_total)}件の解釈 ===
  これらは以下のいずれか:
  a) Lead投入前から存在した既存顧客（紹介・口コミ等）
  b) 別の電話番号/名前でLeadが存在するが名寄せ不一致
  c) Leadを経由せず直接Account作成された案件
""")
