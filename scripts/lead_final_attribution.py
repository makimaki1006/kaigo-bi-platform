"""
リード施策別 受注貢献 最終分析
- Account全電話列（15列）でマッチ
- 部分一致は5文字以上に制限（誤マッチ防止）
- 3カテゴリ: HWのみ / 有料媒体のみ / 両方
- リード内容強化の効果計測
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
print("リード施策別 受注貢献 最終分析")
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

# 有料媒体詳細
leads["paid_detail"] = ""
if "Paid_DataSource__c" in leads.columns:
    leads.loc[has_paid, "paid_detail"] = leads.loc[has_paid, "Paid_DataSource__c"].fillna("不明")

# --- Lead電話番号マップ（4列）---
print("\nLead電話番号マップ構築...")
phone_map = {}  # norm_phone -> {hw, paid, other, details}
for col in ["Phone", "MobilePhone", "Phone2__c", "MobilePhone2__c"]:
    if col not in leads.columns:
        continue
    for _, r in leads[leads[col].notna()][[col, "src", "paid_detail"]].iterrows():
        p = norm_phone(r[col])
        if p:
            if p not in phone_map:
                phone_map[p] = {"hw": False, "paid": False, "other": False, "details": set()}
            if r["src"] in ["HW", "両方"]:
                phone_map[p]["hw"] = True
            if r["src"] in ["有料媒体", "両方"]:
                phone_map[p]["paid"] = True
                if r["paid_detail"]:
                    phone_map[p]["details"].add(r["paid_detail"])
            if r["src"] == "その他":
                phone_map[p]["other"] = True

print(f"  ユニーク電話番号: {len(phone_map):,}")

# --- Lead会社名マップ ---
leads["_cn"] = leads["Company"].apply(norm_company)
company_map = {}
for _, r in leads[leads["_cn"] != ""][["_cn", "src", "paid_detail"]].iterrows():
    cn = r["_cn"]
    if cn not in company_map:
        company_map[cn] = {"hw": False, "paid": False, "other": False, "details": set()}
    if r["src"] in ["HW", "両方"]:
        company_map[cn]["hw"] = True
    if r["src"] in ["有料媒体", "両方"]:
        company_map[cn]["paid"] = True
        if r["paid_detail"]:
            company_map[cn]["details"].add(r["paid_detail"])
    if r["src"] == "その他":
        company_map[cn]["other"] = True

print(f"  ユニーク会社名: {len(company_map):,}")

# --- Account全電話列マップ ---
print("\nAccount全電話列マップ構築...")
account_phone_cols = ["Phone", "PersonMobilePhone", "Phone2__c",
                      "PhoneChecker1__c", "PhoneChecker2__c",
                      "GooglePhoneSearch__c", "Phone2__pc",
                      "MobilePhone2__pc", "Account_Phone__pc",
                      "GooglePhoneSerach__pc", "PhoneChecker1__pc",
                      "PhoneChecker2__pc", "MobilePhoneChecker2__pc",
                      "MobilePhoneChecker1__pc"]

# AccountId → 全電話番号のセット
account_phones = {}
for col in account_phone_cols:
    if col not in accounts.columns:
        continue
    for _, r in accounts[accounts[col].notna()][["Id", col]].iterrows():
        p = norm_phone(r[col])
        if p:
            aid = r["Id"]
            if aid not in account_phones:
                account_phones[aid] = set()
            account_phones[aid].add(p)

print(f"  電話番号付きAccount: {len(account_phones):,}")

# --- ConvertedAccountIdセット ---
converted_account_ids = set(leads[leads["ConvertedAccountId"].notna()]["ConvertedAccountId"].unique())

# --- 受注全件マッチ ---
print("\n" + "=" * 70)
print("全受注225件の5段階マッチ")
print("=" * 70)

won = opps[opps["IsWon"] == True].copy()
won = won.merge(
    accounts[["Id", "Name"]],
    left_on="AccountId", right_on="Id", how="left", suffixes=("", "_acc")
)

results = []
for _, row in won.iterrows():
    acc_id = row["AccountId"]
    acc_name = str(row.get("Name_acc", "") or row.get("Account.Name", "") or "")
    if acc_name == "nan":
        acc_name = ""
    amount = row.get("Amount", 0)

    method = "マッチなし"
    hw = False
    paid = False
    other_lead = False
    paid_details = set()

    # Step1: ConvertedAccountId
    if acc_id in converted_account_ids:
        method = "1.ConvertedAccountId"
        matched = leads[leads["ConvertedAccountId"] == acc_id]
        srcs = set(matched["src"])
        hw = any(s in srcs for s in ["HW", "両方"])
        paid = any(s in srcs for s in ["有料媒体", "両方"])
        other_lead = "その他" in srcs
        paid_details = set(matched[matched["paid_detail"] != ""]["paid_detail"].unique())

    # Step2: Account全電話列 → Lead電話番号
    if method == "マッチなし":
        all_phones = account_phones.get(acc_id, set())
        for p in all_phones:
            if p in phone_map:
                method = "2.電話番号(全列)"
                hw = phone_map[p]["hw"]
                paid = phone_map[p]["paid"]
                other_lead = phone_map[p]["other"]
                paid_details = phone_map[p]["details"]
                break

    # Step3: 会社名完全一致
    if method == "マッチなし":
        cn = norm_company(acc_name)
        if cn and cn in company_map:
            method = "3.会社名完全"
            hw = company_map[cn]["hw"]
            paid = company_map[cn]["paid"]
            other_lead = company_map[cn]["other"]
            paid_details = company_map[cn]["details"]

    # Step4: 会社名部分一致（5文字以上）
    if method == "マッチなし":
        cn = norm_company(acc_name)
        if cn and len(cn) >= 5:
            # Lead会社名にAccount名が含まれる or その逆
            matched_leads = leads[leads["_cn"].apply(
                lambda x: (len(x) >= 5 and cn in x) or (len(cn) >= 5 and x in cn and len(x) >= 5)
                if x else False
            )]
            if len(matched_leads) > 0:
                method = "4.部分一致(5文字+)"
                srcs = set(matched_leads["src"])
                hw = any(s in srcs for s in ["HW", "両方"])
                paid = any(s in srcs for s in ["有料媒体", "両方"])
                other_lead = "その他" in srcs
                paid_details = set(matched_leads[matched_leads["paid_detail"] != ""]["paid_detail"].unique())

    # 3カテゴリ判定
    if hw and paid:
        category = "3.両方(HW+有料)"
    elif hw:
        category = "1.HWのみ"
    elif paid:
        category = "2.有料媒体のみ"
    elif other_lead:
        category = "4.その他Lead"
    else:
        category = "5.Lead不在"

    results.append({
        "account_name": acc_name,
        "amount": amount,
        "method": method,
        "category": category,
        "hw": hw,
        "paid": paid,
        "paid_details": "|".join(sorted(paid_details)) if paid_details else "",
        "opp_category": row.get("OpportunityCategory__c", ""),
        "opp_type": row.get("OpportunityType__c", ""),
        "owner": row.get("Owner.Name", ""),
        "close_date": row.get("CloseDate", ""),
    })

df = pd.DataFrame(results)
total_count = len(df)
total_amount = df["amount"].sum()

# === 集計 ===
print(f"\n■ マッチ方法別:")
for m in ["1.ConvertedAccountId", "2.電話番号(全列)", "3.会社名完全", "4.部分一致(5文字+)", "マッチなし"]:
    s = df[df["method"] == m]
    print(f"  {m}: {len(s):,}件 ({len(s)/total_count*100:.1f}%) / {s['amount'].sum():,.0f}円")

# === 3カテゴリ集計 ===
print("\n" + "=" * 70)
print("リード施策3カテゴリ別 受注貢献")
print("=" * 70)

print(f"\n{'カテゴリ':<22} {'件数':>6} {'構成比':>8} {'受注金額':>14} {'金額構成比':>8} {'平均単価':>10}")
print("-" * 75)

for cat in ["1.HWのみ", "2.有料媒体のみ", "3.両方(HW+有料)", "4.その他Lead", "5.Lead不在"]:
    s = df[df["category"] == cat]
    cnt = len(s)
    amt = s["amount"].sum()
    avg = amt / cnt if cnt > 0 else 0
    print(f"  {cat:<20} {cnt:>5}件 {cnt/total_count*100:>7.1f}% {amt:>13,.0f}円 {amt/total_amount*100:>7.1f}% {avg:>9,.0f}円")

print(f"  {'-'*73}")
print(f"  {'合計':<20} {total_count:>5}件 {100:>7.1f}% {total_amount:>13,.0f}円 {100:>7.1f}%")

# リード強化効果
hw_all = df[df["hw"] == True]
paid_all = df[df["paid"] == True]
media_all = df[df["category"].isin(["1.HWのみ", "2.有料媒体のみ", "3.両方(HW+有料)"])]

print(f"\n  >>> HW関与合計: {len(hw_all)}件 ({len(hw_all)/total_count*100:.1f}%) / {hw_all['amount'].sum():,.0f}円 ({hw_all['amount'].sum()/total_amount*100:.1f}%)")
print(f"  >>> 有料媒体関与合計: {len(paid_all)}件 ({len(paid_all)/total_count*100:.1f}%) / {paid_all['amount'].sum():,.0f}円")
print(f"  >>> 全媒体合計: {len(media_all)}件 ({len(media_all)/total_count*100:.1f}%) / {media_all['amount'].sum():,.0f}円 ({media_all['amount'].sum()/total_amount*100:.1f}%)")

# === 月別 × 3カテゴリ ===
print("\n" + "=" * 70)
print("月別 × 3カテゴリ")
print("=" * 70)

df["close_date_dt"] = pd.to_datetime(df["close_date"], errors="coerce")
df["month"] = df["close_date_dt"].dt.to_period("M")

for m in ["2025-11", "2025-12", "2026-01", "2026-02"]:
    md = df[df["month"] == m]
    if len(md) == 0:
        continue
    m_total = len(md)
    m_amount = md["amount"].sum()

    print(f"\n  --- {m} --- (受注: {m_total}件 / {m_amount:,.0f}円)")

    for cat in ["1.HWのみ", "2.有料媒体のみ", "3.両方(HW+有料)", "4.その他Lead", "5.Lead不在"]:
        s = md[md["category"] == cat]
        if len(s) > 0:
            print(f"    {cat}: {len(s)}件 ({len(s)/m_total*100:.1f}%) / {s['amount'].sum():,.0f}円 ({s['amount'].sum()/m_amount*100:.1f}%)")

    # HW関与合計
    m_hw = md[md["hw"] == True]
    print(f"    --- HW関与合計: {len(m_hw)}件 ({len(m_hw)/m_total*100:.1f}%) / {m_hw['amount'].sum():,.0f}円 ({m_hw['amount'].sum()/m_amount*100:.1f}%)")

# === 再商談 × 3カテゴリ ===
print("\n" + "=" * 70)
print("再商談 × 3カテゴリ")
print("=" * 70)

for label, subset in [("初回商談", df[~df["opp_category"].fillna("").str.contains("再")]),
                       ("再商談", df[df["opp_category"].fillna("").str.contains("再")])]:
    s_total = len(subset)
    s_amount = subset["amount"].sum()
    print(f"\n  {label}: {s_total}件 / {s_amount:,.0f}円")

    for cat in ["1.HWのみ", "2.有料媒体のみ", "3.両方(HW+有料)", "4.その他Lead", "5.Lead不在"]:
        s = subset[subset["category"] == cat]
        if len(s) > 0:
            print(f"    {cat}: {len(s)}件 ({len(s)/s_total*100:.1f}%) / {s['amount'].sum():,.0f}円")

    hw_sub = subset[subset["hw"] == True]
    print(f"    --- HW関与合計: {len(hw_sub)}件 ({len(hw_sub)/s_total*100:.1f}%)")

# === 有料媒体の詳細 ===
print("\n" + "=" * 70)
print("有料媒体の詳細内訳（受注商談）")
print("=" * 70)

paid_won = df[df["paid"] == True]
if len(paid_won) > 0:
    all_details = []
    for _, r in paid_won.iterrows():
        for d in str(r["paid_details"]).split("|"):
            if d and d != "不明":
                all_details.append({"media": d, "amount": r["amount"]})
    if all_details:
        detail_df = pd.DataFrame(all_details)
        summary = detail_df.groupby("media").agg(count=("amount", "size"), total=("amount", "sum")).sort_values("total", ascending=False)
        print(f"\n  {'媒体':<25} {'件数':>6} {'金額':>12}")
        print(f"  {'-'*50}")
        for media, r in summary.iterrows():
            print(f"  {media:<25} {r['count']:>5}件 {r['total']:>11,.0f}円")

# === 商談タイプ × 3カテゴリ ===
print("\n" + "=" * 70)
print("商談タイプ（代表者/担当者）× 3カテゴリ")
print("=" * 70)

for otype in ["代表者商談", "代表者商談（決裁者）", "担当者商談", "担当者商談（決裁者）"]:
    subset = df[df["opp_type"].fillna("") == otype]
    if len(subset) == 0:
        continue
    hw_sub = subset[subset["hw"] == True]
    print(f"\n  {otype}: {len(subset)}件")
    print(f"    HW関与: {len(hw_sub)}件 ({len(hw_sub)/len(subset)*100:.1f}%)")

# === Step4部分一致の詳細検証 ===
print("\n" + "=" * 70)
print("Step4 部分一致マッチの検証（5文字以上）")
print("=" * 70)

step4 = df[df["method"] == "4.部分一致(5文字+)"]
print(f"\n  部分一致マッチ: {len(step4)}件")
for _, r in step4.iterrows():
    src_label = r["category"]
    print(f"    {r['account_name']}: {r['amount']:,.0f}円 [{src_label}]")

# === Lead不在の詳細 ===
print("\n" + "=" * 70)
print("Lead不在案件（金額上位）")
print("=" * 70)

no_lead = df[df["category"] == "5.Lead不在"].sort_values("amount", ascending=False)
print(f"\n  Lead不在: {len(no_lead)}件 / {no_lead['amount'].sum():,.0f}円")
for _, r in no_lead.head(15).iterrows():
    print(f"    {r['account_name']}: {r['amount']:,.0f}円 [{r['opp_category']}] {r['owner']}")

# === リード強化効果 ===
print("\n" + "=" * 70)
print("=" * 70)
print("リード内容強化の効果")
print("=" * 70)
print("=" * 70)

# 「両方」= HWリードに有料媒体で追加情報が付いた、または逆
both = df[df["category"] == "3.両方(HW+有料)"]
hw_only = df[df["category"] == "1.HWのみ"]
paid_only = df[df["category"] == "2.有料媒体のみ"]

print(f"""
■ 3カテゴリ別の受注効率

  1. HWのみ:        {len(hw_only):>3}件 / {hw_only['amount'].sum():>12,.0f}円  平均単価 {hw_only['amount'].mean():>9,.0f}円
  2. 有料媒体のみ:    {len(paid_only):>3}件 / {paid_only['amount'].sum():>12,.0f}円  平均単価 {paid_only['amount'].mean():>9,.0f}円
  3. 両方(HW+有料):  {len(both):>3}件 / {both['amount'].sum():>12,.0f}円  平均単価 {both['amount'].mean():>9,.0f}円

■ リード数 → コンバート率 → 受注率の比較

  ※ Lead総数からの効率比較
""")

# Leadレベルの効率
for label, src_val in [("HWのみ", "HW"), ("有料媒体のみ", "有料媒体"), ("両方", "両方")]:
    lead_count = len(leads[leads["src"] == src_val])
    converted = len(leads[(leads["src"] == src_val) & (leads["IsConverted"] == True)])
    cv_rate = converted / lead_count * 100 if lead_count > 0 else 0

    if label == "HWのみ":
        won_count = len(hw_only)
        won_amount = hw_only["amount"].sum()
    elif label == "有料媒体のみ":
        won_count = len(paid_only)
        won_amount = paid_only["amount"].sum()
    else:
        won_count = len(both)
        won_amount = both["amount"].sum()

    won_rate = won_count / converted * 100 if converted > 0 else 0
    roi_per_lead = won_amount / lead_count if lead_count > 0 else 0

    print(f"  {label}:")
    print(f"    Lead数: {lead_count:>8,}  CV: {converted:>5} ({cv_rate:.2f}%)  受注: {won_count:>3}件  受注率: {won_rate:.1f}%  Lead1件あたり売上: {roi_per_lead:,.0f}円")

# 月次トレンド: 3カテゴリ比率の推移
print(f"\n■ 月別 HW関与率の推移")
print(f"\n  {'月':>8} {'受注':>6} {'HWのみ':>8} {'有料のみ':>8} {'両方':>8} {'Lead不在':>8} {'HW関与率':>8}")
print(f"  {'-'*60}")
for m in ["2025-11", "2025-12", "2026-01", "2026-02"]:
    md = df[df["month"] == m]
    if len(md) == 0:
        continue
    m_hw = len(md[md["category"] == "1.HWのみ"])
    m_paid = len(md[md["category"] == "2.有料媒体のみ"])
    m_both = len(md[md["category"] == "3.両方(HW+有料)"])
    m_nolead = len(md[md["category"] == "5.Lead不在"])
    m_hw_all = len(md[md["hw"] == True])
    print(f"  {m:>8} {len(md):>5}件 {m_hw:>7}件 {m_paid:>7}件 {m_both:>7}件 {m_nolead:>7}件 {m_hw_all/len(md)*100:>7.1f}%")

print("\n" + "=" * 70)
print("分析完了")
print("=" * 70)
