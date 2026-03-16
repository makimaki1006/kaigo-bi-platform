"""
Lead不在146件の追加マッチ
- Account名は「施設名」、Leadは「法人名」→ 部分一致で追跡
- Account.Name の部分文字列がLead.Companyに含まれるか
- 結果: HWリードの真の貢献率を最終確定
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

def norm_phone(val):
    if pd.isna(val) or str(val).strip() == "":
        return ""
    d = re.sub(r"[^\d]", "", str(val))
    return d if 10 <= len(d) <= 11 else ""

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

# Lead電話番号マップ
phone_map = {}
for col in ["Phone", "MobilePhone", "Phone2__c", "MobilePhone2__c"]:
    if col not in leads.columns:
        continue
    for _, r in leads[leads[col].notna()][[col, "src"]].iterrows():
        p = norm_phone(r[col])
        if p:
            if p not in phone_map:
                phone_map[p] = {"hw": False, "paid": False}
            if r["src"] in ["HW", "両方"]:
                phone_map[p]["hw"] = True
            if r["src"] in ["有料媒体", "両方"]:
                phone_map[p]["paid"] = True

# 前段階マッチ済みAccountId
converted_account_ids = set(leads[leads["ConvertedAccountId"].notna()]["ConvertedAccountId"].unique())

# 受注
won = opps[opps["IsWon"] == True].copy()
won = won.merge(
    accounts[["Id", "Name", "Phone", "PersonMobilePhone"]],
    left_on="AccountId", right_on="Id", how="left", suffixes=("", "_acc")
)

# Step1-3マッチ済みを特定
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

leads["_cn"] = leads["Company"].apply(norm_company)
company_map = {}
for _, r in leads[leads["_cn"] != ""][["_cn", "src"]].iterrows():
    cn = r["_cn"]
    if cn not in company_map:
        company_map[cn] = {"hw": False, "paid": False}
    if r["src"] in ["HW", "両方"]:
        company_map[cn]["hw"] = True
    if r["src"] in ["有料媒体", "両方"]:
        company_map[cn]["paid"] = True

# 未マッチ受注を抽出
print("=" * 70)
print("Lead不在146件の追加マッチ（部分一致 + Account全電話列）")
print("=" * 70)

unmatched_rows = []
for _, row in won.iterrows():
    acc_id = row["AccountId"]
    acc_name = str(row.get("Name_acc", "") or row.get("Account.Name", "") or "")

    # Step1-3でマッチ済みか判定
    matched = False
    if acc_id in converted_account_ids:
        matched = True
    if not matched:
        for pcol in ["Phone", "PersonMobilePhone"]:
            p = norm_phone(row.get(pcol, ""))
            if p and p in phone_map:
                matched = True
                break
    if not matched:
        cn = norm_company(acc_name)
        if cn and cn in company_map:
            matched = True

    if not matched:
        unmatched_rows.append(row)

print(f"\nStep1-3で未マッチ: {len(unmatched_rows)}件")

# === 追加マッチ手法 ===
# A) Account側にLead IDへの参照がないか確認
# B) Account全電話列でマッチ（Phone以外にもフィールドがあるか）
# C) Account名の部分一致

# Account電話列を確認
account_phone_cols = [c for c in accounts.columns if "phone" in c.lower() or "tel" in c.lower()]
print(f"\nAccount電話関連列: {account_phone_cols}")

# Lead.Companyの全リストからインデックス構築（3文字以上の部分文字列）
# → 計算量が大きすぎるので、代わにAccount名 → Lead.Companyの直接検索

print("\n部分一致マッチ中...")
additional_matches = []
for row in unmatched_rows:
    acc_name = str(row.get("Name_acc", "") or row.get("Account.Name", "") or "")
    amount = row.get("Amount", 0)

    if not acc_name or acc_name == "nan":
        additional_matches.append({
            "account_name": acc_name,
            "amount": amount,
            "match_type": "名前なし",
            "hw": False,
            "paid": False,
            "matched_company": "",
        })
        continue

    # 法人格除去
    search_name = norm_company(acc_name)
    if len(search_name) < 3:
        additional_matches.append({
            "account_name": acc_name,
            "amount": amount,
            "match_type": "名前短すぎ",
            "hw": False,
            "paid": False,
            "matched_company": "",
        })
        continue

    # Lead.Companyに部分一致検索
    # Account名が Lead.Company に含まれる OR Lead.Company が Account名に含まれる
    mask = leads["_cn"].apply(lambda x: search_name in x or x in search_name if x and len(x) >= 3 else False)
    matched_leads = leads[mask]

    if len(matched_leads) > 0:
        srcs = set(matched_leads["src"])
        hw = any(s in srcs for s in ["HW", "両方"])
        paid = any(s in srcs for s in ["有料媒体", "両方"])
        sample = matched_leads["Company"].iloc[0]
        additional_matches.append({
            "account_name": acc_name,
            "amount": amount,
            "match_type": "部分一致",
            "hw": hw,
            "paid": paid,
            "matched_company": sample,
            "matched_count": len(matched_leads),
        })
    else:
        additional_matches.append({
            "account_name": acc_name,
            "amount": amount,
            "match_type": "マッチなし",
            "hw": False,
            "paid": False,
            "matched_company": "",
        })

add_df = pd.DataFrame(additional_matches)

print(f"\n■ 追加マッチ結果:")
for mt in add_df["match_type"].value_counts().index:
    s = add_df[add_df["match_type"] == mt]
    print(f"  {mt}: {len(s)}件 / {s['amount'].sum():,.0f}円")

# 部分一致の内訳
partial = add_df[add_df["match_type"] == "部分一致"]
if len(partial) > 0:
    hw_partial = partial[partial["hw"] == True]
    paid_partial = partial[partial["paid"] == True]
    print(f"\n  部分一致のうち:")
    print(f"    HW関与: {len(hw_partial)}件 / {hw_partial['amount'].sum():,.0f}円")
    print(f"    有料媒体: {len(paid_partial)}件 / {paid_partial['amount'].sum():,.0f}円")

    print(f"\n  部分一致の例:")
    for _, r in partial.head(20).iterrows():
        src_label = "HW" if r["hw"] else ("有料" if r["paid"] else "その他")
        print(f"    Account: {r['account_name']} → Lead: {r.get('matched_company','')} [{src_label}] {r['amount']:,.0f}円")

# === 最終: 全段階合計 ===
print("\n" + "=" * 70)
print("=" * 70)
print("全段階合計: HW関与の真の受注貢献率")
print("=" * 70)

# Step1-3: 56件 HW関与 / 43,184,345円
# Step4(部分一致): hw_partial
total_won = 225
total_amount = won["Amount"].sum()

prev_hw_count = 56
prev_hw_amount = 43_184_345

new_hw_count = len(hw_partial) if len(partial) > 0 else 0
new_hw_amount = hw_partial["amount"].sum() if len(partial) > 0 else 0

final_hw = prev_hw_count + new_hw_count
final_hw_amount = prev_hw_amount + new_hw_amount

# 全媒体
prev_media = 63
prev_media_amount = 49_724_345
new_media_count = len(partial[partial["hw"] | partial["paid"]]) if len(partial) > 0 else 0
new_media_amount = partial[partial["hw"] | partial["paid"]]["amount"].sum() if len(partial) > 0 else 0

final_media = prev_media + new_media_count
final_media_amount = prev_media_amount + new_media_amount

# 真のマッチなし
true_no_match = add_df[add_df["match_type"] == "マッチなし"]
other_lead_partial = partial[(~partial["hw"]) & (~partial["paid"])]

print(f"""
全受注: {total_won}件 / {total_amount:,.0f}円

■ 4段階マッチ積み上げ
  Step1 ConvertedAccountId:  69件
  Step2 電話番号マッチ:       5件
  Step3 会社名完全一致:       5件
  Step4 会社名部分一致:       {len(partial)}件 ← NEW
  → 真のマッチなし:          {len(true_no_match)}件

■ HW関与の受注（最終）
  件数: {final_hw}/{total_won} = {final_hw/total_won*100:.1f}%
  金額: {final_hw_amount:,.0f}/{total_amount:,.0f}円 = {final_hw_amount/total_amount*100:.1f}%

■ 全媒体経由の受注（最終）
  件数: {final_media}/{total_won} = {final_media/total_won*100:.1f}%
  金額: {final_media_amount:,.0f}/{total_amount:,.0f}円 = {final_media_amount/total_amount*100:.1f}%

■ 真のLead不在
  {len(true_no_match)}件 ({len(true_no_match)/total_won*100:.1f}%)
  → これらは紹介・直接営業等、リスト施策を経由していない可能性が高い

■ 分析の推移
  第1回: 0.0%  (ConvertedOpportunityId空)
  第2回: 25.8% (ConvertedAccountId)
  第3回: 28.0% (+電話+会社名完全一致)
  第4回: {final_media/total_won*100:.1f}% (+会社名部分一致)  ← 最終
""")
