"""
媒体貢献の深掘り分析
- 「Lead紐づけなし」156件の受注を名前/電話番号でLead逆引き
- ハローワーク起源の隠れた貢献を発掘
- ConvertedAccountIdがなくても、Account.Phone → Lead.Phone で追跡
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
LEAD_FILE = DATA_DIR / "Lead_20260305_115825.csv"
OPP_FILE = DATA_DIR / "analysis" / "opportunities_detailed.csv"
ACCOUNT_FILE = DATA_DIR / "Account_20260305_115035.csv"

print("=" * 70)
print("媒体貢献 深掘り分析: Lead紐づけなし受注の逆引き")
print("=" * 70)

# === データ読み込み ===
print("\nデータ読み込み中...")
leads = pd.read_csv(LEAD_FILE, encoding="utf-8-sig", low_memory=False)
opps = pd.read_csv(OPP_FILE, encoding="utf-8-sig", low_memory=False)
accounts = pd.read_csv(ACCOUNT_FILE, encoding="utf-8-sig", low_memory=False)
opps["Amount"] = pd.to_numeric(opps["Amount"], errors="coerce")

print(f"  Lead: {len(leads):,}件 / Opportunity: {len(opps):,}件 / Account: {len(accounts):,}件")

# === Leadの媒体分類 ===
hw_cols = ["Hellowork_Occupation__c", "Hellowork_DataImportDate__c",
           "Hellowork_URL__c", "Hellowork_JobPublicationDate__c"]
paid_cols = ["Paid_Media__c", "Paid_DataSource__c", "Paid_URL__c", "Paid_Memo__c"]

has_hw = leads[hw_cols].notna().any(axis=1) & (leads[hw_cols].astype(str) != "").any(axis=1)
has_paid = leads[paid_cols].notna().any(axis=1) & (leads[paid_cols].astype(str) != "").any(axis=1)

leads["ListSource"] = "その他/不明"
leads.loc[has_hw & has_paid, "ListSource"] = "両方(HW+有料)"
leads.loc[has_hw & ~has_paid, "ListSource"] = "ハローワーク"
leads.loc[~has_hw & has_paid, "ListSource"] = "有料媒体"

# === 電話番号正規化関数 ===
def normalize_phone(phone_str):
    """電話番号を数字のみに正規化"""
    if pd.isna(phone_str) or str(phone_str).strip() == "":
        return ""
    digits = re.sub(r"[^\d]", "", str(phone_str))
    if len(digits) >= 10 and len(digits) <= 11:
        return digits
    return ""

# === Lead電話番号→媒体マップ作成 ===
print("\n" + "=" * 70)
print("1. Lead電話番号 → 媒体マッピング作成")
print("=" * 70)

leads["phone_norm"] = leads["Phone"].apply(normalize_phone)
leads["mobile_norm"] = leads["MobilePhone"].apply(normalize_phone)

# 電話番号→媒体情報のマップ（複数Leadが同じ電話番号を持つ場合あり）
phone_to_source = {}
for _, row in leads.iterrows():
    for phone_col in ["phone_norm", "mobile_norm"]:
        phone = row[phone_col]
        if phone:
            if phone not in phone_to_source:
                phone_to_source[phone] = {
                    "sources": set(),
                    "lead_count": 0,
                    "has_hw": False,
                    "has_paid": False,
                }
            phone_to_source[phone]["sources"].add(row["ListSource"])
            phone_to_source[phone]["lead_count"] += 1
            if row["ListSource"] in ["ハローワーク", "両方(HW+有料)"]:
                phone_to_source[phone]["has_hw"] = True
            if row["ListSource"] in ["有料媒体", "両方(HW+有料)"]:
                phone_to_source[phone]["has_paid"] = True

print(f"  ユニーク電話番号（Lead側）: {len(phone_to_source):,}件")
hw_phones = sum(1 for v in phone_to_source.values() if v["has_hw"])
paid_phones = sum(1 for v in phone_to_source.values() if v["has_paid"])
print(f"  うちHW情報あり: {hw_phones:,}件")
print(f"  うち有料媒体情報あり: {paid_phones:,}件")

# === Lead会社名→媒体マップ作成 ===
print("\n" + "=" * 70)
print("2. Lead会社名 → 媒体マッピング作成")
print("=" * 70)

def normalize_company(name):
    """会社名を正規化（法人格除去、空白除去）"""
    if pd.isna(name) or str(name).strip() == "":
        return ""
    s = str(name).strip()
    # 法人格除去
    for corp in ["株式会社", "有限会社", "合同会社", "一般社団法人", "一般財団法人",
                 "社会福祉法人", "医療法人", "医療法人社団", "医療法人財団",
                 "特定非営利活動法人", "NPO法人", "学校法人", "宗教法人",
                 "社会医療法人", "公益社団法人", "公益財団法人"]:
        s = s.replace(corp, "")
    s = re.sub(r"\s+", "", s)
    return s

leads["company_norm"] = leads["Company"].apply(normalize_company)

company_to_source = {}
for _, row in leads[leads["company_norm"] != ""].iterrows():
    comp = row["company_norm"]
    if comp not in company_to_source:
        company_to_source[comp] = {
            "sources": set(),
            "lead_count": 0,
            "has_hw": False,
            "has_paid": False,
        }
    company_to_source[comp]["sources"].add(row["ListSource"])
    company_to_source[comp]["lead_count"] += 1
    if row["ListSource"] in ["ハローワーク", "両方(HW+有料)"]:
        company_to_source[comp]["has_hw"] = True
    if row["ListSource"] in ["有料媒体", "両方(HW+有料)"]:
        company_to_source[comp]["has_paid"] = True

print(f"  ユニーク会社名（Lead側）: {len(company_to_source):,}件")

# === ConvertedAccountIdベースのマップ（前回分析） ===
converted_leads = leads[leads["ConvertedAccountId"].notna()]
converted_account_ids = set(converted_leads["ConvertedAccountId"].unique())

# === 受注Opportunityの逆引き ===
print("\n" + "=" * 70)
print("3. 受注Opportunity → Account → Phone/Name → Lead 逆引き")
print("=" * 70)

won_opps = opps[opps["IsWon"] == True].copy()
print(f"\n  受注Opportunity: {len(won_opps):,}件")

# Account情報を結合
accounts["phone_norm"] = accounts["Phone"].apply(normalize_phone)
accounts["name_norm"] = accounts["Name"].apply(normalize_company)

won_with_account = won_opps.merge(
    accounts[["Id", "Name", "Phone", "phone_norm", "name_norm"]],
    left_on="AccountId",
    right_on="Id",
    how="left",
    suffixes=("_opp", "_account"),
)

# 3段階マッチ
results = []
for idx, row in won_with_account.iterrows():
    account_id = row["AccountId"]
    account_name = row.get("Name_account", "") or row.get("Account.Name", "")
    account_phone = row.get("phone_norm", "")
    account_name_norm = row.get("name_norm", "")
    amount = row.get("Amount", 0)
    opp_id = row.get("Id_opp", "")

    match_method = "マッチなし"
    matched_source = "不明"
    has_hw = False
    has_paid = False

    # Step1: ConvertedAccountId（前回分析と同じ）
    if account_id in converted_account_ids:
        matched_leads = converted_leads[converted_leads["ConvertedAccountId"] == account_id]
        sources = set(matched_leads["ListSource"].unique())
        has_hw = any(s in sources for s in ["ハローワーク", "両方(HW+有料)"])
        has_paid = any(s in sources for s in ["有料媒体", "両方(HW+有料)"])
        match_method = "ConvertedAccountId"
        if has_hw and has_paid:
            matched_source = "両方(HW+有料)"
        elif has_hw:
            matched_source = "ハローワーク"
        elif has_paid:
            matched_source = "有料媒体"
        else:
            matched_source = "その他/不明"

    # Step2: 電話番号マッチ
    elif account_phone and account_phone in phone_to_source:
        info = phone_to_source[account_phone]
        has_hw = info["has_hw"]
        has_paid = info["has_paid"]
        match_method = "電話番号"
        if has_hw and has_paid:
            matched_source = "両方(HW+有料)"
        elif has_hw:
            matched_source = "ハローワーク"
        elif has_paid:
            matched_source = "有料媒体"
        else:
            matched_source = "その他/不明"

    # Step3: 会社名マッチ
    elif account_name_norm and account_name_norm in company_to_source:
        info = company_to_source[account_name_norm]
        has_hw = info["has_hw"]
        has_paid = info["has_paid"]
        match_method = "会社名"
        if has_hw and has_paid:
            matched_source = "両方(HW+有料)"
        elif has_hw:
            matched_source = "ハローワーク"
        elif has_paid:
            matched_source = "有料媒体"
        else:
            matched_source = "その他/不明"

    results.append({
        "opp_id": opp_id,
        "account_id": account_id,
        "account_name": account_name,
        "amount": amount,
        "match_method": match_method,
        "matched_source": matched_source,
        "has_hw": has_hw,
        "has_paid": has_paid,
        "close_date": row.get("CloseDate", ""),
        "stage": row.get("StageName", ""),
        "opp_category": row.get("OpportunityCategory__c", ""),
        "opp_type": row.get("OpportunityType__c", ""),
        "owner": row.get("Owner.Name", ""),
    })

result_df = pd.DataFrame(results)

# === マッチ方法別集計 ===
print(f"\n  マッチ方法別:")
for method in ["ConvertedAccountId", "電話番号", "会社名", "マッチなし"]:
    subset = result_df[result_df["match_method"] == method]
    count = len(subset)
    amount = subset["amount"].sum()
    print(f"    {method}: {count:,}件 ({count/len(result_df)*100:.1f}%) / {amount:,.0f}円")

# === 媒体元別集計（3段階マッチ後） ===
print(f"\n  媒体元別（3段階マッチ後）:")
for source in ["ハローワーク", "有料媒体", "両方(HW+有料)", "その他/不明", "不明"]:
    subset = result_df[result_df["matched_source"] == source]
    count = len(subset)
    amount = subset["amount"].sum()
    if count > 0:
        print(f"    {source}: {count:,}件 ({count/len(result_df)*100:.1f}%) / {amount:,.0f}円 ({amount/result_df['amount'].sum()*100:.1f}%)")

# === 前回との比較 ===
print("\n" + "=" * 70)
print("4. 前回分析（ConvertedAccountIdのみ）vs 今回（3段階マッチ）")
print("=" * 70)

# 前回: ConvertedAccountIdのみ
prev_matched = result_df[result_df["match_method"] == "ConvertedAccountId"]
prev_media = prev_matched[prev_matched["matched_source"].isin(["ハローワーク", "有料媒体", "両方(HW+有料)"])]

# 今回: 3段階マッチ全体
now_media = result_df[result_df["matched_source"].isin(["ハローワーク", "有料媒体", "両方(HW+有料)"])]

total_count = len(result_df)
total_amount = result_df["amount"].sum()

print(f"\n  {'':20} {'前回':>15} {'今回':>15} {'増分':>15}")
print(f"  {'-'*65}")
print(f"  {'媒体経由（件数）':20} {len(prev_media):>14,}件 {len(now_media):>14,}件 {len(now_media)-len(prev_media):>+14,}件")
print(f"  {'媒体経由（金額）':20} {prev_media['amount'].sum():>13,.0f}円 {now_media['amount'].sum():>13,.0f}円 {now_media['amount'].sum()-prev_media['amount'].sum():>+13,.0f}円")
print(f"  {'貢献率（件数）':20} {len(prev_media)/total_count*100:>14.1f}% {len(now_media)/total_count*100:>14.1f}% {(len(now_media)-len(prev_media))/total_count*100:>+14.1f}%")
print(f"  {'貢献率（金額）':20} {prev_media['amount'].sum()/total_amount*100:>14.1f}% {now_media['amount'].sum()/total_amount*100:>14.1f}% {(now_media['amount'].sum()-prev_media['amount'].sum())/total_amount*100:>+14.1f}%")

# === HW vs 有料の内訳 ===
print("\n" + "=" * 70)
print("5. ハローワーク vs 有料媒体の受注貢献（3段階マッチ後）")
print("=" * 70)

# has_hw / has_paid フラグで集計（重複あり）
hw_any = result_df[result_df["has_hw"] == True]
paid_any = result_df[result_df["has_paid"] == True]
hw_only = result_df[(result_df["has_hw"] == True) & (result_df["has_paid"] == False)]
paid_only = result_df[(result_df["has_hw"] == False) & (result_df["has_paid"] == True)]
both = result_df[(result_df["has_hw"] == True) & (result_df["has_paid"] == True)]
no_match = result_df[result_df["matched_source"] == "不明"]

print(f"\n  {'区分':<25} {'件数':>8} {'構成比':>8} {'金額':>14} {'構成比':>8}")
print(f"  {'-'*70}")
print(f"  {'HW経由（HWのみ）':<25} {len(hw_only):>7,}件 {len(hw_only)/total_count*100:>7.1f}% {hw_only['amount'].sum():>13,.0f}円 {hw_only['amount'].sum()/total_amount*100:>7.1f}%")
print(f"  {'有料媒体経由（有料のみ）':<25} {len(paid_only):>7,}件 {len(paid_only)/total_count*100:>7.1f}% {paid_only['amount'].sum():>13,.0f}円 {paid_only['amount'].sum()/total_amount*100:>7.1f}%")
print(f"  {'両方（HW+有料）':<25} {len(both):>7,}件 {len(both)/total_count*100:>7.1f}% {both['amount'].sum():>13,.0f}円 {both['amount'].sum()/total_amount*100:>7.1f}%")
print(f"  {'その他Lead':<25} {len(result_df[result_df['matched_source']=='その他/不明']):>7,}件")
print(f"  {'マッチなし':<25} {len(no_match):>7,}件 {len(no_match)/total_count*100:>7.1f}% {no_match['amount'].sum():>13,.0f}円 {no_match['amount'].sum()/total_amount*100:>7.1f}%")
print(f"  {'-'*70}")

# HW含む合計
hw_total = result_df[result_df["has_hw"] == True]
print(f"\n  HW関与あり合計: {len(hw_total):,}件 ({len(hw_total)/total_count*100:.1f}%) / {hw_total['amount'].sum():,.0f}円 ({hw_total['amount'].sum()/total_amount*100:.1f}%)")

# === 月別推移 ===
print("\n" + "=" * 70)
print("6. 月別: HW貢献率の推移（3段階マッチ後）")
print("=" * 70)

result_df["CloseDate"] = pd.to_datetime(result_df["close_date"], errors="coerce")
result_df["CloseMonth"] = result_df["CloseDate"].dt.to_period("M")

for month_str in ["2025-11", "2025-12", "2026-01", "2026-02"]:
    month_data = result_df[result_df["CloseMonth"] == month_str]
    if len(month_data) == 0:
        continue

    month_hw = month_data[month_data["has_hw"] == True]
    month_paid = month_data[(month_data["has_paid"] == True) & (month_data["has_hw"] == False)]
    month_none = month_data[month_data["matched_source"] == "不明"]
    month_total = len(month_data)
    month_amount = month_data["amount"].sum()

    print(f"\n  --- {month_str} --- (受注: {month_total}件 / {month_amount:,.0f}円)")
    print(f"    HW関与: {len(month_hw):,}件 ({len(month_hw)/month_total*100:.1f}%) / {month_hw['amount'].sum():,.0f}円 ({month_hw['amount'].sum()/month_amount*100:.1f}%)")
    print(f"    有料のみ: {len(month_paid):,}件 ({len(month_paid)/month_total*100:.1f}%) / {month_paid['amount'].sum():,.0f}円")
    print(f"    マッチなし: {len(month_none):,}件 ({len(month_none)/month_total*100:.1f}%)")

    # マッチ方法の内訳
    for method in ["ConvertedAccountId", "電話番号", "会社名", "マッチなし"]:
        m = month_data[month_data["match_method"] == method]
        if len(m) > 0:
            print(f"      [{method}]: {len(m)}件")

# === 再商談 × 媒体 ===
print("\n" + "=" * 70)
print("7. 再商談の媒体元（3段階マッチ後）")
print("=" * 70)

re_shoudan = result_df[result_df["opp_category"].fillna("").str.contains("再")]
new_shoudan = result_df[~result_df["opp_category"].fillna("").str.contains("再")]

print(f"\n  初回商談の受注:")
new_won = new_shoudan
new_hw = new_won[new_won["has_hw"] == True]
print(f"    全体: {len(new_won):,}件 / {new_won['amount'].sum():,.0f}円")
print(f"    HW関与: {len(new_hw):,}件 ({len(new_hw)/len(new_won)*100:.1f}%) / {new_hw['amount'].sum():,.0f}円")

print(f"\n  再商談の受注:")
re_won = re_shoudan
re_hw = re_won[re_won["has_hw"] == True]
print(f"    全体: {len(re_won):,}件 / {re_won['amount'].sum():,.0f}円")
print(f"    HW関与: {len(re_hw):,}件 ({len(re_hw)/len(re_won)*100:.1f}%) / {re_hw['amount'].sum():,.0f}円")

# === マッチなし案件の詳細 ===
print("\n" + "=" * 70)
print("8. マッチなし案件の分析")
print("=" * 70)

no_match_detail = result_df[result_df["matched_source"] == "不明"]
print(f"\n  マッチなし: {len(no_match_detail):,}件 / {no_match_detail['amount'].sum():,.0f}円")

# これらのAccountの特徴
no_match_accounts = no_match_detail["account_id"].unique()
no_match_account_info = accounts[accounts["Id"].isin(no_match_accounts)]

# Accountに電話番号があるか
has_phone = no_match_account_info["phone_norm"].apply(lambda x: x != "").sum()
no_phone = len(no_match_account_info) - has_phone

print(f"  うちAccountに電話番号あり: {has_phone:,}件")
print(f"  うちAccountに電話番号なし: {no_phone:,}件")

# 電話番号があるのにLeadにマッチしないケース
# → これらのAccountは「Lead投入されていない」真の新規 or Leadが別の電話番号だった
print(f"\n  → 電話番号ありだがLead未発見 = Lead投入前から存在した顧客（紹介等）の可能性")

# 代表的な案件名
print(f"\n  マッチなし受注のオーナー分布:")
owner_counts = no_match_detail["owner"].value_counts().head(10)
for owner, count in owner_counts.items():
    print(f"    {owner}: {count:,}件")

# === 最終サマリー ===
print("\n" + "=" * 70)
print("=" * 70)
print("最終サマリー: リスト施策（特にHW）の真の貢献率")
print("=" * 70)
print("=" * 70)

print(f"""
全受注: {total_count:,}件 / {total_amount:,.0f}円

■ 3段階マッチ結果
  1. ConvertedAccountId: {len(result_df[result_df['match_method']=='ConvertedAccountId']):,}件
  2. 電話番号マッチ:     {len(result_df[result_df['match_method']=='電話番号']):,}件 ← 新規発掘
  3. 会社名マッチ:       {len(result_df[result_df['match_method']=='会社名']):,}件 ← 新規発掘
  4. マッチなし:         {len(no_match_detail):,}件

■ ハローワーク関与の受注
  件数: {len(hw_total):,} / {total_count:,}件 = {len(hw_total)/total_count*100:.1f}%
  金額: {hw_total['amount'].sum():,.0f} / {total_amount:,.0f}円 = {hw_total['amount'].sum()/total_amount*100:.1f}%

■ 前回分析との比較
  前回（ConvertedAccountIdのみ）: 25.8%
  今回（3段階マッチ）: {len(now_media)/total_count*100:.1f}%
  → Lead投入の貢献が+{(len(now_media)-len(prev_media))/total_count*100:.1f}pt発掘された
""")

print("=" * 70)
print("分析完了")
print("=" * 70)
