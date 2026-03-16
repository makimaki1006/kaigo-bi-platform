"""
商談への媒体貢献分析
- 全Opportunityに対してAccountId経由でLeadの媒体情報を逆引き
- 再商談（OpportunityType）で媒体貢献が消える構造を可視化
- 「本来の媒体貢献率」を算出
"""

import pandas as pd
import numpy as np
import sys
import io
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data" / "output"
LEAD_FILE = DATA_DIR / "Lead_20260305_115825.csv"
OPP_FILE = DATA_DIR / "analysis" / "opportunities_detailed.csv"

print("=" * 70)
print("商談への媒体貢献分析（再商談の隠れた貢献を含む）")
print("=" * 70)

# === データ読み込み ===
leads = pd.read_csv(LEAD_FILE, encoding="utf-8-sig", low_memory=False)
opps = pd.read_csv(OPP_FILE, encoding="utf-8-sig", low_memory=False)
opps["Amount"] = pd.to_numeric(opps["Amount"], errors="coerce")

print(f"Lead: {len(leads):,}件 / Opportunity: {len(opps):,}件")

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

# 有料媒体詳細
leads["MediaDetail"] = ""
if "Paid_DataSource__c" in leads.columns:
    leads.loc[has_paid, "MediaDetail"] = leads.loc[has_paid, "Paid_DataSource__c"].fillna("不明")

# === AccountId別: そのAccountに紐づくLeadの媒体情報を集約 ===
print("\n" + "=" * 70)
print("1. AccountId → Lead媒体情報の逆引きマップ作成")
print("=" * 70)

# ConvertedAccountIdを持つLead
converted_leads = leads[leads["ConvertedAccountId"].notna()].copy()
print(f"  ConvertedAccountIdを持つLead: {len(converted_leads):,}件")

# AccountId別の媒体情報を集約（1つのAccountに複数Leadがある場合あり）
def aggregate_sources(group):
    sources = set(group["ListSource"].unique())
    media_details = set(group["MediaDetail"].dropna().unique()) - {"", "不明"}
    # 最初にコンバートされたLeadの日付
    dates = pd.to_datetime(group["ConvertedDate"], errors="coerce")
    first_convert = dates.min() if dates.notna().any() else pd.NaT
    return pd.Series({
        "lead_count": len(group),
        "sources": "|".join(sorted(sources)),
        "has_hw": "ハローワーク" in sources or "両方(HW+有料)" in sources,
        "has_paid": "有料媒体" in sources or "両方(HW+有料)" in sources,
        "has_other": "その他/不明" in sources,
        "media_details": "|".join(sorted(media_details)) if media_details else "",
        "primary_source": group["ListSource"].mode().iloc[0] if len(group) > 0 else "不明",
        "first_convert_date": first_convert,
    })

account_media = converted_leads.groupby("ConvertedAccountId").apply(aggregate_sources).reset_index()
account_media.rename(columns={"ConvertedAccountId": "AccountId"}, inplace=True)

print(f"  ユニークAccountId: {len(account_media):,}件")

# Opportunityと結合
opps_with_media = opps.merge(account_media, on="AccountId", how="left")

# 媒体情報が見つかったか
has_media = opps_with_media["primary_source"].notna()
print(f"\n  Opportunity全体: {len(opps):,}件")
print(f"  うち媒体情報あり（Lead経由）: {has_media.sum():,}件 ({has_media.sum()/len(opps)*100:.1f}%)")
print(f"  媒体情報なし: {(~has_media).sum():,}件 ({(~has_media).sum()/len(opps)*100:.1f}%)")

# === 2. OpportunityType（新規 vs 再商談）× 媒体 ===
print("\n" + "=" * 70)
print("2. 商談タイプ × 媒体貢献")
print("=" * 70)

if "OpportunityType__c" in opps_with_media.columns:
    print("\n  OpportunityType__c の分布:")
    type_counts = opps_with_media["OpportunityType__c"].fillna("(空)").value_counts()
    for t, c in type_counts.items():
        pct = c / len(opps_with_media) * 100
        print(f"    {t}: {c:,}件 ({pct:.1f}%)")

if "OpportunityCategory__c" in opps_with_media.columns:
    print("\n  OpportunityCategory__c の分布:")
    cat_counts = opps_with_media["OpportunityCategory__c"].fillna("(空)").value_counts()
    for t, c in cat_counts.items():
        pct = c / len(opps_with_media) * 100
        print(f"    {t}: {c:,}件 ({pct:.1f}%)")

# === 3. 全商談の媒体貢献クロス集計 ===
print("\n" + "=" * 70)
print("3. 全商談: 媒体有無 × 商談タイプ")
print("=" * 70)

# 「媒体経由」フラグ
opps_with_media["media_flag"] = "媒体なし"
opps_with_media.loc[opps_with_media["has_hw"] == True, "media_flag"] = "HW経由"
opps_with_media.loc[opps_with_media["has_paid"] == True, "media_flag"] = "有料媒体経由"
opps_with_media.loc[(opps_with_media["has_hw"] == True) & (opps_with_media["has_paid"] == True), "media_flag"] = "両方経由"
opps_with_media.loc[opps_with_media["primary_source"].isna(), "media_flag"] = "Lead紐づけなし"

opp_type_col = "OpportunityCategory__c" if "OpportunityCategory__c" in opps_with_media.columns else "OpportunityType__c"

print(f"\n  クロス集計: media_flag × {opp_type_col}")
cross = pd.crosstab(
    opps_with_media["media_flag"],
    opps_with_media[opp_type_col].fillna("(空)"),
    margins=True,
)
print(cross.to_string())

# === 4. 受注に絞った媒体貢献 ===
print("\n" + "=" * 70)
print("4. 受注商談の媒体貢献")
print("=" * 70)

won = opps_with_media[opps_with_media["IsWon"] == True].copy()
print(f"\n  受注商談: {len(won):,}件 / 合計金額: {won['Amount'].sum():,.0f}円")

print(f"\n  受注 × 媒体フラグ:")
for flag in ["HW経由", "有料媒体経由", "両方経由", "媒体なし", "Lead紐づけなし"]:
    subset = won[won["media_flag"] == flag]
    count = len(subset)
    amount = subset["Amount"].sum()
    pct_count = count / len(won) * 100 if len(won) > 0 else 0
    pct_amount = amount / won["Amount"].sum() * 100 if won["Amount"].sum() > 0 else 0
    print(f"    {flag}: {count:,}件 ({pct_count:.1f}%) / {amount:,.0f}円 ({pct_amount:.1f}%)")

# === 5. 再商談の媒体貢献（隠れた貢献） ===
print("\n" + "=" * 70)
print("5. 再商談の隠れた媒体貢献")
print("=" * 70)

# 再商談 = OpportunityCategory__cが「再商談」
if opp_type_col in opps_with_media.columns:
    reshoudan = opps_with_media[opps_with_media[opp_type_col].fillna("").str.contains("再")]
    new_shoudan = opps_with_media[~opps_with_media[opp_type_col].fillna("").str.contains("再")]

    print(f"\n  新規商談: {len(new_shoudan):,}件")
    print(f"  再商談: {len(reshoudan):,}件")

    # 再商談のうち、元々媒体経由でAccountが作られたもの
    reshoudan_with_media = reshoudan[reshoudan["media_flag"].isin(["HW経由", "有料媒体経由", "両方経由"])]
    print(f"\n  再商談のうち、元Lead媒体情報あり: {len(reshoudan_with_media):,}件 ({len(reshoudan_with_media)/len(reshoudan)*100:.1f}%)")

    # 再商談×受注
    reshoudan_won = reshoudan[reshoudan["IsWon"] == True]
    reshoudan_won_media = reshoudan_won[reshoudan_won["media_flag"].isin(["HW経由", "有料媒体経由", "両方経由"])]

    print(f"\n  再商談の受注: {len(reshoudan_won):,}件 / {reshoudan_won['Amount'].sum():,.0f}円")
    print(f"  うち媒体経由: {len(reshoudan_won_media):,}件 / {reshoudan_won_media['Amount'].sum():,.0f}円")

    # 新規商談の受注
    new_won = new_shoudan[new_shoudan["IsWon"] == True]
    new_won_media = new_won[new_won["media_flag"].isin(["HW経由", "有料媒体経由", "両方経由"])]

    print(f"\n  新規商談の受注: {len(new_won):,}件 / {new_won['Amount'].sum():,.0f}円")
    print(f"  うち媒体経由: {len(new_won_media):,}件 / {new_won_media['Amount'].sum():,.0f}円")

# === 6. 2月分析の再計算（媒体貢献込み） ===
print("\n" + "=" * 70)
print("6. 2月分析の再計算（媒体貢献込み）")
print("=" * 70)

opps_with_media["CloseDate"] = pd.to_datetime(opps_with_media["CloseDate"], errors="coerce")
opps_with_media["CloseMonth"] = opps_with_media["CloseDate"].dt.to_period("M")

# 新規営業チームのみ（前回分析と同じフィルタ）
exclude_teams = ["マーケ", "エンタープライズ", "プロダクト", "MEO", "セールス部", "CyXen", "マーケ兼AI"]

def is_new_sales_team(unit_str):
    if pd.isna(unit_str) or str(unit_str) == "":
        return False
    for exc in exclude_teams:
        if exc in str(unit_str):
            return False
    return True

opps_with_media["is_new_sales"] = opps_with_media["AppointUnit__c"].apply(is_new_sales_team)
new_sales = opps_with_media[opps_with_media["is_new_sales"]].copy()

for month_str in ["2025-11", "2025-12", "2026-01", "2026-02"]:
    month_data = new_sales[new_sales["CloseMonth"] == month_str]
    month_won = month_data[month_data["IsWon"] == True]

    if len(month_won) == 0:
        continue

    print(f"\n  --- {month_str} ---")
    print(f"  受注: {len(month_won):,}件 / {month_won['Amount'].sum():,.0f}円")

    for flag in ["HW経由", "有料媒体経由", "両方経由", "媒体なし", "Lead紐づけなし"]:
        subset = month_won[month_won["media_flag"] == flag]
        count = len(subset)
        amount = subset["Amount"].sum()
        if count > 0:
            print(f"    {flag}: {count:,}件 ({count/len(month_won)*100:.1f}%) / {amount:,.0f}円")

# === 7. 有料媒体の詳細内訳（受注商談） ===
print("\n" + "=" * 70)
print("7. 有料媒体の詳細内訳（受注商談のAccount元Lead）")
print("=" * 70)

won_with_details = won[won["media_details"].notna() & (won["media_details"] != "")].copy()
if len(won_with_details) > 0:
    # media_detailsを展開
    all_details = []
    for _, row in won_with_details.iterrows():
        for detail in str(row["media_details"]).split("|"):
            if detail:
                all_details.append({
                    "media": detail,
                    "amount": row["Amount"],
                    "opp_type": row.get(opp_type_col, ""),
                })

    detail_df = pd.DataFrame(all_details)
    if len(detail_df) > 0:
        summary = detail_df.groupby("media").agg(
            count=("amount", "size"),
            total_amount=("amount", "sum"),
        ).sort_values("total_amount", ascending=False)

        print(f"\n  {'媒体':<20} {'受注件数':>8} {'受注金額':>14}")
        print("-" * 50)
        for media, row in summary.iterrows():
            print(f"  {media:<20} {row['count']:>8,}件 {row['total_amount']:>13,.0f}円")

# === 8. 再商談×代表者 の媒体元 ===
print("\n" + "=" * 70)
print("8. 再商談×代表者（2月+11件）の媒体元トレース")
print("=" * 70)

if "BusinessNegotiatorRole__c" in opps_with_media.columns:
    feb_data = new_sales[new_sales["CloseMonth"] == "2026-02"]
    feb_won = feb_data[feb_data["IsWon"] == True]

    # 再商談×代表者
    re_daihyou = feb_won[
        (feb_won[opp_type_col].fillna("").str.contains("再")) &
        (feb_won["BusinessNegotiatorRole__c"].fillna("").str.contains("代表|社長|理事長|院長|園長|オーナー", regex=True))
    ]

    print(f"\n  2月の再商談×代表者: {len(re_daihyou):,}件")

    if len(re_daihyou) > 0:
        for flag in ["HW経由", "有料媒体経由", "両方経由", "媒体なし", "Lead紐づけなし"]:
            subset = re_daihyou[re_daihyou["media_flag"] == flag]
            if len(subset) > 0:
                print(f"    {flag}: {len(subset):,}件 / {subset['Amount'].sum():,.0f}円")

        # 詳細
        print(f"\n  詳細:")
        for _, row in re_daihyou.iterrows():
            account = row.get("Account.Name", "?")
            amount = row.get("Amount", 0)
            media = row.get("media_flag", "?")
            sources = row.get("sources", "")
            details = row.get("media_details", "")
            print(f"    {account}: {amount:,.0f}円 [{media}] 元Lead施策={sources} 媒体={details}")

# === 9. 総合: 本来の媒体貢献率 ===
print("\n" + "=" * 70)
print("9. 総合: 本来の媒体貢献率（再商談の隠れた貢献を含む）")
print("=" * 70)

total_won_count = len(won)
total_won_amount = won["Amount"].sum()

# 媒体経由（直接 + 再商談の隠れた貢献）
media_contributed = won[won["media_flag"].isin(["HW経由", "有料媒体経由", "両方経由"])]
media_count = len(media_contributed)
media_amount = media_contributed["Amount"].sum()

# 媒体なし（Leadはあるが媒体情報なし）
no_media = won[won["media_flag"] == "媒体なし"]

# Lead紐づけなし（そもそもLead経由でない）
no_lead = won[won["media_flag"] == "Lead紐づけなし"]

print(f"\n  全受注: {total_won_count:,}件 / {total_won_amount:,.0f}円")
print(f"\n  媒体経由（HW/有料/両方）: {media_count:,}件 ({media_count/total_won_count*100:.1f}%) / {media_amount:,.0f}円 ({media_amount/total_won_amount*100:.1f}%)")
print(f"  Lead紐づきあり・媒体なし: {len(no_media):,}件 ({len(no_media)/total_won_count*100:.1f}%) / {no_media['Amount'].sum():,.0f}円")
print(f"  Lead紐づけなし: {len(no_lead):,}件 ({len(no_lead)/total_won_count*100:.1f}%) / {no_lead['Amount'].sum():,.0f}円")

# 前回の0.34%との比較
print(f"\n  --- 前回分析との比較 ---")
print(f"  前回: リード貢献率 0.34%（Lead全体÷コンバート数の粗い計算）")
print(f"  今回: 受注ベース媒体貢献率 {media_count/total_won_count*100:.1f}%（AccountId逆引き、再商談含む）")
print(f"        金額ベース媒体貢献率 {media_amount/total_won_amount*100:.1f}%")

print("\n" + "=" * 70)
print("分析完了")
print("=" * 70)
