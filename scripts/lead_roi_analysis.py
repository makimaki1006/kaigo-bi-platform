"""
リード投資ROI分析スクリプト
- Lead → Opportunity の紐づけを ConvertedOpportunityId で追跡
- リスト施策別（ハローワーク / 有料媒体 / その他）の受注貢献を算出
- 正確な貢献率を計算（架電済み / 未架電を分離）
"""

import pandas as pd
import numpy as np
import sys
import io
from pathlib import Path
from datetime import datetime

# Windows cp932エンコーディング対策
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# === パス設定 ===
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data" / "output"
LEAD_FILE = DATA_DIR / "Lead_20260305_115825.csv"
OPP_FILE = DATA_DIR / "analysis" / "opportunities_detailed.csv"
OUTPUT_DIR = DATA_DIR / "analysis"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

print("=" * 70)
print("リード投資ROI分析")
print("=" * 70)

# === データ読み込み ===
print("\nデータ読み込み中...")
leads = pd.read_csv(LEAD_FILE, encoding="utf-8-sig", low_memory=False)
opps = pd.read_csv(OPP_FILE, encoding="utf-8-sig", low_memory=False)
print(f"  Lead: {len(leads):,}件")
print(f"  Opportunity: {len(opps):,}件")

# === 1. リスト施策の分類 ===
print("\n" + "=" * 70)
print("1. リスト施策別リード分類")
print("=" * 70)


def classify_lead_source(row):
    """リードのリスト施策を分類"""
    # ハローワーク系: Hellowork_* フィールドに値があるか
    hw_fields = [
        "Hellowork_Occupation__c",
        "Hellowork_DataImportDate__c",
        "Hellowork_URL__c",
        "Hellowork_JobPublicationDate__c",
    ]
    has_hw = any(pd.notna(row.get(f, np.nan)) and str(row.get(f, "")) != "" for f in hw_fields)

    # 有料媒体系: Paid_* フィールドに値があるか
    paid_fields = [
        "Paid_Media__c",
        "Paid_DataSource__c",
        "Paid_URL__c",
        "Paid_Memo__c",
    ]
    has_paid = any(pd.notna(row.get(f, np.nan)) and str(row.get(f, "")) != "" for f in paid_fields)

    if has_hw and has_paid:
        return "両方"
    elif has_hw:
        return "ハローワーク"
    elif has_paid:
        return "有料媒体"
    else:
        return "その他/不明"


# 高速化: ベクトル化で施策分類
print("  施策分類中...")
hw_cols = ["Hellowork_Occupation__c", "Hellowork_DataImportDate__c",
           "Hellowork_URL__c", "Hellowork_JobPublicationDate__c"]
paid_cols = ["Paid_Media__c", "Paid_DataSource__c", "Paid_URL__c", "Paid_Memo__c"]

# 各フィールドが値を持つかチェック
has_hw = leads[hw_cols].notna().any(axis=1) & (leads[hw_cols].astype(str) != "").any(axis=1)
has_paid = leads[paid_cols].notna().any(axis=1) & (leads[paid_cols].astype(str) != "").any(axis=1)

leads["ListSource"] = "その他/不明"
leads.loc[has_hw & has_paid, "ListSource"] = "両方"
leads.loc[has_hw & ~has_paid, "ListSource"] = "ハローワーク"
leads.loc[~has_hw & has_paid, "ListSource"] = "有料媒体"

# 有料媒体の詳細分類
leads["PaidMediaDetail"] = ""
paid_media_mask = leads["ListSource"].isin(["有料媒体", "両方"])
if "Paid_DataSource__c" in leads.columns:
    leads.loc[paid_media_mask, "PaidMediaDetail"] = leads.loc[paid_media_mask, "Paid_DataSource__c"].fillna("")

source_counts = leads["ListSource"].value_counts()
print("\n  リスト施策別リード数:")
for source, count in source_counts.items():
    pct = count / len(leads) * 100
    print(f"    {source}: {count:,}件 ({pct:.1f}%)")

# === 2. コンバート分析 ===
print("\n" + "=" * 70)
print("2. コンバート（Lead → Opportunity）追跡")
print("=" * 70)

# コンバート済みリード
converted = leads[leads["IsConverted"] == True].copy()
print(f"\n  コンバート済みリード: {len(converted):,}件 / {len(leads):,}件 ({len(converted)/len(leads)*100:.2f}%)")

# ConvertedOpportunityIdでOpportunityと結合
converted_with_opp = converted[converted["ConvertedOpportunityId"].notna()].copy()
print(f"  うちOpportunity紐づきあり: {len(converted_with_opp):,}件")

# Opportunityデータと結合
merged = converted_with_opp.merge(
    opps,
    left_on="ConvertedOpportunityId",
    right_on="Id",
    how="left",
    suffixes=("_lead", "_opp"),
)

print(f"  Opportunity詳細取得成功: {len(merged[merged['StageName'].notna()]):,}件")

# === 3. リスト施策別コンバート率 ===
print("\n" + "=" * 70)
print("3. リスト施策別コンバート率")
print("=" * 70)

for source in ["ハローワーク", "有料媒体", "両方", "その他/不明"]:
    source_leads = leads[leads["ListSource"] == source]
    source_converted = source_leads[source_leads["IsConverted"] == True]
    total = len(source_leads)
    conv = len(source_converted)
    rate = conv / total * 100 if total > 0 else 0
    print(f"\n  【{source}】")
    print(f"    リード数: {total:,}件")
    print(f"    コンバート: {conv:,}件 ({rate:.2f}%)")

# === 4. リスト施策別受注貢献 ===
print("\n" + "=" * 70)
print("4. リスト施策別受注貢献（ConvertedOpportunityId経由）")
print("=" * 70)

# 受注したOpportunity
won_opps = merged[merged["IsWon"] == True].copy()
print(f"\n  コンバート経由受注: {len(won_opps):,}件")

if len(won_opps) > 0:
    won_opps["Amount"] = pd.to_numeric(won_opps["Amount"], errors="coerce")

    for source in ["ハローワーク", "有料媒体", "両方", "その他/不明"]:
        source_won = won_opps[won_opps["ListSource"] == source]
        count = len(source_won)
        amount = source_won["Amount"].sum()
        print(f"\n  【{source}】")
        print(f"    受注件数: {count:,}件")
        print(f"    受注金額: {amount:,.0f}円")
        if count > 0:
            print(f"    平均単価: {amount/count:,.0f}円")

# === 5. 全体ファネル ===
print("\n" + "=" * 70)
print("5. 全体ファネル（リスト施策別）")
print("=" * 70)

print(f"\n{'施策':<12} {'リード':>10} {'コンバート':>10} {'CV率':>8} {'受注':>8} {'受注率':>8} {'受注金額':>14}")
print("-" * 80)

for source in ["ハローワーク", "有料媒体", "両方", "その他/不明"]:
    source_leads_df = leads[leads["ListSource"] == source]
    source_converted_df = source_leads_df[source_leads_df["IsConverted"] == True]
    source_won = won_opps[won_opps["ListSource"] == source] if len(won_opps) > 0 else pd.DataFrame()

    total = len(source_leads_df)
    conv = len(source_converted_df)
    won = len(source_won)
    amount = source_won["Amount"].sum() if len(source_won) > 0 else 0
    cv_rate = conv / total * 100 if total > 0 else 0
    won_rate = won / conv * 100 if conv > 0 else 0

    print(f"{source:<12} {total:>10,} {conv:>10,} {cv_rate:>7.2f}% {won:>7,} {won_rate:>7.1f}% {amount:>13,.0f}円")

# === 6. 架電済みベースの正確な貢献率 ===
print("\n" + "=" * 70)
print("6. 架電済みリードベースの貢献率")
print("=" * 70)

# Status__c でステータスを確認
if "Status" in leads.columns:
    status_counts = leads["Status"].value_counts()
    print("\n  Lead Status分布:")
    for status, count in status_counts.head(15).items():
        pct = count / len(leads) * 100
        print(f"    {status}: {count:,}件 ({pct:.1f}%)")

    # 架電済み = Status が「新規」「未対応」以外と推定
    # 実際のステータス値を確認して判定
    untouched_statuses = ["新規", "未対応", "Open - Not Contacted"]
    contacted = leads[~leads["Status"].isin(untouched_statuses)]
    print(f"\n  架電/接触済みリード（Status≠新規/未対応）: {contacted:,}件" if isinstance(contacted, int) else "")
    print(f"  架電/接触済みリード: {len(contacted):,}件 / {len(leads):,}件 ({len(contacted)/len(leads)*100:.1f}%)")

    # 架電済みベースのコンバート率
    contacted_converted = contacted[contacted["IsConverted"] == True]
    print(f"  うちコンバート: {len(contacted_converted):,}件 ({len(contacted_converted)/len(contacted)*100:.2f}%)")

    # 施策別
    print(f"\n  架電済みベースの施策別コンバート率:")
    for source in ["ハローワーク", "有料媒体", "両方", "その他/不明"]:
        source_contacted = contacted[contacted["ListSource"] == source]
        source_conv = source_contacted[source_contacted["IsConverted"] == True]
        total = len(source_contacted)
        conv = len(source_conv)
        rate = conv / total * 100 if total > 0 else 0
        print(f"    {source}: {conv:,}/{total:,} ({rate:.2f}%)")

# === 7. 有料媒体の詳細内訳 ===
print("\n" + "=" * 70)
print("7. 有料媒体 詳細内訳（Paid_DataSource__c別）")
print("=" * 70)

paid_leads = leads[leads["ListSource"].isin(["有料媒体", "両方"])]
if "Paid_DataSource__c" in paid_leads.columns:
    paid_detail = paid_leads["Paid_DataSource__c"].fillna("不明").value_counts()
    print("\n  媒体別リード数:")
    for media, count in paid_detail.items():
        pct = count / len(paid_leads) * 100
        # コンバート数
        conv = paid_leads[(paid_leads["Paid_DataSource__c"].fillna("") == media) &
                          (paid_leads["IsConverted"] == True)]
        print(f"    {media}: {count:,}件 (CV: {len(conv):,}件)")

# === 8. 月別トレンド ===
print("\n" + "=" * 70)
print("8. 月別コンバートトレンド（リスト施策別）")
print("=" * 70)

if "ConvertedDate" in leads.columns:
    converted_df = leads[leads["IsConverted"] == True].copy()
    converted_df["ConvertedDate"] = pd.to_datetime(converted_df["ConvertedDate"], errors="coerce")
    converted_df["ConvertedMonth"] = converted_df["ConvertedDate"].dt.to_period("M")

    monthly = converted_df.groupby(["ConvertedMonth", "ListSource"]).size().unstack(fill_value=0)
    print("\n  月別コンバート件数:")
    print(monthly.to_string())

# === 9. AccountId経由の間接紐づけ ===
print("\n" + "=" * 70)
print("9. AccountId経由の間接紐づけ（ConvertedAccountId → Opportunity.AccountId）")
print("=" * 70)

# ConvertedOpportunityIdがないが、ConvertedAccountIdがあるケース
# そのAccountIdに紐づくOpportunityを探す
converted_account_only = converted[
    converted["ConvertedOpportunityId"].isna() &
    converted["ConvertedAccountId"].notna()
].copy()

print(f"\n  ConvertedOpportunityIdなし but ConvertedAccountIdあり: {len(converted_account_only):,}件")

if len(converted_account_only) > 0 and "AccountId" in opps.columns:
    # AccountId経由でOpportunityを探す
    account_ids = set(converted_account_only["ConvertedAccountId"].dropna().unique())
    indirect_opps = opps[opps["AccountId"].isin(account_ids)].copy()
    indirect_won = indirect_opps[indirect_opps["IsWon"] == True]

    print(f"  → このAccountIdに紐づくOpportunity: {len(indirect_opps):,}件")
    print(f"  → うち受注: {len(indirect_won):,}件")

    if len(indirect_won) > 0:
        indirect_won["Amount"] = pd.to_numeric(indirect_won["Amount"], errors="coerce")
        total_amount = indirect_won["Amount"].sum()
        print(f"  → 受注金額合計: {total_amount:,.0f}円")

        # 施策別
        # AccountId → LeadのListSourceを逆引き
        account_source_map = converted_account_only.groupby("ConvertedAccountId")["ListSource"].first().to_dict()
        indirect_won["ListSource_indirect"] = indirect_won["AccountId"].map(account_source_map)

        for source in ["ハローワーク", "有料媒体", "両方", "その他/不明"]:
            sw = indirect_won[indirect_won["ListSource_indirect"] == source]
            if len(sw) > 0:
                print(f"\n    【{source}】間接受注: {len(sw):,}件 / {sw['Amount'].sum():,.0f}円")

# === 10. 総合サマリー ===
print("\n" + "=" * 70)
print("10. 総合サマリー: リスト施策のROI")
print("=" * 70)

# 直接（ConvertedOpportunityId）+ 間接（ConvertedAccountId経由）の合算
print("\n  ■ 直接紐づけ（ConvertedOpportunityId経由）")
if len(won_opps) > 0:
    direct_total = won_opps["Amount"].sum()
    print(f"    受注件数: {len(won_opps):,}件")
    print(f"    受注金額: {direct_total:,.0f}円")

print("\n  ■ 間接紐づけ（ConvertedAccountId経由）")
if len(converted_account_only) > 0 and "AccountId" in opps.columns:
    print(f"    受注件数: {len(indirect_won):,}件")
    if len(indirect_won) > 0:
        indirect_total = indirect_won["Amount"].sum()
        print(f"    受注金額: {indirect_total:,.0f}円")

# Opportunity全体の受注
all_won = opps[opps["IsWon"] == True].copy()
all_won["Amount"] = pd.to_numeric(all_won["Amount"], errors="coerce")
total_won_amount = all_won["Amount"].sum()
total_won_count = len(all_won)

print(f"\n  ■ Opportunity全体の受注")
print(f"    受注件数: {total_won_count:,}件")
print(f"    受注金額: {total_won_amount:,.0f}円")

# リスト施策経由の貢献割合
direct_count = len(won_opps) if len(won_opps) > 0 else 0
direct_amount = won_opps["Amount"].sum() if len(won_opps) > 0 else 0

print(f"\n  ■ リスト施策の受注貢献（直接紐づけベース）")
print(f"    件数ベース: {direct_count:,}/{total_won_count:,} ({direct_count/total_won_count*100:.1f}%)")
print(f"    金額ベース: {direct_amount:,.0f}/{total_won_amount:,.0f}円 ({direct_amount/total_won_amount*100:.1f}%)")

print("\n" + "=" * 70)
print("分析完了")
print("=" * 70)
