"""
1月受注低調の根幹原因: HW更新遅延 vs 季節性を分離する

仮説検証アプローチ:
1. 1月に受注した27件 vs しなかった案件のHWデータ有無を比較
2. 代表者商談の減少はHWデータと相関するか
3. 1月に受注0だった4名の営業はHW依存度が高かったか
4. 12月駆け込みで1月パイプラインが空洞化した定量的証拠
5. Leadの「最終更新日」とHW更新時期の突合
6. 前年（があれば）との比較
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
accounts = pd.read_csv(DATA_DIR / "Account_20260305_115035.csv", encoding="utf-8-sig", low_memory=False)
opps["Amount"] = pd.to_numeric(opps["Amount"], errors="coerce")
opps["CloseDate_dt"] = pd.to_datetime(opps["CloseDate"], errors="coerce")
opps["CreatedDate_dt"] = pd.to_datetime(opps["CreatedDate"], errors="coerce")
opps["close_month"] = opps["CloseDate_dt"].dt.to_period("M")

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

# Lead電話番号正規化
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

# Lead会社名マップ
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

# Account全電話列
account_phone_cols = ["Phone", "PersonMobilePhone", "Phone2__c",
                      "PhoneChecker1__c", "PhoneChecker2__c",
                      "GooglePhoneSearch__c", "Phone2__pc",
                      "MobilePhone2__pc", "Account_Phone__pc",
                      "GooglePhoneSerach__pc", "PhoneChecker1__pc",
                      "PhoneChecker2__pc", "MobilePhoneChecker2__pc",
                      "MobilePhoneChecker1__pc"]
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

converted_account_ids = set(leads[leads["ConvertedAccountId"].notna()]["ConvertedAccountId"].unique())

# === 全受注に対してHWデータ有無を判定 ===
won = opps[opps["IsWon"] == True].copy()
won = won.merge(
    accounts[["Id", "Name"]],
    left_on="AccountId", right_on="Id", how="left", suffixes=("", "_acc")
)

def match_lead(row):
    acc_id = row["AccountId"]
    acc_name = str(row.get("Name_acc", "") or "")
    if acc_name == "nan":
        acc_name = ""

    hw = False
    paid = False
    method = "マッチなし"

    # Step1
    if acc_id in converted_account_ids:
        method = "ConvertedAccountId"
        matched = leads[leads["ConvertedAccountId"] == acc_id]
        srcs = set(matched["src"])
        hw = any(s in srcs for s in ["HW", "両方"])
        paid = any(s in srcs for s in ["有料媒体", "両方"])
        return method, hw, paid

    # Step2: 電話番号
    all_phones = account_phones.get(acc_id, set())
    for p in all_phones:
        if p in phone_map:
            method = "電話番号"
            hw = phone_map[p]["hw"]
            paid = phone_map[p]["paid"]
            return method, hw, paid

    # Step3: 会社名完全一致
    cn = norm_company(acc_name)
    if cn and cn in company_map:
        method = "会社名完全"
        hw = company_map[cn]["hw"]
        paid = company_map[cn]["paid"]
        return method, hw, paid

    # Step4: 部分一致
    if cn and len(cn) >= 5:
        matched_leads = leads[leads["_cn"].apply(
            lambda x: (len(x) >= 5 and cn in x) or (len(cn) >= 5 and x in cn and len(x) >= 5)
            if x else False
        )]
        if len(matched_leads) > 0:
            method = "部分一致"
            srcs = set(matched_leads["src"])
            hw = any(s in srcs for s in ["HW", "両方"])
            paid = any(s in srcs for s in ["有料媒体", "両方"])
            return method, hw, paid

    return method, hw, paid

print("マッチング中...")
match_results = won.apply(match_lead, axis=1, result_type="expand")
won["match_method"] = match_results[0]
won["hw"] = match_results[1]
won["paid"] = match_results[2]
won["has_lead"] = won["match_method"] != "マッチなし"

print("=" * 80)
print("1月受注低調の根幹原因: HW更新遅延 vs 季節性の分離")
print("=" * 80)

# ======================================================================
# 検証1: 月別のHWデータ有無別受注件数
# ======================================================================
print("\n" + "=" * 80)
print("検証1: 月別 × HWデータ有無別 受注件数")
print("=" * 80)

print(f"\n  {'月':>8} {'HW有':>6} {'HW無':>6} {'有料有':>6} {'Lead不在':>8} {'計':>4} {'HW有率':>8} {'Lead有率':>8}")
print(f"  {'-'*65}")

for m in ["2025-10", "2025-11", "2025-12", "2026-01", "2026-02"]:
    md = won[won["close_month"] == m]
    hw_yes = len(md[md["hw"] == True])
    hw_no = len(md[(md["hw"] == False) & (md["has_lead"] == True)])
    paid_yes = len(md[md["paid"] == True])
    no_lead = len(md[md["has_lead"] == False])
    total = len(md)
    hw_rate = hw_yes / total * 100 if total > 0 else 0
    lead_rate = len(md[md["has_lead"] == True]) / total * 100 if total > 0 else 0
    print(f"  {m:>8} {hw_yes:>5}件 {hw_no:>5}件 {paid_yes:>5}件 {no_lead:>7}件 {total:>3}件 {hw_rate:>7.1f}% {lead_rate:>7.1f}%")

# ======================================================================
# 検証2: HW有受注の減少率 vs HW無受注の減少率
# ======================================================================
print("\n" + "=" * 80)
print("検証2: HW有 vs HW無の減少率比較（11-12月平均 → 1月）")
print("=" * 80)

for label, cond in [("HW有", won["hw"] == True),
                     ("HW無+Lead有", (won["hw"] == False) & (won["has_lead"] == True)),
                     ("Lead不在", won["has_lead"] == False)]:
    subset = won[cond]
    avg_11_12 = (len(subset[subset["close_month"] == "2025-11"]) + len(subset[subset["close_month"] == "2025-12"])) / 2
    jan = len(subset[subset["close_month"] == "2026-01"])
    feb = len(subset[subset["close_month"] == "2026-02"])
    decline = (avg_11_12 - jan) / avg_11_12 * 100 if avg_11_12 > 0 else 0
    recovery = (feb - jan) / jan * 100 if jan > 0 else 0
    print(f"  {label}:")
    print(f"    11月: {len(subset[subset['close_month'] == '2025-11'])}件 / 12月: {len(subset[subset['close_month'] == '2025-12'])}件 / 平均: {avg_11_12:.1f}件")
    print(f"    1月: {jan}件 → 減少率: {decline:.1f}%")
    print(f"    2月: {feb}件 → 回復率: {recovery:.1f}%")

# ======================================================================
# 検証3: 代表者商談とHWデータの相関
# ======================================================================
print("\n" + "=" * 80)
print("検証3: 代表者商談 × HWデータの有無")
print("=" * 80)

won["is_daihyo"] = won["OpportunityType__c"].fillna("").str.contains("代表者")

for m in ["2025-11", "2025-12", "2026-01", "2026-02"]:
    md = won[won["close_month"] == m]
    daihyo = md[md["is_daihyo"] == True]
    tanto = md[~md["is_daihyo"]]

    daihyo_hw = len(daihyo[daihyo["hw"] == True])
    tanto_hw = len(tanto[tanto["hw"] == True])

    print(f"\n  {m}:")
    print(f"    代表者商談: {len(daihyo)}件 (うちHW有: {daihyo_hw}件 = {daihyo_hw/len(daihyo)*100:.0f}%)" if len(daihyo) > 0 else f"    代表者商談: 0件")
    print(f"    担当者商談: {len(tanto)}件 (うちHW有: {tanto_hw}件 = {tanto_hw/len(tanto)*100:.0f}%)" if len(tanto) > 0 else f"    担当者商談: 0件")

# ======================================================================
# 検証4: 1月受注0の4名のHW依存度
# ======================================================================
print("\n" + "=" * 80)
print("検証4: 1月受注0の主要4名のHW依存度")
print("=" * 80)

zero_jan_owners = ["久保 潤竜", "志村 亮介", "小松 涼太", "清飛羅 直樹"]
other_owners = [o for o in won["Owner.Name"].unique() if o not in zero_jan_owners]

for label, owners in [("1月0件の4名", zero_jan_owners), ("その他の担当者", other_owners)]:
    subset = won[won["Owner.Name"].isin(owners)]
    hw_yes = len(subset[subset["hw"] == True])
    total = len(subset)
    hw_rate = hw_yes / total * 100 if total > 0 else 0

    print(f"\n  {label}: 全期間 {total}件 (HW有: {hw_yes}件 = {hw_rate:.1f}%)")

    for m in ["2025-11", "2025-12", "2026-01", "2026-02"]:
        md = subset[subset["close_month"] == m]
        md_hw = len(md[md["hw"] == True])
        print(f"    {m}: {len(md)}件 (HW有: {md_hw}件)")

# ======================================================================
# 検証5: 12月駆け込みの定量化 - 月末集中度
# ======================================================================
print("\n" + "=" * 80)
print("検証5: 月末集中度（各月のクローズ日の分布）")
print("=" * 80)

for m_str, m_start, m_end in [
    ("2025-11", "2025-11-01", "2025-11-30"),
    ("2025-12", "2025-12-01", "2025-12-31"),
    ("2026-01", "2026-01-01", "2026-01-31"),
    ("2026-02", "2026-02-01", "2026-02-28"),
]:
    md = won[won["close_month"] == m_str]
    if len(md) == 0:
        continue
    first_half = md[md["CloseDate_dt"].dt.day <= 15]
    second_half = md[md["CloseDate_dt"].dt.day > 15]
    last_week = md[md["CloseDate_dt"].dt.day >= 22]

    print(f"  {m_str}: 全{len(md)}件 / 前半(1-15日) {len(first_half)}件 ({len(first_half)/len(md)*100:.0f}%) / 後半(16-末) {len(second_half)}件 ({len(second_half)/len(md)*100:.0f}%) / 最終週(22-末) {len(last_week)}件 ({len(last_week)/len(md)*100:.0f}%)")

# ======================================================================
# 検証6: 全商談（受注+失注）のHW有無別勝率
# ======================================================================
print("\n" + "=" * 80)
print("検証6: 全商談の勝率をHW有無で分けて月別比較")
print("=" * 80)

# 全クローズ商談にLeadマッチを適用（重い処理だが核心なので実行）
all_closed = opps[opps["IsClosed"] == True].copy()
all_closed = all_closed.merge(
    accounts[["Id", "Name"]],
    left_on="AccountId", right_on="Id", how="left", suffixes=("", "_acc")
)

# 全商談へのマッチはコスト大なので、AccountIdベースの簡易マッチ
# ConvertedAccountId → HWフラグ
acct_hw_flag = {}
for _, r in leads[leads["ConvertedAccountId"].notna()].iterrows():
    aid = r["ConvertedAccountId"]
    if aid not in acct_hw_flag:
        acct_hw_flag[aid] = {"hw": False, "paid": False}
    if r["src"] in ["HW", "両方"]:
        acct_hw_flag[aid]["hw"] = True
    if r["src"] in ["有料媒体", "両方"]:
        acct_hw_flag[aid]["paid"] = True

all_closed["hw_lead"] = all_closed["AccountId"].apply(
    lambda x: acct_hw_flag.get(x, {}).get("hw", False) if pd.notna(x) else False
)
all_closed["has_any_lead"] = all_closed["AccountId"].apply(
    lambda x: x in acct_hw_flag if pd.notna(x) else False
)

print(f"\n  ConvertedAccountId経由のみの簡易マッチ（全商談）")
print(f"\n  {'月':>8} {'HW有勝率':>10} {'HW無勝率':>10} {'Lead有勝率':>10} {'Lead無勝率':>10}")
print(f"  {'-'*55}")

for m in ["2025-11", "2025-12", "2026-01", "2026-02"]:
    md = all_closed[all_closed["close_month"] == m]

    hw_deals = md[md["hw_lead"] == True]
    no_hw_deals = md[(md["hw_lead"] == False) & (md["has_any_lead"] == True)]
    lead_deals = md[md["has_any_lead"] == True]
    no_lead_deals = md[md["has_any_lead"] == False]

    hw_wr = len(hw_deals[hw_deals["IsWon"] == True]) / len(hw_deals) * 100 if len(hw_deals) > 0 else 0
    no_hw_wr = len(no_hw_deals[no_hw_deals["IsWon"] == True]) / len(no_hw_deals) * 100 if len(no_hw_deals) > 0 else 0
    lead_wr = len(lead_deals[lead_deals["IsWon"] == True]) / len(lead_deals) * 100 if len(lead_deals) > 0 else 0
    no_lead_wr = len(no_lead_deals[no_lead_deals["IsWon"] == True]) / len(no_lead_deals) * 100 if len(no_lead_deals) > 0 else 0

    print(f"  {m:>8} {hw_wr:>9.1f}% {no_hw_wr:>9.1f}% {lead_wr:>9.1f}% {no_lead_wr:>9.1f}%")

# ======================================================================
# 検証7: HW更新日の時系列（いつ更新が止まったか）
# ======================================================================
print("\n" + "=" * 80)
print("検証7: HW更新日（Hellowork_DataImportDate__c）の分布")
print("=" * 80)

if "Hellowork_DataImportDate__c" in leads.columns:
    leads["hw_import_dt"] = pd.to_datetime(leads["Hellowork_DataImportDate__c"], errors="coerce")
    hw_leads = leads[leads["hw_import_dt"].notna()].copy()
    hw_leads["hw_import_month"] = hw_leads["hw_import_dt"].dt.to_period("M")

    print(f"\n  HWデータインポート月別件数:")
    for m, cnt in hw_leads["hw_import_month"].value_counts().sort_index().tail(8).items():
        print(f"    {m}: {cnt:,}件")

    # 最終インポート日
    print(f"\n  最終インポート日: {hw_leads['hw_import_dt'].max()}")
    print(f"  最初のインポート日: {hw_leads['hw_import_dt'].min()}")

# ======================================================================
# 検証8: コンバート（アポ化）のタイミング
# ======================================================================
print("\n" + "=" * 80)
print("検証8: 月別コンバート（アポ化）件数とHW有無")
print("=" * 80)

converted = leads[leads["IsConverted"] == True].copy()
converted["ConvertedDate_dt"] = pd.to_datetime(converted["ConvertedDate"], errors="coerce")
converted["cv_month"] = converted["ConvertedDate_dt"].dt.to_period("M")

print(f"\n  {'月':>8} {'CV全体':>8} {'HW有CV':>8} {'有料有CV':>8} {'その他CV':>8} {'HW率':>8}")
print(f"  {'-'*55}")

for m in ["2025-10", "2025-11", "2025-12", "2026-01", "2026-02"]:
    md = converted[converted["cv_month"] == m]
    hw_cv = len(md[md["src"].isin(["HW", "両方"])])
    paid_cv = len(md[md["src"].isin(["有料媒体", "両方"])])
    other_cv = len(md[md["src"] == "その他"])
    total = len(md)
    hw_rate = hw_cv / total * 100 if total > 0 else 0
    print(f"  {m:>8} {total:>7}件 {hw_cv:>7}件 {paid_cv:>7}件 {other_cv:>7}件 {hw_rate:>7.1f}%")

# ======================================================================
# 検証9: 1月の商談作成→受注のリードタイム分析
# ======================================================================
print("\n" + "=" * 80)
print("検証9: 受注商談の作成月分布（いつ作った商談が受注になったか）")
print("=" * 80)

won["created_month"] = won["CreatedDate_dt"].dt.tz_localize(None).dt.to_period("M")

for close_m in ["2025-11", "2025-12", "2026-01", "2026-02"]:
    md = won[won["close_month"] == close_m]
    print(f"\n  {close_m}月受注 ({len(md)}件) の商談作成月:")
    for cm, cnt in md["created_month"].value_counts().sort_index().items():
        print(f"    {cm}: {cnt}件")

# ======================================================================
# 検証10: 再商談（過去客へのアプローチ）のソース分析
# ======================================================================
print("\n" + "=" * 80)
print("検証10: 再商談のHWデータ有無 × 月別")
print("=" * 80)

won["is_re"] = won["OpportunityCategory__c"].fillna("").str.contains("再")

for m in ["2025-11", "2025-12", "2026-01", "2026-02"]:
    md = won[won["close_month"] == m]
    re_opp = md[md["is_re"] == True]
    init_opp = md[md["is_re"] == False]

    if len(re_opp) > 0:
        re_hw = len(re_opp[re_opp["hw"] == True])
        re_total = len(re_opp)
        print(f"  {m} 再商談: {re_total}件 (HW有: {re_hw}件 = {re_hw/re_total*100:.0f}%)")
    else:
        print(f"  {m} 再商談: 0件")

    if len(init_opp) > 0:
        init_hw = len(init_opp[init_opp["hw"] == True])
        init_total = len(init_opp)
        print(f"  {m} 初回: {init_total}件 (HW有: {init_hw}件 = {init_hw/init_total*100:.0f}%)")

# ======================================================================
# 検証11: 「失注の質」の変化
# ======================================================================
print("\n" + "=" * 80)
print("検証11: 失注シーン（LostScene__c）の月別変化")
print("=" * 80)

lost = opps[(opps["IsClosed"] == True) & (opps["IsWon"] != True)].copy()

for m in ["2025-11", "2025-12", "2026-01", "2026-02"]:
    m_lost = lost[lost["close_month"] == m]
    if len(m_lost) == 0:
        continue
    print(f"\n  --- {m} (失注 {len(m_lost)}件) ---")
    for scene, cnt in m_lost["LostScene__c"].fillna("(空)").value_counts().head(5).items():
        print(f"    {scene}: {cnt}件 ({cnt/len(m_lost)*100:.1f}%)")

# ======================================================================
# 検証12: 受注ランク分布の月別変化
# ======================================================================
print("\n" + "=" * 80)
print("検証12: アポイントランク（AppointRank__c）の月別変化")
print("=" * 80)

print(f"\n  {'月':>8}", end="")
for rank in won["AppointRank__c"].fillna("(空)").unique():
    print(f" {str(rank):>8}", end="")
print()

for m in ["2025-11", "2025-12", "2026-01", "2026-02"]:
    md = won[won["close_month"] == m]
    print(f"  {m:>8}", end="")
    for rank in won["AppointRank__c"].fillna("(空)").unique():
        cnt = len(md[md["AppointRank__c"].fillna("(空)") == rank])
        print(f" {cnt:>7}件", end="")
    print()

print("\n" + "=" * 80)
print("根幹原因分析完了")
print("=" * 80)
