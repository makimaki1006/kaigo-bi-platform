"""
看護roo/ナース専科 リードソース成約率分析スクリプト

Leadデータから看護媒体由来のリードを特定し、
ConvertedAccountId経由でOpportunityを取得して成約率を算出する。
"""

import sys
sys.stdout.reconfigure(encoding='utf-8')

import os
import pandas as pd
import requests
from pathlib import Path

# プロジェクトルート
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / 'src'))

from utils.config import sf_config


def get_access_token():
    """Salesforce認証"""
    token_url = f"{sf_config.INSTANCE_URL}/services/oauth2/token"
    payload = {
        'grant_type': 'refresh_token',
        'client_id': sf_config.CLIENT_ID,
        'client_secret': sf_config.CLIENT_SECRET,
        'refresh_token': sf_config.REFRESH_TOKEN
    }
    response = requests.post(token_url, data=payload)
    response.raise_for_status()
    print("[OK] Salesforce認証成功")
    return response.json()['access_token']


def soql_query(access_token, query):
    """SOQL クエリ実行（ページネーション対応）"""
    headers = {'Authorization': f'Bearer {access_token}'}
    url = f"{sf_config.INSTANCE_URL}/services/data/{sf_config.API_VERSION}/query"
    params = {'q': query}

    all_records = []
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()
    all_records.extend(data['records'])

    # ページネーション
    while not data['done']:
        next_url = f"{sf_config.INSTANCE_URL}{data['nextRecordsUrl']}"
        response = requests.get(next_url, headers=headers)
        response.raise_for_status()
        data = response.json()
        all_records.extend(data['records'])

    # attributes カラム除去
    for rec in all_records:
        rec.pop('attributes', None)

    return all_records


def main():
    print("=" * 70)
    print("看護roo / ナース専科 リードソース 成約率分析")
    print("=" * 70)

    # ---- Step 1: Leadデータ読み込み ----
    lead_file = PROJECT_ROOT / 'data' / 'output' / 'Lead_20260305_115825.csv'
    print(f"\n[1] Leadデータ読み込み: {lead_file.name}")

    df_lead = pd.read_csv(lead_file, encoding='utf-8-sig', dtype=str)
    print(f"  全Lead数: {len(df_lead):,}")

    # 看護媒体フィルタ（「看護roo」「看護るー」「ナース専科」）
    kango_sources = ['看護roo', '看護るー', 'ナース専科']
    df_kango = df_lead[df_lead['Paid_DataSource__c'].isin(kango_sources)].copy()
    print(f"  看護roo/ナース専科 リード数: {len(df_kango):,}")

    # 媒体別内訳
    print("\n  【媒体別内訳】")
    source_counts = df_kango['Paid_DataSource__c'].value_counts()
    for src, cnt in source_counts.items():
        print(f"    {src}: {cnt:,}件")

    # ---- Step 2: コンバート分析 ----
    print(f"\n[2] コンバート分析")
    df_kango['IsConverted'] = df_kango['IsConverted'].fillna('false')
    df_converted = df_kango[df_kango['IsConverted'] == 'true']
    print(f"  コンバート済み: {len(df_converted):,}件 / {len(df_kango):,}件 ({len(df_converted)/len(df_kango)*100:.1f}%)")

    # 媒体別コンバート率
    print("\n  【媒体別コンバート率】")
    for src in kango_sources:
        src_total = len(df_kango[df_kango['Paid_DataSource__c'] == src])
        src_conv = len(df_converted[df_converted['Paid_DataSource__c'] == src])
        rate = src_conv / src_total * 100 if src_total > 0 else 0
        print(f"    {src}: {src_conv:,} / {src_total:,} ({rate:.1f}%)")

    # ConvertedAccountId一覧
    account_ids = df_converted['ConvertedAccountId'].dropna()
    account_ids = account_ids[account_ids != ''].unique().tolist()
    print(f"\n  コンバート先AccountID数: {len(account_ids)}")

    if len(account_ids) == 0:
        print("\n  コンバート先Accountがないため、Opportunity分析はスキップします。")
        return

    # ---- Step 3: Salesforce APIでOpportunity取得 ----
    print(f"\n[3] Salesforce APIでOpportunity取得")
    access_token = get_access_token()

    # AccountIdリストをチャンク分割してSOQL IN句で検索
    chunk_size = 100  # SOQL IN句の制限
    all_opps = []

    for i in range(0, len(account_ids), chunk_size):
        chunk = account_ids[i:i + chunk_size]
        ids_str = "','".join(chunk)
        query = f"""
            SELECT Id, Name, AccountId, StageName, Amount, CloseDate,
                   WonDate__c, LostDate__c, CreatedDate, Owner.Name,
                   OpportunityType__c, WonReason__c, LostReason_Large__c
            FROM Opportunity
            WHERE AccountId IN ('{ids_str}')
        """
        records = soql_query(access_token, query)
        all_opps.extend(records)
        print(f"  チャンク {i // chunk_size + 1}: {len(records)}件取得")

    print(f"  Opportunity総数: {len(all_opps):,}件")

    if len(all_opps) == 0:
        print("\n  紐づくOpportunityがありません。")
        print("\n  ※ コンバート時にOpportunityが作成されていない可能性があります。")
        return

    df_opp = pd.DataFrame(all_opps)

    # Owner.Name のフラット化
    if 'Owner' in df_opp.columns:
        df_opp['OwnerName'] = df_opp['Owner'].apply(
            lambda x: x.get('Name', '') if isinstance(x, dict) else ''
        )

    # ---- Step 4: 成約分析 ----
    print(f"\n[4] 成約分析")

    # ステージ別分布
    print("\n  【ステージ別分布】")
    stage_counts = df_opp['StageName'].value_counts()
    for stage, cnt in stage_counts.items():
        print(f"    {stage}: {cnt:,}件")

    # 成約判定（StageName に "Won" や "成約" を含むもの）
    won_stages = df_opp['StageName'].unique()
    won_keywords = ['Won', 'Closed Won', '成約', '受注']
    won_mask = df_opp['StageName'].apply(
        lambda x: any(kw.lower() in str(x).lower() for kw in won_keywords)
    )
    df_won = df_opp[won_mask]

    lost_keywords = ['Lost', 'Closed Lost', '失注', '不成約']
    lost_mask = df_opp['StageName'].apply(
        lambda x: any(kw.lower() in str(x).lower() for kw in lost_keywords)
    )
    df_lost = df_opp[lost_mask]

    total_opps = len(df_opp)
    won_count = len(df_won)
    lost_count = len(df_lost)

    print(f"\n  Opportunity総数: {total_opps:,}")
    print(f"  成約数: {won_count:,}")
    print(f"  失注数: {lost_count:,}")
    print(f"  進行中: {total_opps - won_count - lost_count:,}")

    if total_opps > 0:
        print(f"\n  成約率（全Opp対比）: {won_count / total_opps * 100:.1f}%")
    if won_count + lost_count > 0:
        print(f"  成約率（決着済み対比）: {won_count / (won_count + lost_count) * 100:.1f}%")

    # ---- Step 5: 成約金額 ----
    print(f"\n[5] 成約金額")
    if won_count > 0 and 'Amount' in df_won.columns:
        df_won_amount = df_won['Amount'].dropna()
        df_won_amount = pd.to_numeric(df_won_amount, errors='coerce').dropna()
        if len(df_won_amount) > 0:
            print(f"  金額入力あり: {len(df_won_amount):,}件 / {won_count:,}件")
            print(f"  合計金額: ¥{df_won_amount.sum():,.0f}")
            print(f"  平均金額: ¥{df_won_amount.mean():,.0f}")
            print(f"  中央値: ¥{df_won_amount.median():,.0f}")
            print(f"  最小: ¥{df_won_amount.min():,.0f}")
            print(f"  最大: ¥{df_won_amount.max():,.0f}")
        else:
            print("  金額データなし")
    else:
        print("  成約案件なしまたはAmountフィールドなし")

    # ---- Step 6: 媒体別成約分析 ----
    print(f"\n[6] 媒体別成約分析")

    # Lead → Account マッピング（媒体情報付き）
    lead_acct_map = df_converted[['ConvertedAccountId', 'Paid_DataSource__c']].dropna(subset=['ConvertedAccountId'])
    # 同一AccountIdに複数媒体の場合、最初の媒体を採用
    lead_acct_map = lead_acct_map.drop_duplicates(subset='ConvertedAccountId', keep='first')
    acct_to_source = dict(zip(lead_acct_map['ConvertedAccountId'], lead_acct_map['Paid_DataSource__c']))

    df_opp['MediaSource'] = df_opp['AccountId'].map(acct_to_source)

    print(f"\n  {'媒体':<12} {'Opp数':>6} {'成約':>6} {'失注':>6} {'進行中':>6} {'成約率':>8}")
    print("  " + "-" * 52)

    for src in kango_sources:
        src_opps = df_opp[df_opp['MediaSource'] == src]
        src_won = src_opps[src_opps['StageName'].apply(
            lambda x: any(kw.lower() in str(x).lower() for kw in won_keywords)
        )]
        src_lost = src_opps[src_opps['StageName'].apply(
            lambda x: any(kw.lower() in str(x).lower() for kw in lost_keywords)
        )]
        src_prog = len(src_opps) - len(src_won) - len(src_lost)
        rate = f"{len(src_won) / len(src_opps) * 100:.1f}%" if len(src_opps) > 0 else "N/A"
        print(f"  {src:<12} {len(src_opps):>6} {len(src_won):>6} {len(src_lost):>6} {src_prog:>6} {rate:>8}")

    # ---- Step 7: ファネル全体サマリ ----
    print(f"\n{'=' * 70}")
    print("ファネルサマリ（看護roo + ナース専科 合計）")
    print("=" * 70)
    print(f"  リード総数:           {len(df_kango):>8,}")
    print(f"  コンバート済み:       {len(df_converted):>8,} ({len(df_converted)/len(df_kango)*100:.1f}%)")
    print(f"  Opportunity作成:      {total_opps:>8,} ({total_opps/len(df_kango)*100:.2f}%)")
    print(f"  成約:                 {won_count:>8,} ({won_count/len(df_kango)*100:.2f}%)")
    if won_count > 0 and 'Amount' in df_won.columns:
        df_won_amount = pd.to_numeric(df_won['Amount'], errors='coerce').dropna()
        if len(df_won_amount) > 0:
            print(f"  成約合計金額:     ¥{df_won_amount.sum():>12,.0f}")

    print(f"\n  コンバート → Opp率:   {total_opps/len(df_converted)*100:.1f}%" if len(df_converted) > 0 else "")
    print(f"  Opp → 成約率:         {won_count/total_opps*100:.1f}%" if total_opps > 0 else "")
    print(f"  リード → 成約率:      {won_count/len(df_kango)*100:.2f}%")


if __name__ == '__main__':
    main()
