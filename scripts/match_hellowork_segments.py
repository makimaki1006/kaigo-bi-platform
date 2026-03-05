"""ハローワーク セグメントC〜F Salesforce突合 + 成約先除外

マスタールール準拠:
  STEP 2: 電話番号でAccount/Contact/Lead突合 → 既存/新規振り分け
  STEP 5: 成約先除外（電話番号 OR 法人番号）
"""
import pandas as pd
import re
import sys
from pathlib import Path
from datetime import datetime

sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

# --- パス設定 ---
BASE_DIR = Path(r'C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List\data\output\hellowork_segments')
IMPORT_DIR = BASE_DIR / 'import_ready'
OUTPUT_DIR = BASE_DIR / 'matched'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Salesforceデータ（最新ファイルを自動検出）
def find_latest(pattern):
    files = sorted(BASE_DIR.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        raise FileNotFoundError(f'{pattern} が見つかりません: {BASE_DIR}')
    return files[0]

# 取引開始済Lead除外
EXCLUDED_LEAD_STATUSES = ['取引開始済']


def normalize_phone(val):
    """電話番号正規化"""
    if pd.isna(val) or not val:
        return ''
    s = re.sub(r'[^\d]', '', str(val))
    return s if 10 <= len(s) <= 11 and s.startswith('0') else ''


def normalize_corp_num(val):
    """法人番号正規化"""
    if pd.isna(val) or not val:
        return ''
    s = str(val).strip()
    if s.endswith('.0'):
        s = s[:-2]
    s = re.sub(r'[^0-9]', '', s)
    return s if len(s) >= 10 else ''


def build_phone_set(df, phone_cols):
    """DataFrameから正規化電話番号セットを構築"""
    phones = set()
    for col in phone_cols:
        if col in df.columns:
            for v in df[col].dropna():
                norm = normalize_phone(v)
                if norm:
                    phones.add(norm)
    return phones


def build_phone_to_id_map(df, phone_cols, id_col='Id'):
    """電話番号→IDのマップを構築"""
    phone_map = {}
    for col in phone_cols:
        if col in df.columns:
            for _, row in df.iterrows():
                norm = normalize_phone(row.get(col))
                if norm and pd.notna(row.get(id_col)):
                    if norm not in phone_map:
                        phone_map[norm] = {'id': row[id_col], 'source': col}
    return phone_map


def main():
    print("=" * 70)
    print("ハローワーク セグメントC〜F Salesforce突合 + 成約先除外")
    print(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    # === Salesforceデータ読み込み ===
    print("\n--- Salesforceデータ読み込み ---")

    acc_path = find_latest('Account_*.csv')
    con_path = find_latest('Contact_*.csv')
    lead_path = find_latest('Lead_*.csv')
    contract_path = find_latest('contract_accounts_*.csv')

    print(f"  Account: {acc_path.name}")
    print(f"  Contact: {con_path.name}")
    print(f"  Lead:    {lead_path.name}")
    print(f"  成約先:   {contract_path.name}")

    acc_df = pd.read_csv(acc_path, dtype=str, encoding='utf-8-sig')
    con_df = pd.read_csv(con_path, dtype=str, encoding='utf-8-sig')
    lead_df = pd.read_csv(lead_path, dtype=str, encoding='utf-8-sig')
    contract_df = pd.read_csv(contract_path, dtype=str, encoding='utf-8-sig')

    print(f"  Account: {len(acc_df):,}件")
    print(f"  Contact: {len(con_df):,}件")
    print(f"  Lead:    {len(lead_df):,}件")
    print(f"  成約先:   {len(contract_df):,}件")

    # Lead: 取引開始済を除外
    if 'Status' in lead_df.columns:
        before = len(lead_df)
        lead_df = lead_df[~lead_df['Status'].isin(EXCLUDED_LEAD_STATUSES)]
        excluded_status = before - len(lead_df)
        print(f"  Lead（取引開始済除外後）: {len(lead_df):,}件 （除外: {excluded_status:,}件）")

    # === 電話番号セット構築 ===
    print("\n--- 電話番号セット構築 ---")

    acc_phones = build_phone_set(acc_df, ['Phone', 'Phone2__c'])
    con_phones = build_phone_set(con_df, ['Phone', 'Phone2__c', 'MobilePhone', 'MobilePhone2__c'])
    lead_phones = build_phone_set(lead_df, ['Phone', 'MobilePhone', 'Phone2__c', 'MobilePhone2__c'])
    all_sf_phones = acc_phones | con_phones | lead_phones

    print(f"  Account電話: {len(acc_phones):,}件")
    print(f"  Contact電話: {len(con_phones):,}件")
    print(f"  Lead電話:    {len(lead_phones):,}件")
    print(f"  SF全体（重複除外）: {len(all_sf_phones):,}件")

    # === 成約先セット構築 ===
    print("\n--- 成約先セット構築 ---")

    contract_phones = set()
    if 'Phone' in contract_df.columns:
        for v in contract_df['Phone'].dropna():
            norm = normalize_phone(v)
            if norm:
                contract_phones.add(norm)

    contract_corps = set()
    for col in ['CorporateNumber__c', 'CorporateIdentificationNumber__c']:
        if col in contract_df.columns:
            for v in contract_df[col].dropna():
                norm = normalize_corp_num(v)
                if norm:
                    contract_corps.add(norm)

    print(f"  成約先電話: {len(contract_phones):,}件")
    print(f"  成約先法人番号: {len(contract_corps):,}件")

    # === セグメントデータ読み込み・突合 ===
    seg_files = list(IMPORT_DIR.glob('import_*_*.csv'))
    seg_files = [f for f in seg_files if 'CDEF_all' not in f.name]

    all_existing = []
    all_new = []
    all_contract_excluded = []

    for seg_file in sorted(seg_files):
        seg_name = seg_file.stem.replace('import_', '')
        print(f"\n{'='*70}")
        print(f"セグメント: {seg_name}")
        print(f"{'='*70}")

        df = pd.read_csv(seg_file, dtype=str, encoding='utf-8-sig')
        total = len(df)
        print(f"  入力: {total:,}件")

        # 電話番号正規化（既に正規化済みだが念のため）
        if '電話番号_正規化' in df.columns:
            phone_col = '電話番号_正規化'
        else:
            df['電話番号_正規化'] = df['選考担当者ＴＥＬ'].apply(normalize_phone) if '選考担当者ＴＥＬ' in df.columns else ''
            phone_col = '電話番号_正規化'

        # 法人番号正規化
        if '法人番号_cleaned' in df.columns:
            corp_col = '法人番号_cleaned'
        else:
            df['法人番号_cleaned'] = ''
            corp_col = '法人番号_cleaned'

        # --- STEP 2: SF既存レコード突合 ---
        df['_phone_norm'] = df[phone_col].apply(normalize_phone)

        # Account/Contact/Lead のいずれかに電話番号がマッチするか
        df['_match_account'] = df['_phone_norm'].isin(acc_phones)
        df['_match_contact'] = df['_phone_norm'].isin(con_phones)
        df['_match_lead'] = df['_phone_norm'].isin(lead_phones)
        df['_match_any'] = df['_match_account'] | df['_match_contact'] | df['_match_lead']

        matched = df[df['_match_any']].copy()
        unmatched = df[~df['_match_any']].copy()

        # マッチ先を記録
        def get_match_target(row):
            targets = []
            if row['_match_account']:
                targets.append('Account')
            if row['_match_contact']:
                targets.append('Contact')
            if row['_match_lead']:
                targets.append('Lead')
            return '/'.join(targets)

        matched['突合先'] = matched.apply(get_match_target, axis=1)

        print(f"\n  --- STEP 2: SF突合 ---")
        print(f"    既存マッチ:  {len(matched):>6,}件")
        print(f"      Account:  {df['_match_account'].sum():>6,}件")
        print(f"      Contact:  {df['_match_contact'].sum():>6,}件")
        print(f"      Lead:     {df['_match_lead'].sum():>6,}件")
        print(f"    未マッチ:    {len(unmatched):>6,}件（新規リード候補）")

        # --- STEP 5: 成約先除外（新規リード候補から） ---
        unmatched['_corp_norm'] = unmatched[corp_col].apply(normalize_corp_num)

        phone_match = unmatched['_phone_norm'].isin(contract_phones)
        corp_match = unmatched['_corp_norm'].isin(contract_corps)
        is_contract = phone_match | corp_match

        contract_excluded = unmatched[is_contract].copy()
        true_new = unmatched[~is_contract].copy()

        # 成約先除外理由
        def get_exclude_reason(row):
            reasons = []
            if row['_phone_norm'] in contract_phones:
                reasons.append('電話番号一致')
            if row.get('_corp_norm', '') in contract_corps:
                reasons.append('法人番号一致')
            return '/'.join(reasons)

        if len(contract_excluded) > 0:
            contract_excluded['除外理由'] = contract_excluded.apply(get_exclude_reason, axis=1)

        # 既存マッチ分の成約先チェック
        matched['_corp_norm'] = matched[corp_col].apply(normalize_corp_num)
        matched_phone_contract = matched['_phone_norm'].isin(contract_phones)
        matched_corp_contract = matched['_corp_norm'].isin(contract_corps)
        matched_is_contract = matched_phone_contract | matched_corp_contract

        existing_contract_excluded = matched[matched_is_contract].copy()
        existing_safe = matched[~matched_is_contract].copy()

        if len(existing_contract_excluded) > 0:
            existing_contract_excluded['除外理由'] = existing_contract_excluded.apply(get_exclude_reason, axis=1)

        print(f"\n  --- STEP 5: 成約先除外 ---")
        print(f"    【新規リード候補】")
        print(f"      除外前:    {len(unmatched):>6,}件")
        print(f"      電話一致:  {phone_match.sum():>6,}件")
        print(f"      法人一致:  {corp_match.sum():>6,}件")
        print(f"      除外合計:  {len(contract_excluded):>6,}件")
        print(f"      → 真の新規: {len(true_new):>6,}件")
        print(f"    【既存マッチ】")
        print(f"      成約先除外: {len(existing_contract_excluded):>6,}件")
        print(f"      → 安全更新: {len(existing_safe):>6,}件")

        # 作業用カラム削除
        drop_cols = ['_phone_norm', '_match_account', '_match_contact', '_match_lead', '_match_any', '_corp_norm']
        true_new = true_new.drop(columns=[c for c in drop_cols if c in true_new.columns], errors='ignore')
        existing_safe = existing_safe.drop(columns=[c for c in drop_cols if c in existing_safe.columns], errors='ignore')
        contract_excluded_all = pd.concat([contract_excluded, existing_contract_excluded])
        contract_excluded_all = contract_excluded_all.drop(columns=[c for c in drop_cols if c in contract_excluded_all.columns], errors='ignore')

        # セグメント別CSV出力
        if len(true_new) > 0:
            new_path = OUTPUT_DIR / f'new_leads_{seg_name}.csv'
            true_new.to_csv(new_path, index=False, encoding='utf-8-sig')
            print(f"\n  出力: {new_path.name} ({len(true_new):,}件)")

        if len(existing_safe) > 0:
            existing_path = OUTPUT_DIR / f'existing_{seg_name}.csv'
            existing_safe.to_csv(existing_path, index=False, encoding='utf-8-sig')
            print(f"  出力: {existing_path.name} ({len(existing_safe):,}件)")

        if len(contract_excluded_all) > 0:
            excluded_path = OUTPUT_DIR / f'excluded_contract_{seg_name}.csv'
            contract_excluded_all.to_csv(excluded_path, index=False, encoding='utf-8-sig')
            print(f"  出力: {excluded_path.name} ({len(contract_excluded_all):,}件)")

        # 近接スコア分布
        if '近接スコア' in true_new.columns:
            true_new['近接スコア'] = pd.to_numeric(true_new['近接スコア'], errors='coerce')
            prox = true_new['近接スコア'].value_counts().reindex([5, 4, 3, 2, 1], fill_value=0)
            wh = prox.get(5, 0) + prox.get(4, 0)
            print(f"\n  --- 新規リード 近接スコア ---")
            print(f"    ★5: {prox.get(5,0):,}  ★4: {prox.get(4,0):,}  ★3: {prox.get(3,0):,}  ★2: {prox.get(2,0):,}  ★1: {prox.get(1,0):,}")
            print(f"    ホワイト(★4-5): {wh:,}件 ({wh/len(true_new)*100:.1f}%)")

        all_existing.append(existing_safe)
        all_new.append(true_new)
        if len(contract_excluded_all) > 0:
            all_contract_excluded.append(contract_excluded_all)

    # === 全セグメント統合 ===
    print("\n" + "=" * 70)
    print("全セグメント統合")
    print("=" * 70)

    if all_new:
        all_new_df = pd.concat(all_new).drop_duplicates(subset=['求人番号'])
        all_new_path = OUTPUT_DIR / 'new_leads_CDEF_all.csv'
        all_new_df.to_csv(all_new_path, index=False, encoding='utf-8-sig')
        print(f"  新規リード合計（重複除外）: {len(all_new_df):,}件 → {all_new_path.name}")
    else:
        all_new_df = pd.DataFrame()
        print("  新規リード: 0件")

    if all_existing:
        all_existing_df = pd.concat(all_existing).drop_duplicates(subset=['求人番号'])
        all_existing_path = OUTPUT_DIR / 'existing_CDEF_all.csv'
        all_existing_df.to_csv(all_existing_path, index=False, encoding='utf-8-sig')
        print(f"  既存マッチ合計（重複除外）: {len(all_existing_df):,}件 → {all_existing_path.name}")
    else:
        all_existing_df = pd.DataFrame()
        print("  既存マッチ: 0件")

    if all_contract_excluded:
        all_excl_df = pd.concat(all_contract_excluded).drop_duplicates(subset=['求人番号'])
        all_excl_path = OUTPUT_DIR / 'excluded_contract_CDEF_all.csv'
        all_excl_df.to_csv(all_excl_path, index=False, encoding='utf-8-sig')
        print(f"  成約先除外合計（重複除外）: {len(all_excl_df):,}件 → {all_excl_path.name}")
    else:
        print("  成約先除外: 0件")

    # 最終サマリー
    print("\n" + "=" * 70)
    print("最終サマリー")
    print("=" * 70)

    total_input = sum(len(pd.read_csv(f, dtype=str, encoding='utf-8-sig', nrows=0)) or len(pd.read_csv(f, dtype=str, encoding='utf-8-sig')) for f in sorted(IMPORT_DIR.glob('import_*_*.csv')) if 'CDEF_all' not in f.name)

    print(f"\n  {'項目':<25} {'件数':>8}")
    print(f"  {'-'*40}")
    total_input_count = 0
    for f in sorted(IMPORT_DIR.glob('import_*_*.csv')):
        if 'CDEF_all' not in f.name:
            cnt = len(pd.read_csv(f, dtype=str, encoding='utf-8-sig'))
            total_input_count += cnt
    print(f"  {'入力合計':<25} {total_input_count:>8,}")
    if len(all_existing_df) > 0:
        print(f"  {'既存レコード（更新対象）':<25} {len(all_existing_df):>8,}")
    if all_contract_excluded:
        print(f"  {'成約先除外':<25} {len(all_excl_df):>8,}")
    print(f"  {'真の新規リード':<25} {len(all_new_df):>8,}")

    print(f"\n  出力先: {OUTPUT_DIR}")
    print(f"\n  🔴 成約先除外済み — インポート安全")


if __name__ == '__main__':
    main()
