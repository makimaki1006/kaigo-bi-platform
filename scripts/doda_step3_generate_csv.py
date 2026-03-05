"""
Dodaスクレイピングデータ → Salesforceインポート用CSV生成

処理内容:
1. doda_results.pkl を読み込み
2. 新規リード作成用CSV生成（3名均等割り当て）
3. 既存Lead更新用CSV生成
4. 既存Account更新用CSV生成
"""

import pickle
import sys
import re
import pandas as pd
from pathlib import Path
from datetime import date

# UTF-8出力設定
sys.stdout.reconfigure(encoding='utf-8')

# ====================================
# STEP 1: データ読み込み
# ====================================
base = Path(r'C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List')
pkl_path = base / 'data/output/media_matching/doda_results.pkl'

print("=" * 80)
print("Dodaデータ読み込み中...")
print("=" * 80)

with open(pkl_path, 'rb') as f:
    results = pickle.load(f)

new_leads = results['new_leads']
lead_updates = results['lead_updates']
account_updates = results['account_updates']
today = date.today().isoformat()  # 2026-02-03

print(f"✅ 読み込み完了:")
print(f"  - 新規リード候補: {len(new_leads)} 件")
print(f"  - Lead更新: {len(lead_updates)} 件")
print(f"  - Account更新: {len(account_updates)} 件")
print(f"  - 処理日: {today}")

# サンプルレコードの構造確認
if new_leads:
    print("\n📋 サンプルレコード構造:")
    sample = new_leads[0]
    for key in list(sample.keys())[:10]:
        print(f"  - {key}: {sample.get(key)}")

# ====================================
# 所有者定義（3名均等割り当て）
# ====================================
owners = [
    {'name': '佐藤丈太郎', 'id': '0055i00000CwGDGAA3'},
    {'name': '志村亮介', 'id': '0055i00000CwGCrAAN'},
    {'name': '小林幸太', 'id': '005J3000000ERz4IAG'}
]

# ====================================
# ユーティリティ関数
# ====================================
def is_mobile(phone):
    """携帯電話番号かどうか判定"""
    if not phone:
        return False
    phone_clean = str(phone).strip().replace('-', '')
    return phone_clean.startswith(('070', '080', '090'))

def normalize_phone(phone):
    """電話番号を正規化（0始まり、10-11桁、.0除去）"""
    if not phone or pd.isna(phone):
        return ''

    # 文字列化して.0を除去
    phone = str(phone).strip()
    if phone.endswith('.0'):
        phone = phone[:-2]

    # ハイフン等を除去
    phone_clean = re.sub(r'[^\d]', '', phone)

    # 0始まり、10-11桁チェック
    if phone_clean and phone_clean.startswith('0') and len(phone_clean) in [10, 11]:
        # ハイフン挿入を試みる（03-XXXX-XXXX、090-XXXX-XXXX等）
        if len(phone_clean) == 10:
            if phone_clean.startswith('0'):
                # 市外局番判定
                if phone_clean[:2] in ['03', '04', '06']:
                    return f"{phone_clean[:2]}-{phone_clean[2:6]}-{phone_clean[6:]}"
                else:
                    return f"{phone_clean[:3]}-{phone_clean[3:6]}-{phone_clean[6:]}"
        elif len(phone_clean) == 11:
            return f"{phone_clean[:3]}-{phone_clean[3:7]}-{phone_clean[7:]}"

        # ハイフン挿入できなければそのまま返す
        return phone_clean

    return ''

def is_valid_value(value):
    """有効な値かチェック（空でない、かつダミー値でない）"""
    if not value or pd.isna(value):
        return False
    value_str = str(value).strip()
    if not value_str:
        return False
    # ダミー値パターン
    dummy_patterns = ['不明', 'なし', 'N/A', 'unknown', '-']
    return value_str.lower() not in dummy_patterns

# ====================================
# STEP 2: 新規リードCSV生成
# ====================================
print("\n" + "=" * 80)
print("新規リードCSV生成中...")
print("=" * 80)

new_lead_rows = []
skipped_no_company = 0
skipped_no_phone = 0
owner_counts = {o['id']: 0 for o in owners}
owner_index = 0

for idx, rec in enumerate(new_leads):
    # Company必須チェック（キー名は 'company'）
    company = rec.get('company', '')
    if not is_valid_value(company):
        skipped_no_company += 1
        continue

    # 電話番号抽出
    phones = rec.get('phones', [])
    if not phones:
        phones = []
    elif isinstance(phones, str):
        phones = [phones]

    # 電話番号を正規化
    normalized_phones = [normalize_phone(p) for p in phones]
    normalized_phones = [p for p in normalized_phones if p]  # 空文字除去

    if not normalized_phones:
        skipped_no_phone += 1
        print(f"  ⚠️ Phone空のためスキップ: {company}")
        continue

    # 固定電話と携帯を分類
    mobile_phones = [p for p in normalized_phones if is_mobile(p)]
    fixed_phones = [p for p in normalized_phones if not is_mobile(p)]

    # Phone必須フィールド（固定優先、なければ携帯）
    if fixed_phones:
        phone = fixed_phones[0]
        mobile_phone = mobile_phones[0] if mobile_phones else ''
    else:
        # 携帯のみの場合
        phone = mobile_phones[0]
        mobile_phone = mobile_phones[0]

    # 所有者をラウンドロビン割り当て
    owner = owners[owner_index % len(owners)]
    owner_id = owner['id']
    owner_index += 1
    owner_counts[owner_id] += 1

    # レコード作成
    row = {
        'Company': company,
        'LastName': '担当者',
        'Phone': phone,
        'MobilePhone': mobile_phone,
        'LeadSource': 'Other',
        'Paid_Media__c': 'doda',
        'Paid_DataSource__c': 'doda',
        'Paid_JobTitle__c': rec.get('job_title', ''),
        'Paid_URL__c': rec.get('doda_url', ''),
        'Paid_DataExportDate__c': today,
        'LeadSourceMemo__c': f'【新規作成】有料媒体突合 {today}',
        'OwnerId': owner_id
    }
    new_lead_rows.append(row)

# DataFrame作成
df_new_leads = pd.DataFrame(new_lead_rows, dtype=str)

# 保存
output_dir = base / 'data/output/media_matching'
output_dir.mkdir(parents=True, exist_ok=True)

new_lead_csv = output_dir / f'doda_new_leads_{today.replace("-", "")}.csv'
df_new_leads.to_csv(new_lead_csv, index=False, encoding='utf-8-sig')

print(f"✅ 新規リードCSV保存: {new_lead_csv.name}")
print(f"  - 有効レコード: {len(df_new_leads)} 件")
print(f"  - スキップ（Company空）: {skipped_no_company} 件")
print(f"  - スキップ（Phone空）: {skipped_no_phone} 件")
print(f"\n📊 所有者別割り当て:")
for owner in owners:
    count = owner_counts[owner['id']]
    print(f"  - {owner['name']}: {count} 件")

# サンプル表示
if len(df_new_leads) > 0:
    print(f"\n📋 新規リードサンプル（先頭5件）:")
    sample_cols = ['Company', 'Phone', 'MobilePhone', 'OwnerId']
    print(df_new_leads[sample_cols].head(5).to_string(index=False))

# ====================================
# STEP 3: Lead更新CSV生成
# ====================================
print("\n" + "=" * 80)
print("Lead更新CSV生成中...")
print("=" * 80)

lead_update_rows = []

for rec in lead_updates:
    # マッチしたLeadレコードからIdを取得
    matched_leads = rec.get('matched_leads', [])
    if not matched_leads:
        print(f"  ⚠️ マッチLeadなし（スキップ）: {rec.get('company', 'Unknown')}")
        continue

    # 最初のマッチレコードを使用
    lead = matched_leads[0]
    lead_id = lead.get('Id')
    if not lead_id:
        print(f"  ⚠️ Lead Id取得失敗: {rec.get('company', 'Unknown')}")
        continue

    # 既存のLeadSourceMemo__cに追記
    existing_memo = lead.get('LeadSourceMemo__c', '')
    if existing_memo and not pd.isna(existing_memo):
        new_memo = f"【既存更新】有料媒体突合 {today}\n{existing_memo}"
    else:
        new_memo = f"【既存更新】有料媒体突合 {today}"

    row = {
        'Id': lead_id,
        'Paid_Media__c': 'doda',
        'Paid_DataSource__c': 'doda',
        'Paid_JobTitle__c': rec.get('job_title', ''),
        'Paid_URL__c': rec.get('doda_url', ''),
        'Paid_DataExportDate__c': today,
        'LeadSourceMemo__c': new_memo
    }
    lead_update_rows.append(row)

df_lead_updates = pd.DataFrame(lead_update_rows, dtype=str)

# 保存
lead_update_csv = output_dir / f'doda_lead_updates_{today.replace("-", "")}.csv'
df_lead_updates.to_csv(lead_update_csv, index=False, encoding='utf-8-sig')

print(f"✅ Lead更新CSV保存: {lead_update_csv.name}")
print(f"  - 更新レコード: {len(df_lead_updates)} 件")

# サンプル表示
if len(df_lead_updates) > 0:
    print(f"\n📋 Lead更新サンプル（先頭3件）:")
    sample_cols = ['Id', 'Paid_Media__c', 'Paid_JobTitle__c']
    print(df_lead_updates[sample_cols].head(3).to_string(index=False))

# ====================================
# STEP 4: Account更新CSV生成
# ====================================
print("\n" + "=" * 80)
print("Account更新CSV生成中...")
print("=" * 80)

account_update_rows = []

for rec in account_updates:
    # マッチしたAccountレコードからIdを取得
    # Account突合 or Contact突合の可能性がある
    matched_accounts = rec.get('matched_accounts', [])
    matched_contacts = rec.get('matched_contacts', [])

    account = None
    if matched_accounts:
        account = matched_accounts[0]
    elif matched_contacts:
        # Contact経由でAccountIdを取得
        contact = matched_contacts[0]
        account_id = contact.get('AccountId')
        if not account_id:
            print(f"  ⚠️ Contact経由AccountId取得失敗: {rec.get('company', 'Unknown')}")
            continue
        # Accountレコード情報がないため、Idのみで更新
        account = {'Id': account_id}
    else:
        print(f"  ⚠️ マッチAccountなし（スキップ）: {rec.get('company', 'Unknown')}")
        continue

    account_id = account.get('Id')
    if not account_id:
        print(f"  ⚠️ Account Id取得失敗: {rec.get('company', 'Unknown')}")
        continue

    # 既存のDescriptionに追記
    existing_desc = account.get('Description', '')
    if existing_desc and not pd.isna(existing_desc):
        new_desc = f"【既存更新】有料媒体突合 {today}\n{existing_desc}"
    else:
        new_desc = f"【既存更新】有料媒体突合 {today}"

    row = {
        'Id': account_id,
        'Paid_DataExportDate__c': today,
        'Description': new_desc
    }
    account_update_rows.append(row)

df_account_updates = pd.DataFrame(account_update_rows, dtype=str)

# 保存
account_update_csv = output_dir / f'doda_account_updates_{today.replace("-", "")}.csv'
df_account_updates.to_csv(account_update_csv, index=False, encoding='utf-8-sig')

print(f"✅ Account更新CSV保存: {account_update_csv.name}")
print(f"  - 更新レコード: {len(df_account_updates)} 件")

# ====================================
# 最終サマリー
# ====================================
print("\n" + "=" * 80)
print("処理完了サマリー")
print("=" * 80)
print(f"📅 処理日: {today}")
print(f"\n📊 生成されたCSV:")
print(f"  1. 新規リード: {new_lead_csv.name} ({len(df_new_leads)} 件)")
print(f"  2. Lead更新: {lead_update_csv.name} ({len(df_lead_updates)} 件)")
print(f"  3. Account更新: {account_update_csv.name} ({len(df_account_updates)} 件)")
print(f"\n⚠️ スキップ:")
print(f"  - Company空: {skipped_no_company} 件")
print(f"  - Phone空: {skipped_no_phone} 件")
print(f"\n✅ 次のステップ: Salesforceインポート実行")
print("=" * 80)
