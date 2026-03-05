"""
dodaデータ Salesforceインポートスクリプト (Composite API使用)

処理内容:
1. 更新対象の既存Lead/Accountデータをバックアップ
2. 新規リード作成 (660件)
3. 既存Lead更新 (39件)
4. 既存Account更新 (1件)
"""

import sys
import csv
import json
import time
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime

# 日本語出力対応
sys.stdout.reconfigure(encoding='utf-8')

# Salesforceクライアント初期化
script_dir = Path(__file__).parent.parent
sys.path.insert(0, str(script_dir))
sys.path.insert(0, str(script_dir / 'src'))
from api.salesforce_client import SalesforceClient

print("=" * 80)
print("dodaデータ Salesforceインポート開始")
print("=" * 80)
print(f"実行時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

# Salesforce認証
client = SalesforceClient()
print("Salesforce認証中...")
client.authenticate()
print(f"認証成功: {client.instance_url}\n")

# ディレクトリ設定
base = Path(r'C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List')
output_dir = base / 'data/output/media_matching'
output_dir.mkdir(parents=True, exist_ok=True)

# ===================================================================
# Helper Functions
# ===================================================================

def clean_phone(phone_str):
    """電話番号クリーニング: 先頭の'を削除、.0を削除"""
    if not phone_str or pd.isna(phone_str):
        return ''

    phone_str = str(phone_str).strip()

    # 先頭のシングルクォート削除（CSV安全対策で付いた場合）
    if phone_str.startswith("'"):
        phone_str = phone_str[1:]

    # .0削除（float変換時の副作用）
    if phone_str.endswith('.0'):
        phone_str = phone_str[:-2]

    return phone_str

def clean_record(record_dict):
    """
    レコード辞書から空値・NaN・Noneを削除し、電話番号をクリーニング
    """
    cleaned = {}
    for key, value in record_dict.items():
        # 空値チェック
        if pd.isna(value) or value == '' or value is None:
            continue

        # 電話番号フィールドの特別処理
        if key in ['Phone', 'MobilePhone']:
            value = clean_phone(value)
            if not value:
                continue

        cleaned[key] = value

    return cleaned

def composite_request(client, records, operation='insert', object_name='Lead', batch_size=200):
    """
    Salesforce Composite APIでレコード挿入・更新

    Args:
        client: SalesforceClientインスタンス
        records: レコード辞書のリスト
        operation: 'insert' または 'update'
        object_name: 'Lead' または 'Account'
        batch_size: バッチサイズ（デフォルト200）

    Returns:
        全結果のリスト（success/error情報含む）
    """
    all_results = []
    headers = {
        'Authorization': f'Bearer {client.access_token}',
        'Content-Type': 'application/json'
    }

    total_batches = (len(records) + batch_size - 1) // batch_size

    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        batch_num = i // batch_size + 1

        # レコードクリーニング
        cleaned_batch = [clean_record(rec) for rec in batch]

        # Composite API用ペイロード作成
        if operation == 'insert':
            url = f'{client.instance_url}/services/data/v59.0/composite/sobjects'
            payload = {
                'allOrNone': False,
                'records': [{'attributes': {'type': object_name}, **rec} for rec in cleaned_batch]
            }
            response = requests.post(url, headers=headers, json=payload)
        else:  # update
            url = f'{client.instance_url}/services/data/v59.0/composite/sobjects'
            payload = {
                'allOrNone': False,
                'records': [{'attributes': {'type': object_name}, **rec} for rec in cleaned_batch]
            }
            response = requests.patch(url, headers=headers, json=payload)

        # レスポンス処理
        if response.status_code in [200, 201]:
            results = response.json()
            success_count = sum(1 for r in results if r.get('success'))
            failed_count = sum(1 for r in results if not r.get('success'))
            all_results.extend(results)

            print(f'  バッチ {batch_num}/{total_batches}: {success_count} 成功, {failed_count} 失敗')

            # エラー詳細表示
            if failed_count > 0:
                for j, r in enumerate(results):
                    if not r.get('success'):
                        errors = r.get('errors', [])
                        error_msg = ', '.join([e.get('message', '') for e in errors])
                        print(f'    エラー行 {i+j+1}: {error_msg[:200]}')
        else:
            print(f'  バッチ {batch_num}/{total_batches}: HTTP {response.status_code} - {response.text[:500]}')
            all_results.extend([{'success': False, 'errors': [{'message': response.text[:200]}]}] * len(batch))

        # レート制限対策
        time.sleep(1)

    return all_results

def backup_existing_records(client, ids, object_name, fields, backup_file):
    """
    既存レコードを指定フィールドでクエリしてバックアップCSVに保存
    """
    if not ids:
        print(f"  バックアップ対象なし ({object_name})")
        return

    print(f"  {len(ids)} 件の{object_name}をバックアップ中...")

    # SOQL作成（Idで検索）
    fields_str = ', '.join(fields)
    ids_str = "', '".join(ids)
    soql = f"SELECT {fields_str} FROM {object_name} WHERE Id IN ('{ids_str}')"

    # クエリ実行
    url = f"{client.instance_url}/services/data/v59.0/query"
    headers = client._get_headers()
    params = {'q': soql}

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        result = response.json()
        records = result.get('records', [])

        # 属性情報削除
        for rec in records:
            if 'attributes' in rec:
                del rec['attributes']

        # CSV保存
        if records:
            df = pd.DataFrame(records)
            df.to_csv(backup_file, index=False, encoding='utf-8-sig')
            print(f"  バックアップ保存: {backup_file} ({len(records)} 件)")
        else:
            print(f"  警告: クエリ結果0件 ({object_name})")
    else:
        print(f"  エラー: バックアップクエリ失敗 (HTTP {response.status_code})")
        print(f"  {response.text[:500]}")

# ===================================================================
# STEP 1: 既存データバックアップ
# ===================================================================

print("[STEP 1] 既存データバックアップ")
print("-" * 80)

# Lead更新対象のID取得
lead_update_file = output_dir / 'doda_lead_updates_20260203.csv'
account_update_file = output_dir / 'doda_account_updates_20260203.csv'

lead_update_ids = []
if lead_update_file.exists():
    df_lead = pd.read_csv(lead_update_file, dtype=str, encoding='utf-8-sig')
    if 'Id' in df_lead.columns:
        lead_update_ids = df_lead['Id'].dropna().tolist()
        print(f"Lead更新対象: {len(lead_update_ids)} 件")

account_update_ids = []
if account_update_file.exists():
    df_account = pd.read_csv(account_update_file, dtype=str, encoding='utf-8-sig')
    if 'Id' in df_account.columns:
        account_update_ids = df_account['Id'].dropna().tolist()
        print(f"Account更新対象: {len(account_update_ids)} 件")

# バックアップ実行
lead_backup_fields = ['Id', 'Company', 'LastName', 'Phone', 'MobilePhone', 'Description', 'OwnerId']
account_backup_fields = ['Id', 'Name', 'Phone', 'Description', 'OwnerId']

backup_existing_records(
    client,
    lead_update_ids,
    'Lead',
    lead_backup_fields,
    output_dir / 'backup_Lead_20260203.csv'
)

backup_existing_records(
    client,
    account_update_ids,
    'Account',
    account_backup_fields,
    output_dir / 'backup_Account_20260203.csv'
)

print()

# ===================================================================
# STEP 2: 新規リード作成 (660件)
# ===================================================================

print("[STEP 2] 新規リード作成")
print("-" * 80)

new_leads_file = output_dir / 'doda_new_leads_20260203.csv'

if not new_leads_file.exists():
    print(f"エラー: {new_leads_file} が見つかりません")
    sys.exit(1)

df_new = pd.read_csv(new_leads_file, dtype=str, encoding='utf-8-sig')
print(f"新規リード件数: {len(df_new)} 件")

# レコード辞書リスト作成
new_lead_records = df_new.to_dict('records')

print(f"インポート開始 (Composite API, バッチサイズ200)...")
new_lead_results = composite_request(
    client,
    new_lead_records,
    operation='insert',
    object_name='Lead',
    batch_size=200
)

# 成功したLeadのID収集
created_lead_ids = [r['id'] for r in new_lead_results if r.get('success')]
success_count_new = len(created_lead_ids)
failed_count_new = len(new_lead_results) - success_count_new

print(f"新規リード作成完了: {success_count_new} 成功, {failed_count_new} 失敗")

# 作成済みLeadID保存
if created_lead_ids:
    created_ids_file = output_dir / 'created_lead_ids_20260203.csv'
    pd.DataFrame({'Id': created_lead_ids}).to_csv(created_ids_file, index=False, encoding='utf-8-sig')
    print(f"作成済みLeadID保存: {created_ids_file}")

print()

# ===================================================================
# STEP 3: 既存Lead更新 (39件)
# ===================================================================

print("[STEP 3] 既存Lead更新")
print("-" * 80)

if lead_update_file.exists():
    df_lead_update = pd.read_csv(lead_update_file, dtype=str, encoding='utf-8-sig')
    print(f"Lead更新件数: {len(df_lead_update)} 件")

    lead_update_records = df_lead_update.to_dict('records')

    print(f"更新開始 (Composite API, バッチサイズ200)...")
    lead_update_results = composite_request(
        client,
        lead_update_records,
        operation='update',
        object_name='Lead',
        batch_size=200
    )

    success_count_lead_update = sum(1 for r in lead_update_results if r.get('success'))
    failed_count_lead_update = len(lead_update_results) - success_count_lead_update

    print(f"Lead更新完了: {success_count_lead_update} 成功, {failed_count_lead_update} 失敗")
else:
    print("Lead更新ファイルなし")
    lead_update_results = []
    success_count_lead_update = 0
    failed_count_lead_update = 0

print()

# ===================================================================
# STEP 4: 既存Account更新 (1件)
# ===================================================================

print("[STEP 4] 既存Account更新")
print("-" * 80)

if account_update_file.exists():
    df_account_update = pd.read_csv(account_update_file, dtype=str, encoding='utf-8-sig')
    print(f"Account更新件数: {len(df_account_update)} 件")

    account_update_records = df_account_update.to_dict('records')

    print(f"更新開始 (Composite API, バッチサイズ200)...")
    account_update_results = composite_request(
        client,
        account_update_records,
        operation='update',
        object_name='Account',
        batch_size=200
    )

    success_count_account_update = sum(1 for r in account_update_results if r.get('success'))
    failed_count_account_update = len(account_update_results) - success_count_account_update

    print(f"Account更新完了: {success_count_account_update} 成功, {failed_count_account_update} 失敗")
else:
    print("Account更新ファイルなし")
    account_update_results = []
    success_count_account_update = 0
    failed_count_account_update = 0

print()

# ===================================================================
# STEP 5: 最終サマリー
# ===================================================================

print("=" * 80)
print("インポート最終サマリー")
print("=" * 80)

print(f"\n| 操作種別 | 総件数 | 成功 | 失敗 |")
print(f"|----------|--------|------|------|")
print(f"| 新規リード作成 | {len(new_lead_results)} | {success_count_new} | {failed_count_new} |")
print(f"| Lead更新 | {len(lead_update_results)} | {success_count_lead_update} | {failed_count_lead_update} |")
print(f"| Account更新 | {len(account_update_results)} | {success_count_account_update} | {failed_count_account_update} |")

total_success = success_count_new + success_count_lead_update + success_count_account_update
total_failed = failed_count_new + failed_count_lead_update + failed_count_account_update

print(f"| **合計** | **{len(new_lead_results) + len(lead_update_results) + len(account_update_results)}** | **{total_success}** | **{total_failed}** |")

# エラー詳細
if total_failed > 0:
    print(f"\n⚠️ エラー詳細:")

    all_errors = []

    for i, r in enumerate(new_lead_results):
        if not r.get('success'):
            all_errors.append(f"  新規Lead行{i+1}: {r.get('errors', [])}")

    for i, r in enumerate(lead_update_results):
        if not r.get('success'):
            all_errors.append(f"  Lead更新行{i+1}: {r.get('errors', [])}")

    for i, r in enumerate(account_update_results):
        if not r.get('success'):
            all_errors.append(f"  Account更新行{i+1}: {r.get('errors', [])}")

    for err in all_errors[:20]:  # 最大20件表示
        print(err)

    if len(all_errors) > 20:
        print(f"  ... 他 {len(all_errors) - 20} 件のエラー")

print(f"\n完了時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 80)
