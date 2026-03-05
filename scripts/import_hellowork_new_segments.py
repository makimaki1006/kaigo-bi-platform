"""ハローワーク セグメントC〜F 新規リード Salesforceインポート

突合・成約先除外済みの新規リードをSalesforceに作成する
"""
import sys
import io
import csv
import time
import re
import pandas as pd
from pathlib import Path
from datetime import date, datetime

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from src.utils.config import sf_config

# --- パス ---
MATCHED_DIR = Path(r'C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List\data\output\hellowork_segments\matched')
OUTPUT_DIR = Path(r'C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List\data\output\hellowork_segments\import_final')
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# --- 所有者ID ---
OWNER_IDS = {
    '市来': '005dc00000FwuKXAAZ',
    '嶋谷': '005dc000001dryLAAQ',
    '小林': '005J3000000ERz4IAG',
    '熊谷': '0055i00000CDtTOAA1',
    '松風': '0055i00000CwGDpAAN',
    '篠木': '005dc00000HgmfxAAB',
    '澤田': '005dc00000IwKTpAAN',
    '深堀': '0055i00000CwKEhAAN',
    '服部': '005J3000000EYYjIAO',
}

JINZAI_MEMBERS = ['市来', '嶋谷', '小林', '熊谷', '松風', '篠木', '澤田']

PREFECTURES = [
    '北海道','青森県','岩手県','宮城県','秋田県','山形県','福島県',
    '茨城県','栃木県','群馬県','埼玉県','千葉県','東京都','神奈川県',
    '新潟県','富山県','石川県','福井県','山梨県','長野県',
    '岐阜県','静岡県','愛知県','三重県',
    '滋賀県','京都府','大阪府','兵庫県','奈良県','和歌山県',
    '鳥取県','島根県','岡山県','広島県','山口県',
    '徳島県','香川県','愛媛県','高知県',
    '福岡県','佐賀県','長崎県','熊本県','大分県','宮崎県','鹿児島県','沖縄県',
]


def clean_str(val):
    if pd.isna(val) or str(val).strip() in ('', 'nan', 'None'):
        return ''
    return str(val).strip()


def clean_int(val):
    if pd.isna(val) or str(val).strip() in ('', 'nan', 'None'):
        return ''
    try:
        return str(int(float(val)))
    except (ValueError, TypeError):
        return ''


def clean_date(val):
    """日付をYYYY-MM-DD形式に正規化（SFはISO形式必須）"""
    if pd.isna(val) or str(val).strip() in ('', 'nan', 'None'):
        return ''
    s = str(val).strip()[:10]
    # YYYY/MM/DD → YYYY-MM-DD
    s = s.replace('/', '-')
    # 妥当性チェック
    if re.match(r'^\d{4}-\d{2}-\d{2}$', s):
        return s
    return ''


def extract_prefecture(addr):
    if not addr:
        return ''
    for p in PREFECTURES:
        if addr.startswith(p):
            return p
    return ''


def extract_street(addr):
    if not addr:
        return ''
    for p in PREFECTURES:
        if addr.startswith(p):
            return addr[len(p):]
    return addr


def is_mobile(phone):
    if not phone:
        return False
    s = re.sub(r'[^\d]', '', str(phone))
    return bool(re.match(r'^0[789]0\d{8}$', s))


def normalize_phone_for_sf(phone):
    """Salesforceに入れる電話番号（ハイフン付きが望ましいが、元データのまま）"""
    if not phone:
        return ''
    return str(phone).strip()


def generate_import_csv(dry_run=True):
    """新規リードインポートCSV生成"""

    today = date.today().strftime('%Y-%m-%d')
    today_short = date.today().strftime('%Y%m%d')

    # 全セグメント統合CSV読み込み
    all_path = MATCHED_DIR / 'new_leads_CDEF_all.csv'
    if not all_path.exists():
        print(f"ERROR: {all_path} が見つかりません")
        return None

    df = pd.read_csv(all_path, dtype=str, encoding='utf-8-sig')
    print(f"新規リード候補: {len(df):,}件")

    # 所有者割り当て: 最強セグメント（近接スコア5）→ 市来優先
    df['近接スコア'] = pd.to_numeric(df['近接スコア'], errors='coerce')

    # S級（★5）→ 市来に優先
    s_tier = df[df['近接スコア'] == 5].copy()
    others = df[df['近接スコア'] != 5].copy()

    # 市来にS級を割り当て
    s_tier['_owner'] = '市来'

    # 残りを人材開発7名均等 + 深堀 + 服部
    n_others = len(others)
    # 深堀・服部は全体の25%ずつ
    n_fukabori = n_others // 4
    n_hattori = n_others // 4
    n_jinzai = n_others - n_fukabori - n_hattori

    owners_list = []
    # 人材開発メンバー均等
    per_member = n_jinzai // len(JINZAI_MEMBERS)
    remainder = n_jinzai % len(JINZAI_MEMBERS)
    for i, member in enumerate(JINZAI_MEMBERS):
        count = per_member + (1 if i < remainder else 0)
        owners_list.extend([member] * count)
    # 深堀・服部
    owners_list.extend(['深堀'] * n_fukabori)
    owners_list.extend(['服部'] * n_hattori)

    others = others.reset_index(drop=True)
    others['_owner'] = owners_list[:len(others)]

    df_all = pd.concat([s_tier, others], ignore_index=True)

    # レコード生成
    records = []
    skipped_no_company = 0
    skipped_no_phone = 0

    for _, row in df_all.iterrows():
        company = clean_str(row.get('事業所名漢字', ''))
        if not company:
            skipped_no_company += 1
            continue

        # Phone: 正規化済みの電話番号 or 元の電話番号
        phone_raw = clean_str(row.get('選考担当者ＴＥＬ', ''))
        phone_norm = clean_str(row.get('電話番号_正規化', ''))
        phone = phone_raw if phone_raw else phone_norm
        if not phone:
            skipped_no_phone += 1
            continue

        # MobilePhone
        mobile = ''
        if is_mobile(phone_norm):
            mobile = phone
            # Phone も携帯番号で埋める（必須フィールド対応）

        # LastName
        last_name = clean_str(row.get('選考担当者氏名漢字', ''))
        if not last_name:
            last_name = '担当者'

        # Name_Kana__c
        name_kana = clean_str(row.get('選考担当者氏名フリガナ', ''))

        # メモ
        segment = clean_str(row.get('セグメント', ''))
        industry = clean_str(row.get('業界', ''))
        job_type = clean_str(row.get('職種', ''))
        emp_type = clean_str(row.get('雇用形態', ''))
        proximity = clean_str(row.get('近接ランク', ''))

        details = []
        details.append(f"セグメント: {segment}")
        if industry:
            details.append(f"業界: {industry}")
        details.append(f"産業分類: {clean_str(row.get('産業分類（名称）', ''))}")
        if job_type:
            details.append(f"職種: {job_type}")
        if emp_type:
            details.append(f"雇用形態: {emp_type}")
        if clean_int(row.get('従業員数_数値', '')):
            details.append(f"従業員数: {clean_int(row['従業員数_数値'])}")
        if proximity:
            details.append(f"近接スコア: {proximity}")

        publish_text = f"[{today} ハロワ新規_{segment}]\n" + '\n'.join(details)
        lead_source_memo = f'{today_short}_ハロワ_{segment}_{job_type}【{emp_type}】'

        address = clean_str(row.get('事業所所在地', ''))

        record = {
            'Company': company,
            'LastName': last_name,
            'Name_Kana__c': name_kana,
            'Phone': phone,
            'MobilePhone': mobile,
            'Email': clean_str(row.get('選考担当者Ｅメール', '')),
            'PostalCode': clean_str(row.get('事業所郵便番号', '')),
            'Prefecture__c': extract_prefecture(address),
            'Street': extract_street(address),
            'NumberOfEmployees': clean_int(row.get('従業員数_数値', '')),
            'Website': clean_str(row.get('事業所ホームページ', '')),
            'PresidentName__c': clean_str(row.get('代表者名', '')),
            'PresidentTitle__c': clean_str(row.get('代表者役職', '')),
            'Title': clean_str(row.get('選考担当者課係名／役職名', '')),
            'CorporateNumber__c': clean_str(row.get('法人番号_cleaned', '')),
            'Establish__c': clean_str(row.get('設立年', '')),
            'Population__c': clean_int(row.get('市区町村人口_数値', '')),
            'Hellowork_Industry__c': clean_str(row.get('産業分類（名称）', '')),
            'Hellowork_RecuritmentType__c': job_type,
            'Hellowork_EmploymentType__c': emp_type,
            'Hellowork_NumberOfEmployee_Office__c': clean_int(row.get('従業員数就業場所_数値', '')),
            'Hellowork_JobPublicationDate__c': clean_date(row.get('受付年月日（西暦）', '')),
            'Hellowork_JobClosedDate__c': clean_date(row.get('求人有効年月日（西暦）', '')),
            'Hellowork_DataImportDate__c': today,
            'LeadSource': 'Other',
            'LeadSourceMemo__c': lead_source_memo,
            'Publish_ImportText__c': publish_text,
            'OwnerId': OWNER_IDS.get(row.get('_owner', ''), ''),
        }
        records.append(record)

    df_out = pd.DataFrame(records)

    print(f"\nスキップ:")
    print(f"  Company空: {skipped_no_company}件")
    print(f"  Phone空:   {skipped_no_phone}件")
    print(f"インポート対象: {len(df_out):,}件")

    # 所有者別集計
    owner_name_map = {v: k for k, v in OWNER_IDS.items()}
    print(f"\n所有者別内訳:")
    for owner_id, cnt in df_out['OwnerId'].value_counts().items():
        name = owner_name_map.get(owner_id, owner_id)
        print(f"  {name}: {cnt:,}件")

    # サンプル出力
    print(f"\nサンプル（1件目）:")
    if len(df_out) > 0:
        for col, val in df_out.iloc[0].items():
            if val:
                print(f"  {col}: {str(val)[:100]}")

    # CSV保存
    out_path = OUTPUT_DIR / f'sf_import_new_leads_{today_short}.csv'
    df_out.to_csv(out_path, index=False, encoding='utf-8-sig')
    print(f"\nCSV保存: {out_path}")

    return out_path


def bulk_insert(csv_path, dry_run=True):
    """Bulk API 2.0 で新規リード作成"""

    df = pd.read_csv(csv_path, dtype=str, encoding='utf-8-sig')
    print(f"\n{'='*60}")
    print(f"Salesforce Bulk API 2.0 新規Lead作成")
    print(f"{'='*60}")
    print(f"  件数: {len(df):,}")
    print(f"  Dry Run: {dry_run}")

    if dry_run:
        print("\n[DRY RUN] 実行せず終了")
        return None

    # 認証
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    session.mount("https://", adapter)

    token_url = f"{sf_config.INSTANCE_URL}/services/oauth2/token"
    payload = {
        'grant_type': 'refresh_token',
        'client_id': sf_config.CLIENT_ID,
        'client_secret': sf_config.CLIENT_SECRET,
        'refresh_token': sf_config.REFRESH_TOKEN,
    }
    resp = session.post(token_url, data=payload)
    resp.raise_for_status()
    access_token = resp.json()['access_token']
    print("認証成功")

    headers_json = {'Authorization': f'Bearer {access_token}', 'Content-Type': 'application/json'}
    headers_csv = {'Authorization': f'Bearer {access_token}', 'Content-Type': 'text/csv'}

    # チャンク分割（10,000件ずつ）
    chunk_size = 10000
    all_results = []

    for i in range(0, len(df), chunk_size):
        chunk = df.iloc[i:i+chunk_size]
        print(f"\n  チャンク {i//chunk_size+1}: {len(chunk):,}件")

        # Job作成（insert）
        job_url = f"{sf_config.INSTANCE_URL}/services/data/{sf_config.API_VERSION}/jobs/ingest"
        job_payload = {
            'operation': 'insert',
            'object': 'Lead',
            'contentType': 'CSV',
            'lineEnding': 'CRLF',
        }
        resp = session.post(job_url, headers=headers_json, json=job_payload)
        resp.raise_for_status()
        job_id = resp.json()['id']
        print(f"    Job作成: {job_id}")

        # データアップロード
        csv_buffer = io.StringIO()
        chunk.to_csv(csv_buffer, index=False, quoting=csv.QUOTE_ALL)
        csv_data = csv_buffer.getvalue().encode('utf-8')

        upload_url = f"{sf_config.INSTANCE_URL}/services/data/{sf_config.API_VERSION}/jobs/ingest/{job_id}/batches"
        resp = session.put(upload_url, headers=headers_csv, data=csv_data)
        if resp.status_code not in [200, 201]:
            raise Exception(f"アップロード失敗: {resp.text}")
        print(f"    アップロード完了: {len(csv_data):,} bytes")

        # Jobクローズ
        close_url = f"{sf_config.INSTANCE_URL}/services/data/{sf_config.API_VERSION}/jobs/ingest/{job_id}"
        resp = session.patch(close_url, headers=headers_json, json={'state': 'UploadComplete'})
        if resp.status_code not in [200, 201]:
            raise Exception(f"クローズ失敗: {resp.text}")

        # 完了待機
        start = time.time()
        while True:
            resp = session.get(close_url, headers={'Authorization': f'Bearer {access_token}'})
            resp.raise_for_status()
            info = resp.json()
            state = info['state']
            processed = info.get('numberRecordsProcessed', 0)
            failed = info.get('numberRecordsFailed', 0)

            if state == 'JobComplete':
                print(f"    完了: 処理={processed:,} 成功={processed-failed:,} 失敗={failed:,}")
                all_results.append({'chunk': i//chunk_size+1, 'processed': processed, 'failed': failed, 'job_id': job_id})

                # 成功レコードのID取得
                success_url = f"{sf_config.INSTANCE_URL}/services/data/{sf_config.API_VERSION}/jobs/ingest/{job_id}/successfulResults"
                resp = session.get(success_url, headers={'Authorization': f'Bearer {access_token}'})
                if resp.status_code == 200 and resp.content:
                    success_df = pd.read_csv(io.StringIO(resp.text))
                    success_path = OUTPUT_DIR / f'created_ids_chunk{i//chunk_size+1}_{datetime.now().strftime("%Y%m%d")}.csv'
                    success_df.to_csv(success_path, index=False, encoding='utf-8-sig')
                    print(f"    作成ID保存: {success_path.name}")

                # 失敗レコード確認
                if failed > 0:
                    fail_url = f"{sf_config.INSTANCE_URL}/services/data/{sf_config.API_VERSION}/jobs/ingest/{job_id}/failedResults"
                    resp = session.get(fail_url, headers={'Authorization': f'Bearer {access_token}'})
                    if resp.status_code == 200 and resp.content:
                        fail_df = pd.read_csv(io.StringIO(resp.text))
                        fail_path = OUTPUT_DIR / f'failed_chunk{i//chunk_size+1}_{datetime.now().strftime("%Y%m%d")}.csv'
                        fail_df.to_csv(fail_path, index=False, encoding='utf-8-sig')
                        print(f"    失敗レコード: {fail_path.name}")
                        # エラー内容表示
                        if 'sf__Error' in fail_df.columns:
                            for err, cnt in fail_df['sf__Error'].value_counts().head(5).items():
                                print(f"      {err}: {cnt}件")
                break
            elif state in ['Failed', 'Aborted']:
                raise Exception(f"Job失敗: {state} - {info.get('errorMessage', '')}")

            if time.time() - start > 600:
                raise Exception("タイムアウト")

            print(f"    処理中... ({processed:,}件)")
            time.sleep(5)

    # サマリー
    total_processed = sum(r['processed'] for r in all_results)
    total_failed = sum(r['failed'] for r in all_results)
    print(f"\n{'='*60}")
    print(f"インポート完了")
    print(f"{'='*60}")
    print(f"  処理合計: {total_processed:,}件")
    print(f"  成功:     {total_processed - total_failed:,}件")
    print(f"  失敗:     {total_failed:,}件")

    return all_results


def main():
    print("=" * 70)
    print("ハローワーク セグメントC〜F 新規リード Salesforceインポート")
    print(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    # STEP 1: CSV生成
    csv_path = generate_import_csv(dry_run=False)
    if not csv_path:
        return

    # STEP 2: Bulk Insert
    results = bulk_insert(csv_path, dry_run=False)


if __name__ == '__main__':
    main()
