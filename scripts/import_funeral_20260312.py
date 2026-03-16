"""
ハローワーク 葬儀系リード インポート
====================================
- new_leads_E_葬儀.csv（C-Fパイプライン出力）から葬儀リードを作成
- 品質フィルタ再適用（パート除外、従業員11-150）
- SFフィールドマッピング + データクレンジング
- 小林幸太に全件割り当て
- Bulk API 2.0インポート
"""

import sys
sys.stdout.reconfigure(encoding='utf-8')

import pandas as pd
import re
import time
import io
import requests
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'src'))
from src.api.salesforce_client import SalesforceClient

BASE_DIR = Path(r'C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List')
DATA_DIR = BASE_DIR / 'data' / 'output' / 'hellowork_segments'
INPUT_FILE = DATA_DIR / 'matched' / 'new_leads_E_葬儀.csv'
TODAY = datetime.now().strftime('%Y%m%d')
TODAY_ISO = datetime.now().strftime('%Y-%m-%d')

OWNER = {'name': '小林幸太', 'id': '005J3000000ERz4IAG'}


def normalize_phone(p):
    """電話番号正規化: ハイフン除去、先頭ゼロ補完"""
    s = str(p).strip()
    if s in ('', 'nan', 'None'):
        return ''
    digits = re.sub(r'[^0-9]', '', s)
    if len(digits) == 9:
        digits = '0' + digits
    if len(digits) in (10, 11) and digits.startswith('0'):
        return digits
    return ''


def is_mobile(phone):
    """携帯番号判定"""
    return bool(re.match(r'^0[789]0', str(phone)))


def clean_corp_number(val):
    """法人番号クレンジング: .0除去、13桁チェック"""
    s = str(val).strip()
    if s in ('', 'nan', 'None'):
        return ''
    s = re.sub(r'\.0$', '', s)
    s = re.sub(r'[^\d]', '', s)
    return s if len(s) == 13 else ''


def clean_lastname(name):
    """LastNameバリデーション: スペース・記号含むなら「担当者」"""
    s = str(name).strip()
    if s in ('', 'nan', 'None'):
        return '担当者'
    # 全角スペースで姓名分割されている場合、姓のみ取得
    parts = re.split(r'[\s　]+', s)
    s = parts[0] if parts else s
    if not s or re.search(r'[!-/:-@\[-`{-~]', s):
        return '担当者'
    return s


def build_memo(row):
    """Publish_ImportText__cメモ生成"""
    lines = [f'【葬儀系新規】ハローワーク {TODAY_ISO}']
    if row.get('職種', ''):
        lines.append(f'職種: {row["職種"]}')
    if row.get('産業分類（名称）', ''):
        lines.append(f'産業: {row["産業分類（名称）"]}')
    if row.get('事業内容', ''):
        content = str(row['事業内容'])[:200]
        lines.append(f'事業内容: {content}')
    if row.get('仕事内容', ''):
        job = str(row['仕事内容'])[:200]
        lines.append(f'仕事内容: {job}')
    if row.get('近接ランク', ''):
        lines.append(f'決裁者近接度: {row["近接ランク"]}')
    memo = '\n'.join(lines)
    # SF Publish_ImportText__c は長文対応だが念のため制限
    return memo[:2000]


def main():
    print('=' * 60)
    print('ハローワーク 葬儀系リード インポート')
    print(f'実行日: {TODAY_ISO}')
    print('=' * 60)

    # === データ読み込み ===
    df = pd.read_csv(INPUT_FILE, encoding='utf-8-sig', dtype=str)
    df = df.fillna('')
    print(f'\n入力: {len(df)}件（new_leads_E_葬儀.csv）')

    # === 品質フィルタ ===
    print('\n--- 品質フィルタ ---')
    before = len(df)

    # パート除外
    part_mask = df['雇用形態'].str.contains('パート', na=False)
    df = df[~part_mask]
    print(f'  パート除外: {part_mask.sum()}件除外 → {len(df)}件')

    # 従業員数 11-150
    df['_emp'] = pd.to_numeric(df['従業員数_数値'], errors='coerce').fillna(0).astype(int)
    emp_mask = (df['_emp'] >= 11) & (df['_emp'] <= 150)
    excluded_emp = (~emp_mask).sum()
    df = df[emp_mask]
    print(f'  従業員数11-150: {excluded_emp}件除外 → {len(df)}件')

    # 電話番号あり
    df['_phone'] = df['電話番号_正規化'].apply(normalize_phone)
    no_phone = (df['_phone'] == '').sum()
    df = df[df['_phone'] != '']
    print(f'  電話番号なし: {no_phone}件除外 → {len(df)}件')

    # 人口フィルタ（50,000以上）
    df['_pop'] = pd.to_numeric(df['市区町村人口_数値'], errors='coerce').fillna(0).astype(int)
    pop_mask = df['_pop'] >= 50000
    excluded_pop = (~pop_mask).sum()
    df = df[pop_mask]
    print(f'  人口5万未満: {excluded_pop}件除外 → {len(df)}件')

    print(f'\n  品質フィルタ後: {len(df)}件（{before}件から{before - len(df)}件除外）')

    if len(df) == 0:
        print('\n品質フィルタ後のレコードが0件です。処理を終了します。')
        return

    # === 近接スコア分布 ===
    print('\n--- 近接スコア分布 ---')
    for rank in ['★★★★★', '★★★★☆', '★★★☆☆', '★★☆☆☆', '★☆☆☆☆']:
        cnt = (df['近接ランク'] == rank).sum()
        if cnt > 0:
            print(f'  {rank}: {cnt}件')

    # === SFフィールドマッピング ===
    print('\n--- SFフィールドマッピング ---')
    sf_df = pd.DataFrame()

    # 必須フィールド
    sf_df['Company'] = df['事業所名漢字'].apply(lambda x: str(x).strip().replace('　', ' '))
    sf_df['LastName'] = df['選考担当者氏名漢字'].apply(clean_lastname)
    sf_df['Phone'] = df['_phone']

    # 携帯電話（Phone/MobilePhone両方に設定）
    sf_df['MobilePhone'] = df['_phone'].apply(lambda p: p if is_mobile(p) else '')

    # 住所
    sf_df['PostalCode'] = df['事業所郵便番号']
    sf_df['Street'] = df['事業所所在地']
    sf_df['Prefecture__c'] = df['都道府県']

    # 会社情報
    sf_df['NumberOfEmployees'] = df['_emp'].astype(str)
    sf_df['CorporateNumber__c'] = df['法人番号_cleaned'].apply(clean_corp_number)
    sf_df['Establish__c'] = df['設立年'].apply(lambda x: str(x).strip() if str(x).strip() not in ('', 'nan') else '')
    sf_df['Website'] = df['事業所ホームページ'].apply(lambda x: str(x).strip() if str(x).strip() not in ('', 'nan', 'None') else '')
    sf_df['Email'] = df['選考担当者Ｅメール'].apply(lambda x: str(x).strip() if str(x).strip() not in ('', 'nan', 'None') else '')

    # 担当者情報
    sf_df['Title'] = df['選考担当者課係名／役職名'].apply(lambda x: str(x).strip() if str(x).strip() not in ('', 'nan') else '')
    sf_df['Name_Kana__c'] = df['事業所名カナ'].apply(lambda x: str(x).strip() if str(x).strip() not in ('', 'nan') else '')

    # 代表者情報
    sf_df['PresidentName__c'] = df['代表者名'].apply(lambda x: str(x).strip().replace('　', ' ') if str(x).strip() not in ('', 'nan') else '')
    sf_df['PresidentTitle__c'] = df['代表者役職'].apply(lambda x: str(x).strip() if str(x).strip() not in ('', 'nan') else '')

    # ハローワーク固有フィールド
    sf_df['Hellowork_Industry__c'] = df['産業分類（名称）']
    sf_df['Hellowork_EmploymentType__c'] = df['雇用形態']
    sf_df['Hellowork_DataImportDate__c'] = TODAY_ISO

    # リード基本
    sf_df['LeadSource'] = 'ハローワーク'
    sf_df['Status'] = '未架電'

    # メモ
    sf_df['Publish_ImportText__c'] = df.apply(build_memo, axis=1)

    # LeadSourceMemo__c
    sf_df['LeadSourceMemo__c'] = df.apply(
        lambda r: f'ハローワーク葬儀系 {TODAY_ISO} {r.get("産業分類（名称）", "")} {r.get("近接ランク", "")}',
        axis=1
    ).apply(lambda x: x[:252] + '...' if len(x) > 255 else x)

    # Company空チェック
    empty_company = (sf_df['Company'].str.strip() == '').sum()
    if empty_company > 0:
        print(f'  Company空: {empty_company}件除外')
        sf_df = sf_df[sf_df['Company'].str.strip() != '']

    print(f'  マッピング完了: {len(sf_df)}件')

    # === Salesforce認証 ===
    client = SalesforceClient()
    client.authenticate()
    headers = client._get_headers()
    api_ver = client.api_version if hasattr(client, 'api_version') else 'v59.0'
    base_url = f'{client.instance_url}/services/data/{api_ver}'
    print('認証成功')

    # === Prefecture__c 有効値取得 ===
    desc_resp = requests.get(f'{base_url}/sobjects/Lead/describe', headers=headers)
    valid_prefs = set()
    if desc_resp.status_code == 200:
        for field in desc_resp.json().get('fields', []):
            if field['name'] == 'Prefecture__c':
                for pv in field.get('picklistValues', []):
                    if pv.get('active'):
                        valid_prefs.add(pv['value'])
                break
    print(f'有効Prefecture: {len(valid_prefs)}件')

    # Prefecture検証
    invalid_pref = ~sf_df['Prefecture__c'].isin(valid_prefs) & (sf_df['Prefecture__c'] != '')
    if invalid_pref.sum() > 0:
        print(f'  Prefecture__c無効値クリア: {invalid_pref.sum()}件')
        sf_df.loc[invalid_pref, 'Prefecture__c'] = ''

    # === 所有者割り当て ===
    print('\n--- 所有者割り当て ---')
    n = len(sf_df)
    sf_df['OwnerId'] = OWNER['id']
    print(f'  {OWNER["name"]}: {n}件（全件）')

    # === インポート確認 ===
    print(f'\n{"=" * 60}')
    print(f'Salesforceインポート確認')
    print(f'{"=" * 60}')
    print(f'対象: Lead（リード）')
    print(f'操作: 新規作成')
    print(f'件数: {len(sf_df)}件')
    print(f'LeadSource: ハローワーク')
    print(f'Status: 未架電')
    print(f'\nサンプル:')
    for _, row in sf_df.head(3).iterrows():
        print(f'  - {row["Company"]} | {row["Phone"]} | {row["Prefecture__c"]} | 従業員{row["NumberOfEmployees"]}名')
    print(f'\n実行します...')

    # === Bulk API 2.0 インポート ===
    print('\n--- Bulk API 2.0 インポート ---')

    job_body = {
        'object': 'Lead',
        'operation': 'insert',
        'contentType': 'CSV',
        'lineEnding': 'CRLF',
    }
    resp = requests.post(f'{base_url}/jobs/ingest', headers=headers, json=job_body)
    resp.raise_for_status()
    job_id = resp.json()['id']
    print(f'  ジョブID: {job_id}')

    # CSV生成（StringIOでUTF-8保証）
    output = io.StringIO()
    sf_df.to_csv(output, index=False)
    csv_str = output.getvalue()

    # UTF-8バイト列でアップロード
    upload_headers = {**headers, 'Content-Type': 'text/csv; charset=UTF-8'}
    resp = requests.put(
        f'{base_url}/jobs/ingest/{job_id}/batches',
        headers=upload_headers,
        data=csv_str.encode('utf-8'),
    )
    resp.raise_for_status()

    resp = requests.patch(
        f'{base_url}/jobs/ingest/{job_id}',
        headers=headers,
        json={'state': 'UploadComplete'},
    )
    resp.raise_for_status()

    print('  処理中', end='', flush=True)
    for _ in range(60):
        time.sleep(5)
        resp = requests.get(f'{base_url}/jobs/ingest/{job_id}', headers=headers)
        status = resp.json()
        state = status.get('state', '')
        if state in ('JobComplete', 'Failed', 'Aborted'):
            break
        print('.', end='', flush=True)

    print()
    processed = int(status.get('numberRecordsProcessed', 0))
    failed = int(status.get('numberRecordsFailed', 0))
    print(f'  状態: {state}')
    print(f'  成功: {processed}件')
    print(f'  失敗: {failed}件')

    # 失敗レコード
    if failed > 0:
        resp = requests.get(f'{base_url}/jobs/ingest/{job_id}/failedResults', headers=headers)
        if resp.status_code == 200:
            failed_path = DATA_DIR / 'import_ready' / f'failed_funeral_{TODAY}.csv'
            with open(failed_path, 'w', encoding='utf-8-sig') as f:
                f.write(resp.text)
            print(f'  失敗詳細: {failed_path}')
            lines = resp.text.strip().split('\n')
            errors = {}
            for line in lines[1:]:
                if 'sf__Error' not in line and '"' in line:
                    parts = line.split('","')
                    if len(parts) >= 2:
                        err = parts[1].split(':')[0] if ':' in parts[1] else parts[1][:50]
                        errors[err] = errors.get(err, 0) + 1
            for err, cnt in sorted(errors.items(), key=lambda x: -x[1])[:5]:
                print(f'    [{cnt}件] {err}')

    # 成功ID保存
    created_ids = []
    if processed > 0:
        resp = requests.get(f'{base_url}/jobs/ingest/{job_id}/successfulResults', headers=headers)
        if resp.status_code == 200:
            lines = resp.text.strip().split('\n')
            for line in lines[1:]:
                parts = line.split(',')
                if parts and parts[0]:
                    clean_id = parts[0].strip('"')
                    if clean_id and len(clean_id) >= 15:
                        created_ids.append(clean_id)

        id_path = DATA_DIR / 'import_ready' / f'created_funeral_ids_{TODAY}.csv'
        pd.DataFrame({'Id': created_ids}).to_csv(id_path, index=False, encoding='utf-8-sig')
        print(f'  作成済みID: {id_path} ({len(created_ids)}件)')

    # === レポート作成 ===
    print('\n--- レポート作成 ---')
    detail_columns = [
        'OWNER', 'COMPANY', 'LAST_NAME', 'PHONE', 'MOBILE_PHONE',
        'STREET', 'Lead.Prefecture__c', 'EMPLOYEES',
        'Lead.Hellowork_Industry__c',
        'Lead.Hellowork_EmploymentType__c',
        'Lead.Hellowork_DataImportDate__c',
        'STATUS', 'Lead.Publish_ImportText__c',
    ]

    base_filters = [
        {'column': 'Lead.Hellowork_DataImportDate__c', 'operator': 'equals', 'value': TODAY_ISO},
        {'column': 'STATUS', 'operator': 'equals', 'value': '未架電'},
        {'column': 'Lead.Hellowork_Industry__c', 'operator': 'contains', 'value': '葬祭'},
    ]

    def create_report(name, filters):
        report_metadata = {
            'reportMetadata': {
                'name': name,
                'reportFormat': 'TABULAR',
                'reportType': {'type': 'LeadList'},
                'detailColumns': detail_columns,
                'reportFilters': filters,
            }
        }
        resp = requests.post(f'{base_url}/analytics/reports', headers=headers, json=report_metadata)
        if resp.status_code in (200, 201):
            report_id = resp.json()['reportMetadata']['id']
            url = f'{client.instance_url}/lightning/r/Report/{report_id}/view'
            print(f'  OK: {name} -> {url}')
            return url
        else:
            print(f'  NG: {name} ({resp.status_code}) {resp.text[:200]}')
            return None

    report_all = create_report(f'ハロワ葬儀新規_{TODAY_ISO}_{OWNER["name"]}', base_filters)

    # === サマリー ===
    print('\n' + '=' * 60)
    print('完了サマリー')
    print('=' * 60)
    print(f'  入力: 475件（new_leads_E_葬儀.csv）')
    print(f'  品質フィルタ後: {n}件')
    print(f'  インポート成功: {len(created_ids)}件')
    print(f'  インポート失敗: {failed}件')
    print(f'\n  レポート:')
    if report_all:
        print(f'    {OWNER["name"]}: {report_all}')


if __name__ == '__main__':
    main()
