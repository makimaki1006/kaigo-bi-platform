"""ハローワーク新規リード レポート作成（2026-02-27）"""
import sys
import io
import requests

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.path.insert(0, '.')
sys.path.insert(0, 'src')

from api.salesforce_client import SalesforceClient

BATCH_ID = 'BATCH_20260227_HW'

OWNERS = {
    '小林幸太': '005J3000000ERz4IAG',
    '藤巻真弥': '0055i00000BeOKbAAN',
    '服部翔太郎': '005J3000000EYYjIAO',
    '深堀勇侍': '0055i00000CwKEhAAN',
}


def create_report(client, name, description, filters, folder_id=None):
    """Salesforceレポートを作成"""
    url = f'{client.instance_url}/services/data/v59.0/analytics/reports'
    headers = client._get_headers()

    detail_columns = [
        'LEAD.OWNER_FULL_NAME',
        'COMPANY',
        'Lead.LastName',
        'PHONE1',
        'Lead.MobilePhone',
        'Lead.Street',
        'Lead.Prefecture__c',
        'Lead.NumberOfEmployees',
        'Lead.Website',
        'Lead.Hellowork_Industry__c',
        'Lead.Hellowork_RecuritmentType__c',
        'Lead.Hellowork_EmploymentType__c',
        'Lead.Hellowork_RecruitmentReasonCategory__c',
        'Lead.Hellowork_NumberOfRecruitment__c',
        'Lead.Hellowork_DataImportDate__c',
        'Lead.Status',
        'Lead.Publish_ImportText__c',
    ]

    report_metadata = {
        'reportMetadata': {
            'name': name,
            'description': description,
            'reportFormat': 'TABULAR',
            'reportType': {'type': 'LeadList'},
            'detailColumns': detail_columns,
            'reportFilters': filters,
        }
    }

    resp = requests.post(url, headers=headers, json=report_metadata)
    if resp.status_code in (200, 201):
        report_id = resp.json()['reportMetadata']['id']
        report_url = f'{client.instance_url}/lightning/r/Report/{report_id}/view'
        print(f'  OK: {name}')
        print(f'      {report_url}')
        return report_id, report_url
    else:
        print(f'  NG: {name} -> {resp.status_code} {resp.text[:200]}')
        return None, None


def main():
    client = SalesforceClient()
    client.authenticate()

    results = []

    # --- 各所有者のレポート ---
    for owner_name, owner_id in OWNERS.items():
        name = f'HW新規リード_{owner_name}_20260227'
        desc = f'ハローワーク新規リード（{owner_name}担当分）バッチ: {BATCH_ID}'
        filters = [
            {
                'column': 'Lead.Hellowork_DataImportDate__c',
                'operator': 'equals',
                'value': '2026-02-27',
            },
            {
                'column': 'Lead.LeadSource',
                'operator': 'equals',
                'value': 'ハローワーク',
            },
            {
                'column': 'LEAD.OWNER_FULL_NAME',
                'operator': 'equals',
                'value': owner_name,
            },
            {
                'column': 'Lead.Status',
                'operator': 'equals',
                'value': '未架電',
            },
        ]
        report_id, report_url = create_report(client, name, desc, filters)
        results.append((name, report_id, report_url))

    # --- 代表携帯番号レポート（服部・深堀分） ---
    # 代表直通+携帯 → Phone = MobilePhone（携帯がPhoneにも入っている）
    # フィルタ: 携帯あり + 服部 or 深堀
    name = f'HW新規リード_代表携帯_服部深堀_20260227'
    desc = f'ハローワーク新規リード（代表直通+携帯番号 服部・深堀分）バッチ: {BATCH_ID}'
    filters = [
        {
            'column': 'Lead.Hellowork_DataImportDate__c',
            'operator': 'equals',
            'value': '2026-02-27',
        },
        {
            'column': 'Lead.LeadSource',
            'operator': 'equals',
            'value': 'ハローワーク',
        },
        {
            'column': 'Lead.MobilePhone',
            'operator': 'notEqual',
            'value': '',
        },
        {
            'column': 'LEAD.OWNER_FULL_NAME',
            'operator': 'equals',
            'value': '服部翔太郎,深堀勇侍',
        },
        {
            'column': 'Lead.Status',
            'operator': 'equals',
            'value': '未架電',
        },
    ]
    report_id, report_url = create_report(client, name, desc, filters)
    results.append((name, report_id, report_url))

    # --- サマリー ---
    print(f'\n{"="*60}')
    print(f'レポート作成完了')
    print(f'{"="*60}')
    for name, rid, rurl in results:
        status = 'OK' if rid else 'NG'
        print(f'  [{status}] {name}')
        if rurl:
            print(f'       {rurl}')


if __name__ == '__main__':
    main()
