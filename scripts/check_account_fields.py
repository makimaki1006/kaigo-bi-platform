"""
Accountオブジェクトのカスタムフィールドを確認
"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import requests
from src.utils.config import sf_config


def check_fields():
    # 認証
    token_url = f"{sf_config.INSTANCE_URL}/services/oauth2/token"
    payload = {
        'grant_type': 'refresh_token',
        'client_id': sf_config.CLIENT_ID,
        'client_secret': sf_config.CLIENT_SECRET,
        'refresh_token': sf_config.REFRESH_TOKEN
    }
    response = requests.post(token_url, data=payload)
    response.raise_for_status()
    access_token = response.json()['access_token']
    print("認証成功")

    headers = {'Authorization': f'Bearer {access_token}'}

    # Accountのフィールド取得
    url = f"{sf_config.INSTANCE_URL}/services/data/{sf_config.API_VERSION}/sobjects/Account/describe"
    response = requests.get(url, headers=headers)
    response.raise_for_status()

    describe = response.json()
    fields = describe['fields']

    print("\n【Accountカスタムフィールド一覧】")
    print("-" * 50)

    # 検索対象キーワード
    keywords = ['corporate', 'type', 'president', 'service', 'employee', 'billing']

    for field in fields:
        name = field['name']
        label = field['label']
        name_lower = name.lower()

        # カスタムフィールドまたはキーワードに一致
        if name.endswith('__c') or any(kw in name_lower for kw in keywords):
            print(f"{name:40} | {label}")

    # Opportunityのフィールドも確認
    print("\n【Opportunityカスタムフィールド一覧】")
    print("-" * 50)

    url = f"{sf_config.INSTANCE_URL}/services/data/{sf_config.API_VERSION}/sobjects/Opportunity/describe"
    response = requests.get(url, headers=headers)
    response.raise_for_status()

    describe = response.json()
    fields = describe['fields']

    opp_keywords = ['hearing', 'authority', 'stage']

    for field in fields:
        name = field['name']
        label = field['label']
        name_lower = name.lower()

        if name.endswith('__c') or any(kw in name_lower for kw in opp_keywords):
            print(f"{name:40} | {label}")


if __name__ == "__main__":
    check_fields()
