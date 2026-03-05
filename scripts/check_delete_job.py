# -*- coding: utf-8 -*-
"""削除ジョブの失敗原因を確認"""
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.path.insert(0, '.')
from src.api.salesforce_client import SalesforceClient
import requests

client = SalesforceClient()
client.authenticate()

# 失敗したジョブの詳細を取得
job_id = '750dc00000XtSrtAAF'
url = f'{client.instance_url}/services/data/{client.api_version}/jobs/ingest/{job_id}'
response = requests.get(url, headers=client._get_headers())
print('ジョブ詳細:')
import json
print(json.dumps(response.json(), indent=2))

# エラー結果を取得
error_url = f'{url}/failedResults'
response = requests.get(error_url, headers=client._get_headers())
print()
print('失敗結果:')
print(response.text[:2000] if response.text else 'なし')
