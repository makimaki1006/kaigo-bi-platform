"""
代表者情報付きリードの件数調査スクリプト
"""

import sys
from pathlib import Path

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import requests
from src.utils.config import sf_config


class SalesforceQuery:
    """Salesforce REST API Query実行クラス"""

    def __init__(self):
        self.instance_url = sf_config.INSTANCE_URL
        self.api_version = sf_config.API_VERSION
        self.access_token = None
        self.session = requests.Session()

    def authenticate(self) -> str:
        """OAuth認証"""
        token_url = f"{self.instance_url}/services/oauth2/token"
        payload = {
            'grant_type': 'refresh_token',
            'client_id': sf_config.CLIENT_ID,
            'client_secret': sf_config.CLIENT_SECRET,
            'refresh_token': sf_config.REFRESH_TOKEN
        }

        response = self.session.post(token_url, data=payload)
        response.raise_for_status()

        self.access_token = response.json()['access_token']
        print("Salesforce認証成功\n")
        return self.access_token

    def query(self, soql: str) -> dict:
        """SOQLクエリ実行"""
        url = f"{self.instance_url}/services/data/{self.api_version}/query"
        headers = {'Authorization': f'Bearer {self.access_token}'}
        params = {'q': soql}

        response = self.session.get(url, headers=headers, params=params)

        if response.status_code != 200:
            print(f"   エラー詳細: {response.text}")

        response.raise_for_status()

        return response.json()


def main():
    # 認証
    sf = SalesforceQuery()
    sf.authenticate()

    print("=" * 60)
    print("代表者情報付きリード調査結果")
    print("=" * 60)

    # クエリ1: PresidentName__c入力済みの全リード件数
    print("\n■ 1. PresidentName__c入力済みの全リード件数")
    soql1 = "SELECT COUNT(Id) cnt FROM Lead WHERE PresidentName__c != null AND IsConverted = false AND Phone != null"
    print(f"   SOQL: {soql1}")
    result1 = sf.query(soql1)
    cnt1 = result1['records'][0]['cnt']
    print(f"   結果: {cnt1:,} 件")

    # クエリ2: 九州沖縄×代表者名入力済み
    print("\n■ 2. 九州沖縄×代表者名入力済み")
    soql2 = """SELECT COUNT(Id) cnt FROM Lead
               WHERE PresidentName__c != null
               AND IsConverted = false
               AND Phone != null
               AND Prefecture__c IN ('福岡県','佐賀県','長崎県','熊本県','大分県','宮崎県','鹿児島県','沖縄県')"""
    soql2 = ' '.join(soql2.split())  # 改行・余分な空白を除去
    print(f"   SOQL: {soql2}")
    result2 = sf.query(soql2)
    cnt2 = result2['records'][0]['cnt']
    print(f"   結果: {cnt2:,} 件")

    # クエリ3: 株式会社×代表者名入力済み×九州
    print("\n■ 3. 株式会社×代表者名入力済み×九州")
    soql3 = """SELECT COUNT(Id) cnt FROM Lead
               WHERE LegalPersonality__c = '株式会社'
               AND PresidentName__c != null
               AND IsConverted = false
               AND Phone != null
               AND Prefecture__c IN ('福岡県','佐賀県','長崎県','熊本県','大分県','宮崎県','鹿児島県','沖縄県')"""
    soql3 = ' '.join(soql3.split())
    print(f"   SOQL: {soql3}")
    result3 = sf.query(soql3)
    cnt3 = result3['records'][0]['cnt']
    print(f"   結果: {cnt3:,} 件")

    # クエリ4: 各法人格×代表者名入力済み×九州の内訳
    print("\n■ 4. 法人格別内訳（代表者名入力済み×九州）")

    # LegalPersonality__cはGROUP BY不可のため、主要な法人格を個別にクエリ
    legal_types = [
        '株式会社',
        '有限会社',
        '合同会社',
        '医療法人',
        '社会福祉法人',
        '一般社団法人',
        '一般財団法人',
        '公益社団法人',
        '公益財団法人',
        '学校法人',
        '宗教法人',
        'NPO法人',
        '合資会社',
        '合名会社',
    ]

    base_where = """PresidentName__c != null
                    AND IsConverted = false
                    AND Phone != null
                    AND Prefecture__c IN ('福岡県','佐賀県','長崎県','熊本県','大分県','宮崎県','鹿児島県','沖縄県')"""
    base_where = ' '.join(base_where.split())

    results = []
    for legal_type in legal_types:
        soql = f"SELECT COUNT(Id) cnt FROM Lead WHERE LegalPersonality__c = '{legal_type}' AND {base_where}"
        try:
            result = sf.query(soql)
            cnt = result['records'][0]['cnt']
            if cnt > 0:
                results.append({'legal': legal_type, 'cnt': cnt})
        except Exception as e:
            print(f"   警告: {legal_type} のクエリでエラー: {e}")

    # 空白（法人格未設定）もカウント
    soql_null = f"SELECT COUNT(Id) cnt FROM Lead WHERE (LegalPersonality__c = null OR LegalPersonality__c = '') AND {base_where}"
    try:
        result_null = sf.query(soql_null)
        cnt_null = result_null['records'][0]['cnt']
        if cnt_null > 0:
            results.append({'legal': '(空白)', 'cnt': cnt_null})
    except Exception as e:
        print(f"   警告: 空白のクエリでエラー: {e}")

    # 件数でソート
    results_sorted = sorted(results, key=lambda x: x['cnt'], reverse=True)

    print("\n   法人格別件数:")
    print("   " + "-" * 40)
    print(f"   {'法人格':<20} | {'件数':>10}")
    print("   " + "-" * 40)

    total = 0
    for record in results_sorted:
        print(f"   {record['legal']:<20} | {record['cnt']:>10,}")
        total += record['cnt']

    print("   " + "-" * 40)
    print(f"   {'合計':<20} | {total:>10,}")

    # サマリーテーブル
    print("\n" + "=" * 60)
    print("サマリー")
    print("=" * 60)
    print(f"""
| 条件 | 件数 |
|------|-----:|
| 代表者名入力済み（全国） | {cnt1:,} 件 |
| 代表者名入力済み（九州沖縄） | {cnt2:,} 件 |
| 株式会社×代表者名入力済み（九州沖縄） | {cnt3:,} 件 |
""")

    print("\n処理完了")


if __name__ == "__main__":
    main()
