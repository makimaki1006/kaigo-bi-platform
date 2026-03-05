"""
株式会社・社福の架電可能リスト件数詳細調査スクリプト

対象: Account（九州沖縄地域）
条件: 架電可能条件（ステータス除外、アプローチNG除外、電話番号あり）
"""

import sys
from pathlib import Path

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import requests
from src.utils.config import sf_config

class SalesforceQuery:
    """Salesforce SOQLクエリ実行用クラス"""

    def __init__(self):
        self.instance_url = sf_config.INSTANCE_URL
        self.api_version = sf_config.API_VERSION
        self.access_token = None

    def authenticate(self):
        """認証"""
        token_url = f"{self.instance_url}/services/oauth2/token"
        payload = {
            'grant_type': 'refresh_token',
            'client_id': sf_config.CLIENT_ID,
            'client_secret': sf_config.CLIENT_SECRET,
            'refresh_token': sf_config.REFRESH_TOKEN
        }
        response = requests.post(token_url, data=payload)
        response.raise_for_status()
        self.access_token = response.json()['access_token']
        print("[OK] 認証成功")

    def query(self, soql: str) -> dict:
        """SOQLクエリ実行"""
        if not self.access_token:
            self.authenticate()

        url = f"{self.instance_url}/services/data/{self.api_version}/query"
        headers = {'Authorization': f'Bearer {self.access_token}'}
        params = {'q': soql}

        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()

    def count(self, soql: str) -> int:
        """件数取得"""
        result = self.query(soql)
        if result.get('records') and len(result['records']) > 0:
            return result['records'][0].get('cnt', result['records'][0].get('expr0', 0))
        return 0


def main():
    sf = SalesforceQuery()
    sf.authenticate()

    # 九州沖縄の条件
    kyushu_condition = "Prefectures__c IN ('福岡県', '佐賀県', '長崎県', '熊本県', '大分県', '宮崎県', '鹿児島県', '沖縄県')"

    # 架電可能条件
    callable_condition = """
        (Status__c = null OR (
            (NOT Status__c LIKE '%商談中%')
            AND (NOT Status__c LIKE '%プロジェクト進行中%')
            AND (NOT Status__c LIKE '%深耕対象%')
            AND (NOT Status__c LIKE '%過去客%')
        ))
        AND (ApproachNG__c = false OR ApproachNG__c = null)
        AND (CallNotApplicable__c = false OR CallNotApplicable__c = null)
        AND Phone != null
    """

    # 通所除外条件
    tsusho_exclude = "(NOT ServiceType__c LIKE '%通所%')"

    # 株式会社条件
    kabushiki_condition = "Name LIKE '%株式会社%'"

    # 社福条件
    shafu_condition = "(Name LIKE '%社会福祉法人%' OR Name LIKE '%社福%')"

    # 代表者名あり条件
    president_condition = "PresidentName__c != null"

    print("\n" + "="*80)
    print("九州沖縄 架電可能リスト件数調査")
    print("="*80)

    # ========================================
    # 1. 株式会社 × 代表者名あり × 通所除外 × 従業員数帯別
    # ========================================
    print("\n### 1. 株式会社 × 代表者名あり × 通所除外 × 従業員数帯別\n")

    base_kabushiki = f"""
        FROM Account
        WHERE {kyushu_condition}
        AND {callable_condition}
        AND {kabushiki_condition}
        AND {president_condition}
        AND {tsusho_exclude}
    """

    employee_ranges = [
        ("1-10名", "NumberOfEmployees >= 1 AND NumberOfEmployees <= 10"),
        ("11-30名", "NumberOfEmployees >= 11 AND NumberOfEmployees <= 30"),
        ("31-50名", "NumberOfEmployees >= 31 AND NumberOfEmployees <= 50"),
        ("51-100名", "NumberOfEmployees >= 51 AND NumberOfEmployees <= 100"),
        ("101-300名", "NumberOfEmployees >= 101 AND NumberOfEmployees <= 300"),
        ("301名以上", "NumberOfEmployees >= 301"),
        ("未設定", "NumberOfEmployees = null"),
    ]

    print("| 従業員数帯 | 件数 |")
    print("|-----------|------|")

    total_kabushiki = 0
    for label, condition in employee_ranges:
        soql = f"SELECT COUNT(Id) cnt {base_kabushiki} AND ({condition})"
        count = sf.count(soql)
        print(f"| {label} | {count:,} |")
        total_kabushiki += count

    # 合計
    soql_total = f"SELECT COUNT(Id) cnt {base_kabushiki}"
    total_from_query = sf.count(soql_total)
    print(f"| **合計** | **{total_from_query:,}** |")

    # ========================================
    # 2. 社福 × 代表者名あり × 通所除外 × 従業員数帯別
    # ========================================
    print("\n### 2. 社会福祉法人 × 代表者名あり × 通所除外 × 従業員数帯別\n")

    base_shafu = f"""
        FROM Account
        WHERE {kyushu_condition}
        AND {callable_condition}
        AND {shafu_condition}
        AND {president_condition}
        AND {tsusho_exclude}
    """

    print("| 従業員数帯 | 件数 |")
    print("|-----------|------|")

    total_shafu = 0
    for label, condition in employee_ranges:
        soql = f"SELECT COUNT(Id) cnt {base_shafu} AND ({condition})"
        count = sf.count(soql)
        print(f"| {label} | {count:,} |")
        total_shafu += count

    # 合計
    soql_total = f"SELECT COUNT(Id) cnt {base_shafu}"
    total_from_query = sf.count(soql_total)
    print(f"| **合計** | **{total_from_query:,}** |")

    # ========================================
    # 3. 株式会社 × 代表者名あり × サービス種別（上位10）
    # ========================================
    print("\n### 3. 株式会社 × 代表者名あり × サービス種別（上位10）\n")

    # サービス種別でグループ化
    soql_service = f"""
        SELECT ServiceType__c, COUNT(Id) cnt
        FROM Account
        WHERE {kyushu_condition}
        AND {callable_condition}
        AND {kabushiki_condition}
        AND {president_condition}
        GROUP BY ServiceType__c
        ORDER BY COUNT(Id) DESC
        LIMIT 15
    """

    result = sf.query(soql_service)

    print("| サービス種別 | 件数 |")
    print("|-------------|------|")

    count_shown = 0
    for record in result.get('records', []):
        service_type = record.get('ServiceType__c') or '(未設定)'
        cnt = record.get('cnt', 0)
        print(f"| {service_type} | {cnt:,} |")
        count_shown += 1
        if count_shown >= 10:
            break

    # ========================================
    # 4. 社福 × 代表者名あり × サービス種別（上位10）
    # ========================================
    print("\n### 4. 社会福祉法人 × 代表者名あり × サービス種別（上位10）\n")

    soql_service = f"""
        SELECT ServiceType__c, COUNT(Id) cnt
        FROM Account
        WHERE {kyushu_condition}
        AND {callable_condition}
        AND {shafu_condition}
        AND {president_condition}
        GROUP BY ServiceType__c
        ORDER BY COUNT(Id) DESC
        LIMIT 15
    """

    result = sf.query(soql_service)

    print("| サービス種別 | 件数 |")
    print("|-------------|------|")

    count_shown = 0
    for record in result.get('records', []):
        service_type = record.get('ServiceType__c') or '(未設定)'
        cnt = record.get('cnt', 0)
        print(f"| {service_type} | {cnt:,} |")
        count_shown += 1
        if count_shown >= 10:
            break

    # ========================================
    # サマリー
    # ========================================
    print("\n" + "="*80)
    print("サマリー")
    print("="*80)

    # 全体件数（参考）
    soql_all = f"""
        SELECT COUNT(Id) cnt
        FROM Account
        WHERE {kyushu_condition}
        AND {callable_condition}
    """
    total_all = sf.count(soql_all)

    soql_kabushiki_all = f"SELECT COUNT(Id) cnt {base_kabushiki}"
    total_kabushiki_all = sf.count(soql_kabushiki_all)

    soql_shafu_all = f"SELECT COUNT(Id) cnt {base_shafu}"
    total_shafu_all = sf.count(soql_shafu_all)

    print(f"\n九州沖縄 架電可能Account全体: {total_all:,}件")
    print(f"  - 株式会社 × 代表者名あり × 通所除外: {total_kabushiki_all:,}件")
    print(f"  - 社福 × 代表者名あり × 通所除外: {total_shafu_all:,}件")


if __name__ == "__main__":
    main()
