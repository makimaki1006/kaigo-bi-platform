"""
Salesforce データ抽出スクリプト
Account, Contact, Lead オブジェクトを全フィールドでCSV出力
"""

import sys
from pathlib import Path

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.api.salesforce_client import SalesforceClient


def main():
    """メイン実行関数"""
    print("=" * 50)
    print("Salesforce データ抽出ツール")
    print("=" * 50)

    # クライアント初期化
    client = SalesforceClient()

    # 認証
    client.authenticate()

    # 抽出対象オブジェクト
    target_objects = ['Account', 'Contact', 'Lead']

    print(f"\n📋 抽出対象: {', '.join(target_objects)}")
    print("-" * 50)

    # 一括抽出実行
    exported_files = client.export_multiple_objects(target_objects)

    # 結果サマリー
    print("\n" + "=" * 50)
    print("📊 抽出結果サマリー")
    print("=" * 50)
    for file_path in exported_files:
        print(f"  - {file_path}")

    print(f"\n✅ 合計 {len(exported_files)} ファイルを出力しました")


if __name__ == "__main__":
    main()
