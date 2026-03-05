# -*- coding: utf-8 -*-
"""
アタックリスト用Account抽出
優先度S/A/B/CのセグメントをBulk APIで抽出
"""

import sys
import io
from pathlib import Path
from datetime import datetime

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.services.opportunity_service import OpportunityService
import pandas as pd


def main():
    service = OpportunityService()
    service.authenticate()

    print('='*80)
    print('アタックリスト用Account抽出（Bulk API）')
    print('='*80)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_dir = project_root / 'data' / 'output' / 'attack_list'
    output_dir.mkdir(parents=True, exist_ok=True)

    # 各セグメントのSOQL
    segments = {
        'S_株式会社_訪問系': """
            SELECT Id, Name, LegalPersonality__c, ServiceType__c, Prefectures__c, Phone,
                   Owner.Name, RecordType.Name
            FROM Account
            WHERE LegalPersonality__c = '株式会社'
              AND ServiceType__c LIKE '%訪問%'
        """,
        'A_株式会社_その他': """
            SELECT Id, Name, LegalPersonality__c, ServiceType__c, Prefectures__c, Phone,
                   Owner.Name, RecordType.Name
            FROM Account
            WHERE LegalPersonality__c = '株式会社'
              AND (NOT ServiceType__c LIKE '%訪問%')
        """,
        'B_その他法人_訪問通所': """
            SELECT Id, Name, LegalPersonality__c, ServiceType__c, Prefectures__c, Phone,
                   Owner.Name, RecordType.Name
            FROM Account
            WHERE LegalPersonality__c != '株式会社'
              AND LegalPersonality__c != null
              AND (ServiceType__c LIKE '%訪問%' OR ServiceType__c LIKE '%通所%')
        """,
        'C_その他法人_入所等': """
            SELECT Id, Name, LegalPersonality__c, ServiceType__c, Prefectures__c, Phone,
                   Owner.Name, RecordType.Name
            FROM Account
            WHERE LegalPersonality__c != '株式会社'
              AND LegalPersonality__c != null
              AND (NOT ServiceType__c LIKE '%訪問%')
              AND (NOT ServiceType__c LIKE '%通所%')
        """
    }

    results = {}
    for segment_name, soql in segments.items():
        print(f'\n■ {segment_name} を抽出中...')
        try:
            df = service.bulk_query(soql, segment_name)
            results[segment_name] = df
            print(f'  抽出件数: {len(df):,}件')

            # CSVに保存
            output_path = output_dir / f'attack_list_{segment_name}_{timestamp}.csv'
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
            print(f'  保存: {output_path}')
        except Exception as e:
            print(f'  エラー: {e}')
            results[segment_name] = pd.DataFrame()

    # サマリー
    print('\n' + '='*80)
    print('【抽出結果サマリー】')
    print('='*80)

    total = 0
    print(f'''
┌──────┬───────────────────────┬─────────────┐
│優先度│ 条件                  │ 件数        │
├──────┼───────────────────────┼─────────────┤''')

    for segment_name, df in results.items():
        priority = segment_name[0]
        count = len(df)
        total += count
        condition = segment_name[2:].replace('_', ' × ')
        print(f'│  {priority}   │ {condition:<21} │ {count:>9,}件 │')

    print(f'''├──────┼───────────────────────┼─────────────┤
│ 合計 │                       │ {total:>9,}件 │
└──────┴───────────────────────┴─────────────┘''')

    print(f'\n出力ディレクトリ: {output_dir}')

    # 全件を1つのファイルにまとめる（優先度列を追加）
    print('\n■ 統合ファイルを作成中...')
    all_dfs = []
    for segment_name, df in results.items():
        if len(df) > 0:
            df_copy = df.copy()
            df_copy['優先度'] = segment_name[0]
            df_copy['セグメント'] = segment_name
            all_dfs.append(df_copy)

    if all_dfs:
        combined_df = pd.concat(all_dfs, ignore_index=True)
        combined_path = output_dir / f'attack_list_all_{timestamp}.csv'
        combined_df.to_csv(combined_path, index=False, encoding='utf-8-sig')
        print(f'  統合ファイル: {combined_path}')
        print(f'  全件数: {len(combined_df):,}件')

    return results


if __name__ == "__main__":
    results = main()
