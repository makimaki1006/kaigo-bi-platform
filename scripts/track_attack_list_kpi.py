# -*- coding: utf-8 -*-
"""
アタックリスト KPI追跡スクリプト
セグメント別の成約率・タイプC発生率を月次で追跡
"""

import sys
import io
from pathlib import Path
from datetime import datetime, timedelta
import requests

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.services.opportunity_service import OpportunityService


def main():
    service = OpportunityService()
    service.authenticate()

    print('='*80)
    print('アタックリスト KPI追跡レポート')
    print(f'生成日時: {datetime.now().strftime("%Y-%m-%d %H:%M")}')
    print('='*80)

    headers = {
        'Authorization': f'Bearer {service.access_token}',
        'Content-Type': 'application/json'
    }

    def run_query(soql):
        url = f"{service.instance_url}/services/data/{service.api_version}/query"
        params = {'q': soql}
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Query failed: {response.text}")

    # 架電可能条件
    callable_condition = """
        (Account.Status__c = null OR (
            (NOT Account.Status__c LIKE '%商談中%')
            AND (NOT Account.Status__c LIKE '%プロジェクト進行中%')
            AND (NOT Account.Status__c LIKE '%深耕対象%')
            AND (NOT Account.Status__c LIKE '%過去客%')
        ))
        AND (Account.RelatedAccountFlg__c = null OR (
            Account.RelatedAccountFlg__c != 'グループ案件進行中'
            AND Account.RelatedAccountFlg__c != 'グループ過去案件実績あり'
        ))
        AND (Account.ApproachNG__c = false OR Account.ApproachNG__c = null)
        AND (Account.CallNotApplicable__c = false OR Account.CallNotApplicable__c = null)
        AND Account.Phone != null
    """

    # セグメント定義
    segments = {
        'S_中規模': {
            'priority': 'S',
            'employee': '中規模',
            'condition': """
                Account.LegalPersonality__c = '株式会社'
                AND Account.ServiceType__c LIKE '%訪問%'
                AND Account.NumberOfEmployees > 50
                AND Account.NumberOfEmployees <= 200
            """
        },
        'A_中規模': {
            'priority': 'A',
            'employee': '中規模',
            'condition': """
                Account.LegalPersonality__c = '株式会社'
                AND (NOT Account.ServiceType__c LIKE '%訪問%')
                AND Account.NumberOfEmployees > 50
                AND Account.NumberOfEmployees <= 200
            """
        },
        'S_小規模': {
            'priority': 'S',
            'employee': '小規模',
            'condition': """
                Account.LegalPersonality__c = '株式会社'
                AND Account.ServiceType__c LIKE '%訪問%'
                AND Account.NumberOfEmployees <= 50
            """
        },
        'A_小規模': {
            'priority': 'A',
            'employee': '小規模',
            'condition': """
                Account.LegalPersonality__c = '株式会社'
                AND (NOT Account.ServiceType__c LIKE '%訪問%')
                AND Account.NumberOfEmployees <= 50
            """
        },
        'S_大規模': {
            'priority': 'S',
            'employee': '大規模',
            'condition': """
                Account.LegalPersonality__c = '株式会社'
                AND Account.ServiceType__c LIKE '%訪問%'
                AND Account.NumberOfEmployees > 200
            """
        },
        'A_大規模': {
            'priority': 'A',
            'employee': '大規模',
            'condition': """
                Account.LegalPersonality__c = '株式会社'
                AND (NOT Account.ServiceType__c LIKE '%訪問%')
                AND Account.NumberOfEmployees > 200
            """
        },
        'B_中規模': {
            'priority': 'B',
            'employee': '中規模',
            'condition': """
                Account.LegalPersonality__c != '株式会社'
                AND Account.LegalPersonality__c != null
                AND (Account.ServiceType__c LIKE '%訪問%' OR Account.ServiceType__c LIKE '%通所%')
                AND Account.NumberOfEmployees > 50
                AND Account.NumberOfEmployees <= 200
            """
        },
        'C_中規模': {
            'priority': 'C',
            'employee': '中規模',
            'condition': """
                Account.LegalPersonality__c != '株式会社'
                AND Account.LegalPersonality__c != null
                AND (NOT Account.ServiceType__c LIKE '%訪問%')
                AND (NOT Account.ServiceType__c LIKE '%通所%')
                AND Account.NumberOfEmployees > 50
                AND Account.NumberOfEmployees <= 200
            """
        }
    }

    # 期間設定（過去12ヶ月）
    today = datetime.now()
    months = []
    for i in range(12, 0, -1):
        month_start = (today.replace(day=1) - timedelta(days=i*30)).replace(day=1)
        months.append(month_start.strftime('%Y-%m'))

    # 当月も追加
    months.append(today.strftime('%Y-%m'))

    print('\n' + '='*80)
    print('【1. セグメント別 成約率（当月）】')
    print('='*80)

    # 当月のデータ取得
    current_month_start = today.replace(day=1).strftime('%Y-%m-%dT00:00:00Z')

    results = {}
    for seg_name, seg_info in segments.items():
        try:
            # 商談数と成約数を取得
            soql = f"""
                SELECT
                    COUNT(Id) total
                FROM Opportunity
                WHERE {seg_info['condition']}
                AND {callable_condition}
                AND CreatedDate >= {current_month_start}
                AND IsClosed = true
            """
            result = run_query(soql)
            total = result['records'][0]['total'] if result['records'] else 0

            # 成約数
            soql_won = f"""
                SELECT
                    COUNT(Id) won
                FROM Opportunity
                WHERE {seg_info['condition']}
                AND {callable_condition}
                AND CreatedDate >= {current_month_start}
                AND IsWon = true
            """
            result_won = run_query(soql_won)
            won = result_won['records'][0]['won'] if result_won['records'] else 0

            # タイプC発生数
            soql_type_c = f"""
                SELECT
                    COUNT(Id) type_c
                FROM Opportunity
                WHERE {seg_info['condition']}
                AND {callable_condition}
                AND CreatedDate >= {current_month_start}
                AND Hearing_Authority__c = 'あり'
            """
            result_type_c = run_query(soql_type_c)
            type_c = result_type_c['records'][0]['type_c'] if result_type_c['records'] else 0

            win_rate = (won / total * 100) if total > 0 else 0
            type_c_rate = (type_c / total * 100) if total > 0 else 0

            results[seg_name] = {
                'total': total,
                'won': won,
                'type_c': type_c,
                'win_rate': win_rate,
                'type_c_rate': type_c_rate
            }
        except Exception as e:
            print(f'  {seg_name}: エラー - {e}')
            results[seg_name] = {'total': 0, 'won': 0, 'type_c': 0, 'win_rate': 0, 'type_c_rate': 0}

    # 結果表示
    print(f'\n期間: {current_month_start[:10]} 〜 現在\n')
    print(f'{"セグメント":<15} {"商談数":>8} {"成約数":>8} {"成約率":>10} {"タイプC":>8} {"タイプC率":>10}')
    print('-' * 70)

    total_all = 0
    won_all = 0
    type_c_all = 0

    for seg_name in ['S_中規模', 'A_中規模', 'S_小規模', 'A_小規模', 'S_大規模', 'A_大規模', 'B_中規模', 'C_中規模']:
        if seg_name in results:
            r = results[seg_name]
            print(f'{seg_name:<15} {r["total"]:>8,} {r["won"]:>8,} {r["win_rate"]:>9.1f}% {r["type_c"]:>8,} {r["type_c_rate"]:>9.1f}%')
            total_all += r['total']
            won_all += r['won']
            type_c_all += r['type_c']

    print('-' * 70)
    overall_win_rate = (won_all / total_all * 100) if total_all > 0 else 0
    overall_type_c_rate = (type_c_all / total_all * 100) if total_all > 0 else 0
    print(f'{"合計":<15} {total_all:>8,} {won_all:>8,} {overall_win_rate:>9.1f}% {type_c_all:>8,} {overall_type_c_rate:>9.1f}%')

    # 過去データ（直近3ヶ月の推移）
    print('\n' + '='*80)
    print('【2. 月次成約率推移（直近3ヶ月）】')
    print('='*80)

    monthly_results = {}

    for i in range(3, 0, -1):
        month_start = (today.replace(day=1) - timedelta(days=i*30)).replace(day=1)
        month_end = (month_start + timedelta(days=32)).replace(day=1)
        month_key = month_start.strftime('%Y-%m')

        month_start_str = month_start.strftime('%Y-%m-%dT00:00:00Z')
        month_end_str = month_end.strftime('%Y-%m-%dT00:00:00Z')

        monthly_results[month_key] = {}

        for seg_name, seg_info in segments.items():
            try:
                # 商談数
                soql = f"""
                    SELECT COUNT(Id) total
                    FROM Opportunity
                    WHERE {seg_info['condition']}
                    AND {callable_condition}
                    AND CreatedDate >= {month_start_str}
                    AND CreatedDate < {month_end_str}
                    AND IsClosed = true
                """
                result = run_query(soql)
                total = result['records'][0]['total'] if result['records'] else 0

                # 成約数
                soql_won = f"""
                    SELECT COUNT(Id) won
                    FROM Opportunity
                    WHERE {seg_info['condition']}
                    AND {callable_condition}
                    AND CreatedDate >= {month_start_str}
                    AND CreatedDate < {month_end_str}
                    AND IsWon = true
                """
                result_won = run_query(soql_won)
                won = result_won['records'][0]['won'] if result_won['records'] else 0

                win_rate = (won / total * 100) if total > 0 else 0
                monthly_results[month_key][seg_name] = {
                    'total': total,
                    'won': won,
                    'win_rate': win_rate
                }
            except Exception as e:
                monthly_results[month_key][seg_name] = {'total': 0, 'won': 0, 'win_rate': 0}

    # 月次推移表示
    print(f'\n{"セグメント":<15}', end='')
    for month_key in sorted(monthly_results.keys()):
        print(f' {month_key:>12}', end='')
    print()
    print('-' * 60)

    for seg_name in ['S_中規模', 'A_中規模', 'S_小規模', 'A_小規模', 'B_中規模', 'C_中規模']:
        print(f'{seg_name:<15}', end='')
        for month_key in sorted(monthly_results.keys()):
            if seg_name in monthly_results[month_key]:
                r = monthly_results[month_key][seg_name]
                if r['total'] > 0:
                    print(f' {r["win_rate"]:>10.1f}%', end='')
                else:
                    print(f' {"N/A":>11}', end='')
            else:
                print(f' {"N/A":>11}', end='')
        print()

    # KPI達成状況
    print('\n' + '='*80)
    print('【3. KPI達成状況（当月）】')
    print('='*80)

    # 推定KPI（ベースライン）
    baseline_kpi = {
        'S_中規模': 8.5,
        'A_中規模': 6.5,
        'S_小規模': 7.2,
        'A_小規模': 5.5,
        'S_大規模': 9.0,
        'A_大規模': 7.0,
        'B_中規模': 4.5,
        'C_中規模': 3.5
    }

    print(f'\n{"セグメント":<15} {"目標成約率":>12} {"実績成約率":>12} {"達成率":>10} {"判定":>6}')
    print('-' * 60)

    for seg_name in ['S_中規模', 'A_中規模', 'S_小規模', 'A_小規模', 'B_中規模', 'C_中規模']:
        if seg_name in results and seg_name in baseline_kpi:
            target = baseline_kpi[seg_name]
            actual = results[seg_name]['win_rate']
            achievement = (actual / target * 100) if target > 0 else 0

            if results[seg_name]['total'] == 0:
                status = '⚪ N/A'
            elif achievement >= 100:
                status = '🟢 達成'
            elif achievement >= 70:
                status = '🟡 注意'
            else:
                status = '🔴 要改善'

            print(f'{seg_name:<15} {target:>10.1f}% {actual:>10.1f}% {achievement:>9.0f}% {status}')

    # アラート
    print('\n' + '='*80)
    print('【4. アラート】')
    print('='*80)

    alerts = []
    for seg_name, r in results.items():
        if seg_name in baseline_kpi:
            target = baseline_kpi[seg_name]
            if r['total'] > 0 and r['win_rate'] < target * 0.5:
                alerts.append(f'🔴 {seg_name}: 成約率が目標の50%未満 ({r["win_rate"]:.1f}% < {target*0.5:.1f}%)')
            if r['total'] > 0 and r['type_c_rate'] < 2:
                alerts.append(f'🟡 {seg_name}: タイプC発生率が2%未満 ({r["type_c_rate"]:.1f}%)')

    if alerts:
        for alert in alerts:
            print(f'  {alert}')
    else:
        print('  アラートなし ✅')

    print('\n' + '='*80)
    print('レポート生成完了')
    print('='*80)

    return results, monthly_results


if __name__ == "__main__":
    results, monthly_results = main()
