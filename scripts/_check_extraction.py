# -*- coding: utf-8 -*-
"""担当者名・役職の抽出品質を検証"""
import json
import re
import pandas as pd

NDJSON_PATH = r"C:/Users/fuji1/OneDrive/デスクトップ/pythonスクリプト置き場/miidas_night_result_20260302.ndjson"
OUT = "data/output/miidas_extraction_quality.txt"

records = []
with open(NDJSON_PATH, 'r', encoding='utf-8') as f:
    for line in f:
        line = line.strip()
        if line:
            records.append(json.loads(line))

with open(OUT, 'w', encoding='utf-8') as out:
    out.write("=== ミイダス NDJSON 担当者名・役職 抽出品質レポート ===\n\n")

    # 連絡先フィールドの分析
    has_contact = [r for r in records if r.get('連絡先') and r.get('連絡先') != 'N/A']
    no_contact = [r for r in records if not r.get('連絡先') or r.get('連絡先') == 'N/A']
    out.write(f"連絡先あり: {len(has_contact)}件\n")
    out.write(f"連絡先なし: {len(no_contact)}件\n\n")

    # 役職フィールドの分析
    out.write("=== 役職フィールド（元データ） ===\n")
    role_values = {}
    for r in records:
        role = r.get('役職', '')
        if not role:
            role = '(空)'
        # 最初の行のみ
        role_first = role.split('\n')[0].strip() if role else '(空)'
        role_values[role_first] = role_values.get(role_first, 0) + 1
    for role, count in sorted(role_values.items(), key=lambda x: -x[1]):
        out.write(f"  {role}: {count}件\n")
    out.write("\n")

    # 連絡先から担当者名パターンの分析
    out.write("=== 連絡先テキスト内の担当者名パターン ===\n")
    name_patterns_found = 0
    title_in_contact = 0

    for r in has_contact:
        contact = str(r.get('連絡先', ''))
        company = r.get('企業名', '')
        rep = r.get('代表者', '')

        # 担当: パターン
        match_tanto = re.search(r'担当[：:\s]*([^\n\r@0-9]{2,20})', contact)
        # 役職パターン（代表取締役、理事長等）
        title_keywords = ['理事長', '院長', '事務長', '園長', '施設長', '所長',
                          '部長', '課長', '代表取締役', '取締役', '代表', '社長',
                          '総務部', '人事部', '採用担当', 'マネージャー']
        found_title = None
        for tk in title_keywords:
            if tk in contact:
                found_title = tk
                break

        if match_tanto:
            name_patterns_found += 1
            raw_name = match_tanto.group(1).strip()
            out.write(f"  [{company}] 担当パターン: 「{raw_name}」")
            if found_title:
                out.write(f" | 役職: {found_title}")
                title_in_contact += 1
            out.write(f" | 代表: {rep}\n")

    out.write(f"\n担当パターン検出: {name_patterns_found}/{len(has_contact)}件\n")
    out.write(f"連絡先内に役職キーワード: {title_in_contact}件\n\n")

    # 代表者フィールドの分析
    out.write("=== 代表者フィールド ===\n")
    has_rep = sum(1 for r in records if r.get('代表者'))
    out.write(f"代表者あり: {has_rep}/{len(records)}件\n")

    # 代表者に役職が含まれているケース
    rep_with_title = 0
    rep_title_examples = []
    for r in records:
        rep = r.get('代表者', '')
        if not rep:
            continue
        for tk in ['理事長', '代表取締役', '取締役', '代表', '社長']:
            if tk in rep:
                rep_with_title += 1
                if len(rep_title_examples) < 10:
                    rep_title_examples.append(f"  {r.get('企業名', '')}: {rep}")
                break
    out.write(f"代表者に役職含む: {rep_with_title}件\n")
    for ex in rep_title_examples:
        out.write(f"{ex}\n")
    out.write("\n")

    # 新規リードCSVの抽出結果確認
    out.write("=== 新規リードCSV 抽出結果 ===\n")
    df_new = pd.read_csv('data/output/media_matching/miidas_new_leads_20260303_114542.csv',
                         dtype=str, encoding='utf-8-sig')

    title_filled = df_new['Title'].apply(lambda x: pd.notna(x) and str(x).strip() not in ['', 'nan']).sum()
    president_filled = df_new['PresidentName__c'].apply(lambda x: pd.notna(x) and str(x).strip() not in ['', 'nan']).sum()
    name_not_tanto = df_new['LastName'].apply(lambda x: pd.notna(x) and str(x).strip() != '担当者').sum()

    out.write(f"LastName（担当者以外）: {name_not_tanto}/{len(df_new)}件\n")
    out.write(f"Title（役職）あり: {title_filled}/{len(df_new)}件\n")
    out.write(f"PresidentName__c あり: {president_filled}/{len(df_new)}件\n\n")

    # 名前の品質チェック
    out.write("=== 名前品質チェック（問題がありそうなもの） ===\n")
    for _, row in df_new.iterrows():
        name = str(row.get('LastName', ''))
        company = str(row.get('Company', ''))
        title = str(row.get('Title', '') or '')
        issues = []

        if '(' in name or '（' in name or 'ハ' in name or 'カ' in name:
            if len(name) > 8:
                issues.append('カナ含む/長すぎ')
        if '：' in name or ':' in name:
            issues.append('コロン含む')
        if 'まで' in name or 'ください' in name or '連絡' in name:
            issues.append('文言混入')
        if '者' in name and name.startswith('者'):
            issues.append('「者:」プレフィックス残り')
        if len(name) > 15:
            issues.append('長すぎ')

        if issues:
            out.write(f"  [{company}] LastName: 「{name}」 Title: 「{title}」 問題: {', '.join(issues)}\n")

    out.write("\n")

    # Lead更新CSVの確認
    out.write("=== Lead更新CSV 名前更新状況 ===\n")
    df_upd = pd.read_csv('data/output/media_matching/miidas_lead_updates_20260303_114542.csv',
                         dtype=str, encoding='utf-8-sig')
    name_updates = df_upd[df_upd['LastName'].apply(lambda x: pd.notna(x) and str(x).strip() not in ['', 'nan'])]
    out.write(f"LastName更新対象: {len(name_updates)}/{len(df_upd)}件\n")
    for _, row in name_updates.iterrows():
        out.write(f"  ID:{row['Id']} -> {row['LastName']}\n")

    # 全連絡先テキストのサンプル
    out.write("\n=== 連絡先テキスト全サンプル（先頭30件） ===\n")
    for i, r in enumerate(has_contact[:30]):
        company = r.get('企業名', '')
        contact = r.get('連絡先', '')
        rep = r.get('代表者', '')
        role = r.get('役職', '')
        out.write(f"\n--- {i+1}. {company} ---\n")
        out.write(f"代表者: {rep}\n")
        out.write(f"役職フィールド: {role}\n")
        out.write(f"連絡先:\n{contact}\n")

print(f"Report: {OUT}")
