"""
Lead/Accountオブジェクトのメモ系フィールドからキーワード辞書を作成するスクリプト

処理フロー:
  Step 1: Describe APIでメモ系フィールド特定
  Step 2: Bulk API 2.0で該当フィールドのデータ取得
  Step 3: キーワード抽出・辞書作成
"""

import sys
import time
import re
import csv
import io
import json
from pathlib import Path
from datetime import datetime
from collections import Counter, defaultdict

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import requests
from src.utils.config import sf_config, output_config


class MemoKeywordExtractor:
    """メモ系フィールドからキーワードを抽出するクラス"""

    def __init__(self):
        self.instance_url = sf_config.INSTANCE_URL
        self.api_version = sf_config.API_VERSION
        self.access_token = None

    def authenticate(self):
        """OAuth認証"""
        token_url = f"{self.instance_url}/services/oauth2/token"
        payload = {
            'grant_type': 'refresh_token',
            'client_id': sf_config.CLIENT_ID,
            'client_secret': sf_config.CLIENT_SECRET,
            'refresh_token': sf_config.REFRESH_TOKEN,
        }
        response = requests.post(token_url, data=payload)
        response.raise_for_status()
        self.access_token = response.json()['access_token']
        print("[OK] 認証成功")

    def _headers(self):
        return {'Authorization': f'Bearer {self.access_token}'}

    # ========================================
    # Step 1: Describe APIでメモ系フィールド特定
    # ========================================
    def get_memo_fields(self, object_name: str) -> list[dict]:
        """
        テキスト系（メモ・説明・備考）フィールドを取得

        対象: textarea, string型でメモ/ノート/説明系のフィールド
        """
        url = f"{self.instance_url}/services/data/{self.api_version}/sobjects/{object_name}/describe"
        response = requests.get(url, headers=self._headers())
        response.raise_for_status()

        fields = response.json()['fields']

        # メモ系フィールドの判定条件
        memo_keywords_in_name = [
            'description', 'memo', 'note', 'remark', 'comment',
            'reason', 'source', 'history', 'log', 'info',
            'detail', 'summary', 'batch', 'status',
        ]
        memo_keywords_in_label = [
            'メモ', '備考', '説明', 'ノート', '理由', '履歴',
            'ソース', 'リード', '詳細', '概要', 'バッチ',
            'ステータス', '状況', '活動', 'コメント',
        ]

        # textarea型は全て対象 + string型でキーワードに合致するもの
        memo_fields = []
        for field in fields:
            name = field['name']
            label = field.get('label', '')
            field_type = field['type']
            name_lower = name.lower()
            label_lower = label.lower() if label else ''

            is_memo = False

            # textarea型は全て対象
            if field_type == 'textarea':
                is_memo = True

            # string/picklist型でメモ系キーワードに合致
            if field_type in ('string', 'picklist'):
                if any(kw in name_lower for kw in memo_keywords_in_name):
                    is_memo = True
                if any(kw in label_lower for kw in memo_keywords_in_label):
                    is_memo = True

            # LeadSource（リードソース）は必ず含める
            if name in ('LeadSource', 'LeadSource__c'):
                is_memo = True

            if is_memo:
                memo_fields.append({
                    'name': name,
                    'label': label,
                    'type': field_type,
                    'length': field.get('length', 0),
                })

        return memo_fields

    # ========================================
    # Step 2: Bulk API 2.0でデータ取得
    # ========================================
    def bulk_query(self, soql: str) -> str:
        """Bulk API 2.0でクエリ実行し、CSV文字列を返す"""
        job_url = f"{self.instance_url}/services/data/{self.api_version}/jobs/query"
        headers = {
            **self._headers(),
            'Content-Type': 'application/json',
        }
        payload = {
            'operation': 'query',
            'query': soql,
            'contentType': 'CSV',
        }

        # ジョブ作成
        response = requests.post(job_url, headers=headers, json=payload)
        response.raise_for_status()
        job_id = response.json()['id']
        print(f"  Bulk Query Job作成: {job_id}")

        # 完了待ち
        start_time = time.time()
        while True:
            status_resp = requests.get(
                f"{job_url}/{job_id}", headers=self._headers()
            )
            state = status_resp.json()['state']
            records = status_resp.json().get('numberRecordsProcessed', 0)
            print(f"  ステータス: {state} ({records:,}件処理済み)")

            if state == 'JobComplete':
                break
            elif state in ('Failed', 'Aborted'):
                raise Exception(f"Job失敗: {status_resp.json()}")

            if time.time() - start_time > 600:
                raise TimeoutError("Bulk Queryタイムアウト（600秒）")

            time.sleep(5)

        # 結果取得（ページネーション対応）
        all_csv = ""
        locator = None
        first_page = True

        while True:
            result_url = f"{job_url}/{job_id}/results"
            if locator:
                result_url += f"?locator={locator}"

            result_resp = requests.get(result_url, headers=self._headers())
            # Salesforce Bulk API 2.0はUTF-8で返すが、Content-Typeヘッダーに
            # charsetが含まれない場合があるため明示的に指定
            result_resp.encoding = 'utf-8'
            csv_text = result_resp.text

            if first_page:
                all_csv = csv_text
                first_page = False
            else:
                # ヘッダー行をスキップして追加
                lines = csv_text.split('\n', 1)
                if len(lines) > 1:
                    all_csv += lines[1]

            # 次のページがあるか確認
            locator = result_resp.headers.get('Sforce-Locator')
            if not locator or locator == 'null':
                break

        return all_csv

    # ========================================
    # Step 3: キーワード抽出
    # ========================================
    def extract_keywords_from_csv(
        self, csv_text: str, field_names: list[str]
    ) -> dict[str, Counter]:
        """
        CSV文字列からフィールドごとのユニーク値をカウント

        Returns:
            dict: {フィールド名: Counter({値: 出現回数})}
        """
        field_values = {name: Counter() for name in field_names}

        reader = csv.DictReader(io.StringIO(csv_text))
        row_count = 0
        for row in reader:
            row_count += 1
            for field_name in field_names:
                value = row.get(field_name, '').strip()
                if value and value.lower() not in ('', 'null', 'none'):
                    field_values[field_name][value] += 1

        print(f"  {row_count:,}件のレコードを解析")
        return field_values

    def extract_batch_keywords(self, field_values: dict[str, Counter]) -> dict[str, list[str]]:
        """
        メモ欄から【】で囲まれたバッチキーワードやパターンキーワードを抽出

        例: 【HW_20260107】、【BATCH_20260113_KANGO】、【新規作成】等
        """
        batch_pattern = re.compile(r'【([^】]+)】')
        keyword_patterns = {
            'bracket_keywords': batch_pattern,  # 【キーワード】
            'hw_prefix': re.compile(r'(HW_\d+)'),  # HW_20260107
            'batch_prefix': re.compile(r'(BATCH_\d+[_\w]*)'),  # BATCH_20260113_KANGO
            'media_prefix': re.compile(r'(ミイダス|PT・OT|ジョブポスター|看護roo|ナース専科)'),
            'date_tagged': re.compile(r'(\d{4}[-/]\d{2}[-/]\d{2}\s*[^\n,]{0,30})'),
        }

        extracted = defaultdict(Counter)

        for field_name, counter in field_values.items():
            for value, count in counter.items():
                for pattern_name, pattern in keyword_patterns.items():
                    matches = pattern.findall(value)
                    for match in matches:
                        extracted[f"{field_name}__{pattern_name}"][match.strip()] += count

        return extracted


def main():
    extractor = MemoKeywordExtractor()
    extractor.authenticate()

    output_dir = output_config.ensure_dir() / 'keyword_dictionary'
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    results = {}

    for object_name in ['Lead', 'Account']:
        print(f"\n{'='*60}")
        print(f" {object_name} オブジェクト処理")
        print(f"{'='*60}")

        # Step 1: メモ系フィールド特定
        print(f"\n[Step 1] {object_name} のメモ系フィールド特定中...")
        memo_fields = extractor.get_memo_fields(object_name)

        print(f"\n  検出されたメモ系フィールド ({len(memo_fields)}件):")
        print(f"  {'API名':<40} {'ラベル':<30} {'型':<12} {'長さ'}")
        print(f"  {'-'*40} {'-'*30} {'-'*12} {'-'*8}")
        for f in memo_fields:
            print(f"  {f['name']:<40} {f['label']:<30} {f['type']:<12} {f['length']}")

        # フィールド一覧をCSV保存
        fields_csv_path = output_dir / f"{object_name}_memo_fields_{timestamp}.csv"
        with open(fields_csv_path, 'w', encoding='utf-8-sig', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=['name', 'label', 'type', 'length'])
            writer.writeheader()
            writer.writerows(memo_fields)
        print(f"\n  フィールド一覧保存: {fields_csv_path}")

        results[object_name] = {
            'fields': memo_fields,
            'field_values': {},
            'batch_keywords': {},
        }

        if not memo_fields:
            print(f"  メモ系フィールドなし。スキップ。")
            continue

        # Step 2: Bulk API 2.0でデータ取得
        field_names = [f['name'] for f in memo_fields]
        # IdとNameも追加（参照用）
        query_fields = ['Id'] + field_names
        soql = f"SELECT {','.join(query_fields)} FROM {object_name}"

        print(f"\n[Step 2] Bulk API 2.0でデータ取得中...")
        print(f"  SOQL: SELECT {len(query_fields)}フィールド FROM {object_name}")
        csv_text = extractor.bulk_query(soql)

        # Step 3: キーワード抽出
        print(f"\n[Step 3] キーワード抽出中...")
        field_values = extractor.extract_keywords_from_csv(csv_text, field_names)

        # バッチキーワード抽出
        batch_keywords = extractor.extract_batch_keywords(field_values)

        results[object_name]['field_values'] = field_values
        results[object_name]['batch_keywords'] = batch_keywords

        # フィールドごとのユニーク値をCSV保存
        for field_name, counter in field_values.items():
            if not counter:
                continue
            values_csv_path = output_dir / f"{object_name}_{field_name}_values_{timestamp}.csv"
            with open(values_csv_path, 'w', encoding='utf-8-sig', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['値', '出現回数'])
                for value, count in counter.most_common():
                    writer.writerow([value, count])

    # ========================================
    # 辞書レポート生成
    # ========================================
    print(f"\n{'='*60}")
    print(f" キーワード辞書レポート生成")
    print(f"{'='*60}")

    report_lines = []
    report_lines.append(f"# Salesforce メモ系フィールド キーワード辞書")
    report_lines.append(f"")
    report_lines.append(f"生成日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report_lines.append(f"")

    for object_name in ['Lead', 'Account']:
        obj_data = results[object_name]
        report_lines.append(f"## {object_name} オブジェクト")
        report_lines.append(f"")

        # フィールド一覧
        report_lines.append(f"### 検出されたメモ系フィールド ({len(obj_data['fields'])}件)")
        report_lines.append(f"")
        report_lines.append(f"| API名 | ラベル | 型 | 長さ |")
        report_lines.append(f"|-------|--------|-----|------|")
        for f in obj_data['fields']:
            report_lines.append(f"| `{f['name']}` | {f['label']} | {f['type']} | {f['length']} |")
        report_lines.append(f"")

        # フィールドごとのキーワード一覧
        report_lines.append(f"### フィールド別キーワード一覧")
        report_lines.append(f"")

        for field_name, counter in obj_data['field_values'].items():
            if not counter:
                continue

            total_records = sum(counter.values())
            unique_values = len(counter)
            report_lines.append(f"#### `{field_name}` ({unique_values}種類, {total_records:,}件)")
            report_lines.append(f"")

            # picklist/短い文字列の場合は全件表示
            field_info = next((f for f in obj_data['fields'] if f['name'] == field_name), None)
            is_short_field = field_info and (field_info['type'] == 'picklist' or field_info['length'] <= 255)

            if is_short_field or unique_values <= 50:
                report_lines.append(f"| 値 | 件数 |")
                report_lines.append(f"|-----|------|")
                for value, count in counter.most_common():
                    # 長い値は省略
                    display_value = value[:80] + '...' if len(value) > 80 else value
                    display_value = display_value.replace('|', '\\|').replace('\n', ' ')
                    report_lines.append(f"| {display_value} | {count:,} |")
            else:
                # 長文フィールドはTop 30のみ
                report_lines.append(f"*上位30件を表示（全{unique_values}種類）*")
                report_lines.append(f"")
                report_lines.append(f"| 値（先頭80文字） | 件数 |")
                report_lines.append(f"|-----------------|------|")
                for value, count in counter.most_common(30):
                    display_value = value[:80] + '...' if len(value) > 80 else value
                    display_value = display_value.replace('|', '\\|').replace('\n', ' ')
                    report_lines.append(f"| {display_value} | {count:,} |")

            report_lines.append(f"")

        # バッチキーワード
        if obj_data['batch_keywords']:
            report_lines.append(f"### 検出されたバッチ/パターンキーワード")
            report_lines.append(f"")
            for pattern_key, counter in obj_data['batch_keywords'].items():
                if not counter:
                    continue
                report_lines.append(f"#### {pattern_key}")
                report_lines.append(f"")
                report_lines.append(f"| キーワード | 件数 |")
                report_lines.append(f"|-----------|------|")
                for kw, count in counter.most_common():
                    display_kw = kw[:80] + '...' if len(kw) > 80 else kw
                    display_kw = display_kw.replace('|', '\\|').replace('\n', ' ')
                    report_lines.append(f"| {display_kw} | {count:,} |")
                report_lines.append(f"")

        report_lines.append(f"---")
        report_lines.append(f"")

    # サマリーセクション
    report_lines.append(f"## サマリー: レポート検索用キーワード一覧")
    report_lines.append(f"")
    report_lines.append(f"レポートのフィルタ条件に使えるキーワードを集約:")
    report_lines.append(f"")

    all_keywords = set()
    for object_name in ['Lead', 'Account']:
        for pattern_key, counter in results[object_name]['batch_keywords'].items():
            if 'bracket_keywords' in pattern_key:
                for kw in counter:
                    all_keywords.add(kw)

    if all_keywords:
        report_lines.append(f"### 【】囲みキーワード（バッチ識別用）")
        report_lines.append(f"")
        report_lines.append(f"| キーワード | 用途推定 |")
        report_lines.append(f"|-----------|---------|")
        for kw in sorted(all_keywords):
            purpose = ""
            if 'HW' in kw or 'ハローワーク' in kw:
                purpose = "ハローワーク定例更新"
            elif 'BATCH' in kw:
                purpose = "バッチ処理識別"
            elif 'ミイダス' in kw:
                purpose = "ミイダス媒体"
            elif 'PT' in kw or 'OT' in kw or 'ST' in kw:
                purpose = "PT・OT・STネット媒体"
            elif 'ジョブポスター' in kw:
                purpose = "ジョブポスター媒体"
            elif '看護' in kw or 'ナース' in kw or 'KANGO' in kw:
                purpose = "看護媒体"
            elif '新規作成' in kw:
                purpose = "新規リード作成時タグ"
            elif '既存更新' in kw:
                purpose = "既存レコード更新時タグ"
            report_lines.append(f"| 【{kw}】 | {purpose} |")
        report_lines.append(f"")

    # レポート保存
    report_path = output_dir / f"keyword_dictionary_{timestamp}.md"
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report_lines))
    print(f"\n辞書レポート保存: {report_path}")

    # JSON形式でも保存（プログラム利用用）
    json_data = {}
    for object_name in ['Lead', 'Account']:
        obj_json = {
            'fields': results[object_name]['fields'],
            'field_keywords': {},
            'batch_keywords': {},
        }
        for field_name, counter in results[object_name]['field_values'].items():
            if counter:
                obj_json['field_keywords'][field_name] = dict(counter.most_common(100))
        for pattern_key, counter in results[object_name]['batch_keywords'].items():
            if counter:
                obj_json['batch_keywords'][pattern_key] = dict(counter.most_common())
        json_data[object_name] = obj_json

    json_path = output_dir / f"keyword_dictionary_{timestamp}.json"
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(json_data, f, ensure_ascii=False, indent=2)
    print(f"JSON辞書保存: {json_path}")

    # 完了サマリー
    print(f"\n{'='*60}")
    print(f" 処理完了")
    print(f"{'='*60}")
    print(f"  出力先: {output_dir}")
    for object_name in ['Lead', 'Account']:
        fields_count = len(results[object_name]['fields'])
        total_keywords = sum(
            len(c) for c in results[object_name]['field_values'].values()
        )
        print(f"  {object_name}: {fields_count}フィールド, {total_keywords}種類のキーワード")


if __name__ == '__main__':
    main()
