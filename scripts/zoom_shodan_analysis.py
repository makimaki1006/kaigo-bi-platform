# -*- coding: utf-8 -*-
"""商談分析レポート生成（新・商談品質基準プロンプト）
期間: 2026-01-20〜2026-01-30
"""

import os
import re
import json
import time
import requests
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv("config/.env")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

BASE_DIR = Path(__file__).resolve().parent.parent
TRANSCRIPT_DIR = BASE_DIR / "data" / "output" / "zoom_transcripts"
OUTPUT_DIR = TRANSCRIPT_DIR

DATE_START = "2026-01-20"
DATE_END = "2026-01-30"

# テレアポメンバー（自分で商談していない）
TELEAPO_MEMBERS = {"y_mitsuishi", "h_bamba", "k_kitami", "t_nakaya"}

# メンバー表示名
MEMBER_NAMES = {
    "k_sawada": "澤田", "s_shinoki": "篠木", "yo_ichiki": "市来",
    "y_fukabori": "深堀", "j_kubo": "久保", "r_komatsu": "小松",
    "s_hattori": "服部", "j_sato": "佐藤", "r_shimura": "志村",
    "s_shimatani": "嶋谷", "m_akasaka": "赤坂", "k_kobayashi": "小林",
    "y_haino": "灰野", "i_kumagai": "熊谷", "n_kiyohira": "清飛羅",
    "y_kamibayashi": "上林", "h_matsukaze": "松風", "y_abe": "阿部",
    "y_mitsuishi": "三石", "h_bamba": "番場", "k_kitami": "北見",
    "t_nakaya": "中谷",
}

# 除外キーワード（ロープレ・社内イベント等）
EXCLUDE_KEYWORDS = [
    "ロープレ", "roleplay", "RP", "role play",
    "研修", "MTG", "定例", "朝会", "勉強会",
    "VS", "vs", "パーソナルミーティング",
    "YMCX", "Young Man", "新人", "組み手",
    "1on1", "振り返り", "共有会", "報告会",
    "Zoom ミーティング", "New Zoom Meeting",
]

# 商談分析プロンプト
ANALYSIS_PROMPT = """# 指示
あなたは採用支援サービス「Medica（メディカ）」の教育責任者兼、凄腕のセールスイネイブルメント担当です。
以下の【商談ログ】を分析し、営業担当者のパフォーマンスを厳しく採点・フィードバックしてください。

Medicaは、医療・介護・福祉業界特化のRPO（採用代行）サービスであり、人材紹介会社や単なる広告媒体とは異なる「データドリブンな資産蓄積型サービス」です。

評価は以下の【評価基準】に基づき、定量的なスコアと定性的な改善点を提示してください。

---

# 【評価基準】

## A. 勝ちパターン遂行チェック（各20点 / 100点満点）
以下の5項目が商談内で実践されているかを確認し、各項目を採点してください。

1. **データ殴打（Data Shock）**
   - 基準：冒頭（序盤）で、そのエリア×職種の「競合求人数」「検索順位（圏外である事実）」などの客観的データを提示し、顧客の主観を壊せているか。
   - 判定：具体的な数字が出ていればOK。「厳しいですね」等の感想のみはNG。

2. **構造暴露（Structure Reveal）**
   - 基準：人材紹介会社や大手媒体の「不都合な真実（優先順位のロジック、中抜き構造）」を論理的に説明し、依存のリスクを伝えているか。
   - 判定：紹介会社の仕組みへの言及があればOK。単に「高い」と言うだけならNG。

3. **定義転換（Reframing）**
   - 基準：Medicaの費用を「広告費」ではなく、「労働力（プロの人事部）」や「資産（ノウハウ蓄積）」として再定義できているか。
   - 判定：費用対効果のロジックが転換されていればOK。

4. **戦略提案（Data Logic）**
   - 基準：エリアの人口動態、流入経路、競合施設の実名などを出し、勝てる根拠を提示しているか。
   - 判定：固有名称やデータに基づく提案があればOK。抽象的な精神論はNG。

5. **クロージング（Closing Authority）**
   - 基準：検討による「機会損失」を突きつけたり、権限（特典・期限）を行使して「今決める理由」を作れているか。
   - 判定：デッドラインを切っていればOK。「検討お願いします」で終わっていればNG。

## B. 【重要】新人病・レッドカード診断（減点法）
以下の2つの「悪癖」が発生していないか、会話の流れを厳しくチェックしてください。発生している場合は、該当箇所を抜粋し、激しく指摘してください。

1. **【抽象的逃げ】の検知**
   - 症状：顧客の質問に対し、データや固有名詞を使わず「実績があります」「頑張ります」「プロがやります」「多くの企業様は」といった曖昧な言葉で返している箇所。
   - 判定：具体的な数字・事例がない返しは全てNG。

2. **【スルー（無視）】の検知**
   - 症状：顧客が発した「小さな懸念」や「本音」に対し、共感（オウム返し）や回答をせず、強引に次のスクリプト（機能説明など）へ進めている箇所。
   - 判定：「ちなみに」「それはさておき」等で話を逸らしている場合はNG。

---

# 出力フォーマット

## Medica商談・診断レポート

**総合スコア：** [　　] / 100点
**判定ランク：** [ S:即戦力 / A:合格 / B:要指導 / C:再教育 ]

### 1. 勝ちパターン遂行度
* **①データ殴打:** [ ○ / △ / × ] [点数]/20 （寸評：...）
* **②構造暴露:** [ ○ / △ / × ] [点数]/20 （寸評：...）
* **③定義転換:** [ ○ / △ / × ] [点数]/20 （寸評：...）
* **④戦略提案:** [ ○ / △ / × ] [点数]/20 （寸評：...）
* **⑤クロージング:** [ ○ / △ / × ] [点数]/20 （寸評：...）

### 2. レッドカード検出（会話のズレ・抽象化）

* **【抽象的逃げ】検出箇所:**
    * 営業の発言：「......」
    * **教育責任者からの喝:** ...
* **【スルー（無視）】検出箇所:**
    * 顧客の発言：「......」
    * 営業の反応：「......」
    * **教育責任者からの喝:** ...

### 3. 次回への処方箋（To Do）
* [具体的な改善アクションを1つ提示]
* [明日から使えるキラーフレーズを1つ提示]

---
【商談ログ】
{transcript}
"""


def call_gemini(prompt, max_tokens=4096, retries=3):
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={GEMINI_API_KEY}"
    for attempt in range(retries):
        try:
            resp = requests.post(url, json={
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"maxOutputTokens": max_tokens, "temperature": 0.3}
            }, timeout=120)
            data = resp.json()
            if "candidates" in data:
                return data["candidates"][0]["content"]["parts"][0]["text"]
            print(f"  API応答異常 (attempt {attempt+1}): {str(data)[:200]}")
            time.sleep(5 * (attempt + 1))
        except Exception as e:
            print(f"  APIエラー (attempt {attempt+1}): {e}")
            time.sleep(5 * (attempt + 1))
    return None


def detect_actual_salesperson(txt_path):
    """文字起こしの最初の20行から実際の営業担当者を検出"""
    try:
        with open(txt_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()[:20]
        text = ' '.join(lines)
        # メンバー名でマッチ
        for member_id, display_name in MEMBER_NAMES.items():
            if member_id in TELEAPO_MEMBERS:
                continue
            if display_name in text:
                return member_id
    except Exception:
        pass
    return None


def is_excluded(filename):
    for kw in EXCLUDE_KEYWORDS:
        if kw.lower() in filename.lower():
            return True
    return False


def collect_transcripts():
    """対象期間のファイルを収集し、テレアポメンバーを再割当て"""
    all_files = []

    for member_dir in os.listdir(TRANSCRIPT_DIR):
        dpath = TRANSCRIPT_DIR / member_dir
        if not dpath.is_dir() or member_dir.startswith('_'):
            continue
        if member_dir == "k_hirata":  # リクロジは除外
            continue

        for f in os.listdir(dpath):
            if not f.endswith('.txt'):
                continue
            m = re.match(r'(\d{4}-\d{2}-\d{2})_', f)
            if not m:
                continue
            date = m.group(1)
            if date < DATE_START or date > DATE_END:
                continue
            if is_excluded(f):
                continue

            fpath = dpath / f
            # テレアポメンバーの場合、実営業者を検出
            if member_dir in TELEAPO_MEMBERS:
                actual = detect_actual_salesperson(str(fpath))
                if actual:
                    assigned = actual
                else:
                    continue  # 実営業者不明はスキップ
            else:
                assigned = member_dir

            all_files.append({
                'member': assigned,
                'name': MEMBER_NAMES.get(assigned, assigned),
                'file': str(fpath),
                'filename': f,
                'date': date,
            })

    return all_files


def analyze_single(item):
    """1件の商談を分析"""
    fpath = item['file']
    try:
        with open(fpath, 'r', encoding='utf-8') as f:
            text = f.read()
    except Exception as e:
        return {**item, 'result': None, 'error': str(e)}

    # 短すぎるファイルはスキップ
    lines = [l for l in text.strip().split('\n') if l.strip()]
    if len(lines) < 30:
        return {**item, 'result': None, 'error': 'too_short'}

    # トークン制限対策: 長すぎる場合は後半カット
    if len(text) > 60000:
        text = text[:60000] + "\n\n[...以降省略...]"

    prompt = ANALYSIS_PROMPT.format(transcript=text)
    result = call_gemini(prompt, max_tokens=4096)
    return {**item, 'result': result, 'error': None}


def extract_score(result_text):
    """結果テキストからスコアとランクを抽出"""
    if not result_text:
        return None, None, {}

    # 総合スコア
    score_match = re.search(r'総合スコア[：:]\s*\**\s*(\d+)\s*[/／]', result_text)
    score = int(score_match.group(1)) if score_match else None

    # ランク
    rank_match = re.search(r'判定ランク[：:]\s*\**\s*\[?\s*([SABC])', result_text)
    rank = rank_match.group(1) if rank_match else None

    # 各項目のスコア
    axes = {}
    patterns = [
        (r'データ殴打.*?(\d+)\s*[/／]\s*20', 'data_shock'),
        (r'構造暴露.*?(\d+)\s*[/／]\s*20', 'structure_reveal'),
        (r'定義転換.*?(\d+)\s*[/／]\s*20', 'reframing'),
        (r'戦略提案.*?(\d+)\s*[/／]\s*20', 'data_logic'),
        (r'クロージング.*?(\d+)\s*[/／]\s*20', 'closing'),
    ]
    for pat, key in patterns:
        m = re.search(pat, result_text)
        if m:
            axes[key] = int(m.group(1))

    return score, rank, axes


def generate_html(results, member_stats):
    """HTMLレポート生成"""

    # メンバー別集計をスコア順にソート
    sorted_members = sorted(member_stats.items(), key=lambda x: x[1]['avg_score'], reverse=True)

    # 全体統計
    all_scores = [r['score'] for r in results if r['score'] is not None]
    total_avg = sum(all_scores) / len(all_scores) if all_scores else 0
    rank_counts = {'S': 0, 'A': 0, 'B': 0, 'C': 0}
    for r in results:
        if r.get('rank') in rank_counts:
            rank_counts[r['rank']] += 1

    # ランキングテーブル
    ranking_rows = ""
    for i, (member, stats) in enumerate(sorted_members, 1):
        avg = stats['avg_score']
        cnt = stats['count']
        if avg >= 70: color = '#10b981'
        elif avg >= 50: color = '#3b82f6'
        elif avg >= 35: color = '#f59e0b'
        else: color = '#ef4444'

        # 各軸平均
        axes_avg = {}
        for key in ['data_shock', 'structure_reveal', 'reframing', 'data_logic', 'closing']:
            vals = [r['axes'].get(key, 0) for r in stats['results'] if r.get('axes')]
            axes_avg[key] = sum(vals) / len(vals) if vals else 0

        def axis_cell(val):
            if val >= 14: bg, c = 'rgba(16,185,129,0.2)', '#10b981'
            elif val >= 10: bg, c = 'rgba(59,130,246,0.2)', '#60a5fa'
            elif val >= 6: bg, c = 'rgba(245,158,11,0.15)', '#f59e0b'
            else: bg, c = 'rgba(239,68,68,0.15)', '#ef4444'
            return f'<td style="text-align:center;background:{bg};color:{c};font-weight:600">{val:.0f}</td>'

        # ランク分布
        r_dist = {}
        for r in stats['results']:
            rk = r.get('rank', '?')
            r_dist[rk] = r_dist.get(rk, 0) + 1
        rank_str = ' '.join(f'{k}:{v}' for k, v in sorted(r_dist.items()))

        ranking_rows += f'''<tr>
            <td style="text-align:center;font-weight:700;color:{color}">{i}</td>
            <td><strong>{stats["display_name"]}</strong></td>
            <td style="text-align:center;color:#94a3b8">{cnt}件</td>
            <td style="text-align:center;font-weight:700;font-size:18px;color:{color}">{avg:.0f}</td>
            {axis_cell(axes_avg.get('data_shock', 0))}
            {axis_cell(axes_avg.get('structure_reveal', 0))}
            {axis_cell(axes_avg.get('reframing', 0))}
            {axis_cell(axes_avg.get('data_logic', 0))}
            {axis_cell(axes_avg.get('closing', 0))}
            <td style="text-align:center;color:#94a3b8;font-size:12px">{rank_str}</td>
        </tr>'''

    # 個別商談パネル（メンバー別）
    member_sections = ""
    member_buttons = ""
    for i, (member, stats) in enumerate(sorted_members, 1):
        display = stats['display_name']
        member_buttons += f'<button class="member-btn" onclick="showMember(\'{member}\')">{display}({stats["count"]})</button> '

        deals_html = ""
        for j, r in enumerate(sorted(stats['results'], key=lambda x: x.get('score') or 0, reverse=True)):
            score = r.get('score', '?')
            rank = r.get('rank', '?')
            fname = r.get('filename', '')
            date = r.get('date', '')
            result_text = r.get('result', '') or ''

            if isinstance(score, (int, float)):
                if score >= 70: sc = '#10b981'
                elif score >= 50: sc = '#3b82f6'
                elif score >= 35: sc = '#f59e0b'
                else: sc = '#ef4444'
            else:
                sc = '#64748b'

            # 顧客名抽出
            customer = re.sub(r'^\d{4}-\d{2}-\d{2}_\d{4}_', '', fname).replace('.txt', '')
            customer = re.sub(r'^[✅☑◎【]+', '', customer).replace('】', '')

            # Markdownを簡易HTML化
            report_html = result_text.replace('\n', '<br>')
            report_html = re.sub(r'\*\*(.+?)\*\*', r'<strong style="color:#fbbf24">\1</strong>', report_html)
            report_html = re.sub(r'###\s*(.+?)(<br>)', r'<h4 style="color:#60a5fa;margin:8px 0">\1</h4>', report_html)

            deals_html += f'''
            <div style="background:#0f172a;border-radius:8px;padding:16px;margin-bottom:12px;border-left:3px solid {sc}">
                <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
                    <div>
                        <span style="color:#94a3b8;font-size:12px">{date}</span>
                        <span style="color:#e2e8f0;font-size:14px;font-weight:600;margin-left:8px">{customer}</span>
                    </div>
                    <div>
                        <span style="font-size:24px;font-weight:700;color:{sc}">{score}</span>
                        <span style="color:#64748b;font-size:12px">/100</span>
                        <span style="background:{sc};color:white;padding:2px 8px;border-radius:4px;font-size:12px;margin-left:8px">{rank}</span>
                    </div>
                </div>
                <details>
                    <summary style="cursor:pointer;color:#60a5fa;font-size:13px">詳細レポートを表示</summary>
                    <div style="margin-top:8px;font-size:13px;line-height:1.7;color:#cbd5e1;max-height:400px;overflow-y:auto;padding:8px">
                        {report_html}
                    </div>
                </details>
            </div>'''

        avg = stats['avg_score']
        if avg >= 70: tier_color = '#10b981'
        elif avg >= 50: tier_color = '#3b82f6'
        elif avg >= 35: tier_color = '#f59e0b'
        else: tier_color = '#ef4444'

        member_sections += f'''
        <div class="member-section" id="section-{member}" style="display:none">
            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px">
                <h3 style="color:white;font-size:20px">{display}
                    <span style="color:#94a3b8;font-size:14px;margin-left:8px">#{i}</span>
                </h3>
                <div style="text-align:right">
                    <div style="font-size:32px;font-weight:700;color:{tier_color}">{avg:.0f}<span style="font-size:14px;color:#64748b">/100</span></div>
                    <div style="color:#94a3b8;font-size:12px">{stats['count']}件平均</div>
                </div>
            </div>
            {deals_html}
        </div>'''

    # Chart.jsデータ
    chart_names = json.dumps([stats['display_name'] for _, stats in sorted_members], ensure_ascii=False)
    chart_avgs = json.dumps([round(stats['avg_score']) for _, stats in sorted_members])
    chart_colors_list = []
    for _, stats in sorted_members:
        avg = stats['avg_score']
        if avg >= 70: chart_colors_list.append('#10b981')
        elif avg >= 50: chart_colors_list.append('#3b82f6')
        elif avg >= 35: chart_colors_list.append('#f59e0b')
        else: chart_colors_list.append('#ef4444')
    chart_colors = json.dumps(chart_colors_list)

    # 5軸データ（メンバー別平均）
    axes_by_member = {}
    for member, stats in sorted_members:
        axes_vals = {}
        for key in ['data_shock', 'structure_reveal', 'reframing', 'data_logic', 'closing']:
            vals = [r['axes'].get(key, 0) for r in stats['results'] if r.get('axes')]
            axes_vals[key] = round(sum(vals) / len(vals), 1) if vals else 0
        axes_by_member[stats['display_name']] = [axes_vals['data_shock'], axes_vals['structure_reveal'],
                                                   axes_vals['reframing'], axes_vals['data_logic'], axes_vals['closing']]
    axes_json = json.dumps(axes_by_member, ensure_ascii=False)

    # 全体の軸別平均
    all_axes = {'data_shock': [], 'structure_reveal': [], 'reframing': [], 'data_logic': [], 'closing': []}
    for r in results:
        if r.get('axes'):
            for k in all_axes:
                if k in r['axes']:
                    all_axes[k].append(r['axes'][k])
    avg_axes = [round(sum(v)/len(v), 1) if v else 0 for v in all_axes.values()]
    avg_axes_json = json.dumps(avg_axes)

    total_analyzed = len([r for r in results if r.get('result')])

    html = f'''<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Medica商談品質分析レポート 2026年1月20日-30日</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{background:#0f172a;color:#e2e8f0;font-family:'Segoe UI','Hiragino Sans',sans-serif}}
.container{{max-width:1400px;margin:0 auto;padding:24px}}
h1{{text-align:center;font-size:26px;margin-bottom:4px}}
.subtitle{{text-align:center;color:#64748b;margin-bottom:28px;font-size:14px}}
.kpi-grid{{display:grid;grid-template-columns:repeat(6,1fr);gap:12px;margin-bottom:28px}}
.kpi-card{{background:#1e293b;border-radius:12px;padding:14px;text-align:center}}
.kpi-card .label{{color:#94a3b8;font-size:11px;margin-bottom:4px}}
.kpi-card .value{{font-size:26px;font-weight:700}}
.kpi-card .sub{{color:#64748b;font-size:11px;margin-top:4px}}
.section{{background:#1e293b;border-radius:12px;padding:24px;margin-bottom:24px}}
.section h2{{font-size:18px;margin-bottom:16px;color:#f8fafc}}
table{{width:100%;border-collapse:collapse}}
th,td{{padding:10px 8px;border-bottom:1px solid #334155;font-size:13px}}
th{{background:#0f172a;color:#94a3b8;font-weight:600;text-align:center;position:sticky;top:0}}
td{{color:#e2e8f0}}
tr:hover{{background:rgba(59,130,246,0.08)}}
.chart-grid{{display:grid;grid-template-columns:1fr 1fr;gap:24px;margin-bottom:24px}}
.chart-box{{background:#1e293b;border-radius:12px;padding:20px}}
.chart-box h3{{font-size:15px;margin-bottom:12px;color:#f8fafc}}
.member-btn{{background:#334155;border:none;color:#e2e8f0;padding:6px 14px;border-radius:6px;cursor:pointer;font-size:13px;margin:3px;transition:all 0.2s}}
.member-btn:hover{{background:#475569}}
.member-btn.active{{background:#3b82f6;color:white}}
details summary{{list-style:none}}
details summary::-webkit-details-marker{{display:none}}
@media(max-width:900px){{.kpi-grid{{grid-template-columns:repeat(3,1fr)}}.chart-grid{{grid-template-columns:1fr}}}}
</style>
</head>
<body>
<div class="container">
    <h1>Medica 商談品質分析レポート</h1>
    <div class="subtitle">新・商談品質基準（5軸×20点 + レッドカード診断） | 2026年1月20日〜30日 | {total_analyzed}件分析</div>

    <div class="kpi-grid">
        <div class="kpi-card"><div class="label">平均スコア</div><div class="value" style="color:#60a5fa">{total_avg:.0f}</div><div class="sub">/ 100点</div></div>
        <div class="kpi-card"><div class="label">S (即戦力)</div><div class="value" style="color:#10b981">{rank_counts['S']}</div><div class="sub">件</div></div>
        <div class="kpi-card"><div class="label">A (合格)</div><div class="value" style="color:#3b82f6">{rank_counts['A']}</div><div class="sub">件</div></div>
        <div class="kpi-card"><div class="label">B (要指導)</div><div class="value" style="color:#f59e0b">{rank_counts['B']}</div><div class="sub">件</div></div>
        <div class="kpi-card"><div class="label">C (再教育)</div><div class="value" style="color:#ef4444">{rank_counts['C']}</div><div class="sub">件</div></div>
        <div class="kpi-card"><div class="label">分析対象</div><div class="value" style="color:#94a3b8">{total_analyzed}</div><div class="sub">/ {len(results)}件</div></div>
    </div>

    <div class="chart-grid">
        <div class="chart-box"><h3>メンバー別平均スコア</h3><canvas id="barChart" height="400"></canvas></div>
        <div class="chart-box"><h3>ランク分布</h3><canvas id="doughnutChart" height="200"></canvas>
            <div style="margin-top:16px"><h3 style="font-size:14px;margin-bottom:8px">5軸平均（組織全体）</h3>
                <canvas id="radarAll" height="250"></canvas>
            </div>
        </div>
    </div>

    <div class="chart-grid">
        <div class="chart-box"><h3>5軸比較: トップ3 vs 組織平均</h3><canvas id="radarTop3" height="350"></canvas></div>
        <div class="chart-box"><h3>軸別スコア分布</h3><canvas id="axisBar" height="350"></canvas></div>
    </div>

    <div class="section">
        <h2>メンバー別ランキング</h2>
        <div style="overflow-x:auto">
        <table>
            <thead><tr>
                <th>#</th><th style="text-align:left">メンバー</th><th>商談数</th><th>平均点</th>
                <th>データ殴打</th><th>構造暴露</th><th>定義転換</th><th>戦略提案</th><th>CL</th><th>ランク分布</th>
            </tr></thead>
            <tbody>{ranking_rows}</tbody>
        </table>
        </div>
    </div>

    <div class="section">
        <h2>個別商談レポート</h2>
        <div style="text-align:center;margin-bottom:12px">{member_buttons}</div>
        {member_sections}
    </div>
</div>

<script>
const names = {chart_names};
const avgs = {chart_avgs};
const colors = {chart_colors};
const axesData = {axes_json};
const avgAxes = {avg_axes_json};
const axisLabels = ['データ殴打','構造暴露','定義転換','戦略提案','クロージング'];

new Chart(document.getElementById('barChart'),{{type:'bar',data:{{labels:names,datasets:[{{data:avgs,backgroundColor:colors,borderRadius:4}}]}},options:{{indexAxis:'y',responsive:true,plugins:{{legend:{{display:false}}}},scales:{{x:{{max:100,grid:{{color:'#1e293b'}},ticks:{{color:'#94a3b8'}}}},y:{{grid:{{display:false}},ticks:{{color:'#e2e8f0',font:{{size:12}}}}}}}}}}}});

new Chart(document.getElementById('doughnutChart'),{{type:'doughnut',data:{{labels:['S:即戦力','A:合格','B:要指導','C:再教育'],datasets:[{{data:[{rank_counts['S']},{rank_counts['A']},{rank_counts['B']},{rank_counts['C']}],backgroundColor:['#10b981','#3b82f6','#f59e0b','#ef4444']}}]}},options:{{responsive:true,plugins:{{legend:{{position:'bottom',labels:{{color:'#94a3b8'}}}}}}}}}});

new Chart(document.getElementById('radarAll'),{{type:'radar',data:{{labels:axisLabels,datasets:[{{label:'組織平均',data:avgAxes,borderColor:'#3b82f6',backgroundColor:'rgba(59,130,246,0.15)',borderWidth:2}}]}},options:{{responsive:true,scales:{{r:{{min:0,max:20,ticks:{{stepSize:5,color:'#64748b'}},grid:{{color:'#334155'}},pointLabels:{{color:'#e2e8f0'}}}}}},plugins:{{legend:{{display:false}}}}}}}});

const top3names = names.slice(0,3);
const ds = top3names.map((n,i)=>{{const cs=['#10b981','#3b82f6','#f59e0b'];return{{label:n,data:axesData[n],borderColor:cs[i],backgroundColor:cs[i]+'22',pointRadius:3}}}});
ds.push({{label:'組織平均',data:avgAxes,borderColor:'#64748b',backgroundColor:'rgba(100,116,139,0.1)',borderDash:[5,5],pointRadius:2}});
new Chart(document.getElementById('radarTop3'),{{type:'radar',data:{{labels:axisLabels,datasets:ds}},options:{{responsive:true,scales:{{r:{{min:0,max:20,ticks:{{stepSize:5,color:'#64748b'}},grid:{{color:'#334155'}},pointLabels:{{color:'#e2e8f0',font:{{size:12}}}}}}}},plugins:{{legend:{{position:'bottom',labels:{{color:'#94a3b8',usePointStyle:true}}}}}}}}}});

const allAxesVals=Object.values(axesData);
const maxA=axisLabels.map((_,i)=>Math.max(...allAxesVals.map(d=>d[i])));
const minA=axisLabels.map((_,i)=>Math.min(...allAxesVals.map(d=>d[i])));
new Chart(document.getElementById('axisBar'),{{type:'bar',data:{{labels:axisLabels,datasets:[{{label:'最高',data:maxA,backgroundColor:'rgba(16,185,129,0.3)',borderColor:'#10b981',borderWidth:1}},{{label:'平均',data:avgAxes,backgroundColor:'rgba(59,130,246,0.3)',borderColor:'#3b82f6',borderWidth:1}},{{label:'最低',data:minA,backgroundColor:'rgba(239,68,68,0.2)',borderColor:'#ef4444',borderWidth:1}}]}},options:{{responsive:true,scales:{{y:{{max:20,grid:{{color:'#1e293b'}},ticks:{{color:'#94a3b8'}}}},x:{{grid:{{display:false}},ticks:{{color:'#e2e8f0'}}}}}},plugins:{{legend:{{position:'bottom',labels:{{color:'#94a3b8'}}}}}}}}}});

function showMember(id){{
    document.querySelectorAll('.member-section').forEach(s=>s.style.display='none');
    document.querySelectorAll('.member-btn').forEach(b=>b.classList.remove('active'));
    const el=document.getElementById('section-'+id);
    if(el)el.style.display='block';
    if(event&&event.target)event.target.classList.add('active');
}}
</script>
</body>
</html>'''

    return html


def main():
    print("=" * 60)
    print("Medica 商談品質分析（新基準）")
    print(f"期間: {DATE_START} 〜 {DATE_END}")
    print("=" * 60)

    # ファイル収集
    print("\n【Phase 0】ファイル収集・テレアポ再割当て...", flush=True)
    files = collect_transcripts()
    print(f"  対象: {len(files)}件")

    # メンバー別カウント
    member_count = {}
    for f in files:
        member_count.setdefault(f['name'], 0)
        member_count[f['name']] += 1
    for name, cnt in sorted(member_count.items(), key=lambda x: x[1], reverse=True):
        print(f"  {name}: {cnt}件")

    # 分析実行（並列5）
    print(f"\n【Phase 1】商談分析 ({len(files)}件)...", flush=True)
    results = []
    done = 0
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(analyze_single, f): f for f in files}
        for future in as_completed(futures):
            r = future.result()
            done += 1
            if r.get('result'):
                score, rank, axes = extract_score(r['result'])
                r['score'] = score
                r['rank'] = rank
                r['axes'] = axes
                status = f"score={score}, rank={rank}"
            elif r.get('error') == 'too_short':
                r['score'] = None
                r['rank'] = None
                r['axes'] = {}
                status = "スキップ(短すぎ)"
            else:
                r['score'] = None
                r['rank'] = None
                r['axes'] = {}
                status = f"エラー: {r.get('error', 'unknown')}"
            results.append(r)
            print(f"  [{done}/{len(files)}] {r['name']} - {r['filename'][:40]}... {status}", flush=True)

    # メンバー別集計
    print("\n【Phase 2】集計...", flush=True)
    member_stats = {}
    for r in results:
        if r.get('result') is None:
            continue
        m = r['member']
        if m not in member_stats:
            member_stats[m] = {
                'display_name': r['name'],
                'results': [],
                'count': 0,
                'total_score': 0,
            }
        member_stats[m]['results'].append(r)
        member_stats[m]['count'] += 1
        if r.get('score'):
            member_stats[m]['total_score'] += r['score']

    for m, stats in member_stats.items():
        scored = [r['score'] for r in stats['results'] if r.get('score')]
        stats['avg_score'] = sum(scored) / len(scored) if scored else 0

    # HTML生成
    print("\n【Phase 3】HTML生成...", flush=True)
    html = generate_html(results, member_stats)
    output_path = OUTPUT_DIR / "zoom_shodan_analysis_202601.html"
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"\n完了: {output_path}")
    print(f"HTMLサイズ: {len(html):,}文字")

    # Excel出力
    print("\nExcel出力...", flush=True)
    rows = []
    for r in sorted(results, key=lambda x: (x['name'], x.get('date', ''))):
        if r.get('result') is None:
            continue
        customer = re.sub(r'^\d{4}-\d{2}-\d{2}_\d{4}_', '', r['filename']).replace('.txt', '')
        rows.append({
            'メンバー': r['name'],
            '日時': r.get('date', ''),
            '顧客': customer,
            '総合スコア': r.get('score', ''),
            'ランク': r.get('rank', ''),
            'データ殴打': r.get('axes', {}).get('data_shock', ''),
            '構造暴露': r.get('axes', {}).get('structure_reveal', ''),
            '定義転換': r.get('axes', {}).get('reframing', ''),
            '戦略提案': r.get('axes', {}).get('data_logic', ''),
            'クロージング': r.get('axes', {}).get('closing', ''),
            '詳細レポート': r.get('result', ''),
        })

    df = pd.DataFrame(rows)
    excel_path = OUTPUT_DIR / "zoom_shodan_analysis_202601.xlsx"
    with pd.ExcelWriter(str(excel_path), engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='商談分析', index=False)

        # メンバー別サマリーシート
        summary_rows = []
        for m, stats in sorted(member_stats.items(), key=lambda x: x[1]['avg_score'], reverse=True):
            scored = [r['score'] for r in stats['results'] if r.get('score')]
            axes_avg = {}
            for key in ['data_shock', 'structure_reveal', 'reframing', 'data_logic', 'closing']:
                vals = [r['axes'].get(key, 0) for r in stats['results'] if r.get('axes')]
                axes_avg[key] = round(sum(vals) / len(vals), 1) if vals else 0
            summary_rows.append({
                'メンバー': stats['display_name'],
                '商談数': stats['count'],
                '平均スコア': round(stats['avg_score'], 1),
                '最高': max(scored) if scored else '',
                '最低': min(scored) if scored else '',
                'データ殴打(平均)': axes_avg['data_shock'],
                '構造暴露(平均)': axes_avg['structure_reveal'],
                '定義転換(平均)': axes_avg['reframing'],
                '戦略提案(平均)': axes_avg['data_logic'],
                'CL(平均)': axes_avg['closing'],
            })
        pd.DataFrame(summary_rows).to_excel(writer, sheet_name='メンバー別サマリー', index=False)

    print(f"Excel: {excel_path}")
    print("\n全完了!")


if __name__ == '__main__':
    main()
