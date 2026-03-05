# -*- coding: utf-8 -*-
"""営業力スコアリングHTMLレポート生成（メディカ×リクロジ）"""

import pandas as pd
import requests
import os
import re
import json
from dotenv import load_dotenv

load_dotenv("config/.env")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

def call_gemini(prompt, max_tokens=2048):
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={GEMINI_API_KEY}"
    resp = requests.post(url, json={
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"maxOutputTokens": max_tokens, "temperature": 0.2}
    })
    data = resp.json()
    return data["candidates"][0]["content"]["parts"][0]["text"]

def get_tier(rank, total):
    pct = rank / total
    if pct <= 0.15: return 'S', '#10b981'
    if pct <= 0.35: return 'A', '#3b82f6'
    if pct <= 0.65: return 'B', '#f59e0b'
    if pct <= 0.85: return 'C', '#f97316'
    return 'D', '#ef4444'

def score_cell_html(val):
    if val >= 7: bg, c = 'rgba(16,185,129,0.2)', '#10b981'
    elif val >= 5: bg, c = 'rgba(59,130,246,0.2)', '#60a5fa'
    elif val >= 4: bg, c = 'rgba(245,158,11,0.15)', '#f59e0b'
    else: bg, c = 'rgba(239,68,68,0.15)', '#ef4444'
    return f'<td style="text-align:center;background:{bg};color:{c};font-weight:600">{val}</td>'

def md_to_html(text):
    lines = text.split('\n')
    parts = []
    in_list = False
    for line in lines:
        line = line.strip()
        if not line:
            if in_list:
                parts.append('</ul>')
                in_list = False
            continue
        if line.startswith('## '):
            if in_list: parts.append('</ul>'); in_list = False
            parts.append(f'<h3 style="color:#60a5fa;margin:16px 0 8px;font-size:15px">{line[3:]}</h3>')
        elif line.startswith('### '):
            if in_list: parts.append('</ul>'); in_list = False
            parts.append(f'<h4 style="color:#93c5fd;margin:12px 0 6px;font-size:14px">{line[4:]}</h4>')
        elif line.startswith('- ') or line.startswith('* '):
            if not in_list:
                parts.append('<ul style="margin:4px 0;padding-left:20px">')
                in_list = True
            content = re.sub(r'\*\*(.+?)\*\*', r'<strong style="color:#fbbf24">\1</strong>', line[2:])
            parts.append(f'<li style="margin:2px 0;font-size:13px;line-height:1.6">{content}</li>')
        elif line.startswith('|'):
            parts.append(f'<div style="font-size:12px;color:#94a3b8;font-family:monospace">{line}</div>')
        else:
            if in_list: parts.append('</ul>'); in_list = False
            content = re.sub(r'\*\*(.+?)\*\*', r'<strong style="color:#fbbf24">\1</strong>', line)
            parts.append(f'<p style="margin:4px 0;font-size:13px;line-height:1.7;color:#cbd5e1">{content}</p>')
    if in_list: parts.append('</ul>')
    return '\n'.join(parts)


def main():
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    out_dir = os.path.join(base, "data", "output", "zoom_transcripts")

    # データ読み込み
    xls = pd.ExcelFile(os.path.join(out_dir, "zoom_analysis_summary_202601.xlsx"))
    members = {}
    for s in xls.sheet_names:
        if s == '組織サマリー':
            continue
        df = pd.read_excel(xls, sheet_name=s)
        text = str(df.iloc[0]['傾向レポート'])
        mtg = int(df.iloc[0]['商談数'])
        members[s] = {'report': text[:800], 'count': mtg, 'full_report': text, 'dept': 'メディカ'}

    xls2 = pd.ExcelFile(os.path.join(out_dir, "zoom_analysis_hirata_202601.xlsx"))
    df2 = pd.read_excel(xls2, sheet_name='傾向レポート')
    hirata_text = str(df2.iloc[0]['傾向レポート'])
    members['平田'] = {'report': hirata_text[:800], 'count': 36, 'full_report': hirata_text, 'dept': 'リクロジ'}

    df_org = pd.read_excel(xls, sheet_name='組織サマリー')
    org_summary = str(df_org.iloc[0]['レポート'])

    # 相対スコアリング
    print("相対スコアリング実行中...", flush=True)
    PROMPT = """【指示】
あなたは採用支援サービスの教育責任者です。
以下に20名の営業メンバーの傾向レポートを提示します。

【重要な前提】
- 深堀・服部はメディカ事業部で最も成約率が高いトップパフォーマーです
- 平田はリクロジ事業部（製造・物流系）のメンバーです（比較参考）
- 全メンバーを**相対的に比較**して、明確に差がつくようにスコアをつけてください
- 同じ合計点のメンバーを最大2名までにしてください。軸ごとに差をつけて合計で差が出るようにしてください
- スコア分布の目安: トップ 8-9、上位 6-7、中堅上位 5、中堅 4、要改善 2-3

【タスク】
以下の5軸で各メンバーを1〜10の整数でスコアリングしてください。
- 課題発見力: 顧客の潜在課題を引き出す力
- 提案力: 具体的で説得力のある提案ができるか
- 危機感醸成力: 顧客に「今動くべき」と思わせる力
- 差別化力: 自社サービスの独自価値を伝える力
- クロージング力: 次のアクションに結びつける力

【出力形式（厳守）】
メンバー名|課題発見力|提案力|危機感醸成力|差別化力|クロージング力

【メンバーの傾向レポート】
"""
    summaries = []
    for name, data in members.items():
        summaries.append(f"### {name}（{data['count']}件, {data['dept']}）\n{data['report']}")

    result = call_gemini(PROMPT + "\n\n---\n\n".join(summaries), max_tokens=2048)

    scores_data = []
    for line in result.strip().split("\n"):
        line = line.strip().strip('`')
        if not line or "|" not in line or "メンバー" in line:
            continue
        parts = [p.strip() for p in line.split("|")]
        if len(parts) < 6:
            continue
        try:
            vals = [int(p) for p in parts[1:6]]
            name = parts[0]
            dept = members.get(name, {}).get('dept', 'メディカ')
            scores_data.append({
                'name': name, 'dept': dept,
                'discovery': vals[0], 'proposal': vals[1], 'urgency': vals[2],
                'differentiation': vals[3], 'closing': vals[4],
                'total': sum(vals), 'count': members.get(name, {}).get('count', 0)
            })
        except ValueError:
            continue

    scores_data.sort(key=lambda x: x['total'], reverse=True)
    print(f"スコアリング完了: {len(scores_data)}名")
    for i, s in enumerate(scores_data, 1):
        marker = " <--" if s['name'] == '平田' else ""
        print(f"  {i}. {s['name']}({s['dept']}): {s['total']}/50{marker}")

    total_members = len(scores_data)
    hirata_rank = next((i+1 for i, s in enumerate(scores_data) if s['name'] == '平田'), '-')
    hirata_data = next((s for s in scores_data if s['name'] == '平田'), None)

    # メディカ平均
    medica = [s for s in scores_data if s['dept'] == 'メディカ']
    avg = {
        'discovery': sum(s['discovery'] for s in medica) / len(medica),
        'proposal': sum(s['proposal'] for s in medica) / len(medica),
        'urgency': sum(s['urgency'] for s in medica) / len(medica),
        'differentiation': sum(s['differentiation'] for s in medica) / len(medica),
        'closing': sum(s['closing'] for s in medica) / len(medica),
    }

    # ===== HTML構築 =====
    print("HTML生成中...", flush=True)

    # ランキングテーブル
    ranking_rows = ""
    for i, s in enumerate(scores_data, 1):
        tier, color = get_tier(i, total_members)
        is_h = s['name'] == '平田'
        row_style = 'background:rgba(124,58,237,0.12);border-left:3px solid #7c3aed' if is_h else ''
        dept_badge = '<span style="background:#7c3aed;color:white;padding:1px 6px;border-radius:8px;font-size:11px;margin-left:4px">リクロジ</span>' if s['dept'] == 'リクロジ' else ''
        name_html = f'<strong>{s["name"]}</strong>{dept_badge}' if is_h else f'{s["name"]}{dept_badge}'
        ranking_rows += f'''<tr style="{row_style}">
            <td style="text-align:center;font-weight:700;color:{color}">{i}</td>
            <td style="text-align:center"><span style="background:{color};color:white;padding:1px 8px;border-radius:4px;font-size:12px;font-weight:700">{tier}</span></td>
            <td>{name_html}</td>
            <td style="text-align:center;color:#94a3b8">{s['count']}件</td>
            {score_cell_html(s['discovery'])}
            {score_cell_html(s['proposal'])}
            {score_cell_html(s['urgency'])}
            {score_cell_html(s['differentiation'])}
            {score_cell_html(s['closing'])}
            <td style="text-align:center;font-weight:700;font-size:16px;color:{color}">{s['total']}</td>
        </tr>'''

    # メンバー詳細パネル
    member_panels = ""
    member_buttons = ""
    for i, s in enumerate(scores_data, 1):
        name = s['name']
        data = members.get(name, {})
        report = data.get('full_report', '')
        tier, color = get_tier(i, total_members)
        dept_label = f' ({s["dept"]})' if s['dept'] != 'メディカ' else ''
        report_html = md_to_html(report[:3500])

        btn_cls = 'member-btn'
        member_buttons += f'<button class="{btn_cls}" onclick="showMember(\'{name}\')">{name}</button> '

        member_panels += f'''
        <div class="member-panel" id="member-{name}" style="display:none;background:#1e293b;border-radius:12px;padding:24px;margin-top:16px">
            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px">
                <h3 style="color:white;margin:0;font-size:20px">{name}{dept_label}
                    <span style="background:{color};color:white;padding:2px 10px;border-radius:6px;font-size:14px;margin-left:8px">Tier {tier}</span>
                    <span style="color:#94a3b8;font-size:14px;margin-left:8px">#{i} / {total_members}名</span>
                </h3>
                <div style="text-align:right">
                    <div style="font-size:32px;font-weight:700;color:{color}">{s['total']}<span style="font-size:16px;color:#64748b">/50</span></div>
                    <div style="color:#94a3b8;font-size:12px">{s['count']}件の商談分析</div>
                </div>
            </div>
            <div style="display:grid;grid-template-columns:repeat(5,1fr);gap:8px;margin-bottom:20px">
                <div style="background:#0f172a;border-radius:8px;padding:12px;text-align:center">
                    <div style="color:#94a3b8;font-size:11px">課題発見力</div>
                    <div style="font-size:24px;font-weight:700;color:#60a5fa">{s['discovery']}</div>
                </div>
                <div style="background:#0f172a;border-radius:8px;padding:12px;text-align:center">
                    <div style="color:#94a3b8;font-size:11px">提案力</div>
                    <div style="font-size:24px;font-weight:700;color:#a78bfa">{s['proposal']}</div>
                </div>
                <div style="background:#0f172a;border-radius:8px;padding:12px;text-align:center">
                    <div style="color:#94a3b8;font-size:11px">危機感醸成力</div>
                    <div style="font-size:24px;font-weight:700;color:#f472b6">{s['urgency']}</div>
                </div>
                <div style="background:#0f172a;border-radius:8px;padding:12px;text-align:center">
                    <div style="color:#94a3b8;font-size:11px">差別化力</div>
                    <div style="font-size:24px;font-weight:700;color:#34d399">{s['differentiation']}</div>
                </div>
                <div style="background:#0f172a;border-radius:8px;padding:12px;text-align:center">
                    <div style="color:#94a3b8;font-size:11px">クロージング力</div>
                    <div style="font-size:24px;font-weight:700;color:#fbbf24">{s['closing']}</div>
                </div>
            </div>
            <canvas id="radar-{name}" width="300" height="300" style="max-width:350px;margin:0 auto 20px;display:block"></canvas>
            <div style="max-height:500px;overflow-y:auto;padding:12px;background:#0f172a;border-radius:8px">
                {report_html}
            </div>
        </div>'''

    # JS用データ
    radar_json = {}
    for s in scores_data:
        radar_json[s['name']] = [s['discovery'], s['proposal'], s['urgency'], s['differentiation'], s['closing']]
    avg_list = [round(avg['discovery'], 1), round(avg['proposal'], 1), round(avg['urgency'], 1), round(avg['differentiation'], 1), round(avg['closing'], 1)]

    chart_labels_json = json.dumps([s['name'] for s in scores_data], ensure_ascii=False)
    chart_totals_json = json.dumps([s['total'] for s in scores_data])
    chart_colors_json = json.dumps([get_tier(i+1, total_members)[1] for i in range(total_members)])
    radar_json_str = json.dumps(radar_json, ensure_ascii=False)
    avg_json = json.dumps(avg_list)
    top3_json = json.dumps([scores_data[i]['name'] for i in range(min(3, len(scores_data)))], ensure_ascii=False)
    total_meetings = sum(s['count'] for s in scores_data)

    h_disc = hirata_data['discovery'] if hirata_data else '-'
    h_prop = hirata_data['proposal'] if hirata_data else '-'
    h_urg = hirata_data['urgency'] if hirata_data else '-'
    h_diff = hirata_data['differentiation'] if hirata_data else '-'
    h_clos = hirata_data['closing'] if hirata_data else '-'
    h_total = hirata_data['total'] if hirata_data else '-'

    org_html = md_to_html(org_summary[:5000])

    html = f'''<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>営業力スコアリングレポート - 2026年1月</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{background:#0f172a;color:#e2e8f0;font-family:'Segoe UI','Hiragino Sans',sans-serif}}
.container{{max-width:1400px;margin:0 auto;padding:24px}}
h1{{text-align:center;font-size:28px;margin-bottom:8px}}
.subtitle{{text-align:center;color:#64748b;margin-bottom:32px;font-size:14px}}
.kpi-grid{{display:grid;grid-template-columns:repeat(5,1fr);gap:12px;margin-bottom:32px}}
.kpi-card{{background:#1e293b;border-radius:12px;padding:16px;text-align:center}}
.kpi-card .label{{color:#94a3b8;font-size:12px;margin-bottom:4px}}
.kpi-card .value{{font-size:28px;font-weight:700}}
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
.legend{{display:flex;gap:16px;justify-content:center;margin:16px 0;flex-wrap:wrap}}
.legend-item{{display:flex;align-items:center;gap:4px;font-size:12px;color:#94a3b8}}
.legend-dot{{width:12px;height:12px;border-radius:50%}}
@media(max-width:900px){{.kpi-grid{{grid-template-columns:repeat(3,1fr)}}.chart-grid{{grid-template-columns:1fr}}}}
</style>
</head>
<body>
<div class="container">
    <h1>営業力スコアリングレポート</h1>
    <div class="subtitle">メディカ事業部 x リクロジ事業部（平田） | 2026年1月度 | Zoom商談分析 {total_meetings}件</div>

    <div class="kpi-grid">
        <div class="kpi-card"><div class="label">課題発見力 (平均)</div><div class="value" style="color:#60a5fa">{avg['discovery']:.1f}</div><div class="sub">平田: {h_disc}</div></div>
        <div class="kpi-card"><div class="label">提案力 (平均)</div><div class="value" style="color:#a78bfa">{avg['proposal']:.1f}</div><div class="sub">平田: {h_prop}</div></div>
        <div class="kpi-card"><div class="label">危機感醸成力 (平均)</div><div class="value" style="color:#f472b6">{avg['urgency']:.1f}</div><div class="sub">平田: {h_urg}</div></div>
        <div class="kpi-card"><div class="label">差別化力 (平均)</div><div class="value" style="color:#34d399">{avg['differentiation']:.1f}</div><div class="sub">平田: {h_diff}</div></div>
        <div class="kpi-card"><div class="label">クロージング力 (平均)</div><div class="value" style="color:#fbbf24">{avg['closing']:.1f}</div><div class="sub">平田: {h_clos}</div></div>
    </div>

    <div class="chart-grid">
        <div class="chart-box"><h3>総合スコアランキング</h3><canvas id="barChart" height="400"></canvas></div>
        <div class="chart-box">
            <h3>Tier分布</h3>
            <canvas id="doughnutChart" height="200"></canvas>
            <div style="margin-top:16px">
                <h3 style="font-size:14px;margin-bottom:8px">平田の位置</h3>
                <div style="background:#0f172a;border-radius:8px;padding:16px;border:1px solid #7c3aed">
                    <div style="display:flex;justify-content:space-between;align-items:center">
                        <div><span style="font-size:24px;font-weight:700;color:#7c3aed">{hirata_rank}位</span><span style="color:#94a3b8;font-size:13px"> / {total_members}名中</span></div>
                        <div style="text-align:right"><div style="font-size:28px;font-weight:700;color:#7c3aed">{h_total}<span style="font-size:14px;color:#64748b">/50</span></div></div>
                    </div>
                    <div style="color:#94a3b8;font-size:12px;margin-top:8px">リクロジ事業部（製造・物流系人材紹介）36件の商談分析</div>
                </div>
            </div>
        </div>
    </div>

    <div class="chart-grid">
        <div class="chart-box"><h3>軸別比較: トップ3 vs 平田 vs 組織平均</h3><canvas id="radarCompare" height="350"></canvas></div>
        <div class="chart-box"><h3>軸別スコア分布</h3><canvas id="axisChart" height="350"></canvas></div>
    </div>

    <div class="section">
        <h2>総合ランキング</h2>
        <div class="legend">
            <div class="legend-item"><div class="legend-dot" style="background:#10b981"></div>S: トップ</div>
            <div class="legend-item"><div class="legend-dot" style="background:#3b82f6"></div>A: 上位</div>
            <div class="legend-item"><div class="legend-dot" style="background:#f59e0b"></div>B: 中堅</div>
            <div class="legend-item"><div class="legend-dot" style="background:#f97316"></div>C: 下位</div>
            <div class="legend-item"><div class="legend-dot" style="background:#ef4444"></div>D: 要改善</div>
            <div class="legend-item"><div class="legend-dot" style="background:#7c3aed"></div>リクロジ</div>
        </div>
        <div style="overflow-x:auto">
        <table>
            <thead><tr>
                <th style="width:40px">#</th><th style="width:50px">Tier</th><th style="text-align:left">メンバー</th>
                <th>商談数</th><th>課題発見</th><th>提案力</th><th>危機感</th><th>差別化</th><th>CL力</th><th style="width:60px">合計</th>
            </tr></thead>
            <tbody>{ranking_rows}</tbody>
        </table>
        </div>
    </div>

    <div class="section">
        <h2>メンバー詳細</h2>
        <div style="text-align:center;margin-bottom:12px">{member_buttons}</div>
        {member_panels}
    </div>

    <div class="section">
        <h2>組織サマリー（メディカ事業部）</h2>
        <div style="max-height:600px;overflow-y:auto;padding:12px;background:#0f172a;border-radius:8px">{org_html}</div>
    </div>
</div>

<script>
const radarData = {radar_json_str};
const avgData = {avg_json};
const axisLabels = ['課題発見力','提案力','危機感醸成力','差別化力','クロージング力'];
const chartLabels = {chart_labels_json};
const chartTotals = {chart_totals_json};
const chartColors = {chart_colors_json};
const top3 = {top3_json};

// Bar Chart
new Chart(document.getElementById('barChart'), {{
    type:'bar',
    data:{{labels:chartLabels,datasets:[{{data:chartTotals,backgroundColor:chartLabels.map((n,i)=>n==='平田'?'#7c3aed':chartColors[i]),borderColor:chartLabels.map(n=>n==='平田'?'#a78bfa':'transparent'),borderWidth:chartLabels.map(n=>n==='平田'?2:0),borderRadius:4}}]}},
    options:{{indexAxis:'y',responsive:true,plugins:{{legend:{{display:false}}}},scales:{{x:{{max:50,grid:{{color:'#1e293b'}},ticks:{{color:'#94a3b8'}}}},y:{{grid:{{display:false}},ticks:{{color:'#e2e8f0',font:{{size:12}}}}}}}}}}
}});

// Doughnut
const tc={{S:0,A:0,B:0,C:0,D:0}};
chartLabels.forEach((n,i)=>{{const p=(i+1)/{total_members};if(p<=0.15)tc.S++;else if(p<=0.35)tc.A++;else if(p<=0.65)tc.B++;else if(p<=0.85)tc.C++;else tc.D++}});
new Chart(document.getElementById('doughnutChart'),{{type:'doughnut',data:{{labels:['S','A','B','C','D'],datasets:[{{data:Object.values(tc),backgroundColor:['#10b981','#3b82f6','#f59e0b','#f97316','#ef4444']}}]}},options:{{responsive:true,plugins:{{legend:{{position:'bottom',labels:{{color:'#94a3b8'}}}}}}}}}});

// Radar Compare
new Chart(document.getElementById('radarCompare'),{{
    type:'radar',
    data:{{labels:axisLabels,datasets:[
        {{label:top3[0],data:radarData[top3[0]],borderColor:'#10b981',backgroundColor:'rgba(16,185,129,0.1)',pointRadius:3}},
        {{label:top3[1],data:radarData[top3[1]],borderColor:'#3b82f6',backgroundColor:'rgba(59,130,246,0.1)',pointRadius:3}},
        {{label:top3[2],data:radarData[top3[2]],borderColor:'#f59e0b',backgroundColor:'rgba(245,158,11,0.1)',pointRadius:3}},
        {{label:'平田',data:radarData['平田'],borderColor:'#7c3aed',backgroundColor:'rgba(124,58,237,0.15)',borderWidth:3,pointRadius:4}},
        {{label:'組織平均',data:avgData,borderColor:'#64748b',backgroundColor:'rgba(100,116,139,0.1)',borderDash:[5,5],pointRadius:2}}
    ]}},
    options:{{responsive:true,scales:{{r:{{min:0,max:10,ticks:{{stepSize:2,color:'#64748b'}},grid:{{color:'#334155'}},pointLabels:{{color:'#e2e8f0',font:{{size:12}}}}}}}},plugins:{{legend:{{position:'bottom',labels:{{color:'#94a3b8',usePointStyle:true}}}}}}}}
}});

// Axis bar chart
const hirataScores=radarData['平田'];
const allVals=Object.values(radarData);
const maxS=axisLabels.map((_,i)=>Math.max(...allVals.map(d=>d[i])));
const minS=axisLabels.map((_,i)=>Math.min(...allVals.map(d=>d[i])));
new Chart(document.getElementById('axisChart'),{{
    type:'bar',
    data:{{labels:axisLabels,datasets:[
        {{label:'最高',data:maxS,backgroundColor:'rgba(16,185,129,0.3)',borderColor:'#10b981',borderWidth:1}},
        {{label:'平均',data:avgData,backgroundColor:'rgba(100,116,139,0.3)',borderColor:'#64748b',borderWidth:1}},
        {{label:'平田',data:hirataScores,backgroundColor:'rgba(124,58,237,0.4)',borderColor:'#7c3aed',borderWidth:2}},
        {{label:'最低',data:minS,backgroundColor:'rgba(239,68,68,0.2)',borderColor:'#ef4444',borderWidth:1}}
    ]}},
    options:{{responsive:true,scales:{{y:{{max:10,grid:{{color:'#1e293b'}},ticks:{{color:'#94a3b8'}}}},x:{{grid:{{display:false}},ticks:{{color:'#e2e8f0'}}}}}},plugins:{{legend:{{position:'bottom',labels:{{color:'#94a3b8'}}}}}}}}
}});

// Member detail panels
function showMember(name){{
    document.querySelectorAll('.member-panel').forEach(p=>p.style.display='none');
    document.querySelectorAll('.member-btn').forEach(b=>b.classList.remove('active'));
    const panel=document.getElementById('member-'+name);
    if(panel){{
        panel.style.display='block';
        const canvas=document.getElementById('radar-'+name);
        if(canvas&&!canvas.dataset.drawn){{
            new Chart(canvas,{{
                type:'radar',
                data:{{labels:axisLabels,datasets:[
                    {{label:name,data:radarData[name],borderColor:'#3b82f6',backgroundColor:'rgba(59,130,246,0.2)',borderWidth:2}},
                    {{label:'組織平均',data:avgData,borderColor:'#64748b',backgroundColor:'rgba(100,116,139,0.1)',borderDash:[5,5]}}
                ]}},
                options:{{responsive:true,scales:{{r:{{min:0,max:10,ticks:{{stepSize:2,color:'#64748b'}},grid:{{color:'#334155'}},pointLabels:{{color:'#e2e8f0'}}}}}},plugins:{{legend:{{labels:{{color:'#94a3b8'}}}}}}}}
            }});
            canvas.dataset.drawn='true';
        }}
    }}
    if(event&&event.target)event.target.classList.add('active');
}}
</script>
</body>
</html>'''

    output_path = os.path.join(out_dir, "zoom_scoring_report_202601.html")
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"\n完了: {output_path}")
    print(f"HTMLサイズ: {len(html):,}文字")

if __name__ == '__main__':
    main()
