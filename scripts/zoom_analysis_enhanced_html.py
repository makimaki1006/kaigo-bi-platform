"""
強化版HTML生成スクリプト
zoom_analysis_summary_202601.xlsx から可視化付きHTMLを生成する
"""

import io
import re
import sys
from pathlib import Path

import pandas as pd

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "data" / "output" / "zoom_transcripts"
EXCEL_PATH = OUTPUT_DIR / "zoom_analysis_summary_202601.xlsx"


def extract_rating(text: str) -> str:
    """レポートテキストからS/A/B/C/D評価を抽出"""
    m = re.search(r"\*\*([SABCD])[:\s]", text)
    if not m:
        m = re.search(r"\*\*([SABCD])\*\*", text)
    return m.group(1) if m else "C"


def rating_to_score(r: str) -> int:
    return {"S": 5, "A": 4, "B": 3, "C": 2, "D": 1}.get(r, 2)


def rating_color(r: str) -> str:
    return {
        "S": "#7c4dff", "A": "#2979ff", "B": "#00bfa5",
        "C": "#ff9100", "D": "#ff1744",
    }.get(r, "#757575")


def markdown_to_html(text: str) -> str:
    """Markdown→HTML変換（強化版）"""
    import html as h

    lines = text.split("\n")
    result = []
    in_list = False
    in_table = False

    for line in lines:
        stripped = line.strip()
        if not stripped:
            if in_list:
                result.append("</ul>")
                in_list = False
            if in_table:
                result.append("</tbody></table></div>")
                in_table = False
            continue

        # テーブル行
        if "|" in stripped and stripped.startswith("|"):
            cells = [c.strip() for c in stripped.split("|")[1:-1]]
            if all(set(c) <= set("- :") for c in cells):
                continue  # セパレータ行スキップ
            if not in_table:
                result.append('<div class="table-wrap"><table><thead><tr>')
                for c in cells:
                    c_html = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", h.escape(c))
                    result.append(f"<th>{c_html}</th>")
                result.append("</tr></thead><tbody>")
                in_table = True
            else:
                result.append("<tr>")
                for c in cells:
                    c_html = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", h.escape(c))
                    result.append(f"<td>{c_html}</td>")
                result.append("</tr>")
            continue

        if in_table:
            result.append("</tbody></table></div>")
            in_table = False

        if stripped.startswith("### "):
            if in_list:
                result.append("</ul>")
                in_list = False
            result.append(f"<h4>{h.escape(stripped[4:])}</h4>")
        elif stripped.startswith("## "):
            if in_list:
                result.append("</ul>")
                in_list = False
            result.append(f"<h3>{h.escape(stripped[3:])}</h3>")
        elif stripped.startswith("# "):
            if in_list:
                result.append("</ul>")
                in_list = False
            result.append(f"<h2>{h.escape(stripped[2:])}</h2>")
        elif stripped.startswith("- ") or stripped.startswith("* "):
            if not in_list:
                result.append("<ul>")
                in_list = True
            content = stripped[2:]
            content = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", h.escape(content))
            result.append(f"<li>{content}</li>")
        else:
            if in_list:
                result.append("</ul>")
                in_list = False
            content = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", h.escape(stripped))
            result.append(f"<p>{content}</p>")

    if in_list:
        result.append("</ul>")
    if in_table:
        result.append("</tbody></table></div>")

    return "\n".join(result)


def main():
    print("強化版HTML生成開始", flush=True)

    xls = pd.ExcelFile(str(EXCEL_PATH))

    # 組織サマリー
    org_df = pd.read_excel(xls, sheet_name="組織サマリー")
    org_report = str(org_df.iloc[0, 0])

    # 個人データ収集
    members = []
    for s in xls.sheet_names[1:]:
        df = pd.read_excel(xls, sheet_name=s)
        report = str(df["傾向レポート"].iloc[0])
        rating = extract_rating(report)
        members.append({
            "name": s,
            "count": int(df["商談数"].iloc[0]),
            "rating": rating,
            "score": rating_to_score(rating),
            "color": rating_color(rating),
            "report": report,
        })

    # スコア順にソート
    members_sorted = sorted(members, key=lambda x: (-x["score"], -x["count"]))

    # 統計計算
    total_deals = sum(m["count"] for m in members)
    rating_dist = {}
    for m in members:
        rating_dist[m["rating"]] = rating_dist.get(m["rating"], 0) + 1

    # 組織サマリーからスコアカードを抽出
    scorecard_items = ["課題発見力", "提案力", "危機感醸成力", "差別化力", "クロージング力"]
    scorecard_ratings = {}
    for item in scorecard_items:
        m = re.search(rf"{item}[^\n]*?([SABCD])", org_report)
        scorecard_ratings[item] = m.group(1) if m else "C"

    # メンバーカードHTML
    member_cards = []
    for i, m in enumerate(members_sorted):
        rank = i + 1
        medal = ""
        if rank == 1:
            medal = '<span class="medal gold">1st</span>'
        elif rank == 2:
            medal = '<span class="medal silver">2nd</span>'
        elif rank == 3:
            medal = '<span class="medal bronze">3rd</span>'

        member_cards.append(f"""
        <div class="member-card" onclick="openMember({i})">
            <div class="card-header">
                {medal}
                <span class="badge" style="background:{m['color']}">{m['rating']}</span>
                <span class="card-name">{m['name']}</span>
            </div>
            <div class="card-deals">{m['count']}件</div>
        </div>""")

    # メンバー詳細パネル
    member_panels = []
    for i, m in enumerate(members_sorted):
        report_html = markdown_to_html(m["report"])
        member_panels.append(f"""
        <div id="member-{i}" class="member-panel" style="display:none;">
            <div class="panel-header">
                <span class="badge-lg" style="background:{m['color']}">{m['rating']}</span>
                <div>
                    <h2 class="panel-name">{m['name']}</h2>
                    <span class="panel-deals">分析対象: {m['count']}件</span>
                </div>
            </div>
            <div class="panel-body">
                {report_html}
            </div>
        </div>""")

    # Chart.jsデータ
    chart_names = [m["name"] for m in members_sorted]
    chart_scores = [m["score"] for m in members_sorted]
    chart_colors = [m["color"] for m in members_sorted]
    chart_counts = [m["count"] for m in members_sorted]

    # レーダーチャート用データ
    radar_labels = scorecard_items
    radar_scores = [rating_to_score(scorecard_ratings[item]) for item in scorecard_items]

    # 評価分布
    dist_labels = ["S", "A", "B", "C", "D"]
    dist_values = [rating_dist.get(r, 0) for r in dist_labels]
    dist_colors = [rating_color(r) for r in dist_labels]

    org_html = markdown_to_html(org_report)

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Medica 営業分析ダッシュボード - 2026年1月</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<style>
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{ font-family: 'Meiryo', 'Hiragino Sans', -apple-system, sans-serif; background: #0f172a; color: #e2e8f0; min-height: 100vh; }}

/* ヘッダー */
.header {{ background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%); padding: 32px 40px; border-bottom: 1px solid #334155; }}
.header h1 {{ font-size: 28px; color: #f8fafc; margin-bottom: 8px; }}
.header .subtitle {{ color: #94a3b8; font-size: 15px; }}

/* ナビゲーション */
.nav {{ display: flex; gap: 0; background: #1e293b; border-bottom: 1px solid #334155; padding: 0 40px; position: sticky; top: 0; z-index: 100; }}
.nav-btn {{ padding: 14px 24px; background: none; border: none; color: #94a3b8; font-size: 14px; cursor: pointer; border-bottom: 3px solid transparent; transition: all 0.2s; font-weight: 500; }}
.nav-btn:hover {{ color: #e2e8f0; background: #334155; }}
.nav-btn.active {{ color: #60a5fa; border-bottom-color: #3b82f6; }}

/* メインコンテナ */
.container {{ max-width: 1400px; margin: 0 auto; padding: 32px 40px; }}

/* セクション */
.section {{ display: none; }}
.section.active {{ display: block; }}

/* KPIカード */
.kpi-row {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 20px; margin-bottom: 32px; }}
.kpi-card {{ background: #1e293b; border: 1px solid #334155; border-radius: 12px; padding: 24px; }}
.kpi-label {{ color: #94a3b8; font-size: 13px; margin-bottom: 8px; text-transform: uppercase; letter-spacing: 0.5px; }}
.kpi-value {{ font-size: 36px; font-weight: 700; color: #f8fafc; }}
.kpi-sub {{ color: #64748b; font-size: 13px; margin-top: 4px; }}

/* チャートグリッド */
.chart-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 24px; margin-bottom: 32px; }}
.chart-box {{ background: #1e293b; border: 1px solid #334155; border-radius: 12px; padding: 24px; }}
.chart-box h3 {{ color: #f8fafc; font-size: 16px; margin-bottom: 16px; }}
.chart-box canvas {{ max-height: 300px; }}

/* スコアカード */
.scorecard {{ display: grid; grid-template-columns: repeat(5, 1fr); gap: 16px; margin-bottom: 32px; }}
.score-item {{ background: #1e293b; border: 1px solid #334155; border-radius: 12px; padding: 20px; text-align: center; }}
.score-item .axis-name {{ color: #94a3b8; font-size: 13px; margin-bottom: 12px; }}
.score-item .axis-grade {{ font-size: 40px; font-weight: 800; }}

/* メンバーカード */
.member-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(180px, 1fr)); gap: 12px; margin-bottom: 32px; }}
.member-card {{ background: #1e293b; border: 1px solid #334155; border-radius: 10px; padding: 16px; cursor: pointer; transition: all 0.2s; }}
.member-card:hover {{ border-color: #3b82f6; transform: translateY(-2px); box-shadow: 0 4px 12px rgba(59,130,246,0.2); }}
.member-card.selected {{ border-color: #3b82f6; background: #1e3a5f; }}
.card-header {{ display: flex; align-items: center; gap: 8px; margin-bottom: 8px; }}
.card-name {{ font-size: 15px; font-weight: 600; color: #f8fafc; }}
.card-deals {{ color: #64748b; font-size: 13px; }}

/* バッジ */
.badge {{ display: inline-flex; align-items: center; justify-content: center; width: 28px; height: 28px; border-radius: 6px; color: #fff; font-weight: 800; font-size: 14px; flex-shrink: 0; }}
.badge-lg {{ display: inline-flex; align-items: center; justify-content: center; width: 48px; height: 48px; border-radius: 10px; color: #fff; font-weight: 800; font-size: 24px; flex-shrink: 0; }}

/* メダル */
.medal {{ font-size: 11px; font-weight: 700; padding: 2px 6px; border-radius: 4px; }}
.medal.gold {{ background: #fbbf24; color: #78350f; }}
.medal.silver {{ background: #9ca3af; color: #1f2937; }}
.medal.bronze {{ background: #d97706; color: #fff; }}

/* メンバー詳細パネル */
.member-panel {{ background: #1e293b; border: 1px solid #334155; border-radius: 12px; overflow: hidden; }}
.panel-header {{ display: flex; align-items: center; gap: 16px; padding: 24px 32px; background: #0f172a; border-bottom: 1px solid #334155; }}
.panel-name {{ font-size: 22px; font-weight: 700; color: #f8fafc; }}
.panel-deals {{ color: #94a3b8; font-size: 14px; }}
.panel-body {{ padding: 32px; line-height: 1.9; }}
.panel-body h2 {{ color: #60a5fa; font-size: 20px; margin: 28px 0 12px; padding-bottom: 8px; border-bottom: 1px solid #334155; }}
.panel-body h3 {{ color: #38bdf8; font-size: 17px; margin: 24px 0 10px; padding-left: 14px; border-left: 4px solid #3b82f6; }}
.panel-body h4 {{ color: #93c5fd; font-size: 15px; margin: 18px 0 8px; }}
.panel-body p {{ color: #cbd5e1; margin: 8px 0; }}
.panel-body ul {{ margin: 8px 0; padding-left: 24px; }}
.panel-body li {{ color: #cbd5e1; margin: 6px 0; }}
.panel-body strong {{ color: #f87171; }}

/* テーブル */
.table-wrap {{ overflow-x: auto; margin: 12px 0; }}
.panel-body table, .org-content table {{ width: 100%; border-collapse: collapse; margin: 8px 0; }}
.panel-body th, .org-content th {{ background: #334155; color: #e2e8f0; padding: 10px 14px; text-align: left; font-size: 13px; border: 1px solid #475569; }}
.panel-body td, .org-content td {{ padding: 10px 14px; border: 1px solid #334155; color: #cbd5e1; font-size: 13px; }}
.panel-body tr:nth-child(even) td, .org-content tr:nth-child(even) td {{ background: #1a2332; }}

/* 組織サマリー */
.org-content {{ line-height: 1.9; }}
.org-content h2 {{ color: #60a5fa; font-size: 20px; margin: 28px 0 12px; padding-bottom: 8px; border-bottom: 1px solid #334155; }}
.org-content h3 {{ color: #38bdf8; font-size: 17px; margin: 24px 0 10px; padding-left: 14px; border-left: 4px solid #3b82f6; }}
.org-content h4 {{ color: #93c5fd; font-size: 15px; margin: 18px 0 8px; }}
.org-content p {{ color: #cbd5e1; margin: 8px 0; }}
.org-content ul {{ margin: 8px 0; padding-left: 24px; }}
.org-content li {{ color: #cbd5e1; margin: 6px 0; }}
.org-content strong {{ color: #f87171; }}

/* ランキングテーブル */
.ranking-table {{ width: 100%; border-collapse: collapse; }}
.ranking-table th {{ background: #334155; color: #e2e8f0; padding: 12px 16px; text-align: left; font-size: 13px; }}
.ranking-table td {{ padding: 12px 16px; border-bottom: 1px solid #334155; }}
.ranking-table tr:hover {{ background: #1e3a5f; }}

/* レスポンシブ */
@media (max-width: 1024px) {{
    .kpi-row {{ grid-template-columns: repeat(2, 1fr); }}
    .chart-grid {{ grid-template-columns: 1fr; }}
    .scorecard {{ grid-template-columns: repeat(3, 1fr); }}
}}
@media (max-width: 640px) {{
    .kpi-row {{ grid-template-columns: 1fr; }}
    .scorecard {{ grid-template-columns: repeat(2, 1fr); }}
    .container {{ padding: 16px; }}
}}
</style>
</head>
<body>

<div class="header">
    <h1>Medica 営業分析ダッシュボード</h1>
    <div class="subtitle">2026年1月 | {len(members)}名 | {total_deals}商談</div>
</div>

<div class="nav">
    <button class="nav-btn active" onclick="showSection('dashboard', this)">ダッシュボード</button>
    <button class="nav-btn" onclick="showSection('members', this)">メンバー詳細</button>
    <button class="nav-btn" onclick="showSection('org', this)">組織サマリー</button>
</div>

<!-- ===== ダッシュボード ===== -->
<div class="container">
<div id="sec-dashboard" class="section active">

    <!-- KPIカード -->
    <div class="kpi-row">
        <div class="kpi-card">
            <div class="kpi-label">分析対象メンバー</div>
            <div class="kpi-value">{len(members)}<span style="font-size:18px;color:#64748b">名</span></div>
        </div>
        <div class="kpi-card">
            <div class="kpi-label">総商談数</div>
            <div class="kpi-value">{total_deals}<span style="font-size:18px;color:#64748b">件</span></div>
        </div>
        <div class="kpi-card">
            <div class="kpi-label">平均商談数</div>
            <div class="kpi-value">{total_deals // len(members)}<span style="font-size:18px;color:#64748b">件/人</span></div>
        </div>
        <div class="kpi-card">
            <div class="kpi-label">評価A以上</div>
            <div class="kpi-value" style="color:#2979ff">{rating_dist.get('S', 0) + rating_dist.get('A', 0)}<span style="font-size:18px;color:#64748b">名</span></div>
        </div>
    </div>

    <!-- 組織スコアカード -->
    <h3 style="color:#f8fafc;margin-bottom:16px;font-size:18px;">組織スコアカード</h3>
    <div class="scorecard">
        {"".join(f'''
        <div class="score-item">
            <div class="axis-name">{item}</div>
            <div class="axis-grade" style="color:{rating_color(scorecard_ratings[item])}">{scorecard_ratings[item]}</div>
        </div>''' for item in scorecard_items)}
    </div>

    <!-- チャート -->
    <div class="chart-grid">
        <div class="chart-box">
            <h3>メンバー別評価スコア</h3>
            <canvas id="chartScore"></canvas>
        </div>
        <div class="chart-box">
            <h3>評価分布</h3>
            <canvas id="chartDist"></canvas>
        </div>
        <div class="chart-box">
            <h3>メンバー別商談数</h3>
            <canvas id="chartDeals"></canvas>
        </div>
        <div class="chart-box">
            <h3>組織力レーダー</h3>
            <canvas id="chartRadar"></canvas>
        </div>
    </div>

    <!-- ランキング -->
    <div class="chart-box" style="margin-bottom:32px;">
        <h3>メンバーランキング</h3>
        <table class="ranking-table">
            <thead><tr><th>#</th><th>メンバー</th><th>評価</th><th>商談数</th></tr></thead>
            <tbody>
            {"".join(f'''<tr>
                <td style="color:#64748b">{i+1}</td>
                <td><strong style="color:#f8fafc">{m['name']}</strong></td>
                <td><span class="badge" style="background:{m['color']}">{m['rating']}</span></td>
                <td>{m['count']}件</td>
            </tr>''' for i, m in enumerate(members_sorted))}
            </tbody>
        </table>
    </div>

</div>

<!-- ===== メンバー詳細 ===== -->
<div id="sec-members" class="section">
    <h3 style="color:#f8fafc;margin-bottom:16px;font-size:18px;">メンバーを選択</h3>
    <div class="member-grid">
        {"".join(member_cards)}
    </div>
    <div id="member-detail">
        <div style="text-align:center;color:#64748b;padding:60px;">メンバーカードをクリックして詳細を表示</div>
    </div>
    {"".join(member_panels)}
</div>

<!-- ===== 組織サマリー ===== -->
<div id="sec-org" class="section">
    <div class="chart-box org-content">
        <h2 style="margin-top:0;">組織全体サマリーレポート</h2>
        {org_html}
    </div>
</div>

</div>

<script>
// ナビゲーション
function showSection(id, btn) {{
    document.querySelectorAll('.section').forEach(el => el.classList.remove('active'));
    document.querySelectorAll('.nav-btn').forEach(el => el.classList.remove('active'));
    document.getElementById('sec-' + id).classList.add('active');
    btn.classList.add('active');
}}

// メンバー選択
function openMember(idx) {{
    document.querySelectorAll('.member-panel').forEach(el => el.style.display = 'none');
    document.querySelectorAll('.member-card').forEach(el => el.classList.remove('selected'));
    document.getElementById('member-' + idx).style.display = 'block';
    document.querySelectorAll('.member-card')[idx].classList.add('selected');
    document.getElementById('member-detail').innerHTML = '';
    document.getElementById('member-' + idx).scrollIntoView({{ behavior: 'smooth', block: 'nearest' }});
}}

// Chart.js設定
Chart.defaults.color = '#94a3b8';
Chart.defaults.borderColor = '#334155';

// メンバー別スコア
new Chart(document.getElementById('chartScore'), {{
    type: 'bar',
    data: {{
        labels: {chart_names},
        datasets: [{{
            label: 'スコア',
            data: {chart_scores},
            backgroundColor: {chart_colors},
            borderRadius: 6,
            barThickness: 24,
        }}]
    }},
    options: {{
        responsive: true,
        plugins: {{ legend: {{ display: false }} }},
        scales: {{
            y: {{ min: 0, max: 5, ticks: {{ stepSize: 1, callback: v => ['','D','C','B','A','S'][v] }} }},
            x: {{ ticks: {{ font: {{ size: 11 }} }} }}
        }}
    }}
}});

// 評価分布（ドーナツ）
new Chart(document.getElementById('chartDist'), {{
    type: 'doughnut',
    data: {{
        labels: {dist_labels},
        datasets: [{{
            data: {dist_values},
            backgroundColor: {dist_colors},
            borderWidth: 0,
        }}]
    }},
    options: {{
        responsive: true,
        plugins: {{
            legend: {{ position: 'bottom' }},
        }},
        cutout: '55%',
    }}
}});

// 商談数
new Chart(document.getElementById('chartDeals'), {{
    type: 'bar',
    data: {{
        labels: {chart_names},
        datasets: [{{
            label: '商談数',
            data: {chart_counts},
            backgroundColor: '#3b82f6',
            borderRadius: 6,
            barThickness: 24,
        }}]
    }},
    options: {{
        responsive: true,
        plugins: {{ legend: {{ display: false }} }},
        scales: {{
            y: {{ beginAtZero: true }},
            x: {{ ticks: {{ font: {{ size: 11 }} }} }}
        }}
    }}
}});

// レーダー
new Chart(document.getElementById('chartRadar'), {{
    type: 'radar',
    data: {{
        labels: {radar_labels},
        datasets: [{{
            label: '組織平均',
            data: {radar_scores},
            backgroundColor: 'rgba(59,130,246,0.2)',
            borderColor: '#3b82f6',
            borderWidth: 2,
            pointBackgroundColor: '#3b82f6',
        }}]
    }},
    options: {{
        responsive: true,
        scales: {{
            r: {{
                min: 0, max: 5,
                ticks: {{ stepSize: 1, callback: v => ['','D','C','B','A','S'][v], backdropColor: 'transparent' }},
                grid: {{ color: '#334155' }},
                angleLines: {{ color: '#334155' }},
                pointLabels: {{ font: {{ size: 13 }}, color: '#e2e8f0' }},
            }}
        }},
        plugins: {{ legend: {{ display: false }} }},
    }}
}});
</script>
</body>
</html>"""

    out_path = OUTPUT_DIR / "zoom_analysis_dashboard_202601.html"
    out_path.write_text(html, encoding="utf-8")
    print(f"出力: {out_path}", flush=True)
    print("完了", flush=True)


if __name__ == "__main__":
    main()
