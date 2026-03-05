# -*- coding: utf-8 -*-
"""HTMLレポートにレッドカード分析セクションを追加"""
import io
import json
import sys

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

OUTPUT_DIR = "data/output/sato_comparison"

with open(f"{OUTPUT_DIR}/sato_comparison_report.html", "r", encoding="utf-8") as f:
    html = f.read()

with open(f"{OUTPUT_DIR}/redcard_data.json", "r", encoding="utf-8") as f:
    rc_data = json.load(f)

ik = rc_data["ikeike"]
dm = rc_data["damedame"]


def abstract_cards(samples, color):
    cards = ""
    for s in samples[:10]:
        q = s.get("quote", "")[:120]
        fb = s.get("feedback", "")[:120]
        topic = s.get("topic", "")[:35]
        date = s.get("date", "")
        cards += (
            f'<div style="background:#0f172a;border-radius:6px;padding:10px;margin-bottom:6px;border-left:3px solid {color}">'
            f'<div style="font-size:11px;color:#64748b;margin-bottom:3px">{date} {topic}</div>'
            f'<div style="font-size:12px;color:#fbbf24;margin-bottom:3px">営業：「{q}」</div>'
            f'<div style="font-size:11px;color:#f87171">⚡ {fb}</div>'
            f"</div>"
        )
    return cards


def skip_cards(samples, color):
    cards = ""
    for s in samples[:10]:
        cq = s.get("customer_quote", "")[:120]
        sr = s.get("sales_reaction", "")[:120]
        fb = s.get("feedback", "")[:120]
        topic = s.get("topic", "")[:35]
        date = s.get("date", "")
        cards += (
            f'<div style="background:#0f172a;border-radius:6px;padding:10px;margin-bottom:6px;border-left:3px solid {color}">'
            f'<div style="font-size:11px;color:#64748b;margin-bottom:3px">{date} {topic}</div>'
            f'<div style="font-size:12px;color:#60a5fa;margin-bottom:2px">顧客：「{cq}」</div>'
            f'<div style="font-size:12px;color:#fbbf24;margin-bottom:2px">営業：「{sr}」</div>'
            f'<div style="font-size:11px;color:#f87171">⚡ {fb}</div>'
            f"</div>"
        )
    return cards


def keyword_bars(keywords, max_val, color):
    bars = ""
    for kw, cnt in keywords[:8]:
        width = min(cnt / max(max_val, 1) * 100, 100)
        bars += (
            f'<div style="display:flex;align-items:center;gap:6px;margin:3px 0">'
            f'<span style="width:80px;font-size:12px;color:#e2e8f0">「{kw}」</span>'
            f'<div style="flex:1;background:#1e293b;height:16px;border-radius:3px;overflow:hidden">'
            f'<div style="width:{width}%;height:100%;background:{color};border-radius:3px"></div></div>'
            f'<span style="font-size:11px;color:#94a3b8;width:30px">{cnt}</span></div>'
        )
    return bars


ik_abs_cards = abstract_cards(ik["abstract_samples"], "#3b82f6")
dm_abs_cards = abstract_cards(dm["abstract_samples"], "#ef4444")
ik_skip_cards = skip_cards(ik["skip_samples"], "#3b82f6")
dm_skip_cards = skip_cards(dm["skip_samples"], "#ef4444")

max_abs = max(
    [c for _, c in ik["abstract_keywords"]] + [c for _, c in dm["abstract_keywords"]],
    default=1,
)
ik_abs_bars = keyword_bars(ik["abstract_keywords"], max_abs, "#3b82f6")
dm_abs_bars = keyword_bars(dm["abstract_keywords"], max_abs, "#ef4444")

max_skip = max(
    [c for _, c in ik["skip_keywords"]] + [c for _, c in dm["skip_keywords"]],
    default=1,
)
ik_skip_bars = keyword_bars(ik["skip_keywords"], max_skip, "#3b82f6")
dm_skip_bars = keyword_bars(dm["skip_keywords"], max_skip, "#ef4444")

ik_total_per = ik["abstract_per_deal"] + ik["skip_per_deal"]
dm_total_per = dm["abstract_per_deal"] + dm["skip_per_deal"]
abs_diff = dm["abstract_per_deal"] - ik["abstract_per_deal"]
skip_diff = dm["skip_per_deal"] - ik["skip_per_deal"]

redcard_section = f"""
    <div class="section" style="border:2px solid #ef4444">
        <h2 style="color:#ef4444">🚩 レッドカード分析（抽象的逃げ＋顧客懸念スルー）</h2>
        <p style="color:#94a3b8;font-size:13px;margin-bottom:16px">Gemini AIが検出した「新人病」悪癖の比較分析</p>

        <div class="kpi-grid" style="grid-template-columns:repeat(6,1fr);margin-bottom:20px">
            <div class="kpi"><div class="label">🔵 抽象的逃げ/商談</div><div class="value" style="color:#60a5fa">{ik['abstract_per_deal']}</div><div class="sub">件/商談</div></div>
            <div class="kpi"><div class="label">🔴 抽象的逃げ/商談</div><div class="value" style="color:#f87171">{dm['abstract_per_deal']}</div><div class="sub">件/商談</div></div>
            <div class="kpi"><div class="label">差分</div><div class="value" style="color:#ef4444">+{abs_diff:.1f}</div><div class="sub">悪化</div></div>
            <div class="kpi"><div class="label">🔵 スルー/商談</div><div class="value" style="color:#60a5fa">{ik['skip_per_deal']}</div><div class="sub">件/商談</div></div>
            <div class="kpi"><div class="label">🔴 スルー/商談</div><div class="value" style="color:#f87171">{dm['skip_per_deal']}</div><div class="sub">件/商談</div></div>
            <div class="kpi"><div class="label">差分</div><div class="value" style="color:#ef4444">+{skip_diff:.1f}</div><div class="sub">悪化</div></div>
        </div>

        <div class="kpi-grid" style="grid-template-columns:repeat(2,1fr);margin-bottom:20px">
            <div class="kpi"><div class="label">🔵 レッドカード合計/商談</div><div class="value" style="font-size:36px;color:#60a5fa">{ik_total_per:.1f}</div></div>
            <div class="kpi"><div class="label">🔴 レッドカード合計/商談</div><div class="value" style="font-size:36px;color:#f87171">{dm_total_per:.1f}</div></div>
        </div>

        <h3 style="color:#fbbf24;font-size:15px;margin:20px 0 12px">🟡 抽象的逃げ（曖昧表現）</h3>
        <p style="color:#94a3b8;font-size:12px;margin-bottom:12px">データや固有名詞なく「実績があります」「プロがやります」等で逃げている箇所</p>

        <div class="grid2" style="margin-bottom:16px">
            <div>
                <h4 style="color:#60a5fa;font-size:13px;margin-bottom:8px">🔵 イケイケ期 頻出キーワード ({ik['abstract_total']}件)</h4>
                {ik_abs_bars}
            </div>
            <div>
                <h4 style="color:#f87171;font-size:13px;margin-bottom:8px">🔴 だめだめ期 頻出キーワード ({dm['abstract_total']}件)</h4>
                {dm_abs_bars}
            </div>
        </div>

        <div class="grid2" style="margin-bottom:24px">
            <div>
                <h4 style="color:#60a5fa;font-size:13px;margin-bottom:8px">🔵 イケイケ期 サンプル</h4>
                <div style="max-height:400px;overflow-y:auto">{ik_abs_cards}</div>
            </div>
            <div>
                <h4 style="color:#f87171;font-size:13px;margin-bottom:8px">🔴 だめだめ期 サンプル</h4>
                <div style="max-height:400px;overflow-y:auto">{dm_abs_cards}</div>
            </div>
        </div>

        <h3 style="color:#f97316;font-size:15px;margin:20px 0 12px">🟠 顧客懸念スルー（無視）</h3>
        <p style="color:#94a3b8;font-size:12px;margin-bottom:12px">顧客の「小さな懸念」や「本音」に共感・回答せず流してしまった箇所</p>

        <div class="grid2" style="margin-bottom:16px">
            <div>
                <h4 style="color:#60a5fa;font-size:13px;margin-bottom:8px">🔵 イケイケ期 スルーされた懸念 ({ik['skip_total']}件)</h4>
                {ik_skip_bars}
            </div>
            <div>
                <h4 style="color:#f87171;font-size:13px;margin-bottom:8px">🔴 だめだめ期 スルーされた懸念 ({dm['skip_total']}件)</h4>
                {dm_skip_bars}
            </div>
        </div>

        <div class="grid2" style="margin-bottom:16px">
            <div>
                <h4 style="color:#60a5fa;font-size:13px;margin-bottom:8px">🔵 イケイケ期 サンプル</h4>
                <div style="max-height:400px;overflow-y:auto">{ik_skip_cards}</div>
            </div>
            <div>
                <h4 style="color:#f87171;font-size:13px;margin-bottom:8px">🔴 だめだめ期 サンプル</h4>
                <div style="max-height:400px;overflow-y:auto">{dm_skip_cards}</div>
            </div>
        </div>

        <div style="background:#1a0a0a;border:1px solid #7f1d1d;border-radius:8px;padding:16px;margin-top:16px">
            <h3 style="color:#ef4444;font-size:15px;margin-bottom:10px">📊 レッドカード分析まとめ</h3>
            <ul style="color:#e2e8f0;font-size:13px;line-height:1.8;list-style:disc;padding-left:20px">
                <li><strong style="color:#fbbf24">抽象的逃げが1商談あたり+1.0件増加</strong>（5.9→6.9件）: だめだめ期は「ご安心ください」「効果があります」「対応します」等の新しい逃げパターンが出現</li>
                <li><strong style="color:#fbbf24">イケイケ期は「させていただく」連発</strong>（1.69回/商談）: 丁寧だが具体性に欠ける。だめだめ期は減少（0.89回）したが代わりに「頑張ります」「ご安心ください」が増加</li>
                <li><strong style="color:#fbbf24">スルーは微増</strong>（3.0→3.2件/商談）: 両期間とも費用・タイミングへの懸念を流す癖は変わらず。だめだめ期は特に「高い」「コスト」への反応が弱い</li>
                <li><strong style="color:#ef4444">根本的問題: 両期間ともレッドカードが8-10件/商談</strong>: イケイケ期から課題があったが、打席数（月38件vs16件）でカバーしていた</li>
                <li><strong style="color:#10b981">改善の鍵: 「具体例ストック」の再構築</strong>: 成功事例・数値データ・顧客名のストックを作り直し、抽象表現を撲滅すること</li>
            </ul>
        </div>
    </div>
"""

# 挿入: 個別商談一覧の直前に
marker = "イケイケ期 商談一覧"
idx = html.find(marker)
if idx > 0:
    search = html[:idx]
    last_grid = search.rfind('<div class="grid2">')
    if last_grid > 0:
        new_html = html[:last_grid] + redcard_section + "\n" + html[last_grid:]
    else:
        new_html = html.replace("</body>", redcard_section + "\n</body>")
else:
    new_html = html.replace("</body>", redcard_section + "\n</body>")

with open(f"{OUTPUT_DIR}/sato_comparison_report.html", "w", encoding="utf-8") as f:
    f.write(new_html)

print("HTMLレポート更新完了（レッドカードセクション追加）")
print(f"サイズ: {len(new_html):,} 文字")
