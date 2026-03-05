# -*- coding: utf-8 -*-
"""
佐藤丈太郎 商談比較分析（イケイケ期 vs だめだめ期）
Zoom Transcript ダウンロード + Gemini API 分析 + HTML レポート

Phase 1: Transcript ダウンロード（両期間）
Phase 2: Gemini 商談品質分析
Phase 3: 比較レポート生成
"""

import io
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import pandas as pd
import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
load_dotenv(PROJECT_ROOT / "config" / ".env")

# === 設定 ===
ZOOM_ACCOUNT_ID = os.environ["ZOOM_ACCOUNT_ID"]
ZOOM_CLIENT_ID = os.environ["ZOOM_CLIENT_ID"]
ZOOM_CLIENT_SECRET = os.environ["ZOOM_CLIENT_SECRET"]
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
ZOOM_API_BASE = "https://api.zoom.us/v2"

SATO_EMAIL = "j_sato@cyxen.co.jp"

OUTPUT_DIR = PROJECT_ROOT / "data" / "output" / "sato_comparison"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# イケイケ期: 2024-06, 2024-07（Transcript有の月のみ）
# だめだめ期: 2026-01 〜 2026-02-10
PERIODS = {
    "ikeike": {
        "label": "イケイケ期（2024年6-7月）",
        "ranges": [("2024-06-01", "2024-06-30"), ("2024-07-01", "2024-07-31")],
    },
    "damedame": {
        "label": "だめだめ期（2026年1-2月）",
        "ranges": [("2026-01-01", "2026-02-10")],
    },
}

# 除外キーワード
EXCLUDE_KEYWORDS = [
    "ロープレ", "roleplay", "RP", "role play",
    "研修", "MTG", "定例", "朝会", "勉強会",
    "VS", "vs", "パーソナルミーティング",
    "YMCX", "Young Man", "新人", "組み手",
    "1on1", "振り返り", "共有会", "報告会",
    "Zoom ミーティング", "New Zoom Meeting",
    "Personal Meeting Room",
]

# 商談分析プロンプト（既存と同一）
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

# 出力フォーマット（JSON形式で出力してください）

以下のJSON形式で出力してください。説明文やMarkdown装飾は不要です。純粋なJSONのみを出力してください。

```json
{{
  "total_score": 数値(0-100),
  "rank": "S/A/B/C",
  "data_shock": {{"score": 数値(0-20), "evaluation": "○/△/×", "comment": "寸評"}},
  "structure_reveal": {{"score": 数値(0-20), "evaluation": "○/△/×", "comment": "寸評"}},
  "reframing": {{"score": 数値(0-20), "evaluation": "○/△/×", "comment": "寸評"}},
  "data_logic": {{"score": 数値(0-20), "evaluation": "○/△/×", "comment": "寸評"}},
  "closing": {{"score": 数値(0-20), "evaluation": "○/△/×", "comment": "寸評"}},
  "red_cards": {{
    "abstract_escape": [{{"quote": "営業の発言引用", "feedback": "指摘"}}],
    "skip_concern": [{{"customer_quote": "顧客の発言", "sales_reaction": "営業の反応", "feedback": "指摘"}}]
  }},
  "prescription": {{"action": "改善アクション", "killer_phrase": "キラーフレーズ"}},
  "summary": "50文字以内の一言総評"
}}
```

---
【商談ログ】
{transcript}
"""


# === Zoom API ===

def get_zoom_token():
    resp = requests.post(
        "https://zoom.us/oauth/token",
        params={"grant_type": "account_credentials", "account_id": ZOOM_ACCOUNT_ID},
        auth=(ZOOM_CLIENT_ID, ZOOM_CLIENT_SECRET),
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def get_recordings(token, email, from_date, to_date):
    headers = {"Authorization": f"Bearer {token}"}
    all_meetings = []
    next_page = ""
    while True:
        params = {"from": from_date, "to": to_date, "page_size": 300}
        if next_page:
            params["next_page_token"] = next_page
        resp = requests.get(
            f"{ZOOM_API_BASE}/users/{email}/recordings",
            headers=headers, params=params, timeout=60,
        )
        if resp.status_code == 429:
            time.sleep(10)
            continue
        resp.raise_for_status()
        data = resp.json()
        all_meetings.extend(data.get("meetings", []))
        next_page = data.get("next_page_token", "")
        if not next_page:
            break
    return all_meetings


def download_transcript(token, download_url):
    resp = requests.get(download_url, headers={"Authorization": f"Bearer {token}"}, timeout=120)
    return resp.text if resp.status_code == 200 else None


def vtt_to_text(vtt):
    lines = vtt.strip().split("\n")
    result = []
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if "-->" in line:
            timestamp = line.split("-->")[0].strip()
            text_lines = []
            i += 1
            while i < len(lines) and lines[i].strip() and "-->" not in lines[i]:
                text_lines.append(lines[i].strip())
                i += 1
            if text_lines:
                result.append(f"[{timestamp}] {' '.join(text_lines)}")
            continue
        i += 1
    return "\n".join(result)


def is_excluded(topic):
    for kw in EXCLUDE_KEYWORDS:
        if kw.lower() in topic.lower():
            return True
    return False


def sanitize(name):
    return re.sub(r'[<>:"/\\|?*]', '_', name)[:80]


# === Gemini API ===

def call_gemini(prompt, retries=3):
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={GEMINI_API_KEY}"
    for attempt in range(retries):
        try:
            resp = requests.post(url, json={
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"maxOutputTokens": 4096, "temperature": 0.2}
            }, timeout=180)
            data = resp.json()
            if "candidates" in data:
                return data["candidates"][0]["content"]["parts"][0]["text"]
            print(f"    API応答異常 (attempt {attempt+1}): {str(data)[:200]}")
            time.sleep(5 * (attempt + 1))
        except Exception as e:
            print(f"    APIエラー (attempt {attempt+1}): {e}")
            time.sleep(5 * (attempt + 1))
    return None


def parse_gemini_json(text):
    """Geminiの応答からJSONを抽出"""
    if not text:
        return None
    # ```json ... ``` ブロックを抽出
    m = re.search(r'```json\s*(.*?)\s*```', text, re.DOTALL)
    json_str = m.group(1) if m else text
    # JSONとして解析を試行
    try:
        return json.loads(json_str)
    except json.JSONDecodeError:
        # もう少し柔軟に: { から始まる部分を抽出
        m2 = re.search(r'\{.*\}', json_str, re.DOTALL)
        if m2:
            try:
                return json.loads(m2.group(0))
            except json.JSONDecodeError:
                pass
    return None


# === Phase 1: Transcript ダウンロード ===

def download_all_transcripts():
    print("=" * 60)
    print("Phase 1: Zoom Transcript ダウンロード")
    print("=" * 60)

    token = get_zoom_token()
    print("Zoom認証成功\n")

    all_items = []

    for period_key, period_info in PERIODS.items():
        label = period_info["label"]
        period_dir = OUTPUT_DIR / period_key
        period_dir.mkdir(parents=True, exist_ok=True)

        print(f"--- {label} ---")
        meetings = []
        for from_d, to_d in period_info["ranges"]:
            m = get_recordings(token, SATO_EMAIL, from_d, to_d)
            meetings.extend(m)
            print(f"  {from_d}~{to_d}: {len(m)}件")

        downloaded = 0
        skipped = 0
        for meeting in meetings:
            topic = meeting.get("topic", "不明")
            start = meeting.get("start_time", "")
            duration = meeting.get("duration", 0)

            if is_excluded(topic):
                skipped += 1
                continue

            # Transcript を探す
            files = meeting.get("recording_files", [])
            transcript_file = None
            for f in files:
                if f.get("file_type") == "TRANSCRIPT" and f.get("status") == "completed":
                    transcript_file = f
                    break
                # audio_transcript タイプもチェック
                if f.get("recording_type") == "audio_transcript" and f.get("status") == "completed":
                    transcript_file = f
                    break

            if not transcript_file:
                skipped += 1
                continue

            dl_url = transcript_file.get("download_url", "")
            if not dl_url:
                skipped += 1
                continue

            vtt = download_transcript(token, dl_url)
            if not vtt:
                skipped += 1
                continue

            text = vtt_to_text(vtt)
            lines = [l for l in text.strip().split('\n') if l.strip()]
            if len(lines) < 30:
                skipped += 1
                continue

            date_str = start[:10] if start else "unknown"
            time_str = start[11:16].replace(":", "") if len(start) > 15 else ""
            safe_topic = sanitize(topic)
            fname = f"{date_str}_{time_str}_{safe_topic}"

            txt_path = period_dir / f"{fname}.txt"
            txt_path.write_text(text, encoding="utf-8")

            item = {
                "period": period_key,
                "period_label": label,
                "topic": topic,
                "date": date_str,
                "time": start[11:16] if len(start) > 15 else "",
                "duration": duration,
                "file": str(txt_path),
                "filename": f"{fname}.txt",
                "line_count": len(lines),
            }
            all_items.append(item)
            downloaded += 1

            print(f"  ✓ {date_str} {duration:3d}分 {topic[:45]}")
            time.sleep(0.3)

        print(f"  → ダウンロード: {downloaded}件, スキップ: {skipped}件\n")

    # 一覧CSV保存
    if all_items:
        pd.DataFrame(all_items).to_csv(OUTPUT_DIR / "transcripts_list.csv", index=False, encoding="utf-8-sig")
    print(f"合計: {len(all_items)}件のTranscript\n")
    return all_items


# === Phase 2: Gemini 分析 ===

def analyze_transcript(item):
    """1件のTranscriptを分析"""
    fpath = item["file"]
    try:
        with open(fpath, "r", encoding="utf-8") as f:
            text = f.read()
    except Exception as e:
        return {**item, "result": None, "parsed": None, "error": str(e)}

    # 長すぎる場合はカット
    if len(text) > 60000:
        text = text[:60000] + "\n\n[...以降省略...]"

    prompt = ANALYSIS_PROMPT.format(transcript=text)
    raw = call_gemini(prompt)
    parsed = parse_gemini_json(raw) if raw else None

    return {**item, "result": raw, "parsed": parsed, "error": None}


def run_analysis(items):
    print("=" * 60)
    print(f"Phase 2: Gemini 商談品質分析 ({len(items)}件)")
    print("=" * 60)

    results = []
    done = 0
    # 並列5（Gemini APIレート制限考慮）
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(analyze_transcript, item): item for item in items}
        for future in as_completed(futures):
            r = future.result()
            done += 1
            if r.get("parsed"):
                score = r["parsed"].get("total_score", "?")
                rank = r["parsed"].get("rank", "?")
                status = f"score={score}, rank={rank}"
            elif r.get("result"):
                status = "JSON解析失敗（テキストあり）"
            else:
                status = f"エラー: {r.get('error', 'API応答なし')}"
            results.append(r)
            p = "🔵" if r["period"] == "ikeike" else "🔴"
            print(f"  [{done}/{len(items)}] {p} {r['date']} {r['topic'][:35]}... {status}", flush=True)

    # 結果保存
    results_path = OUTPUT_DIR / "analysis_results.json"
    serializable = []
    for r in results:
        s = {k: v for k, v in r.items() if k != "result"}
        s["has_result"] = r.get("result") is not None
        serializable.append(s)
    with open(results_path, "w", encoding="utf-8") as f:
        json.dump(serializable, f, ensure_ascii=False, indent=2)

    print(f"\n分析完了: {len([r for r in results if r.get('parsed')])}件成功\n")
    return results


# === Phase 3: 比較レポート ===

def generate_comparison_report(results):
    print("=" * 60)
    print("Phase 3: 比較レポート生成")
    print("=" * 60)

    # 期間別に分割
    ikeike = [r for r in results if r["period"] == "ikeike" and r.get("parsed")]
    damedame = [r for r in results if r["period"] == "damedame" and r.get("parsed")]

    print(f"  イケイケ期分析済み: {len(ikeike)}件")
    print(f"  だめだめ期分析済み: {len(damedame)}件")

    def period_stats(items):
        scores = [r["parsed"]["total_score"] for r in items if r["parsed"].get("total_score")]
        axes_keys = ["data_shock", "structure_reveal", "reframing", "data_logic", "closing"]
        axes_avg = {}
        for key in axes_keys:
            vals = [r["parsed"].get(key, {}).get("score", 0) for r in items if r.get("parsed")]
            axes_avg[key] = sum(vals) / len(vals) if vals else 0

        ranks = {}
        for r in items:
            rk = r["parsed"].get("rank", "?")
            ranks[rk] = ranks.get(rk, 0) + 1

        return {
            "count": len(items),
            "avg_score": sum(scores) / len(scores) if scores else 0,
            "median_score": sorted(scores)[len(scores)//2] if scores else 0,
            "max_score": max(scores) if scores else 0,
            "min_score": min(scores) if scores else 0,
            "axes_avg": axes_avg,
            "ranks": ranks,
            "scores": scores,
        }

    s1 = period_stats(ikeike)
    s2 = period_stats(damedame)

    # コンソール表示
    print(f"\n{'指標':20s} {'イケイケ期':>12s} {'だめだめ期':>12s} {'差分':>10s}")
    print("-" * 58)
    diff = s1["avg_score"] - s2["avg_score"]
    print(f"{'平均スコア':20s} {s1['avg_score']:>10.1f}点 {s2['avg_score']:>10.1f}点 {diff:>+8.1f}pt")
    print(f"{'中央値':20s} {s1['median_score']:>10.1f}点 {s2['median_score']:>10.1f}点")
    print(f"{'最高':20s} {s1['max_score']:>10}点 {s2['max_score']:>10}点")
    print(f"{'最低':20s} {s1['min_score']:>10}点 {s2['min_score']:>10}点")
    axes_labels = {
        "data_shock": "データ殴打",
        "structure_reveal": "構造暴露",
        "reframing": "定義転換",
        "data_logic": "戦略提案",
        "closing": "クロージング",
    }
    for key, label in axes_labels.items():
        v1 = s1["axes_avg"].get(key, 0)
        v2 = s2["axes_avg"].get(key, 0)
        d = v1 - v2
        print(f"{label:20s} {v1:>10.1f}/20 {v2:>10.1f}/20 {d:>+8.1f}")

    # HTML生成
    html = generate_html_report(ikeike, damedame, s1, s2)
    html_path = OUTPUT_DIR / "sato_comparison_report.html"
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"\nHTML: {html_path}")

    # Excel生成
    generate_excel(results, s1, s2)

    return s1, s2


def generate_html_report(ikeike, damedame, s1, s2):
    """比較HTML生成"""
    axes_labels = ['データ殴打', '構造暴露', '定義転換', '戦略提案', 'クロージング']
    axes_keys = ['data_shock', 'structure_reveal', 'reframing', 'data_logic', 'closing']

    # レーダーチャート用データ
    radar1 = [round(s1['axes_avg'].get(k, 0), 1) for k in axes_keys]
    radar2 = [round(s2['axes_avg'].get(k, 0), 1) for k in axes_keys]

    # ランク分布
    rank_labels = ['S', 'A', 'B', 'C']
    ranks1 = [s1['ranks'].get(r, 0) for r in rank_labels]
    ranks2 = [s2['ranks'].get(r, 0) for r in rank_labels]

    # 個別商談カード
    def deal_cards(items, period_color):
        cards = ""
        for r in sorted(items, key=lambda x: x['parsed'].get('total_score', 0), reverse=True):
            p = r['parsed']
            score = p.get('total_score', 0)
            rank = p.get('rank', '?')
            if score >= 70: sc = '#10b981'
            elif score >= 50: sc = '#3b82f6'
            elif score >= 35: sc = '#f59e0b'
            else: sc = '#ef4444'

            # 各軸のミニバー
            axes_bars = ""
            for key, label in zip(axes_keys, axes_labels):
                val = p.get(key, {}).get('score', 0)
                width = val / 20 * 100
                comment = p.get(key, {}).get('comment', '')[:60]
                axes_bars += f'<div style="margin:2px 0;display:flex;align-items:center;gap:6px"><span style="width:70px;font-size:11px;color:#94a3b8">{label}</span><div style="flex:1;background:#1e293b;height:14px;border-radius:3px;overflow:hidden"><div style="width:{width}%;height:100%;background:{sc};border-radius:3px"></div></div><span style="font-size:11px;color:#e2e8f0;width:30px;text-align:right">{val}</span></div>'

            # レッドカード
            red_count = len(p.get('red_cards', {}).get('abstract_escape', [])) + len(p.get('red_cards', {}).get('skip_concern', []))
            red_badge = f'<span style="background:#ef4444;color:white;padding:1px 6px;border-radius:3px;font-size:10px;margin-left:6px">🚩{red_count}</span>' if red_count > 0 else ''

            summary = p.get('summary', '')

            cards += f'''<div style="background:#0f172a;border-radius:8px;padding:14px;margin-bottom:10px;border-left:3px solid {sc}">
                <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px">
                    <div><span style="color:#94a3b8;font-size:11px">{r['date']}</span>
                    <span style="color:#e2e8f0;font-size:13px;font-weight:600;margin-left:6px">{r['topic'][:40]}</span>{red_badge}</div>
                    <div><span style="font-size:22px;font-weight:700;color:{sc}">{score}</span><span style="color:#64748b;font-size:11px">/100</span>
                    <span style="background:{sc};color:white;padding:1px 6px;border-radius:3px;font-size:11px;margin-left:4px">{rank}</span></div>
                </div>
                <div style="font-size:12px;color:#94a3b8;margin-bottom:6px">{summary}</div>
                {axes_bars}
            </div>'''
        return cards

    ikeike_cards = deal_cards(ikeike, '#3b82f6')
    damedame_cards = deal_cards(damedame, '#ef4444')

    # 差分計算
    diff_score = s1['avg_score'] - s2['avg_score']
    diff_sign = '+' if diff_score >= 0 else ''
    diff_color = '#10b981' if diff_score > 0 else '#ef4444'

    # 軸別比較テーブル
    axes_rows = ""
    for key, label in zip(axes_keys, axes_labels):
        v1 = s1['axes_avg'].get(key, 0)
        v2 = s2['axes_avg'].get(key, 0)
        d = v1 - v2
        dc = '#10b981' if d > 0 else '#ef4444' if d < 0 else '#94a3b8'
        ds = '+' if d > 0 else ''
        axes_rows += f'<tr><td>{label}</td><td style="text-align:center;font-weight:600;color:#60a5fa">{v1:.1f}</td><td style="text-align:center;font-weight:600;color:#f87171">{v2:.1f}</td><td style="text-align:center;font-weight:600;color:{dc}">{ds}{d:.1f}</td></tr>'

    html = f'''<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>佐藤丈太郎 商談比較分析</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{background:#0f172a;color:#e2e8f0;font-family:'Segoe UI','Hiragino Sans',sans-serif}}
.container{{max-width:1400px;margin:0 auto;padding:24px}}
h1{{text-align:center;font-size:24px;margin-bottom:4px}}
.subtitle{{text-align:center;color:#64748b;margin-bottom:24px;font-size:13px}}
.kpi-grid{{display:grid;grid-template-columns:repeat(5,1fr);gap:12px;margin-bottom:24px}}
.kpi{{background:#1e293b;border-radius:10px;padding:14px;text-align:center}}
.kpi .label{{color:#94a3b8;font-size:10px;margin-bottom:4px}}
.kpi .value{{font-size:28px;font-weight:700}}
.kpi .sub{{color:#64748b;font-size:10px;margin-top:2px}}
.section{{background:#1e293b;border-radius:10px;padding:20px;margin-bottom:20px}}
.section h2{{font-size:16px;margin-bottom:12px;color:#f8fafc}}
.grid2{{display:grid;grid-template-columns:1fr 1fr;gap:20px}}
table{{width:100%;border-collapse:collapse}}
th,td{{padding:8px;border-bottom:1px solid #334155;font-size:13px}}
th{{background:#0f172a;color:#94a3b8;font-weight:600}}
@media(max-width:900px){{.kpi-grid{{grid-template-columns:repeat(3,1fr)}}.grid2{{grid-template-columns:1fr}}}}
</style>
</head>
<body>
<div class="container">
    <h1>佐藤丈太郎 商談品質 比較分析</h1>
    <div class="subtitle">イケイケ期（2024年6-7月）vs だめだめ期（2026年1-2月）| Zoom Transcript × Gemini AI 分析</div>

    <div class="kpi-grid">
        <div class="kpi"><div class="label">🔵 イケイケ期 平均</div><div class="value" style="color:#60a5fa">{s1['avg_score']:.0f}</div><div class="sub">{s1['count']}件 / 100点</div></div>
        <div class="kpi"><div class="label">🔴 だめだめ期 平均</div><div class="value" style="color:#f87171">{s2['avg_score']:.0f}</div><div class="sub">{s2['count']}件 / 100点</div></div>
        <div class="kpi"><div class="label">差分</div><div class="value" style="color:{diff_color}">{diff_sign}{diff_score:.0f}</div><div class="sub">ポイント</div></div>
        <div class="kpi"><div class="label">🔵 最高/最低</div><div class="value" style="color:#60a5fa;font-size:20px">{s1['max_score']}/{s1['min_score']}</div></div>
        <div class="kpi"><div class="label">🔴 最高/最低</div><div class="value" style="color:#f87171;font-size:20px">{s2['max_score']}/{s2['min_score']}</div></div>
    </div>

    <div class="grid2">
        <div class="section">
            <h2>5軸レーダー比較</h2>
            <canvas id="radar" height="300"></canvas>
        </div>
        <div class="section">
            <h2>ランク分布比較</h2>
            <canvas id="rankBar" height="200"></canvas>
            <div style="margin-top:16px">
                <h2 style="font-size:14px;margin-bottom:8px">軸別スコア比較</h2>
                <table>
                    <thead><tr><th style="text-align:left">軸</th><th>🔵 イケイケ</th><th>🔴 だめだめ</th><th>差分</th></tr></thead>
                    <tbody>{axes_rows}</tbody>
                </table>
            </div>
        </div>
    </div>

    <div class="grid2">
        <div class="section">
            <h2>🔵 イケイケ期 商談一覧（{s1['count']}件）</h2>
            {ikeike_cards}
        </div>
        <div class="section">
            <h2>🔴 だめだめ期 商談一覧（{s2['count']}件）</h2>
            {damedame_cards}
        </div>
    </div>
</div>

<script>
new Chart(document.getElementById('radar'),{{
    type:'radar',
    data:{{
        labels:{json.dumps(axes_labels, ensure_ascii=False)},
        datasets:[
            {{label:'イケイケ期',data:{json.dumps(radar1)},borderColor:'#3b82f6',backgroundColor:'rgba(59,130,246,0.15)',borderWidth:2,pointRadius:4}},
            {{label:'だめだめ期',data:{json.dumps(radar2)},borderColor:'#ef4444',backgroundColor:'rgba(239,68,68,0.15)',borderWidth:2,pointRadius:4}}
        ]
    }},
    options:{{responsive:true,scales:{{r:{{min:0,max:20,ticks:{{stepSize:5,color:'#64748b'}},grid:{{color:'#334155'}},pointLabels:{{color:'#e2e8f0',font:{{size:13}}}}}}}},plugins:{{legend:{{position:'bottom',labels:{{color:'#94a3b8',usePointStyle:true}}}}}}}}
}});

new Chart(document.getElementById('rankBar'),{{
    type:'bar',
    data:{{
        labels:['S:即戦力','A:合格','B:要指導','C:再教育'],
        datasets:[
            {{label:'イケイケ期',data:{json.dumps(ranks1)},backgroundColor:'rgba(59,130,246,0.6)',borderRadius:4}},
            {{label:'だめだめ期',data:{json.dumps(ranks2)},backgroundColor:'rgba(239,68,68,0.6)',borderRadius:4}}
        ]
    }},
    options:{{responsive:true,scales:{{y:{{grid:{{color:'#1e293b'}},ticks:{{color:'#94a3b8'}}}},x:{{grid:{{display:false}},ticks:{{color:'#e2e8f0'}}}}}},plugins:{{legend:{{position:'bottom',labels:{{color:'#94a3b8'}}}}}}}}
}});
</script>
</body>
</html>'''
    return html


def generate_excel(results, s1, s2):
    """Excel出力"""
    rows = []
    for r in sorted(results, key=lambda x: (x['period'], x.get('date', ''))):
        if not r.get('parsed'):
            continue
        p = r['parsed']
        rows.append({
            '期間': r['period_label'],
            '日付': r.get('date', ''),
            '顧客': r['topic'],
            '時間(分)': r.get('duration', ''),
            '総合スコア': p.get('total_score', ''),
            'ランク': p.get('rank', ''),
            'データ殴打': p.get('data_shock', {}).get('score', ''),
            '構造暴露': p.get('structure_reveal', {}).get('score', ''),
            '定義転換': p.get('reframing', {}).get('score', ''),
            '戦略提案': p.get('data_logic', {}).get('score', ''),
            'クロージング': p.get('closing', {}).get('score', ''),
            '総評': p.get('summary', ''),
        })

    df = pd.DataFrame(rows)
    excel_path = OUTPUT_DIR / "sato_comparison_analysis.xlsx"
    with pd.ExcelWriter(str(excel_path), engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='全商談', index=False)

        # サマリーシート
        axes_keys = ['data_shock', 'structure_reveal', 'reframing', 'data_logic', 'closing']
        axes_labels = ['データ殴打', '構造暴露', '定義転換', '戦略提案', 'クロージング']
        summary = []
        for label_p, s in [('イケイケ期', s1), ('だめだめ期', s2)]:
            row = {'期間': label_p, '商談数': s['count'], '平均スコア': round(s['avg_score'], 1)}
            for k, l in zip(axes_keys, axes_labels):
                row[l] = round(s['axes_avg'].get(k, 0), 1)
            summary.append(row)
        pd.DataFrame(summary).to_excel(writer, sheet_name='サマリー', index=False)

    print(f"Excel: {excel_path}")


# === メイン ===

def main():
    print("=" * 60)
    print("佐藤丈太郎 商談比較分析")
    print("イケイケ期(2024年6-7月) vs だめだめ期(2026年1-2月)")
    print("=" * 60)
    print(f"開始: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # Phase 1: Transcript ダウンロード
    items = download_all_transcripts()
    if not items:
        print("Transcriptが取得できませんでした")
        return

    # Phase 2: Gemini分析
    results = run_analysis(items)

    # Phase 3: 比較レポート
    generate_comparison_report(results)

    print(f"\n{'='*60}")
    print(f"全完了: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"出力先: {OUTPUT_DIR}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
