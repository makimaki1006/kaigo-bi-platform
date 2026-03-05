"""商談チェックポイント分析スクリプト.

受注率上位 vs 下位の商談において、各チェックポイントの出現率・出現時間帯を比較し、
勝ちパターンの時系列、必須チェックポイント、非差別化チェックポイントを特定する。
"""

from __future__ import annotations

import io
import json
import re
import statistics
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# Windows環境でのUTF-8出力を保証
if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(
        sys.stdout.buffer, encoding="utf-8", errors="replace"
    )
if sys.stderr.encoding != "utf-8":
    sys.stderr = io.TextIOWrapper(
        sys.stderr.buffer, encoding="utf-8", errors="replace"
    )

# ============================================================
# 定数定義
# ============================================================

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data" / "output"
TEAM_DIR = DATA_DIR / "team_comparison"
SATO_DIR = DATA_DIR / "sato_comparison"
OUTPUT_PATH = TEAM_DIR / "checkpoint_analysis.json"

# グループ定義
TOP_2024 = ["i_kitao", "y_tejima", "j_sato", "r_shimura", "h_matsukaze"]
BOTTOM_2024 = ["d_watanabe", "y_fukabori", "n_kiyohira", "y_haino", "r_uehata"]
TOP_2026 = ["y_fukabori", "h_matsukaze", "r_shimura", "k_kobayashi", "s_hattori"]
BOTTOM_2026 = ["j_sato", "i_kumagai", "k_sawada", "n_kiyohira", "s_shimatani"]

# チェックポイント定義
CHECKPOINTS: dict[str, dict[str, Any]] = {
    # --- ヒアリング系 ---
    "CP01_職種確認": {
        "keywords": ["職種", "介護", "看護", "保育", "リハビリ", "理学療法", "作業療法", "調理", "栄養"],
        "speaker": "host",
        "type": "question",
    },
    "CP02_人数確認": {
        "keywords": ["何名", "何人", "人数", "一名", "二名", "三名", "1名", "2名", "3名"],
        "speaker": "host",
        "type": "question",
    },
    "CP03_現状手法確認": {
        "keywords": [
            "ハローワーク", "紹介会社", "エージェント", "indeed", "インディード",
            "媒体", "ジョブメドレー", "求人ボックス", "自社ホームページ",
        ],
        "speaker": "host",
        "type": "any",
    },
    "CP04_紹介経験確認": {
        "keywords": [r"人材紹介から採用された", r"紹介から採用", r"紹介会社.*経験", r"紹介.*ご経験"],
        "speaker": "host",
        "type": "question",
    },
    "CP05_課題ヒアリング": {
        "keywords": ["困って", "課題", "悩み", "大変", "厳し", "苦労", "集まらない", "来ない", "足りない", "不足"],
        "speaker": "host",
        "type": "question",
    },
    "CP06_緊急度確認": {
        "keywords": ["いつまで", "いつ頃", "急ぎ", "期限", "何月", "来月", "今月中"],
        "speaker": "host",
        "type": "question",
    },
    "CP07_決裁確認": {
        "keywords": ["決裁", "決定権", "上長", "院長", "理事長", "社長", "承認", "決める方"],
        "speaker": "host",
        "type": "any",
    },
    "CP08_予算確認": {
        "keywords": [r"予算", r"費用.*どのくらい", r"いくら.*かけ", "採用コスト"],
        "speaker": "host",
        "type": "question",
    },
    # --- データ提示系 ---
    "CP10_エリアデータ": {
        "keywords": ["人口", "このエリア", "この地域", "市場", "圏内"],
        "speaker": "host",
        "type": "any",
    },
    "CP11_競合データ": {
        "keywords": ["競合", "他の施設", r"近隣.*施設", r"周り.*施設", "ライバル"],
        "speaker": "host",
        "type": "any",
    },
    "CP12_求人データ": {
        "keywords": ["求人倍率", "有効求人", r"応募.*件", r"検索.*回", r"閲覧.*数"],
        "speaker": "host",
        "type": "any",
    },
    "CP13_調査提示": {
        "keywords": ["調査", r"データ.*見ると", "統計", r"数字.*見て"],
        "speaker": "host",
        "type": "any",
    },
    "CP14_具体数字": {
        "keywords": [r"[0-9]+名", r"[0-9]+件", r"[0-9]+万", r"[0-9]+円", r"[0-9]+%", r"[0-9]+倍"],
        "speaker": "host",
        "type": "any",
    },
    # --- 差別化・価値提案系 ---
    "CP20_人事部リフレーム": {
        "keywords": ["人事部", "採用部門", "採用チーム"],
        "speaker": "host",
        "type": "any",
    },
    "CP21_紹介会社比較": {
        "keywords": [r"紹介会社.*比べ", r"紹介.*違い", r"成功報酬.*対し", "紹介料"],
        "speaker": "host",
        "type": "any",
    },
    "CP22_コスト比較": {
        "keywords": ["一人あたり", "一人当たり", "月額", r"年間.*換算", "コストパフォーマンス", "費用対効果"],
        "speaker": "host",
        "type": "any",
    },
    "CP23_他社事例": {
        "keywords": [r"他の法人", r"他の施設", r"同じ.*エリア.*法人", r"導入.*事例", r"お客.*では"],
        "speaker": "host",
        "type": "any",
    },
    "CP24_サービス説明": {
        "keywords": ["メディカ", r"弊社.*サービス", "ご説明", "ご案内", "ご紹介させて"],
        "speaker": "host",
        "type": "any",
    },
    # --- 危機感醸成系 ---
    "CP30_機会損失": {
        "keywords": ["このまま", "放っておく", "遅れる", "先に取られ", r"機会.*逃", "損失"],
        "speaker": "host",
        "type": "any",
    },
    "CP31_示唆質問": {
        "keywords": [r"もし.*なかったら", r"このまま.*どう", "続けると", "影響", r"負担.*増え"],
        "speaker": "host",
        "type": "any",
    },
    # --- クロージング系 ---
    "CP40_テストクロージング": {
        "keywords": ["いかがですか", "どう思われ", "前向き", "ご興味", "ご検討"],
        "speaker": "host",
        "type": "question",
    },
    "CP41_キャンペーン": {
        "keywords": ["キャンペーン", "割引", "特別", r"今月.*限り", "期間限定"],
        "speaker": "host",
        "type": "any",
    },
    "CP42_期限設定": {
        "keywords": ["今月中", "今週中", r"○日まで", "月末", "いつまでに"],
        "speaker": "host",
        "type": "any",
    },
    "CP43_次回設定": {
        "keywords": ["次回", "来週", "もう一度", "改めて", r"お時間.*いただ"],
        "speaker": "host",
        "type": "any",
    },
    "CP44_プラン提示": {
        "keywords": ["スタンダード", "ライト", "プレミアム", "プラン", r"料金.*は"],
        "speaker": "host",
        "type": "any",
    },
    "CP45_上申支援": {
        "keywords": [r"上長.*ご説明", r"資料.*お送り", r"院長.*ご提案", r"決裁.*資料"],
        "speaker": "host",
        "type": "any",
    },
    # --- ラポール系 ---
    "CP50_名前呼び": {
        "keywords": ["様"],
        "speaker": "host",
        "type": "any",
    },
    "CP51_共感フレーズ": {
        "keywords": ["おっしゃる通り", "そうですよね", r"お気持ち.*わかり", "ごもっとも"],
        "speaker": "host",
        "type": "any",
    },
    "CP52_承知フレーズ": {
        "keywords": ["承知", "かしこまり"],
        "speaker": "host",
        "type": "any",
    },
    # --- 顧客反応系 ---
    "CP60_顧客課題言語化": {
        "keywords": ["困って", "大変", "集まらない", "来ない", "足りない", "不足", "厳し"],
        "speaker": "customer",
        "type": "any",
    },
    "CP61_顧客前向き反応": {
        "keywords": ["いいですね", "面白い", "興味", "やってみたい", "良さそう", "検討したい"],
        "speaker": "customer",
        "type": "any",
    },
    "CP62_顧客料金質問": {
        "keywords": ["いくら", "費用", "料金", "予算", "金額", "値段", "コスト"],
        "speaker": "customer",
        "type": "question",
    },
    "CP63_顧客懐疑反応": {
        "keywords": ["本当", "実績", r"効果.*ある", "うまくいく", "難しい", r"ちょっと.*厳しい"],
        "speaker": "customer",
        "type": "any",
    },
    "CP64_顧客上申言及": {
        "keywords": [r"上に.*確認", r"相談.*し", r"院長.*聞い", r"理事.*話し"],
        "speaker": "customer",
        "type": "any",
    },
}


# ============================================================
# データ構造
# ============================================================


@dataclass
class Utterance:
    """パースされた1発話."""

    timestamp_sec: float
    speaker: str
    text: str


@dataclass
class CheckpointResult:
    """1つのチェックポイントの検出結果."""

    found: bool = False
    first_quarter: int | None = None  # 1-4 (Q1-Q4)
    count: int = 0


@dataclass
class TranscriptAnalysis:
    """1つのTranscriptの分析結果."""

    file_path: str = ""
    total_duration_sec: float = 0.0
    host_speaker: str = ""
    utterance_count: int = 0
    checkpoints: dict[str, CheckpointResult] = field(default_factory=dict)


# ============================================================
# パーサー
# ============================================================

# VTT行パターン: [HH:MM:SS.mmm] 話者名: テキスト
LINE_PATTERN = re.compile(
    r"^\[(\d{2}):(\d{2}):(\d{2})\.(\d{3})\]\s+(.+?):\s+(.*)$"
)


def parse_timestamp(hours: str, minutes: str, seconds: str, millis: str) -> float:
    """タイムスタンプ文字列を秒数に変換."""
    return int(hours) * 3600 + int(minutes) * 60 + int(seconds) + int(millis) / 1000


def parse_transcript(file_path: Path) -> list[Utterance]:
    """VTT形式のTranscriptをパースして発話リストを返す."""
    utterances: list[Utterance] = []
    try:
        text = file_path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        try:
            text = file_path.read_text(encoding="utf-8-sig")
        except Exception:
            return utterances
    except Exception:
        return utterances

    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        m = LINE_PATTERN.match(line)
        if m:
            ts = parse_timestamp(m.group(1), m.group(2), m.group(3), m.group(4))
            speaker = m.group(5).strip()
            utt_text = m.group(6).strip()
            utterances.append(Utterance(timestamp_sec=ts, speaker=speaker, text=utt_text))
    return utterances


def identify_host(utterances: list[Utterance]) -> str:
    """最も文字数が多い話者をホストとして識別."""
    char_counts: dict[str, int] = defaultdict(int)
    for u in utterances:
        char_counts[u.speaker] += len(u.text)
    if not char_counts:
        return ""
    return max(char_counts, key=lambda s: char_counts[s])


def get_quarter(timestamp_sec: float, total_duration_sec: float) -> int:
    """タイムスタンプが商談全体のどのクォーターに属するか(1-4)を返す."""
    if total_duration_sec <= 0:
        return 1
    ratio = timestamp_sec / total_duration_sec
    if ratio < 0.25:
        return 1
    elif ratio < 0.50:
        return 2
    elif ratio < 0.75:
        return 3
    else:
        return 4


def check_keyword_match(text: str, keywords: list[str]) -> bool:
    """テキストがキーワードリストのいずれかにマッチするか判定."""
    for kw in keywords:
        try:
            if re.search(kw, text):
                return True
        except re.error:
            # 正規表現エラー時はリテラルマッチにフォールバック
            if kw in text:
                return True
    return False


def is_question(text: str) -> bool:
    """テキストが質問形式かどうかを判定."""
    # 全角・半角の「?」、「か」で終わる、「ですか」「ますか」等を含む
    if "?" in text or "？" in text:
        return True
    if text.endswith("か") or text.endswith("か。"):
        return True
    question_patterns = [r"ですか", r"ますか", r"でしょうか", r"かね", r"ですかね"]
    for pat in question_patterns:
        if re.search(pat, text):
            return True
    return False


def analyze_transcript(file_path: Path) -> TranscriptAnalysis | None:
    """1つのTranscriptを分析し、各チェックポイントの出現を検出."""
    utterances = parse_transcript(file_path)
    if not utterances or len(utterances) < 5:
        return None

    host = identify_host(utterances)
    if not host:
        return None

    # 総時間の算出（最初と最後の発話間）
    first_ts = utterances[0].timestamp_sec
    last_ts = utterances[-1].timestamp_sec
    total_duration = last_ts - first_ts
    if total_duration <= 0:
        total_duration = 1.0  # 最低1秒

    result = TranscriptAnalysis(
        file_path=str(file_path.name),
        total_duration_sec=total_duration,
        host_speaker=host,
        utterance_count=len(utterances),
    )

    # 各チェックポイントを初期化
    for cp_name in CHECKPOINTS:
        result.checkpoints[cp_name] = CheckpointResult()

    # 各発話を走査してチェックポイントを検出
    for utt in utterances:
        is_host = utt.speaker == host
        is_customer = not is_host
        quarter = get_quarter(utt.timestamp_sec - first_ts, total_duration)

        for cp_name, cp_def in CHECKPOINTS.items():
            # 話者フィルタ
            required_speaker = cp_def["speaker"]
            if required_speaker == "host" and not is_host:
                continue
            if required_speaker == "customer" and not is_customer:
                continue

            # タイプフィルタ（questionの場合は質問形式のみ）
            if cp_def["type"] == "question" and not is_question(utt.text):
                continue

            # キーワードマッチ
            if check_keyword_match(utt.text, cp_def["keywords"]):
                cp_result = result.checkpoints[cp_name]
                cp_result.count += 1
                if not cp_result.found:
                    cp_result.found = True
                    cp_result.first_quarter = quarter

    return result


# ============================================================
# データ収集
# ============================================================


def collect_transcripts(base_dir: Path, member_keys: list[str]) -> list[TranscriptAnalysis]:
    """指定メンバーのTranscriptを収集・分析."""
    results: list[TranscriptAnalysis] = []
    for key in member_keys:
        member_dir = base_dir / key
        if not member_dir.exists():
            print(f"  警告: ディレクトリが存在しません: {member_dir}")
            continue
        txt_files = sorted(member_dir.glob("*.txt"))
        for f in txt_files:
            analysis = analyze_transcript(f)
            if analysis:
                results.append(analysis)
    return results


def collect_sato_transcripts(sato_dir: Path) -> list[TranscriptAnalysis]:
    """佐藤ディレクトリのTranscriptを収集・分析."""
    results: list[TranscriptAnalysis] = []
    if not sato_dir.exists():
        print(f"  警告: ディレクトリが存在しません: {sato_dir}")
        return results
    txt_files = sorted(sato_dir.glob("*.txt"))
    for f in txt_files:
        analysis = analyze_transcript(f)
        if analysis:
            results.append(analysis)
    return results


# ============================================================
# 集計・分析
# ============================================================


def compute_checkpoint_rates(
    analyses: list[TranscriptAnalysis],
) -> dict[str, dict[str, Any]]:
    """各チェックポイントの出現率・平均出現回数・出現クォーター分布を算出."""
    total = len(analyses)
    if total == 0:
        return {}

    stats: dict[str, dict[str, Any]] = {}
    for cp_name in CHECKPOINTS:
        found_count = 0
        total_occurrences = 0
        quarter_dist = {1: 0, 2: 0, 3: 0, 4: 0}
        first_quarters: list[int] = []

        for a in analyses:
            cp = a.checkpoints.get(cp_name)
            if cp and cp.found:
                found_count += 1
                total_occurrences += cp.count
                if cp.first_quarter is not None:
                    quarter_dist[cp.first_quarter] += 1
                    first_quarters.append(cp.first_quarter)

        rate = found_count / total * 100
        avg_count = total_occurrences / total if total > 0 else 0
        median_quarter = (
            statistics.median(first_quarters) if first_quarters else None
        )

        stats[cp_name] = {
            "出現率": round(rate, 1),
            "出現商談数": found_count,
            "全商談数": total,
            "平均出現回数": round(avg_count, 2),
            "出現クォーター分布": {f"Q{k}": v for k, v in quarter_dist.items()},
            "中央値クォーター": round(median_quarter, 1) if median_quarter else None,
            "最初の出現クォーター一覧": first_quarters,
        }
    return stats


def classify_checkpoint(top_rate: float, bottom_rate: float) -> tuple[str, str]:
    """チェックポイントを差分に基づいて分類."""
    diff = top_rate - bottom_rate
    if diff >= 15:
        return "必須（上位のみ）", "★"
    elif diff >= 5:
        return "重要", "◎"
    elif diff >= -5:
        return "非差別化（両方やってる）", "●"
    else:
        return "下位が多い", "△"


def build_timeline(stats: dict[str, dict[str, Any]]) -> list[tuple[str, float | None]]:
    """上位グループの中央値クォーターを使い、時系列順に並べる."""
    timeline: list[tuple[str, float | None]] = []
    for cp_name, s in stats.items():
        mq = s["中央値クォーター"]
        timeline.append((cp_name, mq))
    # Noneは末尾、それ以外はクォーター値でソート
    timeline.sort(key=lambda x: (x[1] is None, x[1] or 99))
    return timeline


# ============================================================
# 出力
# ============================================================


def print_section_header(title: str) -> None:
    """セクションヘッダーを出力."""
    print()
    print("=" * 80)
    print(f"  {title}")
    print("=" * 80)


def print_timeline(
    timeline: list[tuple[str, float | None]], stats: dict[str, dict[str, Any]]
) -> None:
    """勝ちパターンの時系列を表示."""
    current_q = 0.0
    for cp_name, median_q in timeline:
        if median_q is None:
            continue
        rate = stats[cp_name]["出現率"]
        if rate < 5.0:
            continue  # 出現率5%未満は表示しない
        q_label = f"Q{median_q:.1f}"
        # クォーターが変わったら区切り線
        if int(median_q) > int(current_q):
            print(f"\n  --- {q_label} (商談の{int((median_q - 1) * 25)}%-{int(median_q * 25)}%) ---")
            current_q = median_q
        avg_count = stats[cp_name]["平均出現回数"]
        print(f"    {cp_name:<30s}  出現率={rate:5.1f}%  平均回数={avg_count:.1f}  中央値Q={q_label}")


def print_comparison_table(
    top_stats: dict[str, dict[str, Any]],
    bottom_stats: dict[str, dict[str, Any]],
    label: str,
) -> None:
    """上位 vs 下位の比較テーブルを表示."""
    # カテゴリ順にソート
    rows: list[tuple[str, float, float, float, str, str]] = []
    for cp_name in CHECKPOINTS:
        top_rate = top_stats.get(cp_name, {}).get("出現率", 0.0)
        bottom_rate = bottom_stats.get(cp_name, {}).get("出現率", 0.0)
        diff = top_rate - bottom_rate
        judgment_label, judgment_symbol = classify_checkpoint(top_rate, bottom_rate)
        rows.append((cp_name, top_rate, bottom_rate, diff, judgment_symbol, judgment_label))

    # 差分の絶対値でソート（差が大きいものから）
    rows.sort(key=lambda x: -abs(x[3]))

    print(f"\n  {'CP名':<30s} | {'上位出現率':>10s} | {'下位出現率':>10s} | {'差分':>8s} | 判定")
    print(f"  {'-' * 30}-+-{'-' * 10}-+-{'-' * 10}-+-{'-' * 8}-+------")
    for cp_name, top_r, bottom_r, diff, symbol, label_text in rows:
        diff_str = f"{diff:+.1f}%"
        print(
            f"  {cp_name:<30s} | {top_r:>9.1f}% | {bottom_r:>9.1f}% | {diff_str:>8s} | {symbol} {label_text}"
        )

    # サマリー
    critical = [r for r in rows if r[4] == "★"]
    important = [r for r in rows if r[4] == "◎"]
    neutral = [r for r in rows if r[4] == "●"]
    bottom_higher = [r for r in rows if r[4] == "△"]

    print(f"\n  サマリー ({label}):")
    print(f"    ★ 必須（上位のみ）: {len(critical)}件")
    if critical:
        for r in critical:
            print(f"      - {r[0]} (差分: {r[3]:+.1f}%)")
    print(f"    ◎ 重要:            {len(important)}件")
    if important:
        for r in important:
            print(f"      - {r[0]} (差分: {r[3]:+.1f}%)")
    print(f"    ● 非差別化:        {len(neutral)}件")
    print(f"    △ 下位が多い:      {len(bottom_higher)}件")
    if bottom_higher:
        for r in bottom_higher:
            print(f"      - {r[0]} (差分: {r[3]:+.1f}%)")


# ============================================================
# メイン処理
# ============================================================


def main() -> None:
    """メインエントリーポイント."""
    print("商談チェックポイント分析を開始します...")
    print(f"チェックポイント数: {len(CHECKPOINTS)}")

    # ----------------------------------------------------------
    # 1. データ収集
    # ----------------------------------------------------------

    print("\n[1/5] 2024H1データを収集中...")
    top_2024_data = collect_transcripts(TEAM_DIR / "2024h1", TOP_2024)
    bottom_2024_data = collect_transcripts(TEAM_DIR / "2024h1", BOTTOM_2024)
    print(f"  上位: {len(top_2024_data)}件, 下位: {len(bottom_2024_data)}件")

    print("\n[2/5] 2026Janデータを収集中...")
    top_2026_data = collect_transcripts(TEAM_DIR / "2026jan", TOP_2026)
    bottom_2026_data = collect_transcripts(TEAM_DIR / "2026jan", BOTTOM_2026)
    print(f"  上位: {len(top_2026_data)}件, 下位: {len(bottom_2026_data)}件")

    print("\n[3/5] 佐藤比較データを収集中...")
    sato_ikeike = collect_sato_transcripts(SATO_DIR / "ikeike")
    sato_damedame = collect_sato_transcripts(SATO_DIR / "damedame")
    print(f"  イケイケ: {len(sato_ikeike)}件, だめだめ: {len(sato_damedame)}件")

    # ----------------------------------------------------------
    # 2. チェックポイント出現率を算出
    # ----------------------------------------------------------

    print("\n[4/5] チェックポイント出現率を算出中...")

    # 2024H1
    top_2024_stats = compute_checkpoint_rates(top_2024_data)
    bottom_2024_stats = compute_checkpoint_rates(bottom_2024_data)

    # 2026Jan
    top_2026_stats = compute_checkpoint_rates(top_2026_data)
    bottom_2026_stats = compute_checkpoint_rates(bottom_2026_data)

    # 統合: 2024+2026の上位/下位を結合
    combined_top = top_2024_data + top_2026_data
    combined_bottom = bottom_2024_data + bottom_2026_data
    combined_top_stats = compute_checkpoint_rates(combined_top)
    combined_bottom_stats = compute_checkpoint_rates(combined_bottom)

    # 佐藤
    sato_ike_stats = compute_checkpoint_rates(sato_ikeike)
    sato_dame_stats = compute_checkpoint_rates(sato_damedame)

    # ----------------------------------------------------------
    # 3. コンソール出力
    # ----------------------------------------------------------

    print("\n[5/5] 結果を出力中...")

    # ===== 勝ちパターンの時系列 =====
    print_section_header("勝ちパターンの時系列（統合上位グループ）")
    timeline = build_timeline(combined_top_stats)
    print_timeline(timeline, combined_top_stats)

    # ===== 2024H1 比較 =====
    print_section_header("2024H1: 受注率上位 vs 下位")
    print(f"  上位メンバー: {', '.join(TOP_2024)} ({len(top_2024_data)}商談)")
    print(f"  下位メンバー: {', '.join(BOTTOM_2024)} ({len(bottom_2024_data)}商談)")
    print_comparison_table(top_2024_stats, bottom_2024_stats, "2024H1")

    # ===== 2026Jan 比較 =====
    print_section_header("2026Jan: 受注率上位 vs 下位")
    print(f"  上位メンバー: {', '.join(TOP_2026)} ({len(top_2026_data)}商談)")
    print(f"  下位メンバー: {', '.join(BOTTOM_2026)} ({len(bottom_2026_data)}商談)")
    print_comparison_table(top_2026_stats, bottom_2026_stats, "2026Jan")

    # ===== 統合比較 =====
    print_section_header("統合（2024H1 + 2026Jan）: 受注率上位 vs 下位")
    print(f"  上位: {len(combined_top)}商談, 下位: {len(combined_bottom)}商談")
    print_comparison_table(combined_top_stats, combined_bottom_stats, "統合")

    # ===== 佐藤比較 =====
    print_section_header("佐藤: イケイケ vs だめだめ")
    print(f"  イケイケ: {len(sato_ikeike)}商談, だめだめ: {len(sato_damedame)}商談")
    print_comparison_table(sato_ike_stats, sato_dame_stats, "佐藤比較")

    # ===== 時系列詳細（佐藤イケイケ） =====
    print_section_header("佐藤イケイケの時系列パターン")
    sato_timeline = build_timeline(sato_ike_stats)
    print_timeline(sato_timeline, sato_ike_stats)

    # ----------------------------------------------------------
    # 4. JSON出力
    # ----------------------------------------------------------

    # JSON用にデータを整形（first_quarters一覧は省略して軽量化）
    def stats_for_json(stats: dict[str, dict[str, Any]]) -> dict[str, Any]:
        result = {}
        for cp_name, s in stats.items():
            result[cp_name] = {
                "出現率": s["出現率"],
                "出現商談数": s["出現商談数"],
                "全商談数": s["全商談数"],
                "平均出現回数": s["平均出現回数"],
                "出現クォーター分布": s["出現クォーター分布"],
                "中央値クォーター": s["中央値クォーター"],
            }
        return result

    def comparison_for_json(
        top_s: dict[str, dict[str, Any]], bottom_s: dict[str, dict[str, Any]]
    ) -> dict[str, Any]:
        result = {}
        for cp_name in CHECKPOINTS:
            top_rate = top_s.get(cp_name, {}).get("出現率", 0.0)
            bottom_rate = bottom_s.get(cp_name, {}).get("出現率", 0.0)
            diff = top_rate - bottom_rate
            judgment_label, judgment_symbol = classify_checkpoint(top_rate, bottom_rate)
            result[cp_name] = {
                "上位出現率": top_rate,
                "下位出現率": bottom_rate,
                "差分": round(diff, 1),
                "判定": judgment_label,
                "判定記号": judgment_symbol,
            }
        return result

    output_data = {
        "分析概要": {
            "チェックポイント数": len(CHECKPOINTS),
            "データセット": {
                "2024H1_上位": {
                    "メンバー": TOP_2024,
                    "商談数": len(top_2024_data),
                },
                "2024H1_下位": {
                    "メンバー": BOTTOM_2024,
                    "商談数": len(bottom_2024_data),
                },
                "2026Jan_上位": {
                    "メンバー": TOP_2026,
                    "商談数": len(top_2026_data),
                },
                "2026Jan_下位": {
                    "メンバー": BOTTOM_2026,
                    "商談数": len(bottom_2026_data),
                },
                "統合_上位": {"商談数": len(combined_top)},
                "統合_下位": {"商談数": len(combined_bottom)},
                "佐藤_イケイケ": {"商談数": len(sato_ikeike)},
                "佐藤_だめだめ": {"商談数": len(sato_damedame)},
            },
        },
        "チェックポイント出現率": {
            "2024H1_上位": stats_for_json(top_2024_stats),
            "2024H1_下位": stats_for_json(bottom_2024_stats),
            "2026Jan_上位": stats_for_json(top_2026_stats),
            "2026Jan_下位": stats_for_json(bottom_2026_stats),
            "統合_上位": stats_for_json(combined_top_stats),
            "統合_下位": stats_for_json(combined_bottom_stats),
            "佐藤_イケイケ": stats_for_json(sato_ike_stats),
            "佐藤_だめだめ": stats_for_json(sato_dame_stats),
        },
        "比較分析": {
            "2024H1": comparison_for_json(top_2024_stats, bottom_2024_stats),
            "2026Jan": comparison_for_json(top_2026_stats, bottom_2026_stats),
            "統合": comparison_for_json(combined_top_stats, combined_bottom_stats),
            "佐藤": comparison_for_json(sato_ike_stats, sato_dame_stats),
        },
        "勝ちパターン時系列": {
            "統合上位": [
                {
                    "CP": cp,
                    "中央値クォーター": mq,
                    "出現率": combined_top_stats.get(cp, {}).get("出現率", 0),
                }
                for cp, mq in timeline
                if mq is not None
                and combined_top_stats.get(cp, {}).get("出現率", 0) >= 5
            ],
            "佐藤イケイケ": [
                {
                    "CP": cp,
                    "中央値クォーター": mq,
                    "出現率": sato_ike_stats.get(cp, {}).get("出現率", 0),
                }
                for cp, mq in sato_timeline
                if mq is not None
                and sato_ike_stats.get(cp, {}).get("出現率", 0) >= 5
            ],
        },
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)

    print(f"\n結果をJSON出力しました: {OUTPUT_PATH}")
    print("\n分析完了。")


if __name__ == "__main__":
    main()
