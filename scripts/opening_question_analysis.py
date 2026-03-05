"""商談冒頭3分の質問パターン分析スクリプト.

受注率上位 vs 下位メンバーの冒頭3分における質問内容を比較し、
カテゴリ構成比、質問順序パターン、質問文字数等を分析する。
"""

from __future__ import annotations

import io
import json
import re
import sys
from collections import Counter, defaultdict
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
OUTPUT_PATH = TEAM_DIR / "opening_question_analysis.json"

# 冒頭3分の閾値（秒）
OPENING_SECONDS = 180

# 質問カテゴリ定義（キーワードベース）
QUESTION_CATEGORIES: dict[str, list[str]] = {
    "採用状況": [
        "採用", "募集", "求人", "人材", "スタッフ", "職員",
        "応募", "人手", "人員", "充足", "欠員",
    ],
    "課題・困りごと": [
        "困", "課題", "悩", "問題", "大変", "難し", "苦労", "つらい", "厳し",
    ],
    "現状の手法": [
        "今", "現在", "現状", "どのよう", "どういう", "どんな",
        "使って", "利用", "媒体", "紹介会社", "エージェント",
        "ハローワーク", "indeed", "インディード",
    ],
    "規模・体制": [
        "何名", "何人", "人数", "施設", "事業所", "拠点",
        "ベッド", "定員", "規模",
    ],
    "予算・コスト": [
        "予算", "費用", "コスト", "金額", "いくら", "価格", "料金",
    ],
    "時期・緊急度": [
        "いつ", "時期", "急ぎ", "すぐ", "今すぐ",
        "いつまで", "期限", "来月", "今月",
    ],
    "決裁・意思決定": [
        "決裁", "上長", "上司", "理事", "院長", "社長",
        "決定", "承認", "相談",
    ],
    "過去の経験": [
        "以前", "前に", "過去", "経験", "やったこと", "試した",
    ],
    "確認・導入": [
        "ご存知", "聞いたこと", "知って", "見た", "ご覧",
    ],
    "アイスブレイク": [
        "お元気", "天気", "暑い", "寒い", "お忙し", "ありがとう", "よろしく",
    ],
}

# メンバーグループ定義（受注率ベース）
TOP_2024 = ["i_kitao", "y_tejima", "j_sato", "r_shimura", "h_matsukaze"]
BOTTOM_2024 = ["d_watanabe", "y_fukabori", "n_kiyohira", "y_haino", "r_uehata"]
TOP_2026 = ["y_fukabori", "h_matsukaze", "r_shimura", "k_kobayashi", "s_hattori"]
BOTTOM_2026 = ["j_sato", "i_kumagai", "k_sawada", "n_kiyohira", "s_shimatani"]

# タイムスタンプ解析用正規表現
LINE_PATTERN = re.compile(
    r"^\[(\d{2}):(\d{2}):(\d{2})\.(\d{3})\]\s+(.+?):\s+(.+)$"
)


# ============================================================
# データ構造
# ============================================================

@dataclass
class Utterance:
    """Transcript内の1発話を表す."""

    timestamp_sec: float
    speaker: str
    text: str


@dataclass
class Question:
    """冒頭3分内のホスト質問."""

    timestamp_sec: float
    text: str
    category: str
    source_file: str
    member_key: str


@dataclass
class MemberQuestions:
    """メンバー単位の質問集約."""

    member_key: str
    questions: list[Question] = field(default_factory=list)
    file_count: int = 0


# ============================================================
# 解析関数
# ============================================================

def parse_timestamp(hours: str, minutes: str, seconds: str, millis: str) -> float:
    """タイムスタンプ文字列を秒数に変換する."""
    return int(hours) * 3600 + int(minutes) * 60 + int(seconds) + int(millis) / 1000


def parse_transcript(file_path: Path) -> list[Utterance]:
    """Transcriptファイルをパースして発話リストを返す."""
    utterances: list[Utterance] = []
    try:
        text = file_path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        # utf-8-sigでリトライ
        try:
            text = file_path.read_text(encoding="utf-8-sig")
        except Exception:
            return utterances

    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        match = LINE_PATTERN.match(line)
        if match:
            h, m, s, ms, speaker, content = match.groups()
            ts = parse_timestamp(h, m, s, ms)
            utterances.append(Utterance(
                timestamp_sec=ts,
                speaker=speaker.strip(),
                text=content.strip(),
            ))
    return utterances


def identify_host(utterances: list[Utterance]) -> str | None:
    """ホスト（営業担当）を特定する.

    基本ルール: ファイル内の最初の話者をホストとみなす。
    最初の話者が特定できない場合は、最も発話文字数が多い話者をホストとする。
    """
    if not utterances:
        return None

    # 最初の話者
    first_speaker = utterances[0].speaker

    # 発話が1件だけの場合は最初の話者
    if len(utterances) <= 1:
        return first_speaker

    # 話者が1人しかいない場合もそのまま
    speakers = {u.speaker for u in utterances}
    if len(speakers) == 1:
        return first_speaker

    # 最初の話者をホストとする
    return first_speaker


def extract_opening_questions(
    utterances: list[Utterance],
    host: str,
    file_path: Path,
    member_key: str,
) -> list[Question]:
    """冒頭3分のホスト質問を抽出する."""
    questions: list[Question] = []

    for utt in utterances:
        # 冒頭3分を超えたら終了
        if utt.timestamp_sec > OPENING_SECONDS:
            break

        # ホストの発話のみ対象
        if utt.speaker != host:
            continue

        # 「？」で終わる発話を質問とみなす
        if utt.text.endswith("？") or utt.text.endswith("?"):
            category = classify_question(utt.text)
            questions.append(Question(
                timestamp_sec=utt.timestamp_sec,
                text=utt.text,
                category=category,
                source_file=file_path.name,
                member_key=member_key,
            ))

    return questions


def classify_question(text: str) -> str:
    """質問テキストをカテゴリに分類する.

    最初にマッチしたカテゴリを採用。どれにも該当しない場合は「その他」。
    """
    for category, keywords in QUESTION_CATEGORIES.items():
        for keyword in keywords:
            if keyword.lower() in text.lower():
                return category
    return "その他"


def process_member_dir(
    member_dir: Path,
    member_key: str,
) -> MemberQuestions:
    """メンバーディレクトリ内の全Transcriptを処理する."""
    result = MemberQuestions(member_key=member_key)

    if not member_dir.exists():
        return result

    txt_files = sorted(member_dir.glob("*.txt"))
    result.file_count = len(txt_files)

    for txt_file in txt_files:
        utterances = parse_transcript(txt_file)
        if not utterances:
            continue

        host = identify_host(utterances)
        if not host:
            continue

        questions = extract_opening_questions(
            utterances, host, txt_file, member_key
        )
        result.questions.extend(questions)

    return result


# ============================================================
# 集計・分析関数
# ============================================================

def aggregate_category_counts(
    questions: list[Question],
) -> dict[str, int]:
    """質問リストをカテゴリ別に集計する."""
    counts: dict[str, int] = Counter()
    for q in questions:
        counts[q.category] += 1
    return dict(counts)


def compute_category_ratios(
    counts: dict[str, int],
) -> dict[str, float]:
    """カテゴリ別の構成比を計算する."""
    total = sum(counts.values())
    if total == 0:
        return {}
    return {cat: round(cnt / total, 4) for cat, cnt in counts.items()}


def get_all_categories() -> list[str]:
    """全カテゴリ名（「その他」含む）を返す."""
    return list(QUESTION_CATEGORIES.keys()) + ["その他"]


def extract_question_order_patterns(
    questions_by_file: dict[str, list[Question]],
) -> list[list[str]]:
    """ファイル別の質問順序パターンを抽出する."""
    patterns: list[list[str]] = []
    for file_key in sorted(questions_by_file.keys()):
        qs = questions_by_file[file_key]
        # 時系列順にカテゴリ列
        pattern = [q.category for q in sorted(qs, key=lambda x: x.timestamp_sec)]
        if pattern:
            patterns.append(pattern)
    return patterns


def find_common_opening_sequence(
    patterns: list[list[str]],
    max_len: int = 5,
) -> list[tuple[str, int]]:
    """頻出する冒頭カテゴリ順序を特定する.

    各パターンの先頭N個を取り出し、順序付きタプルとしてカウントする。
    """
    seq_counts: Counter[tuple[str, ...]] = Counter()
    for pat in patterns:
        # 先頭max_len個のシーケンスを取得
        seq = tuple(pat[:max_len])
        if seq:
            seq_counts[seq] += 1

    # 上位10件を返す
    return [(list(seq), cnt) for seq, cnt in seq_counts.most_common(10)]


def compute_bigram_transitions(
    patterns: list[list[str]],
) -> dict[str, Counter[str]]:
    """カテゴリ間の遷移（bigram）を集計する."""
    transitions: dict[str, Counter[str]] = defaultdict(Counter)
    for pat in patterns:
        for i in range(len(pat) - 1):
            transitions[pat[i]][pat[i + 1]] += 1
    return dict(transitions)


# ============================================================
# メイン分析パイプライン
# ============================================================

def run_group_analysis(
    member_keys: list[str],
    period: str,
    base_dir: Path,
) -> list[Question]:
    """指定グループ・期間の全質問を収集する."""
    all_questions: list[Question] = []
    for key in member_keys:
        member_dir = base_dir / period / key
        result = process_member_dir(member_dir, key)
        all_questions.extend(result.questions)
    return all_questions


def run_sato_analysis(period_dir: Path, label: str) -> list[Question]:
    """佐藤の特定期間ディレクトリから質問を収集する."""
    all_questions: list[Question] = []
    if not period_dir.exists():
        print(f"  警告: {period_dir} が存在しません")
        return all_questions

    txt_files = sorted(period_dir.glob("*.txt"))
    for txt_file in txt_files:
        utterances = parse_transcript(txt_file)
        if not utterances:
            continue
        host = identify_host(utterances)
        if not host:
            continue
        questions = extract_opening_questions(
            utterances, host, txt_file, f"j_sato_{label}"
        )
        all_questions.extend(questions)
    return all_questions


def build_group_stats(
    questions: list[Question],
    label: str,
) -> dict[str, Any]:
    """グループ統計を構築する."""
    counts = aggregate_category_counts(questions)
    ratios = compute_category_ratios(counts)

    # ファイル別に質問をグループ化
    by_file: dict[str, list[Question]] = defaultdict(list)
    for q in questions:
        by_file[f"{q.member_key}/{q.source_file}"].append(q)

    # 順序パターン
    patterns = extract_question_order_patterns(by_file)
    common_seqs = find_common_opening_sequence(patterns)
    bigrams = compute_bigram_transitions(patterns)

    # 質問文字数
    char_lengths = [len(q.text) for q in questions]
    avg_char = round(sum(char_lengths) / len(char_lengths), 1) if char_lengths else 0.0

    # ファイルあたり質問数
    file_count = len(by_file)
    avg_q_per_file = round(len(questions) / file_count, 2) if file_count > 0 else 0.0

    # メンバー別質問数
    by_member: dict[str, int] = Counter()
    for q in questions:
        by_member[q.member_key] += 1

    return {
        "label": label,
        "total_questions": len(questions),
        "total_files": file_count,
        "avg_questions_per_file": avg_q_per_file,
        "avg_question_length": avg_char,
        "category_counts": counts,
        "category_ratios": ratios,
        "by_member": dict(by_member),
        "common_sequences": [
            {"sequence": seq, "count": cnt} for seq, cnt in common_seqs
        ],
        "bigram_transitions": {
            src: dict(dests) for src, dests in bigrams.items()
        },
    }


def collect_sample_questions(
    questions: list[Question],
    n_samples: int = 5,
) -> dict[str, list[dict[str, str]]]:
    """カテゴリ別にサンプル質問を収集する."""
    by_category: dict[str, list[Question]] = defaultdict(list)
    for q in questions:
        by_category[q.category].append(q)

    samples: dict[str, list[dict[str, str]]] = {}
    for cat in get_all_categories():
        qs = by_category.get(cat, [])
        selected = qs[:n_samples]
        samples[cat] = [
            {
                "text": q.text,
                "member": q.member_key,
                "file": q.source_file,
                "timestamp": f"{int(q.timestamp_sec // 60):02d}:{int(q.timestamp_sec % 60):02d}",
            }
            for q in selected
        ]
    return samples


def compute_category_diff(
    ratios_a: dict[str, float],
    ratios_b: dict[str, float],
) -> list[dict[str, Any]]:
    """2グループのカテゴリ構成比の差分を計算し、差が大きい順にソートする."""
    all_cats = get_all_categories()
    diffs: list[dict[str, Any]] = []
    for cat in all_cats:
        r_a = ratios_a.get(cat, 0.0)
        r_b = ratios_b.get(cat, 0.0)
        diff = round(r_a - r_b, 4)
        diffs.append({
            "category": cat,
            "top_ratio": r_a,
            "bottom_ratio": r_b,
            "diff": diff,
            "abs_diff": abs(diff),
        })
    diffs.sort(key=lambda x: x["abs_diff"], reverse=True)
    return diffs


# ============================================================
# コンソール出力
# ============================================================

def print_separator(char: str = "=", width: int = 70) -> None:
    """区切り線を出力する."""
    print(char * width)


def print_header(title: str) -> None:
    """セクションヘッダーを出力する."""
    print()
    print_separator()
    print(f"  {title}")
    print_separator()


def print_category_comparison(
    diffs: list[dict[str, Any]],
    label_a: str,
    label_b: str,
) -> None:
    """カテゴリ構成比の比較テーブルを出力する."""
    print(f"\n  {'カテゴリ':<14} {label_a:>10} {label_b:>10} {'差分':>10}")
    print(f"  {'-' * 14} {'-' * 10} {'-' * 10} {'-' * 10}")
    for d in diffs:
        cat = d["category"]
        tr = f"{d['top_ratio']:.1%}"
        br = f"{d['bottom_ratio']:.1%}"
        diff_str = f"{d['diff']:+.1%}"
        print(f"  {cat:<14} {tr:>10} {br:>10} {diff_str:>10}")


def print_sample_questions(
    samples: dict[str, list[dict[str, str]]],
    max_per_cat: int = 5,
) -> None:
    """カテゴリ別のサンプル質問を出力する."""
    for cat in get_all_categories():
        qs = samples.get(cat, [])
        if not qs:
            continue
        print(f"\n  [{cat}] ({len(qs)}件)")
        for i, q in enumerate(qs[:max_per_cat], 1):
            # 長い質問は80文字で打ち切り
            text = q["text"][:80] + ("..." if len(q["text"]) > 80 else "")
            print(f"    {i}. [{q['timestamp']}] {q['member']}: {text}")


def print_sequence_patterns(
    common_seqs: list[dict[str, Any]],
) -> None:
    """頻出質問順序パターンを出力する."""
    if not common_seqs:
        print("  パターンなし")
        return
    for i, item in enumerate(common_seqs[:10], 1):
        seq_str = " -> ".join(item["sequence"])
        print(f"  {i:2d}. ({item['count']:2d}件) {seq_str}")


def print_bigram_summary(
    bigrams: dict[str, dict[str, int]],
    top_n: int = 10,
) -> None:
    """カテゴリ遷移の頻出ペアを出力する."""
    # 全bigramをフラットにして頻度順ソート
    flat: list[tuple[str, str, int]] = []
    for src, dests in bigrams.items():
        for dst, cnt in dests.items():
            flat.append((src, dst, cnt))
    flat.sort(key=lambda x: x[2], reverse=True)

    for i, (src, dst, cnt) in enumerate(flat[:top_n], 1):
        print(f"  {i:2d}. {src} -> {dst} ({cnt}件)")


def print_sato_comparison(
    stats_ikeike: dict[str, Any],
    stats_damedame: dict[str, Any],
    diffs: list[dict[str, Any]],
) -> None:
    """佐藤のイケイケ期 vs だめだめ期を出力する."""
    print(f"\n  イケイケ期: {stats_ikeike['total_questions']}質問 / "
          f"{stats_ikeike['total_files']}商談 "
          f"(平均 {stats_ikeike['avg_questions_per_file']}件/商談, "
          f"平均文字数 {stats_ikeike['avg_question_length']})")
    print(f"  だめだめ期: {stats_damedame['total_questions']}質問 / "
          f"{stats_damedame['total_files']}商談 "
          f"(平均 {stats_damedame['avg_questions_per_file']}件/商談, "
          f"平均文字数 {stats_damedame['avg_question_length']})")

    print_category_comparison(diffs, "イケイケ", "だめだめ")


# ============================================================
# メイン処理
# ============================================================

def main() -> None:
    """メインエントリーポイント."""
    print("商談冒頭3分の質問パターン分析を開始します...")

    # 結果格納用
    output_data: dict[str, Any] = {}

    # ----------------------------------------------------------
    # 2024H1: 上位 vs 下位
    # ----------------------------------------------------------
    print_header("2024H1 受注率上位 vs 下位: 冒頭3分質問分析")

    q_top_2024 = run_group_analysis(TOP_2024, "2024h1", TEAM_DIR)
    q_bot_2024 = run_group_analysis(BOTTOM_2024, "2024h1", TEAM_DIR)

    stats_top_2024 = build_group_stats(q_top_2024, "2024H1上位")
    stats_bot_2024 = build_group_stats(q_bot_2024, "2024H1下位")

    print(f"\n  上位グループ: {stats_top_2024['total_questions']}質問 / "
          f"{stats_top_2024['total_files']}商談 "
          f"(平均 {stats_top_2024['avg_questions_per_file']}件/商談)")
    print(f"  下位グループ: {stats_bot_2024['total_questions']}質問 / "
          f"{stats_bot_2024['total_files']}商談 "
          f"(平均 {stats_bot_2024['avg_questions_per_file']}件/商談)")

    diffs_2024 = compute_category_diff(
        stats_top_2024["category_ratios"],
        stats_bot_2024["category_ratios"],
    )
    print("\n  --- カテゴリ構成比比較（差分の大きい順） ---")
    print_category_comparison(diffs_2024, "上位", "下位")

    # メンバー別内訳
    print("\n  --- メンバー別質問数 ---")
    print(f"  上位: {stats_top_2024['by_member']}")
    print(f"  下位: {stats_bot_2024['by_member']}")

    output_data["2024h1"] = {
        "top": stats_top_2024,
        "bottom": stats_bot_2024,
        "category_diff": diffs_2024,
    }

    # ----------------------------------------------------------
    # 2026Jan: 上位 vs 下位
    # ----------------------------------------------------------
    print_header("2026Jan 受注率上位 vs 下位: 冒頭3分質問分析")

    q_top_2026 = run_group_analysis(TOP_2026, "2026jan", TEAM_DIR)
    q_bot_2026 = run_group_analysis(BOTTOM_2026, "2026jan", TEAM_DIR)

    stats_top_2026 = build_group_stats(q_top_2026, "2026Jan上位")
    stats_bot_2026 = build_group_stats(q_bot_2026, "2026Jan下位")

    print(f"\n  上位グループ: {stats_top_2026['total_questions']}質問 / "
          f"{stats_top_2026['total_files']}商談 "
          f"(平均 {stats_top_2026['avg_questions_per_file']}件/商談)")
    print(f"  下位グループ: {stats_bot_2026['total_questions']}質問 / "
          f"{stats_bot_2026['total_files']}商談 "
          f"(平均 {stats_bot_2026['avg_questions_per_file']}件/商談)")

    diffs_2026 = compute_category_diff(
        stats_top_2026["category_ratios"],
        stats_bot_2026["category_ratios"],
    )
    print("\n  --- カテゴリ構成比比較（差分の大きい順） ---")
    print_category_comparison(diffs_2026, "上位", "下位")

    print("\n  --- メンバー別質問数 ---")
    print(f"  上位: {stats_top_2026['by_member']}")
    print(f"  下位: {stats_bot_2026['by_member']}")

    output_data["2026jan"] = {
        "top": stats_top_2026,
        "bottom": stats_bot_2026,
        "category_diff": diffs_2026,
    }

    # ----------------------------------------------------------
    # 上位メンバーの質問サンプル
    # ----------------------------------------------------------
    print_header("上位メンバーの質問サンプル（カテゴリ別）")

    # 2024H1 + 2026Jan 上位を統合してサンプル
    all_top_questions = q_top_2024 + q_top_2026
    samples_top = collect_sample_questions(all_top_questions, n_samples=5)
    print_sample_questions(samples_top)

    output_data["top_samples"] = {
        cat: items for cat, items in samples_top.items() if items
    }

    # ----------------------------------------------------------
    # 質問文字数比較
    # ----------------------------------------------------------
    print_header("質問の平均文字数比較")

    periods = [
        ("2024H1上位", stats_top_2024),
        ("2024H1下位", stats_bot_2024),
        ("2026Jan上位", stats_top_2026),
        ("2026Jan下位", stats_bot_2026),
    ]
    print(f"\n  {'グループ':<14} {'平均文字数':>10} {'質問総数':>10} {'商談数':>10}")
    print(f"  {'-' * 14} {'-' * 10} {'-' * 10} {'-' * 10}")
    for label, stats in periods:
        print(f"  {label:<14} {stats['avg_question_length']:>10.1f} "
              f"{stats['total_questions']:>10} {stats['total_files']:>10}")

    output_data["question_length"] = {
        label: {
            "avg_length": stats["avg_question_length"],
            "total_questions": stats["total_questions"],
        }
        for label, stats in periods
    }

    # ----------------------------------------------------------
    # 佐藤のイケイケ期 vs だめだめ期
    # ----------------------------------------------------------
    print_header("佐藤: イケイケ期 vs だめだめ期 冒頭質問比較")

    q_sato_ikeike = run_sato_analysis(SATO_DIR / "ikeike", "ikeike")
    q_sato_damedame = run_sato_analysis(SATO_DIR / "damedame", "damedame")

    stats_sato_ike = build_group_stats(q_sato_ikeike, "佐藤イケイケ")
    stats_sato_dame = build_group_stats(q_sato_damedame, "佐藤だめだめ")

    diffs_sato = compute_category_diff(
        stats_sato_ike["category_ratios"],
        stats_sato_dame["category_ratios"],
    )
    print_sato_comparison(stats_sato_ike, stats_sato_dame, diffs_sato)

    # 佐藤のイケイケ期サンプル
    print("\n  --- イケイケ期の質問サンプル ---")
    samples_ike = collect_sample_questions(q_sato_ikeike, n_samples=3)
    print_sample_questions(samples_ike, max_per_cat=3)

    print("\n  --- だめだめ期の質問サンプル ---")
    samples_dame = collect_sample_questions(q_sato_damedame, n_samples=3)
    print_sample_questions(samples_dame, max_per_cat=3)

    output_data["sato_comparison"] = {
        "ikeike": stats_sato_ike,
        "damedame": stats_sato_dame,
        "category_diff": diffs_sato,
        "ikeike_samples": {
            cat: items for cat, items in samples_ike.items() if items
        },
        "damedame_samples": {
            cat: items for cat, items in samples_dame.items() if items
        },
    }

    # ----------------------------------------------------------
    # 質問順序パターン分析
    # ----------------------------------------------------------
    print_header("質問の順序パターン分析")

    # 上位メンバーの順序パターン（2024H1 + 2026Jan統合）
    print("\n  --- 上位メンバー: 頻出質問カテゴリ順序 ---")
    print_sequence_patterns(
        stats_top_2024["common_sequences"]
        + stats_top_2026["common_sequences"]
    )

    print("\n  --- 下位メンバー: 頻出質問カテゴリ順序 ---")
    print_sequence_patterns(
        stats_bot_2024["common_sequences"]
        + stats_bot_2026["common_sequences"]
    )

    # カテゴリ間遷移（上位）
    print("\n  --- 上位メンバー: 頻出カテゴリ遷移（bigram） ---")
    # 上位の全bigramを統合
    merged_bigrams_top: dict[str, Counter[str]] = defaultdict(Counter)
    for src, dests in stats_top_2024["bigram_transitions"].items():
        for dst, cnt in dests.items():
            merged_bigrams_top[src][dst] += cnt
    for src, dests in stats_top_2026["bigram_transitions"].items():
        for dst, cnt in dests.items():
            merged_bigrams_top[src][dst] += cnt
    print_bigram_summary(
        {s: dict(d) for s, d in merged_bigrams_top.items()}
    )

    print("\n  --- 下位メンバー: 頻出カテゴリ遷移（bigram） ---")
    merged_bigrams_bot: dict[str, Counter[str]] = defaultdict(Counter)
    for src, dests in stats_bot_2024["bigram_transitions"].items():
        for dst, cnt in dests.items():
            merged_bigrams_bot[src][dst] += cnt
    for src, dests in stats_bot_2026["bigram_transitions"].items():
        for dst, cnt in dests.items():
            merged_bigrams_bot[src][dst] += cnt
    print_bigram_summary(
        {s: dict(d) for s, d in merged_bigrams_bot.items()}
    )

    # 佐藤の順序パターン
    print("\n  --- 佐藤イケイケ期: 頻出質問カテゴリ順序 ---")
    print_sequence_patterns(stats_sato_ike["common_sequences"])

    print("\n  --- 佐藤だめだめ期: 頻出質問カテゴリ順序 ---")
    print_sequence_patterns(stats_sato_dame["common_sequences"])

    output_data["sequence_patterns"] = {
        "top_combined": {
            "common_sequences": (
                stats_top_2024["common_sequences"]
                + stats_top_2026["common_sequences"]
            ),
        },
        "bottom_combined": {
            "common_sequences": (
                stats_bot_2024["common_sequences"]
                + stats_bot_2026["common_sequences"]
            ),
        },
        "top_bigrams": {
            s: dict(d) for s, d in merged_bigrams_top.items()
        },
        "bottom_bigrams": {
            s: dict(d) for s, d in merged_bigrams_bot.items()
        },
    }

    # ----------------------------------------------------------
    # 全期間横断サマリー
    # ----------------------------------------------------------
    print_header("全期間横断サマリー")

    # 上位で一貫して多いカテゴリを特定
    print("\n  上位が一貫して多いカテゴリ（2024H1 & 2026Janの両方で+）:")
    consistent_top = []
    for d24 in diffs_2024:
        for d26 in diffs_2026:
            if d24["category"] == d26["category"]:
                if d24["diff"] > 0 and d26["diff"] > 0:
                    consistent_top.append({
                        "category": d24["category"],
                        "diff_2024": d24["diff"],
                        "diff_2026": d26["diff"],
                    })
    if consistent_top:
        for item in consistent_top:
            print(f"    - {item['category']}: "
                  f"2024H1 +{item['diff_2024']:.1%}, "
                  f"2026Jan +{item['diff_2026']:.1%}")
    else:
        print("    なし")

    print("\n  下位が一貫して多いカテゴリ（2024H1 & 2026Janの両方で-）:")
    consistent_bot = []
    for d24 in diffs_2024:
        for d26 in diffs_2026:
            if d24["category"] == d26["category"]:
                if d24["diff"] < 0 and d26["diff"] < 0:
                    consistent_bot.append({
                        "category": d24["category"],
                        "diff_2024": d24["diff"],
                        "diff_2026": d26["diff"],
                    })
    if consistent_bot:
        for item in consistent_bot:
            print(f"    - {item['category']}: "
                  f"2024H1 {item['diff_2024']:+.1%}, "
                  f"2026Jan {item['diff_2026']:+.1%}")
    else:
        print("    なし")

    output_data["cross_period_summary"] = {
        "consistent_top_categories": consistent_top,
        "consistent_bottom_categories": consistent_bot,
    }

    # ----------------------------------------------------------
    # JSON出力
    # ----------------------------------------------------------
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", encoding="utf-8") as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)

    print_header("完了")
    print(f"\n  JSON出力先: {OUTPUT_PATH}")
    print(f"  分析対象: 2024H1 + 2026Jan + 佐藤比較")
    print(f"  抽出質問総数: "
          f"{stats_top_2024['total_questions'] + stats_bot_2024['total_questions'] + stats_top_2026['total_questions'] + stats_bot_2026['total_questions'] + stats_sato_ike['total_questions'] + stats_sato_dame['total_questions']}件")
    print()


if __name__ == "__main__":
    main()
