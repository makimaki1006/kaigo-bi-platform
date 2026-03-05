# -*- coding: utf-8 -*-
"""お手本商談の文字起こしを抽出（GAS設定用）"""

import re
import sys

sys.stdout.reconfigure(encoding='utf-8')

MODEL_TRANSCRIPT_PATH = r"C:\Users\fuji1\Downloads\GMT20260202-094820_Recording.transcript.vtt"


def parse_vtt(vtt_text):
    """VTTをプレーンテキストに変換"""
    lines = []
    for line in vtt_text.split('\n'):
        line = line.strip()
        if not line or line == 'WEBVTT':
            continue
        if re.match(r'^\d+$', line):
            continue
        if re.match(r'\d{2}:\d{2}:\d{2}', line):
            continue
        if ':' in line:
            lines.append(line)
    return '\n'.join(lines)


def main():
    print("=" * 70)
    print("【お手本商談テキスト抽出】")
    print("=" * 70)
    print()

    with open(MODEL_TRANSCRIPT_PATH, 'r', encoding='utf-8') as f:
        vtt_text = f.read()

    text = parse_vtt(vtt_text)

    # 出力ファイルに保存
    output_path = r"C:\Users\fuji1\OneDrive\デスクトップ\Salesforce_List\data\output\model_transcript.txt"
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(text)

    print(f"文字数: {len(text)}")
    print(f"出力先: {output_path}")
    print()
    print("このテキストをGASの「設定」シートの MODEL_TRANSCRIPT に貼り付けてください。")
    print()
    print("【テキスト冒頭】")
    print("-" * 70)
    print(text[:1000])
    print("...")


if __name__ == '__main__':
    main()
