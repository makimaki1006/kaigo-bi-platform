#!/usr/bin/env python3
"""
Bashコマンド検証フック
危険なコマンドの実行をブロックする
"""

import json
import sys
import re


# 危険なコマンドパターン
DANGEROUS_PATTERNS = [
    r'rm\s+-rf\s+/',           # ルートからの再帰削除
    r'rm\s+-rf\s+\*',          # ワイルドカード削除
    r'rm\s+-rf\s+~',           # ホームディレクトリ削除
    r'sudo\s+rm',              # sudo rm
    r'mkfs\.',                 # ファイルシステム作成
    r'dd\s+if=.*of=/dev',      # デバイス書き込み
    r':\(\)\{.*\};:',          # フォークボム
    r'>\s*/dev/sd',            # ディスクへの直接書き込み
    r'chmod\s+-R\s+777\s+/',   # 危険なパーミッション変更
    r'curl.*\|\s*(ba)?sh',     # パイプからの実行
    r'wget.*\|\s*(ba)?sh',     # パイプからの実行
]

# 警告を出すパターン（ブロックはしない）
WARNING_PATTERNS = [
    r'rm\s+-rf',               # 再帰削除（一般）
    r'git\s+push\s+.*--force', # 強制プッシュ
    r'git\s+reset\s+--hard',   # ハードリセット
    r'DROP\s+TABLE',           # テーブル削除
    r'DROP\s+DATABASE',        # データベース削除
]


def main():
    try:
        data = json.load(sys.stdin)
    except json.JSONDecodeError:
        sys.exit(0)

    tool_input = data.get('tool_input', {})
    command = tool_input.get('command', '')

    # 危険なコマンドをチェック
    for pattern in DANGEROUS_PATTERNS:
        if re.search(pattern, command, re.IGNORECASE):
            print(f'✗ 危険なコマンドがブロックされました: {pattern}', file=sys.stderr)
            sys.exit(2)  # ブロック

    # 警告パターンをチェック（ブロックはしない）
    for pattern in WARNING_PATTERNS:
        if re.search(pattern, command, re.IGNORECASE):
            output = {
                "systemMessage": f"⚠️ 警告: このコマンドは危険な可能性があります ({pattern})"
            }
            print(json.dumps(output))
            sys.exit(0)

    sys.exit(0)


if __name__ == '__main__':
    main()
