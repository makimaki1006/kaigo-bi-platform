#!/usr/bin/env python3
"""
編集後処理フック
Pythonファイルの場合、フォーマットチェックを行う
"""

import json
import sys
import subprocess
import os


def main():
    try:
        data = json.load(sys.stdin)
    except json.JSONDecodeError:
        sys.exit(0)

    tool_input = data.get('tool_input', {})
    tool_response = data.get('tool_response', {})
    file_path = tool_input.get('file_path', '')

    # 成功した場合のみ処理
    if not tool_response.get('success', True):
        sys.exit(0)

    # Pythonファイルの場合
    if file_path.endswith('.py'):
        # blackがインストールされているかチェック
        try:
            result = subprocess.run(
                ['black', '--check', '--quiet', file_path],
                capture_output=True,
                text=True,
                timeout=10
            )
            if result.returncode != 0:
                output = {
                    "systemMessage": f"💡 ヒント: {os.path.basename(file_path)} はblackでフォーマットできます: `black {file_path}`"
                }
                print(json.dumps(output))
        except FileNotFoundError:
            # blackがインストールされていない場合は無視
            pass
        except subprocess.TimeoutExpired:
            pass

    # TypeScript/JavaScriptファイルの場合
    elif file_path.endswith(('.ts', '.tsx', '.js', '.jsx')):
        output = {
            "systemMessage": f"💡 ヒント: prettierでフォーマット可能: `npx prettier --write {file_path}`"
        }
        print(json.dumps(output))

    sys.exit(0)


if __name__ == '__main__':
    main()
