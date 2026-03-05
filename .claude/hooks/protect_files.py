#!/usr/bin/env python3
"""
ファイル保護フック
機密ファイルや重要な設定ファイルの編集をブロックする
"""

import json
import sys
import os


# 編集をブロックするファイル/パターン
PROTECTED_FILES = [
    '.env',                    # 環境変数（機密情報）
    'config/.env',             # プロジェクト環境変数
    '.git/',                   # Gitディレクトリ
    'credentials.json',        # 認証情報
    'secrets/',                # シークレットディレクトリ
    '.pem',                    # 証明書
    '.key',                    # 秘密鍵
]

# 警告を出すが編集は許可するファイル
WARNING_FILES = [
    'requirements.txt',        # 依存関係
    'package.json',            # Node依存関係
    'package-lock.json',       # ロックファイル
    '.gitignore',              # Git除外設定
]


def main():
    try:
        data = json.load(sys.stdin)
    except json.JSONDecodeError:
        sys.exit(0)

    tool_input = data.get('tool_input', {})
    file_path = tool_input.get('file_path', '')

    # パスの正規化
    normalized_path = os.path.normpath(file_path).replace('\\', '/')

    # 保護されたファイルをチェック
    for protected in PROTECTED_FILES:
        if protected in normalized_path or normalized_path.endswith(protected):
            # .env.exampleは許可
            if normalized_path.endswith('.env.example'):
                continue
            print(f'✗ 保護されたファイル: {file_path}', file=sys.stderr)
            print('  このファイルは機密情報を含む可能性があるため、直接編集はブロックされました。', file=sys.stderr)
            sys.exit(2)  # ブロック

    # 警告ファイルをチェック
    for warning in WARNING_FILES:
        if normalized_path.endswith(warning):
            output = {
                "systemMessage": f"⚠️ 注意: {warning} は重要なファイルです。変更内容を確認してください。"
            }
            print(json.dumps(output))
            break

    sys.exit(0)


if __name__ == '__main__':
    main()
