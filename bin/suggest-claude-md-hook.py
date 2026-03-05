#!/usr/bin/env python3
"""
CLAUDE.md改善提案フック
セッション終了時またはコンパクト前に会話履歴を分析し、
CLAUDE.mdの改善提案を生成する

参考: https://zenn.dev/appbrew/articles/e2f38677f6a0ce
"""

import json
import sys
import os
import subprocess
from pathlib import Path
from datetime import datetime


def main():
    # 無限ループ対策: 環境変数でフラグ管理
    if os.environ.get('SUGGEST_CLAUDE_MD_RUNNING') == '1':
        sys.exit(0)

    # stdinからJSON入力を読み取り
    try:
        data = json.load(sys.stdin)
    except json.JSONDecodeError:
        sys.exit(0)

    # transcript_pathを取得
    transcript_path = data.get('transcript_path', '')
    if not transcript_path or not Path(transcript_path).exists():
        sys.exit(0)

    # 会話履歴を抽出
    conversation_text = extract_conversation(transcript_path)
    if not conversation_text:
        sys.exit(0)

    # 会話履歴を一時ファイルに保存
    project_dir = os.environ.get('CLAUDE_PROJECT_DIR', '.')
    temp_dir = Path(project_dir) / 'logs'
    temp_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    temp_file = temp_dir / f'session_transcript_{timestamp}.txt'

    with open(temp_file, 'w', encoding='utf-8') as f:
        f.write(conversation_text)

    # 新しいターミナルでClaude Codeを起動
    launch_claude_for_suggestion(project_dir, temp_file)

    sys.exit(0)


def extract_conversation(transcript_path: str) -> str:
    """JSONLファイルから会話テキストを抽出"""
    conversations = []

    try:
        with open(transcript_path, 'r', encoding='utf-8') as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    entry = json.loads(line)
                    # メッセージタイプに応じてテキストを抽出
                    if entry.get('type') == 'human':
                        content = entry.get('message', {}).get('content', '')
                        if isinstance(content, str):
                            conversations.append(f"[User]: {content}")
                        elif isinstance(content, list):
                            for item in content:
                                if isinstance(item, dict) and item.get('type') == 'text':
                                    conversations.append(f"[User]: {item.get('text', '')}")
                    elif entry.get('type') == 'assistant':
                        content = entry.get('message', {}).get('content', '')
                        if isinstance(content, str):
                            conversations.append(f"[Assistant]: {content[:500]}...")
                        elif isinstance(content, list):
                            for item in content:
                                if isinstance(item, dict) and item.get('type') == 'text':
                                    text = item.get('text', '')
                                    conversations.append(f"[Assistant]: {text[:500]}...")
                except json.JSONDecodeError:
                    continue
    except Exception as e:
        return ""

    return "\n\n".join(conversations[-20:])  # 直近20件


def launch_claude_for_suggestion(project_dir: str, transcript_file: Path):
    """新しいターミナルでClaude Codeを起動してCLAUDE.md改善提案を生成"""

    # Windows用: 新しいコマンドプロンプトウィンドウで実行
    prompt = f"""
以下の会話履歴を分析して、CLAUDE.mdファイルの改善提案を行ってください。

会話履歴ファイル: {transcript_file}

分析観点:
1. 頻繁に使用されるコマンドやパターン
2. プロジェクト固有の規則や制約
3. エラーハンドリングで学んだこと
4. 効率化できるワークフロー

提案形式:
- 具体的な追記内容
- 既存ルールの改善案
- 新しいカスタムコマンドの提案
"""

    # 環境変数を設定して無限ループを防止
    env = os.environ.copy()
    env['SUGGEST_CLAUDE_MD_RUNNING'] = '1'

    try:
        if sys.platform == 'win32':
            # Windows: 新しいコマンドプロンプトで実行
            cmd = f'start cmd /k "cd /d {project_dir} && set SUGGEST_CLAUDE_MD_RUNNING=1 && claude"'
            subprocess.Popen(cmd, shell=True, env=env)
        else:
            # macOS/Linux
            subprocess.Popen(
                ['claude'],
                cwd=project_dir,
                env=env,
                start_new_session=True
            )
    except Exception:
        pass


if __name__ == '__main__':
    main()
