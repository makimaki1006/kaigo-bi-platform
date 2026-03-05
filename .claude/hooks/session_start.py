#!/usr/bin/env python3
"""
セッション開始フック
環境変数の設定とプロジェクト初期化を行う
"""

import json
import sys
import os
from pathlib import Path
from datetime import datetime


def main():
    # CLAUDE_ENV_FILEが設定されている場合、環境変数を追加
    env_file = os.environ.get('CLAUDE_ENV_FILE')
    project_dir = os.environ.get('CLAUDE_PROJECT_DIR', '.')

    if env_file:
        try:
            with open(env_file, 'a', encoding='utf-8') as f:
                # Python仮想環境のアクティベート（存在する場合）
                venv_paths = [
                    Path(project_dir) / 'venv' / 'Scripts' / 'activate',  # Windows
                    Path(project_dir) / 'venv' / 'bin' / 'activate',      # Unix
                    Path(project_dir) / '.venv' / 'Scripts' / 'activate', # Windows alt
                    Path(project_dir) / '.venv' / 'bin' / 'activate',     # Unix alt
                ]

                for venv_path in venv_paths:
                    if venv_path.exists():
                        # 仮想環境のPythonパスを追加
                        venv_dir = venv_path.parent.parent
                        if os.name == 'nt':  # Windows
                            f.write(f'export PATH="{venv_dir}/Scripts:$PATH"\n')
                        else:  # Unix
                            f.write(f'export PATH="{venv_dir}/bin:$PATH"\n')
                        break

                # プロジェクト固有の環境変数
                f.write(f'export PROJECT_ROOT="{project_dir}"\n')
                f.write('export PYTHONPATH="${PROJECT_ROOT}:${PYTHONPATH}"\n')

        except Exception as e:
            print(f'環境変数設定エラー: {e}', file=sys.stderr)

    # セッションログを記録
    log_dir = Path(project_dir) / 'logs'
    log_dir.mkdir(exist_ok=True)

    session_log = log_dir / 'session_history.log'
    try:
        with open(session_log, 'a', encoding='utf-8') as f:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            f.write(f'[{timestamp}] セッション開始\n')
    except Exception:
        pass

    # 起動メッセージを出力
    output = {
        "systemMessage": "🚀 Salesforce List プロジェクトへようこそ！"
    }
    print(json.dumps(output))

    sys.exit(0)


if __name__ == '__main__':
    main()
