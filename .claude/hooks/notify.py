#!/usr/bin/env python3
"""
通知フック
Windows/Mac/Linuxでデスクトップ通知を送信
"""

import json
import sys
import subprocess
import platform


def send_notification(title: str, message: str):
    """OSに応じた通知を送信"""
    system = platform.system()

    try:
        if system == 'Windows':
            # Windows: PowerShellのToast通知
            ps_script = f'''
            [Windows.UI.Notifications.ToastNotificationManager, Windows.UI.Notifications, ContentType = WindowsRuntime] | Out-Null
            $template = [Windows.UI.Notifications.ToastTemplateType]::ToastText02
            $xml = [Windows.UI.Notifications.ToastNotificationManager]::GetTemplateContent($template)
            $text = $xml.GetElementsByTagName("text")
            $text[0].AppendChild($xml.CreateTextNode("{title}")) | Out-Null
            $text[1].AppendChild($xml.CreateTextNode("{message}")) | Out-Null
            $toast = [Windows.UI.Notifications.ToastNotification]::new($xml)
            [Windows.UI.Notifications.ToastNotificationManager]::CreateToastNotifier("Claude Code").Show($toast)
            '''
            subprocess.run(
                ['powershell', '-Command', ps_script],
                capture_output=True,
                timeout=5
            )
        elif system == 'Darwin':
            # macOS: osascript
            subprocess.run([
                'osascript', '-e',
                f'display notification "{message}" with title "{title}"'
            ], capture_output=True, timeout=5)
        elif system == 'Linux':
            # Linux: notify-send
            subprocess.run([
                'notify-send', title, message
            ], capture_output=True, timeout=5)
    except Exception:
        pass  # 通知失敗は無視


def main():
    try:
        data = json.load(sys.stdin)
    except json.JSONDecodeError:
        sys.exit(0)

    notification_type = data.get('type', '')
    message = data.get('message', '')

    # 通知タイプに応じたメッセージ
    notifications = {
        'permission_prompt': ('Claude Code', '🔐 パーミッションが必要です'),
        'idle_prompt': ('Claude Code', '⏳ ユーザー入力を待機中...'),
        'task_complete': ('Claude Code', '✅ タスクが完了しました'),
        'error': ('Claude Code', f'❌ エラー: {message}'),
    }

    if notification_type in notifications:
        title, msg = notifications[notification_type]
        send_notification(title, msg)
    elif message:
        send_notification('Claude Code', message)

    sys.exit(0)


if __name__ == '__main__':
    main()
