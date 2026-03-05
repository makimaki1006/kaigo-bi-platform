# Claude Code Hooks 設定

このディレクトリには、Claude Codeのライフサイクルをカスタマイズするhookスクリプトが含まれています。

## 設定されているHooks

### 1. PreToolUse Hooks

ツール実行前に呼び出され、実行をブロック・許可・変更できます。

| ファイル | 対象 | 機能 |
|---------|------|------|
| `validate_bash.py` | Bash | 危険なコマンドをブロック |
| `protect_files.py` | Edit, Write | 機密ファイルを保護 |

#### ブロックされる危険なコマンド
- `rm -rf /` - ルートからの再帰削除
- `sudo rm` - 管理者権限での削除
- `mkfs.*` - ファイルシステム作成
- `dd if=...of=/dev` - デバイス書き込み
- フォークボム
- `curl|sh`, `wget|sh` - パイプ経由の実行

#### 保護されるファイル
- `.env` - 環境変数（機密情報）
- `credentials.json` - 認証情報
- `.git/` - Gitディレクトリ
- `*.pem`, `*.key` - 証明書・秘密鍵

### 2. PostToolUse Hooks

ツール実行後に呼び出されます。

| ファイル | 対象 | 機能 |
|---------|------|------|
| `post_edit.py` | Edit, Write | フォーマットヒントを表示 |

- Pythonファイル: `black`でのフォーマットを提案
- TypeScript/JavaScript: `prettier`でのフォーマットを提案

### 3. Notification Hooks

通知イベント時に呼び出されます。

| ファイル | 機能 |
|---------|------|
| `notify.py` | OS通知（Windows/Mac/Linux対応） |

### 4. SessionStart Hooks

セッション開始時に呼び出されます。

| ファイル | 機能 |
|---------|------|
| `session_start.py` | 環境変数設定、セッションログ記録 |

## 設定ファイル

hooks設定は `.claude/settings.local.json` に定義されています：

```json
{
  "hooks": {
    "PreToolUse": [...],
    "PostToolUse": [...],
    "Notification": [...],
    "SessionStart": [...]
  }
}
```

## 終了コード

| コード | 動作 |
|--------|------|
| 0 | 成功（処理を継続） |
| 2 | ブロック（ツール実行を中止） |
| その他 | エラー（処理は継続） |

## カスタマイズ

新しいhookを追加する場合：

1. `.claude/hooks/` にPythonスクリプトを作成
2. `settings.local.json` の該当イベントに追加
3. Claude Codeを再起動

## デバッグ

```bash
# 現在のhooks設定を確認
claude /hooks

# デバッグモードで実行
claude --debug
```

## 参考リンク

- [Claude Code Hooks公式ドキュメント](https://docs.anthropic.com/en/docs/claude-code/hooks)
- [Zenn記事: Claude Codeのhooks機能](https://zenn.dev/appbrew/articles/e2f38677f6a0ce)
