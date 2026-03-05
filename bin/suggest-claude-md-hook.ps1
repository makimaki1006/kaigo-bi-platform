# CLAUDE.md改善提案フック (PowerShell版)
# セッション終了時またはコンパクト前に会話履歴を分析

# 無限ループ対策
if ($env:SUGGEST_CLAUDE_MD_RUNNING -eq "1") {
    exit 0
}

# stdinからJSON入力を読み取り
try {
    $inputJson = [Console]::In.ReadToEnd()
    $data = $inputJson | ConvertFrom-Json
} catch {
    exit 0
}

# transcript_pathを取得
$transcriptPath = $data.transcript_path
if (-not $transcriptPath -or -not (Test-Path $transcriptPath)) {
    exit 0
}

# 会話履歴を抽出
$conversations = @()
$lines = Get-Content $transcriptPath -Encoding UTF8

foreach ($line in $lines) {
    if ([string]::IsNullOrWhiteSpace($line)) { continue }

    try {
        $entry = $line | ConvertFrom-Json

        if ($entry.type -eq "human") {
            $content = $entry.message.content
            if ($content -is [string]) {
                $conversations += "[User]: $content"
            }
        }
        elseif ($entry.type -eq "assistant") {
            $content = $entry.message.content
            if ($content -is [string]) {
                $preview = $content.Substring(0, [Math]::Min(500, $content.Length))
                $conversations += "[Assistant]: $preview..."
            }
        }
    } catch {
        continue
    }
}

# 直近20件を保存
$recentConversations = $conversations | Select-Object -Last 20
$conversationText = $recentConversations -join "`n`n"

# 一時ファイルに保存
$projectDir = $env:CLAUDE_PROJECT_DIR
if (-not $projectDir) { $projectDir = "." }

$logDir = Join-Path $projectDir "logs"
if (-not (Test-Path $logDir)) {
    New-Item -ItemType Directory -Path $logDir -Force | Out-Null
}

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$tempFile = Join-Path $logDir "session_transcript_$timestamp.txt"
$conversationText | Out-File -FilePath $tempFile -Encoding UTF8

# 新しいターミナルでClaude Codeを起動
$env:SUGGEST_CLAUDE_MD_RUNNING = "1"
Start-Process cmd -ArgumentList "/k", "cd /d `"$projectDir`" && set SUGGEST_CLAUDE_MD_RUNNING=1 && echo 会話履歴を分析中... && echo ファイル: $tempFile"

exit 0
