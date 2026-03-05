"""
Zoom-Salesforce同期 スケジュール実行スクリプト

Windows Task Schedulerから呼び出されるエントリポイント

使用方法:
    # 直接実行
    python schedule_zoom_sync.py

    # Windows Task Scheduler設定:
    1. タスクスケジューラを開く
    2. 「タスクの作成」を選択
    3. トリガー: 15分ごとに繰り返し
    4. 操作: pythonw.exe schedule_zoom_sync.py
"""

import sys
import os
import logging
from pathlib import Path
from datetime import datetime
import traceback

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# ログ設定
log_dir = project_root / "logs" / "zoom_sync"
log_dir.mkdir(parents=True, exist_ok=True)

log_file = log_dir / f"sync_{datetime.now().strftime('%Y%m%d')}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


def main():
    """メイン処理"""
    logger.info("=" * 60)
    logger.info("Zoom-Salesforce同期 開始")
    logger.info("=" * 60)

    try:
        from scripts.zoom_sf_sync_pipeline import ZoomSFSyncPipeline

        # パイプライン初期化
        pipeline = ZoomSFSyncPipeline()

        # 認証
        logger.info("Salesforce認証中...")
        pipeline.authenticate()
        logger.info("認証完了")

        # フィールド確認
        logger.info("Salesforceフィールド確認中...")
        fields_ok = pipeline.check_zoom_fields()

        if not fields_ok:
            logger.error("カスタムフィールドが不足しています")
            logger.error("Salesforce設定画面でフィールドを追加してください")
            return 1

        # 同期実行
        logger.info("同期処理開始...")
        results_df = pipeline.run_scheduled(dry_run=False)

        if results_df is not None and not results_df.empty:
            success_count = (results_df['status'] == 'success').sum()
            total_count = len(results_df)
            logger.info(f"処理完了: {success_count}/{total_count} 件成功")
        else:
            logger.info("処理対象なし")

        logger.info("=" * 60)
        logger.info("Zoom-Salesforce同期 完了")
        logger.info("=" * 60)

        return 0

    except Exception as e:
        logger.error(f"エラー発生: {e}")
        logger.error(traceback.format_exc())
        return 1


def create_task_scheduler_xml():
    """
    Windows Task Scheduler用のXML設定を生成

    使用方法:
        python schedule_zoom_sync.py --create-task
    """
    xml_content = f'''<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Description>Zoom商談分析結果をSalesforceに同期</Description>
    <Author>Salesforce_List</Author>
  </RegistrationInfo>
  <Triggers>
    <TimeTrigger>
      <Repetition>
        <Interval>PT15M</Interval>
        <StopAtDurationEnd>false</StopAtDurationEnd>
      </Repetition>
      <StartBoundary>{datetime.now().strftime('%Y-%m-%dT09:00:00')}</StartBoundary>
      <Enabled>true</Enabled>
    </TimeTrigger>
  </Triggers>
  <Principals>
    <Principal id="Author">
      <LogonType>InteractiveToken</LogonType>
      <RunLevel>LeastPrivilege</RunLevel>
    </Principal>
  </Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <AllowHardTerminate>true</AllowHardTerminate>
    <StartWhenAvailable>true</StartWhenAvailable>
    <RunOnlyIfNetworkAvailable>true</RunOnlyIfNetworkAvailable>
    <IdleSettings>
      <StopOnIdleEnd>false</StopOnIdleEnd>
      <RestartOnIdle>false</RestartOnIdle>
    </IdleSettings>
    <AllowStartOnDemand>true</AllowStartOnDemand>
    <Enabled>true</Enabled>
    <Hidden>false</Hidden>
    <RunOnlyIfIdle>false</RunOnlyIfIdle>
    <WakeToRun>false</WakeToRun>
    <ExecutionTimeLimit>PT1H</ExecutionTimeLimit>
    <Priority>7</Priority>
  </Settings>
  <Actions Context="Author">
    <Exec>
      <Command>pythonw.exe</Command>
      <Arguments>"{project_root / 'scripts' / 'schedule_zoom_sync.py'}"</Arguments>
      <WorkingDirectory>{project_root}</WorkingDirectory>
    </Exec>
  </Actions>
</Task>
'''
    output_path = project_root / "config" / "zoom_sync_task.xml"
    with open(output_path, 'w', encoding='utf-16') as f:
        f.write(xml_content)

    print(f"タスクスケジューラ設定を生成しました: {output_path}")
    print()
    print("インポート方法:")
    print("  1. タスクスケジューラを開く")
    print("  2. 「タスクのインポート」を選択")
    print(f"  3. {output_path} を選択")
    print()
    print("または、コマンドラインから:")
    print(f'  schtasks /create /xml "{output_path}" /tn "Zoom-Salesforce同期"')


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Zoom-Salesforce同期 スケジュール実行")
    parser.add_argument("--create-task", action="store_true", help="Task Scheduler設定XMLを生成")
    parser.add_argument("--dry-run", action="store_true", help="ドライラン")

    args = parser.parse_args()

    if args.create_task:
        create_task_scheduler_xml()
    else:
        sys.exit(main())
