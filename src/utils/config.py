"""
設定管理モジュール
環境変数とYAML設定ファイルを読み込む
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# プロジェクトルートディレクトリ
PROJECT_ROOT = Path(__file__).parent.parent.parent

# .envファイルを読み込み
env_path = PROJECT_ROOT / "config" / ".env"
load_dotenv(env_path)


class SalesforceConfig:
    """Salesforce API 設定"""
    CLIENT_ID: str = os.getenv("SF_CLIENT_ID", "")
    CLIENT_SECRET: str = os.getenv("SF_CLIENT_SECRET", "")
    REFRESH_TOKEN: str = os.getenv("SF_REFRESH_TOKEN", "")
    INSTANCE_URL: str = os.getenv("SF_INSTANCE_URL", "")
    API_VERSION: str = os.getenv("SF_API_VERSION", "v57.0")


class OutputConfig:
    """出力設定"""
    OUTPUT_DIR: Path = PROJECT_ROOT / os.getenv("OUTPUT_DIR", "data/output")

    @classmethod
    def ensure_dir(cls) -> Path:
        """出力ディレクトリが存在することを確認"""
        cls.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        return cls.OUTPUT_DIR


# グローバル設定インスタンス
sf_config = SalesforceConfig()
output_config = OutputConfig()
