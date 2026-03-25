"""
Turso DB共通ヘルパーモジュール
全スクリプトで共通のDB操作・データ変換ロジックを集約

使用例:
    from turso_helpers import get_turso_config, get_headers, execute_sql, execute_single, make_arg
    from turso_helpers import extract_prefecture, classify_corp_type, parse_int, safe_float, compute_derived
"""
import logging
import os
import re
from datetime import datetime

import requests

logger = logging.getLogger(__name__)

# ============================================================
# Turso接続（環境変数必須、フォールバックなし）
# ============================================================


def get_turso_config():
    """Turso接続情報を環境変数から取得。未設定時はエラー"""
    url = os.environ.get("TURSO_DATABASE_URL")
    token = os.environ.get("TURSO_AUTH_TOKEN")
    if not url:
        raise ValueError("TURSO_DATABASE_URL environment variable is required")
    if not token:
        raise ValueError("TURSO_AUTH_TOKEN environment variable is required")
    return url, token


def get_headers(token):
    """Turso API用HTTPヘッダーを生成"""
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def execute_sql(url, headers, statements, timeout=120):
    """Turso HTTP API v2 pipeline でSQLを実行"""
    resp = requests.post(
        f"{url}/v2/pipeline",
        headers=headers,
        json={"requests": statements},
        timeout=timeout,
    )
    if resp.status_code != 200:
        raise Exception(f"Turso APIエラー: HTTP {resp.status_code}: {resp.text[:300]}")
    return resp.json()


def execute_single(url, headers, sql, args=None, timeout=120):
    """単一SQLを実行"""
    stmt = {"type": "execute", "stmt": {"sql": sql}}
    if args:
        stmt["stmt"]["args"] = args
    return execute_sql(url, headers, [stmt], timeout=timeout)


def make_arg(value):
    """Turso API用の引数フォーマットを生成"""
    if value is None:
        return {"type": "null"}
    elif isinstance(value, int):
        return {"type": "integer", "value": str(value)}
    elif isinstance(value, float):
        return {"type": "float", "value": value}
    else:
        return {"type": "text", "value": str(value)}


# ============================================================
# 都道府県抽出（ホワイトリスト方式に統一）
# ============================================================

VALID_PREFECTURES = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県",
    "岐阜県", "静岡県", "愛知県", "三重県",
    "滋賀県", "京都府", "大阪府", "兵庫県", "奈良県", "和歌山県",
    "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県",
    "福岡県", "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
]

VALID_PREFECTURES_SET = set(VALID_PREFECTURES)


def extract_prefecture(address):
    """住所から都道府県を抽出（ホワイトリスト方式）"""
    if not address:
        return None
    cleaned = re.sub(r"^〒?\d{3}-?\d{4}\s*", "", str(address).strip())
    # 先頭一致を優先
    for pref in VALID_PREFECTURES:
        if cleaned.startswith(pref):
            return pref
    # フォールバック: 文字列内に都道府県名が含まれる場合
    for pref in VALID_PREFECTURES:
        if pref in cleaned:
            return pref
    return None


# ============================================================
# 法人種別分類（統一版）
# ============================================================

_CORP_TYPE_MAPPING = {
    "社会福祉法人": "社会福祉法人",
    "社会医療法人": "社会医療法人",
    "医療法人": "医療法人",
    "株式会社": "株式会社・有限会社等",
    "有限会社": "株式会社・有限会社等",
    "合同会社": "株式会社・有限会社等",
    "合資会社": "株式会社・有限会社等",
    "合名会社": "株式会社・有限会社等",
    "NPO法人": "NPO法人",
    "特定非営利活動法人": "NPO法人",
    "一般社団法人": "社団法人",
    "公益社団法人": "社団法人",
    "一般財団法人": "財団法人",
    "公益財団法人": "財団法人",
    "地方公共団体": "地方公共団体",
}


def classify_corp_type(corp_name):
    """法人名から種別を判定"""
    if not corp_name:
        return "不明"
    for keyword, corp_type in _CORP_TYPE_MAPPING.items():
        if keyword in corp_name:
            return corp_type
    return "その他法人"


# ============================================================
# 数値パース
# ============================================================


def parse_int(value):
    """文字列を安全に整数に変換。空文字・不正値はNone"""
    if not value or not str(value).strip():
        return None
    try:
        return int(float(str(value).strip()))
    except (ValueError, TypeError):
        return None


def safe_float(value):
    """文字列を安全にfloatに変換。全角数字・％記号にも対応"""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip()
    if not s:
        return None
    # 全角→半角
    s = s.translate(str.maketrans("０１２３４５６７８９．", "0123456789."))
    s = s.replace("％", "").replace("%", "").replace(",", "")
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


# ============================================================
# 派生カラム計算
# ============================================================


def compute_derived(row):
    """CSVの1行から派生カラムを計算

    Returns:
        tuple: (prefecture, corp_type, turnover_rate, fulltime_ratio, years_in_business)
    """
    address = row.get("住所", "")
    corp_name = row.get("法人名", "")
    staff_fulltime = parse_int(row.get("従業者_常勤", ""))
    staff_total = parse_int(row.get("従業者_合計", ""))
    left_last_year = parse_int(row.get("前年度退職数", ""))
    start_date = row.get("事業開始日", "")

    prefecture = extract_prefecture(address)
    corp_type = classify_corp_type(corp_name)

    turnover_rate = None
    if staff_total is not None and left_last_year is not None:
        denom = staff_total + left_last_year
        if denom > 0:
            turnover_rate = round(left_last_year / denom, 4)

    fulltime_ratio = None
    if staff_fulltime is not None and staff_total is not None and staff_total > 0:
        fulltime_ratio = round(staff_fulltime / staff_total, 4)

    current_year = datetime.now().year
    years_in_business = None
    if start_date and str(start_date).strip():
        try:
            year = int(str(start_date).strip().split("/")[0])
            years_in_business = current_year - year
        except (ValueError, IndexError):
            pass

    return prefecture, corp_type, turnover_rate, fulltime_ratio, years_in_business
