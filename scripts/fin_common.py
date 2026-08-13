"""決算PDF解析パイプライン 共通モジュール

環境変数が未設定のとき kaigo-bi-backend/.env から読み込む。
（運用手順では $env: で渡す前提だが、ローカル解析のたびに手で貼るのは事故のもと）
"""
import os
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BACKEND_ENV = ROOT / "kaigo-bi-backend" / ".env"


def load_env(keys=("TURSO_DATABASE_URL", "TURSO_AUTH_TOKEN")) -> None:
    """未設定の環境変数だけを .env から補う（既存の環境変数を上書きしない）"""
    if all(os.environ.get(k) for k in keys):
        return
    if not BACKEND_ENV.exists():
        return
    for line in BACKEND_ENV.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k, v = k.strip(), v.strip().strip('"').strip("'")
        if k in keys and not os.environ.get(k):
            os.environ[k] = v


def turso():
    """(url, headers) を返す"""
    load_env()
    from turso_helpers import get_headers, get_turso_config

    url, token = get_turso_config()
    return url, get_headers(token)


def query(sql, args=None, timeout=180):
    """SELECT を実行して dict のリストで返す"""
    from turso_helpers import execute_single

    url, headers = turso()
    res = execute_single(url, headers, sql, args=args, timeout=timeout)
    r = res["results"][0]["response"]["result"]
    cols = [c["name"] for c in r["cols"]]
    out = []
    for row in r["rows"]:
        out.append({c: (cell.get("value") if isinstance(cell, dict) else None)
                    for c, cell in zip(cols, row)})
    return out


BASE_URL = "https://www.kaigokensaku.mhlw.go.jp"


def to_abs_url(path: str) -> str:
    from urllib.parse import quote

    path = (path or "").strip()
    if not path:
        return ""
    if path.startswith("http"):
        return path
    return BASE_URL + quote(path, safe="/?=&%:")


# ---- 数値正規化 -------------------------------------------------

_NUM_RE = re.compile(r"^[△▲\-\(\)]?[\d,]+(?:\.\d+)?\)?$")


def parse_amount(s):
    """会計数値を int に。△▲()は負、空/記号のみは None"""
    if s is None:
        return None
    t = str(s).strip()
    if not t or t in {"-", "―", "－", "‐", "ー", "" }:
        return None
    t = t.translate(str.maketrans("０１２３４５６７８９，．（）△▲－", "0123456789,.()---"))
    t = t.replace(" ", "").replace("　", "")
    neg = False
    if t.startswith(("-", "△", "▲")):
        neg = True
        t = t.lstrip("-△▲")
    if t.startswith("(") and t.endswith(")"):
        neg = True
        t = t[1:-1]
    t = t.replace(",", "")
    if not t or not re.fullmatch(r"\d+(?:\.\d+)?", t):
        return None
    # 桁が多すぎるものは会計数値ではなく、列が連結した誤読。
    # 実際に96桁の値が出てSQLiteの64bit整数を超え、書き込みが落ちた（2026-08-13）。
    # 介護事業者の決算で 1000兆円(1e15) を超える値は存在しない。
    if len(t.split(".")[0]) > 15:
        return None
    v = int(float(t))
    if abs(v) >= 10**15:
        return None
    return -v if neg else v
