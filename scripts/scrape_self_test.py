"""自分のAPIを実際にスクレイピングして、検知できるかを確かめる

これは **自社システムに対する許可された検証** であり、攻撃者の視点で
「ログインさえすれば何ができてしまうか」を実証するためのもの。

想定する攻撃者:
  正規のアカウントを1つ持っている競合。画面を使うふりをせず、
  APIを直接ページ送りして施設データを抜く。

実行:
  python scripts/scrape_self_test.py --pages 30
  python scripts/scrape_self_test.py --pages 30 --prefecture 東京都
  python scripts/scrape_self_test.py --pages 5 --delay 0     # 全力

出力: data/output/scrape_test/harvested.csv（抜けたデータの証拠）
      と、要したリクエスト数・時間・バイト数
"""
import argparse
import csv
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

import jwt

from fin_common import ROOT, load_env

API = os.environ.get("SCRAPE_TARGET", "http://127.0.0.1:3001")
OUT = ROOT / "data" / "output" / "scrape_test"


def make_token() -> str:
    """検証用トークン。実際の攻撃者は正規にログインして同じものを得る"""
    load_env(("JWT_SECRET",))
    now = int(time.time())
    return jwt.encode(
        {"sub": "admin-001", "email": "testadmin@test.com", "name": "管理",
         "role": "admin", "plan": "ma", "exp": now + 7200, "iat": now},
        os.environ["JWT_SECRET"], algorithm="HS256",
    )


def fetch(token: str, path: str, params: dict):
    url = f"{API}{path}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {token}",
        # 攻撃者はブラウザに偽装する。UAだけでは判別できないことを示す
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    })
    t = time.time()
    with urllib.request.urlopen(req, timeout=300) as resp:
        raw = resp.read()
    return json.loads(raw.decode("utf-8")), len(raw), time.time() - t


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pages", type=int, default=30)
    ap.add_argument("--per-page", type=int, default=100)
    ap.add_argument("--prefecture")
    ap.add_argument("--delay", type=float, default=0.0)
    args = ap.parse_args()

    OUT.mkdir(parents=True, exist_ok=True)
    token = make_token()

    rows, total_bytes, reqs = [], 0, 0
    started = time.time()
    total_available = None

    print(f"標的: {API}  1ページ{args.per_page}件 × {args.pages}ページ")
    print(f"{'page':>5} {'件数':>5} {'バイト':>9} {'秒':>6}  累計件数")
    for page in range(1, args.pages + 1):
        params = {"per_page": args.per_page, "page": page,
                  "sort_by": "jigyosho_name", "sort_order": "asc"}
        if args.prefecture:
            params["prefecture"] = args.prefecture
        try:
            data, nbytes, elapsed = fetch(token, "/api/facilities/search", params)
        except urllib.error.HTTPError as e:
            print(f"{page:>5}  HTTP {e.code} で停止: {e.read()[:120]!r}")
            break
        except Exception as e:  # noqa: BLE001
            print(f"{page:>5}  {type(e).__name__} で停止: {e}")
            break

        reqs += 1
        total_bytes += nbytes
        items = data.get("items", [])
        total_available = data.get("total", total_available)
        for it in items:
            rows.append({
                "jigyosho_number": it.get("jigyosho_number"),
                "jigyosho_name": it.get("jigyosho_name"),
                "corp_name": it.get("corp_name"),
                "phone": it.get("phone"),
                "address": it.get("address"),
                "staff_total": it.get("staff_total"),
                "capacity": it.get("capacity"),
            })
        print(f"{page:>5} {len(items):>5} {nbytes:>9,} {elapsed:>6.1f}  {len(rows):>7,}")
        if not items:
            print("  空ページ。終了")
            break
        if args.delay:
            time.sleep(args.delay)

    took = time.time() - started
    path = OUT / "harvested.csv"
    if rows:
        with open(path, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)

    print("\n=== 結果 ===")
    print(f"リクエスト数 : {reqs:,}")
    print(f"取得件数     : {len(rows):,}")
    print(f"取得バイト数 : {total_bytes:,} ({total_bytes/1024/1024:.1f}MB)")
    print(f"所要時間     : {took:.1f}秒")
    if total_available:
        pace = len(rows) / took if took else 0
        remain = int(total_available) - len(rows)
        print(f"母集団       : {int(total_available):,} 件")
        print(f"取得率       : {len(rows)/int(total_available)*100:.2f}%")
        if pace > 0:
            print(f"全件までの推定: 残り{remain:,}件 / このペースで約{remain/pace/60:.0f}分")
    if rows:
        print(f"証拠         : {path}")
        print(f"  例: {rows[0]['jigyosho_name']} / {rows[0]['corp_name']} / {rows[0]['phone']}")


if __name__ == "__main__":
    main()
