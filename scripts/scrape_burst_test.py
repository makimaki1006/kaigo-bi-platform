"""バースト型のスクレイピング検証

直列だと1リクエスト20〜30秒かかり時間がかかる。攻撃者は並列で回すので、
それを再現してレート制限が発火するかを短時間で見る。

実行: python scripts/scrape_burst_test.py --requests 40 --parallel 8
"""
import argparse
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

import jwt

from fin_common import load_env

API = os.environ.get("SCRAPE_TARGET", "http://127.0.0.1:3001")


def make_token():
    load_env(("JWT_SECRET",))
    now = int(time.time())
    return jwt.encode(
        {"sub": "admin-001", "email": "testadmin@test.com", "name": "管理",
         "role": "admin", "plan": "ma", "exp": now + 7200, "iat": now},
        os.environ["JWT_SECRET"], algorithm="HS256")


def hit(token, page):
    params = {"per_page": 100, "page": page, "sort_by": "jigyosho_name", "sort_order": "asc"}
    url = f"{API}/api/facilities/search?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    t = time.time()
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            return page, r.status, len(r.read()), time.time() - t
    except urllib.error.HTTPError as e:
        body = e.read()[:80]
        return page, e.code, len(body), time.time() - t
    except Exception as e:  # noqa: BLE001
        return page, f"ERR:{type(e).__name__}", 0, time.time() - t


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--requests", type=int, default=40)
    ap.add_argument("--parallel", type=int, default=8)
    args = ap.parse_args()
    token = make_token()

    print(f"標的 {API}  {args.requests}リクエストを並列{args.parallel}で発射")
    started = time.time()
    statuses = Counter()
    ok_rows = 0
    results = []
    with ThreadPoolExecutor(max_workers=args.parallel) as pool:
        futs = [pool.submit(hit, token, p) for p in range(1, args.requests + 1)]
        for f in as_completed(futs):
            page, status, nbytes, el = f.result()
            statuses[status] += 1
            if status == 200:
                ok_rows += 100
            results.append((page, status, nbytes, el))

    took = time.time() - started
    print(f"\n所要 {took:.1f}秒")
    print("ステータス内訳:")
    for st, c in sorted(statuses.items(), key=lambda x: str(x[0])):
        label = {200: "成功", 429: "レート制限で拒否"}.get(st, str(st))
        print(f"  {st} ({label}): {c} 件")
    print(f"\n成功したリクエストで取得できた最大件数: 約{ok_rows:,}件")
    blocked = statuses.get(429, 0)
    if blocked:
        print(f"→ {blocked}件を429で遮断。全件取得は成立しない")
    else:
        print("→ 1件も遮断されていない。素通し")


if __name__ == "__main__":
    main()
