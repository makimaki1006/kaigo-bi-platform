"""APIの利用状況を確認する（大量取得の検知）

api_access_logs を集計して、アカウントごとの取得量と挙動を出す。
管理画面を作る前の暫定手段。SQLを書かずに様子を見られるようにするのが目的。

実行:
  python scripts/check_api_usage.py            # 直近7日
  python scripts/check_api_usage.py --days 1   # 直近1日
  python scripts/check_api_usage.py --user <user_id>   # 特定アカウントの明細

見るべきポイント:
  - 取得バイト数が突出しているアカウント
  - 検索APIの呼び出し回数（ページを機械的に回していないか）
  - 深いページ番号（page=200 のような人が押さない操作）
  - IPやUser-Agentの数（アカウント共有・自動化の兆候）
  - 深夜帯の比率
"""
import argparse
import re
from collections import Counter

from fin_common import query


def mb(v):
    return f"{(v or 0) / 1024 / 1024:.1f}MB"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--user")
    args = ap.parse_args()
    since = f"datetime('now', '-{args.days} days')"

    total = query(f"SELECT COUNT(*) n FROM api_access_logs WHERE created_at >= {since}",
                  timeout=180)[0]
    print(f"=== 直近{args.days}日のAPIアクセス: {int(total['n']):,} 件 ===")
    if int(total["n"]) == 0:
        print("記録がありません。バックエンドが新しいビルドで動いているか確認してください。")
        return

    # ------------------------------------------------------------------
    # 自動判定: 通常の画面利用から外れたアカウントを先頭に出す。
    # 閾値は暫定。数日ログを溜めて正常水準が見えたら調整する。
    print("\n--- ⚠ 要注意アカウント（自動判定） ---")
    rows = query(f"""
        SELECT email, user_id,
               COUNT(*) reqs,
               SUM(CASE WHEN response_bytes > 0 THEN response_bytes ELSE 0 END) bytes,
               SUM(CASE WHEN status = 429 THEN 1 ELSE 0 END) blocked,
               SUM(CASE WHEN path LIKE '%/facilities/search%' THEN 1 ELSE 0 END) searches,
               COUNT(DISTINCT ip) ips
        FROM api_access_logs WHERE created_at >= {since}
        GROUP BY user_id
    """, timeout=300)
    flagged = []
    for r in rows:
        reqs, bytes_, blocked = int(r["reqs"]), int(r["bytes"] or 0), int(r["blocked"])
        searches, ips = int(r["searches"]), int(r["ips"])
        reasons = []
        # 1日あたりの目安。通常の画面利用は1セッション数十〜数百リクエスト
        if reqs / max(args.days, 1) > 500:
            reasons.append(f"リクエスト過多({reqs:,})")
        if bytes_ / 1024 / 1024 / max(args.days, 1) > 20:
            reasons.append(f"取得量過多({mb(bytes_)})")
        if blocked > 0:
            reasons.append(f"レート制限に到達({blocked}回)")
        if searches / max(args.days, 1) > 100:
            reasons.append(f"検索連打({searches:,})")
        if ips > 3:
            reasons.append(f"IP分散({ips})")
        if reasons:
            flagged.append((r["email"] or r["user_id"], reasons))
    if flagged:
        for email, reasons in flagged:
            print(f"  🔴 {str(email)[:30]:32s} {' / '.join(reasons)}")
    else:
        print("  なし（全アカウントが通常利用の範囲内）")

    # ------------------------------------------------------------------
    print("\n--- アカウント別（取得バイト数の多い順） ---")
    rows = query(f"""
        SELECT user_id, email, plan,
               COUNT(*) reqs,
               SUM(CASE WHEN response_bytes > 0 THEN response_bytes ELSE 0 END) bytes,
               COUNT(DISTINCT ip) ips,
               COUNT(DISTINCT user_agent) uas,
               SUM(CASE WHEN path LIKE '%/facilities/search%' THEN 1 ELSE 0 END) searches,
               SUM(CASE WHEN CAST(strftime('%H', created_at) AS INTEGER) BETWEEN 0 AND 5
                        THEN 1 ELSE 0 END) night,
               MAX(created_at) last_seen
        FROM api_access_logs WHERE created_at >= {since}
        GROUP BY user_id ORDER BY bytes DESC LIMIT 30
    """, timeout=300)
    print(f"{'メール':26s}{'プラン':7s}{'回数':>7s}{'取得量':>9s}{'検索':>6s}{'IP':>4s}{'UA':>4s}{'深夜':>5s}  最終")
    for r in rows:
        print(f"{str(r['email'] or r['user_id'])[:24]:26s}{str(r['plan'])[:5]:7s}"
              f"{int(r['reqs']):>7,}{mb(int(r['bytes'])):>9s}{int(r['searches']):>6,}"
              f"{int(r['ips']):>4}{int(r['uas']):>4}{int(r['night']):>5}  {r['last_seen'][:16]}")

    # ------------------------------------------------------------------
    print("\n--- よく叩かれているAPI ---")
    rows = query(f"""
        SELECT path, COUNT(*) n,
               SUM(CASE WHEN response_bytes > 0 THEN response_bytes ELSE 0 END) bytes,
               CAST(AVG(duration_ms) AS INTEGER) avg_ms
        FROM api_access_logs WHERE created_at >= {since}
        GROUP BY path ORDER BY n DESC LIMIT 12
    """, timeout=300)
    for r in rows:
        print(f"  {str(r['path'])[:52]:54s}{int(r['n']):>7,}回 {mb(int(r['bytes'])):>9s} "
              f"平均{int(r['avg_ms']):>5}ms")

    # ------------------------------------------------------------------
    print("\n--- 深いページ番号の要求（機械的なページ送りの兆候） ---")
    rows = query(f"""
        SELECT email, query, created_at FROM api_access_logs
        WHERE created_at >= {since} AND query LIKE '%page=%'
        ORDER BY created_at DESC LIMIT 500
    """, timeout=300)
    deep = Counter()
    for r in rows:
        m = re.search(r"[?&]page=(\d+)", r["query"] or "")
        if m and int(m.group(1)) >= 20:
            deep[r["email"]] += 1
    if deep:
        for email, c in deep.most_common(10):
            print(f"  {str(email)[:30]:32s} page>=20 を {c} 回")
    else:
        print("  該当なし")

    # ------------------------------------------------------------------
    if args.user:
        print(f"\n--- {args.user} の明細（直近50件） ---")
        rows = query(f"""
            SELECT created_at, method, path, query, status, response_bytes, duration_ms, ip
            FROM api_access_logs
            WHERE user_id = ? AND created_at >= {since}
            ORDER BY created_at DESC LIMIT 50
        """, args=[{"type": "text", "value": args.user}], timeout=300)
        for r in rows:
            q = f"?{r['query']}" if r["query"] else ""
            print(f"  {r['created_at'][:19]} {r['method']:4s} {r['path']}{q}"[:110])
            print(f"     {r['status']} / {int(r['response_bytes'] or 0):,}B / "
                  f"{r['duration_ms']}ms / {r['ip']}")


if __name__ == "__main__":
    main()
