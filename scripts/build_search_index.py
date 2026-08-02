"""施設名・法人名・電話番号の全文検索インデックス（FTS5 trigram）を構築する。

背景:
    施設検索は `"事業所名" LIKE '%キーワード%' OR "法人名" LIKE ... OR "電話番号" LIKE ...`
    で実装されていた。223,103行に対する全表スキャンとなり、実測で 1 クエリ 20〜29 秒。
    総件数の COUNT と本体で 2 回走るため、体感は 57 秒に達していた。

    LIKE の前方一致でない検索には通常のインデックスが効かないため、
    FTS5 の trigram トークナイザ（3文字単位。日本語の部分一致に対応）で索引を張る。

使い方:
    python scripts/build_search_index.py --build     # 作成 + データ投入
    python scripts/build_search_index.py --refresh   # 作り直し（データ更新後）
    python scripts/build_search_index.py --check     # 件数と速度を確認

環境変数: TURSO_DATABASE_URL / TURSO_AUTH_TOKEN
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from geocode_facilities import turso  # noqa: E402

TABLE = "facilities_fts"
PAGE = 5000


def esc(s: str) -> str:
    return s.replace("'", "''")


def build(run, drop_first: bool) -> None:
    if drop_first:
        run([f"DROP TABLE IF EXISTS {TABLE}"])
        print(f"既存の {TABLE} を削除")

    run([
        f"CREATE VIRTUAL TABLE IF NOT EXISTS {TABLE} USING fts5("
        f"  jigyosho_number UNINDEXED,"
        f"  jigyosho_name,"
        f"  corp_name,"
        f"  phone,"
        f"  tokenize='trigram'"
        f")"
    ])
    print(f"{TABLE} を作成")

    existing = int(run([f"SELECT COUNT(*) FROM {TABLE}"])[0][0][0])
    if existing:
        print(f"既に {existing:,} 行あります。作り直すには --refresh を使ってください")
        return

    total = int(run(['SELECT COUNT(*) FROM facilities'])[0][0][0])
    print(f"投入対象 {total:,} 行")

    done, offset = 0, 0
    while True:
        rows = run([
            f'SELECT "事業所番号", "事業所名", COALESCE("法人名",\'\'), COALESCE("電話番号",\'\') '
            f'FROM facilities ORDER BY rowid LIMIT {PAGE} OFFSET {offset}'
        ])[0]
        if not rows:
            break
        values = ",".join(
            "('{}','{}','{}','{}')".format(esc(r[0] or ""), esc(r[1] or ""),
                                           esc(r[2] or ""), esc(r[3] or ""))
            for r in rows
        )
        run([f"INSERT INTO {TABLE}"
             f"(jigyosho_number, jigyosho_name, corp_name, phone) VALUES {values}"])
        done += len(rows)
        offset += PAGE
        print(f"  投入 {done:,}/{total:,}", end="\r")
    print(f"\n完了: {done:,} 行")


def check(run) -> None:
    n = int(run([f"SELECT COUNT(*) FROM {TABLE}"])[0][0][0])
    print(f"{TABLE}: {n:,} 行")
    for kw in ("ライフプラザ鶴巻", "さくら", "特別養護老人ホーム"):
        t = time.time()
        rows = run([
            f"SELECT jigyosho_number, jigyosho_name FROM {TABLE} "
            f"WHERE {TABLE} MATCH '\"{esc(kw)}\"' LIMIT 10"
        ])[0]
        el = time.time() - t
        print(f"  MATCH '{kw}': {len(rows)}件 / {el:.2f}秒")
        for r in rows[:2]:
            print(f"      {r[1][:44]}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--build", action="store_true")
    ap.add_argument("--refresh", action="store_true")
    ap.add_argument("--check", action="store_true")
    a = ap.parse_args()
    if not (a.build or a.refresh or a.check):
        ap.print_help()
        return
    run = turso()
    if a.build or a.refresh:
        build(run, drop_first=a.refresh)
    if a.check:
        check(run)


if __name__ == "__main__":
    main()
