"""位置参照情報で突合できなかった施設を、国土地理院APIで補完する。

scripts/geocode_facilities.py（国交省 位置参照情報との突合）で 91.9% に座標が付いた。
残る約 19,600 件は北海道の郡部・字・線を含む住所や、
「旭川市東光旭川市東光11条5丁目」のように住所自体が壊れているものが中心。

国土地理院のジオコーディングAPIは無料・APIキー不要で、番地レベルまで解決できる。
ただし公的APIなので、既定では 1 件あたり 0.5 秒の間隔を空けて逐次実行する。
19,600 件で約 3 時間かかる想定。--limit で分割実行できる。

    https://msearch.gsi.go.jp/address-search/AddressSearch

使い方:
    python scripts/geocode_fallback_gsi.py --dry-run --limit 20   # 疎通と成功率の確認
    python scripts/geocode_fallback_gsi.py --limit 2000           # 2000件だけ補完
    python scripts/geocode_fallback_gsi.py                        # 残り全件

環境変数: TURSO_DATABASE_URL / TURSO_AUTH_TOKEN
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from geocode_facilities import turso, Z2H  # noqa: E402

API = "https://msearch.gsi.go.jp/address-search/AddressSearch?q="
SLEEP = 0.5
# 日本の範囲。これを外れた結果は採用しない
LAT_RANGE = (20.0, 46.0)
LON_RANGE = (122.0, 154.0)


def clean_address(addr: str, pref: str) -> str:
    """API に渡す住所を整える。

    郵便番号・建物名・部屋番号は誤ヒットの元になるので落とし、
    都道府県が欠けていれば補う。
    """
    s = addr.translate(Z2H)
    s = re.sub(r"〒?\s*\d{3}-?\d{4}", " ", s)
    s = s.replace("　", " ").strip()
    # 「北海道 札幌市…」のように都道府県が重複しても困らないよう一度落として付け直す
    s = re.sub(r"^" + re.escape(pref), "", s).strip()
    if pref and not s.startswith(pref):
        s = pref + s
    # 建物名・部屋番号らしき末尾（空白以降にカナ/英字が続く塊）を除去
    s = re.sub(r"\s+\S*[ァ-ヶA-Za-z].*$", "", s)
    s = re.sub(r"\s*\d+号室.*$", "", s)
    return s.strip()


CITY_RE = re.compile(r"(\S{1,8}?市\S{1,8}?区|\S{1,8}?[市区町村])")
KAN_NUM = str.maketrans("一二三四五六七八九", "123456789")


def extract_city(s: str, pref: str) -> str | None:
    """住所から市区町村を切り出す（都道府県を除いた先頭部分）。"""
    body = re.sub(r"^" + re.escape(pref), "", s).strip() if pref else s
    m = CITY_RE.match(body)
    return m.group(1) if m else None


def norm_city(c: str | None) -> str:
    """市区町村名の比較用に正規化する。

    「阿寒郡鶴居村」と「鶴居村」、「端野町2区」と「端野町二区」は同じ場所を指すので、
    郡名と数字表記の違いで弾かないようにする。
    """
    if not c:
        return ""
    c = re.sub(r"^.{1,6}郡", "", c)          # 郡名を落とす
    c = c.translate(KAN_NUM)                 # 漢数字を算用数字へ
    return c.replace("ヶ", "ケ").replace("ノ", "の")


def geocode(q: str, pref: str):
    """住所を1件引く。ヒットしない、または結果が信用できなければ None。

    国土地理院APIは市区町村が欠けた曖昧な入力にも「それらしい」座標を返す。
    実際 dry-run で「北海道文京台東町11-24」（江別市）が札幌中心部に解決された。
    返却された正規化住所(title)の市区町村が入力と食い違う場合は採用しない。
    """
    try:
        with urllib.request.urlopen(API + urllib.parse.quote(q), timeout=20) as r:
            data = json.loads(r.read())
    except Exception:                                # noqa: BLE001
        return None, "api_error"
    if not data:
        return None, "no_hit"
    try:
        lon, lat = data[0]["geometry"]["coordinates"]
        title = str(data[0]["properties"].get("title", ""))
    except (KeyError, IndexError, TypeError, ValueError):
        return None, "bad_response"
    if not (LAT_RANGE[0] <= lat <= LAT_RANGE[1] and LON_RANGE[0] <= lon <= LON_RANGE[1]):
        return None, "out_of_japan"

    # 入力に市区町村が書かれているなら、結果の市区町村と一致することを求める
    in_city = extract_city(q, pref)
    out_city = extract_city(title, pref)
    if in_city and out_city and norm_city(in_city) != norm_city(out_city):
        return None, f"city_mismatch({in_city}≠{out_city})"
    # 入力に市区町村が無い住所は、どこにでも寄せられてしまうので採用しない
    if not in_city:
        return None, "no_city_in_input"

    return (float(lat), float(lon)), "ok"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--limit", type=int, default=0, help="処理件数の上限（0=全件）")
    ap.add_argument("--dry-run", action="store_true", help="DBを更新せず結果だけ表示")
    ap.add_argument("--sleep", type=float, default=SLEEP, help="1件あたりの待機秒")
    a = ap.parse_args()

    run = turso()
    limit_sql = f"LIMIT {a.limit}" if a.limit > 0 else ""
    rows = run([
        f'SELECT "事業所番号", prefecture, "住所" FROM facilities '
        f'WHERE latitude IS NULL AND COALESCE("住所", \'\') <> \'\' '
        f'ORDER BY "事業所番号" {limit_sql}'
    ])[0]
    print(f"未ジオコーディング: {len(rows):,} 件を処理します"
          f"（{a.sleep}秒間隔 / 推定 {len(rows) * a.sleep / 60:.0f} 分）")

    ok, ng, updates = 0, 0, []
    reasons: dict[str, int] = {}
    for i, (jno, pref, addr) in enumerate(rows, 1):
        pref = pref or ""
        q = clean_address(addr, pref)
        coord, why = geocode(q, pref)
        if coord:
            ok += 1
            updates.append((jno, coord[0], coord[1]))
            if a.dry_run and ok <= 10:
                print(f"  OK  {q[:38]:38} -> {coord[0]:.5f}, {coord[1]:.5f}")
        else:
            ng += 1
            key = why.split("(")[0]
            reasons[key] = reasons.get(key, 0) + 1
            if a.dry_run and ng <= 8:
                print(f"  --  {q[:38]:38} -> {why}")

        # 一定量たまったらまとめて反映（途中で止めても進捗が残る）
        if not a.dry_run and len(updates) >= 200:
            run([
                f"UPDATE facilities SET latitude={lat}, longitude={lon} "
                f"WHERE \"事業所番号\" = '{j.replace(chr(39), chr(39) * 2)}'"
                for j, lat, lon in updates
            ])
            updates.clear()

        if i % 50 == 0:
            print(f"  {i:,}/{len(rows):,}  成功 {ok:,} / 失敗 {ng:,}", end="\r")
        time.sleep(a.sleep)

    if not a.dry_run and updates:
        run([
            f"UPDATE facilities SET latitude={lat}, longitude={lon} "
            f"WHERE \"事業所番号\" = '{j.replace(chr(39), chr(39) * 2)}'"
            for j, lat, lon in updates
        ])

    print(f"\n完了: 成功 {ok:,} / 失敗 {ng:,}"
          f"（成功率 {ok / max(len(rows), 1) * 100:.1f}%）")
    if a.dry_run:
        print("※ --dry-run のため DB は更新していません")
    else:
        r = run(["SELECT COUNT(*), COUNT(latitude) FROM facilities"])[0][0]
        print(f"座標あり: {int(r[1]):,} / {int(r[0]):,} "
              f"({int(r[1]) / int(r[0]) * 100:.1f}%)")
        print("※ 座標を更新したら facility_metrics の再作成は不要ですが、"
              "周辺検索の索引は自動で追随します")


if __name__ == "__main__":
    main()
