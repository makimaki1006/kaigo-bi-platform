"""facilities.住所 に緯度経度を付与する。

データ源: 国土交通省「位置参照情報（大字・町丁目レベル）」
  https://nlftp.mlit.go.jp/isj/
  出典表示のみで商用利用可。APIキー・課金なし。

精度は町丁目レベル（同一町丁目の施設は同じ座標になる）。
「選んだ施設を中心に周辺施設を見る」用途には十分だが、
番地単位の正確な位置が要る用途には使えない。

使い方:
    # 1. 位置参照情報を取得（初回のみ、47ファイル/約3MB）
    python scripts/geocode_facilities.py --download

    # 2. 突合してCSV出力（DBは変更しない）
    python scripts/geocode_facilities.py --match

    # 3. Tursoへ反映（latitude/longitude カラムを作成して更新）
    python scripts/geocode_facilities.py --apply

環境変数: TURSO_DATABASE_URL / TURSO_AUTH_TOKEN
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
import os
import re
import sys
import urllib.request
import zipfile
from pathlib import Path

ISJ_VERSION = "19.0b"
ISJ_URL = "https://nlftp.mlit.go.jp/isj/dls/data/{v}/{code}000-{v}.zip"

REPO_ROOT = Path(__file__).resolve().parent.parent
ISJ_DIR = REPO_ROOT / "data" / "isj"
OUT_CSV = REPO_ROOT / "data" / "facility_coordinates.csv"

KAN = "〇一二三四五六七八九"
Z2H = str.maketrans("０１２３４５６７８９－―‐−", "0123456789----")


# ---------------------------------------------------------------- 住所正規化

def to_kansuji(n: int) -> str:
    """1..999 を一般的な漢数字表記にする。"""
    if n <= 0 or n > 999:
        return str(n)
    if n < 10:
        return KAN[n]
    if n < 100:
        t, o = divmod(n, 10)
        return ("十" if t == 1 else KAN[t] + "十") + (KAN[o] if o else "")
    h, r = divmod(n, 100)
    return ("百" if h == 1 else KAN[h] + "百") + (to_kansuji(r) if r else "")


def num_to_kan(text: str) -> str:
    """「14条」「1丁目」等のアラビア数字を漢数字へ寄せる。"""
    return re.sub(r"(\d+)\s*(丁目|条|番町|軒|線)",
                  lambda m: to_kansuji(int(m.group(1))) + m.group(2), text)


def canon(s: str) -> str:
    """異体字・表記揺れを吸収する。"""
    return (s.replace("ヶ", "ケ").replace("ｹ", "ケ")
             .replace("ノ", "の").replace("之", "の")
             .replace("﨑", "崎").replace("邊", "辺").replace("澤", "沢")
             .replace("檜", "桧").replace("藏", "蔵"))


def address_candidates(addr: str, pref_hint: str):
    """住所から (都道府県, 市区町村, 大字町丁目候補) を返す。

    「南16条西7-2-20」のように丁目が省略され番地とハイフンで繋がる表記や、
    市区町村ごと省略された表記が実データに多いため、候補を複数出して
    実在するものを後段で選ばせる。
    """
    s = addr.translate(Z2H)
    s = re.sub(r"〒?\s*\d{3}-?\d{4}", " ", s)
    s = s.replace("　", " ").strip()

    m = re.match(r"^(北海道|東京都|(?:京都|大阪)府|.{2,3}県)", s)
    if m:
        pref = m.group(1)
        rest = s[len(pref):]
    else:
        pref = pref_hint
        rest = re.sub(r"^" + re.escape(pref_hint), "", s)
    rest = rest.strip()

    m = re.match(r"^(\S{1,8}?市\S{1,8}?区|\S{1,8}?[市区町村])", rest)
    if m:
        city = m.group(1)
        rest = rest[len(city):].strip()
    else:
        city = ""          # 市区町村が省略された住所 → 町丁目からの逆引きに任せる
    rest = num_to_kan(rest)

    bases = []
    m = re.match(r"^(?:大字)?([^\d\s]{1,20}?[一二三四五六七八九十百]+丁目)", rest)
    if m:
        bases.append(m.group(1))
    m = re.match(r"^(?:大字)?([^\d\s]{1,20}?)(\d{1,2})(?!\d)", rest)
    if m:
        bases.append(m.group(1) + to_kansuji(int(m.group(2))) + "丁目")
        bases.append(m.group(1))
    m = re.match(r"^(?:大字)?([^\d\s]{1,20})", rest)
    if m:
        bases.append(m.group(1))

    # ISJ側の表記揺れを吸収する派生候補:
    #   ・「大字中判田」のように ISJ が大字付きで持っている
    #   ・「渡利字中江町」の 字以降は ISJ に無く「渡利」で持っている
    #   ・「田中町は」の末尾1文字は字符号で、ISJ は「田中町」
    cands = []
    for b in bases:
        for v in (b, "大字" + b):
            cands.append(v)
        if "字" in b:
            head = b.split("字", 1)[0]
            if head:
                cands.append(head)
                cands.append("大字" + head)
        m2 = re.match(r"^(.{2,}?)[ぁ-んァ-ン甲乙丙丁]$", b)
        if m2:
            cands.append(m2.group(1))
            cands.append("大字" + m2.group(1))

    seen, out = set(), []
    for c in cands:
        if c and c not in seen:
            seen.add(c)
            out.append(c)
    return pref, city, out


# ---------------------------------------------------------------- 位置参照情報

def download_isj() -> None:
    ISJ_DIR.mkdir(parents=True, exist_ok=True)
    got = 0
    for i in range(1, 48):
        dest = ISJ_DIR / f"{i:02d}000.zip"
        if dest.exists() and dest.stat().st_size > 0:
            got += 1
            continue
        url = ISJ_URL.format(v=ISJ_VERSION, code=f"{i:02d}")
        try:
            urllib.request.urlretrieve(url, dest)
            got += 1
        except Exception as e:                       # noqa: BLE001
            print(f"  取得失敗 {i:02d}: {e}", file=sys.stderr)
    print(f"位置参照情報: {got}/47 取得済み -> {ISJ_DIR}")


def load_isj() -> dict:
    table = {}
    for zp in sorted(glob.glob(str(ISJ_DIR / "*.zip"))):
        try:
            z = zipfile.ZipFile(zp)
        except zipfile.BadZipFile:
            print(f"  破損: {zp}", file=sys.stderr)
            continue
        names = [x for x in z.namelist() if x.lower().endswith(".csv")]
        if not names:
            continue
        txt = z.read(names[0]).decode("cp932", errors="replace")
        for line in txt.strip().split("\n")[1:]:
            p = [c.strip('"') for c in line.strip().split(",")]
            if len(p) < 8:
                continue
            try:
                table[(p[1], p[3], p[5])] = (float(p[6]), float(p[7]))
            except ValueError:
                continue
    return table


def build_indexes(isj: dict):
    canon_idx, by_town, by_city_town = {}, {}, {}
    for (p, c, t), v in isj.items():
        canon_idx[(canon(p), canon(c), canon(t))] = v
        by_town.setdefault((canon(p), canon(t)), []).append((c, v))
        by_city_town.setdefault((canon(c), canon(t)), []).append((p, v))
    return canon_idx, by_town, by_city_town


def lookup(isj, canon_idx, by_town, by_city_town, pref, city, cands):
    """完全一致 → 異体字正規化 → (都道府県, 町丁目) 逆引き の順に引く。

    都道府県が空の住所（実データに約3千件ある）は (市区町村, 町丁目) から
    都道府県を逆引きする。いずれの逆引きも一意に決まらない場合は採用しない
    （誤った場所に施設を置くより、座標なしのまま残す方が安全）。
    """
    cc = canon(city)
    if pref:
        for t in cands:
            v = isj.get((pref, city, t))
            if v:
                return v, "exact"
        cp = canon(pref)
        for t in cands:
            v = canon_idx.get((cp, cc, canon(t)))
            if v:
                return v, "canon"
        for t in cands:
            hits = by_town.get((cp, canon(t)))
            if not hits:
                continue
            if len(hits) == 1:
                return hits[0][1], "by_town"
            narrowed = [h for h in hits
                        if cc and (canon(h[0]).startswith(cc) or cc.startswith(canon(h[0])))]
            if len(narrowed) == 1:
                return narrowed[0][1], "by_town_narrowed"
        return None, "miss"

    for t in cands:
        hits = by_city_town.get((cc, canon(t)))
        if hits and len(hits) == 1:
            return hits[0][1], "by_city_town"
    return None, "miss_nopref"


# ---------------------------------------------------------------- Turso

def turso():
    url = os.environ.get("TURSO_DATABASE_URL")
    token = os.environ.get("TURSO_AUTH_TOKEN")
    if not url or not token:
        raise SystemExit("TURSO_DATABASE_URL / TURSO_AUTH_TOKEN が未設定です")
    endpoint = re.sub(r"^libsql://", "https://", url).rstrip("/") + "/v2/pipeline"

    def run(stmts):
        reqs = [{"type": "execute", "stmt": {"sql": s}} for s in stmts]
        reqs.append({"type": "close"})
        body = json.dumps({"requests": reqs}).encode()
        req = urllib.request.Request(endpoint, data=body, headers={
            "Authorization": f"Bearer {token}", "Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=120) as r:
            data = json.loads(r.read())
        out = []
        for res in data["results"]:
            if res.get("type") != "ok":
                raise RuntimeError(res.get("error"))
            rr = res.get("response", {}).get("result")
            out.append([[c.get("value") for c in row] for row in rr["rows"]] if rr else None)
        return out

    return run


def fetch_addresses(run):
    rows, offset, page = [], 0, 20000
    while True:
        got = run([
            f'SELECT "事業所番号", prefecture, "住所" FROM facilities '
            f'WHERE COALESCE("住所",\'\') <> \'\' '
            f'ORDER BY "事業所番号" LIMIT {page} OFFSET {offset}'
        ])[0]
        if not got:
            break
        rows.extend(got)
        offset += page
        print(f"  取得 {len(rows):,} 件", end="\r")
    print(f"  取得 {len(rows):,} 件")
    return rows


# ---------------------------------------------------------------- コマンド

def cmd_match(write_csv=True):
    isj = load_isj()
    if not isj:
        raise SystemExit("位置参照情報がありません。先に --download を実行してください")
    print(f"位置参照情報: {len(isj):,} 町丁目")
    canon_idx, by_town, by_city_town = build_indexes(isj)

    run = turso()
    rows = fetch_addresses(run)

    results, stats = [], {}
    for jno, pref, addr in rows:
        pref = pref or ""
        p, c, cands = address_candidates(addr, pref)
        coord, how = lookup(isj, canon_idx, by_town, by_city_town, p, c, cands)
        stats[how] = stats.get(how, 0) + 1
        if coord:
            results.append((jno, coord[0], coord[1]))

    n = len(rows)
    print(f"\n対象 {n:,} 件")
    for k, v in sorted(stats.items(), key=lambda x: -x[1]):
        print(f"  {k:18} {v:7,}  ({v / n * 100:5.1f}%)")
    print(f"座標付与 {len(results):,} 件 ({len(results) / n * 100:.1f}%)")

    if write_csv:
        OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
        with open(OUT_CSV, "w", encoding="utf-8", newline="") as f:
            wr = csv.writer(f)
            wr.writerow(["事業所番号", "latitude", "longitude"])
            wr.writerows(results)
        print(f"出力: {OUT_CSV}")
    return results


def cmd_apply():
    if not OUT_CSV.exists():
        raise SystemExit(f"{OUT_CSV} がありません。先に --match を実行してください")
    run = turso()

    cols = {r[1] for r in run(["PRAGMA table_info(facilities)"])[0]}
    ddl = []
    if "latitude" not in cols:
        ddl.append("ALTER TABLE facilities ADD COLUMN latitude REAL")
    if "longitude" not in cols:
        ddl.append("ALTER TABLE facilities ADD COLUMN longitude REAL")
    if ddl:
        run(ddl)
        print(f"カラム追加: {ddl}")

    with open(OUT_CSV, encoding="utf-8") as f:
        rows = list(csv.reader(f))[1:]
    print(f"更新対象 {len(rows):,} 件")

    done, batch = 0, 400
    for i in range(0, len(rows), batch):
        stmts = [
            f'UPDATE facilities SET latitude={lat}, longitude={lon} '
            f'WHERE "事業所番号" = \'{jno.replace(chr(39), chr(39) * 2)}\''
            for jno, lat, lon in rows[i:i + batch]
        ]
        run(stmts)
        done += len(stmts)
        print(f"  更新 {done:,}/{len(rows):,}", end="\r")
    print(f"\n完了: {done:,} 件")

    chk = run(["SELECT COUNT(*), COUNT(latitude) FROM facilities"])[0][0]
    print(f"検証: 全 {int(chk[0]):,} 件中 座標あり {int(chk[1]):,} 件")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--download", action="store_true", help="位置参照情報を取得")
    ap.add_argument("--match", action="store_true", help="突合してCSV出力（DBは変更しない）")
    ap.add_argument("--apply", action="store_true", help="CSVをTursoへ反映")
    a = ap.parse_args()
    if not (a.download or a.match or a.apply):
        ap.print_help()
        return
    if a.download:
        download_isj()
    if a.match:
        cmd_match()
    if a.apply:
        cmd_apply()


if __name__ == "__main__":
    main()
