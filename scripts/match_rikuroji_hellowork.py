"""
リクロジ アポ先 × ハローワーク 電話番号マッチング

電話番号の正規化→突合→ハローワーク情報をExcelに追加
"""

import io
import os
import re
import sys
import time
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import pandas as pd

# ハローワークCSVから取得するカラム
HW_USE_COLS = [
    '求人番号', '事業所番号', '事業所名漢字', '事業所名カナ',
    '事業所郵便番号', '事業所所在地', '事業所ホームページ',
    '選考担当者ＴＥＬ', '選考担当者ＦＡＸ',
    '産業分類（名称）', '職種', '雇用形態',
    '従業員数企業全体', '従業員数就業場所',
    '創業設立年（西暦）', '資本金',
    '代表者役職', '代表者名', '法人番号',
    '選考担当者課係名／役職名', '選考担当者氏名漢字',
    '賃金', '年間休日数',
    '受付年月日（西暦）', '求人有効年月日（西暦）',
]

# Excel出力時に追加するハローワーク列名プレフィックス
HW_PREFIX = "HW_"


def normalize_phone(phone: str) -> list[str]:
    """
    電話番号を正規化して複数の候補を返す。

    対応パターン:
    - ハイフン付き: 078-306-6551 → 0783066551
    - 81プレフィックス: 810227122920 → 0227122920
    - 全角数字: ０７８ → 078
    - スペース・括弧: (078) 306 6551 → 0783066551
    - +81: +81-78-306-6551 → 0783066551
    - 先頭0欠落: 8099456595 → 08099456595 (9-10桁で0始まりでない)
    - 全角ハイフン: ０９０‐３９０９‐００２４ → 09039090024
    """
    if pd.isna(phone) or str(phone).strip() == '':
        return []

    phone = str(phone).strip()

    # 「後日」等の非電話番号を除外
    if not any(c.isdigit() or c in '０１２３４５６７８９' for c in phone):
        return []

    # 全角→半角
    phone = phone.translate(str.maketrans(
        '０１２３４５６７８９（）ー−‐＋',
        '0123456789()---+',
    ))

    # 数字以外を除去
    digits = re.sub(r'[^0-9]', '', phone)

    if not digits or len(digits) < 9:
        return []

    results = set()

    # 81プレフィックス処理
    if digits.startswith('81') and len(digits) >= 11:
        after81 = digits[2:]
        # 81の後に0がある場合（810XXXXXXXXX）はそのまま
        if after81.startswith('0'):
            domestic = after81
        else:
            domestic = '0' + after81
        if 10 <= len(domestic) <= 11:
            results.add(domestic)

    # 0始まりの国内番号
    if digits.startswith('0') and 10 <= len(digits) <= 11:
        results.add(digits)

    # 先頭0欠落パターン: 9-10桁で0始まりでない場合は0を付与
    if not digits.startswith('0') and not digits.startswith('81'):
        with_zero = '0' + digits
        if 10 <= len(with_zero) <= 11:
            results.add(with_zero)

    # それ以外でも10-11桁ならそのまま追加（一応）
    if 10 <= len(digits) <= 11 and not results:
        results.add(digits)

    return list(results)


def build_hw_index(csv_files: list[str]) -> dict:
    """
    ハローワークCSVから電話番号→レコード情報の辞書を構築。
    チャンク読み込みでメモリ節約。
    """
    phone_index = {}
    total_rows = 0
    total_matched_phones = 0

    for fpath in csv_files:
        fname = os.path.basename(fpath)
        print(f"\n  読み込み中: {fname}...", flush=True)

        # 存在するカラムだけフィルタ
        try:
            header = pd.read_csv(fpath, encoding='cp932', nrows=0, dtype=str)
            available_cols = [c for c in HW_USE_COLS if c in header.columns]
            if '選考担当者ＴＥＬ' not in available_cols:
                print(f"    電話番号カラムなし、スキップ")
                continue
        except Exception as e:
            print(f"    ヘッダー読み込みエラー: {e}")
            continue

        chunk_count = 0
        for chunk in pd.read_csv(fpath, encoding='cp932', dtype=str,
                                  usecols=available_cols, chunksize=50000):
            chunk_count += 1
            total_rows += len(chunk)

            for _, row in chunk.iterrows():
                tel = row.get('選考担当者ＴＥＬ', '')
                phones = normalize_phone(tel)

                # FAXも突合対象に（電話番号としてFAXが使われるケースがある）
                fax = row.get('選考担当者ＦＡＸ', '')
                phones.extend(normalize_phone(fax))

                for p in phones:
                    if p not in phone_index:
                        phone_index[p] = row.to_dict()
                        total_matched_phones += 1

            if chunk_count % 5 == 0:
                print(f"    {total_rows:,}行処理済, インデックス: {len(phone_index):,}件", flush=True)

        print(f"    完了: {total_rows:,}行, インデックス: {len(phone_index):,}件")

    print(f"\n  ハローワーク電話番号インデックス: {len(phone_index):,}件（{total_rows:,}行から）")
    return phone_index


def main():
    print("=" * 70)
    print("リクロジ アポ先 × ハローワーク 電話番号マッチング")
    print("=" * 70)

    # リクロジ読み込み
    riku_path = Path(r"C:\Users\fuji1\Downloads\リクロジ_アポ先_全マッチ結果_統合.xlsx")
    riku = pd.read_excel(riku_path, sheet_name="統合結果", dtype=str)
    print(f"リクロジ: {len(riku)}行")

    # 電話番号カラム特定
    phone_cols = []
    for c in riku.columns:
        if '電話' in c or 'phone' in c.lower():
            phone_cols.append(c)
    print(f"リクロジ電話番号カラム: {phone_cols}")

    # リクロジの電話番号を正規化
    riku_phones = {}  # 正規化番号 → 行インデックスリスト
    for idx, row in riku.iterrows():
        for col in phone_cols:
            val = row.get(col, '')
            for p in normalize_phone(val):
                if p not in riku_phones:
                    riku_phones[p] = []
                riku_phones[p].append(idx)

        # 担当者接続電話番号もチェック
        for col in ['担当者接続電話番号']:
            if col in riku.columns:
                val = row.get(col, '')
                for p in normalize_phone(val):
                    if p not in riku_phones:
                        riku_phones[p] = []
                    riku_phones[p].append(idx)

    print(f"リクロジ正規化電話番号: {len(riku_phones):,}件")

    # ハローワークCSVリスト構築
    hw_files = []

    # デスクトップの2ファイル
    desktop = Path(r"C:\Users\fuji1\OneDrive\デスクトップ")
    for fname in ["RCMEB002002_M100 (2).csv", "RCMEB002002_M100.csv"]:
        fpath = desktop / fname
        if fpath.exists():
            hw_files.append(str(fpath))

    # ハローワーク2025.02.25フォルダ
    hw_dir = desktop / "ハローワーク2025.02.25"
    if hw_dir.is_dir():
        for f in sorted(hw_dir.iterdir()):
            if f.suffix == '.csv' and f.stat().st_size > 1000:
                hw_files.append(str(f))

    # 重複除去（同じファイル名がデスクトップとフォルダにある場合）
    seen_names = set()
    unique_files = []
    for f in hw_files:
        name = os.path.basename(f)
        if name not in seen_names:
            seen_names.add(name)
            unique_files.append(f)
    hw_files = unique_files

    print(f"\nハローワークCSV: {len(hw_files)}ファイル")
    for f in hw_files:
        size = os.path.getsize(f) / 1024 / 1024
        print(f"  {os.path.basename(f)} ({size:.0f}MB)")

    # Phase 1: ハローワーク電話番号インデックス構築
    print(f"\n--- Phase 1: ハローワーク電話番号インデックス構築 ---")
    hw_index = build_hw_index(hw_files)

    # Phase 2: マッチング
    print(f"\n--- Phase 2: マッチング ---")
    matched_count = 0
    matched_rows = set()

    # HW列を追加
    hw_result_cols = [c for c in HW_USE_COLS if c != '選考担当者ＴＥＬ' and c != '選考担当者ＦＡＸ']
    for col in hw_result_cols:
        riku[f'{HW_PREFIX}{col}'] = ''
    riku['HW_マッチ'] = ''
    riku['HW_マッチ電話番号'] = ''

    for norm_phone, row_indices in riku_phones.items():
        if norm_phone in hw_index:
            hw_data = hw_index[norm_phone]
            for idx in row_indices:
                if idx in matched_rows:
                    continue
                matched_rows.add(idx)
                matched_count += 1

                riku.at[idx, 'HW_マッチ'] = 'マッチ'
                riku.at[idx, 'HW_マッチ電話番号'] = norm_phone
                for col in hw_result_cols:
                    val = hw_data.get(col, '')
                    if pd.notna(val) and str(val).strip():
                        riku.at[idx, f'{HW_PREFIX}{col}'] = str(val)

    # アンマッチ設定
    riku.loc[riku['HW_マッチ'] == '', 'HW_マッチ'] = 'アンマッチ'

    print(f"マッチ: {matched_count}社")
    print(f"アンマッチ: {len(riku) - matched_count}社")
    print(f"マッチ率: {matched_count / len(riku) * 100:.1f}%")

    # Phase 3: 保存
    print(f"\n--- Phase 3: 結果保存 ---")

    # Zoom詳細シートも読み込んで一緒に保存
    zoom_detail = pd.read_excel(riku_path, sheet_name="Zoom録画詳細", dtype=str)

    out_path = r"C:\Users\fuji1\Downloads\リクロジ_アポ先_全マッチ結果_統合.xlsx"
    with pd.ExcelWriter(out_path, engine='openpyxl') as writer:
        riku.to_excel(writer, sheet_name='統合結果', index=False)
        zoom_detail.to_excel(writer, sheet_name='Zoom録画詳細', index=False)

    print(f"保存完了: {out_path}")
    print(f"  統合結果: {len(riku)}行 × {len(riku.columns)}カラム")

    # サマリー
    print(f"\n{'='*70}")
    print(f"完了サマリー")
    print(f"{'='*70}")
    cs_m = (riku['cloudsign_match'] == 'マッチ').sum() if 'cloudsign_match' in riku.columns else 0
    zm_m = (riku['zoom_match'] == 'マッチ').sum() if 'zoom_match' in riku.columns else 0
    hw_m = (riku['HW_マッチ'] == 'マッチ').sum()
    print(f"リクロジ全社: {len(riku)}")
    print(f"  cloudsignマッチ: {cs_m} ({cs_m/len(riku)*100:.1f}%)")
    print(f"  Zoomマッチ: {zm_m} ({zm_m/len(riku)*100:.1f}%)")
    print(f"  ハローワークマッチ: {hw_m} ({hw_m/len(riku)*100:.1f}%)")
    all_match = ((riku.get('cloudsign_match','')=='マッチ') | (riku.get('zoom_match','')=='マッチ') | (riku['HW_マッチ']=='マッチ')).sum()
    print(f"  いずれかにマッチ: {all_match} ({all_match/len(riku)*100:.1f}%)")
    none_match = len(riku) - all_match
    print(f"  全てアンマッチ: {none_match} ({none_match/len(riku)*100:.1f}%)")

    # マッチした会社のサンプル
    print(f"\n--- HWマッチ サンプル（先頭10件） ---")
    hw_matched = riku[riku['HW_マッチ'] == 'マッチ'].head(10)
    for _, row in hw_matched.iterrows():
        print(f"  {row.get('会社名','')} | TEL:{row.get('電話番号','')} → HW: {row.get('HW_事業所名漢字','')}")


if __name__ == "__main__":
    main()
