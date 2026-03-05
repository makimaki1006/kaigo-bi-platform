#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
hwiroha.comからハローワーク別有効求人倍率データをスクレイピングするスクリプト

使用方法:
    python scripts/scrape_job_openings_ratio.py [--prefecture 東京都]
"""

import argparse
import csv
import re
import time
from datetime import datetime
from pathlib import Path

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout


# 都道府県コードとローマ字名のマッピング
# 北海道は4地域に分割（道央、道北、道東、道南）
# タプル形式: (一覧ページURL用, 個別ページディレクトリ用)
# 単一文字列の場合は両方同じ値を使用
PREFECTURE_MAPPING = {
    "北海道（道央）": ("Hokkaido-Doo", "Hokkaido_Doo"),
    "北海道（道北）": ("Hokkaido-Dohoku", "Hokkaido_Dohoku"),
    "北海道（道東）": ("Hokkaido-Doto", "Hokkaido_Doto"),
    "北海道（道南）": ("Hokkaido-Donan", "Hokkaido_Donan"),
    "青森県": "Aomori",
    "岩手県": "Iwate",
    "宮城県": "Miyagi",
    "秋田県": "Akita",
    "山形県": "Yamagata",
    "福島県": "Fukushima",
    "茨城県": "Ibaraki",
    "栃木県": "Tochigi",
    "群馬県": "Gunma",
    "埼玉県": "Saitama",
    "千葉県": "Chiba",
    "東京都": "Tokyo",
    "神奈川県": "Kanagawa",
    "新潟県": "Niigata",
    "富山県": "Toyama",
    "石川県": "Ishikawa",
    "福井県": "Fukui",
    "山梨県": "Yamanashi",
    "長野県": "Nagano",
    "岐阜県": "Gifu",
    "静岡県": "Shizuoka",
    "愛知県": "Aichi",
    "三重県": "Mie",
    "滋賀県": "Shiga",
    "京都府": "Kyoto",
    "大阪府": "Osaka",
    "兵庫県": "Hyogo",
    "奈良県": "Nara",
    "和歌山県": "Wakayama",
    "鳥取県": "Tottori",
    "島根県": "Shimane",
    "岡山県": "Okayama",
    "広島県": "Hiroshima",
    "山口県": "Yamaguchi",
    "徳島県": "Tokushima",
    "香川県": "Kagawa",
    "愛媛県": "Ehime",
    "高知県": "Kochi",
    "福岡県": "Fukuoka",
    "佐賀県": "Saga",
    "長崎県": "Nagasaki",
    "熊本県": "Kumamoto",
    "大分県": "Oita",
    "宮崎県": "Miyazaki",
    "鹿児島県": "Kagoshima",
    "沖縄県": "Okinawa",
}


def get_hellowork_list_url(prefecture_romaji: str) -> str:
    """都道府県のハローワーク一覧ページURLを取得"""
    return f"https://www.hwiroha.com/area-{prefecture_romaji}.html"


def get_hellowork_detail_url(prefecture_romaji: str, office_romaji: str) -> str:
    """個別ハローワークページURLを取得"""
    return f"https://www.hwiroha.com/area/{prefecture_romaji}/{office_romaji}.html"


def scrape_hellowork_list(page, list_romaji: str, dir_romaji: str = None) -> list[dict]:
    """都道府県のハローワーク一覧を取得

    Args:
        list_romaji: 一覧ページURL用のローマ字名
        dir_romaji: 個別ページディレクトリ用のローマ字名（Noneの場合はlist_romajiを使用）
    """
    if dir_romaji is None:
        dir_romaji = list_romaji

    url = get_hellowork_list_url(list_romaji)
    print(f"  一覧ページ取得中: {url}")

    page.goto(url, wait_until="domcontentloaded", timeout=60000)
    time.sleep(2)

    offices = []

    # ハローワークへのリンクを取得
    links = page.query_selector_all(f'a[href*="/area/{dir_romaji}/"]')

    for link in links:
        href = link.get_attribute("href")
        text = link.inner_text().strip()

        if href and text:
            # URLからオフィス名を抽出
            match = re.search(rf'/area/{dir_romaji}/(\w+)\.html', href)
            if match:
                office_romaji = match.group(1)
                # オフィス名をクリーンアップ
                office_name = text.replace("ハローワーク", "").strip()
                office_name = re.sub(r'の雇用情勢.*$', '', office_name).strip()
                # URL構築（重複を避ける）
                if href.startswith("http"):
                    url = href
                elif href.startswith("//"):
                    url = f"https:{href}"
                elif href.startswith("/"):
                    url = f"https://www.hwiroha.com{href}"
                else:
                    url = f"https://www.hwiroha.com/area/{dir_romaji}/{office_romaji}.html"
                offices.append({
                    "office_name": office_name,
                    "office_romaji": office_romaji,
                    "url": url
                })

    # 重複除去
    seen = set()
    unique_offices = []
    for office in offices:
        if office["office_romaji"] not in seen:
            seen.add(office["office_romaji"])
            unique_offices.append(office)

    return unique_offices


def scrape_job_openings_ratio(page, url: str) -> dict:
    """個別ハローワークページから有効求人倍率を取得"""
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
        time.sleep(3)  # JavaScriptレンダリング待ち

        result = {
            "url": url,
            "ratio": None,
            "year": None,
            "month": None,
            "jurisdiction": None,
            "error": None
        }

        # JavaScriptでページテキストを取得し、Python側で正規表現処理
        try:
            text = page.evaluate("() => document.body.innerText")

            if text:
                # 有効求人倍率を複数パターンで抽出
                ratio_patterns = [
                    r'有効求人倍率（フルタイム常用）は(\d+\.\d+)倍',
                    r'有効求人倍率（原数値）は(\d+\.\d+)倍',
                    r'有効求人倍率（常用[^）]*）は(\d+\.\d+)倍',  # 北海道用: 「常用※1、原数値」等
                    r'有効求人倍率は(\d+\.\d+)倍',
                    r'有効求人倍率[^0-9]*(\d+\.\d+)倍',
                ]

                for pattern in ratio_patterns:
                    match = re.search(pattern, text)
                    if match:
                        result["ratio"] = float(match.group(1))
                        break

                # 管轄区域を抽出
                juri_match = re.search(r'管轄区域\s*([^\n]+)', text)
                if juri_match:
                    result["jurisdiction"] = juri_match.group(1).strip()

                # 年月を抽出（令和X年X月）
                date_match = re.search(r'令和(\d+)年(\d+)月', text)
                if date_match:
                    result["year"] = int(date_match.group(1)) + 2018
                    result["month"] = int(date_match.group(2))

        except Exception as e:
            result["error"] = f"データ抽出エラー: {str(e)}"

        return result

    except PlaywrightTimeout:
        return {"url": url, "ratio": None, "error": "タイムアウト"}
    except Exception as e:
        return {"url": url, "ratio": None, "error": str(e)}


def scrape_prefecture(prefecture: str, output_dir: Path) -> list[dict]:
    """指定都道府県の全ハローワークデータを取得"""
    mapping = PREFECTURE_MAPPING.get(prefecture)
    if not mapping:
        print(f"エラー: 都道府県名が不正です: {prefecture}")
        return []

    # タプルの場合は(一覧用, ディレクトリ用)、文字列の場合は両方同じ
    if isinstance(mapping, tuple):
        list_romaji, dir_romaji = mapping
    else:
        list_romaji = dir_romaji = mapping

    print(f"\n{'='*60}")
    print(f"{prefecture}のハローワークデータを取得中...")
    print(f"{'='*60}")

    results = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )
        page = context.new_page()

        try:
            # ハローワーク一覧を取得
            offices = scrape_hellowork_list(page, list_romaji, dir_romaji)
            print(f"  {len(offices)}件のハローワークを検出")

            # 各ハローワークのデータを取得
            for i, office in enumerate(offices, 1):
                print(f"  [{i}/{len(offices)}] {office['office_name']}...")

                data = scrape_job_openings_ratio(page, office["url"])

                result = {
                    "prefecture": prefecture,
                    "hellowork_name": office["office_name"],
                    "hellowork_romaji": office["office_romaji"],
                    "jurisdiction": data.get("jurisdiction", ""),
                    "ratio": data.get("ratio"),
                    "year": data.get("year"),
                    "month": data.get("month"),
                    "url": office["url"],
                    "error": data.get("error", "")
                }
                results.append(result)

                if data.get("ratio"):
                    print(f"    → 有効求人倍率: {data['ratio']}倍")
                else:
                    print(f"    → データ取得失敗: {data.get('error', '不明')}")

                # サーバー負荷軽減
                time.sleep(1)

        finally:
            browser.close()

    return results


def save_results(results: list[dict], output_path: Path):
    """結果をCSVに保存"""
    if not results:
        print("保存するデータがありません")
        return

    fieldnames = [
        "prefecture", "hellowork_name", "hellowork_romaji",
        "jurisdiction", "ratio", "year", "month", "url", "error"
    ]

    with open(output_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    print(f"\n結果を保存しました: {output_path}")


def main():
    parser = argparse.ArgumentParser(description="ハローワーク別有効求人倍率スクレイピング")
    parser.add_argument(
        "--prefecture", "-p",
        type=str,
        default=None,
        help="対象都道府県（例: 東京都）。指定なしで全国"
    )
    parser.add_argument(
        "--output-dir", "-o",
        type=str,
        default="data/job_openings_ratio",
        help="出力ディレクトリ"
    )
    args = parser.parse_args()

    # 出力ディレクトリ作成
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    all_results = []

    if args.prefecture:
        # 指定都道府県のみ
        prefectures = [args.prefecture]
    else:
        # 全都道府県
        prefectures = list(PREFECTURE_MAPPING.keys())

    for prefecture in prefectures:
        results = scrape_prefecture(prefecture, output_dir)
        all_results.extend(results)

        # 都道府県ごとに中間保存
        if results:
            mapping = PREFECTURE_MAPPING[prefecture]
            # タプルの場合は2番目（ディレクトリ用）を使用
            file_suffix = mapping[1] if isinstance(mapping, tuple) else mapping
            pref_output = output_dir / f"job_ratio_{file_suffix}.csv"
            save_results(results, pref_output)

    # 全データを統合保存
    if all_results:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        all_output = output_dir / f"job_ratio_all_{timestamp}.csv"
        save_results(all_results, all_output)

        # サマリー出力
        success_count = sum(1 for r in all_results if r["ratio"] is not None)
        print(f"\n{'='*60}")
        print(f"完了サマリー")
        print(f"{'='*60}")
        print(f"総件数: {len(all_results)}")
        print(f"成功: {success_count}")
        print(f"失敗: {len(all_results) - success_count}")


if __name__ == "__main__":
    main()
