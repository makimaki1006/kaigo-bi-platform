# -*- coding: utf-8 -*-
"""
受注率フォーカス: 月別 × 施設形態 × 法人格

受注率の絶対値を軸に、各月でどの組み合わせが最も受注率が高いかを明確化。
"""

import sys
import io
from pathlib import Path
from datetime import datetime

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['PYTHONIOENCODING'] = 'utf-8'

import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')


def main():
    output_dir = project_root / 'data' / 'output' / 'analysis'
    report_dir = project_root / 'claudedocs'
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    # 前回のMECEクロス集計結果を読み込み
    csv_path = sorted(output_dir.glob('mece_cross_results_*.csv'))[-1]
    df = pd.read_csv(csv_path, encoding='utf-8-sig')
    print(f"データ読み込み: {csv_path.name} ({len(df)} セグメント)")

    months = list(range(1, 13))
    month_names = {1:'1月',2:'2月',3:'3月',4:'4月',5:'5月',6:'6月',
                   7:'7月',8:'8月',9:'9月',10:'10月',11:'11月',12:'12月'}

    report = []
    report.append("# 受注率フォーカス: 月別 × 施設形態 × 法人格 分析")
    report.append(f"\n**分析日**: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    report.append("\n**方針**: 受注率の絶対値を軸に、各月で狙うべき組み合わせを明確化")
    report.append("\n---")

    # ========================================
    # 1. 各月の受注率ランキング TOP10
    # ========================================
    print("\n" + "=" * 120)
    print("各月の受注率ランキング（施設形態 × 法人格、月別件数5件以上）")
    print("=" * 120)

    report.append("\n## 1. 各月の受注率ランキング TOP10")
    report.append("\n月ごとに「受注率が高い施設形態×法人格」のTOP10を表示。最低月別5件以上。")

    for m in months:
        rate_col = f'm{m}_rate'
        count_col = f'm{m}_count'

        # 月別件数5件以上、受注率がNaNでない
        df_m = df[(df[count_col] >= 5) & (df[rate_col].notna())].copy()
        df_m = df_m.sort_values(rate_col, ascending=False)

        print(f"\n{'─' * 120}")
        print(f"  {month_names[m]} 受注率ランキング TOP10")
        print(f"{'─' * 120}")
        print(f"  {'#':>2} {'施設形態':<18} {'法人格':<16} {'受注率':>8} "
              f"{'月件数':>6} {'年間率':>7} {'年間件数':>7}")
        print(f"  {'':>2} {'':18} {'':16} {'':>8} "
              f"{'':>6} {'':>7} {'':>7}")

        report.append(f"\n### {month_names[m]}")
        report.append(f"\n| # | 施設形態 | 法人格 | **受注率** | 月件数 | 年間率 | 年間件数 |")
        report.append(f"|---|---|---|---|---|---|---|")

        for i, (_, row) in enumerate(df_m.head(10).iterrows()):
            rate = row[rate_col] * 100
            annual = row['annual_rate'] * 100
            print(f"  {i+1:>2} {str(row['facility'])[:17]:<18} {str(row['legal'])[:15]:<16} "
                  f"{rate:>7.1f}% {int(row[count_col]):>6} {annual:>6.1f}% {int(row['total']):>7}")
            report.append(f"| {i+1} | {row['facility']} | {row['legal']} | "
                         f"**{rate:.1f}%** | {int(row[count_col])} | {annual:.1f}% | {int(row['total'])} |")

    # ========================================
    # 2. 受注率が安定して高いセグメント（全月平均）
    # ========================================
    print("\n\n" + "=" * 120)
    print("受注率が安定して高いセグメント（データのある月の平均受注率）")
    print("=" * 120)

    report.append("\n---")
    report.append("\n## 2. 受注率が安定して高いセグメント")
    report.append("\n年間件数30件以上、かつデータのある月が6ヶ月以上のセグメントで、平均受注率順。")

    # 各月の受注率がある月だけで平均を計算
    rate_cols = [f'm{m}_rate' for m in months]
    count_cols = [f'm{m}_count' for m in months]

    df_stable = df[df['total'] >= 30].copy()
    # データがある月の数
    df_stable['months_with_data'] = df_stable[rate_cols].notna().sum(axis=1)
    # 各月で件数3件以上ある月のみで平均受注率
    for m in months:
        df_stable[f'valid_m{m}'] = df_stable.apply(
            lambda r: r[f'm{m}_rate'] if r[f'm{m}_count'] >= 3 and pd.notna(r[f'm{m}_rate']) else np.nan, axis=1
        )
    valid_rate_cols = [f'valid_m{m}' for m in months]
    df_stable['avg_rate'] = df_stable[valid_rate_cols].mean(axis=1)
    df_stable['std_rate'] = df_stable[valid_rate_cols].std(axis=1)
    df_stable['valid_months'] = df_stable[valid_rate_cols].notna().sum(axis=1)

    # 6ヶ月以上データがあるもの
    df_stable = df_stable[df_stable['valid_months'] >= 6]
    df_stable = df_stable.sort_values('avg_rate', ascending=False)

    print(f"\n  {'#':>2} {'施設形態':<18} {'法人格':<16} {'平均受注率':>9} {'ブレ幅(σ)':>9} "
          f"{'年間率':>7} {'年間件数':>7} {'有効月数':>7}")
    print(f"  " + "-" * 100)

    report.append(f"\n| # | 施設形態 | 法人格 | **平均受注率** | ブレ幅(σ) | 年間率 | 年間件数 | 有効月数 |")
    report.append(f"|---|---|---|---|---|---|---|---|")

    for i, (_, row) in enumerate(df_stable.head(15).iterrows()):
        avg = row['avg_rate'] * 100
        std = row['std_rate'] * 100
        annual = row['annual_rate'] * 100
        print(f"  {i+1:>2} {str(row['facility'])[:17]:<18} {str(row['legal'])[:15]:<16} "
              f"{avg:>8.1f}% {std:>8.1f}% {annual:>6.1f}% {int(row['total']):>7} {int(row['valid_months']):>7}")
        report.append(f"| {i+1} | {row['facility']} | {row['legal']} | "
                     f"**{avg:.1f}%** | {std:.1f}% | {annual:.1f}% | {int(row['total'])} | {int(row['valid_months'])} |")

    # ========================================
    # 3. 受注率×件数マトリクス（月別）
    # ========================================
    print("\n\n" + "=" * 120)
    print("月別 受注率マトリクス（年間30件以上のセグメント、受注率順）")
    print("=" * 120)

    report.append("\n---")
    report.append("\n## 3. 月別受注率マトリクス")
    report.append("\n年間30件以上のセグメントを受注率順に並べ、全月の受注率を一覧化。")
    report.append("\n**凡例**: 数値は受注率(%)。件数3件未満は `--`。受注率15%以上は**太字**。")

    df_matrix = df[df['total'] >= 30].copy()
    df_matrix = df_matrix.sort_values('annual_rate', ascending=False)

    # ヘッダー
    header = f"  {'施設形態':<16} {'法人格':<14} {'年間':>5} {'件数':>5}"
    for m in months:
        header += f" {m:>3}月"
    print(header)
    print(f"  " + "-" * (42 + 5 * 12))

    report_header = "| 施設形態 | 法人格 | 年間 | 件数 |"
    report_sep = "|---|---|---|---|"
    for m in months:
        report_header += f" {m}月 |"
        report_sep += "---|"
    report.append(f"\n{report_header}")
    report.append(report_sep)

    for _, row in df_matrix.iterrows():
        line = f"  {str(row['facility'])[:15]:<16} {str(row['legal'])[:13]:<14} "
        line += f"{row['annual_rate']*100:>4.1f}% {int(row['total']):>5}"

        rpt_line = f"| {row['facility']} | {row['legal']} | {row['annual_rate']*100:.1f}% | {int(row['total'])} |"

        for m in months:
            rate = row[f'm{m}_rate']
            count = row[f'm{m}_count']
            if pd.notna(rate) and count >= 3:
                val = rate * 100
                if val >= 15:
                    line += f" {val:>4.0f}*"
                    rpt_line += f" **{val:.0f}%** |"
                elif val == 0:
                    line += f"    0 "
                    rpt_line += f" 0% |"
                else:
                    line += f" {val:>5.1f}"
                    rpt_line += f" {val:.1f}% |"
            else:
                line += f"   -- "
                rpt_line += " -- |"
        print(line)
        report.append(rpt_line)

    # ========================================
    # 4. 4月の受注率視点での推奨
    # ========================================
    print("\n\n" + "=" * 120)
    print("4月: 受注率で見た推奨（年間30件以上、4月5件以上）")
    print("=" * 120)

    report.append("\n---")
    report.append("\n## 4. 4月の受注率ベース推奨")

    df_apr = df[(df['total'] >= 30) & (df['m4_count'] >= 5) & (df['m4_rate'].notna())].copy()
    df_apr = df_apr.sort_values('m4_rate', ascending=False)

    print(f"\n  {'#':>2} {'施設形態':<18} {'法人格':<16} {'4月受注率':>9} "
          f"{'4月件数':>7} {'年間率':>7} {'年間件数':>7} {'判定':<6}")
    print(f"  " + "-" * 95)

    report.append(f"\n| # | 施設形態 | 法人格 | **4月受注率** | 4月件数 | 年間率 | 年間件数 | 判定 |")
    report.append(f"|---|---|---|---|---|---|---|---|")

    for i, (_, row) in enumerate(df_apr.iterrows()):
        apr = row['m4_rate'] * 100
        annual = row['annual_rate'] * 100

        if apr >= 15:
            verdict = "◎攻め"
        elif apr >= 8:
            verdict = "○可"
        elif apr >= 3:
            verdict = "△注意"
        else:
            verdict = "✕避け"

        print(f"  {i+1:>2} {str(row['facility'])[:17]:<18} {str(row['legal'])[:15]:<16} "
              f"{apr:>8.1f}% {int(row['m4_count']):>7} {annual:>6.1f}% {int(row['total']):>7} {verdict}")
        report.append(f"| {i+1} | {row['facility']} | {row['legal']} | "
                     f"**{apr:.1f}%** | {int(row['m4_count'])} | {annual:.1f}% | {int(row['total'])} | {verdict} |")

    # ========================================
    # 5. 各月のベストセグメント一覧（受注率TOP3）
    # ========================================
    print("\n\n" + "=" * 120)
    print("月別ベストセグメント（受注率TOP3、年間30件以上・月5件以上）")
    print("=" * 120)

    report.append("\n---")
    report.append("\n## 5. 月別ベストセグメント（受注率TOP3）")
    report.append("\n年間30件以上かつ月別5件以上のセグメントから受注率TOP3を選出。")

    df_30 = df[df['total'] >= 30].copy()

    for m in months:
        rate_col = f'm{m}_rate'
        count_col = f'm{m}_count'
        df_m = df_30[(df_30[count_col] >= 5) & (df_30[rate_col].notna())].copy()
        df_m = df_m.sort_values(rate_col, ascending=False)

        top3 = df_m.head(3)
        if len(top3) == 0:
            continue

        print(f"\n  {month_names[m]}:")
        report.append(f"\n**{month_names[m]}:**")

        for _, row in top3.iterrows():
            rate = row[rate_col] * 100
            cnt = int(row[count_col])
            print(f"    → {row['facility']} × {row['legal']}: "
                  f"受注率 {rate:.1f}%（{cnt}件）")
            report.append(f"- {row['facility']} × {row['legal']}: **受注率 {rate:.1f}%**（{cnt}件）")

    # レポート保存
    report_text = "\n".join(report)
    report_path = report_dir / 'winrate_focus_analysis.md'
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(report_text)
    print(f"\n\nレポート保存: {report_path}")


if __name__ == "__main__":
    main()
