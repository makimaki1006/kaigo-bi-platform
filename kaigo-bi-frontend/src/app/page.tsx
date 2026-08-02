import type { Metadata } from "next";
import Link from "next/link";
import { ArrowRight } from "lucide-react";
import PublicLayout from "@/components/public/PublicLayout";
import { buildPublicMetadata } from "@/lib/seo";
import styles from "./landing.module.css";

export const metadata: Metadata = buildPublicMetadata({
  path: "/",
  title: "介護業界の営業・経営・M&Aを速くするデータBI",
  description:
    "全国223,103件の介護施設・サービスデータを横断検索。市場分析、営業リスト作成、法人調査、M&Aの初期調査をひとつの画面で進められる介護業界特化BIです。",
});

const STATS = [
  { label: "対象地域", value: "47都道府県" },
  { label: "ユニーク事業所", value: "約19万" },
  { label: "運営法人", value: "約6.8万" },
] as const;

const SEARCH_EXAMPLE = [
  { key: "地域", value: "東京都", reason: "都道府県・市区町村で営業エリアを限定" },
  { key: "サービス", value: "訪問看護", reason: "提供サービスの種別から対象を抽出" },
  { key: "法人種別", value: "株式会社等", reason: "実際のデータに登録された法人区分を使用" },
  { key: "出力", value: "施設・法人", reason: "個別施設と運営法人の両方から確認" },
] as const;

const USE_CASES = [
  {
    name: "SALES",
    title: "営業先を見つける",
    body: "地域、サービス種別、運営法人などの条件から候補を絞り込み、営業リスト作成へつなげます。",
    href: "/features/sales",
    link: "営業支援を見る",
  },
  {
    name: "MANAGEMENT",
    title: "地域と競合を比べる",
    body: "エリアごとの施設分布やサービス構成、人員・品質の指標を比較し、経営判断の材料を整理します。",
    href: "/features/management",
    link: "経営支援を見る",
  },
  {
    name: "M&A",
    title: "候補法人を調べる",
    body: "同一法人が運営する施設、展開地域、サービス構成を横断し、詳しく調査する候補を絞り込みます。",
    href: "/features/ma",
    link: "M&A支援を見る",
  },
] as const;

const STEPS = [
  {
    number: "01",
    title: "条件を決める",
    body: "地域、サービス種別、法人種別など、目的に合う検索条件を指定します。",
  },
  {
    number: "02",
    title: "施設と法人を比較する",
    body: "地図、ランキング、法人グループなど複数の視点から候補を確認します。",
  },
  {
    number: "03",
    title: "次の行動へ移す",
    body: "営業リスト、経営会議、M&A候補の初期調査へデータを活用します。",
  },
] as const;

const PLANS = [
  { name: "Free", price: "¥0", body: "全国サマリーなど、収録データを試したい方に" },
  { name: "Standard", price: "¥9,800", body: "市場・法人・施設分析を経営に活かしたい方に" },
  {
    name: "Pro",
    price: "¥29,800",
    body: "営業候補の抽出とCSV出力まで進めたい方に",
    recommended: true,
  },
  { name: "M&A", price: "¥49,800", body: "候補探索と初期調査を効率化したい方に" },
] as const;

const FAQ = [
  {
    q: "どのような人が利用できますか？",
    a: "介護事業者の経営・事業開発担当者、介護業界へ商品やサービスを提供する企業、M&A仲介会社や買い手企業などを想定しています。",
  },
  {
    q: "どのようなデータを収録していますか？",
    a: "介護サービス情報の公表制度をはじめとする公開情報を整理し、施設・サービス、運営法人、地域、人員、品質などを横断して確認できる形にしています。",
  },
  {
    q: "223,103件とは何の件数ですか？",
    a: "施設とサービス種別の組み合わせで数えた収録レコード数です。同じ事業所が複数の介護サービスを提供する場合は、サービスごとに1件として収録されます。",
  },
  {
    q: "無料で試せますか？",
    a: "Freeプランは月額0円です。全国サマリーなどからkaigo-biのデータをお試しいただき、必要に応じて有料プランへ変更できます。",
  },
] as const;

const faqJsonLd = {
  "@context": "https://schema.org",
  "@type": "FAQPage",
  mainEntity: FAQ.map((item) => ({
    "@type": "Question",
    name: item.q,
    acceptedAnswer: { "@type": "Answer", text: item.a },
  })),
};

export default function HomePage() {
  return (
    <PublicLayout>
      <script
        type="application/ld+json"
        dangerouslySetInnerHTML={{ __html: JSON.stringify(faqJsonLd) }}
      />

      <div className={styles.page}>
        <section className={styles.hero}>
          <div className={`${styles.shell} ${styles.heroGrid}`}>
            <p className={styles.figure}>
              223,103<span className={styles.figureUnit}>件</span>
            </p>
            <div className={styles.heroCopy}>
              <h1 className={styles.heroTitle}>
                介護業界の
                <span className={styles.noBreak}>営業・経営・M&A</span>
                を、データでもっと速く。
              </h1>
              <p className={styles.heroLead}>
                全国の介護事業所と運営法人を横断検索。市場分析、営業リスト作成、法人調査を、
                ひとつの画面で進められます。
              </p>
              <div className={styles.actions}>
                <Link href="/signup" className={styles.primaryAction}>
                  無料で始める
                  <ArrowRight size={16} aria-hidden="true" />
                </Link>
                <Link href="#search-example" className={styles.secondaryAction}>
                  検索例を見る
                </Link>
              </div>
              <p className={styles.heroNote}>
                施設とサービス種別の組み合わせで数えた収録レコード数です。
              </p>
            </div>
          </div>
        </section>

        <section className={styles.statRail} aria-label="収録データ概要">
          <div className={styles.shell}>
            <dl className={styles.statList}>
              {STATS.map((stat) => (
                <div key={stat.label} className={styles.stat}>
                  <dt>{stat.label}</dt>
                  <dd>{stat.value}</dd>
                </div>
              ))}
            </dl>
          </div>
        </section>

        <section id="search-example" className={styles.section}>
          <div className={styles.shell}>
            <header className={styles.sectionHead}>
              <h2 className={styles.sectionTitle}>欲しい営業先を、条件から絞り込む。</h2>
              <p className={styles.sectionLead}>
                バラバラに公開されている介護情報を、施設・法人・地域の単位で検索できる形に整理。
                名簿を眺めるのではなく、狙う市場を決めてから候補を探せます。
              </p>
            </header>
            <div className={styles.searchExample} aria-label="営業先の検索条件例">
              {SEARCH_EXAMPLE.map((row) => (
                <div key={row.key} className={styles.searchRow}>
                  <span className={styles.searchKey}>{row.key}</span>
                  <span className={styles.searchValue}>{row.value}</span>
                  <span className={styles.searchReason}>{row.reason}</span>
                </div>
              ))}
            </div>
          </div>
        </section>

        <section className={styles.section}>
          <div className={styles.shell}>
            <header className={styles.sectionHead}>
              <h2 className={styles.sectionTitle}>同じデータを、目的に合わせて読む。</h2>
              <p className={styles.sectionLead}>
                営業、経営、M&Aで必要な視点は異なります。kaigo-biは用途ごとに、
                同じ公開情報を異なる切り口で確認できるようにします。
              </p>
            </header>
            <div className={styles.useCaseList}>
              {USE_CASES.map((item) => (
                <article key={item.name} className={styles.useCase}>
                  <p className={styles.useCaseName}>{item.name}</p>
                  <h3 className={styles.useCaseTitle}>{item.title}</h3>
                  <p className={styles.useCaseBody}>{item.body}</p>
                  <Link href={item.href} className={styles.useCaseLink}>
                    {item.link}
                  </Link>
                </article>
              ))}
            </div>
          </div>
        </section>

        <section className={styles.section}>
          <div className={styles.shell}>
            <header className={styles.sectionHead}>
              <h2 className={styles.sectionTitle}>検索から判断まで、3つの動作で。</h2>
              <p className={styles.sectionLead}>
                複数の公開サイトや資料を行き来せず、条件設定から比較までkaigo-biで進めます。
              </p>
            </header>
            <ol className={styles.steps}>
              {STEPS.map((step) => (
                <li key={step.number} className={styles.step}>
                  <span className={styles.stepNumber}>{step.number}</span>
                  <h3 className={styles.stepTitle}>{step.title}</h3>
                  <p className={styles.stepBody}>{step.body}</p>
                </li>
              ))}
            </ol>
          </div>
        </section>

        <section className={styles.section}>
          <div className={styles.shell}>
            <header className={styles.sectionHead}>
              <h2 className={styles.sectionTitle}>無料から、必要な機能だけ。</h2>
              <p className={styles.sectionLead}>
                まずはFreeプランで収録データを確認し、営業リストやM&A調査など、
                目的に応じて機能を追加できます。
              </p>
            </header>
            <table className={styles.pricing}>
              <tbody>
                {PLANS.map((plan) => (
                  <tr key={plan.name}>
                    <th scope="row">
                      {plan.name}
                      {"recommended" in plan && plan.recommended && (
                        <span className={styles.recommended}>営業向け</span>
                      )}
                    </th>
                    <td className={styles.price}>{plan.price} / 月</td>
                    <td className={styles.planDescription}>{plan.body}</td>
                  </tr>
                ))}
              </tbody>
            </table>
            <div className={styles.actions}>
              <Link href="/pricing" className={styles.textAction}>
                機能と料金を比較する
                <ArrowRight size={16} aria-hidden="true" />
              </Link>
            </div>
          </div>
        </section>

        <section className={styles.section}>
          <div className={styles.shell}>
            <header className={styles.sectionHead}>
              <h2 className={styles.sectionTitle}>導入前に確認したいこと。</h2>
              <p className={styles.sectionLead}>
                データの範囲と無料プランについて、よくお問い合わせいただく内容です。
              </p>
            </header>
            <dl className={styles.faq}>
              {FAQ.map((item) => (
                <div key={item.q} className={styles.faqItem}>
                  <dt>{item.q}</dt>
                  <dd>{item.a}</dd>
                </div>
              ))}
            </dl>
            <p className={styles.dataNote}>
              掲載範囲、更新日、指標の定義は
              <Link href="/data" className={styles.textAction}>
                データについて
              </Link>
              および
              <Link href="/methodology" className={styles.textAction}>
                指標とデータの考え方
              </Link>
              で確認できます。
            </p>
          </div>
        </section>

        <section>
          <div className={`${styles.shell} ${styles.finalCta}`}>
            <h2 className={styles.finalTitle}>全国の介護市場から、次の営業先を見つける。</h2>
            <div className={styles.actions}>
              <Link href="/signup" className={styles.primaryAction}>
                無料で始める
                <ArrowRight size={16} aria-hidden="true" />
              </Link>
              <Link href="/login" className={styles.secondaryAction}>
                ログイン
              </Link>
            </div>
          </div>
        </section>
      </div>
    </PublicLayout>
  );
}
