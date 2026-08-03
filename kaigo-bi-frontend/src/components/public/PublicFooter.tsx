import Link from "next/link";
import styles from "@/app/landing.module.css";

const FOOTER_LINKS = [
  { href: "/data", label: "収録データ" },
  { href: "/methodology", label: "指標の定義" },
  { href: "/pricing", label: "料金" },
  { href: "/terms", label: "利用規約" },
  { href: "/privacy", label: "プライバシーポリシー" },
  { href: "/legal", label: "特定商取引法に基づく表記" },
] as const;

export default function PublicFooter() {
  const year = new Date().getFullYear();

  return (
    <footer className={styles.publicFooter}>
      <div className={styles.footerInner}>
        <p className={styles.footerStatement}>
          公開情報を、介護業界の次の判断につなげる。
        </p>
        <div className={styles.footerMeta}>
          <Link href="/" className={styles.wordmark}>
            kaigo-bi
          </Link>
          <nav aria-label="フッターナビゲーション">
            <ul className={styles.footerLinks}>
              {FOOTER_LINKS.map((link) => (
                <li key={link.href}>
                  <Link href={link.href} className={styles.footerLink}>
                    {link.label}
                  </Link>
                </li>
              ))}
            </ul>
          </nav>
        </div>
        <p className={styles.footerFinePrint}>© {year} kaigo-bi</p>
      </div>
    </footer>
  );
}
