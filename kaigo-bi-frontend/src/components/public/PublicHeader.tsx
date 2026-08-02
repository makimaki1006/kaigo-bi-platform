import Link from "next/link";
import styles from "@/app/landing.module.css";

export default function PublicHeader() {
  return (
    <header className={styles.publicNav}>
      <div className={styles.publicNavInner}>
        <Link href="/" className={styles.wordmark}>
          kaigo-bi
        </Link>
        <nav aria-label="公開サイトナビゲーション">
          <div className={styles.navActions}>
            <Link href="/login" className={styles.navLink}>
              ログイン
            </Link>
            <Link href="/signup" className={styles.navCta}>
              無料で始める
            </Link>
          </div>
        </nav>
      </div>
    </header>
  );
}
