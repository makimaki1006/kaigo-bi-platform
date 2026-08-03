# kaigo-bi Googleインデックス登録 手順書

作成日: 2026-07-29  
対象サイト: `https://kaigo-bi.onrender.com`

## コード側の準備状況

- 公開7ページは未ログインで閲覧可能
- 公開ページは`index, follow`
- ログイン・登録・アプリページは`noindex, nofollow`
- canonicalは`https://kaigo-bi.onrender.com`基準
- `robots.txt`はHTMLページのクロールを許可し、`/api/`のみ拒否
- sitemapは公開7ページだけを掲載
- Organization、WebSite、FAQPageのJSON-LDを出力
- Google Search ConsoleのHTMLタグ確認コードを環境変数で設定可能

公開URL:

1. `/`
2. `/features/management`
3. `/features/sales`
4. `/features/ma`
5. `/data`
6. `/methodology`
7. `/pricing`

## デプロイ後の確認

以下がすべてHTTP 200になることを確認する。

```text
https://kaigo-bi.onrender.com/
https://kaigo-bi.onrender.com/robots.txt
https://kaigo-bi.onrender.com/sitemap.xml
```

トップページのHTMLで以下を確認する。

```html
<meta name="robots" content="index, follow">
<link rel="canonical" href="https://kaigo-bi.onrender.com">
```

ログイン画面では以下を確認する。

```html
<meta name="robots" content="noindex, nofollow">
```

## Google Search Console登録

GoogleアカウントでSearch Consoleを開き、次のどちらかで登録する。

### 独自ドメイン導入前

URLプレフィックスプロパティとして登録する。

```text
https://kaigo-bi.onrender.com/
```

HTMLタグ方式を使う場合、Googleから提示された次のタグについて、
`content`の値だけを取得する。

```html
<meta name="google-site-verification" content="この値">
```

ビルド環境の`NEXT_PUBLIC_GOOGLE_SITE_VERIFICATION`へ値を設定し、
再ビルド・再デプロイする。ページHTMLにタグが出たことを確認してから
Search Consoleで「確認」を実行する。

### 独自ドメイン導入後

ドメインプロパティを推奨する。DNSへGoogle指定のTXTレコードを追加する。
この方式ではコードへの確認タグ追加は不要。

## sitemap送信

Search Consoleの「サイトマップ」で以下を送信する。

```text
sitemap.xml
```

送信後、ステータスが「成功しました」になることを確認する。

## 初回インデックス依頼

Search ConsoleのURL検査で、以下の順に確認・登録を依頼する。

1. トップ
2. 料金
3. 主要ターゲットのペルソナページ
4. データ
5. methodology
6. 残りのペルソナページ

各URLで次を確認する。

- URLがGoogleに登録可能
- robots.txtでブロックされていない
- `noindex`が検出されていない
- Googleが選択したcanonicalとユーザー指定canonicalが一致
- 公開本文がレンダリング結果に含まれる

問題がなければ「インデックス登録をリクエスト」を行う。登録は保証されず、
反映まで数日以上かかる場合がある。

## 公開後の監視

週1回:

- ページのインデックス登録レポート
- sitemapの取得状況
- 手動対策・セキュリティ問題
- クロールエラー

月1回:

- 検索クエリ
- 表示回数
- クリック数
- CTR
- 平均掲載順位
- 検索流入から登録開始への遷移

`site:`検索は概況確認にとどめ、個別URLの判断はSearch ConsoleのURL検査を
正とする。

## 独自ドメイン移行時の必須作業

1. `NEXT_PUBLIC_SITE_URL`を独自ドメインへ変更
2. 旧URLから新URLへ恒久的な301リダイレクト
3. canonical、OG URL、robots、sitemapが新ドメインを指すことを確認
4. Search Consoleへ新ドメインを登録
5. 新sitemapを送信
6. 旧Render URLと新URLの両方が200で重複公開されないようにする

独自ドメインへ切り替える際、canonicalだけを変えて旧URLを残す運用は避ける。
