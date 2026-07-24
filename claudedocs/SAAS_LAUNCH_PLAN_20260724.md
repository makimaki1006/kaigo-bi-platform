# kaigo-bi SaaS ローンチ計画 (2026-07-24 起案)

## 確定方針(ユーザー決定)

| 項目 | 決定 |
|------|------|
| 商品形態 | BI SaaS 主軸 + 上位プランでリストCSVダウンロード |
| ターゲット | ①介護業界に営業する企業(人材紹介・SaaS・卸等) ②介護事業者 ③M&A検討層 |
| 差別化機能 | 介護情報公表システムの決算資料PDFをAI解析 → 財務KPI組み込み |
| インフラ | Rust/Axum継続。Render(課金済み)を第一候補、フロントはVercel可 |

## 現状資産(2026-07-24 検証済み)

- リポジトリ: `C:\Users\fuji1\kaigo-bi-platform`(GitHubからクリーンクローン)
- Turso メインDB: facilities **223,103行**、kpi_cache 37キー(東京リージョン、生存)
- Turso 外部統計DB: v2_external_* 10テーブル(人口・有効求人倍率・離職率等、生存)
- cargo check / next build 両方成功
- ダッシュボード8ページ: dashboard / pmi-synergy / quality / revenue / salary / service-portfolio / trends / workforce
- Render `kaigo-bi.onrender.com`: **応答なし** → ダッシュボードで要確認

## フェーズ計画

### Phase 0: 復旧(ほぼ完了)
- [x] リポジトリ復元、.env回収、ビルド検証、DB疎通
- [ ] ローカルフルスタック起動確認(実行中)
- [ ] Renderサービス状態確認・デプロイ経路復旧(GitHub Actions → ghcr.io → Render)
- [ ] データ鮮度確認(最終スクレイピング2026-03、必要なら再スクレイピング)

### Phase 1: SaaS基盤(認証・課金)

**既存実装(2026-07-24コード調査で確認)**: JWT認証一式(login/me/logout/refresh)、admin用ユーザーCRUD(`users`テーブル on Turso、role/is_active/expires_at付き)、CSVエクスポートAPI(`/api/export/csv`、BOM付きUTF-8)、M&A系ルート(ma_screening/due_diligence/pmi/benchmark)が**すべて実装済み**。全データAPIは認証必須になっている。

**実装完了(2026-07-24、feature/saas-phase1ブランチ)**:
- ✅ セルフサインアップ `/api/auth/signup` + `/signup` ページ(登録後即ログイン)
- ✅ `users.plan` カラム + `stripe_customer_id` + `stripe_subscription_id`(マイグレーション実行済み、既存2ユーザーはplan='ma'にグランドファザリング)
- ✅ プラン別ゲート(free=dashboard+meta / standard=BI全機能+外部統計 / pro=+CSVエクスポート / ma=+M&A系。role=adminは全バイパス)
- ✅ Stripe: Checkout + Customer Portal + Webhook(署名検証付き、REST直叩きでSDK依存なし)
- ✅ エクスポート月間クレジット(pro=3,000行/ma=10,000行、export_logsテーブルで記録、`/api/export/usage`で残量取得)
- ✅ フロント: `/signup` `/pricing` `/account` ページ、サイドバーにプランバッジ、admin画面でプラン手動設定
- ✅ JWT Claimsにplan追加(旧トークンはserde defaultで"free"互換)

残工事:
- メール確認(現状は即有効化。Resend等の導入は後続)
- APIレート制限
- **Stripe側の設定(ユーザー作業)**: アカウント作成 → 商品/Price 3つ作成 → Webhookエンドポイント登録 → 環境変数設定(`.env.example`参照)

### Phase 2: リストDL機能(Salesforce_Listノウハウ流用)
- 検索UI: 都道府県×サービス種別×定員×法人格などの条件ビルダー(バックエンドの`FilterParams`+`/api/export/csv`は実装済み、フロントのリスト作成専用ページを新設)
- プラン別の月間DL件数クレジット制
- 電話番号・代表者等の項目は上位プラン限定(エクスポート列のプラン別制御)

### Phase 3: 決算PDF AI解析(M&A層向け目玉)
- 介護情報公表システムから決算資料PDFを収集(既存スクレイパー拡張)
- Claude APIで構造化抽出: 売上・営業利益・人件費率・債務等 → `financials` テーブル
- 財務KPIダッシュボード + M&Aスクリーニング(収益性×地域×規模でソート)
- pmi-synergyページと統合

### Phase 4: ローンチ準備
- LP・料金ページ・オンボーディング
- 利用規約/特商法表記(データ出所: 公開情報である旨明記)
- 監視(ヘルスチェック、エラー通知)、バックアップ

## 料金プラン(たたき台、要ユーザー判断)

| プラン | 想定月額 | 内容 | 主ターゲット |
|--------|---------|------|-------------|
| Free | 0円 | 全国サマリーのみ、都道府県ドリルダウン不可 | リード獲得 |
| Standard | 9,800円 | BI全機能(8ダッシュボード) | 介護事業者 |
| Pro | 29,800円 | + リストDL(月3,000件) | 営業企業 |
| M&A | 49,800円〜 | + 財務KPI・スクリーニング | M&A検討層 |

## 要検討・リスク

1. **データ利用規約**: 介護情報公表システムのデータ商用再配布の条件確認(特にリストDL・決算数値の再提供)
2. **データ鮮度**: 最終スクレイピングは2026-03。ローンチ前に全国再スクレイピング(6-12時間)+定期更新の自動化が必要
3. **Render構成**: render.yamlはfreeプラン記載のまま。課金済みサービスの実態確認後に更新
4. **決算PDFの様式ばらつき**: 都道府県によりPDF様式が異なる可能性 → 抽出精度のサンプリング検証を先行
