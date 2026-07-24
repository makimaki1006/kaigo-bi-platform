/// メール送信サービス（Resend API）
///
/// 環境変数:
///   RESEND_API_KEY - ResendのAPIキー（未設定時は送信せずログにURLを出す開発モード）
///   MAIL_FROM      - 送信元（デフォルト: "介護BI <onboarding@resend.dev>"）
///                    本番ではResendでドメイン認証した送信元に変更すること

use serde_json::json;

const RESEND_API_URL: &str = "https://api.resend.com/emails";

/// パスワードリセットメールを送信する
/// RESEND_API_KEY未設定時はログにリセットURLを出力して成功扱い（ローカル開発用）
pub async fn send_password_reset_email(to: &str, reset_url: &str) -> Result<(), String> {
    let api_key = match std::env::var("RESEND_API_KEY") {
        Ok(k) if !k.is_empty() => k,
        _ => {
            tracing::warn!(
                "RESEND_API_KEY未設定のためメール送信をスキップ。リセットURL: {} (宛先: {})",
                reset_url,
                to
            );
            return Ok(());
        }
    };

    let from = std::env::var("MAIL_FROM")
        .unwrap_or_else(|_| "介護BI <onboarding@resend.dev>".to_string());

    let html = format!(
        r#"<div style="font-family: sans-serif; max-width: 480px; margin: 0 auto;">
  <h2 style="color: #312e81;">パスワード再設定のご案内</h2>
  <p>介護BIのパスワード再設定リクエストを受け付けました。</p>
  <p>以下のボタンから新しいパスワードを設定してください。<strong>リンクの有効期限は30分です。</strong></p>
  <p style="margin: 24px 0;">
    <a href="{reset_url}" style="background: #4f46e5; color: #fff; padding: 12px 24px; border-radius: 8px; text-decoration: none; display: inline-block;">パスワードを再設定する</a>
  </p>
  <p style="color: #6b7280; font-size: 12px;">ボタンが機能しない場合は次のURLをブラウザに貼り付けてください:<br>{reset_url}</p>
  <p style="color: #6b7280; font-size: 12px;">このリクエストに心当たりがない場合は、このメールを無視してください。パスワードは変更されません。</p>
</div>"#
    );

    let body = json!({
        "from": from,
        "to": [to],
        "subject": "【介護BI】パスワード再設定のご案内",
        "html": html,
    });

    let client = reqwest::Client::new();
    let resp = client
        .post(RESEND_API_URL)
        .bearer_auth(&api_key)
        .json(&body)
        .send()
        .await
        .map_err(|e| format!("Resend API接続エラー: {}", e))?;

    let status = resp.status();
    if !status.is_success() {
        let err_body = resp.text().await.unwrap_or_default();
        return Err(format!("Resend APIエラー ({}): {}", status, err_body));
    }

    tracing::info!("パスワードリセットメール送信完了: {}", to);
    Ok(())
}
