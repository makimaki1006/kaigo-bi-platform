/// 住所から都道府県を抽出するユーティリティ
/// 正規表現で47都道府県を判定
/// 住所から抽出できない場合、事業所番号の先頭2桁からフォールバック推定

use regex::Regex;
use std::collections::HashMap;
use std::sync::LazyLock;

/// 都道府県抽出用の正規表現
/// 「北海道」「東京都」「大阪府」「京都府」「〜県」の順でマッチ
static PREFECTURE_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(北海道|東京都|(?:大阪|京都)府|.{2,3}県)").unwrap()
});

/// 事業所番号先頭2桁→都道府県名マッピング
static PREF_CODE_MAP: LazyLock<HashMap<u32, &'static str>> = LazyLock::new(|| {
    let mut m = HashMap::new();
    m.insert(1, "北海道"); m.insert(2, "青森県"); m.insert(3, "岩手県");
    m.insert(4, "宮城県"); m.insert(5, "秋田県"); m.insert(6, "山形県");
    m.insert(7, "福島県"); m.insert(8, "茨城県"); m.insert(9, "栃木県");
    m.insert(10, "群馬県"); m.insert(11, "埼玉県"); m.insert(12, "千葉県");
    m.insert(13, "東京都"); m.insert(14, "神奈川県"); m.insert(15, "新潟県");
    m.insert(16, "富山県"); m.insert(17, "石川県"); m.insert(18, "福井県");
    m.insert(19, "山梨県"); m.insert(20, "長野県"); m.insert(21, "岐阜県");
    m.insert(22, "静岡県"); m.insert(23, "愛知県"); m.insert(24, "三重県");
    m.insert(25, "滋賀県"); m.insert(26, "京都府"); m.insert(27, "大阪府");
    m.insert(28, "兵庫県"); m.insert(29, "奈良県"); m.insert(30, "和歌山県");
    m.insert(31, "鳥取県"); m.insert(32, "島根県"); m.insert(33, "岡山県");
    m.insert(34, "広島県"); m.insert(35, "山口県"); m.insert(36, "徳島県");
    m.insert(37, "香川県"); m.insert(38, "愛媛県"); m.insert(39, "高知県");
    m.insert(40, "福岡県"); m.insert(41, "佐賀県"); m.insert(42, "長崎県");
    m.insert(43, "熊本県"); m.insert(44, "大分県"); m.insert(45, "宮崎県");
    m.insert(46, "鹿児島県"); m.insert(47, "沖縄県");
    m
});

/// 有効な47都道府県名のホワイトリスト
static VALID_PREFECTURES: LazyLock<std::collections::HashSet<&'static str>> = LazyLock::new(|| {
    [
        "北海道","青森県","岩手県","宮城県","秋田県","山形県","福島県",
        "茨城県","栃木県","群馬県","埼玉県","千葉県","東京都","神奈川県",
        "新潟県","富山県","石川県","福井県","山梨県","長野県","岐阜県",
        "静岡県","愛知県","三重県","滋賀県","京都府","大阪府","兵庫県",
        "奈良県","和歌山県","鳥取県","島根県","岡山県","広島県","山口県",
        "徳島県","香川県","愛媛県","高知県","福岡県","佐賀県","長崎県",
        "熊本県","大分県","宮崎県","鹿児島県","沖縄県",
    ].into_iter().collect()
});

/// 住所文字列から都道府県名を抽出する
/// 郵便番号（〒XXX-XXXX）が含まれていても正しく処理する
/// 抽出結果を47都道府県ホワイトリストで検証する
pub fn extract_prefecture(address: &str) -> Option<String> {
    PREFECTURE_RE
        .find(address)
        .map(|m| m.as_str().trim().to_string())
        .filter(|p| VALID_PREFECTURES.contains(p.as_str()))
}

/// 住所から都道府県を抽出し、失敗した場合は事業所番号の先頭2桁から推定する
/// スクレイパーで都道府県が住所に含まれないケースのフォールバック
pub fn extract_prefecture_with_fallback(address: &str, jigyosho_number: &str) -> Option<String> {
    // まず住所から抽出を試みる
    if let Some(pref) = extract_prefecture(address) {
        return Some(pref);
    }
    // フォールバック: 事業所番号の先頭2桁から都道府県を推定
    if jigyosho_number.len() >= 2 {
        let code: u32 = jigyosho_number[..2].parse().ok()?;
        PREF_CODE_MAP.get(&code).map(|s| s.to_string())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_prefecture() {
        // 通常の住所
        assert_eq!(
            extract_prefecture("東京都千代田区岩本町２－１５－３"),
            Some("東京都".to_string())
        );
        // 郵便番号付き
        assert_eq!(
            extract_prefecture("〒101-0032 東京都千代田区岩本町２－１５－３"),
            Some("東京都".to_string())
        );
        // 北海道
        assert_eq!(
            extract_prefecture("北海道札幌市中央区"),
            Some("北海道".to_string())
        );
        // 大阪府
        assert_eq!(
            extract_prefecture("大阪府大阪市北区"),
            Some("大阪府".to_string())
        );
        // 空文字
        assert_eq!(extract_prefecture(""), None);

        // スペース付き住所（先頭スペース混入バグの再発防止）
        assert_eq!(
            extract_prefecture(" 愛知県名古屋市中村区"),
            Some("愛知県".to_string())
        );
        // 全角スペース付き
        assert_eq!(
            extract_prefecture("　神奈川県横浜市"),
            Some("神奈川県".to_string())
        );
    }

    #[test]
    fn test_extract_prefecture_with_fallback() {
        // 住所に都道府県が含まれる場合はそちらを使用
        assert_eq!(
            extract_prefecture_with_fallback("東京都千代田区", "1310100560"),
            Some("東京都".to_string())
        );
        // 住所に都道府県が含まれない場合、事業所番号からフォールバック
        assert_eq!(
            extract_prefecture_with_fallback("千代田区岩本町", "1310100560"),
            Some("東京都".to_string())
        );
        // 事業所番号27→大阪府
        assert_eq!(
            extract_prefecture_with_fallback("大阪市北区", "2710100001"),
            Some("大阪府".to_string())
        );
        // 事業所番号01→北海道
        assert_eq!(
            extract_prefecture_with_fallback("札幌市中央区", "0110100001"),
            Some("北海道".to_string())
        );
        // 両方ない場合はNone
        assert_eq!(
            extract_prefecture_with_fallback("住所不明", "XX"),
            None
        );
    }
}
