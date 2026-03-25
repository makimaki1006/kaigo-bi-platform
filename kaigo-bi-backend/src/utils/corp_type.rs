/// 法人名から法人種別を推定するユーティリティ
/// 法人名に含まれるキーワードから種別を分類
///
/// 判定ロジック:
/// 1. 法人格キーワード（contains）で判定: 営利法人、社会福祉法人、医療法人等
/// 2. 行政区画名（ends_with）で判定: 法人名の末尾が市/区/町/村/県/都/府/道で終わる場合のみ
///    → contains("市")だと「市来」「市川」等の人名を含む法人名を誤分類するため

/// 法人種別の列挙
/// フロントエンドのドーナツチャート等で使用
pub fn classify_corp_type(corp_name: &str) -> &'static str {
    let name = corp_name.trim();

    // 営利法人（株式会社、有限会社、合同会社、合資会社）
    if name.contains("株式会社") || name.contains("有限会社")
        || name.contains("合同会社") || name.contains("合資会社")
    {
        "営利法人"
    // 社会福祉法人（社会医療法人より先に判定）
    } else if name.contains("社会福祉法人") {
        "社会福祉法人"
    // 社会医療法人（医療法人より先に判定）
    } else if name.contains("社会医療法人") {
        "社会医療法人"
    // 医療法人
    } else if name.contains("医療法人") {
        "医療法人"
    // NPO法人
    } else if name.contains("特定非営利活動法人") || name.contains("NPO") || name.contains("ＮＰＯ") {
        "NPO法人"
    // 社団法人
    } else if name.contains("社団法人") || name.contains("一般社団") {
        "社団法人"
    // 財団法人
    } else if name.contains("財団法人") || name.contains("一般財団") {
        "財団法人"
    // 地方公共団体等（事業団、広域連合）
    } else if name.contains("事業団") || name.contains("広域連合") || name.contains("地方公共団体") {
        "地方公共団体"
    // 地方公共団体（法人名の末尾が行政区画名で終わる場合のみ）
    // ends_withを使うことで「市来」「市川」等の人名含む法人名の誤分類を防止
    } else if name.ends_with("市") || name.ends_with("区") || name.ends_with("町")
        || name.ends_with("村") || name.ends_with("県") || name.ends_with("都")
        || name.ends_with("府") || name.ends_with("道")
    {
        "地方公共団体"
    } else {
        "その他"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_corp_type() {
        assert_eq!(classify_corp_type("社会福祉法人多摩同胞会"), "社会福祉法人");
        assert_eq!(classify_corp_type("医療法人社団健友会"), "医療法人");
        assert_eq!(classify_corp_type("株式会社カンケイ舎"), "営利法人");
        assert_eq!(classify_corp_type("有限会社ケアサポート"), "営利法人");
        assert_eq!(classify_corp_type("合同会社ケアサービス"), "営利法人");
        assert_eq!(classify_corp_type("特定非営利活動法人ふれあい"), "NPO法人");
        assert_eq!(classify_corp_type("一般社団法人高齢者支援"), "社団法人");
        assert_eq!(classify_corp_type("不明な法人"), "その他");
    }

    #[test]
    fn test_classify_corp_type_local_government() {
        // 末尾が行政区画名で終わる場合のみ「地方公共団体」
        assert_eq!(classify_corp_type("横浜市"), "地方公共団体");
        assert_eq!(classify_corp_type("世田谷区"), "地方公共団体");
        assert_eq!(classify_corp_type("箱根町"), "地方公共団体");
        assert_eq!(classify_corp_type("檜原村"), "地方公共団体");
    }

    #[test]
    fn test_classify_corp_type_no_false_positive() {
        // 「市」「区」「町」「村」を含むが末尾でない場合は「その他」
        // 修正前: contains("市")で誤って「地方公共団体」に分類されていた
        assert_eq!(classify_corp_type("市来ケアサービス"), "その他");
        assert_eq!(classify_corp_type("市川介護センター"), "その他");
        assert_eq!(classify_corp_type("村上福祉会"), "その他");
        assert_eq!(classify_corp_type("町田デイサービス"), "その他");
    }

    #[test]
    fn test_classify_corp_type_jigyodan() {
        // 事業団、広域連合は地方公共団体
        assert_eq!(classify_corp_type("東京都社会福祉事業団"), "地方公共団体");
        assert_eq!(classify_corp_type("○○広域連合"), "地方公共団体");
    }
}
