/// フィルタパラメータ
/// クエリパラメータからデシリアライズしてDataStoreのフィルタに使用

use serde::Deserialize;

/// 共通フィルタパラメータ
/// 全てのAPIエンドポイントで使用可能
#[derive(Debug, Clone, Deserialize, Default)]
pub struct FilterParams {
    /// 都道府県（カンマ区切りで複数指定可: "東京都,神奈川県"）
    pub prefecture: Option<String>,
    /// サービスコード（カンマ区切りで複数指定可）
    pub service_code: Option<String>,
    /// 法人種別（カンマ区切りで複数指定可: "営利法人,社会福祉法人"）
    pub corp_type: Option<String>,
    /// 従業者数の最小値
    pub staff_min: Option<f64>,
    /// 従業者数の最大値
    pub staff_max: Option<f64>,
    /// キーワード（住所・事業所名の部分一致）
    pub keyword: Option<String>,
}

/// 検索パラメータ（施設検索用）
/// 注意: serde_urlencoded（Axum Queryのデフォルト）は #[serde(flatten)] をサポートしないため、
/// FilterParamsのフィールドを直接定義している
#[derive(Debug, Clone, Deserialize)]
pub struct SearchParams {
    /// 都道府県（カンマ区切りで複数指定可）
    pub prefecture: Option<String>,
    /// サービスコード（カンマ区切りで複数指定可）
    pub service_code: Option<String>,
    /// 法人種別（カンマ区切りで複数指定可）
    pub corp_type: Option<String>,
    /// 従業者数の最小値
    pub staff_min: Option<f64>,
    /// 従業者数の最大値
    pub staff_max: Option<f64>,
    /// テキスト検索（事業所名/法人名/電話番号の部分一致）
    pub q: Option<String>,
    /// ソートカラム名
    pub sort_by: Option<String>,
    /// ソート順序（"asc" or "desc"、デフォルト: "asc"）
    pub sort_order: Option<String>,
    /// ページ番号（1始まり、デフォルト: 1）
    pub page: Option<usize>,
    /// 1ページあたりの件数（デフォルト: 50）
    pub per_page: Option<usize>,
}

impl SearchParams {
    /// FilterParamsに変換するヘルパー
    pub fn to_filter_params(&self) -> FilterParams {
        FilterParams {
            prefecture: self.prefecture.clone(),
            service_code: self.service_code.clone(),
            corp_type: self.corp_type.clone(),
            staff_min: self.staff_min,
            staff_max: self.staff_max,
            keyword: None, // SearchParamsではqを使用するため
        }
    }
}

impl FilterParams {
    /// 都道府県フィルタ値をVecに分割
    pub fn prefecture_list(&self) -> Option<Vec<String>> {
        self.prefecture.as_ref().map(|s| {
            s.split(',')
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
                .collect()
        })
    }

    /// サービスコードフィルタ値をVecに分割
    pub fn service_code_list(&self) -> Option<Vec<String>> {
        self.service_code.as_ref().map(|s| {
            s.split(',')
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
                .collect()
        })
    }

    /// フィルタが未指定（デフォルト状態）かどうかを判定する
    /// キャッシュヒット判定に使用: trueならグローバルキャッシュを返せる
    pub fn is_default(&self) -> bool {
        self.prefecture.is_none()
            && self.service_code.is_none()
            && self.corp_type.is_none()
            && self.keyword.is_none()
            && self.staff_min.is_none()
            && self.staff_max.is_none()
    }

    /// 法人種別フィルタ値をVecに分割
    pub fn corp_type_list(&self) -> Option<Vec<String>> {
        self.corp_type.as_ref().map(|s| {
            s.split(',')
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
                .collect()
        })
    }
}
