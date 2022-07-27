use std::time::Duration;

use bb8::Pool;
use bb8_postgres::{tokio_postgres::NoTls, PostgresConnectionManager};

use actix_web::{get, web::Data, HttpResponse, Responder};

use serde::{Deserialize, Serialize};

use utoipa::Component;

#[derive(Debug, Serialize, Deserialize, Clone, Component)]
pub struct TrendMaterializationSource {
    pub trend_store_part: String,
    pub mapping_function: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, Component)]
pub struct TrendViewMaterialization {
    pub target_trend_store_part: String,
    pub enabled: bool,
    #[serde(with = "humantime_serde")]
    pub processing_delay: Duration,
    #[serde(with = "humantime_serde")]
    pub stability_delay: Duration,
    #[serde(with = "humantime_serde")]
    pub reprocessing_period: Duration,
    pub sources: Vec<TrendMaterializationSource>,
    pub view: String,
    pub fingerprint_function: String,
}

/// Get list of trend view materializations.
///
/// List trend view materializations from Minerva database.
///
/// One could call the api endpoint with following curl command.
/// ```text
/// curl localhost:8080/trend-view-materializations
/// ```
#[utoipa::path(
    responses(
        (status = 200, description = "List current trend view materialization items", body = [TrendViewMaterialization])
    )
)]
#[get("/trend-view-materializations")]
pub(super) async fn get_trend_view_materializations(
    _pool: Data<Pool<PostgresConnectionManager<NoTls>>>,
) -> impl Responder {
    let m = vec![TrendViewMaterialization {
        target_trend_store_part: "some_trend_store_part".to_string(),
        enabled: true,
        processing_delay: Duration::default(),
        stability_delay: Duration::default(),
        reprocessing_period: Duration::default(),
        sources: Vec::new(),
        view: "".to_string(),
        fingerprint_function: "".to_string(),
    }];

    HttpResponse::Ok().json(m)
}
