use bb8::Pool;
use bb8_postgres::{tokio_postgres::NoTls, PostgresConnectionManager};

use actix_web::{
    get,
    web::Data,
    HttpResponse, Responder,
};

use serde::{Serialize, Deserialize};

use utoipa::{Component};

#[derive(Debug, Serialize, Deserialize, Clone, Component)]
pub struct Trend {
    pub id: i32,
    pub trend_store_part: i32,
    pub name: String,
    pub data_type: String,
    pub time_aggregation: String,
    pub entity_aggregation: String,
    pub extra_data: String,
    pub description: String
}

#[derive(Debug, Serialize, Deserialize, Clone, Component)]
pub struct GeneratedTrend {
    pub id: i32,
    pub trend_store_part: i32,
    pub name: String,
    pub data_type: String,
    pub expression: String,
    pub time_aggregation: String,
    pub entity_aggregation: String,
    pub extra_data: String,
    pub description: String
}

#[derive(Debug, Serialize, Deserialize, Clone, Component)]
pub struct TrendStorePart {
    pub id: i32,
    pub name: String,
    pub trend_store: i32,
    pub trends: Vec<Trend>,
    pub generated_trends: Vec<GeneratedTrend>
}

#[utoipa::path(
    responses(
	(status = 200, description = "List all trend store parts", body = [TrendStorePart])
    )
)]
#[get("/trend-store-parts")]
pub(super) async fn get_trend_store_parts(_pool: Data<Pool<PostgresConnectionManager<NoTls>>>) -> impl Responder {
    let m: Vec<TrendStorePart> = vec!();

    HttpResponse::Ok().json(m)
}
