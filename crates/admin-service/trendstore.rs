use lazy_static::lazy_static;
use std::collections::HashMap;
use std::time::Duration;

use bb8::Pool;
use bb8_postgres::{tokio_postgres::NoTls, PostgresConnectionManager};
use tokio_postgres::Client;

use actix_web::{get, post, web::Data, web::Path, web::Query, HttpResponse, Responder};

use serde::{Deserialize, Serialize};
use utoipa::{Component, IntoParams};

use minerva::change::Change;
use minerva::interval::parse_interval;
use minerva::trend_store::{
    load_trend_store, AddTrendStore, AddTrendStorePart, AddTrends, GeneratedTrend, Trend,
    TrendStore, TrendStorePart,
};

use super::error::Error;

lazy_static! {
    static ref PARTITION_SIZE: HashMap<Duration, Duration> = HashMap::from([
        (
            parse_interval("15m").unwrap(),
            parse_interval("1d").unwrap()
        ),
        (parse_interval("1h").unwrap(), parse_interval("4d").unwrap()),
        (
            parse_interval("1d").unwrap(),
            parse_interval("3mons").unwrap()
        ),
        (parse_interval("1w").unwrap(), parse_interval("1y").unwrap()),
        (
            parse_interval("1mon").unwrap(),
            parse_interval("5y").unwrap()
        ),
    ]);
}

#[derive(Debug, Serialize, Deserialize, IntoParams)]
pub struct QueryData {
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, Component)]
pub struct TrendFull {
    pub id: i32,
    pub trend_store_part: i32,
    pub name: String,
    pub data_type: String,
    pub time_aggregation: String,
    pub entity_aggregation: String,
    pub extra_data: serde_json::Value,
    pub description: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, Component)]
pub struct TrendData {
    pub name: String,
    pub data_type: String,
    pub time_aggregation: String,
    pub entity_aggregation: String,
    pub extra_data: serde_json::Value,
    pub description: String,
}

impl TrendData {
    fn as_minerva(&self) -> Trend {
        Trend {
            name: self.name.clone(),
            data_type: self.data_type.clone(),
            description: self.description.clone(),
            time_aggregation: self.time_aggregation.clone(),
            entity_aggregation: self.entity_aggregation.clone(),
            extra_data: self.extra_data.clone(),
        }
    }
}

impl TrendFull {
    fn data(&self) -> TrendData {
        TrendData {
            name: self.name.clone(),
            data_type: self.data_type.clone(),
            time_aggregation: self.time_aggregation.clone(),
            entity_aggregation: self.entity_aggregation.clone(),
            extra_data: self.extra_data.clone(),
            description: self.description.clone(),
        }
    }

    fn as_minerva(&self) -> Trend {
        self.data().as_minerva()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Component)]
pub struct GeneratedTrendFull {
    pub id: i32,
    pub trend_store_part: i32,
    pub name: String,
    pub data_type: String,
    pub expression: String,
    pub extra_data: serde_json::Value,
    pub description: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, Component)]
pub struct GeneratedTrendData {
    pub name: String,
    pub data_type: String,
    pub expression: String,
    pub extra_data: serde_json::Value,
    pub description: String,
}

impl GeneratedTrendData {
    fn as_minerva(&self) -> GeneratedTrend {
        GeneratedTrend {
            name: self.name.clone(),
            data_type: self.data_type.clone(),
            description: self.description.clone(),
            expression: self.expression.clone(),
            extra_data: self.extra_data.clone(),
        }
    }
}

impl GeneratedTrendFull {
    fn data(&self) -> GeneratedTrendData {
        GeneratedTrendData {
            name: self.name.clone(),
            data_type: self.data_type.clone(),
            expression: self.expression.clone(),
            extra_data: self.extra_data.clone(),
            description: self.description.clone(),
        }
    }

    fn as_minerva(&self) -> GeneratedTrend {
        self.data().as_minerva()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Component)]
pub struct TrendStorePartFull {
    pub id: i32,
    pub name: String,
    pub trend_store: i32,
    pub trends: Vec<TrendFull>,
    pub generated_trends: Vec<GeneratedTrendFull>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Component)]
pub struct TrendStorePartData {
    pub name: String,
    pub trends: Vec<TrendData>,
    pub generated_trends: Vec<GeneratedTrendData>,
}

impl TrendStorePartData {
    fn as_minerva(&self) -> TrendStorePart {
        let mut trends: Vec<Trend> = vec![];
        for trend in &self.trends {
            trends.push(trend.as_minerva())
        }
        let mut generated_trends: Vec<GeneratedTrend> = vec![];
        for generated_trend in &self.generated_trends {
            generated_trends.push(generated_trend.as_minerva())
        }
        TrendStorePart {
            name: self.name.clone(),
            trends: trends,
            generated_trends: generated_trends,
        }
    }
}

impl TrendStorePartFull {
    fn data(&self) -> TrendStorePartData {
        let mut trends: Vec<TrendData> = vec![];
        for trend in &self.trends {
            trends.push(trend.data())
        }
        let mut generated_trends: Vec<GeneratedTrendData> = vec![];
        for generated_trend in &self.generated_trends {
            generated_trends.push(generated_trend.data())
        }
        TrendStorePartData {
            name: self.name.clone(),
            trends: trends,
            generated_trends: generated_trends,
        }
    }

    fn as_minerva(&self) -> TrendStorePart {
        self.data().as_minerva()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Component)]
pub struct TrendStoreFull {
    pub id: i32,
    pub entity_type: String,
    pub data_source: String,
    #[serde(with = "humantime_serde")]
    pub granularity: Duration,
    #[serde(with = "humantime_serde")]
    pub partition_size: Duration,
    #[serde(with = "humantime_serde")]
    pub retention_period: Duration,
    pub trend_store_parts: Vec<TrendStorePartFull>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Component)]
pub struct TrendStoreBasicData {
    pub entity_type: String,
    pub data_source: String,
    #[serde(with = "humantime_serde")]
    pub granularity: Duration,
}

impl TrendStoreBasicData {
    async fn as_minerva(&self, client: &mut Client) -> Result<TrendStore, String> {
        let result = load_trend_store(
            client,
            &self.data_source,
            &self.entity_type,
            &self.granularity,
        )
        .await;
        match result {
            Ok(trendstore) => Ok(trendstore),
            Err(_) => {
                let new_trend_store = TrendStore {
                    data_source: self.data_source.clone(),
                    entity_type: self.entity_type.clone(),
                    granularity: self.granularity.clone(),
                    partition_size: *PARTITION_SIZE.get(&self.granularity.clone()).unwrap(),
                    parts: vec![],
                };
                let result = AddTrendStore {
                    trend_store: new_trend_store.clone(),
                }
                .apply(client)
                .await;
                match result {
                    Ok(_) => Ok(new_trend_store),
                    Err(e) => Err(format!(
                        "Unable to find or create trend store: {}",
                        e.to_string()
                    )),
                }
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Component)]
pub struct TrendStorePartCompleteData {
    pub name: String,
    pub entity_type: String,
    pub data_source: String,
    #[serde(with = "humantime_serde")]
    pub granularity: Duration,
    #[serde(with = "humantime_serde")]
    pub partition_size: Duration,
    pub trends: Vec<TrendData>,
    pub generated_trends: Vec<GeneratedTrendData>,
}

impl TrendStorePartCompleteData {
    fn trend_store(&self) -> TrendStoreBasicData {
        TrendStoreBasicData {
            entity_type: self.entity_type.clone(),
            data_source: self.data_source.clone(),
            granularity: self.granularity.clone(),
        }
    }

    fn trend_store_part(&self) -> TrendStorePartData {
        let mut trends: Vec<TrendData> = vec![];
        for trend in &self.trends {
            trends.push(trend.clone())
        }
        let mut generated_trends: Vec<GeneratedTrendData> = vec![];
        for generated_trend in &self.generated_trends {
            generated_trends.push(generated_trend.clone())
        }
        TrendStorePartData {
            name: self.name.clone(),
            trends: trends,
            generated_trends: generated_trends,
        }
    }

    pub async fn create(&self, client: &mut Client) -> Result<TrendStorePartFull, Error> {
        let trendstore_query = self.trend_store().as_minerva(client).await;
        match trendstore_query {
            Err(e) => Err(Error {
                code: 409,
                message: e.to_string(),
            }),
            Ok(trendstore) => {
                let action = AddTrendStorePart {
                    trend_store: trendstore,
                    trend_store_part: self.trend_store_part().as_minerva(),
                };
                let result = action.apply(client).await;
                match result {
                    Err(e) => Err(Error {
                        code: 409,
                        message: format!("Creation of trendstorepart failed: {}", e.to_string()),
                    }),
                    Ok(_) => {
                        let action = AddTrends {
                            trend_store_part: self.trend_store_part().as_minerva(),
                            trends: self.trend_store_part().as_minerva().trends,
                        };
                        let result = action.apply(client).await;
                        match result {
			    Err(e) => Err( Error {
				code: 409,
				message: format!("Creation of trendstorepart succeeded, but inserting trends failed: {}", e.to_string())
			    }),
			    Ok(_) => {
				let query_result = client.query_one("SELECT id, trend_store_id FROM trend_directory.trend_store_part WHERE name=$1", &[&self.name],).await;
				match query_result {
				    Ok(row) => {
					let id: i32 = row.get(0);
					let mut trends: Vec<TrendFull> = vec![];
					let query = client.query("SELECT id, trend_store_part_id, name, data_type, time_aggregation, entity_aggregation, extra_data, description FROM trend_directory.table_trend WHERE trend_store_part_id=$1", &[&id]).await;
					match query {
					    Err(e) => Err( Error { code: 500, message: e.to_string() } ),
					    Ok(result) => {
						for inner_row in result {
						    let trend = TrendFull {
							id: inner_row.get(0),
							trend_store_part: inner_row.get(1),
							name: inner_row.get(2),
							data_type: inner_row.get(3),
							time_aggregation: inner_row.get(4),
							entity_aggregation: inner_row.get(5),
							extra_data: inner_row.get(6),
							description: inner_row.get(7)
						    };
						    trends.push(trend)
						}
						let mut generated_trends: Vec<GeneratedTrendFull> = vec![];
						let query = client.query("SELECT id, trend_store_part_id, name, data_type, expression, extra_data, description FROM trend_directory.generated_table_trend WHERE trend_store_part_id=$1", &[&id]).await;
						match query {
						    Err(e) => Err( Error { code: 500, message: e.to_string() }),
						    Ok(query_result) => {
							for inner_row in query_result {
							    let trend = GeneratedTrendFull {
								id: inner_row.get(0),
								trend_store_part: inner_row.get(1),
								name: inner_row.get(2),
								data_type: inner_row.get(3),
								expression: inner_row.get(4),
								extra_data: inner_row.get(5),
								description: inner_row.get(6)
							    };
							    generated_trends.push(trend)
							};
							let trendstorepart = TrendStorePartFull {
							    id: id,
							    name: self.name.to_string(),
							    trend_store: row.get(1),
							    trends: trends,
							    generated_trends: generated_trends,
							};
							Ok(trendstorepart)
						    }
						}
					    }
					}
				    },
				    Err(_) => Err( Error {
					code: 404,
					message: "Trend store part created, but could not be found after creation".to_string()
				    })
				}
			    }
			}
                    }
                }
            }
        }
    }
}

#[utoipa::path(
    responses(
	(status = 200, description = "List all trend store parts", body = [TrendStorePartFull]),
	(status = 500, description = "Problem interacting with database", body = Error),
    )
)]
#[get("/trend-store-parts")]

pub(super) async fn get_trend_store_parts(
    pool: Data<Pool<PostgresConnectionManager<NoTls>>>,
) -> impl Responder {
    let mut m: Vec<TrendStorePartFull> = vec![];

    let result = pool.get().await;
    match result {
        Err(e) => HttpResponse::InternalServerError().json(Error {
            code: 500,
            message: e.to_string(),
        }),
        Ok(client) => {
            let mut trends: Vec<TrendFull> = vec![];
            let query = client.query("SELECT id, trend_store_part_id, name, data_type, time_aggregation, entity_aggregation, extra_data, description FROM trend_directory.table_trend", &[]).await;
            match query {
                Err(e) => HttpResponse::InternalServerError().json(Error {
                    code: 500,
                    message: e.to_string(),
                }),
                Ok(query_result) => {
                    for row in query_result {
                        let trend = TrendFull {
                            id: row.get(0),
                            trend_store_part: row.get(1),
                            name: row.get(2),
                            data_type: row.get(3),
                            time_aggregation: row.get(4),
                            entity_aggregation: row.get(5),
                            extra_data: row.get(6),
                            description: row.get(7),
                        };
                        trends.push(trend)
                    }

                    let mut generated_trends: Vec<GeneratedTrendFull> = vec![];
                    let query = client.query("SELECT id, trend_store_part_id, name, data_type, expression, extra_data, description FROM trend_directory.generated_table_trend", &[]).await;
                    match query {
                        Err(e) => HttpResponse::InternalServerError().json(Error {
                            code: 500,
                            message: e.to_string(),
                        }),
                        Ok(query_result) => {
                            for row in query_result {
                                let trend = GeneratedTrendFull {
                                    id: row.get(0),
                                    trend_store_part: row.get(1),
                                    name: row.get(2),
                                    data_type: row.get(3),
                                    expression: row.get(4),
                                    extra_data: row.get(5),
                                    description: row.get(6),
                                };
                                generated_trends.push(trend)
                            }

                            let query = client.query(
				"SELECT id, name, trend_store_id FROM trend_directory.trend_store_part", &[],
			    ).await;
                            match query {
                                Err(e) => HttpResponse::InternalServerError().json(Error {
                                    code: 500,
                                    message: e.to_string(),
                                }),
                                Ok(query_result) => {
                                    for row in query_result {
                                        let tspid: i32 = row.get(0);

                                        let mut my_trends: Vec<TrendFull> = vec![];
                                        for trend in &trends {
                                            if trend.trend_store_part == tspid {
                                                my_trends.push(trend.clone())
                                            }
                                        }

                                        let mut my_generated_trends: Vec<GeneratedTrendFull> =
                                            vec![];
                                        for generated_trend in &generated_trends {
                                            if generated_trend.trend_store_part == tspid {
                                                my_generated_trends.push(generated_trend.clone())
                                            }
                                        }

                                        let trendstorepart = TrendStorePartFull {
                                            id: tspid,
                                            name: row.get(1),
                                            trend_store: row.get(2),
                                            trends: my_trends,
                                            generated_trends: my_generated_trends,
                                        };
                                        m.push(trendstorepart)
                                    }

                                    HttpResponse::Ok().json(m)
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

#[utoipa::path(
    responses(
	(status = 200, description = "Get a specific trend store part", body = TrendStorePartFull),
	(status = 404, description = "Trend store part not found", body = Error),
	(status = 500, description = "Failure to interact with database", body = Error)
    )
)]
#[get("/trend-store-part/{id}")]
pub(super) async fn get_trend_store_part(
    pool: Data<Pool<PostgresConnectionManager<NoTls>>>,
    id: Path<i32>,
) -> impl Responder {
    let tsp_id = id.into_inner();

    let result = pool.get().await;
    match result {
        Err(e) => HttpResponse::InternalServerError().json(Error {
            code: 500,
            message: e.to_string(),
        }),
        Ok(client) => {
            let query_result = client
                .query_one(
                    "SELECT name, trend_store_id FROM trend_directory.trend_store_part WHERE id=$1",
                    &[&tsp_id],
                )
                .await;

            match query_result {
                Ok(row) => {
                    let mut trends: Vec<TrendFull> = vec![];
                    let query = client.query("SELECT id, trend_store_part_id, name, data_type, time_aggregation, entity_aggregation, extra_data, description FROM trend_directory.table_trend WHERE trend_store_part_id=$1", &[&tsp_id]).await;
                    match query {
                        Err(e) => HttpResponse::InternalServerError().json(Error {
                            code: 500,
                            message: e.to_string(),
                        }),
                        Ok(query_result) => {
                            for inner_row in query_result {
                                let trend = TrendFull {
                                    id: inner_row.get(0),
                                    trend_store_part: inner_row.get(1),
                                    name: inner_row.get(2),
                                    data_type: inner_row.get(3),
                                    time_aggregation: inner_row.get(4),
                                    entity_aggregation: inner_row.get(5),
                                    extra_data: inner_row.get(6),
                                    description: inner_row.get(7),
                                };
                                trends.push(trend)
                            }
                            let mut generated_trends: Vec<GeneratedTrendFull> = vec![];
                            let query = client.query("SELECT id, trend_store_part_id, name, data_type, expression, extra_data, description FROM trend_directory.generated_table_trend WHERE trend_store_part_id=$1", &[&tsp_id]).await;
                            match query {
                                Err(e) => HttpResponse::InternalServerError().json(Error {
                                    code: 500,
                                    message: e.to_string(),
                                }),
                                Ok(query_result) => {
                                    for inner_row in query_result {
                                        let trend = GeneratedTrendFull {
                                            id: inner_row.get(0),
                                            trend_store_part: inner_row.get(1),
                                            name: inner_row.get(2),
                                            data_type: inner_row.get(3),
                                            expression: inner_row.get(4),
                                            extra_data: inner_row.get(5),
                                            description: inner_row.get(6),
                                        };
                                        generated_trends.push(trend)
                                    }

                                    let trendstorepart = TrendStorePartFull {
                                        id: tsp_id,
                                        name: row.get(0),
                                        trend_store: row.get(1),
                                        trends: trends,
                                        generated_trends: generated_trends,
                                    };

                                    HttpResponse::Ok().json(trendstorepart)
                                }
                            }
                        }
                    }
                }
                Err(_e) => HttpResponse::NotFound().json(Error {
                    code: 404,
                    message: format!("Trend store part with id {} not found", &tsp_id),
                }),
            }
        }
    }
}

#[utoipa::path(
    responses(
	(status = 200, description = "Get a specific trend store part", body = TrendStorePartFull),
	(status = 404, description = "Trend store part not found", body = Error),
	(status = 500, description = "Unable to interact with database", body = Error),
    )
)]
#[get("/trend-store-parts/find")]
pub(super) async fn find_trend_store_part(
    pool: Data<Pool<PostgresConnectionManager<NoTls>>>,
    info: Query<QueryData>,
) -> impl Responder {
    let name = &info.name;
    let result = pool.get().await;
    match result {
        Err(e) => HttpResponse::InternalServerError().json(Error {
            code: 500,
            message: e.to_string(),
        }),
        Ok(client) => {
            let query_result = client
                .query_one(
                    "SELECT id, trend_store_id FROM trend_directory.trend_store_part WHERE name=$1",
                    &[&name],
                )
                .await;

            match query_result {
                Ok(row) => {
                    let id: i32 = row.get(0);
                    let mut trends: Vec<TrendFull> = vec![];
                    let query = client.query("SELECT id, trend_store_part_id, name, data_type, time_aggregation, entity_aggregation, extra_data, description FROM trend_directory.table_trend WHERE trend_store_part_id=$1", &[&id]).await;
                    match query {
                        Err(e) => HttpResponse::InternalServerError().json(Error {
                            code: 500,
                            message: e.to_string(),
                        }),
                        Ok(query_result) => {
                            for inner_row in query_result {
                                let trend = TrendFull {
                                    id: inner_row.get(0),
                                    trend_store_part: inner_row.get(1),
                                    name: inner_row.get(2),
                                    data_type: inner_row.get(3),
                                    time_aggregation: inner_row.get(4),
                                    entity_aggregation: inner_row.get(5),
                                    extra_data: inner_row.get(6),
                                    description: inner_row.get(7),
                                };
                                trends.push(trend)
                            }

                            let mut generated_trends: Vec<GeneratedTrendFull> = vec![];
                            let query = client.query("SELECT id, trend_store_part_id, name, data_type, expression, extra_data, description FROM trend_directory.generated_table_trend WHERE trend_store_part_id=$1", &[&id]).await;
                            match query {
                                Err(e) => HttpResponse::InternalServerError().json(Error {
                                    code: 500,
                                    message: e.to_string(),
                                }),
                                Ok(query_result) => {
                                    for inner_row in query_result {
                                        let trend = GeneratedTrendFull {
                                            id: inner_row.get(0),
                                            trend_store_part: inner_row.get(1),
                                            name: inner_row.get(2),
                                            data_type: inner_row.get(3),
                                            expression: inner_row.get(4),
                                            extra_data: inner_row.get(5),
                                            description: inner_row.get(6),
                                        };
                                        generated_trends.push(trend)
                                    }

                                    let trendstorepart = TrendStorePartFull {
                                        id: id,
                                        name: name.to_string(),
                                        trend_store: row.get(1),
                                        trends: trends,
                                        generated_trends: generated_trends,
                                    };

                                    HttpResponse::Ok().json(trendstorepart)
                                }
                            }
                        }
                    }
                }
                Err(_e) => HttpResponse::NotFound().json(Error {
                    code: 404,
                    message: format!("Trend store part with name {} not found", &name),
                }),
            }
        }
    }
}

#[utoipa::path(
    responses(
	(status = 200, description = "List all trend store parts", body = [TrendStorePartFull]),
	(status = 500, description = "Problems interacting with database", body = Error),
    )
)]
#[get("/trend-stores")]
pub(super) async fn get_trend_stores(
    pool: Data<Pool<PostgresConnectionManager<NoTls>>>,
) -> impl Responder {
    let mut m: Vec<TrendStoreFull> = vec![];

    let result = pool.get().await;
    match result {
        Err(e) => HttpResponse::InternalServerError().json(Error {
            code: 500,
            message: e.to_string(),
        }),
        Ok(client) => {
            let mut trends: Vec<TrendFull> = vec![];
            let query = client.query("SELECT id, trend_store_part_id, name, data_type, time_aggregation, entity_aggregation, extra_data, description FROM trend_directory.table_trend", &[]).await;
            match query {
                Err(e) => HttpResponse::InternalServerError().json(Error {
                    code: 500,
                    message: e.to_string(),
                }),
                Ok(query_result) => {
                    for row in query_result {
                        let trend = TrendFull {
                            id: row.get(0),
                            trend_store_part: row.get(1),
                            name: row.get(2),
                            data_type: row.get(3),
                            time_aggregation: row.get(4),
                            entity_aggregation: row.get(5),
                            extra_data: row.get(6),
                            description: row.get(7),
                        };
                        trends.push(trend)
                    }

                    let mut generated_trends: Vec<GeneratedTrendFull> = vec![];
                    let query = client.query("SELECT id, trend_store_part_id, name, data_type, expression, extra_data, description FROM trend_directory.generated_table_trend", &[]).await;
                    match query {
                        Err(e) => HttpResponse::InternalServerError().json(Error {
                            code: 500,
                            message: e.to_string(),
                        }),
                        Ok(query_result) => {
                            for row in query_result {
                                let trend = GeneratedTrendFull {
                                    id: row.get(0),
                                    trend_store_part: row.get(1),
                                    name: row.get(2),
                                    data_type: row.get(3),
                                    expression: row.get(4),
                                    extra_data: row.get(5),
                                    description: row.get(6),
                                };
                                generated_trends.push(trend)
                            }

                            let mut parts: Vec<TrendStorePartFull> = vec![];
                            let query = client.query(
				    "SELECT id, name, trend_store_id FROM trend_directory.trend_store_part",
				    &[],
				).await;
                            match query {
                                Err(e) => HttpResponse::InternalServerError().json(Error {
                                    code: 500,
                                    message: e.to_string(),
                                }),
                                Ok(query_result) => {
                                    for row in query_result {
                                        let tspid: i32 = row.get(0);
                                        let mut my_trends: Vec<TrendFull> = vec![];
                                        for trend in &trends {
                                            if trend.trend_store_part == tspid {
                                                my_trends.push(trend.clone())
                                            }
                                        }

                                        let mut my_generated_trends: Vec<GeneratedTrendFull> =
                                            vec![];
                                        for generated_trend in &generated_trends {
                                            if generated_trend.trend_store_part == tspid {
                                                my_generated_trends.push(generated_trend.clone())
                                            }
                                        }

                                        let trendstorepart = TrendStorePartFull {
                                            id: tspid,
                                            name: row.get(1),
                                            trend_store: row.get(2),
                                            trends: my_trends,
                                            generated_trends: my_generated_trends,
                                        };
                                        parts.push(trendstorepart)
                                    }
                                    let query =  client.query("SELECT ts.id, entity_type.name, data_source.name, granularity::text, partition_size::text, retention_period::text FROM trend_directory.trend_store ts JOIN directory.entity_type ON ts.entity_type_id = entity_type.id JOIN directory.data_source ON ts.data_source_id = data_source.id", &[]).await;
                                    match query {
                                        Err(e) => HttpResponse::InternalServerError().json(Error {
                                            code: 500,
                                            message: e.to_string(),
                                        }),
                                        Ok(query_result) => {
                                            for row in query_result {
                                                let tsid: i32 = row.get(0);
                                                let mut my_parts: Vec<TrendStorePartFull> = vec![];
                                                for part in &parts {
                                                    if part.trend_store == tsid {
                                                        my_parts.push(part.clone())
                                                    }
                                                }

                                                let trendstore = TrendStoreFull {
                                                    id: tsid,
                                                    entity_type: row.get(1),
                                                    data_source: row.get(2),
                                                    granularity: parse_interval(row.get(3))
                                                        .unwrap(),
                                                    partition_size: parse_interval(row.get(4))
                                                        .unwrap(),
                                                    retention_period: parse_interval(row.get(5))
                                                        .unwrap(),
                                                    trend_store_parts: my_parts,
                                                };

                                                m.push(trendstore)
                                            }

                                            HttpResponse::Ok().json(m)
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

#[utoipa::path(
    responses(
	(status = 200, description = "Get a specific trend store", body = TrendStorePartFull),
	(status = 404, description = "Trend store not found", body = Error),
	(status = 500, description = "Unable to interact with database", body = Error),
    )
)]
#[get("/trend-stores/{id}")]
pub(super) async fn get_trend_store(
    pool: Data<Pool<PostgresConnectionManager<NoTls>>>,
    id: Path<i32>,
) -> impl Responder {
    let tsid = id.into_inner();

    let result = pool.get().await;
    match result {
        Err(e) => HttpResponse::InternalServerError().json(Error {
            code: 500,
            message: e.to_string(),
        }),
        Ok(client) => {
            let query_result = client
		.query_one(
		    "SELECT ts.id, entity_type.name, data_source.name, granularity::text, partition_size::text, retention_period::text FROM trend_directory.trend_store ts JOIN directory.entity_type ON ts.entity_type_id = entity_type.id JOIN directory.data_source ON ts.data_source_id = data_source.id WHERE ts.id = $1",
		    &[&tsid],
		)
		.await;

            match query_result {
                Ok(row) => {
                    let mut trends: Vec<TrendFull> = vec![];
                    let query = client.query("SELECT t.id, t.trend_store_part_id, t.name, t.data_type, t.time_aggregation, t.entity_aggregation, t.extra_data, t.description FROM trend_directory.table_trend t JOIN trend_directory.trend_store_part tsp ON t.trend_store_part_id = tsp.id WHERE tsp.trend_store_id = $1", &[&tsid]).await;
                    match query {
                        Err(e) => HttpResponse::InternalServerError().json(Error {
                            code: 500,
                            message: e.to_string(),
                        }),
                        Ok(query_result) => {
                            for inner_row in query_result {
                                let trend = TrendFull {
                                    id: inner_row.get(0),
                                    trend_store_part: inner_row.get(1),
                                    name: inner_row.get(2),
                                    data_type: inner_row.get(3),
                                    time_aggregation: inner_row.get(4),
                                    entity_aggregation: inner_row.get(5),
                                    extra_data: inner_row.get(6),
                                    description: inner_row.get(7),
                                };
                                trends.push(trend)
                            }

                            let mut generated_trends: Vec<GeneratedTrendFull> = vec![];
                            let query = client.query("SELECT t.id, t.trend_store_part_id, t.name, t.data_type, t.expression, t.extra_data, t.description FROM trend_directory.generated_table_trend t JOIN trend_directory.trend_store_part tsp ON t.trend_store_part_id = tsp.id WHERE tsp.trend_store_id = $1", &[&tsid]).await;
                            match query {
                                Err(e) => HttpResponse::InternalServerError().json(Error {
                                    code: 500,
                                    message: e.to_string(),
                                }),
                                Ok(query_result) => {
                                    for inner_row in query_result {
                                        let trend = GeneratedTrendFull {
                                            id: inner_row.get(0),
                                            trend_store_part: inner_row.get(1),
                                            name: inner_row.get(2),
                                            data_type: inner_row.get(3),
                                            expression: inner_row.get(4),
                                            extra_data: inner_row.get(5),
                                            description: inner_row.get(6),
                                        };
                                        generated_trends.push(trend)
                                    }

                                    let mut parts: Vec<TrendStorePartFull> = vec![];

                                    let query = client.query(
					"SELECT id, name, trend_store_id FROM trend_directory.trend_store_part WHERE trend_store_id = $1",
					&[&tsid],
				    ).await;
                                    match query {
                                        Err(e) => HttpResponse::InternalServerError().json(Error {
                                            code: 500,
                                            message: e.to_string(),
                                        }),
                                        Ok(query_result) => {
                                            for inner_row in query_result {
                                                let tspid: i32 = inner_row.get(0);
                                                let mut my_trends: Vec<TrendFull> = vec![];
                                                for trend in &trends {
                                                    if trend.trend_store_part == tspid {
                                                        my_trends.push(trend.clone())
                                                    }
                                                }

                                                let mut my_generated_trends: Vec<
                                                    GeneratedTrendFull,
                                                > = vec![];
                                                for generated_trend in &generated_trends {
                                                    if generated_trend.trend_store_part == tspid {
                                                        my_generated_trends
                                                            .push(generated_trend.clone())
                                                    }
                                                }

                                                let trendstorepart = TrendStorePartFull {
                                                    id: tspid,
                                                    name: inner_row.get(1),
                                                    trend_store: inner_row.get(2),
                                                    trends: my_trends,
                                                    generated_trends: my_generated_trends,
                                                };
                                                parts.push(trendstorepart)
                                            }

                                            let trendstore = TrendStoreFull {
                                                id: tsid,
                                                entity_type: row.get(1),
                                                data_source: row.get(2),
                                                granularity: parse_interval(row.get(3)).unwrap(),
                                                partition_size: parse_interval(row.get(4)).unwrap(),
                                                retention_period: parse_interval(row.get(5))
                                                    .unwrap(),
                                                trend_store_parts: parts,
                                            };

                                            HttpResponse::Ok().json(trendstore)
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                Err(_e) => HttpResponse::NotFound().json(Error {
                    code: 404,
                    message: format!("Trend store with id {} not found", &tsid),
                }),
            }
        }
    }
}

// curl -H "Content-Type: application/json" -X POST -d '{"name":"kpi-test_One_15m","entity_type":"test","data_source":"kpi","granularity":"15m","partition_size":"1d","trends":[{"name":"downtime","data_type":"numeric","time_aggregation":"SUM","entity_aggregation":"SUM","extra_data":{},"description":""}],"generated_trends":[]}' localhost:8080/trend-store-parts/new
// curl -H "Content-Type: application/json" -X POST -d '{"name":"kpi-test_Two_15m","entity_type":"test","data_source":"kpi","granularity":"15m","partition_size":"1d","trends":[{"name":"downtime","data_type":"numeric","time_aggregation":"SUM","entity_aggregation":"SUM","extra_data":{},"description":""}],"generated_trends":[]}' localhost:8080/trend-store-parts/new

#[utoipa::path(
    responses(
	(status = 200, description = "Create a new trend store part", body = TrendStorePartData),
	(status = 400, description = "Incorrect data format", body = Error),
	(status = 404, description = "Trend store part cannot be found after creation", body = Error),
	(status = 409, description = "Trend store part cannot be created with these data", body = Error),
	(status = 500, description = "Problems interacting with database", body = Error),
    )
)]
#[post("/trend-store-parts/new")]
pub(super) async fn post_trend_store_part(
    pool: Data<Pool<PostgresConnectionManager<NoTls>>>,
    post: String,
) -> impl Responder {
    let input: Result<TrendStorePartCompleteData, serde_json::Error> = serde_json::from_str(&post);
    match input {
        Err(e) => HttpResponse::BadRequest().json(Error {
            code: 400,
            message: e.to_string(),
        }),
        Ok(data) => {
            let result = pool.get().await;
            match result {
                Err(e) => HttpResponse::InternalServerError().json(Error {
                    code: 500,
                    message: e.to_string(),
                }),
                Ok(mut client) => match data.create(&mut client).await {
                    Ok(tsp) => HttpResponse::Ok().json(tsp),
                    Err(e) => match e.code {
                        404 => HttpResponse::NotFound().json(e),
                        409 => HttpResponse::Conflict().json(e),
                        _ => HttpResponse::InternalServerError().json(e),
                    },
                },
            }
        }
    }
}
