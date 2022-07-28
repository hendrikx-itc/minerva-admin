use std::time::Duration;

use bb8::Pool;
use bb8_postgres::{tokio_postgres::NoTls, PostgresConnectionManager};

use actix_web::{get, web::Data, web::Path, HttpResponse, Responder};

use serde::{Deserialize, Serialize};
use utoipa::Component;

use minerva::interval::parse_interval;

#[derive(Debug, Serialize, Deserialize, Clone, Component)]
pub struct Trend {
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
pub struct GeneratedTrend {
    pub id: i32,
    pub trend_store_part: i32,
    pub name: String,
    pub data_type: String,
    pub expression: String,
    pub extra_data: serde_json::Value,
    pub description: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, Component)]
pub struct TrendStorePart {
    pub id: i32,
    pub name: String,
    pub trend_store: i32,
    pub trends: Vec<Trend>,
    pub generated_trends: Vec<GeneratedTrend>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Component)]
pub struct TrendStore {
    pub id: i32,
    pub entity_type: String,
    pub data_source: String,
    #[serde(with = "humantime_serde")]
    pub granularity: Duration,
    #[serde(with = "humantime_serde")]
    pub partition_size: Duration,
    #[serde(with = "humantime_serde")]
    pub retention_period: Duration,
    pub trend_store_parts: Vec<TrendStorePart>,
}

#[utoipa::path(
    responses(
	(status = 200, description = "List all trend store parts", body = [TrendStorePart])
    )
)]
#[get("/trend-store-parts")]
pub(super) async fn get_trend_store_parts(
    pool: Data<Pool<PostgresConnectionManager<NoTls>>>,
) -> impl Responder {
    let mut m: Vec<TrendStorePart> = vec![];

    let client = pool.get().await.unwrap();
    let mut trends: Vec<Trend> = vec![];
    for row in client.query("SELECT id, trend_store_part_id, name, data_type, time_aggregation, entity_aggregation, extra_data, description FROM trend_directory.table_trend", &[]).await.unwrap() {
	let trend = Trend {
	    id: row.get(0),
	    trend_store_part: row.get(1),
	    name: row.get(2),
	    data_type: row.get(3),
	    time_aggregation: row.get(4),
	    entity_aggregation: row.get(5),
	    extra_data: row.get(6),
	    description: row.get(7)
	};
	trends.push(trend)
    };

    let mut generated_trends: Vec<GeneratedTrend> = vec![];
    for row in client.query("SELECT id, trend_store_part_id, name, data_type, expression, extra_data, description FROM trend_directory.generated_table_trend", &[]).await.unwrap() {
	let trend = GeneratedTrend {
	    id: row.get(0),
	    trend_store_part: row.get(1),
	    name: row.get(2),
	    data_type: row.get(3),
	    expression: row.get(4),
	    extra_data: row.get(5),
	    description: row.get(6)
	};
	generated_trends.push(trend)
    };

    for row in client
        .query(
            "SELECT id, name, trend_store_id FROM trend_directory.trend_store_part",
            &[],
        )
        .await
        .unwrap()
    {
        let tspid: i32 = row.get(0);

        let mut my_trends: Vec<Trend> = vec![];
        for trend in &trends {
            if trend.trend_store_part == tspid {
                my_trends.push(trend.clone())
            }
        }

        let mut my_generated_trends: Vec<GeneratedTrend> = vec![];
        for generated_trend in &generated_trends {
            if generated_trend.trend_store_part == tspid {
                my_generated_trends.push(generated_trend.clone())
            }
        }

        let trendstorepart = TrendStorePart {
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

#[utoipa::path(
    responses(
	(status = 200, description = "Get a specific trend store part", body = TrendStorePart),
	(status = 404, description = "Trend store part not found", body = String)
    )
)]
#[get("/trend-store-parts/{id}")]
pub(super) async fn get_trend_store_part(
    pool: Data<Pool<PostgresConnectionManager<NoTls>>>,
    id: Path<i32>,
) -> impl Responder {
    let tsp_id = id.into_inner();

    let client = pool.get().await.unwrap();

    let query_result = client
        .query_one(
            "SELECT name, trend_store_id FROM trend_directory.trend_store_part WHERE id=$1",
            &[&tsp_id],
        )
        .await;

    match query_result {
        Ok(row) => {
            let mut trends: Vec<Trend> = vec![];
            for inner_row in client.query("SELECT id, trend_store_part_id, name, data_type, time_aggregation, entity_aggregation, extra_data, description FROM trend_directory.table_trend WHERE trend_store_part_id=$1", &[&tsp_id]).await.unwrap() {
		let trend = Trend {
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

            let mut generated_trends: Vec<GeneratedTrend> = vec![];
            for inner_row in client.query("SELECT id, trend_store_part_id, name, data_type, expression, extra_data, description FROM trend_directory.generated_table_trend WHERE trend_store_part_id=$1", &[&tsp_id]).await.unwrap() {
		let trend = GeneratedTrend {
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

            let trendstorepart = TrendStorePart {
                id: tsp_id,
                name: row.get(0),
                trend_store: row.get(1),
                trends: trends,
                generated_trends: generated_trends,
            };

            HttpResponse::Ok().json(trendstorepart)
        }
        Err(_e) => {
            HttpResponse::NotFound().body(format!("Trend store part with id {} not found", &tsp_id))
        }
    }
}

#[utoipa::path(
    responses(
	(status = 200, description = "List all trend store parts", body = [TrendStorePart])
    )
)]
#[get("/trend-stores")]
pub(super) async fn get_trend_stores(
    pool: Data<Pool<PostgresConnectionManager<NoTls>>>,
) -> impl Responder {
    let mut m: Vec<TrendStore> = vec![];

    let client = pool.get().await.unwrap();
    let mut trends: Vec<Trend> = vec![];
    for row in client.query("SELECT id, trend_store_part_id, name, data_type, time_aggregation, entity_aggregation, extra_data, description FROM trend_directory.table_trend", &[]).await.unwrap() {
	let trend = Trend {
	    id: row.get(0),
	    trend_store_part: row.get(1),
	    name: row.get(2),
	    data_type: row.get(3),
	    time_aggregation: row.get(4),
	    entity_aggregation: row.get(5),
	    extra_data: row.get(6),
	    description: row.get(7)
	};
	trends.push(trend)
    };

    let mut generated_trends: Vec<GeneratedTrend> = vec![];
    for row in client.query("SELECT id, trend_store_part_id, name, data_type, expression, extra_data, description FROM trend_directory.generated_table_trend", &[]).await.unwrap() {
	let trend = GeneratedTrend {
	    id: row.get(0),
	    trend_store_part: row.get(1),
	    name: row.get(2),
	    data_type: row.get(3),
	    expression: row.get(4),
	    extra_data: row.get(5),
	    description: row.get(6)
	};
	generated_trends.push(trend)
    };

    let mut parts: Vec<TrendStorePart> = vec![];

    for row in client
        .query(
            "SELECT id, name, trend_store_id FROM trend_directory.trend_store_part",
            &[],
        )
        .await
        .unwrap()
    {
        let tspid: i32 = row.get(0);

        let mut my_trends: Vec<Trend> = vec![];
        for trend in &trends {
            if trend.trend_store_part == tspid {
                my_trends.push(trend.clone())
            }
        }

        let mut my_generated_trends: Vec<GeneratedTrend> = vec![];
        for generated_trend in &generated_trends {
            if generated_trend.trend_store_part == tspid {
                my_generated_trends.push(generated_trend.clone())
            }
        }

        let trendstorepart = TrendStorePart {
            id: tspid,
            name: row.get(1),
            trend_store: row.get(2),
            trends: my_trends,
            generated_trends: my_generated_trends,
        };
        parts.push(trendstorepart)
    }

    for row in client.query("SELECT ts.id, entity_type.name, data_source.name, granularity::text, partition_size::text, retention_period::text FROM trend_directory.trend_store ts JOIN directory.entity_type ON ts.entity_type_id = entity_type.id JOIN directory.data_source ON ts.data_source_id = data_source.id", &[]).await.unwrap() {
	let tsid: i32 = row.get(0);
	let mut my_parts: Vec<TrendStorePart> = vec![];
	for part in &parts {
	    if part.trend_store == tsid {
		my_parts.push(part.clone())
	    }
	}

	let trendstore = TrendStore {
	    id: tsid,
	    entity_type: row.get(1),
	    data_source: row.get(2),
	    granularity: parse_interval(row.get(3)).unwrap(),
	    partition_size: parse_interval(row.get(4)).unwrap(),
	    retention_period: parse_interval(row.get(5)).unwrap(),
	    trend_store_parts: my_parts,
	};

	m.push(trendstore)
    };

    HttpResponse::Ok().json(m)
}

#[utoipa::path(
    responses(
	(status = 200, description = "Get a specific trend store", body = TrendStorePart),
	(status = 404, description = "Trend store not found", body = String)
    )
)]
#[get("/trend-stores/{id}")]
pub(super) async fn get_trend_store(
    pool: Data<Pool<PostgresConnectionManager<NoTls>>>,
    id: Path<i32>,
) -> impl Responder {
    let tsid = id.into_inner();

    let client = pool.get().await.unwrap();

    let query_result = client
        .query_one(
            "SELECT ts.id, entity_type.name, data_source.name, granularity::text, partition_size::text, retention_period::text FROM trend_directory.trend_store ts JOIN directory.entity_type ON ts.entity_type_id = entity_type.id JOIN directory.data_source ON ts.data_source_id = data_source.id WHERE ts.id = $1",
            &[&tsid],
        )
        .await;

    match query_result {
        Ok(row) => {
            let mut trends: Vec<Trend> = vec![];
            for inner_row in client.query("SELECT t.id, t.trend_store_part_id, t.name, t.data_type, t.time_aggregation, t.entity_aggregation, t.extra_data, t.description FROM trend_directory.table_trend t JOIN trend_directory.trend_store_part tsp ON t.trend_store_part_id = tsp.id WHERE tsp.trend_store_id = $1", &[&tsid]).await.unwrap() {
		let trend = Trend {
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
	    };

            let mut generated_trends: Vec<GeneratedTrend> = vec![];
            for inner_row in client.query("SELECT t.id, t.trend_store_part_id, t.name, t.data_type, t.expression, t.extra_data, t.description FROM trend_directory.generated_table_trend t JOIN trend_directory.trend_store_part tsp ON t.trend_store_part_id = tsp.id WHERE tsp.trend_store_id = $1", &[&tsid]).await.unwrap() {
		let trend = GeneratedTrend {
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

            let mut parts: Vec<TrendStorePart> = vec![];

            for inner_row in client
		.query(
		    "SELECT id, name, trend_store_id FROM trend_directory.trend_store_part WHERE trend_store_id = $1",
		    &[&tsid],
		)
		.await
		.unwrap()
	    {
		let tspid: i32 = inner_row.get(0);

		let mut my_trends: Vec<Trend> = vec![];
		for trend in &trends {
		    if trend.trend_store_part == tspid {
			my_trends.push(trend.clone())
		    }
		}

		let mut my_generated_trends: Vec<GeneratedTrend> = vec![];
		for generated_trend in &generated_trends {
		    if generated_trend.trend_store_part == tspid {
			my_generated_trends.push(generated_trend.clone())
		    }
		}

		let trendstorepart = TrendStorePart {
		    id: tspid,
		    name: inner_row.get(1),
		    trend_store: inner_row.get(2),
		    trends: my_trends,
		    generated_trends: my_generated_trends,
		};
		parts.push(trendstorepart)
	    }

            let trendstore = TrendStore {
                id: tsid,
                entity_type: row.get(1),
                data_source: row.get(2),
                granularity: parse_interval(row.get(3)).unwrap(),
                partition_size: parse_interval(row.get(4)).unwrap(),
                retention_period: parse_interval(row.get(5)).unwrap(),
                trend_store_parts: parts,
            };

            HttpResponse::Ok().json(trendstore)
        }
        Err(_e) => {
            HttpResponse::NotFound().body(format!("Trend store with id {} not found", &tsid))
        }
    }
}
