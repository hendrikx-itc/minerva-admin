use bb8::Pool;
use bb8_postgres::{tokio_postgres::NoTls, PostgresConnectionManager};

use actix_web::{get, web::Data, web::Path, HttpResponse, Responder};

use serde::{Deserialize, Serialize};
use utoipa::Component;

#[derive(Debug, Serialize, Deserialize, Clone, Component)]
pub struct DataSource {
    pub id: i32,
    pub name: String,
    pub description: String,
}

#[utoipa::path(
    responses(
	(status = 200, description = "List all data sources", body = [DataSource])
    )
)]
#[get("/data-sources")]
pub(super) async fn get_data_sources(
    pool: Data<Pool<PostgresConnectionManager<NoTls>>>,
) -> impl Responder {
    let mut m: Vec<DataSource> = vec![];
    let client = pool.get().await.unwrap();

    for row in client
        .query(
            "SELECT id, name, description FROM directory.data_source",
            &[],
        )
        .await
        .unwrap()
    {
        let datasource = DataSource {
            id: row.get(0),
            name: row.get(1),
            description: row.get(2),
        };

        m.push(datasource)
    }
    HttpResponse::Ok().json(m)
}

#[utoipa::path(
    responses(
	(status = 200, description = "Get a specific data source", body = TrendStorePart),
	(status = 404, description = "Data source not found", body = String)
    )
)]
#[get("/data-sources/{id}")]
pub(super) async fn get_data_source(
    pool: Data<Pool<PostgresConnectionManager<NoTls>>>,
    id: Path<i32>,
) -> impl Responder {
    let ds_id = id.into_inner();

    let client = pool.get().await.unwrap();

    let query_result = client
        .query_one(
            "SELECT id, name, description FROM directory.data_source WHERE id=$1",
            &[&ds_id],
        )
        .await;

    match query_result {
        Ok(row) => {
            let datasource = DataSource {
                id: row.get(0),
                name: row.get(1),
                description: row.get(2),
            };

            HttpResponse::Ok().json(datasource)
        }
        Err(_e) => {
            HttpResponse::NotFound().body(format!("Data source with id {} not found", &ds_id))
        }
    }
}
