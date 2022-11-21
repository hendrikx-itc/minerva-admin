use bb8::Pool;
use bb8_postgres::{tokio_postgres::NoTls, PostgresConnectionManager};

use actix_web::{get, web::Data, web::Path, HttpResponse, Responder};

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::error::Error;

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct DataSource {
    pub id: i32,
    pub name: String,
    pub description: String,
}

#[utoipa::path(
    get,
    path="/data-sources",
    responses(
	(status = 200, description = "List all data sources", body = [DataSource]),
	(status = 500, description = "Problem interacting with database", body = Error)
    )
)]
#[get("/data-sources")]
pub(super) async fn get_data_sources(
    pool: Data<Pool<PostgresConnectionManager<NoTls>>>,
) -> impl Responder {
    let mut m: Vec<DataSource> = vec![];
    let result = pool.get().await;
    match result {
        Err(e) => HttpResponse::InternalServerError().json(Error {
            code: 500,
            message: e.to_string(),
        }),
        Ok(client) => {
            let query = client
                .query(
                    "SELECT id, name, description FROM directory.data_source",
                    &[],
                )
                .await;
            match query {
                Err(e) => HttpResponse::InternalServerError().json(Error {
                    code: 500,
                    message: e.to_string(),
                }),
                Ok(query_result) => {
                    for row in query_result {
                        let datasource = DataSource {
                            id: row.get(0),
                            name: row.get(1),
                            description: row.get(2),
                        };
                        m.push(datasource)
                    }
                    HttpResponse::Ok().json(m)
                }
            }
        }
    }
}

#[utoipa::path(
    get,
    path="/data-sources/{id}",
    responses(
	(status = 200, description = "Get a specific data source", body = TrendStorePart),
	(status = 404, description = "Data source not found", body = Error),
	(status = 500, description = "Problem interacting with database", body = Error),
    )
)]
#[get("/data-sources/{id}")]
pub(super) async fn get_data_source(
    pool: Data<Pool<PostgresConnectionManager<NoTls>>>,
    id: Path<i32>,
) -> impl Responder {
    let ds_id = id.into_inner();

    let result = pool.get().await;
    match result {
        Err(e) => HttpResponse::InternalServerError().json(Error {
            code: 500,
            message: e.to_string(),
        }),
        Ok(client) => {
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
                Err(_e) => HttpResponse::NotFound().json(Error {
                    code: 404,
                    message: format!("Data source with id {} not found", &ds_id),
                }),
            }
        }
    }
}
