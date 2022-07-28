use bb8::Pool;
use bb8_postgres::{tokio_postgres::NoTls, PostgresConnectionManager};

use actix_web::{get, web::Data, web::Path, HttpResponse, Responder};

use serde::{Deserialize, Serialize};
use utoipa::Component;

#[derive(Debug, Serialize, Deserialize, Clone, Component)]
pub struct EntityType {
    pub id: i32,
    pub name: String,
    pub description: String,
}

#[utoipa::path(
    responses(
	(status = 200, description = "List all entity types", body = [EntityType])
    )
)]
#[get("/entity-types")]
pub(super) async fn get_entity_types(
    pool: Data<Pool<PostgresConnectionManager<NoTls>>>,
) -> impl Responder {
    let mut m: Vec<EntityType> = vec![];
    let client = pool.get().await.unwrap();

    for row in client
        .query(
            "SELECT id, name, description FROM directory.entity_type",
            &[],
        )
        .await
        .unwrap()
    {
        let entitytype = EntityType {
            id: row.get(0),
            name: row.get(1),
            description: row.get(2),
        };

        m.push(entitytype)
    }
    HttpResponse::Ok().json(m)
}

#[utoipa::path(
    responses(
	(status = 200, description = "Get a specific entity type", body = TrendStorePart),
	(status = 404, description = "Entity type not found", body = String)
    )
)]
#[get("/entity-types/{id}")]
pub(super) async fn get_entity_type(
    pool: Data<Pool<PostgresConnectionManager<NoTls>>>,
    id: Path<i32>,
) -> impl Responder {
    let et_id = id.into_inner();

    let client = pool.get().await.unwrap();

    let query_result = client
        .query_one(
            "SELECT id, name, description FROM directory.entity_type WHERE id=$1",
            &[&et_id],
        )
        .await;

    match query_result {
        Ok(row) => {
            let entitytype = EntityType {
                id: row.get(0),
                name: row.get(1),
                description: row.get(2),
            };

            HttpResponse::Ok().json(entitytype)
        }
        Err(_e) => {
            HttpResponse::NotFound().body(format!("Entity type with id {} not found", &et_id))
        }
    }
}
