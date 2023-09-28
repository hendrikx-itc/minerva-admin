use deadpool_postgres::Pool;
use std::ops::DerefMut;
use utoipa::{ToSchema};

use actix_web::{get, web::Data, HttpResponse};

use minerva::entity_set::load_entity_sets;

use super::serviceerror::{ServiceError, ServiceErrorKind};
use crate::error::Error;


#[utoipa::path(
    get,
    path="/entitysets",
    responses(
    (status = 200, description = "List of existing triggers", body = [TriggerData]),
    (status = 500, description = "Database unreachable", body = Error),
    )
)]
#[get("/entitysets")]
pub(super) async fn get_entity_sets(pool: Data<Pool>) -> Result<HttpResponse, ServiceError> {
    let mut manager = pool.get().await.map_err(|_| ServiceError {
        kind: ServiceErrorKind::PoolError,
        message: "".to_string(),
    })?;

    let mut client: &mut tokio_postgres::Client = manager.deref_mut().deref_mut();

    let data = load_entity_sets(&mut client).await.map_err(|e| Error { code: 500, message: e.to_string() } )?;

    Ok(HttpResponse::Ok().json(data))
}

