use deadpool_postgres::Pool;
use std::ops::DerefMut;

use actix_web::{get, web::Data, HttpResponse};

use serde::{Deserialize, Serialize};
use utoipa::{ToSchema};

use minerva::trigger::{list_triggers, load_thresholds_with_client, Threshold};

use super::serviceerror::{ServiceError, ServiceErrorKind};
use crate::error::{Error, Success};

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct TriggerData {
    name: String,
    enabled: bool,
    thresholds: Vec<Threshold>
}

#[utoipa::path(
    get,
    path="/triggers",
    responses(
    (status = 200, description = "List of existing triggers", body = [TriggerData]),
    (status = 500, description = "Database unreachable", body = Error),
    )
)]
#[get("/triggers")]
pub(super) async fn get_triggers(pool: Data<Pool>) -> Result<HttpResponse, ServiceError> {
    let mut manager = pool.get().await.map_err(|_| ServiceError {
        kind: ServiceErrorKind::PoolError,
        message: "".to_string(),
    })?;

    let mut client: &mut tokio_postgres::Client = manager.deref_mut().deref_mut();

    let triggerdata = list_triggers(&mut client).await.map_err(|e| Error { code: 500, message: e.to_string() } )?;

    let mut result: Vec<TriggerData> = [].to_vec();

    for trigger in triggerdata.iter() {
        let thresholds = load_thresholds_with_client(&mut client, &trigger.0).await.map_err(|e| Error { code: 500, message: e.to_string() })?;
        result.push(
            TriggerData {
                name: trigger.0.clone(),
                enabled: trigger.5,
                thresholds: thresholds
            }
        )
    }

    Ok(HttpResponse::Ok().json(result))
}
