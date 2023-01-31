use actix_web::error::ResponseError;
use actix_web::HttpResponse;
use derive_more::{Display, From};

use super::error::{BadRequest, Error};

#[derive(Display, From, Debug)]
pub enum ServiceError {
    NotFound,
    PoolError,
    DbError(String),
    BadRequest(BadRequest),
}

impl std::error::Error for ServiceError {}

impl ResponseError for ServiceError {
    fn error_response(&self) -> HttpResponse {
        match self {
            ServiceError::NotFound => HttpResponse::NotFound().finish(),
            ServiceError::PoolError => {
                HttpResponse::InternalServerError().body("pool error".to_string())
            }
            ServiceError::DbError(msg) => HttpResponse::InternalServerError().body(msg.clone()),
            ServiceError::BadRequest(req) => HttpResponse::BadRequest().json(Error {
                code: 400,
                message: req.message.clone(),
            }),
            _ => HttpResponse::InternalServerError().finish(),
        }
    }
}

impl From<tokio_postgres::error::Error> for ServiceError {
    fn from(value: tokio_postgres::error::Error) -> ServiceError {
        ServiceError::DbError(format!("{value:?}"))
    }
}

impl From<Error> for ServiceError {
    fn from(value: Error) -> ServiceError {
        ServiceError::DbError(format!("{value:?}"))
    }
}
