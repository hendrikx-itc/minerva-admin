use core::fmt::Display;
use actix_web::error::ResponseError;
use actix_web::{HttpResponse, http::StatusCode};
use derive_more::{Display, From};
use log::error;

use super::error::Error;

#[derive(Display, From, Debug)]
pub enum ServiceErrorKind {
    NotFound,
    PoolError,
    DbError,
    BadRequest,
    InternalError,
}

#[derive(From, Debug)]
pub struct ServiceError {
    pub kind: ServiceErrorKind,
    pub message: String,
}

impl Display for ServiceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}) {}", self.kind, self.message)
    }
}

impl std::error::Error for ServiceError {}

impl ResponseError for ServiceError {
    fn status_code(&self) -> StatusCode {
        match self.kind {
            ServiceErrorKind::NotFound => StatusCode::NOT_FOUND,
            ServiceErrorKind::PoolError => StatusCode::INTERNAL_SERVER_ERROR,
            ServiceErrorKind::DbError => StatusCode::INTERNAL_SERVER_ERROR,
            ServiceErrorKind::BadRequest => StatusCode::BAD_REQUEST,
            ServiceErrorKind::InternalError => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_response(&self) -> HttpResponse {
        let status_code = self.status_code();

        HttpResponse::build(status_code)
            .json(Error { code: i32::from(status_code.as_u16()), message: self.to_string() })
    }
}

impl From<tokio_postgres::error::Error> for ServiceError {
    fn from(value: tokio_postgres::error::Error) -> ServiceError {
        error!("{value:?}");

        ServiceError { kind: ServiceErrorKind::DbError, message: format!("{value}") }
    }
}

impl From<Error> for ServiceError {
    fn from(value: Error) -> ServiceError {
        error!("{value:?}");

        ServiceError { kind: ServiceErrorKind::InternalError, message: format!("{value:?}") }
    }
}
