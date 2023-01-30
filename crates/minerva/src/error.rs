use std::fmt;

use tokio_postgres;

#[derive(Debug)]
pub struct DatabaseError {
    pub msg: String,
}

impl DatabaseError {
    pub fn from_msg(msg: String) -> DatabaseError {
        DatabaseError { msg }
    }
}

impl From<tokio_postgres::Error> for DatabaseError {
    fn from(err: tokio_postgres::Error) -> DatabaseError {
        DatabaseError {
            msg: format!("{err}"),
        }
    }
}

#[derive(Debug)]
pub struct ConfigurationError {
    pub msg: String,
}

impl ConfigurationError {
    pub fn from_msg(msg: String) -> ConfigurationError {
        ConfigurationError { msg }
    }
}

#[derive(Debug)]
pub struct RuntimeError {
    pub msg: String,
}

impl RuntimeError {
    pub fn from_msg(msg: String) -> RuntimeError {
        RuntimeError { msg }
    }
}

#[derive(Debug)]
pub enum Error {
    Database(DatabaseError),
    Configuration(ConfigurationError),
    Runtime(RuntimeError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Database(e) => write!(f, "{}", &e.msg),
            Error::Configuration(e) => write!(f, "{}", &e.msg),
            Error::Runtime(e) => write!(f, "{}", &e.msg),
        }
    }
}

impl From<DatabaseError> for Error {
    fn from(err: DatabaseError) -> Error {
        Error::Database(err)
    }
}

impl From<ConfigurationError> for Error {
    fn from(err: ConfigurationError) -> Error {
        Error::Configuration(err)
    }
}

impl From<RuntimeError> for Error {
    fn from(err: RuntimeError) -> Error {
        Error::Runtime(err)
    }
}

impl From<tokio_postgres::Error> for Error {
    fn from(err: tokio_postgres::Error) -> Error {
        Error::Database(DatabaseError {
            msg: format!("{err}"),
        })
    }
}
