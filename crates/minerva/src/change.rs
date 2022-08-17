use std::fmt;
use std::future::Future;

use super::error::Error;
use tokio_postgres::Client;

pub type ChangeResult = Result<String, Error>;

pub type Change<R> = fn(&mut Client) -> R;

pub trait Change: fmt::Display {
    type ChangeResultType: Future<Output = ChangeResult>; 

    fn apply(&self, client: &mut Client) -> Self::ChangeResultType;
}
