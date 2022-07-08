use std::fmt;
use std::any::Any;

use postgres::Client;
use super::error::Error;

pub trait Change: fmt::Display {
    fn apply(&self, client: &mut Client) -> Result<String, Error>;
    fn as_any(&self) -> &dyn Any;
}
