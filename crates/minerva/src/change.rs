use std::fmt;
use std::any::Any;

use super::error::Error;
use postgres::Client;

pub trait Change: fmt::Display {
    fn apply(&self, client: &mut Client) -> Result<String, Error>;
    fn as_any(&self) -> &dyn Any;
}
