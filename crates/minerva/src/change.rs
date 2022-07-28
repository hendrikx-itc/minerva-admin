use std::fmt;

use super::error::Error;
use postgres::Client;

pub trait Change: fmt::Display {
    fn apply(&self, client: &mut Client) -> Result<String, Error>;
}
