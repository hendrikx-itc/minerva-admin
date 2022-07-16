use std::fmt;

use postgres::Client;
use super::error::Error;

pub trait Change: fmt::Display {
    fn apply(&self, client: &mut Client) -> Result<String, Error>;
}
