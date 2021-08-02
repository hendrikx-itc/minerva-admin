use std::fmt;

use postgres::Client;

pub trait Change: fmt::Display {
    fn apply(&self, client: &mut Client) -> Result<String, String>;
}
