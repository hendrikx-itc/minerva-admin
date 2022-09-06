use std::fmt;

use super::error::Error;
use async_trait::async_trait;
use tokio_postgres::Client;

pub type ChangeResult = Result<String, Error>;

#[async_trait]
pub trait ChangeStep: fmt::Display + Send + Sync {
    async fn apply(&self, client: &mut Client) -> ChangeResult;
}

#[async_trait]
pub trait Change: fmt::Display + Send + Sync {
    /// Apply the change to the database
    async fn create_steps(
        &self,
        client: &mut Client,
    ) -> Result<Vec<Box<dyn ChangeStep + Send>>, Error>;
}
