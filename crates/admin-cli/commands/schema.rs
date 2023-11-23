use async_trait::async_trait;
use structopt::StructOpt;

use minerva::schema::schema;

use super::common::{Cmd, CmdResult};

#[derive(Debug, StructOpt)]
pub struct SchemaOpt {}

#[async_trait]
impl Cmd for SchemaOpt {
    async fn run(&self) -> CmdResult {
        print!("{}", schema());

        Ok(())
    }
}
