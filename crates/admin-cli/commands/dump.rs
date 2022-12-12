use async_trait::async_trait;
use structopt::StructOpt;

use minerva::instance::dump;

use super::common::{connect_db, Cmd, CmdResult};

#[derive(Debug, StructOpt)]
pub struct DumpOpt {}

#[async_trait]
impl Cmd for DumpOpt {
    async fn run(&self) -> CmdResult {
        let mut client = connect_db().await?;

        dump(&mut client).await;

        Ok(())
    }
}
