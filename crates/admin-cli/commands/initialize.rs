use std::env;
use std::path::PathBuf;

use structopt::StructOpt;
use async_trait::async_trait;

use minerva::error::{Error, ConfigurationError};
use minerva::instance::MinervaInstance;
use minerva::trend_store::{
    create_partitions,
};

use super::common::{Cmd, CmdResult, connect_db, ENV_MINERVA_INSTANCE_ROOT};

#[derive(Debug, StructOpt)]
pub struct InitializeOpt {
    #[structopt(long = "--create-partitions", help = "create partitions")]
    create_partitions: bool,
}

#[async_trait]
impl Cmd for InitializeOpt {
    async fn run(&self) -> CmdResult {
        let minerva_instance_root = match env::var(ENV_MINERVA_INSTANCE_ROOT) {
            Ok(v) => PathBuf::from(v),
            Err(e) => {
                return Err(Error::Configuration(ConfigurationError {
                    msg: format!(
                        "Environment variable '{}' could not be read: {}",
                        &ENV_MINERVA_INSTANCE_ROOT, e
                    ),
                }));
            }
        };

        let mut client = connect_db().await?;

        println!(
            "Initializing Minerva instance from {}",
            minerva_instance_root.to_string_lossy()
        );

        MinervaInstance::load_from(&minerva_instance_root)
            .initialize(&mut client)
            .await;

        if self.create_partitions {
            create_partitions(&mut client, None).await?;
        }

        Ok(())
    }
}