use std::env;
use std::path::PathBuf;

use async_trait::async_trait;
use structopt::StructOpt;

use minerva::error::{ConfigurationError, Error};
use minerva::instance::MinervaInstance;
use minerva::trend_store::create_partitions;

use super::common::{connect_db, Cmd, CmdResult, ENV_MINERVA_INSTANCE_ROOT};

#[derive(Debug, StructOpt)]
pub struct InitializeOpt {
    #[structopt(long = "--create-partitions", help = "create partitions")]
    create_partitions: bool,
    #[structopt(parse(from_os_str), help = "Minerva instance root directory")]
    instance_root: Option<PathBuf>,
}

#[async_trait]
impl Cmd for InitializeOpt {
    async fn run(&self) -> CmdResult {
        let minerva_instance_root = match &self.instance_root {
            Some(root) => {
                // Next to passing on the Minerva instance root directory, we need to set the
                // environment variable for any child processes that might be started during
                // initialization.
                std::env::set_var(&ENV_MINERVA_INSTANCE_ROOT, &root);

                root.clone()
            },
            None => match env::var(ENV_MINERVA_INSTANCE_ROOT) {
                Ok(v) => PathBuf::from(v),
                Err(e) => {
                    return Err(Error::Configuration(ConfigurationError {
                        msg: format!(
                            "Environment variable '{}' could not be read: {}",
                            &ENV_MINERVA_INSTANCE_ROOT, e
                        ),
                    }));
                }
            },
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
