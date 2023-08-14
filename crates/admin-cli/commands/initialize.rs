use std::env;
use std::path::PathBuf;

use async_trait::async_trait;
use structopt::StructOpt;

use minerva::database::create_database;
use minerva::error::{ConfigurationError, Error};
use minerva::instance::MinervaInstance;
use minerva::schema::create_schema;
use minerva::trend_store::create_partitions;

use super::common::{
    connect_db, connect_to_db, get_db_config, Cmd, CmdResult, ENV_MINERVA_INSTANCE_ROOT,
};

#[derive(Debug, StructOpt)]
pub struct InitializeOpt {
    #[structopt(long = "--create-schema", help = "create Minerva schema")]
    create_schema: bool,
    #[structopt(long = "--create-database", help = "create Minerva database")]
    database_name: Option<String>,
    #[structopt(long = "--create-partitions", help = "create partitions")]
    create_partitions: bool,
    #[structopt(
        long = "--with-definition",
        parse(from_os_str),
        help = "Minerva instance definition root directory"
    )]
    instance_root: Option<PathBuf>,
}

#[async_trait]
impl Cmd for InitializeOpt {
    async fn run(&self) -> CmdResult {
        let minerva_instance_root: Option<PathBuf> = match &self.instance_root {
            Some(root) => Some(root.clone()),
            None => match env::var(ENV_MINERVA_INSTANCE_ROOT) {
                Ok(v) => Some(PathBuf::from(v)),
                Err(_) => None,
            },
        };

        let mut client = connect_db().await?;

        if let Some(database_name) = &self.database_name {
            create_database(&mut client, &database_name)
                .await
                .map_err(|e| {
                    Error::Database(minerva::error::DatabaseError::from_msg(format!(
                        "Could not create database '': {e}"
                    )))
                })?;

            // Let connection use newly created database
            let mut config = get_db_config()?;
            config.dbname(database_name);
            client = connect_to_db(&config).await?;

            std::env::set_var("PGDATABASE", &database_name);
        }

        if self.create_schema {
            create_schema(&mut client).await.map_err(|e| {
                Error::Database(minerva::error::DatabaseError::from_msg(format!(
                    "Could not create schema: {e}"
                )))
            })?;
        }

        if let Some(root) = minerva_instance_root {
            if !root.is_dir() {
                return Err(Error::Configuration(ConfigurationError::from_msg(format!(
                    "Not a valid directory: '{}'",
                    &root.to_string_lossy()
                ))));
            }

            println!(
                "Initializing Minerva instance from {}",
                root.to_string_lossy()
            );

            // We need to set the environment variable for any child processes that might be
            // started during initialization.
            std::env::set_var(&ENV_MINERVA_INSTANCE_ROOT, &root);

            MinervaInstance::load_from(&root)
                .initialize(&mut client)
                .await;
        }

        if self.create_partitions {
            create_partitions(&mut client, None).await?;
        }

        Ok(())
    }
}
