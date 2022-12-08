use std::path::PathBuf;

use structopt::StructOpt;
use async_trait::async_trait;

use minerva::change::Change;
use minerva::error::{Error, RuntimeError};
use minerva::trend_materialization;
use minerva::trend_materialization::{
    trend_materialization_from_config, AddTrendMaterialization, UpdateTrendMaterialization,
};

use super::common::{Cmd, CmdResult, connect_db};

#[derive(Debug, StructOpt)]
pub struct TrendMaterializationCreate {
    #[structopt(help = "trend materialization definition file")]
    definition: PathBuf,
}

#[async_trait]
impl Cmd for TrendMaterializationCreate {
    async fn run(&self) -> CmdResult {
        let trend_materialization =
            trend_materialization::trend_materialization_from_config(&self.definition)?;

        println!("Loaded definition, creating trend materialization");
        let mut client = connect_db().await?;

        let change = AddTrendMaterialization {
            trend_materialization,
        };

        let result = change.apply(&mut client).await;

        match result {
            Ok(_) => {
                println!("Created trend materialization");

                Ok(())
            }
            Err(e) => Err(Error::Runtime(RuntimeError {
                msg: format!("Error creating trend materialization: {}", e),
            })),
        }
    }
}

#[derive(Debug, StructOpt)]
pub struct TrendMaterializationUpdate {
    #[structopt(help = "trend materialization definition file")]
    definition: PathBuf,
}

#[async_trait]
impl Cmd for TrendMaterializationUpdate {
    async fn run(&self) -> CmdResult {
        let trend_materialization = trend_materialization_from_config(&self.definition)?;

        println!("Loaded definition, updating trend materialization");
        let mut client = connect_db().await?;

        let change = UpdateTrendMaterialization {
            trend_materialization,
        };

        let result = change.apply(&mut client).await;

        match result {
            Ok(_) => {
                println!("Updated trend materialization");

                Ok(())
            }
            Err(e) => Err(Error::Runtime(RuntimeError {
                msg: format!("Error updating trend materialization: {}", e),
            })),
        }
    }
}

#[derive(Debug, StructOpt)]
pub enum TrendMaterializationOpt {
    #[structopt(about = "create a trend materialization")]
    Create(TrendMaterializationCreate),
    #[structopt(about = "update a trend materialization")]
    Update(TrendMaterializationUpdate),
}

impl TrendMaterializationOpt {
    pub async fn run(&self) -> CmdResult {
        match self {
            TrendMaterializationOpt::Create(trend_materialization_create) => {
                trend_materialization_create.run().await
            }
            TrendMaterializationOpt::Update(trend_materialization_update) => {
                trend_materialization_update.run().await
            }
        }
    }
}